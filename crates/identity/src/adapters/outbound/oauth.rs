//! OAuth2 provider adapter.
//!
//! External token exchange sits behind the [`OAuthProvider`] port so the flow is testable
//! without a live provider or an HTTP client in the dependency tree. [`StaticOAuthProvider`] is
//! a **clearly-marked stand-in**: it builds a real, correctly-shaped authorization URL (with
//! `state`, `code_challenge`, and `S256` method) and, on exchange, derives a deterministic
//! provider identity from the authorization code.
//!
//! Swapping in a real provider is purely additive: implement [`OAuthProvider`] with an HTTP
//! client that POSTs to the provider's token endpoint and GETs the userinfo endpoint. The
//! application layer is unchanged.

use async_trait::async_trait;

use crate::application::ports::{OAuthProvider, OAuthUserInfo, PortError};
use crate::domain::oauth::PKCE_METHOD;

/// A configurable, HTTP-free OAuth provider stand-in (GitHub-style generic provider).
pub struct StaticOAuthProvider {
    provider: String,
    authorize_endpoint: String,
    client_id: String,
    redirect_uri: String,
}

impl StaticOAuthProvider {
    /// Construct with the provider key and endpoint/client details.
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        authorize_endpoint: impl Into<String>,
        client_id: impl Into<String>,
        redirect_uri: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            authorize_endpoint: authorize_endpoint.into(),
            client_id: client_id.into(),
            redirect_uri: redirect_uri.into(),
        }
    }

    /// A sensible default (a generic GitHub-style provider) for local/dev.
    #[must_use]
    pub fn github_style(redirect_uri: impl Into<String>) -> Self {
        Self::new(
            "github",
            "https://github.com/login/oauth/authorize",
            "identity-local-client",
            redirect_uri,
        )
    }
}

#[async_trait]
impl OAuthProvider for StaticOAuthProvider {
    fn provider(&self) -> &str {
        &self.provider
    }

    fn authorization_url(&self, state: &str, code_challenge: &str) -> String {
        format!(
            "{base}?response_type=code&client_id={client}&redirect_uri={redirect}\
             &scope=read:user%20user:email&state={state}\
             &code_challenge={challenge}&code_challenge_method={method}",
            base = self.authorize_endpoint,
            client = self.client_id,
            redirect = self.redirect_uri,
            state = state,
            challenge = code_challenge,
            method = PKCE_METHOD,
        )
    }

    async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<OAuthUserInfo, PortError> {
        // A real adapter would POST {code, code_verifier, client_id, redirect_uri} to the token
        // endpoint, then fetch userinfo. The stand-in validates presence and synthesizes a
        // stable identity from the code so the callback flow (link/create user) is exercisable.
        if code.is_empty() {
            return Err(PortError::Provider("missing authorization code".into()));
        }
        if code_verifier.is_empty() {
            return Err(PortError::Provider("missing PKCE code_verifier".into()));
        }
        Ok(OAuthUserInfo {
            provider: self.provider.clone(),
            subject: format!("ext-{code}"),
            email: format!("{code}@users.noreply.{}.local", self.provider),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn authorization_url_carries_pkce_and_state() {
        let p = StaticOAuthProvider::github_style("http://localhost:8081/oauth/callback");
        let url = p.authorization_url("st4te", "chAllenge");
        assert!(url.contains("state=st4te"));
        assert!(url.contains("code_challenge=chAllenge"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("response_type=code"));
    }

    #[tokio::test]
    async fn exchange_requires_code_and_verifier() {
        let p = StaticOAuthProvider::github_style("http://localhost/cb");
        assert!(p.exchange_code("", "v").await.is_err());
        assert!(p.exchange_code("c", "").await.is_err());
        let info = p.exchange_code("abc", "verifier").await.unwrap();
        assert_eq!(info.provider, "github");
        assert_eq!(info.subject, "ext-abc");
    }
}
