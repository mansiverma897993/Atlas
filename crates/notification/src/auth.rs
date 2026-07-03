//! Connection authentication: verifying the JWT presented at the WebSocket handshake.
//!
//! Defense-in-depth (ARCHITECTURE §2): even though the gateway terminates and verifies auth,
//! the notification service **re-verifies** the token on upgrade before registering a socket.
//!
//! The [`TokenVerifier`] trait is the seam: production wires a [`JwtVerifier`] that validates an
//! **RS256** signature against a public key (PEM from env `APP__JWT__PUBLIC_KEY_PEM`), while
//! tests can substitute an in-memory fake. The verified subject (`sub`) becomes the user id the
//! connection is registered under.

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use thiserror::Error;

/// Reasons a token is rejected or a verifier cannot be built.
#[derive(Debug, Error)]
pub enum AuthError {
    /// The token was missing, malformed, expired, or its signature/claims failed validation.
    #[error("invalid token: {0}")]
    Invalid(String),
    /// The verifier could not be constructed (e.g. a bad public key).
    #[error("verifier misconfigured: {0}")]
    Config(String),
}

/// The subset of JWT claims this service needs. Extra claims are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
    /// Subject — the authenticated user id. Connections register under this.
    pub sub: String,
    /// Expiry (seconds since epoch). Present so [`jsonwebtoken`] enforces `validate_exp`;
    /// we do not read it directly.
    #[serde(default)]
    #[allow(dead_code)]
    pub exp: usize,
}

/// **Seam:** verify a bearer token and return its claims. Implemented by [`JwtVerifier`]; faked
/// in tests.
pub trait TokenVerifier: Send + Sync {
    /// Verify `token`, returning the decoded [`Claims`] on success.
    fn verify(&self, token: &str) -> Result<Claims, AuthError>;
}

/// RS256 JWT verifier backed by a static PEM public key.
pub struct JwtVerifier {
    key: DecodingKey,
    validation: Validation,
}

impl JwtVerifier {
    /// Build a verifier from an RSA public key in PEM form, optionally pinning `issuer`/
    /// `audience`. When `audience` is `None`, audience validation is disabled.
    pub fn from_rsa_pem(
        pem: &[u8],
        issuer: Option<&str>,
        audience: Option<&str>,
    ) -> Result<Self, AuthError> {
        let key = DecodingKey::from_rsa_pem(pem).map_err(|e| AuthError::Config(e.to_string()))?;
        let mut validation = Validation::new(Algorithm::RS256);
        if let Some(iss) = issuer {
            validation.set_issuer(&[iss]);
        }
        match audience {
            Some(aud) => validation.set_audience(&[aud]),
            None => validation.validate_aud = false,
        }
        Ok(Self { key, validation })
    }
}

impl TokenVerifier for JwtVerifier {
    fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        let data = decode::<Claims>(token, &self.key, &self.validation)
            .map_err(|e| AuthError::Invalid(e.to_string()))?;
        Ok(data.claims)
    }
}
