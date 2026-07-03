//! JWT (RS256) verification at the edge (ARCHITECTURE §6.3, ADR-0009).
//!
//! Access tokens are RS256 JWTs signed by Identity. The gateway verifies them against
//! Identity's **public** signing keys. The [`KeySource`] trait is the seam over *where* those
//! keys come from:
//!
//! * [`JwksKeySource`] — the **production** path: an in-memory cache of keys fetched from the
//!   issuer's JWKS endpoint (`cfg.jwt.jwks_url`), selected by the token's `kid`. Refreshing
//!   the cache requires an HTTP client; to keep the workspace on a pure-Rust, no-C-toolchain
//!   dependency set (no `reqwest`), the fetch itself is left as an injection point
//!   ([`JwksKeySource::load_from_json`]) — a production build wires an HTTP fetcher (or a
//!   sidecar-mounted JWKS file) to call it. The JWK→key conversion is implemented and tested.
//! * [`StaticPemKeySource`] — an operational fallback used when `APP__JWT__PUBLIC_KEY_PEM`
//!   supplies the RSA public key directly (handy for local/dev and for environments where the
//!   key is delivered as a mounted secret rather than fetched).
//!
//! Both satisfy the same trait, so the verifier and the rest of the gateway are agnostic to
//! the choice.

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use thiserror::Error;

/// Verified access-token claims (a subset of Identity's `Claims` message, §identity.proto).
#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
    /// Subject — the user id.
    pub sub: String,
    /// Roles held by the subject. Part of the token contract (used for auditing/logging);
    /// RBAC decisions are made on `permissions`.
    #[serde(default)]
    #[allow(dead_code)]
    pub roles: Vec<String>,
    /// Fine-grained permissions (RBAC is enforced against these).
    #[serde(default)]
    pub permissions: Vec<String>,
    /// Expiry (unix seconds). Validated by `jsonwebtoken` during decode.
    #[serde(default)]
    #[allow(dead_code)]
    pub exp: usize,
    /// Issuer. Validated during decode when configured.
    #[serde(default)]
    #[allow(dead_code)]
    pub iss: Option<String>,
}

/// Errors from token verification.
#[derive(Debug, Error)]
pub enum AuthError {
    /// The token could not be parsed/verified (bad signature, expired, wrong iss/aud, ...).
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// No key matched the token's `kid` (JWKS cache miss / not yet loaded).
    #[error("no verification key for kid {0:?}")]
    UnknownKey(Option<String>),
    /// A supplied key material was malformed.
    #[error("invalid key material: {0}")]
    InvalidKey(String),
}

/// Source of RSA public keys used to verify tokens. The verification seam.
#[async_trait]
pub trait KeySource: Send + Sync {
    /// Resolve the decoding key for a token's `kid` (or the sole key if `kid` is absent).
    async fn key_for(&self, kid: Option<&str>) -> Result<DecodingKey, AuthError>;
}

/// A single RSA public key from a JWKS document.
///
/// Part of the JWKS production seam (see module docs); constructed by the fetcher wiring.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Jwk {
    /// Key id.
    pub kid: String,
    /// RSA modulus (base64url).
    pub n: String,
    /// RSA exponent (base64url).
    pub e: String,
    /// Key type (expected `RSA`).
    #[serde(default)]
    pub kty: String,
}

/// A JWKS document (`{"keys":[...]}`).
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Jwks {
    /// The keys in the set.
    pub keys: Vec<Jwk>,
}

/// Convert a JWK into a `jsonwebtoken` decoding key.
#[allow(dead_code)]
fn jwk_to_key(jwk: &Jwk) -> Result<DecodingKey, AuthError> {
    if !jwk.kty.is_empty() && jwk.kty != "RSA" {
        return Err(AuthError::InvalidKey(format!(
            "unsupported key type '{}' (only RSA)",
            jwk.kty
        )));
    }
    DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
        .map_err(|e| AuthError::InvalidKey(e.to_string()))
}

/// The production key source: a `kid`-indexed, in-memory cache of JWKS keys.
///
/// The cache is populated via [`load_from_json`](Self::load_from_json) — the injection point a
/// production HTTP fetcher (or a mounted JWKS file watcher) calls to (re)load keys from
/// `cfg.jwt.jwks_url`. Reads are lock-free-ish (an `RwLock` read) and cheap.
pub struct JwksKeySource {
    keys: RwLock<HashMap<String, DecodingKey>>,
}

impl JwksKeySource {
    /// An empty cache (no keys yet loaded).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
        }
    }

    /// Build a cache pre-loaded from a parsed JWKS.
    #[allow(dead_code)]
    pub fn from_jwks(jwks: &Jwks) -> Result<Self, AuthError> {
        let source = Self::empty();
        source.load(jwks)?;
        Ok(source)
    }

    /// (Re)load the cache from a raw JWKS JSON document. This is the seam an HTTP fetcher calls.
    #[allow(dead_code)]
    pub fn load_from_json(&self, json: &str) -> Result<(), AuthError> {
        let jwks: Jwks =
            serde_json::from_str(json).map_err(|e| AuthError::InvalidKey(e.to_string()))?;
        self.load(&jwks)
    }

    /// Replace the cache contents with the given keys.
    #[allow(dead_code)]
    pub fn load(&self, jwks: &Jwks) -> Result<(), AuthError> {
        let mut map = HashMap::with_capacity(jwks.keys.len());
        for jwk in &jwks.keys {
            map.insert(jwk.kid.clone(), jwk_to_key(jwk)?);
        }
        *self.keys.write().expect("jwks lock poisoned") = map;
        Ok(())
    }
}

#[async_trait]
impl KeySource for JwksKeySource {
    async fn key_for(&self, kid: Option<&str>) -> Result<DecodingKey, AuthError> {
        let keys = self.keys.read().expect("jwks lock poisoned");
        match kid {
            Some(kid) => keys
                .get(kid)
                .cloned()
                .ok_or_else(|| AuthError::UnknownKey(Some(kid.to_string()))),
            // No kid: only unambiguous if exactly one key is cached.
            None if keys.len() == 1 => Ok(keys.values().next().cloned().expect("len==1")),
            None => Err(AuthError::UnknownKey(None)),
        }
    }
}

/// A fixed RSA public key parsed from PEM (operational fallback, see module docs).
pub struct StaticPemKeySource {
    key: DecodingKey,
}

impl StaticPemKeySource {
    /// Parse an RSA public key from PEM.
    pub fn from_pem(pem: &str) -> Result<Self, AuthError> {
        let key = DecodingKey::from_rsa_pem(pem.as_bytes())
            .map_err(|e| AuthError::InvalidKey(e.to_string()))?;
        Ok(Self { key })
    }
}

#[async_trait]
impl KeySource for StaticPemKeySource {
    async fn key_for(&self, _kid: Option<&str>) -> Result<DecodingKey, AuthError> {
        Ok(self.key.clone())
    }
}

/// Verifies RS256 access tokens against a [`KeySource`], enforcing issuer/audience/expiry.
pub struct JwtVerifier {
    source: Box<dyn KeySource>,
    validation: Validation,
}

impl JwtVerifier {
    /// Build a verifier. `issuer`/`audience` are checked against the token's `iss`/`aud`
    /// claims (empty values disable the respective check).
    #[must_use]
    pub fn new(source: Box<dyn KeySource>, issuer: &str, audience: &str) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);
        if !issuer.is_empty() {
            validation.set_issuer(&[issuer]);
        }
        if audience.is_empty() {
            validation.validate_aud = false;
        } else {
            validation.set_audience(&[audience]);
        }
        Self { source, validation }
    }

    /// Verify a raw token string, returning its claims on success.
    pub async fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        let header = decode_header(token).map_err(|e| AuthError::InvalidToken(e.to_string()))?;
        let key = self.source.key_for(header.kid.as_deref()).await?;
        let data = decode::<Claims>(token, &key, &self.validation)
            .map_err(|e| AuthError::InvalidToken(e.to_string()))?;
        Ok(data.claims)
    }
}

/// Extract a bearer token from an `Authorization: Bearer <token>` header value.
#[must_use]
pub fn bearer_token(header_value: Option<&str>) -> Option<&str> {
    header_value?
        .strip_prefix("Bearer ")
        .or_else(|| header_value?.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_parsing() {
        assert_eq!(
            bearer_token(Some("Bearer abc.def.ghi")),
            Some("abc.def.ghi")
        );
        assert_eq!(bearer_token(Some("bearer xyz")), Some("xyz"));
        assert_eq!(bearer_token(Some("Basic abc")), None);
        assert_eq!(bearer_token(Some("Bearer   ")), None);
        assert_eq!(bearer_token(None), None);
    }

    #[tokio::test]
    async fn jwks_cache_miss_reports_unknown_key() {
        let source = JwksKeySource::empty();
        assert!(matches!(
            source.key_for(Some("k1")).await,
            Err(AuthError::UnknownKey(Some(_)))
        ));
    }

    #[tokio::test]
    async fn jwks_loads_and_indexes_keys_by_kid() {
        // Well-formed RSA components (standard exponent AQAB); values need only be valid
        // base64url for the JWK→key conversion to succeed.
        let json = r#"{"keys":[{"kid":"k1","kty":"RSA","n":"sXchDaQ","e":"AQAB"}]}"#;

        // via from_jwks
        let jwks: Jwks = serde_json::from_str(json).unwrap();
        let source = JwksKeySource::from_jwks(&jwks).unwrap();
        assert!(source.key_for(Some("k1")).await.is_ok());
        assert!(source.key_for(Some("missing")).await.is_err());
        // exactly one key => a kid-less lookup is unambiguous
        assert!(source.key_for(None).await.is_ok());

        // via load_from_json (the HTTP-fetcher injection point)
        let reloaded = JwksKeySource::empty();
        reloaded.load_from_json(json).unwrap();
        assert!(reloaded.key_for(Some("k1")).await.is_ok());

        // malformed JSON is rejected
        assert!(reloaded.load_from_json("not json").is_err());
    }
}
