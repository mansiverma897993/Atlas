//! The RS256 JWT issuer (ADR-0009).
//!
//! Access tokens are short-lived JWTs signed with an **RSA private key** held only by Identity.
//! Verifiers (the gateway, other services) fetch the **public** key from the JWKS endpoint and
//! verify locally — no per-request call to Identity on the hot path.
//!
//! ## Key material
//! For a self-contained, dependency-light deployment this issuer **generates a fresh 2048-bit
//! RSA keypair in memory at startup** (see [`JwtIssuer::generate`]). That means tokens do not
//! survive a restart and multiple replicas do not share a key — acceptable for local/dev and a
//! clean demonstration of the JWKS mechanics. A production deployment would instead load a
//! stable private key from configuration/secret storage and rotate via `kid`; the public
//! surface here ([`JwtIssuer::jwks`], the `kid`) is identical either way, so swapping in a
//! loaded key is a one-line change in the composition root.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::Utc;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rsa::pkcs1::{DecodeRsaPrivateKey, EncodeRsaPrivateKey};
use rsa::pkcs8::DecodePrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// The claims carried by an access token. Serialized as the JWT payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessClaims {
    /// Subject — the user id.
    pub sub: String,
    /// Role names.
    pub roles: Vec<String>,
    /// Effective permissions (so verifiers can authorize without a lookup).
    pub permissions: Vec<String>,
    /// Issuer (`iss`).
    pub iss: String,
    /// Audience (`aud`).
    pub aud: String,
    /// Expiry, unix seconds (`exp`).
    pub exp: i64,
    /// Issued-at, unix seconds (`iat`).
    pub iat: i64,
    /// Unique token id (`jti`) — enables per-token revocation lists if ever needed.
    pub jti: String,
}

/// Signs and verifies RS256 access tokens and exposes the public key as a JWKS.
pub struct JwtIssuer {
    encoding: EncodingKey,
    decoding: DecodingKey,
    /// Key id advertised in the JWT header and JWKS (`kid`).
    kid: String,
    /// Base64url public modulus (`n`) for the JWKS.
    jwk_n: String,
    /// Base64url public exponent (`e`) for the JWKS.
    jwk_e: String,
    issuer: String,
    audience: String,
    /// Access-token lifetime in seconds.
    access_ttl: i64,
}

impl JwtIssuer {
    /// Generate a fresh in-memory RSA keypair and build an issuer around it.
    ///
    /// Tokens minted this way do **not** survive a restart and are not shared across replicas
    /// (each mints its own key). This is the local/dev path; production should supply a stable
    /// key via [`from_private_pem`](Self::from_private_pem).
    ///
    /// `access_ttl_seconds` comes from `cfg.jwt.access_ttl_seconds`.
    pub fn generate(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        access_ttl_seconds: u64,
    ) -> anyhow::Result<Self> {
        let mut rng = rand::thread_rng();
        let private = RsaPrivateKey::new(&mut rng, 2048)?;
        Self::from_rsa_key(&private, issuer, audience, access_ttl_seconds)
    }

    /// Build an issuer around a **stable** RSA private key supplied as PEM (the production
    /// path — ADR-0009). Accepts both PKCS#8 (`BEGIN PRIVATE KEY`) and PKCS#1
    /// (`BEGIN RSA PRIVATE KEY`) encodings. Because the key is fixed, tokens survive restarts
    /// and every replica shares one signing identity (one `kid`), so verifiers see a stable
    /// JWKS. Wire it from `APP__JWT__PRIVATE_KEY_PEM` at the composition root.
    pub fn from_private_pem(
        pem: &str,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        access_ttl_seconds: u64,
    ) -> anyhow::Result<Self> {
        let private = RsaPrivateKey::from_pkcs8_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
            .map_err(|e| {
                anyhow::anyhow!(
                    "parsing APP__JWT__PRIVATE_KEY_PEM (expected PKCS#8 or PKCS#1 RSA PEM): {e}"
                )
            })?;
        Self::from_rsa_key(&private, issuer, audience, access_ttl_seconds)
    }

    /// Assemble the issuer from an already-parsed RSA private key (shared by
    /// [`generate`](Self::generate) and [`from_private_pem`](Self::from_private_pem)).
    fn from_rsa_key(
        private: &RsaPrivateKey,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        access_ttl_seconds: u64,
    ) -> anyhow::Result<Self> {
        let public = RsaPublicKey::from(private);

        // jsonwebtoken signs from a PEM private key.
        let pem = private.to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)?;
        let encoding = EncodingKey::from_rsa_pem(pem.as_bytes())?;

        // JWKS / verification use the public modulus and exponent (base64url, big-endian).
        let jwk_n = URL_SAFE_NO_PAD.encode(public.n().to_bytes_be());
        let jwk_e = URL_SAFE_NO_PAD.encode(public.e().to_bytes_be());
        let decoding = DecodingKey::from_rsa_components(&jwk_n, &jwk_e)
            .map_err(|e| anyhow::anyhow!("building decoding key: {e}"))?;

        // A stable `kid`: a truncated SHA-256 thumbprint of the modulus. Deterministic in the
        // key, so a given signing key always advertises the same `kid` across restarts/replicas.
        let kid = {
            let digest = Sha256::digest(public.n().to_bytes_be());
            digest.iter().take(8).map(|b| format!("{b:02x}")).collect()
        };

        Ok(Self {
            encoding,
            decoding,
            kid,
            jwk_n,
            jwk_e,
            issuer: issuer.into(),
            audience: audience.into(),
            access_ttl: access_ttl_seconds as i64,
        })
    }

    /// Mint an access token for a subject with the given roles/permissions.
    ///
    /// Returns the compact JWT and its lifetime in seconds (`expires_in`).
    pub fn issue(
        &self,
        subject: &str,
        roles: &[String],
        permissions: &[String],
    ) -> Result<(String, i64), String> {
        let now = Utc::now().timestamp();
        let claims = AccessClaims {
            sub: subject.to_string(),
            roles: roles.to_vec(),
            permissions: permissions.to_vec(),
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            exp: now + self.access_ttl,
            iat: now,
            jti: Uuid::new_v4().to_string(),
        };
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(self.kid.clone());
        encode(&header, &claims, &self.encoding)
            .map(|token| (token, self.access_ttl))
            .map_err(|e| e.to_string())
    }

    /// Verify an access token's signature, issuer, audience, and expiry, returning its claims.
    pub fn validate(&self, token: &str) -> Result<AccessClaims, String> {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        decode::<AccessClaims>(token, &self.decoding, &validation)
            .map(|data| data.claims)
            .map_err(|e| e.to_string())
    }

    /// The JWKS document (a JSON Web Key Set) served at `/.well-known/jwks.json` so verifiers
    /// can fetch the public key.
    #[must_use]
    pub fn jwks(&self) -> serde_json::Value {
        serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": self.kid,
                "n": self.jwk_n,
                "e": self.jwk_e,
            }]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issuer() -> JwtIssuer {
        JwtIssuer::generate("https://identity.local", "ledger-platform", 900).unwrap()
    }

    #[test]
    fn issued_token_validates_and_carries_claims() {
        let jwt = issuer();
        let roles = vec!["customer".to_string()];
        let perms = vec!["ledger:account:read".to_string()];
        let (token, ttl) = jwt.issue("user-123", &roles, &perms).unwrap();
        assert_eq!(ttl, 900);

        let claims = jwt.validate(&token).unwrap();
        assert_eq!(claims.sub, "user-123");
        assert_eq!(claims.roles, roles);
        assert_eq!(claims.permissions, perms);
        assert_eq!(claims.iss, "https://identity.local");
        assert_eq!(claims.aud, "ledger-platform");
        assert!(!claims.jti.is_empty());
    }

    #[test]
    fn token_from_a_different_key_is_rejected() {
        let (token, _) = issuer().issue("u", &[], &[]).unwrap();
        // A different issuer has a different keypair, so the signature must fail.
        let other = issuer();
        assert!(other.validate(&token).is_err());
    }

    #[test]
    fn fixed_pem_yields_a_stable_shared_identity() {
        use rsa::pkcs8::{EncodePrivateKey, LineEnding};

        // A single private key, as it would arrive from a secret (PKCS#8 PEM).
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = key.to_pkcs8_pem(LineEnding::LF).unwrap();

        // Two "replicas" loading the same key must share one signing identity.
        let a = JwtIssuer::from_private_pem(&pem, "https://identity.local", "ledger-platform", 900)
            .unwrap();
        let b = JwtIssuer::from_private_pem(&pem, "https://identity.local", "ledger-platform", 900)
            .unwrap();
        assert_eq!(a.kid, b.kid, "same key must advertise the same kid");

        // A token minted by replica A verifies on replica B (cross-replica validity).
        let (token, _) = a.issue("user-1", &[], &[]).unwrap();
        assert!(
            b.validate(&token).is_ok(),
            "token must verify across replicas"
        );

        // Malformed PEM is rejected rather than silently generating a throwaway key.
        assert!(
            JwtIssuer::from_private_pem("not a pem", "i", "a", 900).is_err(),
            "invalid key material must fail closed"
        );
    }

    #[test]
    fn jwks_exposes_the_public_key() {
        let jwt = issuer();
        let jwks = jwt.jwks();
        let key = &jwks["keys"][0];
        assert_eq!(key["kty"], "RSA");
        assert_eq!(key["alg"], "RS256");
        assert!(key["n"].as_str().unwrap().len() > 100);
        assert_eq!(key["e"], "AQAB"); // 65537, the standard exponent
    }
}
