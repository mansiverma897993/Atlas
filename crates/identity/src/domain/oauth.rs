//! OAuth2 authorization-code + **PKCE** support (ADR-0009). The `OAuthIdentity` links an
//! external provider account to a local [`UserId`]; the PKCE helpers are pure derivations used
//! by both the start and callback steps.
//!
//! PKCE (RFC 7636) binds the authorization request to the token exchange without a client
//! secret: the client sends `code_challenge = BASE64URL(SHA256(code_verifier))` up front and
//! reveals the `code_verifier` only at exchange time, so an intercepted `code` is useless
//! without it.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::user::UserId;

/// A link between an external provider identity and a local user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthIdentity {
    /// Provider key, e.g. `"github"`.
    pub provider: String,
    /// The provider's stable subject identifier for the user.
    pub subject: String,
    /// The local user this identity is linked to.
    pub user_id: UserId,
    /// Email asserted by the provider (may be empty).
    pub email: String,
}

/// The `S256` PKCE code-challenge method (the only one we use; `plain` is disallowed).
pub const PKCE_METHOD: &str = "S256";

/// Generate a high-entropy PKCE `code_verifier` (RFC 7636 §4.1): 32 random bytes,
/// base64url-encoded without padding (43 characters, within the allowed 43–128).
#[must_use]
pub fn generate_code_verifier() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Derive the `code_challenge` for a verifier using the `S256` method:
/// `BASE64URL(SHA256(ascii(code_verifier)))`. **Pure.**
#[must_use]
pub fn code_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

/// Generate an opaque, unguessable `state` parameter (CSRF protection for the redirect).
#[must_use]
pub fn generate_state() -> String {
    Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenge_is_deterministic_for_a_verifier() {
        let v = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"; // RFC 7636 example verifier
                                                               // RFC 7636 Appendix B expected challenge for this verifier.
        assert_eq!(
            code_challenge(v),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn verifiers_are_unique_and_well_formed() {
        let a = generate_code_verifier();
        let b = generate_code_verifier();
        assert_ne!(a, b);
        assert_eq!(a.len(), 43); // 32 bytes base64url no-pad
        assert!(a
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }
}
