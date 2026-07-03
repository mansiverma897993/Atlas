//! Password credentials, hashed with **Argon2id** (memory-hard, the current best practice for
//! password storage). Hashing and verification are the only place raw passwords are touched;
//! everywhere else we carry the PHC-string hash.
//!
//! The functions here are thin wrappers over the `argon2` crate but are kept in the domain so
//! the roundtrip (hash → verify) is unit-tested and the algorithm choice lives with the model.

use argon2::password_hash::{PasswordHash, SaltString};
use argon2::{Argon2, PasswordHasher, PasswordVerifier};
use rand::rngs::OsRng;

/// Hash a plaintext password into a self-describing PHC string (algorithm, params, salt, and
/// digest), using Argon2id with a fresh random salt and the crate's default parameters.
///
/// Returns the encoded hash on success or a stringified error (hashing can only fail on
/// pathological input / allocation failure).
pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

/// Verify a plaintext password against a stored PHC hash in constant time (as provided by the
/// Argon2 verifier). Returns `false` for any mismatch or malformed hash — never panics.
#[must_use]
pub fn verify_password(stored_hash: &str, password: &str) -> bool {
    match PasswordHash::new(stored_hash) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify_roundtrips() {
        let hash = hash_password("correct horse battery1").unwrap();
        // The hash is a PHC string identifying Argon2id.
        assert!(hash.starts_with("$argon2id$"), "unexpected hash: {hash}");
        assert!(verify_password(&hash, "correct horse battery1"));
        assert!(!verify_password(&hash, "wrong password"));
    }

    #[test]
    fn distinct_salts_produce_distinct_hashes() {
        let a = hash_password("goodpass1").unwrap();
        let b = hash_password("goodpass1").unwrap();
        assert_ne!(a, b, "salt should randomize the digest");
        // ...yet both verify.
        assert!(verify_password(&a, "goodpass1"));
        assert!(verify_password(&b, "goodpass1"));
    }

    #[test]
    fn malformed_hash_verifies_false() {
        assert!(!verify_password("not-a-phc-string", "whatever"));
    }
}
