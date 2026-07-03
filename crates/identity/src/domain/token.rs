//! Refresh tokens: opaque secrets stored **hashed**, grouped into a **family**, and
//! **rotated on every use**. The security-critical decision — whether a presented token should
//! rotate, be rejected, or trigger family revocation (reuse detection) — is a pure function
//! ([`evaluate_refresh`]) so it can be exhaustively unit-tested.
//!
//! We never store the raw refresh token; only its SHA-256 hash. A stolen database therefore
//! does not yield usable refresh tokens.

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::user::UserId;

/// A persisted refresh token record (the raw secret is never stored — only `token_hash`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshToken {
    /// Row identity.
    pub id: Uuid,
    /// Owning user.
    pub user_id: UserId,
    /// SHA-256 hex digest of the opaque token.
    pub token_hash: String,
    /// Family grouping all tokens descended from a single login. Reuse of any rotated-out
    /// member revokes the whole family.
    pub family_id: Uuid,
    /// Whether this token has already been rotated out (single-use).
    pub used: bool,
    /// Absolute expiry.
    pub expires_at: DateTime<Utc>,
    /// When the token was issued.
    pub created_at: DateTime<Utc>,
}

/// The decision produced by [`evaluate_refresh`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshDecision {
    /// The token is valid and unused → rotate: issue a new pair in the same family and mark
    /// this one used.
    Rotate,
    /// The token was already rotated out → theft signal → **revoke the entire family** and
    /// reject (ADR-0009).
    ReuseDetected,
    /// The token is unknown or expired → reject (no family action).
    Invalid,
}

/// Decide what to do with a presented refresh token. **Pure** and total.
///
/// * `record` — the stored token matching the presented hash, if any.
/// * `now` — the current time (injected for testability).
#[must_use]
pub fn evaluate_refresh(record: Option<&RefreshToken>, now: DateTime<Utc>) -> RefreshDecision {
    match record {
        None => RefreshDecision::Invalid,
        Some(t) if t.expires_at <= now => RefreshDecision::Invalid,
        Some(t) if t.used => RefreshDecision::ReuseDetected,
        Some(_) => RefreshDecision::Rotate,
    }
}

/// SHA-256 the opaque token to its lookup/storage hash (lower-case hex).
#[must_use]
pub fn hash_token(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    to_hex(&digest)
}

/// Lower-case hex encoding of a byte slice (avoids pulling in a `hex` crate).
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn token(used: bool, expired: bool) -> RefreshToken {
        let now = Utc::now();
        RefreshToken {
            id: Uuid::new_v4(),
            user_id: UserId::new(),
            token_hash: hash_token("secret"),
            family_id: Uuid::new_v4(),
            used,
            expires_at: if expired {
                now - Duration::minutes(1)
            } else {
                now + Duration::hours(1)
            },
            created_at: now,
        }
    }

    #[test]
    fn unknown_token_is_invalid() {
        assert_eq!(evaluate_refresh(None, Utc::now()), RefreshDecision::Invalid);
    }

    #[test]
    fn expired_token_is_invalid() {
        let t = token(false, true);
        assert_eq!(
            evaluate_refresh(Some(&t), Utc::now()),
            RefreshDecision::Invalid
        );
    }

    #[test]
    fn fresh_unused_token_rotates() {
        let t = token(false, false);
        assert_eq!(
            evaluate_refresh(Some(&t), Utc::now()),
            RefreshDecision::Rotate
        );
    }

    #[test]
    fn already_used_token_is_reuse() {
        // This is the theft signal: a token that was already rotated out is presented again.
        let t = token(true, false);
        assert_eq!(
            evaluate_refresh(Some(&t), Utc::now()),
            RefreshDecision::ReuseDetected
        );
    }

    #[test]
    fn hash_is_stable_and_hex() {
        let h = hash_token("abc");
        assert_eq!(h.len(), 64);
        assert_eq!(h, hash_token("abc"));
        assert_ne!(h, hash_token("abd"));
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
