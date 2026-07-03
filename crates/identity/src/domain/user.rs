//! The `User` entity, the `UserId` value object, and the pure input policies (email shape,
//! password strength) applied at registration.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::error::DomainError;

/// Strongly-typed user identifier (a UUID newtype). Using a distinct type from the ledger's
/// `OwnerId` keeps the two contexts decoupled; the shared surface is the string UUID carried
/// on the `UserRegistered` integration event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UserId(Uuid);

impl UserId {
    /// Mint a fresh random identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wrap an existing UUID (e.g. read from the database).
    #[must_use]
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// The underlying UUID, for binding into SQL.
    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for UserId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for UserId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// Account lifecycle status. Persisted as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserStatus {
    /// Normal, may authenticate.
    Active,
    /// Administratively disabled; authentication is refused.
    Suspended,
}

impl UserStatus {
    /// The database/text representation.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            UserStatus::Active => "ACTIVE",
            UserStatus::Suspended => "SUSPENDED",
        }
    }

    /// Parse from the text representation, defaulting unknown values to `Suspended` (fail
    /// closed).
    #[must_use]
    pub fn from_str_lenient(s: &str) -> Self {
        match s {
            "ACTIVE" => UserStatus::Active,
            _ => UserStatus::Suspended,
        }
    }
}

/// A registered user (identity, not credentials — the password hash lives in `Credential`).
#[derive(Debug, Clone)]
pub struct User {
    /// Stable identifier.
    pub id: UserId,
    /// Login handle; unique, lower-cased.
    pub email: String,
    /// Human-friendly name shown in UIs.
    pub display_name: String,
    /// Lifecycle status.
    pub status: UserStatus,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Minimum acceptable password length (NIST-aligned; length beats composition, but we require
/// a small amount of variety too).
pub const MIN_PASSWORD_LEN: usize = 8;

/// Validate a password against the strength policy. **Pure** — unit-tested directly.
///
/// Policy: at least [`MIN_PASSWORD_LEN`] characters, containing at least one letter and one
/// digit. Deliberately modest and dependency-free; a production system would also screen
/// against a breached-password corpus.
pub fn validate_password(password: &str) -> Result<(), DomainError> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(DomainError::WeakPassword(format!(
            "must be at least {MIN_PASSWORD_LEN} characters"
        )));
    }
    let has_letter = password.chars().any(char::is_alphabetic);
    let has_digit = password.chars().any(|c| c.is_ascii_digit());
    if !has_letter || !has_digit {
        return Err(DomainError::WeakPassword(
            "must contain at least one letter and one digit".into(),
        ));
    }
    Ok(())
}

/// Validate the shape of an email address and return its normalized (trimmed, lower-cased)
/// form. **Pure.** Intentionally conservative rather than RFC-5322 exhaustive: exactly one
/// `@`, a non-empty local part, and a domain containing a dot.
pub fn normalize_email(email: &str) -> Result<String, DomainError> {
    let trimmed = email.trim().to_lowercase();
    let mut parts = trimmed.splitn(2, '@');
    let local = parts.next().unwrap_or_default();
    let domain = parts.next().unwrap_or_default();
    if local.is_empty()
        || domain.is_empty()
        || !domain.contains('.')
        || domain.starts_with('.')
        || domain.ends_with('.')
        || trimmed.matches('@').count() != 1
    {
        return Err(DomainError::InvalidEmail);
    }
    Ok(trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_policy_rejects_short_and_simple() {
        assert!(validate_password("short1").is_err()); // too short
        assert!(validate_password("alllettersnodigits").is_err());
        assert!(validate_password("12345678").is_err()); // no letter
        assert!(validate_password("goodpass1").is_ok());
    }

    #[test]
    fn email_is_normalized_and_validated() {
        assert_eq!(
            normalize_email("  User@Example.COM ").unwrap(),
            "user@example.com"
        );
        assert!(normalize_email("no-at-sign").is_err());
        assert!(normalize_email("a@b").is_err()); // no dot in domain
        assert!(normalize_email("a@@b.com").is_err());
        assert!(normalize_email("@example.com").is_err());
    }

    #[test]
    fn user_id_roundtrips_through_string() {
        let id = UserId::new();
        let parsed: UserId = id.to_string().parse().unwrap();
        assert_eq!(id, parsed);
    }
}
