//! Domain-level errors — rule violations expressed in the ubiquitous language, independent of
//! transport or storage. The application layer maps these onto its own error type, and the
//! gRPC adapter maps *that* onto [`tonic::Status`] codes.

use thiserror::Error;

/// A violation of an Identity domain rule.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// The supplied password does not meet the strength policy.
    #[error("password does not meet policy: {0}")]
    WeakPassword(String),

    /// The supplied email is not a syntactically valid address.
    #[error("invalid email address")]
    InvalidEmail,

    /// Authentication failed (unknown user or wrong password). Deliberately opaque so it does
    /// not reveal which of the two was wrong (user-enumeration defense).
    #[error("invalid credentials")]
    InvalidCredentials,

    /// The presented refresh token is unknown or expired.
    #[error("refresh token is invalid or expired")]
    InvalidRefreshToken,

    /// A rotated-out refresh token was presented again — a theft signal. The whole family is
    /// revoked (ADR-0009).
    #[error("refresh token reuse detected; token family revoked")]
    TokenReuseDetected,

    /// The subject does not hold the permission required for the action.
    #[error("subject is not permitted to perform this action")]
    Forbidden,
}
