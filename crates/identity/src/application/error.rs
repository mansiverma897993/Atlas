//! The application error type. Handlers return this; the gRPC adapter maps it to
//! [`tonic::Status`]. It wraps domain-rule violations and port (infrastructure) failures and
//! adds a few use-case-level cases.

use thiserror::Error;

use crate::application::ports::PortError;
use crate::domain::DomainError;

/// An error from an application use case.
#[derive(Debug, Error)]
pub enum AuthError {
    /// A domain rule was violated (bad input, reuse detected, forbidden, …).
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// An outbound port (database, provider) failed.
    #[error(transparent)]
    Port(#[from] PortError),

    /// Registration conflicted with an existing account.
    #[error("email is already registered")]
    EmailExists,

    /// A JWT could not be minted or verified.
    #[error("token error: {0}")]
    Token(String),

    /// A subject id was not a valid identifier.
    #[error("invalid subject identifier")]
    InvalidSubject,
}
