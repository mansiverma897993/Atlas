//! Infrastructure error type, spanning all adapters.

use thiserror::Error;

/// An error from an infrastructure adapter.
#[derive(Debug, Error)]
pub enum InfraError {
    /// A database operation failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// A Redis operation failed.
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// (De)serialization of an event payload failed.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// The event bus (Kafka/Redpanda) returned an error.
    #[error("event bus error: {0}")]
    Bus(String),

    /// An optimistic-concurrency conflict (expected version did not match).
    #[error("concurrency conflict: expected version {expected}, found {actual}")]
    Conflict {
        /// Version the writer expected.
        expected: u64,
        /// Version actually present in the store.
        actual: u64,
    },

    /// A distributed lock could not be acquired within the deadline.
    #[error("could not acquire lock '{0}' within timeout")]
    LockTimeout(String),

    /// A dependency was unreachable during a health check.
    #[error("dependency unavailable: {0}")]
    Unavailable(String),
}

/// Convenience alias for infra results.
pub type Result<T> = std::result::Result<T, InfraError>;
