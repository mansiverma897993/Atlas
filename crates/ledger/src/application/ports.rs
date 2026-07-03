//! **Ports** — the traits the application layer depends on. Adapters (Postgres/Redis) in the
//! `adapters` module implement them; `main.rs` injects the concrete impls (ADR-0002). Keeping
//! these as traits lets the handlers be tested against in-memory fakes.

use async_trait::async_trait;
use kernel::{AccountId, TransferId};

use crate::domain::events::AccountEvent;
use crate::domain::transfer::TransferSaga;

/// Error surface for application ports (kept string-ish to stay infra-agnostic here).
#[derive(Debug, thiserror::Error)]
pub enum PortError {
    /// Optimistic-concurrency conflict on append.
    #[error("version conflict: expected {expected}, actual {actual}")]
    Conflict {
        /// Expected version.
        expected: u64,
        /// Actual stored version.
        actual: u64,
    },
    /// Any other backing-store error.
    #[error("store error: {0}")]
    Store(String),
}

/// A stored event plus its per-stream version (as returned on load).
#[derive(Debug, Clone)]
pub struct StoredEvent {
    /// Per-stream sequence (1-based).
    pub version: u64,
    /// The domain event.
    pub event: AccountEvent,
}

/// **Port:** the event store for Account aggregates (the write model).
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Load the full ordered history for `stream`. Empty if the aggregate doesn't exist.
    async fn load(&self, stream: AccountId) -> Result<Vec<StoredEvent>, PortError>;

    /// Append `events` to `stream`, asserting the current version equals `expected_version`
    /// (optimistic concurrency — ADR-0003). The metadata (correlation/causation) is attached
    /// so the outbox relay can propagate it. Returns the new version.
    async fn append(
        &self,
        stream: AccountId,
        expected_version: u64,
        events: &[AccountEvent],
        correlation_id: &str,
    ) -> Result<u64, PortError>;
}

/// **Port:** persistence for transfer saga state (its own stream).
#[async_trait]
pub trait TransferStore: Send + Sync {
    /// Persist (insert or update) a saga.
    async fn save(&self, saga: &TransferSaga, correlation_id: &str) -> Result<(), PortError>;
    /// Load a saga by id.
    async fn load(&self, id: TransferId) -> Result<Option<TransferSaga>, PortError>;
    /// List up to `limit` sagas that are not in a terminal state (for the orchestrator to
    /// resume — durable across restarts).
    async fn list_pending(&self, limit: u32) -> Result<Vec<TransferSaga>, PortError>;
}

/// **Port:** client-facing idempotency for money-moving commands (ADR/DOMAIN §4.1).
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Return the transfer id previously created for `key`, if any.
    async fn get(&self, key: &str) -> Result<Option<TransferId>, PortError>;
    /// Record that `key` produced `transfer_id`. Must be atomic with first-writer-wins.
    async fn put(&self, key: &str, transfer_id: TransferId) -> Result<(), PortError>;
}
