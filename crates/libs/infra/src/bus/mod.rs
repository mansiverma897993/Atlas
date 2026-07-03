//! The event backbone: envelope, ports, and topic naming.
//!
//! Services depend on the [`EventPublisher`] / [`EventConsumer`] **ports**; the concrete
//! Redpanda adapter lives in [`kafka`]. Every message is an [`EventEnvelope`]: a typed
//! payload plus metadata (ids, correlation, causation, version) that carries the distributed
//! trace across the async hop (ADR-0012) and the idempotency key for exactly-once *effects*
//! (ADR-0006).

pub mod kafka;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Metadata attached to every event, propagated through Kafka headers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Unique event id — the idempotency/dedup key for consumers.
    pub event_id: String,
    /// The aggregate/stream this event belongs to (also the partition key).
    pub stream_id: String,
    /// Per-stream monotonic version.
    pub version: u64,
    /// Correlation id spanning the whole causal chain.
    pub correlation_id: String,
    /// Id of the message that directly caused this one.
    pub causation_id: Option<String>,
    /// W3C `traceparent` for distributed tracing continuity.
    pub traceparent: Option<String>,
    /// When the event occurred.
    pub occurred_at: DateTime<Utc>,
}

/// A typed event plus its metadata, ready to publish or freshly consumed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Discriminator, e.g. `"FundsReserved"`.
    pub event_type: String,
    /// Metadata (ids, correlation, version).
    pub metadata: EventMetadata,
    /// Opaque JSON payload of the domain event.
    pub payload: serde_json::Value,
}

impl EventEnvelope {
    /// The partition key (stream id) ensuring per-aggregate ordering.
    #[must_use]
    pub fn partition_key(&self) -> &str {
        &self.metadata.stream_id
    }

    /// Serialize the envelope to bytes for transport.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Deserialize an envelope from transport bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Versioned topic names (each has an implicit `<name>.dlq` companion — see [`dlq_topic`]).
pub mod topics {
    /// Account lifecycle & balance events.
    pub const LEDGER_ACCOUNT: &str = "ledger.account.v1";
    /// Transfer saga events.
    pub const LEDGER_TRANSFER: &str = "ledger.transfer.v1";
    /// Identity integration events.
    pub const IDENTITY_USER: &str = "identity.user.v1";
    /// Cross-cutting audit stream.
    pub const AUDIT: &str = "audit.v1";
}

/// The dead-letter topic for a given topic (ADR-0006).
#[must_use]
pub fn dlq_topic(topic: &str) -> String {
    format!("{topic}.dlq")
}

/// **Port:** publish events to the backbone. Implemented by [`kafka::KafkaPublisher`].
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish a single envelope to `topic`, partitioned by its stream id.
    async fn publish(&self, topic: &str, event: &EventEnvelope) -> Result<()>;

    /// Publish a batch to `topic` preserving order within a partition key.
    async fn publish_batch(&self, topic: &str, events: &[EventEnvelope]) -> Result<()> {
        for e in events {
            self.publish(topic, e).await?;
        }
        Ok(())
    }
}

/// The result of handling a consumed message, controlling offset progression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    /// Handled successfully; advance the offset.
    Commit,
    /// Transient failure; redeliver (subject to retry budget → DLQ).
    Retry,
    /// Permanent failure; route to the DLQ and advance past it.
    DeadLetter,
}

/// A handler invoked for each consumed event.
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Process one event. Must be **idempotent** (dedup on `metadata.event_id`) because
    /// delivery is at-least-once.
    async fn handle(&self, event: &EventEnvelope) -> Ack;
}

/// **Port:** consume events from the backbone and drive a handler.
#[async_trait]
pub trait EventConsumer: Send + Sync {
    /// Run the consume loop for `topic`, invoking `handler` per message until cancelled.
    async fn run(&self, topic: &str, handler: std::sync::Arc<dyn EventHandler>) -> Result<()>;
}
