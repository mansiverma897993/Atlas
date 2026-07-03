//! Transactional-outbox relay (ADR-0006).
//!
//! The event store *is* the outbox: events are appended with a monotonic `global_seq` in the
//! same transaction as the state change (no dual-write). This relay tails that sequence and
//! publishes committed events to the bus, persisting its progress so it can resume after a
//! restart. Delivery is at-least-once; consumers dedup on `event_id`.
//!
//! The relay is generic over an [`OutboxSource`] so it works for any context (ledger event
//! store, identity outbox table) without knowing its schema.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::bus::{EventEnvelope, EventPublisher};
use crate::error::Result;

/// A record pulled from an outbox source: what to publish and where.
pub struct OutboxRecord {
    /// Monotonic sequence used to track relay progress.
    pub global_seq: i64,
    /// Destination topic.
    pub topic: String,
    /// The event to publish.
    pub envelope: EventEnvelope,
}

/// **Port:** a source of not-yet-published events, ordered by `global_seq`.
#[async_trait]
pub trait OutboxSource: Send + Sync {
    /// Fetch up to `limit` records with `global_seq` greater than `after`, in order.
    async fn fetch_after(&self, after: i64, limit: i64) -> Result<Vec<OutboxRecord>>;

    /// Load the last successfully published sequence (relay resume point).
    async fn load_checkpoint(&self, relay: &str) -> Result<i64>;

    /// Persist the checkpoint after a successful publish batch.
    async fn store_checkpoint(&self, relay: &str, seq: i64) -> Result<()>;
}

/// Streams committed events from an [`OutboxSource`] to an [`EventPublisher`].
pub struct OutboxRelay {
    name: String,
    source: Arc<dyn OutboxSource>,
    publisher: Arc<dyn EventPublisher>,
    batch_size: i64,
    poll_interval: Duration,
}

impl OutboxRelay {
    /// Construct a relay identified by `name` (its checkpoint key).
    pub fn new(
        name: impl Into<String>,
        source: Arc<dyn OutboxSource>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self {
            name: name.into(),
            source,
            publisher,
            batch_size: 256,
            poll_interval: Duration::from_millis(200),
        }
    }

    /// Run the relay loop until `cancel` fires (graceful shutdown).
    pub async fn run(&self, cancel: CancellationToken) -> Result<()> {
        let mut checkpoint = self.source.load_checkpoint(&self.name).await?;
        tracing::info!(relay = %self.name, checkpoint, "outbox relay started");

        loop {
            if cancel.is_cancelled() {
                tracing::info!(relay = %self.name, "outbox relay stopping");
                return Ok(());
            }

            let batch = self.source.fetch_after(checkpoint, self.batch_size).await?;
            if batch.is_empty() {
                tokio::select! {
                    () = tokio::time::sleep(self.poll_interval) => {}
                    () = cancel.cancelled() => return Ok(()),
                }
                continue;
            }

            for record in batch {
                self.publisher
                    .publish(&record.topic, &record.envelope)
                    .await?;
                checkpoint = record.global_seq;
            }
            // Persist progress only after the whole batch is published (at-least-once).
            self.source.store_checkpoint(&self.name, checkpoint).await?;
            metrics::gauge!("outbox_checkpoint", "relay" => self.name.clone())
                .set(checkpoint as f64);
        }
    }
}
