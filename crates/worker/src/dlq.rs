//! Dead-letter queue monitor + replay tooling (ARCHITECTURE §6.1).
//!
//! Poison messages that exhaust their retry budget are routed by the consumer adapter to a
//! per-topic `<topic>.dlq` companion. This module:
//!
//! * **records** each dead-lettered envelope into `dlq_entries` (idempotent on
//!   `(topic, event_id)`) and exposes the `dlq_depth` metric — [`DlqMonitor`], an
//!   [`EventHandler`] the composition root binds to the `.dlq` topics;
//! * **replays** unresolved entries back onto their main topic on operator demand —
//!   [`DlqReplayer::replay`], surfaced via the `POST /admin/dlq/replay/:topic` admin route.

use async_trait::async_trait;
use sqlx::PgPool;

use infra::bus::kafka::KafkaPublisher;
use infra::bus::{Ack, EventEnvelope, EventHandler, EventPublisher};

/// Given a `.dlq` topic, return the main topic it dead-lettered from. Pure — unit-tested.
///
/// Returns `None` for a topic that is not a dead-letter topic (defensive: we never want to
/// "replay" onto a `.dlq` topic).
#[must_use]
pub fn main_topic_of(dlq_topic: &str) -> Option<String> {
    dlq_topic
        .strip_suffix(".dlq")
        .map(str::to_string)
        .filter(|main| !main.is_empty())
}

/// Whether a recorded entry is eligible for replay: only if it has not already been replayed.
/// Pure decision, split out so it is trivially testable.
#[must_use]
pub fn should_replay(replayed: bool) -> bool {
    !replayed
}

/// A row read back from `dlq_entries` for replay.
#[derive(Debug, Clone)]
pub struct DlqRecord {
    /// Primary key of the entry.
    pub id: i64,
    /// The event id (envelope idempotency key).
    pub event_id: String,
    /// The full envelope JSON, replayed verbatim onto the main topic.
    pub payload: serde_json::Value,
    /// Whether it has already been replayed.
    pub replayed: bool,
}

/// The DLQ monitor: an [`EventHandler`] bound to the `.dlq` topics that records each poison
/// message for later inspection/replay.
pub struct DlqMonitor {
    pool: PgPool,
    /// The `.dlq` topic this monitor instance is consuming (used to label the entry).
    topic: String,
}

impl DlqMonitor {
    /// Build a monitor that tags recorded entries with `dlq_topic`.
    #[must_use]
    pub fn new(pool: PgPool, dlq_topic: impl Into<String>) -> Self {
        Self {
            pool,
            topic: dlq_topic.into(),
        }
    }

    /// Persist one dead-lettered envelope. Returns whether it was newly recorded.
    async fn record(&self, env: &EventEnvelope) -> anyhow::Result<bool> {
        let payload = serde_json::to_value(env)?;
        let error = env
            .payload
            .get("error")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let result = sqlx::query(
            "INSERT INTO dlq_entries (topic, event_id, payload, error) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (topic, event_id) DO NOTHING",
        )
        .bind(&self.topic)
        .bind(&env.metadata.event_id)
        .bind(&payload)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[async_trait]
impl EventHandler for DlqMonitor {
    async fn handle(&self, event: &EventEnvelope) -> Ack {
        match self.record(event).await {
            Ok(inserted) => {
                if inserted {
                    // Depth of the dead-letter backlog, labeled by topic.
                    metrics::counter!("dlq_depth", "topic" => self.topic.clone()).increment(1);
                    tracing::warn!(
                        topic = %self.topic,
                        event_id = %event.metadata.event_id,
                        "recorded dead-letter entry"
                    );
                }
                Ack::Commit
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to record DLQ entry");
                Ack::Retry
            }
        }
    }
}

/// Replays recorded DLQ entries back onto their main topic (operator tooling).
pub struct DlqReplayer<P: EventPublisher> {
    pool: PgPool,
    publisher: P,
}

impl DlqReplayer<KafkaPublisher> {
    /// Build a replayer over the worker DB and the Kafka publisher.
    #[must_use]
    pub fn new(pool: PgPool, publisher: KafkaPublisher) -> Self {
        Self { pool, publisher }
    }
}

impl<P: EventPublisher> DlqReplayer<P> {
    /// Re-publish every not-yet-replayed entry for `main_topic`'s DLQ back onto `main_topic`,
    /// marking each replayed on success. Returns the number of messages replayed.
    ///
    /// `main_topic` is the *main* topic (e.g. `ledger.transfer.v1`); its `.dlq` companion is
    /// derived internally.
    pub async fn replay(&self, topic: &str) -> anyhow::Result<u64> {
        // Tolerate being handed either the main topic or its `.dlq` companion.
        let main_topic = main_topic_of(topic).unwrap_or_else(|| topic.to_string());
        let dlq = infra::bus::dlq_topic(&main_topic);
        let rows: Vec<DlqRecord> = sqlx::query_as::<_, (i64, String, serde_json::Value, bool)>(
            "SELECT id, event_id, payload, replayed FROM dlq_entries \
             WHERE topic = $1 AND NOT replayed ORDER BY id",
        )
        .bind(&dlq)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|(id, event_id, payload, replayed)| DlqRecord {
            id,
            event_id,
            payload,
            replayed,
        })
        .collect();

        let mut replayed = 0u64;
        for row in rows {
            if !should_replay(row.replayed) {
                continue;
            }
            let envelope: EventEnvelope = serde_json::from_value(row.payload)?;
            self.publisher.publish(&main_topic, &envelope).await?;
            sqlx::query("UPDATE dlq_entries SET replayed = true WHERE id = $1")
                .bind(row.id)
                .execute(&self.pool)
                .await?;
            replayed += 1;
            tracing::info!(%main_topic, event_id = %row.event_id, "replayed dead-letter entry");
        }
        metrics::counter!("dlq_replayed_total", "topic" => main_topic.clone()).increment(replayed);
        Ok(replayed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main_topic_strips_dlq_suffix() {
        assert_eq!(
            main_topic_of("ledger.transfer.v1.dlq").as_deref(),
            Some("ledger.transfer.v1")
        );
    }

    #[test]
    fn main_topic_rejects_non_dlq() {
        assert_eq!(main_topic_of("ledger.transfer.v1"), None);
        assert_eq!(main_topic_of(".dlq"), None);
    }

    #[test]
    fn replay_decision_skips_already_replayed() {
        assert!(should_replay(false));
        assert!(!should_replay(true));
    }
}
