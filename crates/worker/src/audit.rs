//! Audit sink — the append-only compliance record (ARCHITECTURE §6.2).
//!
//! An [`EventHandler`] that consumes the `audit.v1` topic (and can mirror
//! `ledger.transfer.v1`) and writes an immutable row per event into `audit_log`. The consumer
//! is **idempotent**: `audit_log.event_id` is `UNIQUE` and inserts use `ON CONFLICT DO
//! NOTHING`, so a redelivered message is recorded at most once. The envelope→row mapping is a
//! pure function ([`AuditRow::from_envelope`]) and is unit-tested in isolation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use infra::bus::{Ack, EventEnvelope, EventHandler};

/// A single immutable audit record, derived from an [`EventEnvelope`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRow {
    /// Idempotency/dedup key (the envelope's event id).
    pub event_id: String,
    /// Who performed the action (from payload `actor`, else `"system"`).
    pub actor: String,
    /// What was done (payload `action`, else the event type).
    pub action: String,
    /// The kind of resource acted upon (payload `resource_type`, else derived).
    pub resource_type: String,
    /// The id of the resource (payload `resource_id`, else the stream id).
    pub resource_id: String,
    /// The full event payload, retained verbatim.
    pub payload: serde_json::Value,
    /// When the event occurred (from envelope metadata).
    pub occurred_at: DateTime<Utc>,
}

impl AuditRow {
    /// Map a consumed envelope to an audit row. **Pure** — no I/O — so it is unit-testable.
    ///
    /// The `audit.v1` payload carries explicit `actor`/`action`/`resource_type`/`resource_id`
    /// fields when produced by a service; for events mirrored from other topics we fall back
    /// to sensible defaults derived from the envelope so nothing is silently dropped.
    #[must_use]
    pub fn from_envelope(env: &EventEnvelope) -> Self {
        let field = |key: &str| -> Option<String> {
            env.payload
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        AuditRow {
            event_id: env.metadata.event_id.clone(),
            actor: field("actor").unwrap_or_else(|| "system".to_string()),
            action: field("action").unwrap_or_else(|| env.event_type.clone()),
            resource_type: field("resource_type").unwrap_or_else(|| derive_resource_type(env)),
            resource_id: field("resource_id").unwrap_or_else(|| env.metadata.stream_id.clone()),
            payload: env.payload.clone(),
            occurred_at: env.metadata.occurred_at,
        }
    }
}

/// Best-effort resource-type derivation for events that don't state one explicitly.
fn derive_resource_type(env: &EventEnvelope) -> String {
    // Event types are CamelCase verbs on a noun (e.g. `FundsReserved`, `AccountOpened`); take
    // the leading noun-ish token as the resource kind, defaulting to the raw event type.
    match env.event_type.as_str() {
        t if t.starts_with("Account") => "account".to_string(),
        t if t.starts_with("Funds") || t.starts_with("Transfer") => "transfer".to_string(),
        t if t.starts_with("User") => "user".to_string(),
        other => other.to_string(),
    }
}

/// The audit-sink consumer: persists each event as an immutable `audit_log` row.
pub struct AuditSink {
    pool: PgPool,
}

impl AuditSink {
    /// Build the sink over the worker database pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert the row, returning whether it was newly recorded (`false` == duplicate).
    async fn record(&self, row: &AuditRow) -> anyhow::Result<bool> {
        let result = sqlx::query(
            "INSERT INTO audit_log \
             (event_id, actor, action, resource_type, resource_id, payload, occurred_at) \
             VALUES ($1::uuid, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&row.event_id)
        .bind(&row.actor)
        .bind(&row.action)
        .bind(&row.resource_type)
        .bind(&row.resource_id)
        .bind(&row.payload)
        .bind(row.occurred_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[async_trait]
impl EventHandler for AuditSink {
    async fn handle(&self, event: &EventEnvelope) -> Ack {
        let row = AuditRow::from_envelope(event);
        match self.record(&row).await {
            Ok(inserted) => {
                if inserted {
                    metrics::counter!("audit_events_total", "action" => row.action.clone())
                        .increment(1);
                    tracing::debug!(event_id = %row.event_id, action = %row.action, "audit recorded");
                } else {
                    tracing::debug!(event_id = %row.event_id, "audit duplicate ignored");
                }
                Ack::Commit
            }
            Err(e) => {
                tracing::error!(error = %e, event_id = %row.event_id, "failed to record audit row");
                // Transient (e.g. DB blip): let the retry budget → DLQ policy handle it.
                Ack::Retry
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use infra::bus::EventMetadata;

    fn envelope(event_type: &str, payload: serde_json::Value) -> EventEnvelope {
        EventEnvelope {
            event_type: event_type.to_string(),
            metadata: EventMetadata {
                event_id: "11111111-1111-1111-1111-111111111111".to_string(),
                stream_id: "22222222-2222-2222-2222-222222222222".to_string(),
                version: 1,
                correlation_id: "corr".to_string(),
                causation_id: None,
                traceparent: None,
                occurred_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            },
            payload,
        }
    }

    #[test]
    fn maps_explicit_audit_fields() {
        let env = envelope(
            "TransferCompleted",
            serde_json::json!({
                "actor": "user-7",
                "action": "transfer.complete",
                "resource_type": "transfer",
                "resource_id": "tx-9",
                "amount": 500
            }),
        );
        let row = AuditRow::from_envelope(&env);
        assert_eq!(row.event_id, env.metadata.event_id);
        assert_eq!(row.actor, "user-7");
        assert_eq!(row.action, "transfer.complete");
        assert_eq!(row.resource_type, "transfer");
        assert_eq!(row.resource_id, "tx-9");
        assert_eq!(row.occurred_at, env.metadata.occurred_at);
    }

    #[test]
    fn falls_back_to_envelope_when_fields_absent() {
        let env = envelope("AccountOpened", serde_json::json!({ "owner": "abc" }));
        let row = AuditRow::from_envelope(&env);
        assert_eq!(row.actor, "system");
        assert_eq!(row.action, "AccountOpened"); // defaults to event type
        assert_eq!(row.resource_type, "account"); // derived from prefix
        assert_eq!(row.resource_id, env.metadata.stream_id); // defaults to stream id
    }

    #[test]
    fn derives_resource_type_from_event_prefix() {
        assert_eq!(
            AuditRow::from_envelope(&envelope("FundsReserved", serde_json::json!({})))
                .resource_type,
            "transfer"
        );
        assert_eq!(
            AuditRow::from_envelope(&envelope("UserRegistered", serde_json::json!({})))
                .resource_type,
            "user"
        );
    }
}
