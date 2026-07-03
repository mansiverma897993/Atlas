//! PostgreSQL adapters — the real write model, projections, and outbox source.
//!
//! Design highlights:
//! * **Append is the commit.** [`PgEventStore::append`] inserts events under
//!   `UNIQUE(stream_id, version)` (optimistic concurrency) and, in the *same transaction*,
//!   updates the read-model projections (synchronous CQRS — ROADMAP Phase 3) so reads are
//!   consistent. The monotonic `global_seq` drives the outbox relay.
//! * **No dual-write.** Nothing here publishes to Kafka; the [`PgOutboxSource`] streams
//!   committed events to the bus separately (ADR-0006).
//! * All SQL uses SQLx parameter binding (never string interpolation) — the injection defense.

use async_trait::async_trait;
use chrono::Utc;
use kernel::{AccountId, Money, TransferId};
use serde_json::json;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::application::ports::{
    EventStore, IdempotencyStore, PortError, StoredEvent, TransferStore,
};
use crate::application::queries::{AccountView, ReadModel, TransactionEntry, TransferView};
use crate::domain::account::Account;
use crate::domain::events::AccountEvent;
use crate::domain::transfer::{TransferSaga, TransferState};
use infra::bus::{topics, EventEnvelope, EventMetadata};
use infra::outbox::{OutboxRecord, OutboxSource};

fn store_err(e: sqlx::Error) -> PortError {
    PortError::Store(e.to_string())
}

/// Postgres event store for the Account aggregate.
#[derive(Clone)]
pub struct PgEventStore {
    pool: PgPool,
}

impl PgEventStore {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventStore for PgEventStore {
    async fn load(&self, stream: AccountId) -> Result<Vec<StoredEvent>, PortError> {
        let rows = sqlx::query(
            "SELECT version, payload FROM events \
             WHERE stream_id = $1 AND topic = $2 ORDER BY version ASC",
        )
        .bind(stream.as_uuid())
        .bind(topics::LEDGER_ACCOUNT)
        .fetch_all(&self.pool)
        .await
        .map_err(store_err)?;

        rows.into_iter()
            .map(|row| {
                let version: i64 = row.get("version");
                let payload: serde_json::Value = row.get("payload");
                let event: AccountEvent =
                    serde_json::from_value(payload).map_err(|e| PortError::Store(e.to_string()))?;
                Ok(StoredEvent {
                    version: version as u64,
                    event,
                })
            })
            .collect()
    }

    async fn append(
        &self,
        stream: AccountId,
        expected_version: u64,
        events: &[AccountEvent],
        correlation_id: &str,
    ) -> Result<u64, PortError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;

        let mut version = expected_version;
        for event in events {
            version += 1;
            let payload =
                serde_json::to_value(event).map_err(|e| PortError::Store(e.to_string()))?;
            let metadata = json!({ "correlation_id": correlation_id });

            let result = sqlx::query(
                "INSERT INTO events (event_id, stream_id, topic, version, event_type, payload, metadata, occurred_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(Uuid::new_v4())
            .bind(stream.as_uuid())
            .bind(topics::LEDGER_ACCOUNT)
            .bind(version as i64)
            .bind(event.event_type())
            .bind(&payload)
            .bind(&metadata)
            .bind(Utc::now())
            .execute(&mut *tx)
            .await;

            if let Err(sqlx::Error::Database(db)) = &result {
                if db.is_unique_violation() {
                    // Another writer advanced the stream — optimistic-concurrency conflict.
                    return Err(PortError::Conflict {
                        expected: expected_version,
                        actual: expected_version,
                    });
                }
            }
            result.map_err(store_err)?;
        }

        // Synchronous projection update in the same transaction (read-your-writes).
        self.project_account(&mut tx, stream).await?;
        self.project_transactions(&mut tx, stream, events).await?;

        tx.commit().await.map_err(store_err)?;
        Ok(version)
    }
}

impl PgEventStore {
    /// Rebuild the `account_balance_view` row by folding the full stream (correct & simple;
    /// snapshotting is the scale optimization noted in DOMAIN §4.1).
    async fn project_account(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        stream: AccountId,
    ) -> Result<(), PortError> {
        let rows = sqlx::query(
            "SELECT payload FROM events WHERE stream_id = $1 AND topic = $2 ORDER BY version",
        )
        .bind(stream.as_uuid())
        .bind(topics::LEDGER_ACCOUNT)
        .fetch_all(&mut **tx)
        .await
        .map_err(store_err)?;
        let history: Vec<AccountEvent> = rows
            .into_iter()
            .map(|r| serde_json::from_value(r.get::<serde_json::Value, _>("payload")))
            .collect::<Result<_, _>>()
            .map_err(|e| PortError::Store(e.to_string()))?;
        if history.is_empty() {
            return Ok(());
        }
        let acc = Account::rehydrate(stream, &history);
        let ccy = acc
            .currency()
            .map(|c| c.code().to_string())
            .unwrap_or_default();
        let status = acc
            .status()
            .map_or("UNKNOWN", crate::domain::account::AccountStatus::as_str);

        sqlx::query(
            "INSERT INTO account_balance_view (account_id, currency, status, posted, reserved, version, updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6, now()) \
             ON CONFLICT (account_id) DO UPDATE SET \
               currency = EXCLUDED.currency, status = EXCLUDED.status, \
               posted = EXCLUDED.posted, reserved = EXCLUDED.reserved, \
               version = EXCLUDED.version, updated_at = now()",
        )
        .bind(stream.as_uuid())
        .bind(ccy)
        .bind(status)
        .bind(acc.posted_balance() as i64)
        .bind(acc.reserved() as i64)
        .bind(acc.version() as i64)
        .execute(&mut **tx)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    /// Append transaction-history rows for capture (debit) and credit events.
    async fn project_transactions(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        stream: AccountId,
        events: &[AccountEvent],
    ) -> Result<(), PortError> {
        for event in events {
            let (transfer_id, direction, amount) = match event {
                AccountEvent::FundsCaptured {
                    transfer_id,
                    amount,
                } => (*transfer_id, "DEBIT", *amount),
                AccountEvent::FundsCredited {
                    transfer_id,
                    amount,
                } => (*transfer_id, "CREDIT", *amount),
                _ => continue,
            };
            sqlx::query(
                "INSERT INTO transaction_history_view \
                 (account_id, transfer_id, direction, amount, currency, occurred_at) \
                 VALUES ($1,$2,$3,$4,$5, now())",
            )
            .bind(stream.as_uuid())
            .bind(transfer_id.as_uuid())
            .bind(direction)
            .bind(amount.minor_units() as i64)
            .bind(amount.currency().code())
            .execute(&mut **tx)
            .await
            .map_err(store_err)?;
        }
        Ok(())
    }
}

/// Postgres transfer/saga store. Persists the saga to `transfer_status_view` and emits
/// transfer lifecycle events (idempotently) into the outbox `events` table for consumers.
#[derive(Clone)]
pub struct PgTransferStore {
    pool: PgPool,
}

impl PgTransferStore {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TransferStore for PgTransferStore {
    async fn save(&self, saga: &TransferSaga, correlation_id: &str) -> Result<(), PortError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        let failure = match &saga.state {
            TransferState::Failed { reason } => Some(reason.clone()),
            _ => None,
        };

        sqlx::query(
            "INSERT INTO transfer_status_view \
             (transfer_id, source, destination, amount, currency, status, failure_reason, created_at, updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7, now(), now()) \
             ON CONFLICT (transfer_id) DO UPDATE SET \
               status = EXCLUDED.status, failure_reason = EXCLUDED.failure_reason, updated_at = now()",
        )
        .bind(saga.transfer_id.as_uuid())
        .bind(saga.source.as_uuid())
        .bind(saga.destination.as_uuid())
        .bind(saga.amount.minor_units() as i64)
        .bind(saga.amount.currency().code())
        .bind(saga.state.as_str())
        .bind(failure)
        .execute(&mut *tx)
        .await
        .map_err(store_err)?;

        // Emit a transfer lifecycle event for meaningful states only, deduped by event_id so
        // repeated saves don't double-publish (ON CONFLICT DO NOTHING).
        let emit = matches!(
            saga.state,
            TransferState::Requested | TransferState::Completed | TransferState::Failed { .. }
        );
        if emit {
            let event_type = format!("Transfer{}", capitalize(saga.state.as_str()));
            let event_id = deterministic_event_id(saga.transfer_id, saga.state.as_str());
            let payload = json!({
                "transfer_id": saga.transfer_id.to_string(),
                "source": saga.source.to_string(),
                "destination": saga.destination.to_string(),
                "amount": { "minor_units": saga.amount.minor_units(), "currency": saga.amount.currency().code() },
                "status": saga.state.as_str(),
            });
            sqlx::query(
                "INSERT INTO events (event_id, stream_id, topic, version, event_type, payload, metadata, occurred_at) \
                 VALUES ($1,$2,$3, \
                   COALESCE((SELECT MAX(version)+1 FROM events WHERE stream_id=$2 AND topic=$3), 1), \
                   $4,$5,$6, now()) \
                 ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(event_id)
            .bind(saga.transfer_id.as_uuid())
            .bind(topics::LEDGER_TRANSFER)
            .bind(event_type)
            .bind(&payload)
            .bind(json!({ "correlation_id": correlation_id }))
            .execute(&mut *tx)
            .await
            .map_err(store_err)?;
        }

        tx.commit().await.map_err(store_err)?;
        Ok(())
    }

    async fn load(&self, id: TransferId) -> Result<Option<TransferSaga>, PortError> {
        let row = sqlx::query(
            "SELECT source, destination, amount, currency, status, failure_reason \
             FROM transfer_status_view WHERE transfer_id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(row.map(|r| row_to_saga(id, &r)))
    }

    async fn list_pending(&self, limit: u32) -> Result<Vec<TransferSaga>, PortError> {
        let rows = sqlx::query(
            "SELECT transfer_id, source, destination, amount, currency, status, failure_reason \
             FROM transfer_status_view \
             WHERE status NOT IN ('COMPLETED','FAILED') \
             ORDER BY updated_at ASC LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let id = TransferId::from_uuid(r.get::<Uuid, _>("transfer_id"));
                row_to_saga(id, &r)
            })
            .collect())
    }
}

/// Postgres idempotency store.
#[derive(Clone)]
pub struct PgIdempotency {
    pool: PgPool,
}

impl PgIdempotency {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IdempotencyStore for PgIdempotency {
    async fn get(&self, key: &str) -> Result<Option<TransferId>, PortError> {
        let row = sqlx::query("SELECT transfer_id FROM idempotency WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_err)?;
        Ok(row.map(|r| TransferId::from_uuid(r.get::<Uuid, _>("transfer_id"))))
    }

    async fn put(&self, key: &str, transfer_id: TransferId) -> Result<(), PortError> {
        sqlx::query(
            "INSERT INTO idempotency (key, transfer_id, created_at) VALUES ($1, $2, now()) \
             ON CONFLICT (key) DO NOTHING",
        )
        .bind(key)
        .bind(transfer_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }
}

/// Postgres read model (queries hit projections only).
#[derive(Clone)]
pub struct PgReadModel {
    pool: PgPool,
}

impl PgReadModel {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ReadModel for PgReadModel {
    async fn account(&self, id: AccountId) -> Result<Option<AccountView>, PortError> {
        let row = sqlx::query(
            "SELECT owner_id, currency, status, posted, reserved, version \
             FROM account_balance_view WHERE account_id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)?;

        Ok(row.map(|r| {
            let currency = kernel::Currency::from_code(&r.get::<String, _>("currency"))
                .unwrap_or(kernel::Currency::Usd);
            let posted = r.get::<i64, _>("posted") as i128;
            let reserved = r.get::<i64, _>("reserved") as i128;
            AccountView {
                account_id: id,
                owner_id: r
                    .try_get::<Uuid, _>("owner_id")
                    .map(|u| u.to_string())
                    .unwrap_or_default(),
                currency: currency.code().to_string(),
                status: r.get::<String, _>("status"),
                posted: Money::from_minor(posted, currency),
                reserved: Money::from_minor(reserved, currency),
                available: Money::from_minor(posted - reserved, currency),
                version: r.get::<i64, _>("version") as u64,
            }
        }))
    }

    async fn transfer(&self, id: TransferId) -> Result<Option<TransferView>, PortError> {
        let row = sqlx::query(
            "SELECT source, destination, amount, currency, status, failure_reason \
             FROM transfer_status_view WHERE transfer_id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(row.map(|r| {
            let ccy = kernel::Currency::from_code(&r.get::<String, _>("currency"))
                .unwrap_or(kernel::Currency::Usd);
            TransferView {
                transfer_id: id,
                source: AccountId::from_uuid(r.get::<Uuid, _>("source")),
                destination: AccountId::from_uuid(r.get::<Uuid, _>("destination")),
                amount: Money::from_minor(r.get::<i64, _>("amount") as i128, ccy),
                status: r.get::<String, _>("status"),
                failure_reason: r.try_get::<String, _>("failure_reason").ok(),
            }
        }))
    }

    async fn transactions(
        &self,
        account: AccountId,
        limit: u32,
        _cursor: Option<String>,
    ) -> Result<(Vec<TransactionEntry>, Option<String>), PortError> {
        let rows = sqlx::query(
            "SELECT transfer_id, direction, amount, currency, \
                    EXTRACT(EPOCH FROM occurred_at)::BIGINT AS ts \
             FROM transaction_history_view WHERE account_id = $1 \
             ORDER BY occurred_at DESC LIMIT $2",
        )
        .bind(account.as_uuid())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(store_err)?;

        let entries = rows
            .into_iter()
            .map(|r| {
                let ccy = kernel::Currency::from_code(&r.get::<String, _>("currency"))
                    .unwrap_or(kernel::Currency::Usd);
                TransactionEntry {
                    transfer_id: TransferId::from_uuid(r.get::<Uuid, _>("transfer_id")),
                    direction: r.get::<String, _>("direction"),
                    amount: Money::from_minor(r.get::<i64, _>("amount") as i128, ccy),
                    occurred_at: r.get::<i64, _>("ts"),
                }
            })
            .collect();
        Ok((entries, None))
    }
}

/// Streams committed events from the `events` table to the bus (ADR-0006).
#[derive(Clone)]
pub struct PgOutboxSource {
    pool: PgPool,
}

impl PgOutboxSource {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OutboxSource for PgOutboxSource {
    async fn fetch_after(&self, after: i64, limit: i64) -> infra::Result<Vec<OutboxRecord>> {
        let rows = sqlx::query(
            "SELECT global_seq, event_id, stream_id, topic, version, event_type, payload, metadata, occurred_at \
             FROM events WHERE global_seq > $1 ORDER BY global_seq ASC LIMIT $2",
        )
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for r in rows {
            let metadata: serde_json::Value = r.get("metadata");
            let correlation_id = metadata
                .get("correlation_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let envelope = EventEnvelope {
                event_type: r.get::<String, _>("event_type"),
                metadata: EventMetadata {
                    event_id: r.get::<Uuid, _>("event_id").to_string(),
                    stream_id: r.get::<Uuid, _>("stream_id").to_string(),
                    version: r.get::<i64, _>("version") as u64,
                    correlation_id,
                    causation_id: None,
                    traceparent: None,
                    occurred_at: r.get::<chrono::DateTime<Utc>, _>("occurred_at"),
                },
                payload: r.get::<serde_json::Value, _>("payload"),
            };
            records.push(OutboxRecord {
                global_seq: r.get::<i64, _>("global_seq"),
                topic: r.get::<String, _>("topic"),
                envelope,
            });
        }
        Ok(records)
    }

    async fn load_checkpoint(&self, relay: &str) -> infra::Result<i64> {
        let row = sqlx::query("SELECT last_published_seq FROM outbox_offset WHERE relay = $1")
            .bind(relay)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map_or(0, |r| r.get::<i64, _>("last_published_seq")))
    }

    async fn store_checkpoint(&self, relay: &str, seq: i64) -> infra::Result<()> {
        sqlx::query(
            "INSERT INTO outbox_offset (relay, last_published_seq) VALUES ($1, $2) \
             ON CONFLICT (relay) DO UPDATE SET last_published_seq = EXCLUDED.last_published_seq",
        )
        .bind(relay)
        .bind(seq)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

// ---- helpers ----

fn row_to_saga(id: TransferId, r: &sqlx::postgres::PgRow) -> TransferSaga {
    let ccy = kernel::Currency::from_code(&r.get::<String, _>("currency"))
        .unwrap_or(kernel::Currency::Usd);
    let status: String = r.get("status");
    let failure: Option<String> = r.try_get("failure_reason").ok();
    let state = match status.as_str() {
        "REQUESTED" => TransferState::Requested,
        "RESERVING" => TransferState::Reserving,
        "CREDITING" => TransferState::Crediting,
        "CAPTURING" => TransferState::Capturing,
        "COMPENSATING" => TransferState::Compensating {
            reason: failure.clone().unwrap_or_default(),
        },
        "COMPLETED" => TransferState::Completed,
        _ => TransferState::Failed {
            reason: failure.unwrap_or_default(),
        },
    };
    TransferSaga {
        transfer_id: id,
        source: AccountId::from_uuid(r.get::<Uuid, _>("source")),
        destination: AccountId::from_uuid(r.get::<Uuid, _>("destination")),
        amount: Money::from_minor(r.get::<i64, _>("amount") as i128, ccy),
        state,
    }
}

fn capitalize(s: &str) -> String {
    let lower = s.to_lowercase();
    let mut c = lower.chars();
    match c.next() {
        Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
        None => lower,
    }
}

/// Deterministic event id from (transfer, state) so lifecycle emission is idempotent.
fn deterministic_event_id(transfer: TransferId, state: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("{transfer}:{state}").as_bytes(),
    )
}
