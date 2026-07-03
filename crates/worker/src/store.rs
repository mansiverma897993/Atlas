//! Idempotency store port + Postgres adapter.
//!
//! Consumers on the event backbone see **at-least-once** delivery, so every handler must be
//! idempotent (ARCHITECTURE §5.4). The provisioning consumer dedups on a *business* id (the
//! user id) via [`DedupStore`]: it records that it has acted on an id and refuses to act
//! twice. The trait is the seam that lets the pure decision logic be unit-tested against an
//! in-memory fake instead of a real database.

use async_trait::async_trait;
use sqlx::PgPool;

/// **Port:** "have we already processed this id?" bookkeeping for idempotent consumers.
#[async_trait]
pub trait DedupStore: Send + Sync {
    /// Returns `true` if `id` has already been marked processed.
    async fn is_processed(&self, id: &str) -> anyhow::Result<bool>;

    /// Record `id` as processed. Idempotent: marking an already-present id is a no-op.
    async fn mark_processed(&self, id: &str) -> anyhow::Result<()>;
}

/// Postgres-backed [`DedupStore`] over the `processed_events` table.
#[derive(Clone)]
pub struct PgDedupStore {
    pool: PgPool,
}

impl PgDedupStore {
    /// Wrap a connection pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DedupStore for PgDedupStore {
    async fn is_processed(&self, id: &str) -> anyhow::Result<bool> {
        let exists: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM processed_events WHERE event_id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(exists.is_some())
    }

    async fn mark_processed(&self, id: &str) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod fakes {
    //! In-memory fakes used by unit tests across the crate.
    use super::DedupStore;
    use async_trait::async_trait;
    use std::collections::HashSet;
    use std::sync::Mutex;

    /// An in-memory [`DedupStore`] for tests.
    #[derive(Default)]
    pub struct InMemoryDedup {
        seen: Mutex<HashSet<String>>,
    }

    #[async_trait]
    impl DedupStore for InMemoryDedup {
        async fn is_processed(&self, id: &str) -> anyhow::Result<bool> {
            Ok(self.seen.lock().unwrap().contains(id))
        }
        async fn mark_processed(&self, id: &str) -> anyhow::Result<()> {
            self.seen.lock().unwrap().insert(id.to_string());
            Ok(())
        }
    }
}
