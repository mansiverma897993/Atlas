//! PostgreSQL adapters — the real repositories, the outbox writer, and the outbox source that
//! feeds the relay.
//!
//! Design notes:
//! * **Registration is atomic.** [`PgUserRepository::register`] inserts the user, credential,
//!   role grants, *and* the `UserRegistered` outbox row in **one transaction** — the outbox
//!   write cannot diverge from the state change (no dual-write, ADR-0006).
//! * **Runtime queries only.** All SQL uses `sqlx::query`/`query_scalar` (not the compile-time
//!   `query!` macro), so the crate builds offline with no `DATABASE_URL`.
//! * All SQL uses parameter binding — never string interpolation — the injection defense.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::application::ports::{
    Credentials, Effective, NewUser, OAuthUserInfo, OutboxEvent, OutboxWriter, PortError,
    RefreshTokenRecord, RefreshTokenRepository, RoleRepository, UserRepository,
};
use crate::domain::user::{UserId, UserStatus};

use infra::bus::{EventEnvelope, EventMetadata};
use infra::outbox::{OutboxRecord, OutboxSource};

fn store_err(e: sqlx::Error) -> PortError {
    PortError::Store(e.to_string())
}

/// Insert one outbox row on the given executor (shared by the repositories and the standalone
/// [`PgOutboxWriter`]). Kept as a free function so registration can enqueue the event inside its
/// own transaction.
async fn insert_outbox<'e, E>(exec: E, event: &OutboxEvent) -> Result<(), PortError>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "INSERT INTO outbox (id, aggregate_id, topic, event_type, payload) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(event.aggregate_id)
    .bind(&event.topic)
    .bind(&event.event_type)
    .bind(&event.payload)
    .execute(exec)
    .await
    .map_err(store_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Users / credentials / OAuth
// ---------------------------------------------------------------------------------------------

/// Postgres-backed [`UserRepository`].
#[derive(Clone)]
pub struct PgUserRepository {
    pool: PgPool,
}

impl PgUserRepository {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UserRepository for PgUserRepository {
    async fn register(&self, new: NewUser) -> Result<(), PortError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;

        let user_row = sqlx::query(
            "INSERT INTO users (id, email, display_name, status, created_at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(new.user.id.as_uuid())
        .bind(&new.user.email)
        .bind(&new.user.display_name)
        .bind(new.user.status.as_str())
        .bind(new.user.created_at)
        .execute(&mut *tx)
        .await;

        if let Err(sqlx::Error::Database(db)) = &user_row {
            if db.is_unique_violation() {
                return Err(PortError::UniqueViolation);
            }
        }
        user_row.map_err(store_err)?;

        sqlx::query(
            "INSERT INTO credentials (user_id, password_hash, updated_at) VALUES ($1, $2, now())",
        )
        .bind(new.user.id.as_uuid())
        .bind(&new.password_hash)
        .execute(&mut *tx)
        .await
        .map_err(store_err)?;

        for role in &new.roles {
            sqlx::query(
                "INSERT INTO user_roles (user_id, role_name) VALUES ($1, $2) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(new.user.id.as_uuid())
            .bind(role)
            .execute(&mut *tx)
            .await
            .map_err(store_err)?;
        }

        insert_outbox(&mut *tx, &new.event).await?;
        tx.commit().await.map_err(store_err)?;
        Ok(())
    }

    async fn find_credentials_by_email(
        &self,
        email: &str,
    ) -> Result<Option<Credentials>, PortError> {
        let row = sqlx::query(
            "SELECT u.id, u.status, c.password_hash \
             FROM users u JOIN credentials c ON c.user_id = u.id \
             WHERE u.email = $1",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)?;

        Ok(row.map(|r| Credentials {
            user_id: UserId::from_uuid(r.get::<Uuid, _>("id")),
            password_hash: r.get::<String, _>("password_hash"),
            active: matches!(
                UserStatus::from_str_lenient(&r.get::<String, _>("status")),
                UserStatus::Active
            ),
        }))
    }

    async fn find_user_id_by_oauth(
        &self,
        provider: &str,
        subject: &str,
    ) -> Result<Option<UserId>, PortError> {
        let row = sqlx::query(
            "SELECT user_id FROM oauth_identities WHERE provider = $1 AND subject = $2",
        )
        .bind(provider)
        .bind(subject)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(row.map(|r| UserId::from_uuid(r.get::<Uuid, _>("user_id"))))
    }

    async fn create_from_oauth(
        &self,
        info: &OAuthUserInfo,
        display_name: &str,
        roles: &[String],
        event: OutboxEvent,
    ) -> Result<UserId, PortError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        let id = UserId::new();

        sqlx::query(
            "INSERT INTO users (id, email, display_name, status, created_at) \
             VALUES ($1, $2, $3, 'ACTIVE', now())",
        )
        .bind(id.as_uuid())
        // OAuth users may share/lack an email; use a synthetic unique handle when empty.
        .bind(if info.email.is_empty() {
            format!("{}:{}@oauth.local", info.provider, info.subject)
        } else {
            info.email.clone()
        })
        .bind(display_name)
        .execute(&mut *tx)
        .await
        .map_err(store_err)?;

        sqlx::query(
            "INSERT INTO oauth_identities (provider, subject, user_id, email) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&info.provider)
        .bind(&info.subject)
        .bind(id.as_uuid())
        .bind(&info.email)
        .execute(&mut *tx)
        .await
        .map_err(store_err)?;

        for role in roles {
            sqlx::query(
                "INSERT INTO user_roles (user_id, role_name) VALUES ($1, $2) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(id.as_uuid())
            .bind(role)
            .execute(&mut *tx)
            .await
            .map_err(store_err)?;
        }

        insert_outbox(&mut *tx, &event).await?;
        tx.commit().await.map_err(store_err)?;
        Ok(id)
    }
}

// ---------------------------------------------------------------------------------------------
// Refresh tokens
// ---------------------------------------------------------------------------------------------

/// Postgres-backed [`RefreshTokenRepository`].
#[derive(Clone)]
pub struct PgRefreshTokenRepository {
    pool: PgPool,
}

impl PgRefreshTokenRepository {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RefreshTokenRepository for PgRefreshTokenRepository {
    async fn insert(&self, record: &RefreshTokenRecord) -> Result<(), PortError> {
        sqlx::query(
            "INSERT INTO refresh_tokens \
             (id, user_id, token_hash, family_id, used, expires_at, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(record.id)
        .bind(record.user_id.as_uuid())
        .bind(&record.token_hash)
        .bind(record.family_id)
        .bind(record.used)
        .bind(record.expires_at)
        .bind(record.created_at)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    async fn find_by_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<RefreshTokenRecord>, PortError> {
        let row = sqlx::query(
            "SELECT id, user_id, token_hash, family_id, used, expires_at, created_at \
             FROM refresh_tokens WHERE token_hash = $1",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)?;

        Ok(row.map(|r| RefreshTokenRecord {
            id: r.get::<Uuid, _>("id"),
            user_id: UserId::from_uuid(r.get::<Uuid, _>("user_id")),
            token_hash: r.get::<String, _>("token_hash"),
            family_id: r.get::<Uuid, _>("family_id"),
            used: r.get::<bool, _>("used"),
            expires_at: r.get::<DateTime<Utc>, _>("expires_at"),
            created_at: r.get::<DateTime<Utc>, _>("created_at"),
        }))
    }

    async fn mark_used(&self, id: Uuid) -> Result<(), PortError> {
        sqlx::query("UPDATE refresh_tokens SET used = true WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(store_err)?;
        Ok(())
    }

    async fn revoke_family(&self, family_id: Uuid) -> Result<u64, PortError> {
        let result = sqlx::query("DELETE FROM refresh_tokens WHERE family_id = $1")
            .bind(family_id)
            .execute(&self.pool)
            .await
            .map_err(store_err)?;
        Ok(result.rows_affected())
    }
}

// ---------------------------------------------------------------------------------------------
// Roles / RBAC
// ---------------------------------------------------------------------------------------------

/// Postgres-backed [`RoleRepository`].
#[derive(Clone)]
pub struct PgRoleRepository {
    pool: PgPool,
}

impl PgRoleRepository {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RoleRepository for PgRoleRepository {
    async fn effective_permissions(&self, user_id: UserId) -> Result<Effective, PortError> {
        let role_rows = sqlx::query("SELECT role_name FROM user_roles WHERE user_id = $1")
            .bind(user_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(store_err)?;
        let roles: Vec<String> = role_rows
            .into_iter()
            .map(|r| r.get::<String, _>("role_name"))
            .collect();

        let perm_rows = sqlx::query(
            "SELECT DISTINCT rp.permission \
             FROM user_roles ur JOIN role_permissions rp ON rp.role_name = ur.role_name \
             WHERE ur.user_id = $1",
        )
        .bind(user_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(store_err)?;
        let permissions: Vec<String> = perm_rows
            .into_iter()
            .map(|r| r.get::<String, _>("permission"))
            .collect();

        Ok(Effective { roles, permissions })
    }
}

// ---------------------------------------------------------------------------------------------
// Outbox writer + source
// ---------------------------------------------------------------------------------------------

/// Standalone [`OutboxWriter`] (used for non-registration events; registration enqueues its
/// event inside its own transaction via [`PgUserRepository::register`]).
#[derive(Clone)]
pub struct PgOutboxWriter {
    pool: PgPool,
}

impl PgOutboxWriter {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OutboxWriter for PgOutboxWriter {
    async fn write(&self, event: OutboxEvent) -> Result<(), PortError> {
        insert_outbox(&self.pool, &event).await
    }
}

/// Streams committed events from the `outbox` table to the bus (ADR-0006). Mirrors the ledger's
/// event-store source, adapted to Identity's simpler CRUD outbox.
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
            "SELECT global_seq, id, aggregate_id, topic, event_type, payload, created_at \
             FROM outbox WHERE global_seq > $1 ORDER BY global_seq ASC LIMIT $2",
        )
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for r in rows {
            let global_seq: i64 = r.get("global_seq");
            let envelope = EventEnvelope {
                event_type: r.get::<String, _>("event_type"),
                metadata: EventMetadata {
                    event_id: r.get::<Uuid, _>("id").to_string(),
                    stream_id: r.get::<Uuid, _>("aggregate_id").to_string(),
                    version: global_seq as u64,
                    correlation_id: r.get::<Uuid, _>("id").to_string(),
                    causation_id: None,
                    traceparent: None,
                    occurred_at: r.get::<DateTime<Utc>, _>("created_at"),
                },
                payload: r.get::<serde_json::Value, _>("payload"),
            };
            records.push(OutboxRecord {
                global_seq,
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
