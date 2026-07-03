//! PostgreSQL connection pool.
//!
//! A single [`sqlx::PgPool`] per service, sized from [`DatabaseConfig`]. Uses SQLx with the
//! rustls TLS backend (no native OpenSSL), so the whole tree builds without a C toolchain.
//! Queries throughout the codebase use SQLx's parameter binding (never string interpolation),
//! which is our SQL-injection defense.

use std::time::Duration;

use config::DatabaseConfig;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool};

use crate::error::Result;

/// Build a connection pool from configuration.
///
/// The pool lazily establishes connections; call [`ping`] afterwards to fail fast at startup
/// if the database is unreachable (readiness).
pub async fn connect(config: &DatabaseConfig) -> Result<PgPool> {
    let connect_opts: PgConnectOptions = config
        .url
        .parse::<PgConnectOptions>()?
        // Route slow statements to the tracing log at WARN.
        .log_slow_statements(tracing::log::LevelFilter::Warn, Duration::from_millis(500));

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(config.acquire_timeout())
        .connect_with(connect_opts)
        .await?;

    Ok(pool)
}

/// Verify the database answers a trivial query. Used by readiness checks.
pub async fn ping(pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}
