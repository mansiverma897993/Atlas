//! Redis connection management.
//!
//! Wraps [`redis::aio::ConnectionManager`], which multiplexes commands over a single
//! connection and transparently reconnects. Cheap to clone and share across tasks.

use config::RedisConfig;
use redis::aio::ConnectionManager;
use redis::Client;

use crate::error::Result;

/// A shareable Redis handle.
#[derive(Clone)]
pub struct RedisPool {
    manager: ConnectionManager,
}

impl RedisPool {
    /// Connect and build a connection manager.
    pub async fn connect(config: &RedisConfig) -> Result<Self> {
        let client = Client::open(config.url.as_str())?;
        let manager = ConnectionManager::new(client).await?;
        Ok(Self { manager })
    }

    /// A cloned connection handle for issuing commands.
    #[must_use]
    pub fn conn(&self) -> ConnectionManager {
        self.manager.clone()
    }

    /// Verify Redis answers `PING`. Used by readiness checks.
    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.manager.clone();
        redis::cmd("PING").query_async::<_, ()>(&mut conn).await?;
        Ok(())
    }
}
