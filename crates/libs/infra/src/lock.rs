//! Redis-backed distributed lock (single-node Redlock).
//!
//! Used to **serialize the command pipeline per account** so concurrent transfers touching
//! the same account don't thrash on optimistic-concurrency retries (DOMAIN §2.1). This lock
//! is a *performance* optimization, **not** the correctness mechanism — correctness rests on
//! the event store's `UNIQUE(stream_id, version)` guard. So a lost lock (e.g. Redis failover)
//! can never cause a double-spend; at worst it causes an extra optimistic retry.
//!
//! Acquire = `SET key <token> NX PX <ttl>`. Release is a Lua compare-and-delete so a holder
//! only ever releases its *own* lock (never one that already expired and was re-acquired).

use std::time::Duration;

use redis::Script;
use uuid::Uuid;

use crate::error::{InfraError, Result};
use crate::redis_pool::RedisPool;

/// A distributed lock manager over Redis.
#[derive(Clone)]
pub struct DistributedLock {
    redis: RedisPool,
}

/// An RAII-ish guard proving ownership of a held lock. Call [`LockGuard::release`] when done;
/// otherwise the lock self-expires after its TTL (a safety net against crashed holders).
pub struct LockGuard {
    redis: RedisPool,
    key: String,
    token: String,
}

impl DistributedLock {
    /// Create a lock manager.
    #[must_use]
    pub fn new(redis: RedisPool) -> Self {
        Self { redis }
    }

    /// Try once to acquire `key` for `ttl`. Returns `None` if already held.
    pub async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<Option<LockGuard>> {
        let token = Uuid::new_v4().to_string();
        let mut conn = self.redis.conn();
        let acquired: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(&token)
            .arg("NX")
            .arg("PX")
            .arg(ttl.as_millis() as u64)
            .query_async(&mut conn)
            .await?;

        Ok(acquired.map(|_| LockGuard {
            redis: self.redis.clone(),
            key: key.to_string(),
            token,
        }))
    }

    /// Acquire `key`, retrying with a small fixed backoff until `wait` elapses.
    pub async fn acquire(&self, key: &str, ttl: Duration, wait: Duration) -> Result<LockGuard> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if let Some(guard) = self.try_acquire(key, ttl).await? {
                return Ok(guard);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(InfraError::LockTimeout(key.to_string()));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl LockGuard {
    /// Release the lock iff we still own it (compare-and-delete). Idempotent.
    pub async fn release(self) -> Result<()> {
        const RELEASE: &str = r"
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end";
        let mut conn = self.redis.conn();
        Script::new(RELEASE)
            .key(&self.key)
            .arg(&self.token)
            .invoke_async::<_, i64>(&mut conn)
            .await?;
        Ok(())
    }
}
