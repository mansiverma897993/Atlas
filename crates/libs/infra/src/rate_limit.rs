//! Distributed token-bucket rate limiter over Redis.
//!
//! Keyed per subject/IP at the gateway. The bucket state (tokens + last-refill timestamp) is
//! held in a Redis hash and updated **atomically in a Lua script**, so concurrent gateway
//! replicas share one consistent limit (the check-and-decrement can't race). Tokens refill
//! continuously at `refill_per_sec`; a request costs one token.

use redis::Script;

use crate::error::Result;
use crate::redis_pool::RedisPool;

/// Outcome of a rate-limit check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateDecision {
    /// Whether the request is allowed.
    pub allowed: bool,
    /// Tokens remaining after this request.
    pub remaining: u64,
    /// Seconds until at least one token is available (0 if allowed now).
    pub retry_after_secs: u64,
}

/// A Redis token-bucket limiter.
#[derive(Clone)]
pub struct RateLimiter {
    redis: RedisPool,
    /// Bucket capacity (burst size).
    capacity: u64,
    /// Steady-state refill rate.
    refill_per_sec: u64,
}

impl RateLimiter {
    /// Create a limiter with a burst `capacity` and steady `refill_per_sec`.
    #[must_use]
    pub fn new(redis: RedisPool, capacity: u64, refill_per_sec: u64) -> Self {
        Self {
            redis,
            capacity,
            refill_per_sec,
        }
    }

    /// Atomically consume one token for `key`. `now_ms` is the caller's clock in millis.
    pub async fn check(&self, key: &str, now_ms: u64) -> Result<RateDecision> {
        // KEYS[1]=bucket  ARGV: capacity, refill_per_sec, now_ms, cost
        const SCRIPT: &str = r"
            local cap   = tonumber(ARGV[1])
            local rate  = tonumber(ARGV[2])
            local now   = tonumber(ARGV[3])
            local cost  = tonumber(ARGV[4])
            local data  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
            local tokens = tonumber(data[1])
            local ts     = tonumber(data[2])
            if tokens == nil then tokens = cap; ts = now end
            -- refill based on elapsed time
            local elapsed = math.max(0, now - ts) / 1000.0
            tokens = math.min(cap, tokens + elapsed * rate)
            local allowed = 0
            local retry = 0
            if tokens >= cost then
                tokens = tokens - cost
                allowed = 1
            else
                local deficit = cost - tokens
                retry = math.ceil(deficit / rate)
            end
            redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
            redis.call('PEXPIRE', KEYS[1], math.ceil(cap / rate * 1000) + 1000)
            return { allowed, math.floor(tokens), retry }";

        let mut conn = self.redis.conn();
        let (allowed, remaining, retry): (i64, i64, i64) = Script::new(SCRIPT)
            .key(key)
            .arg(self.capacity)
            .arg(self.refill_per_sec)
            .arg(now_ms)
            .arg(1)
            .invoke_async(&mut conn)
            .await?;

        Ok(RateDecision {
            allowed: allowed == 1,
            remaining: remaining.max(0) as u64,
            retry_after_secs: retry.max(0) as u64,
        })
    }
}
