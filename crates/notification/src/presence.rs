//! **Presence** tracking in Redis: who is currently connected, cluster-wide.
//!
//! On connect a user is added to the `presence:online` set (for enumeration) and given a
//! per-user key `presence:user:<id>` with a TTL. The socket task refreshes that TTL on a
//! heartbeat, so a node that dies without a clean disconnect lets the user's presence **expire**
//! rather than leak. On a clean disconnect both are removed.
//!
//! Presence is intentionally node-agnostic: with N replicas the same user may be connected to
//! several, but presence is a single logical "is this user online anywhere" — the cross-node
//! message routing (see [`crate::pubsub`]) is what actually reaches a socket on another node.

use infra::redis_pool::RedisPool;
use infra::Result;

/// Redis set holding every currently-online user id.
const ONLINE_SET: &str = "presence:online";

/// Redis-backed presence tracker.
#[derive(Clone)]
pub struct Presence {
    redis: RedisPool,
    /// TTL (seconds) of the per-user liveness key; the heartbeat must refresh within this.
    ttl_seconds: u64,
}

impl Presence {
    /// Build a tracker whose per-user keys live for `ttl_seconds` between heartbeats.
    #[must_use]
    pub fn new(redis: RedisPool, ttl_seconds: u64) -> Self {
        Self {
            redis,
            ttl_seconds: ttl_seconds.max(1),
        }
    }

    fn user_key(user: &str) -> String {
        format!("presence:user:{user}")
    }

    /// Mark `user` online: add to the online set and (re)arm the TTL key.
    pub async fn mark_online(&self, user: &str) -> Result<()> {
        let mut conn = self.redis.conn();
        redis::pipe()
            .cmd("SADD")
            .arg(ONLINE_SET)
            .arg(user)
            .ignore()
            .cmd("SET")
            .arg(Self::user_key(user))
            .arg(1)
            .arg("EX")
            .arg(self.ttl_seconds)
            .ignore()
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }

    /// Refresh the TTL on `user`'s presence key (called on the socket heartbeat).
    pub async fn heartbeat(&self, user: &str) -> Result<()> {
        let mut conn = self.redis.conn();
        redis::cmd("SET")
            .arg(Self::user_key(user))
            .arg(1)
            .arg("EX")
            .arg(self.ttl_seconds)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }

    /// Mark `user` offline (clean disconnect): drop from the set and delete the TTL key.
    pub async fn mark_offline(&self, user: &str) -> Result<()> {
        let mut conn = self.redis.conn();
        redis::pipe()
            .cmd("SREM")
            .arg(ONLINE_SET)
            .arg(user)
            .ignore()
            .cmd("DEL")
            .arg(Self::user_key(user))
            .ignore()
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }

    /// Whether `user` currently has a live presence key. Uses the TTL key (not the set) so a
    /// crashed node's stale entry reads as offline once it expires.
    ///
    /// Exposed for operational/introspection use (e.g. a future "deliver-or-store" path);
    /// not on the hot connect/disconnect path.
    #[allow(dead_code)]
    pub async fn is_online(&self, user: &str) -> Result<bool> {
        let mut conn = self.redis.conn();
        let exists: bool = redis::cmd("EXISTS")
            .arg(Self::user_key(user))
            .query_async(&mut conn)
            .await?;
        Ok(exists)
    }
}
