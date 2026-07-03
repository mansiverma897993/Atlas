//! Event fan-out: the [`EventHandler`] that turns consumed ledger events into client pushes.
//!
//! Registered against both `ledger.transfer.v1` and `ledger.account.v1` (see `main`). For each
//! event it: maps it to a [`ClientMessage`](crate::message::ClientMessage), works out the target
//! users, delivers to any locally-held sockets, and re-publishes to Redis so other replicas
//! deliver to theirs (ARCHITECTURE §5.3).
//!
//! **Idempotency:** delivery is inherently safe to repeat — a client dedups on the message's
//! `event_id` — so the handler always returns [`Ack::Commit`] after a best-effort push; a Redis
//! hiccup degrades reach but never blocks offset progress or dead-letters a good event.
//!
//! **Account → owner resolution:** account-stream events (e.g. `FundsCredited`) name only an
//! account id, not its owner. The handler learns the mapping from `AccountOpened` events (which
//! *do* carry the owner) and caches it in Redis as `account_owner:<account_id>`, then uses it to
//! route later account events. Account ids with no known owner are still routed on directly
//! (a connection may have registered under an account-scoped id) — a documented fallback seam.

use std::collections::HashSet;

use async_trait::async_trait;
use infra::bus::{Ack, EventEnvelope, EventHandler};
use infra::redis_pool::RedisPool;
use infra::Result;

use crate::hub::ConnectionHub;
use crate::message;
use crate::pubsub::PubSubRouter;

/// Fans consumed ledger events out to connected clients.
pub struct FanoutHandler {
    hub: ConnectionHub,
    router: PubSubRouter,
    redis: RedisPool,
}

impl FanoutHandler {
    /// Wire the handler with the local hub, the cross-node router, and Redis (for the
    /// account→owner map).
    #[must_use]
    pub fn new(hub: ConnectionHub, router: PubSubRouter, redis: RedisPool) -> Self {
        Self { hub, router, redis }
    }

    fn owner_key(account: &str) -> String {
        format!("account_owner:{account}")
    }

    /// Cache the owner of an account (learned from `AccountOpened`).
    async fn remember_owner(&self, account: &str, owner: &str) -> Result<()> {
        let mut conn = self.redis.conn();
        redis::cmd("SET")
            .arg(Self::owner_key(account))
            .arg(owner)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }

    /// Look up the cached owner of an account, if known.
    async fn lookup_owner(&self, account: &str) -> Result<Option<String>> {
        let mut conn = self.redis.conn();
        let owner: Option<String> = redis::cmd("GET")
            .arg(Self::owner_key(account))
            .query_async(&mut conn)
            .await?;
        Ok(owner)
    }
}

#[async_trait]
impl EventHandler for FanoutHandler {
    async fn handle(&self, event: &EventEnvelope) -> Ack {
        let Some(mapped) = message::map_event(event) else {
            return Ack::Commit; // not client-relevant
        };

        // Learn account→owner from AccountOpened so later account events can be routed to owners.
        if event.event_type == "AccountOpened" {
            if let Some(owner) = mapped.owners.first() {
                if let Err(e) = self.remember_owner(&event.metadata.stream_id, owner).await {
                    tracing::warn!(error = %e, "failed to cache account owner");
                }
            }
        }

        // Resolve the full recipient set: owners named directly, owners resolved from account
        // ids, and (fallback seam) the account ids themselves.
        let mut recipients: HashSet<String> = mapped.owners.iter().cloned().collect();
        for account in &mapped.accounts {
            match self.lookup_owner(account).await {
                Ok(Some(owner)) => {
                    recipients.insert(owner);
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(error = %e, account, "owner lookup failed"),
            }
            recipients.insert(account.clone());
        }

        if recipients.is_empty() {
            return Ack::Commit;
        }

        let text = match serde_json::to_string(&mapped.message) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error = %e, "failed to serialize client message");
                return Ack::Commit; // unmappable payload is not retryable
            }
        };

        let mut local_delivered = 0usize;
        for user in &recipients {
            // 1. deliver to any socket held on this node
            local_delivered += self.hub.broadcast_to(user, &text).await;
            // 2. route to other replicas that may hold the user's socket
            if let Err(e) = self.router.publish(user, &text).await {
                tracing::warn!(error = %e, user, "cross-node publish failed");
            }
        }

        metrics::counter!("notifications_fanout_total").increment(1);
        tracing::debug!(
            event_type = %event.event_type,
            recipients = recipients.len(),
            local_delivered,
            "fanned out ledger event"
        );
        Ack::Commit
    }
}
