//! Cross-node message routing over **Redis Pub/Sub** — the horizontal-scale seam.
//!
//! With N notification replicas behind a load balancer, the replica that *consumes* a ledger
//! event is rarely the one holding the target user's socket. So delivery has two paths:
//!
//! 1. **Local fast path** — the consumer pushes straight into any socket for the user it holds
//!    itself (via [`crate::hub::ConnectionHub::broadcast_to`]).
//! 2. **Cross-node path** — it also `PUBLISH`es the message to a per-user channel
//!    `ws:user:<id>`. Every replica runs [`run_subscriber`], which `PSUBSCRIBE`s `ws:user:*`
//!    and delivers incoming messages to *its* locally-held sockets.
//!
//! To avoid the origin node delivering twice (once locally, once via its own echo), each
//! published message is tagged with the origin `node_id`; the subscriber drops messages it
//! itself published.
//!
//! Redis's `ConnectionManager` multiplexes normal commands and cannot enter subscribe mode, so
//! the subscriber opens its **own dedicated** connection from the Redis URL.

use std::time::Duration;

use futures::StreamExt;
use infra::redis_pool::RedisPool;
use infra::Result;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::hub::ConnectionHub;

/// Channel-name prefix for per-user routing channels.
pub const USER_CHANNEL_PREFIX: &str = "ws:user:";

/// The per-user Redis channel a message for `user` is published to.
#[must_use]
pub fn user_channel(user: &str) -> String {
    format!("{USER_CHANNEL_PREFIX}{user}")
}

/// Envelope published on a user channel: the client message plus the originating node id (so the
/// origin can skip its own echo).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoutedMessage {
    /// Node that published this (the local fast path already delivered it there).
    origin: String,
    /// The serialized [`crate::message::ClientMessage`] to deliver.
    message: String,
}

/// Publishes outbound messages to per-user Redis channels for other replicas to pick up.
#[derive(Clone)]
pub struct PubSubRouter {
    redis: RedisPool,
    node_id: String,
}

impl PubSubRouter {
    /// Create a router stamping published messages with `node_id`.
    #[must_use]
    pub fn new(redis: RedisPool, node_id: String) -> Self {
        Self { redis, node_id }
    }

    /// Publish `message` to `user`'s channel so other replicas can deliver it locally.
    pub async fn publish(&self, user: &str, message: &str) -> Result<()> {
        let payload = serde_json::to_string(&RoutedMessage {
            origin: self.node_id.clone(),
            message: message.to_string(),
        })?;
        let mut conn = self.redis.conn();
        redis::cmd("PUBLISH")
            .arg(user_channel(user))
            .arg(payload)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }
}

/// Run the cross-node subscriber until `cancel` fires, reconnecting on transient failures.
///
/// `node_id` must match the [`PubSubRouter`]'s so this node ignores its own publications.
pub async fn run_subscriber(
    redis_url: String,
    hub: ConnectionHub,
    node_id: String,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }
        match subscribe_loop(&redis_url, &hub, &node_id, &cancel).await {
            Ok(()) => break, // cancelled cleanly
            Err(e) => {
                tracing::warn!(error = %e, "ws pubsub subscriber dropped; reconnecting");
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
    tracing::info!("ws pubsub subscriber stopped");
}

/// One connect → subscribe → drain cycle. Returns `Ok(())` only on cancellation.
async fn subscribe_loop(
    redis_url: &str,
    hub: &ConnectionHub,
    node_id: &str,
    cancel: &CancellationToken,
) -> Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.psubscribe(format!("{USER_CHANNEL_PREFIX}*")).await?;
    tracing::info!(pattern = %format!("{USER_CHANNEL_PREFIX}*"), "ws pubsub subscribed");

    let mut stream = pubsub.on_message();
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            msg = stream.next() => {
                let Some(msg) = msg else { return Ok(()); };
                let channel = msg.get_channel_name().to_string();
                let user = channel
                    .strip_prefix(USER_CHANNEL_PREFIX)
                    .unwrap_or(&channel)
                    .to_string();
                let Ok(payload) = msg.get_payload::<String>() else { continue };
                let Ok(routed) = serde_json::from_str::<RoutedMessage>(&payload) else { continue };
                // Skip our own echo — the local fast path already delivered it.
                if routed.origin == node_id {
                    continue;
                }
                hub.broadcast_to(&user, &routed.message).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_channel_is_prefixed() {
        assert_eq!(user_channel("abc"), "ws:user:abc");
        assert!(user_channel("abc").starts_with(USER_CHANNEL_PREFIX));
    }

    #[test]
    fn routed_message_round_trips() {
        let r = RoutedMessage {
            origin: "node-1".to_string(),
            message: "{\"type\":\"transfer.completed\"}".to_string(),
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: RoutedMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back.origin, "node-1");
        assert_eq!(back.message, r.message);
    }
}
