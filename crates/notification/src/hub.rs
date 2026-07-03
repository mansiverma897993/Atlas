//! The in-process **connection hub**: a registry of live WebSocket connections keyed by user.
//!
//! Each connected socket owns a [`tokio::sync::mpsc`] sender; the hub maps a user id to the
//! set of that user's currently-connected senders (a user may have several — multiple tabs or
//! devices). Fan-out ([`ConnectionHub::broadcast_to`]) pushes a message onto every sender for a
//! user; the per-connection task in [`crate::adapters::ws`] drains its receiver onto the wire.
//!
//! The map is guarded by a [`RwLock`] (per the service brief — deliberately **not** `DashMap`):
//! reads (broadcast) dominate, registrations/removals are comparatively rare.
//!
//! ### Metrics
//! * `ws_connections` (gauge) — currently-registered sockets on this node.
//! * `ws_messages_sent_total` (counter) — messages enqueued to client sockets.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

/// The principal a connection belongs to — the JWT subject (a user id string).
pub type UserId = String;

/// Sender half of a per-connection channel. The socket task holds the receiver.
pub type Sender = mpsc::UnboundedSender<String>;

/// One registered connection: a stable id (for removal) plus its outbound channel.
struct Connection {
    id: u64,
    tx: Sender,
}

/// Cheaply-clonable registry of live connections, shared across the WS server and the
/// event-fan-out consumer.
#[derive(Clone, Default)]
pub struct ConnectionHub {
    inner: Arc<RwLock<HashMap<UserId, Vec<Connection>>>>,
    next_id: Arc<AtomicU64>,
}

impl ConnectionHub {
    /// Create an empty hub.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a connection for `user`, returning a **connection id** the caller passes back to
    /// [`unregister`](Self::unregister) on disconnect. Bumps the `ws_connections` gauge.
    pub async fn register(&self, user: UserId, tx: Sender) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut guard = self.inner.write().await;
            guard.entry(user).or_default().push(Connection { id, tx });
        }
        metrics::gauge!("ws_connections").increment(1.0);
        id
    }

    /// Remove the connection previously registered under `user` with `id`. Idempotent: removing
    /// an unknown connection is a no-op (and does not touch the gauge).
    pub async fn unregister(&self, user: &str, id: u64) {
        let mut removed = false;
        {
            let mut guard = self.inner.write().await;
            if let Some(conns) = guard.get_mut(user) {
                let before = conns.len();
                conns.retain(|c| c.id != id);
                removed = conns.len() != before;
                if conns.is_empty() {
                    guard.remove(user);
                }
            }
        }
        if removed {
            metrics::gauge!("ws_connections").decrement(1.0);
        }
    }

    /// Enqueue `message` to every connection of `user`, returning the number of sockets it
    /// reached (0 if the user has no local connections). Increments `ws_messages_sent_total`.
    ///
    /// Delivery is best-effort and non-blocking: a send only fails if the receiver has already
    /// been dropped (the socket task is tearing down and will `unregister` itself), so such
    /// connections are simply skipped here.
    pub async fn broadcast_to(&self, user: &str, message: &str) -> usize {
        let guard = self.inner.read().await;
        let Some(conns) = guard.get(user) else {
            return 0;
        };
        let mut delivered = 0usize;
        for conn in conns {
            if conn.tx.send(message.to_string()).is_ok() {
                delivered += 1;
            }
        }
        drop(guard);
        if delivered > 0 {
            metrics::counter!("ws_messages_sent_total").increment(delivered as u64);
        }
        delivered
    }

    /// Number of connections currently held locally for `user` (test/introspection helper).
    #[allow(dead_code)]
    pub async fn connection_count(&self, user: &str) -> usize {
        self.inner.read().await.get(user).map_or(0, Vec::len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::error::TryRecvError;

    #[tokio::test]
    async fn register_broadcast_unregister_roundtrip() {
        let hub = ConnectionHub::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        let id = hub.register("user-1".to_string(), tx).await;
        assert_eq!(hub.connection_count("user-1").await, 1);

        let n = hub.broadcast_to("user-1", "hello").await;
        assert_eq!(n, 1);
        assert_eq!(rx.try_recv().unwrap(), "hello");

        hub.unregister("user-1", id).await;
        assert_eq!(hub.connection_count("user-1").await, 0);

        // after unregister, broadcast reaches nobody
        assert_eq!(hub.broadcast_to("user-1", "again").await, 0);
    }

    #[tokio::test]
    async fn broadcast_fans_out_to_all_of_a_users_connections() {
        let hub = ConnectionHub::new();
        let (tx1, mut rx1) = mpsc::unbounded_channel::<String>();
        let (tx2, mut rx2) = mpsc::unbounded_channel::<String>();
        hub.register("user-1".to_string(), tx1).await;
        hub.register("user-1".to_string(), tx2).await;
        assert_eq!(hub.connection_count("user-1").await, 2);

        let n = hub.broadcast_to("user-1", "ping").await;
        assert_eq!(n, 2);
        assert_eq!(rx1.try_recv().unwrap(), "ping");
        assert_eq!(rx2.try_recv().unwrap(), "ping");
    }

    #[tokio::test]
    async fn broadcast_to_unknown_user_is_zero() {
        let hub = ConnectionHub::new();
        assert_eq!(hub.broadcast_to("nobody", "x").await, 0);
    }

    #[tokio::test]
    async fn unregister_leaves_sibling_connections_intact() {
        let hub = ConnectionHub::new();
        let (tx1, mut rx1) = mpsc::unbounded_channel::<String>();
        let (tx2, _rx2) = mpsc::unbounded_channel::<String>();
        let id1 = hub.register("user-1".to_string(), tx1).await;
        hub.register("user-1".to_string(), tx2).await;

        hub.unregister("user-1", id1).await;
        assert_eq!(hub.connection_count("user-1").await, 1);
        // the surviving connection still receives
        assert_eq!(hub.broadcast_to("user-1", "y").await, 1);
        assert!(matches!(rx1.try_recv(), Err(TryRecvError::Disconnected)));
    }
}
