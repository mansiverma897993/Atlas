//! Cross-context provisioning — auto-open a wallet when a user registers.
//!
//! An [`EventHandler`] over the `identity.user.v1` topic: on a `UserRegistered` event it calls
//! the Ledger's `OpenAccount` gRPC to provision a wallet for the new user (DOMAIN §6, the
//! "Ledger auto-opening a wallet on registration" integration). It is **idempotent** — dedup
//! on the user id via a [`DedupStore`] — so a redelivered registration never opens a second
//! wallet.
//!
//! The gRPC call is hidden behind the [`AccountOpener`] port so the handler's decision logic
//! is unit-testable with a fake and the real adapter (tonic client) lives at the edge.

use std::sync::Arc;

use async_trait::async_trait;

use infra::bus::{Ack, EventEnvelope, EventHandler};
use proto::ledger::ledger_service_client::LedgerServiceClient;
use proto::ledger::OpenAccountRequest;

use crate::store::DedupStore;

/// The event type on `identity.user.v1` that triggers wallet provisioning.
const USER_REGISTERED: &str = "UserRegistered";
/// Wallets are single-currency in v1 (DOMAIN §2.2); provision the platform default.
const DEFAULT_CURRENCY: &str = "USD";

/// **Port:** open a ledger account for an owner, returning the new account id.
#[async_trait]
pub trait AccountOpener: Send + Sync {
    /// Open an account (wallet) for `owner_id` in `currency`.
    async fn open_account(&self, owner_id: &str, currency: &str) -> anyhow::Result<String>;
}

/// gRPC adapter that opens accounts against the Ledger service (`ledger.v1`).
///
/// Connects lazily per call. That keeps worker startup independent of ledger availability and
/// means a transient ledger outage surfaces as a per-message [`Ack::Retry`] (→ retry budget →
/// DLQ) rather than crashing the consumer.
pub struct GrpcAccountOpener {
    endpoint: String,
}

impl GrpcAccountOpener {
    /// Target the Ledger gRPC endpoint (e.g. `http://ledger:50052`).
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

#[async_trait]
impl AccountOpener for GrpcAccountOpener {
    async fn open_account(&self, owner_id: &str, currency: &str) -> anyhow::Result<String> {
        let mut client = LedgerServiceClient::connect(self.endpoint.clone()).await?;
        let resp = client
            .open_account(OpenAccountRequest {
                owner_id: owner_id.to_string(),
                currency: currency.to_string(),
            })
            .await?;
        Ok(resp.into_inner().account_id)
    }
}

/// Extract the user id from a `UserRegistered` envelope (payload `user_id`, else stream id).
#[must_use]
pub fn user_id_of(env: &EventEnvelope) -> String {
    env.payload
        .get("user_id")
        .and_then(serde_json::Value::as_str)
        .map_or_else(|| env.metadata.stream_id.clone(), str::to_string)
}

/// The provisioning consumer: wires the dedup store + account opener together.
pub struct ProvisioningConsumer {
    dedup: Arc<dyn DedupStore>,
    opener: Arc<dyn AccountOpener>,
}

impl ProvisioningConsumer {
    /// Construct from the injected dedup store and account-opener adapters.
    #[must_use]
    pub fn new(dedup: Arc<dyn DedupStore>, opener: Arc<dyn AccountOpener>) -> Self {
        Self { dedup, opener }
    }

    /// The core idempotent decision, separated from the [`EventHandler`] shell so it is easy
    /// to test: ignore non-registration events, skip already-provisioned users, otherwise
    /// open a wallet and mark the user processed.
    async fn provision(&self, env: &EventEnvelope) -> Ack {
        if env.event_type != USER_REGISTERED {
            // Not our concern — commit so the offset advances past it.
            return Ack::Commit;
        }
        let user_id = user_id_of(env);

        match self.dedup.is_processed(&user_id).await {
            Ok(true) => {
                tracing::debug!(%user_id, "wallet already provisioned; skipping");
                return Ack::Commit;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::error!(error = %e, %user_id, "dedup lookup failed");
                return Ack::Retry;
            }
        }

        match self.opener.open_account(&user_id, DEFAULT_CURRENCY).await {
            Ok(account_id) => {
                if let Err(e) = self.dedup.mark_processed(&user_id).await {
                    // The wallet exists but we could not record it. Retrying is safe only if
                    // OpenAccount is idempotent; to avoid a duplicate wallet we still commit
                    // and log loudly for reconciliation.
                    tracing::error!(error = %e, %user_id, %account_id,
                        "wallet opened but mark_processed failed");
                }
                metrics::counter!("wallets_provisioned_total").increment(1);
                tracing::info!(%user_id, %account_id, "provisioned wallet for new user");
                Ack::Commit
            }
            Err(e) => {
                tracing::error!(error = %e, %user_id, "OpenAccount failed; will retry");
                Ack::Retry
            }
        }
    }
}

#[async_trait]
impl EventHandler for ProvisioningConsumer {
    async fn handle(&self, event: &EventEnvelope) -> Ack {
        self.provision(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::fakes::InMemoryDedup;
    use chrono::Utc;
    use infra::bus::EventMetadata;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Fake opener counting calls so we can assert exactly-once provisioning.
    #[derive(Default)]
    struct CountingOpener {
        calls: AtomicUsize,
        fail: bool,
    }
    #[async_trait]
    impl AccountOpener for CountingOpener {
        async fn open_account(&self, owner_id: &str, _currency: &str) -> anyhow::Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                anyhow::bail!("ledger unavailable");
            }
            Ok(format!("acct-for-{owner_id}"))
        }
    }

    fn user_registered(user_id: &str) -> EventEnvelope {
        EventEnvelope {
            event_type: USER_REGISTERED.to_string(),
            metadata: EventMetadata {
                event_id: uuid::Uuid::new_v4().to_string(),
                stream_id: user_id.to_string(),
                version: 1,
                correlation_id: "c".to_string(),
                causation_id: None,
                traceparent: None,
                occurred_at: Utc::now(),
            },
            payload: serde_json::json!({ "user_id": user_id }),
        }
    }

    #[tokio::test]
    async fn provisions_once_then_dedups() {
        let opener = Arc::new(CountingOpener::default());
        let consumer =
            ProvisioningConsumer::new(Arc::new(InMemoryDedup::default()), opener.clone());
        let env = user_registered("user-1");

        assert_eq!(consumer.handle(&env).await, Ack::Commit);
        // A redelivery of the same user must NOT open a second wallet.
        assert_eq!(consumer.handle(&env).await, Ack::Commit);
        assert_eq!(opener.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ignores_unrelated_events() {
        let opener = Arc::new(CountingOpener::default());
        let consumer =
            ProvisioningConsumer::new(Arc::new(InMemoryDedup::default()), opener.clone());
        let mut env = user_registered("user-2");
        env.event_type = "UserLoggedIn".to_string();

        assert_eq!(consumer.handle(&env).await, Ack::Commit);
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn retries_on_opener_failure_without_marking_processed() {
        let opener = Arc::new(CountingOpener {
            fail: true,
            ..Default::default()
        });
        let dedup = Arc::new(InMemoryDedup::default());
        let consumer = ProvisioningConsumer::new(dedup.clone(), opener.clone());
        let env = user_registered("user-3");

        assert_eq!(consumer.handle(&env).await, Ack::Retry);
        // Not marked processed, so a later delivery will try again.
        assert!(!dedup.is_processed("user-3").await.unwrap());
    }

    #[test]
    fn extracts_user_id_from_payload_then_stream() {
        let env = user_registered("user-9");
        assert_eq!(user_id_of(&env), "user-9");
        let mut env2 = env;
        env2.payload = serde_json::json!({}); // no user_id → fall back to stream id
        assert_eq!(user_id_of(&env2), "user-9");
    }
}
