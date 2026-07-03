//! Command handlers (the CQRS **write** side). They load an aggregate through the
//! [`EventStore`] port, call the pure `decide`, and append the resulting events under
//! optimistic concurrency, retrying on conflict. No events are published here — the outbox
//! relay does that from the committed store (ADR-0006).

use std::sync::Arc;

use kernel::{AccountId, Currency, Money, OwnerId, TransferId};

use super::ports::{EventStore, IdempotencyStore, PortError, TransferStore};
use crate::domain::account::{Account, AccountCommand};
use crate::domain::transfer::TransferSaga;

/// Application-level command errors.
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// A business rule rejected the command.
    #[error(transparent)]
    Domain(#[from] crate::domain::error::DomainError),
    /// A port/store error (including exhausted optimistic-concurrency retries).
    #[error(transparent)]
    Port(#[from] PortError),
}

/// Handlers for ledger write commands. Cheap to clone (holds `Arc`s).
#[derive(Clone)]
pub struct CommandHandlers {
    events: Arc<dyn EventStore>,
    transfers: Arc<dyn TransferStore>,
    idempotency: Arc<dyn IdempotencyStore>,
    /// Max optimistic-concurrency retries before surfacing a conflict.
    max_retries: u32,
}

impl CommandHandlers {
    /// Wire the handlers with their ports.
    pub fn new(
        events: Arc<dyn EventStore>,
        transfers: Arc<dyn TransferStore>,
        idempotency: Arc<dyn IdempotencyStore>,
    ) -> Self {
        Self {
            events,
            transfers,
            idempotency,
            max_retries: 5,
        }
    }

    /// Open a new account. Returns its id.
    pub async fn open_account(
        &self,
        owner: OwnerId,
        currency: Currency,
        correlation_id: &str,
    ) -> Result<AccountId, CommandError> {
        let id = AccountId::new();
        self.execute(id, AccountCommand::Open { owner, currency }, correlation_id)
            .await?;
        metrics::counter!("ledger_accounts_opened_total").increment(1);
        Ok(id)
    }

    /// Initiate a transfer. Idempotent on `idempotency_key`. Persists the saga in its
    /// `Requested` state and records `TransferRequested`; the orchestrator drives it forward.
    pub async fn initiate_transfer(
        &self,
        idempotency_key: &str,
        source: AccountId,
        destination: AccountId,
        amount: Money,
        correlation_id: &str,
    ) -> Result<TransferId, CommandError> {
        // Idempotency: a repeated key returns the original transfer.
        if let Some(existing) = self.idempotency.get(idempotency_key).await? {
            return Ok(existing);
        }
        let transfer_id = TransferId::new();
        let saga = TransferSaga::new(transfer_id, source, destination, amount);
        self.transfers.save(&saga, correlation_id).await?;
        self.idempotency.put(idempotency_key, transfer_id).await?;
        metrics::counter!("ledger_transfers_total", "status" => "requested").increment(1);
        Ok(transfer_id)
    }

    /// Load → decide → append one Account command with optimistic-concurrency retry.
    ///
    /// This is the single write path for the aggregate, used by the gRPC handlers *and* the
    /// saga orchestrator (for Reserve/Credit/Capture/Release). On a version conflict it
    /// reloads and re-decides — the domain's idempotency guards make re-decision safe.
    pub async fn execute(
        &self,
        account_id: AccountId,
        command: AccountCommand,
        correlation_id: &str,
    ) -> Result<u64, CommandError> {
        let mut attempt = 0;
        loop {
            let stored = self.events.load(account_id).await?;
            let expected_version = stored.len() as u64;
            let history: Vec<_> = stored.into_iter().map(|s| s.event).collect();
            let account = Account::rehydrate(account_id, &history);

            let new_events = account.decide(command.clone())?;
            if new_events.is_empty() {
                // Idempotent no-op — nothing to append.
                return Ok(expected_version);
            }
            match self
                .events
                .append(account_id, expected_version, &new_events, correlation_id)
                .await
            {
                Ok(v) => return Ok(v),
                Err(PortError::Conflict { .. }) if attempt < self.max_retries => {
                    // Reload and re-decide on a fresh version (loop iterates).
                    attempt += 1;
                    metrics::counter!("ledger_optimistic_retries_total").increment(1);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::outbound::memory::{
        InMemoryEventStore, InMemoryIdempotency, InMemoryTransfers,
    };

    fn handlers() -> CommandHandlers {
        CommandHandlers::new(
            Arc::new(InMemoryEventStore::default()),
            Arc::new(InMemoryTransfers::default()),
            Arc::new(InMemoryIdempotency::default()),
        )
    }

    #[tokio::test]
    async fn open_and_credit_and_reserve() {
        let h = handlers();
        let acc = h
            .open_account(OwnerId::new(), Currency::Usd, "corr")
            .await
            .unwrap();
        let t = TransferId::new();
        h.execute(
            acc,
            AccountCommand::Credit {
                transfer_id: t,
                amount: Money::from_minor(1000, Currency::Usd),
            },
            "corr",
        )
        .await
        .unwrap();
        let t2 = TransferId::new();
        h.execute(
            acc,
            AccountCommand::Reserve {
                transfer_id: t2,
                amount: Money::from_minor(400, Currency::Usd),
            },
            "corr",
        )
        .await
        .unwrap();
        let events = h.events.load(acc).await.unwrap();
        // AccountOpened + FundsCredited + FundsReserved
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn transfer_is_idempotent_on_key() {
        let h = handlers();
        let a = h
            .open_account(OwnerId::new(), Currency::Usd, "c")
            .await
            .unwrap();
        let b = h
            .open_account(OwnerId::new(), Currency::Usd, "c")
            .await
            .unwrap();
        let amt = Money::from_minor(100, Currency::Usd);
        let t1 = h.initiate_transfer("key-1", a, b, amt, "c").await.unwrap();
        let t2 = h.initiate_transfer("key-1", a, b, amt, "c").await.unwrap();
        assert_eq!(t1, t2); // same key => same transfer
    }

    #[tokio::test]
    async fn insufficient_funds_is_domain_error() {
        let h = handlers();
        let acc = h
            .open_account(OwnerId::new(), Currency::Usd, "c")
            .await
            .unwrap();
        let err = h
            .execute(
                acc,
                AccountCommand::Reserve {
                    transfer_id: TransferId::new(),
                    amount: Money::from_minor(1, Currency::Usd),
                },
                "c",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, CommandError::Domain(_)));
    }
}
