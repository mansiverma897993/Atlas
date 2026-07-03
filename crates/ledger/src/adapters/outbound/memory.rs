//! In-memory adapters used by unit/integration tests to exercise the application layer with
//! no database. They implement the same ports as the Postgres adapters, so handler logic is
//! tested identically against both (the "test against the port" principle from ADR-0002).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kernel::{AccountId, Money, TransferId};

use crate::application::ports::{
    EventStore, IdempotencyStore, PortError, StoredEvent, TransferStore,
};
use crate::application::queries::{AccountView, ReadModel, TransactionEntry, TransferView};
use crate::domain::account::Account;
use crate::domain::events::AccountEvent;
use crate::domain::transfer::{TransferSaga, TransferState};

type Streams = Arc<Mutex<HashMap<AccountId, Vec<AccountEvent>>>>;

/// In-memory event store with optimistic-concurrency semantics.
#[derive(Clone, Default)]
pub struct InMemoryEventStore {
    streams: Streams,
}

impl InMemoryEventStore {
    /// Shared handle to the underlying streams (so a read model can observe the same data).
    #[must_use]
    pub fn streams(&self) -> Streams {
        self.streams.clone()
    }
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    async fn load(&self, stream: AccountId) -> Result<Vec<StoredEvent>, PortError> {
        let guard = self.streams.lock().unwrap();
        let events = guard.get(&stream).cloned().unwrap_or_default();
        Ok(events
            .into_iter()
            .enumerate()
            .map(|(i, event)| StoredEvent {
                version: (i + 1) as u64,
                event,
            })
            .collect())
    }

    async fn append(
        &self,
        stream: AccountId,
        expected_version: u64,
        events: &[AccountEvent],
        _correlation_id: &str,
    ) -> Result<u64, PortError> {
        let mut guard = self.streams.lock().unwrap();
        let entry = guard.entry(stream).or_default();
        let actual = entry.len() as u64;
        if actual != expected_version {
            return Err(PortError::Conflict {
                expected: expected_version,
                actual,
            });
        }
        entry.extend_from_slice(events);
        Ok(entry.len() as u64)
    }
}

/// In-memory transfer/saga store.
#[derive(Clone, Default)]
pub struct InMemoryTransfers {
    sagas: Arc<Mutex<HashMap<TransferId, TransferSaga>>>,
}

#[async_trait]
impl TransferStore for InMemoryTransfers {
    async fn save(&self, saga: &TransferSaga, _correlation_id: &str) -> Result<(), PortError> {
        self.sagas
            .lock()
            .unwrap()
            .insert(saga.transfer_id, saga.clone());
        Ok(())
    }
    async fn load(&self, id: TransferId) -> Result<Option<TransferSaga>, PortError> {
        Ok(self.sagas.lock().unwrap().get(&id).cloned())
    }
    async fn list_pending(&self, limit: u32) -> Result<Vec<TransferSaga>, PortError> {
        Ok(self
            .sagas
            .lock()
            .unwrap()
            .values()
            .filter(|s| !s.state.is_terminal())
            .take(limit as usize)
            .cloned()
            .collect())
    }
}

/// In-memory idempotency store (first-writer-wins).
#[derive(Clone, Default)]
pub struct InMemoryIdempotency {
    keys: Arc<Mutex<HashMap<String, TransferId>>>,
}

#[async_trait]
impl IdempotencyStore for InMemoryIdempotency {
    async fn get(&self, key: &str) -> Result<Option<TransferId>, PortError> {
        Ok(self.keys.lock().unwrap().get(key).copied())
    }
    async fn put(&self, key: &str, transfer_id: TransferId) -> Result<(), PortError> {
        self.keys
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert(transfer_id);
        Ok(())
    }
}

/// In-memory read model that derives views by rehydrating aggregates from the shared streams.
#[derive(Clone)]
pub struct InMemoryReadModel {
    events: InMemoryEventStore,
    transfers: InMemoryTransfers,
}

impl InMemoryReadModel {
    /// Build a read model over the same in-memory stores as the write side.
    pub fn new(events: InMemoryEventStore, transfers: InMemoryTransfers) -> Self {
        Self { events, transfers }
    }
}

#[async_trait]
impl ReadModel for InMemoryReadModel {
    async fn account(&self, id: AccountId) -> Result<Option<AccountView>, PortError> {
        let history: Vec<AccountEvent> = self
            .events
            .streams()
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .unwrap_or_default();
        if history.is_empty() {
            return Ok(None);
        }
        let acc = Account::rehydrate(id, &history);
        let ccy = acc.currency().unwrap();
        Ok(Some(AccountView {
            account_id: id,
            owner_id: String::new(),
            currency: ccy.code().to_string(),
            status: acc
                .status()
                .map_or("UNKNOWN", crate::domain::account::AccountStatus::as_str)
                .to_string(),
            posted: Money::from_minor(acc.posted_balance(), ccy),
            reserved: Money::from_minor(acc.reserved(), ccy),
            available: Money::from_minor(acc.available(), ccy),
            version: acc.version(),
        }))
    }

    async fn transfer(&self, id: TransferId) -> Result<Option<TransferView>, PortError> {
        Ok(self.transfers.load(id).await?.map(|s| TransferView {
            transfer_id: s.transfer_id,
            source: s.source,
            destination: s.destination,
            amount: s.amount,
            status: s.state.as_str().to_string(),
            failure_reason: match s.state {
                TransferState::Failed { reason } => Some(reason),
                _ => None,
            },
        }))
    }

    async fn transactions(
        &self,
        _account: AccountId,
        _limit: u32,
        _cursor: Option<String>,
    ) -> Result<(Vec<TransactionEntry>, Option<String>), PortError> {
        Ok((vec![], None))
    }
}
