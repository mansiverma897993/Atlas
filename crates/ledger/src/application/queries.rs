//! Query handlers (the CQRS **read** side). Queries never touch the event store; they read
//! denormalized projections through the [`ReadModel`] port (DOMAIN §4.2). Read and write scale
//! independently (ADR-0004).

use std::sync::Arc;

use async_trait::async_trait;
use kernel::{AccountId, Money, TransferId};

use super::ports::PortError;

/// A materialized account view (from the `account_balance_view` projection).
#[derive(Debug, Clone)]
pub struct AccountView {
    /// Account id.
    pub account_id: AccountId,
    /// Owner id.
    pub owner_id: String,
    /// Currency code.
    pub currency: String,
    /// Status string.
    pub status: String,
    /// Settled balance.
    pub posted: Money,
    /// Held balance.
    pub reserved: Money,
    /// Spendable balance.
    pub available: Money,
    /// Aggregate version.
    pub version: u64,
}

/// A transfer status view (from `transfer_status_view`).
#[derive(Debug, Clone)]
pub struct TransferView {
    /// Transfer id.
    pub transfer_id: TransferId,
    /// Source account.
    pub source: AccountId,
    /// Destination account.
    pub destination: AccountId,
    /// Amount moved.
    pub amount: Money,
    /// Saga status string.
    pub status: String,
    /// Failure reason if failed.
    pub failure_reason: Option<String>,
}

/// One entry in an account's transaction history.
#[derive(Debug, Clone)]
pub struct TransactionEntry {
    /// Related transfer.
    pub transfer_id: TransferId,
    /// `DEBIT` or `CREDIT`.
    pub direction: String,
    /// Amount.
    pub amount: Money,
    /// When it posted (unix seconds).
    pub occurred_at: i64,
}

/// **Port:** read access to projections.
#[async_trait]
pub trait ReadModel: Send + Sync {
    /// Fetch an account view.
    async fn account(&self, id: AccountId) -> Result<Option<AccountView>, PortError>;
    /// Fetch a transfer view.
    async fn transfer(&self, id: TransferId) -> Result<Option<TransferView>, PortError>;
    /// Page an account's transaction history.
    async fn transactions(
        &self,
        account: AccountId,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<(Vec<TransactionEntry>, Option<String>), PortError>;
}

/// Query handlers. Cheap to clone.
#[derive(Clone)]
pub struct QueryHandlers {
    read: Arc<dyn ReadModel>,
}

impl QueryHandlers {
    /// Wire with a [`ReadModel`].
    pub fn new(read: Arc<dyn ReadModel>) -> Self {
        Self { read }
    }

    /// Get an account view.
    pub async fn account(&self, id: AccountId) -> Result<Option<AccountView>, PortError> {
        self.read.account(id).await
    }

    /// Get a transfer status view.
    pub async fn transfer(&self, id: TransferId) -> Result<Option<TransferView>, PortError> {
        self.read.transfer(id).await
    }

    /// Page transactions.
    pub async fn transactions(
        &self,
        account: AccountId,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<(Vec<TransactionEntry>, Option<String>), PortError> {
        self.read
            .transactions(account, limit.clamp(1, 200), cursor)
            .await
    }
}
