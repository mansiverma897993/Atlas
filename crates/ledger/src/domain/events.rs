//! The Account aggregate's domain events (the `ledger.account.v1` catalog) and the transfer
//! saga's lifecycle events. Events are immutable facts; they are the source of truth and the
//! integration contract (DOMAIN §3). Serialized as tagged JSON for the event store and bus.

use kernel::{Currency, Money, OwnerId, TransferId};
use serde::{Deserialize, Serialize};

/// An event appended to an Account's stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AccountEvent {
    /// The account was opened.
    AccountOpened {
        /// Owner (customer) of the account.
        owner: OwnerId,
        /// The account's single currency.
        currency: Currency,
    },
    /// Funds were held for an in-flight transfer (drops available balance).
    FundsReserved {
        /// The transfer this reservation belongs to.
        transfer_id: TransferId,
        /// Amount held.
        amount: Money,
    },
    /// A reservation was released without capture (compensation / expiry).
    ReservationReleased {
        /// The transfer whose reservation is released.
        transfer_id: TransferId,
        /// Amount returned to available.
        amount: Money,
        /// Why it was released.
        reason: String,
    },
    /// A reservation was settled — the debit is now posted.
    FundsCaptured {
        /// The transfer being captured.
        transfer_id: TransferId,
        /// Amount debited.
        amount: Money,
    },
    /// Funds were credited to this account (the receiving leg of a transfer).
    FundsCredited {
        /// The transfer crediting this account.
        transfer_id: TransferId,
        /// Amount credited.
        amount: Money,
    },
    /// The account was frozen (no new reservations/credits).
    AccountFrozen {
        /// Reason for the freeze.
        reason: String,
    },
    /// The account was closed.
    AccountClosed,
}

impl AccountEvent {
    /// The discriminator string used as `event_type` in the store/bus.
    #[must_use]
    pub fn event_type(&self) -> &'static str {
        match self {
            AccountEvent::AccountOpened { .. } => "AccountOpened",
            AccountEvent::FundsReserved { .. } => "FundsReserved",
            AccountEvent::ReservationReleased { .. } => "ReservationReleased",
            AccountEvent::FundsCaptured { .. } => "FundsCaptured",
            AccountEvent::FundsCredited { .. } => "FundsCredited",
            AccountEvent::AccountFrozen { .. } => "AccountFrozen",
            AccountEvent::AccountClosed => "AccountClosed",
        }
    }
}
