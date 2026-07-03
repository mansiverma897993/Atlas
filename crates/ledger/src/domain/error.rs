//! Domain (business-rule) errors for the ledger. Distinct from infra/kernel errors — these
//! represent an aggregate refusing a command because it would break an invariant.

use kernel::KernelError;
use thiserror::Error;

/// A business-rule violation raised by an aggregate's `decide`.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// A command targeted an account that has not been opened.
    #[error("account does not exist")]
    AccountNotFound,

    /// Tried to open an account that already exists.
    #[error("account already opened")]
    AlreadyOpened,

    /// A command requires an open account but it is frozen or closed.
    #[error("account is not open (status: {0})")]
    AccountNotOpen(String),

    /// A debit/reservation exceeds the available balance.
    #[error("insufficient funds: available {available}, requested {requested}")]
    InsufficientFunds {
        /// Available balance at decision time.
        available: String,
        /// Requested amount.
        requested: String,
    },

    /// The amount's currency does not match the account's currency.
    #[error("currency mismatch with account")]
    CurrencyMismatch,

    /// A non-positive amount was supplied where a positive one is required.
    #[error("amount must be positive")]
    NonPositiveAmount,

    /// Close was requested while funds are still reserved.
    #[error("cannot close account with active reservations")]
    ReservationsOutstanding,

    /// A kernel-level value-object error bubbled up.
    #[error(transparent)]
    Kernel(#[from] KernelError),
}
