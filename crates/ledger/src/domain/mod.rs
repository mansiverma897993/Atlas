//! The ledger **domain** layer — pure, framework-free business logic (ADR-0002).
//!
//! * [`account`] — the event-sourced Account aggregate (`decide`/`apply`).
//! * [`events`] — the domain event catalog.
//! * [`transfer`] — the transfer saga's state machine (also pure).
//! * [`error`] — business-rule errors.

pub mod account;
pub mod error;
pub mod events;
pub mod transfer;

pub use account::{Account, AccountCommand, AccountStatus};
pub use error::DomainError;
pub use events::AccountEvent;
pub use transfer::{TransferSaga, TransferState, TransferStep};
