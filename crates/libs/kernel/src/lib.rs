//! # kernel
//!
//! The shared **domain kernel** for the ledger platform. It holds the value objects and
//! primitives that every bounded context agrees on — money, currency, typed identifiers,
//! and correlation metadata — with no dependency on any framework, database, or transport.
//!
//! Design rules for this crate:
//! * **Pure.** No `async`, no I/O, no framework types. It is unit-testable in isolation.
//! * **Self-validating.** Constructors reject invalid state so illegal values are
//!   unrepresentable downstream (e.g. mixed-currency arithmetic cannot compile-to-run).
//! * **Stable.** Everything here is a published contract shared across services, so changes
//!   are additive.
//!
//! See [`money`] for the most important type: money is an integer count of minor units,
//! never a floating-point number (see ADR-0010).

pub mod correlation;
pub mod currency;
pub mod error;
pub mod ids;
pub mod money;
pub mod time;

pub use correlation::{CausationId, CorrelationId, RequestId};
pub use currency::Currency;
pub use error::{KernelError, Result};
pub use ids::{AccountId, EventId, OwnerId, TransferId, UserId};
pub use money::Money;
