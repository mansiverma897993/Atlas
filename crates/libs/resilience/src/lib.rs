//! # resilience
//!
//! Reusable resilience primitives used by the gateway and every outbound client/consumer:
//!
//! * [`circuit_breaker::CircuitBreaker`] — a `Closed → Open → HalfOpen` breaker driven by a
//!   rolling failure ratio, guarding calls to a flaky dependency.
//! * [`backoff::ExponentialBackoff`] — jittered exponential backoff schedule.
//! * [`retry::retry`] — retry an idempotent async operation under a [`retry::RetryPolicy`].
//!
//! The breaker is deterministic and time-injected (via [`kernel::time::Clock`]) so it is unit
//! tested without real time. These are wrapped as Tower layers in the `gateway`/`infra`
//! crates; here they are transport-agnostic so they compose anywhere.

pub mod backoff;
pub mod circuit_breaker;
pub mod retry;

pub use backoff::ExponentialBackoff;
pub use circuit_breaker::{CircuitBreaker, CircuitConfig, CircuitError, CircuitState};
pub use retry::{retry, RetryPolicy};
