//! The ledger **application** layer — use-cases orchestrating the domain through ports.
//!
//! * [`ports`] — the trait seams (event store, transfer store, idempotency, read model).
//! * [`commands`] — the CQRS write side (load → decide → append, optimistic concurrency).
//! * [`queries`] — the CQRS read side (projections via the [`queries::ReadModel`] port).
//! * [`saga`] — the durable transfer-saga orchestrator.

pub mod commands;
pub mod ports;
pub mod queries;
pub mod saga;

pub use commands::{CommandError, CommandHandlers};
pub use queries::{QueryHandlers, ReadModel};
pub use saga::SagaOrchestrator;
