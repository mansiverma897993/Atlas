//! The ledger **adapters** layer — the only place frameworks appear (ADR-0002).
//!
//! * [`inbound`] — driving adapters (the tonic gRPC server).
//! * [`outbound`] — driven adapters implementing the application ports (Postgres event store &
//!   projections, plus in-memory fakes for tests).

pub mod inbound;
pub mod outbound;
