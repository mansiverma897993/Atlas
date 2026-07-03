//! # ledger
//!
//! The event-sourced double-entry ledger service. Structured hexagonally (ADR-0002):
//!
//! * [`domain`] — pure aggregates, events, the transfer saga, invariants.
//! * [`application`] — command/query handlers + the ports (traits) they depend on.
//! * [`adapters`] — gRPC inbound + Postgres/Redis/Kafka outbound implementations.
//!
//! The binary (`main.rs`) is the composition root that wires adapters into ports and runs
//! the gRPC server, metrics/health HTTP, and outbox relay.

pub mod adapters;
pub mod application;
pub mod domain;
