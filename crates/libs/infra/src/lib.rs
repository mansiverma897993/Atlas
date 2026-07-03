//! # infra
//!
//! Shared **outbound adapters** — the concrete side of the ports that service application
//! layers depend on (ADR-0002). Nothing here contains business logic; it is the plumbing:
//!
//! * [`db`] — PostgreSQL connection pool (SQLx, rustls).
//! * [`redis_pool`] — Redis connection manager.
//! * [`lock`] — a Redis-backed [`lock::DistributedLock`] (per-account command serialization).
//! * [`rate_limit`] — a Redis token-bucket [`rate_limit::RateLimiter`].
//! * [`bus`] — the event backbone: [`bus::EventEnvelope`], [`bus::EventPublisher`] /
//!   [`bus::EventConsumer`] ports, and a Redpanda (Kafka API) adapter.
//! * [`outbox`] — the transactional-outbox relay that streams committed events to the bus
//!   (ADR-0006), with dead-letter handling.
//! * [`health`] — readiness/liveness aggregation over dependencies.

pub mod bus;
pub mod db;
pub mod error;
pub mod health;
pub mod lock;
pub mod outbox;
pub mod rate_limit;
pub mod redis_pool;

pub use error::{InfraError, Result};
