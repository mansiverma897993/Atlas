//! Adapters — the concrete edges of the hexagon.
//!
//! * [`inbound`] — the tonic gRPC server implementing `identity.v1.AuthService`.
//! * [`outbound`] — Postgres repositories + outbox, an in-memory implementation for tests, and
//!   the OAuth provider adapter (a testable fake).

pub mod inbound;
pub mod outbound;
