//! # identity
//!
//! The Identity & Access service. Unlike the [`ledger`], Identity is a **supporting context**
//! modeled as classic CRUD plus a transactional outbox — *not* event-sourced — because its
//! invariants are local and its history does not need replay (DOMAIN §6, ADR-0003). The choice
//! to use event sourcing only where it pays for itself is deliberate.
//!
//! It is structured hexagonally (ADR-0002):
//!
//! * [`domain`] — pure types and rules: password policy, RBAC evaluation, refresh-token
//!   reuse-detection decision, PKCE derivation. No I/O.
//! * [`application`] — the use-case handlers ([`application::handlers::AuthHandlers`]) and the
//!   ports (traits) they depend on ([`application::ports`]), plus the RS256 JWT issuer
//!   ([`application::jwt`]).
//! * [`adapters`] — the concrete edges: an inbound tonic gRPC server implementing
//!   `identity.v1.AuthService`, and outbound Postgres / in-memory / OAuth adapters.
//!
//! The binary ([`main`](../identity/index.html)) is the composition root that wires adapters
//! into ports and runs the gRPC server, the health/metrics/JWKS HTTP server, and the outbox
//! relay, mirroring the ledger service.
//!
//! ## Auth model (ADR-0009)
//! * **Access tokens** — short-lived JWT signed **RS256**; verifiers fetch the public key from
//!   the [`/.well-known/jwks.json`](application::jwt::JwtIssuer::jwks) endpoint and verify
//!   locally (no per-request call to Identity).
//! * **Refresh tokens** — opaque, stored **SHA-256 hashed**, grouped into a **family**, and
//!   **rotated on every use**. Presenting an already-rotated token signals theft and revokes
//!   the whole family (reuse detection).
//! * **RBAC** — roles → permissions, re-checked in the application layer for defense in depth.

pub mod adapters;
pub mod application;
pub mod domain;
