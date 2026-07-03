//! # proto
//!
//! The generated gRPC contracts shared by services (ADR-0011). This crate is the single
//! source of truth for the internal service API; the gateway maps REST ⇄ these RPCs. Modules
//! are populated by `build.rs` from the `.proto` files at compile time.

/// Identity & Access service (`identity.v1`).
pub mod identity {
    #![allow(clippy::all, clippy::pedantic, missing_docs)]
    tonic::include_proto!("identity.v1");
}

/// Ledger service (`ledger.v1`).
pub mod ledger {
    #![allow(clippy::all, clippy::pedantic, missing_docs)]
    tonic::include_proto!("ledger.v1");
}
