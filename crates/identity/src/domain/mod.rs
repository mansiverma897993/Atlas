//! The Identity domain: pure entities, value objects, and rules.
//!
//! Everything here is free of I/O and infrastructure so it can be unit-tested in isolation and
//! reasoned about on its own. The application layer orchestrates these types against ports;
//! the adapters persist them.

pub mod credential;
pub mod error;
pub mod oauth;
pub mod role;
pub mod token;
pub mod user;

pub use error::DomainError;
