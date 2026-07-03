//! The application layer: use-case handlers and the ports they depend on.
//!
//! * [`ports`] — the traits ([`ports::UserRepository`], [`ports::RefreshTokenRepository`],
//!   [`ports::RoleRepository`], [`ports::OutboxWriter`], [`ports::OAuthProvider`]) that the
//!   handlers program against. Adapters implement them.
//! * [`jwt`] — the RS256 [`jwt::JwtIssuer`]: key generation, signing, verification, JWKS.
//! * [`handlers`] — [`handlers::AuthHandlers`], one method per `AuthService` RPC plus the
//!   OAuth start/callback use cases.

pub mod error;
pub mod handlers;
pub mod jwt;
pub mod ports;

pub use error::AuthError;
