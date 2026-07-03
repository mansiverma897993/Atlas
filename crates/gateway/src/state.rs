//! Shared application state, constructed once in `main` and cloned into every handler and
//! stateful middleware layer (constructor injection at the composition root, ARCHITECTURE §4.1).

use std::sync::Arc;

use infra::health::HealthRegistry;
use infra::rate_limit::RateLimiter;
use resilience::ExponentialBackoff;

use crate::auth::JwtVerifier;
use crate::clients::Clients;

/// State injected into Axum handlers and middleware.
#[derive(Clone)]
pub struct AppState {
    /// Resilience-wrapped gRPC client pool.
    pub clients: Clients,
    /// RS256 access-token verifier (JWKS/PEM key source behind a trait seam).
    pub verifier: Arc<JwtVerifier>,
    /// Redis token-bucket limiter (keyed by subject or IP).
    pub limiter: RateLimiter,
    /// Backoff schedule for retrying idempotent upstream reads.
    pub retry_backoff: ExponentialBackoff,
    /// Dependency health (readiness includes Redis).
    pub health: HealthRegistry,
}
