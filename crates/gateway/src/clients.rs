//! Outbound gRPC client pool with resilience (ARCHITECTURE §5.1, §6.1).
//!
//! The gateway holds lazily-connected `tonic` clients to Identity and Ledger. Connections are
//! established on first use (`connect_lazy`), so the gateway boots even if an upstream is
//! momentarily down. Every call is:
//!
//! * tagged with the request's correlation ids (injected into gRPC metadata so the downstream
//!   logs the same `x-correlation-id` / `x-request-id`), and
//! * guarded by a **per-upstream circuit breaker** ([`resilience::CircuitBreaker`]); read-only
//!   (idempotent) calls additionally get **retry with jittered backoff**.
//!
//! Upstream addresses come from the environment (documented in [`Upstreams::from_env`]).

use std::future::Future;
use std::sync::Arc;

use kernel::time::SystemClock;
use resilience::{retry, CircuitBreaker, CircuitConfig, ExponentialBackoff, RetryPolicy};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use proto::identity::auth_service_client::AuthServiceClient;
use proto::ledger::ledger_service_client::LedgerServiceClient;

use crate::context::{CorrelationIds, CORRELATION_ID_HEADER, REQUEST_ID_HEADER};
use crate::error::ApiError;

/// Resolved upstream gRPC endpoints.
#[derive(Debug, Clone)]
pub struct Upstreams {
    /// Identity service gRPC endpoint.
    pub identity: String,
    /// Ledger service gRPC endpoint.
    pub ledger: String,
}

impl Upstreams {
    /// Read upstream addresses from the environment.
    ///
    /// These are intentionally read directly from env (not the shared `AppConfig`, which has
    /// no upstream fields): `APP__UPSTREAM__IDENTITY` (default `http://identity:50051`) and
    /// `APP__UPSTREAM__LEDGER` (default `http://ledger:50052`) — matching the ports in
    /// `docs/CONVENTIONS.md`.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            identity: std::env::var("APP__UPSTREAM__IDENTITY")
                .unwrap_or_else(|_| "http://identity:50051".to_string()),
            ledger: std::env::var("APP__UPSTREAM__LEDGER")
                .unwrap_or_else(|_| "http://ledger:50052".to_string()),
        }
    }
}

/// The lazily-connected, resilience-wrapped client pool. Cheap to clone (channels and
/// breakers are shared).
#[derive(Clone)]
pub struct Clients {
    auth: AuthServiceClient<Channel>,
    ledger: LedgerServiceClient<Channel>,
    auth_breaker: Arc<CircuitBreaker<SystemClock>>,
    ledger_breaker: Arc<CircuitBreaker<SystemClock>>,
}

impl Clients {
    /// Build the pool, connecting lazily to both upstreams.
    pub fn connect(upstreams: &Upstreams) -> anyhow::Result<Self> {
        let auth_channel = Channel::from_shared(upstreams.identity.clone())?.connect_lazy();
        let ledger_channel = Channel::from_shared(upstreams.ledger.clone())?.connect_lazy();
        Ok(Self {
            auth: AuthServiceClient::new(auth_channel),
            ledger: LedgerServiceClient::new(ledger_channel),
            auth_breaker: Arc::new(CircuitBreaker::new(CircuitConfig::default(), SystemClock)),
            ledger_breaker: Arc::new(CircuitBreaker::new(CircuitConfig::default(), SystemClock)),
        })
    }

    /// A fresh handle to the Identity client (cloning shares the underlying channel).
    #[must_use]
    pub fn auth(&self) -> AuthServiceClient<Channel> {
        self.auth.clone()
    }

    /// A fresh handle to the Ledger client.
    #[must_use]
    pub fn ledger(&self) -> LedgerServiceClient<Channel> {
        self.ledger.clone()
    }

    /// The Identity upstream circuit breaker.
    #[must_use]
    pub fn auth_breaker(&self) -> &CircuitBreaker<SystemClock> {
        &self.auth_breaker
    }

    /// The Ledger upstream circuit breaker.
    #[must_use]
    pub fn ledger_breaker(&self) -> &CircuitBreaker<SystemClock> {
        &self.ledger_breaker
    }
}

/// Wrap a protobuf message in a `tonic::Request` carrying the correlation ids as metadata.
#[must_use]
pub fn request_with_ids<T>(message: T, ids: &CorrelationIds) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Ok(value) = MetadataValue::try_from(ids.correlation_id.as_str()) {
        request.metadata_mut().insert(CORRELATION_ID_HEADER, value);
    }
    if let Ok(value) = MetadataValue::try_from(ids.request_id.as_str()) {
        request.metadata_mut().insert(REQUEST_ID_HEADER, value);
    }
    request
}

/// Internal error of a guarded upstream call.
#[derive(Debug)]
enum CallError {
    /// The upstream returned a gRPC status.
    Status(tonic::Status),
    /// The circuit breaker rejected the call (upstream considered unhealthy).
    CircuitOpen,
}

impl CallError {
    /// Whether the failure is worth retrying (transient upstream trouble).
    fn is_transient(&self) -> bool {
        match self {
            CallError::CircuitOpen => false,
            CallError::Status(status) => matches!(
                status.code(),
                tonic::Code::Unavailable
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::ResourceExhausted
            ),
        }
    }
}

impl From<CallError> for ApiError {
    fn from(err: CallError) -> Self {
        match err {
            CallError::CircuitOpen => ApiError::upstream_unavailable(),
            CallError::Status(status) => ApiError::from_status(&status),
        }
    }
}

/// Guard a single (non-idempotent) upstream call with the circuit breaker.
///
/// `op` builds the call future; it is invoked at most once. A breaker-open rejection maps to
/// 503, and an upstream `Status` maps through the gRPC→HTTP table.
pub async fn guarded<T, F, Fut>(breaker: &CircuitBreaker<SystemClock>, op: F) -> Result<T, ApiError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    match breaker.call(op).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(status)) => Err(ApiError::from_status(&status)),
        Err(_) => Err(ApiError::upstream_unavailable()),
    }
}

/// Guard an **idempotent** upstream call with the circuit breaker *and* retry-with-backoff.
///
/// `make` must rebuild the call future on each attempt (a fresh `tonic::Request` is consumed
/// per call), so it is a `FnMut` factory. Only transient failures are retried; breaker-open is
/// terminal (fail fast rather than hammer a tripped breaker).
pub async fn guarded_idempotent<T, F, Fut>(
    breaker: &CircuitBreaker<SystemClock>,
    backoff: ExponentialBackoff,
    mut make: F,
) -> Result<T, ApiError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    let policy = RetryPolicy::new(backoff, |e: &CallError| e.is_transient());
    let result = retry(&policy, || {
        let fut = make();
        async {
            match breaker.call(|| fut).await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(status)) => Err(CallError::Status(status)),
                Err(_) => Err(CallError::CircuitOpen),
            }
        }
    })
    .await;
    result.map_err(ApiError::from)
}
