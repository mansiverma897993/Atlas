//! # gateway (binary) — composition root
//!
//! The platform's only internet-facing surface (ARCHITECTURE §2). It terminates public
//! REST/JSON, verifies auth, applies the resilience stack, and routes to Identity/Ledger over
//! gRPC (ADR-0011). This file wires concrete adapters into the application (constructor
//! injection) and runs two HTTP servers until shutdown:
//!
//! * the **public API** (`cfg.server.http_addr`, default `:8080`) — `/api/*`, `/health/*`,
//!   `/swagger`, `/api-docs/openapi.json`;
//! * the **metrics** server (`cfg.server.metrics_addr`, default `:9100`) — `/metrics`.
//!
//! On `SIGTERM`/Ctrl-C it drains within the configured grace period (ARCHITECTURE §6.5),
//! mirroring the `ledger` service.

mod auth;
mod clients;
mod context;
mod dto;
mod error;
mod handlers;
mod middleware;
mod openapi;
mod rbac;
mod router;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use config::AppConfig;
use infra::health::{HealthCheck, HealthRegistry};
use infra::rate_limit::RateLimiter;
use infra::redis_pool::RedisPool;
use resilience::ExponentialBackoff;

use crate::auth::{JwksKeySource, JwtVerifier, KeySource, StaticPemKeySource};
use crate::clients::{Clients, Upstreams};
use crate::state::AppState;

/// Read a `u64` from the environment, falling back to `default`.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = AppConfig::load().context("loading configuration")?;
    let telemetry = telemetry::init(&cfg).context("initializing telemetry")?;
    tracing::info!(service = %cfg.otel.service_name, "gateway starting");

    // ---- outbound adapters (infrastructure) ----
    let redis = RedisPool::connect(&cfg.redis)
        .await
        .context("connecting to redis")?;

    // gRPC upstreams (addresses from env; see `Upstreams::from_env`).
    let upstreams = Upstreams::from_env();
    tracing::info!(identity = %upstreams.identity, ledger = %upstreams.ledger, "gRPC upstreams");
    let clients = Clients::connect(&upstreams).context("building gRPC clients")?;

    // ---- JWT verification key source (JWKS in prod; PEM fallback via env) ----
    let key_source: Box<dyn KeySource> = match std::env::var("APP__JWT__PUBLIC_KEY_PEM") {
        Ok(pem) if !pem.trim().is_empty() => Box::new(
            StaticPemKeySource::from_pem(&pem).context("parsing APP__JWT__PUBLIC_KEY_PEM")?,
        ),
        _ => {
            tracing::warn!(
                jwks_url = %cfg.jwt.jwks_url,
                "no APP__JWT__PUBLIC_KEY_PEM set; starting with an EMPTY JWKS cache. \
                 Protected routes will 401 until keys are loaded — production wires an HTTP \
                 JWKS fetcher (or a mounted JWKS file) to JwksKeySource::load_from_json."
            );
            Box::new(JwksKeySource::empty())
        }
    };
    let verifier = Arc::new(JwtVerifier::new(
        key_source,
        &cfg.jwt.issuer,
        &cfg.jwt.audience,
    ));

    // ---- rate limiter (Redis token bucket) ----
    let capacity = env_u64("APP__RATELIMIT__CAPACITY", 120);
    let refill = env_u64("APP__RATELIMIT__REFILL_PER_SEC", 60);
    let limiter = RateLimiter::new(redis.clone(), capacity, refill);

    // ---- retry schedule for idempotent upstream reads ----
    let retry_backoff = ExponentialBackoff {
        base: Duration::from_millis(20),
        cap: Duration::from_millis(200),
        multiplier: 2.0,
        jitter: true,
        max_retries: 2,
    };

    // ---- health / readiness ----
    let health = HealthRegistry::new(vec![Arc::new(RedisHealth { redis }) as Arc<dyn HealthCheck>]);
    health.mark_started();

    let state = AppState {
        clients,
        verifier,
        limiter,
        retry_backoff,
        health,
    };

    // ---- run API + metrics servers under one shutdown token ----
    let cancel = CancellationToken::new();
    let request_timeout = Duration::from_secs(env_u64("APP__SERVER__REQUEST_TIMEOUT_SECONDS", 15));
    let app = router::build(state, request_timeout);

    let api = spawn_api(cfg.server.http_addr.clone(), app, cancel.clone());
    let metrics = spawn_metrics(
        cfg.server.metrics_addr.clone(),
        telemetry.prometheus.clone(),
        cancel.clone(),
    );

    wait_for_shutdown().await;
    tracing::info!("shutdown signal received; draining");
    cancel.cancel();

    let grace = cfg.server.shutdown_grace();
    let _ = tokio::time::timeout(grace, async {
        let _ = tokio::join!(api, metrics);
    })
    .await;
    telemetry::shutdown();
    tracing::info!("gateway stopped");
    Ok(())
}

/// Serve the public API with connection info (so the rate limiter can key by client IP).
fn spawn_api(addr: String, app: Router, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, %addr, "failed to bind API listener");
                return;
            }
        };
        tracing::info!(%addr, "public API listening");
        let service = app.into_make_service_with_connect_info::<SocketAddr>();
        if let Err(e) = axum::serve(listener, service)
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await
        {
            tracing::error!(error = %e, "API server error");
        }
    })
}

/// Shared state for the metrics endpoint.
#[derive(Clone)]
struct MetricsState {
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
}

/// Serve Prometheus exposition on the metrics port.
fn spawn_metrics(
    addr: String,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(render_metrics))
            .with_state(MetricsState { prometheus });
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, %addr, "failed to bind metrics listener");
                return;
            }
        };
        tracing::info!(%addr, "metrics listening");
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await
        {
            tracing::error!(error = %e, "metrics server error");
        }
    })
}

async fn render_metrics(State(state): State<MetricsState>) -> String {
    state.prometheus.render()
}

/// Wait for Ctrl-C or SIGTERM.
async fn wait_for_shutdown() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

// ---- health checks ----
struct RedisHealth {
    redis: RedisPool,
}

#[async_trait::async_trait]
impl HealthCheck for RedisHealth {
    fn name(&self) -> &'static str {
        "redis"
    }
    async fn check(&self) -> Result<(), String> {
        self.redis.ping().await.map_err(|e| e.to_string())
    }
}
