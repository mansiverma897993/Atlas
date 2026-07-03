//! # notification (binary) — composition root
//!
//! Wires the realtime WebSocket fan-out service and runs its concurrent components until a
//! shutdown signal (mirrors the ledger composition root):
//! * the **WebSocket** server (`GET /ws`) + **health** (`/health/*`) on `server.http_addr` (8083),
//! * the **metrics** endpoint (`/metrics`) on `server.metrics_addr` (9103),
//! * the **Redis pub/sub subscriber** delivering cross-node messages to local sockets,
//! * two **Kafka consumers** fanning out `ledger.transfer.v1` and `ledger.account.v1`.
//!
//! On `SIGTERM`/Ctrl-C every component is cancelled and drained within the configured grace
//! period (ARCHITECTURE §6.5).

mod adapters;
mod auth;
mod consumer;
mod hub;
mod message;
mod presence;
mod pubsub;

use std::sync::Arc;

use anyhow::Context;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use tokio_util::sync::CancellationToken;

use config::AppConfig;
use infra::bus::kafka::{KafkaClient, KafkaConsumer};
use infra::bus::topics::{LEDGER_ACCOUNT, LEDGER_TRANSFER};
use infra::bus::{EventConsumer, EventHandler};
use infra::health::{HealthCheck, HealthRegistry};
use infra::redis_pool::RedisPool;

use adapters::ws::{ws_handler, WsState};
use auth::{JwtVerifier, TokenVerifier};
use consumer::FanoutHandler;
use hub::ConnectionHub;
use presence::Presence;
use pubsub::PubSubRouter;

/// Env var carrying the RS256 **public key** (PEM) used to verify connection JWTs.
const JWT_PUBLIC_KEY_ENV: &str = "APP__JWT__PUBLIC_KEY_PEM";

/// Shared state for the health/metrics HTTP endpoints.
#[derive(Clone)]
struct HttpState {
    health: HealthRegistry,
    metrics: metrics_exporter_prometheus::PrometheusHandle,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = AppConfig::load().context("loading configuration")?;
    let telemetry = telemetry::init(&cfg).context("initializing telemetry")?;
    tracing::info!(service = %cfg.otel.service_name, "notification starting");

    // ---- infrastructure ----
    let redis = RedisPool::connect(&cfg.redis)
        .await
        .context("connecting to redis")?;
    let kafka = KafkaClient::connect(&cfg.kafka.brokers, 1)
        .await
        .context("connecting to redpanda")?;

    // ---- realtime building blocks ----
    // A unique id for this replica, used to suppress our own Redis pub/sub echoes.
    let node_id = uuid::Uuid::new_v4().to_string();
    let hub = ConnectionHub::new();
    let router = PubSubRouter::new(redis.clone(), node_id.clone());
    let presence = Presence::new(redis.clone(), cfg.redis.default_ttl_seconds);
    let verifier = build_verifier(&cfg).context("building JWT verifier")?;

    // ---- health ----
    let health = HealthRegistry::new(vec![Arc::new(RedisHealth {
        redis: redis.clone(),
    }) as Arc<dyn HealthCheck>]);
    health.mark_started();

    // ---- run all components under one shutdown token ----
    let cancel = CancellationToken::new();

    // presence heartbeat interval: refresh well within the TTL.
    let heartbeat_seconds = (cfg.redis.default_ttl_seconds / 2).max(5);
    let ws_state = WsState {
        hub: hub.clone(),
        presence: presence.clone(),
        verifier,
        heartbeat_seconds,
    };
    let http_state = HttpState {
        health: health.clone(),
        metrics: telemetry.prometheus.clone(),
    };

    let http = spawn_http(&cfg, ws_state, http_state, cancel.clone());

    // cross-node subscriber (delivers messages published by other replicas to local sockets)
    let subscriber = {
        let redis_url = cfg.redis.url.clone();
        let hub = hub.clone();
        let node_id = node_id.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pubsub::run_subscriber(redis_url, hub, node_id, cancel).await;
        })
    };

    // event fan-out consumers (one task per topic)
    let handler: Arc<dyn EventHandler> =
        Arc::new(FanoutHandler::new(hub.clone(), router, redis.clone()));
    let consumers: Vec<_> = [LEDGER_TRANSFER, LEDGER_ACCOUNT]
        .into_iter()
        .map(|topic| {
            let consumer = KafkaConsumer::new(
                kafka.clone(),
                redis.clone(),
                cfg.kafka.consumer_group.clone(),
                cfg.kafka.max_delivery_attempts,
            );
            let handler = handler.clone();
            tokio::spawn(async move {
                if let Err(e) = consumer.run(topic, handler).await {
                    tracing::error!(error = %e, topic, "consumer exited with error");
                }
            })
        })
        .collect();

    wait_for_shutdown().await;
    tracing::info!("shutdown signal received; draining");
    cancel.cancel();

    let grace = cfg.server.shutdown_grace();
    let _ = tokio::time::timeout(grace, async {
        let _ = http.await;
        let _ = subscriber.await;
        for c in consumers {
            c.abort();
            let _ = c.await;
        }
    })
    .await;
    telemetry::shutdown();
    tracing::info!("notification stopped");
    Ok(())
}

/// Build the connection JWT verifier from the PEM public key in the environment.
///
/// This is the one seam that must be configured out-of-band (the key isn't in [`AppConfig`]);
/// missing/invalid key material fails boot rather than accepting unauthenticated sockets.
fn build_verifier(cfg: &AppConfig) -> anyhow::Result<Arc<dyn TokenVerifier>> {
    let pem = std::env::var(JWT_PUBLIC_KEY_ENV)
        .with_context(|| format!("{JWT_PUBLIC_KEY_ENV} must be set (RS256 public key, PEM)"))?;
    let issuer = (!cfg.jwt.issuer.is_empty()).then_some(cfg.jwt.issuer.as_str());
    let audience = (!cfg.jwt.audience.is_empty()).then_some(cfg.jwt.audience.as_str());
    let verifier = JwtVerifier::from_rsa_pem(pem.as_bytes(), issuer, audience)
        .context("parsing JWT public key")?;
    Ok(Arc::new(verifier))
}

/// Start the WS + health server (on `http_addr`) and the metrics server (on `metrics_addr`).
fn spawn_http(
    cfg: &AppConfig,
    ws_state: WsState,
    http_state: HttpState,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let http_addr = cfg.server.http_addr.clone();
    let metrics_addr = cfg.server.metrics_addr.clone();
    tokio::spawn(async move {
        // WS + health share the public/admin port; each sub-router carries its own state and is
        // erased to `Router<()>` by `.with_state`, so they merge cleanly.
        let ws_router = Router::new()
            .route("/ws", get(ws_handler))
            .with_state(ws_state);
        let health_router = Router::new()
            .route("/health/live", get(live))
            .route("/health/ready", get(ready))
            .route("/health/startup", get(ready))
            .with_state(http_state.clone());
        let app = ws_router.merge(health_router);

        let metrics_app = Router::new()
            .route("/metrics", get(metrics))
            .with_state(http_state);

        let http_listener = tokio::net::TcpListener::bind(&http_addr)
            .await
            .expect("bind ws/health");
        let metrics_listener = tokio::net::TcpListener::bind(&metrics_addr)
            .await
            .expect("bind metrics");
        tracing::info!(%http_addr, %metrics_addr, "HTTP (ws/health/metrics) listening");

        let c1 = cancel.clone();
        let c2 = cancel.clone();
        let http_srv = axum::serve(http_listener, app)
            .with_graceful_shutdown(async move { c1.cancelled().await });
        let metrics_srv = axum::serve(metrics_listener, metrics_app)
            .with_graceful_shutdown(async move { c2.cancelled().await });
        let _ = tokio::join!(http_srv, metrics_srv);
    })
}

async fn live() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(state): State<HttpState>) -> (StatusCode, Json<infra::health::Readiness>) {
    let readiness = state.health.readiness().await;
    let code = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(readiness))
}

async fn metrics(State(state): State<HttpState>) -> String {
    state.metrics.render()
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
