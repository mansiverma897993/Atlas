//! # worker (binary) — composition root
//!
//! The cross-cutting **background tier** (ARCHITECTURE §3). It runs no gRPC surface of its
//! own; it wires concrete adapters into the module ports and runs several concurrent
//! components until a shutdown signal:
//!
//! * the **scheduler** (`tokio-cron-scheduler`) firing periodic jobs,
//! * the **audit sink** consumer (`audit.v1` → `audit_log`),
//! * the **cross-context provisioning** consumer (`identity.user.v1` → ledger `OpenAccount`),
//! * the **DLQ monitor** consumers over the `.dlq` topics, with a replay admin endpoint,
//! * **health** (`/health/*`), **metrics** (`/metrics`) and **admin** HTTP endpoints.
//!
//! On `SIGTERM`/Ctrl-C it cancels all components and drains within the configured grace
//! period (ARCHITECTURE §6.5). Mirrors the ledger crate's composition root.

mod audit;
mod dlq;
mod provisioning;
mod scheduler;
mod store;

use std::sync::Arc;

use anyhow::Context;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio_util::sync::CancellationToken;

use config::AppConfig;
use infra::bus::kafka::{KafkaClient, KafkaConsumer, KafkaPublisher};
use infra::bus::{dlq_topic, topics, EventConsumer, EventHandler};
use infra::db;
use infra::health::{HealthCheck, HealthRegistry};
use infra::redis_pool::RedisPool;

use crate::audit::AuditSink;
use crate::dlq::{DlqMonitor, DlqReplayer};
use crate::provisioning::{GrpcAccountOpener, ProvisioningConsumer};
use crate::scheduler::{DailyStatementGeneration, ReservationExpirySweep, Scheduler};
use crate::store::PgDedupStore;

/// Env var naming the Ledger gRPC endpoint used by the provisioning consumer.
const LEDGER_ADDR_ENV: &str = "WORKER_LEDGER_GRPC_ADDR";
/// Default Ledger gRPC endpoint (CONVENTIONS: ledger gRPC on 50052).
const LEDGER_ADDR_DEFAULT: &str = "http://ledger:50052";
/// Env var holding the shared secret guarding the DLQ-replay admin endpoint.
const ADMIN_TOKEN_ENV: &str = "WORKER_ADMIN_TOKEN";

/// Shared state for the health/metrics/admin HTTP endpoints.
#[derive(Clone)]
struct HttpState {
    health: HealthRegistry,
    metrics: metrics_exporter_prometheus::PrometheusHandle,
    replayer: Arc<DlqReplayer<KafkaPublisher>>,
    admin_token: Arc<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = AppConfig::load().context("loading configuration")?;
    let telemetry = telemetry::init(&cfg).context("initializing telemetry")?;
    tracing::info!(service = %cfg.otel.service_name, "worker starting");

    // ---- outbound adapters (infrastructure) ----
    let pool = db::connect(&cfg.database)
        .await
        .context("connecting to postgres")?;
    sqlx::migrate!("../../migrations/worker")
        .run(&pool)
        .await
        .context("running migrations")?;
    let redis = RedisPool::connect(&cfg.redis)
        .await
        .context("connecting to redis")?;
    let kafka = KafkaClient::connect(&cfg.kafka.brokers, 1)
        .await
        .context("connecting to redpanda")?;
    let publisher = KafkaPublisher::new(kafka.clone());

    // ---- wire ports (dependency injection) ----
    let ledger_addr =
        std::env::var(LEDGER_ADDR_ENV).unwrap_or_else(|_| LEDGER_ADDR_DEFAULT.to_string());
    let opener = Arc::new(GrpcAccountOpener::new(ledger_addr));
    let dedup = Arc::new(PgDedupStore::new(pool.clone()));

    let audit_sink: Arc<dyn EventHandler> = Arc::new(AuditSink::new(pool.clone()));
    let provisioner: Arc<dyn EventHandler> = Arc::new(ProvisioningConsumer::new(dedup, opener));
    let replayer = Arc::new(DlqReplayer::new(pool.clone(), publisher));

    // ---- health ----
    let health = HealthRegistry::new(vec![
        Arc::new(PgHealth { pool: pool.clone() }) as Arc<dyn HealthCheck>,
        Arc::new(RedisHealth {
            redis: redis.clone(),
        }),
    ]);

    // ---- scheduler (background jobs) ----
    let mut scheduler = Scheduler::new().await.context("building scheduler")?;
    scheduler
        .register(Arc::new(ReservationExpirySweep::default()))
        .await
        .context("registering reservation-expiry sweep")?;
    scheduler
        .register(Arc::new(DailyStatementGeneration::default()))
        .await
        .context("registering daily statement job")?;
    scheduler.start().await.context("starting scheduler")?;

    // ---- consumers (one KafkaConsumer factory reused per topic) ----
    let cancel = CancellationToken::new();
    let make_consumer = || {
        KafkaConsumer::new(
            kafka.clone(),
            redis.clone(),
            cfg.kafka.consumer_group.clone(),
            cfg.kafka.max_delivery_attempts,
        )
    };

    let mut tasks = Vec::new();
    tasks.push(spawn_consumer(
        make_consumer(),
        topics::AUDIT.to_string(),
        audit_sink,
        cancel.clone(),
    ));
    tasks.push(spawn_consumer(
        make_consumer(),
        topics::IDENTITY_USER.to_string(),
        provisioner,
        cancel.clone(),
    ));
    // A DLQ monitor per known main topic's `.dlq` companion.
    for main in [
        topics::LEDGER_TRANSFER,
        topics::LEDGER_ACCOUNT,
        topics::IDENTITY_USER,
        topics::AUDIT,
    ] {
        let dlq = dlq_topic(main);
        let handler: Arc<dyn EventHandler> = Arc::new(DlqMonitor::new(pool.clone(), dlq.clone()));
        tasks.push(spawn_consumer(
            make_consumer(),
            dlq,
            handler,
            cancel.clone(),
        ));
    }

    // Everything is wired and running: open the readiness gate.
    health.mark_started();

    // ---- HTTP (health / metrics / admin) ----
    let admin_token = std::env::var(ADMIN_TOKEN_ENV).unwrap_or_else(|_| "changeme".to_string());
    let http_state = HttpState {
        health: health.clone(),
        metrics: telemetry.prometheus.clone(),
        replayer,
        admin_token: Arc::new(admin_token),
    };
    let http = spawn_http(&cfg, http_state, cancel.clone());

    // ---- run until shutdown ----
    wait_for_shutdown().await;
    tracing::info!("shutdown signal received; draining");
    cancel.cancel();
    let _ = scheduler.shutdown().await;

    let grace = cfg.server.shutdown_grace();
    let _ = tokio::time::timeout(grace, async {
        let _ = http.await;
        for t in tasks {
            let _ = t.await;
        }
    })
    .await;
    telemetry::shutdown();
    tracing::info!("worker stopped");
    Ok(())
}

/// Spawn a consumer task for `topic`, cancellable via `cancel`.
fn spawn_consumer(
    consumer: KafkaConsumer,
    topic: String,
    handler: Arc<dyn EventHandler>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tracing::info!(%topic, "consumer starting");
        tokio::select! {
            result = consumer.run(&topic, handler) => {
                if let Err(e) = result {
                    tracing::error!(error = %e, %topic, "consumer exited with error");
                }
            }
            () = cancel.cancelled() => {
                tracing::info!(%topic, "consumer cancelled");
            }
        }
    })
}

/// Start the health + metrics + admin HTTP servers.
fn spawn_http(
    cfg: &AppConfig,
    state: HttpState,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let health_addr = cfg.server.http_addr.clone();
    let metrics_addr = cfg.server.metrics_addr.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/health/live", get(live))
            .route("/health/ready", get(ready))
            .route("/health/startup", get(ready))
            .route("/admin/dlq/replay/:topic", post(replay))
            .with_state(state.clone());
        let metrics_app = Router::new()
            .route("/metrics", get(metrics))
            .with_state(state);

        let health_listener = tokio::net::TcpListener::bind(&health_addr)
            .await
            .expect("bind health");
        let metrics_listener = tokio::net::TcpListener::bind(&metrics_addr)
            .await
            .expect("bind metrics");
        tracing::info!(%health_addr, %metrics_addr, "HTTP (health/metrics/admin) listening");

        let c1 = cancel.clone();
        let c2 = cancel.clone();
        let health_srv = axum::serve(health_listener, app)
            .with_graceful_shutdown(async move { c1.cancelled().await });
        let metrics_srv = axum::serve(metrics_listener, metrics_app)
            .with_graceful_shutdown(async move { c2.cancelled().await });
        let _ = tokio::join!(health_srv, metrics_srv);
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

/// `POST /admin/dlq/replay/:topic` — replay dead-lettered messages back onto `:topic`.
///
/// Minimally guarded by an `x-admin-token` header matching `WORKER_ADMIN_TOKEN`.
async fn replay(
    State(state): State<HttpState>,
    Path(topic): Path<String>,
    headers: HeaderMap,
) -> (StatusCode, String) {
    let provided = headers.get("x-admin-token").and_then(|v| v.to_str().ok());
    if provided != Some(state.admin_token.as_str()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized".to_string());
    }
    match state.replayer.replay(&topic).await {
        Ok(n) => (
            StatusCode::OK,
            format!("replayed {n} messages from {topic}.dlq"),
        ),
        Err(e) => {
            tracing::error!(error = %e, %topic, "DLQ replay failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
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

// ---- health checks (mirror ledger) ----
struct PgHealth {
    pool: sqlx::PgPool,
}
#[async_trait::async_trait]
impl HealthCheck for PgHealth {
    fn name(&self) -> &'static str {
        "postgres"
    }
    async fn check(&self) -> Result<(), String> {
        db::ping(&self.pool).await.map_err(|e| e.to_string())
    }
}

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
