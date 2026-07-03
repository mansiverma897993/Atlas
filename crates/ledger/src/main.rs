//! # ledger (binary) — composition root
//!
//! Wires concrete adapters into the application ports (dependency injection) and runs the
//! service's concurrent components until a shutdown signal:
//! * the **gRPC** server (`ledger.v1`),
//! * the **saga orchestrator** loop,
//! * the **outbox relay** (event store → Redpanda),
//! * **health** (`/health/*`) and **metrics** (`/metrics`) HTTP endpoints.
//!
//! On `SIGTERM`/Ctrl-C it cancels all components and drains within the configured grace
//! period (ARCHITECTURE §6.5).

use std::sync::Arc;

use anyhow::Context;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use ledger::adapters::inbound::grpc::GrpcLedger;
use ledger::adapters::outbound::postgres::{
    PgEventStore, PgIdempotency, PgOutboxSource, PgReadModel, PgTransferStore,
};
use ledger::application::commands::CommandHandlers;
use ledger::application::queries::QueryHandlers;
use ledger::application::saga::SagaOrchestrator;

use config::AppConfig;
use infra::bus::kafka::{KafkaClient, KafkaPublisher};
use infra::db;
use infra::health::{HealthCheck, HealthRegistry};
use infra::outbox::OutboxRelay;
use infra::redis_pool::RedisPool;
use proto::ledger::ledger_service_server::LedgerServiceServer;

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
    tracing::info!(service = %cfg.otel.service_name, "ledger starting");

    // ---- outbound adapters (infrastructure) ----
    let pool = db::connect(&cfg.database)
        .await
        .context("connecting to postgres")?;
    sqlx::migrate!("../../migrations/ledger")
        .run(&pool)
        .await
        .context("running migrations")?;
    let redis = RedisPool::connect(&cfg.redis)
        .await
        .context("connecting to redis")?;
    let kafka = KafkaClient::connect(&cfg.kafka.brokers, 1)
        .await
        .context("connecting to redpanda")?;
    let publisher = Arc::new(KafkaPublisher::new(kafka));

    // ---- wire ports (dependency injection) ----
    let event_store = Arc::new(PgEventStore::new(pool.clone()));
    let transfer_store = Arc::new(PgTransferStore::new(pool.clone()));
    let idempotency = Arc::new(PgIdempotency::new(pool.clone()));
    let read_model = Arc::new(PgReadModel::new(pool.clone()));
    let outbox_source = Arc::new(PgOutboxSource::new(pool.clone()));

    let commands = CommandHandlers::new(event_store, transfer_store.clone(), idempotency);
    let queries = QueryHandlers::new(read_model);
    let orchestrator = SagaOrchestrator::new(commands.clone(), transfer_store);
    let relay = OutboxRelay::new("ledger", outbox_source, publisher);

    // ---- health ----
    let health = HealthRegistry::new(vec![
        Arc::new(PgHealth { pool: pool.clone() }) as Arc<dyn HealthCheck>,
        Arc::new(RedisHealth {
            redis: redis.clone(),
        }),
    ]);
    health.mark_started();

    // ---- run all components under one shutdown token ----
    let cancel = CancellationToken::new();
    let http_state = HttpState {
        health: health.clone(),
        metrics: telemetry.prometheus.clone(),
    };

    let grpc = spawn_grpc(&cfg, GrpcLedger::new(commands, queries), cancel.clone());
    let http = spawn_http(&cfg, http_state, cancel.clone());
    let saga = {
        let cancel = cancel.clone();
        tokio::spawn(async move { orchestrator.run(cancel).await })
    };
    let relay_task = {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = relay.run(cancel).await {
                tracing::error!(error = %e, "outbox relay exited with error");
            }
        })
    };

    wait_for_shutdown().await;
    tracing::info!("shutdown signal received; draining");
    cancel.cancel();

    let grace = cfg.server.shutdown_grace();
    let _ = tokio::time::timeout(grace, async {
        let _ = tokio::join!(grpc, http, saga, relay_task);
    })
    .await;
    telemetry::shutdown();
    tracing::info!("ledger stopped");
    Ok(())
}

/// Start the gRPC server; stops when `cancel` fires.
fn spawn_grpc(
    cfg: &AppConfig,
    service: GrpcLedger,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let addr = cfg.server.grpc_addr.clone();
    tokio::spawn(async move {
        let addr = addr.parse().expect("valid grpc addr");
        tracing::info!(%addr, "gRPC server listening");
        let result = Server::builder()
            .add_service(LedgerServiceServer::new(service))
            .serve_with_shutdown(addr, async move { cancel.cancelled().await })
            .await;
        if let Err(e) = result {
            tracing::error!(error = %e, "gRPC server error");
        }
    })
}

/// Start the health + metrics HTTP servers.
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
        tracing::info!(%health_addr, %metrics_addr, "HTTP (health/metrics) listening");

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
