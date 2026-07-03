//! # identity (binary) — composition root
//!
//! Wires concrete adapters into the application ports (dependency injection) and runs the
//! service's concurrent components until a shutdown signal:
//! * the **gRPC** server (`identity.v1.AuthService`),
//! * the **outbox relay** (identity outbox table → Redpanda),
//! * **health** (`/health/*`), **metrics** (`/metrics`), the **JWKS** endpoint
//!   (`/.well-known/jwks.json`), and the **OAuth** start/callback endpoints — all on the HTTP
//!   server.
//!
//! On `SIGTERM`/Ctrl-C it cancels all components and drains within the configured grace
//! period. Mirrors the ledger service's structure (ARCHITECTURE §6.5).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use identity::adapters::inbound::grpc::GrpcAuth;
use identity::adapters::outbound::oauth::StaticOAuthProvider;
use identity::adapters::outbound::postgres::{
    PgOutboxSource, PgOutboxWriter, PgRefreshTokenRepository, PgRoleRepository, PgUserRepository,
};
use identity::application::handlers::AuthHandlers;
use identity::application::jwt::JwtIssuer;

use config::AppConfig;
use infra::bus::kafka::{KafkaClient, KafkaPublisher};
use infra::db;
use infra::health::{HealthCheck, HealthRegistry};
use infra::outbox::OutboxRelay;
use infra::redis_pool::RedisPool;
use proto::identity::auth_service_server::AuthServiceServer;

/// Shared state for the HTTP endpoints (health, metrics, JWKS, OAuth).
#[derive(Clone)]
struct HttpState {
    health: HealthRegistry,
    metrics: metrics_exporter_prometheus::PrometheusHandle,
    jwt: Arc<JwtIssuer>,
    handlers: AuthHandlers,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = AppConfig::load().context("loading configuration")?;
    let telemetry = telemetry::init(&cfg).context("initializing telemetry")?;
    tracing::info!(service = %cfg.otel.service_name, "identity starting");

    // ---- outbound infrastructure ----
    let pool = db::connect(&cfg.database)
        .await
        .context("connecting to postgres")?;
    sqlx::migrate!("../../migrations/identity")
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

    // ---- JWT issuer: generate an in-memory RSA keypair at startup (ADR-0009). See
    // `application::jwt` for the rationale and the production key-loading alternative. ----
    let jwt = Arc::new(
        JwtIssuer::generate(
            cfg.jwt.issuer.clone(),
            cfg.jwt.audience.clone(),
            cfg.jwt.access_ttl_seconds,
        )
        .context("generating JWT signing key")?,
    );
    tracing::info!("RS256 signing key generated; JWKS served at /.well-known/jwks.json");

    // ---- wire ports (dependency injection) ----
    let users = Arc::new(PgUserRepository::new(pool.clone()));
    let tokens = Arc::new(PgRefreshTokenRepository::new(pool.clone()));
    let roles = Arc::new(PgRoleRepository::new(pool.clone()));
    let outbox_writer = Arc::new(PgOutboxWriter::new(pool.clone()));
    let oauth = Arc::new(StaticOAuthProvider::github_style(format!(
        "http://{}/oauth/callback",
        cfg.server.http_addr
    )));
    let outbox_source = Arc::new(PgOutboxSource::new(pool.clone()));

    let handlers = AuthHandlers::new(
        users,
        tokens,
        roles,
        outbox_writer,
        oauth,
        jwt.clone(),
        cfg.jwt.refresh_ttl_seconds,
    );
    let relay = OutboxRelay::new("identity", outbox_source, publisher);

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
        jwt: jwt.clone(),
        handlers: handlers.clone(),
    };

    let grpc = spawn_grpc(&cfg, GrpcAuth::new(handlers), cancel.clone());
    let http = spawn_http(&cfg, http_state, cancel.clone());
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
        let _ = tokio::join!(grpc, http, relay_task);
    })
    .await;
    telemetry::shutdown();
    tracing::info!("identity stopped");
    Ok(())
}

/// Start the gRPC server; stops when `cancel` fires.
fn spawn_grpc(
    cfg: &AppConfig,
    service: GrpcAuth,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let addr = cfg.server.grpc_addr.clone();
    tokio::spawn(async move {
        let addr = addr.parse().expect("valid grpc addr");
        tracing::info!(%addr, "gRPC server listening");
        let result = Server::builder()
            .add_service(AuthServiceServer::new(service))
            .serve_with_shutdown(addr, async move { cancel.cancelled().await })
            .await;
        if let Err(e) = result {
            tracing::error!(error = %e, "gRPC server error");
        }
    })
}

/// Start the HTTP server(s): health + JWKS + OAuth on the HTTP port, metrics on the metrics
/// port.
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
            .route("/.well-known/jwks.json", get(jwks))
            .route("/oauth/authorize", get(oauth_authorize))
            .route("/oauth/callback", get(oauth_callback))
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
        tracing::info!(%health_addr, %metrics_addr, "HTTP (health/jwks/oauth/metrics) listening");

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

/// Serve the JWKS so verifiers (gateway, other services) can fetch the RS256 public key.
async fn jwks(State(state): State<HttpState>) -> Json<serde_json::Value> {
    Json(state.jwt.jwks())
}

/// Begin an OAuth2 authorization-code + PKCE flow: returns the provider authorization URL, the
/// CSRF `state`, and the PKCE `code_verifier` (the client keeps the verifier and presents it at
/// the callback).
async fn oauth_authorize(State(state): State<HttpState>) -> Json<serde_json::Value> {
    let start = state.handlers.start_oauth();
    Json(serde_json::json!({
        "authorization_url": start.authorization_url,
        "state": start.state,
        "code_verifier": start.code_verifier,
    }))
}

/// Complete the OAuth flow: exchange `?code=..&code_verifier=..` for a token pair.
async fn oauth_callback(
    State(state): State<HttpState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let code = params
        .get("code")
        .ok_or((StatusCode::BAD_REQUEST, "missing 'code'".to_string()))?;
    let verifier = params.get("code_verifier").ok_or((
        StatusCode::BAD_REQUEST,
        "missing 'code_verifier'".to_string(),
    ))?;
    let tokens = state
        .handlers
        .complete_oauth(code, verifier)
        .await
        .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "access_token": tokens.access_token,
        "refresh_token": tokens.refresh_token,
        "expires_in": tokens.expires_in,
        "token_type": "Bearer",
    })))
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
