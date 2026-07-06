//! Router assembly: the REST surface, the layered middleware stack, health, and Swagger.
//!
//! Two route groups share the outer observability layers but differ on auth: the `public`
//! group (`/api/auth/*`) and health/Swagger are exempt from JWT/RBAC/rate-limiting, while the
//! `protected` group carries the full inner stack (see [`crate::middleware`]).

use std::time::Duration;

use axum::extract::State;
use axum::middleware::{from_fn, from_fn_with_state};
use axum::routing::{get, post};
use axum::{Json, Router};
use http::StatusCode;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use infra::health::Readiness;

use crate::handlers;
use crate::middleware as mw;
use crate::openapi::ApiDoc;
use crate::state::AppState;

/// Build the full public HTTP application (API + health + Swagger) with the middleware stack.
#[must_use]
pub fn build(state: AppState, request_timeout: Duration) -> Router {
    // ---- protected surface: JWT → RBAC → rate-limit → timeout (outermost→innermost) ----
    let protected = Router::new()
        .route("/api/accounts", post(handlers::open_account))
        .route("/api/accounts/:id", get(handlers::get_account))
        .route("/api/accounts/:id/balance", get(handlers::get_balance))
        .route(
            "/api/accounts/:id/transactions",
            get(handlers::list_transactions),
        )
        .route("/api/transfers", post(handlers::create_transfer))
        .route("/api/transfers/:id", get(handlers::get_transfer))
        .layer(
            ServiceBuilder::new()
                .layer(from_fn_with_state(state.clone(), mw::jwt_mw))
                .layer(from_fn(mw::rbac_mw))
                .layer(from_fn_with_state(state.clone(), mw::rate_limit_mw))
                .layer(TimeoutLayer::new(request_timeout)),
        );

    // ---- public auth surface (no JWT/RBAC, but still throttled + bounded) ----
    // These routes are unauthenticated and internet-facing, so they are the brute-force /
    // credential-stuffing surface. They carry (IP-keyed) rate limiting, a request timeout, and a
    // small body cap — matching the protection the authenticated `/api` surface gets, minus
    // JWT/RBAC which don't apply pre-login.
    let public = Router::new()
        .route("/api/auth/register", post(handlers::register))
        .route("/api/auth/login", post(handlers::login))
        .route("/api/auth/refresh", post(handlers::refresh))
        .route("/api/auth/logout", post(handlers::logout))
        .layer(
            ServiceBuilder::new()
                .layer(from_fn_with_state(state.clone(), mw::rate_limit_mw))
                .layer(RequestBodyLimitLayer::new(16 * 1024))
                .layer(TimeoutLayer::new(request_timeout)),
        );

    // ---- health probes ----
    let health = Router::new()
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/health/startup", get(health_startup));

    Router::new()
        .merge(public)
        .merge(protected)
        .merge(health)
        .merge(SwaggerUi::new("/swagger").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
        // ---- outer layers, applied to everything: correlation → trace → metrics → headers ----
        .layer(
            ServiceBuilder::new()
                .layer(from_fn(mw::correlation_mw))
                .layer(from_fn(mw::trace_mw))
                .layer(from_fn(mw::metrics_mw))
                .layer(from_fn(mw::security_headers_mw)),
        )
}

/// Liveness: the process can answer.
async fn health_live() -> StatusCode {
    StatusCode::OK
}

/// Readiness: startup complete and all dependencies (Redis) reachable.
async fn health_ready(State(st): State<AppState>) -> (StatusCode, Json<Readiness>) {
    let readiness = st.health.readiness().await;
    let code = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(readiness))
}

/// Startup gate.
async fn health_startup(State(st): State<AppState>) -> StatusCode {
    if st.health.started() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
