//! The Tower/Axum middleware stack (ARCHITECTURE §5.2, §6.1–6.3).
//!
//! Applied outermost→innermost:
//!
//! 1. [`correlation_mw`] — adopt/mint correlation + request ids; echo them on the response.
//! 2. [`trace_mw`] — open a tracing span carrying those ids.
//! 3. [`metrics_mw`] — RED metrics: `http_requests_total` + `http_request_duration_seconds`.
//!
//! Then, only on the protected `/api` surface (auth routes, health, and swagger are exempt):
//!
//! 4. [`jwt_mw`] — verify the RS256 bearer token; stash [`Claims`] in the request extensions.
//! 5. [`rbac_mw`] — enforce the per-route permission.
//! 6. [`rate_limit_mw`] — Redis token bucket keyed by subject (or client IP), 429 + `Retry-After`.
//! 7. `tower_http::timeout` — bound request latency (wired in [`crate::router`]).

use std::net::SocketAddr;
use std::time::Instant;

use axum::extract::{ConnectInfo, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::header::{AUTHORIZATION, RETRY_AFTER};
use http::{HeaderName, HeaderValue};
use tracing::Instrument;

use crate::auth::{bearer_token, Claims};
use crate::context::{CorrelationIds, CORRELATION_ID_HEADER, REQUEST_ID_HEADER};
use crate::error::ApiError;
use crate::rbac::{self, route_label};
use crate::state::AppState;

/// Adopt/mint correlation ids, expose them to inner layers, and echo them on the response.
pub async fn correlation_mw(mut req: Request, next: Next) -> Response {
    let ids = CorrelationIds::from_headers(req.headers());
    req.extensions_mut().insert(ids.clone());

    let mut res = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&ids.correlation_id) {
        res.headers_mut()
            .insert(HeaderName::from_static(CORRELATION_ID_HEADER), value);
    }
    if let Ok(value) = HeaderValue::from_str(&ids.request_id) {
        res.headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }
    res
}

/// Open a tracing span for the request, tagged with method, path, and correlation ids.
pub async fn trace_mw(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let (correlation_id, request_id) = req
        .extensions()
        .get::<CorrelationIds>()
        .map(|ids| (ids.correlation_id.clone(), ids.request_id.clone()))
        .unwrap_or_default();

    let span = tracing::info_span!(
        "http_request",
        %method,
        %path,
        correlation_id = %correlation_id,
        request_id = %request_id,
    );
    next.run(req).instrument(span).await
}

/// Record RED metrics for the request. Route label is the bounded-cardinality template.
pub async fn metrics_mw(req: Request, next: Next) -> Response {
    let method = req.method().as_str().to_owned();
    let route = route_label(&method, req.uri().path());

    let start = Instant::now();
    let res = next.run(req).await;
    let elapsed = start.elapsed().as_secs_f64();
    let status = res.status().as_u16().to_string();

    metrics::counter!(
        "http_requests_total",
        "method" => method.clone(),
        "route" => route,
        "status" => status,
    )
    .increment(1);
    metrics::histogram!(
        "http_request_duration_seconds",
        "method" => method,
        "route" => route,
    )
    .record(elapsed);

    res
}

/// Verify the RS256 bearer token and attach [`Claims`] to the request extensions.
pub async fn jwt_mw(State(st): State<AppState>, mut req: Request, next: Next) -> Response {
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let Some(token) = bearer_token(header) else {
        return ApiError::unauthorized("missing bearer token").into_response();
    };
    match st.verifier.verify(token).await {
        Ok(claims) => {
            req.extensions_mut().insert(claims);
            next.run(req).await
        }
        Err(e) => {
            tracing::debug!(error = %e, "token verification failed");
            ApiError::unauthorized("invalid or expired token").into_response()
        }
    }
}

/// Enforce the permission required by the matched route against the caller's [`Claims`].
pub async fn rbac_mw(req: Request, next: Next) -> Response {
    let method = req.method().as_str().to_owned();
    let required = rbac::classify(&method, req.uri().path()).and_then(|c| c.permission);

    if let Some(permission) = required {
        let allowed = req
            .extensions()
            .get::<Claims>()
            .is_some_and(|claims| rbac::has_permission(claims, permission));
        if !allowed {
            return ApiError::forbidden(format!("missing permission '{permission}'"))
                .into_response();
        }
    }
    next.run(req).await
}

/// Redis token-bucket rate limiting, keyed by authenticated subject or client IP.
///
/// Fails **open** on a limiter/Redis error (availability over strictness at the edge) but logs
/// the failure.
pub async fn rate_limit_mw(State(st): State<AppState>, req: Request, next: Next) -> Response {
    let key = req
        .extensions()
        .get::<Claims>()
        .map(|c| format!("rl:sub:{}", c.sub))
        .or_else(|| {
            req.extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ci| format!("rl:ip:{}", ci.0.ip()))
        })
        .unwrap_or_else(|| "rl:ip:unknown".to_string());

    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    match st.limiter.check(&key, now_ms).await {
        Ok(decision) if decision.allowed => next.run(req).await,
        Ok(decision) => {
            let mut res = ApiError::too_many_requests("rate limit exceeded").into_response();
            if let Ok(v) = HeaderValue::from_str(&decision.retry_after_secs.to_string()) {
                res.headers_mut().insert(RETRY_AFTER, v);
            }
            if let Ok(v) = HeaderValue::from_str(&decision.remaining.to_string()) {
                res.headers_mut()
                    .insert(HeaderName::from_static("x-ratelimit-remaining"), v);
            }
            res
        }
        Err(e) => {
            tracing::warn!(error = %e, key = %key, "rate limiter unavailable; failing open");
            next.run(req).await
        }
    }
}
