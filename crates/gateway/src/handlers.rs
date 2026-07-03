//! Axum handlers: the REST→gRPC translation layer (ADR-0011).
//!
//! Each handler validates its typed DTO, maps it to the internal protobuf message, injects the
//! request's correlation ids, and dispatches through the resilience-wrapped client pool
//! ([`crate::clients`]). Read-only handlers use the idempotent (retrying) path; writes use the
//! plain circuit-breaker-guarded path. gRPC `Status` errors are mapped to HTTP by [`ApiError`].

use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use http::HeaderMap;

use crate::auth::Claims;
use crate::clients::{guarded, guarded_idempotent, request_with_ids};
use crate::context::CorrelationIds;
use crate::dto;
use crate::error::ApiError;
use crate::state::AppState;

use proto::identity as pid;
use proto::ledger as pl;
use validator::Validate;

// ---------------------------------------------------------------------------
// mapping helpers
// ---------------------------------------------------------------------------

fn money(m: Option<pl::Money>) -> dto::Money {
    m.map_or(
        dto::Money {
            minor_units: 0,
            currency: String::new(),
        },
        |m| dto::Money {
            minor_units: m.minor_units,
            currency: m.currency,
        },
    )
}

fn token_pair(t: pid::TokenPair) -> dto::TokenPair {
    dto::TokenPair {
        access_token: t.access_token,
        refresh_token: t.refresh_token,
        expires_in: t.expires_in,
        token_type: t.token_type,
    }
}

fn account_view(v: pl::AccountView) -> dto::AccountView {
    dto::AccountView {
        account_id: v.account_id,
        owner_id: v.owner_id,
        currency: v.currency,
        status: v.status,
        posted_balance: money(v.posted_balance),
        reserved: money(v.reserved),
        available: money(v.available),
        version: v.version,
    }
}

fn transfer_view(v: pl::TransferView) -> dto::TransferView {
    dto::TransferView {
        transfer_id: v.transfer_id,
        source_account_id: v.source_account_id,
        destination_account_id: v.destination_account_id,
        amount: money(v.amount),
        status: v.status,
        failure_reason: v.failure_reason,
        created_at: v.created_at,
        updated_at: v.updated_at,
    }
}

fn validate<T: Validate>(body: &T) -> Result<(), ApiError> {
    body.validate().map_err(|e| ApiError::from_validation(&e))
}

// ---------------------------------------------------------------------------
// Auth (public — no JWT/RBAC)
// ---------------------------------------------------------------------------

/// Register a new user.
#[utoipa::path(
    post, path = "/api/auth/register", tag = "auth",
    request_body = RegisterRequest,
    responses((status = 200, description = "Registered", body = RegisterResponse),
              (status = 400, description = "Validation error"))
)]
pub async fn register(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Json(body): Json<dto::RegisterRequest>,
) -> Result<Json<dto::RegisterResponse>, ApiError> {
    validate(&body)?;
    let mut client = st.clients.auth();
    let request = request_with_ids(
        pid::RegisterRequest {
            email: body.email,
            password: body.password,
            display_name: body.display_name,
        },
        &ids,
    );
    let resp = guarded(st.clients.auth_breaker(), move || async move {
        client
            .register(request)
            .await
            .map(tonic::Response::into_inner)
    })
    .await?;
    Ok(Json(dto::RegisterResponse {
        user_id: resp.user_id,
    }))
}

/// Log in and receive a token pair.
#[utoipa::path(
    post, path = "/api/auth/login", tag = "auth",
    request_body = LoginRequest,
    responses((status = 200, description = "Authenticated", body = TokenPair),
              (status = 401, description = "Invalid credentials"))
)]
pub async fn login(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Json(body): Json<dto::LoginRequest>,
) -> Result<Json<dto::TokenPair>, ApiError> {
    validate(&body)?;
    let mut client = st.clients.auth();
    let request = request_with_ids(
        pid::LoginRequest {
            email: body.email,
            password: body.password,
        },
        &ids,
    );
    let resp = guarded(st.clients.auth_breaker(), move || async move {
        client.login(request).await.map(tonic::Response::into_inner)
    })
    .await?;
    Ok(Json(token_pair(resp)))
}

/// Rotate a refresh token.
#[utoipa::path(
    post, path = "/api/auth/refresh", tag = "auth",
    request_body = RefreshRequest,
    responses((status = 200, description = "Rotated", body = TokenPair),
              (status = 401, description = "Invalid/reused token"))
)]
pub async fn refresh(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Json(body): Json<dto::RefreshRequest>,
) -> Result<Json<dto::TokenPair>, ApiError> {
    validate(&body)?;
    let mut client = st.clients.auth();
    let request = request_with_ids(
        pid::RefreshRequest {
            refresh_token: body.refresh_token,
        },
        &ids,
    );
    let resp = guarded(st.clients.auth_breaker(), move || async move {
        client
            .refresh_token(request)
            .await
            .map(tonic::Response::into_inner)
    })
    .await?;
    Ok(Json(token_pair(resp)))
}

/// Revoke a refresh-token family (logout).
#[utoipa::path(
    post, path = "/api/auth/logout", tag = "auth",
    request_body = LogoutRequest,
    responses((status = 200, description = "Revoked", body = LogoutResponse))
)]
pub async fn logout(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Json(body): Json<dto::LogoutRequest>,
) -> Result<Json<dto::LogoutResponse>, ApiError> {
    validate(&body)?;
    let mut client = st.clients.auth();
    let request = request_with_ids(
        pid::LogoutRequest {
            refresh_token: body.refresh_token,
        },
        &ids,
    );
    let resp = guarded(st.clients.auth_breaker(), move || async move {
        client
            .logout(request)
            .await
            .map(tonic::Response::into_inner)
    })
    .await?;
    Ok(Json(dto::LogoutResponse {
        revoked: resp.revoked,
    }))
}

// ---------------------------------------------------------------------------
// Ledger (protected — JWT + RBAC + rate limit applied by middleware)
// ---------------------------------------------------------------------------

/// Open an account owned by the authenticated subject.
#[utoipa::path(
    post, path = "/api/accounts", tag = "ledger",
    request_body = OpenAccountRequest,
    responses((status = 200, description = "Opened", body = OpenAccountResponse)),
    security(("bearer" = []))
)]
pub async fn open_account(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<dto::OpenAccountRequest>,
) -> Result<Json<dto::OpenAccountResponse>, ApiError> {
    validate(&body)?;
    let mut client = st.clients.ledger();
    let request = request_with_ids(
        pl::OpenAccountRequest {
            owner_id: claims.sub,
            currency: body.currency,
        },
        &ids,
    );
    let resp = guarded(st.clients.ledger_breaker(), move || async move {
        client
            .open_account(request)
            .await
            .map(tonic::Response::into_inner)
    })
    .await?;
    Ok(Json(dto::OpenAccountResponse {
        account_id: resp.account_id,
    }))
}

/// Fetch an account.
#[utoipa::path(
    get, path = "/api/accounts/{id}", tag = "ledger",
    params(("id" = String, Path, description = "Account id")),
    responses((status = 200, description = "Account", body = AccountView),
              (status = 404, description = "Not found")),
    security(("bearer" = []))
)]
pub async fn get_account(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Path(id): Path<String>,
) -> Result<Json<dto::AccountView>, ApiError> {
    let view = guarded_idempotent(
        st.clients.ledger_breaker(),
        st.retry_backoff.clone(),
        || {
            let mut client = st.clients.ledger();
            let request = request_with_ids(
                pl::GetAccountRequest {
                    account_id: id.clone(),
                },
                &ids,
            );
            async move {
                client
                    .get_account(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        },
    )
    .await?;
    Ok(Json(account_view(view)))
}

/// Fetch an account balance.
#[utoipa::path(
    get, path = "/api/accounts/{id}/balance", tag = "ledger",
    params(("id" = String, Path, description = "Account id")),
    responses((status = 200, description = "Balance", body = BalanceView),
              (status = 404, description = "Not found")),
    security(("bearer" = []))
)]
pub async fn get_balance(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Path(id): Path<String>,
) -> Result<Json<dto::BalanceView>, ApiError> {
    let view = guarded_idempotent(
        st.clients.ledger_breaker(),
        st.retry_backoff.clone(),
        || {
            let mut client = st.clients.ledger();
            let request = request_with_ids(
                pl::GetBalanceRequest {
                    account_id: id.clone(),
                },
                &ids,
            );
            async move {
                client
                    .get_balance(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        },
    )
    .await?;
    Ok(Json(dto::BalanceView {
        account_id: view.account_id,
        posted: money(view.posted),
        reserved: money(view.reserved),
        available: money(view.available),
    }))
}

/// List an account's transactions (paginated).
#[utoipa::path(
    get, path = "/api/accounts/{id}/transactions", tag = "ledger",
    params(("id" = String, Path, description = "Account id"), dto::ListTransactionsQuery),
    responses((status = 200, description = "Transactions", body = TransactionPage)),
    security(("bearer" = []))
)]
pub async fn list_transactions(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Path(id): Path<String>,
    Query(query): Query<dto::ListTransactionsQuery>,
) -> Result<Json<dto::TransactionPage>, ApiError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let cursor = query.cursor.unwrap_or_default();
    let page = guarded_idempotent(
        st.clients.ledger_breaker(),
        st.retry_backoff.clone(),
        || {
            let mut client = st.clients.ledger();
            let request = request_with_ids(
                pl::ListTransactionsRequest {
                    account_id: id.clone(),
                    limit,
                    cursor: cursor.clone(),
                },
                &ids,
            );
            async move {
                client
                    .list_transactions(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        },
    )
    .await?;
    Ok(Json(dto::TransactionPage {
        entries: page
            .entries
            .into_iter()
            .map(|e| dto::TransactionEntry {
                transfer_id: e.transfer_id,
                direction: e.direction,
                amount: money(e.amount),
                occurred_at: e.occurred_at,
            })
            .collect(),
        next_cursor: page.next_cursor,
    }))
}

/// Initiate a transfer. Honors an `Idempotency-Key` header (safe retries).
#[utoipa::path(
    post, path = "/api/transfers", tag = "ledger",
    request_body = TransferRequest,
    params(("Idempotency-Key" = Option<String>, Header, description = "Client idempotency key")),
    responses((status = 200, description = "Accepted", body = TransferAccepted)),
    security(("bearer" = []))
)]
pub async fn create_transfer(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    headers: HeaderMap,
    Json(body): Json<dto::TransferRequest>,
) -> Result<Json<dto::TransferAccepted>, ApiError> {
    validate(&body)?;
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let mut client = st.clients.ledger();
    let request = request_with_ids(
        pl::InitiateTransferRequest {
            idempotency_key,
            source_account_id: body.source_account_id,
            destination_account_id: body.destination_account_id,
            amount: Some(pl::Money {
                minor_units: body.amount.minor_units,
                currency: body.amount.currency,
            }),
        },
        &ids,
    );
    let resp = guarded(st.clients.ledger_breaker(), move || async move {
        client
            .initiate_transfer(request)
            .await
            .map(tonic::Response::into_inner)
    })
    .await?;
    Ok(Json(dto::TransferAccepted {
        transfer_id: resp.transfer_id,
        status: resp.status,
    }))
}

/// Fetch a transfer.
#[utoipa::path(
    get, path = "/api/transfers/{id}", tag = "ledger",
    params(("id" = String, Path, description = "Transfer id")),
    responses((status = 200, description = "Transfer", body = TransferView),
              (status = 404, description = "Not found")),
    security(("bearer" = []))
)]
pub async fn get_transfer(
    State(st): State<AppState>,
    Extension(ids): Extension<CorrelationIds>,
    Path(id): Path<String>,
) -> Result<Json<dto::TransferView>, ApiError> {
    let view = guarded_idempotent(
        st.clients.ledger_breaker(),
        st.retry_backoff.clone(),
        || {
            let mut client = st.clients.ledger();
            let request = request_with_ids(
                pl::GetTransferRequest {
                    transfer_id: id.clone(),
                },
                &ids,
            );
            async move {
                client
                    .get_transfer(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        },
    )
    .await?;
    Ok(Json(transfer_view(view)))
}
