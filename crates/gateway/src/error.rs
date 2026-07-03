//! The gateway's public error type and the gRPC→HTTP status translation (ADR-0011).
//!
//! Every failure that reaches the edge is rendered as a small, stable JSON envelope:
//!
//! ```json
//! { "error": { "code": "not_found", "message": "account not found" } }
//! ```
//!
//! Upstream services speak gRPC, so their [`tonic::Status`] codes are mapped to the closest
//! HTTP status here — the single translation seam the architecture calls for.

use axum::response::{IntoResponse, Response};
use axum::Json;
use http::StatusCode;
use serde_json::json;

/// An error rendered to the client as a JSON envelope with an HTTP status.
#[derive(Debug, Clone)]
pub struct ApiError {
    /// HTTP status to return.
    pub status: StatusCode,
    /// Stable machine-readable code (e.g. `not_found`, `validation_error`).
    pub code: &'static str,
    /// Human-readable detail (safe to surface; never contains secrets).
    pub message: String,
}

impl ApiError {
    /// Construct an error with an explicit status and code.
    #[must_use]
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    /// 401 — missing/invalid credentials.
    #[must_use]
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    /// 403 — authenticated but lacking the required permission.
    #[must_use]
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    /// 429 — rate limit exceeded.
    #[must_use]
    pub fn too_many_requests(message: impl Into<String>) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, "rate_limited", message)
    }

    /// 503 — an upstream is unavailable (e.g. circuit breaker open).
    #[must_use]
    pub fn upstream_unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream_unavailable",
            "upstream service is temporarily unavailable",
        )
    }

    /// Build from a `validator` validation failure (400).
    #[must_use]
    pub fn from_validation(errors: &validator::ValidationErrors) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "validation_error",
            errors.to_string(),
        )
    }

    /// Translate an upstream gRPC [`tonic::Status`] into an edge error.
    #[must_use]
    pub fn from_status(status: &tonic::Status) -> Self {
        Self {
            status: grpc_code_to_http(status.code()),
            code: grpc_code_name(status.code()),
            message: status.message().to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({
            "error": { "code": self.code, "message": self.message }
        }));
        (self.status, body).into_response()
    }
}

/// Map a gRPC status code to the closest HTTP status code.
///
/// This is the canonical edge translation table (a pure function, unit-tested).
#[must_use]
pub fn grpc_code_to_http(code: tonic::Code) -> StatusCode {
    use tonic::Code;
    match code {
        Code::Ok => StatusCode::OK,
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            StatusCode::BAD_REQUEST
        }
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::AlreadyExists | Code::Aborted => StatusCode::CONFLICT,
        Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        Code::Cancelled => StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
        Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::Unknown | Code::Internal | Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Stable string name for a gRPC code (used as the error envelope `code`).
#[must_use]
pub fn grpc_code_name(code: tonic::Code) -> &'static str {
    use tonic::Code;
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn maps_common_codes() {
        assert_eq!(grpc_code_to_http(Code::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(grpc_code_to_http(Code::AlreadyExists), StatusCode::CONFLICT);
        assert_eq!(grpc_code_to_http(Code::Aborted), StatusCode::CONFLICT);
        assert_eq!(
            grpc_code_to_http(Code::InvalidArgument),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            grpc_code_to_http(Code::FailedPrecondition),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            grpc_code_to_http(Code::PermissionDenied),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            grpc_code_to_http(Code::Unauthenticated),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            grpc_code_to_http(Code::ResourceExhausted),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            grpc_code_to_http(Code::Unavailable),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            grpc_code_to_http(Code::DeadlineExceeded),
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            grpc_code_to_http(Code::Internal),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            grpc_code_to_http(Code::Unimplemented),
            StatusCode::NOT_IMPLEMENTED
        );
    }

    #[test]
    fn from_status_carries_code_and_message() {
        let err = ApiError::from_status(&tonic::Status::not_found("account not found"));
        assert_eq!(err.status, StatusCode::NOT_FOUND);
        assert_eq!(err.code, "not_found");
        assert_eq!(err.message, "account not found");
    }

    #[test]
    fn code_names_are_stable() {
        assert_eq!(grpc_code_name(Code::NotFound), "not_found");
        assert_eq!(grpc_code_name(Code::Unavailable), "unavailable");
    }
}
