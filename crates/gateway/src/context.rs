//! Per-request correlation context.
//!
//! The gateway mints (or adopts) two ids for every inbound request (ARCHITECTURE §6.2):
//!
//! * `correlation_id` — follows the whole causal chain across services; adopted from the
//!   `x-correlation-id` header if the client supplied one, otherwise generated.
//! * `request_id` — identifies this single hop; adopted from `x-request-id` or generated.
//!
//! Both are stored in the request extensions (so handlers can read them via
//! [`axum::Extension`]), echoed back on the response, and injected into outbound gRPC
//! metadata so the downstream service logs the same ids.

use http::HeaderMap;

/// Header carrying the end-to-end correlation id.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";
/// Header carrying the per-hop request id.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// The correlation ids attached to a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationIds {
    /// End-to-end id, propagated across the whole causal chain.
    pub correlation_id: String,
    /// Id for this single request hop.
    pub request_id: String,
}

impl CorrelationIds {
    /// Extract ids from inbound headers, generating a fresh UUID for any that are absent or
    /// empty. This is the pure core of the correlation middleware (unit-tested).
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            correlation_id: header_or_generate(headers, CORRELATION_ID_HEADER),
            request_id: header_or_generate(headers, REQUEST_ID_HEADER),
        }
    }
}

/// Return the (non-empty) header value, or a freshly generated id.
fn header_or_generate(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map_or_else(generate_id, ToOwned::to_owned)
}

/// Generate a random id.
fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn adopts_supplied_correlation_id() {
        let mut headers = HeaderMap::new();
        headers.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("abc-123"));
        let ids = CorrelationIds::from_headers(&headers);
        assert_eq!(ids.correlation_id, "abc-123");
        // request id was absent -> generated, non-empty, and different from correlation id
        assert!(!ids.request_id.is_empty());
        assert_ne!(ids.request_id, "abc-123");
    }

    #[test]
    fn generates_when_absent() {
        let ids = CorrelationIds::from_headers(&HeaderMap::new());
        assert!(!ids.correlation_id.is_empty());
        assert!(!ids.request_id.is_empty());
        assert_ne!(ids.correlation_id, ids.request_id);
    }

    #[test]
    fn empty_header_is_treated_as_absent() {
        let mut headers = HeaderMap::new();
        headers.insert(REQUEST_ID_HEADER, HeaderValue::from_static("   "));
        let ids = CorrelationIds::from_headers(&headers);
        assert!(!ids.request_id.is_empty());
        assert_ne!(ids.request_id.trim(), "");
    }
}
