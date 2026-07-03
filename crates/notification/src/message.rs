//! Mapping consumed **ledger events** → outbound **client WebSocket messages**.
//!
//! This is the pure heart of the fan-out: given an [`EventEnvelope`] freshly consumed from
//! `ledger.transfer.v1` / `ledger.account.v1`, decide (a) what JSON message a connected client
//! should receive and (b) which principals it concerns. It performs **no I/O** so it is fully
//! unit-testable — the async [`crate::consumer`] layer resolves account ids to owners and does
//! the actual delivery.
//!
//! ## Recipient model (and its honest limitation)
//!
//! WebSocket connections register under the **JWT subject** (a user id). Ledger events, though,
//! are keyed by *account* / *transfer* aggregate ids and — except for `AccountOpened` — do not
//! carry the owner. Notification is stateless (Redis-only), so it cannot join to the ledger's
//! tables. We therefore split routing identifiers into two buckets:
//!
//! * [`Mapped::owners`] — owner/user ids found **directly** in the payload (route as-is).
//! * [`Mapped::accounts`] — account ids referenced by the event; the consumer resolves these to
//!   owners via a Redis `account_owner:<id>` map that it populates from `AccountOpened` events.

use infra::bus::EventEnvelope;
use serde::Serialize;
use serde_json::Value;

/// Ledger `AccountEvent` discriminators (the `ledger.account.v1` catalog). For these the
/// envelope's `stream_id` is the **account id**.
const ACCOUNT_EVENT_TYPES: &[&str] = &[
    "AccountOpened",
    "FundsReserved",
    "ReservationReleased",
    "FundsCaptured",
    "FundsCredited",
    "AccountFrozen",
    "AccountClosed",
];

/// A message pushed to a connected client. Serialized to JSON text over the socket.
///
/// Fields are optional so one shape serves every event type; `event_id` is always present so
/// clients can **dedup** (delivery is at-least-once).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClientMessage {
    /// Client-facing discriminator, e.g. `"transfer.completed"`, `"account.credited"`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Originating event id — the client dedup key.
    pub event_id: String,
    /// The transfer this message concerns, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<String>,
    /// The account this message concerns, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    /// A human/enum status where the event carries one (e.g. transfer state).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// The monetary amount, passed through verbatim from the event payload (kernel `Money`
    /// JSON: `{ "minor_units": .., "currency": ".." }`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Value>,
    /// When the originating event occurred (RFC 3339).
    pub occurred_at: String,
}

/// The result of mapping one event: the client message plus its routing identifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapped {
    /// The message to deliver.
    pub message: ClientMessage,
    /// Owner/user ids present directly in the event — delivered without a lookup.
    pub owners: Vec<String>,
    /// Account ids referenced by the event — resolved to owners by the consumer.
    pub accounts: Vec<String>,
}

/// Map a consumed event to a client message and its recipients, or `None` if the event is not
/// client-relevant.
#[must_use]
pub fn map_event(env: &EventEnvelope) -> Option<Mapped> {
    let payload = &env.payload;
    let is_account_event = ACCOUNT_EVENT_TYPES.contains(&env.event_type.as_str());

    let kind = client_kind(&env.event_type);

    // ---- gather routing identifiers ----
    let mut owners = Vec::new();
    for key in [
        "owner",
        "owner_id",
        "user_id",
        "source_owner",
        "destination_owner",
    ] {
        if let Some(v) = str_field(payload, key) {
            push_unique(&mut owners, v);
        }
    }

    let mut accounts = Vec::new();
    for key in [
        "source",
        "destination",
        "account",
        "account_id",
        "source_account",
        "destination_account",
    ] {
        if let Some(v) = str_field(payload, key) {
            push_unique(&mut accounts, v);
        }
    }
    // For account-stream events the aggregate id *is* the account id.
    if is_account_event {
        push_unique(&mut accounts, env.metadata.stream_id.clone());
    }

    // A transfer's aggregate id is the transfer id; otherwise take it from the payload.
    let transfer_id = str_field(payload, "transfer_id")
        .or_else(|| (!is_account_event).then(|| env.metadata.stream_id.clone()));

    // The account this message is *about* (for account events, the aggregate).
    let account_id = if is_account_event {
        Some(env.metadata.stream_id.clone())
    } else {
        str_field(payload, "account_id")
    };

    let message = ClientMessage {
        kind,
        event_id: env.metadata.event_id.clone(),
        transfer_id,
        account_id,
        status: str_field(payload, "status").or_else(|| str_field(payload, "state")),
        amount: payload.get("amount").cloned(),
        occurred_at: env.metadata.occurred_at.to_rfc3339(),
    };

    Some(Mapped {
        message,
        owners,
        accounts,
    })
}

/// Translate a ledger event discriminator into a stable, client-facing message `type`.
fn client_kind(event_type: &str) -> String {
    match event_type {
        // transfer saga lifecycle
        "TransferRequested" => "transfer.requested",
        "TransferCompleted" => "transfer.completed",
        "TransferFailed" => "transfer.failed",
        // account stream
        "AccountOpened" => "account.opened",
        "FundsReserved" => "account.funds_reserved",
        "ReservationReleased" => "account.reservation_released",
        "FundsCaptured" => "account.funds_captured",
        "FundsCredited" => "account.funds_credited",
        "AccountFrozen" => "account.frozen",
        "AccountClosed" => "account.closed",
        // unknown: derive a namespaced, snake_cased fallback so new events still flow through.
        other => return format!("ledger.{}", to_snake_case(other)),
    }
    .to_string()
}

/// Read a string-valued field from a JSON object (ignores non-string / absent).
fn str_field(payload: &Value, key: &str) -> Option<String> {
    payload.get(key).and_then(Value::as_str).map(str::to_string)
}

/// Push preserving order and rejecting empty / duplicate values.
fn push_unique(v: &mut Vec<String>, item: String) {
    if !item.is_empty() && !v.contains(&item) {
        v.push(item);
    }
}

/// `TransferCompleted` → `transfer_completed`. Simple ASCII CamelCase splitter.
fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use infra::bus::{EventEnvelope, EventMetadata};
    use serde_json::json;

    fn envelope(event_type: &str, stream_id: &str, payload: Value) -> EventEnvelope {
        EventEnvelope {
            event_type: event_type.to_string(),
            metadata: EventMetadata {
                event_id: "evt-1".to_string(),
                stream_id: stream_id.to_string(),
                version: 1,
                correlation_id: "corr-1".to_string(),
                causation_id: None,
                traceparent: None,
                occurred_at: Utc::now(),
            },
            payload,
        }
    }

    #[test]
    fn account_credited_routes_by_account_stream() {
        let env = envelope(
            "FundsCredited",
            "acct-123",
            json!({ "transfer_id": "trf-9", "amount": { "minor_units": 500, "currency": "USD" } }),
        );
        let mapped = map_event(&env).expect("mapped");
        assert_eq!(mapped.message.kind, "account.funds_credited");
        assert_eq!(mapped.message.event_id, "evt-1");
        assert_eq!(mapped.message.account_id.as_deref(), Some("acct-123"));
        assert_eq!(mapped.message.transfer_id.as_deref(), Some("trf-9"));
        assert_eq!(
            mapped.message.amount,
            Some(json!({ "minor_units": 500, "currency": "USD" }))
        );
        // account-stream event → the aggregate id is a routing account.
        assert_eq!(mapped.accounts, vec!["acct-123".to_string()]);
        assert!(mapped.owners.is_empty());
    }

    #[test]
    fn account_opened_carries_owner() {
        let env = envelope(
            "AccountOpened",
            "acct-777",
            json!({ "owner": "owner-abc", "currency": "USD" }),
        );
        let mapped = map_event(&env).expect("mapped");
        assert_eq!(mapped.message.kind, "account.opened");
        assert_eq!(mapped.owners, vec!["owner-abc".to_string()]);
        assert_eq!(mapped.accounts, vec!["acct-777".to_string()]);
    }

    #[test]
    fn transfer_completed_routes_by_both_accounts() {
        let env = envelope(
            "TransferCompleted",
            "trf-42",
            json!({
                "source": "acct-A",
                "destination": "acct-B",
                "status": "COMPLETED",
                "amount": { "minor_units": 1000, "currency": "USD" }
            }),
        );
        let mapped = map_event(&env).expect("mapped");
        assert_eq!(mapped.message.kind, "transfer.completed");
        // transfer aggregate id becomes the transfer_id.
        assert_eq!(mapped.message.transfer_id.as_deref(), Some("trf-42"));
        assert_eq!(mapped.message.status.as_deref(), Some("COMPLETED"));
        assert_eq!(
            mapped.accounts,
            vec!["acct-A".to_string(), "acct-B".to_string()]
        );
        // not an account-stream event → stream id is NOT treated as an account.
        assert!(!mapped.accounts.contains(&"trf-42".to_string()));
    }

    #[test]
    fn unknown_event_gets_snake_cased_fallback_kind() {
        let env = envelope("SomethingBrandNew", "trf-1", json!({}));
        let mapped = map_event(&env).expect("mapped");
        assert_eq!(mapped.message.kind, "ledger.something_brand_new");
    }

    #[test]
    fn serialized_message_omits_absent_fields() {
        let env = envelope("TransferFailed", "trf-5", json!({ "status": "FAILED" }));
        let text = serde_json::to_string(&map_event(&env).unwrap().message).unwrap();
        assert!(text.contains("\"type\":\"transfer.failed\""));
        assert!(text.contains("\"status\":\"FAILED\""));
        // no amount / account_id in payload → keys omitted
        assert!(!text.contains("\"amount\""));
        assert!(!text.contains("\"account_id\""));
    }
}
