//! Typed request/response DTOs for the REST edge.
//!
//! These are the OpenAPI-documented (`utoipa::ToSchema`) and validated (`validator`) shapes
//! the gateway exposes; handlers translate them to/from the internal protobuf messages. Money
//! is always `(minor_units, currency)` — never a float (ADR-0010).

use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Register a new user.
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct RegisterRequest {
    /// User email (login identifier).
    #[validate(email)]
    pub email: String,
    /// Plaintext password (hashed by Identity; never stored/logged here).
    #[validate(length(min = 8, max = 128))]
    pub password: String,
    /// Human-friendly display name.
    #[validate(length(min = 1, max = 128))]
    pub display_name: String,
}

/// Result of a successful registration.
#[derive(Debug, Serialize, ToSchema)]
pub struct RegisterResponse {
    /// The newly created user id.
    pub user_id: String,
}

/// Exchange credentials for tokens.
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct LoginRequest {
    /// User email.
    #[validate(email)]
    pub email: String,
    /// Password.
    #[validate(length(min = 1))]
    pub password: String,
}

/// Rotate a refresh token.
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct RefreshRequest {
    /// The current (rotating) refresh token.
    #[validate(length(min = 1))]
    pub refresh_token: String,
}

/// Revoke a refresh-token family.
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct LogoutRequest {
    /// The refresh token whose family should be revoked.
    #[validate(length(min = 1))]
    pub refresh_token: String,
}

/// An access + refresh token pair.
#[derive(Debug, Serialize, ToSchema)]
pub struct TokenPair {
    /// Short-lived RS256 access token (JWT).
    pub access_token: String,
    /// Opaque rotating refresh token.
    pub refresh_token: String,
    /// Access-token lifetime, seconds.
    pub expires_in: i64,
    /// Token type (`Bearer`).
    pub token_type: String,
}

/// Result of a logout.
#[derive(Debug, Serialize, ToSchema)]
pub struct LogoutResponse {
    /// Whether the token family was revoked.
    pub revoked: bool,
}

// ---------------------------------------------------------------------------
// Ledger
// ---------------------------------------------------------------------------

/// Money as integer minor units + ISO-4217 currency (ADR-0010).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Money {
    /// Integer minor units (e.g. cents). Never a float.
    pub minor_units: i64,
    /// ISO-4217 currency code (e.g. `USD`).
    pub currency: String,
}

/// Open a new account (owner is the authenticated subject).
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct OpenAccountRequest {
    /// ISO-4217 currency code for the account.
    #[validate(length(min = 3, max = 3))]
    pub currency: String,
}

/// Result of opening an account.
#[derive(Debug, Serialize, ToSchema)]
pub struct OpenAccountResponse {
    /// The new account id.
    pub account_id: String,
}

/// A full account view.
#[derive(Debug, Serialize, ToSchema)]
pub struct AccountView {
    /// Account id.
    pub account_id: String,
    /// Owner (user) id.
    pub owner_id: String,
    /// Account currency.
    pub currency: String,
    /// Lifecycle status (`OPEN` | `FROZEN` | `CLOSED`).
    pub status: String,
    /// Posted (settled) balance.
    pub posted_balance: Money,
    /// Reserved (held) funds.
    pub reserved: Money,
    /// Available balance (posted − reserved).
    pub available: Money,
    /// Aggregate version.
    pub version: u64,
}

/// A balance view.
#[derive(Debug, Serialize, ToSchema)]
pub struct BalanceView {
    /// Account id.
    pub account_id: String,
    /// Posted (settled) balance.
    pub posted: Money,
    /// Reserved (held) funds.
    pub reserved: Money,
    /// Available balance.
    pub available: Money,
}

/// Initiate a transfer. Send an `Idempotency-Key` header to make retries safe.
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct TransferRequest {
    /// Source account id.
    #[validate(length(min = 1))]
    pub source_account_id: String,
    /// Destination account id.
    #[validate(length(min = 1))]
    pub destination_account_id: String,
    /// Amount to move.
    #[validate(nested)]
    pub amount: Money,
}

impl Validate for Money {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        let mut errors = validator::ValidationErrors::new();
        if self.minor_units <= 0 {
            errors.add(
                "minor_units",
                validator::ValidationError::new("must_be_positive"),
            );
        }
        if self.currency.len() != 3 {
            errors.add(
                "currency",
                validator::ValidationError::new("must_be_iso4217"),
            );
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Result of initiating a transfer.
#[derive(Debug, Serialize, ToSchema)]
pub struct TransferAccepted {
    /// The transfer id.
    pub transfer_id: String,
    /// Initial status (`REQUESTED`).
    pub status: String,
}

/// A transfer view.
#[derive(Debug, Serialize, ToSchema)]
pub struct TransferView {
    /// Transfer id.
    pub transfer_id: String,
    /// Source account id.
    pub source_account_id: String,
    /// Destination account id.
    pub destination_account_id: String,
    /// Amount moved.
    pub amount: Money,
    /// Saga status.
    pub status: String,
    /// Failure reason (empty unless failed).
    pub failure_reason: String,
    /// Created-at (unix seconds).
    pub created_at: i64,
    /// Updated-at (unix seconds).
    pub updated_at: i64,
}

/// A single ledger transaction entry.
#[derive(Debug, Serialize, ToSchema)]
pub struct TransactionEntry {
    /// Transfer id.
    pub transfer_id: String,
    /// `DEBIT` | `CREDIT`.
    pub direction: String,
    /// Amount.
    pub amount: Money,
    /// Occurred-at (unix seconds).
    pub occurred_at: i64,
}

/// A page of transactions.
#[derive(Debug, Serialize, ToSchema)]
pub struct TransactionPage {
    /// The entries in this page.
    pub entries: Vec<TransactionEntry>,
    /// Opaque cursor for the next page (empty if none).
    pub next_cursor: String,
}

/// Pagination query for listing transactions.
#[derive(Debug, Deserialize, IntoParams)]
pub struct ListTransactionsQuery {
    /// Max entries to return (default 50).
    pub limit: Option<u32>,
    /// Opaque cursor from a previous page.
    pub cursor: Option<String>,
}
