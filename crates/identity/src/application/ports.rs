//! Ports — the traits the application layer depends on, plus the DTOs crossing them. Adapters
//! (Postgres, in-memory, OAuth provider) implement these; the handlers never see SQL, HTTP, or
//! any concrete infrastructure. This is the dependency-inversion boundary of the hexagon
//! (ADR-0002).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::user::{User, UserId};

/// Failure of an outbound port. Kept storage-agnostic: adapters stringify their own errors.
#[derive(Debug, Error)]
pub enum PortError {
    /// A generic storage/backend failure.
    #[error("store error: {0}")]
    Store(String),

    /// A uniqueness constraint was violated (e.g. duplicate email). Surfaced distinctly so the
    /// handler can translate it into a domain-meaningful conflict.
    #[error("unique constraint violation")]
    UniqueViolation,

    /// The external OAuth provider failed the token exchange.
    #[error("oauth provider error: {0}")]
    Provider(String),
}

/// An event to be appended to the transactional outbox for the relay to publish (ADR-0006).
#[derive(Debug, Clone)]
pub struct OutboxEvent {
    /// The aggregate the event concerns (partition key / stream id) — here, the user id.
    pub aggregate_id: Uuid,
    /// Destination topic (e.g. [`infra::bus::topics::IDENTITY_USER`]).
    pub topic: String,
    /// Event discriminator, e.g. `"UserRegistered"`.
    pub event_type: String,
    /// JSON payload.
    pub payload: serde_json::Value,
}

/// Everything needed to create a user atomically: identity, credentials, roles, and the
/// integration event — all persisted in a **single transaction** so the outbox write cannot
/// diverge from the state change (no dual-write, ADR-0006).
pub struct NewUser {
    /// The user identity to insert.
    pub user: User,
    /// Argon2id PHC hash of the password.
    pub password_hash: String,
    /// Role names to grant (e.g. `["customer"]`).
    pub roles: Vec<String>,
    /// The `UserRegistered` event to enqueue in the same transaction.
    pub event: OutboxEvent,
}

/// Credentials returned by a lookup, used by the login flow.
pub struct Credentials {
    /// The user's id.
    pub user_id: UserId,
    /// The stored Argon2id hash.
    pub password_hash: String,
    /// Whether the account may authenticate (`status = ACTIVE`).
    pub active: bool,
}

/// A user's effective RBAC state: their roles and the union of the permissions those roles
/// grant.
#[derive(Debug, Clone, Default)]
pub struct Effective {
    /// Role names held by the user.
    pub roles: Vec<String>,
    /// De-duplicated permission strings.
    pub permissions: Vec<String>,
}

/// A refresh-token row to persist (already hashed).
#[derive(Debug, Clone)]
pub struct RefreshTokenRecord {
    /// Row id.
    pub id: Uuid,
    /// Owning user.
    pub user_id: UserId,
    /// SHA-256 hex of the opaque token.
    pub token_hash: String,
    /// Family grouping for reuse detection.
    pub family_id: Uuid,
    /// Whether already rotated out.
    pub used: bool,
    /// Absolute expiry.
    pub expires_at: DateTime<Utc>,
    /// Issue time.
    pub created_at: DateTime<Utc>,
}

/// Identity asserted by an OAuth provider after a successful code exchange.
#[derive(Debug, Clone)]
pub struct OAuthUserInfo {
    /// Provider key (e.g. `"github"`).
    pub provider: String,
    /// The provider's stable subject id.
    pub subject: String,
    /// Email asserted by the provider (may be empty).
    pub email: String,
}

/// **Port:** persistence of users, credentials, and OAuth links.
#[async_trait]
pub trait UserRepository: Send + Sync {
    /// Atomically insert the user, credentials, role grants, and the `UserRegistered` outbox
    /// event in one transaction. Returns [`PortError::UniqueViolation`] if the email exists.
    async fn register(&self, new: NewUser) -> Result<(), PortError>;

    /// Look up credentials by (normalized) email for the login flow.
    async fn find_credentials_by_email(
        &self,
        email: &str,
    ) -> Result<Option<Credentials>, PortError>;

    /// Find the local user linked to a provider identity, if any.
    async fn find_user_id_by_oauth(
        &self,
        provider: &str,
        subject: &str,
    ) -> Result<Option<UserId>, PortError>;

    /// Atomically create a user from an OAuth identity (no password), link the provider
    /// identity, grant `roles`, and enqueue the `UserRegistered` event.
    async fn create_from_oauth(
        &self,
        info: &OAuthUserInfo,
        display_name: &str,
        roles: &[String],
        event: OutboxEvent,
    ) -> Result<UserId, PortError>;
}

/// **Port:** persistence and rotation of refresh tokens (families).
#[async_trait]
pub trait RefreshTokenRepository: Send + Sync {
    /// Insert a freshly issued refresh token.
    async fn insert(&self, record: &RefreshTokenRecord) -> Result<(), PortError>;

    /// Find a token by its hash (the lookup key presented at refresh time).
    async fn find_by_hash(&self, token_hash: &str)
        -> Result<Option<RefreshTokenRecord>, PortError>;

    /// Mark a token as rotated out (single-use).
    async fn mark_used(&self, id: Uuid) -> Result<(), PortError>;

    /// Revoke (delete) every token in a family; returns the number affected. Used by logout and
    /// by reuse detection.
    async fn revoke_family(&self, family_id: Uuid) -> Result<u64, PortError>;
}

/// **Port:** RBAC queries — a user's roles and effective permissions.
#[async_trait]
pub trait RoleRepository: Send + Sync {
    /// The roles and de-duplicated permissions a user effectively holds.
    async fn effective_permissions(&self, user_id: UserId) -> Result<Effective, PortError>;
}

/// **Port:** append an event to the transactional outbox (used for non-registration events;
/// registration writes its event atomically via [`UserRepository::register`]).
#[async_trait]
pub trait OutboxWriter: Send + Sync {
    /// Append a single event to the outbox.
    async fn write(&self, event: OutboxEvent) -> Result<(), PortError>;
}

/// **Port:** the external OAuth2 provider. Behind a trait so the flow is testable with a fake
/// and no real HTTP client is required to build or test (ADR-0009).
#[async_trait]
pub trait OAuthProvider: Send + Sync {
    /// The provider key this adapter serves (e.g. `"github"`).
    fn provider(&self) -> &str;

    /// Build the authorization URL the user-agent is redirected to (carries `state` and the
    /// PKCE `code_challenge`).
    fn authorization_url(&self, state: &str, code_challenge: &str) -> String;

    /// Exchange an authorization `code` (+ the PKCE `code_verifier`) for the provider's asserted
    /// user identity.
    async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<OAuthUserInfo, PortError>;
}
