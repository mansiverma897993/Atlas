//! The use-case handlers — one method per `AuthService` RPC, plus the OAuth start/callback
//! flows. This is where the domain rules and the ports are orchestrated. The handlers are
//! transport-agnostic: they take/return plain values, and the gRPC adapter translates.

use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{Duration, Utc};
use rand::RngCore;
use uuid::Uuid;

use crate::application::error::AuthError;
use crate::application::jwt::{AccessClaims, JwtIssuer};
use crate::application::ports::{
    Credentials, NewUser, OAuthProvider, OutboxEvent, OutboxWriter, RefreshTokenRecord,
    RefreshTokenRepository, RoleRepository, UserRepository,
};
use crate::domain::oauth::{code_challenge, generate_code_verifier, generate_state};
use crate::domain::role::authorize;
use crate::domain::token::{evaluate_refresh, hash_token, RefreshDecision};
use crate::domain::user::{normalize_email, validate_password, User, UserId, UserStatus};
use crate::domain::{credential, DomainError};

use infra::bus::topics;

/// A freshly issued access + refresh token pair.
#[derive(Debug, Clone)]
pub struct IssuedTokens {
    /// The signed RS256 access JWT.
    pub access_token: String,
    /// The opaque, rotating refresh token (raw — returned once, stored hashed).
    pub refresh_token: String,
    /// Access-token lifetime in seconds.
    pub expires_in: i64,
}

/// The data a client needs to begin an OAuth2 authorization-code + PKCE flow.
pub struct OAuthStart {
    /// The provider authorization URL to redirect the user-agent to.
    pub authorization_url: String,
    /// Opaque CSRF `state` (echoed back to the callback).
    pub state: String,
    /// The PKCE `code_verifier`. In a public client the SPA keeps this; it must be presented at
    /// the callback to complete the exchange.
    pub code_verifier: String,
}

/// Default role granted to every newly registered user.
const DEFAULT_ROLE: &str = "customer";

/// Orchestrates the Identity use cases over the ports.
#[derive(Clone)]
pub struct AuthHandlers {
    users: Arc<dyn UserRepository>,
    tokens: Arc<dyn RefreshTokenRepository>,
    roles: Arc<dyn RoleRepository>,
    outbox: Arc<dyn OutboxWriter>,
    oauth: Arc<dyn OAuthProvider>,
    jwt: Arc<JwtIssuer>,
    refresh_ttl: Duration,
}

impl AuthHandlers {
    /// Wire the handlers with their ports.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        users: Arc<dyn UserRepository>,
        tokens: Arc<dyn RefreshTokenRepository>,
        roles: Arc<dyn RoleRepository>,
        outbox: Arc<dyn OutboxWriter>,
        oauth: Arc<dyn OAuthProvider>,
        jwt: Arc<JwtIssuer>,
        refresh_ttl_seconds: u64,
    ) -> Self {
        Self {
            users,
            tokens,
            roles,
            outbox,
            oauth,
            jwt,
            refresh_ttl: Duration::seconds(refresh_ttl_seconds as i64),
        }
    }

    /// **Register** a new user: validate input, Argon2id-hash the password, and persist the
    /// user + credential + default role together with the `UserRegistered` outbox event in one
    /// transaction (ADR-0006 — no dual-write).
    pub async fn register(
        &self,
        email: &str,
        password: &str,
        display_name: &str,
    ) -> Result<UserId, AuthError> {
        let email = normalize_email(email)?;
        validate_password(password)?;
        let password_hash = credential::hash_password(password).map_err(AuthError::Token)?;

        let user = User {
            id: UserId::new(),
            email: email.clone(),
            display_name: display_name.to_string(),
            status: UserStatus::Active,
            created_at: Utc::now(),
        };
        let event = OutboxEvent {
            aggregate_id: user.id.as_uuid(),
            topic: topics::IDENTITY_USER.to_string(),
            event_type: "UserRegistered".to_string(),
            payload: serde_json::json!({
                "user_id": user.id.to_string(),
                "email": email,
                "display_name": display_name,
            }),
        };
        let id = user.id;

        match self
            .users
            .register(NewUser {
                user,
                password_hash,
                roles: vec![DEFAULT_ROLE.to_string()],
                event,
            })
            .await
        {
            Ok(()) => {
                metrics::counter!("identity_registrations_total").increment(1);
                Ok(id)
            }
            Err(crate::application::ports::PortError::UniqueViolation) => {
                Err(AuthError::EmailExists)
            }
            Err(e) => Err(AuthError::Port(e)),
        }
    }

    /// **Login**: verify the password and issue a fresh access + refresh pair in a new family.
    pub async fn login(&self, email: &str, password: &str) -> Result<IssuedTokens, AuthError> {
        let email = normalize_email(email).map_err(|_| DomainError::InvalidCredentials)?;
        let creds = self.users.find_credentials_by_email(&email).await?;

        // Uniform failure whether the user is unknown, inactive, or the password is wrong
        // (user-enumeration defense).
        let Some(Credentials {
            user_id,
            password_hash,
            active,
        }) = creds
        else {
            metrics::counter!("identity_logins_total", "result" => "invalid").increment(1);
            return Err(DomainError::InvalidCredentials.into());
        };
        if !active || !credential::verify_password(&password_hash, password) {
            metrics::counter!("identity_logins_total", "result" => "invalid").increment(1);
            return Err(DomainError::InvalidCredentials.into());
        }

        let tokens = self.issue_pair(user_id, Uuid::new_v4()).await?;

        // Best-effort audit trail on the shared audit stream (not on the critical path).
        let _ = self
            .outbox
            .write(OutboxEvent {
                aggregate_id: user_id.as_uuid(),
                topic: topics::AUDIT.to_string(),
                event_type: "UserAuthenticated".to_string(),
                payload: serde_json::json!({ "user_id": user_id.to_string(), "method": "password" }),
            })
            .await;

        metrics::counter!("identity_logins_total", "result" => "success").increment(1);
        Ok(tokens)
    }

    /// **Refresh**: rotate the presented refresh token. Reuse of an already-rotated token
    /// revokes the whole family and is rejected (reuse detection, ADR-0009).
    pub async fn refresh(&self, refresh_token: &str) -> Result<IssuedTokens, AuthError> {
        let presented_hash = hash_token(refresh_token);
        let record = self.tokens.find_by_hash(&presented_hash).await?;

        // Map the DB row to the domain type for the pure decision.
        let domain_record = record.as_ref().map(|r| crate::domain::token::RefreshToken {
            id: r.id,
            user_id: r.user_id,
            token_hash: r.token_hash.clone(),
            family_id: r.family_id,
            used: r.used,
            expires_at: r.expires_at,
            created_at: r.created_at,
        });

        match evaluate_refresh(domain_record.as_ref(), Utc::now()) {
            RefreshDecision::Invalid => Err(DomainError::InvalidRefreshToken.into()),
            RefreshDecision::ReuseDetected => {
                // Theft signal: nuke the family so neither the attacker nor the victim can use
                // any descendant token.
                let family = domain_record.expect("reuse implies a record").family_id;
                self.tokens.revoke_family(family).await?;
                metrics::counter!("identity_refresh_reuse_detected_total").increment(1);
                Err(DomainError::TokenReuseDetected.into())
            }
            RefreshDecision::Rotate => {
                let record = record.expect("rotate implies a record");
                // Single-use: mark the old token spent, then mint a successor in the same family.
                self.tokens.mark_used(record.id).await?;
                let tokens = self.issue_pair(record.user_id, record.family_id).await?;
                metrics::counter!("identity_token_refreshes_total").increment(1);
                Ok(tokens)
            }
        }
    }

    /// **Logout**: revoke the family the presented refresh token belongs to. Idempotent —
    /// returns `false` if the token was already unknown.
    pub async fn logout(&self, refresh_token: &str) -> Result<bool, AuthError> {
        let hash = hash_token(refresh_token);
        let Some(record) = self.tokens.find_by_hash(&hash).await? else {
            return Ok(false);
        };
        let n = self.tokens.revoke_family(record.family_id).await?;
        Ok(n > 0)
    }

    /// **Validate** an access token, returning its verified claims.
    pub fn validate_token(&self, access_token: &str) -> Result<AccessClaims, AuthError> {
        self.jwt.validate(access_token).map_err(AuthError::Token)
    }

    /// **Authorize**: does `subject` hold `permission`? Re-checks RBAC in the service for
    /// defense in depth (ADR-0009).
    pub async fn authorize(&self, subject: &str, permission: &str) -> Result<bool, AuthError> {
        let user_id: UserId = subject.parse().map_err(|_| AuthError::InvalidSubject)?;
        let effective = self.roles.effective_permissions(user_id).await?;
        Ok(authorize(&effective.permissions, permission))
    }

    /// **Begin OAuth**: generate PKCE material and the provider authorization URL.
    #[must_use]
    pub fn start_oauth(&self) -> OAuthStart {
        let state = generate_state();
        let verifier = generate_code_verifier();
        let challenge = code_challenge(&verifier);
        OAuthStart {
            authorization_url: self.oauth.authorization_url(&state, &challenge),
            state,
            code_verifier: verifier,
        }
    }

    /// **Complete OAuth**: exchange the `code` (+ PKCE `verifier`) for the provider identity,
    /// then link it to an existing user or create one, and issue a token pair.
    pub async fn complete_oauth(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<IssuedTokens, AuthError> {
        let info = self.oauth.exchange_code(code, code_verifier).await?;

        let user_id = if let Some(existing) = self
            .users
            .find_user_id_by_oauth(&info.provider, &info.subject)
            .await?
        {
            existing
        } else {
            let display_name = if info.email.is_empty() {
                format!("{}:{}", info.provider, info.subject)
            } else {
                info.email.clone()
            };
            let new_id = UserId::new();
            let event = OutboxEvent {
                aggregate_id: new_id.as_uuid(),
                topic: topics::IDENTITY_USER.to_string(),
                event_type: "UserRegistered".to_string(),
                payload: serde_json::json!({
                    "user_id": new_id.to_string(),
                    "email": info.email,
                    "display_name": display_name,
                    "oauth_provider": info.provider,
                }),
            };
            self.users
                .create_from_oauth(&info, &display_name, &[DEFAULT_ROLE.to_string()], event)
                .await?
        };

        metrics::counter!("identity_oauth_logins_total", "provider" => info.provider.clone())
            .increment(1);
        self.issue_pair(user_id, Uuid::new_v4()).await
    }

    /// Mint an access token (embedding the user's effective RBAC) plus a fresh refresh token in
    /// `family`, persisting the refresh token's hash.
    async fn issue_pair(&self, user_id: UserId, family: Uuid) -> Result<IssuedTokens, AuthError> {
        let effective = self.roles.effective_permissions(user_id).await?;
        let (access_token, expires_in) = self
            .jwt
            .issue(
                &user_id.to_string(),
                &effective.roles,
                &effective.permissions,
            )
            .map_err(AuthError::Token)?;

        let raw_refresh = generate_opaque_token();
        let record = RefreshTokenRecord {
            id: Uuid::new_v4(),
            user_id,
            token_hash: hash_token(&raw_refresh),
            family_id: family,
            used: false,
            expires_at: Utc::now() + self.refresh_ttl,
            created_at: Utc::now(),
        };
        self.tokens.insert(&record).await?;

        Ok(IssuedTokens {
            access_token,
            refresh_token: raw_refresh,
            expires_in,
        })
    }
}

/// Generate a 256-bit opaque token, base64url-encoded (the raw refresh secret).
fn generate_opaque_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}
