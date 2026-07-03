//! End-to-end use-case tests over the in-memory adapters. These exercise the wiring of the
//! [`AuthHandlers`] against the ports — registration, login, refresh **rotation**, **reuse
//! detection**, logout, and RBAC — without a database.

use std::sync::Arc;

use identity::adapters::outbound::memory::InMemoryStore;
use identity::adapters::outbound::oauth::StaticOAuthProvider;
use identity::application::error::AuthError;
use identity::application::handlers::AuthHandlers;
use identity::application::jwt::JwtIssuer;
use identity::application::ports::{
    OAuthProvider, OutboxWriter, RefreshTokenRepository, RoleRepository, UserRepository,
};
use identity::domain::DomainError;

/// Build handlers over a fresh in-memory store (seeded with `customer`/`admin` roles).
fn handlers() -> (AuthHandlers, Arc<InMemoryStore>) {
    let store = Arc::new(InMemoryStore::new());
    let oauth: Arc<dyn OAuthProvider> = Arc::new(StaticOAuthProvider::github_style(
        "http://localhost:8081/oauth/callback",
    ));
    let jwt =
        Arc::new(JwtIssuer::generate("https://identity.local", "ledger-platform", 900).unwrap());

    let users: Arc<dyn UserRepository> = store.clone();
    let tokens: Arc<dyn RefreshTokenRepository> = store.clone();
    let roles: Arc<dyn RoleRepository> = store.clone();
    let outbox: Arc<dyn OutboxWriter> = store.clone();

    let handlers = AuthHandlers::new(users, tokens, roles, outbox, oauth, jwt, 2_592_000);
    (handlers, store)
}

#[tokio::test]
async fn register_then_login_issues_verifiable_tokens() {
    let (h, store) = handlers();

    let user_id = h
        .register("Alice@Example.com", "goodpass1", "Alice")
        .await
        .unwrap();
    // The UserRegistered event was written to the outbox in the same unit of work.
    assert_eq!(store.outbox_len(), 1);

    let tokens = h.login("alice@example.com", "goodpass1").await.unwrap();
    assert!(!tokens.access_token.is_empty());
    assert!(!tokens.refresh_token.is_empty());
    assert_eq!(tokens.expires_in, 900);

    // The access token validates and carries the seeded customer permissions.
    let claims = h.validate_token(&tokens.access_token).unwrap();
    assert_eq!(claims.sub, user_id.to_string());
    assert!(claims
        .permissions
        .contains(&"ledger:transfer:create".to_string()));
}

#[tokio::test]
async fn duplicate_registration_is_rejected() {
    let (h, _) = handlers();
    h.register("bob@example.com", "goodpass1", "Bob")
        .await
        .unwrap();
    let err = h
        .register("bob@example.com", "goodpass1", "Bob")
        .await
        .unwrap_err();
    assert!(matches!(err, AuthError::EmailExists));
}

#[tokio::test]
async fn login_with_wrong_password_is_invalid_credentials() {
    let (h, _) = handlers();
    h.register("carol@example.com", "goodpass1", "Carol")
        .await
        .unwrap();
    let err = h
        .login("carol@example.com", "wrongpass9")
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        AuthError::Domain(DomainError::InvalidCredentials)
    ));
}

#[tokio::test]
async fn weak_password_is_rejected_at_registration() {
    let (h, _) = handlers();
    let err = h
        .register("dave@example.com", "short", "Dave")
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        AuthError::Domain(DomainError::WeakPassword(_))
    ));
}

#[tokio::test]
async fn refresh_rotates_and_reuse_revokes_the_family() {
    let (h, _) = handlers();
    h.register("erin@example.com", "goodpass1", "Erin")
        .await
        .unwrap();
    let first = h.login("erin@example.com", "goodpass1").await.unwrap();

    // Rotating the refresh token yields a new pair.
    let second = h.refresh(&first.refresh_token).await.unwrap();
    assert_ne!(first.refresh_token, second.refresh_token);

    // The new token still works...
    let third = h.refresh(&second.refresh_token).await.unwrap();
    assert_ne!(second.refresh_token, third.refresh_token);

    // ...but presenting the ALREADY-ROTATED first token is theft: reuse detected.
    let err = h.refresh(&first.refresh_token).await.unwrap_err();
    assert!(matches!(
        err,
        AuthError::Domain(DomainError::TokenReuseDetected)
    ));

    // Reuse detection revoked the WHOLE family, so the previously-valid `third` token is dead.
    let err = h.refresh(&third.refresh_token).await.unwrap_err();
    assert!(matches!(
        err,
        AuthError::Domain(DomainError::InvalidRefreshToken)
    ));
}

#[tokio::test]
async fn logout_revokes_family_and_is_idempotent() {
    let (h, _) = handlers();
    h.register("frank@example.com", "goodpass1", "Frank")
        .await
        .unwrap();
    let tokens = h.login("frank@example.com", "goodpass1").await.unwrap();

    assert!(h.logout(&tokens.refresh_token).await.unwrap());
    // Token no longer refreshes after logout.
    assert!(h.refresh(&tokens.refresh_token).await.is_err());
    // Logging out an unknown token is a no-op (idempotent).
    assert!(!h.logout(&tokens.refresh_token).await.unwrap());
}

#[tokio::test]
async fn authorize_enforces_rbac() {
    let (h, _) = handlers();
    let user_id = h
        .register("grace@example.com", "goodpass1", "Grace")
        .await
        .unwrap();
    let subject = user_id.to_string();

    // Customer holds this permission (seeded)...
    assert!(h
        .authorize(&subject, "ledger:transfer:create")
        .await
        .unwrap());
    // ...but not this one.
    assert!(!h.authorize(&subject, "identity:user:delete").await.unwrap());
    // Unknown subject holds nothing.
    let unknown = uuid::Uuid::new_v4().to_string();
    assert!(!h.authorize(&unknown, "ledger:account:read").await.unwrap());
}

#[tokio::test]
async fn oauth_callback_creates_and_relinks_user() {
    let (h, _) = handlers();

    // First callback creates a user and issues tokens.
    let start = h.start_oauth();
    assert!(start
        .authorization_url
        .contains("code_challenge_method=S256"));
    let first = h
        .complete_oauth("authcode-xyz", &start.code_verifier)
        .await
        .unwrap();
    let claims1 = h.validate_token(&first.access_token).unwrap();

    // Second callback with the same code (same provider subject) re-links to the same user.
    let second = h
        .complete_oauth("authcode-xyz", &start.code_verifier)
        .await
        .unwrap();
    let claims2 = h.validate_token(&second.access_token).unwrap();
    assert_eq!(claims1.sub, claims2.sub);
}
