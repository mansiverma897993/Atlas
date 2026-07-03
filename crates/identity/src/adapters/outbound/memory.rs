//! In-memory implementations of the outbound ports, for unit/integration tests without a
//! database. A single [`InMemoryStore`] backs all four ports; share it as `Arc<InMemoryStore>`
//! and coerce to each `dyn` trait.
//!
//! These are intentionally simple (a `Mutex` around plain collections) — correctness over
//! concurrency; tests are single-threaded per store.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use uuid::Uuid;

use crate::application::ports::{
    Credentials, Effective, NewUser, OAuthUserInfo, OutboxEvent, OutboxWriter, PortError,
    RefreshTokenRecord, RefreshTokenRepository, RoleRepository, UserRepository,
};
use crate::domain::user::UserId;

struct UserRow {
    id: UserId,
    email: String,
    password_hash: String,
    active: bool,
    roles: Vec<String>,
}

#[derive(Default)]
struct Inner {
    users: Vec<UserRow>,
    oauth: HashMap<(String, String), UserId>,
    refresh: Vec<RefreshTokenRecord>,
    role_permissions: HashMap<String, Vec<String>>,
    outbox: Vec<OutboxEvent>,
}

/// A fully in-memory backing store implementing every outbound port.
pub struct InMemoryStore {
    inner: Mutex<Inner>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStore {
    /// Create a store seeded with the default `customer` and `admin` roles (mirroring the
    /// migration seed).
    #[must_use]
    pub fn new() -> Self {
        let mut role_permissions = HashMap::new();
        role_permissions.insert(
            "customer".to_string(),
            vec![
                "ledger:transfer:create".to_string(),
                "ledger:account:read".to_string(),
            ],
        );
        role_permissions.insert("admin".to_string(), vec!["*".to_string()]);
        Self {
            inner: Mutex::new(Inner {
                role_permissions,
                ..Inner::default()
            }),
        }
    }

    /// Number of events written to the outbox (test assertion helper).
    #[must_use]
    pub fn outbox_len(&self) -> usize {
        self.inner.lock().unwrap().outbox.len()
    }
}

#[async_trait]
impl UserRepository for InMemoryStore {
    async fn register(&self, new: NewUser) -> Result<(), PortError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.users.iter().any(|u| u.email == new.user.email) {
            return Err(PortError::UniqueViolation);
        }
        inner.users.push(UserRow {
            id: new.user.id,
            email: new.user.email.clone(),
            password_hash: new.password_hash,
            active: matches!(new.user.status, crate::domain::user::UserStatus::Active),
            roles: new.roles,
        });
        inner.outbox.push(new.event);
        Ok(())
    }

    async fn find_credentials_by_email(
        &self,
        email: &str,
    ) -> Result<Option<Credentials>, PortError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .users
            .iter()
            .find(|u| u.email == email)
            .map(|u| Credentials {
                user_id: u.id,
                password_hash: u.password_hash.clone(),
                active: u.active,
            }))
    }

    async fn find_user_id_by_oauth(
        &self,
        provider: &str,
        subject: &str,
    ) -> Result<Option<UserId>, PortError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .oauth
            .get(&(provider.to_string(), subject.to_string()))
            .copied())
    }

    async fn create_from_oauth(
        &self,
        info: &OAuthUserInfo,
        _display_name: &str,
        roles: &[String],
        event: OutboxEvent,
    ) -> Result<UserId, PortError> {
        let mut inner = self.inner.lock().unwrap();
        let id = UserId::new();
        inner.users.push(UserRow {
            id,
            email: info.email.clone(),
            password_hash: String::new(),
            active: true,
            roles: roles.to_vec(),
        });
        inner
            .oauth
            .insert((info.provider.clone(), info.subject.clone()), id);
        inner.outbox.push(event);
        Ok(id)
    }
}

#[async_trait]
impl RefreshTokenRepository for InMemoryStore {
    async fn insert(&self, record: &RefreshTokenRecord) -> Result<(), PortError> {
        self.inner.lock().unwrap().refresh.push(record.clone());
        Ok(())
    }

    async fn find_by_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<RefreshTokenRecord>, PortError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .refresh
            .iter()
            .find(|r| r.token_hash == token_hash)
            .cloned())
    }

    async fn mark_used(&self, id: Uuid) -> Result<(), PortError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(r) = inner.refresh.iter_mut().find(|r| r.id == id) {
            r.used = true;
        }
        Ok(())
    }

    async fn revoke_family(&self, family_id: Uuid) -> Result<u64, PortError> {
        let mut inner = self.inner.lock().unwrap();
        let before = inner.refresh.len();
        inner.refresh.retain(|r| r.family_id != family_id);
        Ok((before - inner.refresh.len()) as u64)
    }
}

#[async_trait]
impl RoleRepository for InMemoryStore {
    async fn effective_permissions(&self, user_id: UserId) -> Result<Effective, PortError> {
        let inner = self.inner.lock().unwrap();
        let Some(user) = inner.users.iter().find(|u| u.id == user_id) else {
            return Ok(Effective::default());
        };
        let mut permissions: Vec<String> = Vec::new();
        for role in &user.roles {
            if let Some(perms) = inner.role_permissions.get(role) {
                for p in perms {
                    if !permissions.contains(p) {
                        permissions.push(p.clone());
                    }
                }
            }
        }
        Ok(Effective {
            roles: user.roles.clone(),
            permissions,
        })
    }
}

#[async_trait]
impl OutboxWriter for InMemoryStore {
    async fn write(&self, event: OutboxEvent) -> Result<(), PortError> {
        self.inner.lock().unwrap().outbox.push(event);
        Ok(())
    }
}
