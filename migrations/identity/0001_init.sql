-- Identity context schema (identity_db). Classic normalized CRUD + a transactional outbox —
-- deliberately NOT event-sourced (DOMAIN §6, ADR-0003). See docs/adr/0009-auth-strategy.md.

-- ---- Users & credentials ----
CREATE TABLE IF NOT EXISTS users (
    id           UUID        PRIMARY KEY,
    email        TEXT        NOT NULL UNIQUE,
    display_name TEXT        NOT NULL DEFAULT '',
    status       TEXT        NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE | SUSPENDED
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One credential row per user; the password is stored as an Argon2id PHC string, never plain.
CREATE TABLE IF NOT EXISTS credentials (
    user_id       UUID        PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    password_hash TEXT        NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- RBAC: roles, permissions, and their many-to-many links ----
CREATE TABLE IF NOT EXISTS roles (
    name        TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS permissions (
    name        TEXT PRIMARY KEY,             -- e.g. 'ledger:transfer:create'
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id   UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_name TEXT NOT NULL REFERENCES roles(name) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_name)
);

CREATE TABLE IF NOT EXISTS role_permissions (
    role_name  TEXT NOT NULL REFERENCES roles(name) ON DELETE CASCADE,
    permission TEXT NOT NULL REFERENCES permissions(name) ON DELETE CASCADE,
    PRIMARY KEY (role_name, permission)
);

-- ---- Refresh tokens (hashed, family-tracked, single-use / rotating) ----
CREATE TABLE IF NOT EXISTS refresh_tokens (
    id         UUID        PRIMARY KEY,
    user_id    UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT        NOT NULL UNIQUE,   -- SHA-256 hex of the opaque token
    family_id  UUID        NOT NULL,          -- reuse-detection grouping
    used       BOOLEAN     NOT NULL DEFAULT false,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family ON refresh_tokens (family_id);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user   ON refresh_tokens (user_id);

-- ---- OAuth2 external identities linked to local users ----
CREATE TABLE IF NOT EXISTS oauth_identities (
    provider TEXT NOT NULL,                   -- e.g. 'github'
    subject  TEXT NOT NULL,                   -- provider's stable user id
    user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email    TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (provider, subject)
);
CREATE INDEX IF NOT EXISTS idx_oauth_identities_user ON oauth_identities (user_id);

-- ---- Transactional outbox (ADR-0006): committed in the same tx as the state change; the
-- relay tails `global_seq` and publishes to the bus, tracking progress in `outbox_offset`. ----
CREATE TABLE IF NOT EXISTS outbox (
    global_seq   BIGSERIAL   PRIMARY KEY,     -- total order; drives the relay
    id           UUID        NOT NULL UNIQUE, -- event id / consumer dedup key
    aggregate_id UUID        NOT NULL,        -- user id (partition key)
    topic        TEXT        NOT NULL,        -- identity.user.v1 | audit.v1
    event_type   TEXT        NOT NULL,        -- UserRegistered, ...
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published    BOOLEAN     NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (global_seq) WHERE published = false;

CREATE TABLE IF NOT EXISTS outbox_offset (
    relay              TEXT   PRIMARY KEY,
    last_published_seq BIGINT NOT NULL DEFAULT 0
);

-- ---- Seed default roles & permissions (idempotent) ----
INSERT INTO permissions (name, description) VALUES
    ('ledger:transfer:create', 'Initiate a transfer'),
    ('ledger:account:read',    'Read account balances and history'),
    ('identity:user:read',     'Read own user profile')
ON CONFLICT (name) DO NOTHING;

INSERT INTO roles (name, description) VALUES
    ('customer', 'Default end-user role'),
    ('admin',    'Administrative superuser')
ON CONFLICT (name) DO NOTHING;

INSERT INTO role_permissions (role_name, permission) VALUES
    ('customer', 'ledger:transfer:create'),
    ('customer', 'ledger:account:read'),
    ('customer', 'identity:user:read')
ON CONFLICT DO NOTHING;
