-- Ledger context schema (ledger_db). Event store (write model) + projections (read models)
-- + outbox checkpoint + idempotency. See docs/DOMAIN.md §4.

-- ---- Write model: append-only event store (source of truth) ----
CREATE TABLE IF NOT EXISTS events (
    global_seq   BIGSERIAL PRIMARY KEY,             -- total order; drives the outbox relay
    event_id     UUID        NOT NULL UNIQUE,        -- consumer idempotency/dedup key
    stream_id    UUID        NOT NULL,               -- aggregate id (account or transfer)
    topic        TEXT        NOT NULL,               -- ledger.account.v1 | ledger.transfer.v1
    version      BIGINT      NOT NULL,               -- per-stream sequence
    event_type   TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    metadata     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, topic, version)               -- optimistic-concurrency guard
);
CREATE INDEX IF NOT EXISTS idx_events_stream ON events (stream_id, topic, version);

-- Aggregate snapshots (replay-cost optimization).
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id  UUID PRIMARY KEY,
    version    BIGINT NOT NULL,
    state      JSONB  NOT NULL,
    taken_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Outbox relay checkpoint (last global_seq published to the bus).
CREATE TABLE IF NOT EXISTS outbox_offset (
    relay              TEXT   PRIMARY KEY,
    last_published_seq BIGINT NOT NULL DEFAULT 0
);

-- Client idempotency keys for money-moving commands.
CREATE TABLE IF NOT EXISTS idempotency (
    key         TEXT PRIMARY KEY,
    transfer_id UUID NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- Read models (projections; rebuildable by replaying events) ----
CREATE TABLE IF NOT EXISTS account_balance_view (
    account_id UUID PRIMARY KEY,
    owner_id   UUID,
    currency   TEXT   NOT NULL,
    status     TEXT   NOT NULL,
    posted     BIGINT NOT NULL DEFAULT 0,            -- minor units
    reserved   BIGINT NOT NULL DEFAULT 0,            -- minor units
    version    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transaction_history_view (
    id          BIGSERIAL PRIMARY KEY,
    account_id  UUID NOT NULL,
    transfer_id UUID NOT NULL,
    direction   TEXT NOT NULL,                       -- DEBIT | CREDIT
    amount      BIGINT NOT NULL,                     -- minor units
    currency    TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_txn_account_time
    ON transaction_history_view (account_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS transfer_status_view (
    transfer_id    UUID PRIMARY KEY,
    source         UUID NOT NULL,
    destination    UUID NOT NULL,
    amount         BIGINT NOT NULL,                  -- minor units
    currency       TEXT NOT NULL,
    status         TEXT NOT NULL,                    -- REQUESTED..COMPLETED|FAILED
    failure_reason TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_transfer_pending
    ON transfer_status_view (updated_at) WHERE status NOT IN ('COMPLETED', 'FAILED');
