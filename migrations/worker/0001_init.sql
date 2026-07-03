-- Worker context schema (worker_db). The worker is the cross-cutting background tier
-- (ARCHITECTURE §3): scheduler, audit sink, cross-context provisioning, and DLQ monitor.
-- None of these tables are event-sourced; they are operational/read-side stores that are
-- safe to rebuild by replaying the event backbone.

-- ---- Audit sink (immutable, append-only) --------------------------------------------------
-- Every state-changing command in the platform emits an `audit.v1` event; the worker records
-- it here for independent, compliance-grade querying (ARCHITECTURE §6.2). Rows are never
-- updated or deleted. `event_id` is UNIQUE so the consumer is idempotent under at-least-once
-- delivery (dedup on the envelope's event_id).
CREATE TABLE IF NOT EXISTS audit_log (
    id            BIGSERIAL   PRIMARY KEY,
    event_id      UUID        NOT NULL UNIQUE,       -- consumer idempotency/dedup key
    actor         TEXT        NOT NULL,              -- who performed the action
    action        TEXT        NOT NULL,              -- what was done (event_type or explicit)
    resource_type TEXT        NOT NULL,              -- kind of resource acted upon
    resource_id   TEXT        NOT NULL,              -- id of the resource (aggregate/stream id)
    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_log (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_audit_occurred ON audit_log (occurred_at DESC);

-- ---- Idempotent-consumer bookkeeping ------------------------------------------------------
-- Generic "have we already acted on this id?" table used by the provisioning consumer so a
-- redelivered UserRegistered event does not open a second wallet. Keyed by the *business* id
-- (the user id) rather than the raw event id, so retries of distinct envelopes for the same
-- user still dedup.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- Dead-letter monitor ------------------------------------------------------------------
-- The DLQ monitor consumes the `.dlq` companion topics and records each poison message here
-- with its failure context, exposing `dlq_depth` and enabling operator replay (ARCHITECTURE
-- §6.1). `replayed` flips to true once the entry has been re-published to its main topic.
CREATE TABLE IF NOT EXISTS dlq_entries (
    id         BIGSERIAL   PRIMARY KEY,
    topic      TEXT        NOT NULL,                 -- the .dlq topic the message arrived on
    event_id   TEXT        NOT NULL,
    payload    JSONB       NOT NULL,                 -- the full EventEnvelope, for replay
    error      TEXT,                                 -- failure detail, if known
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed   BOOLEAN     NOT NULL DEFAULT false
);
-- Idempotent recording: the same dead-lettered event is only stored once per topic.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dlq_topic_event ON dlq_entries (topic, event_id);
CREATE INDEX IF NOT EXISTS idx_dlq_unreplayed ON dlq_entries (topic) WHERE NOT replayed;

-- ---- Scheduler bookkeeping ----------------------------------------------------------------
-- Records each scheduled-job run for observability and to make a "last run" queryable. The
-- scheduler itself is driven by tokio-cron-scheduler; this table is a durable audit of ticks.
CREATE TABLE IF NOT EXISTS scheduler_runs (
    id        BIGSERIAL   PRIMARY KEY,
    job_name  TEXT        NOT NULL,
    ran_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    outcome   TEXT        NOT NULL DEFAULT 'ok'
);
CREATE INDEX IF NOT EXISTS idx_scheduler_job_time ON scheduler_runs (job_name, ran_at DESC);
