# ADR-0006 — Transactional outbox + relay for reliable publishing; DLQ

**Status:** Accepted

## Context
Services must both persist state and publish events to the backbone. Writing to the DB and
to Redpanda as two separate operations creates the **dual-write problem**: a crash between
them leaves the DB and the stream inconsistent (an event lost, or published without the state
change committed). This is unacceptable for a ledger.

## Decision
**Never dual-write.** The event store *is* the outbox: events are appended in one local
transaction and carry a monotonic `global_seq`. A separate **relay** process tails
`global_seq`, publishes committed events to Redpanda, and records `last_published_seq`
transactionally. Delivery is **at-least-once**; consumers are **idempotent** (dedup on
`event_id`). Messages that fail processing after bounded exponential-backoff retries route to
a per-topic **dead-letter queue** (`<topic>.dlq`) with full context and an operator replay
tool.

(For CRUD contexts like Identity, the same pattern uses a dedicated `outbox` table written in
the same transaction as the state change.)

## Consequences
- **+** No lost or phantom events; DB state and published stream cannot diverge.
- **+** The relay can be restarted freely — it resumes from `last_published_seq`.
- **+** Poison messages are isolated in the DLQ instead of blocking the partition, and are
  replayable after a fix.
- **−** At-least-once means consumers *must* be idempotent — enforced by design (dedup key on
  every consumer) and verified in integration tests.
- **−** Publish latency includes the relay poll interval (or CDC lag). Tunable; acceptable for
  an event-driven system where consumers are already asynchronous.
- **Alternative considered:** Debezium/CDC instead of a poll-based relay — more infrastructure
  for marginal benefit at this scale; noted as a future swap behind the same event contract.
