# ADR-0005 — Transfers as sagas with reserve/capture + compensation

**Status:** Accepted

## Context
A transfer moves money between two accounts and must be atomic *in effect*:
`Σ debits = Σ credits`, no double-spend, no partial state. But each Account is an independent
event-sourced aggregate and the consistency boundary for its own balance invariant. You
**cannot** mutate two event-sourced aggregates in one transaction, and a distributed 2PC
across aggregates is heavy, lock-prone, and still can't span external payment rails later.

## Decision
Model the transfer as a **saga (process manager)** with a two-phase **reserve → capture**
protocol and explicit **compensation**:
1. `ReserveFunds` on source (drops `available` immediately — this is what prevents
   double-spend, enforced by optimistic concurrency on the aggregate `version`).
2. `CreditFunds` on destination.
3. `CaptureFunds` on source (settles the reservation).

Any step's failure triggers compensating actions (`ReleaseReservation`) driving the saga to a
consistent `Failed` state. Every step command carries `transfer_id`, so the Account rejects
duplicates — steps are idempotent and safe under at-least-once delivery. The saga's lifecycle
is its own durable event stream, so it survives crashes and resumes.

## Consequences
- **+** No cross-aggregate transaction, no distributed locks for correctness (a Redis lock is
  used only to reduce optimistic-retry churn under contention).
- **+** Failure handling is explicit and testable; compensation replaces impossible rollback.
- **+** The reserve/capture shape extends naturally to external rails (a real PSP) later.
- **+** Idempotent steps give exactly-once *effects* on an at-least-once bus.
- **−** Eventual consistency: a transfer is not instantaneous; clients observe
  `Requested → … → Completed` via `transfer_status_view`. Acceptable and honest — this is how
  real money movement works.
- **−** Saga orchestration is non-trivial (state machine, resume, DLQ). This complexity is the
  point: it's the load-bearing distributed-systems problem the project exists to demonstrate.
