# Domain Model — The Ledger

> The Domain-Driven Design of the money core: ubiquitous language, aggregates, value
> objects, domain events, commands, invariants, the transfer saga, and the read models.
> This is the part that must be correct; the [architecture](./ARCHITECTURE.md) exists to
> serve it.

---

## 1. Ubiquitous language

| Term | Meaning |
|---|---|
| **Account** | A container of money with a single currency, an owner, and a status. The **consistency boundary** for balance rules. Modeled as an event-sourced **aggregate**. |
| **Posting** | A single-sided movement recorded against one account: a **debit** or a **credit** of a `Money` amount. Postings never exist alone. |
| **Transfer** | A balanced movement of money between two accounts (`Σ debits = Σ credits`). Realized by a **saga**, not a single transaction. |
| **Reservation** | A hold placed on a source account's available balance during an in-flight transfer. Guarantees no double-spend without locking the whole account. |
| **Available balance** | `posted_balance − reserved`. What the account may still spend. |
| **Posted balance** | Sum of all captured postings. What has actually settled. |
| **Event** | An immutable fact that already happened (`FundsReserved`). The source of truth. |
| **Command** | A request to change state (`ReserveFunds`) that may be rejected. |
| **Projection / Read model** | A denormalized view built by folding events, optimized for queries. |

**Money is integer minor units** (e.g. cents), typed with its currency. Never a float, never
implicitly cross-currency. See [ADR-0010](./adr/0010-money-representation.md).

---

## 2. Aggregates & value objects

### 2.1 `Account` — the aggregate root (event-sourced)

The Account is the only aggregate that is event-sourced, because it owns the invariants that
must hold transactionally: *you cannot commit or reserve more than is available, and you
cannot post to a frozen or closed account.*

**State (a fold over its event stream):**

```
Account {
  id: AccountId,
  owner: OwnerId,
  currency: Currency,
  status: Open | Frozen | Closed,
  posted_balance: Money,        // settled
  reserved: Money,              // held by in-flight transfers
  version: u64,                 // optimistic-concurrency guard
}
// invariant: available() = posted_balance - reserved  ≥ 0   (for customer/liability accounts)
```

**Commands → decisions → events.** A command handler loads the aggregate (replay events, or
from snapshot + tail), calls a pure `decide(command) -> Result<Vec<Event>, DomainError>`,
then appends the resulting events with the expected `version`. If another writer advanced
the version first, the append fails and the command retries on fresh state — **optimistic
concurrency**, no long-held locks.

| Command | Guard (invariant checked) | Emits |
|---|---|---|
| `OpenAccount` | not already open | `AccountOpened` |
| `ReserveFunds{amount, transfer_id}` | status=Open ∧ `available ≥ amount` ∧ transfer_id not already reserved | `FundsReserved` |
| `CaptureFunds{transfer_id}` | a matching reservation exists | `FundsCaptured` |
| `ReleaseReservation{transfer_id}` | a matching reservation exists (compensation) | `ReservationReleased` |
| `CreditFunds{amount, transfer_id}` | status=Open | `FundsCredited` |
| `FreezeAccount` / `CloseAccount` | status transitions valid; close requires zero reserved | `AccountFrozen` / `AccountClosed` |

Reservations make **no-double-spend** hold without pessimistic locking: `available` drops at
reserve time, so two concurrent transfers competing for the same funds cannot both reserve —
the second fails the `available ≥ amount` guard (enforced by optimistic concurrency on
`version`). A short **Redis distributed lock** per `account_id` additionally serializes the
command pipeline to cut retry churn under contention (optimization, not correctness —
correctness rests on the version check). See [ADR-0005](./adr/0005-transfer-saga.md).

### 2.2 Value objects (in the `kernel` crate, shared)

```
Money      { minor_units: i128, currency: Currency }   // checked add/sub; same-currency only
Currency   ISO-4217 enum (USD, EUR, ...)               // single currency per account (v1)
AccountId  Uuid newtype        OwnerId  Uuid newtype
TransferId Uuid newtype (also the client Idempotency-Key surface)
Version    u64
```

Value objects are immutable, self-validating (`Money::new` rejects negative where illegal,
arithmetic is checked and returns `Result` on overflow/mismatch), and have no identity.

---

## 3. Domain events (the `ledger.*.v1` catalog)

Events are the source of truth and the integration contract. They are **versioned** and
**append-only**. Each carries envelope metadata: `event_id`, `stream_id` (account or
transfer), `version`, `correlation_id`, `causation_id`, `occurred_at`.

```
Account stream (stream_id = account_id):
  AccountOpened      { owner, currency }
  FundsReserved      { transfer_id, amount }
  ReservationReleased{ transfer_id, amount, reason }
  FundsCaptured      { transfer_id, amount }
  FundsCredited      { transfer_id, amount }
  AccountFrozen      { reason }
  AccountClosed      {}

Transfer stream (stream_id = transfer_id) — saga lifecycle:
  TransferRequested  { source, destination, amount, idempotency_key }
  TransferCompleted  {}
  TransferFailed     { reason, failed_step }
```

Schema evolution rule: events are **additive only** — new optional fields, never remove or
repurpose. Breaking changes mint a new topic version (`.v2`) with an upcaster.

---

## 4. Event store & CQRS

### 4.1 Write model — the event store (PostgreSQL, ledger DB)

```sql
CREATE TABLE events (
  global_seq   BIGSERIAL PRIMARY KEY,          -- total order, drives the outbox relay
  event_id     UUID        NOT NULL UNIQUE,     -- idempotency key for consumers
  stream_id    UUID        NOT NULL,            -- aggregate id
  version      BIGINT      NOT NULL,            -- per-stream sequence
  event_type   TEXT        NOT NULL,
  payload      JSONB       NOT NULL,
  metadata     JSONB       NOT NULL,            -- correlation_id, causation_id, actor
  occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (stream_id, version)                   -- optimistic concurrency
);
CREATE TABLE snapshots (                        -- replay-cost optimization
  stream_id UUID PRIMARY KEY, version BIGINT NOT NULL,
  state JSONB NOT NULL, taken_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE outbox_offset ( relay TEXT PRIMARY KEY, last_published_seq BIGINT NOT NULL );
CREATE TABLE idempotency (                       -- client Idempotency-Key cache
  key TEXT PRIMARY KEY, request_hash TEXT NOT NULL,
  response JSONB, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Append is the commit.** A command handler writes N events in one transaction; the
`UNIQUE(stream_id, version)` constraint enforces optimistic concurrency; the
`BIGSERIAL global_seq` gives a durable total order that the outbox relay tails.
Rehydration = latest snapshot + events with higher version. Snapshots are taken every K
events to bound replay cost. See [ADR-0003](./adr/0003-event-sourcing-scope.md).

### 4.2 Read models — projections (CQRS query side)

Projection builders (in the `worker`) consume the event stream and maintain denormalized
tables, each optimized for a query and rebuildable from scratch by replaying events:

| Read model | Shape | Serves |
|---|---|---|
| `account_balance_view` | `(account_id, posted, reserved, available, currency, updated_at)` | GET balance (also Redis-cached) |
| `transaction_history_view` | append row per posting, indexed by `(account_id, occurred_at)` | GET statement / history |
| `transfer_status_view` | `(transfer_id, status, step, updated_at)` | GET transfer status, client polling |

Read and write scale independently: the write side optimizes for correct, ordered appends;
the read side optimizes for query latency and can be replicated and cached freely. Because
projections are pure folds, a bug fix is deployed by **replaying** the stream into a new
projection version and swapping — no data migration. See [ADR-0004](./adr/0004-cqrs.md).

---

## 5. The transfer saga

A transfer moves money between two Account aggregates. Because two event-sourced aggregates
cannot be mutated in one transaction, the transfer is a **process manager (saga)** with
explicit steps and **compensations**. The saga's own lifecycle is itself an event stream
(the `transfer` stream), so it is durable and recoverable after a crash.

### 5.1 State machine

```
                 ┌──────────────────────────────────────────────┐
   Requested ──▶ Reserving ──ok──▶ Crediting ──ok──▶ Capturing ──ok──▶ Completed
       │             │                  │                 │
       │          fail│               fail│              fail│
       │             ▼                  ▼                 ▼
       └────────▶ Releasing (compensate: ReleaseReservation) ──▶ Failed
```

| Step | Action (command to Account) | On success | On failure |
|---|---|---|---|
| 1. Reserve | `ReserveFunds` on **source** | → Crediting | insufficient funds / frozen → `TransferFailed` (no compensation needed) |
| 2. Credit | `CreditFunds` on **destination** | → Capturing | dest closed/frozen → **compensate** step 1, then `TransferFailed` |
| 3. Capture | `CaptureFunds` on **source** | → `TransferCompleted` | retriable; on exhaustion → **compensate**, `TransferFailed` |

### 5.2 Correctness properties

- **Idempotent steps.** Every command carries `transfer_id`; the Account rejects a duplicate
  reserve/credit/capture for the same transfer. So a redelivered Kafka message (at-least-once)
  is safe — effectively exactly-once *effects*.
- **Recoverable.** The saga state lives in the `transfer` stream + `transfer_status_view`. On
  worker restart, in-flight sagas are resumed from their last recorded step.
- **Bounded retries → DLQ.** A step that keeps failing for infrastructure reasons retries
  with exponential backoff, then routes to `ledger.transfer.v1.dlq` with full context for
  operator replay. See [ADR-0006](./adr/0006-transactional-outbox.md).
- **Compensation, not rollback.** You cannot un-happen an event; you emit a compensating
  event (`ReservationReleased`) that returns the system to a consistent state.

### 5.3 Conservation proof obligation

At every saga terminal state, the sum of all postings for the transfer is zero:
`Completed` → `(−amount on source) + (+amount on destination) = 0`; `Failed` → net zero
(reservation released, nothing captured). This invariant is asserted in a **property test**
(`proptest`) over random interleavings of concurrent transfers — see
[ROADMAP Phase 7](./ROADMAP.md).

---

## 6. Identity domain (contrast: deliberately NOT event-sourced)

Identity is a supporting context, modeled as classic CRUD + a transactional outbox — because
its invariants are local and its history doesn't need replay. This contrast is intentional:
using ES only where it pays for itself is a senior signal. See
[ADR-0003](./adr/0003-event-sourcing-scope.md).

Core entities: `User`, `Credential` (Argon2id hash), `Role`, `Permission`, `RefreshToken`
(hashed, family-tracked for reuse detection), `OAuthIdentity`. It emits integration events
(`UserRegistered`, `UserRoleGranted`) to the backbone for other contexts (e.g. Ledger
auto-opening a wallet on registration) — but stores state as normalized tables, not events.

---

*Next: [`ROADMAP.md`](./ROADMAP.md) for how this gets built, phase by phase, with acceptance
criteria — and [`adr/`](./adr) for the reasoning behind each decision above.*
