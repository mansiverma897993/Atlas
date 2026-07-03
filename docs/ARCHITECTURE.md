# Architecture — Ledger Platform

> A production-grade, event-sourced double-entry **payments/ledger** backend in Rust.
> This document is the authoritative system design. Companion docs:
> [`DOMAIN.md`](./DOMAIN.md) (the ledger model), [`ROADMAP.md`](./ROADMAP.md) (delivery plan),
> and [`adr/`](./adr) (decision records).

---

## 1. What this system is

A **money-movement platform** — the backend that a fintech would put behind a wallet,
a marketplace payout system, or an internal treasury. Its job is to record and move
money with **correctness under concurrency**, **auditability**, and **resilience**.

We chose this domain deliberately. Event Sourcing, CQRS, sagas, idempotency, distributed
locking, dead-letter queues and exactly-once processing are **not decorative** here — a
ledger is the canonical domain in which each is load-bearing. A double-entry ledger that
loses, duplicates, or reorders a posting is broken; that constraint is what forces the
architecture to be real.

### Non-negotiable invariants (the whole system exists to protect these)

1. **Conservation** — every transfer is balanced: `Σ debits = Σ credits`. Money is never
   created or destroyed inside the ledger.
2. **No double-spend** — an account cannot commit more than its available balance.
3. **Idempotency** — a client may safely retry any money-moving request; it executes
   at most once.
4. **Immutability & audit** — history is append-only. You can reconstruct the exact state
   of any account at any point in time, and answer "who did what, when, and why."

Everything below serves these four.

---

## 2. System context (C4 level 1)

```
        ┌────────────┐   ┌────────────┐   ┌───────────────┐
        │ Web / SPA  │   │  Mobile    │   │ 3rd-party API │
        │            │   │            │   │  (partners)   │
        └─────┬──────┘   └─────┬──────┘   └───────┬───────┘
              │  HTTPS / WSS    │                  │
              └────────────────┴──────────────────┘
                               │
                     ┌─────────▼──────────┐
                     │    API GATEWAY     │  ← the only publicly exposed surface
                     └─────────┬──────────┘
                               │  gRPC (internal, mTLS-capable)
      ┌────────────────┬───────┴────────┬──────────────────┐
      │                │                │                  │
┌─────▼─────┐   ┌──────▼──────┐   ┌─────▼──────┐   ┌────────▼────────┐
│ IDENTITY  │   │   LEDGER    │   │ NOTIFICATION│   │     WORKER      │
│ & ACCESS  │   │  (ES+CQRS)  │   │  & REALTIME │   │ saga/proj/sched │
└─────┬─────┘   └──────┬──────┘   └─────┬───────┘   └────────┬────────┘
      │                │                │                    │
      └────────────────┴──── EVENT BACKBONE (Redpanda) ──────┘
                               │
   Data plane:  PostgreSQL (one DB per context)  ·  Redis (cache/locks/rate-limit)
   Observability:  OTel Collector → Jaeger (traces) · Prometheus (metrics) · Grafana
```

**Trust boundary:** only the gateway is internet-facing. Every internal service assumes
it sits inside a private network and communicates over gRPC. Auth is verified at the edge
*and* re-verified at each service (defense in depth) via a shared JWKS.

---

## 3. Bounded contexts & service responsibilities

Each context is an independently deployable binary (a crate in the workspace) with a
**single reason to change**. Boundaries follow the domain, not the technology.

| Context | Responsibility | Persistence style | Public? |
|---|---|---|---|
| **Gateway** | TLS/edge, routing, auth verification, rate-limit, resilience, OpenAPI, WS termination | stateless (Redis for limits) | yes |
| **Identity & Access** | users, credentials, JWT/refresh, OAuth2, RBAC | CRUD + outbox | via gateway |
| **Ledger** | accounts, balances, postings — the money | **Event Sourcing** | via gateway |
| **Notification & Realtime** | WebSocket hub, presence, event fan-out to clients | stateless (Redis presence) | WSS via gateway |
| **Worker** | transfer saga orchestration, projection building, scheduler, DLQ handling | read models + saga state | no |

**Why these boundaries:** *Identity* changes for security/authn reasons; *Ledger* changes
for financial-correctness reasons; *Notification* changes for UX/delivery reasons. Coupling
them would mean a login-flow change forces a redeploy of the money core. Kept apart, each
evolves and scales on its own axis. See [ADR-0001](./adr/0001-modular-monorepo.md).

---

## 4. Code organization — modular-monorepo Cargo workspace

We use **one repository, one Cargo workspace, many crates** — compile-time-checked shared
contracts with microservice deployment boundaries. Full polyrepo microservices would be
over-engineering for this scope; a single fat binary would fail the "independent scaling"
goal. See [ADR-0001](./adr/0001-modular-monorepo.md).

```
rustbackend/
├── Cargo.toml                  # [workspace] — shared deps, lints, profiles
├── crates/
│   ├── gateway/                # edge binary
│   ├── identity/               # auth service binary
│   ├── ledger/                 # ledger service binary
│   ├── notification/           # realtime service binary
│   ├── worker/                 # background processing binary
│   └── libs/                   # shared LIBRARY crates (no binaries)
│       ├── kernel/             # domain kernel: Money, ids, Currency, errors, Result
│       ├── proto/              # tonic-generated gRPC contracts (build.rs)
│       ├── config/             # layered typed configuration (figment)
│       ├── telemetry/          # tracing + OTel + metrics + logging bootstrap
│       ├── infra/              # SQLx pool, Redis, Kafka producer/consumer, outbox relay
│       └── resilience/         # Tower layers: retry, breaker, timeout, bulkhead, rate-limit
├── migrations/                 # per-context SQL migrations (sqlx)
├── deploy/
│   ├── docker-compose.yml      # full local stack
│   ├── k8s/                    # Deployments, Services, HPA, probes, secrets, netpol
│   └── observability/          # prometheus.yml, grafana dashboards, otel-collector.yaml
├── docs/                       # this spec set
└── tests/                      # cross-service integration (testcontainers)
```

### 4.1 Internal layout of a service (Hexagonal / Ports & Adapters)

Every service crate is structured the same way so a reviewer can navigate any of them
after learning one. The domain core has **zero** dependency on Axum, SQLx, Redis, or Kafka.
See [ADR-0002](./adr/0002-hexagonal-architecture.md).

```
crates/ledger/src/
├── main.rs                 # composition root: build config, wire adapters into ports, run
├── domain/                 # PURE. aggregates, value objects, domain events, invariants.
│   ├── account.rs          #   Account aggregate (fold events → state, decide command → events)
│   ├── events.rs           #   domain event enum
│   └── error.rs            #   domain errors (business rule violations)
├── application/            # use-cases. orchestrates domain + ports. no framework types.
│   ├── commands/           #   command handlers (write side)
│   ├── queries/            #   query handlers (read side)
│   └── ports/              #   TRAITS: EventStore, ReadModel, Clock, IdGen, UnitOfWork...
└── adapters/               # implements ports + drives the app. framework lives ONLY here.
    ├── inbound/
    │   ├── grpc/           #   tonic service impl → calls application
    │   └── http/           #   (gateway only) Axum handlers
    └── outbound/
        ├── postgres/       #   SQLx EventStore + read-model repositories
        ├── redis/          #   cache, distributed locks
        └── kafka/          #   event publisher / consumer
```

**The dependency rule:** dependencies point *inward*. `adapters → application → domain`.
Domain never imports application; application never imports adapters — it depends on the
**port traits** it defines, and `main.rs` injects concrete adapters. This is our
dependency-injection strategy: constructor injection at the composition root, traits as
seams. It makes the domain unit-testable with in-memory fakes and makes infra swappable.

---

## 5. Communication flows

### 5.1 Protocols by edge

| Hop | Protocol | Why |
|---|---|---|
| client → gateway | HTTPS (REST) + WSS | universal client support, OpenAPI-documented |
| gateway → service | **gRPC (tonic)** | typed contracts, streaming, low overhead, HTTP/2 multiplexing |
| service ↔ service (async) | **Redpanda** (Kafka API) | durable, replayable, decoupled event backbone |
| service → cache/lock | Redis (RESP) | sub-ms |

See [ADR-0011](./adr/0011-grpc-internal-rest-edge.md). The `proto` crate is the single
source of truth for internal contracts; the gateway maps REST/OpenAPI ⇄ gRPC.

### 5.2 Synchronous read path (query)

```
client ──GET /accounts/{id}/balance──▶ gateway
  gateway: verify JWT (JWKS) → rate-limit → circuit-breaker(ledger) → timeout
  gateway ──gRPC GetBalance──▶ ledger
    ledger: query handler → ReadModel (Postgres projection, or Redis cache)
  ◀── AccountBalanceView ──
◀── 200 JSON ──
```

Reads hit **projections/read models**, never the event store. Hot balances are cached in
Redis with write-through invalidation on projection update.

### 5.3 Asynchronous write path (money movement) — the core flow

A transfer is **not** a single DB transaction across two accounts. It is a **saga** with
reserve → capture semantics and compensation. This is the heart of the design; the full
state machine is in [`DOMAIN.md §5`](./DOMAIN.md#5-the-transfer-saga). Summary:

```
client ──POST /transfers  (Idempotency-Key: k)──▶ gateway ──gRPC──▶ ledger
  ledger: idempotency check (k) → append TransferRequested to event store
          → outbox relay publishes to Redpanda topic `ledger.transfer.v1`
  worker (saga orchestrator) consumes TransferRequested:
     1. Reserve on source account   (Account aggregate: append FundsReserved, opt. concurrency)
     2. Credit destination account  (append FundsCredited)
     3. Capture on source account   (append FundsCaptured)   → TransferCompleted
     on failure at any step → compensate (ReservationReleased) → TransferFailed
  each domain event → outbox → Redpanda → projection builders update read models
  notification service consumes events → pushes over WSS to the two account owners
```

**Why async + saga instead of a 2-account DB transaction?** Because the Account is the
consistency boundary for the no-double-spend invariant, and you cannot atomically mutate
two event-sourced aggregates. Modelling the transfer as a saga makes the failure modes
explicit and gives us compensation, retries, and DLQ for free. See
[ADR-0005](./adr/0005-transfer-saga.md).

### 5.4 Reliable publishing — transactional outbox

Services never dual-write to DB *and* Kafka. They write events (and an outbox marker) in
**one** local transaction; a **relay** streams committed events to Redpanda by monotonic
`global_seq`, tracking its published offset. Consumers are **idempotent** (dedup on
`event_id`). Poison messages route to a **DLQ** topic after bounded retries. See
[ADR-0006](./adr/0006-transactional-outbox.md).

---

## 6. Cross-cutting concerns

### 6.1 Resilience (the `resilience` crate — composable Tower layers)

Applied as a Tower middleware stack at the gateway and on every outbound gRPC client:

| Pattern | Implementation | Where |
|---|---|---|
| **Timeout** | `tower::timeout` per route/upstream | gateway + clients |
| **Retry + backoff** | jittered exponential backoff; **idempotent ops only** | outbound clients, consumers |
| **Circuit breaker** | custom `Closed→Open→HalfOpen`, rolling failure-ratio window | per upstream |
| **Bulkhead** | `tower::limit::ConcurrencyLimit` per upstream | gateway |
| **Load shedding** | `tower::load_shed` + queue depth check | gateway |
| **Rate limiting** | Redis token-bucket, keyed by subject/IP | gateway |
| **Backpressure** | bounded channels + consumer lag monitoring | workers |
| **Dead-letter queue** | per-topic `.dlq` after N retries, with replay tooling | consumers |

Backoff policy is centralized and configurable (base, max, multiplier, jitter, cap).
Retries carry the original correlation id so a retried request is traceable end-to-end.

### 6.2 Observability (the `telemetry` crate)

**One bootstrap function** each service calls in `main.rs`. Three pillars, correlated:

- **Tracing** — `tracing` + `tracing-opentelemetry` → OTLP → OTel Collector → **Jaeger**.
  Trace context (`traceparent`) is propagated across gRPC (tonic interceptors) **and**
  Redpanda (message headers), so one transfer is a single distributed trace spanning
  gateway → ledger → worker → notification.
- **Metrics** — RED (Rate/Errors/Duration) per endpoint + domain metrics (transfers/sec,
  saga latency, DLQ depth, consumer lag) → **Prometheus** → **Grafana** dashboards.
- **Logs** — structured JSON via `tracing-subscriber`, every line carries `request_id`,
  `correlation_id`, `trace_id`, `span_id`, `subject`. No secrets, no PII in logs.

**Correlation & request IDs:** the gateway mints a `request_id` per inbound request and a
`correlation_id` that follows the whole causal chain (propagated through gRPC metadata and
Kafka headers). Both appear in every log line and every span.

**Audit log** is separate from operational logging: a dedicated append-only Postgres table
+ `audit.v1` Redpanda topic. Every state-changing command emits an audit record —
`{actor, action, resource, before, after, correlation_id, occurred_at}` — immutable and
independently queryable for compliance.

### 6.3 Security

- **Transport:** TLS at the edge; gRPC internally with optional mTLS between services.
- **AuthN:** Argon2id password hashing; **JWT RS256** access tokens (~15 min) verified via
  **JWKS**; **rotating refresh tokens** with **reuse detection** (token families) — a
  replayed refresh token invalidates the whole family. OAuth2 authorization-code + PKCE for
  one external provider. See [ADR-0009](./adr/0009-auth-strategy.md).
- **AuthZ:** **RBAC** — roles → permissions, enforced as a Tower layer at the gateway and
  re-checked in each service's application layer. Least-privilege DB roles per service.
- **Input validation:** `validator` + typed newtypes at the boundary; reject-by-default.
- **Injection:** SQLx **compile-time-checked** queries; no string-built SQL.
- **Secrets:** never in code or images. Env-injected locally; **k8s Secrets** (+ SOPS/
  sealed-secrets for git-stored encrypted values) in production. See [ADR-0009].
- **Money safety:** `Money` is integer minor units — **never floating point**. See
  [ADR-0010](./adr/0010-money-representation.md).

### 6.4 Configuration (the `config` crate)

Layered, typed, fail-fast. Precedence (low→high): built-in defaults → `config/{env}.toml`
→ environment variables (`APP__LEDGER__DB__URL`) → mounted secrets. Parsed into a strongly
typed `Config` struct and **validated at startup**; a malformed config aborts boot rather
than failing at first request. Twelve-factor: config lives in the environment, images are
immutable across environments.

### 6.5 Lifecycle — health, readiness, graceful shutdown

- **Liveness** (`/health/live`) — process is up.
- **Readiness** (`/health/ready`) — dependencies (DB, Redis, broker) reachable *and*
  migrations applied; controls k8s traffic admission and rolling updates.
- **Startup probe** — guards slow first-boot (migrations, cache warm).
- **Graceful shutdown** — on `SIGTERM`: stop accepting new work → drain in-flight requests
  and consumer batches (commit offsets) → close pools → exit. Bounded by a deadline so a
  stuck drain can't block a rollout forever.

---

## 7. Data architecture

- **One PostgreSQL database per context** — no shared schema, no cross-context foreign keys.
  Contexts integrate through events, not through each other's tables. This is what makes the
  boundaries real. See [ADR-0008](./adr/0008-database-per-context.md).
- **Ledger** uses an append-only `events` table (the source of truth) + `snapshots` +
  projection tables. Optimistic concurrency via `UNIQUE(stream_id, version)`.
- **Redis** for: read-through caches (hot balances), distributed locks (per-account command
  serialization), token-bucket rate limits, WS presence, and idempotency short-cache.
- **Redpanda** topics are versioned (`ledger.transfer.v1`), partitioned by aggregate id
  (ordering per account), with compaction where appropriate and `.dlq` companions.

Full schema, event catalog, and aggregate design are in [`DOMAIN.md`](./DOMAIN.md).

---

## 8. Deployment & scaling

**Local:** `deploy/docker-compose.yml` brings up the full stack — all services + Postgres
instances + Redis + Redpanda + OTel Collector + Jaeger + Prometheus + Grafana — with one
command, so the whole system is reproducible on a laptop.

**Production (Kubernetes):** manifests in `deploy/k8s/`:

- one `Deployment` per stateless service, behind a `Service`, fronted by an `Ingress`;
- **HPA** on CPU + custom metrics (e.g. consumer lag, RPS);
- `readiness`/`liveness`/`startup` probes wired to §6.5 endpoints;
- `PodDisruptionBudget` + `topologySpreadConstraints` for HA across nodes/zones;
- resource `requests`/`limits` per service; `NetworkPolicy` so only the gateway is
  reachable from ingress;
- **migrations as init containers / Jobs**, not at app boot, to keep rollouts safe;
- `ConfigMap` for non-secret config, `Secret` for credentials;
- Postgres/Redpanda as `StatefulSet`s locally; **managed services assumed in real prod**.

**Horizontal scaling model:** gateway and stateless services scale on request load;
workers scale on Redpanda partition count (consumer-group parallelism); the ledger write
side scales by partitioning event streams per account; read side scales by adding
projection replicas and cache. HA comes from N≥2 replicas per service, multi-AZ spread,
and the resilience patterns in §6.1. See [ROADMAP Phase 6](./ROADMAP.md).

---

## 9. Testing strategy (summary — full plan in ROADMAP)

| Level | Tooling | Target |
|---|---|---|
| Unit | std `#[test]` | pure domain logic, in-memory port fakes |
| Property | `proptest` | ledger invariants (conservation, balance ≥ reserved) |
| Integration | `testcontainers` | real Postgres/Redis/Redpanda per test |
| Contract | proto round-trip | gRPC schema compatibility |
| Fuzz | `cargo-fuzz` | API deserialization, money arithmetic, parsers |
| Load / stress | `goose` (Rust) or k6 | throughput, saturation, breaking point |
| Benchmark | `criterion` | hot paths: event replay, projection apply |

CI (GitHub Actions): `fmt` → `clippy -D warnings` → `sqlx prepare` check → unit + property
→ integration (services in CI) → `cargo-audit`/`cargo-deny` → coverage (`llvm-cov`) →
build & scan images. Merges blocked on all green.

---

## 10. Explicit non-goals (scope discipline)

To keep the system coherent rather than a checklist, we deliberately **do not**:

- use Event Sourcing outside the Ledger (Identity is plain CRUD+outbox — see [ADR-0003]);
- implement multiple OAuth2 providers (one proves the pattern);
- build a UI (this is a backend portfolio);
- self-host production Postgres/Redpanda (managed in real prod; StatefulSets only for local);
- support multi-currency FX conversion in v1 (single currency per account; FX is a future
  bounded context).

Restraint is a design decision. Each omission has a reason recorded, not an oversight.

---

*Next: read [`DOMAIN.md`](./DOMAIN.md) for the ledger model, then [`ROADMAP.md`](./ROADMAP.md)
for the phased build plan and acceptance criteria.*
