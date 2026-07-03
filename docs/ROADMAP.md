# Implementation Roadmap

Delivery is **phased**; each phase produces something runnable, demoable, and independently
defensible in a review. No phase depends on a later one existing. Acceptance criteria are the
"definition of done" — a phase isn't complete until they pass in CI.

Legend: 🎯 = the primary skill each phase demonstrates.

---

## Phase 0 — Foundations 🎯 cloud-native scaffolding & project hygiene

Stand up the skeleton everything else plugs into.

- Cargo **workspace** with the crate layout from [ARCHITECTURE §4](./ARCHITECTURE.md#4-code-organization--modular-monorepo-cargo-workspace); shared lints (`clippy` pedantic), release profiles.
- `libs/kernel` — `Money`, `Currency`, id newtypes, error/`Result` types (with property tests for `Money` arithmetic).
- `libs/config` — layered typed config, validated at startup.
- `libs/telemetry` — one-call bootstrap: JSON logs + `tracing` + OTLP exporter.
- `deploy/docker-compose.yml` — Postgres ×N, Redis, Redpanda, OTel Collector, Jaeger, Prometheus, Grafana.
- Health/readiness/liveness + graceful shutdown wired into a trivial placeholder binary.
- GitHub Actions skeleton: `fmt`, `clippy -D warnings`, `test`, `cargo-deny`.

**Acceptance:** `docker compose up` brings the full infra stack healthy; `cargo test` green;
CI passes; a placeholder service reports ready and shuts down gracefully on SIGTERM.

---

## Phase 1 — Identity & Access 🎯 AuthN/Z, clean architecture, middleware

- Hexagonal `identity` service (domain/application/adapters).
- Registration; **Argon2id** password hashing; login.
- **JWT RS256** access tokens + **JWKS** endpoint; **rotating refresh tokens** with reuse
  detection (token families).
- **RBAC**: roles → permissions; permission checks in the application layer.
- **OAuth2** authorization-code + PKCE for one provider.
- Request validation (`validator` + newtypes); CRUD + transactional outbox emitting
  `UserRegistered`.

**Acceptance:** register → login → access protected route → refresh → detect reuse (family
revoked); OAuth2 round-trip works; integration tests on real Postgres via testcontainers;
RBAC denies unauthorized roles.

---

## Phase 2 — API Gateway 🎯 edge, resilience, Tower

- Axum gateway; REST ⇄ gRPC mapping to Identity; **OpenAPI/Swagger** generated.
- Tower stack: `correlation-id` → tracing span → metrics → **JWT verify (JWKS cache)** →
  **RBAC** → **rate-limit (Redis token bucket)** → concurrency limit / load-shed →
  **circuit breaker** → timeout → retry.
- `libs/resilience` crate implementing breaker + backoff + bulkhead as reusable layers.

**Acceptance:** authenticated requests route to Identity over gRPC; rate limit returns 429
past threshold; breaker opens on simulated upstream failure and recovers via half-open;
every response carries `x-request-id`; OpenAPI served and valid.

---

## Phase 3 — Ledger core 🎯 DDD · Event Sourcing · CQRS (the centerpiece)

- `ledger` service, hexagonal.
- Event store (`events`/`snapshots`) with optimistic concurrency; `Account` aggregate with
  the command→event model from [DOMAIN §2](./DOMAIN.md).
- Command handlers (write) + query handlers (read) — **CQRS** split.
- **Idempotency-Key** handling; per-account **Redis distributed lock**.
- Projections built **in-process** first (synchronous) to prove correctness before going async.

**Acceptance:** open account, reserve/capture/credit via gRPC; concurrency test proves
**no double-spend**; optimistic-concurrency conflict retries correctly; balance query served
from projection; idempotent replay of a command produces no duplicate effect.

---

## Phase 4 — Event backbone & distributed processing 🎯 event-driven, sagas, DLQ

- **Transactional outbox relay**: tails `global_seq`, publishes to Redpanda, tracks offset.
- Move projections to **async consumers** (idempotent, dedup on `event_id`).
- **Transfer saga** in `worker`: reserve → credit → capture with compensation; durable,
  resumable; retries with backoff; **DLQ** + replay tool.
- **Scheduler**: reservation-expiry sweep, daily statement generation (`tokio-cron-scheduler`).

**Acceptance:** end-to-end transfer completes across services as a **single distributed
trace**; killing the worker mid-saga and restarting resumes it correctly; poison event lands
in `.dlq` after bounded retries and is replayable; forced compensation returns net-zero.

---

## Phase 5 — Realtime & Notifications 🎯 WebSockets, presence, fan-out

- `notification` service: WSS termination (auth on upgrade), presence in Redis.
- Consumes ledger events; **fans out** transfer/balance updates to the two account owners.
- Horizontal-scale routing: user→node map in Redis (or Redis pub/sub) so the right node
  delivers to a connected client. Trade-off documented in ADR-form.

**Acceptance:** a transfer pushes live updates to both parties over WSS; presence reflects
connect/disconnect; a message reaches a client connected to a different replica than the
one that consumed the event.

---

## Phase 6 — Observability & Ops 🎯 observability, k8s, HA/scaling

- Full OTel: trace context across gRPC **and** Kafka headers; Jaeger shows one trace per
  transfer; Grafana dashboards (RED + domain metrics: transfers/sec, saga latency, consumer
  lag, DLQ depth); **audit log** stream + table.
- `deploy/k8s/`: Deployments, Services, Ingress, **HPA** (CPU + custom metrics), probes,
  `PodDisruptionBudget`, `topologySpreadConstraints`, resource limits, `NetworkPolicy`,
  ConfigMap/Secret, **migrations as init/Job**.
- Hardened CI/CD: image build + scan, `sqlx prepare` check, coverage gate.

**Acceptance:** one distributed trace spans gateway→ledger→worker→notification in Jaeger;
Grafana dashboards populated; `kubectl apply -k deploy/k8s` stands the system up; HPA scales
a service under synthetic load; graceful rollout with zero dropped requests.

---

## Phase 7 — Testing, quality & hardening 🎯 testing rigor

- **Property tests** (`proptest`): conservation & `available ≥ 0` over random concurrent
  transfer interleavings.
- **Fuzz** (`cargo-fuzz`): API deserialization, `Money` arithmetic, event upcasters.
- **Load/stress** (`goose`): throughput curve, saturation point, latency percentiles under
  load; documented breaking point.
- **Benchmarks** (`criterion`): event replay, projection apply, snapshot restore.
- Coverage report; `cargo-audit`/`cargo-deny` clean; threat-model note.

**Acceptance:** property tests find no invariant violation across N random schedules; fuzz
runs clean for a fixed budget; load report with p50/p95/p99 + max sustained TPS committed to
`docs/`; benches tracked to catch regressions.

---

## Sequencing notes

- Phases 0→4 are the critical path and tell the strongest story on their own (a correct,
  observable, event-sourced ledger with sagas). 5–7 are additive polish.
- Each phase merges behind green CI before the next starts; `main` is always deployable.
- Estimated shape (solo, focused): 0–1 small, 3–4 the largest, 5–7 medium. Exact calendar
  time intentionally omitted — depth over deadlines.

*Decisions behind these phases are recorded in [`adr/`](./adr).*
