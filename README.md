# Ledger Platform — a production-grade, event-sourced payments backend in Rust

> An enterprise-grade, event-sourced **double-entry ledger / payments** backend, built to
> demonstrate senior backend & distributed-systems engineering: DDD, hexagonal architecture,
> Event Sourcing, CQRS, sagas, an event-driven backbone, resilience patterns, and
> cloud-native operations.
>
> **Status: implemented.** A Cargo workspace of 5 service binaries + 6 shared libraries. The
> whole workspace compiles, `cargo clippy` is clean, `cargo fmt --check` passes, and **122
> tests pass** — including an in-process end-to-end transfer test and a conservation property
> test over randomized schedules. Local run and cloud deploy are wired
> (`deploy/docker-compose.yml`, `deploy/k8s/`). See [roadmap](./docs/ROADMAP.md) for phases.
>
> The full authentication chain runs out of the box in production: identity signs with a stable
> RSA key (`APP__JWT__PRIVATE_KEY_PEM`), the gateway auto-fetches and refreshes its JWKS, the
> public auth surface is rate-limited and bounded, security headers are emitted at the edge, and
> `RUN_ENV=production` fails fast on insecure defaults. Everything a human must supply — DB URLs,
> keys, tokens — is in **[docs/PRODUCTION_SETUP.md](./docs/PRODUCTION_SETUP.md)** (and the
> printable **[Manual-Setup-Guide.pdf](./Manual-Setup-Guide.pdf)**).

## Why a ledger?

Because a payments ledger is the domain where the "senior" patterns are **load-bearing, not
decorative**. A ledger that loses, duplicates, or reorders a posting is broken — that
constraint is what forces Event Sourcing, CQRS, sagas, idempotency, distributed locking, and
dead-letter queues to be real rather than checklist items. The system exists to protect four
invariants: **conservation** (`Σ debits = Σ credits`), **no double-spend**, **idempotent
retries**, and **immutable audit**.

## The 60-second tour

```
 clients ─HTTPS/WSS─▶ [ API GATEWAY ] ─gRPC─▶ [ IDENTITY ] [ LEDGER (ES+CQRS) ] [ NOTIFICATION ]
                            resilience              │            │                    │
                            + auth + rate-limit     └── Redpanda event backbone ──────┘
                                                              │
                                              [ WORKER: saga · projections · scheduler ]
   data: PostgreSQL (db per context) · Redis (cache/locks/rate-limit)
   observability: OpenTelemetry → Jaeger · Prometheus · Grafana
```

A transfer flows as a **saga**: reserve on source → credit destination → capture on source,
with compensation on failure — coordinated over the event backbone, observable as a single
distributed trace, delivered live to both parties over WebSocket.

## Build, test, run

```bash
# build & verify everything (no infra needed — pure-Rust deps, in-memory test adapters)
cargo build --workspace
cargo test  --workspace          # 119 tests
cargo clippy --workspace
cargo bench -p ledger            # event-replay benchmark (criterion)

# run the full stack locally (services + Postgres + Redis + Redpanda + Jaeger + Prom + Grafana)
docker compose -f deploy/docker-compose.yml up --build

# load / smoke / stress tests against the running gateway
BASE_URL=http://localhost:8080 k6 run tests/load/smoke.js
```

Toolchain note: builds with the stock Rust toolchain — no system `protoc` (vendored), no C
compiler or `librdkafka` (pure-Rust `rskafka`), no native TLS (`rustls`). `deploy/` needs
Docker; `tests/load/` needs [k6].

## Workspace layout

| Crate | Kind | Role |
|---|---|---|
| `crates/libs/kernel` | lib | `Money`, `Currency`, typed ids, correlation ids (pure domain kernel) |
| `crates/libs/config` | lib | layered, typed, fail-fast configuration |
| `crates/libs/telemetry` | lib | tracing + metrics + (feature-gated) OTLP bootstrap |
| `crates/libs/infra` | lib | Postgres, Redis (lock/rate-limit), event bus, outbox, health |
| `crates/libs/resilience` | lib | circuit breaker, retry, exponential backoff |
| `crates/libs/proto` | lib | generated gRPC contracts (tonic) |
| `crates/gateway` | bin | public REST/OpenAPI edge, JWT verify, rate limit, breaker, gRPC routing |
| `crates/identity` | bin | AuthN/Z (gRPC): Argon2id, JWT/JWKS, rotating refresh, RBAC, OAuth2 |
| `crates/ledger` | bin | event-sourced double-entry ledger (gRPC, ES + CQRS + saga) |
| `crates/notification` | bin | WebSocket fan-out hub, presence |
| `crates/worker` | bin | scheduler, audit sink, cross-context provisioning, DLQ monitor |

## Documentation

| Doc | What it covers |
|---|---|
| **[docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)** | The authoritative system design: contexts, communication, resilience, observability, security, config, deployment. |
| **[docs/DOMAIN.md](./docs/DOMAIN.md)** | The ledger DDD model: aggregates, value objects, events, commands, invariants, the transfer saga, read models. |
| **[docs/ROADMAP.md](./docs/ROADMAP.md)** | Phased delivery plan (0–7) with acceptance criteria per phase. |
| **[docs/PRODUCTION_SETUP.md](./docs/PRODUCTION_SETUP.md)** | Everything you must supply by hand — DB URLs, JWT keys, tokens, hostnames — to run locally and in prod. |
| **[docs/adr/](./docs/adr)** | Architecture Decision Records — the *why* behind every major choice. |

## Technology

**Language/runtime:** Rust · Tokio · Axum · Tower · tonic (gRPC) · SQLx ·
**Data:** PostgreSQL · Redis · Redpanda (Kafka API) ·
**Observability:** OpenTelemetry · Jaeger · Prometheus · Grafana ·
**Ops:** Docker Compose · Kubernetes · GitHub Actions ·
**Quality:** proptest · cargo-fuzz · criterion · goose · testcontainers.

## Engineering principles applied here

- **Domain-Driven Design** with explicit bounded contexts and a ubiquitous language.
- **Hexagonal architecture** — pure domain, ports as traits, adapters at the edge, DI at the
  composition root ([ADR-0002](./docs/adr/0002-hexagonal-architecture.md)).
- **Event Sourcing + CQRS**, scoped deliberately to where they pay for themselves
  ([ADR-0003](./docs/adr/0003-event-sourcing-scope.md), [ADR-0004](./docs/adr/0004-cqrs.md)).
- **Resilience by construction** — timeouts, retries with backoff, circuit breakers,
  bulkheads, rate limits, DLQ.
- **Observability by construction** — one distributed trace per transfer across sync and async
  hops ([ADR-0012](./docs/adr/0012-observability-otel.md)).
- **Scope discipline** — every non-goal is a recorded decision, not an oversight
  ([ARCHITECTURE §10](./docs/ARCHITECTURE.md#10-explicit-non-goals-scope-discipline)).

## License

Atlas is licensed under the **Apache License 2.0**. See [LICENSE](./LICENSE) for the full
text.

Copyright (c) 2026 Mansi Verma.
