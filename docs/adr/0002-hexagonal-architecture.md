# ADR-0002 — Hexagonal (ports & adapters) architecture per service

**Status:** Accepted

## Context
The domain logic (especially the ledger's invariants) is the valuable, long-lived part;
frameworks (Axum, SQLx, tonic, Redpanda clients) are replaceable infrastructure. If domain
code depends on framework types, it becomes untestable without infra and brittle to library
churn. We also need a clear dependency-injection strategy.

## Decision
Each service is layered **domain → application → adapters** with dependencies pointing
inward (the dependency rule):
- **domain** — pure aggregates, value objects, domain events, invariants. No I/O, no
  framework, no `async`.
- **application** — use-cases (command/query handlers) that orchestrate the domain and depend
  only on **port traits** (`EventStore`, `ReadModel`, `Clock`, `IdGen`, `UnitOfWork`, …) it
  defines.
- **adapters** — inbound (gRPC/HTTP) drive the application; outbound (Postgres/Redis/Kafka)
  implement the ports. Frameworks appear **only** here.

`main.rs` is the composition root: it builds concrete adapters and injects them into the
application (constructor injection). Traits are the seams; DI is explicit and compile-time.

## Consequences
- **+** Domain is unit-testable with in-memory fakes; no DB needed for business-rule tests.
- **+** Infra is swappable (e.g. Postgres → another store) without touching domain/application.
- **+** Every service reads the same way; learn one, navigate all.
- **−** More upfront structure and trait boilerplate than a controller-calls-SQL style. Justified
  by the correctness demands of the ledger and the portfolio's intent to show clean architecture.
