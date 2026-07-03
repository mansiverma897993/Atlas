# Architecture Decision Records

Each ADR captures one significant decision: its **context**, the **decision**, and the
**consequences** (including what we gave up). They are immutable — a reversed decision gets a
new ADR that supersedes the old one, rather than an edit. This is the audit trail of *why*
the system looks the way it does.

| # | Decision | Status |
|---|---|---|
| [0001](./0001-modular-monorepo.md) | Modular-monorepo Cargo workspace over polyrepo microservices | Accepted |
| [0002](./0002-hexagonal-architecture.md) | Hexagonal (ports & adapters) architecture per service | Accepted |
| [0003](./0003-event-sourcing-scope.md) | Event Sourcing scoped to the Ledger only | Accepted |
| [0004](./0004-cqrs.md) | CQRS with separate read models on the write stream | Accepted |
| [0005](./0005-transfer-saga.md) | Transfers as sagas with reserve/capture + compensation | Accepted |
| [0006](./0006-transactional-outbox.md) | Transactional outbox + relay for reliable publishing; DLQ | Accepted |
| [0007](./0007-redpanda.md) | Redpanda as the event backbone | Accepted |
| [0008](./0008-database-per-context.md) | One PostgreSQL database per bounded context | Accepted |
| [0009](./0009-auth-strategy.md) | JWT RS256 + rotating refresh tokens + JWKS + RBAC | Accepted |
| [0010](./0010-money-representation.md) | Money as integer minor units (no floating point) | Accepted |
| [0011](./0011-grpc-internal-rest-edge.md) | gRPC internally, REST + WS at the edge | Accepted |
| [0012](./0012-observability-otel.md) | OpenTelemetry OTLP → Collector → Jaeger/Prometheus/Grafana | Accepted |
