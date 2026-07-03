# Operational Conventions

Single source of truth for cross-service naming, ports, env vars, and topics. Deploy
manifests, CI, and service code all follow this.

## Services

| Crate / binary | Role | HTTP | gRPC | Metrics | State |
|---|---|---|---|---|---|
| `gateway` | Public edge: REST + OpenAPI/Swagger, resilience, auth verify, routing | `8080` | — | `9100` | Redis (rate-limit) |
| `identity` | AuthN/Z (gRPC), CRUD + outbox | `8081` (health) | `50051` | `9101` | `identity_db`, Redis |
| `ledger` | Event-sourced ledger (gRPC), CQRS | `8082` (health) | `50052` | `9102` | `ledger_db`, Redis |
| `notification` | WebSocket fan-out (consumes events) | `8083` (WS + health) | — | `9103` | Redis (presence) |
| `worker` | Saga orchestrator, projections, scheduler, DLQ | `8084` (health) | — | `9104` | `worker_db`, Redis |

## Health & metrics endpoints (every service)

- `GET /health/live` — liveness (process up)
- `GET /health/ready` — readiness (deps reachable + migrations applied)
- `GET /health/startup` — startup gate
- `GET /metrics` — Prometheus exposition (on the metrics port)

## Configuration (figment: defaults → `config/{RUN_ENV}.toml` → env)

Env prefix `APP`, nested separator `__`:

```
APP__SERVER__HTTP_ADDR=0.0.0.0:8080
APP__SERVER__GRPC_ADDR=0.0.0.0:50051
APP__SERVER__METRICS_ADDR=0.0.0.0:9100
APP__DATABASE__URL=postgres://app:app@postgres:5432/ledger_db
APP__DATABASE__MAX_CONNECTIONS=20
APP__REDIS__URL=redis://redis:6379
APP__KAFKA__BROKERS=redpanda:9092
APP__OTEL__ENDPOINT=http://otel-collector:4317
APP__OTEL__SERVICE_NAME=ledger
APP__JWT__ISSUER=https://identity.local
APP__JWT__ACCESS_TTL_SECONDS=900
APP__JWT__REFRESH_TTL_SECONDS=2592000
APP__LOG__LEVEL=info
APP__LOG__FORMAT=json
RUN_ENV=production            # selects config/{RUN_ENV}.toml
```

## Databases (one per context — ADR-0008)

`identity_db`, `ledger_db`, `worker_db`. Notification is stateless (Redis only).
Migrations live under `migrations/<db>/` and run as an **init container / Job**, never at
app boot.

## Kafka / Redpanda topics (versioned; each has a `.dlq` companion)

| Topic | Producer | Key | Consumers |
|---|---|---|---|
| `ledger.account.v1` | ledger | `account_id` | worker (projections), notification |
| `ledger.transfer.v1` | ledger | `transfer_id` | worker (saga), notification |
| `identity.user.v1` | identity | `user_id` | ledger (auto-open wallet) |
| `audit.v1` | all | `resource_id` | worker (audit sink) |

Partitioned by key for per-aggregate ordering. Consumers are idempotent (dedup on `event_id`).

## Container / image conventions

- Multi-stage build with `cargo-chef` dependency caching; runtime on `debian:bookworm-slim`
  (non-root user). One image per service; binary name = crate name.
- Image tag = git SHA; `latest` on main.
- `SIGTERM` triggers graceful shutdown; k8s `terminationGracePeriodSeconds: 30`.
