# Deployment & Observability

All deployment artifacts for the Ledger Platform. Naming, ports, env vars,
databases, and topics follow [`docs/CONVENTIONS.md`](../docs/CONVENTIONS.md);
the system design is in [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) §7–§8.

```
deploy/
├── docker-compose.yml              # full local stack (one command)
├── postgres/init-databases.sql     # creates identity_db/ledger_db/worker_db + app role
├── docker/
│   ├── Dockerfile                  # ONE parameterized multi-stage build (ARG SERVICE)
│   └── .dockerignore
├── observability/
│   ├── otel-collector.yaml         # OTLP in → Jaeger (traces) + Prometheus (metrics)
│   ├── prometheus.yml              # scrape all services + collector + redpanda
│   └── grafana/
│       ├── provisioning/…          # datasources + dashboard provider
│       └── dashboards/ledger-overview.json
└── k8s/
    ├── base/                       # kustomize base (Deployments, Services, HPA, …)
    └── overlays/production/        # prod replicas/resources/images
```

## Run locally (Docker Compose)

```bash
# From the repo root. Builds all 5 service images from deploy/docker/Dockerfile.
docker compose -f deploy/docker-compose.yml up --build

# Optional: include the Redpanda Console UI
docker compose -f deploy/docker-compose.yml --profile tools up --build
```

Startup order is enforced with `depends_on` conditions:
`postgres` healthy → **`migrate`** runs & completes → services start.

### UIs / endpoints

| URL | What |
|---|---|
| http://localhost:8080/swagger | Gateway REST + Swagger |
| ws://localhost:8083/ws | Notification WebSocket |
| http://localhost:16686 | Jaeger (traces) |
| http://localhost:9090 | Prometheus |
| http://localhost:3000 | Grafana (admin/admin) → "Ledger Platform" folder |
| http://localhost:8090 | Redpanda Console (with `--profile tools`) |

## Migration flow

Migrations **never run at app boot** (CONVENTIONS §Databases). They live in
`migrations/{identity,ledger,worker}/` and are applied by a dedicated step:

- **Compose:** the one-shot `migrate` service runs `sqlx migrate run` per database,
  then exits; every app service `depends_on` it with
  `condition: service_completed_successfully`.
- **Kubernetes:** the `ledger-migrate` Job (`k8s/base/migrate-job.yaml`) runs before
  rollout (Argo CD `PreSync` hook, or `kubectl wait --for=condition=complete`).
- Belt-and-braces: each service's `/health/ready` reports not-ready until migrations
  are applied, so traffic is never admitted against an un-migrated schema.

## Deploy to Kubernetes

```bash
# 1. Provide real secrets (do NOT apply secret.example.yaml as-is).
#    Manage `ledger-secrets` via SOPS / sealed-secrets / external-secrets.

# 2. Apply the production overlay.
kubectl apply -k deploy/k8s/overlays/production

# 3. Wait for migrations, then the rollout proceeds.
kubectl -n ledger wait --for=condition=complete job/ledger-migrate --timeout=300s
kubectl -n ledger rollout status deploy/gateway
```

Postgres/Redis/Redpanda are **managed services** in prod — see
[`k8s/base/README.md`](./k8s/base/README.md) for how to point at them.

## Port map (single source of truth — matches CONVENTIONS.md)

| Service | HTTP / WS / health | gRPC | Metrics | Database |
|---|---|---|---|---|
| gateway | 8080 (REST+Swagger) | — | 9100 | — (Redis) |
| identity | 8081 (health) | 50051 | 9101 | identity_db |
| ledger | 8082 (health) | 50052 | 9102 | ledger_db |
| notification | 8083 (WS+health) | — | 9103 | — (Redis) |
| worker | 8084 (health) | — | 9104 | worker_db |

| Infra | Port(s) |
|---|---|
| PostgreSQL | 5432 |
| Redis | 6379 |
| Redpanda (Kafka API) | 9092 (in-net) / 19092 (host) · admin/metrics 9644 |
| OTel Collector | 4317 (OTLP gRPC) · 4318 (OTLP HTTP) · 8888 (self) · 8889 (prom export) |
| Jaeger | 16686 (UI) |
| Prometheus | 9090 |
| Grafana | 3000 |
| Redpanda Console | 8090 |

Health endpoints on every service: `/health/live`, `/health/ready`,
`/health/startup`; Prometheus exposition at `/metrics` on the metrics port.

## Kafka / Redpanda topics

`ledger.account.v1`, `ledger.transfer.v1`, `identity.user.v1`, `audit.v1` —
each versioned and partitioned by aggregate key, each with a `.dlq` companion
(CONVENTIONS §Kafka/Redpanda topics).

## Observability wiring

Services push OTLP → `otel-collector:4317` (`APP__OTEL__ENDPOINT`). The collector
fans out traces to Jaeger and exposes metrics for Prometheus on `:8889`. Prometheus
also scrapes each service `/metrics` directly and Redpanda's `/public_metrics`.
Grafana auto-provisions both datasources and the **Ledger Platform — Overview**
dashboard (RED metrics + transfers/sec, saga latency, consumer lag, DLQ depth).
