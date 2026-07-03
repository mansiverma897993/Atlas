# Kubernetes base manifests

Kustomize base for the Ledger Platform. Apply with:

```bash
kubectl apply -k deploy/k8s/base                 # base (dev-ish defaults)
kubectl apply -k deploy/k8s/overlays/production  # production overlay
```

## What's here

| File | Contents |
|---|---|
| `namespace.yaml` | `ledger` namespace with restricted Pod Security Standard |
| `configmap.yaml` | Non-secret config (`APP__*`), infra endpoints |
| `secret.example.yaml` | **Documented placeholders only** — never real secrets |
| `migrate-job.yaml` | Runs SQL migrations before rollout (init-container analog) |
| `gateway.yaml` … `worker.yaml` | Per service: Deployment + Service + PDB + HPA |
| `ingress.yaml` | `/api`+`/swagger`→gateway, `/ws`→notification |
| `networkpolicy.yaml` | Only gateway+notification reachable from ingress |
| `kustomization.yaml` | Ties it together |

Each service Deployment has: 2 replicas, resource requests/limits, non-root
`securityContext`, liveness/readiness/startup probes wired to
`/health/live` · `/health/ready` · `/health/startup`, env from ConfigMap+Secret,
and `topologySpreadConstraints` for zone/node HA.

## Stateful dependencies — NOT managed here

Per [ADR-0008] and ARCHITECTURE.md §8/§10, **Postgres, Redis, and Redpanda are
managed services in real production** (RDS/Cloud SQL, ElastiCache/MemoryDB,
Redpanda Cloud/MSK). We deliberately do **not** ship StatefulSets in this base.
Locally they run as containers in `deploy/docker-compose.yml`.

### Pointing at managed services

The manifests reference plain hostnames (`postgres`, `redis`, `redpanda`). To use
managed endpoints:

1. **Non-secret hosts** — override in the overlay's `configMapGenerator` (the
   production overlay already redirects `APP__REDIS__URL` and
   `APP__KAFKA__BROKERS` to `*.internal`).
2. **Database URLs (contain credentials)** — set the `APP__DATABASE__URL__{identity,
   ledger,worker}` keys in the real `ledger-secrets` Secret (SOPS/sealed-secrets),
   pointing at your managed Postgres host, e.g.
   `postgres://app:***@prod-pg.internal:5432/ledger_db`.
3. **Egress** — add explicit egress rules in `networkpolicy.yaml` for the managed
   services' CIDRs/ports (the default policy only allows same-namespace egress + DNS).
4. Optionally create a `Service` of `type: ExternalName` per dependency so the
   in-cluster DNS name keeps working unchanged.

## Migrations

`migrate-job.yaml` applies `migrations/{identity,ledger,worker}` before services
serve traffic. Gate your rollout on it:

```bash
kubectl apply -k deploy/k8s/overlays/production
kubectl -n ledger wait --for=condition=complete job/ledger-migrate --timeout=300s
```

With Argo CD it runs automatically as a `PreSync` hook. Readiness probes also
report "not ready" until migrations are applied, so services never take traffic
against an un-migrated schema.
