# Production Setup & Secrets Guide

> The maintainable source for **`Manual-Setup-Guide.pdf`** (repo root). It lists everything a
> human must supply — databases, keys, credentials, URLs — to run the platform. All wiring,
> resilience, and observability already ship in the repo; this document is only the "fill in the
> blanks" list.

## How configuration works

Three layers, later wins: built-in defaults → `config/{RUN_ENV}.toml` → environment variables
prefixed `APP__` (nesting separator `__`, e.g. `APP__DATABASE__URL`). **Secrets are supplied only
via env / k8s Secret / a secret manager** — never committed. Set `RUN_ENV=local` for dev and
`RUN_ENV=production` in prod (production additionally **fails fast** if the insecure local
defaults — localhost DB, `app:app` creds, the `identity.local` issuer — were left in place).

Port map: gateway http `8080` / metrics `9100` · identity http `8081` / grpc `50051` / metrics
`9101` · ledger http `8082` / grpc `50052` / metrics `9102` · notification ws `8083` / metrics
`9103` · worker http `8084` / metrics `9104`.

## What you must provide

| # | Item | Env variable | Used by | Required? |
|---|------|--------------|---------|-----------|
| 1 | Postgres — identity DB URL | `APP__DATABASE__URL` | identity | **required** |
| 2 | Postgres — ledger DB URL | `APP__DATABASE__URL` | ledger | **required** |
| 3 | Postgres — worker DB URL | `APP__DATABASE__URL` | worker | **required** |
| 4 | Redis URL | `APP__REDIS__URL` | all | **required** |
| 5 | Kafka/Redpanda brokers | `APP__KAFKA__BROKERS` | identity, ledger, notification, worker | **required** |
| 6 | JWT RSA private key (PEM) | `APP__JWT__PRIVATE_KEY_PEM` | identity | **required in prod** |
| 7 | JWT public key delivery | `APP__JWT__JWKS_URL` *or* `APP__JWT__PUBLIC_KEY_PEM` | gateway | **required** |
| 8 | Worker admin token | `WORKER_ADMIN_TOKEN` | worker | **required in prod** |
| 9 | OAuth2 client id/secret | `APP__OAUTH__CLIENT_ID` / `…__CLIENT_SECRET` | identity | optional |
| 10 | OTEL OTLP endpoint | `APP__OTEL__ENDPOINT` | all | optional |
| 11 | Public hostname + TLS | ingress / `APP__JWT__ISSUER` | gateway, identity | **required in prod** |
| 12 | Container registry | CI secrets / image refs | build & deploy | optional |

## 1 · PostgreSQL (three databases)

One server, three logical databases (ADR-0008): `identity_db`, `ledger_db`, `worker_db`.
`notification` is stateless (no DB). The app connects as a least-privilege `app` role.

**Local:** nothing to do — `deploy/postgres/init-databases.sql` creates the role and databases on
first `just up` (creds `app:app`, local only).

**Production:** run once against your managed instance, with a strong password:

```sql
CREATE ROLE app WITH LOGIN PASSWORD '<STRONG_PASSWORD>'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE DATABASE identity_db OWNER app;
CREATE DATABASE ledger_db   OWNER app;
CREATE DATABASE worker_db   OWNER app;
-- per db: REVOKE ALL ON DATABASE <db> FROM PUBLIC; GRANT CONNECT ON DATABASE <db> TO app;
```

Then point each service at **its own** database (append `?sslmode=require` if your provider
mandates TLS):

```
identity  APP__DATABASE__URL=postgres://app:PASS@host:5432/identity_db
ledger    APP__DATABASE__URL=postgres://app:PASS@host:5432/ledger_db
worker    APP__DATABASE__URL=postgres://app:PASS@host:5432/worker_db
```

## 2 · Redis & 3 · Kafka/Redpanda

```
APP__REDIS__URL=redis://:PASS@host:6379        # rediss:// for TLS
APP__REDIS__PASSWORD=<secret>                  # if password-protected
APP__KAFKA__BROKERS=host-0:9092,host-1:9092
APP__KAFKA__CONSUMER_GROUP=ledger
```

## 4 · JWT signing keys (the auth chain)

Access tokens are RS256 JWTs. **identity signs** with a private key; **gateway verifies** with the
matching public key. In production this must be a **stable, shared** key.

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out jwt_private.pem
openssl rsa -in jwt_private.pem -pubout -out jwt_public.pem
```

- **identity** ← `APP__JWT__PRIVATE_KEY_PEM` (PKCS#8 or PKCS#1). *Enforced:* outside
  `RUN_ENV=local`, identity refuses to boot without it (no ephemeral per-pod keys).
- **gateway** ← pick one:
  - **A (recommended, in-cluster):** `APP__JWT__JWKS_URL=http://identity:8081/.well-known/jwks.json`
    — the gateway fetches and auto-refreshes the key (`APP__JWT__JWKS_REFRESH_SECONDS`, default
    `300`). Plaintext HTTP only (east-west path).
  - **B (out-of-mesh):** `APP__JWT__PUBLIC_KEY_PEM=<jwt_public.pem>` — overrides A.
- Both sides must agree on `APP__JWT__ISSUER` (a real hostname in prod, **not** `identity.local`)
  and `APP__JWT__AUDIENCE` (`ledger-platform`).

## 6 · Worker admin token

Guards `POST /admin/dlq/replay/:topic`. Worker refuses to boot outside `local` if unset or left as
`changeme`.

```bash
WORKER_ADMIN_TOKEN=$(openssl rand -hex 32)
WORKER_LEDGER_GRPC_ADDR=http://ledger:50052
```

## 7 · OAuth2 (optional)

Register an app with your provider, set the callback to
`https://identity.<domain>/oauth/callback`, and supply `APP__OAUTH__CLIENT_ID` /
`APP__OAUTH__CLIENT_SECRET`. The shipped provider adapter is a demo stub — implement a real
token-exchange + userinfo call before enabling OAuth in production.

## 8 · Hostname, TLS & ingress

Point DNS at your ingress, terminate TLS there (cert-manager/ACM/managed cert), set `host` and
`tls.secretName` in `deploy/k8s/base/ingress.yaml`, and make `APP__JWT__ISSUER` match the identity
hostname. The gateway serves plain HTTP behind the ingress and emits HSTS + security headers for
you.

## 9 · Container registry (CI/CD)

Only if you build & push images (`.github/workflows/docker.yml`). Provide registry credentials as
repo/org secrets and update image refs in `deploy/k8s/base/*.yaml` from `ledger/<svc>:latest` to
`<your-registry>/<svc>:<tag>`.

## 10 · Where the values go

**Local:** `cp .env.example .env`, fill it in, then `just up`.

**Production (k8s):** populate the `ledger-secrets` Secret (template:
`deploy/k8s/base/secret.example.yaml`) via SOPS / sealed-secrets / an external secrets operator —
never `kubectl apply` raw secrets. Then:

```bash
kubectl apply -f deploy/k8s/base/migrate-job.yaml        # apply schema first
kubectl apply -k deploy/k8s/overlays/production          # deploy the stack
```

## 11 · Production readiness checklist

- [ ] Managed Postgres reachable; `app` role + 3 databases with a strong password
- [ ] Each service's `APP__DATABASE__URL` points at its own database
- [ ] Redis reachable (password/TLS as needed)
- [ ] Kafka/Redpanda brokers reachable; DLQ topics exist
- [ ] `APP__JWT__PRIVATE_KEY_PEM` set on identity
- [ ] Gateway public-key delivery chosen (JWKS URL *or* static PEM) and verified end-to-end
- [ ] `APP__JWT__ISSUER` / `AUDIENCE` set to real values on both sides
- [ ] `WORKER_ADMIN_TOKEN` set to a strong random secret
- [ ] `RUN_ENV=production` set (enables fail-fast on insecure defaults)
- [ ] All secrets delivered via a secret manager, not committed
- [ ] DNS + TLS ingress configured; hostname matches the JWT issuer
- [ ] `migrate-job` applied before services serve traffic
- [ ] OTEL endpoint set (or tracing intentionally disabled)
- [ ] Smoke test passes: `BASE_URL=https://api.<domain> k6 run tests/load/smoke.js`
