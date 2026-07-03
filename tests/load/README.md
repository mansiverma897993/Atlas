# Load & stress tests (k6)

[k6](https://k6.io) scripts that drive the **gateway** REST API end-to-end (auth → open
accounts → transfer → poll status). They exercise the whole system under concurrency, so they
double as smoke tests for a running stack.

## Run

```bash
# against a local docker-compose stack (gateway on :8080)
BASE_URL=http://localhost:8080 k6 run tests/load/transfers.js      # steady load
BASE_URL=http://localhost:8080 k6 run tests/load/smoke.js          # 1-VU sanity
BASE_URL=http://localhost:8080 k6 run tests/load/stress.js         # ramp to breaking point
```

`just loadtest` / `make loadtest` wrap these.

## Thresholds

Each script sets pass/fail SLOs (`http_req_duration` p95, `http_req_failed` rate). A failed
threshold exits non-zero, so these gate in CI when a stack is available.

## What "breaking point" means

`stress.js` ramps virtual users until error rate or latency SLOs break; the last healthy
arrival rate is the sustainable throughput. Record the p50/p95/p99 and max sustained TPS in
`docs/` (ROADMAP Phase 7 acceptance criterion).
