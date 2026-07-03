// k6 smoke test — 1 VU, a few iterations. Verifies the stack is wired end-to-end before a
// real load run.  BASE_URL=http://localhost:8080 k6 run tests/load/smoke.js
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  vus: 1,
  iterations: 5,
  thresholds: { http_req_failed: ['rate<0.01'] },
};

export default function () {
  const live = http.get(`${BASE}/health/live`);
  check(live, { 'gateway live': (r) => r.status === 200 });

  const ready = http.get(`${BASE}/health/ready`);
  check(ready, { 'gateway ready': (r) => r.status === 200 });
}
