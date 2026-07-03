// k6 stress test — ramp arrival rate until SLOs break, to find the sustainable throughput
// (ROADMAP Phase 7). Uses a constant-arrival-rate executor so we control offered load (RPS)
// independently of latency.  BASE_URL=http://localhost:8080 k6 run tests/load/stress.js
import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const errors = new Rate('errors');

export const options = {
  scenarios: {
    ramp_rps: {
      executor: 'ramping-arrival-rate',
      startRate: 50,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 500,
      stages: [
        { duration: '30s', target: 100 },
        { duration: '30s', target: 250 },
        { duration: '30s', target: 500 },
        { duration: '30s', target: 1000 }, // push toward the breaking point
      ],
    },
  },
  thresholds: {
    // When these break, the stage's arrival rate exceeds sustainable capacity.
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(99)<1000'],
  },
};

export default function () {
  // Hit a cheap authenticated-free read path to isolate gateway/routing capacity.
  const res = http.get(`${BASE}/health/ready`);
  const ok = check(res, { 'status 200': (r) => r.status === 200 });
  errors.add(!ok);
}
