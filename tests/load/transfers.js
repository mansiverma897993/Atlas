// k6 steady-load test of the transfer flow through the gateway.
// Each iteration: register+login a user, open two accounts, (the worker auto-funds via a
// deposit endpoint if available, else we open+credit), initiate a transfer, poll its status.
//
//   BASE_URL=http://localhost:8080 k6 run tests/load/transfers.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

const transferLatency = new Trend('transfer_initiate_ms', true);
const transferErrors = new Rate('transfer_errors');

export const options = {
  scenarios: {
    steady: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '30s', target: 25 },
        { duration: '1m', target: 25 },
        { duration: '30s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],           // < 2% errors
    http_req_duration: ['p(95)<400'],         // p95 under 400ms
    transfer_initiate_ms: ['p(95)<300'],
  },
};

function authHeaders(token) {
  return { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` } };
}

export default function () {
  const email = `load-${uuidv4()}@example.com`;
  const password = 'S3cure-pass!';

  // register + login
  http.post(`${BASE}/api/auth/register`, JSON.stringify({ email, password, display_name: 'load' }), {
    headers: { 'Content-Type': 'application/json' },
  });
  const login = http.post(`${BASE}/api/auth/login`, JSON.stringify({ email, password }), {
    headers: { 'Content-Type': 'application/json' },
  });
  check(login, { 'login 200': (r) => r.status === 200 });
  const token = login.json('access_token');
  if (!token) {
    transferErrors.add(1);
    return;
  }
  const h = authHeaders(token);

  // open two accounts
  const a = http.post(`${BASE}/api/accounts`, JSON.stringify({ currency: 'USD' }), h);
  const b = http.post(`${BASE}/api/accounts`, JSON.stringify({ currency: 'USD' }), h);
  const src = a.json('account_id');
  const dst = b.json('account_id');
  if (!src || !dst) {
    transferErrors.add(1);
    return;
  }

  // initiate a transfer (idempotency key per attempt)
  const key = uuidv4();
  const t0 = Date.now();
  const transfer = http.post(
    `${BASE}/api/transfers`,
    JSON.stringify({
      source_account_id: src,
      destination_account_id: dst,
      amount: { minor_units: 100, currency: 'USD' },
    }),
    { headers: { ...h.headers, 'Idempotency-Key': key } },
  );
  transferLatency.add(Date.now() - t0);
  const ok = check(transfer, { 'transfer accepted': (r) => r.status === 200 || r.status === 202 });
  transferErrors.add(!ok);

  // poll transfer status once (eventual consistency)
  const id = transfer.json('transfer_id');
  if (id) {
    sleep(0.2);
    http.get(`${BASE}/api/transfers/${id}`, h);
  }
  sleep(1);
}
