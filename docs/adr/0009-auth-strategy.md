# ADR-0009 — JWT RS256 + rotating refresh tokens + JWKS + RBAC

**Status:** Accepted

## Context
The system needs stateless, horizontally-scalable authentication that the gateway and every
service can verify without a synchronous call to Identity on each request, plus safe
long-lived sessions and role-based authorization. Naive choices fail: symmetric HS256 shares
a secret with every verifier; non-rotating refresh tokens are a replay liability; opaque
tokens require a central lookup on every request.

## Decision
- **Access tokens:** short-lived (~15 min) **JWT signed RS256**. Identity holds the private
  key and publishes public keys at a **JWKS** endpoint; the gateway and services verify
  locally with the public key (no per-request call to Identity). Key rotation via `kid`.
- **Refresh tokens:** long-lived, **opaque, stored hashed**, and **rotated on every use**.
  Tokens are grouped into a **family**; presenting an already-used (rotated-out) refresh token
  signals theft and **revokes the entire family** (reuse detection).
- **OAuth2:** authorization-code flow **with PKCE** for one external provider.
- **AuthZ:** **RBAC** — roles → permissions — enforced as a Tower layer at the gateway and
  **re-checked** in each service's application layer (defense in depth). Least-privilege DB
  roles per service.

## Consequences
- **+** Stateless verification scales horizontally; no auth DB hit on the hot path.
- **+** Short access-token TTL bounds the blast radius of a leak; rotation + reuse detection
  contains refresh-token theft.
- **+** RS256/JWKS lets services verify without holding a shared secret; keys rotate cleanly.
- **−** Access tokens can't be revoked before expiry (inherent to stateless JWT). Mitigated by
  short TTL and a revocation list for emergencies (checked only for sensitive operations).
- **−** JWKS caching and key rotation add operational care; documented in the telemetry/ops runbook.
