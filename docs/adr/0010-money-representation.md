# ADR-0010 — Money as integer minor units (no floating point)

**Status:** Accepted

## Context
Representing money as `f64`/`f32` is a classic, catastrophic bug: binary floating point cannot
represent most decimal fractions exactly (`0.1 + 0.2 != 0.3`), rounding errors accumulate over
many postings, and a ledger that doesn't sum to zero is worthless. This is non-negotiable in a
payments system.

## Decision
Model money as a `Money` value object in the `kernel` crate: an **integer count of minor
units** (`i128 minor_units`) paired with its `Currency` (ISO-4217). All arithmetic is
**checked** and returns `Result` on overflow; operations across differing currencies are a
**compile-or-runtime error**, never an implicit conversion. No floating point anywhere in the
money path. Storage uses `NUMERIC`/`BIGINT` in Postgres, never `FLOAT`/`DOUBLE`.

```
Money { minor_units: i128, currency: Currency }   // e.g. 12_345 = $123.45 USD
```

## Consequences
- **+** Exact arithmetic; conservation invariants hold to the cent; sums are deterministic.
- **+** Currency mixing is caught by the type system, not discovered in production.
- **+** `i128` headroom makes overflow effectively unreachable for realistic amounts, yet still
  checked.
- **−** Callers must think in minor units and handle a `Result` on arithmetic. This friction is
  intentional — it makes the correctness cost visible at every call site.
- **Considered:** `rust_decimal` (arbitrary-precision decimal). Excellent and interoperable;
  integer minor units are chosen for a ledger because postings are inherently whole minor units
  and integer math is the simplest thing that is provably exact. `rust_decimal` remains the
  fallback if sub-minor-unit or high-precision FX is ever required.
