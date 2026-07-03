# Summary

<!-- What does this PR change and why? Link the phase (ROADMAP) or issue. -->

Closes #

## Type of change

- [ ] Feature
- [ ] Fix
- [ ] Refactor / tech debt
- [ ] Docs / tooling / CI
- [ ] Breaking change

## Checklist

- [ ] Conventional-commit title (e.g. `feat(ledger): ...`, `fix(gateway): ...`)
- [ ] `cargo fmt --all` clean
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` clean
- [ ] Tests added/updated and `cargo test --workspace` green
- [ ] Migrations (if any) live under `migrations/<db>/` and run in an init step, not app boot
- [ ] `cargo deny check` passes (no new advisories / disallowed licenses)
- [ ] Docs updated (README / docs/) where behaviour or config changed
- [ ] New env vars reflected in `.env.example` **and** `docs/CONVENTIONS.md`
- [ ] Hexagonal layering respected (domain has no adapter/infra imports)
- [ ] **ADR added under `docs/adr/`** for any architectural decision or trade-off

## How was this tested?

<!-- Commands run, scenarios covered, integration/property tests exercised. -->

## Notes for reviewers

<!-- Anything that needs extra eyes: risk, rollout, follow-ups. -->
