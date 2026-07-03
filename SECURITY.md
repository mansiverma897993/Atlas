# Security Policy

## Supported versions

The project is pre-1.0 and under active development. Security fixes land on `main` and the
latest tagged release only.

| Version        | Supported |
|----------------|-----------|
| `main` (latest)| ✅        |
| Latest `v*` tag| ✅        |
| Older tags     | ❌        |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security problems.**

Report privately via one of:

- GitHub **Security Advisories** — *Security → Report a vulnerability* (preferred; opens a
  private advisory), or
- Email **security@ledger.local** with the details below.

Include as much as you can:

- A description of the issue and its impact.
- Affected service(s) (`gateway` / `identity` / `ledger` / `notification` / `worker`) and
  version, commit SHA, or image tag.
- Steps to reproduce or a proof of concept.
- Any suggested remediation.

## What to expect

- **Acknowledgement** within **2 business days**.
- An initial **assessment and severity** (CVSS) within **5 business days**.
- We aim to ship a fix for confirmed high/critical issues within **30 days** and will keep
  you updated on progress.
- **Coordinated disclosure**: please give us a reasonable window to release a fix before any
  public disclosure. We are happy to credit reporters unless you prefer to remain anonymous.

## Scope

In scope: the service binaries and library crates in this repository, their CI/CD, and the
deployment manifests under `deploy/`.

Out of scope: third-party dependencies (report upstream; we track advisories via
`cargo-deny` and Dependabot), and issues requiring privileged local access or
social engineering.

## Handling of secrets

Never include real credentials, tokens, or production data in a report or a PR. Local
defaults live in `.env.example`; real secrets are injected at deploy time and must never be
committed.
