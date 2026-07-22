# API v2 backend handoff

Date: 2026-07-22  
Branch: `codex/backend-api-v2-alignment`  
Base at handoff: `origin/main` / `79d0f37`

The normative, read-only contract is `workspace-shared-docs/contracts/v2/`. Do not edit that
publication from this backend branch. This document records implementation state and known gaps;
it does not redefine the contract.

## Verified implementation

### Contract and HTTP boundary

- The packaged v2 JSONC schema and examples are validated against Draft 2020-12.
- Public origins and OAuth/OIDC metadata use the contract-fixed production URLs.
- Public immutable Resume template list/detail endpoints are available under `/api/v2`.
- Protected `/api/v2/*` requests require `X-Request-Id` and a Bearer token. The v1 mock/HMAC
  identity cannot silently authenticate v2 resources.
- CORS exposes the v2 protocol headers for exact configured origins; `/identity/v2/*` has no CORS.

### OAuth/OIDC authorization server

- Discovery, protected-resource metadata, Authorization Code + PKCE S256, exact public-client
  redirect registration, one-time authorization codes, RS256/JWKS access and ID tokens, refresh
  rotation/reuse-family revocation, and token revocation are implemented.
- OAuth secrets are persisted as hashes; private signing material is loaded from configured files.
- Migrations `20260722_0008` through `20260722_0010` add migration audit, authorization transaction,
  authorization-code, refresh-family/token, and revoked-access-token persistence.

### Hosted identity

- `/identity/v2/flows` and `/steps` implement browser-session, OAuth transaction, exact Origin,
  Fetch Metadata and CSRF binding with step-id deduplication and server-owned allowed transitions.
- Registration, password login, recovery, recent reauthentication, login sessions, authenticator
  listing/removal and one-time recovery-code bundles are implemented.
- Passwords use scrypt and a minimum 15-character policy; OTPs are random six-digit, hashed,
  expiring, single-use and retain the failed-attempt budget across resend.
- Passkey tests perform real P-256 registration and authentication signatures. The implementation
  validates challenge, exact origin, RP ID, UP/UV, algorithm, credential ID and sign counter.
- Login cookies are `__Host-`, Secure, HttpOnly and SameSite=Lax, with idle and absolute expiry.
- SMTP STARTTLS and memory email adapters exist. Runtime construction rejects memory delivery in
  staging/production.
- Migrations `20260722_0011` and `20260722_0012` add hosted-flow, credential and login-session state.

## Automated evidence at handoff

Run from the repository root:

```bash
uv sync --locked
uv run ruff check .
uv run mypy --strict src/backend
uv run pytest -q
git diff --check
```

Latest result: Ruff passed, strict mypy passed, `337 passed, 1 skipped`. The skipped test is the
documented non-POSIX rendering-hardening branch. No manual browser verification is required for
the covered behavior.

## Known incomplete or production-hardening work

Complete these in order:

1. Add standard `/userinfo`, then implement `/api/v2/me`, Workspace CRUD/membership/invitation and
   authorization checks. Protected v2 tenant routes must remain closed until these exist.
2. Link each OAuth authorization code and refresh-token family to the login session that created
   it. Session deletion currently revokes all refresh families for that user (safe but broader than
   the contract's associated-family behavior).
3. Replace the process-local email send limiter with a PostgreSQL-atomic limiter or transactional
   outbox/rate-limit design covering account, browser device and trusted network across workers.
4. Make email delivery retryable through a durable outbox. SMTP exists, but delivery currently
   occurs after flow-state persistence; a process failure can require an explicit resend.
5. Integrate a production breached-password corpus/service. The current small deny set only proves
   the policy hook and must not be considered complete breach screening.
6. Add cleanup/retention jobs for expired OAuth requests/codes, identity flows, browser sessions,
   revoked token state and rate-limit buckets.
7. Execute revisions `0008`-`0012` against a disposable PostgreSQL 17 + pgvector database, inspect
   grants/constraints, and test downgrade refusal with live security records. Unit migration gates
   pass, but this handoff did not provision a fresh external database.
8. Migrate v2 tenant resources in the contract order: shadow reads first; then strong ETag and
   `If-Match` writes, durable v2 idempotency, transactional outbox, unified Job/Artifact/Event;
   finally client cutover and v1 retirement.

## Configuration notes

- `example.jsonc` now contains `oauth` and `hosted_identity` blocks.
- Development/test may use `hosted_identity.email.mode = "memory"`.
- Staging/production must set `mode = "smtp"`, sender address, host/port, TLS choice and optional
  username/password in the private ignored `config.jsonc`. Never commit real SMTP, OAuth signing,
  model-provider or database secrets.
- The configured OpenRouter API key is unrelated to identity email and must remain private.

## Working tree and publication

At handoff the branch is based directly on `origin/main`; all v2 work is intentionally uncommitted.
Review untracked files as well as tracked diffs before committing:

```bash
git status --short
git diff --check
git diff --stat
git diff
git ls-files --others --exclude-standard
```

Suggested commit split:

1. contract packaging, v2 boundary and template endpoints;
2. OAuth/OIDC services, persistence, migrations and tests;
3. hosted identity services, persistence, migrations, SMTP adapter and tests;
4. implementation-status and handoff documentation.

After committing, re-run the five commands in the automated-evidence section, then push:

```bash
git push -u origin codex/backend-api-v2-alignment
```
