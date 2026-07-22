# API v2 implementation status

The normative source is the read-only `workspace-shared-docs/contracts/v2/` publication. This
document records backend implementation evidence; it does not redefine that contract.

## Implemented on this branch

- Phase 0 gates parse `schema.jsonc` directly, assert Draft 2020-12 validity and formats, validate
  all published examples by `schema_ref`, and check every JSON schema named by the route table.
- The production origin, test Resource Server origin, and Protected Resource Metadata URL are
  frozen in the v2 boundary module.
- The v2 schema is packaged in the wheel as JSONC; no second hand-maintained strict-JSON copy is
  introduced.
- `GET /api/v2/resume-templates` and
  `GET /api/v2/resume-templates/{template_id}?version=...` implement the public immutable-template
  slice with v2 pagination and response validation.
- Every other `/api/v2/*` path requires `X-Request-Id` and returns the v2 Bearer challenge. It never
  accepts the v1 development mock or trusted-proxy assertion as v2 identity.
- CORS permits and exposes the v2 protocol headers while retaining exact configured origins and
  disabled credentialed CORS.
- Alembic revision `20260722_0008` creates an append-only v1-to-v2 migration audit ledger.
- Public OIDC discovery and OAuth Protected Resource Metadata publish the fixed issuer/resource,
  endpoint URLs, header-only Bearer usage, public-client authentication, Authorization Code flow,
  and PKCE `S256`. Insecure legacy grants and PKCE `plain` are not advertised.
- Registered Web/Electron public clients use exact redirects (with only the RFC 8252 loopback-port
  exception), durable short-lived authorization transactions, and one-time PKCE-bound codes.
- `/oauth/jwks`, `/oauth/token`, and `/oauth/revoke` implement persistent RS256 signing keys,
  RFC 9068-style access-token claim validation, nonce-bound ID tokens, hashed opaque refresh
  tokens, rotation on every refresh, family revocation after ancestor reuse, and access-token JTI
  revocation. Token responses and errors are non-cacheable.
- The same-origin hosted identity boundary now implements browser/CSRF transaction binding,
  finite-state registration/login/recovery/reauthentication flows, scrypt password verifiers,
  one-time email and recovery codes, WebAuthn passkey registration/authentication, bounded login
  sessions, authenticator/session management, and OAuth authorization resumption. Identity pages
  and JSON responses are non-cacheable, non-frameable, and never CORS-enabled.
- Verification email delivery has memory and STARTTLS SMTP adapters. Memory delivery is accepted
  only by development/test runtime construction; staging/production fail closed unless SMTP is
  configured.

## Deliberately not advertised as implemented

- Phase 1 still needs `/userinfo`, `/api/v2/me`, Workspace and membership APIs. Hosted identity
  also needs the production-hardening work recorded in `API_V2_HANDOFF.md` before it is declared
  complete.
- Phase 2 tenant-resource shadow reads.
- Phase 3 v2 writes, strong ETag/If-Match, v2 idempotency retention, transactional outbox, unified
  Job/Artifact/Event APIs.
- Phase 4 client cutover and Phase 5 v1 retirement.

Protected v2 resources must remain closed until Phase 1 is implemented and its security tests pass.
Adding a handler that derives identity or Workspace from v1 defaults is forbidden.
