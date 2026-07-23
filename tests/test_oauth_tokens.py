"""End-to-end Authorization Code, JWT, refresh rotation, reuse, and revocation tests."""

from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime, timedelta
from functools import partial
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient

from backend.composition import BackendContainer
from backend.domain.oauth import AuthorizationRequestRecord
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.oauth import InMemoryOAuthAuthorizationRequestRepository
from backend.package_resources import read_contract_schema_text


def _pkce(verifier: str) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")


def _authorize_and_complete(
    client: TestClient,
    *,
    verifier: str,
    scopes: str = "openid profile offline_access workspace.read resume.read",
) -> str:
    response = client.get(
        "/oauth/authorize",
        params={
            "response_type": "code",
            "client_id": "aiws-web-local",
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "scope": scopes,
            "state": "state-token-flow",
            "nonce": "nonce-token-flow",
            "code_challenge": _pkce(verifier),
            "code_challenge_method": "S256",
            "prompt": "consent" if "offline_access" in scopes else "login",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    request_id = response.headers["location"].rsplit("/", 1)[-1]
    container = client.app.state.container
    assert isinstance(container, BackendContainer)
    callback = client.portal.call(
        partial(
            container.oauth.complete_authorization,
            request_id,
            subject="oidc-subject-token-test",
            user_id="usr_local_demo",
            login_session_id="idses_oauth_token_test",
        )
    )
    query = parse_qs(urlsplit(callback).query)
    assert query["state"] == ["state-token-flow"]
    return query["code"][0]


def _exchange(client: TestClient, *, code: str, verifier: str) -> dict[str, object]:
    response = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "aiws-web-local",
            "code": code,
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "code_verifier": verifier,
        },
    )
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"
    payload = response.json()
    ContractValidator.from_jsonc(read_contract_schema_text("v2")).validate_definition(
        "AuthorizationCodeTokenResponse", payload
    )
    return payload


@pytest.mark.asyncio
async def test_revoking_one_login_session_revokes_only_its_refresh_family() -> None:
    """@brief 会话撤销只影响其精确 refresh family / Session revocation targets only its exact refresh family."""

    repository = InMemoryOAuthAuthorizationRequestRepository()
    now = datetime.now(UTC)
    for suffix in ("a", "b"):
        request = AuthorizationRequestRecord(
            id=f"authreq_session_{suffix}",
            client_id="aiws-web-local",
            redirect_uri="https://app.hmalliances.org/oauth/callback",
            scopes=("openid", "offline_access"),
            state=f"state-{suffix}",
            nonce=f"nonce-{suffix}",
            code_challenge=f"challenge-{suffix}",
            code_challenge_method="S256",
            prompt=("consent",),
            screen_hint=None,
            status="pending",
            created_at=now,
            expires_at=now + timedelta(minutes=5),
        )
        await repository.create_authorization_request(request)
        assert await repository.issue_authorization_code(
            request.id,
            subject="oidc-subject-token-test",
            user_id="usr_local_demo",
            login_session_id=f"idses_{suffix}",
            code_hash=f"code-hash-{suffix}",
            auth_time=now,
            expires_at=now + timedelta(minutes=1),
        )
        assert (
            await repository.exchange_authorization_code(
                f"code-hash-{suffix}",
                client_id=request.client_id,
                redirect_uri=request.redirect_uri,
                verifier_challenge=request.code_challenge,
                refresh_family_id=f"rtfam_{suffix}",
                refresh_token_id=f"rt_{suffix}_1",
                refresh_token_hash=f"refresh-hash-{suffix}",
                refresh_expires_at=now + timedelta(days=1),
            )
            is not None
        )

    await repository.revoke_families_for_login_session(
        "usr_local_demo", "idses_a", now + timedelta(seconds=1)
    )

    assert (
        await repository.rotate_refresh_token(
            "refresh-hash-a",
            client_id="aiws-web-local",
            replacement_token_id="rt_a_2",
            replacement_token_hash="refresh-hash-a-2",
            replacement_expires_at=now + timedelta(days=1),
        )
        is None
    )
    assert (
        await repository.rotate_refresh_token(
            "refresh-hash-b",
            client_id="aiws-web-local",
            replacement_token_id="rt_b_2",
            replacement_token_hash="refresh-hash-b-2",
            replacement_expires_at=now + timedelta(days=1),
        )
        is not None
    )


@pytest.mark.asyncio
async def test_user_access_token_epoch_is_monotonic_and_inclusive() -> None:
    """@brief 用户级撤销立即覆盖既有 JWT 且不会倒退 / User-level revocation immediately covers existing JWTs and never regresses."""

    repository = InMemoryOAuthAuthorizationRequestRepository()
    issued_before = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
    cutoff = issued_before + timedelta(seconds=10)
    issued_after = cutoff + timedelta(seconds=1)

    assert not await repository.user_access_tokens_are_revoked("usr_local_demo", issued_before)
    await repository.revoke_access_tokens_for_user("usr_local_demo", cutoff)
    await repository.revoke_access_tokens_for_user("usr_local_demo", cutoff - timedelta(seconds=5))

    assert await repository.user_access_tokens_are_revoked("usr_local_demo", issued_before)
    assert await repository.user_access_tokens_are_revoked("usr_local_demo", cutoff)
    assert not await repository.user_access_tokens_are_revoked("usr_local_demo", issued_after)


def test_jwks_and_authorization_code_exchange_produce_a_signed_but_user_bound_token(
    backend_client: TestClient,
) -> None:
    verifier = "v" * 43
    code = _authorize_and_complete(backend_client, verifier=verifier)
    payload = _exchange(backend_client, code=code, verifier=verifier)
    access_token = payload["access_token"]
    assert isinstance(access_token, str)
    assert isinstance(payload["id_token"], str)
    assert isinstance(payload["refresh_token"], str)

    jwks = backend_client.get("/oauth/jwks")
    assert jwks.status_code == 200
    assert jwks.headers["cache-control"] == "public, max-age=300"
    assert len(jwks.json()["keys"]) == 1
    assert set(jwks.json()["keys"][0]) == {"kty", "use", "alg", "kid", "n", "e"}

    unbound = backend_client.get(
        "/api/v2/me",
        headers={
            "X-Request-Id": "req-valid-jwt-test",
            "Authorization": f"Bearer {access_token}",
        },
    )
    assert unbound.status_code == 401
    assert unbound.json()["code"] == "oauth.invalid_token"

    replay = backend_client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "aiws-web-local",
            "code": code,
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "code_verifier": verifier,
        },
    )
    assert replay.status_code == 400
    assert replay.json()["error"] == "invalid_grant"


def test_wrong_pkce_verifier_does_not_consume_the_authorization_code(
    backend_client: TestClient,
) -> None:
    verifier = "p" * 43
    code = _authorize_and_complete(backend_client, verifier=verifier)
    wrong = backend_client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "aiws-web-local",
            "code": code,
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "code_verifier": "x" * 43,
        },
    )
    assert wrong.status_code == 400
    assert wrong.json()["error"] == "invalid_grant"
    assert _exchange(backend_client, code=code, verifier=verifier)["access_token"]


def test_refresh_tokens_rotate_and_ancestor_reuse_revokes_the_whole_family(
    backend_client: TestClient,
) -> None:
    verifier = "r" * 43
    first = _exchange(
        backend_client,
        code=_authorize_and_complete(backend_client, verifier=verifier),
        verifier=verifier,
    )["refresh_token"]
    assert isinstance(first, str)
    rotated = backend_client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": "aiws-web-local",
            "refresh_token": first,
        },
    )
    assert rotated.status_code == 200
    second = rotated.json()["refresh_token"]
    assert second != first

    reuse = backend_client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": "aiws-web-local",
            "refresh_token": first,
        },
    )
    assert reuse.status_code == 400
    assert reuse.json()["error"] == "invalid_grant"

    family_revoked = backend_client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": "aiws-web-local",
            "refresh_token": second,
        },
    )
    assert family_revoked.status_code == 400


def test_revoke_denies_access_token_and_unknown_revocation_is_always_200(
    backend_client: TestClient,
) -> None:
    verifier = "z" * 43
    payload = _exchange(
        backend_client,
        code=_authorize_and_complete(backend_client, verifier=verifier),
        verifier=verifier,
    )
    access_token = payload["access_token"]
    assert isinstance(access_token, str)
    revoked = backend_client.post("/oauth/revoke", data={"token": access_token})
    assert revoked.status_code == 200
    denied = backend_client.get(
        "/api/v2/me",
        headers={
            "X-Request-Id": "req-revoked-jwt-test",
            "Authorization": f"Bearer {access_token}",
        },
    )
    assert denied.status_code == 401
    assert denied.json()["code"] == "oauth.invalid_token"
    assert backend_client.post("/oauth/revoke", data={"token": "unknown-token"}).status_code == 200


def test_no_offline_access_means_no_refresh_token_and_public_clients_reject_secrets(
    backend_client: TestClient,
) -> None:
    verifier = "n" * 43
    payload = _exchange(
        backend_client,
        code=_authorize_and_complete(
            backend_client,
            verifier=verifier,
            scopes="openid profile workspace.read",
        ),
        verifier=verifier,
    )
    assert "refresh_token" not in payload
    secret_attempt = backend_client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": "aiws-web-local",
            "refresh_token": "rt_" + "x" * 64,
            "client_secret": "must-not-exist",
        },
    )
    assert secret_attempt.status_code == 400
    assert secret_attempt.json()["error"] == "invalid_client"


def test_token_verifier_rejects_tampering_without_leaking_the_reason(
    backend_client: TestClient,
) -> None:
    verifier = "t" * 43
    payload = _exchange(
        backend_client,
        code=_authorize_and_complete(backend_client, verifier=verifier),
        verifier=verifier,
    )
    token = payload["access_token"]
    assert isinstance(token, str)
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    response = backend_client.get(
        "/api/v2/me",
        headers={
            "X-Request-Id": "req-tampered-jwt-test",
            "Authorization": f"Bearer {tampered}",
        },
    )
    assert response.status_code == 401
    assert response.json()["code"] == "oauth.invalid_token"
    assert response.json()["detail"] is None
    assert "signature" not in response.text
