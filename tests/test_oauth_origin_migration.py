"""Strict JWT and opaque-grant behavior for the production Origin migration."""

from __future__ import annotations

import base64
import json
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from backend.api.constants import PUBLIC_ORIGIN
from backend.domain.oauth import (
    ACCESS_TOKEN_USER_ID_CLAIM,
    AuthorizationRequestRecord,
    OAuthTokenValidationError,
)
from backend.infrastructure.oauth import InMemoryOAuthAuthorizationRequestRepository
from backend.infrastructure.oauth_tokens import OAuthTokenSigner

_LEGACY_ORIGIN = "https://api.hmalliances.org:8022"
_LEGACY_USER_ID_CLAIM = f"{_LEGACY_ORIGIN}/claims/user_id"
_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _jwt(
    key: rsa.RSAPrivateKey,
    *,
    kid: str,
    issuer: str,
    audience: str | None,
    issued_at: datetime,
    user_claim: str,
) -> str:
    header = {"alg": "RS256", "kid": kid, "typ": "at+jwt"}
    claims: dict[str, Any] = {
        "iss": issuer,
        "sub": "oidc-subject-origin-migration",
        user_claim: "usr_origin_migration",
        "exp": int((issued_at + timedelta(minutes=10)).timestamp()),
        "nbf": int(issued_at.timestamp()),
        "iat": int(issued_at.timestamp()),
        "jti": "jti_origin_migration",
        "client_id": "aiws-web-local",
        "scope": "workspace.read",
    }
    if audience is not None:
        claims["aud"] = audience
    encoded_header = _b64(json.dumps(header, separators=(",", ":"), sort_keys=True).encode())
    encoded_claims = _b64(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode())
    signing_input = f"{encoded_header}.{encoded_claims}".encode("ascii")
    signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{signing_input.decode()}.{_b64(signature)}"


def _migration_signer(
    key: rsa.RSAPrivateKey,
    *,
    cutover: datetime,
    accept_until: datetime,
) -> OAuthTokenSigner:
    return OAuthTokenSigner(
        (key,),
        origin_cutover_at=cutover,
        legacy_access_token_accept_until=accept_until,
    )


def test_new_and_bounded_legacy_access_tokens_share_keys_but_not_identity_pairs() -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cutover = datetime(2026, 7, 25, 8, 0, tzinfo=UTC)
    current = cutover + timedelta(minutes=5)
    signer = _migration_signer(
        key,
        cutover=cutover,
        accept_until=cutover + timedelta(minutes=10),
    )
    kid = signer.jwks["keys"][0]["kid"]

    new_token = _jwt(
        key,
        kid=kid,
        issuer=PUBLIC_ORIGIN,
        audience=PUBLIC_ORIGIN,
        issued_at=current,
        user_claim=ACCESS_TOKEN_USER_ID_CLAIM,
    )
    legacy_token = _jwt(
        key,
        kid=kid,
        issuer=_LEGACY_ORIGIN,
        audience=_LEGACY_ORIGIN,
        issued_at=cutover - timedelta(minutes=1),
        user_claim=_LEGACY_USER_ID_CLAIM,
    )

    assert signer.verify_access_token(new_token, now=current)["iss"] == PUBLIC_ORIGIN
    legacy_claims = signer.verify_access_token(legacy_token, now=current)
    assert legacy_claims["iss"] == _LEGACY_ORIGIN
    assert legacy_claims[ACCESS_TOKEN_USER_ID_CLAIM] == "usr_origin_migration"


@pytest.mark.parametrize(
    ("issuer", "audience", "issued_offset", "current_offset"),
    [
        (_LEGACY_ORIGIN, PUBLIC_ORIGIN, -60, 60),
        (PUBLIC_ORIGIN, _LEGACY_ORIGIN, 60, 60),
        (_LEGACY_ORIGIN, _LEGACY_ORIGIN, 0, 60),
        (_LEGACY_ORIGIN, _LEGACY_ORIGIN, -60, 601),
        (PUBLIC_ORIGIN, "https://other.example", 60, 60),
    ],
)
def test_origin_migration_rejects_mixed_post_cutover_expired_policy_and_wrong_audience(
    issuer: str,
    audience: str,
    issued_offset: int,
    current_offset: int,
) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cutover = datetime(2026, 7, 25, 8, 0, tzinfo=UTC)
    signer = _migration_signer(
        key,
        cutover=cutover,
        accept_until=cutover + timedelta(minutes=10),
    )
    token = _jwt(
        key,
        kid=signer.jwks["keys"][0]["kid"],
        issuer=issuer,
        audience=audience,
        issued_at=cutover + timedelta(seconds=issued_offset),
        user_claim=(
            _LEGACY_USER_ID_CLAIM if issuer == _LEGACY_ORIGIN else ACCESS_TOKEN_USER_ID_CLAIM
        ),
    )

    with pytest.raises(OAuthTokenValidationError):
        signer.verify_access_token(
            token,
            now=cutover + timedelta(seconds=current_offset),
        )


def test_origin_migration_rejects_missing_audience_unknown_kid_and_wrong_signature() -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cutover = datetime(2026, 7, 25, 8, 0, tzinfo=UTC)
    current = cutover + timedelta(minutes=1)
    signer = _migration_signer(
        key,
        cutover=cutover,
        accept_until=cutover + timedelta(minutes=10),
    )
    kid = signer.jwks["keys"][0]["kid"]
    candidates = (
        _jwt(
            key,
            kid=kid,
            issuer=PUBLIC_ORIGIN,
            audience=None,
            issued_at=current,
            user_claim=ACCESS_TOKEN_USER_ID_CLAIM,
        ),
        _jwt(
            key,
            kid="unknown-kid",
            issuer=PUBLIC_ORIGIN,
            audience=PUBLIC_ORIGIN,
            issued_at=current,
            user_claim=ACCESS_TOKEN_USER_ID_CLAIM,
        ),
        _jwt(
            other_key,
            kid=kid,
            issuer=PUBLIC_ORIGIN,
            audience=PUBLIC_ORIGIN,
            issued_at=current,
            user_claim=ACCESS_TOKEN_USER_ID_CLAIM,
        ),
    )

    for token in candidates:
        with pytest.raises(OAuthTokenValidationError):
            signer.verify_access_token(token, now=current)


@pytest.mark.asyncio
async def test_pre_cutover_authorization_codes_and_refresh_families_are_invalid() -> None:
    repository = InMemoryOAuthAuthorizationRequestRepository()
    created_at = datetime.now(UTC)
    cutover = created_at + timedelta(seconds=1)
    request = AuthorizationRequestRecord(
        id="authreq_pre_cutover",
        client_id="aiws-web-local",
        redirect_uri="https://app.hmalliances.org/oauth/callback",
        scopes=("openid", "offline_access"),
        state="state-pre-cutover",
        nonce="nonce-pre-cutover",
        code_challenge="challenge-pre-cutover",
        code_challenge_method="S256",
        prompt=("consent",),
        screen_hint=None,
        status="pending",
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=5),
    )
    await repository.create_authorization_request(request)
    assert await repository.issue_authorization_code(
        request.id,
        subject="oidc-subject-origin-migration",
        user_id="usr_origin_migration",
        login_session_id="idses_origin_migration",
        code_hash="code-hash-origin-migration",
        auth_time=created_at,
        expires_at=created_at + timedelta(minutes=1),
    )
    assert (
        await repository.exchange_authorization_code(
            "code-hash-origin-migration",
            client_id=request.client_id,
            redirect_uri=request.redirect_uri,
            verifier_challenge=request.code_challenge,
            refresh_family_id="rtfam_origin_migration",
            refresh_token_id="rt_origin_migration_1",
            refresh_token_hash="refresh-hash-origin-migration",
            refresh_expires_at=created_at + timedelta(days=1),
            origin_cutover_at=cutover,
        )
        is None
    )

    post_cutover_request = replace(
        request,
        id="authreq_post_cutover",
        created_at=cutover,
        expires_at=cutover + timedelta(minutes=5),
    )
    await repository.create_authorization_request(post_cutover_request)
    assert await repository.issue_authorization_code(
        post_cutover_request.id,
        subject="oidc-subject-origin-migration",
        user_id="usr_origin_migration",
        login_session_id="idses_origin_migration",
        code_hash="code-hash-post-cutover",
        auth_time=cutover,
        expires_at=cutover + timedelta(minutes=1),
    )
    assert (
        await repository.exchange_authorization_code(
            "code-hash-post-cutover",
            client_id=request.client_id,
            redirect_uri=request.redirect_uri,
            verifier_challenge=request.code_challenge,
            refresh_family_id="rtfam_post_cutover",
            refresh_token_id="rt_post_cutover_1",
            refresh_token_hash="refresh-hash-post-cutover",
            refresh_expires_at=cutover + timedelta(days=1),
        )
        is not None
    )
    assert (
        await repository.rotate_refresh_token(
            "refresh-hash-post-cutover",
            client_id=request.client_id,
            replacement_token_id="rt_post_cutover_2",
            replacement_token_hash="refresh-hash-post-cutover-2",
            replacement_expires_at=cutover + timedelta(days=1),
            origin_cutover_at=cutover + timedelta(seconds=1),
        )
        is None
    )


def test_legacy_origin_is_confined_to_the_removable_verifier_and_migration_fixtures() -> None:
    allowed = {
        Path("src/backend/infrastructure/oauth_tokens.py"),
        Path("tests/test_oauth_origin_migration.py"),
        Path("workspace-shared-docs/contracts/v2/diff.md"),
    }
    tracked = set(
        subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=_PROJECT_ROOT,
            check=True,
            capture_output=True,
        ).stdout.decode().split("\0")
    )
    tracked.update(
        {
            "tests/test_oauth_origin_migration.py",
            "workspace-shared-docs/contracts/v2/diff.md",
        }
    )
    occurrences = {
        relative
        for item in tracked
        if item
        for relative in (Path(item),)
        if (_PROJECT_ROOT / relative).is_file()
        and _LEGACY_ORIGIN
        in (_PROJECT_ROOT / relative).read_text(encoding="utf-8", errors="ignore")
    }

    assert occurrences == allowed
