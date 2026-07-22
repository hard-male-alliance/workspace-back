"""Hosted identity browser binding and flow contract tests."""

from __future__ import annotations

import re
from urllib.parse import urlencode

from fastapi.testclient import TestClient

from backend.api.constants import PUBLIC_ORIGIN
from backend.api.identity import IDENTITY_BROWSER_COOKIE

_CSRF_PATTERN = re.compile(r'data-csrf-token="([A-Za-z0-9_-]+)"')


def _begin(client: TestClient, *, screen_hint: str = "signup") -> tuple[str, str, str]:
    query = urlencode(
        {
            "response_type": "code",
            "client_id": "aiws-web-local",
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "scope": "openid profile workspace.read",
            "state": "state-identity-test",
            "nonce": "nonce-identity-test",
            "code_challenge": "A" * 43,
            "code_challenge_method": "S256",
            "screen_hint": screen_hint,
        }
    )
    started = client.get(f"/oauth/authorize?{query}", follow_redirects=False)
    assert started.status_code == 303
    continued = client.get(started.headers["location"])
    assert continued.status_code == 200
    match = _CSRF_PATTERN.search(continued.text)
    assert match is not None
    request_id = started.headers["location"].rsplit("/", 1)[-1]
    cookie = client.cookies.get(IDENTITY_BROWSER_COOKIE)
    assert cookie is not None
    return request_id, match.group(1), cookie


def _headers(csrf: str, cookie: str) -> dict[str, str]:
    return {
        "Origin": PUBLIC_ORIGIN,
        "Sec-Fetch-Site": "same-origin",
        "X-CSRF-Token": csrf,
        "Cookie": f"{IDENTITY_BROWSER_COOKIE}={cookie}",
    }


def test_flow_is_bound_to_oauth_browser_and_matches_v2_schema(
    backend_client: TestClient,
) -> None:
    request_id, csrf, cookie = _begin(backend_client)
    created = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(csrf, cookie),
        json={"purpose": "register", "authorization_request_id": request_id},
    )
    assert created.status_code == 201, created.text
    payload = created.json()
    assert payload["purpose"] == "register"
    assert payload["status"] == "pending"
    assert payload["allowed_steps"] == ["identify"]
    assert payload["authorization_resume_uri"] is None
    assert payload["expires_at"].endswith("Z")
    restored = backend_client.get(
        f"/identity/v2/flows/{payload['id']}",
        headers={"Cookie": f"{IDENTITY_BROWSER_COOKIE}={cookie}"},
    )
    assert restored.status_code == 200
    assert restored.json() == payload
    for response in (created, restored):
        assert response.headers["cache-control"] == "no-store"
        assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
        assert "access-control-allow-origin" not in response.headers


def test_flow_rejects_cross_site_missing_csrf_and_wrong_browser(
    backend_client: TestClient,
) -> None:
    request_id, csrf, cookie = _begin(backend_client)
    payload = {"purpose": "register", "authorization_request_id": request_id}
    cross_site = backend_client.post(
        "/identity/v2/flows",
        headers={**_headers(csrf, cookie), "Origin": "https://evil.example"},
        json=payload,
    )
    assert cross_site.status_code == 403
    missing_csrf = backend_client.post(
        "/identity/v2/flows",
        headers={
            "Origin": PUBLIC_ORIGIN,
            "Sec-Fetch-Site": "same-origin",
            "Cookie": f"{IDENTITY_BROWSER_COOKIE}={cookie}",
        },
        json=payload,
    )
    assert missing_csrf.status_code == 403
    wrong_cookie = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(csrf, cookie + "tampered"),
        json=payload,
    )
    assert wrong_cookie.status_code == 401
    for response in (cross_site, missing_csrf, wrong_cookie):
        assert "access-control-allow-origin" not in response.headers


def test_flow_cannot_cross_authorization_transactions(
    backend_client: TestClient,
) -> None:
    first_id, first_csrf, first_cookie = _begin(backend_client)
    second_id, _, _ = _begin(backend_client)
    assert first_id != second_id
    response = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(first_csrf, first_cookie),
        json={"purpose": "register", "authorization_request_id": second_id},
    )
    assert response.status_code == 403
