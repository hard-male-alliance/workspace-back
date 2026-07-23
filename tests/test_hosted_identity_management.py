"""Hosted login-session and authenticator management security tests."""

from __future__ import annotations

import re

from fastapi.testclient import TestClient

from backend.api.constants import PUBLIC_ORIGIN
from backend.api.identity import IDENTITY_BROWSER_COOKIE, IDENTITY_LOGIN_COOKIE

_PASSWORD = "correct horse battery staple"
_CSRF_PATTERN = re.compile(r'data-csrf-token="([A-Za-z0-9_-]+)"')


def _begin(client: TestClient, screen_hint: str) -> tuple[str, str, str]:
    response = client.get(
        "/oauth/authorize",
        params={
            "response_type": "code",
            "client_id": "aiws-web-local",
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "scope": "openid profile workspace.read",
            "state": f"state-{screen_hint}-manage",
            "nonce": f"nonce-{screen_hint}-manage",
            "code_challenge": "A" * 43,
            "code_challenge_method": "S256",
            "screen_hint": screen_hint,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    page = client.get(response.headers["location"])
    match = _CSRF_PATTERN.search(page.text)
    assert match is not None
    cookie = client.cookies.get(IDENTITY_BROWSER_COOKIE)
    assert cookie is not None
    return response.headers["location"].rsplit("/", 1)[-1], match.group(1), cookie


def _headers(csrf: str, browser: str) -> dict[str, str]:
    return {
        "Origin": PUBLIC_ORIGIN,
        "Sec-Fetch-Site": "same-origin",
        "X-CSRF-Token": csrf,
        "Cookie": f"{IDENTITY_BROWSER_COOKIE}={browser}",
    }


def _step(
    client: TestClient,
    flow_id: str,
    csrf: str,
    browser: str,
    body: dict[str, object],
):
    return client.post(
        f"/identity/v2/flows/{flow_id}/steps",
        headers=_headers(csrf, browser),
        json=body,
    )


def _register(client: TestClient) -> tuple[str, str, str]:
    request_id, csrf, browser = _begin(client, "signup")
    flow_id = client.post(
        "/identity/v2/flows",
        headers=_headers(csrf, browser),
        json={"purpose": "register", "authorization_request_id": request_id},
    ).json()["id"]
    for body in (
        {"kind": "identify", "step_id": "manage-identify", "identifier": "manage@example.test"},
        {
            "kind": "set_profile",
            "step_id": "manage-profile",
            "display_name": "Manager",
            "locale": "zh-CN",
            "terms_version": "v1",
            "privacy_version": "v1",
        },
        {"kind": "set_password", "step_id": "manage-password", "password": _PASSWORD},
        {"kind": "send_email_code", "step_id": "manage-send"},
    ):
        assert _step(client, flow_id, csrf, browser, body).status_code == 200
    code = client.app.state.container.hosted_identity.test_email_code(flow_id)
    assert (
        _step(
            client,
            flow_id,
            csrf,
            browser,
            {"kind": "verify_email_code", "step_id": "manage-code", "code": code},
        ).status_code
        == 200
    )
    assert (
        _step(
            client, flow_id, csrf, browser, {"kind": "complete", "step_id": "manage-complete"}
        ).status_code
        == 200
    )
    login_cookie = client.cookies.get(IDENTITY_LOGIN_COOKIE)
    assert login_cookie is not None
    return login_cookie, csrf, browser


def test_sessions_reauth_recovery_bundle_and_last_path_protection(
    backend_client: TestClient,
) -> None:
    login_cookie, _, _ = _register(backend_client)
    reauth_request, reauth_csrf, reauth_browser = _begin(backend_client, "login")
    combined_cookie = (
        f"__Host-aiws-authorization={reauth_browser}; {IDENTITY_LOGIN_COOKIE}={login_cookie}"
    )
    write_headers = {
        **_headers(reauth_csrf, reauth_browser),
        "Cookie": combined_cookie,
    }
    created = backend_client.post(
        "/identity/v2/flows",
        headers=write_headers,
        json={"purpose": "reauthenticate", "authorization_request_id": reauth_request},
    )
    assert created.status_code == 201, created.text
    flow_id = created.json()["id"]
    assert created.json()["allowed_steps"] == [
        "verify_password",
        "verify_recovery_code",
        "begin_passkey",
    ]
    verified = backend_client.post(
        f"/identity/v2/flows/{flow_id}/steps",
        headers=write_headers,
        json={"kind": "verify_password", "step_id": "reauth-password", "password": _PASSWORD},
    )
    assert verified.status_code == 200
    completed = backend_client.post(
        f"/identity/v2/flows/{flow_id}/steps",
        headers=write_headers,
        json={"kind": "complete", "step_id": "reauth-complete"},
    )
    assert completed.status_code == 200
    new_login_cookie = backend_client.cookies.get(IDENTITY_LOGIN_COOKIE)
    assert new_login_cookie is not None and new_login_cookie != login_cookie

    login_header = {"Cookie": f"{IDENTITY_LOGIN_COOKIE}={new_login_cookie}"}
    authenticators = backend_client.get("/identity/v2/authenticators", headers=login_header)
    assert authenticators.status_code == 200
    assert [item["kind"] for item in authenticators.json()["items"]] == ["password"]

    management_headers = {
        **_headers(reauth_csrf, reauth_browser),
        "Cookie": f"__Host-aiws-authorization={reauth_browser}; {IDENTITY_LOGIN_COOKIE}={new_login_cookie}",
    }
    bundle = backend_client.post(
        "/identity/v2/recovery-code-bundles",
        headers=management_headers,
        json={"reauthentication_flow_id": flow_id},
    )
    assert bundle.status_code == 201, bundle.text
    assert len(bundle.json()["recovery_codes"]) == 10
    recovery_authenticator_id = bundle.json()["authenticator_id"]
    listed = backend_client.get("/identity/v2/authenticators", headers=login_header)
    assert {item["kind"] for item in listed.json()["items"]} == {"password", "recovery_code"}
    assert "recovery_codes" not in listed.text

    removed = backend_client.delete(
        f"/identity/v2/authenticators/{recovery_authenticator_id}",
        headers={**management_headers, "X-Reauthentication-Flow-Id": flow_id},
    )
    assert removed.status_code == 204
    password_id = backend_client.get("/identity/v2/authenticators", headers=login_header).json()[
        "items"
    ][0]["id"]
    last_path = backend_client.delete(
        f"/identity/v2/authenticators/{password_id}",
        headers={**management_headers, "X-Reauthentication-Flow-Id": flow_id},
    )
    assert last_path.status_code == 409

    sessions = backend_client.get("/identity/v2/sessions", headers=login_header)
    assert sessions.status_code == 200
    current_items = [item for item in sessions.json()["items"] if item["current"]]
    assert len(current_items) == 1
    deleted = backend_client.delete(
        f"/identity/v2/sessions/{current_items[0]['id']}",
        headers=management_headers,
    )
    assert deleted.status_code == 204
