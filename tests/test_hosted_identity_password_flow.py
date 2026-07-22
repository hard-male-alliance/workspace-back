"""End-to-end registration and password login through hosted identity and OAuth."""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlsplit

from fastapi.testclient import TestClient

from backend.api.constants import PUBLIC_ORIGIN
from backend.api.identity import IDENTITY_BROWSER_COOKIE, IDENTITY_LOGIN_COOKIE

_CSRF_PATTERN = re.compile(r'data-csrf-token="([A-Za-z0-9_-]+)"')
_PASSWORD = "correct horse battery staple"


def _begin(client: TestClient, screen_hint: str) -> tuple[str, str, str]:
    response = client.get(
        "/oauth/authorize",
        params={
            "response_type": "code",
            "client_id": "aiws-web-local",
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "scope": "openid profile workspace.read",
            "state": f"state-{screen_hint}-012345",
            "nonce": f"nonce-{screen_hint}-012345",
            "code_challenge": "A" * 43,
            "code_challenge_method": "S256",
            "screen_hint": screen_hint,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    request_id = response.headers["location"].rsplit("/", 1)[-1]
    page = client.get(response.headers["location"])
    match = _CSRF_PATTERN.search(page.text)
    assert match is not None
    browser_cookie = client.cookies.get(IDENTITY_BROWSER_COOKIE)
    assert browser_cookie is not None
    return request_id, match.group(1), browser_cookie


def _headers(csrf: str, browser_cookie: str) -> dict[str, str]:
    return {
        "Origin": PUBLIC_ORIGIN,
        "Sec-Fetch-Site": "same-origin",
        "X-CSRF-Token": csrf,
        "Cookie": f"{IDENTITY_BROWSER_COOKIE}={browser_cookie}",
    }


def _step(
    client: TestClient,
    flow_id: str,
    csrf: str,
    browser_cookie: str,
    body: dict[str, object],
):
    return client.post(
        f"/identity/v2/flows/{flow_id}/steps",
        headers=_headers(csrf, browser_cookie),
        json=body,
    )


def test_registration_login_and_oauth_resume_are_complete_and_secret_safe(
    backend_client: TestClient,
) -> None:
    request_id, csrf, browser_cookie = _begin(backend_client, "signup")
    created = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(csrf, browser_cookie),
        json={"purpose": "register", "authorization_request_id": request_id},
    )
    flow_id = created.json()["id"]
    steps = [
        {"kind": "identify", "step_id": "step-identify", "identifier": "new@example.test"},
        {
            "kind": "set_profile",
            "step_id": "step-profile",
            "display_name": "New User",
            "locale": "zh-CN",
            "terms_version": "2026-07",
            "privacy_version": "2026-07",
        },
        {"kind": "set_password", "step_id": "step-password", "password": _PASSWORD},
        {"kind": "send_email_code", "step_id": "step-send-code"},
    ]
    for body in steps:
        response = _step(backend_client, flow_id, csrf, browser_cookie, body)
        assert response.status_code == 200, response.text
        assert _PASSWORD not in response.text
    code = backend_client.app.state.container.hosted_identity.test_email_code(flow_id)
    verified = _step(
        backend_client,
        flow_id,
        csrf,
        browser_cookie,
        {"kind": "verify_email_code", "step_id": "step-verify-code", "code": code},
    )
    assert verified.status_code == 200
    assert verified.json()["status"] == "verified"
    completed = _step(
        backend_client,
        flow_id,
        csrf,
        browser_cookie,
        {"kind": "complete", "step_id": "step-complete"},
    )
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert completed.json()["authorization_resume_uri"] == f"/oauth/authorize/resume/{request_id}"
    set_cookie = completed.headers["set-cookie"]
    assert "Secure" in set_cookie and "HttpOnly" in set_cookie and "SameSite=lax" in set_cookie
    login_cookie = backend_client.cookies.get(IDENTITY_LOGIN_COOKIE)
    assert login_cookie is not None
    resumed = backend_client.get(
        completed.json()["authorization_resume_uri"],
        headers={"Cookie": f"{IDENTITY_LOGIN_COOKIE}={login_cookie}"},
        follow_redirects=False,
    )
    assert resumed.status_code == 303
    query = parse_qs(urlsplit(resumed.headers["location"]).query)
    assert query["state"] == ["state-signup-012345"]
    assert query["code"][0].startswith("code_")

    login_request_id, login_csrf, login_browser = _begin(backend_client, "login")
    login_flow = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(login_csrf, login_browser),
        json={"purpose": "login", "authorization_request_id": login_request_id},
    ).json()
    identified = _step(
        backend_client,
        login_flow["id"],
        login_csrf,
        login_browser,
        {"kind": "identify", "step_id": "login-identify", "identifier": "new@example.test"},
    )
    assert identified.status_code == 200
    assert identified.json()["allowed_steps"] == [
        "verify_password",
        "verify_recovery_code",
        "begin_passkey",
    ]
    wrong = _step(
        backend_client,
        login_flow["id"],
        login_csrf,
        login_browser,
        {"kind": "verify_password", "step_id": "login-wrong", "password": "not the password"},
    )
    assert wrong.status_code == 400
    assert wrong.json()["error"] == "identity.credentials_invalid"
    authenticated = _step(
        backend_client,
        login_flow["id"],
        login_csrf,
        login_browser,
        {"kind": "verify_password", "step_id": "login-password", "password": _PASSWORD},
    )
    assert authenticated.status_code == 200
    assert authenticated.json()["allowed_steps"] == ["complete"]


def test_identity_step_schema_password_policy_and_step_id_dedupe(
    backend_client: TestClient,
) -> None:
    request_id, csrf, browser_cookie = _begin(backend_client, "signup")
    flow = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(csrf, browser_cookie),
        json={"purpose": "register", "authorization_request_id": request_id},
    ).json()
    identify = {"kind": "identify", "step_id": "same-step", "identifier": "dedupe@example.test"}
    first = _step(backend_client, flow["id"], csrf, browser_cookie, identify)
    second = _step(backend_client, flow["id"], csrf, browser_cookie, identify)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    profile = {
        "kind": "set_profile",
        "step_id": "profile-step",
        "display_name": "Dedupe",
        "locale": "en-US",
        "terms_version": "v1",
        "privacy_version": "v1",
    }
    assert _step(backend_client, flow["id"], csrf, browser_cookie, profile).status_code == 200
    short = _step(
        backend_client,
        flow["id"],
        csrf,
        browser_cookie,
        {"kind": "set_password", "step_id": "short-password", "password": "too-short"},
    )
    assert short.status_code == 422
    breached = _step(
        backend_client,
        flow["id"],
        csrf,
        browser_cookie,
        {"kind": "set_password", "step_id": "breached-password", "password": "passwordpassword"},
    )
    assert breached.status_code == 400


def test_account_recovery_rotates_password_and_revokes_old_login_session(
    backend_client: TestClient,
) -> None:
    # Establish an account and retain its pre-recovery login Cookie.
    request_id, csrf, browser_cookie = _begin(backend_client, "signup")
    flow_id = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(csrf, browser_cookie),
        json={"purpose": "register", "authorization_request_id": request_id},
    ).json()["id"]
    registration_steps = [
        {"kind": "identify", "step_id": "register-identify", "identifier": "recover@example.test"},
        {
            "kind": "set_profile",
            "step_id": "register-profile",
            "display_name": "Recover",
            "locale": "en-US",
            "terms_version": "v1",
            "privacy_version": "v1",
        },
        {"kind": "set_password", "step_id": "register-password", "password": _PASSWORD},
        {"kind": "send_email_code", "step_id": "register-send"},
    ]
    for body in registration_steps:
        assert _step(backend_client, flow_id, csrf, browser_cookie, body).status_code == 200
    registration_code = backend_client.app.state.container.hosted_identity.test_email_code(flow_id)
    assert (
        _step(
            backend_client,
            flow_id,
            csrf,
            browser_cookie,
            {"kind": "verify_email_code", "step_id": "register-code", "code": registration_code},
        ).status_code
        == 200
    )
    registered = _step(
        backend_client,
        flow_id,
        csrf,
        browser_cookie,
        {"kind": "complete", "step_id": "register-complete"},
    )
    old_login_cookie = backend_client.cookies.get(IDENTITY_LOGIN_COOKIE)
    assert registered.status_code == 200 and old_login_cookie is not None

    recovery_request, recovery_csrf, recovery_browser = _begin(backend_client, "recovery")
    recovery_flow = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(recovery_csrf, recovery_browser),
        json={"purpose": "recover", "authorization_request_id": recovery_request},
    ).json()["id"]
    assert (
        _step(
            backend_client,
            recovery_flow,
            recovery_csrf,
            recovery_browser,
            {"kind": "identify", "step_id": "recover-id", "identifier": "recover@example.test"},
        ).status_code
        == 200
    )
    assert (
        _step(
            backend_client,
            recovery_flow,
            recovery_csrf,
            recovery_browser,
            {"kind": "send_email_code", "step_id": "recover-send"},
        ).status_code
        == 200
    )
    recovery_code = backend_client.app.state.container.hosted_identity.test_email_code(
        recovery_flow
    )
    verified = _step(
        backend_client,
        recovery_flow,
        recovery_csrf,
        recovery_browser,
        {"kind": "verify_email_code", "step_id": "recover-code", "code": recovery_code},
    )
    assert verified.status_code == 200
    assert verified.json()["allowed_steps"] == ["set_password"]
    new_password = "a newly rotated secure password"
    assert (
        _step(
            backend_client,
            recovery_flow,
            recovery_csrf,
            recovery_browser,
            {"kind": "set_password", "step_id": "recover-password", "password": new_password},
        ).status_code
        == 200
    )
    recovered = _step(
        backend_client,
        recovery_flow,
        recovery_csrf,
        recovery_browser,
        {"kind": "complete", "step_id": "recover-complete"},
    )
    assert recovered.status_code == 200, recovered.text

    # A separate pending transaction cannot be resumed with the revoked old session.
    pending_request, _, _ = _begin(backend_client, "login")
    stale = backend_client.get(
        f"/oauth/authorize/resume/{pending_request}",
        headers={"Cookie": f"{IDENTITY_LOGIN_COOKIE}={old_login_cookie}"},
        follow_redirects=False,
    )
    assert stale.status_code == 400
