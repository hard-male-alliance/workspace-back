"""Local-only Demo email/password forms through the real OAuth and Workspace boundary."""

from __future__ import annotations

import base64
import hashlib
import re
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import json5
import pytest
from fastapi.testclient import TestClient

from backend import config as backend_config
from backend.api.constants import PUBLIC_ORIGIN
from backend.api.identity import IDENTITY_BROWSER_COOKIE, IDENTITY_LOGIN_COOKIE
from backend.app import create_app
from backend.config import BackendSettings
from workspace_shared.jsonc import ConfigurationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_CSRF_PATTERN = re.compile(r'name="csrf_token" value="([A-Za-z0-9_-]+)"')
_PASSWORD = "correct horse battery staple"
_PKCE_VERIFIER = "v" * 43


def _pkce_challenge() -> str:
    return (
        base64.urlsafe_b64encode(
            hashlib.sha256(_PKCE_VERIFIER.encode("ascii")).digest()
        )
        .decode("ascii")
        .rstrip("=")
    )


@pytest.fixture
def demo_client() -> Iterator[TestClient]:
    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")
    settings = replace(
        settings,
        hosted_identity=replace(
            settings.hosted_identity,
            demo_password_auth_enabled=True,
        ),
        network=replace(
            settings.network,
            cors_allowed_origins=("http://127.0.0.1:5173",),
        ),
    )
    with TestClient(create_app(settings), raise_server_exceptions=False) as client:
        yield client


def _begin(
    client: TestClient,
    screen_hint: str | None,
    *,
    scopes: str = "openid profile email workspace.read workspace.write",
) -> tuple[str, str, str]:
    params = {
        "response_type": "code",
        "client_id": "aiws-web-local",
        "redirect_uri": "https://app.hmalliances.org/oauth/callback",
        "scope": scopes,
        "state": f"state-demo-{screen_hint or 'default'}",
        "nonce": f"nonce-demo-{screen_hint or 'default'}",
        "code_challenge": _pkce_challenge(),
        "code_challenge_method": "S256",
    }
    if screen_hint is not None:
        params["screen_hint"] = screen_hint
    started = client.get("/oauth/authorize", params=params, follow_redirects=False)
    assert started.status_code == 303
    page = client.get(started.headers["location"])
    assert page.status_code == 200
    csrf = _CSRF_PATTERN.search(page.text)
    assert csrf is not None
    browser_cookie = client.cookies.get(IDENTITY_BROWSER_COOKIE)
    assert browser_cookie is not None
    return started.headers["location"], csrf.group(1), browser_cookie


def _form_headers(browser_cookie: str) -> dict[str, str]:
    return {
        "Origin": PUBLIC_ORIGIN,
        "Sec-Fetch-Site": "same-origin",
        "Cookie": f"{IDENTITY_BROWSER_COOKIE}={browser_cookie}",
    }


def _register(
    client: TestClient,
    email: str,
) -> tuple[dict[str, object], str, str, str]:
    continue_uri, csrf, browser_cookie = _begin(client, "signup")
    response = client.post(
        continue_uri,
        headers=_form_headers(browser_cookie),
        data={
            "purpose": "register",
            "csrf_token": csrf,
            "email": email,
            "password": _PASSWORD,
            "confirm_password": _PASSWORD,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303, response.text
    assert response.headers["location"].startswith("/oauth/authorize/resume/")
    login_cookie = client.cookies.get(IDENTITY_LOGIN_COOKIE)
    assert login_cookie is not None
    callback = client.get(
        response.headers["location"],
        headers={"Cookie": f"{IDENTITY_LOGIN_COOKIE}={login_cookie}"},
        follow_redirects=False,
    )
    assert callback.status_code == 303
    callback_query = parse_qs(urlsplit(callback.headers["location"]).query)
    assert callback_query["iss"] == [PUBLIC_ORIGIN]
    token = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "aiws-web-local",
            "code": callback_query["code"][0],
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "code_verifier": _PKCE_VERIFIER,
        },
    )
    assert token.status_code == 200, token.text
    return token.json(), continue_uri, csrf, browser_cookie


def _authorization(token: dict[str, object], request_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token['access_token']}",
        "X-Request-Id": request_id,
    }


def test_demo_configuration_defaults_off_and_fails_closed_when_deployed(
) -> None:
    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")
    assert not settings.hosted_identity.demo_password_auth_enabled

    root = json5.loads((PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8"))
    root["hosted_identity"]["demo_password_auth_enabled"] = True
    for environment in ("staging", "production"):
        with pytest.raises(
            ConfigurationError,
            match="demo_password_auth_enabled is restricted",
        ):
            backend_config._hosted_identity_settings(
                root["hosted_identity"],
                environment,
            )


def test_demo_pages_are_scriptless_secure_and_follow_screen_hint(
    demo_client: TestClient,
) -> None:
    signup_uri, _, _ = _begin(demo_client, "signup")
    signup = demo_client.get(signup_uri)
    assert "<h1>创建账号</h1>" in signup.text
    assert 'name="confirm_password"' in signup.text
    assert 'rel="stylesheet" href="/oauth/demo.css"' in signup.text
    assert "<script" not in signup.text

    stylesheet = demo_client.get("/oauth/demo.css")
    assert stylesheet.status_code == 200
    assert stylesheet.headers["content-type"].startswith("text/css")
    assert "Noto Sans CJK SC" in stylesheet.text
    assert "display: grid" in stylesheet.text

    login_uri, _, _ = _begin(demo_client, "login")
    login = demo_client.get(login_uri)
    assert "<h1>登录</h1>" in login.text
    assert 'name="confirm_password"' not in login.text

    default_uri, _, _ = _begin(demo_client, None)
    default = demo_client.get(default_uri)
    assert "<h1>登录</h1>" in default.text
    for response in (signup, login, default):
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["referrer-policy"] == "origin"
        assert response.headers["x-content-type-options"] == "nosniff"
        assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
        assert (
            "form-action 'self' https://app.hmalliances.org"
            in response.headers["content-security-policy"]
        )


def test_demo_registration_is_unverified_and_replays_cannot_duplicate(
    demo_client: TestClient,
) -> None:
    token, continue_uri, csrf, browser_cookie = _register(
        demo_client,
        " Demo@Example.com ",
    )
    me = demo_client.get(
        "/api/v2/me",
        headers=_authorization(token, "req_demo_registered_me_0001"),
    )
    assert me.status_code == 200, me.text
    assert me.json()["email"] == "demo@example.com"
    assert me.json()["display_name"] == "demo@example.com"
    assert me.json()["email_verified"] is False
    assert me.json()["default_workspace_id"] is not None

    replay = demo_client.post(
        continue_uri,
        headers=_form_headers(browser_cookie),
        data={
            "purpose": "register",
            "csrf_token": csrf,
            "email": " Demo@Example.com ",
            "password": _PASSWORD,
            "confirm_password": _PASSWORD,
        },
    )
    assert replay.status_code == 410
    assert _PASSWORD not in replay.text

    duplicate_uri, duplicate_csrf, duplicate_cookie = _begin(demo_client, "signup")
    duplicate = demo_client.post(
        duplicate_uri,
        headers=_form_headers(duplicate_cookie),
        data={
            "purpose": "register",
            "csrf_token": duplicate_csrf,
            "email": "demo@example.com",
            "password": _PASSWORD,
            "confirm_password": _PASSWORD,
        },
    )
    assert duplicate.status_code == 409
    assert "无法创建账号" in duplicate.text
    assert _PASSWORD not in duplicate.text


def test_demo_form_rejects_cross_site_missing_csrf_and_wrong_password(
    demo_client: TestClient,
) -> None:
    continue_uri, csrf, browser_cookie = _begin(demo_client, "signup")
    body = {
        "purpose": "register",
        "csrf_token": csrf,
        "email": "security@example.test",
        "password": _PASSWORD,
        "confirm_password": _PASSWORD,
    }
    cross_site = demo_client.post(
        continue_uri,
        headers={
            **_form_headers(browser_cookie),
            "Origin": "https://evil.example",
        },
        data=body,
    )
    assert cross_site.status_code == 403

    missing_csrf = demo_client.post(
        continue_uri,
        headers=_form_headers(browser_cookie),
        data={key: value for key, value in body.items() if key != "csrf_token"},
    )
    assert missing_csrf.status_code == 400

    mismatch = demo_client.post(
        continue_uri,
        headers=_form_headers(browser_cookie),
        data={**body, "confirm_password": "different password value"},
    )
    assert mismatch.status_code == 400
    assert _PASSWORD not in mismatch.text


def test_demo_validation_failure_can_retry_the_same_form(
    demo_client: TestClient,
) -> None:
    register_uri, register_csrf, register_cookie = _begin(demo_client, "signup")
    weak_registration = demo_client.post(
        register_uri,
        headers=_form_headers(register_cookie),
        data={
            "purpose": "register",
            "csrf_token": register_csrf,
            "email": "retry@example.test",
            "password": "short",
            "confirm_password": "short",
        },
        follow_redirects=False,
    )
    assert weak_registration.status_code == 400
    assert "密码至少需要 6 个字符" in weak_registration.text

    retried_registration = demo_client.post(
        register_uri,
        headers=_form_headers(register_cookie),
        data={
            "purpose": "register",
            "csrf_token": register_csrf,
            "email": "retry@example.test",
            "password": _PASSWORD,
            "confirm_password": _PASSWORD,
        },
        follow_redirects=False,
    )
    assert retried_registration.status_code == 303, retried_registration.text

    login_uri, login_csrf, login_cookie = _begin(demo_client, "login")
    wrong_login = demo_client.post(
        login_uri,
        headers=_form_headers(login_cookie),
        data={
            "purpose": "login",
            "csrf_token": login_csrf,
            "email": "retry@example.test",
            "password": "incorrect password value",
        },
        follow_redirects=False,
    )
    assert wrong_login.status_code == 400
    assert "邮箱或密码错误" in wrong_login.text

    retried_login = demo_client.post(
        login_uri,
        headers=_form_headers(login_cookie),
        data={
            "purpose": "login",
            "csrf_token": login_csrf,
            "email": "retry@example.test",
            "password": _PASSWORD,
        },
        follow_redirects=False,
    )
    assert retried_login.status_code == 303, retried_login.text


def test_demo_login_and_two_accounts_keep_workspace_lists_isolated(
    demo_client: TestClient,
) -> None:
    token_a, _, _, _ = _register(demo_client, "account-a@example.test")
    me_a = demo_client.get(
        "/api/v2/me",
        headers=_authorization(token_a, "req_demo_me_a_0001"),
    ).json()
    workspaces_a = demo_client.get(
        "/api/v2/workspaces",
        headers=_authorization(token_a, "req_demo_workspaces_a_0001"),
    )
    assert workspaces_a.status_code == 200
    assert len(workspaces_a.json()["items"]) == 1

    token_b, _, _, _ = _register(demo_client, "account-b@example.test")
    me_b = demo_client.get(
        "/api/v2/me",
        headers=_authorization(token_b, "req_demo_me_b_0001"),
    ).json()
    assert me_a["default_workspace_id"] != me_b["default_workspace_id"]

    workspaces_b = demo_client.get(
        "/api/v2/workspaces",
        headers=_authorization(token_b, "req_demo_workspaces_b_0001"),
    )
    assert workspaces_b.status_code == 200
    assert [
        item["workspace"]["id"] for item in workspaces_b.json()["items"]
    ] == [me_b["default_workspace_id"]]

    cross_read = demo_client.get(
        f"/api/v2/workspaces/{me_a['default_workspace_id']}",
        headers=_authorization(token_b, "req_demo_cross_read_0001"),
    )
    assert cross_read.status_code in {403, 404}

    login_uri, login_csrf, login_browser_cookie = _begin(demo_client, "login")
    login = demo_client.post(
        login_uri,
        headers=_form_headers(login_browser_cookie),
        data={
            "purpose": "login",
            "csrf_token": login_csrf,
            "email": "ACCOUNT-A@EXAMPLE.TEST",
            "password": _PASSWORD,
        },
        follow_redirects=False,
    )
    assert login.status_code == 303, login.text
    assert "Secure" in login.headers["set-cookie"]
    assert "HttpOnly" in login.headers["set-cookie"]
    assert "SameSite=lax" in login.headers["set-cookie"]
