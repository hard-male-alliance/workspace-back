"""OAuth public-client registration, redirect, and PKCE authorization tests."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

from fastapi.testclient import TestClient


def _authorize_params(**overrides: str) -> dict[str, str]:
    params = {
        "response_type": "code",
        "client_id": "aiws-web-local",
        "redirect_uri": "https://app.hmalliances.org/oauth/callback",
        "scope": "openid profile offline_access workspace.read resume.read resume.write",
        "state": "state-test-0123456789",
        "nonce": "nonce-test-0123456789",
        "code_challenge": "A" * 43,
        "code_challenge_method": "S256",
        "prompt": "consent",
        "screen_hint": "login",
    }
    params.update(overrides)
    return params


def test_authorize_persists_a_pkce_transaction_and_hands_off_to_hosted_ui(
    backend_client: TestClient,
) -> None:
    response = backend_client.get(
        "/oauth/authorize", params=_authorize_params(), follow_redirects=False
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/oauth/authorize/continue/authreq_")
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert response.headers["referrer-policy"] == "no-referrer"

    continuation = backend_client.get(response.headers["location"])
    assert continuation.status_code == 200
    assert "Continue authorization" in continuation.text
    assert "authreq_" in continuation.text
    assert continuation.headers["cache-control"] == "no-store"


def test_unknown_client_and_unregistered_redirect_never_redirect(
    backend_client: TestClient,
) -> None:
    unknown = backend_client.get(
        "/oauth/authorize",
        params=_authorize_params(client_id="unregistered-client"),
        follow_redirects=False,
    )
    assert unknown.status_code == 400
    assert "location" not in unknown.headers

    unsafe_redirect = backend_client.get(
        "/oauth/authorize",
        params=_authorize_params(redirect_uri="https://attacker.example/callback"),
        follow_redirects=False,
    )
    assert unsafe_redirect.status_code == 400
    assert "location" not in unsafe_redirect.headers


def test_error_after_redirect_validation_returns_only_a_safe_oauth_redirect(
    backend_client: TestClient,
) -> None:
    response = backend_client.get(
        "/oauth/authorize",
        params=_authorize_params(code_challenge_method="plain"),
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = urlsplit(response.headers["location"])
    assert (location.scheme, location.netloc, location.path) == (
        "https",
        "app.hmalliances.org",
        "/oauth/callback",
    )
    assert parse_qs(location.query) == {
        "error": ["invalid_request"],
        "state": ["state-test-0123456789"],
    }
    assert "error_description" not in location.query


def test_offline_access_requires_explicit_consent_until_grants_are_implemented(
    backend_client: TestClient,
) -> None:
    response = backend_client.get(
        "/oauth/authorize",
        params=_authorize_params(prompt="login"),
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert parse_qs(urlsplit(response.headers["location"]).query)["error"] == [
        "consent_required"
    ]


def test_electron_loopback_registration_allows_only_the_ephemeral_port_to_vary(
    backend_client: TestClient,
) -> None:
    valid = backend_client.get(
        "/oauth/authorize",
        params=_authorize_params(
            client_id="aiws-electron-local",
            redirect_uri="http://127.0.0.1:49152/oauth/callback",
        ),
        follow_redirects=False,
    )
    assert valid.status_code == 303
    assert valid.headers["location"].startswith("/oauth/authorize/continue/authreq_")

    hostname_substitution = backend_client.get(
        "/oauth/authorize",
        params=_authorize_params(
            client_id="aiws-electron-local",
            redirect_uri="http://localhost:49152/oauth/callback",
        ),
        follow_redirects=False,
    )
    assert hostname_substitution.status_code == 400
    assert "location" not in hostname_substitution.headers


def test_authorization_endpoint_never_accepts_legacy_grants_or_missing_oidc_binding(
    backend_client: TestClient,
) -> None:
    for override in (
        {"response_type": "token"},
        {"response_type": "password"},
        {"scope": "workspace.read"},
        {"nonce": ""},
    ):
        response = backend_client.get(
            "/oauth/authorize",
            params=_authorize_params(**override),
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "error=" in response.headers["location"]
