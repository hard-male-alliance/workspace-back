"""Cryptographic WebAuthn registration and authentication tests with a virtual authenticator."""

from __future__ import annotations

import base64
import hashlib
import json
import re

import cbor2
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi.testclient import TestClient

from backend.api.constants import PUBLIC_ORIGIN
from backend.api.identity import IDENTITY_BROWSER_COOKIE

_CSRF_PATTERN = re.compile(r'data-csrf-token="([A-Za-z0-9_-]+)"')
_ORIGIN = "https://api.hmalliances.org:8022"
_RP_ID = "api.hmalliances.org"
_PASSWORD = "correct horse battery staple"


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _begin(client: TestClient, screen_hint: str) -> tuple[str, str, str]:
    started = client.get(
        "/oauth/authorize",
        params={
            "response_type": "code",
            "client_id": "aiws-web-local",
            "redirect_uri": "https://app.hmalliances.org/oauth/callback",
            "scope": "openid profile workspace.read",
            "state": f"passkey-{screen_hint}-state",
            "nonce": f"passkey-{screen_hint}-nonce",
            "code_challenge": "A" * 43,
            "code_challenge_method": "S256",
            "screen_hint": screen_hint,
        },
        follow_redirects=False,
    )
    assert started.status_code == 303
    page = client.get(started.headers["location"])
    match = _CSRF_PATTERN.search(page.text)
    assert match is not None
    browser = client.cookies.get(IDENTITY_BROWSER_COOKIE)
    assert browser is not None
    return started.headers["location"].rsplit("/", 1)[-1], match.group(1), browser


def _headers(csrf: str, browser: str) -> dict[str, str]:
    return {
        "Origin": PUBLIC_ORIGIN,
        "Sec-Fetch-Site": "same-origin",
        "X-CSRF-Token": csrf,
        "Cookie": f"{IDENTITY_BROWSER_COOKIE}={browser}",
    }


def _step(client: TestClient, flow: str, csrf: str, browser: str, body: dict[str, object]):
    return client.post(
        f"/identity/v2/flows/{flow}/steps",
        headers=_headers(csrf, browser),
        json=body,
    )


def _registration_credential(
    challenge: str,
    private_key: ec.EllipticCurvePrivateKey,
    credential_id: bytes,
) -> dict[str, object]:
    client_data = json.dumps(
        {
            "type": "webauthn.create",
            "challenge": challenge,
            "origin": _ORIGIN,
            "crossOrigin": False,
        },
        separators=(",", ":"),
    ).encode()
    numbers = private_key.public_key().public_numbers()
    cose_key = cbor2.dumps(
        {1: 2, 3: -7, -1: 1, -2: numbers.x.to_bytes(32, "big"), -3: numbers.y.to_bytes(32, "big")}
    )
    auth_data = (
        hashlib.sha256(_RP_ID.encode()).digest()
        + bytes([0x45])
        + (0).to_bytes(4, "big")
        + bytes(16)
        + len(credential_id).to_bytes(2, "big")
        + credential_id
        + cose_key
    )
    attestation = cbor2.dumps({"fmt": "none", "authData": auth_data, "attStmt": {}})
    encoded_id = _b64(credential_id)
    return {
        "id": encoded_id,
        "rawId": encoded_id,
        "type": "public-key",
        "authenticatorAttachment": "platform",
        "response": {
            "clientDataJSON": _b64(client_data),
            "attestationObject": _b64(attestation),
            "transports": ["internal"],
            "publicKeyAlgorithm": -7,
        },
        "clientExtensionResults": {},
    }


def _authentication_credential(
    challenge: str,
    private_key: ec.EllipticCurvePrivateKey,
    credential_id: bytes,
    *,
    origin: str = _ORIGIN,
) -> dict[str, object]:
    client_data = json.dumps(
        {"type": "webauthn.get", "challenge": challenge, "origin": origin, "crossOrigin": False},
        separators=(",", ":"),
    ).encode()
    auth_data = hashlib.sha256(_RP_ID.encode()).digest() + bytes([0x05]) + (1).to_bytes(4, "big")
    signature = private_key.sign(
        auth_data + hashlib.sha256(client_data).digest(), ec.ECDSA(hashes.SHA256())
    )
    encoded_id = _b64(credential_id)
    return {
        "id": encoded_id,
        "rawId": encoded_id,
        "type": "public-key",
        "authenticatorAttachment": "platform",
        "response": {
            "clientDataJSON": _b64(client_data),
            "authenticatorData": _b64(auth_data),
            "signature": _b64(signature),
            "userHandle": None,
        },
        "clientExtensionResults": {},
    }


def test_passkey_registration_and_login_verify_real_webauthn_signature(
    backend_client: TestClient,
) -> None:
    private_key = ec.generate_private_key(ec.SECP256R1())
    # Assert the virtual authenticator uses an actual EC private key, never server material.
    assert private_key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    credential_id = b"virtual-authenticator-credential"
    request_id, csrf, browser = _begin(backend_client, "signup")
    flow = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(csrf, browser),
        json={"purpose": "register", "authorization_request_id": request_id},
    ).json()["id"]
    for body in (
        {"kind": "identify", "step_id": "passkey-identify", "identifier": "passkey@example.test"},
        {
            "kind": "set_profile",
            "step_id": "passkey-profile",
            "display_name": "Passkey User",
            "locale": "en-US",
            "terms_version": "v1",
            "privacy_version": "v1",
        },
        {"kind": "set_password", "step_id": "passkey-password", "password": _PASSWORD},
        {"kind": "send_email_code", "step_id": "passkey-send"},
    ):
        assert _step(backend_client, flow, csrf, browser, body).status_code == 200
    code = backend_client.app.state.container.hosted_identity.test_email_code(flow)
    assert (
        _step(
            backend_client,
            flow,
            csrf,
            browser,
            {"kind": "verify_email_code", "step_id": "passkey-email-code", "code": code},
        ).status_code
        == 200
    )
    begun = _step(
        backend_client,
        flow,
        csrf,
        browser,
        {"kind": "begin_passkey", "step_id": "passkey-begin-registration"},
    )
    assert begun.status_code == 200
    options = begun.json()["webauthn_options"]
    assert options["rp"]["id"] == _RP_ID
    assert options["authenticatorSelection"]["userVerification"] == "required"
    registered = _step(
        backend_client,
        flow,
        csrf,
        browser,
        {
            "kind": "finish_passkey",
            "step_id": "passkey-finish-registration",
            "credential": _registration_credential(
                options["challenge"], private_key, credential_id
            ),
        },
    )
    assert registered.status_code == 200, registered.text
    assert registered.json()["webauthn_options"] is None
    assert (
        _step(
            backend_client,
            flow,
            csrf,
            browser,
            {"kind": "complete", "step_id": "passkey-register-complete"},
        ).status_code
        == 200
    )

    login_request = _begin(backend_client, "login")
    # Use the latest browser binding and transaction returned by the second call.
    login_request_id, login_csrf, login_browser = login_request
    login_flow = backend_client.post(
        "/identity/v2/flows",
        headers=_headers(login_csrf, login_browser),
        json={"purpose": "login", "authorization_request_id": login_request_id},
    ).json()["id"]
    assert (
        _step(
            backend_client,
            login_flow,
            login_csrf,
            login_browser,
            {
                "kind": "identify",
                "step_id": "passkey-login-identify",
                "identifier": "passkey@example.test",
            },
        ).status_code
        == 200
    )
    authentication = _step(
        backend_client,
        login_flow,
        login_csrf,
        login_browser,
        {"kind": "begin_passkey", "step_id": "passkey-begin-authentication"},
    )
    challenge = authentication.json()["webauthn_options"]["challenge"]
    wrong_origin = _step(
        backend_client,
        login_flow,
        login_csrf,
        login_browser,
        {
            "kind": "finish_passkey",
            "step_id": "passkey-wrong-origin",
            "credential": _authentication_credential(
                challenge, private_key, credential_id, origin="https://evil.example"
            ),
        },
    )
    assert wrong_origin.status_code == 400
    valid = _step(
        backend_client,
        login_flow,
        login_csrf,
        login_browser,
        {
            "kind": "finish_passkey",
            "step_id": "passkey-valid-authentication",
            "credential": _authentication_credential(challenge, private_key, credential_id),
        },
    )
    assert valid.status_code == 200, valid.text
    assert valid.json()["allowed_steps"] == ["complete"]
