"""Same-origin hosted identity JSON API."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from backend.api.constants import PUBLIC_ORIGIN
from backend.composition import BackendContainer
from backend.domain.identity import HostedIdentityError

IDENTITY_PREFIX = "/identity/v2/"
IDENTITY_BROWSER_COOKIE = "__Host-aiws-authorization"
IDENTITY_LOGIN_COOKIE = "__Host-aiws-session"
IDENTITY_CSRF_HEADER = "X-CSRF-Token"

router_identity = APIRouter(prefix="/identity/v2", include_in_schema=False)


def is_hosted_identity_path(path: str) -> bool:
    """Return whether a route belongs to the cookie-authenticated hosted UI boundary."""

    return path.startswith(IDENTITY_PREFIX)


def secure_identity_headers[ResponseT: Response](response: ResponseT) -> ResponseT:
    """Prevent identity responses from being cached, framed, sniffed, or referred."""

    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Content-Security-Policy"] = (
        "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
    )
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _container(request: Request) -> BackendContainer:
    container = getattr(request.app.state, "container", None)
    if not isinstance(container, BackendContainer):
        raise RuntimeError("backend container is unavailable")
    return container


def _error(error: HostedIdentityError) -> JSONResponse:
    return secure_identity_headers(
        JSONResponse(
            {"error": error.code, "error_description": error.title},
            status_code=error.status,
        )
    )


def _validate_same_origin_write(request: Request) -> str:
    """Enforce exact Origin, Fetch Metadata, and a custom CSRF header."""

    if request.headers.get("Origin") != PUBLIC_ORIGIN:
        raise HostedIdentityError("identity.origin_invalid", 403, "Origin validation failed")
    if request.headers.get("Sec-Fetch-Site") != "same-origin":
        raise HostedIdentityError(
            "identity.fetch_metadata_invalid", 403, "Fetch Metadata validation failed"
        )
    token = request.headers.get(IDENTITY_CSRF_HEADER)
    if token is None or not token or len(token) > 256:
        raise HostedIdentityError("identity.csrf_invalid", 403, "CSRF validation failed")
    return token


@router_identity.post("/flows")
async def create_identity_flow(request: Request) -> JSONResponse:
    """Create a flow bound to the exact hosted browser and OAuth transaction."""

    try:
        csrf_token = _validate_same_origin_write(request)
        body = await request.json()
        _container(request).contracts_v2.validate_definition("CreateIdentityFlowRequest", body)
        flow = await _container(request).hosted_identity.create_flow(
            purpose=body["purpose"],
            authorization_request_id=body["authorization_request_id"],
            cookie_value=request.cookies.get(IDENTITY_BROWSER_COOKIE),
            csrf_token=csrf_token,
            login_cookie_value=request.cookies.get(IDENTITY_LOGIN_COOKIE),
        )
        payload = flow.as_public_dict()
        _container(request).contracts_v2.validate_definition("IdentityFlow", payload)
        return secure_identity_headers(JSONResponse(payload, status_code=201))
    except HostedIdentityError as error:
        return _error(error)
    except TypeError, ValueError:
        return _error(
            HostedIdentityError("identity.request_invalid", 400, "Identity request is invalid")
        )


@router_identity.get("/flows/{flow_id}")
async def get_identity_flow(request: Request, flow_id: str) -> JSONResponse:
    """Restore a secret-free projection for the same browser only."""

    try:
        flow = await _container(request).hosted_identity.get_flow(
            flow_id, cookie_value=request.cookies.get(IDENTITY_BROWSER_COOKIE)
        )
        payload = flow.as_public_dict()
        _container(request).contracts_v2.validate_definition("IdentityFlow", payload)
        return secure_identity_headers(JSONResponse(payload))
    except HostedIdentityError as error:
        return _error(error)


@router_identity.post("/flows/{flow_id}/steps")
async def submit_identity_step(request: Request, flow_id: str) -> JSONResponse:
    """Apply one schema-valid, server-allowed and step-id-deduplicated transition."""

    try:
        csrf_token = _validate_same_origin_write(request)
        body = await request.json()
        _container(request).contracts_v2.validate_definition("IdentityFlowStepRequest", body)
        result = await _container(request).hosted_identity.submit_step(
            flow_id,
            body,
            cookie_value=request.cookies.get(IDENTITY_BROWSER_COOKIE),
            csrf_token=csrf_token,
            device_name=request.headers.get("User-Agent"),
            network_identifier=request.client.host if request.client is not None else "unknown",
        )
        payload = result.flow.as_public_dict()
        _container(request).contracts_v2.validate_definition("IdentityFlow", payload)
        response = secure_identity_headers(JSONResponse(payload))
        if result.login_cookie_value is not None:
            response.set_cookie(
                IDENTITY_LOGIN_COOKIE,
                result.login_cookie_value,
                secure=True,
                httponly=True,
                samesite="lax",
                path="/",
            )
        return response
    except HostedIdentityError as error:
        return _error(error)
    except TypeError, ValueError:
        return _error(
            HostedIdentityError("identity.request_invalid", 400, "Identity request is invalid")
        )


@router_identity.get("/sessions")
async def list_identity_sessions(request: Request) -> JSONResponse:
    try:
        payload = await _container(request).hosted_identity.list_sessions(
            request.cookies.get(IDENTITY_LOGIN_COOKIE)
        )
        _container(request).contracts_v2.validate_definition("IdentitySessionList", payload)
        return secure_identity_headers(JSONResponse(payload))
    except HostedIdentityError as error:
        return _error(error)


@router_identity.delete("/sessions/{session_id}")
async def delete_identity_session(request: Request, session_id: str) -> Response:
    try:
        _validate_same_origin_write(request)
        revoked, current = await _container(request).hosted_identity.revoke_session(
            request.cookies.get(IDENTITY_LOGIN_COOKIE), session_id
        )
        if not revoked:
            raise HostedIdentityError(
                "identity.session_not_found", 404, "Login session was not found"
            )
        response = secure_identity_headers(Response(status_code=204))
        if current:
            response.delete_cookie(
                IDENTITY_LOGIN_COOKIE, path="/", secure=True, httponly=True, samesite="lax"
            )
        return response
    except HostedIdentityError as error:
        return _error(error)


@router_identity.get("/authenticators")
async def list_identity_authenticators(request: Request) -> JSONResponse:
    try:
        payload = await _container(request).hosted_identity.list_authenticators(
            request.cookies.get(IDENTITY_LOGIN_COOKIE)
        )
        _container(request).contracts_v2.validate_definition("AuthenticatorList", payload)
        return secure_identity_headers(JSONResponse(payload))
    except HostedIdentityError as error:
        return _error(error)


@router_identity.post("/recovery-code-bundles")
async def create_recovery_code_bundle(request: Request) -> JSONResponse:
    try:
        _validate_same_origin_write(request)
        body = await request.json()
        _container(request).contracts_v2.validate_definition(
            "CreateRecoveryCodeBundleRequest", body
        )
        payload = await _container(request).hosted_identity.create_recovery_code_bundle(
            request.cookies.get(IDENTITY_LOGIN_COOKIE), body["reauthentication_flow_id"]
        )
        _container(request).contracts_v2.validate_definition("RecoveryCodeBundle", payload)
        return secure_identity_headers(JSONResponse(payload, status_code=201))
    except HostedIdentityError as error:
        return _error(error)


@router_identity.delete("/authenticators/{authenticator_id}")
async def delete_identity_authenticator(request: Request, authenticator_id: str) -> Response:
    try:
        _validate_same_origin_write(request)
        reauth_flow_id = request.headers.get("X-Reauthentication-Flow-Id")
        if not reauth_flow_id:
            raise HostedIdentityError(
                "identity.reauthentication_required", 403, "Recent reauthentication is required"
            )
        revoked = await _container(request).hosted_identity.revoke_authenticator(
            request.cookies.get(IDENTITY_LOGIN_COOKIE), authenticator_id, reauth_flow_id
        )
        if not revoked:
            raise HostedIdentityError(
                "identity.authenticator_not_removable", 409, "Authenticator cannot be removed"
            )
        return secure_identity_headers(Response(status_code=204))
    except HostedIdentityError as error:
        return _error(error)


__all__ = [
    "IDENTITY_BROWSER_COOKIE",
    "IDENTITY_CSRF_HEADER",
    "IDENTITY_LOGIN_COOKIE",
    "is_hosted_identity_path",
    "router_identity",
    "secure_identity_headers",
]
