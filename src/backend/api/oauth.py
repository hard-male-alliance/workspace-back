"""Authorization Server browser endpoints for public-client PKCE transactions."""

from __future__ import annotations

import html
from typing import Annotated
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from backend.api.identity import IDENTITY_BROWSER_COOKIE, IDENTITY_LOGIN_COOKIE
from backend.application.oauth import OAuthAuthorizationError, OAuthTokenError
from backend.composition import BackendContainer
from backend.domain.identity import HostedIdentityError

AUTHORIZE_PATH = "/oauth/authorize"
AUTHORIZE_CONTINUE_PREFIX = "/oauth/authorize/continue/"
AUTHORIZE_RESUME_PREFIX = "/oauth/authorize/resume/"
TOKEN_PATH = "/oauth/token"
REVOKE_PATH = "/oauth/revoke"
JWKS_PATH = "/oauth/jwks"

router_oauth = APIRouter()


def is_public_oauth_path(path: str) -> bool:
    """Identify endpoints owned by the Authorization Server rather than legacy identity."""

    return path in {AUTHORIZE_PATH, TOKEN_PATH, REVOKE_PATH, JWKS_PATH} or path.startswith(
        (AUTHORIZE_CONTINUE_PREFIX, AUTHORIZE_RESUME_PREFIX)
    )


def _container(request: Request) -> BackendContainer:
    container = getattr(request.app.state, "container", None)
    if not isinstance(container, BackendContainer):
        raise RuntimeError("backend container is unavailable")
    return container


def _secure_browser_headers(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Content-Security-Policy"] = (
        "default-src 'none'; style-src 'self'; form-action 'self'; frame-ancestors 'none'; "
        "base-uri 'none'"
    )
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _oauth_error_response(error: OAuthAuthorizationError) -> Response:
    if error.redirect_uri is not None:
        parsed = urlsplit(error.redirect_uri)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        query.extend(
            (key, value)
            for key, value in (("error", error.error), ("state", error.state))
            if value is not None
        )
        return _secure_browser_headers(
            RedirectResponse(
                urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), "")),
                status_code=303,
            )
        )
    return _secure_browser_headers(
        HTMLResponse(
            "<!doctype html><html><head><title>Authorization request rejected</title></head>"
            "<body><h1>Authorization request rejected</h1></body></html>",
            status_code=400,
        )
    )


def _token_response(payload: dict[str, object], *, status_code: int = 200) -> JSONResponse:
    response = JSONResponse(payload, status_code=status_code)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


def _token_error_response(error: OAuthTokenError) -> JSONResponse:
    return _token_response(
        {"error": error.error, "error_description": error.description}, status_code=400
    )


@router_oauth.get(AUTHORIZE_PATH, include_in_schema=False)
async def authorize(
    request: Request,
    response_type: Annotated[str | None, Query(max_length=32)] = None,
    client_id: Annotated[str | None, Query(max_length=128)] = None,
    redirect_uri: Annotated[str | None, Query(max_length=2048)] = None,
    scope: Annotated[str | None, Query(max_length=2048)] = None,
    state: Annotated[str | None, Query(max_length=512)] = None,
    nonce: Annotated[str | None, Query(max_length=512)] = None,
    code_challenge: Annotated[str | None, Query(max_length=128)] = None,
    code_challenge_method: Annotated[str | None, Query(max_length=16)] = None,
    prompt: Annotated[str | None, Query(max_length=128)] = None,
    screen_hint: Annotated[str | None, Query(max_length=32)] = None,
) -> Response:
    """Validate and persist an Authorization Code + PKCE browser transaction."""

    try:
        record = await _container(request).oauth.begin_authorization(
            response_type=response_type,
            client_id=client_id,
            redirect_uri=redirect_uri,
            scope=scope,
            state=state,
            nonce=nonce,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            prompt=prompt,
            screen_hint=screen_hint,
        )
    except OAuthAuthorizationError as error:
        return _oauth_error_response(error)
    return _secure_browser_headers(
        RedirectResponse(f"{AUTHORIZE_CONTINUE_PREFIX}{record.id}", status_code=303)
    )


@router_oauth.get(
    f"{AUTHORIZE_CONTINUE_PREFIX}{{authorization_request_id}}", include_in_schema=False
)
async def continue_authorization(request: Request, authorization_request_id: str) -> Response:
    """Expose a same-origin, non-cacheable handoff point for the hosted identity UI."""

    try:
        record = await _container(request).oauth.get_pending_authorization(authorization_request_id)
    except OAuthAuthorizationError:
        return _secure_browser_headers(
            HTMLResponse(
                "<!doctype html><html><head><title>Authorization request expired</title></head>"
                "<body><h1>Authorization request expired</h1></body></html>",
                status_code=410,
            )
        )
    binding = await _container(request).hosted_identity.begin_browser_session(record.id)
    response = HTMLResponse(
        "<!doctype html><html><head><title>Continue authorization</title></head>"
        '<body><main id="authorization" data-authorization-request-id="'
        f"{html.escape(record.id, quote=True)}"
        '" data-csrf-token="'
        f"{html.escape(binding.csrf_token, quote=True)}"
        '"><h1>Continue authorization</h1></main></body></html>',
        status_code=200,
    )
    response.set_cookie(
        IDENTITY_BROWSER_COOKIE,
        binding.cookie_value,
        secure=True,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return _secure_browser_headers(response)


@router_oauth.get(f"{AUTHORIZE_RESUME_PREFIX}{{authorization_request_id}}", include_in_schema=False)
async def resume_authorization(request: Request, authorization_request_id: str) -> Response:
    """Resume only the original server-owned OAuth transaction after hosted authentication."""

    try:
        redirect_uri = await _container(request).hosted_identity.resume_authorization(
            authorization_request_id,
            request.cookies.get(IDENTITY_LOGIN_COOKIE),
        )
    except HostedIdentityError, OAuthAuthorizationError:
        return _secure_browser_headers(
            HTMLResponse(
                "<!doctype html><html><head><title>Authorization cannot resume</title></head>"
                "<body><h1>Authorization cannot resume</h1></body></html>",
                status_code=400,
            )
        )
    return _secure_browser_headers(RedirectResponse(redirect_uri, status_code=303))


@router_oauth.get(JWKS_PATH, include_in_schema=False)
async def jwks(request: Request) -> JSONResponse:
    """Publish current and overlap-period public signing keys."""

    return JSONResponse(
        _container(request).oauth.jwks,
        headers={"Cache-Control": "public, max-age=300"},
    )


@router_oauth.post(TOKEN_PATH, include_in_schema=False)
async def token(
    request: Request,
    grant_type: Annotated[str, Form(max_length=64)],
    client_id: Annotated[str | None, Form(max_length=128)] = None,
    code: Annotated[str | None, Form(max_length=512)] = None,
    redirect_uri: Annotated[str | None, Form(max_length=2048)] = None,
    code_verifier: Annotated[str | None, Form(max_length=256)] = None,
    refresh_token: Annotated[str | None, Form(max_length=512)] = None,
    client_secret: Annotated[str | None, Form(max_length=512)] = None,
) -> JSONResponse:
    """Exchange an authorization code or rotate a refresh token for a public client."""

    if request.headers.get("Authorization") is not None or client_secret is not None:
        return _token_error_response(
            OAuthTokenError("invalid_client", "Public clients must not send a client secret")
        )
    try:
        if grant_type == "authorization_code":
            payload = await _container(request).oauth.exchange_authorization_code(
                code=code,
                client_id=client_id,
                redirect_uri=redirect_uri,
                code_verifier=code_verifier,
            )
            _container(request).contracts_v2.validate_definition(
                "AuthorizationCodeTokenResponse", payload
            )
        elif grant_type == "refresh_token":
            payload = await _container(request).oauth.rotate_refresh_token(
                refresh_token=refresh_token,
                client_id=client_id,
            )
            _container(request).contracts_v2.validate_definition("RefreshTokenResponse", payload)
        else:
            raise OAuthTokenError("unsupported_grant_type", "grant_type is not supported")
    except OAuthTokenError as error:
        error_payload = {"error": error.error, "error_description": error.description}
        _container(request).contracts_v2.validate_definition("OAuthErrorResponse", error_payload)
        return _token_error_response(error)
    return _token_response(payload)


@router_oauth.post(REVOKE_PATH, include_in_schema=False)
async def revoke(
    request: Request,
    token: Annotated[str | None, Form(max_length=8192)] = None,
    token_type_hint: Annotated[str | None, Form(max_length=64)] = None,
    client_id: Annotated[str | None, Form(max_length=128)] = None,
) -> Response:
    """Revoke known tokens while returning 200 for unknown values as required by RFC 7009."""

    del token_type_hint, client_id
    await _container(request).oauth.revoke_token(token)
    return _token_response({})


__all__ = ["is_public_oauth_path", "router_oauth"]
