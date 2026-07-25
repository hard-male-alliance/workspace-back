"""Authorization Server browser endpoints for public-client PKCE transactions."""

from __future__ import annotations

import html
from typing import Annotated
from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from backend.api.constants import PUBLIC_ORIGIN
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
DEMO_STYLES_PATH = "/oauth/demo.css"
_DEMO_FORM_MAX_BYTES = 8_192
_DEMO_LOCALES = ("zh-CN", "en-US")
_DEMO_STYLES = """
:root {
  color-scheme: light;
  font-family: "Noto Sans CJK SC", "Noto Sans SC", "Microsoft YaHei", system-ui, sans-serif;
  color: #172033;
  background: #f4f7fb;
}
* { box-sizing: border-box; }
body {
  min-height: 100vh;
  margin: 0;
  display: grid;
  place-items: center;
  padding: 24px;
  background:
    radial-gradient(circle at top left, #e8efff 0, transparent 42%),
    #f4f7fb;
}
main {
  width: min(100%, 440px);
  padding: 36px;
  border: 1px solid #dbe2ee;
  border-radius: 18px;
  background: #fff;
  box-shadow: 0 18px 55px rgb(33 50 84 / 12%);
}
h1 {
  margin: 0 0 8px;
  font-size: 28px;
  line-height: 1.35;
}
.subtitle {
  margin: 0 0 28px;
  color: #667085;
  font-size: 14px;
  line-height: 1.6;
}
form { display: grid; gap: 18px; }
label {
  display: grid;
  gap: 8px;
  color: #344054;
  font-size: 14px;
  font-weight: 600;
}
input {
  width: 100%;
  min-height: 44px;
  padding: 10px 12px;
  border: 1px solid #cfd7e6;
  border-radius: 9px;
  background: #fff;
  color: #172033;
  font: inherit;
  font-weight: 400;
  outline: none;
}
input:focus {
  border-color: #4f6bed;
  box-shadow: 0 0 0 3px rgb(79 107 237 / 14%);
}
button {
  min-height: 46px;
  margin-top: 4px;
  border: 0;
  border-radius: 9px;
  background: #3659d9;
  color: #fff;
  font: inherit;
  font-weight: 700;
  cursor: pointer;
}
button:hover { background: #2949c1; }
.alternate {
  margin: 24px 0 0;
  text-align: center;
  color: #667085;
  font-size: 14px;
}
a { color: #3659d9; font-weight: 600; text-decoration: none; }
a:hover { text-decoration: underline; }
[role="alert"] {
  margin: 0 0 20px;
  padding: 12px 14px;
  border: 1px solid #fecaca;
  border-radius: 9px;
  background: #fef2f2;
  color: #b42318;
  font-size: 14px;
}
@media (max-width: 520px) {
  body { padding: 14px; }
  main { padding: 26px 22px; border-radius: 14px; }
}
""".strip()

router_oauth = APIRouter()


def is_public_oauth_path(path: str) -> bool:
    """Identify endpoints owned by the Authorization Server rather than legacy identity."""

    return path in {
        AUTHORIZE_PATH,
        TOKEN_PATH,
        REVOKE_PATH,
        JWKS_PATH,
        DEMO_STYLES_PATH,
    } or path.startswith((AUTHORIZE_CONTINUE_PREFIX, AUTHORIZE_RESUME_PREFIX))


def _container(request: Request) -> BackendContainer:
    container = getattr(request.app.state, "container", None)
    if not isinstance(container, BackendContainer):
        raise RuntimeError("backend container is unavailable")
    return container


def _secure_browser_headers(
    response: Response,
    *,
    referrer_policy: str = "no-referrer",
    form_action_origin: str | None = None,
) -> Response:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    form_action = (
        f"'self' {form_action_origin}"
        if form_action_origin is not None
        else "'self'"
    )
    response.headers["Content-Security-Policy"] = (
        f"default-src 'none'; style-src 'self'; form-action {form_action}; "
        "frame-ancestors 'none'; base-uri 'none'"
    )
    response.headers["Referrer-Policy"] = referrer_policy
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _oauth_error_response(error: OAuthAuthorizationError) -> Response:
    if error.redirect_uri is not None:
        parsed = urlsplit(error.redirect_uri)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        query.extend(
            (key, value)
            for key, value in (
                ("error", error.error),
                ("state", error.state),
                ("iss", PUBLIC_ORIGIN),
            )
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


def _demo_page(
    authorization_request_id: str,
    csrf_token: str,
    *,
    purpose: str,
    email_value: str = "",
    error_message: str | None = None,
) -> str:
    signup = purpose == "register"
    title = "创建账号" if signup else "登录"
    submit_label = "创建账号" if signup else "登录"
    alternate_label = "已有账号？登录" if signup else "没有账号？创建账号"
    alternate_action = "login" if signup else "signup"
    error = (
        f'<p role="alert">{html.escape(error_message)}</p>'
        if error_message is not None
        else ""
    )
    confirmation = (
        '<label>确认密码<input name="confirm_password" type="password" '
        'required minlength="6" maxlength="1024" autocomplete="new-password"></label>'
        if signup
        else ""
    )
    autocomplete = "new-password" if signup else "current-password"
    minimum_password_length = 6 if signup else 1
    return (
        "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">"
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<link rel="stylesheet" href="{DEMO_STYLES_PATH}">'
        f"<title>{title}</title></head><body><main><h1>{title}</h1>"
        '<p class="subtitle">使用邮箱和密码继续进入本地 Demo 工作台</p>'
        f"{error}"
        f'<form method="post" action="{AUTHORIZE_CONTINUE_PREFIX}'
        f'{html.escape(authorization_request_id, quote=True)}">'
        f'<input type="hidden" name="csrf_token" value="{html.escape(csrf_token, quote=True)}">'
        f'<input type="hidden" name="purpose" value="{purpose}">'
        '<label>邮箱<input name="email" type="email" required maxlength="320" '
        f'autocomplete="email" value="{html.escape(email_value, quote=True)}"></label>'
        f'<label>密码<input name="password" type="password" required minlength="{minimum_password_length}" '
        f'maxlength="1024" autocomplete="{autocomplete}"></label>'
        f"{confirmation}<button type=\"submit\">{submit_label}</button></form>"
        f'<p class="alternate"><a href="{AUTHORIZE_CONTINUE_PREFIX}'
        f'{html.escape(authorization_request_id, quote=True)}?action={alternate_action}">'
        f"{alternate_label}</a></p></main></body></html>"
    )


@router_oauth.get(DEMO_STYLES_PATH, include_in_schema=False)
async def demo_styles() -> Response:
    """Serve the scriptless Demo identity page stylesheet from the OAuth origin."""

    return _secure_browser_headers(
        Response(_DEMO_STYLES, media_type="text/css; charset=utf-8")
    )


def _demo_error_message(error: HostedIdentityError) -> str:
    if error.code == "identity.credentials_invalid":
        return "邮箱或密码错误"
    if error.code in {"identity.password_policy", "identity.password_breached"}:
        return "密码至少需要 6 个字符"
    if error.code == "identity.identifier_invalid":
        return "请输入有效的邮箱地址"
    if error.code == "identity.flow_cannot_complete":
        return "无法创建账号，请检查输入或改用登录"
    return "请求无法完成，请刷新页面后重试"


def _url_origin(value: str) -> str:
    parsed = urlsplit(value)
    return f"{parsed.scheme}://{parsed.netloc}"


async def _read_demo_form(request: Request) -> dict[str, str]:
    content_type = request.headers.get("Content-Type", "")
    if content_type.partition(";")[0].strip().lower() != "application/x-www-form-urlencoded":
        raise HostedIdentityError(
            "identity.content_type_invalid", 415, "Form content type is invalid"
        )
    content_length = request.headers.get("Content-Length")
    if content_length is not None:
        if not content_length.isascii() or not content_length.isdigit():
            raise HostedIdentityError(
                "identity.content_length_invalid", 400, "Content length is invalid"
            )
        if int(content_length) > _DEMO_FORM_MAX_BYTES:
            raise HostedIdentityError(
                "identity.form_too_large", 413, "Form request is too large"
            )
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > _DEMO_FORM_MAX_BYTES:
            raise HostedIdentityError(
                "identity.form_too_large", 413, "Form request is too large"
            )
    try:
        decoded = body.decode("utf-8")
        parsed = parse_qs(
            decoded,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=5,
        )
    except UnicodeDecodeError, ValueError:
        raise HostedIdentityError(
            "identity.form_invalid", 400, "Form request is invalid"
        ) from None
    if any(len(values) != 1 for values in parsed.values()):
        raise HostedIdentityError("identity.form_invalid", 400, "Form request is invalid")
    return {key: values[0] for key, values in parsed.items()}


def _validate_demo_form_origin(request: Request) -> None:
    if request.headers.get("Origin") != PUBLIC_ORIGIN:
        raise HostedIdentityError("identity.origin_invalid", 403, "Origin validation failed")
    if request.headers.get("Sec-Fetch-Site") != "same-origin":
        raise HostedIdentityError(
            "identity.fetch_metadata_invalid", 403, "Fetch Metadata validation failed"
        )


def _preferred_demo_locale(request: Request) -> str:
    requested = request.headers.get("Accept-Language", "")
    for item in requested.split(","):
        candidate = item.partition(";")[0].strip().lower()
        for supported in _DEMO_LOCALES:
            if candidate == supported.lower() or candidate.partition("-")[0] == supported.partition("-")[0]:
                return supported
    return "zh-CN"


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
    service = _container(request).hosted_identity
    if service.demo_password_auth_enabled:
        action = request.query_params.get("action")
        purpose = (
            "register"
            if action == "signup" or (action is None and record.screen_hint == "signup")
            else "login"
        )
        binding = await service.begin_demo_browser_session(
            record.id,
            request.cookies.get(IDENTITY_BROWSER_COOKIE),
        )
        response = HTMLResponse(
            _demo_page(
                record.id,
                binding.csrf_token,
                purpose=purpose,
            ),
            status_code=200,
        )
    else:
        binding = await service.begin_browser_session(record.id)
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
    return _secure_browser_headers(
        response,
        # Fetch serializes Origin as null for native form POSTs under no-referrer.
        # ``origin`` reveals no path while preserving exact-Origin CSRF validation.
        referrer_policy="origin" if service.demo_password_auth_enabled else "no-referrer",
        form_action_origin=(
            _url_origin(record.redirect_uri)
            if service.demo_password_auth_enabled
            else None
        ),
    )


@router_oauth.post(
    f"{AUTHORIZE_CONTINUE_PREFIX}{{authorization_request_id}}", include_in_schema=False
)
async def submit_demo_authorization(
    request: Request,
    authorization_request_id: str,
) -> Response:
    """Submit the bounded, same-origin local Demo email/password form."""

    service = _container(request).hosted_identity
    if not service.demo_password_auth_enabled:
        return _secure_browser_headers(
            HTMLResponse(
                "<!doctype html><html><head><title>Not found</title></head>"
                "<body><h1>Not found</h1></body></html>",
                status_code=404,
            )
        )
    try:
        authorization = await _container(request).oauth.get_pending_authorization(
            authorization_request_id
        )
    except OAuthAuthorizationError:
        return _secure_browser_headers(
            HTMLResponse(
                "<!doctype html><html><head><title>Authorization request expired</title></head>"
                "<body><h1>Authorization request expired</h1></body></html>",
                status_code=410,
            )
        )
    form_action_origin = _url_origin(authorization.redirect_uri)
    purpose = "login"
    email_value = ""
    csrf_token = ""
    try:
        _validate_demo_form_origin(request)
        form = await _read_demo_form(request)
        purpose = form.get("purpose", "")
        email_value = form.get("email", "")
        csrf_token = form.get("csrf_token", "")
        password = form.get("password", "")
        confirm_password = form.get("confirm_password")
        expected_keys = (
            {"purpose", "email", "password", "confirm_password", "csrf_token"}
            if purpose == "register"
            else {"purpose", "email", "password", "csrf_token"}
        )
        if (
            set(form) != expected_keys
            or purpose not in {"register", "login"}
            or not csrf_token
            or len(csrf_token) > 256
            or len(email_value) > 320
            or len(password) > 1024
        ):
            raise HostedIdentityError(
                "identity.form_invalid", 400, "Form request is invalid"
            )
        if purpose == "register" and password != confirm_password:
            raise HostedIdentityError(
                "identity.password_confirmation", 400, "Password confirmation differs"
            )
        result = await service.submit_demo_password_flow(
            authorization_request_id,
            purpose=purpose,
            email=email_value,
            password=password,
            locale=_preferred_demo_locale(request),
            cookie_value=request.cookies.get(IDENTITY_BROWSER_COOKIE),
            csrf_token=csrf_token,
            device_name=request.headers.get("User-Agent"),
            network_identifier=(
                request.client.host if request.client is not None else "unknown"
            ),
        )
        if (
            result.login_cookie_value is None
            or result.flow.authorization_resume_uri is None
        ):
            raise HostedIdentityError(
                "identity.flow_invalid", 409, "Identity flow is incomplete"
            )
        response: Response = RedirectResponse(
            result.flow.authorization_resume_uri,
            status_code=303,
        )
        response.set_cookie(
            IDENTITY_LOGIN_COOKIE,
            result.login_cookie_value,
            secure=True,
            httponly=True,
            samesite="lax",
            path="/",
        )
        return _secure_browser_headers(
            response,
            form_action_origin=form_action_origin,
        )
    except HostedIdentityError as error:
        response = HTMLResponse(
            _demo_page(
                authorization_request_id,
                csrf_token,
                purpose=purpose if purpose in {"register", "login"} else "login",
                email_value=email_value,
                error_message=(
                    "两次输入的密码不一致"
                    if error.code == "identity.password_confirmation"
                    else _demo_error_message(error)
                ),
            ),
            status_code=error.status,
        )
        return _secure_browser_headers(
            response,
            referrer_policy="origin",
            form_action_origin=form_action_origin,
        )


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
