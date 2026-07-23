"""OAuth Authorization Code, PKCE, token rotation, and revocation services."""

from __future__ import annotations

import base64
import hashlib
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from backend.config import OAuthPublicClientSettings, OAuthSettings
from backend.domain.oauth import (
    ACCESS_TOKEN_USER_ID_CLAIM,
    AuthorizationRequestRecord,
    OAuthTokenValidationError,
    RefreshTokenReuseDetected,
)
from backend.domain.ports import (
    OAuthAuthorizationRequestRepository,
    OAuthTokenIssuerVerifier,
)
from workspace_shared.ids import new_opaque_id

_PKCE_S256_PATTERN = re.compile(r"^[A-Za-z0-9_-]{43}$")
_PKCE_VERIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._~-]{43,128}$")
_PROMPT_VALUES = frozenset({"none", "login", "consent", "select_account"})
_SCREEN_HINT_VALUES = frozenset({"signup", "login", "recovery"})


@dataclass(frozen=True, slots=True)
class OAuthAuthorizationError(Exception):
    """A safe OAuth authorization error, optionally redirectable to a verified client URI."""

    error: str
    description: str
    redirect_uri: str | None = None
    state: str | None = None


@dataclass(frozen=True, slots=True)
class OAuthTokenError(Exception):
    """A cache-safe RFC 6749 token endpoint error."""

    error: str
    description: str


class OAuthAuthorizationService:
    """Own OAuth public-client authorization, token, rotation, and revocation state."""

    def __init__(
        self,
        repository: OAuthAuthorizationRequestRepository,
        settings: OAuthSettings,
        token_signer: OAuthTokenIssuerVerifier,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._token_signer = token_signer
        self._clients = {client.client_id: client for client in settings.public_clients}

    async def begin_authorization(
        self,
        *,
        response_type: str | None,
        client_id: str | None,
        redirect_uri: str | None,
        scope: str | None,
        state: str | None,
        nonce: str | None,
        code_challenge: str | None,
        code_challenge_method: str | None,
        prompt: str | None,
        screen_hint: str | None,
    ) -> AuthorizationRequestRecord:
        """Validate the request before persisting any browser-controlled state."""

        client = self._client(client_id)
        verified_redirect = self._verified_redirect(client, redirect_uri)
        safe_state = self._required_opaque(state, "state", verified_redirect, None)
        if response_type != "code":
            self._raise_redirectable(
                "unsupported_response_type",
                "response_type must be code",
                verified_redirect,
                safe_state,
            )
        requested_scopes = self._scopes(scope, client, verified_redirect, safe_state)
        safe_nonce = self._required_opaque(nonce, "nonce", verified_redirect, safe_state)
        if code_challenge_method != "S256" or not isinstance(code_challenge, str):
            self._raise_redirectable(
                "invalid_request",
                "PKCE code_challenge_method must be S256",
                verified_redirect,
                safe_state,
            )
        if _PKCE_S256_PATTERN.fullmatch(code_challenge) is None:
            self._raise_redirectable(
                "invalid_request",
                "PKCE S256 code_challenge is invalid",
                verified_redirect,
                safe_state,
            )
        prompt_values = self._prompt(prompt, verified_redirect, safe_state)
        if "offline_access" in requested_scopes and "consent" not in prompt_values:
            self._raise_redirectable(
                "consent_required",
                "offline_access requires prompt=consent until a durable grant exists",
                verified_redirect,
                safe_state,
            )
        if screen_hint is not None and screen_hint not in _SCREEN_HINT_VALUES:
            self._raise_redirectable(
                "invalid_request",
                "screen_hint is invalid",
                verified_redirect,
                safe_state,
            )
        created_at = datetime.now(UTC)
        record = AuthorizationRequestRecord(
            id=new_opaque_id("authreq"),
            client_id=client.client_id,
            redirect_uri=verified_redirect,
            scopes=requested_scopes,
            state=safe_state,
            nonce=safe_nonce,
            code_challenge=code_challenge,
            code_challenge_method="S256",
            prompt=prompt_values,
            screen_hint=screen_hint,
            status="pending",
            created_at=created_at,
            expires_at=created_at
            + timedelta(seconds=self._settings.authorization_request_ttl_seconds),
        )
        await self._repository.create_authorization_request(record)
        return record

    async def get_pending_authorization(self, request_id: str) -> AuthorizationRequestRecord:
        """Return a live pending transaction for the same-origin hosted authorization UI."""

        record = await self._repository.get_authorization_request(request_id)
        if record is None:
            raise OAuthAuthorizationError("invalid_request", "Authorization request was not found")
        if record.expires_at <= datetime.now(UTC) or record.status != "pending":
            raise OAuthAuthorizationError(
                "invalid_request", "Authorization request is no longer active"
            )
        return record

    @property
    def jwks(self) -> dict[str, list[dict[str, str]]]:
        """Return public signing keys without exposing private key material."""

        return self._token_signer.jwks

    async def complete_authorization(
        self,
        request_id: str,
        *,
        subject: str,
        user_id: str,
        login_session_id: str,
        auth_time: datetime | None = None,
    ) -> str:
        """Complete an authenticated hosted flow and bind its code to the login session."""

        if (
            not subject
            or len(subject) > 320
            or not user_id
            or len(user_id) > 128
            or not login_session_id
            or len(login_session_id) > 128
        ):
            raise OAuthAuthorizationError("server_error", "Authenticated subject is invalid")
        request = await self.get_pending_authorization(request_id)
        raw_code = f"code_{secrets.token_urlsafe(48)}"
        authenticated_at = auth_time or datetime.now(UTC)
        issued = await self._repository.issue_authorization_code(
            request_id,
            subject=subject,
            user_id=user_id,
            login_session_id=login_session_id,
            code_hash=_secret_hash(raw_code),
            auth_time=authenticated_at,
            expires_at=authenticated_at
            + timedelta(seconds=self._settings.authorization_code_ttl_seconds),
        )
        if not issued:
            raise OAuthAuthorizationError(
                "invalid_request", "Authorization request is no longer active"
            )
        parsed = urlsplit(request.redirect_uri)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        query.extend((("code", raw_code), ("state", request.state)))
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ""))

    async def exchange_authorization_code(
        self,
        *,
        code: str | None,
        client_id: str | None,
        redirect_uri: str | None,
        code_verifier: str | None,
    ) -> dict[str, Any]:
        """Atomically exchange one code, enforcing exact client/redirect and PKCE binding."""

        client, verified_redirect = self._token_client_and_redirect(client_id, redirect_uri)
        if (
            code is None
            or not code.startswith("code_")
            or len(code) > 512
            or code_verifier is None
            or _PKCE_VERIFIER_PATTERN.fullmatch(code_verifier) is None
        ):
            raise OAuthTokenError("invalid_grant", "Authorization code or verifier is invalid")
        refresh_token = f"rt_{secrets.token_urlsafe(48)}"
        now = datetime.now(UTC)
        exchange = await self._repository.exchange_authorization_code(
            _secret_hash(code),
            client_id=client.client_id,
            redirect_uri=verified_redirect,
            verifier_challenge=_pkce_s256(code_verifier),
            refresh_family_id=new_opaque_id("rtfam"),
            refresh_token_id=new_opaque_id("rt"),
            refresh_token_hash=_secret_hash(refresh_token),
            refresh_expires_at=now + timedelta(seconds=self._settings.refresh_token_ttl_seconds),
        )
        if exchange is None:
            raise OAuthTokenError("invalid_grant", "Authorization code is invalid or expired")
        access_token, _, _ = self._token_signer.issue_access_token(
            user_id=exchange.user_id,
            subject=exchange.subject,
            client_id=exchange.client_id,
            scopes=exchange.scopes,
            lifetime_seconds=self._settings.access_token_ttl_seconds,
        )
        payload: dict[str, Any] = {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": self._settings.access_token_ttl_seconds,
            "scope": " ".join(exchange.scopes),
            "id_token": self._token_signer.issue_id_token(
                subject=exchange.subject,
                client_id=exchange.client_id,
                nonce=exchange.nonce,
                lifetime_seconds=self._settings.access_token_ttl_seconds,
                auth_time=exchange.auth_time,
            ),
        }
        if exchange.refresh_family_id is not None:
            payload["refresh_token"] = refresh_token
        return payload

    async def rotate_refresh_token(
        self,
        *,
        refresh_token: str | None,
        client_id: str | None,
    ) -> dict[str, Any]:
        """Rotate a refresh token once and revoke its family if an ancestor is replayed."""

        client = self._clients.get(client_id or "")
        if client is None:
            raise OAuthTokenError("invalid_client", "Public client is not registered")
        if refresh_token is None or not refresh_token.startswith("rt_") or len(refresh_token) > 512:
            raise OAuthTokenError("invalid_grant", "Refresh token is invalid")
        replacement = f"rt_{secrets.token_urlsafe(48)}"
        now = datetime.now(UTC)
        try:
            rotation = await self._repository.rotate_refresh_token(
                _secret_hash(refresh_token),
                client_id=client.client_id,
                replacement_token_id=new_opaque_id("rt"),
                replacement_token_hash=_secret_hash(replacement),
                replacement_expires_at=now
                + timedelta(seconds=self._settings.refresh_token_ttl_seconds),
            )
        except RefreshTokenReuseDetected as error:
            raise OAuthTokenError(
                "invalid_grant", "Refresh token reuse revoked the token family"
            ) from error
        if rotation is None:
            raise OAuthTokenError("invalid_grant", "Refresh token is invalid or expired")
        access_token, _, _ = self._token_signer.issue_access_token(
            user_id=rotation.user_id,
            subject=rotation.subject,
            client_id=rotation.client_id,
            scopes=rotation.scopes,
            lifetime_seconds=self._settings.access_token_ttl_seconds,
        )
        return {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": self._settings.access_token_ttl_seconds,
            "scope": " ".join(rotation.scopes),
            "refresh_token": replacement,
        }

    async def revoke_token(self, token: str | None) -> None:
        """Revoke refresh families or access JTIs; unknown tokens intentionally remain successful."""

        if token is None or len(token) > 8192:
            return
        if token.startswith("rt_"):
            await self._repository.revoke_refresh_token(_secret_hash(token))
            return
        try:
            claims = self._token_signer.verify_access_token(token)
        except OAuthTokenValidationError:
            return
        await self._repository.revoke_access_token(
            claims["jti"], datetime.fromtimestamp(claims["exp"], UTC)
        )

    async def verify_access_token(self, token: str) -> dict[str, Any]:
        """@brief 验证 JTI 与用户级撤销 epoch / Verify both the JTI and user-level revocation epoch.

        @param token Resource Server Bearer JWT / Resource Server Bearer JWT.
        @return 已验证 claims / Verified claims.
        @raise OAuthTokenValidationError token 签名、JTI 或用户 epoch 已失效 / Invalid signature,
            revoked JTI, or revoked user epoch.
        """

        claims = self._token_signer.verify_access_token(token)
        if await self._repository.access_token_is_revoked(claims["jti"]):
            raise OAuthTokenValidationError("access token was revoked")
        issued_at = datetime.fromtimestamp(claims["iat"], UTC)
        if await self._repository.user_access_tokens_are_revoked(
            claims[ACCESS_TOKEN_USER_ID_CLAIM],
            issued_at,
        ):
            raise OAuthTokenValidationError("access token was revoked for its user")
        return claims

    def _token_client_and_redirect(
        self, client_id: str | None, redirect_uri: str | None
    ) -> tuple[OAuthPublicClientSettings, str]:
        client = self._clients.get(client_id or "")
        if client is None:
            raise OAuthTokenError("invalid_client", "Public client is not registered")
        try:
            verified = self._verified_redirect(client, redirect_uri)
        except OAuthAuthorizationError as error:
            raise OAuthTokenError("invalid_grant", "redirect_uri is invalid") from error
        return client, verified

    def _client(self, client_id: str | None) -> OAuthPublicClientSettings:
        if client_id is None or len(client_id) > 128:
            raise OAuthAuthorizationError("invalid_request", "client_id is required")
        client = self._clients.get(client_id)
        if client is None:
            raise OAuthAuthorizationError("unauthorized_client", "Client is not registered")
        return client

    @staticmethod
    def _verified_redirect(
        client: OAuthPublicClientSettings,
        redirect_uri: str | None,
    ) -> str:
        if redirect_uri is None or len(redirect_uri) > 2048:
            raise OAuthAuthorizationError("invalid_request", "redirect_uri is required")
        if client.client_type == "web" and redirect_uri in client.redirect_uris:
            return redirect_uri
        if client.client_type == "electron":
            for registered in client.redirect_uris:
                if redirect_uri == registered or _matches_loopback_redirect(
                    registered, redirect_uri
                ):
                    return redirect_uri
        raise OAuthAuthorizationError("invalid_request", "redirect_uri is not registered")

    @staticmethod
    def _required_opaque(
        value: str | None,
        name: str,
        redirect_uri: str,
        state: str | None,
    ) -> str:
        if (
            value is None
            or not value
            or len(value) > 512
            or any(ord(char) < 0x21 for char in value)
        ):
            raise OAuthAuthorizationError(
                "invalid_request",
                f"{name} is required and must be a bounded opaque value",
                redirect_uri,
                state,
            )
        return value

    @staticmethod
    def _scopes(
        raw_scope: str | None,
        client: OAuthPublicClientSettings,
        redirect_uri: str,
        state: str,
    ) -> tuple[str, ...]:
        scopes = tuple(raw_scope.split()) if raw_scope is not None else ()
        if not scopes or len(scopes) != len(set(scopes)) or "openid" not in scopes:
            raise OAuthAuthorizationError(
                "invalid_scope",
                "scope must include one copy of openid",
                redirect_uri,
                state,
            )
        if not set(scopes).issubset(client.allowed_scopes):
            raise OAuthAuthorizationError(
                "invalid_scope",
                "One or more requested scopes are not registered for this client",
                redirect_uri,
                state,
            )
        return scopes

    @staticmethod
    def _prompt(raw_prompt: str | None, redirect_uri: str, state: str) -> tuple[str, ...]:
        values = tuple(raw_prompt.split()) if raw_prompt else ()
        if (
            len(values) != len(set(values))
            or not set(values).issubset(_PROMPT_VALUES)
            or ("none" in values and len(values) != 1)
        ):
            raise OAuthAuthorizationError(
                "invalid_request", "prompt is invalid", redirect_uri, state
            )
        return values

    @staticmethod
    def _raise_redirectable(
        error: str,
        description: str,
        redirect_uri: str,
        state: str,
    ) -> NoReturn:
        raise OAuthAuthorizationError(error, description, redirect_uri, state)


def _matches_loopback_redirect(registered: str, actual: str) -> bool:
    """Apply the RFC 8252 exception: only the loopback port may vary."""

    try:
        registered_uri = urlsplit(registered)
        actual_uri = urlsplit(actual)
        actual_port = actual_uri.port
    except ValueError:
        return False
    return (
        registered_uri.scheme == actual_uri.scheme == "http"
        and registered_uri.hostname == actual_uri.hostname
        and registered_uri.hostname in {"127.0.0.1", "::1"}
        and registered_uri.port is None
        and actual_port is not None
        and registered_uri.path == actual_uri.path
        and registered_uri.query == actual_uri.query
        and not registered_uri.fragment
        and not actual_uri.fragment
        and actual_uri.username is None
        and actual_uri.password is None
    )


def _secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _pkce_s256(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


__all__ = ["OAuthAuthorizationError", "OAuthAuthorizationService", "OAuthTokenError"]
