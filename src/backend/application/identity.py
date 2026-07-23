"""Hosted identity browser binding, credentials, and finite-state flows."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from webauthn import (
    generate_authentication_options,
    generate_registration_options,
    options_to_json,
    verify_authentication_response,
    verify_registration_response,
)
from webauthn.helpers.exceptions import WebAuthnException
from webauthn.helpers.structs import (
    AuthenticatorSelectionCriteria,
    ResidentKeyRequirement,
    UserVerificationRequirement,
)

from backend.application.oauth import OAuthAuthorizationService
from backend.domain.identity import (
    HostedIdentityError,
    IdentityAuthenticatorRecord,
    IdentityBrowserSessionRecord,
    IdentityFlowRecord,
    IdentitySessionRecord,
    IdentityUserRecord,
)
from backend.domain.ports import (
    BreachedPasswordChecker,
    HostedIdentityRepository,
    IdentityEmailEnqueueError,
    IdentityEmailRateLimitExceeded,
    IdentityEmailSender,
)
from backend.domain.principals import UserId
from workspace_shared.ids import new_opaque_id

_EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_COMMON_BREACHED_PASSWORDS = frozenset(
    {"123456789012345", "passwordpassword", "qwertyuiopasdfgh", "letmeinletmein123"}
)
_FAKE_PASSWORD_VERIFIER = (
    "scrypt$32768$8$1$MDAwMDAwMDAwMDAwMDAwMA$tlFvZIzc7yQTGbP_MgcOEFyf2cg1JGmwvEFGSRfQCAI"
)


@dataclass(frozen=True, slots=True)
class BrowserBinding:
    """Opaque values returned only to the same-origin authorization page."""

    cookie_value: str
    csrf_token: str


@dataclass(frozen=True, slots=True)
class IdentityStepResult:
    """A public flow plus an optional freshly rotated login Cookie value."""

    flow: IdentityFlowRecord
    login_cookie_value: str | None = None


class HostedIdentityService:
    """Create and advance identity flows bound to one OAuth browser transaction."""

    def __init__(
        self,
        repository: HostedIdentityRepository,
        oauth: OAuthAuthorizationService,
        email_sender: IdentityEmailSender,
        *,
        breached_password_checker: BreachedPasswordChecker | None = None,
        lifetime_seconds: int = 600,
        email_code_ttl_seconds: int = 600,
        email_code_max_attempts: int = 5,
        email_send_limit_per_hour: int = 5,
        session_idle_ttl_seconds: int = 1_800,
        session_absolute_ttl_seconds: int = 2_592_000,
        recent_reauthentication_seconds: int = 300,
        allow_test_email_codes: bool = False,
    ) -> None:
        self._repository = repository
        self._oauth = oauth
        self._email_sender = email_sender
        self._breached_password_checker = breached_password_checker
        self._lifetime_seconds = lifetime_seconds
        self._email_code_ttl_seconds = email_code_ttl_seconds
        self._email_code_max_attempts = email_code_max_attempts
        self._email_send_limit_per_hour = email_send_limit_per_hour
        self._session_idle_ttl_seconds = session_idle_ttl_seconds
        self._session_absolute_ttl_seconds = session_absolute_ttl_seconds
        self._recent_reauthentication_seconds = recent_reauthentication_seconds
        self._allow_test_email_codes = allow_test_email_codes
        self._test_email_codes: dict[str, str] = {}
        self._step_lock = asyncio.Lock()

    async def verify_recent(
        self,
        user_id: UserId,
        flow_id: str,
        verified_at: datetime,
    ) -> bool:
        """@brief 验证绑定用户的近期重新认证证明 / Verify a recent user-bound reauth proof.

        @param user_id 已由 access token 认证的本地用户 / Local user authenticated by the
            access token.
        @param flow_id 客户端提交的重新认证流程标识 / Submitted reauthentication-flow ID.
        @param verified_at 应用判定证明的时刻 / Instant at which the application evaluates
            the proof.
        @return 仅当流程已完成、属于该用户且完成证明仍在窗口内时为真 / True only when
            the flow is completed, belongs to the user, and its completion proof is still recent.

        @note 无效、旧版无完成时刻或未来完成时刻的记录均 fail closed / Invalid records,
            legacy records without a completion instant, and future completion instants fail closed.
        """
        if verified_at.tzinfo is None or verified_at.utcoffset() is None:
            return False
        flow = await self._repository.get_flow(flow_id)
        if (
            flow is None
            or flow.purpose != "reauthenticate"
            or flow.status != "completed"
            or flow.user_id != str(user_id)
            or flow.completed_at is None
            or flow.completed_at > verified_at
        ):
            return False
        return verified_at < flow.completed_at + timedelta(
            seconds=self._recent_reauthentication_seconds
        )

    async def begin_browser_session(self, authorization_request_id: str) -> BrowserBinding:
        """Create an opaque HttpOnly-cookie binding and one-time CSRF capability."""

        authorization = await self._oauth.get_pending_authorization(authorization_request_id)
        now = datetime.now(UTC)
        session_id = new_opaque_id("idsess")
        secret = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(32)
        await self._repository.create_browser_session(
            IdentityBrowserSessionRecord(
                id=session_id,
                authorization_request_id=authorization.id,
                browser_secret_hash=_secret_hash(secret),
                csrf_token_hash=_secret_hash(csrf_token),
                user_id=None,
                created_at=now,
                last_seen_at=now,
                expires_at=min(
                    authorization.expires_at,
                    now + timedelta(seconds=self._lifetime_seconds),
                ),
            )
        )
        return BrowserBinding(f"{session_id}.{secret}", csrf_token)

    async def create_flow(
        self,
        *,
        purpose: str,
        authorization_request_id: str,
        cookie_value: str | None,
        csrf_token: str | None,
        login_cookie_value: str | None = None,
    ) -> IdentityFlowRecord:
        """Create a flow only for the browser and OAuth request that initiated it."""

        browser = await self._verified_browser(cookie_value, csrf_token=csrf_token)
        authorization = await self._oauth.get_pending_authorization(authorization_request_id)
        if browser.authorization_request_id != authorization.id:
            raise HostedIdentityError(
                "identity.binding_invalid", 403, "Identity flow binding is invalid"
            )
        expected_purpose = {
            "signup": "register",
            "login": "login",
            "recovery": "recover",
            None: "login",
        }[authorization.screen_hint]
        if purpose not in {"register", "login", "recover", "reauthenticate"}:
            raise HostedIdentityError(
                "identity.purpose_invalid", 400, "Identity flow purpose is invalid"
            )
        if purpose != expected_purpose and purpose != "reauthenticate":
            raise HostedIdentityError(
                "identity.purpose_mismatch",
                400,
                "Identity flow purpose does not match the authorization request",
            )
        now = datetime.now(UTC)
        flow_user_id: str | None = None
        internal_state: dict[str, object] | None = None
        initial_steps: tuple[str, ...] = ("identify",)
        if purpose == "reauthenticate":
            login_session = await self.authenticate_login_cookie(
                login_cookie_value, expected_client_id=authorization.client_id
            )
            flow_user_id = login_session.user_id
            internal_state = {"rotated_session_id": login_session.id}
            initial_steps = ("verify_password", "verify_recovery_code", "begin_passkey")
            await self._repository.bind_browser_user(browser.id, login_session.user_id)
        flow = IdentityFlowRecord(
            id=new_opaque_id("idflow"),
            purpose=purpose,
            status="pending",
            allowed_steps=initial_steps,
            authorization_request_id=authorization.id,
            browser_session_id=browser.id,
            client_id=authorization.client_id,
            redirect_uri=authorization.redirect_uri,
            code_challenge=authorization.code_challenge,
            created_at=now,
            expires_at=min(authorization.expires_at, browser.expires_at),
            user_id=flow_user_id,
            internal_state=internal_state,
        )
        await self._repository.create_flow(flow)
        return flow

    async def get_flow(self, flow_id: str, *, cookie_value: str | None) -> IdentityFlowRecord:
        """Restore only a flow belonging to the current browser binding."""

        browser = await self._verified_browser(cookie_value)
        flow = await self._repository.get_flow(flow_id)
        if flow is None or flow.browser_session_id != browser.id:
            raise HostedIdentityError("identity.flow_not_found", 404, "Identity flow was not found")
        if flow.expires_at <= datetime.now(UTC):
            raise HostedIdentityError("identity.flow_expired", 410, "Identity flow has expired")
        return flow

    async def submit_step(
        self,
        flow_id: str,
        body: dict[str, object],
        *,
        cookie_value: str | None,
        csrf_token: str | None,
        device_name: str | None,
        network_identifier: str,
    ) -> IdentityStepResult:
        """Validate and apply exactly one server-allowed identity transition."""

        browser = await self._verified_browser(cookie_value, csrf_token=csrf_token)
        async with self._step_lock:
            flow = await self.get_flow(flow_id, cookie_value=cookie_value)
            kind, step_id = str(body["kind"]), str(body["step_id"])
            processed_kind = await self._repository.processed_step_kind(flow.id, step_id)
            if processed_kind is not None:
                if processed_kind != kind:
                    raise HostedIdentityError(
                        "identity.step_id_reused", 409, "Identity step ID was already used"
                    )
                return IdentityStepResult(flow)
            if kind not in flow.allowed_steps:
                raise HostedIdentityError(
                    "identity.step_not_allowed", 409, "Identity step is not currently allowed"
                )
            if kind == "identify":
                return IdentityStepResult(
                    await self._identify(flow, browser, step_id, str(body["identifier"]))
                )
            if kind == "set_profile":
                return IdentityStepResult(
                    await self._transition(
                        flow,
                        browser,
                        step_id,
                        kind,
                        allowed_steps=("set_password",),
                        state_updates={
                            "display_name": str(body["display_name"]),
                            "locale": str(body["locale"]),
                            "terms_version": str(body["terms_version"]),
                            "privacy_version": str(body["privacy_version"]),
                        },
                    )
                )
            if kind == "set_password":
                password = str(body["password"])
                await _validate_new_password(password, self._breached_password_checker)
                next_steps = ("complete",) if flow.purpose == "recover" else ("send_email_code",)
                return IdentityStepResult(
                    await self._transition(
                        flow,
                        browser,
                        step_id,
                        kind,
                        allowed_steps=next_steps,
                        state_updates={"password_verifier": _password_hash(password)},
                    )
                )
            if kind == "send_email_code":
                code = f"{secrets.randbelow(1_000_000):06d}"
                now = datetime.now(UTC)
                state = flow.internal_state or {}
                attempts = _state_int(state, "email_code_attempts")
                recipient = state.get("identifier")
                if not isinstance(recipient, str):
                    raise HostedIdentityError(
                        "identity.flow_invalid", 409, "Identity flow is incomplete"
                    )
                try:
                    async with self._email_sender.atomic():
                        updated = await self._transition(
                            flow,
                            browser,
                            step_id,
                            kind,
                            allowed_steps=("send_email_code", "verify_email_code"),
                            state_updates={
                                "email_code_hash": _secret_hash(code),
                                "email_code_expires_at": int(
                                    (
                                        now
                                        + timedelta(seconds=self._email_code_ttl_seconds)
                                    ).timestamp()
                                ),
                                "email_code_attempts": attempts,
                            },
                        )
                        await self._email_sender.send_verification_code(
                            recipient,
                            code,
                            browser_session_id=browser.id,
                            network_identifier=network_identifier,
                            limit_per_hour=self._email_send_limit_per_hour,
                        )
                except IdentityEmailRateLimitExceeded as error:
                    raise HostedIdentityError(
                        "identity.rate_limited",
                        429,
                        "Verification delivery is temporarily limited",
                    ) from error
                except IdentityEmailEnqueueError as error:
                    raise HostedIdentityError(
                        "identity.delivery_unavailable",
                        503,
                        "Verification delivery is temporarily unavailable",
                    ) from error
                if self._allow_test_email_codes:
                    self._test_email_codes[flow.id] = code
                return IdentityStepResult(updated)
            if kind == "verify_email_code":
                return IdentityStepResult(
                    await self._verify_email_code(flow, browser, step_id, str(body["code"]))
                )
            if kind == "verify_password":
                return IdentityStepResult(
                    await self._verify_password(flow, browser, step_id, str(body["password"]))
                )
            if kind == "verify_recovery_code":
                return IdentityStepResult(
                    await self._verify_recovery_code(
                        flow, browser, step_id, str(body["recovery_code"])
                    )
                )
            if kind == "begin_passkey":
                return IdentityStepResult(await self._begin_passkey(flow, browser, step_id))
            if kind == "finish_passkey":
                credential = body.get("credential")
                if not isinstance(credential, dict):
                    raise HostedIdentityError(
                        "identity.passkey_invalid", 400, "Passkey response is invalid"
                    )
                return IdentityStepResult(
                    await self._finish_passkey(flow, browser, step_id, credential)
                )
            if kind == "complete":
                return await self._complete(flow, browser, step_id, device_name=device_name)
            raise HostedIdentityError(
                "identity.step_unavailable", 409, "Identity step is not available"
            )

    def test_email_code(self, flow_id: str) -> str:
        """Return a development code without logging it or exposing an HTTP route."""

        if not self._allow_test_email_codes or flow_id not in self._test_email_codes:
            raise RuntimeError("test email delivery is unavailable")
        return self._test_email_codes[flow_id]

    async def authenticate_login_cookie(
        self, cookie_value: str | None, *, expected_client_id: str | None = None
    ) -> IdentitySessionRecord:
        """Verify a hashed login Cookie and idle plus absolute lifetimes."""

        if cookie_value is None:
            raise HostedIdentityError("identity.session_required", 401, "Login session is required")
        session_id, separator, secret = cookie_value.partition(".")
        record = await self._repository.get_login_session(session_id)
        now = datetime.now(UTC)
        if (
            separator != "."
            or not secret
            or record is None
            or record.revoked_at is not None
            or record.idle_expires_at <= now
            or record.absolute_expires_at <= now
            or (expected_client_id is not None and record.client_id != expected_client_id)
            or not hmac.compare_digest(record.session_secret_hash, _secret_hash(secret))
        ):
            raise HostedIdentityError("identity.session_invalid", 401, "Login session is invalid")
        return record

    async def resume_authorization(self, request_id: str, cookie_value: str | None) -> str:
        """Turn an authenticated hosted session into the original one-time OAuth code redirect."""

        authorization = await self._oauth.get_pending_authorization(request_id)
        session = await self.authenticate_login_cookie(
            cookie_value, expected_client_id=authorization.client_id
        )
        user = await self._repository.get_identity_user(session.user_id)
        if user is None:
            raise HostedIdentityError("identity.session_invalid", 401, "Login session is invalid")
        return await self._oauth.complete_authorization(
            request_id,
            subject=user.subject,
            user_id=user.id,
            login_session_id=session.id,
            auth_time=session.created_at,
        )

    async def list_sessions(self, cookie_value: str | None) -> dict[str, object]:
        """Return active sessions for the Cookie-authenticated account."""

        current = await self.authenticate_login_cookie(cookie_value)
        items = await self._repository.list_login_sessions(current.user_id)
        return {
            "items": [item.as_public_dict(current=item.id == current.id) for item in items],
            "page": {"next_cursor": None, "has_more": False},
        }

    async def revoke_session(self, cookie_value: str | None, session_id: str) -> tuple[bool, bool]:
        """Revoke an owned session and report whether it was the current Cookie."""

        current = await self.authenticate_login_cookie(cookie_value)
        revoked = await self._repository.revoke_login_session(
            current.user_id, session_id, datetime.now(UTC)
        )
        return revoked, current.id == session_id

    async def list_authenticators(self, cookie_value: str | None) -> dict[str, object]:
        """Return verifier-free authenticator metadata."""

        current = await self.authenticate_login_cookie(cookie_value)
        items = await self._repository.list_authenticators(current.user_id)
        return {
            "items": [item.as_public_dict() for item in items],
            "page": {"next_cursor": None, "has_more": False},
        }

    async def create_recovery_code_bundle(
        self, cookie_value: str | None, reauthentication_flow_id: str
    ) -> dict[str, object]:
        """Rotate recovery codes after a completed, recent reauthentication flow."""

        current = await self.authenticate_login_cookie(cookie_value)
        now = datetime.now(UTC)
        if not await self.verify_recent(UserId(current.user_id), reauthentication_flow_id, now):
            raise HostedIdentityError(
                "identity.reauthentication_required", 403, "Recent reauthentication is required"
            )
        codes = tuple(_new_recovery_code() for _ in range(10))
        verifiers = tuple(_password_hash(code) for code in codes)
        authenticator_id = new_opaque_id("authn")
        await self._repository.replace_recovery_codes(
            current.user_id,
            authenticator_id=authenticator_id,
            verifiers=verifiers,
            now=now,
        )
        return {
            "authenticator_id": authenticator_id,
            "recovery_codes": list(codes),
            "generated_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        }

    async def revoke_authenticator(
        self,
        cookie_value: str | None,
        authenticator_id: str,
        reauthentication_flow_id: str,
    ) -> bool:
        """Revoke one authenticator with recent reauth and last-path protection."""

        current = await self.authenticate_login_cookie(cookie_value)
        now = datetime.now(UTC)
        if not await self.verify_recent(UserId(current.user_id), reauthentication_flow_id, now):
            raise HostedIdentityError(
                "identity.reauthentication_required", 403, "Recent reauthentication is required"
            )
        return await self._repository.revoke_authenticator(current.user_id, authenticator_id, now)

    async def _identify(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
        identifier: str,
    ) -> IdentityFlowRecord:
        email = identifier.strip().casefold()
        if len(email) > 320 or _EMAIL_PATTERN.fullmatch(email) is None:
            raise HostedIdentityError("identity.identifier_invalid", 400, "Identifier is invalid")
        user = await self._repository.get_user_by_email(email)
        state: dict[str, object] = {"identifier": email}
        if user is not None:
            state["candidate_user_id"] = user.id
        if flow.purpose == "register":
            allowed: tuple[str, ...] = ("set_profile",)
        elif flow.purpose == "recover":
            allowed = ("send_email_code",)
        else:
            # This projection is deliberately identical for existing and missing accounts.
            allowed = ("verify_password", "verify_recovery_code", "begin_passkey")
        return await self._transition(
            flow,
            browser,
            step_id,
            "identify",
            allowed_steps=allowed,
            state_updates=state,
            user_id=user.id if user is not None else None,
        )

    async def _verify_password(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
        password: str,
    ) -> IdentityFlowRecord:
        user_id = flow.user_id
        verifier = (
            await self._repository.password_verifier(user_id)
            if user_id is not None
            else _FAKE_PASSWORD_VERIFIER
        )
        if not _password_verify(password, verifier or _FAKE_PASSWORD_VERIFIER) or user_id is None:
            raise HostedIdentityError(
                "identity.credentials_invalid", 400, "Identifier or credential is invalid"
            )
        return await self._transition(
            flow,
            browser,
            step_id,
            "verify_password",
            allowed_steps=("complete",),
            status="verified",
            state_updates={},
            user_id=user_id,
        )

    async def _verify_email_code(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
        code: str,
    ) -> IdentityFlowRecord:
        state = flow.internal_state or {}
        attempts = _state_int(state, "email_code_attempts")
        valid = (
            attempts < self._email_code_max_attempts
            and _state_int(state, "email_code_expires_at") > int(datetime.now(UTC).timestamp())
            and hmac.compare_digest(str(state.get("email_code_hash", "")), _secret_hash(code))
        )
        if not valid:
            failed = attempts + 1 >= self._email_code_max_attempts
            await self._transition(
                flow,
                browser,
                step_id,
                "verify_email_code",
                allowed_steps=() if failed else ("verify_email_code",),
                status="failed" if failed else "pending",
                state_updates={"email_code_attempts": attempts + 1},
            )
            raise HostedIdentityError(
                "identity.code_invalid", 400, "Verification code is invalid or expired"
            )
        self._test_email_codes.pop(flow.id, None)
        next_steps = (
            ("set_password",) if flow.purpose == "recover" else ("begin_passkey", "complete")
        )
        next_status = "pending" if flow.purpose == "recover" else "verified"
        return await self._transition(
            flow,
            browser,
            step_id,
            "verify_email_code",
            allowed_steps=next_steps,
            status=next_status,
            state_updates={"email_code_hash": "consumed", "email_code_attempts": attempts + 1},
        )

    async def _begin_passkey(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
    ) -> IdentityFlowRecord:
        challenge = secrets.token_bytes(32)
        state_updates: dict[str, object] = {
            "webauthn_challenge_hash": hashlib.sha256(challenge).hexdigest()
        }
        if flow.purpose == "register":
            state = flow.internal_state or {}
            identifier = state.get("identifier")
            display_name = state.get("display_name")
            if not isinstance(identifier, str) or not isinstance(display_name, str):
                raise HostedIdentityError(
                    "identity.flow_invalid", 409, "Identity flow is incomplete"
                )
            user_handle = secrets.token_bytes(32)
            state_updates.update(
                {
                    "webauthn_ceremony": "registration",
                    "webauthn_user_handle": _b64(user_handle),
                }
            )
            serialized_options = options_to_json(
                generate_registration_options(
                    rp_id="api.hmalliances.org",
                    rp_name="AI Job Workspace",
                    user_name=identifier,
                    user_id=user_handle,
                    user_display_name=display_name,
                    challenge=challenge,
                    authenticator_selection=AuthenticatorSelectionCriteria(
                        resident_key=ResidentKeyRequirement.REQUIRED,
                        user_verification=UserVerificationRequirement.REQUIRED,
                    ),
                )
            )
        else:
            state_updates["webauthn_ceremony"] = "authentication"
            serialized_options = options_to_json(
                generate_authentication_options(
                    rp_id="api.hmalliances.org",
                    challenge=challenge,
                    user_verification=UserVerificationRequirement.REQUIRED,
                )
            )
        public_options = json.loads(serialized_options)
        if not isinstance(public_options, dict):
            raise RuntimeError("WebAuthn library returned invalid options")
        return await self._transition(
            flow,
            browser,
            step_id,
            "begin_passkey",
            allowed_steps=("finish_passkey",),
            state_updates=state_updates,
            user_id=flow.user_id,
            webauthn_options=public_options,
        )

    async def _verify_recovery_code(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
        recovery_code: str,
    ) -> IdentityFlowRecord:
        user_id = flow.user_id
        matching_verifier: str | None = None
        if user_id is not None:
            authenticators = await self._repository.list_authenticators(user_id)
            recovery = next((item for item in authenticators if item.kind == "recovery_code"), None)
            if recovery is not None:
                values = json.loads(recovery.verifier)
                if isinstance(values, list):
                    for value in values:
                        if isinstance(value, str) and _password_verify(recovery_code, value):
                            matching_verifier = value
                            break
        else:
            _password_verify(recovery_code, _FAKE_PASSWORD_VERIFIER)
        if user_id is None or matching_verifier is None:
            raise HostedIdentityError(
                "identity.credentials_invalid", 400, "Identifier or credential is invalid"
            )
        consumed = await self._repository.consume_recovery_code(
            user_id, matching_verifier, datetime.now(UTC)
        )
        if not consumed:
            raise HostedIdentityError(
                "identity.credentials_invalid", 400, "Identifier or credential is invalid"
            )
        return await self._transition(
            flow,
            browser,
            step_id,
            "verify_recovery_code",
            allowed_steps=("complete",),
            status="verified",
            state_updates={},
            user_id=user_id,
        )

    async def _finish_passkey(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
        credential: dict[str, object],
    ) -> IdentityFlowRecord:
        state = flow.internal_state or {}
        raw_challenge = _credential_challenge(credential)
        if not hmac.compare_digest(
            str(state.get("webauthn_challenge_hash", "")),
            hashlib.sha256(raw_challenge).hexdigest(),
        ):
            raise HostedIdentityError(
                "identity.passkey_invalid", 400, "Passkey response is invalid"
            )
        ceremony = state.get("webauthn_ceremony")
        try:
            if ceremony == "registration" and flow.purpose == "register":
                verified = verify_registration_response(
                    credential=_registration_credential(credential),
                    expected_challenge=raw_challenge,
                    expected_rp_id="api.hmalliances.org",
                    expected_origin="https://api.hmalliances.org:8022",
                    require_user_presence=True,
                    require_user_verification=True,
                )
                pending = {
                    "credential_id": _b64(verified.credential_id),
                    "public_key": _b64(verified.credential_public_key),
                    "sign_count": verified.sign_count,
                    "device_type": verified.credential_device_type.value,
                    "backed_up": verified.credential_backed_up,
                    "aaguid": verified.aaguid,
                }
                return await self._transition(
                    flow,
                    browser,
                    step_id,
                    "finish_passkey",
                    allowed_steps=("complete",),
                    status="verified",
                    state_updates={
                        "pending_passkey": pending,
                        "webauthn_challenge_hash": "consumed",
                    },
                    webauthn_options=None,
                )
            if ceremony != "authentication" or flow.user_id is None:
                raise HostedIdentityError(
                    "identity.passkey_invalid", 400, "Passkey response is invalid"
                )
            credential_id = credential.get("id")
            if not isinstance(credential_id, str):
                raise HostedIdentityError(
                    "identity.passkey_invalid", 400, "Passkey response is invalid"
                )
            passkey = await self._repository.get_passkey_by_credential_id(credential_id)
            if passkey is None or passkey.user_id != flow.user_id:
                raise HostedIdentityError(
                    "identity.passkey_invalid", 400, "Passkey response is invalid"
                )
            current_count = _state_int(passkey.credential_metadata, "sign_count")
            verified_authentication = verify_authentication_response(
                credential=_authentication_credential(credential),
                expected_challenge=raw_challenge,
                expected_rp_id="api.hmalliances.org",
                expected_origin="https://api.hmalliances.org:8022",
                credential_public_key=_unb64(passkey.verifier),
                credential_current_sign_count=current_count,
                require_user_verification=True,
            )
            advanced = await self._repository.update_passkey_sign_count(
                passkey.id,
                expected=current_count,
                replacement=verified_authentication.new_sign_count,
                now=datetime.now(UTC),
            )
            if not advanced:
                raise HostedIdentityError(
                    "identity.flow_conflict", 409, "Identity flow state has changed"
                )
            return await self._transition(
                flow,
                browser,
                step_id,
                "finish_passkey",
                allowed_steps=("complete",),
                status="verified",
                state_updates={"webauthn_challenge_hash": "consumed"},
                user_id=flow.user_id,
                webauthn_options=None,
            )
        except WebAuthnException as error:
            raise HostedIdentityError(
                "identity.passkey_invalid", 400, "Passkey response is invalid"
            ) from error

    async def _complete(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
        *,
        device_name: str | None,
    ) -> IdentityStepResult:
        """@brief 完成 flow，并让恢复状态与通知入队同一提交 / Complete a flow with atomic recovery notification enqueue."""

        if flow.purpose != "recover":
            return await self._complete_transaction(
                flow,
                browser,
                step_id,
                device_name=device_name,
            )
        try:
            async with self._email_sender.atomic():
                return await self._complete_transaction(
                    flow,
                    browser,
                    step_id,
                    device_name=device_name,
                )
        except IdentityEmailEnqueueError as error:
            raise HostedIdentityError(
                "identity.delivery_unavailable",
                503,
                "Security notification delivery is temporarily unavailable",
            ) from error

    async def _complete_transaction(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
        *,
        device_name: str | None,
    ) -> IdentityStepResult:
        """@brief 执行可加入外部事务的完整状态推进 / Run all completion transitions inside a joinable transaction."""

        state = flow.internal_state or {}
        user_id = flow.user_id
        if flow.purpose == "register":
            required = ("identifier", "display_name", "locale", "password_verifier")
            if any(not isinstance(state.get(name), str) for name in required):
                raise HostedIdentityError(
                    "identity.flow_invalid", 409, "Identity flow is incomplete"
                )
            now = datetime.now(UTC)
            user_id = new_opaque_id("usr")
            pending_passkey = state.get("pending_passkey")
            passkey = (
                _pending_passkey_record(pending_passkey, user_id, now)
                if isinstance(pending_passkey, dict)
                else None
            )
            created = await self._repository.create_user_with_password(
                user=IdentityUserRecord(
                    id=user_id,
                    subject=f"oidc-{secrets.token_urlsafe(24)}",
                    email=str(state["identifier"]),
                    email_verified=True,
                    display_name=str(state["display_name"]),
                    locale=str(state["locale"]),
                ),
                password_authenticator_id=new_opaque_id("authn"),
                password_verifier=str(state["password_verifier"]),
                now=now,
                passkey=passkey,
            )
            if not created:
                raise HostedIdentityError(
                    "identity.flow_cannot_complete", 409, "Identity flow cannot be completed"
                )
        elif flow.purpose == "recover":
            verifier = state.get("password_verifier")
            if user_id is None or not isinstance(verifier, str):
                raise HostedIdentityError(
                    "identity.flow_invalid", 409, "Identity flow is incomplete"
                )
            replaced = await self._repository.replace_password_and_revoke_sessions(
                user_id, password_verifier=verifier, now=datetime.now(UTC)
            )
            if not replaced:
                raise HostedIdentityError(
                    "identity.flow_cannot_complete", 409, "Identity flow cannot be completed"
                )
            recovered_user = await self._repository.get_identity_user(user_id)
            if recovered_user is not None and recovered_user.email is not None:
                await self._email_sender.send_recovery_notification(recovered_user.email)
        if user_id is None:
            raise HostedIdentityError("identity.flow_invalid", 409, "Identity flow is incomplete")
        now = datetime.now(UTC)
        rotated_session_id = state.get("rotated_session_id")
        if flow.purpose == "reauthenticate" and isinstance(rotated_session_id, str):
            await self._repository.revoke_login_session(user_id, rotated_session_id, now)
        session_id, session_secret = new_opaque_id("loginsess"), secrets.token_urlsafe(32)
        await self._repository.create_login_session(
            IdentitySessionRecord(
                id=session_id,
                user_id=user_id,
                client_id=flow.client_id,
                client_name=flow.client_id,
                device_name=device_name[:200] if device_name else None,
                session_secret_hash=_secret_hash(session_secret),
                created_at=now,
                last_seen_at=now,
                idle_expires_at=now + timedelta(seconds=self._session_idle_ttl_seconds),
                absolute_expires_at=now + timedelta(seconds=self._session_absolute_ttl_seconds),
            )
        )
        await self._repository.bind_browser_user(browser.id, user_id)
        resume_uri = f"/oauth/authorize/resume/{flow.authorization_request_id}"
        updated = await self._transition(
            flow,
            browser,
            step_id,
            "complete",
            allowed_steps=(),
            status="completed",
            state_updates={"password_verifier": "consumed", "email_code_hash": "consumed"},
            user_id=user_id,
            authorization_resume_uri=resume_uri,
            completed_at=now,
        )
        return IdentityStepResult(updated, f"{session_id}.{session_secret}")

    async def _transition(
        self,
        flow: IdentityFlowRecord,
        browser: IdentityBrowserSessionRecord,
        step_id: str,
        kind: str,
        *,
        allowed_steps: tuple[str, ...],
        state_updates: dict[str, object],
        status: str = "pending",
        user_id: str | None = None,
        authorization_resume_uri: str | None = None,
        webauthn_options: dict[str, object] | None = None,
        completed_at: datetime | None = None,
    ) -> IdentityFlowRecord:
        updated = await self._repository.transition_flow(
            flow.id,
            browser_session_id=browser.id,
            step_id=step_id,
            expected_step=kind,
            allowed_steps=allowed_steps,
            status=status,
            state_updates=state_updates,
            user_id=user_id,
            authorization_resume_uri=authorization_resume_uri,
            webauthn_options=webauthn_options,
            completed_at=completed_at,
        )
        if updated is None:
            raise HostedIdentityError(
                "identity.flow_conflict", 409, "Identity flow state has changed"
            )
        return updated

    async def _verified_browser(
        self, cookie_value: str | None, *, csrf_token: str | None = None
    ) -> IdentityBrowserSessionRecord:
        if cookie_value is None:
            raise HostedIdentityError(
                "identity.browser_session_required",
                401,
                "Hosted identity browser session is required",
            )
        session_id, separator, secret = cookie_value.partition(".")
        if separator != "." or not session_id or not secret:
            raise HostedIdentityError(
                "identity.browser_session_invalid",
                401,
                "Hosted identity browser session is invalid",
            )
        browser = await self._repository.get_browser_session(session_id)
        if (
            browser is None
            or browser.expires_at <= datetime.now(UTC)
            or not hmac.compare_digest(browser.browser_secret_hash, _secret_hash(secret))
        ):
            raise HostedIdentityError(
                "identity.browser_session_invalid",
                401,
                "Hosted identity browser session is invalid",
            )
        if csrf_token is not None and not hmac.compare_digest(
            browser.csrf_token_hash, _secret_hash(csrf_token)
        ):
            raise HostedIdentityError("identity.csrf_invalid", 403, "CSRF validation failed")
        return browser


def _secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


async def _validate_new_password(
    password: str,
    checker: BreachedPasswordChecker | None,
) -> None:
    """@brief 校验长度并查询泄露密码语料 / Validate length and query breached-password corpora.

    @param password 仅用于当前身份步骤的候选密码 / Candidate password for the current identity step.
    @param checker 可选生产泄露语料检查端口 / Optional production breach-corpus checking port.
    @raise HostedIdentityError 密码策略不满足、已泄露或检查不可用时抛出 / Raised when the
        password violates policy, is breached, or cannot be checked safely.
    @note 本地极小 denylist 只用于快速拒绝，不能替代生产语料检查。 / The tiny local denylist
        is only a fast rejection path and does not replace the production corpus check.
    """

    if len(password) < 15 or len(password) > 1024:
        raise HostedIdentityError(
            "identity.password_policy", 400, "Password does not satisfy the length policy"
        )
    if password.casefold() in _COMMON_BREACHED_PASSWORDS:
        raise HostedIdentityError(
            "identity.password_breached", 400, "Password appears in the breached-password set"
        )
    if checker is None:
        return
    try:
        breached = await checker.is_breached(password)
    except RuntimeError as error:
        raise HostedIdentityError(
            "identity.password_safety_unavailable",
            503,
            "Password safety verification is temporarily unavailable",
        ) from error
    if breached:
        raise HostedIdentityError(
            "identity.password_breached", 400, "Password appears in the breached-password set"
        )


def _password_hash(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.scrypt(
        password.encode(), salt=salt, n=32768, r=8, p=1, maxmem=64 * 1024 * 1024
    )
    return f"scrypt$32768$8$1${_b64(salt)}${_b64(derived)}"


def _password_verify(password: str, encoded: str) -> bool:
    try:
        algorithm, raw_n, raw_r, raw_p, raw_salt, raw_expected = encoded.split("$")
        n, r, p = int(raw_n), int(raw_r), int(raw_p)
        if algorithm != "scrypt" or (n, r, p) != (32768, 8, 1):
            return False
        salt, expected = _unb64(raw_salt), _unb64(raw_expected)
        actual = hashlib.scrypt(
            password.encode(),
            salt=salt,
            n=n,
            r=r,
            p=p,
            maxmem=64 * 1024 * 1024,
            dklen=len(expected),
        )
        return hmac.compare_digest(actual, expected)
    except ValueError, TypeError:
        return False


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _new_recovery_code() -> str:
    raw = secrets.token_hex(10).upper()
    return "-".join((raw[:5], raw[5:10], raw[10:15], raw[15:]))


def _state_int(state: dict[str, object], name: str) -> int:
    value = state.get(name, 0)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _credential_challenge(credential: dict[str, object]) -> bytes:
    try:
        response = credential["response"]
        if not isinstance(response, dict):
            raise ValueError
        client_data = json.loads(_unb64(str(response["clientDataJSON"])))
        challenge = client_data["challenge"]
        if not isinstance(challenge, str):
            raise ValueError
        return _unb64(challenge)
    except (KeyError, ValueError, TypeError, json.JSONDecodeError) as error:
        raise HostedIdentityError(
            "identity.passkey_invalid", 400, "Passkey response is invalid"
        ) from error


def _registration_credential(credential: dict[str, object]) -> dict[str, object]:
    response = credential.get("response")
    if not isinstance(response, dict):
        return credential
    return {
        key: value
        for key, value in {
            **credential,
            "response": {
                name: value
                for name, value in response.items()
                if name in {"clientDataJSON", "attestationObject", "transports"}
            },
        }.items()
        if key != "authenticatorAttachment"
    }


def _authentication_credential(credential: dict[str, object]) -> dict[str, object]:
    return credential


def _pending_passkey_record(
    payload: dict[str, object], user_id: str, now: datetime
) -> IdentityAuthenticatorRecord:
    credential_id, public_key = payload.get("credential_id"), payload.get("public_key")
    if not isinstance(credential_id, str) or not isinstance(public_key, str):
        raise HostedIdentityError("identity.flow_invalid", 409, "Identity flow is incomplete")
    return IdentityAuthenticatorRecord(
        id=new_opaque_id("authn"),
        user_id=user_id,
        kind="passkey",
        display_name="Passkey",
        verifier=public_key,
        credential_metadata={key: value for key, value in payload.items() if key != "public_key"},
        created_at=now,
        last_used_at=None,
    )


__all__ = ["BrowserBinding", "HostedIdentityService", "IdentityStepResult"]
