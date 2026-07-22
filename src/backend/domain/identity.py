"""Hosted identity domain records with secret-free public projections."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


def utc_timestamp(value: datetime) -> str:
    """Render the v2 contract's canonical UTC ``Z`` timestamp."""

    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class IdentityBrowserSessionRecord:
    """Server-side binding for a hosted authorization browser session."""

    id: str
    authorization_request_id: str
    browser_secret_hash: str
    csrf_token_hash: str
    user_id: str | None
    created_at: datetime
    last_seen_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class IdentityFlowRecord:
    """Secret-free state of one registration, login, recovery, or reauth flow."""

    id: str
    purpose: str
    status: str
    allowed_steps: tuple[str, ...]
    authorization_request_id: str
    browser_session_id: str
    client_id: str
    redirect_uri: str
    code_challenge: str
    created_at: datetime
    expires_at: datetime
    user_id: str | None = None
    internal_state: dict[str, object] | None = None
    authorization_resume_uri: str | None = None
    webauthn_options: dict[str, object] | None = None

    def as_public_dict(self) -> dict[str, object]:
        """Return exactly the API v2 ``IdentityFlow`` projection."""

        return {
            "id": self.id,
            "purpose": self.purpose,
            "status": self.status,
            "allowed_steps": list(self.allowed_steps),
            "expires_at": utc_timestamp(self.expires_at),
            "authorization_resume_uri": self.authorization_resume_uri,
            "webauthn_options": self.webauthn_options,
        }


class HostedIdentityError(ValueError):
    """Stable, non-secret identity-flow failure."""

    def __init__(self, code: str, status: int, title: str) -> None:
        super().__init__(title)
        self.code = code
        self.status = status
        self.title = title


@dataclass(frozen=True, slots=True)
class IdentityUserRecord:
    """Private account projection needed by the hosted identity service."""

    id: str
    subject: str
    email: str
    email_verified: bool
    display_name: str
    locale: str


@dataclass(frozen=True, slots=True)
class IdentitySessionRecord:
    """Server login session represented externally without its Cookie secret."""

    id: str
    user_id: str
    client_id: str
    client_name: str
    device_name: str | None
    session_secret_hash: str
    created_at: datetime
    last_seen_at: datetime
    idle_expires_at: datetime
    absolute_expires_at: datetime
    revoked_at: datetime | None = None

    def as_public_dict(self, *, current: bool) -> dict[str, object]:
        return {
            "id": self.id,
            "client_name": self.client_name,
            "device_name": self.device_name,
            "created_at": utc_timestamp(self.created_at),
            "last_seen_at": utc_timestamp(self.last_seen_at),
            "current": current,
        }


@dataclass(frozen=True, slots=True)
class IdentityAuthenticatorRecord:
    """Safe authenticator projection with its verifier retained only inside the service."""

    id: str
    user_id: str
    kind: str
    display_name: str
    verifier: str
    credential_metadata: dict[str, object]
    created_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None = None

    def as_public_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "kind": self.kind,
            "display_name": self.display_name,
            "created_at": utc_timestamp(self.created_at),
            "last_used_at": (
                utc_timestamp(self.last_used_at) if self.last_used_at is not None else None
            ),
        }


__all__ = [
    "HostedIdentityError",
    "IdentityAuthenticatorRecord",
    "IdentityBrowserSessionRecord",
    "IdentityFlowRecord",
    "IdentitySessionRecord",
    "IdentityUserRecord",
    "utc_timestamp",
]
