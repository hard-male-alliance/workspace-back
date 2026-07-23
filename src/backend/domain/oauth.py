"""Authorization Server domain records that never contain client secrets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Final

ACCESS_TOKEN_USER_ID_CLAIM: Final[str] = "https://api.hmalliances.org:8022/claims/user_id"
"""@brief access token 的本地用户绑定 claim / Local user-binding claim for access tokens."""


@dataclass(frozen=True, slots=True)
class AuthorizationRequestRecord:
    """A short-lived, server-owned OAuth authorization transaction."""

    id: str
    client_id: str
    redirect_uri: str
    scopes: tuple[str, ...]
    state: str
    nonce: str
    code_challenge: str
    code_challenge_method: str
    prompt: tuple[str, ...]
    screen_hint: str | None
    status: str
    created_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class AuthorizationCodeExchange:
    """Claims recovered by atomically consuming a one-time authorization code."""

    subject: str
    user_id: str
    client_id: str
    scopes: tuple[str, ...]
    nonce: str
    auth_time: datetime
    refresh_family_id: str | None


@dataclass(frozen=True, slots=True)
class RefreshTokenRotation:
    """Claims recovered by a successful one-time refresh-token rotation."""

    subject: str
    user_id: str
    client_id: str
    scopes: tuple[str, ...]
    family_id: str


class RefreshTokenReuseDetected(RuntimeError):
    """Raised after atomically revoking a family whose consumed token was reused."""


class OAuthTokenValidationError(ValueError):
    """Stable token validation failure shared across the domain port boundary."""


__all__ = [
    "ACCESS_TOKEN_USER_ID_CLAIM",
    "AuthorizationCodeExchange",
    "AuthorizationRequestRecord",
    "OAuthTokenValidationError",
    "RefreshTokenReuseDetected",
    "RefreshTokenRotation",
]
