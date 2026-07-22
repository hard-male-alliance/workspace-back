"""Memory and PostgreSQL persistence adapters for OAuth token transactions."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from backend.domain.oauth import (
    AuthorizationCodeExchange,
    AuthorizationRequestRecord,
    RefreshTokenReuseDetected,
    RefreshTokenRotation,
)
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    OAuthAuthorizationCodeRecord as OAuthAuthorizationCodeOrmRecord,
)
from backend.infrastructure.persistence.models import (
    OAuthAuthorizationRequestRecord as OAuthAuthorizationRequestOrmRecord,
)
from backend.infrastructure.persistence.models import (
    OAuthRefreshTokenFamilyRecord as OAuthRefreshTokenFamilyOrmRecord,
)
from backend.infrastructure.persistence.models import (
    OAuthRefreshTokenRecord as OAuthRefreshTokenOrmRecord,
)
from backend.infrastructure.persistence.models import (
    OAuthRevokedAccessTokenRecord as OAuthRevokedAccessTokenOrmRecord,
)


class InMemoryOAuthAuthorizationRequestRepository:
    """Process-local deterministic adapter used only in development and tests."""

    def __init__(self) -> None:
        self._records: dict[str, AuthorizationRequestRecord] = {}
        self._codes: dict[str, dict[str, Any]] = {}
        self._families: dict[str, dict[str, Any]] = {}
        self._refresh_tokens: dict[str, dict[str, Any]] = {}
        self._revoked_access: dict[str, datetime] = {}
        self._lock = asyncio.Lock()

    async def create_authorization_request(self, record: AuthorizationRequestRecord) -> None:
        async with self._lock:
            if record.id in self._records:
                raise RuntimeError("authorization request id collision")
            self._records[record.id] = replace(record)

    async def get_authorization_request(self, request_id: str) -> AuthorizationRequestRecord | None:
        async with self._lock:
            record = self._records.get(request_id)
            return replace(record) if record is not None else None

    async def issue_authorization_code(
        self,
        request_id: str,
        *,
        subject: str,
        user_id: str,
        code_hash: str,
        auth_time: datetime,
        expires_at: datetime,
    ) -> bool:
        async with self._lock:
            request = self._records.get(request_id)
            if (
                request is None
                or request.status != "pending"
                or request.expires_at <= datetime.now(UTC)
                or code_hash in self._codes
            ):
                return False
            self._records[request_id] = replace(request, status="code_issued")
            self._codes[code_hash] = {
                "request": request,
                "subject": subject,
                "user_id": user_id,
                "auth_time": auth_time,
                "expires_at": expires_at,
                "consumed_at": None,
            }
            return True

    async def exchange_authorization_code(
        self,
        code_hash: str,
        *,
        client_id: str,
        redirect_uri: str,
        verifier_challenge: str,
        refresh_family_id: str | None,
        refresh_token_id: str | None,
        refresh_token_hash: str | None,
        refresh_expires_at: datetime | None,
    ) -> AuthorizationCodeExchange | None:
        async with self._lock:
            code = self._codes.get(code_hash)
            now = datetime.now(UTC)
            if code is None:
                return None
            request: AuthorizationRequestRecord = code["request"]
            if (
                code["consumed_at"] is not None
                or code["expires_at"] <= now
                or request.client_id != client_id
                or request.redirect_uri != redirect_uri
                or request.code_challenge != verifier_challenge
            ):
                return None
            code["consumed_at"] = now
            effective_family_id = refresh_family_id if "offline_access" in request.scopes else None
            if effective_family_id is not None:
                if (
                    refresh_token_id is None
                    or refresh_token_hash is None
                    or refresh_expires_at is None
                ):
                    raise RuntimeError("refresh token persistence parameters are incomplete")
                self._families[effective_family_id] = {
                    "subject": code["subject"],
                    "user_id": code["user_id"],
                    "client_id": client_id,
                    "scopes": request.scopes,
                    "revoked_at": None,
                    "reuse_detected_at": None,
                }
                self._refresh_tokens[refresh_token_hash] = {
                    "id": refresh_token_id,
                    "family_id": effective_family_id,
                    "sequence": 1,
                    "expires_at": refresh_expires_at,
                    "consumed_at": None,
                    "replaced_by_token_id": None,
                }
            return AuthorizationCodeExchange(
                subject=code["subject"],
                user_id=code["user_id"],
                client_id=client_id,
                scopes=request.scopes,
                nonce=request.nonce,
                auth_time=code["auth_time"],
                refresh_family_id=effective_family_id,
            )

    async def rotate_refresh_token(
        self,
        token_hash: str,
        *,
        client_id: str,
        replacement_token_id: str,
        replacement_token_hash: str,
        replacement_expires_at: datetime,
    ) -> RefreshTokenRotation | None:
        reuse_detected = False
        rotation: RefreshTokenRotation | None = None
        async with self._lock:
            token = self._refresh_tokens.get(token_hash)
            if token is None:
                return None
            family = self._families[token["family_id"]]
            now = datetime.now(UTC)
            if (
                family["revoked_at"] is not None
                or token["expires_at"] <= now
                or family["client_id"] != client_id
            ):
                return None
            if token["consumed_at"] is not None:
                family["revoked_at"] = now
                family["reuse_detected_at"] = now
                reuse_detected = True
            else:
                token["consumed_at"] = now
                token["replaced_by_token_id"] = replacement_token_id
                self._refresh_tokens[replacement_token_hash] = {
                    "id": replacement_token_id,
                    "family_id": token["family_id"],
                    "sequence": token["sequence"] + 1,
                    "expires_at": replacement_expires_at,
                    "consumed_at": None,
                    "replaced_by_token_id": None,
                }
                rotation = RefreshTokenRotation(
                    subject=family["subject"],
                    user_id=family["user_id"],
                    client_id=family["client_id"],
                    scopes=family["scopes"],
                    family_id=token["family_id"],
                )
        if reuse_detected:
            raise RefreshTokenReuseDetected("refresh token reuse revoked its family")
        return rotation

    async def revoke_refresh_token(self, token_hash: str) -> None:
        async with self._lock:
            token = self._refresh_tokens.get(token_hash)
            if token is not None:
                self._families[token["family_id"]]["revoked_at"] = datetime.now(UTC)

    async def revoke_access_token(self, jti: str, expires_at: datetime) -> None:
        async with self._lock:
            self._revoked_access[_sha256(jti)] = expires_at

    async def access_token_is_revoked(self, jti: str) -> bool:
        async with self._lock:
            expires_at = self._revoked_access.get(_sha256(jti))
            return expires_at is not None and expires_at > datetime.now(UTC)


class PostgresOAuthAuthorizationRequestRepository:
    """Durable adapter using unscoped transactions for global identity state."""

    def __init__(self, database: AsyncDatabase) -> None:
        self._database = database

    async def create_authorization_request(self, record: AuthorizationRequestRecord) -> None:
        async with self._database.unscoped_transaction() as session:
            session.add(
                OAuthAuthorizationRequestOrmRecord(
                    id=record.id,
                    client_id=record.client_id,
                    redirect_uri=record.redirect_uri,
                    scope=" ".join(record.scopes),
                    state=record.state,
                    nonce=record.nonce,
                    code_challenge=record.code_challenge,
                    code_challenge_method=record.code_challenge_method,
                    prompt=" ".join(record.prompt),
                    screen_hint=record.screen_hint,
                    status=record.status,
                    created_at=record.created_at,
                    expires_at=record.expires_at,
                )
            )

    async def get_authorization_request(self, request_id: str) -> AuthorizationRequestRecord | None:
        async with self._database.unscoped_transaction() as session:
            orm_record = await session.scalar(
                select(OAuthAuthorizationRequestOrmRecord).where(
                    OAuthAuthorizationRequestOrmRecord.id == request_id
                )
            )
            return _authorization_request_from_orm(orm_record)

    async def issue_authorization_code(
        self,
        request_id: str,
        *,
        subject: str,
        user_id: str,
        code_hash: str,
        auth_time: datetime,
        expires_at: datetime,
    ) -> bool:
        async with self._database.unscoped_transaction() as session:
            request = await session.scalar(
                select(OAuthAuthorizationRequestOrmRecord)
                .where(OAuthAuthorizationRequestOrmRecord.id == request_id)
                .with_for_update()
            )
            if (
                request is None
                or request.status != "pending"
                or request.expires_at <= datetime.now(UTC)
            ):
                return False
            request.status = "code_issued"
            session.add(
                OAuthAuthorizationCodeOrmRecord(
                    id=f"ac_{request.id}",
                    code_hash=code_hash,
                    authorization_request_id=request.id,
                    subject=subject,
                    user_id=user_id,
                    client_id=request.client_id,
                    redirect_uri=request.redirect_uri,
                    scope=request.scope,
                    nonce=request.nonce,
                    code_challenge=request.code_challenge,
                    auth_time=auth_time,
                    expires_at=expires_at,
                )
            )
            return True

    async def exchange_authorization_code(
        self,
        code_hash: str,
        *,
        client_id: str,
        redirect_uri: str,
        verifier_challenge: str,
        refresh_family_id: str | None,
        refresh_token_id: str | None,
        refresh_token_hash: str | None,
        refresh_expires_at: datetime | None,
    ) -> AuthorizationCodeExchange | None:
        async with self._database.unscoped_transaction() as session:
            code = await session.scalar(
                select(OAuthAuthorizationCodeOrmRecord)
                .where(OAuthAuthorizationCodeOrmRecord.code_hash == code_hash)
                .with_for_update()
            )
            now = datetime.now(UTC)
            if (
                code is None
                or code.consumed_at is not None
                or code.expires_at <= now
                or code.client_id != client_id
                or code.redirect_uri != redirect_uri
                or code.code_challenge != verifier_challenge
            ):
                return None
            code.consumed_at = now
            effective_family_id = (
                refresh_family_id if "offline_access" in code.scope.split() else None
            )
            if effective_family_id is not None:
                if (
                    refresh_token_id is None
                    or refresh_token_hash is None
                    or refresh_expires_at is None
                ):
                    raise RuntimeError("refresh token persistence parameters are incomplete")
                session.add(
                    OAuthRefreshTokenFamilyOrmRecord(
                        id=effective_family_id,
                        subject=code.subject,
                        user_id=code.user_id,
                        client_id=client_id,
                        scope=code.scope,
                    )
                )
                session.add(
                    OAuthRefreshTokenOrmRecord(
                        id=refresh_token_id,
                        family_id=effective_family_id,
                        token_hash=refresh_token_hash,
                        sequence=1,
                        expires_at=refresh_expires_at,
                    )
                )
            return AuthorizationCodeExchange(
                subject=code.subject,
                user_id=code.user_id,
                client_id=client_id,
                scopes=tuple(code.scope.split()),
                nonce=code.nonce,
                auth_time=code.auth_time,
                refresh_family_id=effective_family_id,
            )

    async def rotate_refresh_token(
        self,
        token_hash: str,
        *,
        client_id: str,
        replacement_token_id: str,
        replacement_token_hash: str,
        replacement_expires_at: datetime,
    ) -> RefreshTokenRotation | None:
        reuse_detected = False
        rotation: RefreshTokenRotation | None = None
        async with self._database.unscoped_transaction() as session:
            token = await session.scalar(
                select(OAuthRefreshTokenOrmRecord)
                .where(OAuthRefreshTokenOrmRecord.token_hash == token_hash)
                .with_for_update()
            )
            if token is None:
                return None
            family = await session.scalar(
                select(OAuthRefreshTokenFamilyOrmRecord)
                .where(OAuthRefreshTokenFamilyOrmRecord.id == token.family_id)
                .with_for_update()
            )
            if family is None:
                return None
            now = datetime.now(UTC)
            if (
                family.revoked_at is not None
                or token.expires_at <= now
                or family.client_id != client_id
            ):
                return None
            if token.consumed_at is not None:
                family.revoked_at = now
                family.reuse_detected_at = now
                reuse_detected = True
            else:
                token.consumed_at = now
                token.replaced_by_token_id = replacement_token_id
                session.add(
                    OAuthRefreshTokenOrmRecord(
                        id=replacement_token_id,
                        family_id=family.id,
                        token_hash=replacement_token_hash,
                        sequence=token.sequence + 1,
                        expires_at=replacement_expires_at,
                    )
                )
                rotation = RefreshTokenRotation(
                    subject=family.subject,
                    user_id=family.user_id,
                    client_id=family.client_id,
                    scopes=tuple(family.scope.split()),
                    family_id=family.id,
                )
        if reuse_detected:
            raise RefreshTokenReuseDetected("refresh token reuse revoked its family")
        return rotation

    async def revoke_refresh_token(self, token_hash: str) -> None:
        async with self._database.unscoped_transaction() as session:
            token = await session.scalar(
                select(OAuthRefreshTokenOrmRecord).where(
                    OAuthRefreshTokenOrmRecord.token_hash == token_hash
                )
            )
            if token is not None:
                family = await session.get(OAuthRefreshTokenFamilyOrmRecord, token.family_id)
                if family is not None and family.revoked_at is None:
                    family.revoked_at = datetime.now(UTC)

    async def revoke_access_token(self, jti: str, expires_at: datetime) -> None:
        async with self._database.unscoped_transaction() as session:
            statement = insert(OAuthRevokedAccessTokenOrmRecord).values(
                jti_hash=_sha256(jti), expires_at=expires_at
            )
            await session.execute(
                statement.on_conflict_do_update(
                    index_elements=[OAuthRevokedAccessTokenOrmRecord.jti_hash],
                    set_={"expires_at": expires_at},
                )
            )

    async def access_token_is_revoked(self, jti: str) -> bool:
        async with self._database.unscoped_transaction() as session:
            expires_at = await session.scalar(
                select(OAuthRevokedAccessTokenOrmRecord.expires_at).where(
                    OAuthRevokedAccessTokenOrmRecord.jti_hash == _sha256(jti)
                )
            )
            return expires_at is not None and expires_at > datetime.now(UTC)


def _authorization_request_from_orm(
    record: OAuthAuthorizationRequestOrmRecord | None,
) -> AuthorizationRequestRecord | None:
    if record is None:
        return None
    return AuthorizationRequestRecord(
        id=record.id,
        client_id=record.client_id,
        redirect_uri=record.redirect_uri,
        scopes=tuple(record.scope.split()),
        state=record.state,
        nonce=record.nonce,
        code_challenge=record.code_challenge,
        code_challenge_method=record.code_challenge_method,
        prompt=tuple(record.prompt.split()),
        screen_hint=record.screen_hint,
        status=record.status,
        created_at=record.created_at,
        expires_at=record.expires_at,
    )


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


__all__ = [
    "InMemoryOAuthAuthorizationRequestRepository",
    "PostgresOAuthAuthorizationRequestRepository",
]
