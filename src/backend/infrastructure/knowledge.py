"""@brief API V2 Connection、Knowledge 与统一 Upload 持久化 / API V2 persistence for Connections, Knowledge, and unified Uploads.

内存实现提供与 PostgreSQL 相同的 copy-on-write 工作单元语义。PostgreSQL 实现坚持
Workspace-first 查询、受影响行数 CAS、事务级幂等 scope 锁、AEAD 加密授权 launch，
并让领域状态、统一 Job 与 transactional outbox 共享一个提交边界。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from types import TracebackType
from typing import Any, Protocol, Self, cast
from uuid import uuid4

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import or_, select, text, tuple_, update
from sqlalchemy.engine import CursorResult, Result
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from backend.application.ports.access import AccessAuthorizer
from backend.application.ports.knowledge import (
    KnowledgeCasMismatch,
    KnowledgePage,
    KnowledgePageRequest,
    KnowledgeRepository,
)
from backend.domain.connections import (
    Connection,
    ConnectionAggregate,
    ConnectionAuthMethod,
    ConnectionAuthorizationFlow,
    ConnectionAuthorizationIdempotency,
    ConnectionAuthorizationRecord,
    ConnectionAuthorizationSession,
    ConnectionAuthorizationSessionId,
    ConnectionAuthorizationState,
    ConnectionId,
    ConnectionOwnership,
    ConnectionProvider,
    ConnectionStatus,
    CredentialReference,
    ProviderSessionReference,
)
from backend.domain.knowledge_jobs import (
    KnowledgeJobSpec,
    KnowledgeOutboxEvent,
)
from backend.domain.knowledge_sources import (
    AgentScopeGrant,
    CloudDriveSourceInput,
    FileSourceInput,
    GitSourceInput,
    KnowledgeIngestionState,
    KnowledgeIngestionStatus,
    KnowledgeOperation,
    KnowledgeSensitivity,
    KnowledgeSource,
    KnowledgeSourceId,
    KnowledgeSourceInput,
    KnowledgeSourceType,
    KnowledgeSourceVersion,
    KnowledgeSourceVersionId,
    KnowledgeVersionSnapshot,
    KnowledgeVersionStatus,
    KnowledgeVisibilityPolicy,
    ModelRegion,
    PolicyEffect,
    PublicKnowledgeSourceConfig,
    ResumeSourceInput,
)
from backend.domain.outbox import initial_outbox_lifecycle
from backend.domain.platform import Job, JobId, ProblemDetails
from backend.domain.principals import (
    AuthenticatedActor,
    ResourceMeta,
    TokenPrincipal,
    UserId,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef
from backend.domain.upload_sessions import (
    UploadCompletionClaim,
    UploadDeclaration,
    UploadGrant,
    UploadSession,
    UploadSessionId,
    UploadSessionView,
    UploadStatus,
    UploadVerificationId,
)
from backend.infrastructure.access import (
    InMemoryAccessRepository,
    InMemoryAccessStore,
    PostgresAccessRepository,
)
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    ConnectionAuthorizationRecordModel,
    ConnectionRecord,
    JobRecord,
    JsonObject,
    KnowledgeSourceRecord,
    KnowledgeSourceVersionRecord,
    KnowledgeUploadSessionRecord,
    KnowledgeVisibilityGrantRecord,
    KnowledgeVisibilityPolicyRecord,
    OutboxEventRecord,
)

_PROBLEM_ADAPTER: TypeAdapter[ProblemDetails] = TypeAdapter(ProblemDetails)
"""@brief ProblemDetails 的 JSONB codec / JSONB codec for ProblemDetails."""

_SOURCE_INPUT_ADAPTER: TypeAdapter[KnowledgeSourceInput] = TypeAdapter(KnowledgeSourceInput)
"""@brief 私有来源判别联合的 JSONB codec / JSONB codec for private source inputs."""

_PUBLIC_CONFIG_ADAPTER: TypeAdapter[PublicKnowledgeSourceConfig] = TypeAdapter(
    PublicKnowledgeSourceConfig
)
"""@brief 公开来源配置 codec / Public source-config codec."""

_JOB_SPEC_ADAPTER: TypeAdapter[KnowledgeJobSpec] = TypeAdapter(KnowledgeJobSpec)
"""@brief Knowledge worker spec codec / Knowledge worker-spec codec."""

_RESOURCE_REF_ADAPTER: TypeAdapter[ResourceRef] = TypeAdapter(ResourceRef)
"""@brief 跨领域引用 codec / Cross-domain reference codec."""

_EVENT_RETENTION = timedelta(days=30)
"""@brief Outbox replay 保留期 / Outbox replay retention."""


def _dump_object[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> JsonObject:
    """@brief 将领域值编码为 JSON object / Encode a domain value as a JSON object.

    @param adapter 强类型 codec / Typed codec.
    @param value 待编码值 / Value to encode.
    @return JSONB object / JSONB object.
    """
    payload = adapter.dump_python(value, mode="json")
    if not isinstance(payload, dict):
        raise TypeError("Knowledge persistence codec must produce an object")
    return cast(JsonObject, payload)


def _load[ValueT](adapter: TypeAdapter[ValueT], payload: object, label: str) -> ValueT:
    """@brief 从不可信 JSONB 重建领域值 / Rebuild a domain value from untrusted JSONB.

    @param adapter 强类型 codec / Typed codec.
    @param payload 数据库值 / Database value.
    @param label 诊断标签 / Diagnostic label.
    @return 通过领域不变量的值 / Value satisfying domain invariants.
    """
    try:
        return adapter.validate_python(payload)
    except ValidationError as error:
        raise ValueError(f"persisted {label} violates the API V2 domain model") from error


def _problem_payload(problem: ProblemDetails | None) -> JsonObject | None:
    """@brief 编码可选公开问题 / Encode an optional public-safe problem."""
    return None if problem is None else _dump_object(_PROBLEM_ADAPTER, problem)


def _load_problem(payload: object | None) -> ProblemDetails | None:
    """@brief 解码可选公开问题 / Decode an optional public-safe problem."""
    return None if payload is None else _load(_PROBLEM_ADAPTER, payload, "problem")


def _affected_rows(result: Result[Any]) -> int:
    """@brief 返回 DML 受影响行数 / Return the affected-row count for DML."""
    return cast(CursorResult[Any], result).rowcount


def _row_id(prefix: str) -> str:
    """@brief 生成内部 opaque row ID / Generate an internal opaque row ID."""
    return f"{prefix}_{uuid4().hex}"


def _page[ItemT](items: Sequence[ItemT], limit: int, position: str) -> KnowledgePage[ItemT]:
    """@brief 从 limit+1 window 构造 keyset 页面 / Build a keyset page from a limit-plus-one window."""
    has_more = len(items) > limit
    selected = tuple(items[:limit])
    return KnowledgePage(selected, position if selected and has_more else None)


def _version_after(position: str | None) -> int | None:
    """@brief 解码 source-local version keyset / Decode a source-local version keyset."""
    if position is None:
        return None
    try:
        number = int(position)
    except ValueError as error:
        raise ValueError("knowledge version page position is invalid") from error
    if number < 1:
        raise ValueError("knowledge version page position is invalid")
    return number


def _aad_part(value: str) -> bytes:
    """@brief 长度分帧一个 AAD 字段 / Length-frame one AAD field."""
    encoded = value.encode("utf-8")
    return len(encoded).to_bytes(4, "big") + encoded


def _authorization_aad(
    *,
    workspace_id: str,
    created_by: str,
    session_id: str,
    provider: str,
    flow: str,
    expires_at: datetime,
) -> bytes:
    """@brief 构造不可拼接歧义的授权 launch AAD / Build ambiguity-free authorization-launch AAD."""
    expires_us = int(expires_at.timestamp() * 1_000_000)
    return b"knowledge.authorization-launch.v1\x00" + b"".join(
        _aad_part(value)
        for value in (
            workspace_id,
            created_by,
            session_id,
            provider,
            flow,
            str(expires_us),
        )
    )


@dataclass(frozen=True, slots=True)
class EncryptedAuthorizationLaunch:
    """@brief AEAD 密封后的授权 launch / AEAD-sealed authorization launch.

    @param key_id key-ring 标识 / Key-ring identifier.
    @param nonce 96-bit GCM nonce / 96-bit GCM nonce.
    @param ciphertext ciphertext 与 authentication tag / Ciphertext plus authentication tag.
    """

    key_id: str
    nonce: bytes
    ciphertext: bytes


class AuthorizationLaunchCipher(Protocol):
    """@brief 专用授权 launch AEAD 端口 / Dedicated authorization-launch AEAD port."""

    def encrypt(self, record: ConnectionAuthorizationRecord) -> EncryptedAuthorizationLaunch:
        """@brief 加密可重放客户端投影 / Encrypt the replayable client projection."""

    def decrypt(
        self,
        encrypted: EncryptedAuthorizationLaunch,
        *,
        workspace_id: str,
        created_by: str,
        session_id: str,
        provider: str,
        flow: str,
        expires_at: datetime,
    ) -> ConnectionAuthorizationSession:
        """@brief 验证 AAD 后解密客户端投影 / Decrypt the client projection after authenticating AAD."""


class AesGcmAuthorizationLaunchCipher:
    """@brief 使用 AES-256-GCM 和 key-ring 的授权 launch 加密器 / Authorization-launch cipher using AES-256-GCM and a key ring."""

    def __init__(self, keys: Mapping[str, bytes], *, active_key_id: str) -> None:
        """@brief 验证并复制 AES-256 key-ring / Validate and copy an AES-256 key ring.

        @param keys key ID 到 32-byte key 的映射 / Mapping from key IDs to 32-byte keys.
        @param active_key_id 新写入使用的 key / Key used for new writes.
        """
        copied = dict(keys)
        if active_key_id not in copied or not copied:
            raise ValueError("authorization launch key ring lacks the active key")
        if any(
            re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{2,63}", key_id) is None or len(key) != 32
            for key_id, key in copied.items()
        ):
            raise ValueError("authorization launch encryption requires valid AES-256 keys")
        self._keys = copied
        self._active_key_id = active_key_id

    def encrypt(self, record: ConnectionAuthorizationRecord) -> EncryptedAuthorizationLaunch:
        """@brief 以绑定租户、actor、session 与 flow 的 AAD 加密 launch / Encrypt launch with tenant-and-session-bound AAD."""
        session = record.session
        payload = json.dumps(
            {
                "authorization_url": session.authorization_url,
                "verification_uri": session.verification_uri,
                "user_code": session.user_code,
                "poll_interval_ms": session.poll_interval_ms,
            },
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        aad = _authorization_aad(
            workspace_id=str(record.ownership.workspace_id),
            created_by=str(record.ownership.created_by),
            session_id=str(session.id),
            provider=session.provider.value,
            flow=session.flow.value,
            expires_at=session.expires_at,
        )
        nonce = os.urandom(12)
        ciphertext = AESGCM(self._keys[self._active_key_id]).encrypt(nonce, payload, aad)
        return EncryptedAuthorizationLaunch(self._active_key_id, nonce, ciphertext)

    def decrypt(
        self,
        encrypted: EncryptedAuthorizationLaunch,
        *,
        workspace_id: str,
        created_by: str,
        session_id: str,
        provider: str,
        flow: str,
        expires_at: datetime,
    ) -> ConnectionAuthorizationSession:
        """@brief 验证 tag、AAD 与 JSON 形状后重建 session / Rebuild a session after verifying tag, AAD, and JSON shape."""
        key = self._keys.get(encrypted.key_id)
        if key is None:
            raise ValueError("authorization launch encryption key is unavailable")
        aad = _authorization_aad(
            workspace_id=workspace_id,
            created_by=created_by,
            session_id=session_id,
            provider=provider,
            flow=flow,
            expires_at=expires_at,
        )
        try:
            plaintext = AESGCM(key).decrypt(encrypted.nonce, encrypted.ciphertext, aad)
        except InvalidTag as error:
            raise ValueError("authorization launch authentication failed") from error
        try:
            payload = json.loads(plaintext)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("authorization launch payload is invalid") from error
        if not isinstance(payload, dict) or set(payload) != {
            "authorization_url",
            "verification_uri",
            "user_code",
            "poll_interval_ms",
        }:
            raise ValueError("authorization launch payload has an invalid shape")
        return ConnectionAuthorizationSession(
            ConnectionAuthorizationSessionId(session_id),
            ConnectionProvider(provider),
            ConnectionAuthorizationFlow(flow),
            expires_at,
            cast(str | None, payload["authorization_url"]),
            cast(str | None, payload["verification_uri"]),
            cast(str | None, payload["user_code"]),
            cast(int | None, payload["poll_interval_ms"]),
        )


class _TrackingKnowledgeAuthorizer:
    """@brief 固定一个 UoW 的 actor 与 Workspace / Pin one actor and Workspace to a UoW."""

    def __init__(
        self,
        delegate: AccessAuthorizer,
        scope_installer: Any | None = None,
    ) -> None:
        """@brief 绑定集中授权器与可选 RLS installer / Bind the central authorizer and optional RLS installer."""
        self._delegate = delegate
        self._scope_installer = scope_installer
        self.actor_id: UserId | None = None
        self.workspace_id: WorkspaceId | None = None

    async def authenticate(self, principal: TokenPrincipal) -> AuthenticatedActor:
        """@brief 认证并固定 actor / Authenticate and pin the actor."""
        actor = await self._delegate.authenticate(principal)
        if self.actor_id is not None and self.actor_id != actor.user_id:
            raise PermissionError("a Knowledge unit of work cannot switch actors")
        self.actor_id = actor.user_id
        if self._scope_installer is not None:
            await self._scope_installer(actor_id=str(actor.user_id), workspace_id=None)
        return actor

    async def authorize(
        self,
        actor: AuthenticatedActor,
        workspace_id: WorkspaceId,
        action: WorkspaceAction,
    ) -> WorkspaceAccessContext:
        """@brief 授权并固定 Workspace / Authorize and pin the Workspace."""
        if self.actor_id != actor.user_id:
            raise PermissionError("Knowledge authorization requires the authenticated UoW actor")
        context = await self._delegate.authorize(actor, workspace_id, action)
        if self.workspace_id is not None and self.workspace_id != workspace_id:
            raise PermissionError("a Knowledge unit of work cannot switch workspaces")
        self.workspace_id = workspace_id
        if self._scope_installer is not None:
            await self._scope_installer(actor_id=str(actor.user_id), workspace_id=str(workspace_id))
        return context

    def require_actor(self) -> UserId:
        """@brief 返回已固定 actor / Return the pinned actor."""
        if self.actor_id is None:
            raise PermissionError("Knowledge persistence requires prior authentication")
        return self.actor_id

    def require_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 验证 repository 参数属于已授权 Workspace / Verify a repository argument belongs to the authorized Workspace."""
        if self.workspace_id != workspace_id:
            raise PermissionError("Knowledge persistence requires prior Workspace authorization")


@dataclass(slots=True)
class InMemoryKnowledgeStore:
    """@brief API V2 Knowledge 的共享内存真相 / Shared in-memory truth for API V2 Knowledge."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    connections: dict[tuple[WorkspaceId, ConnectionId], ConnectionAggregate] = field(
        default_factory=dict
    )
    authorization_records: dict[
        tuple[WorkspaceId, ConnectionAuthorizationSessionId], ConnectionAuthorizationRecord
    ] = field(default_factory=dict)
    sources: dict[tuple[WorkspaceId, KnowledgeSourceId], KnowledgeSource] = field(
        default_factory=dict
    )
    versions: dict[
        tuple[WorkspaceId, KnowledgeSourceId, KnowledgeSourceVersionId],
        KnowledgeSourceVersion,
    ] = field(default_factory=dict)
    uploads: dict[tuple[WorkspaceId, UploadSessionId], UploadSession] = field(default_factory=dict)
    jobs: dict[JobId, tuple[Job, KnowledgeJobSpec]] = field(default_factory=dict)
    outbox_events: dict[str, KnowledgeOutboxEvent] = field(default_factory=dict)


class InMemoryKnowledgeRepository:
    """@brief copy-on-write snapshot 上的 Knowledge repository / Knowledge repository over a copy-on-write snapshot."""

    def __init__(
        self,
        connections: dict[tuple[WorkspaceId, ConnectionId], ConnectionAggregate],
        authorization_records: dict[
            tuple[WorkspaceId, ConnectionAuthorizationSessionId], ConnectionAuthorizationRecord
        ],
        sources: dict[tuple[WorkspaceId, KnowledgeSourceId], KnowledgeSource],
        versions: dict[
            tuple[WorkspaceId, KnowledgeSourceId, KnowledgeSourceVersionId],
            KnowledgeSourceVersion,
        ],
        uploads: dict[tuple[WorkspaceId, UploadSessionId], UploadSession],
    ) -> None:
        """@brief 绑定隔离事务快照 / Bind an isolated transaction snapshot."""
        self.connections = connections
        self.authorization_records = authorization_records
        self.sources = sources
        self.versions = versions
        self.uploads = uploads

    async def list_connections(
        self, workspace_id: WorkspaceId, page: KnowledgePageRequest
    ) -> KnowledgePage[Connection]:
        """@brief 按 ID keyset 列出 Connection / List Connections by ID keyset."""
        items = sorted(
            (
                aggregate.connection
                for (owner, _), aggregate in self.connections.items()
                if owner == workspace_id
                and (page.after is None or str(aggregate.connection.meta.id) > page.after)
            ),
            key=lambda item: str(item.meta.id),
        )
        window = items[: page.limit + 1]
        position = str(window[min(page.limit, len(window)) - 1].meta.id) if window else ""
        return _page(window, page.limit, position)

    async def get_connection(
        self,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAggregate | None:
        """@brief 精确读取 Connection / Read an exact Connection."""
        del for_update
        return self.connections.get((workspace_id, connection_id))

    async def add_connection(self, connection: ConnectionAggregate) -> None:
        """@brief 添加 Connection / Add a Connection."""
        key = (connection.ownership.workspace_id, connection.connection.meta.id)
        if key in self.connections:
            raise KnowledgeCasMismatch
        self.connections[key] = connection

    async def save_connection(
        self, connection: ConnectionAggregate, *, expected_revision: int
    ) -> None:
        """@brief 以旧 revision CAS 保存 Connection / CAS-save a Connection by old revision."""
        key = (connection.ownership.workspace_id, connection.connection.meta.id)
        current = self.connections.get(key)
        if (
            current is None
            or current.connection.meta.revision != expected_revision
            or connection.connection.meta.revision != expected_revision + 1
        ):
            raise KnowledgeCasMismatch
        self.connections[key] = connection

    async def add_authorization_record(self, record: ConnectionAuthorizationRecord) -> None:
        """@brief 添加 session 并强制 actor-scoped idempotency 唯一 / Add a session and enforce actor-scoped idempotency uniqueness."""
        if (
            await self.get_authorization_record_by_idempotency(
                record.ownership.workspace_id,
                record.ownership.created_by,
                record.idempotency.key_hash,
            )
            is not None
        ):
            raise KnowledgeCasMismatch
        key = (record.ownership.workspace_id, record.session.id)
        if key in self.authorization_records:
            raise KnowledgeCasMismatch
        self.authorization_records[key] = record

    async def get_authorization_record_by_idempotency(
        self,
        workspace_id: WorkspaceId,
        created_by: UserId,
        idempotency_key_hash: str,
        *,
        for_update: bool = False,
    ) -> ConnectionAuthorizationRecord | None:
        """@brief 按 Workspace、actor 与 keyed hash 精确读取 / Read by Workspace, actor, and keyed hash."""
        del for_update
        return next(
            (
                record
                for record in self.authorization_records.values()
                if record.ownership.workspace_id == workspace_id
                and record.ownership.created_by == created_by
                and record.idempotency.key_hash == idempotency_key_hash
            ),
            None,
        )

    async def get_authorization_record(
        self,
        workspace_id: WorkspaceId,
        session_id: ConnectionAuthorizationSessionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAuthorizationRecord | None:
        """@brief 按 Workspace 与 session ID 读取 / Read by Workspace and session ID."""
        del for_update
        return self.authorization_records.get((workspace_id, session_id))

    async def save_authorization_record(
        self, record: ConnectionAuthorizationRecord, *, expected_state: str
    ) -> None:
        """@brief 以旧 state CAS 保存授权结果 / CAS-save an authorization result by old state."""
        key = (record.ownership.workspace_id, record.session.id)
        current = self.authorization_records.get(key)
        if current is None or current.state.value != expected_state:
            raise KnowledgeCasMismatch
        self.authorization_records[key] = record

    async def list_sources(
        self, workspace_id: WorkspaceId, page: KnowledgePageRequest
    ) -> KnowledgePage[KnowledgeSource]:
        """@brief 按 ID keyset 列出来源 / List sources by ID keyset."""
        items = sorted(
            (
                source
                for (owner, _), source in self.sources.items()
                if owner == workspace_id
                and (page.after is None or str(source.meta.id) > page.after)
            ),
            key=lambda item: str(item.meta.id),
        )
        window = items[: page.limit + 1]
        position = str(window[min(page.limit, len(window)) - 1].meta.id) if window else ""
        return _page(window, page.limit, position)

    async def get_source(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        *,
        for_update: bool = False,
    ) -> KnowledgeSource | None:
        """@brief 精确读取来源 / Read an exact source."""
        del for_update
        return self.sources.get((workspace_id, source_id))

    async def list_policy_default_sources(
        self,
        workspace_id: WorkspaceId,
        *,
        include_source_ids: tuple[KnowledgeSourceId, ...],
        exclude_source_ids: tuple[KnowledgeSourceId, ...],
        limit: int,
    ) -> tuple[KnowledgeSource, ...]:
        """@brief 有界返回 policy-default 候选与显式 include / Return bounded policy-default candidates plus explicit includes."""
        if not 1 <= limit <= 200:
            raise ValueError("policy-default source limit is invalid")
        included = set(include_source_ids)
        excluded = set(exclude_source_ids)
        items = sorted(
            (
                source
                for (owner, source_id), source in self.sources.items()
                if owner == workspace_id
                and source_id not in excluded
                and (source.enabled or source_id in included)
                and source.ingestion.status
                not in {KnowledgeIngestionStatus.DELETING, KnowledgeIngestionStatus.DELETED}
            ),
            key=lambda item: str(item.meta.id),
        )
        return tuple(items[:limit])

    async def add_source(
        self,
        source: KnowledgeSource,
        initial_version: KnowledgeSourceVersion | None,
    ) -> None:
        """@brief 原子添加来源与可选首版本 / Atomically add a source and optional first version."""
        key = (source.workspace_id, source.meta.id)
        if key in self.sources:
            raise KnowledgeCasMismatch
        if initial_version is not None:
            version_key = (
                initial_version.workspace_id,
                initial_version.snapshot.source_id,
                initial_version.meta.id,
            )
            if version_key in self.versions:
                raise KnowledgeCasMismatch
            self.versions[version_key] = initial_version
        self.sources[key] = source

    async def save_source(self, source: KnowledgeSource, *, expected_revision: int) -> None:
        """@brief 以旧 revision CAS 保存来源 / CAS-save a source by old revision."""
        key = (source.workspace_id, source.meta.id)
        current = self.sources.get(key)
        if (
            current is None
            or current.meta.revision != expected_revision
            or source.meta.revision != expected_revision + 1
        ):
            raise KnowledgeCasMismatch
        self.sources[key] = source

    async def list_versions(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        page: KnowledgePageRequest,
    ) -> KnowledgePage[KnowledgeSourceVersion]:
        """@brief 按单调 version number 分页 / Page by monotonic version number."""
        after = _version_after(page.after)
        items = sorted(
            (
                version
                for (owner, source, _), version in self.versions.items()
                if owner == workspace_id
                and source == source_id
                and (after is None or version.snapshot.version_number > after)
            ),
            key=lambda item: item.snapshot.version_number,
        )
        window = items[: page.limit + 1]
        position = (
            str(window[min(page.limit, len(window)) - 1].snapshot.version_number) if window else ""
        )
        return _page(window, page.limit, position)

    async def get_version(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        version_id: KnowledgeSourceVersionId,
    ) -> KnowledgeSourceVersion | None:
        """@brief 按 Workspace/source/version 三元组读取 / Read by Workspace/source/version tuple."""
        return self.versions.get((workspace_id, source_id, version_id))

    async def add_version(self, version: KnowledgeSourceVersion) -> None:
        """@brief 添加不可变 version / Add an immutable version."""
        key = (version.workspace_id, version.snapshot.source_id, version.meta.id)
        if key in self.versions or any(
            owner == version.workspace_id
            and source == version.snapshot.source_id
            and item.snapshot.version_number == version.snapshot.version_number
            for (owner, source, _), item in self.versions.items()
        ):
            raise KnowledgeCasMismatch
        self.versions[key] = version

    async def add_upload(self, upload: UploadSession) -> None:
        """@brief 添加统一 upload session / Add a unified upload session."""
        key = (upload.view.workspace_id, upload.view.id)
        if key in self.uploads:
            raise KnowledgeCasMismatch
        self.uploads[key] = upload

    async def get_upload(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        *,
        for_update: bool = False,
    ) -> UploadSession | None:
        """@brief 精确读取统一 upload / Read an exact unified upload."""
        del for_update
        return self.uploads.get((workspace_id, upload_id))

    async def save_upload(self, upload: UploadSession, *, expected_generation: int) -> None:
        """@brief 以 generation CAS 保存 upload / CAS-save an upload by generation."""
        key = (upload.view.workspace_id, upload.view.id)
        current = self.uploads.get(key)
        if (
            current is None
            or current.generation != expected_generation
            or upload.generation != expected_generation + 1
        ):
            raise KnowledgeCasMismatch
        self.uploads[key] = upload


class _MemoryKnowledgeJobSink:
    """@brief 同内存事务 Job sink / In-memory same-transaction Job sink."""

    def __init__(
        self,
        jobs: dict[JobId, tuple[Job, KnowledgeJobSpec]],
        authorizer: _TrackingKnowledgeAuthorizer,
    ) -> None:
        self._jobs = jobs
        self._authorizer = authorizer

    async def add(self, job: Job, spec: KnowledgeJobSpec) -> None:
        """@brief 添加唯一统一 Job / Add one unique unified Job."""
        self._authorizer.require_workspace(job.workspace_id)
        self._authorizer.require_actor()
        if job.meta.id in self._jobs:
            raise KnowledgeCasMismatch
        self._jobs[job.meta.id] = (job, spec)


class _MemoryKnowledgeOutbox:
    """@brief 同内存事务 outbox / In-memory same-transaction outbox."""

    def __init__(
        self,
        events: dict[str, KnowledgeOutboxEvent],
        authorizer: _TrackingKnowledgeAuthorizer,
    ) -> None:
        self._events = events
        self._authorizer = authorizer

    async def add(self, event: KnowledgeOutboxEvent) -> None:
        """@brief 添加唯一 secret-free event / Add one unique secret-free event."""
        self._authorizer.require_workspace(event.workspace_id)
        if self._authorizer.require_actor() != event.actor_id:
            raise PermissionError("outbox actor does not match authenticated actor")
        if str(event.event_id) in self._events:
            raise KnowledgeCasMismatch
        self._events[str(event.event_id)] = event


class InMemoryKnowledgeUnitOfWork:
    """@brief Access 与 Knowledge 一致锁定的 copy-on-write UoW / Copy-on-write UoW consistently locking Access and Knowledge."""

    def __init__(self, store: InMemoryKnowledgeStore, access_store: InMemoryAccessStore) -> None:
        """@brief 绑定共享状态 / Bind shared state."""
        self._store = store
        self._access_store = access_store
        self._repository: InMemoryKnowledgeRepository | None = None
        self._authorizer: _TrackingKnowledgeAuthorizer | None = None
        self._jobs: _MemoryKnowledgeJobSink | None = None
        self._outbox: _MemoryKnowledgeOutbox | None = None
        self._snapshot: tuple[Any, ...] | None = None
        self._entered = False
        self._committed = False
        self._rolled_back = False

    @property
    def repository(self) -> KnowledgeRepository:
        """@brief 返回事务 repository / Return the transactional repository."""
        if self._repository is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._repository

    @property
    def authorizer(self) -> _TrackingKnowledgeAuthorizer:
        """@brief 返回集中 authorizer / Return the central authorizer."""
        if self._authorizer is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._authorizer

    @property
    def jobs(self) -> _MemoryKnowledgeJobSink:
        """@brief 返回同事务 Job sink / Return the same-transaction Job sink."""
        if self._jobs is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._jobs

    @property
    def outbox(self) -> _MemoryKnowledgeOutbox:
        """@brief 返回同事务 outbox / Return the same-transaction outbox."""
        if self._outbox is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._outbox

    async def __aenter__(self) -> Self:
        """@brief 固定顺序加锁并复制不可变聚合映射 / Lock in fixed order and copy immutable-aggregate mappings."""
        if self._entered:
            raise RuntimeError("Knowledge unit of work cannot be re-entered")
        await self._access_store.lock.acquire()
        try:
            await self._store.lock.acquire()
        except BaseException:
            self._access_store.lock.release()
            raise
        self._entered = True
        connections = dict(self._store.connections)
        authorizations = dict(self._store.authorization_records)
        sources = dict(self._store.sources)
        versions = dict(self._store.versions)
        uploads = dict(self._store.uploads)
        jobs = dict(self._store.jobs)
        events = dict(self._store.outbox_events)
        self._snapshot = (
            connections,
            authorizations,
            sources,
            versions,
            uploads,
            jobs,
            events,
        )
        access_repository = InMemoryAccessRepository(
            users=dict(self._access_store.users),
            workspaces=dict(self._access_store.workspaces),
            memberships=dict(self._access_store.memberships),
            invitations=dict(self._access_store.invitations),
            account_deletions=dict(self._access_store.account_deletions),
        )
        self._authorizer = _TrackingKnowledgeAuthorizer(AccessAuthorizer(access_repository))
        self._repository = InMemoryKnowledgeRepository(
            connections, authorizations, sources, versions, uploads
        )
        self._jobs = _MemoryKnowledgeJobSink(jobs, self._authorizer)
        self._outbox = _MemoryKnowledgeOutbox(events, self._authorizer)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 回滚未提交快照并释放锁 / Roll back an uncommitted snapshot and release locks."""
        del exc, traceback
        if self._entered:
            if exc_type is not None or not self._committed:
                await self.rollback()
            self._clear()
            self._entered = False
            self._store.lock.release()
            self._access_store.lock.release()
        return None

    async def commit(self) -> None:
        """@brief 原子发布完整 snapshot / Atomically publish the complete snapshot."""
        if self._snapshot is None or not self._entered:
            raise RuntimeError("Knowledge unit of work has not been entered")
        if self._committed or self._rolled_back:
            raise RuntimeError("Knowledge unit of work cannot commit in its current state")
        (
            self._store.connections,
            self._store.authorization_records,
            self._store.sources,
            self._store.versions,
            self._store.uploads,
            self._store.jobs,
            self._store.outbox_events,
        ) = self._snapshot
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等丢弃 snapshot / Idempotently discard the snapshot."""
        if not self._entered:
            raise RuntimeError("Knowledge unit of work has not been entered")
        self._rolled_back = True

    def _clear(self) -> None:
        """@brief 清除事务内 adapter / Clear transaction-bound adapters."""
        self._repository = None
        self._authorizer = None
        self._jobs = None
        self._outbox = None
        self._snapshot = None


class InMemoryKnowledgeUnitOfWorkFactory:
    """@brief 创建共享状态的内存 Knowledge UoW / Create in-memory Knowledge UoWs over shared state."""

    def __init__(
        self,
        access_store: InMemoryAccessStore,
        *,
        store: InMemoryKnowledgeStore | None = None,
    ) -> None:
        """@brief 绑定 Access store 与可选 Knowledge store / Bind Access and optional Knowledge stores."""
        self.access_store = access_store
        self.store = store or InMemoryKnowledgeStore()

    def __call__(self) -> InMemoryKnowledgeUnitOfWork:
        """@brief 创建未进入 UoW / Create a not-yet-entered UoW."""
        return InMemoryKnowledgeUnitOfWork(self.store, self.access_store)


class PostgresKnowledgeRepository:
    """@brief 绑定一个 PostgreSQL transaction 的 Knowledge repository / Knowledge repository bound to one PostgreSQL transaction."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingKnowledgeAuthorizer,
        launch_cipher: AuthorizationLaunchCipher,
    ) -> None:
        """@brief 绑定 Session、授权上下文与专用 AEAD / Bind the Session, authorization context, and dedicated AEAD."""
        self._session = session
        self._authorizer = authorizer
        self._launch_cipher = launch_cipher
        self._source_state: dict[tuple[str, str], tuple[str, int]] = {}
        self._upload_completed_at: dict[tuple[str, str], datetime | None] = {}

    async def list_connections(
        self, workspace_id: WorkspaceId, page: KnowledgePageRequest
    ) -> KnowledgePage[Connection]:
        """@brief 按 ID keyset 列出安全 Connection / List safe Connections by ID keyset."""
        self._authorizer.require_workspace(workspace_id)
        statement = (
            select(ConnectionRecord)
            .where(ConnectionRecord.workspace_id == str(workspace_id))
            .order_by(ConnectionRecord.id)
            .limit(page.limit + 1)
        )
        if page.after is not None:
            statement = statement.where(ConnectionRecord.id > page.after)
        records = list((await self._session.scalars(statement)).all())
        items = [_connection_from_record(record).connection for record in records]
        return _page(
            items, page.limit, records[min(page.limit, len(records)) - 1].id if records else ""
        )

    async def get_connection(
        self,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAggregate | None:
        """@brief 以 Workspace-first 谓词读取 Connection / Read a Connection using a Workspace-first predicate."""
        self._authorizer.require_workspace(workspace_id)
        statement = select(ConnectionRecord).where(
            ConnectionRecord.workspace_id == str(workspace_id),
            ConnectionRecord.id == str(connection_id),
        )
        if for_update:
            statement = statement.with_for_update()
        record = await self._session.scalar(statement)
        return None if record is None else _connection_from_record(record)

    async def add_connection(self, connection: ConnectionAggregate) -> None:
        """@brief 仅保存 vault reference 的 Connection / Add a Connection storing only a vault reference."""
        public = connection.connection
        self._authorizer.require_workspace(public.workspace_id)
        if self._authorizer.require_actor() != connection.ownership.created_by:
            raise PermissionError("Connection creator does not match authenticated actor")
        self._session.add(
            ConnectionRecord(
                id=str(public.meta.id),
                workspace_id=str(public.workspace_id),
                created_by=str(connection.ownership.created_by),
                provider=public.provider.value,
                auth_method=public.auth_method.value,
                display_name=public.display_name,
                status=public.status.value,
                scopes=list(public.scopes),
                last_validated_at=public.last_validated_at,
                problem=_problem_payload(public.problem),
                credential_reference=str(connection.credential_reference),
                created_at=public.meta.created_at,
                updated_at=public.meta.updated_at,
                revision=public.meta.revision,
                extensions={},
            )
        )

    async def save_connection(
        self, connection: ConnectionAggregate, *, expected_revision: int
    ) -> None:
        """@brief 以 affected-row CAS 保存 Connection / Save a Connection with affected-row CAS."""
        public = connection.connection
        self._authorizer.require_workspace(public.workspace_id)
        if public.meta.revision != expected_revision + 1:
            raise KnowledgeCasMismatch
        result = await self._session.execute(
            update(ConnectionRecord)
            .where(
                ConnectionRecord.workspace_id == str(public.workspace_id),
                ConnectionRecord.id == str(public.meta.id),
                ConnectionRecord.revision == expected_revision,
            )
            .values(
                status=public.status.value,
                scopes=list(public.scopes),
                last_validated_at=public.last_validated_at,
                problem=_problem_payload(public.problem),
                updated_at=public.meta.updated_at,
                revision=public.meta.revision,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise KnowledgeCasMismatch

    async def add_authorization_record(self, record: ConnectionAuthorizationRecord) -> None:
        """@brief 加锁幂等 scope 并写入 AEAD launch / Lock the idempotency scope and write the AEAD-sealed launch."""
        workspace_id = record.ownership.workspace_id
        self._authorizer.require_workspace(workspace_id)
        if self._authorizer.require_actor() != record.ownership.created_by:
            raise PermissionError("authorization-session creator does not match actor")
        await self._lock_authorization_scope(
            workspace_id,
            record.ownership.created_by,
            record.idempotency.key_hash,
        )
        existing = await self._authorization_row_by_idempotency(
            workspace_id,
            record.ownership.created_by,
            record.idempotency.key_hash,
        )
        if existing is not None:
            raise KnowledgeCasMismatch
        encrypted = self._launch_cipher.encrypt(record)
        self._session.add(
            ConnectionAuthorizationRecordModel(
                id=str(record.session.id),
                workspace_id=str(workspace_id),
                created_by=str(record.ownership.created_by),
                idempotency_key_hash=record.idempotency.key_hash,
                request_fingerprint=record.idempotency.request_fingerprint,
                provider=record.session.provider.value,
                flow=record.session.flow.value,
                launch_key_id=encrypted.key_id,
                launch_nonce=encrypted.nonce,
                launch_ciphertext=encrypted.ciphertext,
                expires_at=record.session.expires_at,
                idempotency_expires_at=record.idempotency.expires_at,
                requested_scopes=list(record.requested_scopes),
                state=record.state.value,
                state_sha256=record.state_sha256,
                provider_session_reference=str(record.provider_session_reference),
                connection_id=None if record.connection_id is None else str(record.connection_id),
                problem=_problem_payload(record.problem),
                created_at=record.created_at,
                updated_at=record.created_at,
                revision=1,
                extensions={},
            )
        )

    async def get_authorization_record_by_idempotency(
        self,
        workspace_id: WorkspaceId,
        created_by: UserId,
        idempotency_key_hash: str,
        *,
        for_update: bool = False,
    ) -> ConnectionAuthorizationRecord | None:
        """@brief 按专用 scope 读取并验证 AEAD launch / Read by dedicated scope and authenticate the AEAD launch."""
        self._authorizer.require_workspace(workspace_id)
        if self._authorizer.require_actor() != created_by:
            raise PermissionError("authorization replay actor does not match authenticated actor")
        if for_update:
            await self._lock_authorization_scope(workspace_id, created_by, idempotency_key_hash)
        record = await self._authorization_row_by_idempotency(
            workspace_id, created_by, idempotency_key_hash
        )
        return None if record is None else self._authorization_from_record(record)

    async def get_authorization_record(
        self,
        workspace_id: WorkspaceId,
        session_id: ConnectionAuthorizationSessionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAuthorizationRecord | None:
        """@brief 按 Workspace/session 精确读取 callback 记录 / Read a callback record by exact Workspace/session tuple."""
        self._authorizer.require_workspace(workspace_id)
        statement = select(ConnectionAuthorizationRecordModel).where(
            ConnectionAuthorizationRecordModel.workspace_id == str(workspace_id),
            ConnectionAuthorizationRecordModel.id == str(session_id),
        )
        if for_update:
            statement = statement.with_for_update()
        record = await self._session.scalar(statement)
        return None if record is None else self._authorization_from_record(record)

    async def save_authorization_record(
        self, record: ConnectionAuthorizationRecord, *, expected_state: str
    ) -> None:
        """@brief 以旧 state CAS 一次性完成 callback / Complete a callback once using old-state CAS."""
        self._authorizer.require_workspace(record.ownership.workspace_id)
        result = await self._session.execute(
            update(ConnectionAuthorizationRecordModel)
            .where(
                ConnectionAuthorizationRecordModel.workspace_id
                == str(record.ownership.workspace_id),
                ConnectionAuthorizationRecordModel.id == str(record.session.id),
                ConnectionAuthorizationRecordModel.state == expected_state,
            )
            .values(
                state=record.state.value,
                connection_id=None if record.connection_id is None else str(record.connection_id),
                problem=_problem_payload(record.problem),
                updated_at=datetime.now(UTC),
                revision=ConnectionAuthorizationRecordModel.revision + 1,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise KnowledgeCasMismatch

    async def list_sources(
        self, workspace_id: WorkspaceId, page: KnowledgePageRequest
    ) -> KnowledgePage[KnowledgeSource]:
        """@brief 按 ID keyset 列出来源 / List sources by ID keyset."""
        self._authorizer.require_workspace(workspace_id)
        statement = (
            select(KnowledgeSourceRecord)
            .where(KnowledgeSourceRecord.workspace_id == str(workspace_id))
            .order_by(KnowledgeSourceRecord.id)
            .limit(page.limit + 1)
        )
        if page.after is not None:
            statement = statement.where(KnowledgeSourceRecord.id > page.after)
        records = list((await self._session.scalars(statement)).all())
        items = await self._sources_from_records(records)
        return _page(
            items, page.limit, records[min(page.limit, len(records)) - 1].id if records else ""
        )

    async def get_source(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        *,
        for_update: bool = False,
    ) -> KnowledgeSource | None:
        """@brief Workspace-first 读取并完整 rehydrate 来源 / Workspace-first read and full source rehydration."""
        self._authorizer.require_workspace(workspace_id)
        statement = select(KnowledgeSourceRecord).where(
            KnowledgeSourceRecord.workspace_id == str(workspace_id),
            KnowledgeSourceRecord.id == str(source_id),
        )
        if for_update:
            statement = statement.with_for_update()
        record = await self._session.scalar(statement)
        return None if record is None else await self._source_from_record(record)

    async def list_policy_default_sources(
        self,
        workspace_id: WorkspaceId,
        *,
        include_source_ids: tuple[KnowledgeSourceId, ...],
        exclude_source_ids: tuple[KnowledgeSourceId, ...],
        limit: int,
    ) -> tuple[KnowledgeSource, ...]:
        """@brief 在 SQL 内做 Workspace/exclude/lifecycle bound / Apply Workspace, exclusion, and lifecycle bounds in SQL."""
        self._authorizer.require_workspace(workspace_id)
        if not 1 <= limit <= 200:
            raise ValueError("policy-default source limit is invalid")
        includes = [str(item) for item in include_source_ids]
        excludes = [str(item) for item in exclude_source_ids]
        predicate: Any = KnowledgeSourceRecord.enabled.is_(True)
        if includes:
            predicate = or_(predicate, KnowledgeSourceRecord.id.in_(includes))
        statement = (
            select(KnowledgeSourceRecord)
            .where(
                KnowledgeSourceRecord.workspace_id == str(workspace_id),
                predicate,
                KnowledgeSourceRecord.ingestion_state.not_in(("deleting", "deleted")),
            )
            .order_by(KnowledgeSourceRecord.id)
            .limit(limit)
        )
        if excludes:
            statement = statement.where(KnowledgeSourceRecord.id.not_in(excludes))
        records = list((await self._session.scalars(statement)).all())
        return tuple(await self._sources_from_records(records))

    async def add_source(
        self,
        source: KnowledgeSource,
        initial_version: KnowledgeSourceVersion | None,
    ) -> None:
        """@brief 添加来源、policy 与可选首版本 / Add a source, policy, and optional initial version."""
        self._authorizer.require_workspace(source.workspace_id)
        actor_id = self._authorizer.require_actor()
        if actor_id != source.created_by:
            raise PermissionError("Knowledge source creator does not match authenticated actor")
        self._session.add(_source_record(source, actor_id))
        await self._add_policy(source, actor_id)
        self._source_state[(str(source.workspace_id), str(source.meta.id))] = (
            str(actor_id),
            source.visibility.policy_version,
        )
        if initial_version is not None:
            self._add_version_record(initial_version, actor_id)

    async def save_source(self, source: KnowledgeSource, *, expected_revision: int) -> None:
        """@brief CAS 根并 append 新 policy snapshot / CAS the root and append a new policy snapshot."""
        self._authorizer.require_workspace(source.workspace_id)
        if source.meta.revision != expected_revision + 1:
            raise KnowledgeCasMismatch
        owner_id, current_policy = await self._source_owner_and_policy(
            source.workspace_id, source.meta.id
        )
        result = await self._session.execute(
            update(KnowledgeSourceRecord)
            .where(
                KnowledgeSourceRecord.workspace_id == str(source.workspace_id),
                KnowledgeSourceRecord.id == str(source.meta.id),
                KnowledgeSourceRecord.revision == expected_revision,
            )
            .values(
                title=source.name,
                enabled=source.enabled,
                current_policy_version=source.visibility.policy_version,
                current_version_id=(
                    None if source.current_version_id is None else str(source.current_version_id)
                ),
                version_counter=source.version_counter,
                ingestion_state=source.ingestion.status.value,
                document_count=source.ingestion.document_count,
                chunk_count=source.ingestion.chunk_count,
                last_success_at=source.ingestion.last_success_at,
                last_problem=_problem_payload(source.ingestion.last_problem),
                updated_at=source.meta.updated_at,
                revision=source.meta.revision,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise KnowledgeCasMismatch
        if source.visibility.policy_version != current_policy:
            if source.visibility.policy_version != current_policy + 1:
                raise KnowledgeCasMismatch
            await self._add_policy(source, UserId(owner_id))
        self._source_state[(str(source.workspace_id), str(source.meta.id))] = (
            owner_id,
            source.visibility.policy_version,
        )

    async def list_versions(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        page: KnowledgePageRequest,
    ) -> KnowledgePage[KnowledgeSourceVersion]:
        """@brief 按 source-local version number keyset 分页 / Page by source-local version-number keyset."""
        self._authorizer.require_workspace(workspace_id)
        after = _version_after(page.after)
        statement = (
            select(KnowledgeSourceVersionRecord)
            .where(
                KnowledgeSourceVersionRecord.workspace_id == str(workspace_id),
                KnowledgeSourceVersionRecord.source_id == str(source_id),
            )
            .order_by(KnowledgeSourceVersionRecord.version_no)
            .limit(page.limit + 1)
        )
        if after is not None:
            statement = statement.where(KnowledgeSourceVersionRecord.version_no > after)
        records = list((await self._session.scalars(statement)).all())
        items = [_version_from_record(record) for record in records]
        position = str(records[min(page.limit, len(records)) - 1].version_no) if records else ""
        return _page(items, page.limit, position)

    async def get_version(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        version_id: KnowledgeSourceVersionId,
    ) -> KnowledgeSourceVersion | None:
        """@brief 用三元边界读取 version / Read a version through its three-part boundary."""
        self._authorizer.require_workspace(workspace_id)
        record = await self._session.scalar(
            select(KnowledgeSourceVersionRecord).where(
                KnowledgeSourceVersionRecord.workspace_id == str(workspace_id),
                KnowledgeSourceVersionRecord.source_id == str(source_id),
                KnowledgeSourceVersionRecord.id == str(version_id),
            )
        )
        return None if record is None else _version_from_record(record)

    async def add_version(self, version: KnowledgeSourceVersion) -> None:
        """@brief 使用来源 owner 写入新 version / Add a new version using the source owner."""
        self._authorizer.require_workspace(version.workspace_id)
        owner_id, _ = await self._source_owner_and_policy(
            version.workspace_id, version.snapshot.source_id
        )
        self._add_version_record(version, UserId(owner_id))

    async def add_upload(self, upload: UploadSession) -> None:
        """@brief 写入完整冻结声明与 grant 的统一 upload / Add a unified upload with its full frozen declaration and grant."""
        self._authorizer.require_workspace(upload.view.workspace_id)
        now = upload.created_at
        artifact = upload.view.artifact_ref
        claim = upload.completion_claim
        consumer = upload.claimed_by
        completed_at = now if upload.view.status is UploadStatus.COMPLETED else None
        self._session.add(
            KnowledgeUploadSessionRecord(
                id=str(upload.view.id),
                workspace_id=str(upload.view.workspace_id),
                status=upload.view.status.value,
                filename=upload.declaration.filename,
                media_type=upload.declaration.media_type,
                declared_size_bytes=upload.declaration.size_bytes,
                declared_sha256=upload.declaration.sha256,
                upload_url=upload.view.grant.upload_url,
                required_headers=dict(upload.view.grant.required_headers),
                expires_at=upload.view.expires_at,
                completion_size_bytes=None if claim is None else claim.size_bytes,
                completion_sha256=None if claim is None else claim.sha256,
                verification_operation_id=(
                    None
                    if upload.verification_operation_id is None
                    else str(upload.verification_operation_id)
                ),
                failure_code=upload.failure_code,
                completed_at=completed_at,
                artifact_type=None if artifact is None else artifact.resource_type,
                artifact_id=None if artifact is None else artifact.id,
                artifact_revision=None if artifact is None else artifact.revision,
                claimed_by_type=None if consumer is None else consumer.resource_type,
                claimed_by_id=None if consumer is None else consumer.id,
                claimed_by_revision=None if consumer is None else consumer.revision,
                claimed_by_job_id=(
                    consumer.id
                    if consumer is not None and consumer.resource_type == "job"
                    else None
                ),
                consumed_at=now if consumer is not None else None,
                legacy_payload=False,
                created_at=upload.created_at,
                updated_at=upload.created_at,
                revision=upload.generation,
                extensions={},
            )
        )
        self._upload_completed_at[(str(upload.view.workspace_id), str(upload.view.id))] = (
            completed_at
        )

    async def get_upload(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        *,
        for_update: bool = False,
    ) -> UploadSession | None:
        """@brief 读取非 legacy 的完整 V2 upload / Read a complete non-legacy V2 upload."""
        self._authorizer.require_workspace(workspace_id)
        statement = select(KnowledgeUploadSessionRecord).where(
            KnowledgeUploadSessionRecord.workspace_id == str(workspace_id),
            KnowledgeUploadSessionRecord.id == str(upload_id),
            KnowledgeUploadSessionRecord.legacy_payload.is_(False),
        )
        if for_update:
            statement = statement.with_for_update()
        record = await self._session.scalar(statement)
        if record is None:
            return None
        self._upload_completed_at[(record.workspace_id, record.id)] = record.completed_at
        return _upload_from_record(record)

    async def save_upload(self, upload: UploadSession, *, expected_generation: int) -> None:
        """@brief CAS 状态与唯一 claim / CAS upload state and its sole claim."""
        self._authorizer.require_workspace(upload.view.workspace_id)
        if upload.generation != expected_generation + 1:
            raise KnowledgeCasMismatch
        now = datetime.now(UTC)
        key = (str(upload.view.workspace_id), str(upload.view.id))
        completed_at = self._upload_completed_at.get(key)
        if upload.view.status is UploadStatus.COMPLETED and completed_at is None:
            completed_at = now
        if upload.view.status is not UploadStatus.COMPLETED:
            completed_at = None
        claim = upload.completion_claim
        artifact = upload.view.artifact_ref
        consumer = upload.claimed_by
        result = await self._session.execute(
            update(KnowledgeUploadSessionRecord)
            .where(
                KnowledgeUploadSessionRecord.workspace_id == str(upload.view.workspace_id),
                KnowledgeUploadSessionRecord.id == str(upload.view.id),
                KnowledgeUploadSessionRecord.revision == expected_generation,
                KnowledgeUploadSessionRecord.legacy_payload.is_(False),
            )
            .values(
                status=upload.view.status.value,
                completion_size_bytes=None if claim is None else claim.size_bytes,
                completion_sha256=None if claim is None else claim.sha256,
                verification_operation_id=(
                    None
                    if upload.verification_operation_id is None
                    else str(upload.verification_operation_id)
                ),
                failure_code=upload.failure_code,
                completed_at=completed_at,
                artifact_type=None if artifact is None else artifact.resource_type,
                artifact_id=None if artifact is None else artifact.id,
                artifact_revision=None if artifact is None else artifact.revision,
                claimed_by_type=None if consumer is None else consumer.resource_type,
                claimed_by_id=None if consumer is None else consumer.id,
                claimed_by_revision=None if consumer is None else consumer.revision,
                claimed_by_job_id=(
                    consumer.id
                    if consumer is not None and consumer.resource_type == "job"
                    else None
                ),
                consumed_at=now if consumer is not None else None,
                updated_at=now,
                revision=upload.generation,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise KnowledgeCasMismatch
        self._upload_completed_at[key] = completed_at

    async def _lock_authorization_scope(
        self,
        workspace_id: WorkspaceId,
        created_by: UserId,
        key_hash: str,
    ) -> None:
        """@brief 用 transaction advisory lock 串行化 absent-key create / Serialize absent-key creation with a transaction advisory lock."""
        parts = ("knowledge.authorization", str(workspace_id), str(created_by), key_hash)
        scope = "".join(f"{len(part)}:{part}" for part in parts)
        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
            {"scope": scope},
        )

    async def _authorization_row_by_idempotency(
        self,
        workspace_id: WorkspaceId,
        created_by: UserId,
        key_hash: str,
    ) -> ConnectionAuthorizationRecordModel | None:
        """@brief 执行 actor-scoped exact lookup / Execute the actor-scoped exact lookup."""
        return cast(
            ConnectionAuthorizationRecordModel | None,
            await self._session.scalar(
                select(ConnectionAuthorizationRecordModel).where(
                    ConnectionAuthorizationRecordModel.workspace_id == str(workspace_id),
                    ConnectionAuthorizationRecordModel.created_by == str(created_by),
                    ConnectionAuthorizationRecordModel.idempotency_key_hash == key_hash,
                )
            ),
        )

    def _authorization_from_record(
        self, record: ConnectionAuthorizationRecordModel
    ) -> ConnectionAuthorizationRecord:
        """@brief 认证 ciphertext 并重建私有授权记录 / Authenticate ciphertext and rebuild the private authorization record."""
        session = self._launch_cipher.decrypt(
            EncryptedAuthorizationLaunch(
                record.launch_key_id,
                bytes(record.launch_nonce),
                bytes(record.launch_ciphertext),
            ),
            workspace_id=record.workspace_id,
            created_by=record.created_by,
            session_id=record.id,
            provider=record.provider,
            flow=record.flow,
            expires_at=record.expires_at,
        )
        return ConnectionAuthorizationRecord(
            session,
            ConnectionOwnership(WorkspaceId(record.workspace_id), UserId(record.created_by)),
            tuple(record.requested_scopes),
            ConnectionAuthorizationState(record.state),
            record.state_sha256,
            ProviderSessionReference(record.provider_session_reference),
            ConnectionAuthorizationIdempotency(
                record.idempotency_key_hash,
                record.request_fingerprint,
                record.idempotency_expires_at,
            ),
            record.created_at,
            None if record.connection_id is None else ConnectionId(record.connection_id),
            _load_problem(record.problem),
        )

    async def _source_from_record(self, record: KnowledgeSourceRecord) -> KnowledgeSource:
        """@brief 加载当前 policy/grants 并重建来源 / Load current policy/grants and rebuild a source."""
        return (await self._sources_from_records((record,)))[0]

    async def _sources_from_records(
        self, records: Sequence[KnowledgeSourceRecord]
    ) -> list[KnowledgeSource]:
        """@brief 三次有界查询批量重建来源页 / Rebuild a source page with three bounded queries."""
        if not records:
            return []
        workspace_id = records[0].workspace_id
        if any(record.workspace_id != workspace_id for record in records):
            raise ValueError("Knowledge source hydration cannot cross Workspaces")
        policy_keys = [(record.id, record.current_policy_version) for record in records]
        policy_records = list(
            (
                await self._session.scalars(
                    select(KnowledgeVisibilityPolicyRecord).where(
                        KnowledgeVisibilityPolicyRecord.workspace_id == workspace_id,
                        tuple_(
                            KnowledgeVisibilityPolicyRecord.source_id,
                            KnowledgeVisibilityPolicyRecord.policy_version,
                        ).in_(policy_keys),
                    )
                )
            ).all()
        )
        policies = {(policy.source_id, policy.policy_version): policy for policy in policy_records}
        if len(policies) != len(policy_keys):
            raise ValueError("Knowledge source has no unique current visibility policy")
        policy_ids = [policy.id for policy in policy_records]
        grant_records = list(
            (
                await self._session.scalars(
                    select(KnowledgeVisibilityGrantRecord)
                    .where(
                        KnowledgeVisibilityGrantRecord.workspace_id == workspace_id,
                        KnowledgeVisibilityGrantRecord.policy_id.in_(policy_ids),
                    )
                    .order_by(
                        KnowledgeVisibilityGrantRecord.policy_id,
                        KnowledgeVisibilityGrantRecord.ordinal,
                    )
                )
            ).all()
        )
        grants_by_policy: dict[str, list[KnowledgeVisibilityGrantRecord]] = {}
        for grant in grant_records:
            grants_by_policy.setdefault(grant.policy_id, []).append(grant)
        return [
            self._source_from_rows(
                record,
                policies[(record.id, record.current_policy_version)],
                grants_by_policy.get(
                    policies[(record.id, record.current_policy_version)].id,
                    [],
                ),
            )
            for record in records
        ]

    def _source_from_rows(
        self,
        record: KnowledgeSourceRecord,
        policy_record: KnowledgeVisibilityPolicyRecord,
        grant_records: Sequence[KnowledgeVisibilityGrantRecord],
    ) -> KnowledgeSource:
        """@brief 从已批量读取的 relational rows 重建来源 / Rebuild a source from preloaded relational rows."""
        policy = KnowledgeVisibilityPolicy(
            KnowledgeSensitivity(policy_record.sensitivity),
            PolicyEffect(policy_record.default_effect),
            tuple(
                AgentScopeGrant(
                    grant.agent_scope,
                    PolicyEffect(grant.effect),
                    tuple(KnowledgeOperation(item) for item in grant.allowed_operations),
                )
                for grant in grant_records
            ),
            policy_record.session_override_allowed,
            tuple(ModelRegion(item) for item in policy_record.allowed_model_regions),
            policy_record.allow_external_model_processing,
            policy_record.retention_days,
            policy_record.policy_version,
        )
        source = KnowledgeSource(
            ResourceMeta(
                KnowledgeSourceId(record.id),
                record.revision,
                record.created_at,
                record.updated_at,
            ),
            WorkspaceId(record.workspace_id),
            UserId(record.resource_owner_id),
            record.title,
            _source_type(record.source_type),
            record.enabled,
            _load(_PUBLIC_CONFIG_ADAPTER, record.public_config, "Knowledge public config"),
            policy,
            KnowledgeIngestionState(
                KnowledgeIngestionStatus(record.ingestion_state),
                record.document_count,
                record.chunk_count,
                record.last_success_at,
                _load_problem(record.last_problem),
            ),
            (
                None
                if record.current_version_id is None
                else KnowledgeSourceVersionId(record.current_version_id)
            ),
            record.version_counter,
            _load(_SOURCE_INPUT_ADAPTER, record.source_input, "Knowledge source input"),
        )
        _verify_source_reference_columns(record, source.source_input)
        self._source_state[(record.workspace_id, record.id)] = (
            record.resource_owner_id,
            record.current_policy_version,
        )
        return source

    async def _source_owner_and_policy(
        self, workspace_id: WorkspaceId, source_id: KnowledgeSourceId
    ) -> tuple[str, int]:
        """@brief 获取 child-row 所需稳定 source owner 与 policy 水位 / Get the stable source owner and policy watermark needed by child rows."""
        key = (str(workspace_id), str(source_id))
        cached = self._source_state.get(key)
        if cached is not None:
            return cached
        row = (
            await self._session.execute(
                select(
                    KnowledgeSourceRecord.resource_owner_id,
                    KnowledgeSourceRecord.current_policy_version,
                ).where(
                    KnowledgeSourceRecord.workspace_id == str(workspace_id),
                    KnowledgeSourceRecord.id == str(source_id),
                )
            )
        ).one_or_none()
        if row is None:
            raise KnowledgeCasMismatch
        result = (str(row[0]), int(row[1]))
        self._source_state[key] = result
        return result

    async def _add_policy(self, source: KnowledgeSource, owner_id: UserId) -> None:
        """@brief append policy snapshot 与有序 grants / Append a policy snapshot and ordered grants."""
        policy = source.visibility
        policy_id = _row_id("knowledge_policy")
        self._session.add(
            KnowledgeVisibilityPolicyRecord(
                id=policy_id,
                workspace_id=str(source.workspace_id),
                resource_owner_id=str(owner_id),
                source_id=str(source.meta.id),
                policy_version=policy.policy_version,
                default_effect=policy.default_effect.value,
                sensitivity=policy.sensitivity.value,
                session_override_allowed=policy.session_override_allowed,
                allow_external_model_processing=policy.allow_external_model_processing,
                allowed_model_regions=[item.value for item in policy.allowed_model_regions],
                retention_days=policy.retention_days,
                created_at=source.meta.updated_at,
                updated_at=source.meta.updated_at,
                revision=1,
                extensions={},
            )
        )
        # Grants reference the policy row but the persistence models deliberately expose no
        # relationship.  Establish the foreign-key parent before adding children instead of
        # relying on incidental SQLAlchemy INSERT ordering.
        await self._session.flush()
        for ordinal, grant in enumerate(policy.agent_grants):
            self._session.add(
                KnowledgeVisibilityGrantRecord(
                    id=_row_id("knowledge_grant"),
                    workspace_id=str(source.workspace_id),
                    resource_owner_id=str(owner_id),
                    policy_id=policy_id,
                    ordinal=ordinal,
                    agent_scope=grant.agent_scope,
                    effect=grant.effect.value,
                    allowed_operations=[item.value for item in grant.allowed_operations],
                    created_at=source.meta.updated_at,
                    updated_at=source.meta.updated_at,
                    revision=1,
                    extensions={},
                )
            )

    def _add_version_record(self, version: KnowledgeSourceVersion, owner_id: UserId) -> None:
        """@brief 持久化 immutable content snapshot / Persist an immutable content snapshot."""
        artifact = version.snapshot.artifact_ref
        self._session.add(
            KnowledgeSourceVersionRecord(
                id=str(version.meta.id),
                workspace_id=str(version.workspace_id),
                resource_owner_id=str(owner_id),
                source_id=str(version.snapshot.source_id),
                version_no=version.snapshot.version_number,
                content_hash=version.snapshot.content_sha256,
                content_sha256=version.snapshot.content_sha256,
                size_bytes=version.snapshot.size_bytes,
                status=version.status.value,
                artifact_type=artifact.resource_type,
                artifact_id=artifact.id,
                artifact_revision=artifact.revision,
                origin={},
                parser_metadata={},
                indexed_at=version.indexed_at,
                created_at=version.meta.created_at,
                updated_at=version.meta.updated_at,
                revision=version.meta.revision,
                extensions={},
            )
        )


class _PostgresKnowledgeJobSink:
    """@brief 与 Knowledge 领域写同事务的统一 Job sink / Unified Job sink sharing the Knowledge transaction."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingKnowledgeAuthorizer,
    ) -> None:
        self._session = session
        self._authorizer = authorizer

    async def add(self, job: Job, spec: KnowledgeJobSpec) -> None:
        """@brief 写入统一 Job 与 typed worker spec / Write a unified Job and typed worker spec."""
        self._authorizer.require_workspace(job.workspace_id)
        actor_id = self._authorizer.require_actor()
        self._session.add(
            JobRecord(
                id=str(job.meta.id),
                workspace_id=str(job.workspace_id),
                resource_owner_id=str(actor_id),
                job_type=job.kind,
                status=job.status.value,
                phase="queued",
                completed_units=0,
                progress_unit="unknown",
                target_resource_type=job.subject.resource_type,
                target_resource_id=job.subject.id,
                target_resource_revision=job.subject.revision,
                result_refs=[],
                request_payload={
                    "subject": _dump_object(_RESOURCE_REF_ADAPTER, job.subject),
                    "spec": _dump_object(_JOB_SPEC_ADAPTER, spec),
                },
                created_at=job.meta.created_at,
                updated_at=job.meta.updated_at,
                revision=job.meta.revision,
                extensions={},
            )
        )


class _PostgresKnowledgeOutbox:
    """@brief 与 Knowledge 领域写同事务的 outbox / Outbox sharing the Knowledge transaction."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingKnowledgeAuthorizer,
    ) -> None:
        self._session = session
        self._authorizer = authorizer

    async def add(self, event: KnowledgeOutboxEvent) -> None:
        """@brief 添加 secret-free pending event / Add a secret-free pending event."""
        self._authorizer.require_workspace(event.workspace_id)
        actor_id = self._authorizer.require_actor()
        if actor_id != event.actor_id:
            raise PermissionError("outbox actor does not match authenticated actor")
        lifecycle = initial_outbox_lifecycle(
            event.event_type,
            occurred_at=event.occurred_at,
        )
        self._session.add(
            OutboxEventRecord(
                id=str(event.event_id),
                workspace_id=str(event.workspace_id),
                resource_owner_id=str(actor_id),
                aggregate_type=event.subject.resource_type,
                aggregate_id=event.subject.id,
                subject_revision=event.subject.revision,
                event_type=event.event_type,
                sequence=event.subject.revision or 1,
                occurred_at=event.occurred_at,
                payload={
                    "actor_id": str(event.actor_id),
                    "subject": _dump_object(_RESOURCE_REF_ADAPTER, event.subject),
                    "data": dict(event.data),
                },
                replay_expires_at=event.occurred_at + _EVENT_RETENTION,
                status=lifecycle.status,
                published_at=lifecycle.published_at,
                created_at=event.occurred_at,
                updated_at=event.occurred_at,
                revision=1,
                extensions={},
            )
        )


class PostgresKnowledgeUnitOfWork:
    """@brief 一个 PostgreSQL Knowledge 短事务工作单元 / One PostgreSQL Knowledge short-transaction unit of work."""

    def __init__(
        self,
        database: AsyncDatabase,
        launch_cipher: AuthorizationLaunchCipher,
    ) -> None:
        """@brief 绑定数据库与专用 launch cipher / Bind the database and dedicated launch cipher."""
        self._database = database
        self._launch_cipher = launch_cipher
        self._session: AsyncSession | None = None
        self._transaction: AsyncSessionTransaction | None = None
        self._repository: PostgresKnowledgeRepository | None = None
        self._authorizer: _TrackingKnowledgeAuthorizer | None = None
        self._jobs: _PostgresKnowledgeJobSink | None = None
        self._outbox: _PostgresKnowledgeOutbox | None = None
        self._committed = False
        self._rolled_back = False

    @property
    def repository(self) -> KnowledgeRepository:
        """@brief 返回事务 repository / Return the transactional repository."""
        if self._repository is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._repository

    @property
    def authorizer(self) -> _TrackingKnowledgeAuthorizer:
        """@brief 返回集中 authorizer / Return the central authorizer."""
        if self._authorizer is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._authorizer

    @property
    def jobs(self) -> _PostgresKnowledgeJobSink:
        """@brief 返回统一 Job sink / Return the unified Job sink."""
        if self._jobs is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._jobs

    @property
    def outbox(self) -> _PostgresKnowledgeOutbox:
        """@brief 返回 transactional outbox / Return the transactional outbox."""
        if self._outbox is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._outbox

    async def __aenter__(self) -> Self:
        """@brief 创建 Session 并组装同事务 adapters / Create a Session and assemble same-transaction adapters."""
        if self._session is not None:
            raise RuntimeError("Knowledge unit of work cannot be re-entered")
        self._session = self._database.new_session()
        self._transaction = await self._session.begin()
        access_repository = PostgresAccessRepository(self._session)
        self._authorizer = _TrackingKnowledgeAuthorizer(
            AccessAuthorizer(access_repository),
            partial(self._database.install_v2_request_scope, self._session),
        )
        self._repository = PostgresKnowledgeRepository(
            self._session,
            self._authorizer,
            self._launch_cipher,
        )
        self._jobs = _PostgresKnowledgeJobSink(self._session, self._authorizer)
        self._outbox = _PostgresKnowledgeOutbox(self._session, self._authorizer)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 回滚未提交工作并关闭 Session / Roll back uncommitted work and close the Session."""
        del exc, traceback
        if self._session is not None:
            if exc_type is not None or not self._committed:
                await self.rollback()
            await self._session.close()
        self._session = None
        self._transaction = None
        self._repository = None
        self._authorizer = None
        self._jobs = None
        self._outbox = None
        return None

    async def commit(self) -> None:
        """@brief flush 后提交领域、Job 与 outbox / Flush and commit domain state, Job, and outbox."""
        session, transaction = self._require_active()
        if self._committed or self._rolled_back:
            raise RuntimeError("Knowledge unit of work cannot commit in its current state")
        await session.flush()
        await transaction.commit()
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等回滚活动 transaction / Idempotently roll back the active transaction."""
        if self._transaction is not None and self._transaction.is_active:
            await self._transaction.rollback()
        self._rolled_back = True

    def _require_active(self) -> tuple[AsyncSession, AsyncSessionTransaction]:
        """@brief 要求活动 Session/transaction / Require an active Session/transaction."""
        if self._session is None or self._transaction is None:
            raise RuntimeError("Knowledge unit of work has not been entered")
        return self._session, self._transaction


class PostgresKnowledgeUnitOfWorkFactory:
    """@brief 创建 PostgreSQL Knowledge UoW / Create PostgreSQL Knowledge UoWs."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        launch_cipher: AuthorizationLaunchCipher,
    ) -> None:
        """@brief 绑定数据库和显式 key-ring adapter / Bind the database and explicit key-ring adapter."""
        self._database = database
        self._launch_cipher = launch_cipher

    def __call__(self) -> PostgresKnowledgeUnitOfWork:
        """@brief 创建未进入 PostgreSQL UoW / Create a not-yet-entered PostgreSQL UoW."""
        return PostgresKnowledgeUnitOfWork(self._database, self._launch_cipher)


def _connection_from_record(record: ConnectionRecord) -> ConnectionAggregate:
    """@brief 从 token-free row 重建 Connection 聚合 / Rebuild a Connection aggregate from a token-free row."""
    connection = Connection(
        ResourceMeta(
            ConnectionId(record.id),
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        WorkspaceId(record.workspace_id),
        ConnectionProvider(record.provider),
        ConnectionAuthMethod(record.auth_method),
        record.display_name,
        ConnectionStatus(record.status),
        tuple(record.scopes),
        record.last_validated_at,
        _load_problem(record.problem),
    )
    return ConnectionAggregate(
        connection,
        ConnectionOwnership(WorkspaceId(record.workspace_id), UserId(record.created_by)),
        CredentialReference(record.credential_reference),
    )


def _source_type(value: str) -> KnowledgeSourceType:
    """@brief 校验并返回来源判别值 / Validate and return a source discriminator."""
    return KnowledgeSourceType(value)


def _source_record(source: KnowledgeSource, owner_id: UserId) -> KnowledgeSourceRecord:
    """@brief 构造 KnowledgeSource ORM row / Construct a KnowledgeSource ORM row."""
    connection_id: str | None = None
    upload_id: str | None = None
    resume_id: str | None = None
    if isinstance(source.source_input, FileSourceInput):
        upload_id = str(source.source_input.upload_session_id)
    elif isinstance(source.source_input, ResumeSourceInput):
        resume_id = str(source.source_input.resume_id)
    elif isinstance(source.source_input, CloudDriveSourceInput):
        connection_id = str(source.source_input.connection_id)
    elif isinstance(source.source_input, GitSourceInput):
        connection_id = (
            None
            if source.source_input.connection_id is None
            else str(source.source_input.connection_id)
        )
    return KnowledgeSourceRecord(
        id=str(source.meta.id),
        workspace_id=str(source.workspace_id),
        resource_owner_id=str(owner_id),
        source_type=source.source_type.value,
        title=source.name,
        config={},
        source_input=_dump_object(_SOURCE_INPUT_ADAPTER, source.source_input),
        public_config=_dump_object(_PUBLIC_CONFIG_ADAPTER, source.public_config),
        enabled=source.enabled,
        connection_id=connection_id,
        upload_session_id=upload_id,
        resume_id=resume_id,
        current_policy_version=source.visibility.policy_version,
        current_version_id=(
            None if source.current_version_id is None else str(source.current_version_id)
        ),
        version_counter=source.version_counter,
        revision_mode="latest",
        ingestion_state=source.ingestion.status.value,
        document_count=source.ingestion.document_count,
        chunk_count=source.ingestion.chunk_count,
        last_success_at=source.ingestion.last_success_at,
        last_problem=_problem_payload(source.ingestion.last_problem),
        created_at=source.meta.created_at,
        updated_at=source.meta.updated_at,
        revision=source.meta.revision,
        extensions={},
    )


def _verify_source_reference_columns(
    record: KnowledgeSourceRecord, source_input: KnowledgeSourceInput
) -> None:
    """@brief 验证 JSON 判别联合与关系列一致 / Verify the JSON union agrees with relational reference columns."""
    expected_connection: str | None = None
    expected_upload: str | None = None
    expected_resume: str | None = None
    if isinstance(source_input, FileSourceInput):
        expected_upload = str(source_input.upload_session_id)
    elif isinstance(source_input, ResumeSourceInput):
        expected_resume = str(source_input.resume_id)
    elif isinstance(source_input, CloudDriveSourceInput):
        expected_connection = str(source_input.connection_id)
    elif isinstance(source_input, GitSourceInput) and source_input.connection_id is not None:
        expected_connection = str(source_input.connection_id)
    if (
        record.connection_id != expected_connection
        or record.upload_session_id != expected_upload
        or record.resume_id != expected_resume
    ):
        raise ValueError("Knowledge source JSON input diverges from relational references")


def _version_from_record(record: KnowledgeSourceVersionRecord) -> KnowledgeSourceVersion:
    """@brief 重建 immutable version snapshot / Rebuild an immutable version snapshot."""
    return KnowledgeSourceVersion(
        ResourceMeta(
            KnowledgeSourceVersionId(record.id),
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        WorkspaceId(record.workspace_id),
        KnowledgeVersionSnapshot(
            KnowledgeSourceId(record.source_id),
            record.version_no,
            record.content_sha256,
            record.size_bytes,
            ResourceRef(record.artifact_type, record.artifact_id, record.artifact_revision),
        ),
        KnowledgeVersionStatus(record.status),
        record.indexed_at,
    )


def _upload_from_record(record: KnowledgeUploadSessionRecord) -> UploadSession:
    """@brief 从完整非 legacy row 重建 UploadSession / Rebuild an UploadSession from a complete non-legacy row."""
    required_headers = record.required_headers
    if not isinstance(required_headers, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in required_headers.items()
    ):
        raise ValueError("persisted upload required_headers is invalid")
    if any(
        value is None
        for value in (
            record.filename,
            record.media_type,
            record.declared_size_bytes,
            record.declared_sha256,
            record.upload_url,
        )
    ):
        raise ValueError("complete API V2 upload lacks its frozen declaration")
    artifact = (
        None
        if record.artifact_type is None or record.artifact_id is None
        else ResourceRef(record.artifact_type, record.artifact_id, record.artifact_revision)
    )
    consumer = (
        None
        if record.claimed_by_type is None or record.claimed_by_id is None
        else ResourceRef(
            record.claimed_by_type,
            record.claimed_by_id,
            record.claimed_by_revision,
        )
    )
    completion = (
        None
        if record.completion_size_bytes is None or record.completion_sha256 is None
        else UploadCompletionClaim(record.completion_size_bytes, record.completion_sha256)
    )
    return UploadSession(
        UploadSessionView(
            UploadSessionId(record.id),
            WorkspaceId(record.workspace_id),
            UploadStatus(record.status),
            UploadGrant(cast(str, record.upload_url), required_headers),
            record.expires_at,
            artifact,
        ),
        UploadDeclaration(
            cast(str, record.filename),
            cast(str, record.media_type),
            cast(int, record.declared_size_bytes),
            cast(str, record.declared_sha256),
        ),
        record.created_at,
        record.revision,
        completion,
        (
            None
            if record.verification_operation_id is None
            else UploadVerificationId(record.verification_operation_id)
        ),
        record.failure_code,
        consumer,
    )


__all__ = [
    "AesGcmAuthorizationLaunchCipher",
    "AuthorizationLaunchCipher",
    "EncryptedAuthorizationLaunch",
    "InMemoryKnowledgeRepository",
    "InMemoryKnowledgeStore",
    "InMemoryKnowledgeUnitOfWork",
    "InMemoryKnowledgeUnitOfWorkFactory",
    "PostgresKnowledgeRepository",
    "PostgresKnowledgeUnitOfWork",
    "PostgresKnowledgeUnitOfWorkFactory",
]
