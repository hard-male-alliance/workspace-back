"""@brief PostgreSQL 运行时 Repository 适配器 / PostgreSQL runtime repository adapters.

本模块保留尚未拆分的旧 Workspace/Resume/Knowledge/Job 端口。Agent、Interview
与 Artifact 已拥有 API V2 专用 Unit of Work（UoW），其不变量与旧端口不等价；
对应旧 PostgreSQL 方法因此在任何数据库访问前显式失败，绝不把旧 payload 投影到
canonical V2 真相表。

仍启用的公开方法自行打开 ``AsyncDatabase`` 短事务（short transaction），并在
进入后安装 ``ActorScope``。Repository 不会跨 ``asyncio.Task`` 共享 Session，且
应用层 tenant 谓词与 PostgreSQL RLS（Row-Level Security）同时生效。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Never, cast

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.domain.agent import (
    AgentRunRecord as DomainAgentRunRecord,
)
from backend.domain.agent import (
    ConversationRecord as DomainConversationRecord,
)
from backend.domain.agent import (
    MessageRecord as DomainMessageRecord,
)
from backend.domain.common import DomainError, Job, JobStatus, Problem
from backend.domain.interview import InterviewSessionRecord as DomainInterviewSessionRecord
from backend.domain.knowledge import (
    EmbeddingSpace,
    KnowledgeChunk,
    KnowledgeClassification,
    KnowledgeContentType,
    KnowledgeDocumentPart,
)
from backend.domain.knowledge import KnowledgeSourceRecord as DomainKnowledgeSourceRecord
from backend.domain.proposal import (
    ResumeProposalRecord as DomainResumeProposalRecord,
)
from backend.domain.resume import ResumeRecord
from backend.infrastructure.idempotency import IdempotentResponse
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    EmbeddingSpaceRecord as EmbeddingSpaceOrmRecord,
)
from backend.infrastructure.persistence.models import (
    IdempotencyRecord as IdempotencyOrmRecord,
)
from backend.infrastructure.persistence.models import (
    JobRecord as JobOrmRecord,
)
from backend.infrastructure.persistence.models import (
    KnowledgeChunkRecord as KnowledgeChunkOrmRecord,
)
from backend.infrastructure.persistence.models import (
    KnowledgeEmbeddingRecord as KnowledgeEmbeddingOrmRecord,
)
from backend.infrastructure.persistence.models import (
    KnowledgeSourceRecord as KnowledgeSourceOrmRecord,
)
from backend.infrastructure.persistence.models import (
    KnowledgeSourceVersionRecord as KnowledgeSourceVersionOrmRecord,
)
from backend.infrastructure.persistence.models import (
    KnowledgeVisibilityGrantRecord as KnowledgeVisibilityGrantOrmRecord,
)
from backend.infrastructure.persistence.models import (
    KnowledgeVisibilityPolicyRecord as KnowledgeVisibilityPolicyOrmRecord,
)
from backend.infrastructure.persistence.models import (
    ResumeDocumentRecord as ResumeDocumentOrmRecord,
)
from backend.infrastructure.persistence.models import (
    ResumeRenderJobRecord as ResumeRenderJobOrmRecord,
)
from backend.infrastructure.persistence.models import (
    ResumeRevisionRecord as ResumeRevisionOrmRecord,
)
from backend.infrastructure.persistence.models import (
    ResumeTemplateRecord as ResumeTemplateOrmRecord,
)
from backend.infrastructure.persistence.models import (
    UserRecord,
    WorkspaceMemberRecord,
    WorkspaceRecord,
)
from backend.infrastructure.persistence.repositories import scoped_select
from workspace_shared.ids import new_opaque_id
from workspace_shared.tenancy import ActorScope

_RUNTIME_EXTENSION_KEY = "runtime"
"""@brief JSONB 扩展中的运行时命名空间 / Runtime namespace inside JSONB extensions."""

_PENDING_CLAIM_TOKEN_KEY = "pending_claim_token"
"""@brief pending 幂等 claim 的私有令牌字段 / Private token field for a pending idempotency claim."""

_RETIRED_V1_POSTGRES_MESSAGE = (
    "legacy PostgreSQL persistence surface is retired; use the API V2 unit-of-work"
)
"""@brief 已退休 V1 PostgreSQL 端口的稳定错误 / Stable error for retired V1 PostgreSQL ports."""


def _reject_retired_v1_postgres_surface(surface: str) -> Never:
    """@brief 在触碰 V2 真相表前拒绝旧持久化语义 / Reject legacy persistence semantics before touching V2 truth tables.

    @param surface 被拒绝的旧端口名称 / Name of the retired legacy port.
    @raise RuntimeError 该端口不能安全投影到 canonical V2 模型 / The port cannot be safely
        projected onto the canonical V2 model.
    @note V1 的可变 Message、事件型 Interview 和 Resume 专用 Artifact 表达与 V2
        不变量不等价；这里显式失败，避免伪兼容导致跨产品数据损坏。
        / V1 mutable Messages, event-shaped Interviews, and Resume-specific Artifacts are not
        equivalent to V2 invariants; failing explicitly prevents pseudo-compatibility from
        corrupting product data.
    """
    raise RuntimeError(f"{_RETIRED_V1_POSTGRES_MESSAGE}: {surface}")


def _same_scope(left: ActorScope, right: ActorScope) -> bool:
    """@brief 比较两个资源租户边界 / Compare two resource tenant scopes.

    @param left 请求范围 / Requested scope.
    @param right 聚合自身范围 / Aggregate scope.
    @return workspace 与 resource owner 同时相等时为真。
    """
    return (
        left.workspace_id == right.workspace_id
        and left.resource_owner_id == right.resource_owner_id
    )


def _assert_record_scope(scope: ActorScope, record_scope: ActorScope) -> None:
    """@brief 拒绝跨租户写入 / Reject a cross-tenant write.

    @param scope 调用方范围 / Caller scope.
    @param record_scope 聚合声明的范围 / Aggregate-declared scope.
    @raise PermissionError 两个范围不一致时抛出。
    """
    if not _same_scope(scope, record_scope):
        raise PermissionError("resource is outside the supplied scope")


def _runtime_payload(extensions: object) -> dict[str, Any]:
    """@brief 安全读取运行时 JSONB 命名空间 / Read the runtime JSONB namespace safely.

    @param extensions ORM 的 extensions 列 / ORM extensions column.
    @return 可变副本；错误形状退化为空对象。
    """
    if not isinstance(extensions, dict):
        return {}
    candidate = extensions.get(_RUNTIME_EXTENSION_KEY)
    return deepcopy(candidate) if isinstance(candidate, dict) else {}


def _with_runtime(extensions: object, runtime: dict[str, Any]) -> dict[str, Any]:
    """@brief 在不丢弃未来扩展的前提下更新 runtime / Update runtime without discarding future extensions.

    @param extensions 已存 JSONB extensions / Existing JSONB extensions.
    @param runtime 本模块拥有的运行时负载 / Runtime payload owned by this module.
    @return 可持久化的 extensions 对象。
    """
    result = deepcopy(extensions) if isinstance(extensions, dict) else {}
    result[_RUNTIME_EXTENSION_KEY] = deepcopy(runtime)
    return cast(dict[str, Any], result)


def _json_hash(value: object) -> str:
    """@brief 计算稳定 JSON 摘要 / Compute a stable JSON digest.

    @param value 可 JSON 序列化的值 / JSON-serializable value.
    @return SHA-256 十六进制摘要。
    """
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _source_content_hash(record: DomainKnowledgeSourceRecord) -> str:
    """Prefer a validated file digest; otherwise hash the runtime text payload."""
    candidate = record.config.get("sha256")
    if (
        record.source_type == "file"
        and isinstance(candidate, str)
        and len(candidate) == 64
        and all(character in "0123456789abcdef" for character in candidate.lower())
    ):
        return candidate.lower()
    return _json_hash(record.mock_content)


def _bytes_hash(value: bytes) -> str:
    """@brief 计算二进制内容 SHA-256 / Compute a SHA-256 for binary content.

    @param value 二进制内容 / Binary content.
    @return 小写十六进制 SHA-256 摘要。
    """
    return hashlib.sha256(value).hexdigest()


def _stable_id(prefix: str, *parts: str) -> str:
    """@brief 构造确定性内部 ID / Build a deterministic internal ID.

    @param prefix 资源类型前缀 / Resource type prefix.
    @param parts 身份组成部分 / Identity components.
    @return 最大长度安全的内部 ID。
    """
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:40]
    return f"{prefix}_{digest}"


def _as_datetime(value: object, fallback: datetime) -> datetime:
    """@brief 解析 RFC 3339 时间或使用回退值 / Parse an RFC 3339 time or use a fallback.

    @param value datetime 或 RFC 3339 字符串 / Datetime or RFC 3339 string.
    @param fallback 解析失败时使用的 UTC 时间 / UTC time to use on parsing failure.
    @return 带 UTC 时区的 datetime。
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return fallback
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    return fallback


def _as_json_object(value: object) -> dict[str, Any]:
    """@brief 将未知 JSON 值收窄为对象 / Narrow an unknown JSON value to an object.

    @param value 待检查值 / Candidate value.
    @return 深复制的对象；非对象返回空对象。
    """
    return deepcopy(value) if isinstance(value, dict) else {}


def _as_string_list(value: object) -> list[str]:
    """@brief 读取字符串数组 / Read a list of strings.

    @param value 待检查值 / Candidate value.
    @return 保留字符串元素的列表。
    """
    return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []


def _problem_to_json(problem: Problem | None) -> dict[str, Any] | None:
    """@brief 序列化领域 Problem / Serialize a domain Problem.

    @param problem 可选领域问题 / Optional domain problem.
    @return 可持久化 JSON 或 None。
    """
    return problem.as_dict() if problem is not None else None


def _problem_from_json(value: object) -> Problem | None:
    """@brief 还原领域 Problem / Rebuild a domain Problem.

    @param value 已存错误 JSON / Stored error JSON.
    @return Problem；无效形状返回 None。
    """
    if not isinstance(value, dict):
        return None
    code = value.get("code")
    title = value.get("title")
    status = value.get("status")
    if (
        not isinstance(code, str)
        or not isinstance(title, str)
        or isinstance(status, bool)
        or not isinstance(status, int)
    ):
        return None
    violations = value.get("violations")
    return Problem(
        code=code,
        status=status,
        title=title,
        detail=value.get("detail") if isinstance(value.get("detail"), str) else None,
        retryable=bool(value.get("retryable", False)),
        violations=[_as_json_object(item) for item in violations]
        if isinstance(violations, list)
        else [],
        extensions=_as_json_object(value.get("extensions")),
    )


async def _ensure_scope_identities(session: AsyncSession, scope: ActorScope) -> None:
    """@brief 按需创建受 RLS 保护的运行时 identity 根 / Create RLS-protected runtime identity roots on demand.

    @param session 已安装 scope 的写事务 / Scoped write transaction.
    @param scope 当前调用方范围 / Current caller scope.
    @return 无返回值。

    @note 这不是认证系统的替代品。生产认证适配器可预先创建更完整的 identity 资料；
    此最小幂等种子仅保证 API 默认 scope 首次写入时不会违反外键。
    """
    for principal_id in {scope.actor_id, scope.resource_owner_id}:
        await session.execute(
            insert(UserRecord)
            .values(
                id=principal_id,
                external_subject=f"runtime:{principal_id}",
                display_name=None,
                email=None,
                locale="zh-CN",
            )
            .on_conflict_do_nothing(index_elements=[UserRecord.id])
        )
    await session.execute(
        insert(WorkspaceRecord)
        .values(
            id=scope.workspace_id,
            resource_owner_id=scope.resource_owner_id,
            name="Runtime workspace",
            default_locale="zh-CN",
        )
        .on_conflict_do_nothing(index_elements=[WorkspaceRecord.id])
    )
    if scope.actor_id == scope.resource_owner_id:
        member_digest = hashlib.sha256(
            f"{scope.workspace_id}:{scope.actor_id}".encode()
        ).hexdigest()[:32]
        await session.execute(
            insert(WorkspaceMemberRecord)
            .values(
                id=f"wsm_{member_digest}",
                workspace_id=scope.workspace_id,
                resource_owner_id=scope.resource_owner_id,
                user_id=scope.actor_id,
                role="owner",
                status="active",
                joined_at=func.now(),
            )
            .on_conflict_do_nothing(
                index_elements=[
                    WorkspaceMemberRecord.workspace_id,
                    WorkspaceMemberRecord.user_id,
                ]
            )
        )


async def _scoped_one(
    session: AsyncSession,
    model_type: type[Any],
    scope: ActorScope,
    record_id: str,
    *,
    lock: bool = False,
) -> Any | None:
    """@brief 按 ID 读取一个 tenant 资源 / Read one tenant resource by ID.

    @param session 已安装 scope 的事务 / Transaction with installed scope.
    @param model_type ORM 模型类型 / ORM model type.
    @param scope 必填 workspace/owner 范围 / Required workspace/owner scope.
    @param record_id 资源 ID / Resource ID.
    @param lock 是否请求 ``FOR UPDATE`` / Whether to request ``FOR UPDATE``.
    @return 同 scope 记录或 None。
    """
    model = cast(Any, model_type)
    statement = scoped_select(model_type, scope).where(model.id == record_id)
    if lock:
        statement = statement.with_for_update()
    return (await session.scalars(statement)).first()


def _resume_runtime(record: ResumeRecord) -> dict[str, Any]:
    """@brief 编码简历的幂等与冲突状态 / Encode resume idempotency and conflict state.

    @param record 简历聚合 / Resume aggregate.
    @return 仅属于 runtime 命名空间的 JSON 对象。
    """
    changed_targets = {
        str(revision): [list(target) for target in sorted(targets)]
        for revision, targets in record.changed_targets.items()
    }
    return {
        "operation_ids": sorted(record.operation_ids),
        "batch_hashes": deepcopy(record.batch_hashes),
        "batch_results": deepcopy(record.batch_results),
        "changed_targets": changed_targets,
    }


def _resume_state(
    runtime: dict[str, Any],
) -> tuple[set[str], dict[str, str], dict[str, dict[str, Any]], dict[int, set[tuple[str, ...]]]]:
    """@brief 解码简历的幂等与冲突状态 / Decode resume idempotency and conflict state.

    @param runtime 已存 runtime JSON / Stored runtime JSON.
    @return operation IDs、batch hashes、batch results 与 changed targets。
    """
    operation_ids = set(_as_string_list(runtime.get("operation_ids")))
    raw_hashes = _as_json_object(runtime.get("batch_hashes"))
    batch_hashes = {str(key): value for key, value in raw_hashes.items() if isinstance(value, str)}
    raw_results = _as_json_object(runtime.get("batch_results"))
    batch_results = {str(key): _as_json_object(value) for key, value in raw_results.items()}
    changed_targets: dict[int, set[tuple[str, ...]]] = {}
    raw_targets = _as_json_object(runtime.get("changed_targets"))
    for raw_revision, raw_paths in raw_targets.items():
        try:
            revision = int(raw_revision)
        except TypeError, ValueError:
            continue
        if not isinstance(raw_paths, list):
            continue
        changed_targets[revision] = {
            tuple(str(part) for part in path)
            for path in raw_paths
            if isinstance(path, list) and path
        }
    return operation_ids, batch_hashes, batch_results, changed_targets


def _timestamp_text(value: datetime) -> str:
    """Serialize one database timestamp as canonical UTC text."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _workspace_payload(row: WorkspaceRecord) -> dict[str, Any]:
    """Map the normalized workspace row to the shared resource shape."""
    digest = hashlib.sha256(str(row.id).encode("utf-8")).hexdigest()[:12]
    return {
        "id": str(row.id),
        "created_at": _timestamp_text(row.created_at),
        "updated_at": _timestamp_text(row.updated_at),
        "revision": int(row.revision),
        "name": str(row.name),
        "slug": f"workspace-{digest}",
        "default_locale": str(row.default_locale),
        "timezone": "Asia/Shanghai",
        "plan": "free",
        "extensions": {},
    }


def _workspace_member_payload(row: WorkspaceMemberRecord) -> dict[str, Any]:
    """Map one membership row without exposing internal invitation metadata."""
    status = "suspended" if str(row.status) == "disabled" else str(row.status)
    return {
        "id": str(row.id),
        "created_at": _timestamp_text(row.created_at),
        "updated_at": _timestamp_text(row.updated_at),
        "revision": int(row.revision),
        "workspace_id": str(row.workspace_id),
        "user_id": str(row.user_id),
        "role": str(row.role),
        "status": status,
        "extensions": {},
    }


class PostgresWorkspaceRepository:
    """@brief 尚未拆分端口共享的 PostgreSQL Repository / PostgreSQL repository shared by ports not yet split.

    该对象为旧 composition 保留结构化端口形状，但 Agent、Interview 与 Artifact 的
    PostgreSQL 方法统一 fail closed；生产 V2 仅使用各自的专用 UoW。对象本身无可变
    请求状态，仍启用的方法每次调用新建 session/transaction，可被并发请求安全复用。

    @param database 已配置的异步 PostgreSQL 资源所有者 / Configured async PostgreSQL owner.
    """

    def __init__(self, database: AsyncDatabase) -> None:
        """@brief 绑定数据库资源所有者 / Bind the database resource owner.

        @param database 生命周期由 composition root 管理的数据库 / Database owned by the composition root.
        """
        self._database = database

    async def get_current_user(self, scope: ActorScope) -> dict[str, Any] | None:
        """Read the current actor only when the asserted workspace is authorized."""
        if not await self.list_workspaces(scope):
            return None
        async with self._database.read_session(scope) as session:
            user = await session.get(UserRecord, scope.actor_id)
            if user is None or user.deleted_at is not None:
                return None
            return {
                "id": str(user.id),
                "display_name": str(user.display_name or "Workspace User"),
                "email": user.email,
                "locale": str(user.locale),
                "timezone": "Asia/Shanghai",
                "default_workspace_id": scope.workspace_id,
                "created_at": _timestamp_text(user.created_at),
            }

    async def list_workspaces(self, scope: ActorScope) -> list[dict[str, Any]]:
        """List only the workspace carried by the verified identity assertion."""
        async with self._database.read_session(scope) as session:
            workspace = await session.get(WorkspaceRecord, scope.workspace_id)
            if workspace is None or workspace.deleted_at is not None:
                return []
            member = (
                await session.scalars(
                    scoped_select(WorkspaceMemberRecord, scope).where(
                        WorkspaceMemberRecord.workspace_id == scope.workspace_id,
                        WorkspaceMemberRecord.user_id == scope.actor_id,
                        WorkspaceMemberRecord.status == "active",
                    )
                )
            ).first()
            if scope.actor_id != workspace.resource_owner_id and member is None:
                return []
            return [_workspace_payload(workspace)]

    async def get_workspace(
        self, scope: ActorScope, workspace_id: str
    ) -> dict[str, Any] | None:
        """Read the asserted workspace after membership authorization."""
        if workspace_id != scope.workspace_id:
            return None
        workspaces = await self.list_workspaces(scope)
        return workspaces[0] if workspaces else None

    async def list_workspace_members(
        self, scope: ActorScope, workspace_id: str
    ) -> list[dict[str, Any]]:
        """List active members of the asserted workspace."""
        if await self.get_workspace(scope, workspace_id) is None:
            return []
        async with self._database.read_session(scope) as session:
            statement = scoped_select(WorkspaceMemberRecord, scope).where(
                WorkspaceMemberRecord.workspace_id == workspace_id
            ).order_by(WorkspaceMemberRecord.created_at.asc(), WorkspaceMemberRecord.id.asc())
            rows = (await session.scalars(statement)).all()
            return [_workspace_member_payload(row) for row in rows]

    async def create_resume(self, scope: ActorScope, record: ResumeRecord) -> None:
        """@brief 创建简历及不可变初始 revision / Create a resume and immutable initial revisions.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 简历聚合 / Resume aggregate.
        """
        _assert_record_scope(scope, record.scope)
        async with self._database.transaction(scope) as session:
            await _ensure_scope_identities(session, scope)
            existing = await _scoped_one(
                session, ResumeDocumentOrmRecord, scope, record.id, lock=True
            )
            if existing is None:
                await self._insert_resume(session, scope, record)
            else:
                await self._save_resume_locked(session, scope, record, existing)

    async def get_resume(self, scope: ActorScope, resume_id: str) -> ResumeRecord | None:
        """@brief 读取完整简历聚合 / Read a complete resume aggregate.

        @param scope 请求租户范围 / Request tenant scope.
        @param resume_id 简历 ID / Resume ID.
        @return 简历聚合或 None。
        """
        async with self._database.read_session(scope) as session:
            row = await _scoped_one(session, ResumeDocumentOrmRecord, scope, resume_id)
            return await self._resume_from_row(session, scope, row) if row is not None else None

    async def list_resumes(self, scope: ActorScope) -> list[ResumeRecord]:
        """@brief 列出当前 tenant 的简历 / List resumes in the current tenant.

        @param scope 请求租户范围 / Request tenant scope.
        @return 以最近更新时间倒序的简历聚合。
        """
        async with self._database.read_session(scope) as session:
            statement = scoped_select(ResumeDocumentOrmRecord, scope).order_by(
                ResumeDocumentOrmRecord.updated_at.desc()
            )
            rows = (await session.scalars(statement)).all()
            return [await self._resume_from_row(session, scope, row) for row in rows]

    async def save_resume(self, scope: ActorScope, record: ResumeRecord) -> None:
        """@brief 使用乐观内容检查保存简历 / Save a resume with optimistic content validation.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 已修改的简历聚合 / Modified resume aggregate.
        @raise RuntimeError 检测到跨 worker 的写入冲突时抛出。
        """
        _assert_record_scope(scope, record.scope)
        async with self._database.transaction(scope) as session:
            row = await _scoped_one(session, ResumeDocumentOrmRecord, scope, record.id, lock=True)
            if row is None:
                raise RuntimeError("cannot save a resume that does not exist in this scope")
            await self._save_resume_locked(session, scope, record, row)

    async def save_resume_and_job(
        self,
        scope: ActorScope,
        record: ResumeRecord,
        job: Job,
    ) -> None:
        """Atomically persist a Resume revision/idempotency result and queued render Job."""
        _assert_record_scope(scope, record.scope)
        async with self._database.transaction(scope) as session:
            row = await _scoped_one(session, ResumeDocumentOrmRecord, scope, record.id, lock=True)
            if row is None:
                raise RuntimeError("cannot save a resume that does not exist in this scope")
            await self._save_resume_locked(session, scope, record, row)
            await self._save_job_locked(session, scope, job, allow_create=True)

    async def commit_resume_workflow(
        self,
        scope: ActorScope,
        record: ResumeRecord,
        knowledge_source: DomainKnowledgeSourceRecord,
        knowledge_job: Job,
        render_job: Job | None,
        *,
        create_resume: bool,
    ) -> None:
        """Atomically accept a Resume revision and its knowledge/render work intents."""
        _assert_record_scope(scope, record.scope)
        _assert_record_scope(scope, knowledge_source.scope)
        async with self._database.transaction(scope) as session:
            await _ensure_scope_identities(session, scope)
            resume_row = await _scoped_one(
                session, ResumeDocumentOrmRecord, scope, record.id, lock=True
            )
            if resume_row is None:
                if not create_resume:
                    raise RuntimeError("cannot save a resume that does not exist in this scope")
                await self._insert_resume(session, scope, record)
            else:
                await self._save_resume_locked(session, scope, record, resume_row)

            source_row = await _scoped_one(
                session, KnowledgeSourceOrmRecord, scope, knowledge_source.id, lock=True
            )
            if source_row is None:
                source_row = KnowledgeSourceOrmRecord(
                    id=knowledge_source.id,
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    source_type=self._storage_source_type(knowledge_source.source_type),
                    title=knowledge_source.name,
                    config=deepcopy(knowledge_source.config),
                    revision_mode="latest",
                    ingestion_state="new",
                    created_at=knowledge_source.created_at,
                    updated_at=knowledge_source.updated_at,
                    revision=knowledge_source.revision,
                    extensions={},
                )
                session.add(source_row)
                await session.flush()
            await self._write_source_locked(session, scope, source_row, knowledge_source)
            await self._save_job_locked(session, scope, knowledge_job, allow_create=True)
            if render_job is not None:
                await self._save_job_locked(session, scope, render_job, allow_create=True)

    async def _insert_resume(
        self, session: AsyncSession, scope: ActorScope, record: ResumeRecord
    ) -> None:
        """@brief 在已有写事务中插入简历 / Insert a resume inside an existing write transaction.

        @param session 已安装 scope 的写事务 / Scoped write transaction.
        @param scope 请求租户范围 / Request tenant scope.
        @param record 简历聚合 / Resume aggregate.
        """
        document = deepcopy(record.document)
        template_version_id = await self._ensure_resume_template(session, scope, document)
        created_at = _as_datetime(document.get("created_at"), datetime.now(UTC))
        updated_at = _as_datetime(document.get("updated_at"), created_at)
        row = ResumeDocumentOrmRecord(
            id=record.id,
            workspace_id=scope.workspace_id,
            resource_owner_id=scope.resource_owner_id,
            template_version_id=template_version_id,
            title=str(document.get("title", "Untitled resume")),
            locale=str(document.get("locale", "zh-CN")),
            current_revision_no=record.revision,
            created_at=created_at,
            updated_at=updated_at,
            revision=record.revision,
            extensions=_with_runtime({}, _resume_runtime(record)),
        )
        session.add(row)
        await session.flush()
        for revision_no, snapshot in record.revisions.items():
            session.add(
                ResumeRevisionOrmRecord(
                    id=_stable_id("rrev", record.id, str(revision_no)),
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    resume_id=record.id,
                    revision_no=revision_no,
                    semantic_document=deepcopy(snapshot),
                    content_hash=_json_hash(snapshot),
                    created_by_actor_id=scope.actor_id,
                    source="runtime",
                    created_at=_as_datetime(snapshot.get("updated_at"), updated_at),
                    updated_at=_as_datetime(snapshot.get("updated_at"), updated_at),
                    revision=revision_no,
                    extensions={},
                )
            )

    async def _save_resume_locked(
        self,
        session: AsyncSession,
        scope: ActorScope,
        record: ResumeRecord,
        row: Any,
    ) -> None:
        """@brief 在锁定的简历行上执行比较并保存 / Compare and save against a locked resume row.

        @param session 已安装 scope 的写事务 / Scoped write transaction.
        @param scope 请求租户范围 / Request tenant scope.
        @param record 内存中的候选聚合 / In-memory candidate aggregate.
        @param row 已 ``FOR UPDATE`` 锁定的文档 ORM 行 / Locked document ORM row.
        @raise RuntimeError 检测到非幂等并发覆盖时抛出。
        """
        revision_rows = (
            await session.scalars(
                scoped_select(ResumeRevisionOrmRecord, scope)
                .where(ResumeRevisionOrmRecord.resume_id == record.id)
                .order_by(ResumeRevisionOrmRecord.revision_no.asc())
            )
        ).all()
        existing_by_number = {int(item.revision_no): item for item in revision_rows}
        current_number = int(row.current_revision_no)
        incoming_number = record.revision
        current_row = existing_by_number.get(current_number)
        if current_row is None:
            raise RuntimeError("resume persistence is missing its current immutable revision")
        incoming_snapshot = record.snapshot(incoming_number)
        if incoming_number < current_number or incoming_number > current_number + 1:
            raise RuntimeError("resume revision is not a single-step optimistic update")
        if incoming_number == current_number:
            if current_row.content_hash != _json_hash(incoming_snapshot):
                raise RuntimeError("concurrent resume update conflicts with the persisted revision")
        else:
            prior_snapshot = record.revisions.get(current_number)
            if prior_snapshot is None or current_row.content_hash != _json_hash(prior_snapshot):
                raise RuntimeError(
                    "concurrent resume update conflicts with the persisted base revision"
                )
        for revision_no, snapshot in record.revisions.items():
            persisted = existing_by_number.get(revision_no)
            snapshot_hash = _json_hash(snapshot)
            if persisted is not None:
                if persisted.content_hash != snapshot_hash:
                    raise RuntimeError("immutable resume revision content does not match")
                continue
            session.add(
                ResumeRevisionOrmRecord(
                    id=_stable_id("rrev", record.id, str(revision_no)),
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    resume_id=record.id,
                    revision_no=revision_no,
                    semantic_document=deepcopy(snapshot),
                    content_hash=snapshot_hash,
                    created_by_actor_id=scope.actor_id,
                    source="runtime",
                    created_at=_as_datetime(snapshot.get("updated_at"), row.updated_at),
                    updated_at=_as_datetime(snapshot.get("updated_at"), row.updated_at),
                    revision=revision_no,
                    extensions={},
                )
            )
        document = deepcopy(record.document)
        row.template_version_id = await self._ensure_resume_template(session, scope, document)
        row.title = str(document.get("title", row.title))
        row.locale = str(document.get("locale", row.locale))
        row.current_revision_no = incoming_number
        row.revision = incoming_number
        row.updated_at = _as_datetime(document.get("updated_at"), datetime.now(UTC))
        row.extensions = _with_runtime(row.extensions, _resume_runtime(record))

    async def _ensure_resume_template(
        self,
        session: AsyncSession,
        scope: ActorScope,
        document: dict[str, Any],
    ) -> str:
        """@brief 确保文档引用的模板版本存在 / Ensure the document's template version exists.

        @param session 已安装 scope 的写事务 / Scoped write transaction.
        @param scope 请求租户范围 / Request tenant scope.
        @param document ResumeDocument SIR / ResumeDocument SIR.
        @return 可供外键引用的 template-version ID。
        """
        template = _as_json_object(document.get("template"))
        template_id = str(template.get("template_id", "tpl_runtime_default"))
        template_version = str(template.get("template_version", "1.0"))
        statement = scoped_select(ResumeTemplateOrmRecord, scope).where(
            ResumeTemplateOrmRecord.template_id == template_id,
            ResumeTemplateOrmRecord.template_version == template_version,
        )
        existing = (await session.scalars(statement)).first()
        if existing is not None:
            return str(existing.id)
        internal_id = _stable_id(
            "tplver", scope.workspace_id, scope.resource_owner_id, template_id, template_version
        )
        session.add(
            ResumeTemplateOrmRecord(
                id=internal_id,
                workspace_id=scope.workspace_id,
                resource_owner_id=scope.resource_owner_id,
                template_id=template_id,
                template_version=template_version,
                manifest={"template": deepcopy(template)},
                renderer_binding={"adapter": "runtime"},
                extensions={},
            )
        )
        await session.flush()
        return internal_id

    async def _resume_from_row(
        self,
        session: AsyncSession,
        scope: ActorScope,
        row: Any,
    ) -> ResumeRecord:
        """@brief 将规范化行重新组装为简历聚合 / Rehydrate a resume aggregate from normalized rows.

        @param session 已安装 scope 的读事务 / Scoped read transaction.
        @param scope 请求租户范围 / Request tenant scope.
        @param row 简历文档 ORM 行 / Resume document ORM row.
        @return 可供领域服务修改的 ResumeRecord。
        """
        revision_rows = (
            await session.scalars(
                scoped_select(ResumeRevisionOrmRecord, scope)
                .where(ResumeRevisionOrmRecord.resume_id == row.id)
                .order_by(ResumeRevisionOrmRecord.revision_no.asc())
            )
        ).all()
        revisions = {
            int(item.revision_no): deepcopy(item.semantic_document) for item in revision_rows
        }
        current_revision = int(row.current_revision_no)
        document = revisions.get(current_revision)
        if document is None:
            raise RuntimeError("resume persistence is missing its current immutable revision")
        operation_ids, batch_hashes, batch_results, changed_targets = _resume_state(
            _runtime_payload(row.extensions)
        )
        return ResumeRecord(
            scope=scope,
            document=deepcopy(document),
            revisions=revisions,
            operation_ids=operation_ids,
            batch_hashes=batch_hashes,
            batch_results=batch_results,
            changed_targets=changed_targets,
        )

    async def create_proposal(
        self, scope: ActorScope, record: DomainResumeProposalRecord
    ) -> None:
        """@brief 拒绝旧版 Resume Proposal 写入 / Reject legacy Resume-Proposal writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 旧版 Proposal 聚合 / Legacy Proposal aggregate.
        @raise RuntimeError PostgreSQL Proposal 只能经 V2 Agent/Resume UoW 写入。
        """
        del scope, record
        _reject_retired_v1_postgres_surface("agent")

    async def get_proposal(
        self, scope: ActorScope, proposal_id: str
    ) -> DomainResumeProposalRecord | None:
        """@brief 拒绝旧版 Resume Proposal 投影 / Reject legacy Resume-Proposal projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param proposal_id 旧版 Proposal ID / Legacy Proposal ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Proposal 只能经 V2 Agent/Resume UoW 读取。
        """
        del scope, proposal_id
        _reject_retired_v1_postgres_surface("agent")

    async def list_proposals(
        self, scope: ActorScope, resume_id: str
    ) -> list[DomainResumeProposalRecord]:
        """@brief 拒绝旧版 Resume Proposal 列表投影 / Reject legacy Resume-Proposal list projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param resume_id 旧版 Resume ID / Legacy Resume ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Proposal 只能经 V2 Agent/Resume UoW 列出。
        """
        del scope, resume_id
        _reject_retired_v1_postgres_surface("agent")

    async def save_proposal(
        self, scope: ActorScope, record: DomainResumeProposalRecord
    ) -> None:
        """@brief 拒绝旧版 Resume Proposal 决策写入 / Reject legacy Resume-Proposal decision writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 旧版 Proposal 聚合 / Legacy Proposal aggregate.
        @raise RuntimeError PostgreSQL Proposal 只能经 V2 Agent/Resume UoW 写入。
        """
        del scope, record
        _reject_retired_v1_postgres_surface("agent")

    async def create_conversation(
        self,
        scope: ActorScope,
        record: DomainConversationRecord,
    ) -> None:
        """@brief 拒绝旧版 Agent Conversation 写入 / Reject legacy Agent-Conversation writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param record Conversation 聚合 / Conversation aggregate.
        @raise RuntimeError PostgreSQL Agent 只能经 V2 UoW 写入。
        """
        del scope, record
        _reject_retired_v1_postgres_surface("agent")

    async def get_conversation(
        self,
        scope: ActorScope,
        conversation_id: str,
    ) -> DomainConversationRecord | None:
        """@brief 拒绝旧版 Agent Conversation 投影 / Reject legacy Agent-Conversation projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param conversation_id 会话 ID / Conversation ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Agent 只能经 V2 UoW 读取。
        """
        del scope, conversation_id
        _reject_retired_v1_postgres_surface("agent")

    async def create_message(self, scope: ActorScope, record: DomainMessageRecord) -> None:
        """@brief 拒绝旧版可变消息写入 / Reject legacy mutable-message writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 消息实体 / Message entity.
        @raise RuntimeError PostgreSQL 仅允许 V2 append-only Message UoW 写入。
        """
        del scope, record
        _reject_retired_v1_postgres_surface("agent.message")

    async def get_message(self, scope: ActorScope, message_id: str) -> DomainMessageRecord | None:
        """@brief 拒绝旧版消息读取投影 / Reject legacy Message read projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param message_id 消息 ID / Message ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Message 只能经 V2 UoW 读取。
        """
        del scope, message_id
        _reject_retired_v1_postgres_surface("agent.message")

    async def list_messages(
        self,
        scope: ActorScope,
        conversation_id: str,
    ) -> list[DomainMessageRecord]:
        """@brief 拒绝旧版消息列表投影 / Reject legacy Message list projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param conversation_id 会话 ID / Conversation ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Message 只能经 V2 UoW 列出。
        """
        del scope, conversation_id
        _reject_retired_v1_postgres_surface("agent.message")

    async def create_run(self, scope: ActorScope, record: DomainAgentRunRecord) -> None:
        """@brief 拒绝旧版 Agent Run 写入 / Reject legacy Agent-Run writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param record Agent Run 记录 / Agent Run record.
        @raise RuntimeError PostgreSQL Agent 只能经 V2 UoW 写入。
        """
        del scope, record
        _reject_retired_v1_postgres_surface("agent")

    async def get_run(self, scope: ActorScope, run_id: str) -> DomainAgentRunRecord | None:
        """@brief 拒绝旧版 Agent Run 投影 / Reject legacy Agent-Run projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param run_id Run ID / Run ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Agent 只能经 V2 UoW 读取。
        """
        del scope, run_id
        _reject_retired_v1_postgres_surface("agent")

    async def save_run(self, scope: ActorScope, record: DomainAgentRunRecord) -> None:
        """@brief 拒绝旧版 Agent Run 状态写入 / Reject legacy Agent-Run state writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param record Agent Run 记录 / Agent Run record.
        @raise RuntimeError PostgreSQL Agent 只能经 V2 UoW 写入。
        """
        del scope, record
        _reject_retired_v1_postgres_surface("agent")

    async def create_session(
        self,
        scope: ActorScope,
        record: DomainInterviewSessionRecord,
    ) -> None:
        """@brief 拒绝旧版面试会话写入 / Reject legacy Interview-session writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 面试 Session 聚合 / Interview Session aggregate.
        @raise RuntimeError PostgreSQL Interview 只能经 V2 UoW 写入。
        """
        del scope, record
        _reject_retired_v1_postgres_surface("interview")

    async def get_session(
        self,
        scope: ActorScope,
        session_id: str,
    ) -> DomainInterviewSessionRecord | None:
        """@brief 拒绝旧版面试会话投影 / Reject legacy Interview-session projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param session_id Session ID / Session ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Interview 只能经 V2 UoW 读取。
        """
        del scope, session_id
        _reject_retired_v1_postgres_surface("interview")

    async def list_sessions(
        self,
        scope: ActorScope,
    ) -> list[DomainInterviewSessionRecord]:
        """@brief 拒绝旧版面试列表投影 / Reject legacy Interview list projections.

        @param scope 请求租户范围 / Request tenant scope.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Interview 只能经 V2 UoW 列出。
        """
        del scope
        _reject_retired_v1_postgres_surface("interview")

    async def save_session(self, scope: ActorScope, record: DomainInterviewSessionRecord) -> None:
        """@brief 拒绝旧版面试状态写入 / Reject legacy Interview-state writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 面试 Session 聚合 / Interview Session aggregate.
        @raise RuntimeError PostgreSQL Interview 只能经 V2 UoW 写入。
        """
        del scope, record
        _reject_retired_v1_postgres_surface("interview")

    async def save_report(self, scope: ActorScope, report: dict[str, Any]) -> None:
        """@brief 拒绝旧版面试报告写入 / Reject legacy Interview-report writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param report 公开 InterviewReport 对象 / Public InterviewReport object.
        @raise RuntimeError PostgreSQL Interview Report 只能经 V2 UoW 写入。
        """
        del scope, report
        _reject_retired_v1_postgres_surface("interview")

    async def get_report(self, scope: ActorScope, report_id: str) -> dict[str, Any] | None:
        """@brief 拒绝旧版面试报告投影 / Reject legacy Interview-report projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param report_id 报告 ID / Report ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Interview Report 只能经 V2 UoW 读取。
        """
        del scope, report_id
        _reject_retired_v1_postgres_surface("interview")

    async def create_source(
        self,
        scope: ActorScope,
        record: DomainKnowledgeSourceRecord,
    ) -> None:
        """@brief 创建知识来源与默认拒绝策略 / Create a knowledge source and its default-deny policy.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 知识来源聚合 / Knowledge source aggregate.
        """
        _assert_record_scope(scope, record.scope)
        async with self._database.transaction(scope) as session:
            await _ensure_scope_identities(session, scope)
            row = await _scoped_one(session, KnowledgeSourceOrmRecord, scope, record.id, lock=True)
            if row is None:
                row = KnowledgeSourceOrmRecord(
                    id=record.id,
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    source_type=self._storage_source_type(record.source_type),
                    title=record.name,
                    config=deepcopy(record.config),
                    revision_mode="latest",
                    ingestion_state="new",
                    created_at=record.created_at,
                    updated_at=record.updated_at,
                    revision=record.revision,
                    extensions={},
                )
                session.add(row)
                await session.flush()
            await self._write_source_locked(session, scope, row, record)

    async def get_source(
        self,
        scope: ActorScope,
        source_id: str,
    ) -> DomainKnowledgeSourceRecord | None:
        """@brief 读取知识来源、版本、chunk 与 embedding / Read a source, its version, chunks, and embeddings.

        @param scope 请求租户范围 / Request tenant scope.
        @param source_id 来源 ID / Source ID.
        @return KnowledgeSourceRecord 或 None。
        """
        async with self._database.read_session(scope) as session:
            row = await _scoped_one(session, KnowledgeSourceOrmRecord, scope, source_id)
            return await self._source_from_row(session, scope, row) if row is not None else None

    async def list_sources(self, scope: ActorScope) -> list[DomainKnowledgeSourceRecord]:
        """@brief 列出范围内知识来源 / List scoped knowledge sources.

        @param scope 请求租户范围 / Request tenant scope.
        @return 最近更新优先的知识来源聚合。
        """
        async with self._database.read_session(scope) as session:
            statement = scoped_select(KnowledgeSourceOrmRecord, scope).order_by(
                KnowledgeSourceOrmRecord.updated_at.desc()
            )
            rows = (await session.scalars(statement)).all()
            return [await self._source_from_row(session, scope, row) for row in rows]

    async def save_source(self, scope: ActorScope, record: DomainKnowledgeSourceRecord) -> None:
        """@brief 保存来源状态、版本与向量 chunk / Save source state, version, and vector chunks.

        @param scope 请求租户范围 / Request tenant scope.
        @param record 知识来源聚合 / Knowledge source aggregate.
        """
        _assert_record_scope(scope, record.scope)
        async with self._database.transaction(scope) as session:
            row = await _scoped_one(session, KnowledgeSourceOrmRecord, scope, record.id, lock=True)
            if row is None:
                raise RuntimeError("cannot save a knowledge source outside the supplied scope")
            await self._write_source_locked(session, scope, row, record)

    async def save_source_if_revision(
        self,
        scope: ActorScope,
        record: DomainKnowledgeSourceRecord,
        expected_revision: int,
    ) -> bool:
        """Compare-and-set a source revision inside one PostgreSQL transaction."""
        _assert_record_scope(scope, record.scope)
        async with self._database.transaction(scope) as session:
            row = await _scoped_one(
                session,
                KnowledgeSourceOrmRecord,
                scope,
                record.id,
                lock=True,
            )
            if row is None or int(row.revision) != expected_revision:
                return False
            await self._write_source_locked(session, scope, row, record)
            return True

    async def save_source_and_job(
        self,
        scope: ActorScope,
        record: DomainKnowledgeSourceRecord,
        job: Job,
    ) -> None:
        """Atomically publish source/chunks/embeddings and the ingestion Job state."""
        _assert_record_scope(scope, record.scope)
        async with self._database.transaction(scope) as session:
            await _ensure_scope_identities(session, scope)
            source_row = await _scoped_one(
                session, KnowledgeSourceOrmRecord, scope, record.id, lock=True
            )
            if source_row is None:
                source_row = KnowledgeSourceOrmRecord(
                    id=record.id,
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    source_type=self._storage_source_type(record.source_type),
                    title=record.name,
                    config=deepcopy(record.config),
                    revision_mode="latest",
                    ingestion_state="new",
                    created_at=record.created_at,
                    updated_at=record.updated_at,
                    revision=record.revision,
                    extensions={},
                )
                session.add(source_row)
                await session.flush()
            await self._write_source_locked(session, scope, source_row, record)
            await self._save_job_locked(session, scope, job, allow_create=True)

    async def get_embedding_space(self, scope: ActorScope) -> EmbeddingSpace | None:
        """@brief 读取当前 workspace/owner 的默认 embedding 空间 / Read the scoped default embedding space.

        @param scope 请求租户范围 / Request tenant scope.
        @return 最新未 retired 的 EmbeddingSpace 或 None。
        """
        async with self._database.read_session(scope) as session:
            statement = (
                scoped_select(EmbeddingSpaceOrmRecord, scope)
                .where(EmbeddingSpaceOrmRecord.retired_at.is_(None))
                .order_by(EmbeddingSpaceOrmRecord.created_at.desc())
                .limit(1)
            )
            row = (await session.scalars(statement)).first()
            return self._embedding_space_from_row(row) if row is not None else None

    async def save_embedding_space(self, scope: ActorScope, space: EmbeddingSpace) -> None:
        """@brief 创建或验证不可变 embedding 空间 / Create or verify an immutable embedding space.

        @param scope 请求租户范围 / Request tenant scope.
        @param space 不可变 embedding space / Immutable embedding space.
        @raise ValueError 已有不同 active space 或维度不兼容时抛出。
        """
        if space.dimension != 1024:
            raise ValueError("v0.1 PostgreSQL vector columns require 1024-dimensional embeddings")
        async with self._database.transaction(scope) as session:
            await _ensure_scope_identities(session, scope)
            existing_rows = (
                await session.scalars(
                    scoped_select(EmbeddingSpaceOrmRecord, scope).where(
                        EmbeddingSpaceOrmRecord.retired_at.is_(None)
                    )
                )
            ).all()
            for existing in existing_rows:
                if str(existing.id) == space.id:
                    return
                if self._embedding_space_from_row(existing) != space:
                    raise ValueError(
                        "embedding spaces are immutable; create a data migration for a new space"
                    )
                return
            session.add(
                EmbeddingSpaceOrmRecord(
                    id=space.id,
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    provider=space.provider,
                    model=space.model,
                    model_revision=space.model_revision,
                    dimension=space.dimension,
                    distance_metric=space.distance_metric,
                    normalization=space.normalization,
                    created_at=space.created_at,
                    updated_at=space.created_at,
                    revision=1,
                    extensions={},
                )
            )

    async def rank_chunks_by_vector(
        self,
        scope: ActorScope,
        chunk_ids: list[str],
        embedding_space_id: str,
        query_vector: tuple[float, ...],
        limit: int,
    ) -> list[tuple[str, float]]:
        """Use pgvector cosine distance over an already authorized chunk subset."""
        if not chunk_ids or limit <= 0:
            return []
        if len(query_vector) != 1024:
            raise ValueError("v0.1 PostgreSQL vector search requires 1024 dimensions")
        distance = KnowledgeEmbeddingOrmRecord.embedding.cosine_distance(
            list(query_vector)
        ).label("distance")
        statement = (
            select(KnowledgeEmbeddingOrmRecord.chunk_id, distance)
            .where(
                KnowledgeEmbeddingOrmRecord.workspace_id == scope.workspace_id,
                KnowledgeEmbeddingOrmRecord.resource_owner_id == scope.resource_owner_id,
                KnowledgeEmbeddingOrmRecord.embedding_space_id == embedding_space_id,
                KnowledgeEmbeddingOrmRecord.chunk_id.in_(chunk_ids),
            )
            .order_by(distance.asc())
            .limit(limit)
        )
        async with self._database.read_session(scope) as session:
            rows = (await session.execute(statement)).all()
        return [
            (
                str(chunk_id),
                min(1.0, max(0.0, (float(1.0 - distance_value) + 1.0) / 2.0)),
            )
            for chunk_id, distance_value in rows
        ]

    @staticmethod
    def _storage_source_type(source_type: str) -> str:
        """@brief 将 mock 扩展类型降级到数据库受限 enum / Lower mock extension types to the database enum.

        @param source_type 领域来源类型 / Domain source type.
        @return 满足数据库 CHECK 的来源类型。
        """
        return "url" if source_type in {"website", "blog_feed"} else source_type

    @staticmethod
    def _storage_ingestion_state(status: str) -> str:
        """@brief 映射领域导入状态到数据库 enum / Map domain ingestion state to database enum.

        @param status 领域导入状态 / Domain ingestion status.
        @return 满足数据库 CHECK 的导入状态。
        """
        return {
            "not_started": "new",
            "queued": "queued",
            "fetching": "indexing",
            "parsing": "indexing",
            "chunking": "indexing",
            "embedding": "indexing",
            "ready": "ready",
            "stale": "stale",
            "failed": "failed",
        }.get(status, "new")

    @staticmethod
    def _domain_ingestion_state(status: str) -> str:
        """@brief 映射数据库导入状态到领域状态 / Map database ingestion state to domain state.

        @param status 数据库存储状态 / Database storage state.
        @return 领域可见导入状态。
        """
        return {"new": "not_started", "indexing": "chunking"}.get(status, status)

    async def _write_source_locked(
        self,
        session: AsyncSession,
        scope: ActorScope,
        row: Any,
        record: DomainKnowledgeSourceRecord,
    ) -> None:
        """@brief 在锁定来源行上保存其聚合状态 / Save aggregate state on a locked source row.

        @param session 已安装 scope 的写事务 / Scoped write transaction.
        @param scope 请求租户范围 / Request tenant scope.
        @param row 已锁定来源 ORM 行 / Locked source ORM row.
        @param record 候选知识来源聚合 / Candidate knowledge-source aggregate.
        @raise RuntimeError 候选 revision 落后于已持久化 revision 时抛出 / Raised when the candidate revision lags the persisted revision.

        @note 等 revision 仍允许写入，以维持同一 Job 重试的幂等性；只有严格较旧的
        异步 worker 会被拒绝。该检查在 ``FOR UPDATE`` 行锁持有期间执行，因而跨 worker
        的陈旧 Resume 索引不能在较新来源 revision 之后写回。
        """
        if record.revision < int(row.revision):
            raise RuntimeError("stale knowledge source write would overwrite a newer revision")
        row.source_type = self._storage_source_type(record.source_type)
        row.title = record.name
        row.config = deepcopy(record.config)
        row.ingestion_state = self._storage_ingestion_state(record.ingestion_status)
        row.updated_at = record.updated_at
        row.revision = record.revision
        row.extensions = _with_runtime(
            row.extensions,
            {
                "source_type": record.source_type,
                "visibility": deepcopy(record.visibility),
                "mock_content": record.mock_content,
                "source_version_id": record.source_version_id,
                "ingestion_status": record.ingestion_status,
                "enabled": record.enabled,
                "classification": record.classification.as_dict(),
                "source_metadata": deepcopy(record.source_metadata),
                "private_metadata": deepcopy(record.private_metadata),
                "document_parts": [
                    {
                        "text": part.text,
                        "content_type": part.content_type.value,
                        "metadata": deepcopy(part.metadata),
                    }
                    for part in record.document_parts
                ],
            },
        )
        await self._write_visibility_policy(session, scope, record)
        if record.source_version_id is not None:
            await self._write_source_version_and_chunks(session, scope, record)

    async def _write_visibility_policy(
        self,
        session: AsyncSession,
        scope: ActorScope,
        record: DomainKnowledgeSourceRecord,
    ) -> None:
        """@brief 保存当前可见性策略和 grants / Persist the current visibility policy and grants.

        @param session 已安装 scope 的写事务 / Scoped write transaction.
        @param scope 请求租户范围 / Request tenant scope.
        @param record 知识来源聚合 / Knowledge source aggregate.
        """
        policy = _as_json_object(record.visibility)
        policy_version = policy.get("policy_version")
        numeric_version = (
            int(policy_version) if isinstance(policy_version, int) and policy_version > 0 else 1
        )
        statement = scoped_select(KnowledgeVisibilityPolicyOrmRecord, scope).where(
            KnowledgeVisibilityPolicyOrmRecord.source_id == record.id,
            KnowledgeVisibilityPolicyOrmRecord.policy_version == numeric_version,
        )
        row = (await session.scalars(statement)).first()
        if row is None:
            row = KnowledgeVisibilityPolicyOrmRecord(
                id=_stable_id("kpol", record.id, str(numeric_version)),
                workspace_id=scope.workspace_id,
                resource_owner_id=scope.resource_owner_id,
                source_id=record.id,
                policy_version=numeric_version,
                default_effect="deny",
                sensitivity="confidential",
                session_override_allowed=False,
                allow_external_model_processing=False,
                allowed_model_regions=[],
                extensions={},
            )
            session.add(row)
            await session.flush()
        default_effect = policy.get("default_effect")
        row.default_effect = default_effect if default_effect in {"allow", "deny"} else "deny"
        row.sensitivity = str(policy.get("sensitivity", "confidential"))
        row.session_override_allowed = bool(policy.get("session_override_allowed", False))
        row.allow_external_model_processing = bool(
            policy.get("allow_external_model_processing", False)
        )
        row.allowed_model_regions = _as_string_list(policy.get("allowed_model_regions"))
        retention_days = policy.get("retention_days")
        row.retention_days = retention_days if isinstance(retention_days, int) else None
        row.revision = numeric_version
        row.extensions = _with_runtime(row.extensions, {"policy": deepcopy(policy)})
        await session.execute(
            delete(KnowledgeVisibilityGrantOrmRecord).where(
                KnowledgeVisibilityGrantOrmRecord.workspace_id == scope.workspace_id,
                KnowledgeVisibilityGrantOrmRecord.resource_owner_id == scope.resource_owner_id,
                KnowledgeVisibilityGrantOrmRecord.policy_id == row.id,
            )
        )
        await session.flush()
        grants = policy.get("agent_grants")
        if not isinstance(grants, list):
            return
        for ordinal, grant in enumerate(grants):
            if not isinstance(grant, dict):
                continue
            agent_scope = grant.get("agent_scope")
            effect = grant.get("effect")
            if not isinstance(agent_scope, str) or effect not in {"allow", "deny"}:
                continue
            session.add(
                KnowledgeVisibilityGrantOrmRecord(
                    id=_stable_id("kgrant", row.id, agent_scope, str(ordinal)),
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    policy_id=str(row.id),
                    agent_scope=agent_scope,
                    effect=effect,
                    allowed_operations=_as_string_list(grant.get("allowed_operations")),
                    extensions={},
                )
            )

    async def _write_source_version_and_chunks(
        self,
        session: AsyncSession,
        scope: ActorScope,
        record: DomainKnowledgeSourceRecord,
    ) -> None:
        """@brief 保存不可变来源版本及其 chunk/embedding / Persist an immutable source version and its chunks/embeddings.

        @param session 已安装 scope 的写事务 / Scoped write transaction.
        @param scope 请求租户范围 / Request tenant scope.
        @param record 已完成或正在完成导入的来源 / Source being or having been ingested.
        @raise ValueError chunk 引用不存在 embedding space 或维度错误时抛出。
        """
        source_version_id = record.source_version_id
        if source_version_id is None:
            return
        version_row = await _scoped_one(
            session, KnowledgeSourceVersionOrmRecord, scope, source_version_id, lock=True
        )
        if version_row is None:
            maximum_statement = select(
                func.coalesce(func.max(KnowledgeSourceVersionOrmRecord.version_no), 0)
            ).where(
                KnowledgeSourceVersionOrmRecord.workspace_id == scope.workspace_id,
                KnowledgeSourceVersionOrmRecord.resource_owner_id == scope.resource_owner_id,
                KnowledgeSourceVersionOrmRecord.source_id == record.id,
            )
            maximum = await session.scalar(maximum_statement)
            version_row = KnowledgeSourceVersionOrmRecord(
                id=source_version_id,
                workspace_id=scope.workspace_id,
                resource_owner_id=scope.resource_owner_id,
                source_id=record.id,
                version_no=int(maximum if maximum is not None else 0) + 1,
                content_hash=_source_content_hash(record),
                origin={"source_type": record.source_type, "config": deepcopy(record.config)},
                parser_metadata=deepcopy(record.source_metadata),
                indexed_at=record.updated_at,
                created_at=record.updated_at,
                updated_at=record.updated_at,
                revision=1,
                extensions=_with_runtime(
                    {},
                    {"private_metadata": deepcopy(record.private_metadata)},
                ),
            )
            session.add(version_row)
            await session.flush()
        existing_chunks = (
            await session.scalars(
                scoped_select(KnowledgeChunkOrmRecord, scope).where(
                    KnowledgeChunkOrmRecord.source_version_id == source_version_id
                )
            )
        ).all()
        existing_chunk_ids = [str(chunk.id) for chunk in existing_chunks]
        if existing_chunk_ids:
            await session.execute(
                delete(KnowledgeEmbeddingOrmRecord).where(
                    KnowledgeEmbeddingOrmRecord.workspace_id == scope.workspace_id,
                    KnowledgeEmbeddingOrmRecord.resource_owner_id == scope.resource_owner_id,
                    KnowledgeEmbeddingOrmRecord.chunk_id.in_(existing_chunk_ids),
                )
            )
        await session.execute(
            delete(KnowledgeChunkOrmRecord).where(
                KnowledgeChunkOrmRecord.workspace_id == scope.workspace_id,
                KnowledgeChunkOrmRecord.resource_owner_id == scope.resource_owner_id,
                KnowledgeChunkOrmRecord.source_version_id == source_version_id,
            )
        )
        await session.flush()
        for chunk in record.chunks:
            if chunk.source_id != record.id or chunk.source_version_id != source_version_id:
                raise ValueError(
                    "knowledge chunk does not belong to the source version being saved"
                )
            if len(chunk.vector) != 1024:
                raise ValueError(
                    "v0.1 PostgreSQL vector columns require 1024-dimensional embeddings"
                )
            space = await _scoped_one(
                session, EmbeddingSpaceOrmRecord, scope, chunk.embedding_space_id
            )
            if space is None:
                raise ValueError("knowledge chunk references an unknown embedding space")
            session.add(
                KnowledgeChunkOrmRecord(
                    id=chunk.id,
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    source_version_id=source_version_id,
                    ordinal=chunk.ordinal,
                    text_content=chunk.text,
                    content_hash=_json_hash(chunk.text),
                    origin={
                        "source_id": record.id,
                        "source_version_id": source_version_id,
                        "metadata": deepcopy(chunk.metadata),
                    },
                    token_count=len(chunk.text.split()),
                    extensions={"aiws": {"classification": chunk.classification.as_dict()}},
                )
            )
            session.add(
                KnowledgeEmbeddingOrmRecord(
                    id=_stable_id("emb", chunk.id, chunk.embedding_space_id),
                    workspace_id=scope.workspace_id,
                    resource_owner_id=scope.resource_owner_id,
                    chunk_id=chunk.id,
                    embedding_space_id=chunk.embedding_space_id,
                    embedding=list(chunk.vector),
                    extensions={},
                )
            )

    async def _source_from_row(
        self,
        session: AsyncSession,
        scope: ActorScope,
        row: Any,
    ) -> DomainKnowledgeSourceRecord:
        """@brief 将来源相关规范化行还原为领域聚合 / Rehydrate source-related normalized rows into an aggregate.

        @param session 已安装 scope 的读事务 / Scoped read transaction.
        @param scope 请求租户范围 / Request tenant scope.
        @param row 来源 ORM 行 / Source ORM row.
        @return KnowledgeSourceRecord。
        """
        runtime = _runtime_payload(row.extensions)
        source_version_id = runtime.get("source_version_id")
        chunks: list[KnowledgeChunk] = []
        if isinstance(source_version_id, str):
            chunk_statement = (
                scoped_select(KnowledgeChunkOrmRecord, scope)
                .where(KnowledgeChunkOrmRecord.source_version_id == source_version_id)
                .order_by(KnowledgeChunkOrmRecord.ordinal.asc())
            )
            chunk_rows = (await session.scalars(chunk_statement)).all()
            chunk_ids = [str(chunk.id) for chunk in chunk_rows]
            embeddings_by_chunk: dict[str, Any] = {}
            if chunk_ids:
                embedding_statement = scoped_select(KnowledgeEmbeddingOrmRecord, scope).where(
                    KnowledgeEmbeddingOrmRecord.chunk_id.in_(chunk_ids)
                )
                embeddings_by_chunk = {
                    str(embedding.chunk_id): embedding
                    for embedding in (await session.scalars(embedding_statement)).all()
                }
            for chunk_row in chunk_rows:
                embedding = embeddings_by_chunk.get(str(chunk_row.id))
                if embedding is None:
                    continue
                chunks.append(
                    KnowledgeChunk(
                        id=str(chunk_row.id),
                        source_id=str(row.id),
                        source_version_id=source_version_id,
                        embedding_space_id=str(embedding.embedding_space_id),
                        ordinal=int(chunk_row.ordinal),
                        text=str(chunk_row.text_content),
                        vector=tuple(float(value) for value in embedding.embedding),
                        classification=KnowledgeClassification.from_dict(
                            _as_json_object(chunk_row.extensions)
                            .get("aiws", {})
                            .get("classification")
                            if isinstance(_as_json_object(chunk_row.extensions).get("aiws"), dict)
                            else None
                        ),
                        metadata=_as_json_object(
                            _as_json_object(chunk_row.origin).get("metadata")
                        ),
                    )
                )
        source_type = runtime.get("source_type")
        mock_content_candidate = runtime.get("mock_content")
        mock_content = mock_content_candidate if isinstance(mock_content_candidate, str) else ""
        document_parts: list[KnowledgeDocumentPart] = []
        raw_parts = runtime.get("document_parts")
        if isinstance(raw_parts, list):
            for part in raw_parts:
                if not isinstance(part, dict) or not isinstance(part.get("text"), str):
                    continue
                try:
                    content_type = KnowledgeContentType(str(part.get("content_type", "general")))
                except ValueError:
                    content_type = KnowledgeContentType.GENERAL
                document_parts.append(
                    KnowledgeDocumentPart(
                        text=str(part["text"]),
                        content_type=content_type,
                        metadata=_as_json_object(part.get("metadata")),
                    )
                )
        return DomainKnowledgeSourceRecord(
            scope=scope,
            id=str(row.id),
            created_at=row.created_at,
            updated_at=row.updated_at,
            name=str(row.title),
            source_type=source_type if isinstance(source_type, str) else str(row.source_type),
            config=deepcopy(cast(dict[str, Any], row.config)),
            visibility=_as_json_object(runtime.get("visibility")),
            revision=int(row.revision),
            enabled=bool(runtime.get("enabled", True)),
            ingestion_status=(
                str(runtime["ingestion_status"])
                if isinstance(runtime.get("ingestion_status"), str)
                else self._domain_ingestion_state(str(row.ingestion_state))
            ),
            source_version_id=source_version_id if isinstance(source_version_id, str) else None,
            chunks=chunks,
            mock_content=mock_content,
            classification=KnowledgeClassification.from_dict(runtime.get("classification")),
            source_metadata=_as_json_object(runtime.get("source_metadata")),
            private_metadata=_as_json_object(runtime.get("private_metadata")),
            document_parts=document_parts,
        )

    @staticmethod
    def _embedding_space_from_row(row: Any) -> EmbeddingSpace:
        """@brief 将 ORM embedding-space 行还原为领域值对象 / Rehydrate an ORM embedding-space row into a value object.

        @param row EmbeddingSpace ORM 行 / EmbeddingSpace ORM row.
        @return 不可变 EmbeddingSpace。
        """
        return EmbeddingSpace(
            id=str(row.id),
            provider=str(row.provider),
            model=str(row.model),
            model_revision=str(row.model_revision),
            dimension=int(row.dimension),
            distance_metric=str(row.distance_metric),
            normalization=str(row.normalization),
            created_at=row.created_at,
        )

    async def create_job(self, scope: ActorScope, job: Job) -> None:
        """@brief 创建统一长任务 / Create a unified long-running job.

        @param scope 请求租户范围 / Request tenant scope.
        @param job Job 领域实体 / Domain Job entity.
        """
        async with self._database.transaction(scope) as session:
            await _ensure_scope_identities(session, scope)
            await self._save_job_locked(session, scope, job, allow_create=True)

    async def get_job(self, scope: ActorScope, job_id: str) -> Job | None:
        """@brief 读取范围内 Job / Read a scoped Job.

        @param scope 请求租户范围 / Request tenant scope.
        @param job_id Job ID / Job ID.
        @return Job 或 None。
        """
        async with self._database.read_session(scope) as session:
            row = await _scoped_one(session, JobOrmRecord, scope, job_id)
            return self._job_from_row(row) if row is not None else None

    async def claim_job(
        self,
        scope: ActorScope,
        job_id: str,
        stale_after_seconds: int = 900,
    ) -> Job | None:
        """Atomically claim one queued Job across workers using a row lock."""
        async with self._database.transaction(scope) as session:
            row = await _scoped_one(session, JobOrmRecord, scope, job_id, lock=True)
            if row is None:
                return None
            job = self._job_from_row(row)
            stale = (
                job.status is JobStatus.RUNNING
                and job.started_at is not None
                and job.started_at
                <= datetime.now(UTC) - timedelta(seconds=stale_after_seconds)
            )
            if job.status is not JobStatus.QUEUED and not stale:
                return None
            if stale:
                job.status = JobStatus.QUEUED
                job.phase = "queued"
                job.started_at = None
            job.start()
            self._write_job_row(row, job)
            if job.job_type == "resume.render":
                await self._sync_resume_render_job(session, scope, job)
            return job

    async def save_job(self, scope: ActorScope, job: Job) -> None:
        """@brief 保存 Job 生命周期与结果状态 / Save Job lifecycle and result state.

        @param scope 请求租户范围 / Request tenant scope.
        @param job Job 领域实体 / Domain Job entity.
        """
        async with self._database.transaction(scope) as session:
            await self._save_job_locked(session, scope, job, allow_create=False)

    async def _save_job_locked(
        self,
        session: AsyncSession,
        scope: ActorScope,
        job: Job,
        *,
        allow_create: bool,
    ) -> None:
        """Upsert a Job inside an existing tenant transaction."""
        row = await _scoped_one(session, JobOrmRecord, scope, job.id, lock=True)
        if row is None:
            if not allow_create:
                raise RuntimeError("cannot save a job outside the supplied scope")
            row = JobOrmRecord(
                id=job.id,
                workspace_id=scope.workspace_id,
                resource_owner_id=scope.resource_owner_id,
                job_type=job.job_type,
                status=job.status.value,
                phase=job.phase,
                completed_units=job.completed_units,
                total_units=job.total_units,
                percent=job.percent,
                request_id=job.request_id,
                created_at=job.created_at,
                updated_at=job.created_at,
                revision=1,
                extensions={},
            )
            session.add(row)
        self._write_job_row(row, job)
        if job.job_type == "resume.render":
            await session.flush()
            await self._sync_resume_render_job(session, scope, job)

    async def _sync_resume_render_job(
        self,
        session: AsyncSession,
        scope: ActorScope,
        job: Job,
    ) -> None:
        """Persist the Resume-specific link for a unified render Job."""
        resume_id = job.extensions.get("resume_id")
        resume_revision = job.extensions.get("resume_revision")
        if (
            not isinstance(resume_id, str)
            or isinstance(resume_revision, bool)
            or not isinstance(resume_revision, int)
        ):
            raise ValueError("resume render job requires a resume id and revision")

        resume = await _scoped_one(session, ResumeDocumentOrmRecord, scope, resume_id)
        revision_statement = scoped_select(ResumeRevisionOrmRecord, scope).where(
            ResumeRevisionOrmRecord.resume_id == resume_id,
            ResumeRevisionOrmRecord.revision_no == resume_revision,
        )
        revision_row = (await session.scalars(revision_statement)).first()
        if resume is None or revision_row is None:
            raise ValueError("resume render job references an unknown scoped resume revision")

        link_statement = (
            scoped_select(ResumeRenderJobOrmRecord, scope)
            .where(ResumeRenderJobOrmRecord.job_id == job.id)
            .with_for_update()
        )
        link = (await session.scalars(link_statement)).first()
        artifact_id = await self._render_job_artifact_id(
            session,
            scope,
            job,
            resume_id,
            str(revision_row.id),
        )
        diagnostics = job.extensions.get("diagnostics")
        diagnostics_payload = (
            deepcopy(diagnostics)
            if isinstance(diagnostics, dict)
            else {"items": deepcopy(diagnostics) if isinstance(diagnostics, list) else []}
        )
        timestamp = job.finished_at or job.started_at or job.created_at
        render_profile = job.extensions.get("render_profile", "preview")
        if not isinstance(render_profile, str) or not render_profile:
            raise ValueError("resume render job requires a render profile")

        if link is None:
            link = ResumeRenderJobOrmRecord(
                id=_stable_id("renderjob", job.id),
                workspace_id=scope.workspace_id,
                resource_owner_id=scope.resource_owner_id,
                job_id=job.id,
                resume_id=resume_id,
                resume_revision_id=str(revision_row.id),
                artifact_id=artifact_id,
                render_profile=render_profile,
                diagnostics=diagnostics_payload,
                created_at=job.created_at,
                updated_at=timestamp,
                revision=1,
                extensions={},
            )
            session.add(link)
            return

        link.resume_id = resume_id
        link.resume_revision_id = str(revision_row.id)
        if artifact_id is not None:
            link.artifact_id = artifact_id
        link.render_profile = render_profile
        link.diagnostics = diagnostics_payload
        link.updated_at = timestamp
        link.revision = int(link.revision) + 1

    @staticmethod
    async def _render_job_artifact_id(
        session: AsyncSession,
        scope: ActorScope,
        job: Job,
        resume_id: str,
        resume_revision_id: str,
    ) -> str | None:
        """@brief 拒绝旧 Job 到 V2 Artifact 的伪链接 / Reject pseudo-linking a legacy Job to a V2 Artifact.

        @param session 已安装 scope 的事务 / Transaction with installed scope.
        @param scope 请求租户范围 / Request tenant scope.
        @param job 旧版 Job 聚合 / Legacy Job aggregate.
        @param resume_id 旧版 Resume ID / Legacy Resume ID.
        @param resume_revision_id 旧版内部 revision ID / Legacy internal revision ID.
        @return Job 尚无产物时返回 None；否则不返回 / None while the Job has no artifact;
            otherwise this method never returns.
        @raise RuntimeError 旧 Artifact 写入端口已停用。
        """
        artifacts = job.extensions.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            return None
        del session, scope, resume_id, resume_revision_id
        _reject_retired_v1_postgres_surface("artifact")

    @staticmethod
    def _write_job_row(row: Any, job: Job) -> None:
        """@brief 将 Job 领域状态映射到 ORM 行 / Map domain Job state onto an ORM row.

        @param row Job ORM 行 / Job ORM row.
        @param job Job 领域实体 / Domain Job entity.
        @return 无返回值。
        """
        target_type, target_id = PostgresWorkspaceRepository._job_target(job)
        row.job_type = job.job_type
        row.status = job.status.value
        row.phase = job.phase
        row.completed_units = job.completed_units
        row.total_units = job.total_units
        row.percent = job.percent
        row.request_id = job.request_id
        row.target_resource_type = target_type
        row.target_resource_id = target_id
        row.result = _as_json_object(job.extensions.get("result")) or None
        row.error = _problem_to_json(job.error)
        row.started_at = job.started_at
        row.finished_at = job.finished_at
        row.updated_at = job.finished_at or job.started_at or job.created_at
        row.extensions = _with_runtime(row.extensions, {"extensions": deepcopy(job.extensions)})

    @staticmethod
    def _job_target(job: Job) -> tuple[str | None, str | None]:
        """@brief 从稳定 Job extensions 推导目标资源 / Infer the target resource from stable Job extensions.

        @param job Job 领域实体 / Domain Job entity.
        @return ``(resource_type, resource_id)``，未知时两者均为 None。
        """
        for key, resource_type in (
            ("resume_id", "resume"),
            ("source_id", "knowledge_source"),
            ("session_id", "interview_session"),
        ):
            value = job.extensions.get(key)
            if isinstance(value, str):
                return resource_type, value
        return None, None

    @staticmethod
    def _job_from_row(row: Any) -> Job:
        """@brief 将 ORM Job 行还原为领域实体 / Rehydrate an ORM Job row into a domain entity.

        @param row Job ORM 行 / Job ORM row.
        @return Job。
        """
        runtime = _runtime_payload(row.extensions)
        try:
            status = JobStatus(str(row.status))
        except ValueError:
            status = JobStatus.FAILED
        return Job(
            id=str(row.id),
            job_type=str(row.job_type),
            created_at=row.created_at,
            request_id=row.request_id,
            status=status,
            phase=str(row.phase),
            completed_units=int(row.completed_units),
            total_units=int(row.total_units) if row.total_units is not None else None,
            percent=float(row.percent) if row.percent is not None else None,
            started_at=row.started_at,
            finished_at=row.finished_at,
            error=_problem_from_json(row.error),
            extensions=_as_json_object(runtime.get("extensions")),
        )

    async def save_artifact(
        self,
        scope: ActorScope,
        artifact: dict[str, Any],
        content: bytes,
        source_map: dict[str, Any] | None,
    ) -> None:
        """@brief 拒绝旧版 Artifact 写入 / Reject legacy Artifact writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param artifact 公开产物元数据 / Public artifact metadata.
        @param content 原始二进制内容 / Raw binary content.
        @param source_map 可选语义 source map / Optional semantic source map.
        @raise RuntimeError PostgreSQL Artifact 只能经 V2 Resume/Platform UoW 写入。
        """
        del scope, artifact, content, source_map
        _reject_retired_v1_postgres_surface("artifact")

    async def save_artifact_and_job(
        self,
        scope: ActorScope,
        artifact: dict[str, Any],
        content: bytes,
        source_map: dict[str, Any] | None,
        job: Job,
    ) -> None:
        """@brief 拒绝旧版 Artifact 与 Job 双写 / Reject legacy Artifact-and-Job dual writes.

        @param scope 请求租户范围 / Request tenant scope.
        @param artifact 旧版 Artifact metadata / Legacy Artifact metadata.
        @param content 旧版 Artifact bytes / Legacy Artifact bytes.
        @param source_map 旧版 source map / Legacy source map.
        @param job 旧版 Job / Legacy Job.
        @raise RuntimeError PostgreSQL Artifact 只能经 V2 Resume/Platform UoW 原子写入。
        """
        del scope, artifact, content, source_map, job
        _reject_retired_v1_postgres_surface("artifact")

    async def get_artifact(
        self,
        scope: ActorScope,
        artifact_id: str,
    ) -> tuple[dict[str, Any], bytes, dict[str, Any] | None] | None:
        """@brief 拒绝旧版 Artifact 读取投影 / Reject legacy Artifact read projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param artifact_id 产物 ID / Artifact ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Artifact 只能经 V2 Platform UoW 读取。
        """
        del scope, artifact_id
        _reject_retired_v1_postgres_surface("artifact")

    async def list_artifacts(
        self, scope: ActorScope, resume_id: str
    ) -> list[dict[str, Any]]:
        """@brief 拒绝旧版 Artifact 列表投影 / Reject legacy Artifact list projections.

        @param scope 请求租户范围 / Request tenant scope.
        @param resume_id 旧版 Resume ID / Legacy Resume ID.
        @return 此端口不会返回 / This port never returns.
        @raise RuntimeError PostgreSQL Artifact 只能经 V2 Platform UoW 列出。
        """
        del scope, resume_id
        _reject_retired_v1_postgres_surface("artifact")


@dataclass(frozen=True, slots=True)
class _IdempotencyClaimDecision:
    """@brief pending claim 竞争结果 / Outcome of pending-claim arbitration.

    @param replay 已完成时可直接回放的响应 / Replayable response when already complete.
    @param claim_token 仅当前 claimant 持有的私有令牌 / Private token held only by the caller that won.

    @note ``claim_token`` 永不进入 HTTP 响应、Problem Details 或日志属性。它只在本进程
    的 ``execute`` 调用链中传递，用于防止过期 claimant 覆盖接管者。
    """

    replay: IdempotentResponse | None
    """@brief 已完成响应 / Completed replayable response."""

    claim_token: str | None
    """@brief 当前 caller 的不透明 claim 令牌 / Opaque claim token for the current caller."""


class PostgresIdempotencyRegistry:
    """@brief 跨进程持久化 HTTP 幂等注册表 / Cross-process durable HTTP idempotency registry.

    @param database 生命周期由 composition root 管理的数据库 / Database owned by the composition root.
    @param retention HTTP 成功响应的保留期 / Retention for successful HTTP responses.
    @param pending_timeout 单个尚未完成 claim 的接管超时 / Takeover timeout for an incomplete claim.

    @note 外部 ``operation`` 可能打开其自身的业务短事务，因而不能把 callback 放在
    idempotency 行锁持有期间执行。实现先提交 pending claim，再有界等待完成；进程崩溃
    后仅在 ``pending_timeout`` 到期才允许新请求接管，避免通常情况下的重复副作用。
    """

    def __init__(
        self,
        database: AsyncDatabase,
        retention: timedelta = timedelta(hours=24),
        pending_timeout: timedelta = timedelta(minutes=5),
    ) -> None:
        """@brief 配置持久化幂等注册表 / Configure the durable idempotency registry.

        @param database 生命周期由 composition root 管理的数据库 / Database owned by the composition root.
        @param retention HTTP 成功响应的保留期 / Retention for successful HTTP responses.
        @param pending_timeout pending claim 的接管超时 / Takeover timeout for a pending claim.
        @raise ValueError 保留期或超时不为正时抛出。
        """
        if retention <= timedelta() or pending_timeout <= timedelta():
            raise ValueError("idempotency retention and pending timeout must be positive")
        self._database = database
        self._retention = retention
        self._pending_timeout = pending_timeout

    async def execute(
        self,
        scope: ActorScope,
        path: str,
        key: str,
        payload: object,
        status_code: int,
        operation: Callable[[], Awaitable[dict[str, Any]]],
    ) -> IdempotentResponse:
        """@brief 执行、等待或回放持久化幂等命令 / Execute, await, or replay a durable idempotent command.

        @param scope actor/workspace/owner 范围 / Actor/workspace/owner scope.
        @param path 稳定 HTTP 路径模板 / Stable HTTP path template.
        @param key Idempotency-Key / Idempotency-Key.
        @param payload 请求 payload / Request payload.
        @param status_code 首次成功 HTTP 状态 / First-success HTTP status.
        @param operation 首次执行的异步业务操作 / Async business operation for the first execution.
        @return 首次成功或可重放的响应 / First-success or replayable response.
        @raise DomainError 同 key 不同请求，或已有请求仍在执行时抛出。
        """
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode()
        ).hexdigest()
        decision = await self._claim(scope, path, key, digest)
        if decision.replay is not None:
            return decision.replay
        claim_token = decision.claim_token
        if claim_token is None:
            replay = await self._wait_for_completion(scope, path, key, digest)
            if replay is not None:
                return replay
            raise DomainError(
                Problem(
                    "idempotency.in_progress",
                    409,
                    "An identical request is still being processed",
                    retryable=True,
                )
            )
        try:
            body = await operation()
        except BaseException:
            await self._release_pending_claim(scope, path, key, digest, claim_token)
            raise
        response = IdempotentResponse(status_code, deepcopy(body))
        completed = await self._complete_claim(
            scope,
            path,
            key,
            digest,
            claim_token,
            response,
        )
        if completed is not None:
            return completed
        replay = await self._wait_for_completion(scope, path, key, digest)
        if replay is not None:
            return replay
        raise DomainError(
            Problem(
                "idempotency.in_progress",
                409,
                "An identical request is still being processed",
                retryable=True,
            )
        )

    async def _claim(
        self,
        scope: ActorScope,
        path: str,
        key: str,
        digest: str,
    ) -> _IdempotencyClaimDecision:
        """@brief 原子读取或创建 pending claim / Atomically read or create a pending claim.

        @param scope actor/workspace/owner 范围 / Actor/workspace/owner scope.
        @param path 稳定 HTTP 路径模板 / Stable HTTP path template.
        @param key Idempotency-Key / Idempotency-Key.
        @param digest 规范化请求摘要 / Canonical request digest.
        @return 已完成回放响应或当前 caller 的私有 claim token。
        """
        now = datetime.now(UTC)
        async with self._database.transaction(scope) as session:
            await _ensure_scope_identities(session, scope)
            statement = (
                scoped_select(IdempotencyOrmRecord, scope)
                .where(
                    IdempotencyOrmRecord.actor_id == scope.actor_id,
                    IdempotencyOrmRecord.request_target == path,
                    IdempotencyOrmRecord.idempotency_key == key,
                )
                .with_for_update()
            )
            existing = (await session.scalars(statement)).first()
            if existing is not None:
                replay = self._validate_existing_claim(existing, digest, now)
                if replay is not None:
                    return _IdempotencyClaimDecision(replay=replay, claim_token=None)
                if self._claim_is_expired(existing, now):
                    await session.delete(existing)
                    await session.flush()
                else:
                    return _IdempotencyClaimDecision(replay=None, claim_token=None)
            claim_id = new_opaque_id("idem")
            claim_token = secrets.token_urlsafe(32)
            try:
                inserted = await session.execute(
                    insert(IdempotencyOrmRecord)
                    .values(
                        id=claim_id,
                        workspace_id=scope.workspace_id,
                        resource_owner_id=scope.resource_owner_id,
                        actor_id=scope.actor_id,
                        request_target=path,
                        idempotency_key=key,
                        request_hash=digest,
                        response_status=None,
                        response_body=None,
                        expires_at=now + self._retention,
                        created_at=now,
                        updated_at=now,
                        revision=1,
                        extensions=_with_runtime(
                            {},
                            {
                                _PENDING_CLAIM_TOKEN_KEY: claim_token,
                                "pending_until": (now + self._pending_timeout)
                                .isoformat()
                                .replace("+00:00", "Z")
                            },
                        ),
                    )
                    .on_conflict_do_nothing(
                        index_elements=[
                            IdempotencyOrmRecord.workspace_id,
                            IdempotencyOrmRecord.resource_owner_id,
                            IdempotencyOrmRecord.actor_id,
                            IdempotencyOrmRecord.request_target,
                            IdempotencyOrmRecord.idempotency_key,
                        ]
                    )
                    .returning(IdempotencyOrmRecord.id)
                )
            except IntegrityError:
                return _IdempotencyClaimDecision(replay=None, claim_token=None)
            if inserted.scalar_one_or_none() is None:
                return _IdempotencyClaimDecision(replay=None, claim_token=None)
            return _IdempotencyClaimDecision(replay=None, claim_token=claim_token)

    def _validate_existing_claim(
        self,
        row: Any,
        digest: str,
        now: datetime,
    ) -> IdempotentResponse | None:
        """@brief 校验已有 claim 并在完成时返回缓存响应 / Validate an existing claim and return cache when complete.

        @param row Idempotency ORM 行 / Idempotency ORM row.
        @param digest 当前请求摘要 / Current request digest.
        @param now 当前 UTC 时间 / Current UTC time.
        @return 可重放响应；pending 或过期记录返回 None。
        @raise DomainError 同 key 不同 body 时抛出。
        """
        if str(row.request_hash) != digest:
            raise DomainError(
                Problem(
                    "idempotency.key_reused", 409, "Idempotency key was reused with different input"
                )
            )
        if row.expires_at <= now:
            return None
        if row.response_status is None or not isinstance(row.response_body, dict):
            return None
        return IdempotentResponse(int(row.response_status), deepcopy(row.response_body))

    def _claim_is_expired(self, row: Any, now: datetime) -> bool:
        """@brief 判断 pending claim 是否可被接管 / Determine whether a pending claim may be taken over.

        @param row Idempotency ORM 行 / Idempotency ORM row.
        @param now 当前 UTC 时间 / Current UTC time.
        @return 已过期或 pending 超时时为真。
        """
        if row.expires_at <= now:
            return True
        runtime = _runtime_payload(row.extensions)
        pending_until = _as_datetime(
            runtime.get("pending_until"), row.created_at + self._pending_timeout
        )
        return pending_until <= now

    def _claim_token_matches(self, row: Any, claim_token: str) -> bool:
        """@brief 校验 caller 是否仍拥有 pending claim / Check whether caller still owns a pending claim.

        @param row 已加行锁的 Idempotency ORM 行 / Locked idempotency ORM row.
        @param claim_token 仅首次成功 claim 返回给内部调用链的令牌 / Token returned only to the internal winning claim path.
        @return 当前持有者匹配时为真 / True only when the current holder matches.

        @note 令牌以高熵随机值写入私有 JSONB runtime 区域，且比较使用
        ``hmac.compare_digest``。无 token 的旧 pending 行一律不视为当前 caller
        所有，宁可等待或超时接管，也绝不让旧 worker 写入新 claim。
        """
        runtime = _runtime_payload(row.extensions)
        current_token = runtime.get(_PENDING_CLAIM_TOKEN_KEY)
        return isinstance(current_token, str) and hmac.compare_digest(current_token, claim_token)

    async def _wait_for_completion(
        self,
        scope: ActorScope,
        path: str,
        key: str,
        digest: str,
    ) -> IdempotentResponse | None:
        """@brief 有界等待其它 worker 完成 claim / Wait boundedly for another worker to complete a claim.

        @param scope actor/workspace/owner 范围 / Actor/workspace/owner scope.
        @param path 稳定 HTTP 路径模板 / Stable HTTP path template.
        @param key Idempotency-Key / Idempotency-Key.
        @param digest 规范化请求摘要 / Canonical request digest.
        @return 已完成的响应；超时或 claim 消失时返回 None。
        """
        deadline = asyncio.get_running_loop().time() + min(
            self._pending_timeout.total_seconds(), 5.0
        )
        while asyncio.get_running_loop().time() < deadline:
            now = datetime.now(UTC)
            async with self._database.read_session(scope) as session:
                statement = scoped_select(IdempotencyOrmRecord, scope).where(
                    IdempotencyOrmRecord.actor_id == scope.actor_id,
                    IdempotencyOrmRecord.request_target == path,
                    IdempotencyOrmRecord.idempotency_key == key,
                )
                row = (await session.scalars(statement)).first()
                if row is None:
                    return None
                replay = self._validate_existing_claim(row, digest, now)
                if replay is not None:
                    return replay
            await asyncio.sleep(0.025)
        return None

    async def _complete_claim(
        self,
        scope: ActorScope,
        path: str,
        key: str,
        digest: str,
        claim_token: str,
        response: IdempotentResponse,
    ) -> IdempotentResponse | None:
        """@brief 将 pending claim 原子完成为可回放响应 / Atomically complete a pending claim into a replayable response.

        @param scope actor/workspace/owner 范围 / Actor/workspace/owner scope.
        @param path 稳定 HTTP 路径模板 / Stable HTTP path template.
        @param key Idempotency-Key / Idempotency-Key.
        @param digest 规范化请求摘要 / Canonical request digest.
        @param claim_token 当前 caller 所持私有 claim 令牌 / Private claim token held by the caller.
        @param response 首次成功响应 / First-success response.
        @return 本次或已抢先完成的响应；claim 已被接管时返回 ``None``。
        """
        now = datetime.now(UTC)
        async with self._database.transaction(scope) as session:
            statement = (
                scoped_select(IdempotencyOrmRecord, scope)
                .where(
                    IdempotencyOrmRecord.actor_id == scope.actor_id,
                    IdempotencyOrmRecord.request_target == path,
                    IdempotencyOrmRecord.idempotency_key == key,
                )
                .with_for_update()
            )
            row = (await session.scalars(statement)).first()
            if row is None:
                return None
            replay = self._validate_existing_claim(row, digest, now)
            if replay is not None:
                return replay
            if str(row.request_hash) != digest:
                raise DomainError(
                    Problem(
                        "idempotency.key_reused",
                        409,
                        "Idempotency key was reused with different input",
                    )
                )
            if not self._claim_token_matches(row, claim_token):
                return None
            row.response_status = response.status_code
            row.response_body = deepcopy(response.body)
            row.updated_at = now
            row.revision = int(row.revision) + 1
            runtime = _runtime_payload(row.extensions)
            runtime.pop(_PENDING_CLAIM_TOKEN_KEY, None)
            runtime.pop("pending_until", None)
            runtime["completed"] = True
            row.extensions = _with_runtime(row.extensions, runtime)
            return IdempotentResponse(response.status_code, deepcopy(response.body))

    async def _release_pending_claim(
        self,
        scope: ActorScope,
        path: str,
        key: str,
        digest: str,
        claim_token: str,
    ) -> None:
        """@brief 在业务失败后释放自己的 pending claim / Release this pending claim after business failure.

        @param scope actor/workspace/owner 范围 / Actor/workspace/owner scope.
        @param path 稳定 HTTP 路径模板 / Stable HTTP path template.
        @param key Idempotency-Key / Idempotency-Key.
        @param digest 规范化请求摘要 / Canonical request digest.
        @param claim_token 当前 caller 所持私有 claim 令牌 / Private claim token held by the caller.
        @return 无返回值。
        """
        async with self._database.transaction(scope) as session:
            statement = (
                scoped_select(IdempotencyOrmRecord, scope)
                .where(
                    IdempotencyOrmRecord.actor_id == scope.actor_id,
                    IdempotencyOrmRecord.request_target == path,
                    IdempotencyOrmRecord.idempotency_key == key,
                )
                .with_for_update()
            )
            row = (await session.scalars(statement)).first()
            if (
                row is not None
                and str(row.request_hash) == digest
                and row.response_status is None
                and self._claim_token_matches(row, claim_token)
            ):
                await session.delete(row)


__all__ = [
    "PostgresIdempotencyRegistry",
    "PostgresWorkspaceRepository",
]
