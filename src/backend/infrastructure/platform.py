"""@brief API V2 Job、Artifact、Event 与 Audit 持久化适配器 / API V2 platform persistence adapters.

本模块把统一 Job projection、Artifact metadata/content、transactional outbox event feed 与
append-only audit journal 固定在冻结的 Platform ports 后面。PostgreSQL UoW 始终通过
``AsyncDatabase.new_session`` 加入请求级 atomic envelope；内存实现使用 copy-on-write，
因此测试与本地运行具有相同的 commit/rollback 语义。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from functools import partial
from hashlib import sha256
from types import TracebackType
from typing import Any, Protocol, Self, cast

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import and_, func, null, or_, select, update
from sqlalchemy.engine import CursorResult, Result
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from backend.application.ports.access import AccessAuthorizer, AuthorizationDenied
from backend.application.ports.platform import (
    ArtifactContentStream,
    ArtifactQuery,
    Clock,
    CollectionPage,
    ContentRange,
    EventReplayRequest,
    EventReplayWindowExpired,
    JobCancellationRejected,
    JobCasMismatch,
    JobQuery,
    MutationContext,
    PageRequest,
    PlatformAuthorizationRequest,
    PlatformPermission,
    PlatformResourceTarget,
    PlatformTargetKind,
)
from backend.domain.outbox import initial_outbox_lifecycle
from backend.domain.platform import (
    ApiArtifactContentUrl,
    ApiEvent,
    ApiEventId,
    Artifact,
    ArtifactId,
    ArtifactKind,
    AuditEvent,
    AuditEventId,
    AuditOutcome,
    Job,
    JobId,
    JobProgress,
    JobProgressUnit,
    JobStatus,
    PdfSourceMap,
    ProblemDetails,
    ResourceRef,
)
from backend.domain.principals import (
    AuthenticatedActor,
    ResourceMeta,
    TokenPrincipal,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
)
from backend.infrastructure.access import (
    InMemoryAccessRepository,
    InMemoryAccessStore,
    PostgresAccessRepository,
)
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    AgentRunRecord,
    ArtifactContentRecord,
    ArtifactPdfSourceMapRecord,
    ArtifactRecord,
    AuditEventRecord,
    ConnectionRecord,
    InterviewReportJobRecord,
    InterviewSessionRecord,
    JobRecord,
    JsonObject,
    KnowledgeSourceRecord,
    KnowledgeUploadSessionRecord,
    OutboxEventRecord,
    ToolApprovalRecord,
)
from workspace_shared.ids import new_opaque_id

_RESOURCE_REF_ADAPTER: TypeAdapter[ResourceRef] = TypeAdapter(ResourceRef)
"""@brief ResourceRef 的持久化 codec / Persistence codec for ResourceRef."""

_RESOURCE_REFS_ADAPTER: TypeAdapter[tuple[ResourceRef, ...]] = TypeAdapter(
    tuple[ResourceRef, ...]
)
"""@brief Job result_refs 的持久化 codec / Persistence codec for Job result_refs."""

_PROBLEM_ADAPTER: TypeAdapter[ProblemDetails] = TypeAdapter(ProblemDetails)
"""@brief Job problem 的持久化 codec / Persistence codec for Job problems."""

_SOURCE_MAP_ADAPTER: TypeAdapter[PdfSourceMap] = TypeAdapter(PdfSourceMap)
"""@brief PDF source map 的持久化 codec / Persistence codec for PDF source maps."""

_EVENT_RETENTION = timedelta(days=30)
"""@brief API event 的稳定 replay window / Stable replay window for API events."""

_CONTENT_CHUNK_SIZE = 64 * 1024
"""@brief BYTEA 内容返回的默认 chunk 大小 / Default chunk size for BYTEA content."""

_PERMISSION_ACTION: Mapping[PlatformPermission, WorkspaceAction] = {
    PlatformPermission.LIST_JOBS: WorkspaceAction.LIST_JOBS,
    PlatformPermission.READ_JOB: WorkspaceAction.READ_JOB,
    PlatformPermission.CANCEL_JOB: WorkspaceAction.CANCEL_JOB,
    PlatformPermission.LIST_ARTIFACTS: WorkspaceAction.LIST_ARTIFACTS,
    PlatformPermission.READ_ARTIFACT: WorkspaceAction.READ_ARTIFACT,
    PlatformPermission.READ_ARTIFACT_CONTENT: WorkspaceAction.READ_ARTIFACT_CONTENT,
    PlatformPermission.READ_ARTIFACT_SOURCE_MAP: WorkspaceAction.READ_ARTIFACT_SOURCE_MAP,
    PlatformPermission.READ_EVENTS: WorkspaceAction.READ_EVENTS,
    PlatformPermission.LIST_AUDIT_EVENTS: WorkspaceAction.LIST_AUDIT_EVENTS,
}
"""@brief Platform permission 到集中 Workspace action 的穷尽映射 / Exhaustive permission-to-action mapping."""


def _dump_object[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> JsonObject:
    """@brief 编码领域值为 JSON object / Encode a domain value as a JSON object.

    @param adapter 对应 Pydantic adapter / Corresponding Pydantic adapter.
    @param value 待编码值 / Value to encode.
    @return JSONB 可接受的 object / Object accepted by JSONB.
    """
    payload = adapter.dump_python(value, mode="json")
    if not isinstance(payload, dict):
        raise TypeError("platform persistence codec must produce an object")
    return cast(JsonObject, payload)


def _dump_array[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> list[JsonObject]:
    """@brief 编码领域值为 JSON object array / Encode a domain value as a JSON object array."""
    payload = adapter.dump_python(value, mode="json")
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise TypeError("platform persistence codec must produce an object array")
    return cast(list[JsonObject], payload)


def _load[ValueT](adapter: TypeAdapter[ValueT], payload: object, label: str) -> ValueT:
    """@brief 从不可信持久化 JSON 重建领域值 / Rebuild a domain value from untrusted JSON.

    @param adapter 对应 Pydantic adapter / Corresponding Pydantic adapter.
    @param payload 数据库解码值 / Database-decoded value.
    @param label 诊断标签 / Diagnostic label.
    @return 通过领域不变量的值 / Value satisfying domain invariants.
    @raise ValueError JSON 不满足领域模型时抛出 / Raised when JSON violates the domain model.
    """
    try:
        return adapter.validate_python(payload)
    except ValidationError as error:
        raise ValueError(f"persisted {label} violates the API V2 domain model") from error


def _affected_rows(result: Result[Any]) -> int:
    """@brief 返回 DML affected-row count / Return the DML affected-row count."""
    return cast(CursorResult[Any], result).rowcount


def _page_position(at: datetime, identifier: str) -> str:
    """@brief 编码稳定时间+ID keyset 位置 / Encode a stable timestamp-and-ID keyset position."""
    microseconds = int(at.timestamp() * 1_000_000)
    return f"{microseconds}:{identifier}"


def _parse_page_position(position: str | None) -> tuple[datetime, str] | None:
    """@brief 解码稳定时间+ID keyset 位置 / Decode a stable timestamp-and-ID keyset position."""
    if position is None:
        return None
    raw_microseconds, separator, identifier = position.partition(":")
    if not separator or not identifier:
        raise ValueError("platform page position is invalid")
    try:
        microseconds = int(raw_microseconds)
        instant = datetime.fromtimestamp(microseconds / 1_000_000, tz=UTC)
    except (OverflowError, ValueError) as error:
        raise ValueError("platform page position is invalid") from error
    return instant, identifier


class _V2ScopeInstaller(Protocol):
    """@brief 安装当前 transaction 的 V2 RLS scope / Install V2 RLS scope in the current transaction."""

    async def __call__(self, *, actor_id: str, workspace_id: str | None) -> None:
        """@brief 安装 actor/workspace scope / Install actor/workspace scope."""


class _JobAuthorizationLookup(Protocol):
    """@brief cancellation 授权所需最小 Job lookup / Minimal Job lookup needed by cancellation authorization."""

    async def __call__(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
    ) -> tuple[str, str] | None:
        """@brief 返回 ``(kind, subject_type)`` / Return ``(kind, subject_type)``."""


class _CancellationDomain(StrEnum):
    """@brief 统一 Job 取消时需要同步的闭合领域集合 / Closed domains synchronized by unified Job cancellation."""

    RESUME_IMPORT = "resume_import"
    """@brief 释放一次性导入 UploadSession / Release a one-shot import UploadSession."""

    JOB_ONLY = "job_only"
    """@brief 不存在额外活动态的纯 Job / Pure Job without extra active state."""

    CONNECTION_REVOKE = "connection_revoke"
    """@brief 恢复尚未开始的 Connection revoke / Recover an unstarted Connection revoke."""

    KNOWLEDGE_DELETE = "knowledge_delete"
    """@brief 恢复尚未开始的 Knowledge deletion / Recover an unstarted Knowledge deletion."""

    KNOWLEDGE_PROCESS = "knowledge_process"
    """@brief 补偿 Knowledge ingestion/sync / Compensate Knowledge ingestion or sync."""

    AGENT_RUN = "agent_run"
    """@brief 同步取消 AgentRun 与 pending approval / Cancel an AgentRun and pending approval."""

    INTERVIEW_END = "interview_end"
    """@brief 同步终止尚未 finalize 的 Interview Session / Terminate an unfinalized Interview Session."""

    INTERVIEW_REPORT = "interview_report"
    """@brief 丢弃尚未发布的 Report 计算 / Discard unpublished Report computation."""


@dataclass(frozen=True, slots=True)
class _CancellationPolicy:
    """@brief 一个精确 Job kind/subject 的授权与补偿策略 / Authorization and compensation policy for an exact Job kind/subject.

    @param action 领域级第二道 Workspace 授权 / Domain-level secondary Workspace authorization.
    @param domain 补偿处理器判别值 / Compensation-handler discriminator.
    """

    action: WorkspaceAction
    domain: _CancellationDomain


_CANCELLATION_POLICIES: Mapping[tuple[str, str], _CancellationPolicy] = {
    ("resume.import", "upload_session"): _CancellationPolicy(
        WorkspaceAction.CREATE_RESUME_IMPORT_JOB,
        _CancellationDomain.RESUME_IMPORT,
    ),
    ("resume.restore", "resume"): _CancellationPolicy(
        WorkspaceAction.CREATE_RESUME_RESTORE_JOB,
        _CancellationDomain.JOB_ONLY,
    ),
    ("resume.render", "resume"): _CancellationPolicy(
        WorkspaceAction.CREATE_RESUME_RENDER_JOB,
        _CancellationDomain.JOB_ONLY,
    ),
    ("connection.revoke", "connection"): _CancellationPolicy(
        WorkspaceAction.DELETE_CONNECTION,
        _CancellationDomain.CONNECTION_REVOKE,
    ),
    ("knowledge.delete", "knowledge_source"): _CancellationPolicy(
        WorkspaceAction.DELETE_KNOWLEDGE_SOURCE,
        _CancellationDomain.KNOWLEDGE_DELETE,
    ),
    ("knowledge.ingest", "knowledge_source"): _CancellationPolicy(
        WorkspaceAction.CREATE_KNOWLEDGE_JOB,
        _CancellationDomain.KNOWLEDGE_PROCESS,
    ),
    ("knowledge.sync", "knowledge_source"): _CancellationPolicy(
        WorkspaceAction.CREATE_KNOWLEDGE_JOB,
        _CancellationDomain.KNOWLEDGE_PROCESS,
    ),
    ("agent.run", "agent_run"): _CancellationPolicy(
        WorkspaceAction.CANCEL_AGENT_RUN,
        _CancellationDomain.AGENT_RUN,
    ),
    ("interview.end", "interview_session"): _CancellationPolicy(
        WorkspaceAction.END_INTERVIEW_SESSION,
        _CancellationDomain.INTERVIEW_END,
    ),
    ("interview.report", "interview_session"): _CancellationPolicy(
        WorkspaceAction.CREATE_INTERVIEW_REPORT_JOB,
        _CancellationDomain.INTERVIEW_REPORT,
    ),
}
"""@brief 只接受产品实际创建的 exact Job bindings / Accept only exact bindings created by the product."""


def _cancellation_policy(kind: str, subject_type: str) -> _CancellationPolicy:
    """@brief 解析 exact cancellation policy / Resolve an exact cancellation policy.

    @param kind 持久 Job kind / Persisted Job kind.
    @param subject_type 持久 subject type / Persisted subject type.
    @return 闭合授权与补偿策略 / Closed authorization and compensation policy.
    @raise AuthorizationDenied 未知或错配绑定时 fail closed / Fail closed for an unknown or
        mismatched binding.
    """
    try:
        return _CANCELLATION_POLICIES[(kind, subject_type)]
    except KeyError as error:
        raise AuthorizationDenied("authorization.job_domain_policy_missing") from error


def _cancellation_domain_action(kind: str, subject_type: str) -> WorkspaceAction:
    """@brief 将 Job kind/subject 映射到第二道领域授权 / Map Job kind/subject to a second domain authorization.

    @raise AuthorizationDenied 开放 Job kind 没有冻结领域策略时 fail-closed / Fail closed when
        an open Job kind lacks a frozen domain policy.
    """
    return _cancellation_policy(kind, subject_type).action


class _TrackingPlatformAuthorizer:
    """@brief 复用集中 authorizer 并密封精确 Platform proof / Reuse central authorization and seal exact Platform proofs."""

    def __init__(
        self,
        delegate: AccessAuthorizer,
        job_lookup: _JobAuthorizationLookup,
        scope_installer: _V2ScopeInstaller | None = None,
    ) -> None:
        """@brief 绑定集中授权、Job policy lookup 与 scope installer / Bind authorization, lookup, and scope installer."""
        self._delegate = delegate
        self._job_lookup = job_lookup
        self._scope_installer = scope_installer
        self._actor: AuthenticatedActor | None = None
        self._workspace_id: WorkspaceId | None = None
        self._proofs: dict[int, PlatformAuthorizationRequest] = {}

    async def authenticate(self, principal: TokenPrincipal) -> AuthenticatedActor:
        """@brief 认证并固定 UoW actor / Authenticate and pin the UoW actor."""
        actor = await self._delegate.authenticate(principal)
        if self._actor is not None and self._actor != actor:
            raise PermissionError("a Platform unit of work cannot switch actors")
        self._actor = actor
        if self._scope_installer is not None:
            await self._scope_installer(actor_id=str(actor.user_id), workspace_id=None)
        return actor

    async def authorize(
        self,
        actor: AuthenticatedActor,
        workspace_id: WorkspaceId,
        request: PlatformAuthorizationRequest,
    ) -> WorkspaceAccessContext:
        """@brief 产生精确 permission proof，并为 cancellation 叠加领域授权 / Issue an exact proof with domain cancellation authorization."""
        if self._actor != actor:
            raise PermissionError("Platform authorization requires the authenticated UoW actor")
        if self._workspace_id is not None and self._workspace_id != workspace_id:
            raise PermissionError("a Platform unit of work cannot switch workspaces")
        action = _PERMISSION_ACTION[request.permission]
        access = await self._delegate.authorize(actor, workspace_id, action)
        self._workspace_id = workspace_id
        if self._scope_installer is not None:
            await self._scope_installer(
                actor_id=str(actor.user_id),
                workspace_id=str(workspace_id),
            )
        if request.permission is PlatformPermission.CANCEL_JOB:
            target = request.target
            if target is None:
                raise PermissionError("cancellation proof requires a Job target")
            descriptor = await self._job_lookup(workspace_id, JobId(target.id))
            if descriptor is not None:
                domain_action = _cancellation_domain_action(*descriptor)
                access = await self._delegate.authorize(actor, workspace_id, domain_action)
        self._proofs[id(access)] = request
        return access

    def require(
        self,
        access: WorkspaceAccessContext,
        permission: PlatformPermission,
        *,
        target: PlatformResourceTarget | None = None,
    ) -> None:
        """@brief 验证 repository 收到的是本 UoW 精确 proof / Verify an exact proof issued by this UoW."""
        request = self._proofs.get(id(access))
        if (
            request is None
            or request.permission is not permission
            or request.target != target
            or access.workspace_id != self._workspace_id
        ):
            raise PermissionError("Platform repository received an invalid authorization proof")

    def request_for(self, access: WorkspaceAccessContext) -> PlatformAuthorizationRequest:
        """@brief 返回当前 UoW 签发的精确 request / Return the exact request issued by this UoW."""
        request = self._proofs.get(id(access))
        if request is None or access.workspace_id != self._workspace_id:
            raise PermissionError("Platform repository received an unknown authorization proof")
        return request


@dataclass(frozen=True, slots=True)
class _StoredArtifactContent:
    """@brief 内存对象存储 metadata 与 bytes / In-memory object-storage metadata and bytes."""

    storage_key: str
    media_type: str
    size_bytes: int
    sha256: str
    content: bytes


class InMemoryPlatformStore:
    """@brief Platform adapters 共享的进程内状态 / Shared in-process state for Platform adapters."""

    def __init__(self, *, lock: asyncio.Lock | None = None) -> None:
        """@brief 初始化空状态与统一互斥锁 / Initialize empty state and one shared mutex."""
        self.lock = lock or asyncio.Lock()
        self.condition = asyncio.Condition(self.lock)
        self.jobs: dict[JobId, Job] = {}
        self.artifacts: dict[ArtifactId, Artifact] = {}
        self.contents: dict[ArtifactId, _StoredArtifactContent] = {}
        self.source_maps: dict[ArtifactId, PdfSourceMap] = {}
        self.audit_events: dict[AuditEventId, AuditEvent] = {}
        self.events: dict[WorkspaceId, list[tuple[ApiEvent, datetime]]] = {}
        self.last_sequences: dict[WorkspaceId, int] = {}

    def seed_job(self, job: Job) -> None:
        """@brief 在并发运行前加入已验证 Job / Seed a validated Job before concurrent use."""
        self.jobs[job.meta.id] = job

    def seed_artifact(
        self,
        artifact: Artifact,
        content: bytes,
        *,
        source_map: PdfSourceMap | None = None,
        storage_key: str | None = None,
    ) -> None:
        """@brief 在并发运行前原子加入 Artifact metadata/content/source-map / Seed an Artifact aggregate before concurrent use."""
        digest = sha256(content).hexdigest()
        if len(content) != artifact.size_bytes or digest != artifact.sha256:
            raise ValueError("seed Artifact content does not match metadata")
        if source_map is not None:
            source_map.validate_for(artifact)
        self.artifacts[artifact.meta.id] = artifact
        self.contents[artifact.meta.id] = _StoredArtifactContent(
            storage_key or f"memory://artifacts/{artifact.meta.id}",
            artifact.media_type,
            len(content),
            digest,
            bytes(content),
        )
        if source_map is not None:
            self.source_maps[artifact.meta.id] = source_map

    def seed_audit_event(self, event: AuditEvent) -> None:
        """@brief 在并发运行前加入 AuditEvent / Seed an AuditEvent before concurrent use."""
        self.audit_events[event.id] = event

    def seed_event(
        self,
        workspace_id: WorkspaceId,
        event: ApiEvent,
        *,
        replay_expires_at: datetime,
    ) -> None:
        """@brief 在并发运行前加入有 replay window 的 ApiEvent / Seed an ApiEvent with a replay window."""
        if replay_expires_at <= event.occurred_at:
            raise ValueError("event replay expiration must follow occurrence")
        current = self.last_sequences.get(workspace_id, 0)
        if event.sequence <= current:
            raise ValueError("seed event sequence must strictly increase")
        self.events.setdefault(workspace_id, []).append((event, replay_expires_at))
        self.last_sequences[workspace_id] = event.sequence


class _MemoryPlatformRepository:
    """@brief 绑定 copy-on-write snapshot 的 Platform repository / Platform repository bound to a copy-on-write snapshot."""

    def __init__(
        self,
        jobs: dict[JobId, Job],
        artifacts: dict[ArtifactId, Artifact],
        source_maps: dict[ArtifactId, PdfSourceMap],
        audits: dict[AuditEventId, AuditEvent],
        authorizer: _TrackingPlatformAuthorizer,
    ) -> None:
        """@brief 绑定 snapshot 与 proof verifier / Bind snapshot and proof verifier."""
        self._jobs = jobs
        self._artifacts = artifacts
        self._source_maps = source_maps
        self._audits = audits
        self._authorizer = authorizer

    async def list_jobs(
        self,
        access: WorkspaceAccessContext,
        query: JobQuery,
        page: PageRequest,
    ) -> CollectionPage[Job]:
        """@brief 以稳定 keyset 列出 Job / List Jobs with a stable keyset."""
        self._authorizer.require(access, PlatformPermission.LIST_JOBS)
        selected = [
            job
            for job in self._jobs.values()
            if job.workspace_id == access.workspace_id
            and (query.kind is None or job.kind == query.kind)
            and (
                query.subject.subject_type is None
                or job.subject.resource_type == query.subject.subject_type
            )
            and (
                query.subject.subject_id is None
                or job.subject.id == query.subject.subject_id
            )
        ]
        selected.sort(key=lambda item: (item.meta.created_at, str(item.meta.id)), reverse=True)
        after = _parse_page_position(page.after)
        if after is not None:
            selected = [
                item
                for item in selected
                if (item.meta.created_at, str(item.meta.id)) < after
            ]
        return _memory_page(selected, page)

    async def get_job(
        self,
        access: WorkspaceAccessContext,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 在 proof Workspace 读取 Job / Read a Job in the proof Workspace."""
        del for_update
        permission = (
            PlatformPermission.CANCEL_JOB
            if self._authorizer_request_is(access, PlatformPermission.CANCEL_JOB)
            else PlatformPermission.READ_JOB
        )
        self._authorizer.require(
            access,
            permission,
            target=PlatformResourceTarget(PlatformTargetKind.JOB, job_id),
        )
        item = self._jobs.get(job_id)
        return item if item is not None and item.workspace_id == access.workspace_id else None

    async def save_job(
        self,
        access: WorkspaceAccessContext,
        job: Job,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 使用 revision CAS 保存 Job / Save a Job using revision CAS."""
        self._authorizer.require(
            access,
            PlatformPermission.CANCEL_JOB,
            target=PlatformResourceTarget(PlatformTargetKind.JOB, job.meta.id),
        )
        current = self._jobs.get(job.meta.id)
        if (
            current is None
            or current.workspace_id != access.workspace_id
            or current.meta.revision != expected_revision
            or job.meta.revision != expected_revision + 1
        ):
            raise JobCasMismatch
        self._jobs[job.meta.id] = job

    async def synchronize_cancellation(
        self,
        access: WorkspaceAccessContext,
        job: Job,
        *,
        at: datetime,
    ) -> None:
        """@brief 校验内存 Job 的闭合取消策略 / Validate the closed cancellation policy for an in-memory Job.

        @note 进程内 Platform store 只保存统一投影；领域内存 stores 不共享 transaction，生产
            完整补偿由 PostgreSQL adapter 提供。/ The process-local Platform store contains only
            unified projections; domain memory stores do not share a transaction, while the
            PostgreSQL adapter provides complete production compensation.
        """
        del at
        self._authorizer.require(
            access,
            PlatformPermission.CANCEL_JOB,
            target=PlatformResourceTarget(PlatformTargetKind.JOB, job.meta.id),
        )
        if job.workspace_id != access.workspace_id:
            raise JobCasMismatch
        _cancellation_policy(job.kind, job.subject.resource_type)

    async def list_artifacts(
        self,
        access: WorkspaceAccessContext,
        query: ArtifactQuery,
        page: PageRequest,
    ) -> CollectionPage[Artifact]:
        """@brief 以稳定 keyset 列出 Artifact / List Artifacts with a stable keyset."""
        self._authorizer.require(access, PlatformPermission.LIST_ARTIFACTS)
        selected = [
            artifact
            for artifact in self._artifacts.values()
            if artifact.workspace_id == access.workspace_id
            and (query.kind is None or artifact.kind is query.kind)
            and (
                query.subject.subject_type is None
                or artifact.subject.resource_type == query.subject.subject_type
            )
            and (
                query.subject.subject_id is None
                or artifact.subject.id == query.subject.subject_id
            )
        ]
        selected.sort(key=lambda item: (item.meta.created_at, str(item.meta.id)), reverse=True)
        after = _parse_page_position(page.after)
        if after is not None:
            selected = [
                item
                for item in selected
                if (item.meta.created_at, str(item.meta.id)) < after
            ]
        return _memory_page(selected, page)

    async def get_artifact(
        self,
        access: WorkspaceAccessContext,
        artifact_id: ArtifactId,
    ) -> Artifact | None:
        """@brief 在 proof Workspace 读取 Artifact / Read an Artifact in the proof Workspace."""
        permission = self._artifact_permission(access)
        self._authorizer.require(
            access,
            permission,
            target=PlatformResourceTarget(PlatformTargetKind.ARTIFACT, artifact_id),
        )
        item = self._artifacts.get(artifact_id)
        return item if item is not None and item.workspace_id == access.workspace_id else None

    async def get_pdf_source_map(
        self,
        access: WorkspaceAccessContext,
        artifact_id: ArtifactId,
    ) -> PdfSourceMap | None:
        """@brief 读取 Artifact source map / Read an Artifact source map."""
        self._authorizer.require(
            access,
            PlatformPermission.READ_ARTIFACT_SOURCE_MAP,
            target=PlatformResourceTarget(PlatformTargetKind.ARTIFACT, artifact_id),
        )
        artifact = self._artifacts.get(artifact_id)
        if artifact is None or artifact.workspace_id != access.workspace_id:
            return None
        return self._source_maps.get(artifact_id)

    async def list_audit_events(
        self,
        access: WorkspaceAccessContext,
        page: PageRequest,
    ) -> CollectionPage[AuditEvent]:
        """@brief 以稳定 keyset 列出 append-only audit events / List append-only audit events with a stable keyset."""
        self._authorizer.require(access, PlatformPermission.LIST_AUDIT_EVENTS)
        selected = [
            event
            for event in self._audits.values()
            if event.workspace_id == access.workspace_id
        ]
        selected.sort(key=lambda item: (item.occurred_at, str(item.id)), reverse=True)
        after = _parse_page_position(page.after)
        if after is not None:
            selected = [
                item for item in selected if (item.occurred_at, str(item.id)) < after
            ]
        return _memory_page(selected, page)

    def _authorizer_request_is(
        self,
        access: WorkspaceAccessContext,
        permission: PlatformPermission,
    ) -> bool:
        """@brief 无异常探测 proof permission / Probe a proof permission without leaking errors."""
        try:
            return self._authorizer.request_for(access).permission is permission
        except PermissionError:
            return False

    def _artifact_permission(self, access: WorkspaceAccessContext) -> PlatformPermission:
        """@brief 返回单 Artifact proof 的精确 permission / Return the exact single-Artifact permission."""
        request = self._authorizer.request_for(access)
        if request.permission not in {
            PlatformPermission.READ_ARTIFACT,
            PlatformPermission.READ_ARTIFACT_CONTENT,
            PlatformPermission.READ_ARTIFACT_SOURCE_MAP,
        }:
            raise PermissionError("Artifact read requires an item proof")
        return request.permission


def _memory_page[ItemT: Job | Artifact | AuditEvent](
    selected: list[ItemT],
    page: PageRequest,
) -> CollectionPage[ItemT]:
    """@brief 从已排序内存结果构建 keyset 页面 / Build a keyset page from sorted memory results."""
    items = tuple(selected[: page.limit])
    has_more = len(selected) > page.limit
    next_position: str | None = None
    if has_more and items:
        last = items[-1]
        if isinstance(last, (Job, Artifact)):
            next_position = _page_position(last.meta.created_at, str(last.meta.id))
        else:
            next_position = _page_position(last.occurred_at, str(last.id))
    return CollectionPage(items, next_position)


class _MemoryPlatformJournal:
    """@brief 与内存 Job CAS 共 snapshot 的 event/audit journal / Event and audit journal sharing the Job snapshot."""

    def __init__(
        self,
        audits: dict[AuditEventId, AuditEvent],
        events: dict[WorkspaceId, list[tuple[ApiEvent, datetime]]],
        last_sequences: dict[WorkspaceId, int],
        authorizer: _TrackingPlatformAuthorizer,
        retention: timedelta,
    ) -> None:
        """@brief 绑定 journal snapshot / Bind the journal snapshot."""
        self._audits = audits
        self._events = events
        self._last_sequences = last_sequences
        self._authorizer = authorizer
        self._retention = retention

    async def job_cancelled(
        self,
        access: WorkspaceAccessContext,
        before: Job,
        after: Job,
        context: MutationContext,
    ) -> None:
        """@brief 原子追加 job.updated 与 job.cancel / Atomically append job.updated and job.cancel."""
        target = PlatformResourceTarget(PlatformTargetKind.JOB, before.meta.id)
        self._authorizer.require(access, PlatformPermission.CANCEL_JOB, target=target)
        if (
            before.workspace_id != access.workspace_id
            or after.workspace_id != access.workspace_id
            or after.meta.revision != before.meta.revision + 1
        ):
            raise ValueError("Job cancellation journal received inconsistent aggregates")
        sequence = self._last_sequences.get(access.workspace_id, 0) + 1
        self._last_sequences[access.workspace_id] = sequence
        event = ApiEvent(
            ApiEventId(new_opaque_id("evt")),
            sequence,
            "job.updated",
            after.meta.updated_at,
            ResourceRef("job", str(after.meta.id), after.meta.revision),
            {"status": after.status.value},
            context.trace_id,
        )
        self._events.setdefault(access.workspace_id, []).append(
            (event, event.occurred_at + self._retention)
        )
        audit = AuditEvent(
            AuditEventId(new_opaque_id("audit")),
            access.workspace_id,
            after.meta.updated_at,
            ResourceRef("user", str(access.actor.user_id)),
            "job.cancel",
            ResourceRef("job", str(after.meta.id), after.meta.revision),
            AuditOutcome.ALLOWED,
            context.request_id,
        )
        self._audits[audit.id] = audit


class InMemoryPlatformUnitOfWork:
    """@brief Platform state 的 copy-on-write UoW / Copy-on-write UoW for Platform state."""

    def __init__(
        self,
        store: InMemoryPlatformStore,
        access_store: InMemoryAccessStore,
        *,
        retention: timedelta,
    ) -> None:
        """@brief 绑定共享 stores / Bind shared stores."""
        self._store = store
        self._access_store = access_store
        self._retention = retention
        self._entered = False
        self._committed = False
        self._rolled_back = False
        self._authorizer: _TrackingPlatformAuthorizer | None = None
        self._repository: _MemoryPlatformRepository | None = None
        self._journal: _MemoryPlatformJournal | None = None
        self._snapshot: tuple[
            dict[JobId, Job],
            dict[ArtifactId, Artifact],
            dict[ArtifactId, _StoredArtifactContent],
            dict[ArtifactId, PdfSourceMap],
            dict[AuditEventId, AuditEvent],
            dict[WorkspaceId, list[tuple[ApiEvent, datetime]]],
            dict[WorkspaceId, int],
        ] | None = None

    @property
    def authorizer(self) -> _TrackingPlatformAuthorizer:
        """@brief 返回 transaction-bound authorizer / Return the transaction-bound authorizer."""
        if self._authorizer is None:
            raise RuntimeError("Platform unit of work has not been entered")
        return self._authorizer

    @property
    def repository(self) -> _MemoryPlatformRepository:
        """@brief 返回 transaction-bound repository / Return the transaction-bound repository."""
        if self._repository is None:
            raise RuntimeError("Platform unit of work has not been entered")
        return self._repository

    @property
    def journal(self) -> _MemoryPlatformJournal:
        """@brief 返回 transaction-bound journal / Return the transaction-bound journal."""
        if self._journal is None:
            raise RuntimeError("Platform unit of work has not been entered")
        return self._journal

    async def __aenter__(self) -> Self:
        """@brief 依固定 Access→Platform 顺序锁定并复制状态 / Lock in Access-to-Platform order and copy state."""
        if self._entered:
            raise RuntimeError("Platform unit of work cannot be re-entered")
        await self._access_store.lock.acquire()
        try:
            await self._store.lock.acquire()
        except BaseException:
            self._access_store.lock.release()
            raise
        self._entered = True
        jobs = dict(self._store.jobs)
        artifacts = dict(self._store.artifacts)
        contents = dict(self._store.contents)
        source_maps = dict(self._store.source_maps)
        audits = dict(self._store.audit_events)
        events = {workspace: list(items) for workspace, items in self._store.events.items()}
        sequences = dict(self._store.last_sequences)
        self._snapshot = (
            jobs,
            artifacts,
            contents,
            source_maps,
            audits,
            events,
            sequences,
        )
        access_repository = InMemoryAccessRepository(
            users=dict(self._access_store.users),
            workspaces=dict(self._access_store.workspaces),
            memberships=dict(self._access_store.memberships),
            invitations=dict(self._access_store.invitations),
            account_deletions=dict(self._access_store.account_deletions),
        )

        async def lookup(
            workspace_id: WorkspaceId,
            job_id: JobId,
        ) -> tuple[str, str] | None:
            """@brief 在已锁 snapshot 中查 cancellation policy / Look up cancellation policy in the locked snapshot."""
            job = jobs.get(job_id)
            if job is None or job.workspace_id != workspace_id:
                return None
            return job.kind, job.subject.resource_type

        self._authorizer = _TrackingPlatformAuthorizer(
            AccessAuthorizer(access_repository),
            lookup,
        )
        self._repository = _MemoryPlatformRepository(
            jobs,
            artifacts,
            source_maps,
            audits,
            self._authorizer,
        )
        self._journal = _MemoryPlatformJournal(
            audits,
            events,
            sequences,
            self._authorizer,
            self._retention,
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时丢弃 snapshot 并释放锁 / Discard uncommitted state and release locks."""
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
        """@brief 原子发布 Job、event 与 audit snapshot / Atomically publish Job, event, and audit state."""
        if self._snapshot is None or not self._entered:
            raise RuntimeError("Platform unit of work has not been entered")
        if self._committed:
            raise RuntimeError("Platform unit of work is already committed")
        if self._rolled_back:
            raise RuntimeError("rolled-back Platform unit of work cannot commit")
        (
            self._store.jobs,
            self._store.artifacts,
            self._store.contents,
            self._store.source_maps,
            self._store.audit_events,
            self._store.events,
            self._store.last_sequences,
        ) = self._snapshot
        self._committed = True
        self._store.condition.notify_all()

    async def rollback(self) -> None:
        """@brief 幂等丢弃未发布 snapshot / Idempotently discard the unpublished snapshot."""
        if not self._entered:
            raise RuntimeError("Platform unit of work has not been entered")
        self._rolled_back = True

    def _clear(self) -> None:
        """@brief 清除 transaction-bound adapters / Clear transaction-bound adapters."""
        self._snapshot = None
        self._authorizer = None
        self._repository = None
        self._journal = None


class _SystemClock:
    """@brief Event replay retention 使用的 UTC 时钟 / UTC clock used for event replay retention."""

    def now(self) -> datetime:
        """@brief 返回当前 UTC 时刻 / Return the current UTC instant."""
        return datetime.now(UTC)


class InMemoryArtifactContentStore:
    """@brief 验证完整对象后提供单 Range 的内存 content store / In-memory content store validating whole objects before ranges."""

    def __init__(self, store: InMemoryPlatformStore, *, chunk_size: int = _CONTENT_CHUNK_SIZE) -> None:
        """@brief 绑定共享 store 与 chunk 大小 / Bind shared store and chunk size."""
        if chunk_size < 1:
            raise ValueError("Artifact chunk size must be positive")
        self._store = store
        self._chunk_size = chunk_size

    async def open(
        self,
        access: WorkspaceAccessContext,
        artifact: Artifact,
        selected_range: ContentRange | None,
    ) -> ArtifactContentStream:
        """@brief 交叉验证 metadata、完整 SHA-256 与 Range / Cross-check metadata, whole SHA-256, and Range."""
        if access.action is not WorkspaceAction.READ_ARTIFACT_CONTENT:
            raise PermissionError("Artifact content requires its exact access proof")
        if access.workspace_id != artifact.workspace_id:
            raise PermissionError("Artifact content proof crossed a Workspace boundary")
        async with self._store.lock:
            stored = self._store.contents.get(artifact.meta.id)
        if stored is None:
            raise FileNotFoundError(str(artifact.meta.id))
        digest = sha256(stored.content).hexdigest()
        if (
            stored.media_type != artifact.media_type
            or stored.size_bytes != artifact.size_bytes
            or stored.size_bytes != len(stored.content)
            or stored.sha256 != artifact.sha256
            or digest != artifact.sha256
        ):
            raise ValueError("Artifact object metadata or digest is inconsistent")
        content = stored.content
        if selected_range is not None:
            content = content[selected_range.first : selected_range.last_inclusive + 1]
        return ArtifactContentStream(
            _byte_chunks(content, self._chunk_size),
            stored.media_type,
            stored.size_bytes,
            stored.sha256,
            selected_range,
        )


class InMemoryPlatformEventFeed:
    """@brief 无 replay/live 缺口的内存 event feed / In-memory event feed without a replay/live gap."""

    def __init__(
        self,
        store: InMemoryPlatformStore,
        *,
        clock: Clock | None = None,
    ) -> None:
        """@brief 绑定共享 store 与 retention clock / Bind shared store and retention clock."""
        self._store = store
        self._clock = clock or _SystemClock()

    async def open(
        self,
        access: WorkspaceAccessContext,
        replay: EventReplayRequest,
    ) -> AsyncIterator[ApiEvent]:
        """@brief 在持锁点验证 replay 并返回连续 live tail / Validate replay under lock and return a continuous live tail."""
        if access.action is not WorkspaceAction.READ_EVENTS:
            raise PermissionError("Event feed requires its exact access proof")
        def initial_cursor() -> int:
            """@brief 在当前锁保护下验证 replay / Validate replay under the currently held lock."""
            rows = self._store.events.get(access.workspace_id, [])
            if replay.after_event_id is None:
                return self._store.last_sequences.get(access.workspace_id, 0)
            matched = next(
                (
                    item
                    for item in rows
                    if item[0].event_id == replay.after_event_id
                    and item[1] > self._clock.now()
                ),
                None,
            )
            if matched is None:
                raise EventReplayWindowExpired(replay.after_event_id)
            return matched[0].sequence

        async with self._store.lock:
            cursor = initial_cursor()

        async def stream() -> AsyncIterator[ApiEvent]:
            """@brief 按 sequence 轮询共享 committed state / Poll committed shared state by sequence."""
            nonlocal cursor
            while True:
                async with self._store.condition:
                    available = [
                        event
                        for event, expires_at in self._store.events.get(access.workspace_id, [])
                        if event.sequence > cursor and expires_at > self._clock.now()
                    ]
                    if not available:
                        await self._store.condition.wait()
                        continue
                for event in available:
                    cursor = event.sequence
                    yield event

        return stream()


class InMemoryPlatformUnitOfWorkFactory:
    """@brief 组装共享状态的内存 Platform adapters / Assemble in-memory Platform adapters over shared state."""

    def __init__(
        self,
        access_store: InMemoryAccessStore,
        *,
        store: InMemoryPlatformStore | None = None,
        retention: timedelta = _EVENT_RETENTION,
        clock: Clock | None = None,
    ) -> None:
        """@brief 绑定 Access/Platform stores，并公开 content/event adapters / Bind stores and expose content/event adapters."""
        if retention <= timedelta(0):
            raise ValueError("event retention must be positive")
        self.access_store = access_store
        self.store = store or InMemoryPlatformStore()
        self.content_store = InMemoryArtifactContentStore(self.store)
        self.event_feed = InMemoryPlatformEventFeed(self.store, clock=clock)
        self._retention = retention

    def __call__(self) -> InMemoryPlatformUnitOfWork:
        """@brief 创建未进入的内存 UoW / Create a not-yet-entered in-memory UoW."""
        return InMemoryPlatformUnitOfWork(
            self.store,
            self.access_store,
            retention=self._retention,
        )


async def _byte_chunks(content: bytes, chunk_size: int) -> AsyncIterator[bytes]:
    """@brief 将不可变 bytes 分块返回 / Yield immutable bytes in chunks."""
    for offset in range(0, len(content), chunk_size):
        yield content[offset : offset + chunk_size]


def _job_from_record(record: JobRecord) -> Job:
    """@brief 从统一 Job row 重建领域 Job / Rebuild a domain Job from a unified Job row."""
    result_refs = _load(
        _RESOURCE_REFS_ADAPTER,
        record.result_refs,
        "Job result_refs",
    )
    problem = (
        _load(_PROBLEM_ADAPTER, record.problem, "Job problem")
        if record.problem is not None
        else None
    )
    progress = JobProgress(
        record.phase,
        record.completed_units,
        record.total_units,
        JobProgressUnit(record.progress_unit),
    )
    return Job(
        ResourceMeta(
            JobId(record.id),
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        WorkspaceId(record.workspace_id),
        record.job_type,
        ResourceRef(
            record.target_resource_type,
            record.target_resource_id,
            record.target_resource_revision,
        ),
        JobStatus(record.status),
        progress,
        result_refs,
        problem,
        record.started_at,
        record.finished_at,
    )


def _artifact_from_record(record: ArtifactRecord, api_origin: str) -> Artifact:
    """@brief 从唯一 metadata row 重建同源 Artifact / Rebuild a same-origin Artifact from the sole metadata row."""
    artifact_id = ArtifactId(record.id)
    workspace_id = WorkspaceId(record.workspace_id)
    return Artifact(
        ResourceMeta(
            artifact_id,
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        workspace_id,
        ArtifactKind(record.kind),
        ResourceRef(
            record.subject_type,
            record.subject_id,
            record.subject_revision,
        ),
        record.media_type,
        record.size_bytes,
        record.sha256,
        ApiArtifactContentUrl.build(api_origin, workspace_id, artifact_id),
        record.page_count,
        record.expires_at,
    )


def _source_map_from_record(record: ArtifactPdfSourceMapRecord) -> PdfSourceMap:
    """@brief 从规范 JSONB 重建 PDF source map / Rebuild a PDF source map from canonical JSONB."""
    payload: JsonObject = {
        "artifact_id": record.artifact_id,
        "resume_id": record.resume_id,
        "resume_revision": record.resume_revision,
        "nodes": record.nodes,
    }
    return _load(_SOURCE_MAP_ADAPTER, payload, "PDF source map")


def _audit_from_record(record: AuditEventRecord) -> AuditEvent:
    """@brief 从 append-only row 重建 AuditEvent / Rebuild an AuditEvent from an append-only row."""
    return AuditEvent(
        AuditEventId(record.id),
        WorkspaceId(record.workspace_id),
        record.occurred_at,
        ResourceRef(record.actor_type, record.actor_id, record.actor_revision),
        record.action,
        ResourceRef(
            record.resource_type,
            record.resource_id,
            record.resource_revision,
        ),
        AuditOutcome(record.outcome),
        record.request_id,
    )


def _event_data(payload: JsonObject) -> Mapping[str, Any]:
    """@brief 兼容 outbox envelope 并返回契约 data object / Return contract data from an outbox envelope."""
    nested = payload.get("data")
    if isinstance(nested, dict):
        return nested
    return payload


def _job_spec(record: JobRecord) -> Mapping[str, Any]:
    """@brief 从不可信 request_payload 提取 typed Job spec object / Extract the typed Job spec object from untrusted request_payload.

    @param record 持久统一 Job / Persisted unified Job.
    @return 只读形态的 spec mapping / Spec mapping treated as read-only.
    @raise JobCasMismatch payload 缺失或不是对象时抛出 / Raised when the payload is absent or
        not an object.
    """
    payload = record.request_payload
    if not isinstance(payload, Mapping):
        raise JobCasMismatch
    spec = payload.get("spec")
    if not isinstance(spec, Mapping):
        raise JobCasMismatch
    return spec


def _knowledge_recovery_state(
    spec: Mapping[str, Any],
    source: KnowledgeSourceRecord,
) -> tuple[str, JsonObject | None]:
    """@brief 解析 Knowledge Job 排队前的稳定恢复点 / Resolve the stable pre-queue recovery point for a Knowledge Job.

    @param spec 持久 typed Job spec / Persisted typed Job spec.
    @param source 当前锁定 Source row / Currently locked Source row.
    @return ``(ingestion_state, last_problem)`` 补偿值 / Compensation values.
    @raise JobCasMismatch 显式快照损坏时抛出 / Raised for a corrupt explicit snapshot.
    @note 0019 之前的活动 Job 没有补偿字段；有历史成功版本时保守恢复 stale，否则恢复
        not_started，绝不猜测 ready。/ Active Jobs predating migration 0019 lack compensation
        fields; they conservatively recover to stale when a successful version exists, otherwise
        to not_started, and never guess ready.
    """
    raw_status = spec.get("previous_ingestion_status")
    if raw_status is None:
        status = (
            "stale"
            if source.current_version_id is not None and source.last_success_at is not None
            else "not_started"
        )
        return status, None
    if raw_status not in {"not_started", "ready", "stale", "failed"}:
        raise JobCasMismatch
    raw_problem = spec.get("previous_problem")
    if raw_status == "failed":
        if not isinstance(raw_problem, Mapping):
            raise JobCasMismatch
        return raw_status, cast(JsonObject, dict(raw_problem))
    if raw_problem is not None:
        raise JobCasMismatch
    return raw_status, None


def _domain_revision_at(requested: datetime, current: datetime) -> datetime:
    """@brief 为独立领域 revision 生成严格单调时间 / Produce a strictly monotonic timestamp for an independent domain revision.

    @param requested Job 取消时刻 / Job cancellation instant.
    @param current 领域 row 当前更新时间 / Domain row's current update time.
    @return 不早于 requested 且严格晚于 current 的时刻 / Instant no earlier than requested and
        strictly later than current.
    @note Job 与领域资源 revision 独立；固定测试时钟或轻微 clock skew 不应触发 ORM
        ``onupdate=now()`` 回退到数据库墙钟。/ Job and domain resource revisions are independent;
        a fixed test clock or small clock skew must not fall through to the ORM database-wall-clock
        onupdate default.
    """
    return max(requested, current + timedelta(microseconds=1))


def _event_from_record(record: OutboxEventRecord) -> ApiEvent:
    """@brief 从 committed outbox row 重建 ApiEvent / Rebuild an ApiEvent from a committed outbox row."""
    return ApiEvent(
        ApiEventId(record.id),
        record.sequence,
        record.event_type,
        record.occurred_at,
        ResourceRef(
            record.aggregate_type,
            record.aggregate_id,
            record.subject_revision,
        ),
        _event_data(record.payload),
        record.trace_id,
    )


class _PostgresPlatformRepository:
    """@brief 绑定一个 PostgreSQL transaction 的 Platform repository / Platform repository bound to one PostgreSQL transaction."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingPlatformAuthorizer,
        api_origin: str,
    ) -> None:
        """@brief 绑定 Session、proof verifier 与 API Origin / Bind Session, proof verifier, and API origin."""
        self._session = session
        self._authorizer = authorizer
        self._api_origin = api_origin

    async def list_jobs(
        self,
        access: WorkspaceAccessContext,
        query: JobQuery,
        page: PageRequest,
    ) -> CollectionPage[Job]:
        """@brief 以 ``created_at,id`` keyset 列出 Job / List Jobs using a ``created_at,id`` keyset."""
        self._authorizer.require(access, PlatformPermission.LIST_JOBS)
        statement = select(JobRecord).where(
            JobRecord.workspace_id == str(access.workspace_id)
        )
        if query.kind is not None:
            statement = statement.where(JobRecord.job_type == query.kind)
        if query.subject.subject_type is not None:
            statement = statement.where(
                JobRecord.target_resource_type == query.subject.subject_type
            )
        if query.subject.subject_id is not None:
            statement = statement.where(
                JobRecord.target_resource_id == query.subject.subject_id
            )
        after = _parse_page_position(page.after)
        if after is not None:
            after_at, after_id = after
            statement = statement.where(
                or_(
                    JobRecord.created_at < after_at,
                    and_(
                        JobRecord.created_at == after_at,
                        JobRecord.id < after_id,
                    ),
                )
            )
        rows = (
            await self._session.scalars(
                statement.order_by(JobRecord.created_at.desc(), JobRecord.id.desc()).limit(
                    page.limit + 1
                )
            )
        ).all()
        items = tuple(_job_from_record(row) for row in rows[: page.limit])
        next_position = (
            _page_position(items[-1].meta.created_at, str(items[-1].meta.id))
            if len(rows) > page.limit and items
            else None
        )
        return CollectionPage(items, next_position)

    async def get_job(
        self,
        access: WorkspaceAccessContext,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 在 proof Workspace 读取或锁定 Job / Read or lock a Job in the proof Workspace."""
        request = self._authorizer.request_for(access)
        permission = request.permission
        if permission not in {PlatformPermission.READ_JOB, PlatformPermission.CANCEL_JOB}:
            raise PermissionError("Job item read requires its exact proof")
        self._authorizer.require(
            access,
            permission,
            target=PlatformResourceTarget(PlatformTargetKind.JOB, job_id),
        )
        statement = select(JobRecord).where(
            JobRecord.workspace_id == str(access.workspace_id),
            JobRecord.id == str(job_id),
        )
        if for_update:
            preliminary = await self._session.scalar(statement)
            if preliminary is None:
                return None
            policy = _cancellation_policy(
                preliminary.job_type,
                preliminary.target_resource_type,
            )
            if policy.domain in {
                _CancellationDomain.AGENT_RUN,
                _CancellationDomain.INTERVIEW_END,
                _CancellationDomain.INTERVIEW_REPORT,
            }:
                await self._lock_domain_before_job(preliminary, policy)
            statement = statement.with_for_update()
        row = await self._session.scalar(statement)
        return _job_from_record(row) if row is not None else None

    async def save_job(
        self,
        access: WorkspaceAccessContext,
        job: Job,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 用 affected-row CAS 保存 Job state / Save Job state using an affected-row CAS."""
        self._authorizer.require(
            access,
            PlatformPermission.CANCEL_JOB,
            target=PlatformResourceTarget(PlatformTargetKind.JOB, job.meta.id),
        )
        if job.workspace_id != access.workspace_id or job.meta.revision != expected_revision + 1:
            raise JobCasMismatch
        result = await self._session.execute(
            update(JobRecord)
            .where(
                JobRecord.workspace_id == str(access.workspace_id),
                JobRecord.id == str(job.meta.id),
                JobRecord.revision == expected_revision,
            )
            .values(
                status=job.status.value,
                phase=job.progress.phase if job.progress is not None else "unknown",
                completed_units=job.progress.completed if job.progress is not None else 0,
                total_units=job.progress.total if job.progress is not None else None,
                progress_unit=(
                    job.progress.unit.value
                    if job.progress is not None
                    else JobProgressUnit.UNKNOWN.value
                ),
                result_refs=_dump_array(_RESOURCE_REFS_ADAPTER, job.result_refs),
                problem=(
                    _dump_object(_PROBLEM_ADAPTER, job.problem)
                    if job.problem is not None
                    else null()
                ),
                started_at=job.started_at,
                finished_at=job.finished_at,
                revision=job.meta.revision,
                updated_at=job.meta.updated_at,
            )
        )
        if _affected_rows(result) != 1:
            raise JobCasMismatch

    async def synchronize_cancellation(
        self,
        access: WorkspaceAccessContext,
        job: Job,
        *,
        at: datetime,
    ) -> None:
        """@brief 在同事务补偿 Job 对应的领域活动态 / Compensate Job-bound domain-active state in the same transaction."""
        self._authorizer.require(
            access,
            PlatformPermission.CANCEL_JOB,
            target=PlatformResourceTarget(PlatformTargetKind.JOB, job.meta.id),
        )
        if job.workspace_id != access.workspace_id:
            raise JobCasMismatch
        record = await self._session.scalar(
            select(JobRecord)
            .where(
                JobRecord.workspace_id == str(access.workspace_id),
                JobRecord.id == str(job.meta.id),
                JobRecord.revision == job.meta.revision,
            )
            .with_for_update()
        )
        if record is None:
            raise JobCasMismatch
        policy = _cancellation_policy(record.job_type, record.target_resource_type)
        if (
            record.job_type != job.kind
            or record.target_resource_id != job.subject.id
            or record.target_resource_revision != job.subject.revision
        ):
            raise JobCasMismatch
        if policy.domain is _CancellationDomain.JOB_ONLY:
            return
        if policy.domain is _CancellationDomain.RESUME_IMPORT:
            await self._cancel_resume_import(access, record, at=at)
            return
        if policy.domain is _CancellationDomain.CONNECTION_REVOKE:
            await self._cancel_connection_revoke(access, record, at=at)
            return
        if policy.domain is _CancellationDomain.KNOWLEDGE_DELETE:
            await self._cancel_knowledge_delete(access, record, at=at)
            return
        if policy.domain is _CancellationDomain.KNOWLEDGE_PROCESS:
            await self._cancel_knowledge_process(access, record, at=at)
            return
        if policy.domain is _CancellationDomain.AGENT_RUN:
            await self._cancel_agent_run(access, record, at=at)
            return
        if policy.domain is _CancellationDomain.INTERVIEW_END:
            await self._cancel_interview_end(access, record, at=at)
            return
        if policy.domain is _CancellationDomain.INTERVIEW_REPORT:
            await self._validate_interview_report_binding(record)
            return
        raise AssertionError("unhandled closed Job cancellation domain")

    async def _lock_domain_before_job(
        self,
        job: JobRecord,
        policy: _CancellationPolicy,
    ) -> None:
        """@brief 对 domain-first worker 采用相同锁顺序 / Match the lock order of domain-first workers.

        @param job 未锁定的 Job snapshot / Unlocked Job snapshot.
        @param policy exact cancellation policy / Exact cancellation policy.
        @note Agent 与 Interview worker 都按 aggregate → Job 加锁；通用取消沿用该顺序，避免
            与高并发 provider completion 形成数据库死锁。/ Agent and Interview workers lock
            aggregate then Job; generic cancellation follows that order to avoid deadlocks with
            concurrent provider completion.
        """
        if policy.domain is _CancellationDomain.AGENT_RUN:
            await self._session.scalar(
                select(AgentRunRecord.id)
                .where(
                    AgentRunRecord.workspace_id == job.workspace_id,
                    AgentRunRecord.id == job.target_resource_id,
                )
                .with_for_update()
            )
            return
        if policy.domain in {
            _CancellationDomain.INTERVIEW_END,
            _CancellationDomain.INTERVIEW_REPORT,
        }:
            await self._session.scalar(
                select(InterviewSessionRecord.id)
                .where(
                    InterviewSessionRecord.workspace_id == job.workspace_id,
                    InterviewSessionRecord.id == job.target_resource_id,
                )
                .with_for_update()
            )

    async def _cancel_resume_import(
        self,
        access: WorkspaceAccessContext,
        job: JobRecord,
        *,
        at: datetime,
    ) -> None:
        """@brief 释放取消导入占用的统一 UploadSession / Release the unified UploadSession claimed by a cancelled import."""
        upload = await self._session.scalar(
            select(KnowledgeUploadSessionRecord)
            .where(
                KnowledgeUploadSessionRecord.workspace_id == str(access.workspace_id),
                KnowledgeUploadSessionRecord.id == job.target_resource_id,
            )
            .with_for_update()
        )
        if (
            upload is None
            or upload.claimed_by_type != "job"
            or upload.claimed_by_id != job.id
            or upload.claimed_by_job_id != job.id
        ):
            raise JobCasMismatch
        upload.claimed_by_type = None
        upload.claimed_by_id = None
        upload.claimed_by_revision = None
        upload.claimed_by_job_id = None
        upload.consumed_at = None
        upload.updated_at = _domain_revision_at(at, upload.updated_at)
        upload.revision += 1

    async def _cancel_connection_revoke(
        self,
        access: WorkspaceAccessContext,
        job: JobRecord,
        *,
        at: datetime,
    ) -> None:
        """@brief 仅在 provider 副作用开始前恢复 Connection / Restore a Connection only before provider side effects start."""
        if job.status != JobStatus.QUEUED.value:
            raise JobCancellationRejected(
                "job.cancellation_too_late",
                "connection credential revocation has already started and cannot be safely undone",
            )
        spec = _job_spec(job)
        if spec.get("connection_id") != job.target_resource_id:
            raise JobCasMismatch
        connection = await self._session.scalar(
            select(ConnectionRecord)
            .where(
                ConnectionRecord.workspace_id == str(access.workspace_id),
                ConnectionRecord.id == job.target_resource_id,
            )
            .with_for_update()
        )
        if connection is None or connection.status != "revoking":
            raise JobCasMismatch
        previous_status = spec.get("previous_status", "reauthorization_required")
        if previous_status not in {"active", "reauthorization_required", "failed"}:
            raise JobCasMismatch
        previous_problem = spec.get("previous_problem")
        if previous_status == "failed":
            if not isinstance(previous_problem, Mapping):
                raise JobCasMismatch
            connection.problem = cast(JsonObject, dict(previous_problem))
        else:
            if previous_problem is not None:
                raise JobCasMismatch
            connection.problem = None
        connection.status = previous_status
        connection.updated_at = _domain_revision_at(at, connection.updated_at)
        connection.revision += 1

    async def _cancel_knowledge_delete(
        self,
        access: WorkspaceAccessContext,
        job: JobRecord,
        *,
        at: datetime,
    ) -> None:
        """@brief 在 eraser 开始前恢复 KnowledgeSource / Restore a KnowledgeSource before the eraser starts."""
        if job.status != JobStatus.QUEUED.value:
            raise JobCancellationRejected(
                "job.cancellation_too_late",
                "knowledge erasure has already started and cannot be safely undone",
            )
        spec = _job_spec(job)
        if spec.get("source_id") != job.target_resource_id:
            raise JobCasMismatch
        source = await self._session.scalar(
            select(KnowledgeSourceRecord)
            .where(
                KnowledgeSourceRecord.workspace_id == str(access.workspace_id),
                KnowledgeSourceRecord.id == job.target_resource_id,
            )
            .with_for_update()
        )
        if source is None or source.ingestion_state != "deleting":
            raise JobCasMismatch
        status, problem = _knowledge_recovery_state(spec, source)
        previous_enabled = spec.get("previous_enabled", True)
        if not isinstance(previous_enabled, bool):
            raise JobCasMismatch
        source.enabled = previous_enabled
        source.ingestion_state = status
        source.last_problem = problem
        source.deleted_at = None
        source.updated_at = _domain_revision_at(at, source.updated_at)
        source.revision += 1

    async def _cancel_knowledge_process(
        self,
        access: WorkspaceAccessContext,
        job: JobRecord,
        *,
        at: datetime,
    ) -> None:
        """@brief 恢复 ingestion/sync 排队前稳定状态 / Restore the stable state before ingestion or sync queueing."""
        spec = _job_spec(job)
        if spec.get("source_id") != job.target_resource_id:
            raise JobCasMismatch
        source = await self._session.scalar(
            select(KnowledgeSourceRecord)
            .where(
                KnowledgeSourceRecord.workspace_id == str(access.workspace_id),
                KnowledgeSourceRecord.id == job.target_resource_id,
            )
            .with_for_update()
        )
        if source is None or source.ingestion_state not in {
            "queued",
            "fetching",
            "parsing",
            "chunking",
            "embedding",
        }:
            raise JobCasMismatch
        status, problem = _knowledge_recovery_state(spec, source)
        source.ingestion_state = status
        source.last_problem = problem
        source.updated_at = _domain_revision_at(at, source.updated_at)
        source.revision += 1

    async def _cancel_agent_run(
        self,
        access: WorkspaceAccessContext,
        job: JobRecord,
        *,
        at: datetime,
    ) -> None:
        """@brief 原子取消 AgentRun 并关闭 pending approval / Atomically cancel an AgentRun and close its pending approval."""
        run = await self._session.scalar(
            select(AgentRunRecord)
            .where(
                AgentRunRecord.workspace_id == str(access.workspace_id),
                AgentRunRecord.id == job.target_resource_id,
            )
            .with_for_update()
        )
        if run is None or run.job_id != job.id:
            raise JobCasMismatch
        expected_states = {"queued"} if job.status == "queued" else {"running", "waiting_for_approval"}
        if run.status not in expected_states:
            raise JobCasMismatch
        if run.pending_approval_id is not None:
            approval = await self._session.scalar(
                select(ToolApprovalRecord)
                .where(
                    ToolApprovalRecord.workspace_id == str(access.workspace_id),
                    ToolApprovalRecord.id == run.pending_approval_id,
                    ToolApprovalRecord.run_id == run.id,
                )
                .with_for_update()
            )
            if approval is None or approval.status != "pending":
                raise JobCasMismatch
            approval_at = _domain_revision_at(at, approval.updated_at)
            approval.status = "expired" if approval_at >= approval.expires_at else "rejected"
            approval.decision_by_type = "user"
            approval.decision_by_id = str(access.actor.user_id)
            approval.decision_by_revision = None
            approval.updated_at = approval_at
            approval.revision += 1
        run.status = "cancelled"
        run.pending_approval_id = None
        run.active_tool_call_id = None
        run.updated_at = _domain_revision_at(at, run.updated_at)
        run.revision += 1

    async def _cancel_interview_end(
        self,
        access: WorkspaceAccessContext,
        job: JobRecord,
        *,
        at: datetime,
    ) -> None:
        """@brief 在 media finalization 开始前取消 Session / Cancel a Session before media finalization starts."""
        if job.status != JobStatus.QUEUED.value:
            raise JobCancellationRejected(
                "job.cancellation_too_late",
                "interview media finalization has already started and cannot be safely undone",
            )
        session = await self._interview_session_for_job(job)
        if session.status != "ending" or session.pending_end_job_id != job.id:
            raise JobCasMismatch
        session.status = "cancelled"
        session.pending_end_job_id = None
        session.end_reason = None
        session_at = _domain_revision_at(at, session.updated_at)
        session.ended_at = session_at
        session.updated_at = session_at
        session.revision += 1

    async def _validate_interview_report_binding(self, job: JobRecord) -> None:
        """@brief 验证取消 Report Job 不会覆盖已发布 Report / Ensure Report cancellation cannot overwrite a published Report."""
        session = await self._interview_session_for_job(job)
        if session.status != "completed" or session.report_id is not None:
            raise JobCasMismatch

    async def _interview_session_for_job(
        self,
        job: JobRecord,
    ) -> InterviewSessionRecord:
        """@brief 锁定并验证 Interview typed Job binding / Lock and validate an Interview typed Job binding."""
        binding = await self._session.scalar(
            select(InterviewReportJobRecord).where(
                InterviewReportJobRecord.workspace_id == job.workspace_id,
                InterviewReportJobRecord.job_id == job.id,
                InterviewReportJobRecord.session_id == job.target_resource_id,
                InterviewReportJobRecord.job_kind == job.job_type,
            )
        )
        session = await self._session.scalar(
            select(InterviewSessionRecord)
            .where(
                InterviewSessionRecord.workspace_id == job.workspace_id,
                InterviewSessionRecord.id == job.target_resource_id,
            )
            .with_for_update()
        )
        if binding is None or session is None:
            raise JobCasMismatch
        return session

    async def list_artifacts(
        self,
        access: WorkspaceAccessContext,
        query: ArtifactQuery,
        page: PageRequest,
    ) -> CollectionPage[Artifact]:
        """@brief 以 ``created_at,id`` keyset 列出 Artifact / List Artifacts using a ``created_at,id`` keyset."""
        self._authorizer.require(access, PlatformPermission.LIST_ARTIFACTS)
        statement = select(ArtifactRecord).where(
            ArtifactRecord.workspace_id == str(access.workspace_id),
            ArtifactRecord.deleted_at.is_(None),
        )
        if query.kind is not None:
            statement = statement.where(ArtifactRecord.kind == query.kind.value)
        if query.subject.subject_type is not None:
            statement = statement.where(
                ArtifactRecord.subject_type == query.subject.subject_type
            )
        if query.subject.subject_id is not None:
            statement = statement.where(
                ArtifactRecord.subject_id == query.subject.subject_id
            )
        after = _parse_page_position(page.after)
        if after is not None:
            after_at, after_id = after
            statement = statement.where(
                or_(
                    ArtifactRecord.created_at < after_at,
                    and_(
                        ArtifactRecord.created_at == after_at,
                        ArtifactRecord.id < after_id,
                    ),
                )
            )
        rows = (
            await self._session.scalars(
                statement.order_by(
                    ArtifactRecord.created_at.desc(),
                    ArtifactRecord.id.desc(),
                ).limit(page.limit + 1)
            )
        ).all()
        items = tuple(
            _artifact_from_record(row, self._api_origin) for row in rows[: page.limit]
        )
        next_position = (
            _page_position(items[-1].meta.created_at, str(items[-1].meta.id))
            if len(rows) > page.limit and items
            else None
        )
        return CollectionPage(items, next_position)

    async def get_artifact(
        self,
        access: WorkspaceAccessContext,
        artifact_id: ArtifactId,
    ) -> Artifact | None:
        """@brief 在 proof Workspace 读取 Artifact / Read an Artifact in the proof Workspace."""
        request = self._authorizer.request_for(access)
        if request.permission not in {
            PlatformPermission.READ_ARTIFACT,
            PlatformPermission.READ_ARTIFACT_CONTENT,
            PlatformPermission.READ_ARTIFACT_SOURCE_MAP,
        }:
            raise PermissionError("Artifact item read requires its exact proof")
        self._authorizer.require(
            access,
            request.permission,
            target=PlatformResourceTarget(PlatformTargetKind.ARTIFACT, artifact_id),
        )
        row = await self._session.scalar(
            select(ArtifactRecord).where(
                ArtifactRecord.workspace_id == str(access.workspace_id),
                ArtifactRecord.id == str(artifact_id),
                ArtifactRecord.deleted_at.is_(None),
            )
        )
        return _artifact_from_record(row, self._api_origin) if row is not None else None

    async def get_pdf_source_map(
        self,
        access: WorkspaceAccessContext,
        artifact_id: ArtifactId,
    ) -> PdfSourceMap | None:
        """@brief 读取规范 PDF source map / Read the canonical PDF source map."""
        self._authorizer.require(
            access,
            PlatformPermission.READ_ARTIFACT_SOURCE_MAP,
            target=PlatformResourceTarget(PlatformTargetKind.ARTIFACT, artifact_id),
        )
        row = await self._session.scalar(
            select(ArtifactPdfSourceMapRecord).where(
                ArtifactPdfSourceMapRecord.workspace_id == str(access.workspace_id),
                ArtifactPdfSourceMapRecord.artifact_id == str(artifact_id),
            )
        )
        return _source_map_from_record(row) if row is not None else None

    async def list_audit_events(
        self,
        access: WorkspaceAccessContext,
        page: PageRequest,
    ) -> CollectionPage[AuditEvent]:
        """@brief 以 ``occurred_at,id`` keyset 列出 AuditEvent / List AuditEvents using an ``occurred_at,id`` keyset."""
        self._authorizer.require(access, PlatformPermission.LIST_AUDIT_EVENTS)
        statement = select(AuditEventRecord).where(
            AuditEventRecord.workspace_id == str(access.workspace_id)
        )
        after = _parse_page_position(page.after)
        if after is not None:
            after_at, after_id = after
            statement = statement.where(
                or_(
                    AuditEventRecord.occurred_at < after_at,
                    and_(
                        AuditEventRecord.occurred_at == after_at,
                        AuditEventRecord.id < after_id,
                    ),
                )
            )
        rows = (
            await self._session.scalars(
                statement.order_by(
                    AuditEventRecord.occurred_at.desc(),
                    AuditEventRecord.id.desc(),
                ).limit(page.limit + 1)
            )
        ).all()
        items = tuple(_audit_from_record(row) for row in rows[: page.limit])
        next_position = (
            _page_position(items[-1].occurred_at, str(items[-1].id))
            if len(rows) > page.limit and items
            else None
        )
        return CollectionPage(items, next_position)


async def append_workspace_outbox_event(
    session: AsyncSession,
    *,
    event_id: ApiEventId,
    workspace_id: WorkspaceId,
    resource_owner_id: str,
    subject: ResourceRef,
    event_type: str,
    occurred_at: datetime,
    data: Mapping[str, Any],
    trace_id: str | None,
    replay_expires_at: datetime,
) -> int:
    """@brief 通过统一 trigger 追加并分配 Workspace event sequence / Append through the shared Workspace-sequence trigger.

    @return 数据库原子分配的 sequence / Sequence atomically allocated by the database.
    @note 所有领域 producer 应调用此窄入口；migration trigger 仍会覆盖旧 producer 提供的
        resource revision，保证整个 Workspace stream 唯一有序。
        / All domain producers should call this narrow entry point; the migration trigger also
        overrides legacy producer values so the entire Workspace stream remains uniquely ordered.
    """
    payload = TypeAdapter(dict[str, Any]).dump_python(dict(data), mode="json")
    if not isinstance(payload, dict):
        raise TypeError("ApiEvent data must encode as an object")
    lifecycle = initial_outbox_lifecycle(event_type, occurred_at=occurred_at)
    row = OutboxEventRecord(
        id=str(event_id),
        workspace_id=str(workspace_id),
        resource_owner_id=resource_owner_id,
        aggregate_type=subject.resource_type,
        aggregate_id=subject.id,
        subject_revision=subject.revision,
        event_type=event_type,
        sequence=0,
        occurred_at=occurred_at,
        payload=cast(JsonObject, payload),
        trace_id=trace_id,
        replay_expires_at=replay_expires_at,
        status=lifecycle.status,
        published_at=lifecycle.published_at,
        created_at=occurred_at,
        updated_at=occurred_at,
        revision=1,
        extensions={},
    )
    session.add(row)
    await session.flush([row])
    await session.refresh(row, attribute_names=["sequence"])
    return row.sequence


class _PostgresPlatformJournal:
    """@brief 与 Job CAS 共 transaction 的 outbox/audit journal / Outbox and audit journal sharing the Job transaction."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingPlatformAuthorizer,
        retention: timedelta,
    ) -> None:
        """@brief 绑定 Session、proof verifier 与 replay retention / Bind Session, proof verifier, and replay retention."""
        self._session = session
        self._authorizer = authorizer
        self._retention = retention

    async def job_cancelled(
        self,
        access: WorkspaceAccessContext,
        before: Job,
        after: Job,
        context: MutationContext,
    ) -> None:
        """@brief 同事务追加 ``job.updated`` 与 ``job.cancel`` / Append ``job.updated`` and ``job.cancel`` in the same transaction."""
        target = PlatformResourceTarget(PlatformTargetKind.JOB, before.meta.id)
        self._authorizer.require(access, PlatformPermission.CANCEL_JOB, target=target)
        if (
            before.workspace_id != access.workspace_id
            or after.workspace_id != access.workspace_id
            or after.meta.revision != before.meta.revision + 1
        ):
            raise ValueError("Job cancellation journal received inconsistent aggregates")
        resource_owner_id = await self._session.scalar(
            select(JobRecord.resource_owner_id).where(
                JobRecord.workspace_id == str(access.workspace_id),
                JobRecord.id == str(after.meta.id),
            )
        )
        if resource_owner_id is None:
            raise JobCasMismatch
        occurred_at = after.meta.updated_at
        self._session.add(
            AuditEventRecord(
                id=new_opaque_id("audit"),
                workspace_id=str(access.workspace_id),
                resource_owner_id=resource_owner_id,
                occurred_at=occurred_at,
                actor_type="user",
                actor_id=str(access.actor.user_id),
                actor_revision=None,
                action="job.cancel",
                resource_type="job",
                resource_id=str(after.meta.id),
                resource_revision=after.meta.revision,
                request_id=context.request_id,
                outcome=AuditOutcome.ALLOWED.value,
                details={},
                created_at=occurred_at,
                updated_at=occurred_at,
                revision=1,
                extensions={},
            )
        )
        await append_workspace_outbox_event(
            self._session,
            event_id=ApiEventId(new_opaque_id("evt")),
            workspace_id=access.workspace_id,
            resource_owner_id=resource_owner_id,
            subject=ResourceRef("job", str(after.meta.id), after.meta.revision),
            event_type="job.updated",
            occurred_at=occurred_at,
            data={"status": after.status.value},
            trace_id=context.trace_id,
            replay_expires_at=occurred_at + self._retention,
        )


class PostgresArtifactContentStore:
    """@brief PostgreSQL BYTEA Artifact content store / PostgreSQL BYTEA Artifact content store."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        chunk_size: int = _CONTENT_CHUNK_SIZE,
    ) -> None:
        """@brief 绑定 database 与 chunk 大小 / Bind the database and chunk size."""
        if chunk_size < 1:
            raise ValueError("Artifact chunk size must be positive")
        self._database = database
        self._chunk_size = chunk_size

    async def open(
        self,
        access: WorkspaceAccessContext,
        artifact: Artifact,
        selected_range: ContentRange | None,
    ) -> ArtifactContentStream:
        """@brief 读取前验证完整对象 metadata、length 与 SHA-256 / Validate whole-object metadata, length, and SHA-256 before opening."""
        if access.action is not WorkspaceAction.READ_ARTIFACT_CONTENT:
            raise PermissionError("Artifact content requires its exact access proof")
        if access.workspace_id != artifact.workspace_id:
            raise PermissionError("Artifact content proof crossed a Workspace boundary")
        session = self._database.new_session()
        try:
            async with session.begin():
                await self._database.install_v2_request_scope(
                    session,
                    actor_id=str(access.actor.user_id),
                    workspace_id=str(access.workspace_id),
                )
                pair = (
                    await session.execute(
                        select(ArtifactRecord, ArtifactContentRecord)
                        .join(
                            ArtifactContentRecord,
                            ArtifactContentRecord.artifact_id == ArtifactRecord.id,
                        )
                        .where(
                            ArtifactRecord.workspace_id == str(access.workspace_id),
                            ArtifactRecord.id == str(artifact.meta.id),
                            ArtifactRecord.deleted_at.is_(None),
                            ArtifactContentRecord.workspace_id == str(access.workspace_id),
                        )
                    )
                ).one_or_none()
        finally:
            await session.close()
        if pair is None:
            raise FileNotFoundError(str(artifact.meta.id))
        metadata, stored = pair
        digest = sha256(stored.content).hexdigest()
        if (
            metadata.storage_key != stored.storage_key
            or metadata.media_type != artifact.media_type
            or metadata.size_bytes != artifact.size_bytes
            or metadata.sha256 != artifact.sha256
            or stored.media_type != artifact.media_type
            or stored.size_bytes != artifact.size_bytes
            or stored.size_bytes != len(stored.content)
            or stored.sha256 != artifact.sha256
            or digest != artifact.sha256
        ):
            raise ValueError("Artifact object metadata or digest is inconsistent")
        content = stored.content
        if selected_range is not None:
            content = content[selected_range.first : selected_range.last_inclusive + 1]
        return ArtifactContentStream(
            _byte_chunks(content, self._chunk_size),
            stored.media_type,
            stored.size_bytes,
            stored.sha256,
            selected_range,
        )


class PostgresPlatformEventFeed:
    """@brief 以 committed outbox 为来源的 gap-free polling feed / Gap-free polling feed sourced from committed outbox rows."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        poll_interval: float = 0.25,
        batch_size: int = 200,
    ) -> None:
        """@brief 绑定 database、poll interval 与 batch size / Bind database, poll interval, and batch size."""
        if poll_interval <= 0:
            raise ValueError("event poll interval must be positive")
        if not 1 <= batch_size <= 1000:
            raise ValueError("event feed batch size must be between one and 1000")
        self._database = database
        self._poll_interval = poll_interval
        self._batch_size = batch_size

    async def open(
        self,
        access: WorkspaceAccessContext,
        replay: EventReplayRequest,
    ) -> AsyncIterator[ApiEvent]:
        """@brief 原子确定 replay cursor，再轮询更大 sequence / Atomically determine a replay cursor, then poll larger sequences."""
        if access.action is not WorkspaceAction.READ_EVENTS:
            raise PermissionError("Event feed requires its exact access proof")
        cursor = await self._initial_cursor(access, replay)

        async def stream() -> AsyncIterator[ApiEvent]:
            """@brief 从 committed outbox 连续追赶 live tail / Continuously catch up with the committed outbox live tail."""
            nonlocal cursor
            while True:
                rows = await self._read_after(access, cursor)
                if not rows:
                    await asyncio.sleep(self._poll_interval)
                    continue
                for row in rows:
                    event = _event_from_record(row)
                    cursor = event.sequence
                    yield event

        return stream()

    async def _initial_cursor(
        self,
        access: WorkspaceAccessContext,
        replay: EventReplayRequest,
    ) -> int:
        """@brief 在一个 snapshot 中验证 Last-Event-ID 或锚定 live head / Validate Last-Event-ID or anchor the live head."""
        session = self._database.new_session()
        try:
            async with session.begin():
                await self._database.install_v2_request_scope(
                    session,
                    actor_id=str(access.actor.user_id),
                    workspace_id=str(access.workspace_id),
                )
                if replay.after_event_id is None:
                    value = await session.scalar(
                        select(func.coalesce(func.max(OutboxEventRecord.sequence), 0)).where(
                            OutboxEventRecord.workspace_id == str(access.workspace_id)
                        )
                    )
                    return int(value or 0)
                row = await session.scalar(
                    select(OutboxEventRecord).where(
                        OutboxEventRecord.workspace_id == str(access.workspace_id),
                        OutboxEventRecord.id == str(replay.after_event_id),
                    )
                )
                if row is None or row.replay_expires_at <= datetime.now(UTC):
                    raise EventReplayWindowExpired(replay.after_event_id)
                return row.sequence
        finally:
            await session.close()

    async def _read_after(
        self,
        access: WorkspaceAccessContext,
        sequence: int,
    ) -> list[OutboxEventRecord]:
        """@brief 读取当前 committed snapshot 的下一批事件 / Read the next batch from the current committed snapshot."""
        session = self._database.new_session()
        try:
            async with session.begin():
                await self._database.install_v2_request_scope(
                    session,
                    actor_id=str(access.actor.user_id),
                    workspace_id=str(access.workspace_id),
                )
                rows = await session.scalars(
                    select(OutboxEventRecord)
                    .where(
                        OutboxEventRecord.workspace_id == str(access.workspace_id),
                        OutboxEventRecord.sequence > sequence,
                        OutboxEventRecord.replay_expires_at > datetime.now(UTC),
                    )
                    .order_by(OutboxEventRecord.sequence)
                    .limit(self._batch_size)
                )
                return list(rows.all())
        finally:
            await session.close()


class PostgresPlatformUnitOfWork:
    """@brief 一个 PostgreSQL Platform 短事务 UoW / One PostgreSQL Platform short-transaction UoW."""

    def __init__(
        self,
        database: AsyncDatabase,
        api_origin: str,
        *,
        retention: timedelta,
    ) -> None:
        """@brief 绑定 database、API Origin 与 replay retention / Bind database, API origin, and replay retention."""
        self._database = database
        self._api_origin = api_origin
        self._retention = retention
        self._session: AsyncSession | None = None
        self._transaction: AsyncSessionTransaction | None = None
        self._authorizer: _TrackingPlatformAuthorizer | None = None
        self._repository: _PostgresPlatformRepository | None = None
        self._journal: _PostgresPlatformJournal | None = None
        self._committed = False
        self._rolled_back = False

    @property
    def authorizer(self) -> _TrackingPlatformAuthorizer:
        """@brief 返回 transaction-bound authorizer / Return the transaction-bound authorizer."""
        if self._authorizer is None:
            raise RuntimeError("Platform unit of work has not been entered")
        return self._authorizer

    @property
    def repository(self) -> _PostgresPlatformRepository:
        """@brief 返回 transaction-bound repository / Return the transaction-bound repository."""
        if self._repository is None:
            raise RuntimeError("Platform unit of work has not been entered")
        return self._repository

    @property
    def journal(self) -> _PostgresPlatformJournal:
        """@brief 返回 transaction-bound journal / Return the transaction-bound journal."""
        if self._journal is None:
            raise RuntimeError("Platform unit of work has not been entered")
        return self._journal

    async def __aenter__(self) -> Self:
        """@brief 通过 ``new_session`` 创建或加入 atomic envelope / Create or join an atomic envelope through ``new_session``."""
        if self._session is not None:
            raise RuntimeError("Platform unit of work cannot be re-entered")
        self._session = self._database.new_session()
        self._transaction = await self._session.begin()
        session = self._session
        access_repository = PostgresAccessRepository(session)

        async def lookup(
            workspace_id: WorkspaceId,
            job_id: JobId,
        ) -> tuple[str, str] | None:
            """@brief 在已安装 RLS scope 中查 cancellation policy / Look up cancellation policy under installed RLS scope."""
            row = (
                await session.execute(
                    select(JobRecord.job_type, JobRecord.target_resource_type).where(
                        JobRecord.workspace_id == str(workspace_id),
                        JobRecord.id == str(job_id),
                    )
                )
            ).one_or_none()
            return (row[0], row[1]) if row is not None else None

        self._authorizer = _TrackingPlatformAuthorizer(
            AccessAuthorizer(access_repository),
            lookup,
            partial(self._database.install_v2_request_scope, session),
        )
        self._repository = _PostgresPlatformRepository(
            session,
            self._authorizer,
            self._api_origin,
        )
        self._journal = _PostgresPlatformJournal(
            session,
            self._authorizer,
            self._retention,
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时回滚并关闭 Session / Roll back uncommitted work and close the Session."""
        del exc, traceback
        if self._session is not None:
            if exc_type is not None or not self._committed:
                await self.rollback()
            await self._session.close()
        self._session = None
        self._transaction = None
        self._authorizer = None
        self._repository = None
        self._journal = None
        return None

    async def commit(self) -> None:
        """@brief 原子提交 Job CAS、outbox 与 audit / Atomically commit Job CAS, outbox, and audit."""
        session, transaction = self._require_active()
        if self._committed:
            raise RuntimeError("Platform unit of work is already committed")
        if self._rolled_back:
            raise RuntimeError("rolled-back Platform unit of work cannot commit")
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
            raise RuntimeError("Platform unit of work has not been entered")
        return self._session, self._transaction


class PostgresPlatformUnitOfWorkFactory:
    """@brief 组装 PostgreSQL Platform adapters / Assemble PostgreSQL Platform adapters."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        api_origin: str,
        retention: timedelta = _EVENT_RETENTION,
        event_poll_interval: float = 0.25,
    ) -> None:
        """@brief 绑定 database，并公开 content/event adapters / Bind database and expose content/event adapters."""
        if retention <= timedelta(0):
            raise ValueError("event retention must be positive")
        # Domain constructor is the single canonical Origin validator.
        ApiArtifactContentUrl.build(
            api_origin,
            WorkspaceId("workspace_validation"),
            ArtifactId("artifact_validation"),
        )
        self._database = database
        self._api_origin = api_origin
        self._retention = retention
        self.content_store = PostgresArtifactContentStore(database)
        self.event_feed = PostgresPlatformEventFeed(
            database,
            poll_interval=event_poll_interval,
        )

    def __call__(self) -> PostgresPlatformUnitOfWork:
        """@brief 创建未进入的 PostgreSQL UoW / Create a not-yet-entered PostgreSQL UoW."""
        return PostgresPlatformUnitOfWork(
            self._database,
            self._api_origin,
            retention=self._retention,
        )


__all__ = [
    "InMemoryArtifactContentStore",
    "InMemoryPlatformEventFeed",
    "InMemoryPlatformStore",
    "InMemoryPlatformUnitOfWork",
    "InMemoryPlatformUnitOfWorkFactory",
    "PostgresArtifactContentStore",
    "PostgresPlatformEventFeed",
    "PostgresPlatformUnitOfWork",
    "PostgresPlatformUnitOfWorkFactory",
    "append_workspace_outbox_event",
]
