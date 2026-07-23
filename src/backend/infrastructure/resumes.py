"""@brief API V2 Resume 事务持久化适配器 / API V2 Resume transactional persistence adapters.

PostgreSQL 实现把集中 ``AccessAuthorizer``、Resume repository、Job、upload claim 与
outbox 固定在同一 ``AsyncSession``。内存实现使用 copy-on-write 工作单元，供本地运行
与确定性测试复用相同事务语义。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from hashlib import sha256
from types import TracebackType
from typing import Any, Protocol, Self, cast
from uuid import uuid4

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult, Result
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from backend.application.ports.access import AccessAuthorizer
from backend.application.ports.resume_worker import (
    PersistedResumeJob,
    RenderedResumeArtifact,
    ResumeCapabilityFailure,
    ResumeImportSource,
    resume_worker_artifact_id,
)
from backend.application.ports.resumes import (
    CollectionPage,
    OperationBatchReceipt,
    PageRequest,
    ResumeCasMismatch,
    ResumeRepository,
    ResumeTemplateCatalog,
)
from backend.domain.outbox import initial_outbox_lifecycle
from backend.domain.platform import (
    ApiArtifactContentUrl,
    Artifact,
    ArtifactId,
    ArtifactKind,
    Job,
    JobId,
    JobProgress,
    JobProgressUnit,
    JobStatus,
    PdfRect,
    PdfSourceMap,
    PdfSourceNode,
    PlatformDomainError,
    ProblemDetails,
)
from backend.domain.platform import (
    JsonValue as PlatformJsonValue,
)
from backend.domain.principals import (
    AuthenticatedActor,
    ResourceMeta,
    TokenPrincipal,
    UserId,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.resume_jobs import (
    RenderFormat,
    ResumeJobKind,
    ResumeJobSpec,
    ResumeOutboxEvent,
)
from backend.domain.resume_proposals import ResumeProposal, ResumeProposalStatus
from backend.domain.resumes import (
    ChangeTarget,
    OperationLedgerEntry,
    PageSize,
    ResourceRef,
    ResumeAggregate,
    ResumeBatchId,
    ResumeDocument,
    ResumeId,
    ResumeOperation,
    ResumeOperationId,
    ResumeOperationOutcome,
    ResumeProposalId,
    ResumeRevision,
    ResumeRevisionSummary,
    ResumeSectionKind,
    ResumeSummary,
    RevisionChange,
    TemplatePolicy,
    TemplateRef,
    TemplateSettingRule,
    TemplateSettingValueType,
    TemplateZonePolicy,
)
from backend.domain.templates import get_template_manifest
from backend.domain.upload_sessions import UploadSessionId
from backend.infrastructure.access import (
    InMemoryAccessRepository,
    InMemoryAccessStore,
    PostgresAccessRepository,
)
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    ArtifactContentRecord,
    ArtifactPdfSourceMapRecord,
    ArtifactRecord,
    JobRecord,
    JsonObject,
    OutboxEventRecord,
    ResumeDocumentRecord,
    ResumeImportUploadSessionRecord,
    ResumeOperationBatchRecord,
    ResumeOperationRecord,
    ResumeProposalOperationRecord,
    ResumeProposalRecord,
    ResumeRenderJobRecord,
    ResumeRevisionRecord,
)

_DOCUMENT_ADAPTER: TypeAdapter[ResumeDocument] = TypeAdapter(ResumeDocument)
"""@brief Resume SIR 的唯一持久化 codec / Sole persistence codec for Resume SIR."""

_OPERATION_ADAPTER: TypeAdapter[ResumeOperation] = TypeAdapter(ResumeOperation)
"""@brief Resume operation codec / Resume operation codec."""

_OUTCOME_ADAPTER: TypeAdapter[ResumeOperationOutcome] = TypeAdapter(ResumeOperationOutcome)
"""@brief 可精确重放的 batch outcome codec / Exactly replayable batch-outcome codec."""

_RESOURCE_REFS_ADAPTER: TypeAdapter[tuple[ResourceRef, ...]] = TypeAdapter(tuple[ResourceRef, ...])
"""@brief proposal evidence references codec / Proposal evidence-reference codec."""

_JOB_SPEC_ADAPTER: TypeAdapter[ResumeJobSpec] = TypeAdapter(ResumeJobSpec)
"""@brief Resume Job 判别联合 codec / Resume Job discriminated-union codec."""

_RESOURCE_REF_ADAPTER: TypeAdapter[ResourceRef] = TypeAdapter(ResourceRef)
"""@brief 单个资源引用 codec / Single resource-reference codec."""

_PROBLEM_ADAPTER: TypeAdapter[ProblemDetails] = TypeAdapter(ProblemDetails)
"""@brief 统一 Job ProblemDetails codec / Unified Job ProblemDetails codec."""

_PDF_SOURCE_NODES_ADAPTER: TypeAdapter[tuple[PdfSourceNode, ...]] = TypeAdapter(
    tuple[PdfSourceNode, ...]
)
"""@brief 已验证 PDF source nodes codec / Validated PDF source-node codec."""

_RECEIPT_RETENTION = timedelta(days=30)
"""@brief 离线 batch receipt 的最短保留期 / Minimum offline-batch receipt retention."""

_EVENT_RETENTION = timedelta(days=30)
"""@brief Resume 通知的 replay 保留期 / Replay retention for Resume notifications."""

_DEFAULT_ARTIFACT_API_ORIGIN = "https://api.hmalliances.org:8022"
"""@brief 未显式注入时使用的契约公开 Origin / Contract public Origin used when not explicitly injected."""

type _MutablePlatformJson = (
    None
    | bool
    | int
    | float
    | str
    | list[_MutablePlatformJson]
    | dict[str, _MutablePlatformJson]
)
"""@brief PostgreSQL JSONB driver 接受的普通 JSON 树 / Plain JSON tree accepted by the PostgreSQL JSONB driver."""


def _dump_object[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> JsonObject:
    """@brief 将强类型值编码为 JSON object / Encode a typed value as a JSON object.

    @param adapter 对应值类型的 Pydantic adapter / Pydantic adapter for the value type.
    @param value 待编码值 / Value to encode.
    @return 可直接写入 JSONB 的 object / Object suitable for JSONB persistence.
    """
    payload = adapter.dump_python(value, mode="json")
    if not isinstance(payload, dict):
        raise TypeError("persistence codec must produce a JSON object")
    return cast(JsonObject, payload)


def _load_object[ValueT](adapter: TypeAdapter[ValueT], payload: object) -> ValueT:
    """@brief 从不可信持久化 JSON 重建领域值 / Rebuild a domain value from untrusted stored JSON.

    @param adapter 对应值类型的 Pydantic adapter / Pydantic adapter for the value type.
    @param payload JSONB 解码值 / Decoded JSONB value.
    @return 完整通过领域不变量的值 / Value satisfying all domain invariants.
    @raise ValueError 持久化数据不再满足领域模型时抛出 / Raised for invalid persisted data.
    """
    try:
        return adapter.validate_python(payload)
    except ValidationError as error:
        raise ValueError("persisted Resume V2 payload violates the domain model") from error


def _dump_array[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> list[JsonObject]:
    """@brief 将强类型 collection 编码为 JSON object 数组 / Encode a typed collection as an array of JSON objects.

    @param adapter collection 对应 codec / Codec for the collection.
    @param value 待编码 collection / Collection to encode.
    @return JSONB 可持久数组 / JSONB-persistable array.
    """
    payload = adapter.dump_python(value, mode="json")
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise TypeError("persistence codec must produce an array of JSON objects")
    return cast(list[JsonObject], payload)


def _dump_problem(problem: ProblemDetails) -> JsonObject:
    """@brief 显式投影深度冻结的 ProblemDetails / Explicitly project deeply frozen ProblemDetails.

    @param problem 已通过领域不变量验证的公开问题 / Public problem validated by domain invariants.
    @return 可直接写入 JSONB 的普通容器 / Plain containers suitable for JSONB persistence.
    @note Pydantic 无法把 ``MappingProxyType`` 直接序列化为 JSON；这里保持领域对象不可变，
        仅在基础设施边界生成可变 JSON 投影。 / Pydantic cannot serialize ``MappingProxyType``
        directly to JSON; this preserves the immutable domain object and creates mutable JSON only
        at the infrastructure boundary.
    """
    return {
        "type_uri": problem.type_uri,
        "title": problem.title,
        "status": problem.status,
        "code": problem.code,
        "request_id": problem.request_id,
        "retryable": problem.retryable,
        "detail": problem.detail,
        "instance": problem.instance,
        "errors": [
            {
                "pointer": error.pointer,
                "code": error.code,
                "message_key": error.message_key,
                "params": dict(error.params),
            }
            for error in problem.errors
        ],
        "extensions": _thaw_json(problem.extensions),
    }


def _thaw_json(value: PlatformJsonValue) -> _MutablePlatformJson:
    """@brief 把深度冻结 JSON 复制为普通容器 / Copy deeply frozen JSON into plain containers.

    @param value 已由领域层验证的 JSON 值 / JSON value already validated by the domain layer.
    @return 等价的 dict/list/scalar JSON 树 / Equivalent dict/list/scalar JSON tree.
    """
    if isinstance(value, Mapping):
        return {key: _thaw_json(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(nested) for nested in value]
    return value


def _canonical_hash(payload: JsonObject) -> str:
    """@brief 计算规范 JSON SHA-256 / Compute a canonical JSON SHA-256.

    @param payload JSON object / JSON object.
    @return 小写十六进制摘要 / Lowercase hexadecimal digest.
    """
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def decode_resume_document(
    payload: JsonObject,
    *,
    expected_sha256: str,
) -> ResumeDocument:
    """@brief 从持久化快照验证 hash 并解码 Resume / Verify a persisted snapshot hash and decode the Resume.

    @param payload PostgreSQL JSONB 中的 canonical SIR / Canonical SIR from PostgreSQL JSONB.
    @param expected_sha256 revision 行保存的完整性摘要 / Integrity digest stored on the revision row.
    @return 完整通过领域不变量的 Resume / Resume satisfying all domain invariants.
    @raise ValueError 摘要或领域模型不匹配时抛出 / Raised for a digest or domain-model mismatch.
    @note 这是 Resume 与 Agent infrastructure 共享的唯一读取 codec；跨域 adapter 不得复制
        hash 或 Pydantic 规则。/ This is the sole read codec shared by Resume and Agent
        infrastructure; cross-domain adapters must not duplicate hash or Pydantic rules.
    """

    if _canonical_hash(payload) != expected_sha256:
        raise ValueError("persisted Resume revision content hash mismatch")
    return _load_object(_DOCUMENT_ADAPTER, payload)


def decode_resume_operation(payload: object) -> ResumeOperation:
    """@brief 从未信任 JSON 解码 Resume operation / Decode a Resume operation from untrusted JSON.

    @param payload 含服务端 operation ID 的候选对象 / Candidate object carrying a server operation ID.
    @return 六种 operation 判别联合之一 / One of the six operation-union variants.
    @raise ValueError payload 不满足领域类型时抛出 / Raised when the payload violates the domain type.
    """

    return _load_object(_OPERATION_ADAPTER, payload)


def encode_resume_operation(operation: ResumeOperation) -> JsonObject:
    """@brief 将已验证 operation 编码为持久 JSON / Encode a validated operation for persistence.

    @param operation 领域 operation / Domain operation.
    @return canonical JSON object / Canonical JSON object.
    """

    return _dump_object(_OPERATION_ADAPTER, operation)


def _row_id(prefix: str) -> str:
    """@brief 生成仅用于数据库行的稳定形状 ID / Generate a stable-shaped database-row ID.

    @param prefix 短表前缀 / Short table prefix.
    @return 不向 API 暴露的唯一 ID / Unique ID not exposed through the API.
    """
    return f"{prefix}_{uuid4().hex}"


def _affected_rows(result: Result[Any]) -> int:
    """@brief 读取 DML affected-row count / Read a DML affected-row count.

    @param result SQLAlchemy DML result / SQLAlchemy DML result.
    @return 受影响行数 / Number of affected rows.
    """
    return cast(CursorResult[Any], result).rowcount


def _page_offset(position: str | None) -> int:
    """@brief 解码 revision 内部位置 / Decode an internal revision position.

    @param position 上一页结束位置 / Previous-page ending position.
    @return 非负 offset / Non-negative offset.
    """
    if position is None:
        return 0
    try:
        value = int(position)
    except ValueError as error:
        raise ValueError("revision page position is invalid") from error
    if value < 0:
        raise ValueError("revision page position is invalid")
    return value


def _change_targets(change: RevisionChange | None) -> list[JsonObject]:
    """@brief 将 revision 因果目标投影为 JSONB / Project revision causal targets to JSONB.

    @param change 目标 revision 的变更记录 / Change record for the target revision.
    @return 稳定顺序目标数组 / Stably ordered target array.
    """
    if change is None:
        return []
    return [
        {"entity_id": target.entity_id, "field_path": list(target.field_path)}
        for target in sorted(change.targets, key=lambda item: (item.entity_id, item.field_path))
    ]


def _load_change_targets(revision: int, payload: object) -> RevisionChange:
    """@brief 从 revision JSONB 重建因果目标 / Rebuild causal targets from revision JSONB.

    @param revision revision 编号 / Revision number.
    @param payload JSONB target array / JSONB target array.
    @return 领域 change record / Domain change record.
    """
    if not isinstance(payload, list):
        raise ValueError("persisted revision targets must be an array")
    targets: set[ChangeTarget] = set()
    for raw in payload:
        if not isinstance(raw, dict):
            raise ValueError("persisted revision target must be an object")
        entity_id = raw.get("entity_id")
        field_path = raw.get("field_path")
        if (
            not isinstance(entity_id, str)
            or not isinstance(field_path, list)
            or not all(isinstance(item, str) for item in field_path)
        ):
            raise ValueError("persisted revision target is invalid")
        targets.add(ChangeTarget(entity_id, tuple(field_path)))
    return RevisionChange(revision, frozenset(targets))


class BuiltinResumeTemplateCatalog:
    """@brief 将公开不可变 manifest 投影为领域策略 / Project public immutable manifests to policies."""

    async def get_policy(self, template: TemplateRef) -> TemplatePolicy | None:
        """@brief 读取精确模板版本策略 / Read one exact template-version policy.

        @param template 模板引用 / Template reference.
        @return 策略或不存在 / Policy or absence.
        """
        manifest = get_template_manifest(template.template_id, template.version)
        if manifest is None:
            return None
        return _template_policy(manifest)


class MappingResumeTemplateCatalog:
    """@brief 测试与私有部署的不可变策略映射 / Immutable policy mapping for tests and private deployments.

    @param policies 精确版本到策略的映射 / Exact-version-to-policy mapping.
    """

    def __init__(self, policies: Mapping[TemplateRef, TemplatePolicy]) -> None:
        """@brief 复制不可变策略映射 / Copy an immutable policy mapping.

        @param policies 模板策略 / Template policies.
        """
        self._policies = dict(policies)

    async def get_policy(self, template: TemplateRef) -> TemplatePolicy | None:
        """@brief 返回精确策略 / Return an exact policy.

        @param template 模板引用 / Template reference.
        @return 策略或不存在 / Policy or absence.
        """
        return self._policies.get(template)


def _template_policy(manifest: Mapping[str, Any]) -> TemplatePolicy:
    """@brief 从公开 manifest 构建原子校验策略 / Build atomic validation policy from a public manifest.

    @param manifest 已由契约发布的不可变 manifest / Contract-published immutable manifest.
    @return Resume 领域策略 / Resume domain policy.
    """
    template_id = str(manifest["id"])
    version = str(manifest.get("version", manifest.get("template_version")))
    supported_kinds = frozenset(
        ResumeSectionKind(value)
        for value in manifest["supported_section_kinds"]
        if value in {item.value for item in ResumeSectionKind}
    )
    zones: list[TemplateZonePolicy] = []
    for raw_zone in manifest["zones"]:
        zone = cast(Mapping[str, Any], raw_zone)
        accepted = frozenset(
            ResumeSectionKind(value)
            for value in zone["accepted_section_kinds"]
            if value in {item.value for item in ResumeSectionKind}
        )
        zones.append(
            TemplateZonePolicy(
                str(zone.get("id", zone.get("zone_id"))),
                accepted,
                cast(int | None, zone.get("max_sections")),
            )
        )
    settings: list[TemplateSettingRule] = []
    for raw_setting in manifest.get("settings", []):
        setting = cast(Mapping[str, Any], raw_setting)
        settings.append(
            TemplateSettingRule(
                str(setting["key"]),
                TemplateSettingValueType(str(setting["value_type"])),
                cast(Any, deepcopy(setting.get("default"))),
                cast(float | None, setting.get("minimum")),
                cast(float | None, setting.get("maximum")),
                tuple(cast(list[Any], deepcopy(setting.get("choices", [])))),
                cast(tuple[str, Any] | None, deepcopy(setting.get("visible_when"))),
            )
        )
    capabilities = cast(Mapping[str, Any], manifest["capabilities"])
    return TemplatePolicy(
        TemplateRef(template_id, version),
        frozenset(str(value) for value in manifest["supported_locales"]),
        frozenset(PageSize(str(value)) for value in manifest["supported_page_sizes"]),
        frozenset(str(value) for value in manifest["supported_output_formats"]),
        supported_kinds,
        tuple(zones),
        frozenset(str(value) for value in manifest["font_family_tokens"]),
        frozenset(str(value) for value in manifest["date_format_tokens"]),
        frozenset(str(value) for value in manifest["bullet_style_tokens"]),
        tuple(settings),
        bool(capabilities["supports_custom_sections"]),
    )


class _V2ScopeInstaller(Protocol):
    """@brief 在当前 transaction 安装 V2 actor/workspace GUC 的 callable / Callable installing V2 actor/workspace GUCs."""

    async def __call__(
        self,
        *,
        actor_id: str,
        workspace_id: str | None,
    ) -> None:
        """@brief 安装 RLS scope 与 timeout / Install RLS scope and timeouts."""


class _TrackingResumeAuthorizer:
    """@brief 复用集中授权器并记录同事务 actor/workspace / Reuse central authorization and track transaction scope."""

    def __init__(
        self,
        delegate: AccessAuthorizer | None,
        scope_installer: _V2ScopeInstaller | None = None,
        *,
        worker_scope: tuple[WorkspaceId, UserId] | None = None,
    ) -> None:
        """@brief 绑定集中 authorizer 或 durable worker scope / Bind the central authorizer or a durable worker scope.

        @param delegate Access slice 的唯一授权实现 / Sole Access-slice authorization implementation.
        @param worker_scope outbox 提交时冻结的 Workspace/actor / Workspace and actor frozen by the committed outbox event.
        """
        if (delegate is None) is (worker_scope is None):
            raise ValueError("Resume persistence requires exactly one authorization mode")
        self._delegate = delegate
        self._scope_installer = scope_installer
        self._worker_scope = worker_scope
        self.actor_id: UserId | None = None
        self.workspace_id: WorkspaceId | None = None

    async def install_worker_scope(self) -> None:
        """@brief 在任何 worker 业务读取前安装冻结 RLS scope / Install the frozen RLS scope before any worker business read."""
        if self._worker_scope is None or self._scope_installer is None:
            raise PermissionError("Resume worker scope is unavailable")
        workspace_id, actor_id = self._worker_scope
        self.workspace_id = workspace_id
        self.actor_id = actor_id
        await self._scope_installer(
            actor_id=str(actor_id),
            workspace_id=str(workspace_id),
        )

    async def authenticate(self, principal: TokenPrincipal) -> AuthenticatedActor:
        """@brief 认证并固定 actor / Authenticate and pin the actor.

        @param principal 已验证 token 投影 / Verified token projection.
        @return 本地 actor / Local actor.
        """
        if self._delegate is None:
            raise PermissionError("Resume worker scope cannot authenticate public principals")
        actor = await self._delegate.authenticate(principal)
        if self.actor_id is not None and self.actor_id != actor.user_id:
            raise PermissionError("a Resume unit of work cannot switch actors")
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
        """@brief 授权并固定 Workspace / Authorize and pin the Workspace.

        @param actor 已认证 actor / Authenticated actor.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param action 精确 action / Exact action.
        @return 集中授权器签发的上下文 / Context issued by the central authorizer.
        """
        if self._delegate is None:
            raise PermissionError("Resume worker scope cannot authorize public requests")
        context = await self._delegate.authorize(actor, workspace_id, action)
        if self.workspace_id is not None and self.workspace_id != workspace_id:
            raise PermissionError("a Resume unit of work cannot switch workspaces")
        self.workspace_id = workspace_id
        if self._scope_installer is not None:
            await self._scope_installer(
                actor_id=str(actor.user_id),
                workspace_id=str(workspace_id),
            )
        return context

    def require_actor(self) -> UserId:
        """@brief 要求事务已认证 actor / Require an authenticated transaction actor.

        @return actor ID / Actor ID.
        """
        if self.actor_id is None:
            raise PermissionError("Resume persistence requires prior centralized authentication")
        return self.actor_id

    def require_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 要求参数与已授权 Workspace 相同 / Require a parameter to match the authorized Workspace.

        @param workspace_id Repository 方法的 Workspace / Workspace supplied to a repository method.
        """
        if self.workspace_id != workspace_id:
            raise PermissionError("Resume persistence requires prior Workspace authorization")


@dataclass(frozen=True, slots=True)
class InMemoryResumeUpload:
    """@brief 内存 upload session 的最小可领取状态 / Minimal claimable in-memory upload state."""

    workspace_id: WorkspaceId
    upload_session_id: str
    completed_at: datetime
    expires_at: datetime
    claimed_by_job_id: JobId | None = None
    consumed_at: datetime | None = None


@dataclass(slots=True)
class InMemoryResumeStore:
    """@brief Resume V2 的共享进程内状态 / Shared in-process Resume V2 state."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    resumes: dict[tuple[WorkspaceId, ResumeId], ResumeAggregate] = field(default_factory=dict)
    revisions: dict[tuple[WorkspaceId, ResumeId, int], ResumeRevision] = field(default_factory=dict)
    receipts: dict[tuple[WorkspaceId, ResumeId, ResumeBatchId], OperationBatchReceipt] = field(
        default_factory=dict
    )
    proposals: dict[tuple[WorkspaceId, ResumeProposalId], ResumeProposal] = field(
        default_factory=dict
    )
    jobs: dict[JobId, tuple[Job, ResumeJobSpec]] = field(default_factory=dict)
    outbox_events: dict[str, ResumeOutboxEvent] = field(default_factory=dict)
    uploads: dict[str, InMemoryResumeUpload] = field(default_factory=dict)

    def add_completed_upload(
        self,
        workspace_id: WorkspaceId,
        upload_session_id: str,
        *,
        completed_at: datetime,
        expires_at: datetime,
    ) -> None:
        """@brief 由 upload slice 测试夹具登记 completed session / Register a completed session from an upload fixture.

        @param workspace_id upload 所属 Workspace / Owning Workspace.
        @param upload_session_id upload ID / Upload ID.
        @param completed_at 完成时刻 / Completion instant.
        @param expires_at 到期时刻 / Expiry instant.
        """
        if completed_at.tzinfo is None or expires_at.tzinfo is None or expires_at <= completed_at:
            raise ValueError("completed upload timestamps must be aware and ordered")
        self.uploads[upload_session_id] = InMemoryResumeUpload(
            workspace_id,
            upload_session_id,
            completed_at,
            expires_at,
        )


class InMemoryResumeRepository:
    """@brief copy-on-write snapshot 上的 Resume repository / Resume repository over a copy-on-write snapshot."""

    def __init__(
        self,
        resumes: dict[tuple[WorkspaceId, ResumeId], ResumeAggregate],
        revisions: dict[tuple[WorkspaceId, ResumeId, int], ResumeRevision],
        receipts: dict[tuple[WorkspaceId, ResumeId, ResumeBatchId], OperationBatchReceipt],
        proposals: dict[tuple[WorkspaceId, ResumeProposalId], ResumeProposal],
    ) -> None:
        """@brief 绑定隔离状态 / Bind isolated state."""
        self.resumes = resumes
        self.revisions = revisions
        self.receipts = receipts
        self.proposals = proposals

    async def list_resumes(
        self, workspace_id: WorkspaceId, page: PageRequest
    ) -> CollectionPage[ResumeSummary]:
        """@brief 按不透明 ID keyset 列出 Resume / List Resumes using an opaque-ID keyset."""
        items = sorted(
            (
                aggregate.document.summary()
                for (owner, _), aggregate in self.resumes.items()
                if owner == workspace_id
            ),
            key=lambda item: str(item.meta.id),
        )
        selected = [item for item in items if page.after is None or str(item.meta.id) > page.after]
        window = selected[: page.limit + 1]
        has_more = len(window) > page.limit
        result = tuple(window[: page.limit])
        return CollectionPage(result, str(result[-1].meta.id) if result and has_more else None)

    async def get_resume(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        for_update: bool = False,
    ) -> ResumeAggregate | None:
        """@brief 读取 Workspace Resume / Read a Workspace Resume."""
        del for_update
        return self.resumes.get((workspace_id, resume_id))

    async def add_resume(self, aggregate: ResumeAggregate, revision: ResumeRevision) -> None:
        """@brief 添加聚合与首版快照 / Add an aggregate and first snapshot."""
        key = (aggregate.document.workspace_id, aggregate.document.meta.id)
        if key in self.resumes or revision.revision != 1:
            raise ResumeCasMismatch
        self.resumes[key] = aggregate
        self.revisions[(*key, revision.revision)] = revision

    async def save_resume(
        self,
        aggregate: ResumeAggregate,
        revision: ResumeRevision,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以最终 CAS 保存聚合与 append-only revision / Save with final CAS and append-only revision."""
        key = (aggregate.document.workspace_id, aggregate.document.meta.id)
        current = self.resumes.get(key)
        if (
            current is None
            or current.document.meta.revision != expected_revision
            or aggregate.document.meta.revision != expected_revision + 1
            or (*key, revision.revision) in self.revisions
        ):
            raise ResumeCasMismatch
        self.resumes[key] = aggregate
        self.revisions[(*key, revision.revision)] = revision

    async def delete_resume(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以 CAS 删除当前聚合但保留 revision history / Delete current aggregate via CAS, retaining history."""
        key = (workspace_id, resume_id)
        current = self.resumes.get(key)
        if current is None or current.document.meta.revision != expected_revision:
            raise ResumeCasMismatch
        del self.resumes[key]

    async def list_revisions(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        page: PageRequest,
    ) -> CollectionPage[ResumeRevisionSummary]:
        """@brief 按 revision 升序分页 / Page revisions in ascending order."""
        items = sorted(
            (
                revision.summary()
                for (owner, target, _), revision in self.revisions.items()
                if owner == workspace_id and target == resume_id
            ),
            key=lambda item: item.revision,
        )
        offset = _page_offset(page.after)
        window = items[offset : offset + page.limit + 1]
        has_more = len(window) > page.limit
        result = tuple(window[: page.limit])
        return CollectionPage(result, str(offset + len(result)) if has_more else None)

    async def get_revision(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        revision: int,
    ) -> ResumeRevision | None:
        """@brief 读取不可变 revision / Read an immutable revision."""
        return self.revisions.get((workspace_id, resume_id, revision))

    async def get_batch_receipt(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        batch_id: ResumeBatchId,
    ) -> OperationBatchReceipt | None:
        """@brief 读取精确 batch receipt / Read an exact batch receipt."""
        return self.receipts.get((workspace_id, resume_id, batch_id))

    async def add_batch_receipt(self, receipt: OperationBatchReceipt) -> None:
        """@brief 添加不可变 batch receipt / Add an immutable batch receipt."""
        key = (receipt.workspace_id, receipt.resume_id, receipt.batch_id)
        if key in self.receipts:
            raise ResumeCasMismatch
        self.receipts[key] = receipt

    async def list_proposals(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        page: PageRequest,
    ) -> CollectionPage[ResumeProposal]:
        """@brief 以 ID keyset 列出 proposal / List proposals with an ID keyset."""
        items = sorted(
            (
                proposal
                for (owner, _), proposal in self.proposals.items()
                if owner == workspace_id and proposal.resume_id == resume_id
            ),
            key=lambda item: str(item.meta.id),
        )
        selected = [item for item in items if page.after is None or str(item.meta.id) > page.after]
        window = selected[: page.limit + 1]
        has_more = len(window) > page.limit
        result = tuple(window[: page.limit])
        return CollectionPage(result, str(result[-1].meta.id) if result and has_more else None)

    async def get_proposal(
        self,
        workspace_id: WorkspaceId,
        proposal_id: ResumeProposalId,
        *,
        for_update: bool = False,
    ) -> ResumeProposal | None:
        """@brief 读取 Workspace proposal / Read a Workspace proposal."""
        del for_update
        return self.proposals.get((workspace_id, proposal_id))

    async def save_proposal(self, proposal: ResumeProposal, *, expected_revision: int) -> None:
        """@brief 以 CAS 保存 proposal decision / Save a proposal decision via CAS."""
        key = (proposal.workspace_id, proposal.meta.id)
        current = self.proposals.get(key)
        if (
            current is None
            or current.meta.revision != expected_revision
            or proposal.meta.revision != expected_revision + 1
        ):
            raise ResumeCasMismatch
        self.proposals[key] = proposal


class _MemoryImportSourceVerifier:
    """@brief 内存 upload session 条件领取器 / Conditional in-memory upload-session claimer."""

    def __init__(
        self,
        uploads: dict[str, InMemoryResumeUpload],
        authorizer: _TrackingResumeAuthorizer,
    ) -> None:
        """@brief 绑定事务 upload 快照 / Bind the transactional upload snapshot."""
        self._uploads = uploads
        self._authorizer = authorizer

    async def claim(
        self,
        workspace_id: WorkspaceId,
        upload_session_id: str,
        job_id: JobId,
    ) -> bool:
        """@brief 仅一次领取 completed 且未过期的 upload / Claim a completed, live upload exactly once."""
        self._authorizer.require_workspace(workspace_id)
        upload = self._uploads.get(upload_session_id)
        now = datetime.now(UTC)
        if (
            upload is None
            or upload.workspace_id != workspace_id
            or upload.expires_at <= now
            or upload.claimed_by_job_id is not None
        ):
            return False
        self._uploads[upload_session_id] = InMemoryResumeUpload(
            upload.workspace_id,
            upload.upload_session_id,
            upload.completed_at,
            upload.expires_at,
            job_id,
            now,
        )
        return True


class _MemoryResumeJobSink:
    """@brief 事务内存 Job sink / Transactional in-memory Job sink."""

    def __init__(
        self,
        jobs: dict[JobId, tuple[Job, ResumeJobSpec]],
        authorizer: _TrackingResumeAuthorizer,
    ) -> None:
        """@brief 绑定 Job 快照 / Bind the Job snapshot."""
        self._jobs = jobs
        self._authorizer = authorizer

    async def add(self, job: Job, spec: ResumeJobSpec) -> None:
        """@brief 添加唯一统一 Job 与私有 worker spec / Add one unified job and private worker spec."""
        self._authorizer.require_workspace(job.workspace_id)
        self._authorizer.require_actor()
        if job.meta.id in self._jobs:
            raise ResumeCasMismatch
        self._jobs[job.meta.id] = (job, spec)


class _MemoryResumeOutbox:
    """@brief 事务内存 outbox / Transactional in-memory outbox."""

    def __init__(
        self,
        events: dict[str, ResumeOutboxEvent],
        authorizer: _TrackingResumeAuthorizer,
    ) -> None:
        """@brief 绑定 outbox 快照 / Bind the outbox snapshot."""
        self._events = events
        self._authorizer = authorizer

    async def add(self, event: ResumeOutboxEvent) -> None:
        """@brief 添加唯一 outbox event / Add one unique outbox event."""
        self._authorizer.require_workspace(event.workspace_id)
        if self._authorizer.require_actor() != event.actor_id:
            raise PermissionError("outbox actor does not match authenticated actor")
        if event.event_id in self._events:
            raise ResumeCasMismatch
        self._events[event.event_id] = event


class InMemoryResumeUnitOfWork:
    """@brief Access 与 Resume 状态一致锁定的 copy-on-write UoW / Copy-on-write UoW locking Access and Resume consistently."""

    def __init__(
        self,
        store: InMemoryResumeStore,
        access_store: InMemoryAccessStore,
        templates: ResumeTemplateCatalog,
    ) -> None:
        """@brief 绑定共享状态和不可变 catalog / Bind shared state and immutable catalog.

        @param store Resume 共享状态 / Shared Resume state.
        @param access_store 身份与成员共享状态 / Shared identity and membership state.
        @param templates 不可变模板 catalog / Immutable template catalog.
        """
        self._store = store
        self._access_store = access_store
        self._templates = templates
        self._repository: InMemoryResumeRepository | None = None
        self._authorizer: _TrackingResumeAuthorizer | None = None
        self._import_sources: _MemoryImportSourceVerifier | None = None
        self._jobs: _MemoryResumeJobSink | None = None
        self._outbox: _MemoryResumeOutbox | None = None
        self._resume_snapshot: (
            tuple[
                dict[tuple[WorkspaceId, ResumeId], ResumeAggregate],
                dict[tuple[WorkspaceId, ResumeId, int], ResumeRevision],
                dict[tuple[WorkspaceId, ResumeId, ResumeBatchId], OperationBatchReceipt],
                dict[tuple[WorkspaceId, ResumeProposalId], ResumeProposal],
                dict[JobId, tuple[Job, ResumeJobSpec]],
                dict[str, ResumeOutboxEvent],
                dict[str, InMemoryResumeUpload],
            ]
            | None
        ) = None
        self._entered = False
        self._committed = False
        self._rolled_back = False

    @property
    def repository(self) -> InMemoryResumeRepository:
        """@brief 返回事务 repository / Return the transactional repository."""
        if self._repository is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._repository

    @property
    def authorizer(self) -> _TrackingResumeAuthorizer:
        """@brief 返回集中授权 wrapper / Return the central-authorization wrapper."""
        if self._authorizer is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._authorizer

    @property
    def templates(self) -> ResumeTemplateCatalog:
        """@brief 返回不可变模板 catalog / Return the immutable template catalog."""
        return self._templates

    @property
    def import_sources(self) -> _MemoryImportSourceVerifier:
        """@brief 返回 upload claim adapter / Return the upload-claim adapter."""
        if self._import_sources is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._import_sources

    @property
    def jobs(self) -> _MemoryResumeJobSink:
        """@brief 返回事务 Job sink / Return the transactional Job sink."""
        if self._jobs is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._jobs

    @property
    def outbox(self) -> _MemoryResumeOutbox:
        """@brief 返回事务 outbox / Return the transactional outbox."""
        if self._outbox is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._outbox

    async def __aenter__(self) -> Self:
        """@brief 以固定顺序锁定 Access 和 Resume 并复制状态 / Lock Access then Resume and copy state."""
        if self._entered:
            raise RuntimeError("Resume unit of work cannot be re-entered")
        await self._access_store.lock.acquire()
        try:
            await self._store.lock.acquire()
        except BaseException:
            self._access_store.lock.release()
            raise
        self._entered = True
        resumes = deepcopy(self._store.resumes)
        revisions = deepcopy(self._store.revisions)
        receipts = deepcopy(self._store.receipts)
        proposals = deepcopy(self._store.proposals)
        jobs = deepcopy(self._store.jobs)
        events = deepcopy(self._store.outbox_events)
        uploads = deepcopy(self._store.uploads)
        self._resume_snapshot = (
            resumes,
            revisions,
            receipts,
            proposals,
            jobs,
            events,
            uploads,
        )
        access_repository = InMemoryAccessRepository(
            users=dict(self._access_store.users),
            workspaces=dict(self._access_store.workspaces),
            memberships=dict(self._access_store.memberships),
            invitations=dict(self._access_store.invitations),
            account_deletions=dict(self._access_store.account_deletions),
        )
        self._authorizer = _TrackingResumeAuthorizer(AccessAuthorizer(access_repository))
        self._repository = InMemoryResumeRepository(
            resumes,
            revisions,
            receipts,
            proposals,
        )
        self._import_sources = _MemoryImportSourceVerifier(uploads, self._authorizer)
        self._jobs = _MemoryResumeJobSink(jobs, self._authorizer)
        self._outbox = _MemoryResumeOutbox(events, self._authorizer)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 回滚未提交快照并释放两把锁 / Roll back uncommitted state and release both locks."""
        del exc, traceback
        if self._entered:
            if exc_type is not None or not self._committed:
                await self.rollback()
            self._clear_adapters()
            self._entered = False
            self._store.lock.release()
            self._access_store.lock.release()
        return None

    async def commit(self) -> None:
        """@brief 原子发布完整 Resume snapshot / Atomically publish the complete Resume snapshot."""
        if self._resume_snapshot is None or not self._entered:
            raise RuntimeError("Resume unit of work has not been entered")
        if self._committed:
            raise RuntimeError("Resume unit of work is already committed")
        if self._rolled_back:
            raise RuntimeError("rolled-back Resume unit of work cannot commit")
        (
            self._store.resumes,
            self._store.revisions,
            self._store.receipts,
            self._store.proposals,
            self._store.jobs,
            self._store.outbox_events,
            self._store.uploads,
        ) = self._resume_snapshot
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等丢弃未发布快照 / Idempotently discard the unpublished snapshot."""
        if not self._entered:
            raise RuntimeError("Resume unit of work has not been entered")
        self._rolled_back = True

    def _clear_adapters(self) -> None:
        """@brief 清除只能在事务中使用的 adapter / Clear adapters valid only inside the transaction."""
        self._repository = None
        self._authorizer = None
        self._import_sources = None
        self._jobs = None
        self._outbox = None
        self._resume_snapshot = None


class InMemoryResumeUnitOfWorkFactory:
    """@brief 创建共享状态的内存 Resume UoW / Create in-memory Resume UoWs over shared state."""

    def __init__(
        self,
        access_store: InMemoryAccessStore,
        *,
        store: InMemoryResumeStore | None = None,
        templates: ResumeTemplateCatalog | None = None,
    ) -> None:
        """@brief 绑定 Access state、Resume state 与 catalog / Bind Access state, Resume state, and catalog.

        @param access_store 必须与 identity provisioning 共用的状态 / State shared with identity provisioning.
        @param store 可选 Resume 状态 / Optional Resume state.
        @param templates 可选 immutable catalog / Optional immutable catalog.
        """
        self.access_store = access_store
        self.store = store or InMemoryResumeStore()
        self.templates = templates or BuiltinResumeTemplateCatalog()

    def __call__(self) -> InMemoryResumeUnitOfWork:
        """@brief 创建未进入的内存 UoW / Create a not-yet-entered in-memory UoW."""
        return InMemoryResumeUnitOfWork(self.store, self.access_store, self.templates)


class PostgresResumeRepository:
    """@brief 绑定一个 PostgreSQL 事务的 Resume repository / Resume repository bound to one PostgreSQL transaction."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingResumeAuthorizer,
    ) -> None:
        """@brief 绑定 Session 与已跟踪的授权上下文 / Bind the Session and tracked authorization context."""
        self._session = session
        self._authorizer = authorizer
        self._pending_aggregate: ResumeAggregate | None = None
        self._current_aggregate: ResumeAggregate | None = None

    async def list_resumes(
        self, workspace_id: WorkspaceId, page: PageRequest
    ) -> CollectionPage[ResumeSummary]:
        """@brief 按 ID keyset 列出未删除 Resume / List live Resumes by ID keyset."""
        self._authorizer.require_workspace(workspace_id)
        statement = (
            select(ResumeDocumentRecord)
            .where(
                ResumeDocumentRecord.workspace_id == str(workspace_id),
                ResumeDocumentRecord.deleted_at.is_(None),
            )
            .order_by(ResumeDocumentRecord.id)
            .limit(page.limit + 1)
        )
        if page.after is not None:
            statement = statement.where(ResumeDocumentRecord.id > page.after)
        records = list((await self._session.scalars(statement)).all())
        has_more = len(records) > page.limit
        selected = records[: page.limit]
        items = tuple(_summary_from_document_record(record) for record in selected)
        return CollectionPage(items, selected[-1].id if selected and has_more else None)

    async def get_resume(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        for_update: bool = False,
    ) -> ResumeAggregate | None:
        """@brief 读取并完整 rehydrate Resume 聚合 / Read and fully rehydrate a Resume aggregate."""
        self._authorizer.require_workspace(workspace_id)
        statement = select(ResumeDocumentRecord).where(
            ResumeDocumentRecord.workspace_id == str(workspace_id),
            ResumeDocumentRecord.id == str(resume_id),
            ResumeDocumentRecord.deleted_at.is_(None),
        )
        if for_update:
            statement = statement.with_for_update()
        record = await self._session.scalar(statement)
        if record is None:
            return None
        aggregate = await self._rehydrate(record)
        self._current_aggregate = aggregate
        return aggregate

    async def add_resume(self, aggregate: ResumeAggregate, revision: ResumeRevision) -> None:
        """@brief 添加 Resume 根与首个不可变 revision / Add a Resume root and first immutable revision."""
        document = aggregate.document
        self._authorizer.require_workspace(document.workspace_id)
        actor_id = self._authorizer.require_actor()
        if document.meta.revision != 1 or revision.revision != 1:
            raise ResumeCasMismatch
        self._session.add(
            ResumeDocumentRecord(
                id=str(document.meta.id),
                workspace_id=str(document.workspace_id),
                resource_owner_id=str(actor_id),
                template_version_id=None,
                template_id=document.template.template_id,
                template_version=document.template.version,
                title=document.title,
                locale=document.locale,
                current_revision_no=1,
                created_at=document.meta.created_at,
                updated_at=document.meta.updated_at,
                revision=1,
                extensions={},
            )
        )
        self._session.add(_revision_record(revision, aggregate, actor_id))

    async def save_resume(
        self,
        aggregate: ResumeAggregate,
        revision: ResumeRevision,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 使用 affected-row CAS 更新根并追加 revision / Update the root by affected-row CAS and append a revision."""
        document = aggregate.document
        self._authorizer.require_workspace(document.workspace_id)
        actor_id = self._authorizer.require_actor()
        if (
            document.meta.revision != expected_revision + 1
            or revision.revision != document.meta.revision
        ):
            raise ResumeCasMismatch
        result = await self._session.execute(
            update(ResumeDocumentRecord)
            .where(
                ResumeDocumentRecord.workspace_id == str(document.workspace_id),
                ResumeDocumentRecord.id == str(document.meta.id),
                ResumeDocumentRecord.current_revision_no == expected_revision,
                ResumeDocumentRecord.revision == expected_revision,
                ResumeDocumentRecord.deleted_at.is_(None),
            )
            .values(
                template_id=document.template.template_id,
                template_version=document.template.version,
                title=document.title,
                locale=document.locale,
                current_revision_no=document.meta.revision,
                updated_at=document.meta.updated_at,
                revision=document.meta.revision,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise ResumeCasMismatch
        self._session.add(_revision_record(revision, aggregate, actor_id))
        self._pending_aggregate = aggregate
        self._current_aggregate = aggregate

    async def delete_resume(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 软删除 Resume 根且保留审计 revision / Soft-delete the Resume root while retaining audit revisions."""
        self._authorizer.require_workspace(workspace_id)
        now = datetime.now(UTC)
        result = await self._session.execute(
            update(ResumeDocumentRecord)
            .where(
                ResumeDocumentRecord.workspace_id == str(workspace_id),
                ResumeDocumentRecord.id == str(resume_id),
                ResumeDocumentRecord.current_revision_no == expected_revision,
                ResumeDocumentRecord.revision == expected_revision,
                ResumeDocumentRecord.deleted_at.is_(None),
            )
            .values(deleted_at=now, updated_at=now)
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise ResumeCasMismatch

    async def list_revisions(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        page: PageRequest,
    ) -> CollectionPage[ResumeRevisionSummary]:
        """@brief 以 offset position 列出 append-only revisions / List append-only revisions with an offset position."""
        self._authorizer.require_workspace(workspace_id)
        offset = _page_offset(page.after)
        records = list(
            (
                await self._session.scalars(
                    select(ResumeRevisionRecord)
                    .where(
                        ResumeRevisionRecord.workspace_id == str(workspace_id),
                        ResumeRevisionRecord.resume_id == str(resume_id),
                    )
                    .order_by(ResumeRevisionRecord.revision_no)
                    .offset(offset)
                    .limit(page.limit + 1)
                )
            ).all()
        )
        has_more = len(records) > page.limit
        selected = records[: page.limit]
        items = tuple(_revision_summary_from_record(record) for record in selected)
        return CollectionPage(items, str(offset + len(items)) if has_more else None)

    async def get_revision(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        revision: int,
    ) -> ResumeRevision | None:
        """@brief 读取并校验一个不可变 revision / Read and verify one immutable revision."""
        self._authorizer.require_workspace(workspace_id)
        record = await self._session.scalar(
            select(ResumeRevisionRecord).where(
                ResumeRevisionRecord.workspace_id == str(workspace_id),
                ResumeRevisionRecord.resume_id == str(resume_id),
                ResumeRevisionRecord.revision_no == revision,
            )
        )
        return _revision_from_record(record) if record is not None else None

    async def get_batch_receipt(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        batch_id: ResumeBatchId,
    ) -> OperationBatchReceipt | None:
        """@brief 读取尚在保留期内的精确 receipt / Read an exact receipt still within retention."""
        self._authorizer.require_workspace(workspace_id)
        record = await self._session.scalar(
            select(ResumeOperationBatchRecord).where(
                ResumeOperationBatchRecord.workspace_id == str(workspace_id),
                ResumeOperationBatchRecord.resume_id == str(resume_id),
                ResumeOperationBatchRecord.client_batch_id == str(batch_id),
                ResumeOperationBatchRecord.request_fingerprint.is_not(None),
                ResumeOperationBatchRecord.outcome.is_not(None),
                ResumeOperationBatchRecord.expires_at > datetime.now(UTC),
            )
        )
        if record is None or record.request_fingerprint is None or record.outcome is None:
            return None
        return OperationBatchReceipt(
            workspace_id,
            resume_id,
            batch_id,
            record.request_fingerprint,
            _load_object(_OUTCOME_ADAPTER, record.outcome),
            record.created_at,
        )

    async def add_batch_receipt(self, receipt: OperationBatchReceipt) -> None:
        """@brief 持久化精确 outcome 并将新 ledger entries 绑定到 batch / Persist an exact outcome and bind new ledger entries to its batch."""
        self._authorizer.require_workspace(receipt.workspace_id)
        actor_id = self._authorizer.require_actor()
        batch_row_id = _row_id("rbat")
        current_revision = receipt.outcome.resume.meta.revision
        self._session.add(
            ResumeOperationBatchRecord(
                id=batch_row_id,
                workspace_id=str(receipt.workspace_id),
                resource_owner_id=str(actor_id),
                resume_id=str(receipt.resume_id),
                client_batch_id=str(receipt.batch_id),
                base_revision_no=None,
                applied_revision_no=current_revision,
                conflict_strategy=None,
                status="applied",
                request_fingerprint=receipt.request_fingerprint,
                outcome=_dump_object(_OUTCOME_ADAPTER, receipt.outcome),
                expires_at=receipt.created_at + _RECEIPT_RETENTION,
                created_at=receipt.created_at,
                updated_at=receipt.created_at,
                revision=1,
                extensions={},
            )
        )
        if self._pending_aggregate is None:
            return
        existing = set(
            await self._session.scalars(
                select(ResumeOperationRecord.operation_id).where(
                    ResumeOperationRecord.operation_id.in_(
                        [
                            str(item.operation_id)
                            for item in self._pending_aggregate.operation_ledger
                        ]
                    )
                )
            )
        )
        fresh = [
            item
            for item in self._pending_aggregate.operation_ledger
            if item.applied_revision == current_revision and str(item.operation_id) not in existing
        ]
        for ordinal, item in enumerate(fresh):
            self._session.add(
                ResumeOperationRecord(
                    id=_row_id("rop"),
                    workspace_id=str(receipt.workspace_id),
                    resource_owner_id=str(actor_id),
                    batch_id=batch_row_id,
                    operation_id=str(item.operation_id),
                    ordinal=ordinal,
                    operation_type="ledger",
                    payload={},
                    fingerprint=item.fingerprint,
                    applied_revision_no=item.applied_revision,
                    created_at=receipt.created_at,
                    updated_at=receipt.created_at,
                    revision=1,
                    extensions={},
                )
            )
        self._pending_aggregate = None

    async def list_proposals(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        page: PageRequest,
    ) -> CollectionPage[ResumeProposal]:
        """@brief 以 ID keyset 列出 proposal / List proposals using an ID keyset."""
        self._authorizer.require_workspace(workspace_id)
        statement = (
            select(ResumeProposalRecord)
            .where(
                ResumeProposalRecord.workspace_id == str(workspace_id),
                ResumeProposalRecord.resume_id == str(resume_id),
            )
            .order_by(ResumeProposalRecord.id)
            .limit(page.limit + 1)
        )
        if page.after is not None:
            statement = statement.where(ResumeProposalRecord.id > page.after)
        records = list((await self._session.scalars(statement)).all())
        has_more = len(records) > page.limit
        selected = records[: page.limit]
        items = tuple([await self._proposal_from_record(record) for record in selected])
        return CollectionPage(items, selected[-1].id if selected and has_more else None)

    async def get_proposal(
        self,
        workspace_id: WorkspaceId,
        proposal_id: ResumeProposalId,
        *,
        for_update: bool = False,
    ) -> ResumeProposal | None:
        """@brief 读取 proposal；决策路径获取行锁 / Read a proposal, locking on decision paths."""
        self._authorizer.require_workspace(workspace_id)
        statement = select(ResumeProposalRecord).where(
            ResumeProposalRecord.workspace_id == str(workspace_id),
            ResumeProposalRecord.id == str(proposal_id),
        )
        if for_update:
            statement = statement.with_for_update()
        record = await self._session.scalar(statement)
        return await self._proposal_from_record(record) if record is not None else None

    async def save_proposal(self, proposal: ResumeProposal, *, expected_revision: int) -> None:
        """@brief 以 revision+pending predicate CAS 保存 proposal decision / Save a proposal decision with revision and pending-state CAS."""
        self._authorizer.require_workspace(proposal.workspace_id)
        if proposal.meta.revision != expected_revision + 1:
            raise ResumeCasMismatch
        decided_at = (
            proposal.meta.updated_at
            if proposal.status is not ResumeProposalStatus.PENDING
            else None
        )
        result = await self._session.execute(
            update(ResumeProposalRecord)
            .where(
                ResumeProposalRecord.workspace_id == str(proposal.workspace_id),
                ResumeProposalRecord.id == str(proposal.meta.id),
                ResumeProposalRecord.revision == expected_revision,
                ResumeProposalRecord.status == ResumeProposalStatus.PENDING.value,
            )
            .values(
                status=proposal.status.value,
                decision_payload={
                    "accepted_operation_ids": [
                        str(item) for item in proposal.accepted_operation_ids
                    ]
                },
                decided_by_actor_id=(
                    str(proposal.decided_by) if proposal.decided_by is not None else None
                ),
                decided_at=decided_at,
                updated_at=proposal.meta.updated_at,
                revision=proposal.meta.revision,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise ResumeCasMismatch
        accepted = {str(item) for item in proposal.accepted_operation_ids}
        operation_records = list(
            (
                await self._session.scalars(
                    select(ResumeProposalOperationRecord).where(
                        ResumeProposalOperationRecord.proposal_id == str(proposal.meta.id)
                    )
                )
            ).all()
        )
        for operation in operation_records:
            if operation.operation_id in accepted:
                operation.decision = "accepted"
                aggregate = self._pending_aggregate or self._current_aggregate
                if aggregate is None:
                    raise ValueError("accepted proposal lacks its same-transaction Resume change")
                ledger = next(
                    (
                        entry
                        for entry in aggregate.operation_ledger
                        if str(entry.operation_id) == operation.operation_id
                    ),
                    None,
                )
                if ledger is None:
                    raise ValueError("accepted proposal operation is absent from Resume ledger")
                operation.fingerprint = ledger.fingerprint
                operation.applied_revision_no = ledger.applied_revision
            else:
                operation.decision = "rejected"
        self._pending_aggregate = None

    async def _rehydrate(self, record: ResumeDocumentRecord) -> ResumeAggregate:
        """@brief 从 snapshot、ledger 与 change targets 重建聚合 / Rebuild an aggregate from snapshot, ledger, and change targets."""
        revision_record = await self._session.scalar(
            select(ResumeRevisionRecord).where(
                ResumeRevisionRecord.workspace_id == record.workspace_id,
                ResumeRevisionRecord.resume_id == record.id,
                ResumeRevisionRecord.revision_no == record.current_revision_no,
            )
        )
        if revision_record is None:
            raise ValueError("Resume root has no matching current revision")
        document = _document_from_revision(revision_record)
        if (
            str(document.meta.id) != record.id
            or str(document.workspace_id) != record.workspace_id
            or document.meta.revision != record.current_revision_no
            or document.title != record.title
            or document.locale != record.locale
            or document.template != TemplateRef(record.template_id, record.template_version)
        ):
            raise ValueError("Resume root metadata diverges from its current snapshot")
        offline_rows = (
            await self._session.execute(
                select(
                    ResumeOperationRecord.operation_id,
                    ResumeOperationRecord.fingerprint,
                    ResumeOperationRecord.applied_revision_no,
                )
                .join(
                    ResumeOperationBatchRecord,
                    ResumeOperationBatchRecord.id == ResumeOperationRecord.batch_id,
                )
                .where(
                    ResumeOperationBatchRecord.workspace_id == record.workspace_id,
                    ResumeOperationBatchRecord.resume_id == record.id,
                )
            )
        ).all()
        proposal_rows = (
            await self._session.execute(
                select(
                    ResumeProposalOperationRecord.operation_id,
                    ResumeProposalOperationRecord.fingerprint,
                    ResumeProposalOperationRecord.applied_revision_no,
                )
                .join(
                    ResumeProposalRecord,
                    ResumeProposalRecord.id == ResumeProposalOperationRecord.proposal_id,
                )
                .where(
                    ResumeProposalRecord.workspace_id == record.workspace_id,
                    ResumeProposalRecord.resume_id == record.id,
                    ResumeProposalOperationRecord.applied_revision_no.is_not(None),
                )
            )
        ).all()
        ledger_rows = cast(
            list[tuple[str, str, int | None]],
            [tuple(row) for row in offline_rows],
        )
        ledger_rows.extend(
            cast(
                list[tuple[str, str, int | None]],
                [tuple(row) for row in proposal_rows],
            )
        )
        ledger_by_id: dict[str, OperationLedgerEntry] = {}
        for operation_id, fingerprint, applied_revision in ledger_rows:
            if not isinstance(applied_revision, int):
                raise ValueError("persisted Resume ledger lacks applied revision")
            entry = OperationLedgerEntry(
                ResumeOperationId(operation_id),
                fingerprint,
                applied_revision,
            )
            previous = ledger_by_id.get(operation_id)
            if previous is not None and previous != entry:
                raise ValueError("persisted Resume ledger contains conflicting operation IDs")
            ledger_by_id[operation_id] = entry
        revision_rows = list(
            (
                await self._session.scalars(
                    select(ResumeRevisionRecord)
                    .where(
                        ResumeRevisionRecord.workspace_id == record.workspace_id,
                        ResumeRevisionRecord.resume_id == record.id,
                    )
                    .order_by(ResumeRevisionRecord.revision_no)
                )
            ).all()
        )
        changes = tuple(
            _load_change_targets(item.revision_no, item.change_targets)
            for item in revision_rows
            if item.change_targets
        )
        return ResumeAggregate(
            document,
            tuple(sorted(ledger_by_id.values(), key=lambda item: str(item.operation_id))),
            changes,
        )

    async def _proposal_from_record(self, record: ResumeProposalRecord) -> ResumeProposal:
        """@brief 从 proposal 根与有序 operations 重建聚合 / Rebuild a proposal from its root and ordered operations."""
        operations = list(
            (
                await self._session.scalars(
                    select(ResumeProposalOperationRecord)
                    .where(
                        ResumeProposalOperationRecord.workspace_id == record.workspace_id,
                        ResumeProposalOperationRecord.proposal_id == record.id,
                    )
                    .order_by(ResumeProposalOperationRecord.ordinal)
                )
            ).all()
        )
        decision_payload = record.decision_payload or {}
        accepted_raw = decision_payload.get("accepted_operation_ids", [])
        if not isinstance(accepted_raw, list) or not all(
            isinstance(item, str) for item in accepted_raw
        ):
            raise ValueError("persisted proposal decision payload is invalid")
        return ResumeProposal(
            _resource_meta(record, ResumeProposalId(record.id)),
            WorkspaceId(record.workspace_id),
            ResumeId(record.resume_id),
            record.base_revision_no,
            record.title,
            ResumeProposalStatus(record.status),
            tuple(_load_object(_OPERATION_ADAPTER, item.payload) for item in operations),
            _load_object(_RESOURCE_REFS_ADAPTER, record.evidence_refs),
            record.expires_at,
            UserId(record.decided_by_actor_id) if record.decided_by_actor_id else None,
            tuple(ResumeOperationId(item) for item in accepted_raw),
        )


class _PostgresImportSourceVerifier:
    """@brief 使用单条条件 UPDATE 的 PostgreSQL upload claimer / PostgreSQL upload claimer using one conditional UPDATE."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingResumeAuthorizer,
    ) -> None:
        """@brief 绑定事务 Session / Bind the transactional Session."""
        self._session = session
        self._authorizer = authorizer

    async def claim(
        self,
        workspace_id: WorkspaceId,
        upload_session_id: str,
        job_id: JobId,
    ) -> bool:
        """@brief 原子领取 completed、未过期且未消费的 upload / Atomically claim a completed, live, unconsumed upload."""
        self._authorizer.require_workspace(workspace_id)
        now = datetime.now(UTC)
        result = await self._session.execute(
            update(ResumeImportUploadSessionRecord)
            .where(
                ResumeImportUploadSessionRecord.workspace_id == str(workspace_id),
                ResumeImportUploadSessionRecord.id == upload_session_id,
                ResumeImportUploadSessionRecord.status == "completed",
                ResumeImportUploadSessionRecord.completed_at.is_not(None),
                ResumeImportUploadSessionRecord.expires_at > now,
                ResumeImportUploadSessionRecord.claimed_by_type.is_(None),
                ResumeImportUploadSessionRecord.claimed_by_id.is_(None),
                ResumeImportUploadSessionRecord.claimed_by_job_id.is_(None),
                ResumeImportUploadSessionRecord.consumed_at.is_(None),
            )
            .values(
                claimed_by_type="job",
                claimed_by_id=str(job_id),
                claimed_by_revision=1,
                claimed_by_job_id=str(job_id),
                consumed_at=now,
                updated_at=now,
                revision=ResumeImportUploadSessionRecord.revision + 1,
            )
            .execution_options(synchronize_session=False)
        )
        return _affected_rows(result) == 1


class _PostgresResumeJobSink:
    """@brief 与 Resume 变更共事务的 PostgreSQL Job sink / PostgreSQL Job sink sharing the Resume transaction."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingResumeAuthorizer,
    ) -> None:
        """@brief 绑定事务 Session / Bind the transactional Session."""
        self._session = session
        self._authorizer = authorizer

    async def add(self, job: Job, spec: ResumeJobSpec) -> None:
        """@brief 写入统一 agent.jobs 表及私有 worker spec / Write the unified agent.jobs row and private worker spec."""
        self._authorizer.require_workspace(job.workspace_id)
        actor_id = self._authorizer.require_actor()
        if job.status is not JobStatus.QUEUED or job.meta.revision != 1:
            raise ResumeCasMismatch
        self._session.add(
            JobRecord(
                id=str(job.meta.id),
                workspace_id=str(job.workspace_id),
                resource_owner_id=str(actor_id),
                job_type=job.kind,
                status=job.status.value,
                phase="queued",
                completed_units=0,
                total_units=None,
                progress_unit=JobProgressUnit.UNKNOWN.value,
                target_resource_type=job.subject.resource_type,
                target_resource_id=job.subject.id,
                target_resource_revision=job.subject.revision,
                result_refs=[],
                problem=None,
                started_at=None,
                finished_at=None,
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

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> PersistedResumeJob | None:
        """@brief Workspace-first 读取 Job 与私有 spec / Read a Job and private spec Workspace-first."""
        self._authorizer.require_workspace(workspace_id)
        actor_id = self._authorizer.require_actor()
        statement = select(JobRecord).where(
            JobRecord.workspace_id == str(workspace_id),
            JobRecord.resource_owner_id == str(actor_id),
            JobRecord.id == str(job_id),
            JobRecord.job_type.like("resume.%"),
        )
        if for_update:
            statement = statement.with_for_update()
        record = await self._session.scalar(statement)
        if record is None:
            return None
        job = _resume_job_from_record(record)
        payload = record.request_payload
        if not isinstance(payload, dict) or "spec" not in payload:
            return PersistedResumeJob(job, None, "resume.job_spec_invalid")
        try:
            spec: ResumeJobSpec = _load_object(_JOB_SPEC_ADAPTER, payload["spec"])
        except (TypeError, ValueError):
            return PersistedResumeJob(job, None, "resume.job_spec_invalid")
        return PersistedResumeJob(job, spec)

    async def save(self, job: Job, *, expected_revision: int) -> None:
        """@brief affected-row CAS 保存 Resume Job / Save a Resume Job with affected-row CAS."""
        self._authorizer.require_workspace(job.workspace_id)
        actor_id = self._authorizer.require_actor()
        if (
            not job.kind.startswith("resume.")
            or job.meta.revision != expected_revision + 1
        ):
            raise ResumeCasMismatch
        progress = job.progress
        result = await self._session.execute(
            update(JobRecord)
            .where(
                JobRecord.workspace_id == str(job.workspace_id),
                JobRecord.resource_owner_id == str(actor_id),
                JobRecord.id == str(job.meta.id),
                JobRecord.job_type == job.kind,
                JobRecord.target_resource_type == job.subject.resource_type,
                JobRecord.target_resource_id == job.subject.id,
                JobRecord.revision == expected_revision,
            )
            .values(
                status=job.status.value,
                phase="queued" if progress is None else progress.phase,
                completed_units=0 if progress is None else progress.completed,
                total_units=None if progress is None else progress.total,
                progress_unit=(
                    JobProgressUnit.UNKNOWN.value
                    if progress is None
                    else progress.unit.value
                ),
                result_refs=_dump_array(_RESOURCE_REFS_ADAPTER, job.result_refs),
                problem=(
                    None
                    if job.problem is None
                    else _dump_problem(job.problem)
                ),
                started_at=job.started_at,
                finished_at=job.finished_at,
                updated_at=job.meta.updated_at,
                revision=job.meta.revision,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise ResumeCasMismatch

    async def get_import_source(
        self,
        workspace_id: WorkspaceId,
        upload_session_id: str,
        job_id: JobId,
    ) -> ResumeImportSource | None:
        """@brief 读取精确绑定 Job 的 completed upload 证明 / Read completed-upload evidence bound to the exact Job."""
        self._authorizer.require_workspace(workspace_id)
        record = await self._session.scalar(
            select(ResumeImportUploadSessionRecord).where(
                ResumeImportUploadSessionRecord.workspace_id == str(workspace_id),
                ResumeImportUploadSessionRecord.id == upload_session_id,
                ResumeImportUploadSessionRecord.status == "completed",
                ResumeImportUploadSessionRecord.claimed_by_type == "job",
                ResumeImportUploadSessionRecord.claimed_by_id == str(job_id),
                ResumeImportUploadSessionRecord.claimed_by_job_id == str(job_id),
                ResumeImportUploadSessionRecord.consumed_at.is_not(None),
            )
        )
        if (
            record is None
            or record.media_type is None
            or record.completion_size_bytes is None
            or record.completion_sha256 is None
        ):
            return None
        return ResumeImportSource(
            UploadSessionId(record.id),
            record.media_type,
            record.completion_size_bytes,
            record.completion_sha256,
        )


@dataclass(frozen=True, slots=True)
class _ValidatedRenderResult:
    """@brief 持久化前完成领域验证的 render 结果 / Render result fully domain-validated before persistence."""

    rendered: RenderedResumeArtifact
    """@brief renderer 原始不可变字节 / Immutable bytes returned by the renderer."""

    artifact: Artifact
    """@brief 满足统一 Artifact 不变量的领域对象 / Domain object satisfying unified Artifact invariants."""

    source_map: PdfSourceMap | None
    """@brief 已与 Artifact/Resume snapshot 交叉验证的 source map / Source map cross-validated with the Artifact and Resume snapshot."""


class _PostgresResumeWorkerResults:
    """@brief 统一 Artifact/content 与 Resume render binding sink / Unified Artifact/content and Resume-render binding sink."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingResumeAuthorizer,
        api_origin: str,
    ) -> None:
        """@brief 绑定第二阶段事务与可信 API Origin / Bind the phase-two transaction and trusted API Origin.

        @param session 第二阶段数据库 Session / Phase-two database session.
        @param authorizer 已密封 worker scope / Sealed worker scope.
        @param api_origin Artifact 同源内容地址的可信 Origin / Trusted origin for same-origin Artifact content URLs.
        """
        self._session = session
        self._authorizer = authorizer
        self._api_origin = api_origin

    async def add_render_results(
        self,
        job: Job,
        revision: ResumeRevision,
        artifacts: Sequence[RenderedResumeArtifact],
        *,
        operation_id: str,
        created_at: datetime,
    ) -> tuple[ResourceRef, ...]:
        """@brief 先完整领域验证，再原子写 Artifact 与 Job binding / Fully domain-validate before atomically writing Artifacts and the Job binding."""
        self._authorizer.require_workspace(job.workspace_id)
        actor_id = self._authorizer.require_actor()
        if (
            job.kind != ResumeJobKind.RENDER.value
            or job.subject.resource_type != "resume"
            or job.subject.id != revision.resume_id
            or job.subject.revision != revision.revision
            or not artifacts
            or len({artifact.format for artifact in artifacts}) != len(artifacts)
        ):
            raise ValueError("Resume render result does not match its persisted Job")
        try:
            validated = tuple(
                _validate_render_result(
                    job,
                    revision,
                    rendered,
                    operation_id=operation_id,
                    created_at=created_at,
                    api_origin=self._api_origin,
                )
                for rendered in artifacts
            )
        except (KeyError, TypeError, ValueError, ArithmeticError, PlatformDomainError) as error:
            raise ResumeCapabilityFailure(
                "resume.source_map_invalid",
                retryable=False,
            ) from error
        revision_record = await self._session.scalar(
            select(ResumeRevisionRecord).where(
                ResumeRevisionRecord.workspace_id == str(job.workspace_id),
                ResumeRevisionRecord.resume_id == str(revision.resume_id),
                ResumeRevisionRecord.revision_no == revision.revision,
            )
        )
        if revision_record is None:
            raise ResumeCasMismatch
        refs: list[ResourceRef] = []
        for result in validated:
            rendered = result.rendered
            artifact = result.artifact
            artifact_id = str(artifact.meta.id)
            storage_key = (
                f"resume/{job.workspace_id}/{revision.resume_id}/"
                f"{revision.revision}/{artifact_id}.{rendered.format.value}"
            )
            artifact_record = ArtifactRecord(
                id=artifact_id,
                workspace_id=str(artifact.workspace_id),
                kind=artifact.kind.value,
                subject_type=artifact.subject.resource_type,
                subject_id=artifact.subject.id,
                subject_revision=artifact.subject.revision,
                media_type=artifact.media_type,
                size_bytes=artifact.size_bytes,
                sha256=artifact.sha256,
                storage_key=storage_key,
                page_count=artifact.page_count,
                expires_at=artifact.expires_at,
                deleted_at=None,
                created_at=artifact.meta.created_at,
                updated_at=artifact.meta.updated_at,
                revision=artifact.meta.revision,
                extensions={},
            )
            self._session.add(artifact_record)
            # 统一 content/source-map 表保留 non-deferrable composite FK；先 flush metadata，
            # 仍由外层第二阶段 transaction 原子提交。/ The unified content/source-map tables
            # retain non-deferrable composite FKs; flush metadata first while keeping the outer
            # phase-two transaction atomic.
            await self._session.flush((artifact_record,))
            self._session.add(
                ArtifactContentRecord(
                    artifact_id=artifact_id,
                    workspace_id=str(artifact.workspace_id),
                    storage_key=storage_key,
                    media_type=artifact.media_type,
                    size_bytes=artifact.size_bytes,
                    sha256=artifact.sha256,
                    content=rendered.content,
                    created_at=artifact.meta.created_at,
                    updated_at=artifact.meta.updated_at,
                    revision=1,
                    extensions={},
                )
            )
            if result.source_map is not None:
                self._session.add(
                    ArtifactPdfSourceMapRecord(
                        artifact_id=artifact_id,
                        workspace_id=str(artifact.workspace_id),
                        resume_id=result.source_map.resume_id,
                        resume_revision=result.source_map.resume_revision,
                        nodes=_dump_array(
                            _PDF_SOURCE_NODES_ADAPTER,
                            result.source_map.nodes,
                        ),
                        created_at=artifact.meta.created_at,
                        updated_at=artifact.meta.updated_at,
                        revision=1,
                        extensions={},
                    )
                )
            refs.append(ResourceRef("artifact", artifact_id, artifact.meta.revision))
        self._session.add(
            ResumeRenderJobRecord(
                id=_derived_worker_id("renderjob", operation_id),
                workspace_id=str(job.workspace_id),
                resource_owner_id=str(actor_id),
                job_id=str(job.meta.id),
                resume_id=str(revision.resume_id),
                resume_revision_id=revision_record.id,
                artifact_id=refs[0].id,
                render_profile="api-v2",
                diagnostics={
                    "formats": [artifact.format.value for artifact in artifacts],
                },
                created_at=created_at,
                updated_at=created_at,
                revision=1,
                extensions={},
            )
        )
        return tuple(refs)


_RENDERER_SOURCE_MAP_FIELDS = frozenset(
    {
        "schema_version",
        "artifact_id",
        "resume_id",
        "resume_revision",
        "page_count",
        "nodes",
    }
)
"""@brief renderer 内部 source-map envelope 的封闭字段 / Closed renderer source-map envelope fields."""

_RENDERER_SOURCE_NODE_FIELDS = frozenset(
    {"entity_id", "field_path", "page", "rects"}
)
"""@brief renderer source node 的封闭字段 / Closed renderer source-node fields."""

_RENDERER_PDF_RECT_FIELDS = frozenset({"x", "y", "width", "height", "unit"})
"""@brief renderer PDF rect 的封闭字段 / Closed renderer PDF-rectangle fields."""


def _validate_render_result(
    job: Job,
    revision: ResumeRevision,
    rendered: RenderedResumeArtifact,
    *,
    operation_id: str,
    created_at: datetime,
    api_origin: str,
) -> _ValidatedRenderResult:
    """@brief 把 renderer 输出提升为完整领域 Artifact/source map / Promote renderer output into complete domain Artifact/source-map objects.

    @param job 持久 running Job / Persisted running Job.
    @param revision 不可变 Resume snapshot / Immutable Resume snapshot.
    @param rendered renderer 输出 / Renderer output.
    @param operation_id crash 重放稳定幂等键 / Crash-replay-stable idempotency key.
    @param created_at Artifact 创建时间 / Artifact creation time.
    @param api_origin 同源内容 URL 的可信 Origin / Trusted origin for same-origin content URLs.
    @return 已完成全部领域交叉验证的结果 / Result with all domain cross-validation complete.
    """
    expected_id = resume_worker_artifact_id(operation_id, rendered.format)
    if rendered.artifact_id != expected_id:
        raise ValueError("renderer Artifact identity does not match its operation")
    expected_media_type = {
        RenderFormat.PDF: "application/pdf",
        RenderFormat.JSON: "application/json",
        RenderFormat.DOCX: (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ),
    }[rendered.format]
    if rendered.media_type.casefold() != expected_media_type:
        raise ValueError("renderer media type does not match its output format")
    if (rendered.page_count is None) is (rendered.format is RenderFormat.PDF):
        raise ValueError("only PDF render results require a page count")
    artifact = Artifact(
        ResourceMeta(expected_id, 1, created_at, created_at),
        job.workspace_id,
        _artifact_kind(rendered.format),
        ResourceRef("resume", str(revision.resume_id), revision.revision),
        expected_media_type,
        len(rendered.content),
        sha256(rendered.content).hexdigest(),
        ApiArtifactContentUrl.build(
            api_origin,
            job.workspace_id,
            expected_id,
        ),
        rendered.page_count,
    )
    source_map = (
        None
        if rendered.source_map is None
        else _source_map_from_renderer(
            rendered.source_map,
            artifact,
            revision.document,
        )
    )
    return _ValidatedRenderResult(rendered, artifact, source_map)


def _source_map_from_renderer(
    payload: Mapping[str, object],
    artifact: Artifact,
    document: ResumeDocument,
) -> PdfSourceMap:
    """@brief 严格解析 renderer dict 并调用 ``validate_for`` / Strictly parse a renderer dict and invoke ``validate_for``.

    @param payload renderer 返回的 canonical JSON object / Canonical JSON object returned by the renderer.
    @param artifact 已验证的目标 PDF Artifact / Validated target PDF Artifact.
    @param document source node 必须绑定的 immutable SIR / Immutable SIR to which source nodes must bind.
    @return 与 Artifact、revision 和字段路径一致的领域 source map / Domain source map bound to the Artifact, revision, and field paths.
    """
    if frozenset(payload) != _RENDERER_SOURCE_MAP_FIELDS:
        raise ValueError("renderer source-map envelope is invalid")
    if payload["schema_version"] != "1.0":
        raise ValueError("renderer source-map schema version is unsupported")
    page_count = _strict_positive_int(payload["page_count"], "source-map page count")
    if artifact.page_count != page_count:
        raise ValueError("renderer source-map page count does not match the PDF")
    raw_nodes = payload["nodes"]
    if not isinstance(raw_nodes, list):
        raise TypeError("renderer source-map nodes must be an array")
    if len(raw_nodes) > 10_000:
        raise ValueError("renderer source map exceeds the node limit")
    nodes = tuple(_source_node_from_renderer(item) for item in raw_nodes)
    artifact_id = payload["artifact_id"]
    resume_id = payload["resume_id"]
    resume_revision = payload["resume_revision"]
    if not isinstance(artifact_id, str) or not isinstance(resume_id, str):
        raise TypeError("renderer source-map identities must be strings")
    source_map = PdfSourceMap(
        ArtifactId(artifact_id),
        resume_id,
        _strict_positive_int(resume_revision, "source-map Resume revision"),
        nodes,
    )
    source_map.validate_for(artifact)
    _validate_source_node_bindings(document, nodes)
    return source_map


def _source_node_from_renderer(value: object) -> PdfSourceNode:
    """@brief 从 renderer JSON 构造一个领域 source node / Construct one domain source node from renderer JSON.

    @param value 未信任 renderer node / Untrusted renderer node.
    @return 完整验证的 PdfSourceNode / Fully validated PdfSourceNode.
    """
    if not isinstance(value, Mapping) or frozenset(value) != _RENDERER_SOURCE_NODE_FIELDS:
        raise TypeError("renderer source node is invalid")
    entity_id = value["entity_id"]
    field_path = value["field_path"]
    raw_rects = value["rects"]
    if not isinstance(entity_id, str):
        raise TypeError("renderer source-node entity ID must be a string")
    if not isinstance(field_path, list) or not all(
        isinstance(part, str) for part in field_path
    ):
        raise TypeError("renderer source-node field path must be a string array")
    if not isinstance(raw_rects, list):
        raise TypeError("renderer source-node rectangles must be an array")
    return PdfSourceNode(
        entity_id,
        tuple(field_path),
        _strict_positive_int(value["page"], "source-node page"),
        tuple(_pdf_rect_from_renderer(item) for item in raw_rects),
    )


def _pdf_rect_from_renderer(value: object) -> PdfRect:
    """@brief 从 renderer JSON 构造有限 point 矩形 / Construct a finite point rectangle from renderer JSON.

    @param value 未信任 renderer rectangle / Untrusted renderer rectangle.
    @return 完整验证的 PdfRect / Fully validated PdfRect.
    """
    if not isinstance(value, Mapping) or frozenset(value) != _RENDERER_PDF_RECT_FIELDS:
        raise TypeError("renderer PDF rectangle is invalid")
    if value["unit"] != "pt":
        raise ValueError("renderer PDF rectangle unit must be points")
    return PdfRect(
        _strict_number(value["x"], "rectangle x"),
        _strict_number(value["y"], "rectangle y"),
        _strict_number(value["width"], "rectangle width"),
        _strict_number(value["height"], "rectangle height"),
    )


def _strict_positive_int(value: object, label: str) -> int:
    """@brief 读取非 bool 正整数 / Read a positive non-boolean integer.

    @param value 候选值 / Candidate value.
    @param label 安全诊断标签 / Safe diagnostic label.
    @return 正整数 / Positive integer.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise TypeError(f"{label} must be a positive integer")
    return value


def _strict_number(value: object, label: str) -> float:
    """@brief 读取非 bool JSON number / Read a non-boolean JSON number.

    @param value 候选值 / Candidate value.
    @param label 安全诊断标签 / Safe diagnostic label.
    @return 交给 PdfRect 做有限性验证的 float / Float passed to PdfRect for finiteness validation.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number")
    try:
        return float(value)
    except OverflowError as error:
        raise ValueError(f"{label} is outside the finite number range") from error


def _validate_source_node_bindings(
    document: ResumeDocument,
    nodes: Sequence[PdfSourceNode],
) -> None:
    """@brief 校验每个 node 的 entity 与 object-only field path / Validate every node's entity and object-only field path.

    @param document renderer 使用的 immutable Resume snapshot / Immutable Resume snapshot used by the renderer.
    @param nodes 已通过 source-map 内在不变量的 nodes / Nodes satisfying source-map intrinsic invariants.
    """
    entities = _resume_entity_payloads(document)
    for node in nodes:
        cursor: object = entities.get(node.entity_id)
        if cursor is None:
            raise ValueError("renderer source node references an absent Resume entity")
        for part in node.field_path:
            if not isinstance(cursor, Mapping) or part not in cursor:
                raise ValueError("renderer source node field path is not bound to the Resume")
            cursor = cursor[part]


def _resume_entity_payloads(document: ResumeDocument) -> dict[str, JsonObject]:
    """@brief 索引 canonical SIR 中所有可定位实体 / Index every addressable entity in the canonical SIR.

    @param document immutable Resume snapshot / Immutable Resume snapshot.
    @return entity ID 到其 canonical object 的映射 / Mapping from entity ID to its canonical object.
    """
    payload = _dump_object(_DOCUMENT_ADAPTER, document)
    entities = {str(document.meta.id): payload}
    profile = payload.get("profile")
    if not isinstance(profile, dict):
        raise TypeError("Resume profile payload is invalid")
    contacts = profile.get("contacts")
    if not isinstance(contacts, list):
        raise TypeError("Resume contacts payload is invalid")
    for contact in contacts:
        _index_resume_entity(entities, contact)
    sections = payload.get("sections")
    if not isinstance(sections, list):
        raise TypeError("Resume sections payload is invalid")
    for section in sections:
        section_payload = _index_resume_entity(entities, section)
        items = section_payload.get("items")
        if not isinstance(items, list):
            raise TypeError("Resume section items payload is invalid")
        for item in items:
            _index_resume_entity(entities, item)
    return entities


def _index_resume_entity(
    entities: dict[str, JsonObject],
    value: object,
) -> JsonObject:
    """@brief 把一个 canonical Resume entity 加入索引 / Add one canonical Resume entity to the index.

    @param entities 正在构建的 entity index / Entity index under construction.
    @param value 候选 canonical entity object / Candidate canonical entity object.
    @return 已验证 entity payload / Validated entity payload.
    """
    if not isinstance(value, dict) or not isinstance(value.get("id"), str):
        raise TypeError("Resume entity payload is invalid")
    entity_id = cast(str, value["id"])
    if entity_id in entities:
        raise ValueError("Resume entity index contains duplicate IDs")
    entity = cast(JsonObject, value)
    entities[entity_id] = entity
    return entity


def _resume_job_from_record(record: JobRecord) -> Job:
    """@brief 从统一 ORM row 重建 Resume Job / Rebuild a Resume Job from a unified ORM row."""
    status = JobStatus(record.status)
    progress = (
        None
        if status is JobStatus.QUEUED
        and record.phase == "queued"
        and record.completed_units == 0
        and record.total_units is None
        else JobProgress(
            record.phase,
            record.completed_units,
            record.total_units,
            JobProgressUnit(record.progress_unit),
        )
    )
    problem = (
        None
        if record.problem is None
        else _load_object(_PROBLEM_ADAPTER, record.problem)
    )
    return Job(
        _resource_meta(record, JobId(record.id)),
        WorkspaceId(record.workspace_id),
        record.job_type,
        ResourceRef(
            record.target_resource_type,
            record.target_resource_id,
            record.target_resource_revision,
        ),
        status,
        progress,
        _load_object(_RESOURCE_REFS_ADAPTER, record.result_refs),
        problem,
        record.started_at,
        record.finished_at,
    )


def _artifact_kind(output_format: RenderFormat) -> ArtifactKind:
    """@brief 映射 render format 到统一 Artifact kind / Map a render format to a unified Artifact kind."""
    return {
        RenderFormat.PDF: ArtifactKind.RESUME_PDF,
        RenderFormat.JSON: ArtifactKind.RESUME_JSON,
        RenderFormat.DOCX: ArtifactKind.RESUME_DOCX,
    }[output_format]


def _derived_worker_id(prefix: str, operation_id: str, discriminator: str = "result") -> str:
    """@brief 从稳定 operation ID 派生可重放资源 ID / Derive a replay-stable resource ID from an operation ID."""
    digest = sha256(
        f"aiws:v2:resume-worker:{prefix}:{operation_id}:{discriminator}".encode()
    ).hexdigest()[:32]
    return f"{prefix}_{digest}"


class _PostgresResumeOutbox:
    """@brief 与 Resume 变更共事务的 PostgreSQL outbox / PostgreSQL outbox sharing the Resume transaction."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingResumeAuthorizer,
    ) -> None:
        """@brief 绑定事务 Session / Bind the transactional Session."""
        self._session = session
        self._authorizer = authorizer

    async def add(self, event: ResumeOutboxEvent) -> None:
        """@brief 写入 secret-free pending event / Write a secret-free pending event."""
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
                id=event.event_id,
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
                    "data": deepcopy(event.data),
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


class PostgresResumeUnitOfWork:
    """@brief 一个 PostgreSQL Resume 短事务工作单元 / One PostgreSQL Resume short-transaction unit of work."""

    def __init__(
        self,
        database: AsyncDatabase,
        templates: ResumeTemplateCatalog,
        *,
        api_origin: str = _DEFAULT_ARTIFACT_API_ORIGIN,
        worker_scope: tuple[WorkspaceId, UserId] | None = None,
    ) -> None:
        """@brief 绑定数据库和不可变模板 catalog / Bind the database and immutable template catalog.

        @param database 共享异步数据库 / Shared async database.
        @param templates 不可变模板 catalog / Immutable template catalog.
        @param api_origin Artifact 同源内容地址的可信 Origin / Trusted origin for same-origin Artifact content URLs.
        @param worker_scope 可选 durable event 身份 / Optional durable-event identity.
        """
        self._database = database
        self._templates = templates
        self._api_origin = api_origin
        self._worker_scope = worker_scope
        self._session: AsyncSession | None = None
        self._transaction: AsyncSessionTransaction | None = None
        self._repository: PostgresResumeRepository | None = None
        self._authorizer: _TrackingResumeAuthorizer | None = None
        self._import_sources: _PostgresImportSourceVerifier | None = None
        self._jobs: _PostgresResumeJobSink | None = None
        self._worker_results: _PostgresResumeWorkerResults | None = None
        self._outbox: _PostgresResumeOutbox | None = None
        self._committed = False
        self._rolled_back = False

    @property
    def repository(self) -> ResumeRepository:
        """@brief 返回事务 Resume repository / Return the transactional Resume repository."""
        if self._repository is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._repository

    @property
    def authorizer(self) -> _TrackingResumeAuthorizer:
        """@brief 返回同 Session 的集中 authorizer / Return the central authorizer on the same Session."""
        if self._authorizer is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._authorizer

    @property
    def templates(self) -> ResumeTemplateCatalog:
        """@brief 返回不可变模板 catalog / Return the immutable template catalog."""
        return self._templates

    @property
    def import_sources(self) -> _PostgresImportSourceVerifier:
        """@brief 返回条件 upload claimer / Return the conditional upload claimer."""
        if self._import_sources is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._import_sources

    @property
    def jobs(self) -> _PostgresResumeJobSink:
        """@brief 返回同事务 Job sink / Return the same-transaction Job sink."""
        if self._jobs is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._jobs

    @property
    def worker_jobs(self) -> _PostgresResumeJobSink:
        """@brief 返回可读写 Job/spec worker store / Return the read-write Job/spec worker store."""
        if self._jobs is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._jobs

    @property
    def worker_results(self) -> _PostgresResumeWorkerResults:
        """@brief 返回第二阶段 Artifact/result sink / Return the phase-two Artifact/result sink."""
        if self._worker_results is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._worker_results

    @property
    def outbox(self) -> _PostgresResumeOutbox:
        """@brief 返回同事务 outbox / Return the same-transaction outbox."""
        if self._outbox is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._outbox

    async def __aenter__(self) -> Self:
        """@brief 创建独占 Session 并组装全部同事务 adapter / Create an exclusive Session and assemble all same-transaction adapters."""
        if self._session is not None:
            raise RuntimeError("Resume unit of work cannot be re-entered")
        self._session = self._database.new_session()
        self._transaction = await self._session.begin()
        access_repository = (
            None
            if self._worker_scope is not None
            else PostgresAccessRepository(self._session)
        )
        self._authorizer = _TrackingResumeAuthorizer(
            None if access_repository is None else AccessAuthorizer(access_repository),
            partial(self._database.install_v2_request_scope, self._session),
            worker_scope=self._worker_scope,
        )
        if self._worker_scope is not None:
            await self._authorizer.install_worker_scope()
        self._repository = PostgresResumeRepository(self._session, self._authorizer)
        self._import_sources = _PostgresImportSourceVerifier(self._session, self._authorizer)
        self._jobs = _PostgresResumeJobSink(self._session, self._authorizer)
        self._worker_results = _PostgresResumeWorkerResults(
            self._session,
            self._authorizer,
            self._api_origin,
        )
        self._outbox = _PostgresResumeOutbox(self._session, self._authorizer)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 未提交或异常时回滚并关闭 Session / Roll back uncommitted or failed work and close the Session."""
        del exc, traceback
        if self._session is not None:
            if exc_type is not None or not self._committed:
                await self.rollback()
            await self._session.close()
        self._session = None
        self._transaction = None
        self._repository = None
        self._authorizer = None
        self._import_sources = None
        self._jobs = None
        self._worker_results = None
        self._outbox = None
        return None

    async def commit(self) -> None:
        """@brief flush 后原子提交 Resume、revision、Job 与 outbox / Flush then atomically commit Resume, revision, Job, and outbox."""
        session, transaction = self._require_active()
        if self._committed:
            raise RuntimeError("Resume unit of work is already committed")
        if self._rolled_back:
            raise RuntimeError("rolled-back Resume unit of work cannot commit")
        await session.flush()
        await transaction.commit()
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等回滚活动事务 / Idempotently roll back the active transaction."""
        if self._transaction is not None and self._transaction.is_active:
            await self._transaction.rollback()
        self._rolled_back = True

    def _require_active(self) -> tuple[AsyncSession, AsyncSessionTransaction]:
        """@brief 要求活动 Session 和 transaction / Require an active Session and transaction."""
        if self._session is None or self._transaction is None:
            raise RuntimeError("Resume unit of work has not been entered")
        return self._session, self._transaction


class PostgresResumeUnitOfWorkFactory:
    """@brief 创建 PostgreSQL Resume UoW / Create PostgreSQL Resume UoWs."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        templates: ResumeTemplateCatalog | None = None,
        api_origin: str = _DEFAULT_ARTIFACT_API_ORIGIN,
    ) -> None:
        """@brief 绑定数据库与可选 catalog / Bind the database and optional catalog.

        @param database 共享数据库 / Shared database.
        @param templates 不可变模板 catalog / Immutable template catalog.
        @param api_origin Artifact 同源内容地址的可信 Origin / Trusted origin for same-origin Artifact content URLs.
        """
        self._database = database
        self._templates = templates or BuiltinResumeTemplateCatalog()
        self._api_origin = api_origin

    def __call__(self) -> PostgresResumeUnitOfWork:
        """@brief 创建未进入的 PostgreSQL UoW / Create a not-yet-entered PostgreSQL UoW."""
        return PostgresResumeUnitOfWork(
            self._database,
            self._templates,
            api_origin=self._api_origin,
        )


class PostgresResumeWorkerUnitOfWorkFactory:
    """@brief 从 durable outbox 身份创建 Resume worker UoW / Create Resume worker UoWs from durable-outbox identity."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        templates: ResumeTemplateCatalog | None = None,
        api_origin: str = _DEFAULT_ARTIFACT_API_ORIGIN,
    ) -> None:
        """@brief 绑定数据库、模板与 Artifact Origin / Bind the database, templates, and Artifact origin.

        @param database 共享数据库 / Shared database.
        @param templates 不可变模板 catalog / Immutable template catalog.
        @param api_origin Artifact 同源内容地址的可信 Origin / Trusted origin for same-origin Artifact content URLs.
        """
        self._database = database
        self._templates = templates or BuiltinResumeTemplateCatalog()
        self._api_origin = api_origin

    def __call__(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
    ) -> PostgresResumeUnitOfWork:
        """@brief 创建已密封事件 actor/Workspace 的 UoW / Create a UoW sealed to the event actor and Workspace."""
        return PostgresResumeUnitOfWork(
            self._database,
            self._templates,
            api_origin=self._api_origin,
            worker_scope=(workspace_id, actor_id),
        )


def _resource_meta[IdT: str](record: Any, identifier: IdT) -> Any:
    """@brief 从 ORM lifecycle 字段构建 ResourceMeta / Build ResourceMeta from ORM lifecycle fields.

    @param record 含 revision/timestamps 的 ORM 行 / ORM row carrying revision and timestamps.
    @param identifier 领域 ID / Domain ID.
    @return 类型化 ResourceMeta / Typed ResourceMeta.
    """
    from backend.domain.principals import ResourceMeta

    return ResourceMeta(identifier, record.revision, record.created_at, record.updated_at)


def _summary_from_document_record(record: ResumeDocumentRecord) -> ResumeSummary:
    """@brief 从 Resume 根投影轻量摘要 / Project a lightweight summary from a Resume root."""
    from backend.domain.principals import ResourceMeta

    return ResumeSummary(
        ResourceMeta(
            ResumeId(record.id),
            record.current_revision_no,
            record.created_at,
            record.updated_at,
        ),
        WorkspaceId(record.workspace_id),
        record.title,
        record.locale,
        TemplateRef(record.template_id, record.template_version),
    )


def _revision_record(
    revision: ResumeRevision,
    aggregate: ResumeAggregate,
    actor_id: UserId,
) -> ResumeRevisionRecord:
    """@brief 构造 append-only revision ORM 行 / Construct an append-only revision ORM row.

    @param revision 不可变领域 revision / Immutable domain revision.
    @param aggregate revision 所属聚合 / Aggregate owning the revision.
    @param actor_id 当前事务 actor / Current transaction actor.
    @return 未加入 Session 的 ORM 行 / ORM row not yet added to the Session.
    """
    payload = _dump_object(_DOCUMENT_ADAPTER, revision.document)
    change = next(
        (item for item in aggregate.revision_changes if item.revision == revision.revision),
        None,
    )
    return ResumeRevisionRecord(
        id=_row_id("rrev"),
        workspace_id=str(revision.document.workspace_id),
        resource_owner_id=str(actor_id),
        resume_id=str(revision.resume_id),
        revision_no=revision.revision,
        semantic_document=payload,
        content_hash=_canonical_hash(payload),
        created_by_actor_id=str(revision.created_by),
        source="v2",
        change_targets=_change_targets(change),
        created_at=revision.created_at,
        updated_at=revision.created_at,
        revision=1,
        extensions={},
    )


def _document_from_revision(record: ResumeRevisionRecord) -> ResumeDocument:
    """@brief 验证 hash 后重建 Resume document / Rebuild a Resume document after verifying its hash."""
    return decode_resume_document(
        record.semantic_document,
        expected_sha256=record.content_hash,
    )


def _revision_from_record(record: ResumeRevisionRecord) -> ResumeRevision:
    """@brief 从 ORM 行重建 immutable revision / Rebuild an immutable revision from an ORM row."""
    if record.created_by_actor_id is None:
        raise ValueError("V2 Resume revision lacks its creating actor")
    document = _document_from_revision(record)
    return ResumeRevision(
        ResumeId(record.resume_id),
        record.revision_no,
        record.created_at,
        UserId(record.created_by_actor_id),
        document,
    )


def _revision_summary_from_record(record: ResumeRevisionRecord) -> ResumeRevisionSummary:
    """@brief 从 ORM 行投影 revision 摘要 / Project a revision summary from an ORM row."""
    if record.created_by_actor_id is None:
        raise ValueError("V2 Resume revision lacks its creating actor")
    return ResumeRevisionSummary(
        ResumeId(record.resume_id),
        record.revision_no,
        record.created_at,
        UserId(record.created_by_actor_id),
    )


__all__ = [
    "BuiltinResumeTemplateCatalog",
    "InMemoryResumeRepository",
    "InMemoryResumeStore",
    "InMemoryResumeUnitOfWork",
    "InMemoryResumeUnitOfWorkFactory",
    "InMemoryResumeUpload",
    "MappingResumeTemplateCatalog",
    "PostgresResumeRepository",
    "PostgresResumeUnitOfWork",
    "PostgresResumeUnitOfWorkFactory",
    "PostgresResumeWorkerUnitOfWorkFactory",
    "decode_resume_document",
    "decode_resume_operation",
    "encode_resume_operation",
]
