"""@brief API v2 通用平台应用端口 / API v2 common-platform application ports."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import TracebackType
from typing import Literal, Protocol, Self

from backend.domain.platform import (
    ApiEvent,
    ApiEventId,
    Artifact,
    ArtifactId,
    ArtifactKind,
    AuditEvent,
    Job,
    JobId,
    PdfSourceMap,
)
from backend.domain.principals import (
    AuthenticatedActor,
    TokenPrincipal,
    WorkspaceAccessContext,
    WorkspaceId,
)

_OPAQUE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API v2 OpaqueId 过滤语法 / API v2 OpaqueId filter grammar."""

_RESOURCE_NAME = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief Job kind 与 subject type 语法 / Job-kind and subject-type grammar."""

_TRACE_ID = re.compile(r"^[a-f0-9]{32}$")
"""@brief W3C trace ID 语法 / W3C trace-ID grammar."""


class PlatformPermission(StrEnum):
    """@brief 5.6 端点的精确授权意图 / Exact authorization intents for section 5.6 endpoints."""

    LIST_JOBS = "platform.jobs.list"
    READ_JOB = "platform.jobs.read"
    CANCEL_JOB = "platform.jobs.cancel"
    LIST_ARTIFACTS = "platform.artifacts.list"
    READ_ARTIFACT = "platform.artifacts.read"
    READ_ARTIFACT_CONTENT = "platform.artifacts.content.read"
    READ_ARTIFACT_SOURCE_MAP = "platform.artifacts.source_map.read"
    READ_EVENTS = "platform.events.read"
    LIST_AUDIT_EVENTS = "platform.audit_events.list"


class PlatformTargetKind(StrEnum):
    """@brief 授权目标资源种类 / Authorization-target resource kinds."""

    JOB = "job"
    ARTIFACT = "artifact"


@dataclass(frozen=True, slots=True)
class PlatformResourceTarget:
    """@brief 单资源授权目标 / Single-resource authorization target.

    @param kind 目标种类 / Target kind.
    @param id 不透明目标 ID / Opaque target ID.
    """

    kind: PlatformTargetKind
    id: str

    def __post_init__(self) -> None:
        """@brief 拒绝不规范目标 ID / Reject a non-canonical target ID.

        @raise ValueError ID 为空或含外围空白时抛出 / Raised for an empty or padded ID.
        @note 完整 OpaqueId 语法仍由领域实体和 HTTP Schema 双重验证。
            / Domain entities and HTTP Schema also validate the full OpaqueId grammar.
        """
        if _OPAQUE_ID.fullmatch(self.id) is None:
            raise ValueError("platform authorization target ID must be canonical")


@dataclass(frozen=True, slots=True)
class PlatformAuthorizationRequest:
    """@brief 精确 permission 与可选资源目标 / Exact permission and optional resource target.

    @param permission 请求权限 / Requested permission.
    @param target 单资源操作的目标；集合操作为空 / Target for single-resource operations; absent
        for collections.
    """

    permission: PlatformPermission
    target: PlatformResourceTarget | None = None

    def __post_init__(self) -> None:
        """@brief 校验 permission 与 target 判别关系 / Validate permission-target discrimination.

        @raise ValueError 集合权限带目标或单项权限缺少/错配目标时抛出 / Raised for a target on
            collections or a missing/mismatched target on item operations.
        """
        job_permissions = {
            PlatformPermission.READ_JOB,
            PlatformPermission.CANCEL_JOB,
        }
        artifact_permissions = {
            PlatformPermission.READ_ARTIFACT,
            PlatformPermission.READ_ARTIFACT_CONTENT,
            PlatformPermission.READ_ARTIFACT_SOURCE_MAP,
        }
        if self.permission in job_permissions:
            if self.target is None or self.target.kind is not PlatformTargetKind.JOB:
                raise ValueError("job permission requires a Job target")
        elif self.permission in artifact_permissions:
            if self.target is None or self.target.kind is not PlatformTargetKind.ARTIFACT:
                raise ValueError("artifact permission requires an Artifact target")
        elif self.target is not None:
            raise ValueError("collection permission cannot carry a resource target")


@dataclass(frozen=True, slots=True)
class PageRequest:
    """@brief 解码后的稳定 keyset 分页请求 / Decoded stable-keyset page request.

    @param limit 返回上限，契约范围 1..200 / Return limit in the contract range 1..200.
    @param after opaque cursor 解码出的内部位置 / Internal position decoded from an opaque cursor.
    """

    limit: int = 50
    after: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验分页边界 / Validate pagination bounds.

        @raise ValueError limit 或位置非法时抛出 / Raised for an invalid limit or position.
        """
        if not 1 <= self.limit <= 200:
            raise ValueError("page limit must be between one and 200")
        if self.after is not None and (
            not self.after or len(self.after) > 2048 or self.after.strip() != self.after
        ):
            raise ValueError("page position must be canonical and at most 2048 characters")


@dataclass(frozen=True, slots=True)
class CollectionPage[ItemT]:
    """@brief 稳定排序的 keyset 页面 / Stably ordered keyset page.

    @param items 当前页项目 / Current page items.
    @param next_position 有下一页时的内部位置 / Internal next position when another page exists.
    """

    items: tuple[ItemT, ...]
    next_position: str | None

    def __post_init__(self) -> None:
        """@brief 校验页面投影边界 / Validate page-projection bounds.

        @raise ValueError 项目数或 next position 非法时抛出 / Raised for an invalid item count or
            next position.
        """
        if len(self.items) > 200:
            raise ValueError("collection page cannot exceed 200 items")
        if self.next_position is not None and (
            not self.next_position
            or len(self.next_position) > 2048
            or self.next_position.strip() != self.next_position
        ):
            raise ValueError("next page position must be canonical and at most 2048 characters")

    @property
    def has_more(self) -> bool:
        """@brief 投影契约 ``page.has_more`` / Project contract ``page.has_more``.

        @return 存在下一位置时为真 / True when a next position exists.
        """
        return self.next_position is not None


@dataclass(frozen=True, slots=True)
class SubjectFilter:
    """@brief Job/Artifact 共用 subject 过滤器 / Subject filter shared by Job and Artifact queries.

    @param subject_type 可选 ResourceRef 类型 / Optional ResourceRef type.
    @param subject_id 可选 ResourceRef ID / Optional ResourceRef ID.
    """

    subject_type: str | None = None
    subject_id: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验过滤值可安全绑定 cursor / Validate values for safe cursor binding.

        @raise ValueError 过滤值为空、过长或含外围空白时抛出 / Raised for empty, oversized, or
            padded filter values.
        """
        if self.subject_type is not None and _RESOURCE_NAME.fullmatch(self.subject_type) is None:
            raise ValueError("subject type filter is invalid")
        if self.subject_id is not None and _OPAQUE_ID.fullmatch(self.subject_id) is None:
            raise ValueError("subject ID filter is invalid")

    @property
    def cursor_binding(self) -> tuple[str | None, str | None]:
        """@brief 返回 cursor 必须签名的规范值 / Return canonical values a cursor must bind.

        @return ``(subject_type, subject_id)`` / ``(subject_type, subject_id)``.
        """
        return self.subject_type, self.subject_id


@dataclass(frozen=True, slots=True)
class JobQuery:
    """@brief Job 列表过滤条件 / Job-list filters.

    @param kind 可选开放 Job kind / Optional open Job kind.
    @param subject subject 类型/ID 过滤 / Subject type/ID filter.
    """

    kind: str | None = None
    subject: SubjectFilter = SubjectFilter()

    def __post_init__(self) -> None:
        """@brief 校验开放 kind 边界 / Validate open-kind bounds.

        @raise ValueError kind 不规范时抛出 / Raised for a non-canonical kind.
        """
        if self.kind is not None and _RESOURCE_NAME.fullmatch(self.kind) is None:
            raise ValueError("Job kind filter is invalid")

    @property
    def cursor_binding(self) -> tuple[str | None, str | None, str | None]:
        """@brief 返回 cursor 过滤绑定 / Return the cursor filter binding.

        @return ``(kind, subject_type, subject_id)`` / Canonical filter tuple.
        """
        return self.kind, *self.subject.cursor_binding


@dataclass(frozen=True, slots=True)
class ArtifactQuery:
    """@brief Artifact 列表过滤条件 / Artifact-list filters.

    @param kind 可选封闭 Artifact kind / Optional closed Artifact kind.
    @param subject subject 类型/ID 过滤 / Subject type/ID filter.
    """

    kind: ArtifactKind | None = None
    subject: SubjectFilter = SubjectFilter()

    @property
    def cursor_binding(self) -> tuple[str | None, str | None, str | None]:
        """@brief 返回 cursor 过滤绑定 / Return the cursor filter binding.

        @return ``(kind, subject_type, subject_id)`` / Canonical filter tuple.
        """
        return (self.kind.value if self.kind is not None else None, *self.subject.cursor_binding)


@dataclass(frozen=True, slots=True)
class ByteRangeRequest:
    """@brief 已解析但尚未按对象长度归一化的单 byte range / Parsed unresolved single byte range.

    @param first 首字节位置；suffix form 时为空 / First byte offset; absent for suffix form.
    @param last_inclusive 可选末字节位置 / Optional inclusive final byte offset.
    @param suffix_length suffix form 的尾部字节数 / Tail byte count for suffix form.
    """

    first: int | None = None
    last_inclusive: int | None = None
    suffix_length: int | None = None

    def __post_init__(self) -> None:
        """@brief 校验 RFC 9110 单 range 形态 / Validate a single RFC 9110 range shape.

        @raise ValueError range 同时使用两种形态或数值非法时抛出 / Raised for mixed forms or
            invalid values.
        """
        suffix_length = self.suffix_length
        if suffix_length is not None:
            if self.first is not None or self.last_inclusive is not None or suffix_length <= 0:
                raise ValueError("suffix byte range must contain only a positive suffix length")
            return
        if self.first is None or self.first < 0:
            raise ValueError("byte range requires a non-negative first offset")
        if self.last_inclusive is not None and self.last_inclusive < self.first:
            raise ValueError("byte range last offset cannot precede first offset")

    def resolve(self, total_size_bytes: int) -> ContentRange:
        """@brief 按 Artifact 大小解析 range / Resolve the range against Artifact size.

        @param total_size_bytes 完整对象字节数 / Complete object size in bytes.
        @return 有界 ContentRange / Bounded ContentRange.
        @raise RangeNotSatisfiable 空对象或首位置越界时抛出 / Raised for an empty object or an
            out-of-bounds first offset.
        """
        if total_size_bytes < 0:
            raise ValueError("artifact total size cannot be negative")
        if total_size_bytes == 0:
            raise RangeNotSatisfiable(total_size_bytes)
        if self.suffix_length is not None:
            first = max(0, total_size_bytes - self.suffix_length)
            return ContentRange(first, total_size_bytes - 1, total_size_bytes)
        if self.first is None or self.first >= total_size_bytes:
            raise RangeNotSatisfiable(total_size_bytes)
        last = total_size_bytes - 1
        if self.last_inclusive is not None:
            last = min(last, self.last_inclusive)
        return ContentRange(self.first, last, total_size_bytes)


@dataclass(frozen=True, slots=True)
class ContentRange:
    """@brief 已解析的闭区间 byte range / Resolved inclusive byte range.

    @param first 首字节位置 / First byte offset.
    @param last_inclusive 末字节位置 / Inclusive final byte offset.
    @param total_size_bytes 完整对象字节数 / Complete object size.
    """

    first: int
    last_inclusive: int
    total_size_bytes: int

    def __post_init__(self) -> None:
        """@brief 校验范围包含关系 / Validate range containment.

        @raise ValueError 范围不在完整对象内时抛出 / Raised unless the range is within the object.
        """
        if (
            self.total_size_bytes <= 0
            or self.first < 0
            or self.last_inclusive < self.first
            or self.last_inclusive >= self.total_size_bytes
        ):
            raise ValueError("content range must be contained in a non-empty artifact")

    @property
    def length(self) -> int:
        """@brief 计算选择字节数 / Calculate selected byte count.

        @return 闭区间长度 / Inclusive-range length.
        """
        return self.last_inclusive - self.first + 1


class RangeNotSatisfiable(ValueError):
    """@brief 请求 range 无法由 Artifact 满足 / Requested range cannot be satisfied by the Artifact.

    @param total_size_bytes 完整对象大小 / Complete object size.
    """

    total_size_bytes: int
    """@brief 完整 Artifact 字节数 / Complete Artifact byte count."""

    def __init__(self, total_size_bytes: int) -> None:
        """@brief 初始化 416 所需结果 / Initialize the information required for HTTP 416.

        @param total_size_bytes 完整对象大小 / Complete object size.
        """
        super().__init__("requested byte range is not satisfiable")
        self.total_size_bytes = total_size_bytes


@dataclass(frozen=True, slots=True)
class ArtifactContentStream:
    """@brief 对象存储打开后的受验证流描述 / Validated stream descriptor opened by object storage.

    @param chunks 异步二进制块 / Asynchronous binary chunks.
    @param media_type 对象存储元数据中的媒体类型 / Media type from object-storage metadata.
    @param total_size_bytes 完整对象大小 / Complete object size.
    @param sha256 完整对象已验证摘要 / Verified complete-object digest.
    @param selected_range 实际选择范围；完整响应为空 / Actual selected range; absent for full content.
    """

    chunks: AsyncIterator[bytes]
    media_type: str
    total_size_bytes: int
    sha256: str
    selected_range: ContentRange | None


@dataclass(frozen=True, slots=True)
class ArtifactDownload:
    """@brief application 返回给 HTTP adapter 的下载描述 / Download descriptor returned to HTTP.

    @param artifact 已授权 Artifact 元数据 / Authorized Artifact metadata.
    @param chunks 计数及完整响应摘要校验后的异步块 / Async chunks wrapped with length and full-body
        digest validation.
    @param selected_range 实际 byte range / Actual byte range.
    @param etag 基于内容摘要的强 ETag / Strong content-digest ETag.
    """

    artifact: Artifact
    chunks: AsyncIterator[bytes]
    selected_range: ContentRange | None
    etag: str

    @property
    def content_length(self) -> int:
        """@brief 返回本响应内容长度 / Return this response's content length.

        @return 完整大小或 range 长度 / Full size or selected-range length.
        """
        if self.selected_range is None:
            return self.artifact.size_bytes
        return self.selected_range.length


@dataclass(frozen=True, slots=True)
class EventReplayRequest:
    """@brief SSE 重放起点 / SSE replay starting point.

    @param after_event_id ``Last-Event-ID``；为空表示新订阅 / ``Last-Event-ID``; absent for a new
        subscription.
    """

    after_event_id: ApiEventId | None = None

    def __post_init__(self) -> None:
        """@brief 校验重放 ID 可安全传给 event store / Validate replay ID for the event store.

        @raise ValueError ID 为空或含外围空白时抛出 / Raised for an empty or padded ID.
        """
        if self.after_event_id is not None and _OPAQUE_ID.fullmatch(self.after_event_id) is None:
            raise ValueError("Last-Event-ID must be canonical")


class EventReplayWindowExpired(LookupError):
    """@brief Last-Event-ID 已不在保留窗口 / Last-Event-ID is outside the retained replay window.

    @param after_event_id 无法恢复的事件 ID / Event ID which cannot be resumed.
    @note HTTP adapter 必须映射为 409 ``event.replay_window_expired``。
        / The HTTP adapter must map this to 409 ``event.replay_window_expired``.
    """

    code: Literal["event.replay_window_expired"] = "event.replay_window_expired"
    """@brief 契约稳定错误码 / Contract-stable error code."""

    after_event_id: ApiEventId
    """@brief 无法恢复的事件 ID / Event ID which could not be resumed."""

    def __init__(self, after_event_id: ApiEventId) -> None:
        """@brief 初始化窗口过期错误 / Initialize the replay-window error.

        @param after_event_id 无法恢复的事件 ID / Event ID which cannot be resumed.
        """
        super().__init__(self.code)
        self.after_event_id = after_event_id


@dataclass(frozen=True, slots=True)
class MutationContext:
    """@brief 平台 mutation 的审计与 trace 上下文 / Audit and trace context for a platform mutation.

    @param request_id 强制请求关联 ID / Required request correlation ID.
    @param trace_id 可选 W3C trace ID / Optional W3C trace ID.
    """

    request_id: str
    trace_id: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验审计关联标识 / Validate audit-correlation identifiers.

        @raise ValueError request 或 trace ID 不匹配契约时抛出 / Raised unless request or trace ID
            matches the contract.
        """
        if _OPAQUE_ID.fullmatch(self.request_id) is None:
            raise ValueError("platform mutation request ID is invalid")
        if self.trace_id is not None and _TRACE_ID.fullmatch(self.trace_id) is None:
            raise ValueError("platform mutation trace ID is invalid")


class JobCasMismatch(RuntimeError):
    """@brief Job 最终 compare-and-swap 失败 / Final Job compare-and-swap failure."""


class JobCancellationRejected(RuntimeError):
    """@brief 领域补偿无法安全执行时拒绝取消 / Reject cancellation when domain compensation is unsafe.

    @param code 稳定冲突码 / Stable conflict code.
    @param detail 可公开诊断 / Public-safe diagnostic detail.
    """

    code: str
    """@brief 稳定冲突码 / Stable conflict code."""

    detail: str
    """@brief 可公开诊断 / Public-safe diagnostic detail."""

    def __init__(self, code: str, detail: str) -> None:
        """@brief 保存结构化取消拒绝 / Store a structured cancellation rejection.

        @param code 稳定冲突码 / Stable conflict code.
        @param detail 可公开诊断 / Public-safe diagnostic detail.
        """
        super().__init__(detail)
        self.code = code
        self.detail = detail


class Clock(Protocol):
    """@brief 平台用例的可替换时钟 / Replaceable clock for platform use cases."""

    def now(self) -> datetime:
        """@brief 返回带时区当前时刻 / Return the timezone-aware current instant.

        @return 当前时刻 / Current instant.
        """


class PlatformWorkspaceAuthorizer(Protocol):
    """@brief token、membership、目标资源交集授权端口 / Token, membership, and target intersection authorizer."""

    async def authenticate(self, principal: TokenPrincipal) -> AuthenticatedActor:
        """@brief 将签名 principal 绑定到本地用户 / Bind a signed principal to a local user.

        @param principal 已密码学验证 principal / Cryptographically verified principal.
        @return 本地 actor / Local actor.
        """

    async def authorize(
        self,
        actor: AuthenticatedActor,
        workspace_id: WorkspaceId,
        request: PlatformAuthorizationRequest,
    ) -> WorkspaceAccessContext:
        """@brief 为精确 5.6 操作签发 Workspace 证明 / Issue a Workspace proof for one exact operation.

        @param actor 已认证 actor / Authenticated actor.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param request 精确 permission 与目标 / Exact permission and target.
        @return 密封 Workspace 授权上下文 / Sealed Workspace authorization context.
        @note cancellation adapter 必须按 Job kind/subject 应用对应领域 scope 和 role，不能只检查
            Workspace 存在。/ Cancellation adapters must apply domain scope and role from Job
            kind/subject, not merely Workspace existence.
        """


class PlatformRepository(Protocol):
    """@brief Workspace 证明约束下的平台查询与 Job CAS 端口 / Platform query and Job-CAS port."""

    async def list_jobs(
        self,
        access: WorkspaceAccessContext,
        query: JobQuery,
        page: PageRequest,
    ) -> CollectionPage[Job]:
        """@brief 列出已授权 Workspace Job / List Jobs in the authorized Workspace."""

    async def get_job(
        self,
        access: WorkspaceAccessContext,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 在授权 Workspace 中读取 Job / Read a Job in the authorized Workspace."""

    async def save_job(self, access: WorkspaceAccessContext, job: Job, *, expected_revision: int) -> None:
        """@brief 使用 revision CAS 保存 Job / Save a Job using revision CAS.

        @param access 精确 cancellation 授权证明 / Exact cancellation authorization proof.
        @param job 已迁移 Job / Transitioned Job.
        @param expected_revision UPDATE 要求的旧 revision / Old revision required by UPDATE.
        @raise JobCasMismatch 影响行数不是一时抛出 / Raised unless exactly one row changes.
        """

    async def synchronize_cancellation(
        self,
        access: WorkspaceAccessContext,
        job: Job,
        *,
        at: datetime,
    ) -> None:
        """@brief 在 Job CAS 同事务补偿领域活动态 / Compensate domain-active state in the Job-CAS transaction.

        @param access 精确 cancellation 授权证明 / Exact cancellation authorization proof.
        @param job 锁定后的尚未取消 Job / Locked not-yet-cancelled Job.
        @param at 取消生效时刻 / Cancellation instant.
        @raise JobCancellationRejected 外部副作用已进入不可安全回滚阶段时抛出 / Raised when
            an external side effect has entered a phase that cannot be safely rolled back.
        @raise JobCasMismatch Job 与领域绑定损坏或发生并发迁移时抛出 / Raised for a broken
            Job-domain binding or concurrent transition.
        @note 实现必须使 Run、Session、KnowledgeSource、Connection、UploadSession 等领域投影
            不会在 Job 终态后永久停留在活动态。/ Implementations must prevent domain projections
            from remaining permanently active after the Job becomes terminal.
        """

    async def list_artifacts(
        self,
        access: WorkspaceAccessContext,
        query: ArtifactQuery,
        page: PageRequest,
    ) -> CollectionPage[Artifact]:
        """@brief 列出已授权 Workspace Artifact / List Artifacts in the authorized Workspace."""

    async def get_artifact(
        self,
        access: WorkspaceAccessContext,
        artifact_id: ArtifactId,
    ) -> Artifact | None:
        """@brief 在授权 Workspace 中读取 Artifact / Read an Artifact in the authorized Workspace."""

    async def get_pdf_source_map(
        self,
        access: WorkspaceAccessContext,
        artifact_id: ArtifactId,
    ) -> PdfSourceMap | None:
        """@brief 读取 Artifact 的 PDF source map / Read an Artifact's PDF source map."""

    async def list_audit_events(
        self,
        access: WorkspaceAccessContext,
        page: PageRequest,
    ) -> CollectionPage[AuditEvent]:
        """@brief 列出已授权 Workspace 审计事件 / List authorized Workspace audit events."""


class ArtifactContentStore(Protocol):
    """@brief 可验证且支持单 byte range 的 Artifact 内容端口 / Verifiable single-range Artifact content port."""

    async def open(
        self,
        access: WorkspaceAccessContext,
        artifact: Artifact,
        selected_range: ContentRange | None,
    ) -> ArtifactContentStream:
        """@brief 打开已授权 Artifact 内容 / Open authorized Artifact content.

        @param access 精确 content-read 授权证明 / Exact content-read authorization proof.
        @param artifact 已授权 metadata / Authorized metadata.
        @param selected_range 可选已归一化 range / Optional normalized range.
        @return 与存储 metadata 交叉校验的流 / Stream cross-checked against storage metadata.
        @note adapter 必须在返回前验证底层完整对象的 size/media/SHA-256；partial response 不能只
            对切片摘要冒充完整摘要。/ The adapter must verify whole-object size/media/SHA-256
            before returning; a partial digest cannot impersonate the complete-object digest.
        """


class PlatformEventFeed(Protocol):
    """@brief 至少一次且单 Workspace 有序的 SSE event feed / At-least-once ordered Workspace event feed."""

    async def open(
        self,
        access: WorkspaceAccessContext,
        replay: EventReplayRequest,
    ) -> AsyncIterator[ApiEvent]:
        """@brief 打开并在返回前验证 replay 起点 / Open and validate replay before returning.

        @param access 精确 event-read 授权证明 / Exact event-read authorization proof.
        @param replay ``Last-Event-ID`` 恢复请求 / ``Last-Event-ID`` resume request.
        @return 允许相邻相同事件重投、但 sequence 不倒退的流 / Stream allowing adjacent
            redelivery of an identical event but never decreasing sequence.
        @raise EventReplayWindowExpired ID 未找到或已离开保留窗口时，在返回 iterator 前抛出
            / Raised before returning the iterator when the ID is unknown or outside retention.
        @note 端口必须原子确定 replay 边界再订阅 live tail，避免 replay/live 之间丢事件。
            / The port must atomically bridge replay and live tail to avoid a gap.
        """


class PlatformMutationJournal(Protocol):
    """@brief 与 Job CAS 同事务的事件 outbox 与审计 journal / Transactional event-outbox and audit journal."""

    async def job_cancelled(
        self,
        access: WorkspaceAccessContext,
        before: Job,
        after: Job,
        context: MutationContext,
    ) -> None:
        """@brief 记录一次成功 Job cancellation / Record one successful Job cancellation.

        @param access 精确 cancellation 授权证明及 actor / Exact cancellation proof and actor.
        @param before 取消前 Job / Job before cancellation.
        @param after cancelled 新 revision / New cancelled revision.
        @param context request/trace 关联 / Request/trace correlation.
        @note adapter 必须在当前事务写入 ``job.updated`` ApiEvent outbox 和 ``job.cancel``
            AuditEvent；event sequence 在 Workspace 内原子分配。/ The adapter must write a
            ``job.updated`` ApiEvent outbox row and ``job.cancel`` AuditEvent in the current
            transaction, atomically allocating the Workspace event sequence.
        """


class PlatformUnitOfWork(Protocol):
    """@brief 授权、Job CAS 与平台读取的原子工作单元 / Atomic authorization, Job-CAS, and read unit."""

    @property
    def authorizer(self) -> PlatformWorkspaceAuthorizer:
        """@brief 返回事务绑定 authorizer / Return the transaction-bound authorizer."""

    @property
    def repository(self) -> PlatformRepository:
        """@brief 返回事务绑定 repository / Return the transaction-bound repository."""

    @property
    def journal(self) -> PlatformMutationJournal:
        """@brief 返回同事务 mutation journal / Return the same-transaction mutation journal."""

    async def __aenter__(self) -> Self:
        """@brief 开始工作单元 / Enter the unit of work.

        @return 当前工作单元 / Current unit of work.
        """

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时回滚 / Roll back on exceptions or absent commit."""

    async def commit(self) -> None:
        """@brief 原子提交 Job cancellation / Atomically commit Job cancellation."""

    async def rollback(self) -> None:
        """@brief 幂等回滚 / Roll back idempotently."""


class PlatformUnitOfWorkFactory(Protocol):
    """@brief 为一次平台用例创建工作单元 / Create a unit of work per platform use case."""

    def __call__(self) -> PlatformUnitOfWork:
        """@brief 创建未进入的工作单元 / Create a not-yet-entered unit of work.

        @return 新工作单元 / New unit of work.
        """


__all__ = [
    "ArtifactContentStore",
    "ArtifactContentStream",
    "ArtifactDownload",
    "ArtifactQuery",
    "ByteRangeRequest",
    "Clock",
    "CollectionPage",
    "ContentRange",
    "EventReplayRequest",
    "EventReplayWindowExpired",
    "JobCancellationRejected",
    "JobCasMismatch",
    "JobQuery",
    "MutationContext",
    "PageRequest",
    "PlatformAuthorizationRequest",
    "PlatformEventFeed",
    "PlatformMutationJournal",
    "PlatformPermission",
    "PlatformRepository",
    "PlatformResourceTarget",
    "PlatformTargetKind",
    "PlatformUnitOfWork",
    "PlatformUnitOfWorkFactory",
    "PlatformWorkspaceAuthorizer",
    "RangeNotSatisfiable",
    "SubjectFilter",
]
