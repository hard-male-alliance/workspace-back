"""@brief API v2 Job、Artifact、事件与审计领域模型 / API v2 platform domain models.

本模块只表达 ``contracts/v2`` 5.6 的稳定语义；HTTP、数据库和对象存储细节由外层
adapter 负责。不可变聚合与判别联合让非法状态无法在正常构造路径中存活。
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Literal, NewType
from urllib.parse import urlsplit

from backend.domain.principals import DomainInvariantError, ResourceMeta, WorkspaceId
from backend.domain.resources import ResourceRef

JobId = NewType("JobId", str)
"""@brief 统一 Job 不透明标识 / Unified opaque Job identifier."""

ArtifactId = NewType("ArtifactId", str)
"""@brief Artifact 不透明标识 / Opaque Artifact identifier."""

ApiEventId = NewType("ApiEventId", str)
"""@brief API 事件不透明标识 / Opaque API-event identifier."""

AuditEventId = NewType("AuditEventId", str)
"""@brief 审计事件不透明标识 / Opaque audit-event identifier."""

type JsonValue = (
    None
    | bool
    | int
    | float
    | str
    | tuple[JsonValue, ...]
    | Mapping[str, JsonValue]
)
"""@brief 深度不可变 JSON 值 / Deeply immutable JSON value."""

type ProblemParameter = None | bool | int | float | str
"""@brief 字段错误参数允许的 JSON 标量 / JSON scalars allowed in field-error parameters."""

_OPAQUE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API v2 不透明 ID 语法 / API v2 opaque-ID grammar."""

_STABLE_NAME = re.compile(r"^[a-z][a-z0-9_.-]{2,127}$")
"""@brief 事件、错误和 action 稳定名称语法 / Stable event, error, and action grammar."""

_RESOURCE_TYPE = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief ResourceRef 类型名语法 / ResourceRef type-name grammar."""

_MEDIA_TYPE = re.compile(r"^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$")
"""@brief Artifact 媒体类型语法 / Artifact media-type grammar."""

_SHA256 = re.compile(r"^[a-f0-9]{64}$")
"""@brief 小写 SHA-256 十六进制语法 / Lowercase SHA-256 hexadecimal grammar."""

_TRACE_ID = re.compile(r"^[a-f0-9]{32}$")
"""@brief API event trace ID 语法 / API-event trace-ID grammar."""

_EXTENSION_KEY = re.compile(r"^[a-z][a-z0-9]*(?:\.[a-z0-9][a-z0-9_-]*)+$")
"""@brief 命名空间扩展键语法 / Namespaced extension-key grammar."""

_MAX_ARTIFACT_BYTES = 1_073_741_824
"""@brief 契约允许的单 Artifact 最大字节数 / Contract maximum bytes per Artifact."""


class PlatformDomainError(DomainInvariantError):
    """@brief API v2 平台领域不变量错误 / API v2 platform-domain invariant error."""


class JobTransitionError(PlatformDomainError):
    """@brief Job 状态机拒绝迁移 / Job state machine rejected a transition.

    @param current 当前状态 / Current state.
    @param requested 请求目标状态 / Requested target state.
    """

    current: JobStatus
    """@brief 当前 Job 状态 / Current Job state."""

    requested: JobStatus
    """@brief 请求的 Job 状态 / Requested Job state."""

    def __init__(self, current: JobStatus, requested: JobStatus) -> None:
        """@brief 初始化非法迁移错误 / Initialize an invalid-transition error.

        @param current 当前状态 / Current state.
        @param requested 请求目标状态 / Requested target state.
        """
        super().__init__(f"job cannot transition from {current.value} to {requested.value}")
        self.current = current
        self.requested = requested


class JobStatus(StrEnum):
    """@brief 契约冻结的 Job 状态 / Contract-frozen Job states."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"

    @property
    def is_terminal(self) -> bool:
        """@brief 判断状态是否终态 / Test whether the state is terminal.

        @return 成功、失败、取消或过期时为真 / True for succeeded, failed, cancelled, or expired.
        """
        return self in {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.EXPIRED,
        }


class JobProgressUnit(StrEnum):
    """@brief Job progress 单位 / Job-progress units."""

    ITEMS = "items"
    BYTES = "bytes"
    PAGES = "pages"
    STEPS = "steps"
    UNKNOWN = "unknown"


class ArtifactKind(StrEnum):
    """@brief 契约冻结的 Artifact 种类 / Contract-frozen Artifact kinds."""

    RESUME_PDF = "resume_pdf"
    RESUME_JSON = "resume_json"
    RESUME_DOCX = "resume_docx"
    INTERVIEW_AUDIO = "interview_audio"
    INTERVIEW_VIDEO = "interview_video"
    INTERVIEW_TRANSCRIPT = "interview_transcript"
    GENERIC = "generic"


class AuditOutcome(StrEnum):
    """@brief 审计决策结果 / Audit decision outcomes."""

    ALLOWED = "allowed"
    DENIED = "denied"
    FAILED = "failed"


class PdfUnit(StrEnum):
    """@brief PDF 坐标唯一允许的单位 / Sole PDF coordinate unit."""

    POINT = "pt"


@dataclass(frozen=True, slots=True)
class ProblemFieldError:
    """@brief RFC 9457 problem 的结构化字段错误 / Structured field error for an RFC 9457 problem.

    @param pointer JSON Pointer 或稳定字段位置 / JSON Pointer or stable field location.
    @param code 稳定机器错误码 / Stable machine-readable error code.
    @param message_key 可选本地化键 / Optional localization key.
    @param params 有界 JSON 标量参数 / Bounded JSON-scalar parameters.
    """

    pointer: str
    code: str
    message_key: str | None = None
    params: Mapping[str, ProblemParameter] = MappingProxyType({})

    def __post_init__(self) -> None:
        """@brief 校验并冻结字段错误 / Validate and freeze the field error.

        @raise PlatformDomainError 字段错误不满足 Schema 时抛出 / Raised when the error violates
            its schema.
        """
        if len(self.pointer) > 1024 or _has_control(self.pointer):
            raise PlatformDomainError("problem field pointer is invalid")
        _require_pattern(self.code, _STABLE_NAME, "problem field code")
        if self.message_key is not None and len(self.message_key) > 200:
            raise PlatformDomainError("problem message key is too long")
        if len(self.params) > 20:
            raise PlatformDomainError("problem field params cannot exceed 20 entries")
        copied: dict[str, ProblemParameter] = {}
        for key, value in self.params.items():
            if not isinstance(key, str) or not key:
                raise PlatformDomainError("problem parameter keys must be non-empty strings")
            _require_json_scalar(value, f"problem parameter {key}")
            copied[key] = value
        object.__setattr__(self, "params", MappingProxyType(copied))


@dataclass(frozen=True, slots=True)
class ProblemDetails:
    """@brief Job failure 使用的契约级 ProblemDetails / Contract ProblemDetails for Job failure.

    @param type_uri 可解析 HTTPS problem type / Resolvable HTTPS problem type.
    @param title 短人类可读标题 / Short human-readable title.
    @param status HTTP 状态码 / HTTP status code.
    @param code 稳定机器错误码 / Stable machine-readable error code.
    @param request_id 触发失败的请求 ID / Request ID which caused the failure.
    @param retryable 客户端是否可重试 / Whether the client may retry.
    @param detail 可选安全详情 / Optional public-safe detail.
    @param instance 可选问题实例 URI-reference / Optional problem-instance URI reference.
    @param errors 结构化字段错误 / Structured field errors.
    @param extensions 有命名空间的显式扩展 / Explicit namespaced extensions.
    """

    type_uri: str
    title: str
    status: int
    code: str
    request_id: str
    retryable: bool
    detail: str | None = None
    instance: str | None = None
    errors: tuple[ProblemFieldError, ...] = ()
    extensions: Mapping[str, JsonValue] = MappingProxyType({})

    def __post_init__(self) -> None:
        """@brief 校验并冻结 ProblemDetails / Validate and freeze ProblemDetails.

        @raise PlatformDomainError ProblemDetails 不满足契约时抛出 / Raised when the problem
            violates the contract.
        """
        _require_https_url(self.type_uri, "problem type")
        if not 1 <= len(self.title) <= 200 or _has_control(self.title):
            raise PlatformDomainError("problem title must be safe and 1 to 200 characters")
        if not 400 <= self.status <= 599:
            raise PlatformDomainError("problem status must be between 400 and 599")
        _require_pattern(self.code, _STABLE_NAME, "problem code")
        _require_opaque_id(self.request_id, "problem request id")
        if self.detail is not None and (len(self.detail) > 2000 or _has_control(self.detail)):
            raise PlatformDomainError("problem detail must be safe and at most 2000 characters")
        if self.instance is not None:
            _require_uri_reference(self.instance, "problem instance", 2048)
        if len(self.errors) > 100:
            raise PlatformDomainError("problem errors cannot exceed 100 entries")
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))


@dataclass(frozen=True, slots=True)
class JobProgress:
    """@brief 有界 Job 进度快照 / Bounded Job-progress snapshot.

    @param phase 当前稳定阶段名 / Current stable phase name.
    @param completed 已完成单位 / Completed units.
    @param total 可选总单位 / Optional total units.
    @param unit 计数单位 / Counting unit.
    """

    phase: str
    completed: int
    total: int | None
    unit: JobProgressUnit

    def __post_init__(self) -> None:
        """@brief 校验进度关联 / Validate progress associations.

        @raise PlatformDomainError phase、计数或 total 关联非法时抛出 / Raised for an invalid
            phase, count, or total association.
        """
        if not 1 <= len(self.phase) <= 80 or self.phase.strip() != self.phase:
            raise PlatformDomainError("job progress phase must be canonical and 1 to 80 characters")
        if self.completed < 0:
            raise PlatformDomainError("job progress completed cannot be negative")
        if self.total is not None and (self.total < 0 or self.completed > self.total):
            raise PlatformDomainError("job progress completed cannot exceed total")


@dataclass(frozen=True, slots=True)
class Job:
    """@brief 统一 Workspace Job 不可变聚合 / Unified immutable Workspace Job aggregate.

    @param meta 资源 revision 元数据 / Resource-revision metadata.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param kind 开放但有稳定语法的 Job 种类 / Open, stable-syntax Job kind.
    @param subject 领域工作目标 / Domain-work subject.
    @param status 状态机判别值 / State-machine discriminator.
    @param progress 可选进度快照 / Optional progress snapshot.
    @param result_refs 仅成功态允许的结果引用 / Result references allowed only on success.
    @param problem 仅失败态要求的问题 / Problem required only for failure.
    @param started_at 实际开始时刻 / Actual start instant.
    @param finished_at 终态完成时刻 / Terminal completion instant.
    """

    meta: ResourceMeta[JobId]
    workspace_id: WorkspaceId
    kind: str
    subject: ResourceRef
    status: JobStatus = JobStatus.QUEUED
    progress: JobProgress | None = None
    result_refs: tuple[ResourceRef, ...] = ()
    problem: ProblemDetails | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        """@brief 穷尽校验 Job 判别状态 / Exhaustively validate discriminated Job state.

        @raise PlatformDomainError 标识、状态关联或时间线非法时抛出 / Raised for invalid
            identifiers, state associations, or timeline.
        """
        _require_opaque_id(self.meta.id, "job id")
        _require_opaque_id(self.workspace_id, "job workspace id")
        _require_pattern(self.kind, _RESOURCE_TYPE, "job kind")
        if len(self.result_refs) > 50:
            raise PlatformDomainError("job result references cannot exceed 50 entries")
        _validate_job_timeline(self)
        _validate_job_discriminant(self)

    @property
    def is_terminal(self) -> bool:
        """@brief 判断 Job 是否终态 / Test whether the Job is terminal.

        @return 当前状态为终态时为真 / True when the current state is terminal.
        """
        return self.status.is_terminal

    def start(
        self,
        *,
        at: datetime,
        progress: JobProgress | None = None,
    ) -> Job:
        """@brief 将 queued Job 开始为 running / Start a queued Job as running.

        @param at 实际开始时刻 / Actual start instant.
        @param progress 可选首个进度快照 / Optional initial progress snapshot.
        @return running 的下一 revision / Next revision in running state.
        @raise JobTransitionError 当前不是 queued 时抛出 / Raised unless currently queued.
        """
        self._require_transition(JobStatus.RUNNING, {JobStatus.QUEUED})
        return replace(
            self,
            meta=self.meta.advance(at),
            status=JobStatus.RUNNING,
            progress=progress,
            started_at=at,
        )

    def report_progress(self, progress: JobProgress, *, at: datetime) -> Job:
        """@brief 更新 running Job 的进度 / Update progress for a running Job.

        @param progress 新进度快照 / New progress snapshot.
        @param at 更新时刻 / Update instant.
        @return running 的下一 revision / Next running revision.
        @raise JobTransitionError 当前不是 running 时抛出 / Raised unless currently running.
        """
        self._require_transition(JobStatus.RUNNING, {JobStatus.RUNNING})
        return replace(self, meta=self.meta.advance(at), progress=progress)

    def succeed(
        self,
        result_refs: Sequence[ResourceRef],
        *,
        at: datetime,
        progress: JobProgress | None = None,
    ) -> Job:
        """@brief 完成 running Job / Complete a running Job successfully.

        @param result_refs 成功资源引用，最多 50 个 / Successful resource references, at most 50.
        @param at 完成时刻 / Completion instant.
        @param progress 可选最终进度；省略则保留现值 / Optional final progress; omission retains it.
        @return succeeded 的下一 revision / Next revision in succeeded state.
        @raise JobTransitionError 当前不是 running 时抛出 / Raised unless currently running.
        """
        self._require_transition(JobStatus.SUCCEEDED, {JobStatus.RUNNING})
        return replace(
            self,
            meta=self.meta.advance(at),
            status=JobStatus.SUCCEEDED,
            progress=self.progress if progress is None else progress,
            result_refs=tuple(result_refs),
            finished_at=at,
        )

    def fail(
        self,
        problem: ProblemDetails,
        *,
        at: datetime,
        progress: JobProgress | None = None,
    ) -> Job:
        """@brief 以结构化 problem 终止 running Job / Fail a running Job with a problem.

        @param problem 契约级失败详情 / Contract-level failure details.
        @param at 失败时刻 / Failure instant.
        @param progress 可选最终进度；省略则保留现值 / Optional final progress; omission retains it.
        @return failed 的下一 revision / Next revision in failed state.
        @raise JobTransitionError 当前不是 running 时抛出 / Raised unless currently running.
        """
        self._require_transition(JobStatus.FAILED, {JobStatus.RUNNING})
        return replace(
            self,
            meta=self.meta.advance(at),
            status=JobStatus.FAILED,
            progress=self.progress if progress is None else progress,
            problem=problem,
            finished_at=at,
        )

    def cancel(self, *, at: datetime) -> Job:
        """@brief 取消尚可中止的 Job / Cancel a Job that can still be stopped.

        @param at 取消生效时刻 / Cancellation instant.
        @return cancelled 的下一 revision / Next revision in cancelled state.
        @raise JobTransitionError 当前不是 queued/running 时抛出 / Raised unless queued or running.
        @note 相同请求的幂等重放由外层 durable idempotency 处理，不给状态机增加 self-loop。
            / Durable outer idempotency handles request replay; the state machine has no self-loop.
        """
        self._require_transition(JobStatus.CANCELLED, {JobStatus.QUEUED, JobStatus.RUNNING})
        return replace(
            self,
            meta=self.meta.advance(at),
            status=JobStatus.CANCELLED,
            finished_at=at,
        )

    def expire(self, *, at: datetime) -> Job:
        """@brief 使尚未开始的 Job 过期 / Expire a Job that never started.

        @param at 过期生效时刻 / Expiration instant.
        @return expired 的下一 revision / Next revision in expired state.
        @raise JobTransitionError 当前不是 queued 时抛出 / Raised unless currently queued.
        """
        self._require_transition(JobStatus.EXPIRED, {JobStatus.QUEUED})
        return replace(
            self,
            meta=self.meta.advance(at),
            status=JobStatus.EXPIRED,
            finished_at=at,
        )

    def _require_transition(
        self,
        requested: JobStatus,
        allowed_from: set[JobStatus],
    ) -> None:
        """@brief 检查一个显式迁移边 / Check one explicit transition edge.

        @param requested 请求目标状态 / Requested target state.
        @param allowed_from 唯一允许的来源状态 / Sole allowed source states.
        @raise JobTransitionError 当前状态不在允许集合时抛出 / Raised when the current state is
            not allowed.
        """
        if self.status not in allowed_from:
            raise JobTransitionError(self.status, requested)


@dataclass(frozen=True, slots=True)
class ApiArtifactContentUrl:
    """@brief 与配置 API Origin 同源的精确 Artifact 地址 / Exact same-origin Artifact URL.

    @param value 完整下载 URL / Complete download URL.
    @param api_origin 受信部署 API Origin / Trusted deployment API origin.
    @param workspace_id URL 中的 Workspace / Workspace embedded in the URL.
    @param artifact_id URL 中的 Artifact / Artifact embedded in the URL.
    """

    value: str
    api_origin: str
    workspace_id: WorkspaceId
    artifact_id: ArtifactId

    def __post_init__(self) -> None:
        """@brief 校验同源且精确路径 / Validate same origin and exact path.

        @raise PlatformDomainError Origin 或 URL 不精确时抛出 / Raised for an invalid origin or
            non-exact URL.
        """
        _require_network_origin(self.api_origin)
        _require_opaque_id(self.workspace_id, "artifact URL workspace id")
        _require_opaque_id(self.artifact_id, "artifact URL artifact id")
        expected = (
            f"{self.api_origin}/api/v2/workspaces/{self.workspace_id}"
            f"/artifacts/{self.artifact_id}/content"
        )
        if self.value != expected:
            raise PlatformDomainError("API artifact URL must exactly identify its workspace artifact")

    @classmethod
    def build(
        cls,
        api_origin: str,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> ApiArtifactContentUrl:
        """@brief 从受信 Origin 构造精确 URL / Build an exact URL from a trusted origin.

        @param api_origin 部署 API Origin / Deployment API origin.
        @param workspace_id 所属 Workspace / Owning Workspace.
        @param artifact_id Artifact 标识 / Artifact identifier.
        @return 可验证的同源 URL 值 / Verifiable same-origin URL value.
        """
        return cls(
            f"{api_origin}/api/v2/workspaces/{workspace_id}/artifacts/{artifact_id}/content",
            api_origin,
            workspace_id,
            artifact_id,
        )


@dataclass(frozen=True, slots=True)
class SignedArtifactContentUrl:
    """@brief 显式短期单用途跨域签名 URL / Explicit short-lived single-use cross-origin URL.

    @param value HTTPS 签名地址 / HTTPS signed URL.
    @param token_id 签发策略生成的单用途 token 标识 / Single-use token ID from the signer policy.
    @param issued_at 签发时刻 / Issuance instant.
    @param expires_at URL 过期时刻 / URL expiration instant.
    @param maximum_lifetime 签发策略允许的最大寿命 / Maximum lifetime allowed by signer policy.
    @param single_use 必须为真；用于类型表达单用途约束 / Must be true; encodes single-use intent.
    """

    value: str
    token_id: str
    issued_at: datetime
    expires_at: datetime
    maximum_lifetime: timedelta
    single_use: Literal[True] = True

    def __post_init__(self) -> None:
        """@brief 校验 HTTPS、短期与单用途证明 / Validate HTTPS, lifetime, and single-use proof.

        @raise PlatformDomainError 签名授权不满足策略时抛出 / Raised when the signed grant violates
            policy.
        """
        _require_https_url(self.value, "signed artifact URL")
        _require_opaque_id(self.token_id, "signed artifact URL token id")
        _require_aware(self.issued_at, "signed artifact URL issued_at")
        _require_aware(self.expires_at, "signed artifact URL expires_at")
        if self.maximum_lifetime <= timedelta(0):
            raise PlatformDomainError("signed artifact URL maximum lifetime must be positive")
        lifetime = self.expires_at - self.issued_at
        if lifetime <= timedelta(0) or lifetime > self.maximum_lifetime:
            raise PlatformDomainError("signed artifact URL must obey its short-lived policy")
        if self.single_use is not True:
            raise PlatformDomainError("signed artifact URL must be single-use")


type ArtifactContentLocation = ApiArtifactContentUrl | SignedArtifactContentUrl
"""@brief Artifact 内容地址判别联合 / Discriminated Artifact content-location union."""


@dataclass(frozen=True, slots=True)
class Artifact:
    """@brief Workspace 隔离的不可变 Artifact 元数据 / Workspace-isolated immutable Artifact metadata.

    @param meta 资源 revision 元数据 / Resource-revision metadata.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param kind 契约 Artifact 种类 / Contract Artifact kind.
    @param subject 产物来源资源 / Source resource.
    @param media_type 下载媒体类型 / Download media type.
    @param size_bytes 内容字节数 / Content size in bytes.
    @param sha256 下载前后校验摘要 / Digest verified before and after download.
    @param content_location 同源或显式签名地址 / Same-origin or explicitly signed location.
    @param page_count 可选页数 / Optional page count.
    @param expires_at 可选 Artifact 生命周期终点 / Optional Artifact lifetime endpoint.
    """

    meta: ResourceMeta[ArtifactId]
    workspace_id: WorkspaceId
    kind: ArtifactKind
    subject: ResourceRef
    media_type: str
    size_bytes: int
    sha256: str
    content_location: ArtifactContentLocation
    page_count: int | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        """@brief 校验 Artifact 元数据关联 / Validate Artifact metadata associations.

        @raise PlatformDomainError 标识、媒体、摘要、URL 或生命周期非法时抛出 / Raised for an
            invalid identity, media type, digest, URL, or lifetime.
        """
        _require_opaque_id(self.meta.id, "artifact id")
        _require_opaque_id(self.workspace_id, "artifact workspace id")
        _require_pattern(self.media_type, _MEDIA_TYPE, "artifact media type")
        if not 0 <= self.size_bytes <= _MAX_ARTIFACT_BYTES:
            raise PlatformDomainError("artifact size must be between zero and one GiB")
        _require_pattern(self.sha256, _SHA256, "artifact SHA-256")
        if self.page_count is not None and self.page_count < 1:
            raise PlatformDomainError("artifact page count must be at least one")
        if self.expires_at is not None:
            _require_aware(self.expires_at, "artifact expires_at")
            if self.expires_at <= self.meta.created_at:
                raise PlatformDomainError("artifact expiration must follow creation")
        if isinstance(self.content_location, ApiArtifactContentUrl):
            if (
                self.content_location.workspace_id != self.workspace_id
                or self.content_location.artifact_id != self.meta.id
            ):
                raise PlatformDomainError("artifact URL must identify the current artifact")
        elif self.expires_at is not None and self.content_location.expires_at > self.expires_at:
            raise PlatformDomainError("signed URL cannot outlive the artifact")

    @property
    def content_url(self) -> str:
        """@brief 投影契约 ``content_url`` 字符串 / Project the contract ``content_url`` string.

        @return 同源或签名 URL / Same-origin or signed URL.
        """
        return self.content_location.value

    def is_expired(self, at: datetime) -> bool:
        """@brief 判断 Artifact 在指定时刻是否过期 / Test Artifact expiration at an instant.

        @param at 待判断带时区时刻 / Timezone-aware instant to test.
        @return 有过期时间且不晚于 ``at`` 时为真 / True when expiration exists and is not after ``at``.
        """
        _require_aware(at, "artifact expiration check")
        return self.expires_at is not None and self.expires_at <= at


@dataclass(frozen=True, slots=True)
class PdfRect:
    """@brief PDF point 坐标矩形 / PDF point-coordinate rectangle.

    @param x 左上角横坐标 / Left x coordinate.
    @param y 左上角纵坐标 / Top y coordinate.
    @param width 非负宽度 / Non-negative width.
    @param height 非负高度 / Non-negative height.
    @param unit 固定 point 单位 / Fixed point unit.
    """

    x: float
    y: float
    width: float
    height: float
    unit: PdfUnit = PdfUnit.POINT

    def __post_init__(self) -> None:
        """@brief 校验有限坐标与非负尺寸 / Validate finite coordinates and non-negative dimensions.

        @raise PlatformDomainError 坐标非有限或尺寸为负时抛出 / Raised for non-finite coordinates
            or negative dimensions.
        """
        for label, value in (("x", self.x), ("y", self.y), ("width", self.width), ("height", self.height)):
            if isinstance(value, bool) or not math.isfinite(value):
                raise PlatformDomainError(f"PDF rectangle {label} must be finite")
        if self.width < 0 or self.height < 0:
            raise PlatformDomainError("PDF rectangle dimensions cannot be negative")


@dataclass(frozen=True, slots=True)
class PdfSourceNode:
    """@brief 一个 Resume 字段到 PDF 矩形的映射 / Mapping from one Resume field to PDF rectangles.

    @param entity_id Resume entity ID / Resume entity identifier.
    @param field_path 字段路径，最多 20 段 / Field path with at most 20 segments.
    @param page 一起始页码 / One-based page number.
    @param rects 非空矩形集合 / Non-empty rectangle collection.
    """

    entity_id: str
    field_path: tuple[str, ...]
    page: int
    rects: tuple[PdfRect, ...]

    def __post_init__(self) -> None:
        """@brief 校验 source node / Validate the source node.

        @raise PlatformDomainError entity、路径、页码或矩形集合非法时抛出 / Raised for an
            invalid entity, path, page, or rectangle collection.
        """
        _require_opaque_id(self.entity_id, "PDF source entity id")
        if len(self.field_path) > 20 or any(len(part) > 100 for part in self.field_path):
            raise PlatformDomainError("PDF source field path violates contract bounds")
        if self.page < 1:
            raise PlatformDomainError("PDF source page must be at least one")
        if not self.rects:
            raise PlatformDomainError("PDF source node requires at least one rectangle")


@dataclass(frozen=True, slots=True)
class PdfSourceMap:
    """@brief Resume PDF Artifact 的可验证 source map / Verifiable source map for a Resume PDF Artifact.

    @param artifact_id 对应 Artifact ID / Corresponding Artifact ID.
    @param resume_id 对应 Resume ID / Corresponding Resume ID.
    @param resume_revision 对应 Resume revision / Corresponding Resume revision.
    @param nodes 最多 10000 个映射节点 / At most 10,000 mapping nodes.
    """

    artifact_id: ArtifactId
    resume_id: str
    resume_revision: int
    nodes: tuple[PdfSourceNode, ...]

    def __post_init__(self) -> None:
        """@brief 校验 source map 自身边界 / Validate intrinsic source-map bounds.

        @raise PlatformDomainError 标识、revision 或节点数非法时抛出 / Raised for an invalid
            identity, revision, or node count.
        """
        _require_opaque_id(self.artifact_id, "PDF source-map artifact id")
        _require_opaque_id(self.resume_id, "PDF source-map resume id")
        if self.resume_revision < 1:
            raise PlatformDomainError("PDF source-map resume revision must be at least one")
        if len(self.nodes) > 10_000:
            raise PlatformDomainError("PDF source map cannot exceed 10000 nodes")

    def validate_for(self, artifact: Artifact) -> None:
        """@brief 交叉校验 Artifact、Resume revision 与页边界 / Cross-check Artifact, Resume revision, and pages.

        @param artifact source map 所属 Artifact / Artifact owning this source map.
        @raise PlatformDomainError Artifact 不是匹配的 Resume PDF 时抛出 / Raised unless the
            Artifact is the matching Resume PDF.
        """
        subject = artifact.subject
        if artifact.meta.id != self.artifact_id:
            raise PlatformDomainError("PDF source map references a different artifact")
        if artifact.media_type.lower() != "application/pdf" or artifact.page_count is None:
            raise PlatformDomainError("PDF source map requires a paginated PDF artifact")
        if (
            subject.resource_type != "resume"
            or subject.id != self.resume_id
            or subject.revision != self.resume_revision
        ):
            raise PlatformDomainError("PDF source map must match the artifact Resume revision")
        if any(node.page > artifact.page_count for node in self.nodes):
            raise PlatformDomainError("PDF source node exceeds the artifact page count")


@dataclass(frozen=True, slots=True)
class ApiEvent:
    """@brief Workspace SSE 的通用变化提示 / Generic change hint for Workspace SSE.

    @param event_id 客户端去重 ID / Client deduplication ID.
    @param sequence 单 Workspace stream 单调序号 / Monotonic sequence in one Workspace stream.
    @param type 稳定开放事件类型 / Stable open event type.
    @param occurred_at 事件发生时刻 / Event occurrence instant.
    @param subject 最终权威 GET 的资源目标 / Resource whose GET is authoritative.
    @param data 最多 40 个属性的增量提示 / Incremental hint with at most 40 properties.
    @param trace_id 可选 32 位小写十六进制 trace ID / Optional lowercase 32-hex trace ID.
    """

    event_id: ApiEventId
    sequence: int
    type: str
    occurred_at: datetime
    subject: ResourceRef
    data: Mapping[str, JsonValue]
    trace_id: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验并深度冻结 API event / Validate and deeply freeze the API event.

        @raise PlatformDomainError 信封或 JSON data 非法时抛出 / Raised for an invalid envelope
            or JSON data.
        """
        _require_opaque_id(self.event_id, "API event id")
        if self.sequence < 1:
            raise PlatformDomainError("API event sequence must be at least one")
        _require_pattern(self.type, _STABLE_NAME, "API event type")
        _require_aware(self.occurred_at, "API event occurred_at")
        if len(self.data) > 40:
            raise PlatformDomainError("API event data cannot exceed 40 properties")
        frozen: dict[str, JsonValue] = {}
        for key, value in self.data.items():
            if not isinstance(key, str):
                raise PlatformDomainError("API event data keys must be strings")
            frozen[key] = _freeze_json(value, f"API event data.{key}")
        object.__setattr__(self, "data", MappingProxyType(frozen))
        if self.trace_id is not None:
            _require_pattern(self.trace_id, _TRACE_ID, "API event trace id")


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """@brief Workspace 审计事件 / Workspace audit event.

    @param id 审计事件 ID / Audit-event ID.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param occurred_at 决策发生时刻 / Decision occurrence instant.
    @param actor 发起者资源引用 / Actor resource reference.
    @param action 稳定审计动作 / Stable audit action.
    @param target 目标资源引用 / Target resource reference.
    @param outcome 允许、拒绝或失败 / Allowed, denied, or failed outcome.
    @param request_id 关联请求 ID / Correlated request ID.
    """

    id: AuditEventId
    workspace_id: WorkspaceId
    occurred_at: datetime
    actor: ResourceRef
    action: str
    target: ResourceRef
    outcome: AuditOutcome
    request_id: str

    def __post_init__(self) -> None:
        """@brief 校验 AuditEvent 信封 / Validate the AuditEvent envelope.

        @raise PlatformDomainError 标识、时间或 action 非法时抛出 / Raised for an invalid
            identity, timestamp, or action.
        """
        _require_opaque_id(self.id, "audit event id")
        _require_opaque_id(self.workspace_id, "audit event workspace id")
        _require_aware(self.occurred_at, "audit event occurred_at")
        _require_pattern(self.action, _STABLE_NAME, "audit action")
        _require_opaque_id(self.request_id, "audit request id")


def _validate_job_timeline(job: Job) -> None:
    """@brief 校验 Job 时间线 / Validate the Job timeline.

    @param job 候选 Job / Candidate Job.
    @raise PlatformDomainError 时间缺失、倒退或超过资源更新时间时抛出 / Raised for a missing,
        reversed, or future-associated timestamp.
    """
    for label, value in (("started_at", job.started_at), ("finished_at", job.finished_at)):
        if value is not None:
            _require_aware(value, f"job {label}")
            if value < job.meta.created_at or value > job.meta.updated_at:
                raise PlatformDomainError(f"job {label} must lie within its resource timeline")
    if (
        job.started_at is not None
        and job.finished_at is not None
        and job.finished_at < job.started_at
    ):
        raise PlatformDomainError("job finished_at cannot precede started_at")


def _validate_job_discriminant(job: Job) -> None:
    """@brief 穷尽校验 Job 状态关联字段 / Exhaustively validate Job state-associated fields.

    @param job 候选 Job / Candidate Job.
    @raise PlatformDomainError 状态关联字段不一致时抛出 / Raised for inconsistent state fields.
    """
    has_results = bool(job.result_refs)
    if job.status is JobStatus.QUEUED:
        if job.started_at is not None or job.finished_at is not None or job.problem is not None:
            raise PlatformDomainError("queued job cannot have start, finish, or problem")
    elif job.status is JobStatus.RUNNING:
        if job.started_at is None or job.finished_at is not None or job.problem is not None:
            raise PlatformDomainError("running job requires only started_at")
    elif job.status is JobStatus.SUCCEEDED:
        if job.started_at is None or job.finished_at is None or job.problem is not None:
            raise PlatformDomainError("succeeded job requires start and finish without problem")
    elif job.status is JobStatus.FAILED:
        if job.started_at is None or job.finished_at is None or job.problem is None:
            raise PlatformDomainError("failed job requires start, finish, and problem")
    elif job.status is JobStatus.CANCELLED:
        if job.finished_at is None or job.problem is not None:
            raise PlatformDomainError("cancelled job requires finish without problem")
    elif job.status is JobStatus.EXPIRED and (
        job.started_at is not None or job.finished_at is None or job.problem is not None
    ):
        raise PlatformDomainError("expired job requires only finished_at")
    if has_results and job.status is not JobStatus.SUCCEEDED:
        raise PlatformDomainError("only succeeded jobs may expose result references")


def _freeze_extensions(values: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    """@brief 校验并深度冻结 namespaced extensions / Validate and freeze namespaced extensions.

    @param values 候选扩展映射 / Candidate extension mapping.
    @return 深度不可变副本 / Deeply immutable copy.
    @raise PlatformDomainError 键或值非法时抛出 / Raised for an invalid key or value.
    """
    if len(values) > 32:
        raise PlatformDomainError("problem extensions cannot exceed 32 entries")
    frozen: dict[str, JsonValue] = {}
    for key, value in values.items():
        _require_pattern(key, _EXTENSION_KEY, "problem extension key")
        frozen[key] = _freeze_json(value, f"problem extension {key}")
    return MappingProxyType(frozen)


def _freeze_json(value: object, label: str) -> JsonValue:
    """@brief 验证 JSON 值并生成不可变副本 / Validate JSON and produce an immutable copy.

    @param value 候选 JSON 值 / Candidate JSON value.
    @param label 错误上下文 / Error context.
    @return 深度不可变 JSON 值 / Deeply immutable JSON value.
    @raise PlatformDomainError 值不是有限 JSON 时抛出 / Raised unless the value is finite JSON.
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PlatformDomainError(f"{label} number must be finite")
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, JsonValue] = {}
        for key, nested in value.items():
            if not isinstance(key, str):
                raise PlatformDomainError(f"{label} object keys must be strings")
            frozen[key] = _freeze_json(nested, f"{label}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_freeze_json(item, f"{label}[]") for item in value)
    raise PlatformDomainError(f"{label} must be a JSON value")


def _require_json_scalar(value: object, label: str) -> None:
    """@brief 要求有限 JSON 标量 / Require a finite JSON scalar.

    @param value 候选值 / Candidate value.
    @param label 错误上下文 / Error context.
    @raise PlatformDomainError 值不是允许标量时抛出 / Raised unless the value is an allowed scalar.
    """
    if value is None or isinstance(value, (bool, str, int)):
        return
    if isinstance(value, float) and math.isfinite(value):
        return
    raise PlatformDomainError(f"{label} must be a finite JSON scalar")


def _require_opaque_id(value: str, label: str) -> None:
    """@brief 校验 API v2 不透明 ID / Validate an API v2 opaque ID.

    @param value 候选 ID / Candidate ID.
    @param label 错误标签 / Error label.
    @raise PlatformDomainError ID 不匹配契约时抛出 / Raised unless the ID matches the contract.
    """
    _require_pattern(value, _OPAQUE_ID, label)


def _require_pattern(value: str, pattern: re.Pattern[str], label: str) -> None:
    """@brief 要求字符串完整匹配语法 / Require a full regular-expression match.

    @param value 候选字符串 / Candidate string.
    @param pattern 已编译语法 / Compiled grammar.
    @param label 错误标签 / Error label.
    @raise PlatformDomainError 不匹配时抛出 / Raised on mismatch.
    """
    if pattern.fullmatch(value) is None:
        raise PlatformDomainError(f"{label} does not satisfy the API v2 grammar")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 要求时区感知 datetime / Require a timezone-aware datetime.

    @param value 候选时刻 / Candidate instant.
    @param label 错误标签 / Error label.
    @raise PlatformDomainError 缺少 UTC offset 时抛出 / Raised when no UTC offset exists.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise PlatformDomainError(f"{label} must be timezone-aware")


def _require_https_url(value: str, label: str) -> None:
    """@brief 要求无用户信息的绝对 HTTPS URL / Require an absolute HTTPS URL without userinfo.

    @param value 候选 URL / Candidate URL.
    @param label 错误标签 / Error label.
    @raise PlatformDomainError URL 不安全时抛出 / Raised for an unsafe URL.
    """
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise PlatformDomainError(f"{label} is invalid") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or _has_control(value)
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise PlatformDomainError(f"{label} must be a safe absolute HTTPS URL")


def _require_network_origin(value: str) -> None:
    """@brief 校验可配置 API Network Origin / Validate a configured API network origin.

    @param value 候选 Origin / Candidate origin.
    @raise PlatformDomainError 非 HTTPS 或非冻结 dev HTTP Origin 时抛出 / Raised unless HTTPS or
        the frozen development HTTP origin.
    """
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise PlatformDomainError("API origin is invalid") from exc
    is_https = parsed.scheme == "https" and parsed.hostname is not None
    is_dev = value == "http://dev.hmalliances.org:9000"
    if (
        not (is_https or is_dev)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise PlatformDomainError("API origin must be a canonical HTTPS origin or frozen dev origin")


def _require_uri_reference(value: str, label: str, maximum_length: int) -> None:
    """@brief 校验安全 URI-reference / Validate a safe URI reference.

    @param value 候选 URI-reference / Candidate URI reference.
    @param label 错误标签 / Error label.
    @param maximum_length 最大字符数 / Maximum character count.
    @raise PlatformDomainError 值过长或含非法字符时抛出 / Raised for an oversized or unsafe value.
    """
    if len(value) > maximum_length or _has_control(value) or any(char.isspace() for char in value):
        raise PlatformDomainError(f"{label} is not a safe URI reference")
    try:
        urlsplit(value)
    except ValueError as exc:
        raise PlatformDomainError(f"{label} is not a valid URI reference") from exc


def _has_control(value: str) -> bool:
    """@brief 判断字符串是否含控制字符 / Test whether a string contains control characters.

    @param value 候选字符串 / Candidate string.
    @return 含 C0/C1 控制字符时为真 / True when C0/C1 controls are present.
    """
    return any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value)


__all__ = [
    "ApiArtifactContentUrl",
    "ApiEvent",
    "ApiEventId",
    "Artifact",
    "ArtifactContentLocation",
    "ArtifactId",
    "ArtifactKind",
    "AuditEvent",
    "AuditEventId",
    "AuditOutcome",
    "Job",
    "JobId",
    "JobProgress",
    "JobProgressUnit",
    "JobStatus",
    "JobTransitionError",
    "JsonValue",
    "PdfRect",
    "PdfSourceMap",
    "PdfSourceNode",
    "PdfUnit",
    "PlatformDomainError",
    "ProblemDetails",
    "ProblemFieldError",
    "ResourceRef",
    "SignedArtifactContentUrl",
]
