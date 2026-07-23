"""@brief API v2 Interview 领域核心 / API v2 Interview domain core.

本模块表达 ``contracts/v2`` 5.5 的 Scenario→Session→Realtime input/Transcript→Report
生命周期。统一 Job、Artifact 和 ResourceRef 直接来自 platform/resources；模型 provider
只能产出公开安全的报告草稿，私有推理和原始 provider payload 没有持久化类型入口。
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Literal, NewType
from urllib.parse import urlsplit

from backend.domain.knowledge_retrieval import (
    InferenceIntent,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    ModelRegion,
)
from backend.domain.platform import Artifact, Job, JobId, JobStatus, JsonValue
from backend.domain.principals import DomainInvariantError, ResourceMeta, UserId, WorkspaceId
from backend.domain.resources import ResourceRef

InterviewScenarioId = NewType("InterviewScenarioId", str)
"""@brief InterviewScenario 不透明标识 / Opaque InterviewScenario identifier."""

InterviewSessionId = NewType("InterviewSessionId", str)
"""@brief InterviewSession 不透明标识 / Opaque InterviewSession identifier."""

RealtimeConnectionId = NewType("RealtimeConnectionId", str)
"""@brief 短期 RealtimeConnection 标识 / Short-lived RealtimeConnection identifier."""

RealtimeInputId = NewType("RealtimeInputId", str)
"""@brief 客户端实时输入幂等标识 / Client realtime-input idempotency identifier."""

TranscriptSegmentId = NewType("TranscriptSegmentId", str)
"""@brief Transcript segment 标识 / Transcript-segment identifier."""

InterviewReportId = NewType("InterviewReportId", str)
"""@brief InterviewReport 标识 / InterviewReport identifier."""

InterviewOutboxId = NewType("InterviewOutboxId", str)
"""@brief Interview 统一 outbox 记录标识 / Interview unified-outbox record identifier."""

INTERVIEW_END_JOB_KIND = "interview.end"
"""@brief 统一 Job 的 Session end kind / Session-end kind in the unified Job store."""

INTERVIEW_REPORT_JOB_KIND = "interview.report"
"""@brief 统一 Job 的 Report generation kind / Report-generation kind in the unified Job store."""

_OPAQUE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API v2 opaque ID 语法 / API v2 opaque-ID grammar."""

_STABLE_NAME = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief 稳定名称语法 / Stable-name grammar."""

_LOCALE = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")
"""@brief 契约 Locale 语法 / Contract Locale grammar."""

_SHA256 = re.compile(r"^[a-f0-9]{64}$")
"""@brief 小写 SHA-256 语法 / Lowercase SHA-256 grammar."""

_MAX_REALTIME_LIFETIME = timedelta(minutes=15)
"""@brief 单次实时连接授权最大寿命 / Maximum lifetime of one realtime connection grant."""


class InterviewDomainError(DomainInvariantError):
    """@brief Interview V2 领域不变量错误 / Interview V2 domain-invariant error."""


class InterviewTransitionError(InterviewDomainError):
    """@brief Scenario 或 Session 状态迁移被拒绝 / Scenario or Session transition rejected."""


class InterviewScenarioStatus(StrEnum):
    """@brief Scenario 状态 / Scenario states."""

    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"


class InterviewDifficulty(StrEnum):
    """@brief 面试难度 / Interview difficulty."""

    INTRODUCTORY = "introductory"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"
    ADAPTIVE = "adaptive"


class AvatarOutputMode(StrEnum):
    """@brief Avatar 输出模式 / Avatar output modes."""

    NONE = "none"
    AUDIO_ONLY = "audio_only"
    CLIENT_RENDER = "client_render"
    SERVER_VIDEO = "server_video"


class FallbackTransport(StrEnum):
    """@brief 媒体 fallback transport / Media fallback transports."""

    NONE = "none"
    AUDIO_ONLY = "audio_only"
    WEBSOCKET = "websocket"


class InterviewSessionStatus(StrEnum):
    """@brief InterviewSession 状态 / InterviewSession states."""

    CREATED = "created"
    CONNECTING = "connecting"
    ACTIVE = "active"
    ENDING = "ending"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """@brief 判断是否终态 / Test whether the state is terminal."""
        return self in {
            InterviewSessionStatus.COMPLETED,
            InterviewSessionStatus.FAILED,
            InterviewSessionStatus.CANCELLED,
        }


class EndInterviewReason(StrEnum):
    """@brief Session 结束原因 / Session end reasons."""

    COMPLETED = "completed"
    USER_CANCELLED = "user_cancelled"
    TECHNICAL_FAILURE = "technical_failure"


class RealtimeTransport(StrEnum):
    """@brief Realtime transport / Realtime transports."""

    WEBRTC = "webrtc"
    WEBSOCKET = "websocket"


class TranscriptSpeaker(StrEnum):
    """@brief Transcript speaker / Transcript speakers."""

    INTERVIEWER = "interviewer"
    CANDIDATE = "candidate"
    SYSTEM = "system"


class RealtimeControl(StrEnum):
    """@brief 客户端实时控制信号 / Client realtime-control signals."""

    CONNECTED = "connected"
    MEDIA_STARTED = "media_started"
    HEARTBEAT = "heartbeat"
    DISCONNECTED = "disconnected"


class ActionPriority(StrEnum):
    """@brief 行动计划优先级 / Action-plan priorities."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True, slots=True)
class ScoreScale:
    """@brief Rubric 数值范围 / Numeric rubric scale."""

    minimum: float
    maximum: float
    labels: Mapping[str, str] = MappingProxyType({})

    def __post_init__(self) -> None:
        """@brief 校验有限递增范围并冻结 labels / Validate a finite increasing range and freeze labels."""
        if not math.isfinite(self.minimum) or not math.isfinite(self.maximum):
            raise InterviewDomainError("score scale bounds must be finite")
        if self.minimum >= self.maximum:
            raise InterviewDomainError("score scale minimum must be below maximum")
        if len(self.labels) > 20 or any(len(value) > 200 for value in self.labels.values()):
            raise InterviewDomainError("score scale labels violate bounds")
        object.__setattr__(self, "labels", MappingProxyType(dict(self.labels)))

    def contains(self, value: float) -> bool:
        """@brief 判断 score 是否在范围内 / Test whether a score lies in the scale."""
        return math.isfinite(value) and self.minimum <= value <= self.maximum


@dataclass(frozen=True, slots=True)
class RubricDimension:
    """@brief 冻结的 Rubric dimension / Frozen rubric dimension."""

    dimension_id: str
    name: str
    description: str
    weight: float
    observable_indicators: tuple[str, ...]
    scoring_scale: ScoreScale

    def __post_init__(self) -> None:
        """@brief 校验 dimension identity、文本与权重 / Validate dimension identity, text, and weight."""
        _require_opaque_id(self.dimension_id, "rubric dimension id")
        _require_text(self.name, "rubric dimension name", 1, 200)
        _require_text(self.description, "rubric dimension description", 1, 4_000)
        if not math.isfinite(self.weight) or not 0 < self.weight <= 1:
            raise InterviewDomainError("rubric dimension weight must be in (0,1]")
        _require_unique_texts(self.observable_indicators, "rubric indicators", 50, 1_000)


@dataclass(frozen=True, slots=True)
class InterviewRubric:
    """@brief 可版本化、Session 创建时冻结的 Rubric / Versioned rubric frozen at Session creation."""

    rubric_id: str
    rubric_version: str
    name: str
    dimensions: tuple[RubricDimension, ...]
    overall_scale: ScoreScale

    def __post_init__(self) -> None:
        """@brief 校验唯一 dimensions 与权重和为一 / Validate unique dimensions whose weights sum to one."""
        _require_opaque_id(self.rubric_id, "rubric id")
        _require_text(self.rubric_version, "rubric version", 1, 80)
        _require_text(self.name, "rubric name", 1, 200)
        if not 1 <= len(self.dimensions) <= 50:
            raise InterviewDomainError("rubric must contain 1 to 50 dimensions")
        ids = tuple(item.dimension_id for item in self.dimensions)
        if len(set(ids)) != len(ids):
            raise InterviewDomainError("rubric dimension ids must be unique")
        if not math.isclose(math.fsum(item.weight for item in self.dimensions), 1.0, abs_tol=1e-9):
            raise InterviewDomainError("rubric dimension weights must sum to one")


@dataclass(frozen=True, slots=True)
class JobTarget:
    """@brief 面试职位目标 / Interview job target."""

    title: str
    company: str | None
    location: str | None
    description: str | None
    source_url: str | None
    seniority: str | None
    skills: tuple[str, ...]

    def __post_init__(self) -> None:
        """@brief 校验职位字段和安全 URL / Validate job fields and safe URL."""
        _require_text(self.title, "job title", 1, 300)
        for label, value, maximum in (
            ("company", self.company, 300),
            ("location", self.location, 300),
            ("description", self.description, 100_000),
            ("seniority", self.seniority, 100),
        ):
            if value is not None:
                _require_text(value, label, 0, maximum)
        if self.source_url is not None:
            _require_http_url(self.source_url, "job source URL")
        _require_unique_texts(self.skills, "job skills", 200, 100)


@dataclass(frozen=True, slots=True)
class InterviewScenarioSpec:
    """@brief Scenario 输入的不可变值对象 / Immutable value object for Scenario input."""

    name: str
    description: str
    locale: str
    interview_type: str
    difficulty: InterviewDifficulty
    duration_minutes: int
    target_question_count: int
    focus_areas: tuple[str, ...]
    allow_followups: bool
    allow_barge_in: bool
    rubric: InterviewRubric

    def __post_init__(self) -> None:
        """@brief 校验 Scenario 输入边界 / Validate Scenario-input bounds."""
        _require_text(self.name, "scenario name", 1, 200)
        _require_text(self.description, "scenario description", 0, 4_000)
        _require_locale(self.locale)
        _require_stable_name(self.interview_type, "interview type")
        if not 5 <= self.duration_minutes <= 240:
            raise InterviewDomainError("scenario duration must be 5 to 240 minutes")
        if not 1 <= self.target_question_count <= 100:
            raise InterviewDomainError("target question count must be 1 to 100")
        _require_unique_texts(self.focus_areas, "focus areas", 50, 200)


@dataclass(frozen=True, slots=True)
class InterviewScenarioPatch:
    """@brief UpdateInterviewScenarioRequest 的显式 supplied-field 形式 / Explicit supplied-field form of UpdateInterviewScenarioRequest."""

    values: Mapping[str, object]

    def __post_init__(self) -> None:
        """@brief 拒绝空或未知 patch / Reject empty or unknown patches."""
        allowed = {
            "name",
            "description",
            "locale",
            "interview_type",
            "difficulty",
            "duration_minutes",
            "target_question_count",
            "focus_areas",
            "allow_followups",
            "allow_barge_in",
            "rubric",
            "status",
        }
        if not self.values or not set(self.values) <= allowed:
            raise InterviewDomainError("scenario patch is empty or contains unknown fields")
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))


@dataclass(frozen=True, slots=True)
class InterviewScenario:
    """@brief 强 revision InterviewScenario 聚合 / Strong-revision InterviewScenario aggregate."""

    meta: ResourceMeta[InterviewScenarioId]
    workspace_id: WorkspaceId
    spec: InterviewScenarioSpec
    status: InterviewScenarioStatus = InterviewScenarioStatus.DRAFT

    def __post_init__(self) -> None:
        """@brief 校验 Scenario identity / Validate Scenario identity."""
        _require_opaque_id(self.meta.id, "scenario id")
        _require_opaque_id(self.workspace_id, "scenario workspace id")

    def update(self, patch: InterviewScenarioPatch, *, at: datetime) -> InterviewScenario:
        """@brief 原子应用 PATCH 并推进 revision / Atomically apply a PATCH and advance revision."""
        values = dict(patch.values)
        requested_status = values.pop("status", self.status)
        if not isinstance(requested_status, InterviewScenarioStatus):
            raise InterviewDomainError("scenario patch status has an invalid type")
        _validate_scenario_transition(self.status, requested_status)
        spec_values = {item.name: getattr(self.spec, item.name) for item in fields(self.spec)}
        spec_values.update(values)
        try:
            next_spec = InterviewScenarioSpec(**spec_values)
        except TypeError as error:
            raise InterviewDomainError("scenario patch field has an invalid type") from error
        return replace(
            self,
            meta=self.meta.advance(at),
            spec=next_spec,
            status=requested_status,
        )


@dataclass(frozen=True, slots=True)
class RecordingConsent:
    """@brief 录音、录像和转录的显式同意与 retention / Explicit consent and retention for audio, video, and transcript."""

    record_audio: bool
    record_video: bool
    store_transcript: bool
    retention_days: int
    consented_at: datetime | None
    consent_version: str | None

    def __post_init__(self) -> None:
        """@brief 校验同意证明和 retention / Validate consent proof and retention."""
        if not 0 <= self.retention_days <= 3_650:
            raise InterviewDomainError("recording retention must be 0 to 3650 days")
        if self.consented_at is not None:
            _require_aware(self.consented_at, "recording consented_at")
        requested = self.record_audio or self.record_video or self.store_transcript
        if requested and (self.consented_at is None or not self.consent_version):
            raise InterviewDomainError("recording or transcript storage requires explicit consent")
        if self.consent_version is not None:
            _require_text(self.consent_version, "consent version", 1, 80)

    @property
    def retention_until(self) -> datetime | None:
        """@brief 计算 consent-based retention 截止 / Compute the consent-based retention deadline."""
        if self.consented_at is None:
            return None
        return self.consented_at + timedelta(days=self.retention_days)


@dataclass(frozen=True, slots=True)
class InterviewAvatarPreferences:
    """@brief Avatar 与 voice 偏好 / Avatar and voice preferences."""

    output_mode: AvatarOutputMode
    avatar_id: str | None
    voice_id: str | None
    preferred_audio_codecs: tuple[str, ...]
    preferred_video_codecs: tuple[str, ...]
    include_visemes: bool
    include_expression_cues: bool

    def __post_init__(self) -> None:
        """@brief 校验 avatar IDs 与 codec lists / Validate avatar IDs and codec lists."""
        for label, value in (("avatar id", self.avatar_id), ("voice id", self.voice_id)):
            if value is not None:
                _require_text(value, label, 0, 200)
        _require_unique_texts(self.preferred_audio_codecs, "audio codecs", 20, 80)
        _require_unique_texts(self.preferred_video_codecs, "video codecs", 20, 80)


@dataclass(frozen=True, slots=True)
class InterviewMediaPreferences:
    """@brief Session 媒体偏好 / Session media preferences."""

    user_audio: bool
    user_video: bool
    screen_share: bool
    max_video_width: int
    max_video_height: int
    max_video_fps: int
    avatar: InterviewAvatarPreferences
    fallback_transport: FallbackTransport

    def __post_init__(self) -> None:
        """@brief 校验视频上限 / Validate video limits."""
        if not 1 <= self.max_video_width <= 7_680:
            raise InterviewDomainError("max video width is invalid")
        if not 1 <= self.max_video_height <= 4_320:
            raise InterviewDomainError("max video height is invalid")
        if not 1 <= self.max_video_fps <= 240:
            raise InterviewDomainError("max video FPS is invalid")


@dataclass(frozen=True, slots=True)
class InterviewSessionSpec:
    """@brief Session 创建时冻结的完整执行快照 / Complete execution snapshot frozen at Session creation."""

    scenario_id: InterviewScenarioId
    scenario_revision: int
    rubric_snapshot: InterviewRubric
    resume_ref: ResourceRef | None
    job_target: JobTarget
    knowledge: KnowledgeSelection
    locale: str
    media: InterviewMediaPreferences
    recording: RecordingConsent
    inference: InferenceIntent

    def __post_init__(self) -> None:
        """@brief 校验 Scenario snapshot、Resume 与 consent/media 关联 / Validate snapshot, Resume, and consent/media associations."""
        _require_opaque_id(self.scenario_id, "session scenario id")
        if self.scenario_revision < 1:
            raise InterviewDomainError("session scenario revision must be positive")
        if self.resume_ref is not None and (
            self.resume_ref.resource_type != "resume" or self.resume_ref.revision is None
        ):
            raise InterviewDomainError("session Resume must be an exact resume revision")
        _require_locale(self.locale)
        if self.recording.record_audio and not self.media.user_audio:
            raise InterviewDomainError("audio recording requires user_audio")
        if self.recording.record_video and not self.media.user_video:
            raise InterviewDomainError("video recording requires user_video")


@dataclass(frozen=True, slots=True)
class InterviewKnowledgeContext:
    """@brief Session 执行时授权的精确 Knowledge 版本 / Exact Knowledge version authorized for Session execution."""

    source_id: KnowledgeSourceId
    version_id: KnowledgeSourceVersionId
    policy_version: int

    def __post_init__(self) -> None:
        """@brief 校验 Knowledge identity 与 policy 水位 / Validate Knowledge identity and policy watermark."""
        _require_opaque_id(self.source_id, "Interview Knowledge source id")
        _require_opaque_id(self.version_id, "Interview Knowledge version id")
        if self.policy_version < 1:
            raise InterviewDomainError("Interview Knowledge policy version must be positive")


@dataclass(frozen=True, slots=True)
class InterviewExecutionGrant:
    """@brief Scenario/Resume/Knowledge/model policy 交集证明 / Scenario/Resume/Knowledge/model policy-intersection proof."""

    scenario_ref: ResourceRef
    resume_ref: ResourceRef | None
    agent_scope: str
    model_ref: ResourceRef
    model_region: ModelRegion
    external_model_processing: bool
    knowledge_contexts: tuple[InterviewKnowledgeContext, ...]
    policy_version: int

    def __post_init__(self) -> None:
        """@brief 校验 grant 结构 / Validate grant structure."""
        if self.scenario_ref.resource_type != "interview_scenario" or self.scenario_ref.revision is None:
            raise InterviewDomainError("Interview grant requires an exact Scenario revision")
        if self.resume_ref is not None and (
            self.resume_ref.resource_type != "resume" or self.resume_ref.revision is None
        ):
            raise InterviewDomainError("Interview grant Resume must be an exact revision")
        _require_stable_name(self.agent_scope, "Interview agent scope")
        if self.model_ref.resource_type != "model" or self.model_ref.revision is None:
            raise InterviewDomainError("Interview grant requires an exact model revision")
        if self.policy_version < 1:
            raise InterviewDomainError("Interview execution policy version must be positive")
        sources = tuple(item.source_id for item in self.knowledge_contexts)
        if len(self.knowledge_contexts) > 200 or len(set(sources)) != len(sources):
            raise InterviewDomainError("Interview Knowledge contexts must be source-unique and bounded")

    def validate_for(
        self,
        scenario: InterviewScenario,
        spec: InterviewSessionSpec,
    ) -> None:
        """@brief 交叉校验 grant 与 Session snapshot / Cross-check the grant against the Session snapshot."""
        if scenario.status is not InterviewScenarioStatus.ACTIVE:
            raise InterviewDomainError("Interview grant requires an active Scenario")
        if (
            self.scenario_ref.id != scenario.meta.id
            or self.scenario_ref.revision != scenario.meta.revision
            or spec.scenario_id != scenario.meta.id
            or spec.scenario_revision != scenario.meta.revision
        ):
            raise InterviewDomainError("Interview grant does not match the Scenario snapshot")
        if self.resume_ref != spec.resume_ref:
            raise InterviewDomainError("Interview grant does not match the Resume snapshot")
        if self.agent_scope != spec.knowledge.agent_scope:
            raise InterviewDomainError("Interview grant agent scope is mismatched")
        if self.model_region is not spec.inference.data_region:
            raise InterviewDomainError("Interview grant model region is mismatched")
        if self.external_model_processing and not spec.inference.allow_external_model_processing:
            raise InterviewDomainError("Interview grant exceeds external-processing intent")
        selected = {item.source_id for item in self.knowledge_contexts}
        if selected & set(spec.knowledge.exclude_source_ids):
            raise InterviewDomainError("Interview grant includes an excluded Knowledge source")
        if spec.knowledge.mode is KnowledgeSelectionMode.NONE and selected:
            raise InterviewDomainError("none Knowledge selection cannot authorize context")
        if spec.knowledge.mode is KnowledgeSelectionMode.EXPLICIT and selected != set(
            spec.knowledge.include_source_ids
        ):
            raise InterviewDomainError("explicit Knowledge selection requires every source")
        pins = {item.source_id: item.version_id for item in spec.knowledge.pinned_versions}
        if any(
            pins.get(item.source_id, item.version_id) != item.version_id
            for item in self.knowledge_contexts
        ):
            raise InterviewDomainError("Interview grant violates a pinned Knowledge version")


@dataclass(frozen=True, slots=True)
class InterviewSessionView:
    """@brief 严格对应公开 InterviewSession Schema / Projection matching the public InterviewSession schema."""

    meta: ResourceMeta[InterviewSessionId]
    workspace_id: WorkspaceId
    scenario_id: InterviewScenarioId
    resume_ref: ResourceRef | None
    job_target: JobTarget
    status: InterviewSessionStatus
    locale: str
    media: InterviewMediaPreferences
    recording: RecordingConsent
    started_at: datetime | None
    ended_at: datetime | None
    report_id: InterviewReportId | None

    def __post_init__(self) -> None:
        """@brief 穷尽校验 Session 状态关联字段 / Exhaustively validate Session state associations."""
        _require_opaque_id(self.meta.id, "session id")
        _require_opaque_id(self.workspace_id, "session workspace id")
        _require_opaque_id(self.scenario_id, "session scenario id")
        _require_locale(self.locale)
        for label, value in (("started_at", self.started_at), ("ended_at", self.ended_at)):
            if value is not None:
                _require_aware(value, f"session {label}")
                if value < self.meta.created_at or value > self.meta.updated_at:
                    raise InterviewDomainError(f"session {label} is outside its timeline")
        if self.status.is_terminal:
            if self.ended_at is None:
                raise InterviewDomainError("terminal session requires ended_at")
        elif self.ended_at is not None:
            raise InterviewDomainError("non-terminal session cannot have ended_at")
        if self.status is InterviewSessionStatus.COMPLETED and self.started_at is None:
            raise InterviewDomainError("completed session requires started_at")
        if self.started_at is not None and self.ended_at is not None and self.ended_at < self.started_at:
            raise InterviewDomainError("session ended_at cannot precede started_at")
        if self.report_id is not None:
            _require_opaque_id(self.report_id, "session report id")
            if self.status is not InterviewSessionStatus.COMPLETED:
                raise InterviewDomainError("only completed session may reference a report")


@dataclass(frozen=True, slots=True)
class InterviewSession:
    """@brief 含冻结策略与内部 end Job 绑定的 Session 聚合 / Session aggregate with frozen policy and internal end-Job binding."""

    view: InterviewSessionView
    spec: InterviewSessionSpec = field(repr=False)
    grant: InterviewExecutionGrant = field(repr=False)
    pending_end_job_id: JobId | None = field(default=None, repr=False)
    end_reason: EndInterviewReason | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """@brief 校验公开投影与冻结 spec 一致 / Validate public projection against the frozen spec."""
        if (
            self.view.scenario_id != self.spec.scenario_id
            or self.view.resume_ref != self.spec.resume_ref
            or self.view.job_target != self.spec.job_target
            or self.view.locale != self.spec.locale
            or self.view.media != self.spec.media
            or self.view.recording != self.spec.recording
        ):
            raise InterviewDomainError("session view does not match its frozen creation spec")
        if self.grant.scenario_ref.id != self.spec.scenario_id:
            raise InterviewDomainError("session grant does not match its Scenario")
        if self.view.status is InterviewSessionStatus.ENDING:
            if self.pending_end_job_id is None or self.end_reason is None:
                raise InterviewDomainError("ending session requires end Job and reason")
        elif self.pending_end_job_id is not None or self.end_reason is not None:
            raise InterviewDomainError("only ending session may retain end Job and reason")

    @property
    def meta(self) -> ResourceMeta[InterviewSessionId]:
        """@brief 返回强 revision 元数据 / Return strong-revision metadata."""
        return self.view.meta

    @property
    def workspace_id(self) -> WorkspaceId:
        """@brief 返回所属 Workspace / Return owning Workspace."""
        return self.view.workspace_id

    def mark_connecting(self, *, at: datetime) -> InterviewSession:
        """@brief created → connecting；active 可签发替换 connection 而不改状态 / Mark connecting; active sessions may replace a connection without state change."""
        if self.view.status is InterviewSessionStatus.ACTIVE:
            return self
        self._require_state({InterviewSessionStatus.CREATED}, "connect")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=InterviewSessionStatus.CONNECTING,
            ),
        )

    def activate(self, *, at: datetime) -> InterviewSession:
        """@brief connecting → active / Transition connecting to active."""
        self._require_state({InterviewSessionStatus.CONNECTING}, "activate")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=InterviewSessionStatus.ACTIVE,
                started_at=at,
            ),
        )

    def begin_end(self, job_id: JobId, reason: EndInterviewReason, *, at: datetime) -> InterviewSession:
        """@brief 非终态 → ending 并绑定统一 Job / Transition to ending and bind a unified Job."""
        self._require_state(
            {
                InterviewSessionStatus.CREATED,
                InterviewSessionStatus.CONNECTING,
                InterviewSessionStatus.ACTIVE,
            },
            "end",
        )
        _require_opaque_id(job_id, "session end job id")
        if reason is EndInterviewReason.COMPLETED and self.view.status is not InterviewSessionStatus.ACTIVE:
            raise InterviewTransitionError("only an active session can complete successfully")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=InterviewSessionStatus.ENDING,
            ),
            pending_end_job_id=job_id,
            end_reason=reason,
        )

    def finish_end(self, *, at: datetime) -> InterviewSession:
        """@brief ending → completed/failed/cancelled / Finish an ending Session in its reason-derived terminal state."""
        self._require_state({InterviewSessionStatus.ENDING}, "finish")
        if self.end_reason is None:
            raise InterviewDomainError("ending session lost its reason")
        terminal = {
            EndInterviewReason.COMPLETED: InterviewSessionStatus.COMPLETED,
            EndInterviewReason.USER_CANCELLED: InterviewSessionStatus.CANCELLED,
            EndInterviewReason.TECHNICAL_FAILURE: InterviewSessionStatus.FAILED,
        }[self.end_reason]
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=terminal,
                ended_at=at,
            ),
            pending_end_job_id=None,
            end_reason=None,
        )

    def fail_end(self, *, at: datetime) -> InterviewSession:
        """@brief 媒体 finalize 失败时 ending → failed / Transition ending to failed after media-finalization failure."""
        self._require_state({InterviewSessionStatus.ENDING}, "fail end")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=InterviewSessionStatus.FAILED,
                ended_at=at,
            ),
            pending_end_job_id=None,
            end_reason=None,
        )

    def attach_report(self, report_id: InterviewReportId, *, at: datetime) -> InterviewSession:
        """@brief completed Session 一次性关联 Report / Attach a Report once to a completed Session."""
        self._require_state({InterviewSessionStatus.COMPLETED}, "attach report")
        _require_opaque_id(report_id, "report id")
        if self.view.report_id is not None:
            raise InterviewTransitionError("session already has a report")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                report_id=report_id,
            ),
        )

    def _require_state(self, allowed: set[InterviewSessionStatus], action: str) -> None:
        """@brief 检查 Session 状态边 / Check a Session state edge."""
        if self.view.status not in allowed:
            raise InterviewTransitionError(f"session cannot {action} from {self.view.status.value}")


@dataclass(frozen=True, slots=True)
class EphemeralToken:
    """@brief 默认脱敏的短期 signaling token / Redacted-by-default short-lived signaling token."""

    _value: str = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 token 长度 / Validate token length."""
        if not 20 <= len(self._value) <= 8_192:
            raise InterviewDomainError("ephemeral token length is invalid")

    def reveal_to_transport(self) -> str:
        """@brief 仅向 HTTP transport adapter 暴露 / Reveal only to the HTTP transport adapter."""
        return self._value

    def __str__(self) -> str:
        """@brief 防止隐式日志泄漏 / Prevent implicit log leakage."""
        return "<redacted>"


@dataclass(frozen=True, slots=True)
class IceServer:
    """@brief ICE server 短期配置 / Short-lived ICE-server configuration."""

    urls: tuple[str, ...]
    username: str | None
    credential: str | None = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 URI 与凭据边界 / Validate URI and credential bounds."""
        if not self.urls or any(not url or len(url) > 2_048 for url in self.urls):
            raise InterviewDomainError("ICE server URLs are invalid")
        if self.username is not None and len(self.username) > 512:
            raise InterviewDomainError("ICE username is too long")
        if self.credential is not None and len(self.credential) > 2_048:
            raise InterviewDomainError("ICE credential is too long")


@dataclass(frozen=True, slots=True)
class CreateRealtimeConnectionSpec:
    """@brief CreateRealtimeConnectionRequest 的类型化形式 / Typed CreateRealtimeConnectionRequest."""

    supported_transports: tuple[RealtimeTransport, ...]
    audio_codecs: tuple[str, ...]
    video_codecs: tuple[str, ...]

    def __post_init__(self) -> None:
        """@brief 校验 transports 与 codecs 唯一 / Validate unique transports and codecs."""
        if not self.supported_transports or len(set(self.supported_transports)) != len(
            self.supported_transports
        ):
            raise InterviewDomainError("supported transports must be non-empty and unique")
        _require_unique_texts(self.audio_codecs, "connection audio codecs", 20, 80)
        _require_unique_texts(self.video_codecs, "connection video codecs", 20, 80)


@dataclass(frozen=True, slots=True)
class RealtimeConnection:
    """@brief 单 Session、单 audience 的短期实时连接 / Short-lived realtime connection for one Session and audience."""

    id: RealtimeConnectionId
    workspace_id: WorkspaceId = field(repr=False)
    session_id: InterviewSessionId
    audience: ResourceRef = field(repr=False)
    transport: RealtimeTransport
    signaling_url: str
    ephemeral_token: EphemeralToken = field(repr=False)
    ice_servers: tuple[IceServer, ...]
    issued_at: datetime
    expires_at: datetime
    heartbeat_interval_ms: int

    def __post_init__(self) -> None:
        """@brief 校验 connection binding、URL 与短期寿命 / Validate binding, URL, and short lifetime."""
        _require_opaque_id(self.id, "realtime connection id")
        _require_opaque_id(self.workspace_id, "realtime connection workspace id")
        _require_opaque_id(self.session_id, "realtime connection session id")
        _require_realtime_url(self.signaling_url)
        _require_aware(self.issued_at, "connection issued_at")
        _require_aware(self.expires_at, "connection expires_at")
        lifetime = self.expires_at - self.issued_at
        if lifetime <= timedelta(0) or lifetime > _MAX_REALTIME_LIFETIME:
            raise InterviewDomainError("realtime connection must be short-lived")
        if not 1_000 <= self.heartbeat_interval_ms <= 120_000:
            raise InterviewDomainError("heartbeat interval is invalid")
        if len(self.ice_servers) > 20:
            raise InterviewDomainError("ICE servers cannot exceed 20")


@dataclass(frozen=True, slots=True)
class RealtimeConnectionLease:
    """@brief 可持久化但不含 token/ICE secret 的连接绑定 / Persistable connection binding without token or ICE secrets."""

    id: RealtimeConnectionId
    workspace_id: WorkspaceId
    session_id: InterviewSessionId
    audience: ResourceRef
    transport: RealtimeTransport
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验 lease identity 与过期时间 / Validate lease identity and expiry."""
        _require_opaque_id(self.id, "realtime lease id")
        _require_opaque_id(self.workspace_id, "realtime lease workspace id")
        _require_opaque_id(self.session_id, "realtime lease session id")
        _require_aware(self.issued_at, "realtime lease issued_at")
        _require_aware(self.expires_at, "realtime lease expires_at")
        lifetime = self.expires_at - self.issued_at
        if lifetime <= timedelta(0) or lifetime > _MAX_REALTIME_LIFETIME:
            raise InterviewDomainError("realtime lease must be short-lived")

    @classmethod
    def from_connection(cls, connection: RealtimeConnection) -> RealtimeConnectionLease:
        """@brief 从公开 grant 派生无 secret lease / Derive a secret-free lease from a public grant."""
        return cls(
            connection.id,
            connection.workspace_id,
            connection.session_id,
            connection.audience,
            connection.transport,
            connection.issued_at,
            connection.expires_at,
        )


@dataclass(frozen=True, slots=True)
class CandidateUtteranceInput:
    """@brief 候选人可见文本实时输入 / Candidate user-visible text realtime input."""

    text: str
    start_ms: int
    end_ms: int
    type: Literal["candidate_utterance"] = "candidate_utterance"

    def __post_init__(self) -> None:
        """@brief 校验文本和时间范围 / Validate text and time range."""
        _require_text(self.text, "candidate utterance", 1, 20_000)
        _require_time_range(self.start_ms, self.end_ms, "candidate utterance")


@dataclass(frozen=True, slots=True)
class RealtimeControlInput:
    """@brief 实时生命周期控制输入 / Realtime lifecycle-control input."""

    control: RealtimeControl
    type: Literal["control"] = "control"


type RealtimeInputPayload = CandidateUtteranceInput | RealtimeControlInput
"""@brief 实时输入封闭判别联合 / Closed discriminated union for realtime input."""


@dataclass(frozen=True, slots=True)
class RealtimeInputEnvelope:
    """@brief 绑定 Session/Connection 的幂等实时输入 / Idempotent realtime input bound to a Session and Connection."""

    input_id: RealtimeInputId
    workspace_id: WorkspaceId
    session_id: InterviewSessionId
    connection_id: RealtimeConnectionId
    occurred_at: datetime
    payload: RealtimeInputPayload
    fingerprint_sha256: str

    def __post_init__(self) -> None:
        """@brief 校验身份、时间和规范指纹 / Validate identity, time, and canonical fingerprint."""
        _require_opaque_id(self.input_id, "realtime input id")
        _require_opaque_id(self.workspace_id, "realtime input workspace id")
        _require_opaque_id(self.session_id, "realtime input session id")
        _require_opaque_id(self.connection_id, "realtime input connection id")
        _require_aware(self.occurred_at, "realtime input occurred_at")
        if _SHA256.fullmatch(self.fingerprint_sha256) is None:
            raise InterviewDomainError("realtime input fingerprint is invalid")
        if self.fingerprint_sha256 != realtime_input_fingerprint(self.payload):
            raise InterviewDomainError("realtime input fingerprint does not match payload")

    def ledger_record(self) -> RealtimeInputLedgerRecord:
        """@brief 丢弃正文后生成幂等账本记录 / Build an idempotency-ledger record after discarding plaintext."""
        return RealtimeInputLedgerRecord(
            input_id=self.input_id,
            workspace_id=self.workspace_id,
            session_id=self.session_id,
            connection_id=self.connection_id,
            occurred_at=self.occurred_at,
            fingerprint_sha256=self.fingerprint_sha256,
        )


@dataclass(frozen=True, slots=True)
class RealtimeInputLedgerRecord:
    """@brief 不含候选人正文的幂等账本记录 / Idempotency-ledger record containing no candidate plaintext."""

    input_id: RealtimeInputId
    workspace_id: WorkspaceId
    session_id: InterviewSessionId
    connection_id: RealtimeConnectionId
    occurred_at: datetime
    fingerprint_sha256: str

    def __post_init__(self) -> None:
        """@brief 校验账本 identity、时间与指纹 / Validate ledger identity, time, and fingerprint."""
        _require_opaque_id(self.input_id, "realtime ledger input id")
        _require_opaque_id(self.workspace_id, "realtime ledger workspace id")
        _require_opaque_id(self.session_id, "realtime ledger session id")
        _require_opaque_id(self.connection_id, "realtime ledger connection id")
        _require_aware(self.occurred_at, "realtime ledger occurred_at")
        if _SHA256.fullmatch(self.fingerprint_sha256) is None:
            raise InterviewDomainError("realtime ledger fingerprint is invalid")


@dataclass(frozen=True, slots=True)
class RealtimeInputReceipt:
    """@brief 原子输入 ledger 返回的 sequence 与重放标记 / Sequence and replay marker returned by the atomic input ledger."""

    sequence: int
    replayed: bool

    def __post_init__(self) -> None:
        """@brief 校验 sequence / Validate sequence."""
        if self.sequence < 1:
            raise InterviewDomainError("realtime input sequence must be positive")


@dataclass(frozen=True, slots=True)
class TranscriptSegment:
    """@brief append-only Transcript segment / Append-only Transcript segment."""

    id: TranscriptSegmentId
    workspace_id: WorkspaceId = field(repr=False)
    session_id: InterviewSessionId = field(repr=False)
    sequence: int = field(repr=False)
    source_ref: ResourceRef = field(repr=False)
    speaker: TranscriptSpeaker = TranscriptSpeaker.SYSTEM
    start_ms: int = 0
    end_ms: int = 0
    text: str = ""

    def __post_init__(self) -> None:
        """@brief 校验 segment identity、顺序与时间 / Validate segment identity, sequence, and time."""
        _require_opaque_id(self.id, "transcript segment id")
        _require_opaque_id(self.workspace_id, "transcript workspace id")
        _require_opaque_id(self.session_id, "transcript session id")
        if self.sequence < 1:
            raise InterviewDomainError("transcript sequence must be positive")
        if self.source_ref.resource_type not in {"realtime_input", "artifact"}:
            raise InterviewDomainError(
                "transcript provenance must reference a realtime input or Artifact"
            )
        if (
            self.source_ref.resource_type == "realtime_input"
            and self.source_ref.revision is not None
        ):
            raise InterviewDomainError("realtime-input provenance cannot carry a revision")
        if self.source_ref.resource_type == "artifact" and self.source_ref.revision is None:
            raise InterviewDomainError("Artifact provenance requires an exact revision")
        _require_time_range(self.start_ms, self.end_ms, "transcript segment")
        _require_text(self.text, "transcript text", 0, 20_000)


@dataclass(frozen=True, slots=True)
class InterviewRichText:
    """@brief Report 中的简化 RichText / Simplified RichText in an Interview Report."""

    plain_text: str

    def __post_init__(self) -> None:
        """@brief 校验文本长度 / Validate text length."""
        _require_text(self.plain_text, "interview rich text", 0, 10_000)


@dataclass(frozen=True, slots=True)
class InterviewEvidence:
    """@brief Report score 的 Transcript evidence / Transcript evidence for a Report score."""

    segment_id: TranscriptSegmentId
    start_ms: int
    end_ms: int
    quote: str | None

    def __post_init__(self) -> None:
        """@brief 校验证据 identity 与时间 / Validate evidence identity and time."""
        _require_opaque_id(self.segment_id, "evidence segment id")
        _require_time_range(self.start_ms, self.end_ms, "interview evidence")
        if self.quote is not None:
            _require_text(self.quote, "evidence quote", 0, 4_000)


@dataclass(frozen=True, slots=True)
class RubricScore:
    """@brief 单 Rubric dimension 的评分与证据 / Score and evidence for one rubric dimension."""

    dimension_id: str
    score: float
    confidence: float
    summary: InterviewRichText
    evidence: tuple[InterviewEvidence, ...]
    improvement_actions: tuple[str, ...]

    def __post_init__(self) -> None:
        """@brief 校验通用 Schema 范围 / Validate general schema bounds."""
        _require_opaque_id(self.dimension_id, "rubric score dimension id")
        if not math.isfinite(self.score) or not 0 <= self.score <= 100:
            raise InterviewDomainError("rubric score must be in [0,100]")
        if not math.isfinite(self.confidence) or not 0 <= self.confidence <= 1:
            raise InterviewDomainError("rubric confidence must be in [0,1]")
        if len(self.evidence) > 50:
            raise InterviewDomainError("rubric evidence cannot exceed 50")
        _require_text_list(self.improvement_actions, "improvement actions", 50, 1_000)


@dataclass(frozen=True, slots=True)
class InterviewCommunicationMetrics:
    """@brief 公开 Communication metrics / Public communication metrics."""

    speaking_time_ms: int | None
    average_answer_length_ms: int | None
    words_per_minute: float | None
    filler_word_count: int | None
    long_pause_count: int | None
    interruption_count: int | None
    notes: tuple[str, ...]

    def __post_init__(self) -> None:
        """@brief 校验非负 metrics / Validate non-negative metrics."""
        values = (
            self.speaking_time_ms,
            self.average_answer_length_ms,
            self.filler_word_count,
            self.long_pause_count,
            self.interruption_count,
        )
        if any(value is not None and value < 0 for value in values):
            raise InterviewDomainError("communication metrics cannot be negative")
        if self.words_per_minute is not None and (
            not math.isfinite(self.words_per_minute) or self.words_per_minute < 0
        ):
            raise InterviewDomainError("words per minute is invalid")
        _require_text_list(self.notes, "communication notes", 50, 1_000)


@dataclass(frozen=True, slots=True)
class InterviewActionPlanItem:
    """@brief Interview report 行动项 / Interview-report action item."""

    priority: ActionPriority
    title: str
    why: str
    practice: str
    success_criterion: str

    def __post_init__(self) -> None:
        """@brief 校验行动项文本 / Validate action-item text."""
        _require_text(self.title, "action title", 1, 300)
        _require_text(self.why, "action why", 0, 2_000)
        _require_text(self.practice, "action practice", 0, 4_000)
        _require_text(self.success_criterion, "action success criterion", 0, 2_000)


@dataclass(frozen=True, slots=True)
class InterviewReportDraft:
    """@brief Report provider 允许返回的公开安全草稿 / Public-safe draft a Report provider may return.

    @note 不含私有 reasoning、原始 provider response、embedding 或 prompt。
        / Contains no private reasoning, raw provider response, embedding, or prompt.
    """

    report_version: str
    rubric_id: str
    rubric_version: str
    engine_version: str
    overall_score: float | None
    overall_confidence: float
    executive_summary: InterviewRichText
    rubric_scores: tuple[RubricScore, ...]
    strengths: tuple[InterviewRichText, ...]
    improvements: tuple[InterviewRichText, ...]
    communication_metrics: InterviewCommunicationMetrics
    action_plan: tuple[InterviewActionPlanItem, ...]
    limitations: tuple[str, ...]

    def __post_init__(self) -> None:
        """@brief 校验 Report 草稿通用边界 / Validate general Report-draft bounds."""
        _require_text(self.report_version, "report version", 1, 80)
        _require_opaque_id(self.rubric_id, "report rubric id")
        _require_text(self.rubric_version, "report rubric version", 1, 80)
        _require_text(self.engine_version, "report engine version", 1, 120)
        if self.overall_score is not None and (
            not math.isfinite(self.overall_score) or not 0 <= self.overall_score <= 100
        ):
            raise InterviewDomainError("overall score must be null or in [0,100]")
        if not math.isfinite(self.overall_confidence) or not 0 <= self.overall_confidence <= 1:
            raise InterviewDomainError("overall confidence must be in [0,1]")
        for label, values in (
            ("rubric scores", self.rubric_scores),
            ("strengths", self.strengths),
            ("improvements", self.improvements),
            ("action plan", self.action_plan),
        ):
            if len(values) > 50:
                raise InterviewDomainError(f"report {label} cannot exceed 50")
        _require_text_list(self.limitations, "report limitations", 50, 1_000)

    def validate_against(
        self,
        rubric: InterviewRubric,
        segments: Sequence[TranscriptSegment],
        session_id: InterviewSessionId,
    ) -> None:
        """@brief 校验 rubric snapshot、score scale 与真实 Transcript evidence / Validate rubric snapshot, score scales, and real Transcript evidence."""
        if self.rubric_id != rubric.rubric_id or self.rubric_version != rubric.rubric_version:
            raise InterviewDomainError("report rubric does not match the frozen Session rubric")
        dimensions = {item.dimension_id: item for item in rubric.dimensions}
        score_ids = tuple(item.dimension_id for item in self.rubric_scores)
        if len(set(score_ids)) != len(score_ids) or set(score_ids) != set(dimensions):
            raise InterviewDomainError("report must score every frozen rubric dimension exactly once")
        if self.overall_score is not None and not rubric.overall_scale.contains(self.overall_score):
            raise InterviewDomainError("overall score is outside the frozen rubric scale")
        segment_map = {item.id: item for item in segments}
        if len(segment_map) != len(segments) or any(
            item.session_id != session_id for item in segments
        ):
            raise InterviewDomainError("report Transcript evidence crosses Session boundaries")
        for score in self.rubric_scores:
            if not dimensions[score.dimension_id].scoring_scale.contains(score.score):
                raise InterviewDomainError("dimension score is outside its frozen scale")
            for evidence in score.evidence:
                segment = segment_map.get(evidence.segment_id)
                if segment is None:
                    raise InterviewDomainError("report evidence references an unknown segment")
                if evidence.start_ms < segment.start_ms or evidence.end_ms > segment.end_ms:
                    raise InterviewDomainError("report evidence exceeds its Transcript segment")
                if evidence.quote is not None and evidence.quote not in segment.text:
                    raise InterviewDomainError("report evidence quote is not present in its segment")


@dataclass(frozen=True, slots=True)
class InterviewReport:
    """@brief 创建后不可变的 InterviewReport / Immutable InterviewReport after creation."""

    meta: ResourceMeta[InterviewReportId]
    workspace_id: WorkspaceId
    session_id: InterviewSessionId
    draft: InterviewReportDraft
    generated_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验 Report identity 与不可变 revision / Validate Report identity and immutable revision."""
        _require_opaque_id(self.meta.id, "report id")
        _require_opaque_id(self.workspace_id, "report workspace id")
        _require_opaque_id(self.session_id, "report session id")
        _require_aware(self.generated_at, "report generated_at")
        if self.meta.revision != 1 or self.meta.created_at != self.meta.updated_at:
            raise InterviewDomainError("InterviewReport must remain immutable at revision one")
        if self.generated_at != self.meta.created_at:
            raise InterviewDomainError("report generated_at must equal creation time")


@dataclass(frozen=True, slots=True)
class EndSessionJobSpec:
    """@brief end worker 的内部 typed spec / Internal typed spec for the end worker."""

    session_id: InterviewSessionId
    reason: EndInterviewReason
    recording: RecordingConsent


@dataclass(frozen=True, slots=True)
class ReportJobSpec:
    """@brief report worker 的内部 typed spec / Internal typed spec for the report worker."""

    session_id: InterviewSessionId
    rubric_id: str
    rubric_version: str


type InterviewJobSpec = EndSessionJobSpec | ReportJobSpec
"""@brief Interview worker spec 判别联合 / Discriminated union of Interview worker specs."""


@dataclass(frozen=True, slots=True)
class InterviewJobQueuedRecord:
    """@brief 写入统一 outbox 的 Job 排队信号 / Job-queued signal written to the unified outbox."""

    id: InterviewOutboxId
    workspace_id: WorkspaceId
    actor_id: UserId
    session_ref: ResourceRef
    job_ref: ResourceRef
    occurred_at: datetime
    kind: Literal["interview.job.queued"] = "interview.job.queued"

    def __post_init__(self) -> None:
        """@brief 校验 outbox 引用 / Validate outbox references."""
        _require_opaque_id(self.id, "interview outbox id")
        _require_opaque_id(self.workspace_id, "interview outbox workspace id")
        _require_opaque_id(self.actor_id, "interview outbox actor id")
        _require_aware(self.occurred_at, "interview outbox occurred_at")
        if self.session_ref.resource_type != "interview_session" or self.job_ref.resource_type != "job":
            raise InterviewDomainError("Interview outbox requires Session and Job refs")

    def as_payload(self) -> Mapping[str, JsonValue]:
        """@brief 生成固定白名单 payload / Build a fixed-allowlist payload."""
        return MappingProxyType(
            {
                "actor_id": self.actor_id,
                "session_id": self.session_ref.id,
                "job_id": self.job_ref.id,
            }
        )


def realtime_input_fingerprint(payload: RealtimeInputPayload) -> str:
    """@brief 生成实时输入的规范 SHA-256 指纹 / Generate the canonical SHA-256 fingerprint of realtime input."""
    encoded = json.dumps(
        asdict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def validate_interview_job_alignment(session: InterviewSession, job: Job) -> None:
    """@brief 验证 Session 与统一 Job identity/state / Validate Session identity/state against a unified Job."""
    if (
        job.workspace_id != session.workspace_id
        or job.subject.resource_type != "interview_session"
        or job.subject.id != session.meta.id
    ):
        raise InterviewDomainError("Interview Job does not belong to the Session")
    if job.kind == INTERVIEW_END_JOB_KIND:
        if session.view.status is InterviewSessionStatus.ENDING and job.status not in {
            JobStatus.QUEUED,
            JobStatus.RUNNING,
        }:
            raise InterviewDomainError("ending Session requires a live end Job")
        if session.view.status.is_terminal and job.status not in {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        }:
            raise InterviewDomainError("terminal Session requires a terminal end Job")
    elif job.kind != INTERVIEW_REPORT_JOB_KIND:
        raise InterviewDomainError("Interview Job kind is invalid")


def validate_artifacts_for_session(
    artifacts: Sequence[Artifact],
    session: InterviewSession,
) -> None:
    """@brief 校验统一 Artifact 均属于 Session 且遵守 consent / Validate unified Artifacts against Session ownership and consent."""
    for artifact in artifacts:
        if (
            artifact.workspace_id != session.workspace_id
            or artifact.subject.resource_type != "interview_session"
            or artifact.subject.id != session.meta.id
        ):
            raise InterviewDomainError("recording Artifact crosses Session boundaries")
        if artifact.kind.value == "interview_audio" and not session.spec.recording.record_audio:
            raise InterviewDomainError("audio Artifact lacks recording consent")
        if artifact.kind.value == "interview_video" and not session.spec.recording.record_video:
            raise InterviewDomainError("video Artifact lacks recording consent")
        if artifact.kind.value == "interview_transcript" and not session.spec.recording.store_transcript:
            raise InterviewDomainError("Transcript Artifact lacks storage consent")
        retention = session.spec.recording.retention_until
        if retention is not None and artifact.expires_at != retention:
            raise InterviewDomainError("recording Artifact retention does not match consent")


def _validate_scenario_transition(
    current: InterviewScenarioStatus,
    requested: InterviewScenarioStatus,
) -> None:
    """@brief 校验 Scenario 单向状态机 / Validate the one-way Scenario state machine."""
    allowed = {
        InterviewScenarioStatus.DRAFT: {
            InterviewScenarioStatus.DRAFT,
            InterviewScenarioStatus.ACTIVE,
            InterviewScenarioStatus.ARCHIVED,
        },
        InterviewScenarioStatus.ACTIVE: {
            InterviewScenarioStatus.ACTIVE,
            InterviewScenarioStatus.ARCHIVED,
        },
        InterviewScenarioStatus.ARCHIVED: {InterviewScenarioStatus.ARCHIVED},
    }[current]
    if requested not in allowed:
        raise InterviewTransitionError(
            f"scenario cannot transition from {current.value} to {requested.value}"
        )


def _require_time_range(start_ms: int, end_ms: int, label: str) -> None:
    """@brief 校验非负闭区间 / Validate a non-negative closed time range."""
    if start_ms < 0 or end_ms < start_ms:
        raise InterviewDomainError(f"{label} requires 0 <= start_ms <= end_ms")


def _require_unique_texts(values: Sequence[str], label: str, maximum_items: int, maximum: int) -> None:
    """@brief 校验唯一、非空文本集合 / Validate a unique non-empty text collection."""
    if len(values) > maximum_items or len(set(values)) != len(values):
        raise InterviewDomainError(f"{label} must be unique and bounded")
    _require_text_list(values, label, maximum_items, maximum, minimum=1)


def _require_text_list(
    values: Sequence[str],
    label: str,
    maximum_items: int,
    maximum: int,
    *,
    minimum: int = 0,
) -> None:
    """@brief 校验文本列表 / Validate a text list."""
    if len(values) > maximum_items:
        raise InterviewDomainError(f"{label} exceeds item limit")
    for value in values:
        _require_text(value, label, minimum, maximum)


def _require_text(value: str, label: str, minimum: int, maximum: int) -> None:
    """@brief 校验公开安全文本 / Validate public-safe text."""
    if not minimum <= len(value) <= maximum or any(
        ord(char) < 32 and char not in "\n\r\t" for char in value
    ):
        raise InterviewDomainError(f"{label} violates contract bounds")


def _require_opaque_id(value: str, label: str) -> None:
    """@brief 校验 opaque ID / Validate an opaque ID."""
    if _OPAQUE_ID.fullmatch(value) is None:
        raise InterviewDomainError(f"{label} is invalid")


def _require_stable_name(value: str, label: str) -> None:
    """@brief 校验稳定名称 / Validate a stable name."""
    if _STABLE_NAME.fullmatch(value) is None:
        raise InterviewDomainError(f"{label} is invalid")


def _require_locale(value: str) -> None:
    """@brief 校验契约 Locale / Validate a contract Locale."""
    if not 2 <= len(value) <= 35 or _LOCALE.fullmatch(value) is None:
        raise InterviewDomainError("Interview locale is invalid")


def _require_http_url(value: str, label: str) -> None:
    """@brief 校验无 userinfo HTTP(S) URL / Validate an HTTP(S) URL without userinfo."""
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username:
        raise InterviewDomainError(f"{label} is invalid")


def _require_realtime_url(value: str) -> None:
    """@brief 校验生产 HTTPS/WSS 或固定 dev Origin / Validate production HTTPS/WSS or the fixed dev origin."""
    parsed = urlsplit(value)
    production = parsed.scheme in {"https", "wss"} and parsed.hostname is not None
    development = (
        parsed.scheme in {"http", "ws"}
        and parsed.hostname == "dev.hmalliances.org"
        and parsed.port == 9000
    )
    if parsed.username or not (production or development):
        raise InterviewDomainError("realtime signaling URL is invalid")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 校验 timezone-aware datetime / Validate a timezone-aware datetime."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise InterviewDomainError(f"{label} must be timezone-aware")


__all__ = [
    "INTERVIEW_END_JOB_KIND",
    "INTERVIEW_REPORT_JOB_KIND",
    "ActionPriority",
    "AvatarOutputMode",
    "CandidateUtteranceInput",
    "CreateRealtimeConnectionSpec",
    "EndInterviewReason",
    "EndSessionJobSpec",
    "EphemeralToken",
    "FallbackTransport",
    "IceServer",
    "InterviewActionPlanItem",
    "InterviewAvatarPreferences",
    "InterviewCommunicationMetrics",
    "InterviewDifficulty",
    "InterviewDomainError",
    "InterviewEvidence",
    "InterviewExecutionGrant",
    "InterviewJobQueuedRecord",
    "InterviewJobSpec",
    "InterviewKnowledgeContext",
    "InterviewMediaPreferences",
    "InterviewOutboxId",
    "InterviewReport",
    "InterviewReportDraft",
    "InterviewReportId",
    "InterviewRichText",
    "InterviewRubric",
    "InterviewScenario",
    "InterviewScenarioId",
    "InterviewScenarioPatch",
    "InterviewScenarioSpec",
    "InterviewScenarioStatus",
    "InterviewSession",
    "InterviewSessionId",
    "InterviewSessionSpec",
    "InterviewSessionStatus",
    "InterviewSessionView",
    "InterviewTransitionError",
    "JobTarget",
    "RealtimeConnection",
    "RealtimeConnectionId",
    "RealtimeConnectionLease",
    "RealtimeControl",
    "RealtimeControlInput",
    "RealtimeInputEnvelope",
    "RealtimeInputId",
    "RealtimeInputLedgerRecord",
    "RealtimeInputPayload",
    "RealtimeInputReceipt",
    "RealtimeTransport",
    "RecordingConsent",
    "ReportJobSpec",
    "RubricDimension",
    "RubricScore",
    "ScoreScale",
    "TranscriptSegment",
    "TranscriptSegmentId",
    "TranscriptSpeaker",
    "realtime_input_fingerprint",
    "validate_artifacts_for_session",
    "validate_interview_job_alignment",
]
