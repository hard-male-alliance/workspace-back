"""@brief API v2 Interview 应用 Ports / API v2 Interview application ports.

所有 repository 查询以 ``workspace_id`` 为首键；Scenario/Session 使用 revision CAS，实时输入
用 ``(workspace, session, input_id, fingerprint)`` 原子去重并分配 sequence，Transcript 由
持久化端口原子分配顺序。外部 realtime/media/report providers 不属于 UoW，禁止在数据库
锁内执行网络 I/O。Job、Artifact、outbox 与 audit 均复用平台统一存储。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import TracebackType
from typing import NewType, Protocol, Self

from backend.domain.interview_v2 import (
    CreateRealtimeConnectionSpec,
    InterviewExecutionGrant,
    InterviewJobQueuedRecord,
    InterviewJobSpec,
    InterviewReport,
    InterviewReportDraft,
    InterviewReportId,
    InterviewRubric,
    InterviewScenario,
    InterviewScenarioId,
    InterviewSession,
    InterviewSessionId,
    InterviewSessionSpec,
    JobTarget,
    RealtimeConnection,
    RealtimeConnectionId,
    RealtimeConnectionLease,
    RealtimeInputLedgerRecord,
    RealtimeInputReceipt,
    TranscriptSegment,
)
from backend.domain.platform import Artifact, AuditEvent, Job, JobId
from backend.domain.principals import TokenPrincipal, UserId, WorkspaceId
from backend.domain.resources import ResourceRef

_WORKER_FAILURE_CODE = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief 可持久化 worker Port 错误码语法 / Persistable worker-port error-code grammar."""

InterviewWorkerOperationId = NewType("InterviewWorkerOperationId", str)
"""@brief 外部副作用跨 outbox 重放稳定的幂等键 / Idempotency key stable across outbox replays for external side effects."""


class InterviewCasMismatch(RuntimeError):
    """@brief revision 条件写没有精确影响一行 / Revision CAS did not affect exactly one row."""


class RealtimeInputKeyReused(RuntimeError):
    """@brief 相同实时 input_id 被不同 fingerprint 重用 / Realtime input ID reused with a different fingerprint."""


class InterviewPolicyDenied(RuntimeError):
    """@brief Scenario/Resume/Knowledge/model policy 交集拒绝 Session / Policy intersection denied a Session."""


class InterviewWorkerPortFailure(RuntimeError):
    """@brief 外部 worker Port 的已分类公开安全失败 / Classified public-safe external worker-port failure.

    @note ``retryable=True`` 只用于调用方以稳定 operation identity 可安全重试的瞬态错误；
        配置缺失、能力未实现和 provider 返回非法结果必须终态失败。
        / ``retryable=True`` is only for transient failures that callers can safely retry with a
        stable operation identity. Missing configuration, unavailable capabilities, and invalid
        provider results are terminal.
    """

    code: str
    """@brief 可写入 outbox/Job 的脱敏稳定码 / Redacted stable code safe for outbox and Job state."""

    retryable: bool
    """@brief 是否允许至少一次重放 / Whether at-least-once replay is safe."""

    def __init__(self, code: str, *, retryable: bool) -> None:
        """@brief 初始化分类失败 / Initialize a classified failure.

        @param code 不含 provider 正文的稳定错误码 / Stable code without provider details.
        @param retryable 外部操作能否以同一 operation identity 重试 / Whether the external
            operation can be retried under the same operation identity.
        """
        if _WORKER_FAILURE_CODE.fullmatch(code) is None:
            raise ValueError("Interview worker port failure code is invalid")
        if not isinstance(retryable, bool):
            raise TypeError("Interview worker port retryable flag must be boolean")
        super().__init__(code)
        self.code = code
        self.retryable = retryable


class InterviewPermission(StrEnum):
    """@brief 5.5 十二个 endpoint 的独立精确权限 / Independent exact permissions for the twelve section-5.5 endpoints."""

    LIST_SCENARIOS = "interview_scenario.list"
    CREATE_SCENARIO = "interview_scenario.create"
    READ_SCENARIO = "interview_scenario.read"
    UPDATE_SCENARIO = "interview_scenario.update"
    LIST_SESSIONS = "interview_session.list"
    CREATE_SESSION = "interview_session.create"
    READ_SESSION = "interview_session.read"
    CREATE_CONNECTION = "interview_session.connection.create"
    END_SESSION = "interview_session.end"
    READ_TRANSCRIPT = "interview_session.transcript.read"
    CREATE_REPORT_JOB = "interview_session.report.create"
    READ_REPORT = "interview_report.read"


@dataclass(frozen=True, slots=True)
class InterviewPermissionRequest:
    """@brief 单 endpoint 精确权限请求 / Exact permission request for one endpoint."""

    workspace_id: WorkspaceId
    permission: InterviewPermission
    target: ResourceRef

    def __post_init__(self) -> None:
        """@brief 校验 permission-target 关联 / Validate permission-target association."""
        expected = {
            InterviewPermission.LIST_SCENARIOS: "workspace",
            InterviewPermission.CREATE_SCENARIO: "workspace",
            InterviewPermission.READ_SCENARIO: "interview_scenario",
            InterviewPermission.UPDATE_SCENARIO: "interview_scenario",
            InterviewPermission.LIST_SESSIONS: "workspace",
            InterviewPermission.CREATE_SESSION: "interview_scenario",
            InterviewPermission.READ_SESSION: "interview_session",
            InterviewPermission.CREATE_CONNECTION: "interview_session",
            InterviewPermission.END_SESSION: "interview_session",
            InterviewPermission.READ_TRANSCRIPT: "interview_session",
            InterviewPermission.CREATE_REPORT_JOB: "interview_session",
            InterviewPermission.READ_REPORT: "interview_report",
        }[self.permission]
        if self.target.resource_type != expected:
            raise ValueError("Interview permission target type is invalid")
        if expected == "workspace" and self.target.id != self.workspace_id:
            raise ValueError("Interview workspace target must equal the path Workspace")


@dataclass(frozen=True, slots=True)
class InterviewPermissionGrant:
    """@brief 集中 authorizer 返回的精确 grant / Exact grant returned by the central authorizer."""

    actor_id: UserId
    request: InterviewPermissionRequest


@dataclass(frozen=True, slots=True)
class InterviewPageRequest:
    """@brief 绑定 principal/Workspace/filter 后的内部 keyset / Internal keyset after principal/Workspace/filter binding."""

    limit: int = 50
    after: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验分页边界 / Validate pagination bounds."""
        if isinstance(self.limit, bool) or not 1 <= self.limit <= 200:
            raise ValueError("Interview page limit must be 1 to 200")
        if self.after is not None and (not self.after or len(self.after) > 2_048):
            raise ValueError("Interview keyset position is invalid")


@dataclass(frozen=True, slots=True)
class InterviewPage[ItemT]:
    """@brief 稳定 keyset 页面 / Stable keyset page."""

    items: tuple[ItemT, ...]
    next_position: str | None


@dataclass(frozen=True, slots=True)
class TranscriptSequenceReservation:
    """@brief persistence 原子保留的 Transcript sequence / Transcript sequence atomically reserved by persistence."""

    sequence: int

    def __post_init__(self) -> None:
        """@brief 校验正 sequence / Validate a positive sequence."""
        if self.sequence < 1:
            raise ValueError("Transcript sequence must be positive")


@dataclass(frozen=True, slots=True)
class InterviewSessionPolicyRequest:
    """@brief Session 创建的完整本地策略请求 / Complete local-policy request for Session creation."""

    actor_id: UserId
    workspace_id: WorkspaceId
    scenario: InterviewScenario
    spec: InterviewSessionSpec


@dataclass(frozen=True, slots=True)
class EndSessionOutput:
    """@brief 媒体 finalizer 返回的统一 Artifact 集合 / Unified Artifacts returned by the media finalizer."""

    artifacts: tuple[Artifact, ...]

    def __post_init__(self) -> None:
        """@brief 校验 Artifact 数量 / Validate Artifact count."""
        if len(self.artifacts) > 20:
            raise ValueError("Interview end output cannot exceed 20 Artifacts")


@dataclass(frozen=True, slots=True)
class ReportGenerationRequest:
    """@brief Report provider 的公开安全输入 / Public-safe input for a Report provider.

    @note 不包含原始音视频、secret、prompt 或私有推理。
        / Contains no raw media, secrets, prompt, or private reasoning.
    """

    session_id: InterviewSessionId
    locale: str
    job_target: JobTarget
    rubric: InterviewRubric
    transcript: tuple[TranscriptSegment, ...]


class InterviewPermissionAuthorizer(Protocol):
    """@brief 独立精确 Interview 权限 Port / Independent exact Interview-permission port."""

    async def authorize(
        self,
        principal: TokenPrincipal,
        request: InterviewPermissionRequest,
    ) -> InterviewPermissionGrant:
        """@brief 授权单个 endpoint action / Authorize one endpoint action.

        @note 一个集中 adapter 必须穷尽映射到既有 AccessAuthorizer；业务服务不得临时借用
            WorkspaceAction.READ/UPDATE。/ One central adapter exhaustively maps into the existing
            AccessAuthorizer; business services do not borrow coarse Workspace actions.
        """


class InterviewSessionPolicy(Protocol):
    """@brief Resume/Knowledge/agent-scope/model-region 本地策略 Port / Local Resume/Knowledge/agent-scope/model-region policy port."""

    async def authorize_session(
        self,
        request: InterviewSessionPolicyRequest,
    ) -> InterviewExecutionGrant:
        """@brief 解析精确资源 revision 与策略交集 / Resolve exact revisions and the policy intersection.

        @note 只执行本地 DB/policy 工作，不调用模型 provider。
            / Performs local DB/policy work only and never calls a model provider.
        """


class InterviewRepository(Protocol):
    """@brief Workspace-first Interview repository / Workspace-first Interview repository."""

    async def list_scenarios(
        self,
        workspace_id: WorkspaceId,
        page: InterviewPageRequest,
    ) -> InterviewPage[InterviewScenario]:
        """@brief 按 ``(created_at,id)`` keyset 列出 Scenario / List Scenarios by ``(created_at,id)`` keyset."""

    async def get_scenario(
        self,
        workspace_id: WorkspaceId,
        scenario_id: InterviewScenarioId,
        *,
        for_update: bool = False,
    ) -> InterviewScenario | None:
        """@brief 在 Workspace 内读取 Scenario / Read a Scenario inside one Workspace."""

    async def add_scenario(self, scenario: InterviewScenario) -> None:
        """@brief 添加 Scenario / Add a Scenario."""

    async def save_scenario(
        self,
        scenario: InterviewScenario,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以旧 revision CAS Scenario / CAS a Scenario using its old revision."""

    async def list_sessions(
        self,
        workspace_id: WorkspaceId,
        page: InterviewPageRequest,
    ) -> InterviewPage[InterviewSession]:
        """@brief 按 ``(created_at,id)`` keyset 列出 Session / List Sessions by ``(created_at,id)`` keyset."""

    async def get_session(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        for_update: bool = False,
    ) -> InterviewSession | None:
        """@brief 在 Workspace 内读取 Session / Read a Session inside one Workspace."""

    async def add_session(self, session: InterviewSession) -> None:
        """@brief 添加含冻结 rubric/policy 的 Session / Add a Session with frozen rubric/policy."""

    async def save_session(
        self,
        session: InterviewSession,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以旧 revision CAS Session / CAS a Session using its old revision."""

    async def add_connection_lease(self, lease: RealtimeConnectionLease) -> None:
        """@brief 保存无 token/ICE secret 的连接 lease / Save a connection lease without token or ICE secrets."""

    async def get_connection_lease(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        connection_id: RealtimeConnectionId,
    ) -> RealtimeConnectionLease | None:
        """@brief 以 Workspace+Session+Connection 三元组读取 lease / Read a lease by Workspace+Session+Connection tuple."""

    async def append_realtime_input(
        self,
        record: RealtimeInputLedgerRecord,
    ) -> RealtimeInputReceipt:
        """@brief 原子幂等追加输入并分配严格 sequence / Atomically append idempotent input and allocate strict sequence.

        @note 相同 input ID+fingerprint 重放原 receipt；不同 fingerprint 抛
            ``RealtimeInputKeyReused``；任何分支都不能重复推进 sequence。
            / Same ID+fingerprint replays its receipt; a different fingerprint raises
            ``RealtimeInputKeyReused``; no branch advances sequence twice.
        @note 该类型不含 candidate 正文；禁止实现为幂等而持久化未获得
            Transcript storage consent 的内容。/ The type contains no candidate plaintext;
            implementations must not persist content lacking Transcript-storage consent merely
            for idempotency.
        """

    async def allocate_transcript_sequence(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
    ) -> TranscriptSequenceReservation:
        """@brief 在 Session 行/计数器上原子分配 Transcript sequence / Atomically allocate a Transcript sequence on the Session counter."""

    async def add_transcript_segment(self, segment: TranscriptSegment) -> None:
        """@brief 添加 immutable Transcript segment / Add an immutable Transcript segment."""

    async def list_transcript(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        page: InterviewPageRequest,
    ) -> InterviewPage[TranscriptSegment]:
        """@brief 按 ``(sequence,id)`` keyset 列出 Transcript / List Transcript by ``(sequence,id)`` keyset."""

    async def load_transcript_snapshot(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        maximum_segments: int,
    ) -> tuple[TranscriptSegment, ...]:
        """@brief 在一致性快照中加载有界完整 Transcript / Load a bounded full Transcript in one consistent snapshot."""

    async def get_report(
        self,
        workspace_id: WorkspaceId,
        report_id: InterviewReportId,
    ) -> InterviewReport | None:
        """@brief 在 Workspace 内读取 Report / Read a Report inside one Workspace."""

    async def add_report(self, report: InterviewReport) -> None:
        """@brief 添加 immutable Report / Add an immutable Report."""

    async def has_live_report_job(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
    ) -> bool:
        """@brief 判断 Session 是否已有非终态 Report Job / Test whether a Session already has a live Report Job."""


class InterviewJobStore(Protocol):
    """@brief 直接复用统一 platform Job 表的 Port / Port directly reusing the unified platform Job table."""

    async def add(self, job: Job, spec: InterviewJobSpec) -> None:
        """@brief 原子写入统一 Job 与 typed worker spec / Atomically add a unified Job and typed worker spec."""

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 在 Workspace 内读取统一 Job / Read a unified Job inside one Workspace."""

    async def get_owned(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 按 outbox creator 精确读取 Job / Read a Job exactly by its outbox creator.

        @param workspace_id outbox 独立列中的 Workspace / Workspace from the outbox's dedicated column.
        @param actor_id outbox 独立列中的原始 creator / Original creator from the outbox's dedicated column.
        @param job_id outbox subject Job ID / Job ID from the outbox subject.
        @param for_update 是否锁行 / Whether to lock the row.
        @return 仅在三元 identity 全部匹配时返回 / Returned only when the full identity tuple matches.
        """

    async def save(self, job: Job, *, expected_revision: int) -> None:
        """@brief 以旧 revision CAS 统一 Job / CAS the unified Job using its old revision."""


class InterviewArtifactStore(Protocol):
    """@brief 直接复用统一 platform Artifact 表的 Port / Port directly reusing the unified platform Artifact table."""

    async def add(self, artifact: Artifact) -> None:
        """@brief 添加统一 Artifact，不建 Interview 平行 Artifact / Add a unified Artifact, never an Interview-specific duplicate."""


class InterviewOutbox(Protocol):
    """@brief 平台统一 transactional outbox Port / Platform-wide transactional-outbox port."""

    async def add(self, record: InterviewJobQueuedRecord) -> None:
        """@brief 写统一 outbox，不建 Interview Event 真相 / Write the unified outbox without creating a parallel Interview Event truth."""


class InterviewAuditSink(Protocol):
    """@brief 平台统一 AuditEvent sink / Platform-wide AuditEvent sink."""

    async def add(self, event: AuditEvent) -> None:
        """@brief 同事务写统一审计 / Write unified audit in the same transaction."""


class InterviewRealtimeGateway(Protocol):
    """@brief 外部 WebRTC/WS signaling gateway / External WebRTC/WS signaling gateway."""

    async def issue(
        self,
        workspace_id: WorkspaceId,
        session: InterviewSession,
        audience: ResourceRef,
        spec: CreateRealtimeConnectionSpec,
        *,
        issued_at: datetime,
    ) -> RealtimeConnection:
        """@brief 签发短期单 Session/单 audience grant / Issue a short-lived single-Session/single-audience grant.

        @note 只可在 UoW 之外调用；token、ICE credential 不进日志、outbox 或 repository。
            / Call only outside a UoW; tokens and ICE credentials never enter logs, outbox, or repository.
        """

    async def revoke(self, connection_id: RealtimeConnectionId) -> None:
        """@brief 第二阶段 CAS 失败时撤销孤儿 grant / Revoke an orphan grant after second-phase CAS failure."""


class InterviewMediaFinalizer(Protocol):
    """@brief 外部媒体 finalize Port / External media-finalization port."""

    async def finalize(
        self,
        session: InterviewSession,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> EndSessionOutput:
        """@brief 按 consent/retention 生成统一 Artifacts / Produce unified Artifacts under consent/retention.

        @param operation_id 同一 Job 所有尝试共享的 provider 幂等键 / Provider idempotency key
            shared by every attempt of the same Job.
        @note 只可在结束 Job 的两个短事务之间调用。
            / Call only between the two short end-Job transactions.
        """


class InterviewReportProvider(Protocol):
    """@brief 外部 Report evaluation provider / External Report-evaluation provider."""

    async def generate(
        self,
        request: ReportGenerationRequest,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> InterviewReportDraft:
        """@brief 生成公开安全 Report 草稿 / Generate a public-safe Report draft.

        @param operation_id 同一 Job 所有尝试共享的 provider 幂等键 / Provider idempotency key
            shared by every attempt of the same Job.
        @note 只可在 UoW 外调用；adapter 丢弃私有推理和原始 provider payload。
            / Call only outside a UoW; adapters discard private reasoning and raw provider payloads.
        """


class InterviewUnitOfWork(Protocol):
    """@brief Scenario、Session、Report、Job、Artifact、outbox、audit 单一 UoW / Single UoW for all Interview aggregates and platform records."""

    @property
    def authorizer(self) -> InterviewPermissionAuthorizer:
        """@brief 返回精确权限 adapter / Return the exact-permission adapter."""

    @property
    def policy(self) -> InterviewSessionPolicy:
        """@brief 返回 Session policy / Return the Session policy."""

    @property
    def repository(self) -> InterviewRepository:
        """@brief 返回事务绑定 repository / Return the transaction-bound repository."""

    @property
    def jobs(self) -> InterviewJobStore:
        """@brief 返回统一 Job store / Return the unified Job store."""

    @property
    def artifacts(self) -> InterviewArtifactStore:
        """@brief 返回统一 Artifact store / Return the unified Artifact store."""

    @property
    def outbox(self) -> InterviewOutbox:
        """@brief 返回统一 outbox / Return the unified outbox."""

    @property
    def audit(self) -> InterviewAuditSink:
        """@brief 返回统一 audit sink / Return the unified audit sink."""

    async def __aenter__(self) -> Self:
        """@brief 开始 UoW / Enter the UoW."""

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时回滚 / Roll back on exception or absent commit."""

    async def commit(self) -> None:
        """@brief 原子提交聚合、统一平台记录与外层幂等 receipt / Atomically commit aggregates, platform records, and outer idempotency receipt."""

    async def rollback(self) -> None:
        """@brief 幂等回滚 / Roll back idempotently."""


class InterviewUnitOfWorkFactory(Protocol):
    """@brief 创建 Interview UoW / Create Interview UoWs."""

    def __call__(self) -> InterviewUnitOfWork:
        """@brief 创建未进入的 UoW / Create a not-yet-entered UoW."""


__all__ = [
    "EndSessionOutput",
    "InterviewArtifactStore",
    "InterviewAuditSink",
    "InterviewCasMismatch",
    "InterviewJobStore",
    "InterviewOutbox",
    "InterviewPage",
    "InterviewPageRequest",
    "InterviewPermission",
    "InterviewPermissionAuthorizer",
    "InterviewPermissionGrant",
    "InterviewPermissionRequest",
    "InterviewPolicyDenied",
    "InterviewRealtimeGateway",
    "InterviewReportProvider",
    "InterviewRepository",
    "InterviewSessionPolicy",
    "InterviewSessionPolicyRequest",
    "InterviewUnitOfWork",
    "InterviewUnitOfWorkFactory",
    "InterviewWorkerOperationId",
    "InterviewWorkerPortFailure",
    "RealtimeInputKeyReused",
    "ReportGenerationRequest",
    "TranscriptSequenceReservation",
]
