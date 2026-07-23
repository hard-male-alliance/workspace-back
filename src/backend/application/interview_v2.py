"""@brief API v2 Interview 应用用例 / API v2 Interview application use cases.

本模块逐一覆盖 ``contract.md`` 5.5 的 12 个 HTTP 无关用例。所有资源都以
路径 Workspace 为首键重新校验；Scenario/Session/Job 使用 revision CAS；
realtime 输入以不含正文的 ledger 原子幂等去重。signaling、media 与 report provider
调用均在短 UoW 之外，不持有数据库锁跨网络等待。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from backend.application.ports.interview_v2 import (
    InterviewCasMismatch,
    InterviewMediaFinalizer,
    InterviewPage,
    InterviewPageRequest,
    InterviewPermission,
    InterviewPermissionGrant,
    InterviewPermissionRequest,
    InterviewRealtimeGateway,
    InterviewReportProvider,
    InterviewSessionPolicyRequest,
    InterviewUnitOfWork,
    InterviewUnitOfWorkFactory,
    InterviewWorkerOperationId,
    InterviewWorkerPortFailure,
    ReportGenerationRequest,
)
from backend.domain.interview_v2 import (
    INTERVIEW_END_JOB_KIND,
    INTERVIEW_REPORT_JOB_KIND,
    CandidateUtteranceInput,
    CreateRealtimeConnectionSpec,
    EndInterviewReason,
    EndSessionJobSpec,
    InterviewJobQueuedRecord,
    InterviewMediaPreferences,
    InterviewOutboxId,
    InterviewReport,
    InterviewReportId,
    InterviewScenario,
    InterviewScenarioId,
    InterviewScenarioPatch,
    InterviewScenarioSpec,
    InterviewScenarioStatus,
    InterviewSession,
    InterviewSessionId,
    InterviewSessionSpec,
    InterviewSessionStatus,
    InterviewSessionView,
    JobTarget,
    RealtimeConnection,
    RealtimeConnectionId,
    RealtimeConnectionLease,
    RealtimeControl,
    RealtimeControlInput,
    RealtimeInputEnvelope,
    RealtimeInputReceipt,
    RecordingConsent,
    ReportJobSpec,
    TranscriptSegment,
    TranscriptSegmentId,
    TranscriptSpeaker,
    validate_artifacts_for_session,
    validate_interview_job_alignment,
)
from backend.domain.knowledge_retrieval import InferenceIntent, KnowledgeSelection
from backend.domain.platform import (
    AuditEvent,
    AuditEventId,
    AuditOutcome,
    Job,
    JobId,
    JobProgress,
    JobProgressUnit,
    JobStatus,
    ProblemDetails,
)
from backend.domain.principals import ResourceMeta, TokenPrincipal, UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from workspace_shared.ids import new_opaque_id

V2_INTERVIEW_ENDPOINT_METHODS = (
    "list_scenarios",
    "create_scenario",
    "get_scenario",
    "update_scenario",
    "list_sessions",
    "create_session",
    "get_session",
    "create_realtime_connection",
    "create_end_request",
    "get_transcript",
    "create_report_job",
    "get_report",
)
"""@brief 5.5 实际 12 个路由对应的应用方法 / Application methods for the 12 actual section-5.5 routes."""


class Clock(Protocol):
    """@brief 可替换应用时钟 / Replaceable application clock."""

    def now(self) -> datetime:
        """@brief 返回带时区当前时刻 / Return the current timezone-aware instant."""


class OpaqueIdFactory(Protocol):
    """@brief 可替换 opaque-ID 工厂 / Replaceable opaque-ID factory."""

    def __call__(self, prefix: str) -> str:
        """@brief 生成指定前缀的 ID / Generate an ID with the given prefix."""


class UtcClock:
    """@brief UTC 生产时钟 / UTC production clock."""

    def now(self) -> datetime:
        """@brief 返回 UTC 当前时刻 / Return the current UTC instant."""
        return datetime.now(UTC)


class NewOpaqueIdFactory:
    """@brief 共享 opaque-ID 生成器 / Shared opaque-ID generator."""

    def __call__(self, prefix: str) -> str:
        """@brief 生成新 ID / Generate a new ID."""
        return new_opaque_id(prefix)


class InterviewApplicationError(Exception):
    """@brief 可稳定映射为 RFC 9457 problem 的应用错误 / Application error mappable to RFC 9457."""

    code: str
    """@brief 稳定机器错误码 / Stable machine-readable error code."""

    detail: str
    """@brief 不泄漏跨 Workspace 信息的说明 / Public detail without cross-Workspace disclosure."""

    def __init__(self, code: str, detail: str) -> None:
        """@brief 初始化结构化错误 / Initialize a structured error."""
        super().__init__(detail)
        self.code = code
        self.detail = detail


class InterviewResourceNotFound(InterviewApplicationError):
    """@brief 资源不存在或为防枚举而隐藏 / Resource absent or hidden to prevent enumeration."""

    def __init__(self, resource: str) -> None:
        """@brief 生成统一不泄漏 404 来源 / Create a uniform non-disclosing not-found result."""
        super().__init__(f"{resource}.not_found", f"{resource} was not found")


class InterviewPreconditionFailed(InterviewApplicationError):
    """@brief 强 ETag 对应 revision 已过期 / Revision represented by a strong ETag is stale."""

    def __init__(self) -> None:
        """@brief 生成统一 412 来源 / Create a uniform precondition-failed result."""
        super().__init__("http.precondition_failed", "resource revision precondition failed")


class InterviewConflict(InterviewApplicationError):
    """@brief 当前状态拒绝命令 / Current state rejects a command."""


class InterviewPortProtocolError(InterviewApplicationError):
    """@brief Adapter 返回跨越租户、顺序或策略边界的数据 / Adapter violated tenant, ordering, or policy boundaries."""


class InvalidInterviewCommand(InterviewApplicationError):
    """@brief 命令不满足 5.5 边界 / Command violates section-5.5 bounds."""


class InterviewWorkerError(InterviewApplicationError):
    """@brief Worker 已安全记录终态失败 / Worker durably recorded a terminal failure."""


class InterviewWorkerRetry(InterviewApplicationError):
    """@brief Worker 保留 running Job 并请求统一 outbox 重放 / Worker retained a running Job and requests unified-outbox replay."""


@dataclass(frozen=True, slots=True)
class InterviewMutationContext:
    """@brief 写请求审计关联 / Audit correlation for a write request."""

    request_id: str

    def __post_init__(self) -> None:
        """@brief 校验 request ID 基本边界 / Validate basic request-ID bounds."""
        if not 8 <= len(self.request_id) <= 160 or not self.request_id[0].isalpha():
            raise InvalidInterviewCommand("request.invalid_id", "request id is invalid")


@dataclass(frozen=True, slots=True)
class CreateInterviewScenarioCommand:
    """@brief CreateInterviewScenarioRequest 的类型化命令 / Typed CreateInterviewScenarioRequest command."""

    spec: InterviewScenarioSpec


@dataclass(frozen=True, slots=True)
class CreateInterviewSessionCommand:
    """@brief CreateInterviewSessionRequest 的类型化命令 / Typed CreateInterviewSessionRequest command."""

    scenario_id: InterviewScenarioId
    resume_ref: ResourceRef | None
    job_target: JobTarget
    knowledge: KnowledgeSelection
    locale: str
    media: InterviewMediaPreferences
    recording: RecordingConsent
    inference: InferenceIntent


@dataclass(frozen=True, slots=True)
class EndInterviewSessionCommand:
    """@brief EndInterviewSessionRequest 的类型化命令 / Typed EndInterviewSessionRequest command."""

    reason: EndInterviewReason


@dataclass(frozen=True, slots=True)
class CreateInterviewReportJobCommand:
    """@brief CreateInterviewReportJobRequest 的类型化命令 / Typed CreateInterviewReportJobRequest command."""

    rubric_version: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验可选 Rubric 版本 / Validate the optional Rubric version."""
        if self.rubric_version is not None and not 1 <= len(self.rubric_version) <= 80:
            raise InvalidInterviewCommand(
                "interview_report.invalid_rubric_version",
                "rubric version must contain 1 to 80 characters",
            )


@dataclass(frozen=True, slots=True)
class InterviewMediaCapabilities:
    """@brief 部署实际可完成的录音/录像能力 / Recording capabilities the deployment can actually finalize.

    @param audio_recording 是否配置 audio capture/finalizer/storage / Whether audio capture, finalization, and storage are configured.
    @param video_recording 是否配置 video capture/finalizer/storage / Whether video capture, finalization, and storage are configured.
    """

    audio_recording: bool = False
    video_recording: bool = False

    def require(self, consent: RecordingConsent) -> None:
        """@brief 在持久化 Session 前验证所请求媒体能力 / Validate requested media capabilities before persisting a Session.

        @param consent 已验证的冻结 recording consent / Validated recording consent to freeze.
        @raise InvalidInterviewCommand 请求了部署无法完成的 recording 时抛出 / Raised when recording is requested but unavailable in this deployment.
        """

        unavailable = (
            consent.record_audio and not self.audio_recording
        ) or (consent.record_video and not self.video_recording)
        if unavailable:
            raise InvalidInterviewCommand(
                "interview.recording_unavailable",
                "requested recording is not available in this deployment",
            )


class InterviewApplicationService:
    """@brief 5.5 十二个 transport-independent 用例 / Twelve transport-independent section-5.5 use cases."""

    def __init__(
        self,
        uow_factory: InterviewUnitOfWorkFactory,
        realtime_gateway: InterviewRealtimeGateway,
        *,
        media_capabilities: InterviewMediaCapabilities | None = None,
        clock: Clock | None = None,
        id_factory: OpaqueIdFactory | None = None,
    ) -> None:
        """@brief 注入 UoW、realtime、真实媒体能力、时钟与 ID 工厂 / Inject UoW, realtime, actual media capabilities, clock, and ID factory.

        @param media_capabilities 部署可兑现的 recording 能力；默认全部关闭 / Recording capabilities this deployment can fulfill; all disabled by default.
        """
        self._uow_factory = uow_factory
        self._realtime_gateway = realtime_gateway
        self._media_capabilities = media_capabilities or InterviewMediaCapabilities()
        self._clock = clock or UtcClock()
        self._ids = id_factory or NewOpaqueIdFactory()

    async def list_scenarios(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        page: InterviewPageRequest,
    ) -> InterviewPage[InterviewScenario]:
        """@brief 列出 Scenario / List Scenarios."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.LIST_SCENARIOS,
                _workspace_ref(workspace_id),
            )
            result = await uow.repository.list_scenarios(workspace_id, page)
            _validate_scenario_page(result, workspace_id)
            await uow.commit()
            return result

    async def create_scenario(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateInterviewScenarioCommand,
        context: InterviewMutationContext,
    ) -> InterviewScenario:
        """@brief 创建 draft Scenario / Create a draft Scenario."""
        now = self._clock.now()
        scenario = InterviewScenario(
            meta=ResourceMeta(
                InterviewScenarioId(self._ids("scenario")),
                1,
                now,
                now,
            ),
            workspace_id=workspace_id,
            spec=command.spec,
        )
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.CREATE_SCENARIO,
                _workspace_ref(workspace_id),
            )
            await uow.repository.add_scenario(scenario)
            await uow.audit.add(
                self._audit(
                    principal,
                    workspace_id,
                    "interview_scenario.create",
                    _scenario_ref(scenario),
                    context,
                    now,
                )
            )
            await uow.commit()
            return scenario

    async def get_scenario(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        scenario_id: InterviewScenarioId,
    ) -> InterviewScenario:
        """@brief 读取 Workspace 内的 Scenario / Read a Scenario inside the Workspace."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.READ_SCENARIO,
                ResourceRef("interview_scenario", scenario_id),
            )
            scenario = await self._scenario(uow, workspace_id, scenario_id)
            await uow.commit()
            return scenario

    async def update_scenario(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        scenario_id: InterviewScenarioId,
        patch: InterviewScenarioPatch,
        *,
        expected_revision: int,
        context: InterviewMutationContext,
    ) -> InterviewScenario:
        """@brief 以强 If-Match 更新 Scenario / Update a Scenario with strong If-Match."""
        now = self._clock.now()
        try:
            async with self._uow_factory() as uow:
                await self._authorize(
                    uow,
                    principal,
                    workspace_id,
                    InterviewPermission.UPDATE_SCENARIO,
                    ResourceRef("interview_scenario", scenario_id),
                )
                before = await self._scenario(
                    uow,
                    workspace_id,
                    scenario_id,
                    for_update=True,
                )
                _require_revision(before.meta.revision, expected_revision)
                after = before.update(patch, at=now)
                await uow.repository.save_scenario(
                    after,
                    expected_revision=before.meta.revision,
                )
                await uow.audit.add(
                    self._audit(
                        principal,
                        workspace_id,
                        "interview_scenario.update",
                        _scenario_ref(after),
                        context,
                        now,
                    )
                )
                await uow.commit()
                return after
        except InterviewCasMismatch as error:
            raise InterviewPreconditionFailed from error

    async def get_scenario_for_update(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        scenario_id: InterviewScenarioId,
    ) -> InterviewScenario:
        """@brief 用 UPDATE 精确权限读取 If-Match 快照 / Read an If-Match snapshot under the exact UPDATE permission."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.UPDATE_SCENARIO,
                ResourceRef("interview_scenario", scenario_id),
            )
            scenario = await self._scenario(uow, workspace_id, scenario_id)
            await uow.commit()
            return scenario

    async def list_sessions(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        page: InterviewPageRequest,
    ) -> InterviewPage[InterviewSessionView]:
        """@brief 列出 Session 的公开投影 / List public Session projections."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.LIST_SESSIONS,
                _workspace_ref(workspace_id),
            )
            result = await uow.repository.list_sessions(workspace_id, page)
            _validate_session_page(result, workspace_id)
            await uow.commit()
            return InterviewPage(tuple(item.view for item in result.items), result.next_position)

    async def create_session(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateInterviewSessionCommand,
        context: InterviewMutationContext,
    ) -> InterviewSessionView:
        """@brief 从 active Scenario 创建并冻结 Session 快照 / Create a Session and freeze its active Scenario snapshot."""
        now = self._clock.now()
        async with self._uow_factory() as uow:
            permission = await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.CREATE_SESSION,
                ResourceRef("interview_scenario", command.scenario_id),
            )
            self._media_capabilities.require(command.recording)
            scenario = await self._scenario(
                uow,
                workspace_id,
                command.scenario_id,
                for_update=True,
            )
            if scenario.status is not InterviewScenarioStatus.ACTIVE:
                raise InterviewConflict(
                    "interview_scenario.not_active",
                    "an Interview Session requires an active Scenario",
                )
            spec = InterviewSessionSpec(
                scenario_id=scenario.meta.id,
                scenario_revision=scenario.meta.revision,
                rubric_snapshot=scenario.spec.rubric,
                resume_ref=command.resume_ref,
                job_target=command.job_target,
                knowledge=command.knowledge,
                locale=command.locale,
                media=command.media,
                recording=command.recording,
                inference=command.inference,
            )
            execution_grant = await uow.policy.authorize_session(
                InterviewSessionPolicyRequest(
                    actor_id=permission.actor_id,
                    workspace_id=workspace_id,
                    scenario=scenario,
                    spec=spec,
                )
            )
            execution_grant.validate_for(scenario, spec)
            view = InterviewSessionView(
                meta=ResourceMeta(
                    InterviewSessionId(self._ids("session")),
                    1,
                    now,
                    now,
                ),
                workspace_id=workspace_id,
                scenario_id=scenario.meta.id,
                resume_ref=spec.resume_ref,
                job_target=spec.job_target,
                status=InterviewSessionStatus.CREATED,
                locale=spec.locale,
                media=spec.media,
                recording=spec.recording,
                started_at=None,
                ended_at=None,
                report_id=None,
            )
            session = InterviewSession(view, spec, execution_grant)
            await uow.repository.add_session(session)
            await uow.audit.add(
                self._audit(
                    principal,
                    workspace_id,
                    "interview_session.create",
                    _session_ref(session),
                    context,
                    now,
                )
            )
            await uow.commit()
            return session.view

    async def get_session(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
    ) -> InterviewSessionView:
        """@brief 读取 Session 公开投影 / Read a public Session projection."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.READ_SESSION,
                ResourceRef("interview_session", session_id),
            )
            session = await self._session(uow, workspace_id, session_id)
            await uow.commit()
            return session.view

    async def create_realtime_connection(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        spec: CreateRealtimeConnectionSpec,
        context: InterviewMutationContext,
    ) -> RealtimeConnection:
        """@brief 以两段短事务签发不持久化 secret 的 realtime grant / Issue a realtime grant with two short transactions and no persisted secret."""
        issued_at = self._clock.now()
        async with self._uow_factory() as first:
            permission = await self._authorize(
                first,
                principal,
                workspace_id,
                InterviewPermission.CREATE_CONNECTION,
                ResourceRef("interview_session", session_id),
            )
            base = await self._session(first, workspace_id, session_id)
            if base.view.status not in {
                InterviewSessionStatus.CREATED,
                InterviewSessionStatus.CONNECTING,
                InterviewSessionStatus.ACTIVE,
            }:
                raise InterviewConflict(
                    "interview_session.connection_forbidden",
                    "the Session cannot accept a realtime connection in its current state",
                )
            audience = ResourceRef("user", permission.actor_id)
            await first.commit()

        connection = await self._realtime_gateway.issue(
            workspace_id,
            base,
            audience,
            spec,
            issued_at=issued_at,
        )
        try:
            _validate_connection_grant(
                connection,
                workspace_id,
                session_id,
                audience,
                spec,
                issued_at,
            )
            try:
                async with self._uow_factory() as second:
                    await self._authorize(
                        second,
                        principal,
                        workspace_id,
                        InterviewPermission.CREATE_CONNECTION,
                        ResourceRef("interview_session", session_id),
                    )
                    current = await self._session(
                        second,
                        workspace_id,
                        session_id,
                        for_update=True,
                    )
                    if current.meta.revision != base.meta.revision:
                        raise InterviewConflict(
                            "interview_session.connection_race",
                            "the Session changed while the realtime connection was issued",
                        )
                    changed = current.mark_connecting(at=self._clock.now())
                    if changed.meta.revision != current.meta.revision:
                        await second.repository.save_session(
                            changed,
                            expected_revision=current.meta.revision,
                        )
                    await second.repository.add_connection_lease(
                        RealtimeConnectionLease.from_connection(connection)
                    )
                    await second.audit.add(
                        self._audit(
                            principal,
                            workspace_id,
                            "interview_session.connection.create",
                            ResourceRef("realtime_connection", connection.id),
                            context,
                            self._clock.now(),
                        )
                    )
                    await second.commit()
            except InterviewCasMismatch as error:
                raise InterviewConflict(
                    "interview_session.connection_race",
                    "the Session changed while the realtime connection was committed",
                ) from error
        except BaseException:
            try:
                await self._realtime_gateway.revoke(connection.id)
            except Exception as revoke_error:
                raise InterviewPortProtocolError(
                    "realtime_connection.revoke_failed",
                    "an uncommitted realtime connection could not be revoked",
                ) from revoke_error
            raise
        return connection

    async def create_end_request(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        command: EndInterviewSessionCommand,
        *,
        expected_revision: int,
        context: InterviewMutationContext,
    ) -> Job:
        """@brief 以 Session CAS 原子创建统一 end Job / Atomically create a unified end Job with Session CAS."""
        now = self._clock.now()
        job_id = JobId(self._ids("job"))
        try:
            async with self._uow_factory() as uow:
                await self._authorize(
                    uow,
                    principal,
                    workspace_id,
                    InterviewPermission.END_SESSION,
                    ResourceRef("interview_session", session_id),
                )
                before = await self._session(
                    uow,
                    workspace_id,
                    session_id,
                    for_update=True,
                )
                _require_revision(before.meta.revision, expected_revision)
                after = before.begin_end(job_id, command.reason, at=now)
                job = Job(
                    meta=ResourceMeta(job_id, 1, now, now),
                    workspace_id=workspace_id,
                    kind=INTERVIEW_END_JOB_KIND,
                    subject=_session_ref(after),
                )
                validate_interview_job_alignment(after, job)
                await uow.repository.save_session(
                    after,
                    expected_revision=before.meta.revision,
                )
                await uow.jobs.add(
                    job,
                    EndSessionJobSpec(session_id, command.reason, before.spec.recording),
                )
                await uow.outbox.add(
                    self._queued_record(after, job, principal.user_id, now)
                )
                await uow.audit.add(
                    self._audit(
                        principal,
                        workspace_id,
                        "interview_session.end",
                        _session_ref(after),
                        context,
                        now,
                    )
                )
                await uow.commit()
                return job
        except InterviewCasMismatch as error:
            raise InterviewPreconditionFailed from error

    async def get_session_for_end(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
    ) -> InterviewSessionView:
        """@brief 用 END 精确权限读取 If-Match 快照 / Read an If-Match snapshot under the exact END permission."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.END_SESSION,
                ResourceRef("interview_session", session_id),
            )
            session = await self._session(uow, workspace_id, session_id)
            await uow.commit()
            return session.view

    async def get_transcript(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        page: InterviewPageRequest,
    ) -> InterviewPage[TranscriptSegment]:
        """@brief 按 persistence sequence 读取已同意存储的 Transcript / Read a consented Transcript by persisted sequence."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.READ_TRANSCRIPT,
                ResourceRef("interview_session", session_id),
            )
            session = await self._session(uow, workspace_id, session_id)
            if not session.spec.recording.store_transcript:
                raise InterviewConflict(
                    "interview_transcript.storage_disabled",
                    "Transcript storage was not consented for this Session",
                )
            result = await uow.repository.list_transcript(workspace_id, session_id, page)
            _validate_transcript(result, workspace_id, session_id)
            await uow.commit()
            return result

    async def create_report_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        command: CreateInterviewReportJobCommand,
        context: InterviewMutationContext,
    ) -> Job:
        """@brief 为 completed Session 创建统一 Report Job / Create a unified Report Job for a completed Session."""
        now = self._clock.now()
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.CREATE_REPORT_JOB,
                ResourceRef("interview_session", session_id),
            )
            session = await self._session(
                uow,
                workspace_id,
                session_id,
                for_update=True,
            )
            if session.view.status is not InterviewSessionStatus.COMPLETED:
                raise InterviewConflict(
                    "interview_report.session_not_completed",
                    "a Report can only be generated for a completed Session",
                )
            if session.view.report_id is not None:
                raise InterviewConflict(
                    "interview_report.already_exists",
                    "the Session already has an Interview Report",
                )
            frozen = session.spec.rubric_snapshot
            if command.rubric_version is not None and command.rubric_version != frozen.rubric_version:
                raise InterviewConflict(
                    "interview_report.rubric_mismatch",
                    "the requested Rubric version does not match the frozen Session Rubric",
                )
            if await uow.repository.has_live_report_job(workspace_id, session_id):
                raise InterviewConflict(
                    "interview_report.job_exists",
                    "the Session already has a live Report Job",
                )
            job = Job(
                meta=ResourceMeta(JobId(self._ids("job")), 1, now, now),
                workspace_id=workspace_id,
                kind=INTERVIEW_REPORT_JOB_KIND,
                subject=_session_ref(session),
            )
            await uow.jobs.add(
                job,
                ReportJobSpec(session_id, frozen.rubric_id, frozen.rubric_version),
            )
            await uow.outbox.add(
                self._queued_record(session, job, principal.user_id, now)
            )
            await uow.audit.add(
                self._audit(
                    principal,
                    workspace_id,
                    "interview_report.create",
                    _session_ref(session),
                    context,
                    now,
                )
            )
            await uow.commit()
            return job

    async def get_report(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        report_id: InterviewReportId,
    ) -> InterviewReport:
        """@brief 读取 immutable InterviewReport / Read an immutable InterviewReport."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                InterviewPermission.READ_REPORT,
                ResourceRef("interview_report", report_id),
            )
            report = await uow.repository.get_report(workspace_id, report_id)
            if report is None or report.workspace_id != workspace_id:
                raise InterviewResourceNotFound("interview_report")
            await uow.commit()
            return report

    async def ingest_realtime_input(
        self,
        audience: ResourceRef,
        envelope: RealtimeInputEnvelope,
    ) -> RealtimeInputReceipt:
        """@brief 原子幂等接收 realtime 输入，且仅在同意时存 Transcript / Atomically ingest realtime input and store Transcript only with consent."""
        now = self._clock.now()
        try:
            async with self._uow_factory() as uow:
                lease = await uow.repository.get_connection_lease(
                    envelope.workspace_id,
                    envelope.session_id,
                    envelope.connection_id,
                )
                if (
                    lease is None
                    or lease.workspace_id != envelope.workspace_id
                    or lease.session_id != envelope.session_id
                    or lease.id != envelope.connection_id
                    or lease.audience != audience
                ):
                    raise InterviewResourceNotFound("realtime_connection")
                receipt = await uow.repository.append_realtime_input(envelope.ledger_record())
                if receipt.replayed:
                    await uow.commit()
                    return receipt
                if now >= lease.expires_at:
                    raise InterviewConflict(
                        "realtime_connection.expired",
                        "the realtime connection has expired",
                    )
                session = await self._session(
                    uow,
                    envelope.workspace_id,
                    envelope.session_id,
                    for_update=True,
                )
                if session.view.status not in {
                    InterviewSessionStatus.CONNECTING,
                    InterviewSessionStatus.ACTIVE,
                }:
                    raise InterviewConflict(
                        "interview_session.realtime_input_forbidden",
                        "the Session cannot accept realtime input in its current state",
                    )
                if (
                    isinstance(envelope.payload, CandidateUtteranceInput)
                    and session.view.status is not InterviewSessionStatus.ACTIVE
                ):
                    raise InterviewConflict(
                        "interview_session.not_active",
                        "candidate utterances require an active Session",
                    )
                if (
                    isinstance(envelope.payload, RealtimeControlInput)
                    and envelope.payload.control is RealtimeControl.MEDIA_STARTED
                    and session.view.status is InterviewSessionStatus.CONNECTING
                ):
                    active = session.activate(at=now)
                    await uow.repository.save_session(
                        active,
                        expected_revision=session.meta.revision,
                    )
                    session = active
                if (
                    isinstance(envelope.payload, CandidateUtteranceInput)
                    and session.spec.recording.store_transcript
                ):
                    reservation = await uow.repository.allocate_transcript_sequence(
                        envelope.workspace_id,
                        envelope.session_id,
                    )
                    await uow.repository.add_transcript_segment(
                        TranscriptSegment(
                            id=TranscriptSegmentId(self._ids("segment")),
                            workspace_id=envelope.workspace_id,
                            session_id=envelope.session_id,
                            sequence=reservation.sequence,
                            source_ref=ResourceRef("realtime_input", envelope.input_id),
                            speaker=TranscriptSpeaker.CANDIDATE,
                            start_ms=envelope.payload.start_ms,
                            end_ms=envelope.payload.end_ms,
                            text=envelope.payload.text,
                        )
                    )
                await uow.commit()
                return receipt
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview_session.realtime_race",
                "the Session changed while realtime input was committed",
            ) from error

    async def authorize_media_capture(
        self,
        audience: ResourceRef,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        connection_id: RealtimeConnectionId,
        kind: str,
    ) -> None:
        """Validate lease, active state, and frozen recording consent before writing a chunk."""
        now = self._clock.now()
        async with self._uow_factory() as uow:
            lease = await uow.repository.get_connection_lease(
                workspace_id, session_id, connection_id
            )
            if (
                lease is None
                or lease.audience != audience
                or now >= lease.expires_at
            ):
                raise InterviewResourceNotFound("realtime_connection")
            session = await self._session(uow, workspace_id, session_id)
            if session.view.status not in {
                InterviewSessionStatus.CONNECTING,
                InterviewSessionStatus.ACTIVE,
            }:
                raise InterviewConflict(
                    "interview_session.media_capture_forbidden",
                    "the Session cannot accept media in its current state",
                )
            allowed = (
                session.spec.recording.record_audio
                if kind == "audio"
                else session.spec.recording.record_video
                if kind == "video"
                else False
            )
            if not allowed:
                raise InterviewConflict(
                    "interview_session.recording_not_consented",
                    "recording consent does not allow this media kind",
                )
            await uow.commit()

    async def _authorize(
        self,
        uow: InterviewUnitOfWork,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        permission: InterviewPermission,
        target: ResourceRef,
    ) -> InterviewPermissionGrant:
        """@brief 发出并校验单 endpoint 精确权限 / Issue and validate an exact endpoint permission."""
        request = InterviewPermissionRequest(workspace_id, permission, target)
        grant = await uow.authorizer.authorize(principal, request)
        if grant.actor_id != principal.user_id or grant.request != request:
            raise InterviewPortProtocolError(
                "interview.authorization_protocol_violation",
                "Interview authorizer returned a mismatched grant",
            )
        return grant

    async def _scenario(
        self,
        uow: InterviewUnitOfWork,
        workspace_id: WorkspaceId,
        scenario_id: InterviewScenarioId,
        *,
        for_update: bool = False,
    ) -> InterviewScenario:
        """@brief Workspace-first 读取并二次校验 Scenario / Read and revalidate a Workspace-scoped Scenario."""
        scenario = await uow.repository.get_scenario(
            workspace_id,
            scenario_id,
            for_update=for_update,
        )
        if scenario is None or scenario.workspace_id != workspace_id:
            raise InterviewResourceNotFound("interview_scenario")
        return scenario

    async def _session(
        self,
        uow: InterviewUnitOfWork,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        for_update: bool = False,
    ) -> InterviewSession:
        """@brief Workspace-first 读取并二次校验 Session / Read and revalidate a Workspace-scoped Session."""
        session = await uow.repository.get_session(
            workspace_id,
            session_id,
            for_update=for_update,
        )
        if session is None or session.workspace_id != workspace_id:
            raise InterviewResourceNotFound("interview_session")
        return session

    def _queued_record(
        self,
        session: InterviewSession,
        job: Job,
        actor_id: UserId,
        at: datetime,
    ) -> InterviewJobQueuedRecord:
        """@brief 构造固定白名单 outbox 记录 / Build a fixed-allowlist outbox record."""
        return InterviewJobQueuedRecord(
            id=InterviewOutboxId(self._ids("outbox")),
            workspace_id=session.workspace_id,
            actor_id=actor_id,
            session_ref=_session_ref(session),
            job_ref=ResourceRef("job", job.meta.id, job.meta.revision),
            occurred_at=at,
        )

    def _audit(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        action: str,
        target: ResourceRef,
        context: InterviewMutationContext,
        at: datetime,
    ) -> AuditEvent:
        """@brief 构造统一 AuditEvent / Build a unified AuditEvent."""
        return AuditEvent(
            id=AuditEventId(self._ids("audit")),
            workspace_id=workspace_id,
            occurred_at=at,
            actor=ResourceRef("user", principal.user_id),
            action=action,
            target=target,
            outcome=AuditOutcome.ALLOWED,
            request_id=context.request_id,
        )


class InterviewWorkerService:
    """@brief 在短事务之间隔离 media/report 网络 I/O 的 worker / Worker isolating media/report network I/O between short transactions."""

    def __init__(
        self,
        uow_factory: InterviewUnitOfWorkFactory,
        media_finalizer: InterviewMediaFinalizer,
        report_provider: InterviewReportProvider,
        *,
        service_actor: ResourceRef,
        clock: Clock | None = None,
        id_factory: OpaqueIdFactory | None = None,
        maximum_report_segments: int = 20_000,
    ) -> None:
        """@brief 注入 Ports、真实 service actor 与有界 Transcript / Inject ports, a real service actor, and a bounded Transcript.

        @param service_actor 与 persistence scope 完全一致的服务身份 / Service identity exactly matching the persistence scope.
        """
        if not 1 <= maximum_report_segments <= 100_000:
            raise ValueError("maximum report segments must be 1 to 100000")
        if service_actor.resource_type != "service" or service_actor.revision is not None:
            raise ValueError("Interview worker actor must be an unversioned service ref")
        self._uow_factory = uow_factory
        self._media_finalizer = media_finalizer
        self._report_provider = report_provider
        self._clock = clock or UtcClock()
        self._ids = id_factory or NewOpaqueIdFactory()
        self._service_actor = service_actor
        self._maximum_report_segments = maximum_report_segments

    async def execute_queued_job(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        job_id: JobId,
        *,
        attempt_count: int,
        maximum_attempts: int,
    ) -> None:
        """@brief 按持久 Job kind 幂等分派统一 outbox 工作 / Idempotently dispatch unified-outbox work by persisted Job kind.

        @param workspace_id claim 的精确 Workspace / Exact Workspace from the claim.
        @param session_id 严格验证后的 payload Session / Strictly validated payload Session.
        @param job_id 同时绑定 subject 与 payload 的统一 Job / Unified Job bound to both subject
            and payload.
        @param attempt_count 包含本次的 outbox 尝试次数 / Outbox attempt number including this run.
        @param maximum_attempts 与通用 dispatcher 相同的硬上限 / Hard cap shared with the generic
            dispatcher.
        @note 终态失败 Job 的重放是成功 no-op；瞬态 Port 失败保留 ``running``，仅在最后一次
            尝试原子写入领域失败。/ Replays of terminally failed Jobs are successful no-ops.
            Transient port failures retain ``running`` and are durably failed only on the last try.
        """
        if (
            isinstance(attempt_count, bool)
            or isinstance(maximum_attempts, bool)
            or not 1 <= attempt_count <= maximum_attempts <= 100
        ):
            raise ValueError("Interview worker attempt bounds are invalid")
        async with self._uow_factory() as probe:
            job = await _worker_job(probe, workspace_id, job_id, for_update=False)
            if (
                job.subject.resource_type != "interview_session"
                or job.subject.id != session_id
            ):
                raise InterviewPortProtocolError(
                    "interview.job_binding_mismatch",
                    "the persisted Interview Job is not bound to the claimed Session",
                )
            await probe.commit()

        if job.status.is_terminal and job.status is not JobStatus.SUCCEEDED:
            return
        try:
            if job.kind == INTERVIEW_END_JOB_KIND:
                await self.execute_end_job(workspace_id, session_id, job_id)
            elif job.kind == INTERVIEW_REPORT_JOB_KIND:
                await self.execute_report_job(workspace_id, session_id, job_id)
            else:
                raise InterviewPortProtocolError(
                    "interview.job_kind_unsupported",
                    "the persisted Interview Job kind is unsupported",
                )
        except InterviewWorkerError:
            # The concrete executor already committed a terminal Job/domain failure. Completing the
            # outbox row is the idempotent acknowledgement; retrying cannot turn that Job successful.
            return
        except InterviewWorkerRetry:
            if attempt_count < maximum_attempts:
                raise
            await self._fail_exhausted_job(workspace_id, session_id, job_id)
        except InterviewConflict as error:
            if await self._job_is_terminal(workspace_id, job_id):
                return
            if attempt_count >= maximum_attempts:
                await self._fail_exhausted_job(workspace_id, session_id, job_id)
                return
            raise InterviewWorkerRetry(
                "interview.worker_race",
                "Interview Job changed while the worker was executing",
            ) from error

    async def fail_exhausted(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
    ) -> None:
        """@brief 仅按可信 outbox header 闭合 Interview Job / Close an Interview Job using trusted outbox headers only.

        @param workspace_id outbox 独立列中的 Workspace / Workspace from the outbox's dedicated column.
        @param actor_id outbox 独立列中的原始 Job creator / Original Job creator from the
            outbox's dedicated column.
        @param job_id outbox subject 的统一 Job ID / Unified Job ID from the outbox subject.
        @note payload 可能损坏，Session identity 只从 creator 匹配的持久 Job subject 读取。
            无法定位或已终态是幂等成功。/ The payload may be malformed, so Session identity is
            read only from the persisted Job subject whose creator matches. Missing or terminal
            work is an idempotent success.
        """
        async with self._uow_factory() as probe:
            job = await probe.jobs.get_owned(
                workspace_id,
                actor_id,
                job_id,
                for_update=False,
            )
            if job is None or job.status.is_terminal:
                await probe.commit()
                return
            if (
                job.kind not in {INTERVIEW_END_JOB_KIND, INTERVIEW_REPORT_JOB_KIND}
                or job.subject.resource_type != "interview_session"
            ):
                await probe.commit()
                return
            session_id = InterviewSessionId(job.subject.id)
            await probe.commit()
        await self._fail_from_exhausted_header(
            workspace_id,
            actor_id,
            session_id,
            job_id,
        )

    async def _fail_from_exhausted_header(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        session_id: InterviewSessionId,
        job_id: JobId,
    ) -> None:
        """@brief 以 aggregate→Job 锁顺序原子记录耗尽 / Atomically record exhaustion with aggregate-before-Job locking.

        @param workspace_id 精确 Workspace / Exact Workspace.
        @param actor_id 持久 Job creator / Persisted Job creator.
        @param session_id 从持久 Job subject 取得的 Session / Session read from the persisted Job subject.
        @param job_id 已耗尽 Job / Exhausted Job.
        """
        failed_at = self._clock.now()
        try:
            async with self._uow_factory() as uow:
                session = await uow.repository.get_session(
                    workspace_id,
                    session_id,
                    for_update=True,
                )
                job = await uow.jobs.get_owned(
                    workspace_id,
                    actor_id,
                    job_id,
                    for_update=True,
                )
                if job is None or job.status.is_terminal:
                    await uow.commit()
                    return
                if (
                    job.kind not in {INTERVIEW_END_JOB_KIND, INTERVIEW_REPORT_JOB_KIND}
                    or job.subject.resource_type != "interview_session"
                    or job.subject.id != session_id
                ):
                    await uow.commit()
                    return
                problem = _worker_problem(
                    job.meta.id,
                    "interview.worker_attempts_exhausted",
                    "Interview worker retries exhausted",
                    "The Interview operation could not be completed after bounded retries.",
                )
                running_job = (
                    job.start(at=failed_at)
                    if job.status is JobStatus.QUEUED
                    else job
                )
                if running_job is not job:
                    await uow.jobs.save(
                        running_job,
                        expected_revision=job.meta.revision,
                    )
                failed_job = running_job.fail(problem, at=failed_at)
                target = ResourceRef(
                    "job",
                    failed_job.meta.id,
                    failed_job.meta.revision,
                )
                action = "interview.job.fail"
                if (
                    job.kind == INTERVIEW_END_JOB_KIND
                    and session is not None
                    and session.view.status is InterviewSessionStatus.ENDING
                    and session.pending_end_job_id == job.meta.id
                ):
                    failed_session = session.fail_end(at=failed_at)
                    validate_interview_job_alignment(failed_session, failed_job)
                    await uow.repository.save_session(
                        failed_session,
                        expected_revision=session.meta.revision,
                    )
                    action = "interview_session.end.fail"
                    target = _session_ref(failed_session)
                elif job.kind == INTERVIEW_REPORT_JOB_KIND:
                    action = "interview_report.generate.fail"
                await uow.jobs.save(
                    failed_job,
                    expected_revision=running_job.meta.revision,
                )
                await uow.audit.add(
                    _service_audit(
                        self._ids,
                        self._service_actor,
                        workspace_id,
                        action,
                        target,
                        failed_job.meta.id,
                        failed_at,
                        AuditOutcome.FAILED,
                    )
                )
                await uow.commit()
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview.worker_race",
                "the Interview Job changed while outbox exhaustion was recorded",
            ) from error

    async def execute_end_job(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        job_id: JobId,
    ) -> InterviewSessionView:
        """@brief 以两段短事务 finalize Session 与统一 Artifact / Finalize a Session and unified Artifacts with two short transactions."""
        started_at = self._clock.now()
        try:
            async with self._uow_factory() as first:
                session = await _worker_session(
                    first,
                    workspace_id,
                    session_id,
                    for_update=True,
                )
                job = await _worker_job(first, workspace_id, job_id, for_update=True)
                if (
                    job.kind != INTERVIEW_END_JOB_KIND
                    or job.subject.resource_type != "interview_session"
                    or job.subject.id != session_id
                ):
                    raise InterviewPortProtocolError(
                        "interview_end.job_binding_mismatch",
                        "the end Job is not bound to the Session",
                    )
                if job.status is JobStatus.SUCCEEDED:
                    validate_interview_job_alignment(session, job)
                    await first.commit()
                    return session.view
                if job.status.is_terminal:
                    raise InterviewConflict(
                        "interview_end.job_terminal",
                        "the end Job is already terminal without a successful Session",
                    )
                if session.pending_end_job_id != job_id:
                    raise InterviewPortProtocolError(
                        "interview_end.job_binding_mismatch",
                        "the live end Job is not pending on the Session",
                    )
                validate_interview_job_alignment(session, job)
                if job.status is JobStatus.QUEUED:
                    started_job = job.start(
                        at=started_at,
                        progress=JobProgress(
                            phase="media_finalize",
                            completed=0,
                            total=None,
                            unit=JobProgressUnit.STEPS,
                        ),
                    )
                    await first.jobs.save(
                        started_job,
                        expected_revision=job.meta.revision,
                    )
                else:
                    started_job = job
                await first.commit()
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview_end.worker_race",
                "the end Job changed before execution started",
            ) from error

        try:
            output = await self._media_finalizer.finalize(
                session,
                operation_id=_worker_operation_id(started_job),
            )
            validate_artifacts_for_session(output.artifacts, session)
        except InterviewWorkerPortFailure as error:
            if error.retryable:
                raise InterviewWorkerRetry(
                    error.code,
                    "Interview media finalization is temporarily unavailable",
                ) from error
            await self._fail_end_job(
                session,
                started_job,
                failure_code=error.code,
            )
            raise InterviewWorkerError(
                error.code,
                "Interview Session finalization failed",
            ) from error
        except Exception as error:
            await self._fail_end_job(session, started_job)
            raise InterviewWorkerError(
                "interview_end.finalization_failed",
                "Interview Session finalization failed",
            ) from error

        finished_at = self._clock.now()
        try:
            async with self._uow_factory() as second:
                current = await _worker_session(
                    second,
                    workspace_id,
                    session_id,
                    for_update=True,
                )
                current_job = await _worker_job(second, workspace_id, job_id, for_update=True)
                if (
                    current.meta.revision != session.meta.revision
                    or current_job.meta.revision != started_job.meta.revision
                ):
                    raise InterviewConflict(
                        "interview_end.worker_race",
                        "the Session or Job changed during media finalization",
                    )
                validate_interview_job_alignment(current, current_job)
                validate_artifacts_for_session(output.artifacts, current)
                for artifact, content in zip(
                    output.artifacts,
                    output.contents,
                    strict=True,
                ):
                    await second.artifacts.add(artifact, content)
                completed = current.finish_end(at=finished_at)
                completed_job = current_job.succeed(
                    [
                        ResourceRef("artifact", artifact.meta.id, artifact.meta.revision)
                        for artifact in output.artifacts
                    ],
                    at=finished_at,
                )
                validate_interview_job_alignment(completed, completed_job)
                await second.repository.save_session(
                    completed,
                    expected_revision=current.meta.revision,
                )
                await second.jobs.save(
                    completed_job,
                    expected_revision=current_job.meta.revision,
                )
                await second.audit.add(
                    _service_audit(
                        self._ids,
                        self._service_actor,
                        workspace_id,
                        "interview_session.end.complete",
                        _session_ref(completed),
                        completed_job.meta.id,
                        finished_at,
                        AuditOutcome.ALLOWED,
                    )
                )
                await second.commit()
                return completed.view
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview_end.worker_race",
                "the Session or Job changed while finalization was committed",
            ) from error

    async def execute_report_job(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        job_id: JobId,
    ) -> InterviewReport:
        """@brief 用冻结 Rubric 和一致 Transcript 生成 immutable Report / Generate an immutable Report from a frozen Rubric and consistent Transcript."""
        started_at = self._clock.now()
        try:
            async with self._uow_factory() as first:
                session = await _worker_session(
                    first,
                    workspace_id,
                    session_id,
                    for_update=True,
                )
                if session.view.status is not InterviewSessionStatus.COMPLETED:
                    raise InterviewConflict(
                        "interview_report.session_not_completed",
                        "Report generation requires a completed Session",
                    )
                job = await _worker_job(first, workspace_id, job_id, for_update=True)
                _validate_report_job_binding(session, job)
                if job.status is JobStatus.SUCCEEDED:
                    report_id = session.view.report_id
                    if report_id is None or not any(
                        result.resource_type == "interview_report"
                        and result.id == report_id
                        for result in job.result_refs
                    ):
                        raise InterviewPortProtocolError(
                            "interview_report.result_binding_mismatch",
                            "the succeeded Report Job is not bound to its Report",
                        )
                    existing = await first.repository.get_report(workspace_id, report_id)
                    if existing is None or existing.session_id != session_id:
                        raise InterviewPortProtocolError(
                            "interview_report.result_missing",
                            "the succeeded Report Job has no immutable Report",
                        )
                    await first.commit()
                    return existing
                if job.status.is_terminal:
                    raise InterviewConflict(
                        "interview_report.job_terminal",
                        "the Report Job is already terminal without a Report",
                    )
                if session.view.report_id is not None:
                    raise InterviewPortProtocolError(
                        "interview_report.state_mismatch",
                        "a live Report Job cannot target a Session with a Report",
                    )
                _validate_report_job(session, job)
                transcript = await first.repository.load_transcript_snapshot(
                    workspace_id,
                    session_id,
                    maximum_segments=self._maximum_report_segments,
                )
                _validate_transcript_snapshot(transcript, workspace_id, session_id)
                if job.status is JobStatus.QUEUED:
                    started_job = job.start(
                        at=started_at,
                        progress=JobProgress(
                            phase="report_generation",
                            completed=0,
                            total=None,
                            unit=JobProgressUnit.STEPS,
                        ),
                    )
                    await first.jobs.save(
                        started_job,
                        expected_revision=job.meta.revision,
                    )
                else:
                    started_job = job
                request = ReportGenerationRequest(
                    session_id=session_id,
                    locale=session.spec.locale,
                    job_target=session.spec.job_target,
                    rubric=session.spec.rubric_snapshot,
                    transcript=transcript,
                )
                await first.commit()
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview_report.worker_race",
                "the Report Job changed before execution started",
            ) from error

        try:
            draft = await self._report_provider.generate(
                request,
                operation_id=_worker_operation_id(started_job),
            )
            draft.validate_against(
                request.rubric,
                request.transcript,
                request.session_id,
            )
        except InterviewWorkerPortFailure as error:
            if error.retryable:
                raise InterviewWorkerRetry(
                    error.code,
                    "Interview Report generation is temporarily unavailable",
                ) from error
            await self._fail_report_job(
                workspace_id,
                started_job,
                failure_code=error.code,
            )
            raise InterviewWorkerError(
                error.code,
                "Interview Report generation failed",
            ) from error
        except Exception as error:
            await self._fail_report_job(workspace_id, started_job)
            raise InterviewWorkerError(
                "interview_report.generation_failed",
                "Interview Report generation failed",
            ) from error

        generated_at = self._clock.now()
        try:
            async with self._uow_factory() as second:
                current = await _worker_session(
                    second,
                    workspace_id,
                    session_id,
                    for_update=True,
                )
                current_job = await _worker_job(second, workspace_id, job_id, for_update=True)
                if (
                    current.meta.revision != session.meta.revision
                    or current_job.meta.revision != started_job.meta.revision
                ):
                    raise InterviewConflict(
                        "interview_report.worker_race",
                        "the Session or Job changed during Report generation",
                    )
                _validate_report_job(current, current_job)
                draft.validate_against(
                    current.spec.rubric_snapshot,
                    transcript,
                    current.meta.id,
                )
                report = InterviewReport(
                    meta=ResourceMeta(
                        InterviewReportId(self._ids("report")),
                        1,
                        generated_at,
                        generated_at,
                    ),
                    workspace_id=workspace_id,
                    session_id=session_id,
                    draft=draft,
                    generated_at=generated_at,
                )
                completed = current.attach_report(report.meta.id, at=generated_at)
                completed_job = current_job.succeed(
                    [ResourceRef("interview_report", report.meta.id, report.meta.revision)],
                    at=generated_at,
                )
                await second.repository.add_report(report)
                await second.repository.save_session(
                    completed,
                    expected_revision=current.meta.revision,
                )
                await second.jobs.save(
                    completed_job,
                    expected_revision=current_job.meta.revision,
                )
                await second.audit.add(
                    _service_audit(
                        self._ids,
                        self._service_actor,
                        workspace_id,
                        "interview_report.generate",
                        ResourceRef("interview_report", report.meta.id, 1),
                        completed_job.meta.id,
                        generated_at,
                        AuditOutcome.ALLOWED,
                    )
                )
                await second.commit()
                return report
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview_report.worker_race",
                "the Session or Job changed while the Report was committed",
            ) from error

    async def _job_is_terminal(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
    ) -> bool:
        """@brief 在竞争后重新读取 Job 终态 / Re-read Job terminal state after a race.

        @param workspace_id 精确 Workspace / Exact Workspace.
        @param job_id 统一 Job / Unified Job.
        @return Job 是否已经终态 / Whether the Job is already terminal.
        """
        async with self._uow_factory() as uow:
            job = await _worker_job(uow, workspace_id, job_id, for_update=False)
            await uow.commit()
            return job.status.is_terminal

    async def _fail_exhausted_job(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        job_id: JobId,
    ) -> None:
        """@brief 最后一次重试原子终结 Session/Job / Atomically terminate Session/Job on the final retry.

        @param workspace_id 精确 Workspace / Exact Workspace.
        @param session_id claim 绑定的 Session / Session bound by the claim.
        @param job_id 已耗尽重试的统一 Job / Unified Job whose retries are exhausted.
        """
        failed_at = self._clock.now()
        try:
            async with self._uow_factory() as uow:
                session = await _worker_session(
                    uow,
                    workspace_id,
                    session_id,
                    for_update=True,
                )
                job = await _worker_job(uow, workspace_id, job_id, for_update=True)
                if job.status.is_terminal:
                    await uow.commit()
                    return
                if (
                    job.subject.resource_type != "interview_session"
                    or job.subject.id != session_id
                ):
                    raise InterviewPortProtocolError(
                        "interview.job_binding_mismatch",
                        "the exhausted Job is not bound to the claimed Session",
                    )
                if job.kind == INTERVIEW_END_JOB_KIND:
                    if session.pending_end_job_id != job.meta.id:
                        raise InterviewPortProtocolError(
                            "interview_end.job_binding_mismatch",
                            "the exhausted end Job is not pending on the Session",
                        )
                    validate_interview_job_alignment(session, job)
                elif job.kind == INTERVIEW_REPORT_JOB_KIND:
                    _validate_report_job(session, job)
                else:
                    raise InterviewPortProtocolError(
                        "interview.job_kind_unsupported",
                        "the exhausted Interview Job kind is unsupported",
                    )
                problem = _worker_problem(
                    job.meta.id,
                    "interview.worker_attempts_exhausted",
                    "Interview worker retries exhausted",
                    "The Interview operation could not be completed after bounded retries.",
                )
                running_job = job.start(at=failed_at) if job.status is JobStatus.QUEUED else job
                if running_job is not job:
                    await uow.jobs.save(
                        running_job,
                        expected_revision=job.meta.revision,
                    )
                failed_job = running_job.fail(problem, at=failed_at)
                if job.kind == INTERVIEW_END_JOB_KIND:
                    failed_session = session.fail_end(at=failed_at)
                    validate_interview_job_alignment(failed_session, failed_job)
                    await uow.repository.save_session(
                        failed_session,
                        expected_revision=session.meta.revision,
                    )
                    action = "interview_session.end.fail"
                    target = _session_ref(failed_session)
                elif job.kind == INTERVIEW_REPORT_JOB_KIND:
                    action = "interview_report.generate.fail"
                    target = ResourceRef(
                        "job",
                        failed_job.meta.id,
                        failed_job.meta.revision,
                    )
                await uow.jobs.save(
                    failed_job,
                    expected_revision=running_job.meta.revision,
                )
                await uow.audit.add(
                    _service_audit(
                        self._ids,
                        self._service_actor,
                        workspace_id,
                        action,
                        target,
                        failed_job.meta.id,
                        failed_at,
                        AuditOutcome.FAILED,
                    )
                )
                await uow.commit()
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview.worker_race",
                "the Interview Job changed while retry exhaustion was recorded",
            ) from error

    async def _fail_end_job(
        self,
        started_session: InterviewSession,
        started_job: Job,
        *,
        failure_code: str = "interview_end.finalization_failed",
    ) -> None:
        """@brief 不泄漏 provider 错误地持久化 end 失败 / Durably fail an end Job without leaking provider errors."""
        failed_at = self._clock.now()
        problem = _worker_problem(
            started_job.meta.id,
            failure_code,
            "Interview finalization failed",
            "The Interview Session could not be finalized.",
        )
        try:
            async with self._uow_factory() as uow:
                session = await _worker_session(
                    uow,
                    started_session.workspace_id,
                    started_session.meta.id,
                    for_update=True,
                )
                job = await _worker_job(
                    uow,
                    started_session.workspace_id,
                    started_job.meta.id,
                    for_update=True,
                )
                if (
                    session.meta.revision != started_session.meta.revision
                    or job.meta.revision != started_job.meta.revision
                ):
                    raise InterviewConflict(
                        "interview_end.worker_race",
                        "the Session or Job changed before failure was recorded",
                    )
                failed_session = session.fail_end(at=failed_at)
                failed_job = job.fail(problem, at=failed_at)
                validate_interview_job_alignment(failed_session, failed_job)
                await uow.repository.save_session(
                    failed_session,
                    expected_revision=session.meta.revision,
                )
                await uow.jobs.save(failed_job, expected_revision=job.meta.revision)
                await uow.audit.add(
                    _service_audit(
                        self._ids,
                        self._service_actor,
                        session.workspace_id,
                        "interview_session.end.fail",
                        _session_ref(failed_session),
                        failed_job.meta.id,
                        failed_at,
                        AuditOutcome.FAILED,
                    )
                )
                await uow.commit()
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview_end.worker_race",
                "the Session or Job changed while failure was recorded",
            ) from error

    async def _fail_report_job(
        self,
        workspace_id: WorkspaceId,
        started_job: Job,
        *,
        failure_code: str = "interview_report.generation_failed",
    ) -> None:
        """@brief 不泄漏 provider 错误地持久化 Report 失败 / Durably fail a Report Job without leaking provider errors."""
        failed_at = self._clock.now()
        problem = _worker_problem(
            started_job.meta.id,
            failure_code,
            "Interview Report generation failed",
            "The Interview Report could not be generated.",
        )
        try:
            async with self._uow_factory() as uow:
                job = await _worker_job(
                    uow,
                    workspace_id,
                    started_job.meta.id,
                    for_update=True,
                )
                if job.meta.revision != started_job.meta.revision:
                    raise InterviewConflict(
                        "interview_report.worker_race",
                        "the Report Job changed before failure was recorded",
                    )
                failed_job = job.fail(problem, at=failed_at)
                await uow.jobs.save(failed_job, expected_revision=job.meta.revision)
                await uow.audit.add(
                    _service_audit(
                        self._ids,
                        self._service_actor,
                        workspace_id,
                        "interview_report.generate.fail",
                        ResourceRef("job", failed_job.meta.id, failed_job.meta.revision),
                        failed_job.meta.id,
                        failed_at,
                        AuditOutcome.FAILED,
                    )
                )
                await uow.commit()
        except InterviewCasMismatch as error:
            raise InterviewConflict(
                "interview_report.worker_race",
                "the Report Job changed while failure was recorded",
            ) from error


async def _worker_session(
    uow: InterviewUnitOfWork,
    workspace_id: WorkspaceId,
    session_id: InterviewSessionId,
    *,
    for_update: bool,
) -> InterviewSession:
    """@brief Worker 读取精确 Workspace Session / Read an exact Workspace Session for a worker."""
    session = await uow.repository.get_session(
        workspace_id,
        session_id,
        for_update=for_update,
    )
    if session is None or session.workspace_id != workspace_id:
        raise InterviewResourceNotFound("interview_session")
    return session


async def _worker_job(
    uow: InterviewUnitOfWork,
    workspace_id: WorkspaceId,
    job_id: JobId,
    *,
    for_update: bool,
) -> Job:
    """@brief Worker 读取精确统一 Job / Read an exact unified Job for a worker."""
    job = await uow.jobs.get(workspace_id, job_id, for_update=for_update)
    if job is None or job.workspace_id != workspace_id:
        raise InterviewPortProtocolError(
            "interview.job_protocol_violation",
            "the Interview aggregate is missing its unified Job",
        )
    return job


def _validate_report_job(session: InterviewSession, job: Job) -> None:
    """@brief 校验 Report Job 与 Session 绑定 / Validate Report-Job binding to a Session."""
    _validate_report_job_binding(session, job)
    if job.status not in {JobStatus.QUEUED, JobStatus.RUNNING}:
        raise InterviewConflict(
            "interview_report.job_terminal",
            "the Report Job is already terminal",
        )


def _validate_report_job_binding(session: InterviewSession, job: Job) -> None:
    """@brief 校验 Report Job 的稳定 Workspace/Session identity / Validate stable Workspace/Session identity for a Report Job."""
    if (
        job.workspace_id != session.workspace_id
        or job.kind != INTERVIEW_REPORT_JOB_KIND
        or job.subject.resource_type != "interview_session"
        or job.subject.id != session.meta.id
    ):
        raise InterviewPortProtocolError(
            "interview_report.job_binding_mismatch",
            "the Report Job is not bound to the Session",
        )


def _validate_connection_grant(
    connection: RealtimeConnection,
    workspace_id: WorkspaceId,
    session_id: InterviewSessionId,
    audience: ResourceRef,
    spec: CreateRealtimeConnectionSpec,
    issued_at: datetime,
) -> None:
    """@brief 校验 signaling adapter 返回的单 Session grant / Validate a single-Session grant returned by signaling."""
    if (
        connection.workspace_id != workspace_id
        or connection.session_id != session_id
        or connection.audience != audience
        or connection.issued_at != issued_at
        or connection.transport not in spec.supported_transports
    ):
        raise InterviewPortProtocolError(
            "realtime_connection.protocol_violation",
            "realtime gateway returned a mismatched grant",
        )


def _validate_scenario_page(
    page: InterviewPage[InterviewScenario],
    workspace_id: WorkspaceId,
) -> None:
    """@brief 校验 Scenario page 租户与稳定顺序 / Validate Scenario-page tenancy and stable order."""
    if any(item.workspace_id != workspace_id for item in page.items):
        raise InterviewPortProtocolError(
            "interview.repository_scope_violation",
            "Scenario repository returned a cross-Workspace item",
        )
    positions = tuple((item.meta.created_at, item.meta.id) for item in page.items)
    if positions != tuple(sorted(positions)):
        raise InterviewPortProtocolError(
            "interview.repository_order_violation",
            "Scenario repository returned unstable ordering",
        )


def _validate_session_page(
    page: InterviewPage[InterviewSession],
    workspace_id: WorkspaceId,
) -> None:
    """@brief 校验 Session page 租户与稳定顺序 / Validate Session-page tenancy and stable order."""
    if any(item.workspace_id != workspace_id for item in page.items):
        raise InterviewPortProtocolError(
            "interview.repository_scope_violation",
            "Session repository returned a cross-Workspace item",
        )
    positions = tuple((item.meta.created_at, item.meta.id) for item in page.items)
    if positions != tuple(sorted(positions)):
        raise InterviewPortProtocolError(
            "interview.repository_order_violation",
            "Session repository returned unstable ordering",
        )


def _validate_transcript(
    page: InterviewPage[TranscriptSegment],
    workspace_id: WorkspaceId,
    session_id: InterviewSessionId,
) -> None:
    """@brief 校验 Transcript page 边界与严格顺序 / Validate Transcript-page boundaries and strict ordering."""
    _validate_transcript_snapshot(page.items, workspace_id, session_id)


def _validate_transcript_snapshot(
    segments: tuple[TranscriptSegment, ...],
    workspace_id: WorkspaceId,
    session_id: InterviewSessionId,
) -> None:
    """@brief 校验有界 Transcript 快照 / Validate a bounded Transcript snapshot."""
    if any(
        item.workspace_id != workspace_id or item.session_id != session_id
        for item in segments
    ):
        raise InterviewPortProtocolError(
            "interview_transcript.scope_violation",
            "Transcript repository returned a cross-Session segment",
        )
    positions = tuple((item.sequence, item.id) for item in segments)
    sequences = tuple(item.sequence for item in segments)
    identifiers = tuple(item.id for item in segments)
    if (
        len(set(sequences)) != len(sequences)
        or len(set(identifiers)) != len(identifiers)
        or positions != tuple(sorted(positions))
    ):
        raise InterviewPortProtocolError(
            "interview_transcript.order_violation",
            "Transcript repository returned duplicate or unstable sequence ordering",
        )


def _workspace_ref(workspace_id: WorkspaceId) -> ResourceRef:
    """@brief 构造 Workspace target / Build a Workspace target."""
    return ResourceRef("workspace", workspace_id)


def _scenario_ref(scenario: InterviewScenario) -> ResourceRef:
    """@brief 构造带 revision 的 Scenario target / Build a revision-bearing Scenario target."""
    return ResourceRef("interview_scenario", scenario.meta.id, scenario.meta.revision)


def _session_ref(session: InterviewSession) -> ResourceRef:
    """@brief 构造带 revision 的 Session target / Build a revision-bearing Session target."""
    return ResourceRef("interview_session", session.meta.id, session.meta.revision)


def _require_revision(actual: int, expected: int) -> None:
    """@brief 校验强 If-Match revision / Validate a strong If-Match revision."""
    if expected < 1 or actual != expected:
        raise InterviewPreconditionFailed


def _worker_operation_id(job: Job) -> InterviewWorkerOperationId:
    """@brief 从不可变 Job identity 构造 provider 幂等键 / Build a provider idempotency key from immutable Job identity.

    @param job 已严格绑定的 Interview Job / Strictly bound Interview Job.
    @return 同一 Job 每次 outbox 重放都相同的 operation ID / Operation ID identical across
        every outbox replay of the same Job.
    """
    if job.kind not in {INTERVIEW_END_JOB_KIND, INTERVIEW_REPORT_JOB_KIND}:
        raise InterviewPortProtocolError(
            "interview.job_kind_unsupported",
            "cannot derive an operation identity for an unsupported Job kind",
        )
    return InterviewWorkerOperationId(f"{job.kind}:{job.meta.id}")


def _worker_problem(
    job_id: JobId,
    code: str,
    title: str,
    detail: str,
    *,
    retryable: bool = False,
) -> ProblemDetails:
    """@brief 构造不泄漏 provider 数据的 worker ProblemDetails / Build worker ProblemDetails without provider-data leakage.

    @param retryable 终态 Problem 是否仍建议重新发起新操作 / Whether the terminal Problem
        recommends initiating a new operation.
    """
    return ProblemDetails(
        type_uri=f"https://api.hmalliances.org:8022/problems/interview/{code.replace('.', '-')}",
        title=title,
        status=500,
        code=code,
        request_id=job_id,
        retryable=retryable,
        detail=detail,
    )


def _service_audit(
    ids: OpaqueIdFactory,
    service_actor: ResourceRef,
    workspace_id: WorkspaceId,
    action: str,
    target: ResourceRef,
    job_id: JobId,
    at: datetime,
    outcome: AuditOutcome,
) -> AuditEvent:
    """@brief 构造 worker 服务 actor 审计事件 / Build a worker service-actor audit event.

    @param service_actor 部署显式配置的真实服务身份 / Real service identity explicitly configured by the deployment.
    """
    return AuditEvent(
        id=AuditEventId(ids("audit")),
        workspace_id=workspace_id,
        occurred_at=at,
        actor=service_actor,
        action=action,
        target=target,
        outcome=outcome,
        request_id=job_id,
    )


__all__ = [
    "V2_INTERVIEW_ENDPOINT_METHODS",
    "Clock",
    "CreateInterviewReportJobCommand",
    "CreateInterviewScenarioCommand",
    "CreateInterviewSessionCommand",
    "EndInterviewSessionCommand",
    "InterviewApplicationError",
    "InterviewApplicationService",
    "InterviewConflict",
    "InterviewMutationContext",
    "InterviewPortProtocolError",
    "InterviewPreconditionFailed",
    "InterviewResourceNotFound",
    "InterviewWorkerError",
    "InterviewWorkerRetry",
    "InterviewWorkerService",
    "InvalidInterviewCommand",
    "NewOpaqueIdFactory",
    "OpaqueIdFactory",
    "UtcClock",
]
