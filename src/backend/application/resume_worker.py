"""@brief 可恢复的 API V2 Resume Job worker / Recoverable API V2 Resume-job worker.

每次执行先用短事务把 queued Job 推进到 running 并冻结输入，然后在事务外完成
import/render 转换，最后用第二个短事务 CAS 写结果与终态。重复 outbox 投递只观察同一
持久 Job；已经终态的 Job 会被幂等确认。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Protocol, assert_never

from backend.application.ports.outbox_dispatch import (
    OutboxDispatchClaim,
    OutboxHandlerFailure,
)
from backend.application.ports.resume_worker import (
    RenderedResumeArtifact,
    ResumeCapabilityFailure,
    ResumeImportCapability,
    ResumeImportedContent,
    ResumeImportSource,
    ResumeRenderCapability,
    ResumeWorkerUnitOfWork,
    ResumeWorkerUnitOfWorkFactory,
)
from backend.application.resumes import Clock, UtcClock
from backend.domain.platform import (
    Job,
    JobId,
    JobProgress,
    JobProgressUnit,
    JobStatus,
    ProblemDetails,
)
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resume_jobs import (
    ResumeImportSpec,
    ResumeJobKind,
    ResumeJobSpec,
    ResumeRenderSpec,
    ResumeRestoreSpec,
)
from backend.domain.resumes import (
    ChangeTarget,
    ResumeAggregate,
    ResumeDomainError,
    ResumeId,
    ResumeProfile,
    ResumeRevision,
    RevisionChange,
    RichText,
    TemplatePolicy,
    create_resume_document,
)

RESUME_WORK_EVENT_TYPES = frozenset({"resume.job_created"})
"""@brief Resume dispatcher 独占的工作事件 / Work events exclusively owned by the Resume dispatcher."""

_EVENT_PAYLOAD_FIELDS = frozenset({"actor_id", "subject", "data"})
"""@brief Resume 工作事件 envelope 的封闭字段集 / Closed Resume-work event envelope fields."""

_SUBJECT_FIELDS = frozenset({"resource_type", "id", "revision"})
"""@brief payload subject 的封闭字段集 / Closed payload-subject fields."""


class ResumeWorkerRetry(RuntimeError):
    """@brief 可由相同稳定 operation ID 重试的 worker 失败 / Retryable worker failure for the same stable operation ID."""

    code: str
    """@brief dispatcher 可持久的稳定 code / Stable code persistable by the dispatcher."""

    def __init__(self, code: str) -> None:
        """@brief 初始化脱敏 retry / Initialize a redacted retry.

        @param code 稳定错误码 / Stable error code.
        """
        super().__init__(code)
        self.code = code


class ResumeJobExecutionClaim(Protocol):
    """@brief worker 所需的 durable event 最小投影 / Minimal durable-event projection required by the worker."""

    @property
    def workspace_id(self) -> WorkspaceId:
        """@brief 返回事件 Workspace / Return the event Workspace."""

    @property
    def actor_id(self) -> UserId:
        """@brief 返回创建 Job 的真实 actor / Return the real actor that created the Job."""

    @property
    def job_id(self) -> JobId:
        """@brief 返回统一 Job ID / Return the unified Job identifier."""

    @property
    def attempt_count(self) -> int:
        """@brief 返回包含本次的 outbox 尝试数 / Return the outbox attempt including this run."""

    @property
    def maximum_attempts(self) -> int:
        """@brief 返回 worker failure budget / Return the worker failure budget."""


@dataclass(frozen=True, slots=True)
class _ResumeExecutionClaim:
    """@brief handler 交给 worker 的已验证 claim / Validated claim passed from the handler to the worker."""

    workspace_id: WorkspaceId
    actor_id: UserId
    job_id: JobId
    attempt_count: int
    maximum_attempts: int


@dataclass(frozen=True, slots=True)
class _ImportPreparation:
    """@brief import 第一阶段冻结输入 / Import input frozen by phase one."""

    job: Job
    spec: ResumeImportSpec
    source: ResumeImportSource
    policy: TemplatePolicy


@dataclass(frozen=True, slots=True)
class _RestorePreparation:
    """@brief restore 第一阶段冻结输入 / Restore input frozen by phase one."""

    job: Job
    spec: ResumeRestoreSpec
    current: ResumeAggregate
    source: ResumeRevision
    policy: TemplatePolicy


@dataclass(frozen=True, slots=True)
class _RenderPreparation:
    """@brief render 第一阶段冻结输入 / Render input frozen by phase one."""

    job: Job
    spec: ResumeRenderSpec
    revision: ResumeRevision


type _Preparation = _ImportPreparation | _RestorePreparation | _RenderPreparation
"""@brief Resume Job 第一阶段结果穷尽 union / Exhaustive union of Resume-job preparations."""


class ResumeJobWorkerService:
    """@brief 按持久 kind 分派且可 crash-resume 的 Resume worker / Crash-resumable Resume worker dispatching by persisted kind."""

    def __init__(
        self,
        uow_factory: ResumeWorkerUnitOfWorkFactory,
        importer: ResumeImportCapability,
        renderer: ResumeRenderCapability,
        *,
        clock: Clock | None = None,
    ) -> None:
        """@brief 注入短事务与真实转换能力 / Inject short transactions and real conversion capabilities.

        @param uow_factory durable claim 身份 scope 的 UoW factory / UoW factory scoped by durable-claim identity.
        @param importer 安全 import converter / Safe import converter.
        @param renderer 多格式 renderer / Multi-format renderer.
        @param clock 可测试 UTC 时钟 / Testable UTC clock.
        """
        self._uow_factory = uow_factory
        self._importer = importer
        self._renderer = renderer
        self._clock = clock or UtcClock()

    async def execute(self, dispatch: ResumeJobExecutionClaim) -> Job | None:
        """@brief 幂等执行一个持久 Resume Job / Idempotently execute one persisted Resume Job.

        @param dispatch 已验证 durable event claim / Validated durable-event claim.
        @return 当前终态 Job；不存在时为空 / Current terminal Job, or absence when not found.
        @raise ResumeWorkerRetry failure budget 尚未耗尽的瞬态失败 / A transient failure while budget remains.
        """
        preparation = await self._prepare(dispatch)
        if isinstance(preparation, Job) or preparation is None:
            return preparation
        operation_id = _operation_id(preparation.job)
        try:
            match preparation:
                case _ImportPreparation():
                    converted = await self._importer.import_resume(
                        dispatch.workspace_id,
                        preparation.source,
                        operation_id=operation_id,
                    )
                    output: object = _import_document(preparation, converted, dispatch.actor_id)
                case _RestorePreparation():
                    output = _restored_aggregate(preparation, dispatch.actor_id)
                case _RenderPreparation():
                    output = await self._renderer.render_resume(
                        preparation.revision.document,
                        preparation.spec.formats,
                        operation_id=operation_id,
                    )
        except ResumeCapabilityFailure as error:
            if error.retryable and dispatch.attempt_count < dispatch.maximum_attempts:
                raise ResumeWorkerRetry(error.code) from error
            return await self._fail_job(dispatch, error.code, retryable=error.retryable)
        except ResumeDomainError as error:
            return await self._fail_job(dispatch, error.code, retryable=False)
        except Exception as error:
            if dispatch.attempt_count < dispatch.maximum_attempts:
                raise ResumeWorkerRetry("resume.worker_failed") from error
            return await self._fail_job(dispatch, "resume.worker_failed", retryable=False)
        try:
            return await self._complete(dispatch, preparation, output, operation_id)
        except ResumeCapabilityFailure as error:
            if error.retryable and dispatch.attempt_count < dispatch.maximum_attempts:
                raise ResumeWorkerRetry(error.code) from error
            return await self._fail_job(dispatch, error.code, retryable=error.retryable)
        except ResumeWorkerRetry:
            if dispatch.attempt_count < dispatch.maximum_attempts:
                raise
            return await self._fail_job(dispatch, "resume.worker_race", retryable=False)
        except Exception as error:
            if dispatch.attempt_count < dispatch.maximum_attempts:
                raise ResumeWorkerRetry("resume.worker_commit_failed") from error
            return await self._fail_job(dispatch, "resume.worker_commit_failed", retryable=False)

    async def fail_exhausted(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
    ) -> Job | None:
        """@brief 幂等闭合 payload 不可信的耗尽 Job / Idempotently close an exhausted Job whose payload is untrusted.

        @param workspace_id outbox 独立列中的可信 Workspace / Trusted Workspace from the
            outbox's dedicated column.
        @param actor_id outbox 独立列中的原始 Job creator / Original Job creator from the
            outbox's dedicated column.
        @param job_id outbox subject 中的统一 Job ID / Unified Job ID from the outbox subject.
        @return 当前失败或既有终态 Job；无法定位时为空 / Current failed or pre-existing
            terminal Job, or absence when the trusted subject cannot be located.
        @note 本方法不读取 outbox payload；事务失败会向上传播，使 dispatcher 保留
            ``processing`` 供租约恢复。/ This method never reads the outbox payload; transaction
            failures propagate so the dispatcher retains ``processing`` for lease recovery.
        """
        return await self._fail_job_by_identity(
            workspace_id,
            actor_id,
            job_id,
            "resume.worker_attempts_exhausted",
            retryable=False,
        )

    async def _prepare(
        self,
        dispatch: ResumeJobExecutionClaim,
    ) -> _Preparation | Job | None:
        """@brief 第一短事务锁定 Job、推进 running 并冻结输入 / Lock/start the Job and freeze input in the first short transaction."""
        async with self._uow_factory(dispatch.workspace_id, dispatch.actor_id) as uow:
            persisted = await uow.worker_jobs.get(
                dispatch.workspace_id,
                dispatch.job_id,
                for_update=True,
            )
            if persisted is None:
                await uow.commit()
                return None
            job = persisted.job
            if job.status.is_terminal:
                await uow.commit()
                return job
            if persisted.spec is None:
                return await _fail_inside(
                    uow,
                    job,
                    persisted.spec_error or "resume.job_spec_invalid",
                    self._clock.now(),
                )
            spec = persisted.spec
            binding_error = _job_binding_error(job, spec)
            if binding_error is not None:
                return await _fail_inside(uow, job, binding_error, self._clock.now())
            if job.status is JobStatus.QUEUED:
                started = job.start(
                    at=self._clock.now(),
                    progress=JobProgress(
                        phase=_initial_phase(spec),
                        completed=0,
                        total=1,
                        unit=JobProgressUnit.STEPS,
                    ),
                )
                await uow.worker_jobs.save(started, expected_revision=job.meta.revision)
            elif job.status is JobStatus.RUNNING:
                started = job
            else:
                return await _fail_inside(
                    uow,
                    job,
                    "resume.job_state_invalid",
                    self._clock.now(),
                )
            match spec:
                case ResumeImportSpec():
                    policy = await uow.templates.get_policy(spec.template)
                    source = await uow.worker_jobs.get_import_source(
                        dispatch.workspace_id,
                        spec.upload_session_id,
                        job.meta.id,
                    )
                    if policy is None or source is None:
                        code = (
                            "resume.template_not_found"
                            if policy is None
                            else "resume.import_source_unavailable"
                        )
                        return await _fail_inside(uow, started, code, self._clock.now())
                    preparation: _Preparation = _ImportPreparation(started, spec, source, policy)
                case ResumeRestoreSpec():
                    current = await uow.repository.get_resume(
                        dispatch.workspace_id,
                        spec.resume_id,
                    )
                    source_revision = await uow.repository.get_revision(
                        dispatch.workspace_id,
                        spec.resume_id,
                        spec.source_revision,
                    )
                    if current is None or source_revision is None:
                        return await _fail_inside(
                            uow,
                            started,
                            "resume.restore_source_unavailable",
                            self._clock.now(),
                        )
                    if job.subject.revision != current.document.meta.revision:
                        return await _fail_inside(
                            uow,
                            started,
                            "resume.restore_target_changed",
                            self._clock.now(),
                        )
                    policy = await uow.templates.get_policy(source_revision.document.template)
                    if policy is None:
                        return await _fail_inside(
                            uow,
                            started,
                            "resume.template_not_found",
                            self._clock.now(),
                        )
                    preparation = _RestorePreparation(
                        started,
                        spec,
                        current,
                        source_revision,
                        policy,
                    )
                case ResumeRenderSpec():
                    revision = await uow.repository.get_revision(
                        dispatch.workspace_id,
                        spec.resume_id,
                        spec.resume_revision,
                    )
                    if revision is None:
                        return await _fail_inside(
                            uow,
                            started,
                            "resume.render_source_unavailable",
                            self._clock.now(),
                        )
                    policy = await uow.templates.get_policy(revision.document.template)
                    if policy is None:
                        return await _fail_inside(
                            uow,
                            started,
                            "resume.template_not_found",
                            self._clock.now(),
                        )
                    try:
                        policy.validate(
                            revision.document,
                            output_formats=tuple(item.value for item in spec.formats),
                        )
                    except ResumeDomainError as error:
                        return await _fail_inside(uow, started, error.code, self._clock.now())
                    preparation = _RenderPreparation(started, spec, revision)
            await uow.commit()
            return preparation

    async def _complete(
        self,
        dispatch: ResumeJobExecutionClaim,
        preparation: _Preparation,
        output: object,
        operation_id: str,
    ) -> Job:
        """@brief 第二短事务原子写领域结果与 Job 终态 / Atomically write domain results and the terminal Job in phase two."""
        async with self._uow_factory(dispatch.workspace_id, dispatch.actor_id) as uow:
            persisted = await uow.worker_jobs.get(
                dispatch.workspace_id,
                dispatch.job_id,
                for_update=True,
            )
            if persisted is None:
                raise ResumeWorkerRetry("resume.job_disappeared")
            current_job = persisted.job
            if current_job.status.is_terminal:
                await uow.commit()
                return current_job
            if (
                current_job.status is not JobStatus.RUNNING
                or current_job.meta.revision != preparation.job.meta.revision
                or current_job.kind != preparation.job.kind
            ):
                raise ResumeWorkerRetry("resume.worker_race")
            finished_at = self._clock.now()
            result_refs: tuple[ResourceRef, ...]
            match preparation:
                case _ImportPreparation():
                    aggregate, revision = _expect_import_output(output)
                    await uow.repository.add_resume(aggregate, revision)
                    result_refs = (
                        ResourceRef("resume", str(aggregate.document.meta.id), 1),
                    )
                case _RestorePreparation():
                    aggregate, revision = _expect_restore_output(output)
                    current = await uow.repository.get_resume(
                        dispatch.workspace_id,
                        preparation.spec.resume_id,
                        for_update=True,
                    )
                    if (
                        current is None
                        or current.document.meta.revision
                        != preparation.current.document.meta.revision
                    ):
                        raise ResumeWorkerRetry("resume.restore_target_changed")
                    await uow.repository.save_resume(
                        aggregate,
                        revision,
                        expected_revision=preparation.current.document.meta.revision,
                    )
                    result_refs = (
                        ResourceRef(
                            "resume",
                            str(aggregate.document.meta.id),
                            aggregate.document.meta.revision,
                        ),
                    )
                case _RenderPreparation():
                    artifacts = _expect_render_output(output)
                    result_refs = await uow.worker_results.add_render_results(
                        current_job,
                        preparation.revision,
                        artifacts,
                        operation_id=operation_id,
                        created_at=finished_at,
                    )
            succeeded = current_job.succeed(
                result_refs,
                at=finished_at,
                progress=JobProgress(
                    phase="completed",
                    completed=1,
                    total=1,
                    unit=JobProgressUnit.STEPS,
                ),
            )
            await uow.worker_jobs.save(
                succeeded,
                expected_revision=current_job.meta.revision,
            )
            await uow.commit()
            return succeeded

    async def _fail_job(
        self,
        dispatch: ResumeJobExecutionClaim,
        code: str,
        *,
        retryable: bool,
    ) -> Job | None:
        """@brief 用独立短事务持久化确定性或耗尽失败 / Persist deterministic or exhausted failure in an independent short transaction."""
        return await self._fail_job_by_identity(
            dispatch.workspace_id,
            dispatch.actor_id,
            dispatch.job_id,
            code,
            retryable=retryable,
        )

    async def _fail_job_by_identity(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
        code: str,
        *,
        retryable: bool,
    ) -> Job | None:
        """@brief 仅按可信 identity 在短事务中失败 Job / Fail a Job in a short transaction using trusted identity only.

        @param workspace_id Job Workspace / Job Workspace.
        @param actor_id Job creator / Job creator.
        @param job_id 统一 Job ID / Unified Job ID.
        @param code 稳定公开错误码 / Stable public-safe error code.
        @param retryable 客户端是否可重试 / Whether the client may retry.
        @return 当前终态或新失败 Job；不存在时为空 / Current terminal or newly failed Job,
            or absence when not found.
        """
        async with self._uow_factory(workspace_id, actor_id) as uow:
            persisted = await uow.worker_jobs.get(
                workspace_id,
                job_id,
                for_update=True,
            )
            if persisted is None:
                await uow.commit()
                return None
            if persisted.job.status.is_terminal:
                await uow.commit()
                return persisted.job
            return await _fail_inside(
                uow,
                persisted.job,
                code,
                self._clock.now(),
                retryable=retryable,
            )


class ResumeJobOutboxHandler:
    """@brief 把统一 outbox claim 适配为 Resume worker claim / Adapt unified outbox claims to Resume worker claims."""

    def __init__(self, worker: ResumeJobWorkerService, *, maximum_attempts: int) -> None:
        """@brief 绑定 lifespan-owned worker 与一致 failure budget / Bind the worker and matching failure budget."""
        if isinstance(maximum_attempts, bool) or not 1 <= maximum_attempts <= 100:
            raise ValueError("Resume worker maximum attempts must be between 1 and 100")
        self._worker = worker
        self._maximum_attempts = maximum_attempts

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 校验事件绑定后执行持久 Job / Validate event binding and execute the persisted Job."""
        dispatch = _execution_claim(claim, self._maximum_attempts)
        try:
            await self._worker.execute(dispatch)
        except ResumeWorkerRetry as error:
            raise OutboxHandlerFailure(error.code) from error

    async def on_exhausted(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
    ) -> None:
        """@brief 在 source outbox failed 前闭合 Resume Job / Close the Resume Job before the source outbox fails.

        @param claim 仍持有租约的最后一次 claim / Final-attempt claim whose lease is still held.
        @param error_code dispatcher 即将记录的脱敏错误码 / Redacted error code the dispatcher
            is about to persist.
        @note payload 可能正是失败原因，故补偿仅使用独立 header 列。无法定位的 subject
            是幂等 no-op。/ The payload may be the failure source, so compensation uses only
            dedicated header columns. An unlocatable subject is an idempotent no-op.
        """
        del error_code
        if (
            claim.event_type not in RESUME_WORK_EVENT_TYPES
            or claim.subject.resource_type != "job"
        ):
            return
        await self._worker.fail_exhausted(
            claim.workspace_id,
            claim.actor_id,
            JobId(claim.subject.id),
        )


async def _fail_inside(
    uow: ResumeWorkerUnitOfWork,
    job: Job,
    code: str,
    at: datetime,
    *,
    retryable: bool = False,
) -> Job:
    """@brief 在当前短事务把非终态 Job 推进 failed / Transition a non-terminal Job to failed in the current short transaction."""
    running = job
    if job.status is JobStatus.QUEUED:
        running = job.start(
            at=at,
            progress=JobProgress(
                phase="validation",
                completed=0,
                total=1,
                unit=JobProgressUnit.STEPS,
            ),
        )
        await uow.worker_jobs.save(running, expected_revision=job.meta.revision)
    if running.status is not JobStatus.RUNNING:
        raise ResumeWorkerRetry("resume.job_state_invalid")
    failed = running.fail(
        _worker_problem(running.meta.id, code, retryable=retryable),
        at=at,
        progress=JobProgress(
            phase="failed",
            completed=0,
            total=1,
            unit=JobProgressUnit.STEPS,
        ),
    )
    await uow.worker_jobs.save(failed, expected_revision=running.meta.revision)
    await uow.commit()
    return failed


def _execution_claim(
    claim: OutboxDispatchClaim,
    maximum_attempts: int,
) -> _ResumeExecutionClaim:
    """@brief 防御性解析 Resume Job 事件 / Defensively parse a Resume-job event."""
    payload = claim.payload
    subject = payload.get("subject")
    data = payload.get("data")
    if (
        claim.event_type not in RESUME_WORK_EVENT_TYPES
        or claim.subject.resource_type != "job"
        or claim.subject.revision != 1
        or frozenset(payload) != _EVENT_PAYLOAD_FIELDS
        or not isinstance(subject, Mapping)
        or frozenset(subject) != _SUBJECT_FIELDS
        or not isinstance(data, Mapping)
        or frozenset(data) != {"kind"}
    ):
        raise OutboxHandlerFailure("resume.job_event_invalid")
    if (
        payload.get("actor_id") != claim.actor_id
        or subject.get("resource_type") != "job"
        or subject.get("id") != claim.subject.id
        or subject.get("revision") != 1
        or not isinstance(data.get("kind"), str)
    ):
        raise OutboxHandlerFailure("resume.job_event_invalid")
    return _ResumeExecutionClaim(
        claim.workspace_id,
        claim.actor_id,
        JobId(claim.subject.id),
        claim.attempt_count,
        maximum_attempts,
    )


def _job_binding_error(job: Job, spec: ResumeJobSpec) -> str | None:
    """@brief 校验 persisted kind/spec/subject 穷尽绑定 / Validate exhaustive persisted kind/spec/subject binding."""
    match job.kind, spec:
        case ResumeJobKind.IMPORT.value, ResumeImportSpec():
            valid = (
                job.subject.resource_type == "upload_session"
                and job.subject.id == spec.upload_session_id
                and job.subject.revision is None
            )
        case ResumeJobKind.RESTORE.value, ResumeRestoreSpec():
            valid = (
                job.subject.resource_type == "resume"
                and job.subject.id == spec.resume_id
                and job.subject.revision is not None
            )
        case ResumeJobKind.RENDER.value, ResumeRenderSpec():
            valid = (
                job.subject.resource_type == "resume"
                and job.subject.id == spec.resume_id
                and job.subject.revision == spec.resume_revision
            )
        case _:
            return "resume.job_kind_unsupported"
    return None if valid else "resume.job_binding_mismatch"


def _initial_phase(spec: ResumeJobSpec) -> str:
    """@brief 返回 Job kind 的稳定首阶段 / Return the stable initial phase for a Job kind."""
    match spec:
        case ResumeImportSpec():
            return "document_import"
        case ResumeRestoreSpec():
            return "revision_restore"
        case ResumeRenderSpec():
            return "document_render"
        case _ as unreachable:
            assert_never(unreachable)


def _operation_id(job: Job) -> str:
    """@brief 从不可变 persisted kind+ID 构造幂等键 / Build an idempotency key from immutable persisted kind and ID."""
    if job.kind not in {item.value for item in ResumeJobKind}:
        raise ResumeDomainError("resume.job_kind_unsupported", "unsupported Resume Job kind")
    return f"{job.kind}:{job.meta.id}"


def _import_document(
    preparation: _ImportPreparation,
    content: ResumeImportedContent,
    actor_id: UserId,
) -> tuple[ResumeAggregate, ResumeRevision]:
    """@brief 将 converter 输出构造成有效 revision=1 SIR / Build a valid revision-one SIR from converter output."""
    created_at = preparation.job.started_at
    if created_at is None:
        raise ResumeDomainError("resume.job_state_invalid", "import Job lacks started_at")
    resume_id = _derived_resume_id(_operation_id(preparation.job))
    document = create_resume_document(
        resume_id=resume_id,
        workspace_id=preparation.job.workspace_id,
        title=preparation.spec.title,
        locale=preparation.spec.locale,
        template_policy=preparation.policy,
        created_at=created_at,
        full_name=content.full_name,
    )
    profile = ResumeProfile(
        full_name=document.profile.full_name,
        headline=document.profile.headline,
        summary=RichText(content.plain_text),
        contacts=document.profile.contacts,
    )
    document = replace(document, profile=profile)
    preparation.policy.validate(document)
    return ResumeAggregate.create(document, actor_id)


def _restored_aggregate(
    preparation: _RestorePreparation,
    actor_id: UserId,
) -> tuple[ResumeAggregate, ResumeRevision]:
    """@brief 从 immutable snapshot 构造下一 revision / Build the next revision from an immutable snapshot."""
    at = preparation.job.started_at
    if at is None:
        raise ResumeDomainError("resume.job_state_invalid", "restore Job lacks started_at")
    current = preparation.current
    restored = replace(
        preparation.source.document,
        meta=current.document.meta.advance(at),
        workspace_id=current.document.workspace_id,
    )
    preparation.policy.validate(restored)
    change = RevisionChange(
        restored.meta.revision,
        frozenset({ChangeTarget(str(restored.meta.id))}),
    )
    aggregate = ResumeAggregate(
        restored,
        current.operation_ledger,
        (*current.revision_changes, change),
    )
    return aggregate, ResumeRevision(
        restored.meta.id,
        restored.meta.revision,
        at,
        actor_id,
        restored,
    )


def _derived_resume_id(operation_id: str) -> ResumeId:
    """@brief 从稳定 operation ID 派生 import Resume ID / Derive an import Resume ID from a stable operation ID."""
    import hashlib

    digest = hashlib.sha256(f"aiws:v2:resume-import:{operation_id}".encode()).hexdigest()[:32]
    return ResumeId(f"resume_{digest}")


def _worker_problem(job_id: JobId, code: str, *, retryable: bool) -> ProblemDetails:
    """@brief 构造不泄漏异常正文的 Job problem / Build a Job problem without exception text."""
    return ProblemDetails(
        type_uri=f"https://api.hmalliances.org:8022/problems/resume/{code.replace('.', '-')}",
        title="Resume job failed",
        status=500,
        code=code,
        request_id=str(job_id),
        retryable=retryable,
        detail="The Resume job could not be completed.",
    )


def _expect_import_output(value: object) -> tuple[ResumeAggregate, ResumeRevision]:
    """@brief 防御性验证 import 内部输出 / Defensively validate an internal import output."""
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or not isinstance(value[0], ResumeAggregate)
        or not isinstance(value[1], ResumeRevision)
    ):
        raise TypeError("Resume importer produced an invalid internal output")
    return value


def _expect_restore_output(value: object) -> tuple[ResumeAggregate, ResumeRevision]:
    """@brief 防御性验证 restore 内部输出 / Defensively validate an internal restore output."""
    return _expect_import_output(value)


def _expect_render_output(value: object) -> tuple[RenderedResumeArtifact, ...]:
    """@brief 防御性验证 render 内部输出 / Defensively validate an internal render output."""
    if not isinstance(value, tuple) or not value or not all(
        isinstance(item, RenderedResumeArtifact) for item in value
    ):
        raise TypeError("Resume renderer produced an invalid internal output")
    return value


__all__ = [
    "RESUME_WORK_EVENT_TYPES",
    "ResumeJobOutboxHandler",
    "ResumeJobWorkerService",
    "ResumeWorkerRetry",
]
