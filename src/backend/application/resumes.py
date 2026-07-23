"""@brief API v2 Resume 与 proposal 应用用例 / API v2 Resume and proposal use cases."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from backend.application.ports.resumes import (
    CollectionPage,
    OperationBatchReceipt,
    PageRequest,
    ResumeCasMismatch,
    ResumeRepository,
    ResumeUnitOfWork,
    ResumeUnitOfWorkFactory,
)
from backend.domain.platform import Job, JobId
from backend.domain.principals import (
    ResourceMeta,
    TokenPrincipal,
    UserId,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.resume_jobs import (
    RenderFormat,
    RenderMode,
    ResumeImportSpec,
    ResumeJobKind,
    ResumeOutboxEvent,
    ResumeRenderSpec,
    ResumeRestoreSpec,
)
from backend.domain.resume_proposals import (
    ProposalDecision,
    ProposalDecisionCommand,
    ResumeProposal,
)
from backend.domain.resumes import (
    ConflictStrategy,
    JsonValue,
    RenderHint,
    ResourceRef,
    ResumeAggregate,
    ResumeBatchId,
    ResumeBatchKeyReused,
    ResumeDocument,
    ResumeDomainError,
    ResumeId,
    ResumeOperation,
    ResumeOperationBatch,
    ResumeOperationOutcome,
    ResumeProposalId,
    ResumeRevision,
    ResumeRevisionSummary,
    ResumeSummary,
    SetResumeTemplate,
    TemplatePolicy,
    TemplateRef,
    clone_resume_document,
    create_resume_document,
)
from workspace_shared.ids import new_opaque_id


class Clock(Protocol):
    """@brief 应用层可替换时钟 / Replaceable application clock."""

    def now(self) -> datetime:
        """@brief 返回带时区当前时刻 / Return the timezone-aware current instant.

        @return 带时区时间 / Timezone-aware instant.
        """


class UtcClock:
    """@brief 生产 UTC 时钟 / Production UTC clock."""

    def now(self) -> datetime:
        """@brief 返回 UTC 当前时刻 / Return the current UTC instant.

        @return UTC 当前时刻 / Current UTC instant.
        """
        return datetime.now(UTC)


class ResumeApplicationError(Exception):
    """@brief 可映射为稳定 API problem 的应用错误 / Application error mappable to a stable API problem."""

    code: str
    """@brief 稳定错误码 / Stable error code."""

    detail: str
    """@brief 可公开错误说明 / Public-safe error detail."""

    def __init__(self, code: str, detail: str) -> None:
        """@brief 初始化应用错误 / Initialize an application error.

        @param code 稳定错误码 / Stable error code.
        @param detail 可公开说明 / Public-safe detail.
        """
        super().__init__(detail)
        self.code = code
        self.detail = detail


class ResumeResourceNotFound(ResumeApplicationError):
    """@brief 资源不存在或不可向调用者暴露 / Resource is absent or must not be disclosed."""

    def __init__(self, resource: str) -> None:
        """@brief 创建防枚举的 404 错误 / Create an enumeration-safe not-found error.

        @param resource 稳定资源类型 / Stable resource kind.
        """
        super().__init__(f"{resource}.not_found", f"{resource} was not found")


class ResumePreconditionFailed(ResumeApplicationError):
    """@brief 强 ETag 对应的 revision 已过期 / Revision represented by a strong ETag is stale."""

    def __init__(self) -> None:
        """@brief 创建统一预条件失败 / Create the uniform precondition failure."""
        super().__init__(
            "http.precondition_failed",
            "resource revision precondition failed",
        )


class InvalidResumeCommand(ResumeApplicationError):
    """@brief 应用边界拒绝空或矛盾命令 / Application boundary rejects empty or contradictory commands."""


@dataclass(frozen=True, slots=True)
class CreateResumeCommand:
    """@brief CreateResumeRequest 的类型化形式 / Typed form of CreateResumeRequest."""

    title: str
    locale: str
    template: TemplateRef
    clone_from_resume_id: ResumeId | None = None


@dataclass(frozen=True, slots=True)
class UpdateResumeMetadataCommand:
    """@brief UpdateResumeMetadataRequest 的 merge-patch 形式 / Merge-patch form of UpdateResumeMetadataRequest."""

    title: str | None = None
    locale: str | None = None

    def __post_init__(self) -> None:
        """@brief 拒绝空 PATCH / Reject an empty PATCH.

        @raise InvalidResumeCommand 两个字段均缺失时抛出 / Raised when both fields are absent.
        """
        if self.title is None and self.locale is None:
            raise InvalidResumeCommand(
                "resume.patch_empty",
                "resume metadata patch must contain a field",
            )


@dataclass(frozen=True, slots=True)
class CreateResumeImportJobCommand:
    """@brief CreateResumeImportJobRequest 的类型化形式 / Typed form of CreateResumeImportJobRequest."""

    upload_session_id: str
    title: str
    locale: str
    template: TemplateRef


@dataclass(frozen=True, slots=True)
class CreateRestoreJobCommand:
    """@brief CreateRestoreJobRequest 的类型化形式 / Typed form of CreateRestoreJobRequest."""

    source_revision: int

    def __post_init__(self) -> None:
        """@brief 校验来源 revision / Validate the source revision.

        @raise InvalidResumeCommand revision 非正整数时抛出 / Raised unless revision is positive.
        """
        if self.source_revision < 1:
            raise InvalidResumeCommand(
                "resume.invalid_restore_request",
                "source revision must be positive",
            )


@dataclass(frozen=True, slots=True)
class CreateRenderJobCommand:
    """@brief CreateRenderJobRequest 的类型化形式 / Typed form of CreateRenderJobRequest."""

    resume_revision: int
    mode: RenderMode
    formats: tuple[RenderFormat, ...]

    def __post_init__(self) -> None:
        """@brief 校验渲染请求 / Validate the render request.

        @raise InvalidResumeCommand revision 或 formats 无效时抛出 / Raised for invalid render requests.
        """
        if self.resume_revision < 1 or not self.formats:
            raise InvalidResumeCommand(
                "resume.invalid_render_request",
                "render revision and formats are required",
            )
        if len(set(self.formats)) != len(self.formats):
            raise InvalidResumeCommand(
                "resume.invalid_render_request",
                "render formats must be unique",
            )


class ResumeApplicationService:
    """@brief Resume、revision、Job 与 proposal 的原子应用协调器 / Atomic Resume application coordinator.

    @param uow_factory 每个用例的工作单元工厂 / Unit-of-work factory per use case.
    @param clock 可测试时钟 / Testable clock.
    @param id_factory 可测试不透明 ID 工厂 / Testable opaque-ID factory.
    """

    def __init__(
        self,
        uow_factory: ResumeUnitOfWorkFactory,
        *,
        clock: Clock | None = None,
        id_factory: Callable[[str], str] = new_opaque_id,
    ) -> None:
        """@brief 组装 Resume v2 应用服务 / Assemble the Resume v2 application service.

        @param uow_factory Resume 工作单元工厂 / Resume unit-of-work factory.
        @param clock 可选时钟 / Optional clock.
        @param id_factory 不透明 ID 工厂 / Opaque-ID factory.
        """
        self._uow_factory = uow_factory
        self._clock = clock or UtcClock()
        self._id_factory = id_factory

    async def list_resumes(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        page: PageRequest | None = None,
    ) -> CollectionPage[ResumeSummary]:
        """@brief 列出路径 Workspace 中的 Resume / List Resumes in the path Workspace.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param page keyset 分页 / Keyset page request.
        @return Resume 摘要分页 / Resume-summary page.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.LIST_RESUMES,
            )
            return await uow.repository.list_resumes(workspace_id, page or PageRequest())

    async def create_resume(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateResumeCommand,
    ) -> ResumeDocument:
        """@brief 原子创建 Resume、revision 与 outbox 事件 / Atomically create Resume, revision, and outbox event.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param command 创建命令 / Create command.
        @return 已提交 Resume / Committed Resume.
        """
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_RESUME,
            )
            policy = await self._require_template(uow, command.template)
            now = self._clock.now()
            resume_id = ResumeId(self._id_factory("resume"))
            if command.clone_from_resume_id is None:
                document = create_resume_document(
                    resume_id=resume_id,
                    workspace_id=workspace_id,
                    title=command.title,
                    locale=command.locale,
                    template_policy=policy,
                    created_at=now,
                )
            else:
                source = await self._require_resume(
                    uow.repository,
                    workspace_id,
                    command.clone_from_resume_id,
                )
                document = clone_resume_document(
                    source.document,
                    resume_id=resume_id,
                    workspace_id=workspace_id,
                    title=command.title,
                    locale=command.locale,
                    template_policy=policy,
                    created_at=now,
                )
            aggregate, revision = ResumeAggregate.create(
                document,
                context.actor.user_id,
            )
            await uow.repository.add_resume(aggregate, revision)
            await self._emit(
                uow,
                context,
                "resume.created",
                ResourceRef("resume", str(resume_id), 1),
                {"revision": 1},
                now,
            )
            await uow.commit()
            return document

    async def get_resume(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
    ) -> ResumeDocument:
        """@brief 读取路径 Workspace 内的权威 Resume / Read the authoritative Resume in the path Workspace.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @return Resume SIR / Resume SIR.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.READ_RESUME,
            )
            return (
                await self._require_resume(uow.repository, workspace_id, resume_id)
            ).document

    async def update_resume_metadata(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        command: UpdateResumeMetadataCommand,
        *,
        expected_revision: int,
    ) -> ResumeDocument:
        """@brief 通过 CAS 修改 Resume metadata 并写 revision / Update Resume metadata via CAS and write a revision.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param command merge-patch 命令 / Merge-patch command.
        @param expected_revision If-Match 解出的 revision / Revision decoded from If-Match.
        @return 已提交 Resume / Committed Resume.
        """
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.UPDATE_RESUME,
            )
            aggregate = await self._require_resume(
                uow.repository,
                workspace_id,
                resume_id,
                for_update=True,
            )
            self._require_revision(aggregate.document.meta.revision, expected_revision)
            now = self._clock.now()
            change = aggregate.update_metadata(
                title=command.title,
                locale=command.locale,
                at=now,
                actor_id=context.actor.user_id,
            )
            policy = await self._require_template(uow, change.aggregate.document.template)
            policy.validate(change.aggregate.document)
            if change.revision is None:
                raise AssertionError("metadata update must produce a revision")
            await self._save_resume(
                uow,
                change.aggregate,
                change.revision,
                expected_revision=expected_revision,
            )
            await self._emit(
                uow,
                context,
                "resume.metadata_updated",
                ResourceRef(
                    "resume",
                    str(resume_id),
                    change.aggregate.document.meta.revision,
                ),
                {"revision": change.aggregate.document.meta.revision},
                now,
            )
            await uow.commit()
            return change.aggregate.document

    async def delete_resume(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 使用 CAS 删除 Workspace Resume / Delete a Workspace Resume via CAS.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param expected_revision If-Match revision / If-Match revision.
        """
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.DELETE_RESUME,
            )
            aggregate = await self._require_resume(
                uow.repository,
                workspace_id,
                resume_id,
                for_update=True,
            )
            self._require_revision(aggregate.document.meta.revision, expected_revision)
            try:
                await uow.repository.delete_resume(
                    workspace_id,
                    resume_id,
                    expected_revision=expected_revision,
                )
            except ResumeCasMismatch as error:
                raise ResumePreconditionFailed from error
            now = self._clock.now()
            await self._emit(
                uow,
                context,
                "resume.deleted",
                ResourceRef("resume", str(resume_id), expected_revision),
                {"revision": expected_revision},
                now,
            )
            await uow.commit()

    async def list_revisions(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        page: PageRequest | None = None,
    ) -> CollectionPage[ResumeRevisionSummary]:
        """@brief 列出 Resume revision 摘要 / List Resume revision summaries."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.READ_RESUME_REVISIONS,
            )
            await self._require_resume(uow.repository, workspace_id, resume_id)
            return await uow.repository.list_revisions(
                workspace_id,
                resume_id,
                page or PageRequest(),
            )

    async def get_revision(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        revision: int,
    ) -> ResumeRevision:
        """@brief 读取不可变 Resume revision / Read an immutable Resume revision."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.READ_RESUME_REVISIONS,
            )
            item = await uow.repository.get_revision(
                workspace_id,
                resume_id,
                revision,
            )
            if item is None:
                raise ResumeResourceNotFound("resume_revision")
            return item

    async def apply_operations(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        batch: ResumeOperationBatch,
        *,
        expected_revision: int,
    ) -> ResumeOperationOutcome:
        """@brief 在单事务内去重、CAS、写 revision、Job 与 outbox / Deduplicate, CAS, and write revision, job, and outbox atomically.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param batch 离线 operation batch / Offline operation batch.
        @param expected_revision 当前资源强 ETag revision / Current strong-ETag revision.
        @return 可重放的 ResumeOperationResult 领域投影 / Replayable ResumeOperationResult projection.
        """
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.APPLY_RESUME_OPERATIONS,
            )
            if batch.render_hint is not RenderHint.NONE:
                await self._authorize(
                    uow,
                    principal,
                    workspace_id,
                    WorkspaceAction.CREATE_RESUME_RENDER_JOB,
                )
            aggregate = await self._require_resume(
                uow.repository,
                workspace_id,
                resume_id,
                for_update=True,
            )
            receipt = await uow.repository.get_batch_receipt(
                workspace_id,
                resume_id,
                batch.client_batch_id,
            )
            fingerprint = batch.fingerprint()
            if receipt is not None:
                if receipt.request_fingerprint != fingerprint:
                    raise ResumeBatchKeyReused
                return receipt.outcome
            self._require_revision(aggregate.document.meta.revision, expected_revision)
            policies = await self._load_operation_policies(
                uow,
                aggregate,
                batch.operations,
            )
            now = self._clock.now()
            change = aggregate.apply_batch(
                batch,
                at=now,
                actor_id=context.actor.user_id,
                template_policies=policies,
            )
            if change.revision is not None:
                await self._save_resume(
                    uow,
                    change.aggregate,
                    change.revision,
                    expected_revision=aggregate.document.meta.revision,
                )
            render_job_ref = await self._create_hint_job(
                uow,
                context,
                workspace_id,
                change.aggregate.document,
                batch.render_hint,
                now,
            )
            outcome = ResumeOperationOutcome(
                change.aggregate.document,
                change.applied_operation_ids,
                (),
                render_job_ref,
            )
            await uow.repository.add_batch_receipt(
                OperationBatchReceipt(
                    workspace_id,
                    resume_id,
                    batch.client_batch_id,
                    fingerprint,
                    outcome,
                    now,
                )
            )
            await self._emit(
                uow,
                context,
                "resume.operations_applied",
                ResourceRef(
                    "resume",
                    str(resume_id),
                    change.aggregate.document.meta.revision,
                ),
                {
                    "revision": change.aggregate.document.meta.revision,
                    "operation_count": len(batch.operations),
                },
                now,
            )
            await uow.commit()
            return outcome

    async def create_import_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateResumeImportJobCommand,
    ) -> Job:
        """@brief 创建不相信文档转换的异步 import Job / Create an asynchronous untrusted-document import job."""
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_RESUME_IMPORT_JOB,
            )
            policy = await self._require_template(uow, command.template)
            if command.locale not in policy.supported_locales:
                raise ResumeDomainError(
                    "resume.template_incompatible",
                    "template does not support the requested locale",
                )
            now = self._clock.now()
            spec = ResumeImportSpec(
                command.upload_session_id,
                command.title,
                command.locale,
                command.template,
            )
            job = self._job(
                workspace_id,
                ResumeJobKind.IMPORT,
                ResourceRef("upload_session", command.upload_session_id),
                now,
            )
            if not await uow.import_sources.claim(
                workspace_id,
                command.upload_session_id,
                job.meta.id,
            ):
                raise ResumeResourceNotFound("upload_session")
            await uow.jobs.add(job, spec)
            await self._emit_job_created(uow, context, job, now)
            await uow.commit()
            return job

    async def create_restore_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        command: CreateRestoreJobCommand,
        *,
        expected_revision: int,
    ) -> Job:
        """@brief 校验快照后创建异步 restore Job / Create an asynchronous restore job after validating its snapshot."""
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_RESUME_RESTORE_JOB,
            )
            aggregate = await self._require_resume(
                uow.repository,
                workspace_id,
                resume_id,
            )
            self._require_revision(aggregate.document.meta.revision, expected_revision)
            revision = await uow.repository.get_revision(
                workspace_id,
                resume_id,
                command.source_revision,
            )
            if revision is None:
                raise ResumeResourceNotFound("resume_revision")
            now = self._clock.now()
            spec = ResumeRestoreSpec(resume_id, command.source_revision)
            job = self._job(
                workspace_id,
                ResumeJobKind.RESTORE,
                ResourceRef(
                    "resume",
                    str(resume_id),
                    aggregate.document.meta.revision,
                ),
                now,
            )
            await uow.jobs.add(job, spec)
            await self._emit_job_created(uow, context, job, now)
            await uow.commit()
            return job

    async def create_render_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        command: CreateRenderJobCommand,
    ) -> Job:
        """@brief 对不可变 revision 创建渲染 Job / Create a render job for an immutable revision."""
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_RESUME_RENDER_JOB,
            )
            revision = await uow.repository.get_revision(
                workspace_id,
                resume_id,
                command.resume_revision,
            )
            if revision is None:
                raise ResumeResourceNotFound("resume_revision")
            policy = await self._require_template(uow, revision.document.template)
            policy.validate(
                revision.document,
                output_formats=tuple(item.value for item in command.formats),
            )
            now = self._clock.now()
            spec = ResumeRenderSpec(
                resume_id,
                command.resume_revision,
                command.mode,
                command.formats,
            )
            job = self._job(
                workspace_id,
                ResumeJobKind.RENDER,
                ResourceRef("resume", str(resume_id), command.resume_revision),
                now,
            )
            await uow.jobs.add(job, spec)
            await self._emit_job_created(uow, context, job, now)
            await uow.commit()
            return job

    async def list_proposals(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        page: PageRequest | None = None,
    ) -> CollectionPage[ResumeProposal]:
        """@brief 列出 Resume 的可审核 proposals / List reviewable proposals for a Resume."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.LIST_RESUME_PROPOSALS,
            )
            await self._require_resume(uow.repository, workspace_id, resume_id)
            return await uow.repository.list_proposals(
                workspace_id,
                resume_id,
                page or PageRequest(),
            )

    async def get_proposal(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        proposal_id: ResumeProposalId,
    ) -> ResumeProposal:
        """@brief 在 Workspace 边界内读取 proposal / Read a proposal within a Workspace boundary."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.READ_RESUME_PROPOSAL,
            )
            return await self._require_proposal(
                uow.repository,
                workspace_id,
                proposal_id,
            )

    async def decide_proposal(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        proposal_id: ResumeProposalId,
        command: ProposalDecisionCommand,
        *,
        expected_revision: int,
    ) -> ResumeOperationOutcome:
        """@brief 在同一事务中决策 proposal 并可选提交 Resume / Decide a proposal and optionally apply Resume operations atomically.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param proposal_id proposal 标识 / Proposal identifier.
        @param command 判别联合 decision / Discriminated decision command.
        @param expected_revision proposal If-Match revision / Proposal If-Match revision.
        @return ResumeOperationResult 领域投影 / ResumeOperationResult domain projection.
        """
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.DECIDE_RESUME_PROPOSAL,
            )
            proposal = await self._require_proposal(
                uow.repository,
                workspace_id,
                proposal_id,
                for_update=True,
            )
            self._require_revision(proposal.meta.revision, expected_revision)
            aggregate = await self._require_resume(
                uow.repository,
                workspace_id,
                proposal.resume_id,
                for_update=True,
            )
            now = self._clock.now()
            decided, selected = proposal.decide(
                command,
                actor_id=context.actor.user_id,
                at=now,
            )
            if command.decision is ProposalDecision.REJECT:
                outcome = ResumeOperationOutcome(aggregate.document, ())
            else:
                batch = ResumeOperationBatch(
                    ResumeBatchId(str(proposal.meta.id)),
                    proposal.base_revision,
                    ConflictStrategy.REJECT,
                    selected,
                    RenderHint.NONE,
                )
                policies = await self._load_operation_policies(
                    uow,
                    aggregate,
                    selected,
                )
                change = aggregate.apply_batch(
                    batch,
                    at=now,
                    actor_id=context.actor.user_id,
                    template_policies=policies,
                )
                if change.revision is not None:
                    await self._save_resume(
                        uow,
                        change.aggregate,
                        change.revision,
                        expected_revision=aggregate.document.meta.revision,
                    )
                aggregate = change.aggregate
                outcome = ResumeOperationOutcome(
                    aggregate.document,
                    change.applied_operation_ids,
                )
            try:
                await uow.repository.save_proposal(
                    decided,
                    expected_revision=expected_revision,
                )
            except ResumeCasMismatch as error:
                raise ResumePreconditionFailed from error
            await self._emit(
                uow,
                context,
                "resume.proposal_decided",
                ResourceRef(
                    "resume_proposal",
                    str(proposal_id),
                    decided.meta.revision,
                ),
                {
                    "decision": command.decision.value,
                    "resume_revision": aggregate.document.meta.revision,
                },
                now,
            )
            await uow.commit()
            return outcome

    async def _authorize(
        self,
        uow: ResumeUnitOfWork,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        action: WorkspaceAction,
    ) -> WorkspaceAccessContext:
        """@brief 验证 authorizer 签发的精确上下文 / Verify the exact context issued by the authorizer."""
        actor = await uow.authorizer.authenticate(principal)
        context = await uow.authorizer.authorize(actor, workspace_id, action)
        if (
            context.workspace_id != workspace_id
            or context.action is not action
            or context.actor.principal != principal
        ):
            raise PermissionError("authorization context does not match the requested action")
        return context

    @staticmethod
    async def _require_resume(
        repository: ResumeRepository,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        for_update: bool = False,
    ) -> ResumeAggregate:
        """@brief 读取 Resume 并统一隐藏跨租户结果 / Read a Resume and uniformly hide cross-tenant results."""
        aggregate = await repository.get_resume(
            workspace_id,
            resume_id,
            for_update=for_update,
        )
        if aggregate is None or aggregate.document.workspace_id != workspace_id:
            raise ResumeResourceNotFound("resume")
        return aggregate

    @staticmethod
    async def _require_proposal(
        repository: ResumeRepository,
        workspace_id: WorkspaceId,
        proposal_id: ResumeProposalId,
        *,
        for_update: bool = False,
    ) -> ResumeProposal:
        """@brief 读取 proposal 并统一隐藏跨租户结果 / Read a proposal and hide cross-tenant results."""
        proposal = await repository.get_proposal(
            workspace_id,
            proposal_id,
            for_update=for_update,
        )
        if proposal is None or proposal.workspace_id != workspace_id:
            raise ResumeResourceNotFound("resume_proposal")
        return proposal

    @staticmethod
    def _require_revision(current: int, expected: int) -> None:
        """@brief 在领域修改前校验 If-Match revision / Validate If-Match revision before domain mutation."""
        if current != expected:
            raise ResumePreconditionFailed

    @staticmethod
    async def _require_template(
        uow: ResumeUnitOfWork,
        template: TemplateRef,
    ) -> TemplatePolicy:
        """@brief 读取模板策略或返回领域 404 / Read a template policy or fail with a domain 404."""
        policy = await uow.templates.get_policy(template)
        if policy is None:
            raise ResumeResourceNotFound("resume_template")
        return policy

    async def _load_operation_policies(
        self,
        uow: ResumeUnitOfWork,
        aggregate: ResumeAggregate,
        operations: Sequence[ResumeOperation],
    ) -> Mapping[TemplateRef, TemplatePolicy]:
        """@brief 一次加载批次可能提交的全部模板策略 / Load every template policy a batch may commit."""
        references = {aggregate.document.template}
        references.update(
            operation.template
            for operation in operations
            if isinstance(operation, SetResumeTemplate)
        )
        policies: dict[TemplateRef, TemplatePolicy] = {}
        for reference in references:
            policies[reference] = await self._require_template(uow, reference)
        return policies

    async def _save_resume(
        self,
        uow: ResumeUnitOfWork,
        aggregate: ResumeAggregate,
        revision: ResumeRevision,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 将 repository CAS 失败统一为预条件失败 / Normalize repository CAS failure as precondition failure."""
        try:
            await uow.repository.save_resume(
                aggregate,
                revision,
                expected_revision=expected_revision,
            )
        except ResumeCasMismatch as error:
            raise ResumePreconditionFailed from error

    def _job(
        self,
        workspace_id: WorkspaceId,
        kind: ResumeJobKind,
        subject: ResourceRef,
        at: datetime,
    ) -> Job:
        """@brief 构造统一 queued Resume Job / Construct a unified queued Resume job."""
        return Job(
            ResourceMeta(
                JobId(self._id_factory("job")),
                1,
                at,
                at,
            ),
            workspace_id,
            kind.value,
            subject,
        )

    async def _create_hint_job(
        self,
        uow: ResumeUnitOfWork,
        context: WorkspaceAccessContext,
        workspace_id: WorkspaceId,
        document: ResumeDocument,
        hint: RenderHint,
        at: datetime,
    ) -> ResourceRef | None:
        """@brief 将 operation render_hint 转为同事务 Job / Convert an operation render hint into a same-transaction job."""
        if hint is RenderHint.NONE:
            return None
        mode = RenderMode.PREVIEW if hint is RenderHint.PREVIEW else RenderMode.FINAL
        spec = ResumeRenderSpec(
            document.meta.id,
            document.meta.revision,
            mode,
            (RenderFormat.PDF,),
        )
        job = self._job(
            workspace_id,
            ResumeJobKind.RENDER,
            ResourceRef("resume", str(document.meta.id), document.meta.revision),
            at,
        )
        await uow.jobs.add(job, spec)
        await self._emit_job_created(uow, context, job, at)
        return ResourceRef("job", str(job.meta.id), job.meta.revision)

    async def _emit_job_created(
        self,
        uow: ResumeUnitOfWork,
        context: WorkspaceAccessContext,
        job: Job,
        at: datetime,
    ) -> None:
        """@brief 记录 Job 创建 outbox 事件 / Record a job-created outbox event."""
        await self._emit(
            uow,
            context,
            "resume.job_created",
            ResourceRef("job", str(job.meta.id), 1),
            {"kind": job.kind},
            at,
        )

    async def _emit(
        self,
        uow: ResumeUnitOfWork,
        context: WorkspaceAccessContext,
        event_type: str,
        subject: ResourceRef,
        data: dict[str, JsonValue],
        at: datetime,
    ) -> None:
        """@brief 写入不含秘密的同事务 outbox 事件 / Write a secret-free same-transaction outbox event."""
        await uow.outbox.add(
            ResumeOutboxEvent(
                self._id_factory("evt"),
                context.workspace_id,
                event_type,
                at,
                UserId(context.actor.user_id),
                subject,
                data,
            )
        )


__all__ = [
    "Clock",
    "CreateRenderJobCommand",
    "CreateRestoreJobCommand",
    "CreateResumeCommand",
    "CreateResumeImportJobCommand",
    "InvalidResumeCommand",
    "ResumeApplicationError",
    "ResumeApplicationService",
    "ResumePreconditionFailed",
    "ResumeResourceNotFound",
    "UpdateResumeMetadataCommand",
    "UtcClock",
]
