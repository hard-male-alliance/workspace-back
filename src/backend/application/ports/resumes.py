"""@brief API v2 Resume 应用端口 / API v2 Resume application ports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self

from backend.domain.platform import Job, JobId
from backend.domain.principals import (
    AuthenticatedActor,
    TokenPrincipal,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.resume_jobs import ResumeJobSpec, ResumeOutboxEvent
from backend.domain.resume_proposals import ResumeProposal
from backend.domain.resumes import (
    ResumeAggregate,
    ResumeBatchId,
    ResumeId,
    ResumeOperationOutcome,
    ResumeProposalId,
    ResumeRevision,
    ResumeRevisionSummary,
    ResumeSummary,
    TemplatePolicy,
    TemplateRef,
)


class ResumeCasMismatch(RuntimeError):
    """@brief repository 的最终 compare-and-swap 失败 / Repository-level final compare-and-swap failure."""


@dataclass(frozen=True, slots=True)
class PageRequest:
    """@brief 应用层的 keyset 分页请求 / Application-level keyset page request.

    @param limit 最大返回数 / Maximum item count.
    @param after 上一页最后的内部位置 / Internal position after the previous page.
    """

    limit: int = 50
    after: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验契约分页上限 / Validate contract pagination limits.

        @raise ValueError limit 不在 1..200 时抛出 / Raised unless limit is 1..200.
        """
        if not 1 <= self.limit <= 200:
            raise ValueError("page limit must be between one and 200")


@dataclass(frozen=True, slots=True)
class CollectionPage[ItemT]:
    """@brief 稳定排序的 keyset 分页 / Stably ordered keyset page.

    @param items 当前页项目 / Current page items.
    @param next_position 下一页内部位置 / Internal next-page position.
    """

    items: tuple[ItemT, ...]
    next_position: str | None


@dataclass(frozen=True, slots=True)
class OperationBatchReceipt:
    """@brief 保留至少 30 天的 Resume batch 重放记录 / Resume batch replay record retained at least 30 days.

    @param workspace_id Workspace 边界 / Workspace boundary.
    @param resume_id Resume 标识 / Resume identifier.
    @param batch_id 客户端批次标识 / Client batch identifier.
    @param request_fingerprint 规范请求 SHA-256 / Canonical request SHA-256.
    @param outcome 可精确重放的领域结果 / Exactly replayable domain outcome.
    @param created_at 首次提交时刻 / Initial commit instant.
    """

    workspace_id: WorkspaceId
    resume_id: ResumeId
    batch_id: ResumeBatchId
    request_fingerprint: str
    outcome: ResumeOperationOutcome
    created_at: datetime


class ResumeWorkspaceAuthorizer(Protocol):
    """@brief 集中的 token-scope 与 Workspace-role 授权端口 / Central token-scope and role authorizer."""

    async def authenticate(self, principal: TokenPrincipal) -> AuthenticatedActor:
        """@brief 将签名 token principal 绑定到本地用户 / Bind a signed token principal to a local user.

        @param principal 已验证 token principal / Verified token principal.
        @return 已认证本地 actor / Authenticated local actor.
        """

    async def authorize(
        self,
        actor: AuthenticatedActor,
        workspace_id: WorkspaceId,
        action: WorkspaceAction,
    ) -> WorkspaceAccessContext:
        """@brief 为一次精确 Resume 操作签发授权上下文 / Authorize one exact Resume action.

        @param actor 已认证本地 actor / Authenticated local actor.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param action 精确 action / Exact action.
        @return 密封的 Workspace 授权上下文 / Sealed Workspace authorization context.
        """


class ResumeRepository(Protocol):
    """@brief Workspace 隔离且支持 CAS 的 Resume repository / Workspace-isolated CAS Resume repository."""

    async def list_resumes(
        self,
        workspace_id: WorkspaceId,
        page: PageRequest,
    ) -> CollectionPage[ResumeSummary]:
        """@brief 列出 Workspace Resume 摘要 / List Workspace Resume summaries.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param page keyset 分页 / Keyset pagination.
        @return 稳定顺序分页 / Stably ordered page.
        """

    async def get_resume(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        for_update: bool = False,
    ) -> ResumeAggregate | None:
        """@brief 在 Workspace 内读取 Resume 聚合 / Read a Resume aggregate within a Workspace.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param for_update 是否获取并发写锁 / Whether to acquire a concurrency write lock.
        @return Resume 或不存在 / Resume or absence.
        """

    async def add_resume(
        self,
        aggregate: ResumeAggregate,
        revision: ResumeRevision,
    ) -> None:
        """@brief 原子添加 Resume 与首个 revision / Atomically add a Resume and its first revision.

        @param aggregate 新 Resume 聚合 / New Resume aggregate.
        @param revision 首个 revision 快照 / First revision snapshot.
        """

    async def save_resume(
        self,
        aggregate: ResumeAggregate,
        revision: ResumeRevision,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 使用影响行数校验 CAS 保存 Resume / Save a Resume with affected-row CAS verification.

        @param aggregate 新 Resume 聚合 / New Resume aggregate.
        @param revision 同事务 revision 快照 / Same-transaction revision snapshot.
        @param expected_revision UPDATE 的旧 revision / Old revision required by UPDATE.
        @note adapter 必须在影响行数非 1 时抛出 CAS 错误 / The adapter must fail unless exactly one row changes.
        """

    async def delete_resume(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 使用 CAS 删除 Resume / Delete a Resume with CAS.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param expected_revision 预期 revision / Expected revision.
        """

    async def list_revisions(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        page: PageRequest,
    ) -> CollectionPage[ResumeRevisionSummary]:
        """@brief 列出 Resume revision 摘要 / List Resume revision summaries."""

    async def get_revision(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        revision: int,
    ) -> ResumeRevision | None:
        """@brief 读取一个不可变 revision / Read one immutable revision."""

    async def get_batch_receipt(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        batch_id: ResumeBatchId,
    ) -> OperationBatchReceipt | None:
        """@brief 读取离线 batch 重放记录 / Read an offline-batch replay receipt."""

    async def add_batch_receipt(self, receipt: OperationBatchReceipt) -> None:
        """@brief 在 Resume 提交事务中添加 batch 记录 / Add a batch receipt in the Resume transaction."""

    async def list_proposals(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        page: PageRequest,
    ) -> CollectionPage[ResumeProposal]:
        """@brief 列出属于 Resume 的 proposals / List proposals belonging to a Resume."""

    async def get_proposal(
        self,
        workspace_id: WorkspaceId,
        proposal_id: ResumeProposalId,
        *,
        for_update: bool = False,
    ) -> ResumeProposal | None:
        """@brief 在 Workspace 内读取 proposal / Read a proposal within a Workspace."""

    async def save_proposal(
        self,
        proposal: ResumeProposal,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 使用 CAS 保存 proposal decision / Save a proposal decision with CAS."""


class ResumeTemplateCatalog(Protocol):
    """@brief 唯一 v2 manifest 来源的不可变策略端口 / Immutable policy port over the sole v2 manifest source."""

    async def get_policy(self, template: TemplateRef) -> TemplatePolicy | None:
        """@brief 读取不可变模板版本策略 / Read an immutable template-version policy.

        @param template 模板版本引用 / Template version reference.
        @return 策略或不存在 / Policy or absence.
        """


class ResumeImportSourceVerifier(Protocol):
    """@brief 原子领取 import upload session 的事务端口 / Transactional port for atomically claiming import uploads."""

    async def claim(
        self,
        workspace_id: WorkspaceId,
        upload_session_id: str,
        job_id: JobId,
    ) -> bool:
        """@brief 仅在 upload 归属匹配、已完成且未消费时绑定 Job / Bind a job only to an owned, complete, unconsumed upload.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param upload_session_id 不透明 upload session ID / Opaque upload-session ID.
        @param job_id 消费 upload 的 Job / Job consuming the upload.
        @return 成功原子领取时为真 / True when the atomic claim succeeds.
        @note adapter 必须通过条件 UPDATE 或等价锁防止两个 Job 消费同一 upload / The adapter must use conditional UPDATE or equivalent locking.
        """


class ResumeJobSink(Protocol):
    """@brief 与业务写入同事务的 Job sink / Job sink sharing the business transaction."""

    async def add(self, job: Job, spec: ResumeJobSpec) -> None:
        """@brief 持久化统一 queued Job 与私有 worker spec / Persist a unified queued job and private worker spec.

        @param job 已校验统一 Job / Validated unified job.
        @param spec 不进入公共 Job payload 的 Resume worker 输入 / Resume worker input excluded from the public Job payload.
        """


class ResumeOutbox(Protocol):
    """@brief 与业务写入同事务的 outbox / Outbox sharing the business transaction."""

    async def add(self, event: ResumeOutboxEvent) -> None:
        """@brief 持久化待发布事件 / Persist an event awaiting publication.

        @param event 不含秘密的事件 / Secret-free event.
        """


class ResumeUnitOfWork(Protocol):
    """@brief Resume、revision、Job、proposal 与 outbox 的原子工作单元 / Atomic Resume unit of work."""

    @property
    def repository(self) -> ResumeRepository:
        """@brief 返回事务绑定 repository / Return the transaction-bound repository."""

    @property
    def authorizer(self) -> ResumeWorkspaceAuthorizer:
        """@brief 返回同事务授权器 / Return the same-transaction authorizer."""

    @property
    def templates(self) -> ResumeTemplateCatalog:
        """@brief 返回不可变模板 catalog / Return the immutable template catalog."""

    @property
    def import_sources(self) -> ResumeImportSourceVerifier:
        """@brief 返回 import upload 验证器 / Return the import-upload verifier."""

    @property
    def jobs(self) -> ResumeJobSink:
        """@brief 返回 Job sink / Return the job sink."""

    @property
    def outbox(self) -> ResumeOutbox:
        """@brief 返回 outbox / Return the outbox."""

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
        """@brief 异常或未提交时回滚 / Roll back on exceptions or absent commit.

        @param exc_type 异常类型 / Exception type.
        @param exc 异常实例 / Exception instance.
        @param traceback traceback / Traceback.
        @return 不吞异常 / Does not suppress exceptions.
        """

    async def commit(self) -> None:
        """@brief 原子提交全部 Resume 写入 / Atomically commit all Resume writes."""

    async def rollback(self) -> None:
        """@brief 幂等回滚 / Idempotently roll back."""


class ResumeUnitOfWorkFactory(Protocol):
    """@brief 为每个 Resume 用例创建工作单元 / Create a unit of work per Resume use case."""

    def __call__(self) -> ResumeUnitOfWork:
        """@brief 创建未进入的工作单元 / Create a not-yet-entered unit of work.

        @return 新工作单元 / New unit of work.
        """


__all__ = [
    "CollectionPage",
    "OperationBatchReceipt",
    "PageRequest",
    "ResumeCasMismatch",
    "ResumeImportSourceVerifier",
    "ResumeJobSink",
    "ResumeOutbox",
    "ResumeRepository",
    "ResumeTemplateCatalog",
    "ResumeUnitOfWork",
    "ResumeUnitOfWorkFactory",
    "ResumeWorkspaceAuthorizer",
]
