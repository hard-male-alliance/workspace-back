"""@brief API v2 Resume 领域与应用核心测试 / API v2 Resume domain and application-core tests."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from types import TracebackType

import pytest

from backend.application.ports.access import WORKSPACE_AUTHORIZATION_MATRIX
from backend.application.ports.resumes import (
    CollectionPage,
    OperationBatchReceipt,
    PageRequest,
    ResumeCasMismatch,
)
from backend.application.resumes import (
    CreateRenderJobCommand,
    CreateRestoreJobCommand,
    CreateResumeCommand,
    CreateResumeImportJobCommand,
    ResumeApplicationService,
    ResumePreconditionFailed,
    UpdateResumeMetadataCommand,
)
from backend.domain.platform import Job, JobId
from backend.domain.principals import (
    AuthenticatedActor,
    ClientId,
    MembershipId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceAction,
    WorkspaceId,
    _issue_workspace_access_context,
)
from backend.domain.resume_jobs import (
    RenderFormat,
    RenderMode,
    ResumeJobSpec,
    ResumeOutboxEvent,
)
from backend.domain.resume_proposals import (
    ProposalDecision,
    ProposalDecisionCommand,
    ResumeProposal,
    ResumeProposalStatus,
)
from backend.domain.resumes import (
    ConflictStrategy,
    DateRange,
    EntityKind,
    MoveResumeEntity,
    PageSize,
    PartialDate,
    RemoveResumeEntity,
    RenderHint,
    ResumeAggregate,
    ResumeBatchId,
    ResumeBatchKeyReused,
    ResumeDomainError,
    ResumeId,
    ResumeItem,
    ResumeItemKind,
    ResumeOperationBatch,
    ResumeOperationId,
    ResumeProposalId,
    ResumeRevision,
    ResumeRevisionConflict,
    ResumeRevisionSummary,
    ResumeSection,
    ResumeSectionKind,
    ResumeSummary,
    RichText,
    SetResumeField,
    SetResumeTemplate,
    TemplatePolicy,
    TemplateRef,
    TemplateSettingRule,
    TemplateSettingValueType,
    TemplateZonePolicy,
    TextMark,
    TextMarkKind,
    UpsertResumeItem,
    UpsertResumeSection,
    create_resume_document,
)
from backend.domain.workspaces import WorkspaceRole

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 测试固定时刻 / Fixed test instant."""

WORKSPACE_ID = WorkspaceId("ws_00000001")
"""@brief 测试 Workspace ID / Test Workspace ID."""

USER_ID = UserId("user_00000001")
"""@brief 测试用户 ID / Test user ID."""

TEMPLATE_REF = TemplateRef("template_00000001", "1.0")
"""@brief 测试模板引用 / Test template reference."""

RESUME_ID = ResumeId("resume_00000001")
"""@brief 测试 Resume ID / Test Resume ID."""


class FixedClock:
    """@brief 可控测试时钟 / Controllable test clock."""

    def __init__(self, now: datetime = NOW) -> None:
        """@brief 初始化固定时刻 / Initialize the fixed instant."""
        self.value = now

    def now(self) -> datetime:
        """@brief 返回当前测试时刻 / Return the current test instant."""
        return self.value


class IdSequence:
    """@brief 生成契约有效的可预测 ID / Generate predictable contract-valid IDs."""

    def __init__(self) -> None:
        """@brief 初始化计数器 / Initialize counters."""
        self._counts: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        """@brief 生成下一个 ID / Generate the next ID."""
        value = self._counts.get(prefix, 0) + 1
        self._counts[prefix] = value
        return f"{prefix}_{value:08d}"


def _policy() -> TemplatePolicy:
    """@brief 构造支持所有测试操作的模板策略 / Build a template policy supporting all test operations."""
    kinds = frozenset(ResumeSectionKind)
    return TemplatePolicy(
        TEMPLATE_REF,
        frozenset({"zh-CN", "en-US"}),
        frozenset({PageSize.A4}),
        frozenset({"pdf", "json", "docx"}),
        kinds,
        (TemplateZonePolicy("main", kinds, 100),),
        frozenset({"body.default"}),
        frozenset({"yyyy_mm"}),
        frozenset({"bullet.default"}),
        (
            TemplateSettingRule(
                "compact",
                TemplateSettingValueType.BOOLEAN,
                False,
            ),
        ),
    )


def _principal(*scopes: str) -> TokenPrincipal:
    """@brief 构造已验证 token principal / Build a verified token principal."""
    return TokenPrincipal(
        USER_ID,
        Subject("subject_00000001"),
        ClientId("client_00000001"),
        frozenset(Scope(scope) for scope in scopes),
    )


def _document(
    *,
    resume_id: ResumeId = RESUME_ID,
    title: str = "Distributed Systems Engineer",
) -> ResumeAggregate:
    """@brief 构造 revision=1 的 Resume 聚合 / Build a Resume aggregate at revision one."""
    document = create_resume_document(
        resume_id=resume_id,
        workspace_id=WORKSPACE_ID,
        title=title,
        locale="zh-CN",
        template_policy=_policy(),
        created_at=NOW,
        full_name="Klee",
    )
    return ResumeAggregate.create(document, USER_ID)[0]


def _section() -> ResumeSection:
    """@brief 构造带首个 item 的经历 section / Build an experience section with one item."""
    return ResumeSection(
        "section_00000001",
        ResumeSectionKind.EXPERIENCE,
        "Experience",
        items=(
            ResumeItem(
                "item_00000001",
                ResumeItemKind.EXPERIENCE,
                title="Backend Engineer",
            ),
        ),
    )


def test_partial_dates_and_rich_text_enforce_semantic_invariants() -> None:
    """@brief 验证日历日期与 RichText mark 非仅做格式检查 / Verify real calendar and mark invariants."""
    assert PartialDate("2024-02-29").lower_bound().day == 29
    with pytest.raises(ResumeDomainError, match="partial date"):
        PartialDate("2023-02-29")
    with pytest.raises(ResumeDomainError, match="reversed"):
        DateRange(PartialDate("2025-01"), PartialDate("2024-12"))
    with pytest.raises(ResumeDomainError, match="overlap illegally"):
        RichText(
            "abcdefgh",
            (
                TextMark(0, 5, TextMarkKind.STRONG),
                TextMark(3, 7, TextMarkKind.EMPHASIS),
            ),
        )
    RichText(
        "abcdefgh",
        (
            TextMark(0, 8, TextMarkKind.STRONG),
            TextMark(2, 4, TextMarkKind.EMPHASIS),
        ),
    )


def test_resume_document_rejects_cross_kind_duplicate_entity_ids() -> None:
    """@brief 验证 section/item 全局 ID 唯一 / Verify global section/item ID uniqueness."""
    aggregate = _document()
    with pytest.raises(ResumeDomainError, match="globally unique"):
        replace(
            aggregate.document,
            sections=(
                ResumeSection(
                    "section_00000001",
                    ResumeSectionKind.EXPERIENCE,
                    "Experience",
                    items=(
                        ResumeItem(
                            "section_00000001",
                            ResumeItemKind.EXPERIENCE,
                        ),
                    ),
                ),
            ),
        )


def test_operation_engine_applies_all_six_v2_operations_atomically() -> None:
    """@brief 验证六种 v2 operation 共用一个强类型聚合引擎 / Verify all six operations share one typed aggregate engine."""
    aggregate = _document()
    second_item = ResumeItem(
        "item_00000002",
        ResumeItemKind.PROJECT,
        title="Consensus simulator",
    )
    operations = (
        UpsertResumeSection(
            ResumeOperationId("op_00000001"),
            _section(),
            None,
        ),
        UpsertResumeItem(
            ResumeOperationId("op_00000002"),
            "section_00000001",
            second_item,
            "item_00000001",
        ),
        SetResumeField(
            ResumeOperationId("op_00000003"),
            "item_00000001",
            ("title",),
            "Senior Backend Engineer",
        ),
        MoveResumeEntity(
            ResumeOperationId("op_00000004"),
            EntityKind.ITEM,
            "item_00000002",
            "section_00000001",
            None,
        ),
        RemoveResumeEntity(
            ResumeOperationId("op_00000005"),
            EntityKind.ITEM,
            "item_00000002",
        ),
        SetResumeTemplate(
            ResumeOperationId("op_00000006"),
            TEMPLATE_REF,
            {"compact": True},
        ),
    )
    batch = ResumeOperationBatch(
        ResumeBatchId("batch_00000001"),
        1,
        ConflictStrategy.REJECT,
        operations,
        RenderHint.NONE,
    )

    change = aggregate.apply_batch(
        batch,
        at=NOW + timedelta(seconds=1),
        actor_id=USER_ID,
        template_policies={TEMPLATE_REF: _policy()},
    )

    assert change.aggregate.document.meta.revision == 2
    assert change.aggregate.document.sections[0].items[0].title == "Senior Backend Engineer"
    assert len(change.aggregate.document.sections[0].items) == 1
    assert change.aggregate.document.style.template_settings == {"compact": True}
    assert len(change.aggregate.operation_ledger) == 6
    assert change.revision is not None
    assert change.revision.document == change.aggregate.document


def test_operation_engine_deduplicates_and_rejects_changed_operation_payload() -> None:
    """@brief 验证 operation ID 重放不增 revision，但变更 payload 必须 409 / Verify operation dedup and payload-reuse rejection."""
    operation = SetResumeField(
        ResumeOperationId("op_00000001"),
        "resume_00000001",
        ("title",),
        "Senior Engineer",
    )
    first = _document().apply_batch(
        ResumeOperationBatch(
            ResumeBatchId("batch_00000001"),
            1,
            ConflictStrategy.REJECT,
            (operation,),
            RenderHint.NONE,
        ),
        at=NOW + timedelta(seconds=1),
        actor_id=USER_ID,
        template_policies={TEMPLATE_REF: _policy()},
    ).aggregate
    replay = first.apply_batch(
        ResumeOperationBatch(
            ResumeBatchId("batch_00000002"),
            2,
            ConflictStrategy.REJECT,
            (operation,),
            RenderHint.NONE,
        ),
        at=NOW + timedelta(seconds=2),
        actor_id=USER_ID,
        template_policies={TEMPLATE_REF: _policy()},
    )
    assert replay.revision is None
    assert replay.aggregate.document.meta.revision == 2
    assert replay.deduplicated_operation_ids == (ResumeOperationId("op_00000001"),)
    changed = SetResumeField(
        ResumeOperationId("op_00000001"),
        "resume_00000001",
        ("title",),
        "Different",
    )
    with pytest.raises(ResumeBatchKeyReused):
        first.apply_batch(
            ResumeOperationBatch(
                ResumeBatchId("batch_00000003"),
                2,
                ConflictStrategy.REJECT,
                (changed,),
                RenderHint.NONE,
            ),
            at=NOW + timedelta(seconds=3),
            actor_id=USER_ID,
            template_policies={TEMPLATE_REF: _policy()},
        )


def test_operation_batch_never_exposes_a_partially_mutated_aggregate() -> None:
    """@brief 验证后续 operation 失败时前置变更不可见 / Verify a later failure never exposes earlier mutations."""
    aggregate = _document()
    batch = ResumeOperationBatch(
        ResumeBatchId("batch_00000009"),
        1,
        ConflictStrategy.REJECT,
        (
            UpsertResumeSection(
                ResumeOperationId("op_00000009"),
                _section(),
                None,
            ),
            RemoveResumeEntity(
                ResumeOperationId("op_00000010"),
                EntityKind.ITEM,
                "item_missing_0001",
            ),
        ),
        RenderHint.NONE,
    )

    with pytest.raises(ResumeDomainError, match="not found"):
        aggregate.apply_batch(
            batch,
            at=NOW + timedelta(seconds=1),
            actor_id=USER_ID,
            template_policies={TEMPLATE_REF: _policy()},
        )

    assert aggregate.document.meta.revision == 1
    assert aggregate.document.sections == ()
    assert aggregate.operation_ledger == ()


def test_safe_rebase_accepts_disjoint_targets_and_rejects_overlap() -> None:
    """@brief 验证 rebase 仅允许因果目标不重叠 / Verify rebase only for causally disjoint targets."""
    aggregate = _document()
    first = aggregate.apply_batch(
        ResumeOperationBatch(
            ResumeBatchId("batch_00000001"),
            1,
            ConflictStrategy.REJECT,
            (
                SetResumeField(
                    ResumeOperationId("op_00000001"),
                    "resume_00000001",
                    ("title",),
                    "Changed title",
                ),
            ),
            RenderHint.NONE,
        ),
        at=NOW + timedelta(seconds=1),
        actor_id=USER_ID,
        template_policies={TEMPLATE_REF: _policy()},
    ).aggregate
    safe = first.apply_batch(
        ResumeOperationBatch(
            ResumeBatchId("batch_00000002"),
            1,
            ConflictStrategy.REBASE_IF_SAFE,
            (
                SetResumeField(
                    ResumeOperationId("op_00000002"),
                    "resume_00000001",
                    ("locale",),
                    "en-US",
                ),
            ),
            RenderHint.NONE,
        ),
        at=NOW + timedelta(seconds=2),
        actor_id=USER_ID,
        template_policies={TEMPLATE_REF: _policy()},
    )
    assert safe.aggregate.document.meta.revision == 3
    with pytest.raises(ResumeRevisionConflict) as captured:
        first.apply_batch(
            ResumeOperationBatch(
                ResumeBatchId("batch_00000003"),
                1,
                ConflictStrategy.REBASE_IF_SAFE,
                (
                    SetResumeField(
                        ResumeOperationId("op_00000003"),
                        "resume_00000001",
                        ("title",),
                        "Conflicting title",
                    ),
                ),
                RenderHint.NONE,
            ),
            at=NOW + timedelta(seconds=2),
            actor_id=USER_ID,
            template_policies={TEMPLATE_REF: _policy()},
        )
    assert captured.value.current_revision == 2
    assert captured.value.conflicts[0].operation_id == ResumeOperationId("op_00000003")


@dataclass(slots=True)
class MemoryStore:
    """@brief Resume 应用用例的事务内存状态 / In-memory transactional state for Resume use cases."""

    resumes: dict[tuple[WorkspaceId, ResumeId], ResumeAggregate] = field(default_factory=dict)
    revisions: dict[tuple[WorkspaceId, ResumeId, int], ResumeRevision] = field(default_factory=dict)
    receipts: dict[tuple[WorkspaceId, ResumeId, ResumeBatchId], OperationBatchReceipt] = field(default_factory=dict)
    proposals: dict[tuple[WorkspaceId, ResumeProposalId], ResumeProposal] = field(default_factory=dict)
    jobs: list[tuple[Job, ResumeJobSpec]] = field(default_factory=list)
    events: list[ResumeOutboxEvent] = field(default_factory=list)
    claimed_uploads: set[str] = field(default_factory=set)
    commits: int = 0


class MemoryRepository:
    """@brief 实现 CAS 语义的测试 repository / Test repository implementing CAS semantics."""

    def __init__(self, store: MemoryStore) -> None:
        """@brief 绑定内存状态 / Bind in-memory state."""
        self.store = store

    async def list_resumes(self, workspace_id: WorkspaceId, page: PageRequest) -> CollectionPage[ResumeSummary]:
        """@brief 按 ID 列出 Resume / List Resumes by ID."""
        items = sorted(
            (
                aggregate.document.summary()
                for (owner, _), aggregate in self.store.resumes.items()
                if owner == workspace_id
            ),
            key=lambda item: item.meta.id,
        )
        start = next((index + 1 for index, item in enumerate(items) if item.meta.id == page.after), 0)
        selected = tuple(items[start : start + page.limit])
        has_more = start + page.limit < len(items)
        return CollectionPage(selected, str(selected[-1].meta.id) if selected and has_more else None)

    async def get_resume(self, workspace_id: WorkspaceId, resume_id: ResumeId, *, for_update: bool = False) -> ResumeAggregate | None:
        """@brief 读取 Workspace Resume / Read a Workspace Resume."""
        del for_update
        return self.store.resumes.get((workspace_id, resume_id))

    async def add_resume(self, aggregate: ResumeAggregate, revision: ResumeRevision) -> None:
        """@brief 添加 Resume 与首个 revision / Add a Resume and first revision."""
        key = (aggregate.document.workspace_id, aggregate.document.meta.id)
        if key in self.store.resumes:
            raise RuntimeError("duplicate resume")
        self.store.resumes[key] = aggregate
        self.store.revisions[(*key, revision.revision)] = revision

    async def save_resume(self, aggregate: ResumeAggregate, revision: ResumeRevision, *, expected_revision: int) -> None:
        """@brief 通过 CAS 保存 Resume / Save a Resume via CAS."""
        key = (aggregate.document.workspace_id, aggregate.document.meta.id)
        current = self.store.resumes.get(key)
        if current is None or current.document.meta.revision != expected_revision:
            raise ResumeCasMismatch
        self.store.resumes[key] = aggregate
        self.store.revisions[(*key, revision.revision)] = revision

    async def delete_resume(self, workspace_id: WorkspaceId, resume_id: ResumeId, *, expected_revision: int) -> None:
        """@brief 通过 CAS 删除 Resume / Delete a Resume via CAS."""
        key = (workspace_id, resume_id)
        current = self.store.resumes.get(key)
        if current is None or current.document.meta.revision != expected_revision:
            raise ResumeCasMismatch
        del self.store.resumes[key]

    async def list_revisions(self, workspace_id: WorkspaceId, resume_id: ResumeId, page: PageRequest) -> CollectionPage[ResumeRevisionSummary]:
        """@brief 列出 revision 摘要 / List revision summaries."""
        items = sorted(
            (
                revision.summary()
                for (owner, target, _), revision in self.store.revisions.items()
                if owner == workspace_id and target == resume_id
            ),
            key=lambda item: item.revision,
        )
        start = int(page.after) if page.after is not None else 0
        selected = tuple(items[start : start + page.limit])
        next_position = str(start + page.limit) if start + page.limit < len(items) else None
        return CollectionPage(selected, next_position)

    async def get_revision(self, workspace_id: WorkspaceId, resume_id: ResumeId, revision: int) -> ResumeRevision | None:
        """@brief 读取 revision / Read a revision."""
        return self.store.revisions.get((workspace_id, resume_id, revision))

    async def get_batch_receipt(self, workspace_id: WorkspaceId, resume_id: ResumeId, batch_id: ResumeBatchId) -> OperationBatchReceipt | None:
        """@brief 读取 batch receipt / Read a batch receipt."""
        return self.store.receipts.get((workspace_id, resume_id, batch_id))

    async def add_batch_receipt(self, receipt: OperationBatchReceipt) -> None:
        """@brief 添加 batch receipt / Add a batch receipt."""
        key = (receipt.workspace_id, receipt.resume_id, receipt.batch_id)
        if key in self.store.receipts:
            raise RuntimeError("duplicate receipt")
        self.store.receipts[key] = receipt

    async def list_proposals(self, workspace_id: WorkspaceId, resume_id: ResumeId, page: PageRequest) -> CollectionPage[ResumeProposal]:
        """@brief 列出 Resume proposals / List Resume proposals."""
        items = sorted(
            (
                proposal
                for (owner, _), proposal in self.store.proposals.items()
                if owner == workspace_id and proposal.resume_id == resume_id
            ),
            key=lambda item: item.meta.id,
        )
        start = next((index + 1 for index, item in enumerate(items) if item.meta.id == page.after), 0)
        selected = tuple(items[start : start + page.limit])
        has_more = start + page.limit < len(items)
        return CollectionPage(selected, str(selected[-1].meta.id) if selected and has_more else None)

    async def get_proposal(self, workspace_id: WorkspaceId, proposal_id: ResumeProposalId, *, for_update: bool = False) -> ResumeProposal | None:
        """@brief 读取 Workspace proposal / Read a Workspace proposal."""
        del for_update
        return self.store.proposals.get((workspace_id, proposal_id))

    async def save_proposal(self, proposal: ResumeProposal, *, expected_revision: int) -> None:
        """@brief 通过 CAS 保存 proposal / Save a proposal via CAS."""
        key = (proposal.workspace_id, proposal.meta.id)
        current = self.store.proposals.get(key)
        if current is None or current.meta.revision != expected_revision:
            raise ResumeCasMismatch
        self.store.proposals[key] = proposal


class MemoryAuthorizer:
    """@brief 为测试签发精确 WorkspaceAccessContext / Issue exact WorkspaceAccessContext values for tests."""

    async def authenticate(self, principal: TokenPrincipal) -> AuthenticatedActor:
        """@brief 将 token principal 绑定到测试 actor / Bind a token principal to the test actor."""
        return AuthenticatedActor(principal.user_id, principal)

    async def authorize(self, actor: AuthenticatedActor, workspace_id: WorkspaceId, action: WorkspaceAction):
        """@brief 校验 scope 并签发上下文 / Validate scope and issue context."""
        rule = WORKSPACE_AUTHORIZATION_MATRIX[action]
        if rule.scope not in actor.principal.scopes or WorkspaceRole.EDITOR not in rule.roles:
            raise PermissionError("denied")
        return _issue_workspace_access_context(
            actor,
            workspace_id,
            MembershipId("member_00000001"),
            WorkspaceRole.EDITOR,
            action,
        )


class MemoryTemplates:
    """@brief 测试不可变模板 catalog / Test immutable template catalog."""

    async def get_policy(self, template: TemplateRef) -> TemplatePolicy | None:
        """@brief 读取测试模板策略 / Read the test template policy."""
        return _policy() if template == TEMPLATE_REF else None


class MemoryImportSources:
    """@brief 认可测试 upload session 的 import verifier / Import verifier accepting test upload sessions."""

    def __init__(self, store: MemoryStore) -> None:
        """@brief 绑定可原子领取的测试状态 / Bind claimable test state."""
        self._store = store

    async def claim(
        self,
        workspace_id: WorkspaceId,
        upload_session_id: str,
        job_id: JobId,
    ) -> bool:
        """@brief 仅一次领取测试 Workspace 的规范 upload ID / Claim a canonical test upload exactly once."""
        del job_id
        if (
            workspace_id != WORKSPACE_ID
            or not upload_session_id.startswith("upload_")
            or upload_session_id in self._store.claimed_uploads
        ):
            return False
        self._store.claimed_uploads.add(upload_session_id)
        return True


class MemorySink:
    """@brief outbox 测试 adapter / Outbox test adapter."""

    def __init__(self, target: list[object]) -> None:
        """@brief 绑定目标列表 / Bind a target list."""
        self.target = target

    async def add(self, value: object) -> None:
        """@brief 添加一项 / Add one item."""
        self.target.append(value)


class MemoryJobSink:
    """@brief 保留统一 Job 与 typed spec 的测试 adapter / Test adapter retaining unified Jobs and typed specs."""

    def __init__(self, target: list[tuple[Job, ResumeJobSpec]]) -> None:
        """@brief 绑定 Job 列表 / Bind the Job list."""
        self.target = target

    async def add(self, job: Job, spec: ResumeJobSpec) -> None:
        """@brief 记录公开 Job 与私有 worker spec / Record the public Job and private worker spec."""
        self.target.append((job, spec))


class MemoryUnitOfWork:
    """@brief Resume 应用测试工作单元 / Resume application test unit of work."""

    def __init__(self, store: MemoryStore) -> None:
        """@brief 组装事务端口 / Assemble transactional ports."""
        self._store = store
        self._repository = MemoryRepository(store)
        self._authorizer = MemoryAuthorizer()
        self._templates = MemoryTemplates()
        self._import_sources = MemoryImportSources(store)
        self._jobs = MemoryJobSink(store.jobs)
        self._outbox = MemorySink(store.events)
        self._committed = False

    @property
    def repository(self) -> MemoryRepository:
        """@brief 返回 repository / Return the repository."""
        return self._repository

    @property
    def authorizer(self) -> MemoryAuthorizer:
        """@brief 返回 authorizer / Return the authorizer."""
        return self._authorizer

    @property
    def templates(self) -> MemoryTemplates:
        """@brief 返回 template catalog / Return the template catalog."""
        return self._templates

    @property
    def import_sources(self) -> MemoryImportSources:
        """@brief 返回 import source verifier / Return the import-source verifier."""
        return self._import_sources

    @property
    def jobs(self) -> MemoryJobSink:
        """@brief 返回 Job sink / Return the job sink."""
        return self._jobs

    @property
    def outbox(self) -> MemorySink:
        """@brief 返回 outbox / Return the outbox."""
        return self._outbox

    async def __aenter__(self) -> MemoryUnitOfWork:
        """@brief 进入工作单元 / Enter the unit of work."""
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None) -> bool:
        """@brief 不吞异常 / Do not suppress exceptions."""
        del exc_type, exc, traceback
        return False

    async def commit(self) -> None:
        """@brief 记录提交 / Record a commit."""
        self._committed = True
        self._store.commits += 1

    async def rollback(self) -> None:
        """@brief 测试 adapter 的空回滚 / No-op rollback for this test adapter."""


class MemoryUnitOfWorkFactory:
    """@brief 从共享内存状态创建工作单元 / Create units from shared memory state."""

    def __init__(self, store: MemoryStore) -> None:
        """@brief 绑定共享状态 / Bind shared state."""
        self.store = store

    def __call__(self) -> MemoryUnitOfWork:
        """@brief 创建工作单元 / Create a unit of work."""
        return MemoryUnitOfWork(self.store)


def _service(store: MemoryStore, ids: IdSequence | None = None) -> ResumeApplicationService:
    """@brief 组装测试服务 / Assemble a test service."""
    return ResumeApplicationService(
        MemoryUnitOfWorkFactory(store),
        clock=FixedClock(),
        id_factory=ids or IdSequence(),
    )


@pytest.mark.asyncio
async def test_application_crud_writes_revisions_outbox_and_enforces_cas() -> None:
    """@brief 验证 CRUD 在 Workspace 授权、revision、outbox 与 CAS 下运行 / Verify authorized CRUD, revisions, outbox, and CAS."""
    store = MemoryStore()
    service = _service(store)
    principal = _principal("resume.read", "resume.write", "resume.render")

    created = await service.create_resume(
        principal,
        WORKSPACE_ID,
        CreateResumeCommand("Backend Resume", "zh-CN", TEMPLATE_REF),
    )
    assert created.meta.revision == 1
    assert await service.get_resume(principal, WORKSPACE_ID, created.meta.id) == created
    page = await service.list_resumes(principal, WORKSPACE_ID)
    assert [item.meta.id for item in page.items] == [created.meta.id]

    updated = await service.update_resume_metadata(
        principal,
        WORKSPACE_ID,
        created.meta.id,
        UpdateResumeMetadataCommand(title="Staff Backend Resume"),
        expected_revision=1,
    )
    assert updated.meta.revision == 2
    revisions = await service.list_revisions(
        principal,
        WORKSPACE_ID,
        created.meta.id,
    )
    assert [item.revision for item in revisions.items] == [1, 2]
    assert (await service.get_revision(principal, WORKSPACE_ID, created.meta.id, 1)).document.title == "Backend Resume"

    with pytest.raises(ResumePreconditionFailed):
        await service.update_resume_metadata(
            principal,
            WORKSPACE_ID,
            created.meta.id,
            UpdateResumeMetadataCommand(locale="en-US"),
            expected_revision=1,
        )
    await service.delete_resume(
        principal,
        WORKSPACE_ID,
        created.meta.id,
        expected_revision=2,
    )
    assert len(store.events) == 3
    assert store.commits == 3


@pytest.mark.asyncio
async def test_application_operation_receipt_replays_before_stale_if_match_check() -> None:
    """@brief 验证同 batch 精确重放原始结果，不被后续 revision 破坏 / Verify exact batch replay precedes stale preconditions."""
    store = MemoryStore()
    service = _service(store)
    principal = _principal("resume.read", "resume.write", "resume.render")
    created = await service.create_resume(
        principal,
        WORKSPACE_ID,
        CreateResumeCommand("Backend Resume", "zh-CN", TEMPLATE_REF),
    )
    batch = ResumeOperationBatch(
        ResumeBatchId("batch_00000001"),
        1,
        ConflictStrategy.REJECT,
        (
            UpsertResumeSection(
                ResumeOperationId("op_00000001"),
                _section(),
                None,
            ),
        ),
        RenderHint.PREVIEW,
    )
    first = await service.apply_operations(
        principal,
        WORKSPACE_ID,
        created.meta.id,
        batch,
        expected_revision=1,
    )
    replay = await service.apply_operations(
        principal,
        WORKSPACE_ID,
        created.meta.id,
        batch,
        expected_revision=1,
    )
    assert replay == first
    assert first.resume.meta.revision == 2
    assert first.render_job_ref is not None
    assert len(store.jobs) == 1
    work_events = [event for event in store.events if event.event_type == "resume.job_created"]
    assert len(work_events) == 1
    assert work_events[0].subject == first.render_job_ref
    assert work_events[0].data == {"kind": "resume.render"}
    changed = ResumeOperationBatch(
        batch.client_batch_id,
        1,
        ConflictStrategy.REJECT,
        (
            SetResumeField(
                ResumeOperationId("op_00000002"),
                str(created.meta.id),
                ("title",),
                "Different",
            ),
        ),
        RenderHint.PREVIEW,
    )
    with pytest.raises(ResumeBatchKeyReused):
        await service.apply_operations(
            principal,
            WORKSPACE_ID,
            created.meta.id,
            changed,
            expected_revision=2,
        )


@pytest.mark.asyncio
async def test_application_creates_import_restore_and_render_jobs_after_validation() -> None:
    """@brief 验证 import/restore/render 只写 queued Job 与 outbox / Verify import, restore, and render create queued jobs and outbox events."""
    store = MemoryStore()
    service = _service(store)
    principal = _principal("resume.read", "resume.write", "resume.render")
    created = await service.create_resume(
        principal,
        WORKSPACE_ID,
        CreateResumeCommand("Backend Resume", "zh-CN", TEMPLATE_REF),
    )
    import_job = await service.create_import_job(
        principal,
        WORKSPACE_ID,
        CreateResumeImportJobCommand(
            "upload_00000001",
            "Imported Resume",
            "zh-CN",
            TEMPLATE_REF,
        ),
    )
    restore_job = await service.create_restore_job(
        principal,
        WORKSPACE_ID,
        created.meta.id,
        CreateRestoreJobCommand(1),
        expected_revision=1,
    )
    render_job = await service.create_render_job(
        principal,
        WORKSPACE_ID,
        created.meta.id,
        CreateRenderJobCommand(1, RenderMode.FINAL, (RenderFormat.PDF, RenderFormat.DOCX)),
    )
    assert [job.kind for job in (import_job, restore_job, render_job)] == [
        "resume.import",
        "resume.restore",
        "resume.render",
    ]
    assert len(store.jobs) == 3


@pytest.mark.asyncio
async def test_proposal_decision_applies_operations_and_terminal_state_atomically() -> None:
    """@brief 验证 proposal 决策与 Resume revision 在一个工作单元内提交 / Verify proposal decision and Resume revision share a unit of work."""
    store = MemoryStore()
    service = _service(store)
    principal = _principal("resume.read", "resume.write")
    created = await service.create_resume(
        principal,
        WORKSPACE_ID,
        CreateResumeCommand("Backend Resume", "zh-CN", TEMPLATE_REF),
    )
    proposal_id = ResumeProposalId("proposal_00000001")
    proposal = ResumeProposal(
        ResourceMeta(proposal_id, 1, NOW, NOW),
        WORKSPACE_ID,
        created.meta.id,
        1,
        "Improve title",
        ResumeProposalStatus.PENDING,
        (
            SetResumeField(
                ResumeOperationId("op_00000001"),
                str(created.meta.id),
                ("title",),
                "Staff Backend Resume",
            ),
        ),
    )
    store.proposals[(WORKSPACE_ID, proposal_id)] = proposal

    outcome = await service.decide_proposal(
        principal,
        WORKSPACE_ID,
        proposal_id,
        ProposalDecisionCommand(ProposalDecision.ACCEPT),
        expected_revision=1,
    )

    assert outcome.resume.title == "Staff Backend Resume"
    assert outcome.resume.meta.revision == 2
    saved = store.proposals[(WORKSPACE_ID, proposal_id)]
    assert saved.status is ResumeProposalStatus.ACCEPTED
    assert saved.meta.revision == 2
    assert saved.accepted_operation_ids == (ResumeOperationId("op_00000001"),)


def test_central_authorization_matrix_uses_resume_scopes_and_denies_viewer_writes() -> None:
    """@brief 验证 Resume 权限是 token scope 与 Workspace role 的交集 / Verify Resume permission is the intersection of token scope and role."""
    read = WORKSPACE_AUTHORIZATION_MATRIX[WorkspaceAction.READ_RESUME]
    write = WORKSPACE_AUTHORIZATION_MATRIX[WorkspaceAction.APPLY_RESUME_OPERATIONS]
    render = WORKSPACE_AUTHORIZATION_MATRIX[WorkspaceAction.CREATE_RESUME_RENDER_JOB]
    assert read.scope == Scope("resume.read")
    assert set(read.roles) == set(WorkspaceRole)
    assert write.scope == Scope("resume.write")
    assert write.roles == frozenset(
        {WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.EDITOR}
    )
    assert render.scope == Scope("resume.render")
    assert render.roles == write.roles
