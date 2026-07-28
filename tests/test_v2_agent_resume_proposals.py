"""@brief Agent→Resume Proposal 防腐层测试 / Agent-to-Resume Proposal anti-corruption tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from backend.application.ports.agent_v2 import AgentModelRoute, AgentProposalFailure
from backend.domain.agent_v2 import AgentResumeContext, AgentResumeOperationDraft
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resumes import (
    DateRange,
    EntityKind,
    MoveResumeEntity,
    PageSize,
    PartialDate,
    RemoveResumeEntity,
    ResumeDomainError,
    ResumeId,
    ResumeItem,
    ResumeItemKind,
    ResumeOperation,
    ResumeOperationId,
    ResumeSection,
    ResumeSectionKind,
    SetResumeField,
    SetResumeTemplate,
    TemplatePolicy,
    TemplateRef,
    TemplateZonePolicy,
    UpsertResumeItem,
    UpsertResumeSection,
    create_resume_document,
    preview_resume_operations,
)
from backend.infrastructure.access import InMemoryAccessStore
from backend.infrastructure.agent_resume_proposals import (
    _materialize_operations,
    _persisted_invocation_status,
)
from backend.infrastructure.agent_v2 import (
    InMemoryAgentPolicyStore,
    InMemoryAgentStore,
    InMemoryAgentWorkerUnitOfWorkFactory,
)
from backend.infrastructure.resumes import (
    decode_resume_operation_wire,
    encode_resume_operation,
    encode_resume_operation_wire,
)

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 固定测试时间 / Fixed test instant."""

WORKSPACE_ID = WorkspaceId("workspace_agentproposal1")
"""@brief 测试 Workspace / Test Workspace."""

USER_ID = UserId("user_agentproposal01")
"""@brief 测试 actor / Test actor."""

TEMPLATE_REF = TemplateRef("template_agentproposal1", "1.0")
"""@brief 测试模板引用 / Test template reference."""


def _template_policy() -> TemplatePolicy:
    """@brief 构造 Proposal 预演所需模板策略 / Build the template policy used by Proposal previews."""

    kinds = frozenset(ResumeSectionKind)
    return TemplatePolicy(
        TEMPLATE_REF,
        frozenset({"zh-CN"}),
        frozenset({PageSize.A4}),
        frozenset({"pdf", "json"}),
        kinds,
        (TemplateZonePolicy("main", kinds, 100),),
        frozenset({"body.default"}),
        frozenset({"yyyy_mm"}),
        frozenset({"bullet.default"}),
        (),
    )


def _base_context() -> AgentResumeContext:
    """@brief 构造 revision=1 的精确 Resume 快照 / Build an exact revision-one Resume snapshot."""

    document = create_resume_document(
        resume_id=ResumeId("resume_agentproposal1"),
        workspace_id=WORKSPACE_ID,
        title="Distributed Systems Engineer",
        locale="zh-CN",
        template_policy=_template_policy(),
        created_at=NOW,
        full_name="Klee",
    )
    return AgentResumeContext(
        ResourceRef("resume", str(document.meta.id), document.meta.revision),
        document,
    )


def _draft(operation: ResumeOperation) -> AgentResumeOperationDraft:
    """@brief 从类型化 operation 构造无服务端 ID 草案 / Build an ID-free draft from a typed operation."""

    payload = encode_resume_operation_wire(operation)
    del payload["operation_id"]
    return AgentResumeOperationDraft(payload)


def test_materialization_validates_all_six_operations_and_derives_stable_ids() -> None:
    """@brief 六类草案均经服务端 union 校验并获稳定 ID / Validate all six drafts through the server union and derive stable IDs."""

    temporary_section = "temporary_section01"
    temporary_item_one = "temporary_item0001"
    temporary_item_two = "temporary_item0002"
    drafts = (
        _draft(
            UpsertResumeSection(
                ResumeOperationId("ignored_operation01"),
                ResumeSection(
                    temporary_section,
                    ResumeSectionKind.EXPERIENCE,
                    "Experience",
                    items=(
                        ResumeItem(
                            temporary_item_one,
                            ResumeItemKind.EXPERIENCE,
                            title="Backend Engineer",
                        ),
                    ),
                ),
                None,
            )
        ),
        _draft(
            UpsertResumeItem(
                ResumeOperationId("ignored_operation02"),
                temporary_section,
                ResumeItem(
                    temporary_item_two,
                    ResumeItemKind.PROJECT,
                    title="Consensus simulator",
                ),
                temporary_item_one,
            )
        ),
        _draft(
            SetResumeField(
                ResumeOperationId("ignored_operation03"),
                temporary_item_one,
                ("title",),
                "Senior Backend Engineer",
            )
        ),
        _draft(
            MoveResumeEntity(
                ResumeOperationId("ignored_operation04"),
                EntityKind.ITEM,
                temporary_item_two,
                temporary_section,
                None,
            )
        ),
        _draft(
            RemoveResumeEntity(
                ResumeOperationId("ignored_operation05"),
                EntityKind.ITEM,
                temporary_item_two,
            )
        ),
        _draft(
            SetResumeTemplate(
                ResumeOperationId("ignored_operation06"),
                TEMPLATE_REF,
                {},
            )
        ),
    )
    context = _base_context()

    first = _materialize_operations("agent_run_proposal01", context, drafts)
    replay = _materialize_operations("agent_run_proposal01", context, drafts)
    preview = preview_resume_operations(context.document, first)

    assert first == replay
    assert len({str(operation.operation_id) for operation in first}) == 6
    serialized = json.dumps(
        [encode_resume_operation(operation) for operation in first],
        sort_keys=True,
    )
    assert temporary_section not in serialized
    assert temporary_item_one not in serialized
    assert temporary_item_two not in serialized
    assert preview.sections[0].items[0].title == "Senior Backend Engineer"
    assert len(preview.sections[0].items) == 1


def test_materialization_decodes_dated_item_wire_shape() -> None:
    """@brief Proposal 边界解码公开日期格式 / Proposal boundary decodes the public date format."""

    draft = _draft(
        UpsertResumeSection(
            ResumeOperationId("ignored_operation_date01"),
            ResumeSection(
                "temporary_section_date01",
                ResumeSectionKind.EXPERIENCE,
                "工作经历",
                items=(
                    ResumeItem(
                        "temporary_item_date0001",
                        ResumeItemKind.EXPERIENCE,
                        title="高级前端工程师",
                        organization="杭州云衡科技有限公司",
                        date_range=DateRange(
                            PartialDate("2024-03"),
                            None,
                            present=True,
                        ),
                    ),
                ),
            ),
            None,
        )
    )

    operations = _materialize_operations(
        "agent_run_dated_proposal01",
        _base_context(),
        (draft,),
    )

    operation = operations[0]
    assert isinstance(operation, UpsertResumeSection)
    assert operation.section.items[0].date_range == DateRange(
        PartialDate("2024-03"),
        None,
        present=True,
    )


def _dated_item_wire(start: str, end: str | None) -> dict[str, object]:
    """@brief 构造日期归一化测试 operation / Build an operation for date-normalization tests."""

    return {
        "operation_id": "operation_date_normalization01",
        "op": "upsert_item",
        "section_id": "section_date_normalization01",
        "item": {
            "id": "item_date_normalization001",
            "kind": "experience",
            "title": "Engineer",
            "subtitle": None,
            "organization": None,
            "location": None,
            "date_range": {"start": start, "end": end},
            "summary": None,
            "highlights": [],
            "skills": [],
            "tags": [],
            "visible": True,
            "url": None,
        },
        "after_item_id": None,
    }


@pytest.mark.parametrize(
    ("start", "end", "expected_start", "expected_end", "present"),
    (
        ("2024", None, "2024", None, False),
        ("2024-3", None, "2024-03", None, False),
        ("2024.03", "至今", "2024-03", None, True),
        ("2024/03", "现在", "2024-03", None, True),
        ("2024年3月", "目前", "2024-03", None, True),
        ("2024年3月5日", "Present", "2024-03-05", None, True),
        ("２０２４－０３", "CURRENT", "2024-03", None, True),
        ("2022-7", "2024-2", "2022-07", "2024-02", False),
    ),
)
def test_wire_date_normalization_accepts_only_unambiguous_variants(
    start: str,
    end: str | None,
    expected_start: str,
    expected_end: str | None,
    present: bool,
) -> None:
    """@brief 无歧义本地日期归一化为领域日期 / Normalize unambiguous local dates."""

    operation = decode_resume_operation_wire(_dated_item_wire(start, end))

    assert isinstance(operation, UpsertResumeItem)
    assert operation.item.date_range == DateRange(
        PartialDate(expected_start),
        PartialDate(expected_end) if expected_end is not None else None,
        present=present,
    )


@pytest.mark.parametrize(
    "value",
    (
        "03/04/2024",
        "04-05",
        "24年3月",
        "去年3月",
        "2024年初",
        "2024 Q2",
        "2024-13",
        "2024-02-30",
        "",
    ),
)
def test_wire_date_normalization_rejects_ambiguous_or_invalid_variants(
    value: str,
) -> None:
    """@brief 歧义与非法日期不得被猜测 / Never guess ambiguous or invalid dates."""

    with pytest.raises(ResumeDomainError) as captured:
        decode_resume_operation_wire(_dated_item_wire(value, None))

    assert captured.value.code == "resume.invalid_date"


@pytest.mark.parametrize(
    ("provider_status", "persisted_status"),
    (
        ("completed", "completed"),
        ("decision_required", "decision_required"),
        ("invalid", "failed"),
        ("failure", "failed"),
    ),
)
def test_tool_diagnostic_status_matches_closed_persistence_contract(
    provider_status: str,
    persisted_status: str,
) -> None:
    """Richer provider outcomes must not violate the existing database constraint."""

    assert _persisted_invocation_status(provider_status) == persisted_status


@pytest.mark.parametrize(
    "mutation",
    ("unknown_top_level", "unknown_nested", "mismatched_discriminator"),
)
def test_materialization_rejects_fields_the_domain_codec_would_ignore(
    mutation: str,
) -> None:
    """@brief 拒绝未知字段与伪 discriminator，不依赖宽松 codec / Reject unknown fields and forged discriminators independently of a permissive codec."""

    payload = encode_resume_operation(
        UpsertResumeSection(
            ResumeOperationId("ignored_operation07"),
            ResumeSection(
                "temporary_section03",
                ResumeSectionKind.EXPERIENCE,
                "Experience",
            ),
            None,
        )
    )
    del payload["operation_id"]
    if mutation == "unknown_top_level":
        payload["untrusted_extra"] = True
    elif mutation == "unknown_nested":
        section = payload["section"]
        assert isinstance(section, dict)
        section["untrusted_extra"] = True
    else:
        payload["op"] = "set_template"

    with pytest.raises(ValueError, match=r"unknown|unsupported"):
        _materialize_operations(
            "agent_run_proposal02",
            _base_context(),
            (AgentResumeOperationDraft(payload),),
        )


@pytest.mark.asyncio
async def test_memory_agent_uow_fails_closed_for_durable_resume_proposals() -> None:
    """@brief memory UoW 对跨域持久 Proposal 明确返回 503 / Memory UoW explicitly returns 503 for cross-context durable Proposals."""

    factory = InMemoryAgentWorkerUnitOfWorkFactory(
        InMemoryAgentStore(),
        InMemoryAccessStore(),
        policy_store=InMemoryAgentPolicyStore(),
        model_routes=(
            AgentModelRoute(
                ResourceRef("model", "model_agentproposal1", 1),
                ModelRegion.GLOBAL,
                False,
            ),
        ),
    )
    unit = factory(WORKSPACE_ID, USER_ID)

    async with unit:
        with pytest.raises(AgentProposalFailure) as captured:
            await unit.resume_proposals.load_base(
                WORKSPACE_ID,
                ResourceRef("resume", "resume_agentproposal1", 1),
            )

    assert captured.value.problem.code == "service.durable_runtime_required"
    assert captured.value.problem.status == 503
    with pytest.raises(RuntimeError, match="has not been entered"):
        _ = unit.resume_proposals
