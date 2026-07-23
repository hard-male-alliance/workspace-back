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
    EntityKind,
    MoveResumeEntity,
    PageSize,
    RemoveResumeEntity,
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
from backend.infrastructure.agent_resume_proposals import _materialize_operations
from backend.infrastructure.agent_v2 import (
    InMemoryAgentPolicyStore,
    InMemoryAgentStore,
    InMemoryAgentWorkerUnitOfWorkFactory,
)
from backend.infrastructure.resumes import encode_resume_operation

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

    payload = encode_resume_operation(operation)
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
