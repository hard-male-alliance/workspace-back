"""Tests for the Run-bound, fine-grained Resume tool instruction set."""

from datetime import UTC, datetime

import pytest

from backend.domain.agent_v2 import AgentResumeContext
from backend.domain.principals import ResourceMeta, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resumes import (
    PageSize,
    ResumeId,
    ResumeSectionKind,
    TemplatePolicy,
    TemplateRef,
    TemplateZonePolicy,
    create_resume_document,
)
from backend.infrastructure.agent_prompt import render_resume_agent_system_prompt
from backend.infrastructure.resume_agent_tools import (
    ResumeToolSession,
    resume_agent_tools,
    tool_catalog,
)


def _context() -> AgentResumeContext:
    now = datetime(2026, 7, 27, tzinfo=UTC)
    section_kinds = frozenset(ResumeSectionKind)
    policy = TemplatePolicy(
        ref=TemplateRef("template_basic", "1"),
        supported_locales=frozenset({"zh-CN"}),
        supported_page_sizes=frozenset({PageSize.A4}),
        supported_output_formats=frozenset({"pdf"}),
        supported_section_kinds=section_kinds,
        zones=(TemplateZonePolicy("main", section_kinds, 100),),
        font_family_tokens=frozenset({"sans"}),
        date_format_tokens=frozenset({"yyyy-mm"}),
        bullet_style_tokens=frozenset({"disc"}),
    )
    document = create_resume_document(
        resume_id=ResumeId("resume_tool_test"),
        workspace_id=WorkspaceId("workspace_tool_test"),
        title="Tool test",
        locale="zh-CN",
        template_policy=policy,
        created_at=now,
    )
    assert document.meta == ResourceMeta(
        ResumeId("resume_tool_test"),
        1,
        now,
        now,
    )
    return AgentResumeContext(ResourceRef("resume", "resume_tool_test", 1), document)


def test_prompt_is_a_resource_and_assigns_orchestration_to_the_agent() -> None:
    prompt = render_resume_agent_system_prompt(response_locale="zh-CN")

    assert "client does not classify intent" in prompt
    assert "ProposalDecision" in prompt
    assert "`resume_draft_set_profile_field`" in prompt
    assert "`resume_draft_upsert_section`" in prompt
    assert "`resume_request_proposal_decision`" in prompt
    assert "tmp_section_projects_01" in prompt
    assert '"kind":"projects"' in prompt
    assert '"kind":"project"' in prompt
    assert '"visible":true,"content":null,"items"' in prompt
    assert "conversation only; no tool and no proposal" in prompt
    assert "no Resume root ID is supplied or guessed" in prompt
    assert '"kind":"invalid_draft"' in prompt
    assert "do not retry or continue the earlier failed edit" in prompt
    assert "ask before staging any change" in prompt
    assert "Respond in zh-CN" in prompt


@pytest.mark.asyncio
async def test_tools_hide_run_metadata_and_stage_small_operations() -> None:
    session = ResumeToolSession(_context())
    tools = {tool.name: tool for tool in resume_agent_tools(session)}
    catalog = tool_catalog(tuple(tools.values()))

    assert len(tools) == 12
    assert all(
        "workspace_id" not in str(item)
        and "resume_id" not in str(item)
        and "revision" not in str(item)
        and "actor_id" not in str(item)
        for item in catalog
    )

    profile = await tools["resume_read_profile"].ainvoke({})
    assert '"kind":"resume_profile"' in profile

    staged_profile = await tools["resume_draft_set_profile_field"].ainvoke(
        {
            "field": "full_name",
            "value": "Klee",
        }
    )
    assert staged_profile == '{"kind":"resume_change_staged","operation_number":1}'
    assert session.drafts[0].payload == {
        "op": "set_field",
        "entity_id": "resume_tool_test",
        "field_path": ("profile", "full_name"),
        "value": "Klee",
    }

    invalid = await tools["resume_draft_set_field"].ainvoke(
        {
            "entity_id": "section_does_not_exist",
            "field_path": ["title"],
            "value": "Invalid",
        }
    )
    assert invalid == (
        '{"kind":"invalid_draft","code":"resume.entity_not_found","recoverable":true}'
    )
    assert len(session.drafts) == 1

    staged = await tools["resume_draft_set_field"].ainvoke(
        {
            "entity_id": "resume_tool_test",
            "field_path": ["title"],
            "value": "A better title",
        }
    )
    assert staged == '{"kind":"resume_change_staged","operation_number":2}'
    assert session.drafts[1].payload["op"] == "set_field"
    assert "operation_id" not in session.drafts[1].payload

    decision = await tools["resume_request_proposal_decision"].ainvoke(
        {"title": "Improve the Resume title"}
    )
    assert decision == '{"kind":"proposal_decision_required","operation_count":2}'
    assert session.proposal_title == "Improve the Resume title"
