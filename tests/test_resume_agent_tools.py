"""Tests for the Run-bound, fine-grained Resume tool instruction set."""

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

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
    assert "`resume_draft_set_contacts`" in prompt
    assert "`resume_draft_upsert_section`" in prompt
    assert "`resume_read_snapshot`" in prompt
    assert "`resume_draft_set_fields`" in prompt
    assert "`resume_draft_upsert_sections`" in prompt
    assert "`resume_draft_upsert_items`" in prompt
    assert "`resume_draft_operations`" not in prompt
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
    assert "个人简介 + 专业技能" in prompt
    assert "`invalid_tool_arguments`" in prompt
    assert "`2024.03`" in prompt
    assert "`03/04/2024`" in prompt
    assert "Respond in zh-CN" in prompt


@pytest.mark.asyncio
async def test_tools_hide_run_metadata_and_stage_small_operations() -> None:
    session = ResumeToolSession(_context())
    tools = {tool.name: tool for tool in resume_agent_tools(session)}
    catalog = tool_catalog(tuple(tools.values()))

    assert len(tools) == 18
    assert all(
        "workspace_id" not in str(item)
        and "resume_id" not in str(item)
        and "revision" not in str(item)
        and "actor_id" not in str(item)
        for item in catalog
    )
    batch_schema = next(
        item["input_schema"]
        for item in catalog
        if item["name"] == "resume_draft_set_fields"
    )
    assert "updates" in batch_schema["properties"]
    assert "discriminator" not in str(batch_schema)
    assert "resume_draft_operations" not in tools

    profile = await tools["resume_read_profile"].ainvoke({})
    assert '"kind":"resume_profile"' in profile
    snapshot = await tools["resume_read_snapshot"].ainvoke({})
    assert '"kind":"resume_snapshot"' in snapshot
    assert '"id":"resume_tool_test"' in snapshot
    assert "workspace_tool_test" not in snapshot
    assert '"revision"' not in snapshot

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

    contacts = await tools["resume_draft_set_contacts"].ainvoke(
        {
            "contacts": [
                {
                    "id": "tmp_contact_email_01",
                    "kind": "email",
                    "label": "邮箱",
                    "value": "klee@example.com",
                    "url": None,
                }
            ]
        }
    )
    assert contacts == '{"kind":"resume_change_staged","operation_number":2}'
    assert session.drafts[1].payload["field_path"] == ("profile", "contacts")

    with pytest.raises(ValidationError):
        await tools["resume_draft_set_profile_field"].ainvoke(
            {
                "field": "contacts",
                "value": [],
            }
        )

    invalid = await tools["resume_draft_set_field"].ainvoke(
        {
            "entity_id": "section_does_not_exist",
            "field_path": ["title"],
            "value": "Invalid",
        }
    )
    invalid_result = json.loads(invalid)
    assert invalid_result["kind"] == "invalid_draft"
    assert invalid_result["code"] == "resume.entity_not_found"
    assert invalid_result["issues"] == [
        {
            "operation_index": 0,
            "path": "title",
            "issue": "resume.entity_not_found",
        }
    ]
    assert invalid_result["retry"]["suggested_tool"] == "resume_draft_set_field"
    assert invalid_result["draft_state"]["operation_count"] == 2
    assert len(session.drafts) == 2

    staged = await tools["resume_draft_set_field"].ainvoke(
        {
            "entity_id": "resume_tool_test",
            "field_path": ["title"],
            "value": "A better title",
        }
    )
    assert staged == '{"kind":"resume_change_staged","operation_number":3}'
    assert session.drafts[2].payload["op"] == "set_field"
    assert "operation_id" not in session.drafts[2].payload

    decision = await tools["resume_request_proposal_decision"].ainvoke(
        {"title": "Improve the Resume title"}
    )
    assert decision == '{"kind":"proposal_decision_required","operation_count":3}'
    assert session.proposal_title == "Improve the Resume title"


@pytest.mark.asyncio
async def test_homogeneous_batch_tool_stages_all_operations_or_none() -> None:
    session = ResumeToolSession(_context())
    tools = {tool.name: tool for tool in resume_agent_tools(session)}

    invalid = await tools["resume_draft_set_fields"].ainvoke(
        {
            "updates": [
                {
                    "entity_id": "resume_tool_test",
                    "field_path": ["profile", "full_name"],
                    "value": "Klee",
                },
                {
                    "entity_id": "missing_section",
                    "field_path": ["title"],
                    "value": "Invalid",
                },
            ]
        }
    )

    invalid_result = json.loads(invalid)
    assert invalid_result["code"] == "resume.entity_not_found"
    assert invalid_result["issues"][0]["operation_index"] == 1
    assert invalid_result["retry"]["suggested_tool"] == "resume_draft_set_field"
    assert session.drafts == ()

    staged = await tools["resume_draft_set_fields"].ainvoke(
        {
            "updates": [
                {
                    "entity_id": "resume_tool_test",
                    "field_path": ["profile", "full_name"],
                    "value": "Klee",
                },
                {
                    "entity_id": "resume_tool_test",
                    "field_path": ["title"],
                    "value": "Focused Resume",
                },
            ]
        }
    )

    staged_result = json.loads(staged)
    assert staged_result["kind"] == "resume_change_batch_staged"
    assert staged_result["operation_count"] == 2
    assert staged_result["inserted_operation_count"] == 2
    assert staged_result["draft_state"]["operations"] == [
        {"number": 1, "op": "set_field"},
        {"number": 2, "op": "set_field"},
    ]
    assert len(session.drafts) == 2


@pytest.mark.asyncio
async def test_corrected_operation_replaces_the_same_draft_slot() -> None:
    """@brief 工具纠错不得追加冲突草稿 / Tool correction must replace the same draft slot."""

    session = ResumeToolSession(_context())
    tools = {tool.name: tool for tool in resume_agent_tools(session)}

    await tools["resume_draft_set_profile_field"].ainvoke(
        {"field": "headline", "value": "Frontend Engineer"}
    )
    corrected = await tools["resume_draft_set_profile_field"].ainvoke(
        {"field": "headline", "value": "Senior Frontend Engineer"}
    )

    assert json.loads(corrected)["operation_number"] == 1
    assert len(session.drafts) == 1
    assert session.drafts[0].payload["value"] == "Senior Frontend Engineer"


@pytest.mark.asyncio
async def test_profile_and_new_sections_use_small_homogeneous_batches() -> None:
    """@brief 长需求可用少量强类型调用新增模块 / Add sections with a few strongly typed calls."""

    session = ResumeToolSession(_context())
    tools = {tool.name: tool for tool in resume_agent_tools(session)}

    profile = await tools["resume_draft_set_profile_fields"].ainvoke(
        {
            "updates": [
                {"field": "full_name", "value": "林泽宇"},
                {"field": "headline", "value": "高级前端工程师"},
            ]
        }
    )
    sections = await tools["resume_draft_upsert_sections"].ainvoke(
        {
            "updates": [
                {
                    "section": {
                        "id": "tmp_section_skills_01",
                        "kind": "skills",
                        "title": "专业技能",
                        "visible": True,
                        "content": {
                            "text": "Vue、React、TypeScript、Node.js",
                            "marks": [],
                        },
                        "items": [],
                    },
                    "after_section_id": None,
                },
                {
                    "section": {
                        "id": "tmp_section_profile_01",
                        "kind": "custom",
                        "title": "个人简介",
                        "visible": True,
                        "content": {
                            "text": "四年前端开发经验，熟悉工程化与数据可视化。",
                            "marks": [],
                        },
                        "items": [],
                    },
                    "after_section_id": "tmp_section_skills_01",
                },
            ]
        }
    )

    profile_result = json.loads(profile)
    sections_result = json.loads(sections)
    assert profile_result["operation_count"] == 2
    assert sections_result["operation_count"] == 2
    assert sections_result["inserted_operation_count"] == 2
    assert len(session.drafts) == 4
    assert [draft.payload["op"] for draft in session.drafts] == [
        "set_field",
        "set_field",
        "upsert_section",
        "upsert_section",
    ]


@pytest.mark.asyncio
async def test_dated_experience_section_uses_public_date_range_shape() -> None:
    """@brief Agent 可用公开日期结构新增经历 / Agent can add experience with the public date shape."""

    session = ResumeToolSession(_context())
    tools = {tool.name: tool for tool in resume_agent_tools(session)}

    staged = await tools["resume_draft_upsert_section"].ainvoke(
        {
            "section": {
                "id": "tmp_section_experience_01",
                "kind": "experience",
                "title": "工作经历",
                "visible": True,
                "content": None,
                "items": [
                    {
                        "id": "tmp_item_experience_01",
                        "kind": "experience",
                        "title": "高级前端工程师",
                        "subtitle": None,
                        "organization": "杭州云衡科技有限公司",
                        "location": "杭州",
                        "date_range": {
                            "start": "2024-03",
                            "end": "present",
                        },
                        "summary": None,
                        "highlights": [
                            {
                                "text": "负责企业级数据分析平台建设。",
                                "marks": [],
                            }
                        ],
                        "skills": ["Vue 3", "TypeScript"],
                        "tags": [],
                        "visible": True,
                        "url": None,
                    }
                ],
            },
            "after_section_id": None,
        }
    )

    assert json.loads(staged)["kind"] == "resume_change_staged"
    date_range = session.drafts[0].payload["section"]["items"][0]["date_range"]
    assert date_range == {"start": "2024-03", "end": "present"}


@pytest.mark.asyncio
async def test_invalid_public_date_preserves_domain_error_code() -> None:
    """@brief 非法公开日期返回精确错误码 / Invalid public dates preserve the exact domain code."""

    session = ResumeToolSession(_context())
    tools = {tool.name: tool for tool in resume_agent_tools(session)}

    result = await tools["resume_draft_upsert_item"].ainvoke(
        {
            "section_id": "section_existing_01",
            "item": {
                "id": "tmp_item_experience_02",
                "kind": "experience",
                "title": "前端工程师",
                "subtitle": None,
                "organization": "示例公司",
                "location": None,
                "date_range": {
                    "start": "03/04/2024",
                    "end": "present",
                },
                "summary": None,
                "highlights": [],
                "skills": [],
                "tags": [],
                "visible": True,
                "url": None,
            },
            "after_item_id": None,
        }
    )

    decoded = json.loads(result)
    assert decoded["code"] == "resume.invalid_date"
    assert decoded["diagnostics"]["date_rejection_reason"] == "ambiguous_order"


@pytest.mark.asyncio
async def test_tool_normalizes_local_date_without_persisting_raw_variant() -> None:
    """@brief 工具保存规范日期并报告脱敏计数 / Store canonical dates and report a safe count."""

    session = ResumeToolSession(_context())
    tools = {tool.name: tool for tool in resume_agent_tools(session)}

    result = await tools["resume_draft_upsert_section"].ainvoke(
        {
            "section": {
                "id": "tmp_section_experience_03",
                "kind": "experience",
                "title": "工作经历",
                "visible": True,
                "content": None,
                "items": [
                    {
                        "id": "tmp_item_experience_03",
                        "kind": "experience",
                        "title": "前端工程师",
                        "subtitle": None,
                        "organization": "示例公司",
                        "location": None,
                        "date_range": {
                            "start": "２０２４．３",
                            "end": "至今",
                        },
                        "summary": None,
                        "highlights": [],
                        "skills": [],
                        "tags": [],
                        "visible": True,
                        "url": None,
                    }
                ],
            },
            "after_section_id": None,
        }
    )

    decoded = json.loads(result)
    assert decoded["diagnostics"]["date_normalization_count"] == 1
    date_range = session.drafts[0].payload["section"]["items"][0]["date_range"]
    assert date_range == {"start": "2024-03", "end": "present"}
