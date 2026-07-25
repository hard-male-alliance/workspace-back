"""Tests for server-owned AI Resume editing policy."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from backend.domain.principals import WorkspaceId
from backend.domain.resume_editing import (
    ResumeEditPurpose,
    ResumeEditScope,
    build_resume_edit_policy,
    resolve_resume_edit_scope,
)
from backend.domain.resumes import (
    PageSize,
    ResumeId,
    ResumeSection,
    ResumeSectionKind,
    TemplatePolicy,
    TemplateRef,
    TemplateZonePolicy,
    create_resume_document,
)


def _document():
    kinds = frozenset(ResumeSectionKind)
    policy = TemplatePolicy(
        TemplateRef("template_00000001", "1.0"),
        frozenset({"zh-CN"}),
        frozenset({PageSize.A4}),
        frozenset({"pdf"}),
        kinds,
        (TemplateZonePolicy("main", kinds),),
        frozenset({"body.default"}),
        frozenset({"yyyy_mm"}),
        frozenset({"bullet.default"}),
    )
    document = create_resume_document(
        resume_id=ResumeId("resume_00000001"),
        workspace_id=WorkspaceId("workspace_00000001"),
        title="Backend Engineer",
        locale="zh-CN",
        template_policy=policy,
        created_at=datetime(2026, 7, 24, tzinfo=UTC),
    )
    return replace(
        document,
        sections=(
            ResumeSection(
                "section_experience_01",
                ResumeSectionKind.EXPERIENCE,
                "工作经历",
            ),
            ResumeSection(
                "section_projects_001",
                ResumeSectionKind.PROJECTS,
                "项目经历",
            ),
        ),
    )


def test_explicit_section_title_resolves_to_single_section_scope() -> None:
    scope = resolve_resume_edit_scope(_document(), "请优化工作经历，但不要改其他模块")

    assert scope == ResumeEditScope("single_section", ("section_experience_01",))
    policy = scope.as_provider_policy()
    assert policy["target_section_ids"] == ["section_experience_01"]
    assert "Only draft operations" in str(policy["instruction"])


def test_ambiguous_multi_section_request_is_not_silently_narrowed() -> None:
    scope = resolve_resume_edit_scope(_document(), "同时优化工作经历和项目经历")

    assert scope == ResumeEditScope("whole_document", ())


def test_stable_section_id_can_define_scope_without_localized_title() -> None:
    scope = resolve_resume_edit_scope(
        _document(),
        "Improve section_projects_001 for clarity",
    )

    assert scope.target_section_ids == ("section_projects_001",)


def test_job_tailoring_policy_separates_requirements_from_candidate_facts() -> None:
    policy = build_resume_edit_policy(
        _document(),
        "根据这份目标岗位 JD 优化工作经历",
        has_authorized_evidence=True,
    )

    encoded = policy.as_provider_policy()
    assert policy.purpose is ResumeEditPurpose.JOB_TAILORING
    assert encoded["scope"] == "single_section"
    assert encoded["has_authorized_evidence"] is True
    constraints = " ".join(str(item) for item in encoded["constraints"])
    assert "not proof" in constraints
    assert "Never invent" in constraints
    assert "citation" in constraints


def test_general_edit_does_not_claim_missing_evidence() -> None:
    policy = build_resume_edit_policy(
        _document(),
        "精简措辞",
        has_authorized_evidence=False,
    )

    assert policy.purpose is ResumeEditPurpose.GENERAL
    assert policy.as_provider_policy()["has_authorized_evidence"] is False


def test_translation_policy_preserves_facts_and_respects_section_scope() -> None:
    policy = build_resume_edit_policy(
        _document(),
        "把项目经历翻译成英文版",
        has_authorized_evidence=False,
    )

    encoded = policy.as_provider_policy()
    assert policy.purpose is ResumeEditPurpose.TRANSLATION
    assert encoded["target_section_ids"] == ["section_projects_001"]
    constraints = " ".join(str(item) for item in encoded["constraints"])
    assert "Preserve dates, numbers, URLs" in constraints
    assert "Do not optimize" in constraints


def test_translation_takes_precedence_over_incidental_job_wording() -> None:
    policy = build_resume_edit_policy(
        _document(),
        "翻译目标岗位相关的工作经历",
        has_authorized_evidence=False,
    )

    assert policy.purpose is ResumeEditPurpose.TRANSLATION
