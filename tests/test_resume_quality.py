"""Tests for deterministic Resume quality diagnostics."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from backend.domain.principals import WorkspaceId
from backend.domain.resume_quality import (
    ResumeDiagnosticSeverity,
    analyze_resume_quality,
)
from backend.domain.resumes import (
    ContactKind,
    ContactMethod,
    PageSize,
    ResumeId,
    ResumeItem,
    ResumeItemKind,
    ResumeProfile,
    ResumeSection,
    ResumeSectionKind,
    RichText,
    TemplatePolicy,
    TemplateRef,
    TemplateZonePolicy,
    create_resume_document,
)


def _policy() -> TemplatePolicy:
    kinds = frozenset(ResumeSectionKind)
    return TemplatePolicy(
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


def _document():
    return create_resume_document(
        resume_id=ResumeId("resume_00000001"),
        workspace_id=WorkspaceId("workspace_00000001"),
        title="Backend Engineer",
        locale="zh-CN",
        template_policy=_policy(),
        created_at=datetime(2026, 7, 24, tzinfo=UTC),
        full_name="未命名求职者",
    )


def test_quality_report_finds_structural_and_content_gaps() -> None:
    item = ResumeItem(
        "item_00000001",
        ResumeItemKind.EXPERIENCE,
        title="Backend Engineer",
        summary=RichText("负责接口开发"),
    )
    section = ResumeSection(
        "section_00000001",
        ResumeSectionKind.EXPERIENCE,
        "工作经历",
        items=(item, replace(item, id="item_00000002")),
    )
    document = replace(_document(), sections=(section,))

    report = analyze_resume_quality(document)

    codes = {finding.code for finding in report.diagnostics}
    assert report.resume_id == "resume_00000001"
    assert report.revision == 1
    assert report.score < 100
    assert "resume.quality.placeholder_name" in codes
    assert "resume.quality.missing_direct_contact" in codes
    assert "resume.quality.missing_skills" in codes
    assert "resume.quality.unquantified_item" in codes
    assert "resume.quality.possible_duplicate_item" in codes
    assert any(
        finding.severity is ResumeDiagnosticSeverity.ERROR
        for finding in report.diagnostics
    )


def test_complete_resume_has_no_error_diagnostics() -> None:
    profile = ResumeProfile(
        "Klee",
        "Python Backend Engineer",
        RichText("五年后端开发经验。"),
        (
            ContactMethod(
                "contact_00000001",
                ContactKind.EMAIL,
                None,
                "klee@example.com",
                None,
            ),
        ),
    )
    experience = ResumeSection(
        "section_00000001",
        ResumeSectionKind.EXPERIENCE,
        "工作经历",
        items=(
            ResumeItem(
                "item_00000001",
                ResumeItemKind.EXPERIENCE,
                title="Backend Engineer",
                summary=RichText("将接口延迟降低 35%。"),
            ),
        ),
    )
    skills = ResumeSection(
        "section_00000002",
        ResumeSectionKind.SKILLS,
        "技能",
        content=RichText("Python、FastAPI、PostgreSQL"),
    )
    report = analyze_resume_quality(
        replace(_document(), profile=profile, sections=(experience, skills))
    )

    assert all(
        finding.severity is not ResumeDiagnosticSeverity.ERROR
        for finding in report.diagnostics
    )
