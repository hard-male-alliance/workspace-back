"""Tests for value-free Resume revision and variant comparison."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from backend.domain.principals import WorkspaceId
from backend.domain.resume_comparison import ResumeChangeKind, compare_resumes
from backend.domain.resumes import (
    PageSize,
    ResumeId,
    ResumeItem,
    ResumeItemKind,
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
        frozenset({"en-US"}),
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
        title="Backend Resume",
        locale="en-US",
        template_policy=policy,
        created_at=datetime(2026, 7, 24, tzinfo=UTC),
        full_name="Klee",
    )
    return replace(
        document,
        sections=(
            ResumeSection(
                "section_experience_01",
                ResumeSectionKind.EXPERIENCE,
                "Experience",
                items=(
                    ResumeItem(
                        "item_experience_001",
                        ResumeItemKind.EXPERIENCE,
                        title="Backend Engineer",
                    ),
                ),
            ),
        ),
    )


def test_comparison_reports_paths_but_not_sensitive_values() -> None:
    left = _document()
    updated_item = replace(
        left.sections[0].items[0],
        title="Senior Backend Engineer",
    )
    added_item = ResumeItem(
        "item_experience_002",
        ResumeItemKind.EXPERIENCE,
        title="Platform Engineer",
    )
    updated_section = replace(
        left.sections[0],
        items=(updated_item, added_item),
    )
    right = replace(
        left,
        meta=replace(left.meta, revision=2),
        title="Platform Resume",
        sections=(updated_section,),
    )

    comparison = compare_resumes(left, right)

    assert comparison.changed is True
    assert comparison.left_revision == 1
    assert comparison.right_revision == 2
    assert any(
        finding.entity_type == "resume"
        and finding.field_paths == (("title",),)
        for finding in comparison.findings
    )
    assert any(
        finding.kind is ResumeChangeKind.MODIFIED
        and finding.entity_id == "item_experience_001"
        and finding.field_paths == (("title",),)
        for finding in comparison.findings
    )
    assert any(
        finding.kind is ResumeChangeKind.ADDED
        and finding.entity_id == "item_experience_002"
        for finding in comparison.findings
    )
    assert "Senior Backend Engineer" not in repr(comparison)


def test_identical_snapshots_have_no_findings() -> None:
    document = _document()

    comparison = compare_resumes(document, document)

    assert comparison.changed is False
    assert comparison.findings == ()
