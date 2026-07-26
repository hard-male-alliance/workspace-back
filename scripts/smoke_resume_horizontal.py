#!/usr/bin/env python3
"""Run the Resume horizontal-expansion smoke flow and emit a real PDF."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

from pypdf import PdfReader

from backend.config import RendererSettings
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resume_comparison import compare_resumes
from backend.domain.resume_editing import (
    ResumeEditPurpose,
    build_resume_edit_policy,
)
from backend.domain.resume_jobs import RenderFormat
from backend.domain.resume_quality import (
    ResumeDiagnosticSeverity,
    analyze_resume_quality,
)
from backend.domain.resumes import (
    ConflictStrategy,
    ContactKind,
    ContactMethod,
    DateRange,
    PartialDate,
    RenderHint,
    ResumeAggregate,
    ResumeBatchId,
    ResumeDocument,
    ResumeId,
    ResumeItem,
    ResumeItemKind,
    ResumeOperationBatch,
    ResumeOperationId,
    ResumeProfile,
    ResumeSection,
    ResumeSectionKind,
    RichText,
    TemplatePolicy,
    TemplateRef,
    UpsertResumeSection,
    clone_resume_document,
    create_resume_document,
)
from backend.infrastructure.rendering import SandboxedXeLaTeXRenderer
from backend.infrastructure.resume_worker import MultiFormatResumeRenderer
from backend.infrastructure.resumes import BuiltinResumeTemplateCatalog

_NOW = datetime(2026, 7, 25, 8, 0, tzinfo=UTC)
_WORKSPACE_ID = WorkspaceId("workspace_smoke_0001")
_ACTOR_ID = UserId("user_smoke_00000001")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Resume horizontal capabilities and generate a real PDF."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/resume-horizontal-smoke"),
    )
    parser.add_argument(
        "--logical-only",
        action="store_true",
        help="Run deterministic feature checks without invoking XeLaTeX.",
    )
    return parser.parse_args()


async def _main() -> int:
    arguments = _arguments()
    output_dir = arguments.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    catalog = BuiltinResumeTemplateCatalog()
    default_ref = TemplateRef("tpl_default_v1", "1.0")
    ats_ref = TemplateRef("tpl_ats_v1", "1.0")
    default_policy = await catalog.get_policy(default_ref)
    ats_policy = await catalog.get_policy(ats_ref)
    if default_policy is None or ats_policy is None:
        raise RuntimeError("required built-in Resume templates are unavailable")

    incomplete = create_resume_document(
        resume_id=ResumeId("resume_smoke_empty_01"),
        workspace_id=_WORKSPACE_ID,
        title="Incomplete Resume",
        locale="en-US",
        template_policy=default_policy,
        created_at=_NOW,
    )
    incomplete_report = analyze_resume_quality(incomplete)

    source = _realistic_resume(default_policy)
    default_policy.validate(source)
    complete_report = analyze_resume_quality(source)
    if any(
        item.severity is ResumeDiagnosticSeverity.ERROR
        for item in complete_report.diagnostics
    ):
        raise AssertionError("realistic Resume still contains blocking diagnostics")
    if complete_report.score <= incomplete_report.score:
        raise AssertionError("quality score did not improve after completing the Resume")

    single_section = build_resume_edit_policy(
        source,
        "请只优化工作经历，保持其他模块不变",
        has_authorized_evidence=False,
    )
    jd_tailoring = build_resume_edit_policy(
        source,
        "根据目标岗位 JD 优化工作经历，JD 要求 Python、FastAPI 和 PostgreSQL",
        has_authorized_evidence=True,
    )
    translation = build_resume_edit_policy(
        source,
        "把项目经历翻译成英文版",
        has_authorized_evidence=False,
    )
    if single_section.scope.target_section_ids != ("section_experience_01",):
        raise AssertionError("single-section edit scope was not enforced")
    if jd_tailoring.purpose is not ResumeEditPurpose.JOB_TAILORING:
        raise AssertionError("JD tailoring policy was not selected")
    if translation.purpose is not ResumeEditPurpose.TRANSLATION:
        raise AssertionError("translation policy was not selected")

    variant = clone_resume_document(
        source,
        resume_id=ResumeId("resume_smoke_variant_1"),
        workspace_id=_WORKSPACE_ID,
        title="ATS Backend Platform Resume",
        locale="en-US",
        template_policy=ats_policy,
        created_at=_NOW,
    )
    aggregate, _ = ResumeAggregate.create(variant, _ACTOR_ID)
    optimized_experience = replace(
        variant.sections[0],
        items=(
            replace(
                variant.sections[0].items[0],
                summary=RichText(
                    "Designed Python and FastAPI services backed by PostgreSQL, "
                    "preserving the verified 42% latency reduction."
                ),
            ),
        ),
    )
    operation = UpsertResumeSection(
        ResumeOperationId("operation_smoke_0001"),
        optimized_experience,
        None,
    )
    batch = ResumeOperationBatch(
        ResumeBatchId("batch_smoke_00000001"),
        1,
        ConflictStrategy.REJECT,
        (operation,),
        RenderHint.NONE,
    )
    accepted = aggregate.apply_batch(
        batch,
        at=_NOW.replace(minute=1),
        actor_id=_ACTOR_ID,
        template_policies={ats_ref: ats_policy},
    )
    final_document = accepted.aggregate.document
    if final_document.meta.revision != 2:
        raise AssertionError("accepted operation batch did not advance the revision")

    comparison = compare_resumes(variant, final_document)
    if not comparison.changed or not any(
        finding.entity_id == "item_experience_001"
        and ("summary",) in finding.field_paths
        for finding in comparison.findings
    ):
        raise AssertionError("revision comparison did not locate the optimized field")

    report: dict[str, Any] = {
        "quality": {
            "incomplete_score": incomplete_report.score,
            "complete_score": complete_report.score,
            "incomplete_diagnostic_codes": [
                item.code for item in incomplete_report.diagnostics
            ],
            "complete_diagnostic_codes": [
                item.code for item in complete_report.diagnostics
            ],
        },
        "edit_policies": {
            "single_section": single_section.as_provider_policy(),
            "jd_tailoring": jd_tailoring.as_provider_policy(),
            "translation": translation.as_provider_policy(),
        },
        "variant": {
            "source_resume_id": str(source.meta.id),
            "variant_resume_id": str(final_document.meta.id),
            "template_id": final_document.template.template_id,
            "revision": final_document.meta.revision,
            "applied_operation_ids": [
                str(value) for value in accepted.applied_operation_ids
            ],
        },
        "comparison": {
            "changed": comparison.changed,
            "findings": [
                {
                    "kind": finding.kind.value,
                    "entity_type": finding.entity_type,
                    "entity_id": finding.entity_id,
                    "field_paths": [list(path) for path in finding.field_paths],
                }
                for finding in comparison.findings
            ],
        },
    }

    if not arguments.logical_only:
        pdf_result = await _render_pdf(final_document, output_dir)
        report["pdf"] = pdf_result
    report_path = output_dir / "verification-report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps({"status": "passed", "report": str(report_path), **report}, ensure_ascii=False))
    return 0


def _realistic_resume(template_policy: TemplatePolicy) -> ResumeDocument:
    document = create_resume_document(
        resume_id=ResumeId("resume_smoke_source_01"),
        workspace_id=_WORKSPACE_ID,
        title="Backend Platform Engineer",
        locale="en-US",
        template_policy=template_policy,
        created_at=_NOW,
        full_name="Ming Wang",
    )
    profile = ResumeProfile(
        "Ming Wang",
        "Python / FastAPI Backend Engineer",
        RichText(
            "Backend engineer with five years of experience building reliable "
            "APIs, retrieval systems, and PostgreSQL platforms."
        ),
        (
            ContactMethod(
                "contact_email_0001",
                ContactKind.EMAIL,
                "Email",
                "ming.wang@example.com",
                None,
            ),
            ContactMethod(
                "contact_github_001",
                ContactKind.GITHUB,
                "GitHub",
                "github.com/mingwang",
                "https://github.com/mingwang",
            ),
        ),
    )
    experience = ResumeSection(
        "section_experience_01",
        ResumeSectionKind.EXPERIENCE,
        "Experience",
        items=(
            ResumeItem(
                "item_experience_001",
                ResumeItemKind.EXPERIENCE,
                title="Backend Engineer",
                organization="Northwind Cloud",
                location="Tianjin, China",
                date_range=DateRange(PartialDate("2021-07"), None, True),
                summary=RichText(
                    "Built Python and FastAPI services backed by PostgreSQL."
                ),
                highlights=(
                    RichText("Reduced API p95 latency by 42%."),
                    RichText("Improved deployment success rate to 99.5%."),
                ),
                skills=("Python", "FastAPI", "PostgreSQL", "Docker"),
            ),
        ),
    )
    projects = ResumeSection(
        "section_projects_001",
        ResumeSectionKind.PROJECTS,
        "Projects",
        items=(
            ResumeItem(
                "item_project_000001",
                ResumeItemKind.PROJECT,
                title="Evidence-grounded Resume Assistant",
                date_range=DateRange(
                    PartialDate("2025-01"),
                    PartialDate("2025-06"),
                ),
                summary=RichText(
                    "Implemented pgvector retrieval and citation-backed editing."
                ),
                highlights=(
                    RichText("Indexed 120,000 knowledge chunks."),
                    RichText("Reached 93% citation precision in evaluation."),
                ),
                skills=("PostgreSQL", "pgvector", "RAG"),
            ),
        ),
    )
    education = ResumeSection(
        "section_education_01",
        ResumeSectionKind.EDUCATION,
        "Education",
        items=(
            ResumeItem(
                "item_education_001",
                ResumeItemKind.EDUCATION,
                title="B.Eng. in Software Engineering",
                organization="Nankai University",
                date_range=DateRange(
                    PartialDate("2017-09"),
                    PartialDate("2021-06"),
                ),
                summary=RichText("Graduated with GPA 3.8/4.0."),
            ),
        ),
    )
    skills = ResumeSection(
        "section_skills_0001",
        ResumeSectionKind.SKILLS,
        "Skills",
        content=RichText(
            "Python, FastAPI, PostgreSQL, pgvector, Redis, Docker, Linux"
        ),
    )
    return replace(
        document,
        profile=profile,
        sections=(experience, projects, education, skills),
    )


async def _render_pdf(
    document: ResumeDocument,
    output_dir: Path,
) -> dict[str, Any]:
    xelatex = shutil.which("xelatex")
    if xelatex is None:
        raise RuntimeError(
            "XeLaTeX is unavailable; run this script inside the project runtime image"
        )
    renderer = MultiFormatResumeRenderer(
        SandboxedXeLaTeXRenderer(
            RendererSettings(
                adapter="xelatex",
                xelatex_command=xelatex,
                timeout_ms=30_000,
                max_input_bytes=1_048_576,
                max_output_bytes=10_485_760,
                memory_limit_bytes=1_073_741_824,
                allowed_font_directories=(),
                artifact_directory=output_dir,
            ),
            xelatex_path=xelatex,
        )
    )
    rendered = await renderer.render_resume(
        document,
        (RenderFormat.PDF,),
        operation_id="render_horizontal_smoke_0001",
    )
    artifact = rendered[0]
    output_path = output_dir / "backend-platform-resume-ats.pdf"
    output_path.write_bytes(artifact.content)
    if not artifact.content.startswith(b"%PDF-"):
        raise AssertionError("renderer output is not a PDF")
    reader = PdfReader(BytesIO(artifact.content))
    extracted = "\n".join(page.extract_text() or "" for page in reader.pages)
    normalized_text = extracted
    for hyphen in ("\u00ad", "\u2010", "\u2011", "\u2012", "\u2013", "\u2212"):
        normalized_text = normalized_text.replace(hyphen, "-")
    required_text = (
        "Ming Wang",
        "Experience",
        "Backend Engineer",
        "42%",
        "Evidence-grounded Resume Assistant",
        "Python",
        "PostgreSQL",
    )
    missing = [value for value in required_text if value not in normalized_text]
    if missing:
        raise AssertionError(f"rendered PDF is missing expected text: {missing}")
    return {
        "path": str(output_path),
        "content_type": artifact.media_type,
        "size_bytes": len(artifact.content),
        "page_count": len(reader.pages),
        "sha256": hashlib.sha256(artifact.content).hexdigest(),
        "text_assertions": list(required_text),
    }


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
