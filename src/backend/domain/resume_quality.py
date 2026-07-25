"""Deterministic, provider-independent Resume quality diagnostics."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from backend.domain.resumes import (
    ContactKind,
    ResumeDocument,
    ResumeItem,
    ResumeSectionKind,
)

_METRIC = re.compile(r"(?:\d+(?:[.,]\d+)?%?|[一二三四五六七八九十百千万]+(?:个|项|次|人|万)?)")
_PLACEHOLDER_NAMES = frozenset({"untitled candidate", "未命名求职者", "未命名候选人"})
_CORE_ITEM_SECTIONS = frozenset(
    {
        ResumeSectionKind.EXPERIENCE,
        ResumeSectionKind.EDUCATION,
        ResumeSectionKind.PROJECTS,
    }
)


class ResumeDiagnosticSeverity(StrEnum):
    """Stable diagnostic severity ordered by user impact."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True, slots=True)
class ResumeDiagnostic:
    """One actionable finding anchored to a stable Resume entity."""

    code: str
    severity: ResumeDiagnosticSeverity
    message: str
    entity_id: str
    field_path: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResumeQualityReport:
    """Immutable diagnostics for one exact Resume revision."""

    resume_id: str
    revision: int
    score: int
    diagnostics: tuple[ResumeDiagnostic, ...]

    def __post_init__(self) -> None:
        if not 0 <= self.score <= 100:
            raise ValueError("Resume quality score must be between zero and 100")


def analyze_resume_quality(document: ResumeDocument) -> ResumeQualityReport:
    """Analyze structural completeness without calling a model or changing the Resume."""

    findings: list[ResumeDiagnostic] = []
    profile = document.profile
    resume_id = str(document.meta.id)
    if profile.full_name.strip().casefold() in _PLACEHOLDER_NAMES:
        findings.append(
            _finding(
                "resume.quality.placeholder_name",
                ResumeDiagnosticSeverity.ERROR,
                "Replace the placeholder candidate name.",
                resume_id,
                "profile",
                "full_name",
            )
        )
    if not profile.headline or not profile.headline.strip():
        findings.append(
            _finding(
                "resume.quality.missing_headline",
                ResumeDiagnosticSeverity.WARNING,
                "Add a concise professional headline.",
                resume_id,
                "profile",
                "headline",
            )
        )
    if profile.summary is None or not profile.summary.text.strip():
        findings.append(
            _finding(
                "resume.quality.missing_summary",
                ResumeDiagnosticSeverity.WARNING,
                "Add a factual professional summary.",
                resume_id,
                "profile",
                "summary",
            )
        )
    contact_kinds = {contact.kind for contact in profile.contacts if contact.value.strip()}
    if ContactKind.EMAIL not in contact_kinds and ContactKind.PHONE not in contact_kinds:
        findings.append(
            _finding(
                "resume.quality.missing_direct_contact",
                ResumeDiagnosticSeverity.ERROR,
                "Add at least one email address or phone number.",
                resume_id,
                "profile",
                "contacts",
            )
        )

    visible_sections = [section for section in document.sections if section.visible]
    visible_kinds = {section.kind for section in visible_sections}
    if ResumeSectionKind.EXPERIENCE not in visible_kinds:
        findings.append(
            _finding(
                "resume.quality.missing_experience",
                ResumeDiagnosticSeverity.WARNING,
                "Add a visible experience section when applicable.",
                resume_id,
                "sections",
            )
        )
    if ResumeSectionKind.SKILLS not in visible_kinds:
        findings.append(
            _finding(
                "resume.quality.missing_skills",
                ResumeDiagnosticSeverity.WARNING,
                "Add a visible skills section.",
                resume_id,
                "sections",
            )
        )

    duplicate_keys: dict[tuple[str, str, str], str] = {}
    for section in visible_sections:
        if (
            section.kind in _CORE_ITEM_SECTIONS
            and not section.items
            and (section.content is None or not section.content.text.strip())
        ):
            findings.append(
                _finding(
                    "resume.quality.empty_section",
                    ResumeDiagnosticSeverity.WARNING,
                    "Remove this empty section or add relevant content.",
                    section.id,
                    "items",
                )
            )
        for item in (item for item in section.items if item.visible):
            _analyze_item(item, findings)
            key = (
                item.kind.value,
                (item.title or "").strip().casefold(),
                (item.organization or "").strip().casefold(),
            )
            if key[1] and key in duplicate_keys:
                findings.append(
                    _finding(
                        "resume.quality.possible_duplicate_item",
                        ResumeDiagnosticSeverity.WARNING,
                        "This item appears to duplicate another visible entry.",
                        item.id,
                    )
                )
            elif key[1]:
                duplicate_keys[key] = item.id

    penalty = sum(
        {
            ResumeDiagnosticSeverity.ERROR: 15,
            ResumeDiagnosticSeverity.WARNING: 7,
            ResumeDiagnosticSeverity.INFO: 2,
        }[finding.severity]
        for finding in findings
    )
    return ResumeQualityReport(
        resume_id,
        document.meta.revision,
        max(0, 100 - penalty),
        tuple(findings),
    )


def _analyze_item(item: ResumeItem, findings: list[ResumeDiagnostic]) -> None:
    if not item.title or not item.title.strip():
        findings.append(
            _finding(
                "resume.quality.missing_item_title",
                ResumeDiagnosticSeverity.ERROR,
                "Add a title to this entry.",
                item.id,
                "title",
            )
        )
    if item.date_range is None:
        findings.append(
            _finding(
                "resume.quality.missing_item_dates",
                ResumeDiagnosticSeverity.INFO,
                "Add dates when they are relevant and known.",
                item.id,
                "date_range",
            )
        )
    substantive_text = " ".join(
        text
        for text in (
            item.summary.text if item.summary is not None else "",
            *(highlight.text for highlight in item.highlights),
        )
        if text.strip()
    )
    if not substantive_text:
        findings.append(
            _finding(
                "resume.quality.missing_item_evidence",
                ResumeDiagnosticSeverity.WARNING,
                "Add responsibilities, outcomes, or other factual evidence.",
                item.id,
                "highlights",
            )
        )
    elif not _METRIC.search(substantive_text):
        findings.append(
            _finding(
                "resume.quality.unquantified_item",
                ResumeDiagnosticSeverity.INFO,
                "Quantify impact when a supported metric is available.",
                item.id,
                "highlights",
            )
        )


def _finding(
    code: str,
    severity: ResumeDiagnosticSeverity,
    message: str,
    entity_id: str,
    *field_path: str,
) -> ResumeDiagnostic:
    return ResumeDiagnostic(code, severity, message, entity_id, tuple(field_path))
