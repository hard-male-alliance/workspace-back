"""Server-owned policy for reviewable AI Resume edits."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from backend.domain.resumes import ResumeDocument, ResumeSectionKind

_SECTION_ALIASES: dict[ResumeSectionKind, tuple[str, ...]] = {
    ResumeSectionKind.EXPERIENCE: ("experience", "work history", "工作经历", "工作经验"),
    ResumeSectionKind.EDUCATION: ("education", "教育经历", "教育背景"),
    ResumeSectionKind.PROJECTS: ("projects", "project", "项目经历", "项目"),
    ResumeSectionKind.SKILLS: ("skills", "skill", "专业技能", "技能"),
    ResumeSectionKind.PUBLICATIONS: ("publications", "publication", "论文", "出版物"),
    ResumeSectionKind.AWARDS: ("awards", "award", "奖项", "荣誉"),
    ResumeSectionKind.CERTIFICATIONS: ("certifications", "certification", "证书", "认证"),
    ResumeSectionKind.LANGUAGES: ("languages", "language", "语言能力", "语言"),
    ResumeSectionKind.VOLUNTEER: ("volunteer", "志愿经历", "志愿者"),
    ResumeSectionKind.CUSTOM: ("custom section", "自定义模块", "自定义"),
}
_TAILORING_MARKERS = (
    "job description",
    "target role",
    "target job",
    "jd",
    "岗位描述",
    "职位描述",
    "目标岗位",
    "招聘要求",
)
_TRANSLATION_MARKERS = (
    "translate",
    "translation",
    "翻译",
    "译成",
    "英文版",
    "中文版",
)


class ResumeEditPurpose(StrEnum):
    """Server-recognized purpose used only to strengthen drafting constraints."""

    GENERAL = "general"
    JOB_TAILORING = "job_tailoring"
    TRANSLATION = "translation"


@dataclass(frozen=True, slots=True)
class ResumeEditScope:
    """Resolved edit scope supplied to the model as an enforceable drafting policy."""

    mode: str
    target_section_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.mode not in {"whole_document", "single_section"}:
            raise ValueError("unsupported Resume edit scope")
        if self.mode == "single_section" and len(self.target_section_ids) != 1:
            raise ValueError("single-section scope requires exactly one target")
        if self.mode == "whole_document" and self.target_section_ids:
            raise ValueError("whole-document scope cannot carry section targets")

    def as_provider_policy(self) -> dict[str, object]:
        """Return a JSON-compatible policy without mutable domain state."""

        if self.mode == "single_section":
            instruction = (
                "Only draft operations for the listed section. Do not modify profile, "
                "template, style, other sections, or unrelated items. Prefer the smallest "
                "set of field updates and preserve every factual claim."
            )
        else:
            instruction = (
                "Draft only changes explicitly requested by the user. Prefer the smallest "
                "set of operations and preserve every factual claim."
            )
        return {
            "scope": self.mode,
            "target_section_ids": list(self.target_section_ids),
            "instruction": instruction,
        }


@dataclass(frozen=True, slots=True)
class ResumeEditPolicy:
    """Complete trusted drafting policy for one exact Resume snapshot."""

    scope: ResumeEditScope
    purpose: ResumeEditPurpose
    has_authorized_evidence: bool

    def as_provider_policy(self) -> dict[str, object]:
        """Return the policy included in the trusted tool context."""

        policy = self.scope.as_provider_policy()
        policy["purpose"] = self.purpose.value
        policy["has_authorized_evidence"] = self.has_authorized_evidence
        constraints = [
            str(policy.pop("instruction")),
            "Never invent employers, dates, skills, metrics, credentials, or outcomes.",
        ]
        if self.purpose is ResumeEditPurpose.JOB_TAILORING:
            constraints.extend(
                (
                    "Treat the job description as target criteria, not proof that the candidate has those qualifications.",
                    "Use only facts already present in the Resume or authorized evidence. Expose unsupported requirements as gaps instead of adding them.",
                    "Keep citation selections attached to claims derived from authorized evidence.",
                )
            )
        elif self.purpose is ResumeEditPurpose.TRANSLATION:
            constraints.extend(
                (
                    "Translate only the fields in scope into the requested response locale.",
                    "Preserve dates, numbers, URLs, identifiers, product names, and factual meaning exactly.",
                    "Do not optimize, summarize, add, or remove content unless the user explicitly requests that separately.",
                )
            )
        policy["constraints"] = constraints
        return policy


def resolve_resume_edit_scope(
    document: ResumeDocument,
    user_instruction: str,
) -> ResumeEditScope:
    """Resolve an explicit section reference; ambiguous requests remain document-scoped."""

    normalized = user_instruction.strip().casefold()
    if not normalized:
        return ResumeEditScope("whole_document", ())
    matches: set[str] = set()
    for section in document.sections:
        candidates = (
            section.id.casefold(),
            section.title.strip().casefold(),
            section.kind.value.casefold(),
            *(_SECTION_ALIASES.get(section.kind, ())),
        )
        if any(candidate and candidate in normalized for candidate in candidates):
            matches.add(section.id)
    if len(matches) == 1:
        return ResumeEditScope("single_section", tuple(matches))
    return ResumeEditScope("whole_document", ())


def build_resume_edit_policy(
    document: ResumeDocument,
    user_instruction: str,
    *,
    has_authorized_evidence: bool,
) -> ResumeEditPolicy:
    """Build a stable policy while keeping user text non-authoritative."""

    normalized = user_instruction.strip().casefold()
    if any(marker in normalized for marker in _TRANSLATION_MARKERS):
        purpose = ResumeEditPurpose.TRANSLATION
    elif any(marker in normalized for marker in _TAILORING_MARKERS):
        purpose = ResumeEditPurpose.JOB_TAILORING
    else:
        purpose = ResumeEditPurpose.GENERAL
    return ResumeEditPolicy(
        resolve_resume_edit_scope(document, user_instruction),
        purpose,
        has_authorized_evidence,
    )
