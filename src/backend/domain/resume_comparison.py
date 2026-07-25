"""Deterministic structural comparison for Resume revisions and variants."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from backend.domain.resumes import ResumeDocument, ResumeItem, ResumeSection


class ResumeChangeKind(StrEnum):
    """Stable structural change kinds."""

    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


@dataclass(frozen=True, slots=True)
class ResumeComparisonFinding:
    """One entity-level change without copying sensitive field values."""

    kind: ResumeChangeKind
    entity_type: str
    entity_id: str
    field_paths: tuple[tuple[str, ...], ...] = ()

    def __post_init__(self) -> None:
        if self.entity_type not in {"resume", "profile", "section", "item"}:
            raise ValueError("unsupported Resume comparison entity type")
        if self.kind is ResumeChangeKind.MODIFIED and not self.field_paths:
            raise ValueError("modified Resume entity requires changed fields")
        if self.kind is not ResumeChangeKind.MODIFIED and self.field_paths:
            raise ValueError("added or removed Resume entity cannot carry changed fields")


@dataclass(frozen=True, slots=True)
class ResumeComparison:
    """Value-free comparison safe to log or present after authorization."""

    left_resume_id: str
    left_revision: int
    right_resume_id: str
    right_revision: int
    findings: tuple[ResumeComparisonFinding, ...]

    @property
    def changed(self) -> bool:
        return bool(self.findings)


def compare_resumes(
    left: ResumeDocument,
    right: ResumeDocument,
) -> ResumeComparison:
    """Compare exact immutable snapshots without mutating or exposing their contents."""

    findings: list[ResumeComparisonFinding] = []
    root_fields = _changed_fields(
        left,
        right,
        ("title", "locale", "template", "style"),
    )
    if root_fields:
        findings.append(
            ResumeComparisonFinding(
                ResumeChangeKind.MODIFIED,
                "resume",
                str(right.meta.id),
                root_fields,
            )
        )
    profile_fields = _changed_fields(
        left.profile,
        right.profile,
        ("full_name", "headline", "summary", "contacts"),
    )
    if profile_fields:
        findings.append(
            ResumeComparisonFinding(
                ResumeChangeKind.MODIFIED,
                "profile",
                str(right.meta.id),
                tuple(("profile", *path) for path in profile_fields),
            )
        )
    _compare_sections(left.sections, right.sections, findings)
    return ResumeComparison(
        str(left.meta.id),
        left.meta.revision,
        str(right.meta.id),
        right.meta.revision,
        tuple(findings),
    )


def _compare_sections(
    left_sections: tuple[ResumeSection, ...],
    right_sections: tuple[ResumeSection, ...],
    findings: list[ResumeComparisonFinding],
) -> None:
    left = {section.id: section for section in left_sections}
    right = {section.id: section for section in right_sections}
    for section_id in sorted(left.keys() - right.keys()):
        findings.append(
            ResumeComparisonFinding(
                ResumeChangeKind.REMOVED,
                "section",
                section_id,
            )
        )
    for section_id in sorted(right.keys() - left.keys()):
        findings.append(
            ResumeComparisonFinding(
                ResumeChangeKind.ADDED,
                "section",
                section_id,
            )
        )
    left_positions = {section.id: index for index, section in enumerate(left_sections)}
    right_positions = {section.id: index for index, section in enumerate(right_sections)}
    for section_id in sorted(left.keys() & right.keys()):
        before = left[section_id]
        after = right[section_id]
        fields = list(
            _changed_fields(before, after, ("kind", "title", "visible", "content"))
        )
        if left_positions[section_id] != right_positions[section_id]:
            fields.append(("position",))
        if fields:
            findings.append(
                ResumeComparisonFinding(
                    ResumeChangeKind.MODIFIED,
                    "section",
                    section_id,
                    tuple(fields),
                )
            )
        _compare_items(before.items, after.items, findings)


def _compare_items(
    left_items: tuple[ResumeItem, ...],
    right_items: tuple[ResumeItem, ...],
    findings: list[ResumeComparisonFinding],
) -> None:
    left = {item.id: item for item in left_items}
    right = {item.id: item for item in right_items}
    for item_id in sorted(left.keys() - right.keys()):
        findings.append(
            ResumeComparisonFinding(ResumeChangeKind.REMOVED, "item", item_id)
        )
    for item_id in sorted(right.keys() - left.keys()):
        findings.append(
            ResumeComparisonFinding(ResumeChangeKind.ADDED, "item", item_id)
        )
    left_positions = {item.id: index for index, item in enumerate(left_items)}
    right_positions = {item.id: index for index, item in enumerate(right_items)}
    item_fields = (
        "kind",
        "title",
        "subtitle",
        "organization",
        "location",
        "date_range",
        "summary",
        "highlights",
        "skills",
        "tags",
        "visible",
        "url",
    )
    for item_id in sorted(left.keys() & right.keys()):
        fields = list(_changed_fields(left[item_id], right[item_id], item_fields))
        if left_positions[item_id] != right_positions[item_id]:
            fields.append(("position",))
        if fields:
            findings.append(
                ResumeComparisonFinding(
                    ResumeChangeKind.MODIFIED,
                    "item",
                    item_id,
                    tuple(fields),
                )
            )


def _changed_fields(
    left: object,
    right: object,
    fields: tuple[str, ...],
) -> tuple[tuple[str, ...], ...]:
    return tuple(
        (field,)
        for field in fields
        if getattr(left, field) != getattr(right, field)
    )
