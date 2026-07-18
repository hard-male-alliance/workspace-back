"""Resume AI Proposal domain model and decision rules."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from backend.domain.common import DomainError, Problem, iso_timestamp, utc_now
from backend.domain.knowledge import KnowledgeTrustLevel
from workspace_shared.tenancy import ActorScope


class ResumeProposalStatus(StrEnum):
    """Lifecycle of an AI-authored change proposal."""

    PENDING = "pending"
    ACCEPTED = "accepted"
    PARTIALLY_ACCEPTED = "partially_accepted"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CONFLICTED = "conflicted"


@dataclass(frozen=True, slots=True)
class ProposalCitation:
    """A stable reference to the exact knowledge chunk supporting a change."""

    source_id: str
    source_version_id: str
    chunk_id: str
    quote: str
    trust_level: KnowledgeTrustLevel
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "source_version_id": self.source_version_id,
            "chunk_id": self.chunk_id,
            "quote": self.quote,
            "trust_level": self.trust_level.value,
            "metadata": deepcopy(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class ResumeProposalOperation:
    """One reviewable Resume operation plus its evidence and atomic group."""

    id: str
    operation: dict[str, Any]
    reason: str
    atomic_group_id: str
    citations: tuple[ProposalCitation, ...] = ()
    trust_level: KnowledgeTrustLevel = KnowledgeTrustLevel.GENERATED

    def as_dict(self) -> dict[str, Any]:
        payload = deepcopy(self.operation)
        payload.setdefault("operation_id", self.id)
        payload["extensions"] = {
            "aiws": {
                "reason": self.reason,
                "atomic_group_id": self.atomic_group_id,
                "trust_level": self.trust_level.value,
                "citations": [citation.as_dict() for citation in self.citations],
            }
        }
        return payload


@dataclass(slots=True)
class ResumeProposalRecord:
    """AI-authored but human-controlled Resume change proposal aggregate."""

    scope: ActorScope
    id: str
    created_at: datetime
    updated_at: datetime
    resume_id: str
    base_revision: int
    source_run_id: str
    title: str
    summary: str
    operations: list[ResumeProposalOperation]
    status: ResumeProposalStatus = ResumeProposalStatus.PENDING
    revision: int = 1
    expires_at: datetime | None = None
    selected_operation_ids: list[str] = field(default_factory=list)
    decision_comment: str | None = None
    decided_by_actor_id: str | None = None
    decided_at: datetime | None = None
    render_hint: dict[str, Any] | None = None
    application_result: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "created_at": iso_timestamp(self.created_at),
            "updated_at": iso_timestamp(self.updated_at),
            "revision": self.revision,
            "resume_id": self.resume_id,
            "base_revision": self.base_revision,
            "source_run_id": self.source_run_id,
            "title": self.title,
            "summary": _rich_text(self.summary) if self.summary else None,
            "operations": [operation.as_dict() for operation in self.operations],
            "status": self.status.value,
            "expires_at": iso_timestamp(self.expires_at) if self.expires_at else None,
            "extensions": {
                "aiws": {
                    "selected_operation_ids": list(self.selected_operation_ids),
                    "decision_comment": self.decision_comment,
                    "decided_by_actor_id": self.decided_by_actor_id,
                    "decided_at": iso_timestamp(self.decided_at) if self.decided_at else None,
                    "render_hint": deepcopy(self.render_hint),
                    "application_result": deepcopy(self.application_result),
                }
            },
        }

    def expire_if_needed(self, now: datetime | None = None) -> bool:
        """Move a pending proposal to expired when its deadline has passed."""
        if self.status is not ResumeProposalStatus.PENDING or self.expires_at is None:
            return False
        if self.expires_at > (now or utc_now()):
            return False
        self._finish(ResumeProposalStatus.EXPIRED, [], None, None)
        return True

    def select_operations(self, decision: str, operation_ids: list[str] | None) -> list[ResumeProposalOperation]:
        """Validate a decision and return whole atomic groups only."""
        if self.expire_if_needed():
            raise DomainError(Problem("resume.proposal_expired", 409, "Resume proposal has expired"))
        if self.status is not ResumeProposalStatus.PENDING:
            raise DomainError(Problem("resume.proposal_already_decided", 409, "Resume proposal was already decided"))
        if decision == "reject":
            return []
        if decision == "accept_all":
            return list(self.operations)
        if decision != "accept_selected":
            raise DomainError(Problem("resume.invalid_proposal_decision", 422, "Proposal decision is invalid"))
        requested = set(operation_ids or [])
        known = {operation.id for operation in self.operations}
        if not requested or not requested <= known:
            raise DomainError(Problem("resume.invalid_proposal_selection", 422, "Proposal operation selection is invalid"))
        groups: dict[str, set[str]] = {}
        for operation in self.operations:
            groups.setdefault(operation.atomic_group_id, set()).add(operation.id)
        partial_groups = [group_id for group_id, members in groups.items() if requested & members and not members <= requested]
        if partial_groups:
            raise DomainError(
                Problem(
                    "resume.partial_atomic_group",
                    422,
                    "All operations in an atomic group must be selected together",
                    extensions={"atomic_group_ids": partial_groups},
                )
            )
        return [operation for operation in self.operations if operation.id in requested]

    def mark_decided(
        self,
        decision: str,
        selected: list[ResumeProposalOperation],
        actor_id: str,
        comment: str | None,
    ) -> None:
        """Persist the terminal human decision after Resume operations commit."""
        if decision == "reject":
            status = ResumeProposalStatus.REJECTED
        elif len(selected) == len(self.operations):
            status = ResumeProposalStatus.ACCEPTED
        else:
            status = ResumeProposalStatus.PARTIALLY_ACCEPTED
        self._finish(status, [operation.id for operation in selected], actor_id, comment)

    def mark_conflicted(self) -> None:
        """Make a stale proposal terminal; phase one never silently merges it."""
        self._finish(ResumeProposalStatus.CONFLICTED, [], None, None)

    def _finish(
        self,
        status: ResumeProposalStatus,
        operation_ids: list[str],
        actor_id: str | None,
        comment: str | None,
    ) -> None:
        timestamp = utc_now()
        self.status = status
        self.selected_operation_ids = list(operation_ids)
        self.decision_comment = comment
        self.decided_by_actor_id = actor_id
        self.decided_at = timestamp
        self.updated_at = timestamp
        self.revision += 1


def _rich_text(text: str) -> dict[str, Any]:
    """Build a minimal contract-valid RichText value."""
    return {
        "schema_version": "1.0",
        "blocks": [
            {
                "block_id": "blk_proposal_summary",
                "type": "paragraph",
                "align": "start",
                "spans": [{"text": text, "marks": []}],
            }
        ],
        "plain_text": text,
    }
