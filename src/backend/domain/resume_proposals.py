"""@brief API v2 Resume proposal 聚合 / API v2 Resume proposal aggregate."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum

from backend.domain.principals import ResourceMeta, UserId, WorkspaceId
from backend.domain.resumes import (
    ResourceRef,
    ResumeDomainError,
    ResumeId,
    ResumeOperation,
    ResumeOperationId,
    ResumeProposalId,
)


class ResumeProposalStatus(StrEnum):
    """@brief API v2 proposal 状态 / API v2 proposal states."""

    PENDING = "pending"
    ACCEPTED = "accepted"
    PARTIALLY_ACCEPTED = "partially_accepted"
    REJECTED = "rejected"
    EXPIRED = "expired"


class ProposalDecision(StrEnum):
    """@brief proposal decision 输入判别值 / Proposal-decision input discriminants."""

    ACCEPT = "accept"
    ACCEPT_SELECTED = "accept_selected"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class ProposalDecisionCommand:
    """@brief 穷尽的 proposal decision 命令 / Exhaustive proposal-decision command.

    @param decision accept、accept_selected 或 reject / accept, accept_selected, or reject.
    @param accepted_operation_ids 仅 accept_selected 为非空 / Non-empty only for accept_selected.
    """

    decision: ProposalDecision
    accepted_operation_ids: tuple[ResumeOperationId, ...] = ()

    def __post_init__(self) -> None:
        """@brief 校验 decision 判别联合 / Validate the decision discriminated union.

        @raise ResumeDomainError 判别值与 operation IDs 不一致时抛出 / Raised for inconsistent fields.
        """
        if self.decision is ProposalDecision.ACCEPT_SELECTED:
            if not 1 <= len(self.accepted_operation_ids) <= 200:
                raise ResumeDomainError(
                    "resume.invalid_proposal_decision",
                    "accept_selected requires one to 200 operation IDs",
                )
            if len(set(self.accepted_operation_ids)) != len(self.accepted_operation_ids):
                raise ResumeDomainError(
                    "resume.invalid_proposal_decision",
                    "accepted operation IDs must be unique",
                )
        elif self.accepted_operation_ids:
            raise ResumeDomainError(
                "resume.invalid_proposal_decision",
                "accept and reject require an empty accepted_operation_ids array",
            )


@dataclass(frozen=True, slots=True)
class ResumeProposal:
    """@brief 人类决策前不得改写 Resume 的 proposal 聚合 / Proposal aggregate that never writes before decision.

    @param meta proposal 资源元数据 / Proposal resource metadata.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param resume_id 目标 Resume / Target Resume.
    @param base_revision 生成 proposal 时的 Resume revision / Resume revision at proposal creation.
    @param title 用户可见标题 / User-visible title.
    @param status 生命周期状态 / Lifecycle status.
    @param operations 待审核 operation / Operations awaiting review.
    @param evidence_refs 证据资源引用 / Evidence resource references.
    @param expires_at 内部过期时刻 / Internal expiry instant.
    @param decided_by 决策用户 / Deciding user.
    @param accepted_operation_ids 最终接受的 operation IDs / Finally accepted operation IDs.
    """

    meta: ResourceMeta[ResumeProposalId]
    workspace_id: WorkspaceId
    resume_id: ResumeId
    base_revision: int
    title: str
    status: ResumeProposalStatus
    operations: tuple[ResumeOperation, ...]
    evidence_refs: tuple[ResourceRef, ...] = ()
    expires_at: datetime | None = None
    decided_by: UserId | None = None
    accepted_operation_ids: tuple[ResumeOperationId, ...] = ()

    def __post_init__(self) -> None:
        """@brief 校验 proposal 不变量 / Validate proposal invariants.

        @raise ResumeDomainError 状态、标识或 operation 无效时抛出 / Raised for invalid proposals.
        """
        if self.base_revision < 1 or not 1 <= len(self.operations) <= 200:
            raise ResumeDomainError(
                "resume.invalid_proposal",
                "proposal base revision or operation count is invalid",
            )
        if not 1 <= len(self.title) <= 300 or len(self.evidence_refs) > 200:
            raise ResumeDomainError(
                "resume.invalid_proposal",
                "proposal title or evidence count is invalid",
            )
        operation_ids = [operation.operation_id for operation in self.operations]
        if len(set(operation_ids)) != len(operation_ids):
            raise ResumeDomainError(
                "resume.invalid_proposal",
                "proposal operation IDs must be unique",
            )
        if self.expires_at is not None and (
            self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None
        ):
            raise ResumeDomainError(
                "resume.invalid_proposal",
                "proposal expiry must be timezone-aware",
            )
        terminal = self.status is not ResumeProposalStatus.PENDING
        if terminal != (self.decided_by is not None or self.status is ResumeProposalStatus.EXPIRED):
            raise ResumeDomainError(
                "resume.invalid_proposal",
                "proposal decision metadata is inconsistent with status",
            )
        if not set(self.accepted_operation_ids) <= set(operation_ids):
            raise ResumeDomainError(
                "resume.invalid_proposal",
                "accepted operation IDs are not a proposal subset",
            )

    def expire(self, at: datetime) -> ResumeProposal:
        """@brief 在截止时间后将 pending proposal 过期 / Expire a pending proposal after its deadline.

        @param at 当前时刻 / Current instant.
        @return 原 proposal 或新的 expired revision / Original proposal or a new expired revision.
        """
        if (
            self.status is ResumeProposalStatus.PENDING
            and self.expires_at is not None
            and at >= self.expires_at
        ):
            return replace(
                self,
                meta=self.meta.advance(at),
                status=ResumeProposalStatus.EXPIRED,
            )
        return self

    def select(self, command: ProposalDecisionCommand, *, at: datetime) -> tuple[ResumeOperation, ...]:
        """@brief 验证当前状态并选择要提交的 operations / Validate state and select operations.

        @param command 已类型化 decision / Typed decision.
        @param at 决策时刻 / Decision instant.
        @return 按 proposal 顺序排列的选中 operations / Selected operations in proposal order.
        @raise ResumeDomainError proposal 不再 pending、已过期或选择无效时抛出 / Raised for invalid decisions.
        """
        if self.status is not ResumeProposalStatus.PENDING:
            raise ResumeDomainError(
                "resume.proposal_already_decided",
                "only a pending proposal can be decided",
            )
        if self.expires_at is not None and at >= self.expires_at:
            raise ResumeDomainError(
                "resume.proposal_expired",
                "resume proposal has expired",
            )
        if command.decision is ProposalDecision.REJECT:
            return ()
        if command.decision is ProposalDecision.ACCEPT:
            return self.operations
        selected_ids = set(command.accepted_operation_ids)
        known_ids = {operation.operation_id for operation in self.operations}
        if not selected_ids <= known_ids:
            raise ResumeDomainError(
                "resume.invalid_proposal_decision",
                "selected operation was not found in the proposal",
            )
        return tuple(
            operation
            for operation in self.operations
            if operation.operation_id in selected_ids
        )

    def decide(
        self,
        command: ProposalDecisionCommand,
        *,
        actor_id: UserId,
        at: datetime,
    ) -> tuple[ResumeProposal, tuple[ResumeOperation, ...]]:
        """@brief 产生终态 proposal，但不直接写 Resume / Produce a terminal proposal without directly writing Resume.

        @param command decision 命令 / Decision command.
        @param actor_id 决策用户 / Deciding user.
        @param at 决策时刻 / Decision instant.
        @return 终态 proposal 与要由应用层原子提交的 operations / Terminal proposal and operations for atomic application.
        """
        selected = self.select(command, at=at)
        if command.decision is ProposalDecision.REJECT:
            status = ResumeProposalStatus.REJECTED
        elif len(selected) == len(self.operations):
            status = ResumeProposalStatus.ACCEPTED
        else:
            status = ResumeProposalStatus.PARTIALLY_ACCEPTED
        updated = replace(
            self,
            meta=self.meta.advance(at),
            status=status,
            decided_by=actor_id,
            accepted_operation_ids=tuple(operation.operation_id for operation in selected),
        )
        return updated, selected


__all__ = [
    "ProposalDecision",
    "ProposalDecisionCommand",
    "ResumeProposal",
    "ResumeProposalStatus",
]
