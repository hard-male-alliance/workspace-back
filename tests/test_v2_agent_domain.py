"""API v2 Conversation 与 Agent 领域核心测试。"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from datetime import UTC, datetime, timedelta

import pytest

from backend.domain.agent_v2 import (
    AGENT_RUN_JOB_KIND,
    AgentDomainError,
    AgentExecutionGrant,
    AgentOutboxId,
    AgentOutputMode,
    AgentProviderCompleted,
    AgentRun,
    AgentRunId,
    AgentRunQueuedDispatch,
    AgentRunSpec,
    AgentRunStatus,
    AgentRunTransitionError,
    AgentRunView,
    AgentUsage,
    CitationContentPart,
    Conversation,
    ConversationCapability,
    ConversationId,
    ConversationPatch,
    ConversationStatus,
    ConversationUnavailable,
    Message,
    MessageId,
    MessageRole,
    ResumeProposalContentPart,
    TextContentPart,
    ToolApproval,
    ToolApprovalDecisionError,
    ToolApprovalId,
    ToolCallBinding,
    ToolCallId,
    ToolDecision,
    ToolDecisionDispatch,
    ToolRisk,
    validate_run_job_alignment,
)
from backend.domain.knowledge_retrieval import (
    HybridScore,
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeCitation,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.platform import Job, JobId, JobProgress, JobProgressUnit
from backend.domain.principals import ResourceMeta, UserId, WorkspaceId
from backend.domain.resources import ResourceRef

NOW = datetime(2026, 7, 23, 1, 0, tzinfo=UTC)
WORKSPACE = WorkspaceId("workspace_0001")
CONVERSATION_ID = ConversationId("conversation_0001")
INPUT_ID = MessageId("message_input_0001")
RUN_ID = AgentRunId("agent_run_0001")
JOB_ID = JobId("agent_job_0001")
USER_ID = UserId("user_actor_0001")


def _conversation(*, revision: int = 1) -> Conversation:
    return Conversation(
        ResourceMeta(CONVERSATION_ID, revision, NOW, NOW),
        WORKSPACE,
        "研究助手",
        ConversationCapability.GENERAL,
    )


def _input_message() -> Message:
    return Message(
        ResourceMeta(INPUT_ID, 1, NOW, NOW),
        WORKSPACE,
        CONVERSATION_ID,
        1,
        MessageRole.USER,
        None,
        (TextContentPart("请解释线性化一致性。"),),
    )


def _spec() -> AgentRunSpec:
    return AgentRunSpec(
        conversation_id=CONVERSATION_ID,
        input_message_id=INPUT_ID,
        capability=ConversationCapability.GENERAL,
        context_refs=(ResourceRef("resume", "resume_context_0001", 7),),
        knowledge=KnowledgeSelection(
            KnowledgeSelectionMode.NONE,
            (),
            (),
            (),
            "general_agent",
        ),
        inference=InferenceIntent(
            InferenceQualityTier.BALANCED,
            15_000,
            InferenceCostTier.STANDARD,
            ModelRegion.CN,
            False,
            False,
        ),
        output_modes=(AgentOutputMode.TEXT,),
        response_locale="zh-CN",
    )


def _grant(*, region: ModelRegion = ModelRegion.CN) -> AgentExecutionGrant:
    return AgentExecutionGrant(
        session_ref=ResourceRef("conversation", CONVERSATION_ID, 1),
        agent_scope="general_agent",
        model_ref=ResourceRef("model", "model_policy_0001", 3),
        model_region=region,
        external_model_processing=False,
        context_refs=(ResourceRef("resume", "resume_context_0001", 7),),
        knowledge_contexts=(),
        policy_version=4,
    )


def _run() -> AgentRun:
    return AgentRun(
        AgentRunView(
            ResourceMeta(RUN_ID, 1, NOW, NOW),
            WORKSPACE,
            CONVERSATION_ID,
            INPUT_ID,
            ConversationCapability.GENERAL,
            AgentRunStatus.QUEUED,
        ),
        JOB_ID,
        USER_ID,
        _spec(),
        _grant(),
    )


def _job() -> Job:
    return Job(
        ResourceMeta(JOB_ID, 1, NOW, NOW),
        WORKSPACE,
        AGENT_RUN_JOB_KIND,
        ResourceRef("agent_run", RUN_ID),
    )


def _binding(*, call_id: str = "tool_call_0001") -> ToolCallBinding:
    return ToolCallBinding(
        ToolCallId(call_id),
        "calendar.create_event",
        "创建一场面试准备会议",
        ToolRisk.HIGH,
        NOW + timedelta(minutes=10),
        ResourceRef("tool_invocation", "invocation_0001", 1),
    )


def test_conversation_patch_revision_and_soft_delete_are_strong() -> None:
    conversation = _conversation()

    archived = conversation.update(
        ConversationPatch(title_supplied=True, title=None, status=ConversationStatus.ARCHIVED),
        at=NOW + timedelta(seconds=1),
    )
    assert archived.meta.revision == 2
    assert archived.title is None
    assert not archived.is_writable

    deleted = archived.soft_delete(at=NOW + timedelta(seconds=2))
    assert deleted.meta.revision == 3
    assert deleted.is_deleted
    with pytest.raises(ConversationUnavailable):
        deleted.update(
            ConversationPatch(status=ConversationStatus.ACTIVE),
            at=NOW + timedelta(seconds=3),
        )


def test_message_is_append_only_and_role_content_is_discriminated() -> None:
    message = _input_message()
    assert message.sequence == 1
    with pytest.raises(FrozenInstanceError):
        message.sequence = 2  # type: ignore[misc]

    citation = CitationContentPart(
        KnowledgeCitation(
            source_id="source_0001",  # type: ignore[arg-type]
            version_id="version_0001",  # type: ignore[arg-type]
            locator="section:1",
            quote="linearizable",
            score=HybridScore(0.8, 0.9, 0.85).fused,
        )
    )
    with pytest.raises(AgentDomainError):
        Message(
            ResourceMeta(MessageId("message_invalid_0001"), 1, NOW, NOW),
            WORKSPACE,
            CONVERSATION_ID,
            2,
            MessageRole.USER,
            INPUT_ID,
            (citation,),
        )

    assistant = Message(
        ResourceMeta(MessageId("message_output_0001"), 1, NOW, NOW),
        WORKSPACE,
        CONVERSATION_ID,
        2,
        MessageRole.ASSISTANT,
        INPUT_ID,
        (citation,),
        RUN_ID,
    )
    assert assistant.source_run_id == RUN_ID


def test_execution_grant_enforces_session_scope_and_model_region() -> None:
    spec = _spec()
    conversation = _conversation()
    _grant().validate_for(conversation, spec)

    with pytest.raises(AgentDomainError, match="model region"):
        _grant(region=ModelRegion.GLOBAL).validate_for(conversation, spec)

    wrong_scope = AgentExecutionGrant(
        session_ref=ResourceRef("conversation", CONVERSATION_ID, 1),
        agent_scope="different_agent",
        model_ref=ResourceRef("model", "model_policy_0001", 3),
        model_region=ModelRegion.CN,
        external_model_processing=False,
        context_refs=(ResourceRef("resume", "resume_context_0001", 7),),
        knowledge_contexts=(),
        policy_version=4,
    )
    with pytest.raises(AgentDomainError, match="agent scope"):
        wrong_scope.validate_for(conversation, spec)


def test_agent_run_and_unified_job_state_machines_remain_aligned() -> None:
    run = _run()
    job = _job()
    validate_run_job_alignment(run, job)

    started_at = NOW + timedelta(seconds=1)
    running = run.start(at=started_at)
    running_job = job.start(
        at=started_at,
        progress=JobProgress("model_execution", 0, None, JobProgressUnit.STEPS),
    )
    validate_run_job_alignment(running, running_job)

    waiting_at = NOW + timedelta(seconds=2)
    approval_id = ToolApprovalId("approval_0001")
    waiting = running.wait_for_tool(approval_id, ToolCallId("tool_call_0001"), at=waiting_at)
    waiting_job = running_job.report_progress(
        JobProgress("waiting_for_approval", 0, None, JobProgressUnit.STEPS),
        at=waiting_at,
    )
    validate_run_job_alignment(waiting, waiting_job)

    resumed_at = NOW + timedelta(seconds=3)
    resumed = waiting.resume_after_decision(
        approval_id,
        ToolCallId("tool_call_0001"),
        at=resumed_at,
    )
    resumed_job = waiting_job.report_progress(
        JobProgress("tool_decision_recorded", 0, None, JobProgressUnit.STEPS),
        at=resumed_at,
    )
    validate_run_job_alignment(resumed, resumed_job)

    with pytest.raises(AgentRunTransitionError):
        resumed.start(at=NOW + timedelta(seconds=4))


def test_tool_approval_is_exact_expiring_and_one_time() -> None:
    running = _run().start(at=NOW + timedelta(seconds=1))
    binding = _binding()
    approval_id = ToolApprovalId("approval_0001")
    approval = ToolApproval.create(
        ResourceMeta(approval_id, 1, NOW + timedelta(seconds=2), NOW + timedelta(seconds=2)),
        WORKSPACE,
        RUN_ID,
        binding,
    )
    waiting = running.wait_for_tool(
        approval_id,
        binding.tool_call_id,
        at=NOW + timedelta(seconds=2),
    )
    assert approval.matches_waiting_run(waiting)

    actor = ResourceRef("user", "user_actor_0001")
    decided = approval.decide(ToolDecision.APPROVE, actor, at=NOW + timedelta(minutes=1))
    assert decided.view.status.value == "approved"
    with pytest.raises(ToolApprovalDecisionError):
        decided.decide(ToolDecision.REJECT, actor, at=NOW + timedelta(minutes=2))

    expiring = ToolApproval.create(
        ResourceMeta(
            ToolApprovalId("approval_0002"),
            1,
            NOW,
            NOW,
        ),
        WORKSPACE,
        RUN_ID,
        _binding(call_id="tool_call_0002"),
    )
    with pytest.raises(ToolApprovalDecisionError, match="expired"):
        expiring.decide(ToolDecision.APPROVE, actor, at=expiring.view.expires_at)


def test_provider_output_cannot_directly_overwrite_authoritative_resources() -> None:
    result = AgentProviderCompleted(
        content=(
            TextContentPart("我准备了一份待审阅修改。"),
            ResumeProposalContentPart(
                ResourceRef("resume_proposal", "proposal_0001", 1)
            ),
        ),
        proposal_refs=(ResourceRef("resume_proposal", "proposal_0001", 1),),
        usage=AgentUsage(20, 30, "120"),
    )
    with pytest.raises(AgentDomainError, match="directly"):
        result.validate_modes((AgentOutputMode.TEXT, AgentOutputMode.RESUME_OPERATIONS))

    with pytest.raises(AgentDomainError, match="Proposal"):
        AgentProviderCompleted(
            content=(TextContentPart("已完成"),),
            proposal_refs=(ResourceRef("resume", "resume_context_0001", 7),),
            usage=AgentUsage(1, 1, "1"),
        )

    public_field_names = {
        field.name
        for model in (AgentRunView, Message, AgentProviderCompleted)
        for field in fields(model)
    }
    assert not {"chain_of_thought", "reasoning", "scratchpad"} & public_field_names


def test_outbox_payload_is_closed_and_contains_no_provider_content() -> None:
    queued = AgentRunQueuedDispatch(
        AgentOutboxId("outbox_0001"),
        WORKSPACE,
        USER_ID,
        ResourceRef("agent_run", RUN_ID, 1),
        ResourceRef("job", JOB_ID, 1),
        NOW,
    )
    assert dict(queued.as_payload()) == {
        "actor_id": USER_ID,
        "run_id": RUN_ID,
        "job_id": JOB_ID,
    }

    decision = ToolDecisionDispatch(
        AgentOutboxId("outbox_0002"),
        WORKSPACE,
        USER_ID,
        ResourceRef("agent_run", RUN_ID, 3),
        ResourceRef("job", JOB_ID, 3),
        ResourceRef("tool_approval", "approval_0001", 2),
        ToolCallId("tool_call_0001"),
        ToolDecision.APPROVE,
        NOW,
    )
    assert set(decision.as_payload()) == {
        "run_id",
        "run_revision",
        "job_id",
        "job_revision",
        "approval_id",
        "approval_revision",
        "tool_call_id",
        "decision",
        "actor_id",
    }
