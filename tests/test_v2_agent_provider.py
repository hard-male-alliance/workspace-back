"""OpenAI Agents SDK integration tests for the API V2 Agent provider."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from agents.items import ModelResponse, TResponseStreamEvent
from agents.models.interface import Model, ModelTracing
from agents.usage import Usage
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)

from backend.domain.agent_v2 import (
    AgentExecutionGrant,
    AgentOutputMode,
    AgentProposalDecisionContext,
    AgentProviderCompleted,
    AgentProviderProposalDecisionRequired,
    AgentProviderRequest,
    AgentResumeContext,
    AgentRunId,
    AgentRunSpec,
    ConversationCapability,
    ConversationId,
    Message,
    MessageId,
    MessageRole,
    TextContentPart,
)
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.principals import ResourceMeta, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resumes import (
    PageSize,
    ResumeId,
    ResumeSectionKind,
    TemplatePolicy,
    TemplateRef,
    TemplateZonePolicy,
    create_resume_document,
)
from backend.infrastructure.agents_sdk_provider import OpenAIAgentsSDKProvider

NOW = datetime(2026, 7, 23, 8, 0, tzinfo=UTC)
WORKSPACE_ID = WorkspaceId("workspace_provider_0001")


def _text_response(text: str, response_id: str) -> ModelResponse:
    return ModelResponse(
        output=[
            ResponseOutputMessage(
                id=f"message_{response_id}",
                content=[
                    ResponseOutputText(
                        annotations=[],
                        text=text,
                        type="output_text",
                    )
                ],
                role="assistant",
                status="completed",
                type="message",
            )
        ],
        usage=Usage(input_tokens=11, output_tokens=7, total_tokens=18),
        response_id=response_id,
    )


def _tool_response(name: str, arguments: str, call_id: str) -> ModelResponse:
    return ModelResponse(
        output=[
            ResponseFunctionToolCall(
                arguments=arguments,
                call_id=call_id,
                name=name,
                type="function_call",
                status="completed",
            )
        ],
        usage=Usage(input_tokens=13, output_tokens=5, total_tokens=18),
        response_id=f"response_{call_id}",
    )


class SequenceModel(Model):
    """Return SDK-native model items and capture the real tool catalog."""

    def __init__(self, responses: list[ModelResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[Any, list[Any], Any]] = []

    async def get_response(
        self,
        system_instructions: str | None,
        input: Any,
        model_settings: Any,
        tools: list[Any],
        output_schema: Any,
        handoffs: list[Any],
        tracing: ModelTracing,
        *,
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: Any,
    ) -> ModelResponse:
        del (
            system_instructions,
            model_settings,
            handoffs,
            tracing,
            previous_response_id,
            conversation_id,
            prompt,
        )
        self.calls.append((input, tools, output_schema))
        return self.responses.pop(0)

    async def stream_response(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncIterator[TResponseStreamEvent]:
        del args, kwargs
        if False:
            yield


def _request() -> AgentProviderRequest:
    conversation_id = ConversationId("conversation_provider_0001")
    message_id = MessageId("message_provider_0001")
    spec = AgentRunSpec(
        conversation_id,
        message_id,
        ConversationCapability.GENERAL,
        (),
        KnowledgeSelection(KnowledgeSelectionMode.NONE, (), (), (), "general_agent"),
        InferenceIntent(
            InferenceQualityTier.BALANCED,
            10_000,
            InferenceCostTier.STANDARD,
            ModelRegion.CN,
            False,
            False,
        ),
        (AgentOutputMode.TEXT,),
        "zh-CN",
    )
    grant = AgentExecutionGrant(
        ResourceRef("conversation", conversation_id, 1),
        "general_agent",
        ResourceRef("model", "model_provider_0001", 1),
        ModelRegion.CN,
        False,
        (),
        (),
        1,
    )
    message = Message(
        ResourceMeta(message_id, 1, NOW, NOW),
        WORKSPACE_ID,
        conversation_id,
        1,
        MessageRole.USER,
        None,
        (TextContentPart("hello"),),
    )
    return AgentProviderRequest(
        AgentRunId("agent_run_provider_0001"),
        spec,
        grant,
        message,
    )


def _resume_request() -> AgentProviderRequest:
    request = _request()
    kinds = frozenset(ResumeSectionKind)
    document = create_resume_document(
        resume_id=ResumeId("resume_provider_0001"),
        workspace_id=WORKSPACE_ID,
        title="Original title",
        locale="zh-CN",
        template_policy=TemplatePolicy(
            TemplateRef("template_provider_0001", "1"),
            frozenset({"zh-CN"}),
            frozenset({PageSize.A4}),
            frozenset({"pdf"}),
            kinds,
            (TemplateZonePolicy("main", kinds, 100),),
            frozenset({"sans"}),
            frozenset({"yyyy-mm"}),
            frozenset({"disc"}),
        ),
        created_at=NOW,
    )
    resume_ref = ResourceRef("resume", str(document.meta.id), document.meta.revision)
    return replace(
        request,
        spec=replace(
            request.spec,
            capability=ConversationCapability.RESUME_EDIT,
            context_refs=(resume_ref,),
            output_modes=(AgentOutputMode.TEXT, AgentOutputMode.RESUME_OPERATIONS),
            knowledge=KnowledgeSelection(
                KnowledgeSelectionMode.NONE,
                (),
                (),
                (),
                "resume_assistant",
            ),
        ),
        grant=replace(
            request.grant,
            agent_scope="resume_assistant",
            context_refs=(resume_ref,),
        ),
        resume_context=AgentResumeContext(resume_ref, document),
    )


@pytest.mark.asyncio
async def test_runner_returns_plain_assistant_output_without_structured_output() -> None:
    model = SequenceModel([_text_response("直接回答", "response_direct")])
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=1_000_000,
        output_cost_microusd_per_million_tokens=1_000_000,
    )

    outcome = await provider.execute(_request())

    assert isinstance(outcome, AgentProviderCompleted)
    assert outcome.content == (TextContentPart("直接回答"),)
    assert outcome.usage.input_tokens == 11
    assert outcome.usage.output_tokens == 7
    assert model.calls[0][1] == []
    assert model.calls[0][2] is None


@pytest.mark.asyncio
async def test_native_tool_call_interrupts_and_resumes_same_sdk_state() -> None:
    model = SequenceModel(
        [
            _tool_response(
                "resume_draft_set_field",
                '{"entity_id":"resume_provider_0001","field_path":["title"],'
                '"value":"Focused title"}',
                "call_draft_0001",
            ),
            _tool_response(
                "resume_request_proposal_decision",
                '{"title":"Improve title"}',
                "call_proposal_0001",
            ),
            _text_response("已按你的决定继续处理。", "response_resumed"),
        ]
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )
    request = _resume_request()

    interrupted = await provider.execute(request)

    assert isinstance(interrupted, AgentProviderProposalDecisionRequired)
    assert interrupted.tool_call_id == "call_proposal_0001"
    assert interrupted.proposal_title == "Improve title"
    assert interrupted.resume_operations[0].payload["op"] == "set_field"
    assert "$schemaVersion" in interrupted.provider_state
    native_tools = model.calls[0][1]
    assert {tool.name for tool in native_tools} >= {
        "resume_read_section",
        "resume_draft_set_field",
        "resume_request_proposal_decision",
    }
    proposal_tool = next(
        tool for tool in native_tools if tool.name == "resume_request_proposal_decision"
    )
    assert proposal_tool.needs_approval is True
    assert model.calls[0][2] is None

    resumed = await provider.execute(
        replace(
            request,
            proposal_decision=AgentProposalDecisionContext(
                ResourceRef("resume_proposal", "proposal_provider_0001", 2),
                "accept",
                ResourceRef("resume", "resume_provider_0001", 2),
            ),
            provider_state=interrupted.provider_state,
        )
    )

    assert isinstance(resumed, AgentProviderCompleted)
    assert resumed.content == (TextContentPart("已按你的决定继续处理。"),)
    assert resumed.tool_invocations[0].ordinal == 3
    assert resumed.tool_invocations[0].tool_name == "resume_request_proposal_decision"
    resumed_input = model.calls[-1][0]
    assert "call_proposal_0001" in repr(resumed_input)
