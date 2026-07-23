"""API V2 Agent 文本 provider 适配器测试。"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from backend.application.ports.agent_v2 import AgentProviderFailure
from backend.domain.agent_v2 import (
    AgentExecutionGrant,
    AgentKnowledgeEvidence,
    AgentOutputMode,
    AgentProviderCompleted,
    AgentProviderRequest,
    AgentRunId,
    AgentRunSpec,
    AuthorizedKnowledgeContext,
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
    KnowledgeCitation,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    ModelRegion,
)
from backend.domain.principals import ResourceMeta, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.agent_provider import StreamingTextAgentProvider
from backend.infrastructure.providers import CapabilityDescriptor

NOW = datetime(2026, 7, 23, 8, 0, tzinfo=UTC)
"""固定测试时刻。"""

WORKSPACE_ID = WorkspaceId("workspace_provider_0001")
"""测试 Workspace。"""


class RecordingStreamProvider:
    """记录 provider-safe 请求并返回严格 JSON 分片。"""

    def __init__(self, response: dict[str, object] | None = None) -> None:
        """初始化空调用记录。"""
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.response = response or {
            "protocol_version": "agent.output.strict_json.v1",
            "text": "world",
            "citation_indices": [],
            "resume_proposal": None,
        }

    def capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        """声明测试 transport 支持 general 的原生结构化输出。"""
        return (CapabilityDescriptor("general", True, False, True),)

    async def stream_text(self, prompt: str, request: dict[str, object]):  # type: ignore[no-untyped-def]
        """记录调用并流式返回一个 JSON 文档。"""
        self.calls.append((prompt, request))
        encoded = json.dumps(self.response)
        midpoint = len(encoded) // 2
        yield encoded[:midpoint]
        yield encoded[midpoint:]


class RawStreamProvider:
    """返回指定原始字符串，用于验证本地封闭解析。"""

    def __init__(self, response: str) -> None:
        self.response = response
        self.calls = 0

    def capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (CapabilityDescriptor("general", True, False, True),)

    async def stream_text(self, prompt: str, request: dict[str, object]):  # type: ignore[no-untyped-def]
        del prompt, request
        self.calls += 1
        yield self.response


class CapabilityBlindProvider:
    """缺少能力发现的 transport；网络方法不应被调用。"""

    def __init__(self) -> None:
        self.calls = 0

    async def stream_text(self, prompt: str, request: dict[str, object]):  # type: ignore[no-untyped-def]
        del prompt, request
        self.calls += 1
        yield "{}"


def _request(*, modes: tuple[AgentOutputMode, ...]) -> AgentProviderRequest:
    """构造已授权 provider request。"""
    conversation_id = ConversationId("conversation_provider_0001")
    message_id = MessageId("message_provider_0001")
    wants_citations = AgentOutputMode.CITATIONS in modes
    source_id = KnowledgeSourceId("knowledge_source_provider_0001")
    version_id = KnowledgeSourceVersionId("knowledge_version_provider_0001")
    spec = AgentRunSpec(
        conversation_id,
        message_id,
        ConversationCapability.GENERAL,
        (),
        KnowledgeSelection(
            (
                KnowledgeSelectionMode.EXPLICIT
                if wants_citations
                else KnowledgeSelectionMode.NONE
            ),
            (source_id,) if wants_citations else (),
            (),
            (),
            "general_agent",
        ),
        InferenceIntent(
            InferenceQualityTier.BALANCED,
            10_000,
            InferenceCostTier.STANDARD,
            ModelRegion.CN,
            False,
            False,
        ),
        modes,
        "zh-CN",
    )
    grant = AgentExecutionGrant(
        ResourceRef("conversation", conversation_id, 1),
        "general_agent",
        ResourceRef("model", "model_provider_0001", 1),
        ModelRegion.CN,
        False,
        (),
        (
            AuthorizedKnowledgeContext(source_id, version_id, 1),
        )
        if wants_citations
        else (),
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
    evidence = (
        AgentKnowledgeEvidence(
            0,
            "knowledge_chunk_provider_0001",
            KnowledgeCitation(source_id, version_id, "page/1", "evidence", 0.9),
        ),
    )
    return AgentProviderRequest(
        AgentRunId("agent_run_provider_0001"),
        spec,
        grant,
        message,
        evidence if wants_citations else (),
    )


@pytest.mark.asyncio
async def test_streaming_provider_returns_public_text_and_integer_metering() -> None:
    """适配器只返回公开文本并用整数可复算计量。"""
    delegate = RecordingStreamProvider()
    provider = StreamingTextAgentProvider(
        delegate,
        input_cost_microusd_per_million_tokens=1_000_000,
        output_cost_microusd_per_million_tokens=1_000_000,
    )

    outcome = await provider.execute(_request(modes=(AgentOutputMode.TEXT,)))

    assert isinstance(outcome, AgentProviderCompleted)
    assert outcome.content == (TextContentPart("world"),)
    assert outcome.usage.input_tokens == 2
    assert outcome.usage.output_tokens > 2
    assert int(outcome.usage.cost_micro_usd) > 4
    assert delegate.calls[0][0] == "hello"
    assert delegate.calls[0][1]["capability"] == "general"
    assert delegate.calls[0][1]["response_format"] == "agent.output.strict_json.v1"
    response_schema = delegate.calls[0][1]["response_schema"]
    assert isinstance(response_schema, dict)
    assert response_schema["additionalProperties"] is False


@pytest.mark.asyncio
async def test_text_adapter_fails_explicitly_when_text_was_not_requested() -> None:
    """不能以文本假冒 citation-only 请求。"""
    provider = StreamingTextAgentProvider(
        RecordingStreamProvider(
            {
                "protocol_version": "agent.output.strict_json.v1",
                "text": "unrequested",
                "citation_indices": [0],
                "resume_proposal": None,
            }
        ),
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )

    with pytest.raises(AgentProviderFailure) as captured:
        await provider.execute(_request(modes=(AgentOutputMode.CITATIONS,)))

    assert captured.value.problem.code == "agent.provider_protocol_error"
    assert captured.value.problem.retryable is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    (
        "{}",
        '```json\n{"protocol_version":"agent.output.strict_json.v1"}\n```',
        (
            '{"protocol_version":"agent.output.strict_json.v1","text":"ok",'
            '"citation_indices":[],"resume_proposal":null,"extra":true}'
        ),
        (
            '{"protocol_version":"agent.output.strict_json.v1","text":"ok",'
            '"citation_indices":[],"resume_proposal":null}{}'
        ),
    ),
)
async def test_malformed_structured_outputs_fail_closed(raw: str) -> None:
    """Markdown、字段漂移和多个 JSON 文档都不能进入领域层。"""
    provider = StreamingTextAgentProvider(
        RawStreamProvider(raw),
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )

    with pytest.raises(AgentProviderFailure) as captured:
        await provider.execute(_request(modes=(AgentOutputMode.TEXT,)))

    assert captured.value.problem.code == "agent.provider_protocol_error"
    assert captured.value.problem.retryable is False


@pytest.mark.asyncio
async def test_capability_and_prompt_limits_fail_before_network_io() -> None:
    """缺失原生 structured-output 能力或超大 prompt 都在发网前拒绝。"""
    blind = CapabilityBlindProvider()
    provider = StreamingTextAgentProvider(
        blind,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )
    with pytest.raises(AgentProviderFailure) as capability_error:
        await provider.execute(_request(modes=(AgentOutputMode.TEXT,)))
    assert capability_error.value.problem.code == "agent.provider_capability_unavailable"
    assert blind.calls == 0

    transport = RecordingStreamProvider()
    bounded = StreamingTextAgentProvider(
        transport,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )
    request = _request(modes=(AgentOutputMode.TEXT,))
    oversized_message = replace(
        request.input_message,
        content=tuple(TextContentPart("x" * 200_000) for _ in range(6)),
    )
    with pytest.raises(AgentProviderFailure) as size_error:
        await bounded.execute(replace(request, input_message=oversized_message))
    assert size_error.value.problem.code == "agent.provider_input_too_large"
    assert transport.calls == []
