"""OpenAI Agents SDK integration tests for the API V2 Agent provider."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from agents.exceptions import ModelBehaviorError
from agents.items import ModelResponse, TResponseStreamEvent
from agents.models.chatcmpl_converter import Converter
from agents.models.interface import Model, ModelTracing
from agents.usage import Usage
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)

from backend.application.ports.agent_v2 import (
    AgentKnowledgeRetrievalRequest,
    AgentProviderFailure,
)
from backend.domain.agent_v2 import (
    AgentDomainError,
    AgentExecutionGrant,
    AgentKnowledgeEvidence,
    AgentOutputMode,
    AgentProposalDecisionContext,
    AgentProviderCompleted,
    AgentProviderProposalDecisionRequired,
    AgentProviderRequest,
    AgentResumeContext,
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
from backend.domain.principals import ResourceMeta, UserId, WorkspaceId
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
from backend.infrastructure.agents_sdk_provider import (
    OpenAIAgentsSDKProvider,
    _checkpoint_tool_call_count,
)

NOW = datetime(2026, 7, 23, 8, 0, tzinfo=UTC)
WORKSPACE_ID = WorkspaceId("workspace_provider_0001")


def test_persisted_checkpoint_preserves_tool_ordinal_offset() -> None:
    """@brief JSONB 数组仍须计入既有工具序号 / Count persisted JSONB arrays in the tool ordinal offset."""

    assert _checkpoint_tool_call_count(
        {
            "generated_items": [
                {"type": "tool_call_item"},
                {"type": "tool_call_output_item"},
                {"type": "tool_call_item"},
            ]
        }
    ) == 2


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
        self.instructions: list[str | None] = []

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
            model_settings,
            handoffs,
            tracing,
            previous_response_id,
            conversation_id,
            prompt,
        )
        self.instructions.append(system_instructions)
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


class ChatCompletionValidatingSequenceModel(SequenceModel):
    """@brief 使用真实 Chat Completions 转换器校验 SDK 输入 / Validate SDK input with the real Chat Completions converter."""

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
        """@brief 在返回脚本响应前执行生产转换 / Run the production conversion before returning a scripted response.

        @param system_instructions 系统指令 / System instructions.
        @param input SDK 模型输入 / SDK model input.
        @param model_settings 模型设置 / Model settings.
        @param tools 工具目录 / Tool catalog.
        @param output_schema 输出 Schema / Output schema.
        @param handoffs Agent 交接 / Agent handoffs.
        @param tracing 跟踪设置 / Tracing settings.
        @param previous_response_id 前一响应 ID / Previous response ID.
        @param conversation_id Provider 会话 ID / Provider conversation ID.
        @param prompt Provider prompt / Provider prompt.
        @return 下一条脚本化模型响应 / The next scripted model response.
        """

        Converter.items_to_messages(input)
        return await super().get_response(
            system_instructions,
            input,
            model_settings,
            tools,
            output_schema,
            handoffs,
            tracing,
            previous_response_id=previous_response_id,
            conversation_id=conversation_id,
            prompt=prompt,
        )


class RecordingTelemetry:
    """@brief 记录测试遥测且不执行 I/O / Record test telemetry without performing I/O."""

    def __init__(self) -> None:
        """@brief 初始化捕获列表 / Initialize capture lists."""

        self.metrics: list[tuple[str, dict[str, object]]] = []
        self.logs: list[tuple[str, dict[str, object]]] = []

    def record_metric(
        self,
        name: str,
        value: float,
        scope: object,
        request_id: str | None,
        attributes: dict[str, object],
        **kwargs: object,
    ) -> bool:
        """@brief 捕获 metric / Capture one metric.

        @param name 仪器名 / Instrument name.
        @param value 指标值 / Metric value.
        @param scope actor 范围 / Actor scope.
        @param request_id 请求 ID / Request ID.
        @param attributes 脱敏属性 / Redacted attributes.
        @param kwargs 可选端口参数 / Optional port arguments.
        @return 始终接受 / Always accepted.
        """

        del value, scope, request_id, kwargs
        self.metrics.append((name, dict(attributes)))
        return True

    def record_log(
        self,
        name: str,
        severity_number: object,
        severity_text: str,
        scope: object,
        request_id: str | None,
        attributes: dict[str, object],
        **kwargs: object,
    ) -> bool:
        """@brief 捕获 log / Capture one log.

        @param name 事件名 / Event name.
        @param severity_number 严重度编号 / Severity number.
        @param severity_text 严重度文本 / Severity text.
        @param scope actor 范围 / Actor scope.
        @param request_id 请求 ID / Request ID.
        @param attributes 脱敏属性 / Redacted attributes.
        @param kwargs 可选端口参数 / Optional port arguments.
        @return 始终接受 / Always accepted.
        """

        del severity_number, severity_text, scope, request_id, kwargs
        self.logs.append((name, dict(attributes)))
        return True


class EmptyKnowledgeRetriever:
    """@brief 记录原生工具检索并返回零命中 / Record native tool retrieval and return no hits."""

    def __init__(self) -> None:
        """@brief 初始化请求列表 / Initialize the request list."""

        self.requests: list[AgentKnowledgeRetrievalRequest] = []

    async def retrieve(
        self,
        request: AgentKnowledgeRetrievalRequest,
    ) -> tuple[AgentKnowledgeEvidence, ...]:
        """@brief 返回空证据 / Return empty evidence.

        @param request 已授权检索请求 / Authorized retrieval request.
        @return 空证据元组 / Empty evidence tuple.
        """

        self.requests.append(request)
        return ()


class StaticKnowledgeRetriever:
    """@brief 返回固定的已授权证据 / Return fixed authorized evidence."""

    def __init__(self, evidence: tuple[AgentKnowledgeEvidence, ...]) -> None:
        """@brief 绑定测试证据 / Bind test evidence.

        @param evidence 固定证据 / Fixed evidence.
        """

        self.evidence = evidence

    async def retrieve(
        self,
        request: AgentKnowledgeRetrievalRequest,
    ) -> tuple[AgentKnowledgeEvidence, ...]:
        """@brief 返回固定证据 / Return fixed evidence.

        @param request 已授权检索请求 / Authorized retrieval request.
        @return 固定证据 / Fixed evidence.
        """

        del request
        return self.evidence


class FailingKnowledgeRetriever:
    """@brief 模拟检索基础设施失败 / Simulate retrieval infrastructure failure."""

    async def retrieve(
        self,
        request: AgentKnowledgeRetrievalRequest,
    ) -> tuple[AgentKnowledgeEvidence, ...]:
        """@brief 抛出不向用户泄漏的底层错误 / Raise an internal error not exposed to users.

        @param request 已授权检索请求 / Authorized retrieval request.
        @return 永不返回 / Never returns.
        @raise RuntimeError 模拟适配器故障 / Simulated adapter failure.
        """

        del request
        raise RuntimeError("simulated private retrieval failure")


class EvidenceThenFailingRetriever:
    """@brief 首次返回证据、随后模拟故障 / Return evidence once, then simulate failure."""

    def __init__(self, evidence: AgentKnowledgeEvidence) -> None:
        """@brief 绑定首次证据 / Bind first-call evidence.

        @param evidence 首次返回的证据 / Evidence returned on the first call.
        """

        self.evidence = evidence
        self.call_count = 0

    async def retrieve(
        self,
        request: AgentKnowledgeRetrievalRequest,
    ) -> tuple[AgentKnowledgeEvidence, ...]:
        """@brief 首次成功、后续失败 / Succeed once and fail afterwards.

        @param request 已授权检索请求 / Authorized retrieval request.
        @return 首次调用的证据 / Evidence on the first call.
        @raise RuntimeError 第二次及以后模拟故障 / Simulated failure after the first call.
        """

        del request
        self.call_count += 1
        if self.call_count == 1:
            return (self.evidence,)
        raise RuntimeError("simulated private retrieval failure after evidence")


class SlowModel(SequenceModel):
    """@brief 模拟超过 Run 总时限的模型 / Simulate a model exceeding the Run deadline."""

    def __init__(self, responses: list[ModelResponse], *, delay_seconds: float = 1) -> None:
        """@brief 创建可控延迟模型 / Create a model with a controllable delay.

        @param responses 按顺序返回的模型结果 / Model responses returned in order.
        @param delay_seconds 每次模型调用的等待秒数 / Delay in seconds for each model call.
        """

        super().__init__(responses)
        self.delay_seconds = delay_seconds

    async def get_response(self, *args: Any, **kwargs: Any) -> ModelResponse:
        """@brief 等待后返回模型结果 / Return a model result after a delay.

        @param args SDK 位置参数 / SDK positional arguments.
        @param kwargs SDK 关键字参数 / SDK keyword arguments.
        @return 不应在超时测试中到达的结果 / A result not reached by the timeout test.
        """

        await asyncio.sleep(self.delay_seconds)
        return await super().get_response(*args, **kwargs)


class FailingModel(SequenceModel):
    """@brief 抛出指定 Provider 异常 / Raise a specified provider exception."""

    def __init__(self, error: Exception) -> None:
        super().__init__([])
        self.error = error

    async def get_response(self, *args: Any, **kwargs: Any) -> ModelResponse:
        """@brief 抛出测试异常 / Raise the test exception.

        @param args SDK 位置参数 / SDK positional arguments.
        @param kwargs SDK 关键字参数 / SDK keyword arguments.
        @return 永不返回 / Never returns.
        """

        del args, kwargs
        raise self.error


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


def test_proposal_continuation_cannot_supply_resume_context_outside_grant() -> None:
    """A committed decision does not exempt its Resume snapshot from grant coverage."""

    request = _resume_request()
    assert request.resume_context is not None
    with pytest.raises(AgentDomainError, match="Resume context exceeds"):
        replace(
            request,
            grant=replace(request.grant, context_refs=()),
            proposal_decision=AgentProposalDecisionContext(
                ResourceRef("resume_proposal", "proposal_provider_0001", 2),
                "accept",
                request.resume_context.resume_ref,
            ),
            provider_state={"response_id": "response_proposal_0001"},
        )


def test_proposal_continuation_requires_its_authorized_resume_context() -> None:
    """A Proposal decision is only valid for a Resume-edit provider request."""

    request = _request()
    with pytest.raises(AgentDomainError, match="Proposal decision does not match"):
        replace(
            request,
            proposal_decision=AgentProposalDecisionContext(
                ResourceRef("resume_proposal", "proposal_provider_0001", 2),
                "accept",
                ResourceRef("resume", "resume_provider_0001", 1),
            ),
            provider_state={"response_id": "response_proposal_0001"},
        )


@pytest.mark.asyncio
async def test_resume_provider_preserves_explicit_date_facts_in_model_input() -> None:
    """@brief 显式年份进入模型输入且提示禁止替换 / Preserve an explicit year and forbid replacing it."""

    model = SequenceModel([_text_response("已记住。", "response_explicit_year")])
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )
    request = _resume_request()
    history = Message(
        ResourceMeta(MessageId("message_explicit_year_0001"), 1, NOW, NOW),
        WORKSPACE_ID,
        request.spec.conversation_id,
        1,
        MessageRole.USER,
        None,
        (TextContentPart("我于 2026 年毕业。"),),
    )

    await provider.execute(
        replace(
            request,
            input_message=replace(request.input_message, sequence=2),
            conversation_history=(history,),
        )
    )

    model_input = repr(model.calls[0][0])
    assert "2026" in model_input
    assert "2025" not in model_input
    instructions = model.instructions[0] or ""
    assert "explicit dates" in instructions
    assert "Never replace" in instructions


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
async def test_run_telemetry_distinguishes_empty_authorized_knowledge_retrieval() -> None:
    """@brief Run 遥测区分已授权零命中检索 / Run telemetry distinguishes authorized zero-hit retrieval."""

    source_id = KnowledgeSourceId("knowledge_source_provider_0001")
    version_id = KnowledgeSourceVersionId("knowledge_version_provider_0001")
    base = _resume_request()
    request = replace(
        base,
        spec=replace(
            base.spec,
            knowledge=KnowledgeSelection(
                KnowledgeSelectionMode.EXPLICIT,
                (source_id,),
                (),
                (),
                "resume_assistant",
            ),
        ),
        grant=replace(
            base.grant,
            knowledge_contexts=(AuthorizedKnowledgeContext(source_id, version_id, 1),),
        ),
    )
    model = SequenceModel(
        [
            _tool_response(
                "knowledge_search",
                '{"query":"高级前端工程师岗位要求","top_k":10}',
                "call_knowledge_empty_0001",
            ),
            _text_response("未找到可引用内容。", "response_empty_knowledge"),
        ]
    )
    telemetry = RecordingTelemetry()
    retriever = EmptyKnowledgeRetriever()
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
        telemetry=telemetry,  # type: ignore[arg-type]
        knowledge_retriever=retriever,
    )

    outcome = await provider.execute(
        replace(request, actor_id=UserId("user_provider_knowledge_0001"))
    )

    assert isinstance(outcome, AgentProviderCompleted)
    attributes = next(
        attributes for name, attributes in telemetry.metrics if name == "aiws.agent.run.duration"
    )
    assert attributes["knowledge_context_count"] == 1
    assert attributes["knowledge_source_count"] == 1
    assert attributes["knowledge_evidence_attached_count"] == 0
    assert attributes["knowledge_retrieval_status"] == "completed_empty"
    assert attributes["knowledge_tool_call_count"] == 1
    assert retriever.requests[0].query == "高级前端工程师岗位要求"
    assert {tool.name for tool in model.calls[0][1]} >= {
        "knowledge_search",
        "resume_read_snapshot",
    }


@pytest.mark.asyncio
async def test_native_knowledge_tool_returns_untrusted_evidence_and_preserves_provenance() -> None:
    """@brief 原生检索返回数据并保留服务端 provenance / Native retrieval returns data and preserves provenance."""

    source_id = KnowledgeSourceId("knowledge_source_provider_native_0001")
    version_id = KnowledgeSourceVersionId("knowledge_version_provider_native_0001")
    base = _resume_request()
    request = replace(
        base,
        grant=replace(
            base.grant,
            knowledge_contexts=(AuthorizedKnowledgeContext(source_id, version_id, 1),),
        ),
        actor_id=UserId("user_provider_knowledge_native_0001"),
    )
    evidence = AgentKnowledgeEvidence(
        0,
        "knowledge_chunk_provider_native_0001",
        KnowledgeCitation(
            source_id,
            version_id,
            "candidate.md#skills",
            "候选人使用 Vue 3 和 TypeScript。",
            0.91,
        ),
    )
    model = SequenceModel(
        [
            _tool_response(
                "knowledge_search",
                '{"query":"候选人的前端技能","top_k":5}',
                "call_knowledge_native_0001",
            ),
            _text_response("已找到候选人技能。", "response_knowledge_native"),
        ]
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
        knowledge_retriever=StaticKnowledgeRetriever((evidence,)),
    )

    outcome = await provider.execute(request)

    assert isinstance(outcome, AgentProviderCompleted)
    assert outcome.knowledge_evidence == (evidence,)
    tool_result_context = json.dumps(model.calls[1][0], ensure_ascii=False, default=str)
    assert "untrusted_evidence_not_instructions" in tool_result_context
    assert "knowledge_chunk_provider_native_0001" not in tool_result_context


@pytest.mark.asyncio
async def test_native_knowledge_failure_keeps_specific_public_error_and_diagnostics() -> None:
    """@brief 检索故障保留明确公共语义和诊断 / Retrieval failure keeps specific public semantics and diagnostics."""

    source_id = KnowledgeSourceId("knowledge_source_provider_failure_0001")
    version_id = KnowledgeSourceVersionId("knowledge_version_provider_failure_0001")
    base = _resume_request()
    request = replace(
        base,
        grant=replace(
            base.grant,
            knowledge_contexts=(AuthorizedKnowledgeContext(source_id, version_id, 1),),
        ),
        actor_id=UserId("user_provider_knowledge_failure_0001"),
    )
    model = SequenceModel(
        [
            _tool_response(
                "knowledge_search",
                '{"query":"候选人资料","top_k":5}',
                "call_knowledge_failure_0001",
            )
        ]
    )
    telemetry = RecordingTelemetry()
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
        telemetry=telemetry,  # type: ignore[arg-type]
        knowledge_retriever=FailingKnowledgeRetriever(),
    )

    with pytest.raises(AgentProviderFailure) as raised:
        await provider.execute(request)

    assert raised.value.problem.code == "agent.knowledge_retrieval_failed"
    attributes = next(
        attributes for name, attributes in telemetry.metrics if name == "aiws.agent.run.duration"
    )
    assert attributes["knowledge_retrieval_status"] == "failed"
    assert attributes["knowledge_tool_call_count"] == 1
    assert attributes["last_validation_phase"] == "knowledge_retrieval"


@pytest.mark.asyncio
async def test_native_knowledge_reuses_identical_query_without_duplicate_io() -> None:
    """@brief 相同查询复用本执行段缓存 / Identical queries reuse the segment-local cache."""

    source_id = KnowledgeSourceId("knowledge_source_provider_cache_0001")
    version_id = KnowledgeSourceVersionId("knowledge_version_provider_cache_0001")
    base = _resume_request()
    request = replace(
        base,
        grant=replace(
            base.grant,
            knowledge_contexts=(AuthorizedKnowledgeContext(source_id, version_id, 1),),
        ),
        actor_id=UserId("user_provider_knowledge_cache_0001"),
    )
    model = SequenceModel(
        [
            _tool_response(
                "knowledge_search",
                '{"query":"候选人技能","top_k":8}',
                "call_knowledge_cache_0001",
            ),
            _tool_response(
                "knowledge_search",
                '{"query":"  候选人技能  ","top_k":8}',
                "call_knowledge_cache_0002",
            ),
            _text_response("没有更多证据。", "response_knowledge_cache"),
        ]
    )
    telemetry = RecordingTelemetry()
    retriever = EmptyKnowledgeRetriever()
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
        telemetry=telemetry,  # type: ignore[arg-type]
        knowledge_retriever=retriever,
    )

    outcome = await provider.execute(request)

    assert isinstance(outcome, AgentProviderCompleted)
    assert len(retriever.requests) == 1
    attributes = next(
        attributes for name, attributes in telemetry.metrics if name == "aiws.agent.run.duration"
    )
    assert attributes["knowledge_tool_call_count"] == 2
    assert attributes["knowledge_retrieval_count"] == 1
    assert attributes["knowledge_cache_hit_count"] == 1


@pytest.mark.asyncio
async def test_native_knowledge_degrades_after_evidence_instead_of_failing_run() -> None:
    """@brief 已有证据后检索故障应降级继续 / Retrieval failure after evidence degrades instead of failing."""

    source_id = KnowledgeSourceId("knowledge_source_provider_degraded_0001")
    version_id = KnowledgeSourceVersionId("knowledge_version_provider_degraded_0001")
    base = _resume_request()
    request = replace(
        base,
        grant=replace(
            base.grant,
            knowledge_contexts=(AuthorizedKnowledgeContext(source_id, version_id, 1),),
        ),
        actor_id=UserId("user_provider_knowledge_degraded_0001"),
    )
    evidence = AgentKnowledgeEvidence(
        0,
        "knowledge_chunk_provider_degraded_0001",
        KnowledgeCitation(
            source_id,
            version_id,
            "candidate.md#profile",
            "候选人具备四年前端开发经验。",
            0.88,
        ),
    )
    retriever = EvidenceThenFailingRetriever(evidence)
    model = SequenceModel(
        [
            _tool_response(
                "knowledge_search",
                '{"query":"候选人前端经验","top_k":8}',
                "call_knowledge_degraded_0001",
            ),
            _tool_response(
                "knowledge_search",
                '{"query":"候选人项目指标","top_k":8}',
                "call_knowledge_degraded_0002",
            ),
            _text_response("已使用现有证据继续处理。", "response_knowledge_degraded"),
        ]
    )
    telemetry = RecordingTelemetry()
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
        telemetry=telemetry,  # type: ignore[arg-type]
        knowledge_retriever=retriever,
    )

    outcome = await provider.execute(request)

    assert isinstance(outcome, AgentProviderCompleted)
    assert outcome.knowledge_evidence == (evidence,)
    assert outcome.tool_invocations[-1].status == "failure"
    second_tool_result = json.dumps(model.calls[2][0], ensure_ascii=False, default=str)
    assert "knowledge_search_degraded" in second_tool_result
    attributes = next(
        attributes for name, attributes in telemetry.metrics if name == "aiws.agent.run.duration"
    )
    assert attributes["knowledge_retrieval_status"] == "degraded_with_cached_evidence"


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
        "resume_draft_set_document_title",
        "resume_draft_set_field",
        "resume_request_proposal_decision",
    }
    proposal_tool = next(
        tool for tool in native_tools if tool.name == "resume_request_proposal_decision"
    )
    assert proposal_tool.needs_approval is True
    assert model.calls[0][2] is None

    assert request.resume_context is not None
    accepted_document = replace(
        request.resume_context.document,
        meta=request.resume_context.document.meta.advance(NOW),
    )
    accepted_ref = ResourceRef("resume", "resume_provider_0001", 2)
    resumed = await provider.execute(
        replace(
            request,
            spec=replace(request.spec, context_refs=(accepted_ref,)),
            grant=replace(request.grant, context_refs=(accepted_ref,)),
            resume_context=AgentResumeContext(accepted_ref, accepted_document),
            proposal_decision=AgentProposalDecisionContext(
                ResourceRef("resume_proposal", "proposal_provider_0001", 2),
                "accept",
                accepted_ref,
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


@pytest.mark.asyncio
async def test_proposal_resume_normalizes_serialized_assistant_history_for_chat_completions() -> None:
    """@brief 恢复 Proposal 时保持历史 assistant 消息可被真实转换器读取 / Keep restored assistant history readable by the production converter."""

    model = ChatCompletionValidatingSequenceModel(
        [
            _tool_response(
                "resume_draft_set_field",
                '{"entity_id":"resume_provider_0001","field_path":["title"],'
                '"value":"Focused title"}',
                "call_draft_history_0001",
            ),
            _tool_response(
                "resume_request_proposal_decision",
                '{"title":"Improve title with history"}',
                "call_proposal_history_0001",
            ),
            _text_response("Proposal continuation completed.", "response_history_resumed"),
        ]
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )
    request = _resume_request()
    history = Message(
        ResourceMeta(MessageId("message_assistant_history_0001"), 1, NOW, NOW),
        WORKSPACE_ID,
        request.spec.conversation_id,
        1,
        MessageRole.ASSISTANT,
        None,
        (TextContentPart("Previously confirmed facts."),),
        source_run_id=AgentRunId("agent_run_history_0001"),
    )
    request_with_history = replace(
        request,
        input_message=replace(request.input_message, sequence=2),
        conversation_history=(history,),
    )

    interrupted = await provider.execute(request_with_history)
    assert isinstance(interrupted, AgentProviderProposalDecisionRequired)
    assert request.resume_context is not None
    accepted_ref = ResourceRef("resume", "resume_provider_0001", 2)
    accepted_document = replace(
        request.resume_context.document,
        meta=request.resume_context.document.meta.advance(NOW),
    )

    resumed = await provider.execute(
        replace(
            request_with_history,
            spec=replace(request.spec, context_refs=(accepted_ref,)),
            grant=replace(request.grant, context_refs=(accepted_ref,)),
            resume_context=AgentResumeContext(accepted_ref, accepted_document),
            proposal_decision=AgentProposalDecisionContext(
                ResourceRef("resume_proposal", "proposal_provider_history_0001", 2),
                "accept",
                accepted_ref,
            ),
            provider_state=interrupted.provider_state,
        )
    )

    assert isinstance(resumed, AgentProviderCompleted)
    assert resumed.content == (TextContentPart("Proposal continuation completed."),)


@pytest.mark.asyncio
async def test_run_deadline_returns_specific_retryable_problem() -> None:
    model = SlowModel(
        [_text_response("late", "response_late")],
        delay_seconds=0.2,
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
        execution_timeout_ms=50,
    )
    request = replace(
        _request(),
        spec=replace(
            _request().spec,
            inference=replace(_request().spec.inference, latency_budget_ms=100),
        ),
    )

    with pytest.raises(AgentProviderFailure) as captured:
        await provider.execute(request)

    assert captured.value.problem.code == "agent.execution_timeout"
    assert captured.value.problem.status == 504
    assert captured.value.problem.retryable is True


@pytest.mark.asyncio
async def test_latency_budget_does_not_cancel_the_agent_run() -> None:
    model = SlowModel(
        [_text_response("completed after latency target", "response_completed")],
        delay_seconds=0.2,
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
        execution_timeout_ms=1_000,
    )
    request = replace(
        _request(),
        spec=replace(
            _request().spec,
            inference=replace(_request().spec.inference, latency_budget_ms=100),
        ),
    )

    outcome = await provider.execute(request)

    assert isinstance(outcome, AgentProviderCompleted)
    assert outcome.content == (TextContentPart("completed after latency target"),)


@pytest.mark.asyncio
async def test_model_behavior_error_is_not_masked_as_generic_provider_failure() -> None:
    provider = OpenAIAgentsSDKProvider(
        FailingModel(ModelBehaviorError("invalid tool protocol")),
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )

    with pytest.raises(AgentProviderFailure) as captured:
        await provider.execute(_request())

    assert captured.value.problem.code == "agent.provider_protocol_error"
    assert captured.value.problem.status == 502
    assert captured.value.problem.retryable is False


@pytest.mark.asyncio
async def test_repetitive_valid_tool_loop_returns_tool_call_budget_problem() -> None:
    responses = [
        _tool_response("resume_read_snapshot", "{}", f"call_snapshot_{index:02d}")
        for index in range(20)
    ]
    provider = OpenAIAgentsSDKProvider(
        SequenceModel(responses),
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )

    with pytest.raises(AgentProviderFailure) as captured:
        await provider.execute(_resume_request())

    assert captured.value.problem.code == "agent.tool_call_budget_exhausted"
    assert captured.value.problem.status == 503
    assert captured.value.problem.retryable is False


@pytest.mark.asyncio
async def test_proposal_decision_remains_resumable_at_tool_call_budget() -> None:
    """@brief 编辑预算用尽也不能阻断已提交 Proposal 的决定 / Preserve proposal decisions at the tool budget."""

    model = SequenceModel(
        [
            *[
                _tool_response(
                    "resume_read_snapshot",
                    "{}",
                    f"call_snapshot_{index:02d}",
                )
                for index in range(15)
            ],
            _tool_response(
                "resume_draft_set_field",
                '{"entity_id":"resume_provider_0001","field_path":["title"],'
                '"value":"Focused title"}',
                "call_draft_budget_0001",
            ),
            _tool_response(
                "resume_request_proposal_decision",
                '{"title":"Improve title at budget"}',
                "call_proposal_budget_0001",
            ),
            _text_response("已接受建议。", "response_budget_resumed"),
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
    assert request.resume_context is not None
    # @brief 接受修改后的权威简历引用 / Authoritative Resume reference after acceptance.
    accepted_ref = ResourceRef("resume", "resume_provider_0001", 2)
    # @brief 接受修改后的权威简历文档 / Authoritative Resume document after acceptance.
    accepted_document = replace(
        request.resume_context.document,
        meta=request.resume_context.document.meta.advance(NOW),
    )

    assert request.resume_context is not None
    accepted_ref = ResourceRef("resume", "resume_provider_0001", 2)
    accepted_document = replace(
        request.resume_context.document,
        meta=request.resume_context.document.meta.advance(NOW),
    )
    resumed = await provider.execute(
        replace(
            request,
            spec=replace(request.spec, context_refs=(accepted_ref,)),
            grant=replace(request.grant, context_refs=(accepted_ref,)),
            resume_context=AgentResumeContext(accepted_ref, accepted_document),
            proposal_decision=AgentProposalDecisionContext(
                ResourceRef("resume_proposal", "proposal_provider_budget_0001", 2),
                "accept",
                accepted_ref,
            ),
            provider_state=interrupted.provider_state,
        )
    )

    assert isinstance(resumed, AgentProviderCompleted)
    assert resumed.content == (TextContentPart("已接受建议。"),)
    assert resumed.tool_invocations[0].tool_name == "resume_request_proposal_decision"


@pytest.mark.asyncio
async def test_invalid_tool_arguments_are_returned_to_model_for_recovery() -> None:
    model = SequenceModel(
        [
            _tool_response(
                "resume_draft_set_fields",
                '{"updates":[]}',
                "call_invalid_batch_0001",
            ),
            _tool_response(
                "resume_draft_set_field",
                '{"entity_id":"resume_provider_0001","field_path":["title"],'
                '"value":"Recovered title"}',
                "call_recovered_draft_0001",
            ),
            _tool_response(
                "resume_request_proposal_decision",
                '{"title":"Recover from malformed batch arguments"}',
                "call_recovered_proposal_0001",
            ),
        ]
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )

    outcome = await provider.execute(_resume_request())

    assert isinstance(outcome, AgentProviderProposalDecisionRequired)
    assert [trace.status for trace in outcome.tool_invocations] == [
        "invalid",
        "completed",
        "decision_required",
    ]
    assert "invalid_tool_arguments" in repr(model.calls[1][0])
    invalid_trace = outcome.tool_invocations[0]
    assert invalid_trace.result_kind == "invalid_tool_arguments"
    assert invalid_trace.result_code == "agent.tool_arguments_invalid"
    assert invalid_trace.validation_phase == "arguments_schema"
    assert invalid_trace.consecutive_invalid_count == 1
    assert invalid_trace.argument_signature is not None


@pytest.mark.asyncio
async def test_invalid_section_json_feedback_recommends_smaller_draft_tools() -> None:
    """@brief 章节 JSON 错误返回可执行的拆分建议 / Return an executable decomposition hint for invalid section JSON."""

    model = SequenceModel(
        [
            _tool_response(
                "resume_draft_upsert_section",
                '{"section":',
                "call_invalid_section_json_0001",
            ),
            _text_response("请重试。", "response_after_invalid_section_json"),
        ]
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )

    outcome = await provider.execute(_resume_request())

    assert isinstance(outcome, AgentProviderCompleted)
    feedback = repr(model.calls[1][0])
    assert "resume_draft_upsert_section" in feedback
    assert "items=[]" in feedback
    assert "resume_draft_upsert_item" in feedback


@pytest.mark.asyncio
async def test_date_normalization_emits_content_free_tool_telemetry() -> None:
    """@brief 日期归一化进入遥测但不泄漏原文 / Emit date telemetry without raw content."""

    arguments = {
        "section": {
            "id": "tmp_section_experience_01",
            "kind": "experience",
            "title": "工作经历",
            "visible": True,
            "content": None,
            "items": [
                {
                    "id": "tmp_item_experience_01",
                    "kind": "experience",
                    "title": "高级前端工程师",
                    "subtitle": None,
                    "organization": "示例公司",
                    "location": None,
                    "date_range": {
                        "start": "２０２４．３",
                        "end": "至今",
                    },
                    "summary": None,
                    "highlights": [],
                    "skills": [],
                    "tags": [],
                    "visible": True,
                    "url": None,
                }
            ],
        },
        "after_section_id": None,
    }
    model = SequenceModel(
        [
            _tool_response(
                "resume_draft_upsert_section",
                json.dumps(arguments, ensure_ascii=False),
                "call_normalized_date_0001",
            ),
            _tool_response(
                "resume_request_proposal_decision",
                '{"title":"新增工作经历"}',
                "call_normalized_date_proposal_0001",
            ),
        ]
    )
    telemetry = RecordingTelemetry()
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
        telemetry=telemetry,  # type: ignore[arg-type]
    )

    outcome = await provider.execute(_resume_request())

    assert isinstance(outcome, AgentProviderProposalDecisionRequired)
    tool_attributes = next(
        attributes
        for name, attributes in telemetry.metrics
        if name == "aiws.agent.tool.count"
        and attributes["operation"] == "resume_draft_upsert_section"
    )
    assert tool_attributes["date_normalization_count"] == 1
    assert tool_attributes["date_normalization_applied"] is True
    serialized = repr((telemetry.metrics, telemetry.logs))
    assert "２０２４" not in serialized
    assert "至今" not in serialized


@pytest.mark.asyncio
async def test_repeated_identical_invalid_call_stops_with_recovery_error() -> None:
    model = SequenceModel(
        [
            _tool_response(
                "resume_draft_set_fields",
                '{"updates":[]}',
                "call_invalid_batch_0001",
            ),
            _tool_response(
                "resume_draft_set_fields",
                '{"updates":[]}',
                "call_invalid_batch_0002",
            ),
            _tool_response(
                "resume_draft_set_fields",
                '{"updates":[]}',
                "call_invalid_batch_0003",
            ),
        ]
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )

    with pytest.raises(AgentProviderFailure) as captured:
        await provider.execute(_resume_request())

    assert captured.value.problem.code == "agent.tool_recovery_exhausted"
    assert captured.value.problem.status == 502
    assert captured.value.problem.retryable is True
    assert len(captured.value.invocations) == 3
    assert captured.value.invocations[-1].consecutive_invalid_count == 3
    assert captured.value.invocations[-1].validation_issues == (("updates", "too_short"),)
    assert (
        captured.value.invocations[0].argument_signature
        == captured.value.invocations[2].argument_signature
    )


@pytest.mark.asyncio
async def test_second_identical_invalid_section_call_can_recover_on_final_attempt() -> None:
    """@brief 第二次相同参数错误仍把纠正信息返回模型 / Return correction feedback after a second identical argument error."""

    invalid_arguments = '{"section":"education","after_section_id":null}'
    valid_arguments = json.dumps(
        {
            "section": {
                "id": "tmp_section_education_01",
                "kind": "education",
                "title": "教育经历",
                "visible": True,
                "content": None,
                "items": [],
            },
            "after_section_id": None,
        },
        ensure_ascii=False,
    )
    model = SequenceModel(
        [
            _tool_response(
                "resume_draft_upsert_section",
                invalid_arguments,
                "call_invalid_section_0001",
            ),
            _tool_response(
                "resume_draft_upsert_section",
                invalid_arguments,
                "call_invalid_section_0002",
            ),
            _tool_response(
                "resume_draft_upsert_section",
                valid_arguments,
                "call_recovered_section_0001",
            ),
            _tool_response(
                "resume_request_proposal_decision",
                '{"title":"新增教育经历"}',
                "call_recovered_section_proposal_0001",
            ),
        ]
    )
    provider = OpenAIAgentsSDKProvider(
        model,
        input_cost_microusd_per_million_tokens=0,
        output_cost_microusd_per_million_tokens=0,
    )

    outcome = await provider.execute(_resume_request())

    assert isinstance(outcome, AgentProviderProposalDecisionRequired)
    assert [trace.status for trace in outcome.tool_invocations] == [
        "invalid",
        "invalid",
        "completed",
        "decision_required",
    ]
    second_feedback = repr(model.calls[2][0])
    assert '"path":"section"' in second_feedback
    assert '"issue":"model_type"' in second_feedback
    assert outcome.resume_operations[0].payload["op"] == "upsert_section"
