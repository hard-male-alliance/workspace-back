"""@brief API V2 Agent 策略交集测试 / API V2 Agent policy-intersection tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from backend.application.ports.agent_v2 import (
    AgentModelRoute,
    AgentPolicyDenied,
    AgentRunPolicyRequest,
)
from backend.domain.agent_v2 import (
    AgentOutputMode,
    AgentRunSpec,
    Conversation,
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
    KnowledgeVersionPin,
)
from backend.domain.knowledge_sources import (
    KnowledgeSensitivity,
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    KnowledgeVisibilityPolicy,
    ModelRegion,
    PolicyEffect,
)
from backend.domain.principals import ResourceMeta, UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.agent_v2 import (
    InMemoryAgentContextResolver,
    InMemoryAgentPolicyStore,
    InMemoryAgentRunPolicy,
    InMemoryKnowledgePolicyEntry,
)

NOW = datetime(2026, 7, 23, tzinfo=UTC)
"""@brief 固定策略测试时间 / Fixed policy-test instant."""

WORKSPACE = WorkspaceId("workspace_policy0001")
"""@brief 策略测试 Workspace / Policy-test Workspace."""

SOURCE_ID = KnowledgeSourceId("source_policy0001")
"""@brief 策略测试 Knowledge source / Policy-test Knowledge source."""

VERSION_ID = KnowledgeSourceVersionId("version_policy0001")
"""@brief 策略测试 ready version / Policy-test ready version."""


def _conversation() -> Conversation:
    """@brief 构造策略测试会话 / Build the policy-test conversation."""
    return Conversation(
        ResourceMeta(ConversationId("conversation_policy0001"), 4, NOW, NOW),
        WORKSPACE,
        "Policy intersection",
        ConversationCapability.GENERAL,
    )


def _input_message() -> Message:
    """@brief 构造策略测试输入消息 / Build the policy-test input message."""
    return Message(
        ResourceMeta(MessageId("message_policyinput1"), 1, NOW, NOW),
        WORKSPACE,
        ConversationId("conversation_policy0001"),
        1,
        MessageRole.USER,
        None,
        (TextContentPart("Use the pinned source."),),
    )


def _inference(*, region: ModelRegion = ModelRegion.CN) -> InferenceIntent:
    """@brief 构造显式区域推理意图 / Build an explicit-region inference intent."""
    return InferenceIntent(
        InferenceQualityTier.BALANCED,
        5_000,
        InferenceCostTier.STANDARD,
        region,
        False,
        False,
    )


def _spec(
    *,
    context_revision: int = 7,
    version_id: KnowledgeSourceVersionId = VERSION_ID,
    inference: InferenceIntent | None = None,
) -> AgentRunSpec:
    """@brief 构造带精确 context 与 Knowledge pin 的 spec / Build a spec with exact context and Knowledge pin."""
    return AgentRunSpec(
        ConversationId("conversation_policy0001"),
        MessageId("message_policyinput1"),
        ConversationCapability.GENERAL,
        (ResourceRef("resume", "resume_policy0001", context_revision),),
        KnowledgeSelection(
            KnowledgeSelectionMode.EXPLICIT,
            (SOURCE_ID,),
            (),
            (KnowledgeVersionPin(SOURCE_ID, version_id),),
            "general_agent",
        ),
        _inference() if inference is None else inference,
        (AgentOutputMode.TEXT,),
        "zh-CN",
    )


def _visibility_policy(
    *,
    regions: tuple[ModelRegion, ...] = (ModelRegion.CN,),
) -> KnowledgeVisibilityPolicy:
    """@brief 构造默认允许但受 model region 约束的 policy / Build a default-allow, model-region-bound policy."""
    return KnowledgeVisibilityPolicy(
        KnowledgeSensitivity.CONFIDENTIAL,
        PolicyEffect.ALLOW,
        (),
        False,
        regions,
        False,
        30,
        5,
    )


def _store() -> InMemoryAgentPolicyStore:
    """@brief 构造跨域 context 与 Knowledge 真相 / Build cross-domain context and Knowledge truth."""
    store = InMemoryAgentPolicyStore()
    store.contexts[(WORKSPACE, "resume", "resume_policy0001")] = ResourceRef(
        "resume", "resume_policy0001", 7
    )
    store.knowledge[(WORKSPACE, SOURCE_ID)] = InMemoryKnowledgePolicyEntry(
        WORKSPACE,
        SOURCE_ID,
        True,
        VERSION_ID,
        frozenset({VERSION_ID}),
        _visibility_policy(),
    )
    return store


def _request(spec: AgentRunSpec) -> AgentRunPolicyRequest:
    """@brief 构造完整策略授权请求 / Build a complete policy-authorization request."""
    return AgentRunPolicyRequest(
        UserId("user_policyactor1"),
        WORKSPACE,
        _conversation(),
        _input_message(),
        spec,
    )


@pytest.mark.asyncio
async def test_policy_freezes_exact_context_pin_and_model_route() -> None:
    """@brief 成功授权冻结 context、pin、region 与 policy version / Successful authorization freezes context, pin, region, and policy version."""
    store = _store()
    policy = InMemoryAgentRunPolicy(
        store,
        InMemoryAgentContextResolver(store),
        (AgentModelRoute(ResourceRef("model", "model_policyroute1", 3), ModelRegion.CN, False),),
    )

    grant = await policy.authorize_run(_request(_spec()))

    assert grant.context_refs == (ResourceRef("resume", "resume_policy0001", 7),)
    assert grant.knowledge_contexts[0].source_id == SOURCE_ID
    assert grant.knowledge_contexts[0].version_id == VERSION_ID
    assert grant.model_region is ModelRegion.CN
    assert grant.policy_version == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["stale_context", "unready_pin", "region"])
async def test_policy_fails_closed_across_each_cross_domain_boundary(failure: str) -> None:
    """@brief context、pin 或 region 任一不成立都拒绝 / Reject when context, pin, or region fails independently."""
    store = _store()
    spec = _spec()
    routes = (
        AgentModelRoute(ResourceRef("model", "model_policyroute1", 3), ModelRegion.CN, False),
    )
    if failure == "stale_context":
        spec = _spec(context_revision=6)
    elif failure == "unready_pin":
        spec = _spec(version_id=KnowledgeSourceVersionId("version_unready001"))
    else:
        entry = store.knowledge[(WORKSPACE, SOURCE_ID)]
        store.knowledge[(WORKSPACE, SOURCE_ID)] = replace(
            entry,
            policy=_visibility_policy(regions=(ModelRegion.GLOBAL,)),
        )
    policy = InMemoryAgentRunPolicy(store, InMemoryAgentContextResolver(store), routes)

    with pytest.raises(AgentPolicyDenied):
        await policy.authorize_run(_request(spec))
