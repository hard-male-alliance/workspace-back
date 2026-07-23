"""API V2 Agent Knowledge provenance 运行时测试。"""

from __future__ import annotations

from dataclasses import replace

import pytest

from backend.application.ports.agent_v2 import AgentKnowledgeRetrievalRequest
from backend.application.ports.knowledge import HybridSearchResponse
from backend.domain.agent_v2 import AgentExecutionGrant, AuthorizedKnowledgeContext
from backend.domain.knowledge_retrieval import (
    HybridScore,
    KnowledgeSearchHit,
    KnowledgeSearchPlan,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    ModelRegion,
)
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.agent_retrieval import GrantedAgentKnowledgeRetriever

WORKSPACE = WorkspaceId("workspace_retrieval_0001")
ACTOR = UserId("user_retrieval_0001")
SOURCE_A = KnowledgeSourceId("knowledge_source_retrieval_a")
SOURCE_B = KnowledgeSourceId("knowledge_source_retrieval_b")
VERSION_A = KnowledgeSourceVersionId("knowledge_version_retrieval_a")
VERSION_B = KnowledgeSourceVersionId("knowledge_version_retrieval_b")


class RecordingSearch:
    """返回注入响应并记录授权计划。"""

    def __init__(self, response: HybridSearchResponse) -> None:
        self.response = response
        self.plans: list[KnowledgeSearchPlan] = []

    async def search(self, plan: KnowledgeSearchPlan) -> HybridSearchResponse:
        self.plans.append(plan)
        return self.response


def _request() -> AgentKnowledgeRetrievalRequest:
    """构造包含两个精确 source/version 的刷新 grant。"""
    grant = AgentExecutionGrant(
        ResourceRef("conversation", "conversation_retrieval_0001", 4),
        "knowledge_agent",
        ResourceRef("model", "model_retrieval_0001", 2),
        ModelRegion.CN,
        False,
        (),
        (
            AuthorizedKnowledgeContext(SOURCE_A, VERSION_A, 3),
            AuthorizedKnowledgeContext(SOURCE_B, VERSION_B, 3),
        ),
        3,
    )
    return AgentKnowledgeRetrievalRequest(
        WORKSPACE,
        ACTOR,
        grant,
        "distributed systems",
        20,
    )


def _hit(
    chunk_id: str,
    source_id: KnowledgeSourceId,
    version_id: KnowledgeSourceVersionId,
    score: float,
) -> KnowledgeSearchHit:
    """构造合法检索候选。"""
    return KnowledgeSearchHit(
        chunk_id,
        WORKSPACE,
        source_id,
        version_id,
        f"page/{chunk_id[-1]}",
        f"evidence from {chunk_id}",
        HybridScore(score, None, score),
    )


@pytest.mark.asyncio
async def test_retriever_uses_exact_grant_and_stably_labels_server_evidence() -> None:
    """乱序 adapter 结果按服务端分数排序并获得连续 label。"""
    search = RecordingSearch(
        HybridSearchResponse(
            (
                _hit("knowledge_chunk_retrieval_b", SOURCE_B, VERSION_B, 0.4),
                _hit("knowledge_chunk_retrieval_a", SOURCE_A, VERSION_A, 0.9),
            ),
            3,
        )
    )

    evidence = await GrantedAgentKnowledgeRetriever(search).retrieve(_request())

    assert [item.label for item in evidence] == [0, 1]
    assert [item.chunk_id for item in evidence] == [
        "knowledge_chunk_retrieval_a",
        "knowledge_chunk_retrieval_b",
    ]
    plan = search.plans[0]
    assert plan.workspace_id == WORKSPACE
    assert plan.actor_id == ACTOR
    assert {
        (scope.source_id, scope.version_id, scope.policy_version)
        for scope in plan.scopes
    } == {
        (SOURCE_A, VERSION_A, 3),
        (SOURCE_B, VERSION_B, 3),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    (
        HybridSearchResponse(
            (
                replace(
                    _hit(
                        "knowledge_chunk_retrieval_a",
                        SOURCE_A,
                        VERSION_A,
                        0.9,
                    ),
                    workspace_id=WorkspaceId("workspace_retrieval_foreign"),
                ),
            ),
            3,
        ),
        HybridSearchResponse(
            (
                _hit("knowledge_chunk_retrieval_a", SOURCE_A, VERSION_A, 0.9),
                _hit("knowledge_chunk_retrieval_a", SOURCE_A, VERSION_A, 0.8),
            ),
            3,
        ),
        HybridSearchResponse(
            (_hit("knowledge_chunk_retrieval_a", SOURCE_A, VERSION_A, 0.9),),
            2,
        ),
    ),
)
async def test_retriever_rejects_foreign_duplicate_or_stale_results(
    response: HybridSearchResponse,
) -> None:
    """不可信 adapter 不能越出 Workspace、重复 chunk 或降级 policy 水位。"""
    retriever = GrantedAgentKnowledgeRetriever(RecordingSearch(response))

    with pytest.raises(RuntimeError):
        await retriever.retrieve(_request())
