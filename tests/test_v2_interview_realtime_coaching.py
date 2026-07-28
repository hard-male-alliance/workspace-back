"""Provider-neutral realtime Interview follow-up tests."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from backend.application.interview_v2 import RealtimeCoachingContext
from backend.application.ports.knowledge import HybridSearchResponse
from backend.domain.interview_v2 import InterviewKnowledgeContext, JobTarget
from backend.domain.knowledge_retrieval import (
    HybridScore,
    KnowledgeSearchHit,
    KnowledgeSearchPlan,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
)
from backend.domain.principals import UserId, WorkspaceId
from backend.infrastructure.interview_realtime_coaching import (
    ProviderRealtimeInterviewCoach,
)


@dataclass(slots=True)
class _Provider:
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    async def stream_text(
        self,
        prompt: str,
        request: dict[str, Any],
    ) -> AsyncIterator[str]:
        self.calls.append((prompt, request))
        yield "你如何"
        yield "验证该结论？"


@dataclass(slots=True)
class _KnowledgeSearch:
    """@brief 记录计划并返回冻结 provenance 的知识检索桩 / Knowledge-search stub recording plans and returning frozen provenance."""

    plans: list[KnowledgeSearchPlan] = field(default_factory=list)

    async def search(self, plan: KnowledgeSearchPlan) -> HybridSearchResponse:
        """@brief 返回与请求 scope 完全一致的单条证据 / Return one evidence hit exactly matching the requested scope.

        @param plan 面试实时检索计划 / Realtime Interview retrieval plan.
        @return 单条可验证知识证据 / One verifiable Knowledge evidence hit.
        """

        self.plans.append(plan)
        scope = plan.scopes[0]
        return HybridSearchResponse(
            (
                KnowledgeSearchHit(
                    "chunk_interview_knowledge01",
                    plan.workspace_id,
                    scope.source_id,
                    scope.version_id,
                    "manual-note/architecture",
                    "所有数据库变更必须先验证回滚路径。",
                    HybridScore(0.8, None, 0.8),
                ),
            ),
            scope.policy_version,
        )


@pytest.mark.asyncio
async def test_followup_stream_uses_frozen_policy_and_visual_context() -> None:
    provider = _Provider()
    coach = ProviderRealtimeInterviewCoach(provider, None)
    context = RealtimeCoachingContext(
        "Backend",
        "Backend practical interview",
        "technical",
        "advanced",
        ("Python", "PostgreSQL"),
        "zh-CN",
        True,
        (),
        "global",
        WorkspaceId("workspace_realtime_coach01"),
        UserId("user_realtime_coach0001"),
        "interview_coach",
        (),
        JobTarget(
            "Backend Engineer",
            "Example",
            None,
            "Build reliable services.",
            None,
            "senior",
            ("Python", "PostgreSQL"),
        ),
    )

    chunks = [
        value
        async for value in coach.stream_followup(
            context,
            "我通过慢查询日志发现缺少索引。",
            "候选人展示查询计划。",
            (("interviewer", "你如何定位性能瓶颈？"),),
            operation_id="input_followup0001:followup",
        )
    ]

    assert chunks == ["你如何", "验证该结论？"]
    prompt, request = provider.calls[0]
    assert "慢查询日志" in prompt
    assert "候选人展示查询计划" in prompt
    assert "你如何定位性能瓶颈" in prompt
    assert request["capability"] == "interview_coach"
    assert request["inference"] == {
        "data_region": "global",
        "allow_external_model_processing": True,
        "allow_provider_fallback": False,
    }


@pytest.mark.asyncio
async def test_followup_stream_injects_authorized_knowledge_evidence() -> None:
    """@brief 已授权知识检索结果进入模型提示词 / Authorized Knowledge retrieval results enter the model prompt."""

    provider = _Provider()
    knowledge_search = _KnowledgeSearch()
    source_id = KnowledgeSourceId("source_interview_knowledge01")
    version_id = KnowledgeSourceVersionId("version_interview_knowledge01")
    coach = ProviderRealtimeInterviewCoach(provider, None, knowledge_search)
    context = RealtimeCoachingContext(
        "Backend",
        "Backend practical interview",
        "technical",
        "advanced",
        ("Python", "PostgreSQL"),
        "zh-CN",
        True,
        (),
        "global",
        WorkspaceId("workspace_realtime_coach02"),
        UserId("user_realtime_coach0002"),
        "interview_coach",
        (InterviewKnowledgeContext(source_id, version_id, 7),),
        JobTarget(
            "Backend Engineer",
            "Example",
            None,
            "Build reliable services.",
            None,
            "senior",
            ("Python", "PostgreSQL"),
        ),
    )

    chunks = [
        value
        async for value in coach.stream_followup(
            context,
            "我会先设计数据库迁移。",
            None,
            (),
            operation_id="input_followup0002:followup",
        )
    ]

    assert chunks == ["你如何", "验证该结论？"]
    assert len(knowledge_search.plans) == 1
    assert knowledge_search.plans[0].agent_scope == "interview_coach"
    payload = json.loads(provider.calls[0][0])
    assert payload["authorized_knowledge_evidence"] == [
        {
            "source_id": str(source_id),
            "version_id": str(version_id),
            "locator": "manual-note/architecture",
            "quote": "所有数据库变更必须先验证回滚路径。",
        }
    ]
    assert (
        "Ground the question in at least one concrete fact or topic from "
        "authorized_knowledge_evidence"
    ) in payload["task"]
