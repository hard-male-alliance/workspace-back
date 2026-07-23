"""@brief Agent 对已授权 Knowledge allowlist 的真实检索 / Real Agent retrieval over an authorized Knowledge allowlist.

本 adapter 不重新解释 ``KnowledgeSelection``；它只消费 worker 在执行时刷新的
``AgentExecutionGrant``，并在返回前对不可信 search adapter 的 provenance 做二次校验。
"""

from __future__ import annotations

from types import MappingProxyType

from backend.application.ports.agent_v2 import (
    AgentKnowledgeRetrievalRequest,
)
from backend.application.ports.knowledge import HybridKnowledgeSearch
from backend.domain.agent_v2 import AgentKnowledgeEvidence
from backend.domain.knowledge_retrieval import (
    KnowledgeCitation,
    KnowledgeSearchPlan,
    KnowledgeSearchScope,
    SearchFilters,
)


class GrantedAgentKnowledgeRetriever:
    """@brief 复用混合检索并强制 grant provenance / Reuse hybrid search while enforcing grant provenance."""

    def __init__(self, search: HybridKnowledgeSearch) -> None:
        """@brief 绑定真实 hybrid-search adapter / Bind the real hybrid-search adapter.

        @param search PostgreSQL FTS + vector 或等价生产 adapter / PostgreSQL FTS
            plus vector search, or an equivalent production adapter.
        """
        self._search = search

    async def retrieve(
        self,
        request: AgentKnowledgeRetrievalRequest,
    ) -> tuple[AgentKnowledgeEvidence, ...]:
        """@brief 仅在 grant 精确 source/version/policy 边界内检索 / Retrieve only inside exact grant source/version/policy boundaries.

        @param request 执行时已重新授权的请求 / Execution-time reauthorized request.
        @return 排序稳定、带 label 的 server evidence / Stably ordered labelled server evidence.
        @raise RuntimeError adapter 越出 Workspace、allowlist、policy watermark 或返回重复
            chunk 时抛出 / Raised when the adapter exceeds the Workspace, allowlist, policy
            watermark, or returns duplicate chunks.
        """
        scopes = tuple(
            KnowledgeSearchScope(
                context.source_id,
                context.version_id,
                context.policy_version,
            )
            for context in request.grant.knowledge_contexts
        )
        if not scopes:
            return ()
        plan = KnowledgeSearchPlan(
            request.workspace_id,
            request.actor_id,
            request.query,
            scopes,
            request.grant.agent_scope,
            request.top_k,
            SearchFilters(MappingProxyType({})),
        )
        response = await self._search.search(plan)
        expected_watermark = max(scope.policy_version for scope in scopes)
        if response.policy_version != expected_watermark:
            raise RuntimeError("Agent Knowledge search returned a stale policy watermark")
        allowed = {(scope.source_id, scope.version_id) for scope in scopes}
        seen_chunks: set[str] = set()
        for hit in response.hits:
            if (
                hit.workspace_id != request.workspace_id
                or (hit.source_id, hit.version_id) not in allowed
            ):
                raise RuntimeError("Agent Knowledge search escaped its authorized provenance")
            if hit.chunk_id in seen_chunks:
                raise RuntimeError("Agent Knowledge search returned a duplicate chunk")
            seen_chunks.add(hit.chunk_id)
        ordered = sorted(
            response.hits,
            key=lambda hit: (
                -hit.score.fused,
                str(hit.source_id),
                str(hit.version_id),
                hit.locator,
                hit.chunk_id,
            ),
        )[: request.top_k]
        return tuple(
            AgentKnowledgeEvidence(index, hit.chunk_id, KnowledgeCitation.from_hit(hit))
            for index, hit in enumerate(ordered)
        )


__all__ = ["GrantedAgentKnowledgeRetriever"]
