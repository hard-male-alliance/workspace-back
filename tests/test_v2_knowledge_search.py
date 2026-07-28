"""@brief Knowledge 混合检索 SQL 组装回归 / Knowledge hybrid-search SQL assembly regressions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, cast

import pytest
from sqlalchemy.engine import RowMapping

from backend.domain.knowledge_retrieval import (
    KnowledgeSearchPlan,
    KnowledgeSearchScope,
    SearchFilters,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
)
from backend.domain.principals import UserId, WorkspaceId
from backend.infrastructure.knowledge_search import (
    _DENSE_SQL,
    _LEXICAL_SQL,
    _SUBSTRING_LEXICAL_SQL,
    EmbeddingSpaceSelection,
    PostgresHybridKnowledgeSearch,
    _substring_terms,
)


class _FailingEmbedder:
    """@brief 模拟实时路径中的 embedding 故障 / Simulate an embedding failure on the realtime path."""

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        """@brief 始终使语义召回失败 / Always fail semantic recall.

        @param texts 待编码文本 / Texts to encode.
        @return 永不返回 / Never returns.
        """

        del texts
        raise RuntimeError("embedding provider unavailable")


class _LexicalRecallSearch(PostgresHybridKnowledgeSearch):
    """@brief 以固定词法行隔离数据库的检索测试桩 / Search test double isolating the database with a fixed lexical row."""

    async def _recall_rows(
        self,
        plan: KnowledgeSearchPlan,
        statement: str,
        parameters: Mapping[str, object],
    ) -> Sequence[Mapping[str, object] | RowMapping]:
        """@brief 返回一条中文 substring 命中 / Return one Chinese-substring match.

        @param plan 已授权检索计划 / Authorized retrieval plan.
        @param statement 待执行 SQL / SQL to execute.
        @param parameters 参数化绑定 / Parameter bindings.
        @return 固定词法候选 / Fixed lexical candidate.
        """

        del plan
        assert "substring_terms" in parameters
        assert "lexical_candidates" in statement
        return (
            {
                "chunk_id": "knowledge_chunk_frontend01",
                "source_id": "knowledge_source_frontend01",
                "version_id": "knowledge_version_frontend01",
                "locator": "前端测试/1",
                "quote": "React 组件性能优化需要先定位重复渲染。",
                "score": 0.5,
            },
        )


def test_hybrid_search_sql_preserves_postgres_json_path_literals() -> None:
    """@brief Python 模板不得消费 PostgreSQL JSON path / Python formatting must preserve PostgreSQL JSON paths."""

    lexical = _LEXICAL_SQL.format(filters="")
    dense = _DENSE_SQL.format(filters="")

    assert "#>> '{metadata,path}'" in lexical
    assert "#>> '{metadata,path}'" in dense


def test_realtime_lexical_sql_preserves_json_path_and_unicode_terms() -> None:
    """@brief 实时词法 SQL 保留 JSON path 并绑定 Unicode 查询词 / Realtime lexical SQL preserves JSON paths and binds Unicode terms."""

    statement = _SUBSTRING_LEXICAL_SQL.format(filters="")

    assert "#>> '{metadata,path}'" in statement
    assert "CAST(:substring_terms AS text[])" in statement
    assert _substring_terms("前端工程师 React TypeScript React") == (
        "前端工程师",
        "react",
        "typescript",
    )


@pytest.mark.asyncio
async def test_realtime_search_keeps_lexical_hits_when_embedding_fails() -> None:
    """@brief embedding 故障不得丢弃已取得的实时词法证据 / An embedding failure must not discard retrieved realtime lexical evidence."""

    search = _LexicalRecallSearch(
        cast(Any, object()),
        _FailingEmbedder(),
        EmbeddingSpaceSelection("provider", "model", "revision", 1024),
        semantic_timeout_seconds=0.8,
        allow_lexical_fallback=True,
        substring_lexical_fallback=True,
    )
    plan = KnowledgeSearchPlan(
        WorkspaceId("workspace_frontend01"),
        UserId("user_frontend0001"),
        "前端工程师 React TypeScript",
        (
            KnowledgeSearchScope(
                KnowledgeSourceId("knowledge_source_frontend01"),
                KnowledgeSourceVersionId("knowledge_version_frontend01"),
                3,
            ),
        ),
        "interview_coach",
        5,
        SearchFilters(MappingProxyType({})),
    )

    response = await search.search(plan)

    assert response.policy_version == 3
    assert len(response.hits) == 1
    assert response.hits[0].quote == "React 组件性能优化需要先定位重复渲染。"
    assert response.hits[0].score.semantic is None
