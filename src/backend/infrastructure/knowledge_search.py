"""@brief Knowledge 跨 context 校验与 PostgreSQL hybrid search / Knowledge cross-context verification and PostgreSQL hybrid search.

检索 adapter 在 SQL 内部同时施加 Workspace、source/version pair、当前 policy watermark
与严格 filter allowlist；应用层仍会二次复验 provenance。Lexical 使用 PostgreSQL FTS，
dense 使用 pgvector cosine distance，最终在有界候选集上做可解释加权融合。
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Protocol, cast

import sqlalchemy as sa
from sqlalchemy.engine import RowMapping

from backend.application.ports.knowledge import HybridSearchResponse
from backend.domain.knowledge_retrieval import (
    HybridScore,
    KnowledgeSearchHit,
    KnowledgeSearchPlan,
    SearchFilterValue,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    ResumeId,
)
from backend.domain.principals import UserId, WorkspaceId
from backend.infrastructure.persistence.database import AsyncDatabase


class KnowledgeQueryEmbedder(Protocol):
    """@brief query/chunk 共用 embedding Port / Embedding port shared by queries and chunks."""

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        """@brief 返回与输入一一对应的向量 / Return one vector per input."""


@dataclass(frozen=True, slots=True)
class EmbeddingSpaceSelection:
    """@brief 不可混用的 production embedding space 身份 / Identity of a non-interchangeable production embedding space."""

    provider: str
    model: str
    model_revision: str
    dimension: int
    distance_metric: str = "cosine"
    normalization: str = "l2"

    def __post_init__(self) -> None:
        """@brief 校验与当前 pgvector schema 一致 / Validate against the current pgvector schema."""

        components = (
            (self.provider, 128),
            (self.model, 256),
            (self.model_revision, 256),
        )
        if any(
            not value or value.strip() != value or len(value) > maximum or "\x00" in value
            for value, maximum in components
        ):
            raise ValueError("embedding-space identity contains empty fields")
        if self.dimension != 1024:
            raise ValueError("current Knowledge index requires 1024-dimensional embeddings")
        if self.distance_metric != "cosine" or self.normalization != "l2":
            raise ValueError("hybrid search currently requires L2-normalized cosine embeddings")


class PostgresKnowledgeDependencyVerifier:
    """@brief 使用真实 actor+Workspace RLS 校验 Resume 引用 / Verify Resume references using real actor-and-Workspace RLS."""

    def __init__(self, database: AsyncDatabase) -> None:
        """@brief 绑定共享数据库 / Bind the shared database."""

        self._database = database

    async def resume_exists(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        actor_id: UserId,
    ) -> bool:
        """@brief 在路径 Workspace 内检查未删除 Resume / Check a non-deleted Resume inside the path Workspace."""

        async with self._database.new_session() as session:
            async with session.begin():
                await self._database.install_v2_request_scope(
                    session,
                    actor_id=str(actor_id),
                    workspace_id=str(workspace_id),
                )
                exists = await session.scalar(
                    sa.text(
                        "SELECT EXISTS ("
                        "SELECT 1 FROM resume.documents "
                        "WHERE workspace_id = :workspace_id AND id = :resume_id "
                        "AND deleted_at IS NULL)"
                    ),
                    {"workspace_id": str(workspace_id), "resume_id": str(resume_id)},
                )
        if not isinstance(exists, bool):
            raise RuntimeError("Resume dependency query returned an invalid result")
        return exists


class MemoryKnowledgeDependencyVerifier:
    """@brief development/test 的 Workspace-first Resume verifier / Workspace-first Resume verifier for development/test."""

    def __init__(self, resumes: Sequence[tuple[WorkspaceId, ResumeId]]) -> None:
        """@brief 冻结允许的二元组 / Freeze the allowed tuples."""

        self._resumes = frozenset(resumes)

    async def resume_exists(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        actor_id: UserId,
    ) -> bool:
        """@brief 检查精确 Workspace+Resume pair / Check an exact Workspace-and-Resume pair."""

        del actor_id
        return (workspace_id, resume_id) in self._resumes


@dataclass(frozen=True, slots=True)
class _Candidate:
    """@brief SQL 返回的内部候选 / Internal candidate returned by SQL."""

    chunk_id: str
    source_id: KnowledgeSourceId
    version_id: KnowledgeSourceVersionId
    locator: str
    quote: str
    lexical: float | None
    semantic: float | None


class PostgresHybridKnowledgeSearch:
    """@brief PostgreSQL FTS + pgvector 的 Workspace/policy-safe adapter / Workspace-and-policy-safe PostgreSQL FTS plus pgvector adapter."""

    def __init__(
        self,
        database: AsyncDatabase,
        embedder: KnowledgeQueryEmbedder,
        embedding_space: EmbeddingSpaceSelection,
        *,
        lexical_weight: float = 0.45,
        semantic_weight: float = 0.55,
        candidate_multiplier: int = 4,
    ) -> None:
        """@brief 注入查询 embedder 与融合参数 / Inject the query embedder and fusion parameters."""

        if (
            not math.isfinite(lexical_weight)
            or not math.isfinite(semantic_weight)
            or lexical_weight <= 0
            or semantic_weight <= 0
        ):
            raise ValueError("hybrid-search weights must be finite and positive")
        if not 2 <= candidate_multiplier <= 20:
            raise ValueError("hybrid-search candidate multiplier must be 2 to 20")
        total = lexical_weight + semantic_weight
        self._database = database
        self._embedder = embedder
        self._embedding_space = embedding_space
        self._lexical_weight = lexical_weight / total
        self._semantic_weight = semantic_weight / total
        self._candidate_multiplier = candidate_multiplier

    async def search(self, plan: KnowledgeSearchPlan) -> HybridSearchResponse:
        """@brief 在 SQL allowlist 中分别召回后融合 / Recall within the SQL allowlist, then fuse."""

        if not plan.scopes:
            return HybridSearchResponse((), 1)
        vectors = await self._embedder.embed([plan.query])
        if len(vectors) != 1:
            raise RuntimeError("query embedder returned an incompatible vector")
        query_vector = l2_normalized_vector(
            vectors[0],
            expected_dimension=self._embedding_space.dimension,
        )
        filters, filter_parameters = _sql_filters(plan.filters.values)
        limit = min(200, plan.top_k * self._candidate_multiplier)
        parameters: dict[str, object] = {
            "workspace_id": str(plan.workspace_id),
            "source_ids": [str(scope.source_id) for scope in plan.scopes],
            "version_ids": [str(scope.version_id) for scope in plan.scopes],
            "policy_versions": [scope.policy_version for scope in plan.scopes],
            "query": plan.query,
            "query_vector": _vector_literal(query_vector),
            "provider": self._embedding_space.provider,
            "model": self._embedding_space.model,
            "model_revision": self._embedding_space.model_revision,
            "dimension": self._embedding_space.dimension,
            "distance_metric": self._embedding_space.distance_metric,
            "normalization": self._embedding_space.normalization,
            "candidate_limit": limit,
            **filter_parameters,
        }
        lexical_sql = _LEXICAL_SQL.format(filters=filters)
        dense_sql = _DENSE_SQL.format(filters=filters)
        async with self._database.new_session() as session:
            async with session.begin():
                await self._database.install_v2_request_scope(
                    session,
                    actor_id=str(plan.actor_id),
                    workspace_id=str(plan.workspace_id),
                )
                lexical_rows = (
                    (await session.execute(sa.text(lexical_sql), parameters)).mappings().all()
                )
                dense_rows = (
                    (await session.execute(sa.text(dense_sql), parameters)).mappings().all()
                )
        candidates: dict[str, _Candidate] = {}
        for row in lexical_rows:
            candidate = _candidate_from_row(
                row, lexical=_bounded_score(row["score"]), semantic=None
            )
            candidates[candidate.chunk_id] = candidate
        for row in dense_rows:
            semantic = _bounded_score(row["score"])
            candidate = _candidate_from_row(row, lexical=None, semantic=semantic)
            existing = candidates.get(candidate.chunk_id)
            candidates[candidate.chunk_id] = (
                candidate
                if existing is None
                else _Candidate(
                    existing.chunk_id,
                    existing.source_id,
                    existing.version_id,
                    existing.locator,
                    existing.quote,
                    existing.lexical,
                    semantic,
                )
            )
        hits = [self._hit(plan.workspace_id, candidate) for candidate in candidates.values()]
        hits.sort(
            key=lambda hit: (
                -hit.score.fused,
                str(hit.source_id),
                str(hit.version_id),
                hit.locator,
            )
        )
        return HybridSearchResponse(
            tuple(hits[: plan.top_k]),
            max(scope.policy_version for scope in plan.scopes),
        )

    def _hit(self, workspace_id: WorkspaceId, candidate: _Candidate) -> KnowledgeSearchHit:
        """@brief 将双路候选归一化为 domain hit / Normalize a dual-channel candidate into a domain hit."""

        active_weight = (self._lexical_weight if candidate.lexical is not None else 0.0) + (
            self._semantic_weight if candidate.semantic is not None else 0.0
        )
        fused = (
            self._lexical_weight * (candidate.lexical or 0.0)
            + self._semantic_weight * (candidate.semantic or 0.0)
        ) / active_weight
        return KnowledgeSearchHit(
            candidate.chunk_id,
            workspace_id,
            candidate.source_id,
            candidate.version_id,
            candidate.locator,
            candidate.quote,
            HybridScore(candidate.lexical, candidate.semantic, _bounded_score(fused)),
        )


@dataclass(frozen=True, slots=True)
class MemoryKnowledgeIndexEntry:
    """@brief development/test memory index entry / Development/test memory-index entry."""

    workspace_id: WorkspaceId
    source_id: KnowledgeSourceId
    version_id: KnowledgeSourceVersionId
    locator: str
    text: str
    metadata: Mapping[str, str]
    vector: tuple[float, ...] | None = None
    ordinal: int = 0

    def __post_init__(self) -> None:
        """@brief 校验 development index 的顺序字段 / Validate the development-index ordinal."""

        if self.ordinal < 0:
            raise ValueError("memory Knowledge entry ordinal cannot be negative")


class MemoryHybridKnowledgeSearch:
    """@brief 有真实边界和确定性融合的 memory adapter / Memory adapter with real boundaries and deterministic fusion."""

    def __init__(
        self,
        entries: Sequence[MemoryKnowledgeIndexEntry],
        embedder: KnowledgeQueryEmbedder | None = None,
    ) -> None:
        """@brief 冻结 entries 与可选 dense embedder / Freeze entries and an optional dense embedder."""

        self._entries = tuple(entries)
        self._embedder = embedder

    async def search(self, plan: KnowledgeSearchPlan) -> HybridSearchResponse:
        """@brief 对 exact scope subset 做 token-overlap + cosine / Run token overlap plus cosine over the exact scope subset."""

        allowed = {(scope.source_id, scope.version_id) for scope in plan.scopes}
        query_tokens = _tokens(plan.query)
        query_vector: tuple[float, ...] | None = None
        if self._embedder is not None:
            values = await self._embedder.embed([plan.query])
            if len(values) != 1:
                raise RuntimeError("memory query embedder returned no vector")
            query_vector = values[0]
        filters = _memory_filters(plan.filters.values)
        hits: list[KnowledgeSearchHit] = []
        for entry in self._entries:
            if (
                entry.workspace_id != plan.workspace_id
                or (entry.source_id, entry.version_id) not in allowed
            ):
                continue
            if not filters(entry):
                continue
            entry_tokens = _tokens(entry.text)
            lexical = len(query_tokens & entry_tokens) / max(len(query_tokens), 1)
            semantic = (
                _cosine_score(query_vector, entry.vector)
                if query_vector is not None and entry.vector is not None
                else None
            )
            if lexical <= 0 and semantic is None:
                continue
            fused = lexical if semantic is None else (0.45 * lexical + 0.55 * semantic)
            hits.append(
                KnowledgeSearchHit(
                    "knowledge_chunk_"
                    + sha256(
                        f"{entry.source_id}:{entry.version_id}:{entry.ordinal}".encode()
                    ).hexdigest()[:32],
                    plan.workspace_id,
                    entry.source_id,
                    entry.version_id,
                    entry.locator,
                    entry.text[:4_000],
                    HybridScore(lexical if lexical > 0 else None, semantic, fused),
                )
            )
        hits.sort(key=lambda item: (-item.score.fused, str(item.source_id), item.locator))
        watermark = max((scope.policy_version for scope in plan.scopes), default=1)
        return HybridSearchResponse(tuple(hits[: plan.top_k]), watermark)


_AUTHORIZED_CTE = """
WITH authorized AS (
    SELECT source_id, version_id, policy_version
    FROM unnest(
        CAST(:source_ids AS text[]),
        CAST(:version_ids AS text[]),
        CAST(:policy_versions AS integer[])
    ) AS allowed(source_id, version_id, policy_version)
), bounded_chunks AS (
    SELECT chunk.id,
           version.source_id,
           version.id AS version_id,
           chunk.ordinal,
           chunk.text_content,
           chunk.search_vector,
           chunk.origin
    FROM knowledge.chunks AS chunk
    JOIN knowledge.source_versions AS version
      ON version.id = chunk.source_version_id
     AND version.workspace_id = chunk.workspace_id
    JOIN knowledge.sources AS source
      ON source.id = version.source_id
     AND source.workspace_id = version.workspace_id
    JOIN authorized
      ON authorized.source_id = version.source_id
     AND authorized.version_id = version.id
     AND authorized.policy_version = source.current_policy_version
    WHERE chunk.workspace_id = :workspace_id
      AND source.workspace_id = :workspace_id
      AND version.workspace_id = :workspace_id
      AND source.enabled = true
      AND source.ingestion_state = 'ready'
      AND version.status = 'ready'
      {filters}
)
"""
"""@brief 两条召回 SQL 共用的授权 CTE / Authorization CTE shared by both recall queries."""

_LEXICAL_SQL = (
    _AUTHORIZED_CTE
    + """
SELECT id AS chunk_id,
       source_id,
       version_id,
       COALESCE(origin #>> '{metadata,path}', origin ->> 'path', 'chunk/' || ordinal::text) AS locator,
       left(text_content, 4000) AS quote,
       LEAST(1.0, GREATEST(0.0,
           ts_rank_cd(
               search_vector,
               plainto_tsquery('simple', :query),
               32
           )
       )) AS score
FROM bounded_chunks
WHERE search_vector @@ plainto_tsquery('simple', :query)
ORDER BY score DESC, id ASC
LIMIT :candidate_limit
"""
)
"""@brief SQL-side lexical recall / SQL-side lexical recall."""

_DENSE_SQL = (
    _AUTHORIZED_CTE
    + """
SELECT chunk.id AS chunk_id,
       chunk.source_id,
       chunk.version_id,
       COALESCE(chunk.origin #>> '{metadata,path}', chunk.origin ->> 'path',
                'chunk/' || chunk.ordinal::text) AS locator,
       left(chunk.text_content, 4000) AS quote,
       LEAST(1.0, GREATEST(0.0,
           1.0 - ((embedding.embedding <=> CAST(:query_vector AS vector)) / 2.0)
       )) AS score
FROM bounded_chunks AS chunk
JOIN knowledge.embeddings AS embedding
  ON embedding.chunk_id = chunk.id
 AND embedding.workspace_id = :workspace_id
JOIN knowledge.embedding_spaces AS space
  ON space.id = embedding.embedding_space_id
 AND space.workspace_id = :workspace_id
WHERE space.provider = :provider
  AND space.model = :model
  AND space.model_revision = :model_revision
  AND space.dimension = :dimension
  AND space.distance_metric = :distance_metric
  AND space.normalization = :normalization
  AND space.retired_at IS NULL
ORDER BY embedding.embedding <=> CAST(:query_vector AS vector), chunk.id ASC
LIMIT :candidate_limit
"""
)
"""@brief SQL-side dense recall / SQL-side dense recall."""


def _sql_filters(values: Mapping[str, SearchFilterValue]) -> tuple[str, dict[str, object]]:
    """@brief 把严格 allowlist filter 编译为参数化 SQL / Compile the strict filter allowlist to parameterized SQL."""

    allowed = {"content_type", "path_prefix", "minimum_ordinal", "maximum_ordinal"}
    unknown = set(values) - allowed
    if unknown:
        raise ValueError("knowledge search contains an unsupported filter")
    clauses: list[str] = []
    parameters: dict[str, object] = {}
    content_type = values.get("content_type")
    if content_type is not None:
        content_types = _filter_strings(content_type, "content_type")
        clauses.append(
            "AND COALESCE(chunk.origin #>> '{metadata,content_type}', chunk.origin ->> 'content_type') = ANY(CAST(:filter_content_types AS text[]))"
        )
        parameters["filter_content_types"] = list(content_types)
    path_prefix = values.get("path_prefix")
    if path_prefix is not None:
        if not isinstance(path_prefix, str) or not path_prefix or len(path_prefix) > 1_000:
            raise ValueError("knowledge path_prefix filter is invalid")
        clauses.append(
            "AND COALESCE(chunk.origin #>> '{metadata,path}', chunk.origin ->> 'path', '') LIKE :filter_path_prefix ESCAPE '\\\\'"
        )
        parameters["filter_path_prefix"] = _like_prefix(path_prefix)
    for name, operator in (("minimum_ordinal", ">="), ("maximum_ordinal", "<=")):
        value = values.get(name)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 10_000_000:
            raise ValueError(f"knowledge {name} filter is invalid")
        clauses.append(f"AND chunk.ordinal {operator} :filter_{name}")
        parameters[f"filter_{name}"] = value
    return "\n      ".join(clauses), parameters


def _memory_filters(
    values: Mapping[str, SearchFilterValue],
) -> Callable[[MemoryKnowledgeIndexEntry], bool]:
    """@brief 为 memory adapter 编译同一 filter allowlist / Compile the same filter allowlist for the memory adapter."""

    _sql_filters(values)
    content_type_value = values.get("content_type")
    content_types = (
        None
        if content_type_value is None
        else frozenset(_filter_strings(content_type_value, "content_type"))
    )
    path_prefix = values.get("path_prefix")
    minimum_ordinal = values.get("minimum_ordinal")
    maximum_ordinal = values.get("maximum_ordinal")

    def predicate(entry: MemoryKnowledgeIndexEntry) -> bool:
        """@brief 应用 entry metadata filters / Apply entry-metadata filters."""

        if content_types is not None and entry.metadata.get("content_type") not in content_types:
            return False
        if isinstance(path_prefix, str) and not entry.metadata.get("path", "").startswith(
            path_prefix
        ):
            return False
        if isinstance(minimum_ordinal, int) and entry.ordinal < minimum_ordinal:
            return False
        return not isinstance(maximum_ordinal, int) or entry.ordinal <= maximum_ordinal

    return predicate


def _filter_strings(value: SearchFilterValue, label: str) -> tuple[str, ...]:
    """@brief 规范化单字符串或 tuple 字符串 / Normalize one string or a tuple of strings."""

    items: tuple[str, ...]
    if isinstance(value, str):
        items = (value,)
    elif isinstance(value, tuple) and all(isinstance(item, str) for item in value):
        items = cast(tuple[str, ...], value)
    else:
        raise ValueError(f"knowledge {label} filter is invalid")
    if not items or len(items) > 20 or any(not item or len(item) > 200 for item in items):
        raise ValueError(f"knowledge {label} filter is invalid")
    return items


def _like_prefix(value: str) -> str:
    """@brief 转义 LIKE 元字符并附加 prefix wildcard / Escape LIKE metacharacters and append a prefix wildcard."""

    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def _candidate_from_row(
    row: Mapping[str, object] | RowMapping,
    *,
    lexical: float | None,
    semantic: float | None,
) -> _Candidate:
    """@brief 验证 SQL row 投影 / Validate a projected SQL row."""

    chunk_id = row.get("chunk_id")
    source_id = row.get("source_id")
    version_id = row.get("version_id")
    locator = row.get("locator")
    quote = row.get("quote")
    if not all(
        isinstance(value, str) for value in (chunk_id, source_id, version_id, locator, quote)
    ):
        raise RuntimeError("hybrid search returned an invalid row projection")
    return _Candidate(
        cast(str, chunk_id),
        KnowledgeSourceId(cast(str, source_id)),
        KnowledgeSourceVersionId(cast(str, version_id)),
        cast(str, locator),
        cast(str, quote),
        lexical,
        semantic,
    )


def _bounded_score(value: object) -> float:
    """@brief 把数据库 numeric/float 防御性夹到 0..1 / Defensively clamp a database numeric or float to 0..1."""

    if isinstance(value, bool):
        raise RuntimeError("hybrid search returned a boolean score")
    try:
        score = float(cast(float | int | str, value))
    except (TypeError, ValueError) as error:
        raise RuntimeError("hybrid search returned a non-numeric score") from error
    if not math.isfinite(score):
        raise RuntimeError("hybrid search returned a non-finite score")
    return min(1.0, max(0.0, score))


def _vector_literal(vector: Sequence[float]) -> str:
    """@brief 构造只含有限数字的 pgvector text 参数 / Build a pgvector text parameter containing only finite numbers."""

    if not vector or any(not math.isfinite(value) for value in vector):
        raise ValueError("query vector must be non-empty and finite")
    return "[" + ",".join(format(value, ".17g") for value in vector) + "]"


def l2_normalized_vector(
    vector: Sequence[float],
    *,
    expected_dimension: int,
) -> tuple[float, ...]:
    """@brief 校验并确定性 L2 归一化 embedding / Validate and deterministically L2-normalize an embedding.

    @param vector provider 返回的向量 / Vector returned by a provider.
    @param expected_dimension 不可变 embedding-space 维度 / Immutable embedding-space dimension.
    @return 单位 L2 范数的有限 tuple / Finite tuple with unit L2 norm.
    """

    if len(vector) != expected_dimension or any(not math.isfinite(value) for value in vector):
        raise RuntimeError("embedding provider returned an incompatible vector")
    squared_norm = math.fsum(value * value for value in vector)
    if not math.isfinite(squared_norm) or squared_norm <= 1e-24:
        raise RuntimeError("embedding provider returned a zero or unstable vector")
    norm = math.sqrt(squared_norm)
    return tuple(value / norm for value in vector)


def _tokens(value: str) -> set[str]:
    """@brief development adapter 的 Unicode-ish token set / Unicode-ish token set for the development adapter."""

    return {item.casefold() for item in re.findall(r"[\w-]+", value, flags=re.UNICODE)}


def _cosine_score(left: Sequence[float], right: Sequence[float]) -> float:
    """@brief 计算映射到 0..1 的 cosine similarity / Compute cosine similarity mapped to 0..1."""

    if (
        len(left) != len(right)
        or not left
        or any(not math.isfinite(value) for value in (*left, *right))
    ):
        raise ValueError("memory index vectors have incompatible dimensions")
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        raise ValueError("memory index vectors cannot be zero")
    cosine = sum(a * b for a, b in zip(left, right, strict=True)) / (left_norm * right_norm)
    return min(1.0, max(0.0, (cosine + 1.0) / 2.0))


__all__ = [
    "EmbeddingSpaceSelection",
    "KnowledgeQueryEmbedder",
    "MemoryHybridKnowledgeSearch",
    "MemoryKnowledgeDependencyVerifier",
    "MemoryKnowledgeIndexEntry",
    "PostgresHybridKnowledgeSearch",
    "PostgresKnowledgeDependencyVerifier",
    "l2_normalized_vector",
]
