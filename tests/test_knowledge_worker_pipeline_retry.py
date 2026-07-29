"""@brief Knowledge embedding 批次内重试测试 / Knowledge embedding in-batch retry tests."""

from __future__ import annotations

import pytest

from backend.domain.common import DomainError, Problem
from backend.infrastructure.knowledge_search import EmbeddingSpaceSelection
from backend.infrastructure.knowledge_worker_pipeline import KnowledgeIndexPipeline


class _UnusedParser:
    """@brief 本测试不会调用的 parser 替身 / Parser double unused by these focused tests."""


class _TransientEmbeddingProvider:
    """@brief 首次瞬时失败、随后成功的 embedding 替身 / Embedding double failing transiently once before succeeding."""

    def __init__(self) -> None:
        """@brief 初始化调用计数 / Initialize the invocation count."""

        self.calls = 0

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        """@brief 首次抛出 retryable Problem，随后返回向量 / Raise a retryable Problem once, then return vectors.

        @param texts 当前批次文本 / Current batch texts.
        @return 每个输入对应一个 1024 维向量 / One 1024-dimensional vector per input.
        """

        self.calls += 1
        if self.calls == 1:
            raise DomainError(
                Problem(
                    "knowledge.embedding_provider_unavailable",
                    502,
                    "Embedding provider unavailable",
                    retryable=True,
                )
            )
        return [tuple([1.0] + [0.0] * 1023) for _ in texts]


@pytest.mark.anyio
async def test_retryable_embedding_failure_retries_only_current_batch() -> None:
    """@brief 瞬时错误在批次内恢复而不升级为整任务重试 / A transient error recovers inside the batch without escalating the whole job."""

    embedder = _TransientEmbeddingProvider()
    pipeline = KnowledgeIndexPipeline(
        _UnusedParser(),  # type: ignore[arg-type]
        embedder,
        EmbeddingSpaceSelection("test", "test-model", "v1", 1024),
        model_region="private_deployment",
        external_model_processing=False,
        maximum_extracted_characters=10_000,
        maximum_chunks=100,
        chunk_max_characters=800,
        chunk_overlap_characters=80,
        embedding_batch_size=64,
        embedding_batch_maximum_attempts=2,
    )

    vectors = await pipeline._embed_batch_with_retry(["first"], batch_ordinal=0)

    assert embedder.calls == 2
    assert len(vectors) == 1
    assert len(vectors[0]) == 1024
