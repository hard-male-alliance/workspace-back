"""OpenAI-compatible embedding adapter protocol tests."""

from __future__ import annotations

import json
import math

import httpx
import pytest

from backend.domain.common import DomainError
from backend.infrastructure.embeddings import OpenAICompatibleEmbeddingProvider


@pytest.mark.anyio
async def test_openrouter_embedding_payload_and_normalized_response() -> None:
    """The adapter requests the configured MRL dimension and validates vectors."""
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("authorization")
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "object": "list",
                "data": [
                    {"object": "embedding", "index": 1, "embedding": [0.0, 3.0]},
                    {"object": "embedding", "index": 0, "embedding": [4.0, 0.0]},
                ],
            },
        )

    provider = OpenAICompatibleEmbeddingProvider(
        base_url="https://openrouter.ai/api/v1",
        api_key="secret-test-key",
        model="qwen/qwen3-embedding-8b",
        dimension=2,
        connect_timeout_ms=1000,
        read_timeout_ms=1000,
        transport=httpx.MockTransport(handler),
    )
    vectors = await provider.embed(["first", "second"])

    assert captured["url"] == "https://openrouter.ai/api/v1/embeddings"
    assert captured["authorization"] == "Bearer secret-test-key"
    assert captured["body"] == {
        "model": "qwen/qwen3-embedding-8b",
        "input": ["first", "second"],
        "dimensions": 2,
        "encoding_format": "float",
    }
    assert vectors == [(1.0, 0.0), (0.0, 1.0)]
    assert all(math.isclose(sum(item * item for item in vector), 1.0) for vector in vectors)


@pytest.mark.anyio
async def test_embedding_adapter_rejects_incompatible_dimension() -> None:
    """Malformed provider output never enters the vector store."""

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [{"index": 0, "embedding": [1.0]}]})

    provider = OpenAICompatibleEmbeddingProvider(
        base_url="https://openrouter.ai/api/v1",
        api_key="secret-test-key",
        model="qwen/qwen3-embedding-8b",
        dimension=2,
        connect_timeout_ms=1000,
        read_timeout_ms=1000,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(DomainError) as raised:
        await provider.embed(["first"])
    assert raised.value.problem.code == "knowledge.embedding_response_invalid"

