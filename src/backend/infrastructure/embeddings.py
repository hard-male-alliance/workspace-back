"""Deterministic and OpenAI-compatible embedding adapters."""

from __future__ import annotations

import hashlib
import math
from urllib.parse import urlsplit, urlunsplit

import httpx

from backend.domain.common import DomainError, Problem


class DeterministicEmbeddingProvider:
    """Stable 1024-dimensional development/test embedding implementation."""

    def __init__(self, dimension: int) -> None:
        self._dimension = dimension

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        """Embed each text without network access or secret material."""
        return [self._vector(text) for text in texts]

    def _vector(self, text: str) -> tuple[float, ...]:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        values = [
            ((digest[index % len(digest)] / 255.0) * 2 - 1)
            for index in range(self._dimension)
        ]
        norm = sum(value * value for value in values) ** 0.5
        return (
            tuple(value / norm for value in values)
            if norm
            else tuple(0.0 for _ in values)
        )


class OpenAICompatibleEmbeddingProvider:
    """Bounded OpenAI-compatible embeddings client suitable for OpenRouter."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        dimension: int,
        connect_timeout_ms: int,
        read_timeout_ms: int,
        outbound_proxy_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not api_key.strip() or not model.strip() or dimension <= 0:
            raise ValueError("embedding provider settings are incomplete")
        self._endpoint = _embeddings_endpoint(base_url)
        self._api_key = api_key
        self._model = model
        self._dimension = dimension
        self._timeout = httpx.Timeout(
            read_timeout_ms / 1000,
            connect=connect_timeout_ms / 1000,
            write=connect_timeout_ms / 1000,
            pool=connect_timeout_ms / 1000,
        )
        self._proxy = outbound_proxy_url
        self._transport = transport

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        """Send one bounded batch and return validated, L2-normalized vectors."""
        if not texts:
            return []
        if any(not isinstance(text, str) or not text for text in texts):
            raise ValueError("embedding inputs must be non-empty strings")
        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                proxy=self._proxy,
                transport=self._transport,
                trust_env=False,
                follow_redirects=False,
            ) as client:
                response = await client.post(
                    self._endpoint,
                    headers={
                        "Authorization": f"Bearer {self._api_key}",
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    json={
                        "model": self._model,
                        "input": texts,
                        "dimensions": self._dimension,
                        "encoding_format": "float",
                    },
                )
        except httpx.HTTPError as error:
            raise DomainError(
                Problem(
                    "knowledge.embedding_provider_unavailable",
                    502,
                    "Embedding provider is unavailable",
                    retryable=True,
                )
            ) from error
        if response.status_code < 200 or response.status_code >= 300:
            raise DomainError(
                Problem(
                    "knowledge.embedding_provider_rejected",
                    502,
                    "Embedding provider rejected the request",
                    retryable=response.status_code == 429 or response.status_code >= 500,
                    extensions={"provider_status": response.status_code},
                )
            )
        try:
            payload = response.json()
            data = payload["data"]
            ordered = sorted(data, key=lambda item: int(item["index"]))
            raw_vectors = [item["embedding"] for item in ordered]
        except (KeyError, TypeError, ValueError) as error:
            raise DomainError(
                Problem(
                    "knowledge.embedding_response_invalid",
                    502,
                    "Embedding provider returned an invalid response",
                    retryable=True,
                )
            ) from error
        if len(raw_vectors) != len(texts):
            raise DomainError(
                Problem(
                    "knowledge.embedding_response_invalid",
                    502,
                    "Embedding provider returned an incomplete batch",
                    retryable=True,
                )
            )
        return [_normalised_vector(vector, self._dimension) for vector in raw_vectors]


def _normalised_vector(value: object, dimension: int) -> tuple[float, ...]:
    if not isinstance(value, list) or len(value) != dimension:
        raise DomainError(
            Problem(
                "knowledge.embedding_response_invalid",
                502,
                "Embedding provider returned an incompatible vector dimension",
                retryable=True,
            )
        )
    try:
        vector = tuple(float(item) for item in value)
    except (TypeError, ValueError) as error:
        raise DomainError(
            Problem(
                "knowledge.embedding_response_invalid",
                502,
                "Embedding provider returned a non-numeric vector",
                retryable=True,
            )
        ) from error
    if not all(math.isfinite(item) for item in vector):
        raise DomainError(
            Problem(
                "knowledge.embedding_response_invalid",
                502,
                "Embedding provider returned a non-finite vector",
                retryable=True,
            )
        )
    norm = math.sqrt(sum(item * item for item in vector))
    if norm == 0:
        raise DomainError(
            Problem(
                "knowledge.embedding_response_invalid",
                502,
                "Embedding provider returned a zero vector",
                retryable=True,
            )
        )
    return tuple(item / norm for item in vector)


def _embeddings_endpoint(base_url: str) -> str:
    parsed = urlsplit(base_url)
    host = parsed.hostname
    if (
        parsed.scheme not in {"http", "https"}
        or not host
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("embedding base_url must be an absolute credential-free URL")
    if parsed.scheme == "http" and host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("embedding base_url must use HTTPS outside loopback")
    path = parsed.path.rstrip("/")
    if not path.endswith("/embeddings"):
        path = f"{path}/embeddings"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


__all__ = ["DeterministicEmbeddingProvider", "OpenAICompatibleEmbeddingProvider"]
