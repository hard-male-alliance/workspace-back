"""Deterministic embedding adapter used until a production model is selected."""

from __future__ import annotations

import hashlib


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


__all__ = ["DeterministicEmbeddingProvider"]
