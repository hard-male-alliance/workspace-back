"""@brief 知识来源、可见性与 embedding 空间领域模型 / Knowledge-source, visibility, and embedding-space domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from backend.domain.common import iso_timestamp
from workspace_shared.tenancy import ActorScope


@dataclass(frozen=True, slots=True)
class EmbeddingSpace:
    """@brief 不可变 embedding 空间 / Immutable embedding space.

    @note 不同 space 的向量绝不可直接比较。
    """

    id: str
    provider: str
    model: str
    model_revision: str
    dimension: int
    distance_metric: str
    normalization: str
    created_at: datetime


@dataclass(slots=True)
class KnowledgeSourceRecord:
    """@brief 带版本与 chunk 的知识来源聚合 / Knowledge source aggregate with versions and chunks."""

    scope: ActorScope
    id: str
    created_at: datetime
    updated_at: datetime
    name: str
    source_type: str
    config: dict[str, Any]
    visibility: dict[str, Any]
    revision: int = 1
    enabled: bool = True
    ingestion_status: str = "not_started"
    source_version_id: str | None = None
    chunks: list[KnowledgeChunk] = field(default_factory=list)
    mock_content: str = ""

    def as_dict(self) -> dict[str, Any]:
        """@brief 转换为公开 KnowledgeSource / Convert to public KnowledgeSource.

        @return 契约 KnowledgeSource 表示 / Contract KnowledgeSource representation.
        """
        return {
            "id": self.id,
            "created_at": iso_timestamp(self.created_at),
            "updated_at": iso_timestamp(self.updated_at),
            "revision": self.revision,
            "workspace_id": self.scope.workspace_id,
            "name": self.name,
            "source_type": self.source_type,
            "config": self.config,
            "visibility": self.visibility,
            "ingestion": {
                "status": self.ingestion_status,
                "active_job_id": None,
                "indexed_version_id": self.source_version_id if self.ingestion_status == "ready" else None,
                "document_count": 1 if self.source_version_id else 0,
                "chunk_count": len(self.chunks),
                "last_success_at": iso_timestamp(self.updated_at) if self.ingestion_status == "ready" else None,
                "last_error": None,
            },
            "sync_schedule": None,
            "enabled": self.enabled,
            "extensions": {},
        }


@dataclass(frozen=True, slots=True)
class KnowledgeChunk:
    """@brief 可检索的版本化知识分块 / Retrievable versioned knowledge chunk."""

    id: str
    source_id: str
    source_version_id: str
    embedding_space_id: str
    ordinal: int
    text: str
    vector: tuple[float, ...]
