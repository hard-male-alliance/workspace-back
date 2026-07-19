"""@brief 知识来源、可见性与 embedding 空间领域模型 / Knowledge-source, visibility, and embedding-space domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from backend.domain.common import iso_timestamp
from workspace_shared.tenancy import ActorScope


class KnowledgeSourceRole(StrEnum):
    """Role a source plays in grounding an AI decision."""

    PERSONAL_EVIDENCE = "personal_evidence"
    RESUME_CURRENT = "resume_current"
    RESUME_HISTORY = "resume_history"
    JOB_TARGET = "job_target"
    EXTERNAL_REFERENCE = "external_reference"
    AI_GENERATED_DRAFT = "ai_generated_draft"


class KnowledgeContentType(StrEnum):
    """Semantic content category used for filtering and prompt assembly."""

    PROFILE = "profile"
    EDUCATION = "education"
    WORK_EXPERIENCE = "work_experience"
    PROJECT = "project"
    SKILL = "skill"
    ACHIEVEMENT = "achievement"
    CERTIFICATE = "certificate"
    PUBLICATION = "publication"
    OPEN_SOURCE = "open_source"
    JOB_REQUIREMENT = "job_requirement"
    GENERAL = "general"


class KnowledgeTrustLevel(StrEnum):
    """How strongly a knowledge item may be treated as a factual claim."""

    VERIFIED = "verified"
    USER_PROVIDED = "user_provided"
    INFERRED = "inferred"
    GENERATED = "generated"
    USER_CONFIRMED = "user_confirmed"


class KnowledgeLifecycle(StrEnum):
    """Lifecycle state used to keep stale resume revisions out of retrieval."""

    PENDING = "pending"
    CURRENT = "current"
    STALE = "stale"
    ARCHIVED = "archived"
    DELETED = "deleted"


class KnowledgeAgentVisibility(StrEnum):
    """Named consumers allowed to use an item as context."""

    RESUME_ASSISTANT = "resume_assistant"
    INTERVIEW_AGENT = "interview_agent"
    GENERAL_AGENT = "general_agent"
    PRIVATE_ONLY = "private_only"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class KnowledgeClassification:
    """Orthogonal, machine-readable knowledge classification."""

    source_role: KnowledgeSourceRole = KnowledgeSourceRole.PERSONAL_EVIDENCE
    content_type: KnowledgeContentType = KnowledgeContentType.GENERAL
    trust_level: KnowledgeTrustLevel = KnowledgeTrustLevel.USER_PROVIDED
    lifecycle: KnowledgeLifecycle = KnowledgeLifecycle.CURRENT
    visibility: tuple[KnowledgeAgentVisibility, ...] = (
        KnowledgeAgentVisibility.RESUME_ASSISTANT,
        KnowledgeAgentVisibility.INTERVIEW_AGENT,
        KnowledgeAgentVisibility.GENERAL_AGENT,
    )

    def as_dict(self) -> dict[str, Any]:
        """Return the stable persistence/API representation."""
        return {
            "source_role": self.source_role.value,
            "content_type": self.content_type.value,
            "trust_level": self.trust_level.value,
            "lifecycle": self.lifecycle.value,
            "visibility": [value.value for value in self.visibility],
        }

    @classmethod
    def from_dict(cls, value: object) -> KnowledgeClassification:
        """Rehydrate classification while accepting records written before phase one."""
        if not isinstance(value, dict):
            return cls()
        raw_visibility = value.get("visibility")
        visibility = (
            tuple(KnowledgeAgentVisibility(str(item)) for item in raw_visibility)
            if isinstance(raw_visibility, list)
            else cls().visibility
        )
        return cls(
            source_role=KnowledgeSourceRole(str(value.get("source_role", cls().source_role.value))),
            content_type=KnowledgeContentType(str(value.get("content_type", cls().content_type.value))),
            trust_level=KnowledgeTrustLevel(str(value.get("trust_level", cls().trust_level.value))),
            lifecycle=KnowledgeLifecycle(str(value.get("lifecycle", cls().lifecycle.value))),
            visibility=visibility,
        )


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentPart:
    """A semantic input unit preserving its Resume section/item provenance."""

    text: str
    content_type: KnowledgeContentType
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StoredKnowledgeBlob:
    """Opaque metadata for one file stored behind the blob-storage port."""

    file_id: str
    storage_key: str
    filename: str
    content_type: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class ParsedKnowledgeDocument:
    """Structured parser output with stable source locators."""

    parts: tuple[KnowledgeDocumentPart, ...]
    metadata: dict[str, Any] = field(default_factory=dict)


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
    classification: KnowledgeClassification = field(default_factory=KnowledgeClassification)
    source_metadata: dict[str, Any] = field(default_factory=dict)
    private_metadata: dict[str, Any] = field(default_factory=dict)
    document_parts: list[KnowledgeDocumentPart] = field(default_factory=list)

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
            "extensions": {
                "aiws": {
                    "classification": self.classification.as_dict(),
                    "source_metadata": self.source_metadata,
                }
            },
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
    classification: KnowledgeClassification = field(default_factory=KnowledgeClassification)
    metadata: dict[str, Any] = field(default_factory=dict)
