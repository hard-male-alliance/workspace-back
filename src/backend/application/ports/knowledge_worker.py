"""@brief Knowledge durable worker 的类型化 ports / Typed ports for the durable Knowledge worker."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Protocol

from backend.domain.connections import ConnectionProvider, CredentialReference
from backend.domain.knowledge import ParsedKnowledgeDocument
from backend.domain.knowledge_jobs import KnowledgeJobKind, KnowledgeJobSpec
from backend.domain.knowledge_sources import (
    KnowledgeSourceInput,
    KnowledgeSourceType,
    KnowledgeSourceVersionId,
)
from backend.domain.platform import ApiEventId, JobId
from backend.domain.principals import UserId, WorkspaceId


@dataclass(frozen=True, slots=True)
class KnowledgeWorkerClaim:
    """@brief 由第一段短事务冻结的 worker 输入 / Worker input frozen by the first short transaction.

    @param event_id 触发 outbox 事件 / Triggering outbox event.
    @param job_id 统一 Job / Unified Job.
    @param workspace_id 真实 Workspace RLS scope / Real Workspace RLS scope.
    @param actor_id 事件提交时真实 actor / Real actor captured when the event committed.
    @param kind typed Job kind / Typed Job kind.
    @param spec 持久化 worker spec / Persisted worker spec.
    @param job_revision claim 后 Job revision / Job revision after claiming.
    @param source_revision claim 后 source revision / Source revision after claiming.
    @param source_type 可选来源类型 / Optional source type.
    @param source_input 私有来源输入 / Private source input.
    @param connection_revision revoke claim 后 Connection fencing revision / Connection fencing
        revision after a revocation claim.
    @param connection_provider revoke provider / Provider for revocation.
    @param credential_reference 仅 worker 可解引用的 vault reference / Vault reference resolvable only by the worker.
    """

    event_id: ApiEventId
    job_id: JobId
    workspace_id: WorkspaceId
    actor_id: UserId
    kind: KnowledgeJobKind
    spec: KnowledgeJobSpec
    job_revision: int
    source_revision: int | None
    source_type: KnowledgeSourceType | None
    source_input: KnowledgeSourceInput | None = field(default=None, repr=False)
    source_owner_id: UserId | None = None
    source_metadata: dict[str, str] = field(default_factory=dict)
    allow_external_model_processing: bool = False
    allowed_model_regions: tuple[str, ...] = ()
    connection_revision: int | None = None
    connection_provider: ConnectionProvider | None = None
    credential_reference: CredentialReference | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 kind-specific claim 完整性 / Validate kind-specific claim completeness."""

        if self.job_revision < 1:
            raise ValueError("knowledge worker claim requires a positive Job revision")
        if self.kind is KnowledgeJobKind.CONNECTION_REVOKE:
            if (
                self.connection_provider is None
                or self.credential_reference is None
                or self.connection_revision is None
                or self.connection_revision < 1
                or self.source_revision is not None
                or self.source_type is not None
                or self.source_input is not None
                or self.source_owner_id is not None
                or self.source_metadata
                or self.allow_external_model_processing
                or self.allowed_model_regions
            ):
                raise ValueError("connection-revoke claim has inconsistent fields")
            return
        if (
            self.source_revision is None
            or self.source_type is None
            or self.source_owner_id is None
            or (self.kind is not KnowledgeJobKind.KNOWLEDGE_DELETE and self.source_input is None)
            or self.connection_provider is not None
            or self.credential_reference is not None
            or self.connection_revision is not None
        ):
            raise ValueError("knowledge-source claim has inconsistent fields")
        if len(self.source_metadata) > 40 or not self.allowed_model_regions:
            raise ValueError("knowledge-source claim policy or metadata is incomplete")


@dataclass(frozen=True, slots=True)
class KnowledgeMaterial:
    """@brief 外部 I/O 后取得的不可变原始 material / Immutable raw material obtained after external I/O."""

    content: bytes = field(repr=False)
    filename: str
    media_type: str
    source_metadata: dict[str, str]

    def __post_init__(self) -> None:
        """@brief 校验 material 非空且有界元数据 / Validate non-empty material and bounded metadata."""

        if not self.content:
            raise ValueError("knowledge material cannot be empty")
        if not self.filename or len(self.filename) > 300 or not self.media_type:
            raise ValueError("knowledge material filename or media type is invalid")
        if len(self.source_metadata) > 40:
            raise ValueError("knowledge material metadata exceeds its bound")


@dataclass(frozen=True, slots=True)
class PreparedKnowledgeChunk:
    """@brief transaction 外构建的 chunk+embedding / Chunk and embedding built outside a transaction."""

    ordinal: int
    text: str
    locator: str
    content_type: str
    embedding: tuple[float, ...]

    def __post_init__(self) -> None:
        """@brief 校验 chunk、locator 与有限向量 / Validate chunk, locator, and finite vector."""

        if self.ordinal < 0 or not self.text or len(self.text) > 50_000:
            raise ValueError("prepared Knowledge chunk is invalid")
        if not self.locator or len(self.locator) > 1_000 or not self.content_type:
            raise ValueError("prepared Knowledge chunk provenance is invalid")
        if not self.embedding or any(not math.isfinite(value) for value in self.embedding):
            raise ValueError("prepared Knowledge embedding is empty or non-finite")


@dataclass(frozen=True, slots=True)
class PreparedEmbeddingSpace:
    """@brief 不可混用的 embedding 空间身份 / Identity of a non-interchangeable embedding space."""

    id: str
    provider: str
    model: str
    model_revision: str
    dimension: int
    distance_metric: str
    normalization: str

    def __post_init__(self) -> None:
        """@brief 校验当前 pgvector schema 的约束 / Validate current pgvector-schema constraints."""

        values = (self.id, self.provider, self.model, self.model_revision)
        if any(not value or value.strip() != value for value in values):
            raise ValueError("prepared embedding-space identity is invalid")
        if self.dimension != 1024:
            raise ValueError("prepared embedding space must contain 1024-dimensional vectors")
        if self.distance_metric != "cosine" or self.normalization != "l2":
            raise ValueError("prepared embedding space requires L2-normalized cosine vectors")


@dataclass(frozen=True, slots=True)
class PreparedKnowledgeIndex:
    """@brief parse/chunk/embed 的完整不可变输出 / Complete immutable parse/chunk/embed output."""

    content_sha256: str
    size_bytes: int
    parsed: ParsedKnowledgeDocument
    chunks: tuple[PreparedKnowledgeChunk, ...]
    embedding_space: PreparedEmbeddingSpace

    def __post_init__(self) -> None:
        """@brief 校验 hash、序号与非空 index / Validate hash, ordinals, and a non-empty index."""

        if len(self.content_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.content_sha256
        ):
            raise ValueError("prepared Knowledge content SHA-256 is invalid")
        if self.size_bytes < 1:
            raise ValueError("prepared Knowledge index size or embedding space is invalid")
        if not self.chunks or tuple(chunk.ordinal for chunk in self.chunks) != tuple(
            range(len(self.chunks))
        ):
            raise ValueError("prepared Knowledge chunks require contiguous zero-based ordinals")
        if any(len(chunk.embedding) != self.embedding_space.dimension for chunk in self.chunks):
            raise ValueError("prepared Knowledge chunks do not match their embedding space")
        if any(
            not math.isclose(
                math.fsum(value * value for value in chunk.embedding),
                1.0,
                rel_tol=1e-6,
                abs_tol=1e-6,
            )
            for chunk in self.chunks
        ):
            raise ValueError("prepared Knowledge chunks require L2-normalized embeddings")


class KnowledgeWorkerStore(Protocol):
    """@brief 所有 worker 数据库短事务的 Port / Port for every short worker database transaction."""

    async def claim(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        event_id: ApiEventId,
        job_id: JobId,
    ) -> KnowledgeWorkerClaim | None:
        """@brief claim queued/running typed Job；terminal 返回空 / Claim a queued/running typed Job; return none for terminal."""

    async def complete_connection_revocation(self, claim: KnowledgeWorkerClaim) -> None:
        """@brief 原子对齐 Connection、Job、outbox 与 audit / Atomically align Connection, Job, outbox, and audit."""

    async def complete_source_deletion(self, claim: KnowledgeWorkerClaim) -> None:
        """@brief 原子删除 index 并对齐来源/Job/outbox/audit / Atomically delete the index and align source, Job, outbox, and audit."""

    async def complete_processing(
        self,
        claim: KnowledgeWorkerClaim,
        prepared: PreparedKnowledgeIndex,
    ) -> KnowledgeSourceVersionId:
        """@brief 原子 upsert version index 并完成 Job / Atomically upsert a version index and finish the Job."""

    async def fail(self, claim: KnowledgeWorkerClaim, *, error_code: str) -> None:
        """@brief 最终尝试时原子 fail source/Job/outbox/audit / Atomically fail source, Job, outbox, and audit on the final attempt."""

    async def fail_exhausted(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        event_id: ApiEventId,
        event_type: str,
        job_id: JobId,
    ) -> None:
        """@brief 不依赖 payload 闭合耗尽工作 / Close exhausted work without relying on payload.

        @param workspace_id outbox 独立列中的 Workspace / Workspace from the outbox's dedicated column.
        @param actor_id outbox 独立列中的 Job creator / Job creator from the outbox's dedicated column.
        @param event_id 失败事件的稳定关联 ID / Stable correlation ID of the failed event.
        @param event_type 已注册的 Knowledge 工作类型 / Registered Knowledge work-event type.
        @param job_id outbox subject 中的 Job ID / Job ID from the outbox subject.
        @note 实现必须从 Job 的持久 kind/target 定位领域聚合，不能读取 event payload。
            / Implementations locate the domain aggregate from the Job's persisted kind and target
            and never read the event payload.
        """


class KnowledgeCredentialRevoker(Protocol):
    """@brief provider credential 撤销 Port / Provider-credential revocation port."""

    async def revoke(
        self,
        claim: KnowledgeWorkerClaim,
        *,
        operation_id: str,
    ) -> None:
        """@brief 以稳定 operation ID 幂等撤销 / Idempotently revoke by a stable operation ID."""


class KnowledgeSourceEraser(Protocol):
    """@brief 删除数据库外 source material 的 Port / Port deleting source material outside the database."""

    async def erase(self, claim: KnowledgeWorkerClaim, *, operation_id: str) -> None:
        """@brief 幂等擦除外部对象 / Idempotently erase external objects."""


class KnowledgeMaterialLoader(Protocol):
    """@brief 按来源判别联合读取 material 的 Port / Port loading material by source discriminated union."""

    async def load(self, claim: KnowledgeWorkerClaim) -> KnowledgeMaterial:
        """@brief 在数据库事务外抓取 snapshot / Fetch a snapshot outside database transactions."""


class KnowledgeIndexBuilder(Protocol):
    """@brief parse/chunk/embed pipeline Port / Parse/chunk/embed pipeline port."""

    async def build(
        self,
        claim: KnowledgeWorkerClaim,
        material: KnowledgeMaterial,
    ) -> PreparedKnowledgeIndex:
        """@brief 在数据库事务外构建 index / Build an index outside database transactions."""


class KnowledgeWorkerTerminalFailure(RuntimeError):
    """@brief 不应重试的确定性 worker failure / Deterministic worker failure that should not retry."""

    code: str

    def __init__(self, code: str) -> None:
        """@brief 保存稳定且无敏感正文的 code / Store a stable code without sensitive detail."""

        if not code or len(code) > 100:
            raise ValueError("knowledge terminal failure code is invalid")
        super().__init__(code)
        self.code = code


__all__ = [
    "KnowledgeCredentialRevoker",
    "KnowledgeIndexBuilder",
    "KnowledgeMaterial",
    "KnowledgeMaterialLoader",
    "KnowledgeSourceEraser",
    "KnowledgeWorkerClaim",
    "KnowledgeWorkerStore",
    "KnowledgeWorkerTerminalFailure",
    "PreparedEmbeddingSpace",
    "PreparedKnowledgeChunk",
    "PreparedKnowledgeIndex",
]
