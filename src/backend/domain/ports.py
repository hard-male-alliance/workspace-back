"""@brief 领域端口（Repository 与 Provider 抽象）/ Domain ports (Repository and provider abstractions)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol

from backend.domain.agent import AgentRunRecord, ConversationRecord, MessageRecord
from backend.domain.common import Job
from backend.domain.interview import InterviewSessionRecord
from backend.domain.knowledge import (
    EmbeddingSpace,
    KnowledgeSourceRecord,
    ParsedKnowledgeDocument,
    StoredKnowledgeBlob,
)
from backend.domain.proposal import ResumeProposalRecord
from backend.domain.resume import ResumeRecord
from workspace_shared.observability import TelemetryRecord
from workspace_shared.tenancy import ActorScope


class ResumeRepository(Protocol):
    """@brief 简历 Repository 端口 / Resume repository port."""

    async def create_resume(self, scope: ActorScope, record: ResumeRecord) -> None:
        """@brief 保存新简历 / Persist a new resume.

        @param scope workspace 范围 / Workspace scope.
        @param record 简历聚合 / Resume aggregate.
        """

    async def get_resume(self, scope: ActorScope, resume_id: str) -> ResumeRecord | None:
        """@brief 范围内查询简历 / Read a scoped resume.

        @param scope workspace 范围 / Workspace scope.
        @param resume_id 简历 ID / Resume ID.
        @return 聚合或 None / Aggregate or None.
        """

    async def list_resumes(self, scope: ActorScope) -> list[ResumeRecord]:
        """@brief 列出范围内简历 / List scoped resumes.

        @param scope workspace 范围 / Workspace scope.
        @return 简历聚合 / Resume aggregates.
        """

    async def save_resume(self, scope: ActorScope, record: ResumeRecord) -> None:
        """@brief 保存已有简历 / Persist an existing resume.

        @param scope workspace 范围 / Workspace scope.
        @param record 简历聚合 / Resume aggregate.
        """


class ResumeProposalRepository(Protocol):
    """Persistence port for reviewable Resume AI proposals."""

    async def create_proposal(self, scope: ActorScope, record: ResumeProposalRecord) -> None:
        """Persist a new proposal within the supplied tenant scope."""

    async def get_proposal(
        self, scope: ActorScope, proposal_id: str
    ) -> ResumeProposalRecord | None:
        """Read a proposal without crossing workspace or owner boundaries."""

    async def list_proposals(
        self, scope: ActorScope, resume_id: str
    ) -> list[ResumeProposalRecord]:
        """List proposals for one scoped Resume in newest-first order."""

    async def save_proposal(self, scope: ActorScope, record: ResumeProposalRecord) -> None:
        """Persist proposal decision state."""


class ResumeKnowledgeBridge(Protocol):
    """@brief 简历到知识来源的内部派生桥 / Internal resume-to-knowledge-source derivation bridge.

    @note 这是应用层内部端口，不是新的 HTTP 或公开 contract。实现必须在相同的
    ``workspace_id`` 与 ``resource_owner_id`` 范围内工作，且只能从已持久化的
    ResumeDocument SIR（Semantic Intermediate Representation，语义中间表示）派生内容。
    """

    async def synchronize_resume(
        self,
        scope: ActorScope,
        document: dict[str, Any],
        request_id: str | None,
    ) -> None:
        """@brief 将一个 Resume revision 同步为其派生 KnowledgeSource / Synchronize one Resume revision into its derived KnowledgeSource.

        @param scope workspace/owner 范围 / Workspace and owner scope.
        @param document 已持久化 ResumeDocument 快照 / Persisted ResumeDocument snapshot.
        @param request_id 可选请求追踪 ID / Optional request trace ID.

        @note 同步可异步提交索引 Job，但不得创建新的公开 API 契约；过载必须保留为
        可观察的来源/Job 状态，而不是丢失派生意图。
        """


class AgentRepository(Protocol):
    """@brief Agent Repository 端口 / Agent repository port."""

    async def create_conversation(self, scope: ActorScope, record: ConversationRecord) -> None:
        """@brief 保存会话 / Persist a conversation.

        @param scope workspace 范围 / Workspace scope.
        @param record 会话聚合 / Conversation aggregate.
        """

    async def get_conversation(self, scope: ActorScope, conversation_id: str) -> ConversationRecord | None:
        """@brief 范围内查询会话 / Read a scoped conversation.

        @param scope workspace 范围 / Workspace scope.
        @param conversation_id 会话 ID / Conversation ID.
        @return 会话或 None / Conversation or None.
        """

    async def create_message(self, scope: ActorScope, record: MessageRecord) -> None:
        """@brief 保存消息 / Persist a message.

        @param scope workspace 范围 / Workspace scope.
        @param record 消息实体 / Message entity.
        """

    async def get_message(self, scope: ActorScope, message_id: str) -> MessageRecord | None:
        """@brief 范围内查询消息 / Read a scoped message.

        @param scope workspace 范围 / Workspace scope.
        @param message_id 消息 ID / Message ID.
        @return 消息或 None / Message or None.
        """

    async def list_messages(self, scope: ActorScope, conversation_id: str) -> list[MessageRecord]:
        """@brief 列出会话消息 / List conversation messages.

        @param scope workspace 范围 / Workspace scope.
        @param conversation_id 会话 ID / Conversation ID.
        @return 消息列表 / Message list.
        """

    async def create_run(self, scope: ActorScope, record: AgentRunRecord) -> None:
        """@brief 保存 Agent Run / Persist an Agent Run.

        @param scope workspace 范围 / Workspace scope.
        @param record Run 记录 / Run record.
        """

    async def get_run(self, scope: ActorScope, run_id: str) -> AgentRunRecord | None:
        """@brief 范围内查询 Run / Read a scoped run.

        @param scope workspace 范围 / Workspace scope.
        @param run_id Run ID / Run ID.
        @return Run 或 None / Run or None.
        """

    async def save_run(self, scope: ActorScope, record: AgentRunRecord) -> None:
        """@brief 保存 Run 状态 / Persist Run state.

        @param scope workspace 范围 / Workspace scope.
        @param record Run 记录 / Run record.
        """


class InterviewRepository(Protocol):
    """@brief 面试 Repository 端口 / Interview repository port."""

    async def create_session(self, scope: ActorScope, record: InterviewSessionRecord) -> None:
        """@brief 保存面试会话 / Persist an interview session.

        @param scope workspace 范围 / Workspace scope.
        @param record Session 记录 / Session record.
        """

    async def get_session(self, scope: ActorScope, session_id: str) -> InterviewSessionRecord | None:
        """@brief 范围内查询面试 / Read a scoped interview session.

        @param scope workspace 范围 / Workspace scope.
        @param session_id Session ID / Session ID.
        @return Session 或 None / Session or None.
        """

    async def save_session(self, scope: ActorScope, record: InterviewSessionRecord) -> None:
        """@brief 保存面试状态 / Persist interview state.

        @param scope workspace 范围 / Workspace scope.
        @param record Session 记录 / Session record.
        """

    async def save_report(self, scope: ActorScope, report: dict[str, Any]) -> None:
        """@brief 保存面试报告 / Persist an interview report.

        @param scope workspace 范围 / Workspace scope.
        @param report 报告对象 / Report object.
        """

    async def get_report(self, scope: ActorScope, report_id: str) -> dict[str, Any] | None:
        """@brief 范围内查询报告 / Read a scoped interview report.

        @param scope workspace 范围 / Workspace scope.
        @param report_id 报告 ID / Report ID.
        @return 报告或 None / Report or None.
        """


class KnowledgeRepository(Protocol):
    """@brief 知识库 Repository 端口 / Knowledge repository port."""

    async def create_source(self, scope: ActorScope, record: KnowledgeSourceRecord) -> None:
        """@brief 保存知识来源 / Persist a knowledge source.

        @param scope workspace 范围 / Workspace scope.
        @param record 来源聚合 / Source aggregate.
        """

    async def get_source(self, scope: ActorScope, source_id: str) -> KnowledgeSourceRecord | None:
        """@brief 范围内查询来源 / Read a scoped source.

        @param scope workspace 范围 / Workspace scope.
        @param source_id 来源 ID / Source ID.
        @return 来源或 None / Source or None.
        """

    async def list_sources(self, scope: ActorScope) -> list[KnowledgeSourceRecord]:
        """@brief 列出范围内来源 / List scoped sources.

        @param scope workspace 范围 / Workspace scope.
        @return 来源聚合 / Source aggregates.
        """

    async def save_source(self, scope: ActorScope, record: KnowledgeSourceRecord) -> None:
        """@brief 保存来源状态 / Persist source state.

        @param scope workspace 范围 / Workspace scope.
        @param record 来源聚合 / Source aggregate.
        """

    async def get_embedding_space(self, scope: ActorScope) -> EmbeddingSpace | None:
        """@brief 查询范围内默认 embedding space / Read the scoped default embedding space.

        @param scope workspace 范围 / Workspace scope.
        @return embedding space 或 None / Embedding space or None.
        """

    async def save_embedding_space(self, scope: ActorScope, space: EmbeddingSpace) -> None:
        """@brief 保存不可变 embedding space / Persist an immutable embedding space.

        @param scope workspace 范围 / Workspace scope.
        @param space embedding space / Embedding space.
        """

    async def rank_chunks_by_vector(
        self,
        scope: ActorScope,
        chunk_ids: list[str],
        embedding_space_id: str,
        query_vector: tuple[float, ...],
        limit: int,
    ) -> list[tuple[str, float]]:
        """Rank an authorized chunk subset with the configured vector space."""


class KnowledgeBlobStorage(Protocol):
    """Private binary storage used by file-backed knowledge sources."""

    async def put(
        self,
        scope: ActorScope,
        file_id: str,
        filename: str,
        content_type: str,
        content: bytes,
    ) -> StoredKnowledgeBlob:
        """Persist validated bytes and return opaque storage metadata."""

    async def read(self, scope: ActorScope, storage_key: str) -> bytes:
        """Read bytes only when the key belongs to the supplied actor scope."""

    async def delete(self, scope: ActorScope, storage_key: str) -> None:
        """Delete a blob owned by the supplied actor scope if it exists."""


class KnowledgeFileParser(Protocol):
    """Parser boundary for bounded, supported knowledge files."""

    async def parse(
        self,
        filename: str,
        content_type: str,
        content: bytes,
    ) -> ParsedKnowledgeDocument:
        """Parse bytes into semantic parts or raise a stable domain error."""


class EmbeddingProvider(Protocol):
    """Replaceable embedding adapter with an immutable configured dimension."""

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        """Return one normalized vector for each input text."""


class JobRepository(Protocol):
    """@brief Job Repository 端口 / Job repository port."""

    async def create_job(self, scope: ActorScope, job: Job) -> None:
        """@brief 保存 Job / Persist a job.

        @param scope workspace 范围 / Workspace scope.
        @param job Job 实体 / Job entity.
        """

    async def get_job(self, scope: ActorScope, job_id: str) -> Job | None:
        """@brief 范围内查询 Job / Read a scoped job.

        @param scope workspace 范围 / Workspace scope.
        @param job_id Job ID / Job ID.
        @return Job 或 None / Job or None.
        """

    async def save_job(self, scope: ActorScope, job: Job) -> None:
        """@brief 保存 Job 状态 / Persist job state.

        @param scope workspace 范围 / Workspace scope.
        @param job Job 实体 / Job entity.
        """


class ArtifactRepository(Protocol):
    """@brief 渲染产物 Repository 端口 / Render artifact repository port."""

    async def save_artifact(
        self,
        scope: ActorScope,
        artifact: dict[str, Any],
        content: bytes,
        source_map: dict[str, Any] | None,
    ) -> None:
        """@brief 保存渲染产物 / Persist a render artifact.

        @param scope workspace 范围 / Workspace scope.
        @param artifact 公开产物元数据 / Public artifact metadata.
        @param content 二进制内容 / Binary content.
        @param source_map 可选 source map / Optional source map.
        """

    async def get_artifact(
        self,
        scope: ActorScope,
        artifact_id: str,
    ) -> tuple[dict[str, Any], bytes, dict[str, Any] | None] | None:
        """@brief 范围内查询渲染产物 / Read a scoped render artifact.

        @param scope workspace 范围 / Workspace scope.
        @param artifact_id 产物 ID / Artifact ID.
        @return metadata、内容、source map 或 None / Metadata, content, source map, or None.
        """

    async def list_artifacts(
        self,
        scope: ActorScope,
        resume_id: str,
    ) -> list[dict[str, Any]]:
        """List artifact metadata for one scoped Resume in newest-first order."""


class Renderer(Protocol):
    """@brief 私有渲染器端口 / Private renderer port."""

    async def render(self, document: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
        """@brief 渲染 PDF 及 source map / Render a PDF and source map.

        @param document ResumeDocument SIR / ResumeDocument SIR.
        @return PDF 字节和 source map / PDF bytes and source map.
        """


class ModelProvider(Protocol):
    """@brief Provider 无关的模型端口 / Provider-independent model port."""

    def stream_text(self, prompt: str, request: dict[str, Any]) -> AsyncIterator[str]:
        """@brief 流式产生文本 / Stream text.

        @param prompt 已授权的输入文本 / Authorized input text.
        @param request 推理意图 / Inference intent.
        @return 文本分片异步迭代器 / Async iterator of text chunks.
        """


class TelemetryWriter(Protocol):
    """@brief telemetry 持久化端口 / Telemetry persistence port."""

    async def write_batch(self, records: list[TelemetryRecord]) -> None:
        """@brief 批量写 telemetry / Write telemetry as a batch.

        @param records 已过滤记录 / Filtered records.
        """
