"""@brief 确定性内存 Repository 适配器 / Deterministic in-memory repository adapters."""

from __future__ import annotations

import asyncio
import hashlib
from copy import deepcopy
from datetime import timedelta
from typing import Any

from backend.domain.agent import AgentRunRecord, ConversationRecord, MessageRecord
from backend.domain.common import Job, JobStatus, iso_timestamp, utc_now
from backend.domain.interview import InterviewSessionRecord
from backend.domain.knowledge import EmbeddingSpace, KnowledgeSourceRecord
from backend.domain.proposal import ResumeProposalRecord
from backend.domain.resume import ResumeRecord
from workspace_shared.tenancy import ActorScope


class InMemoryWorkspaceRepository:
    """@brief 全范围检查的确定性内存存储 / Deterministic in-memory store with mandatory scope checks.

    @note MOCK — 测试/研发适配器，接口形状与 PostgreSQL Repository 保持一致。
    """

    def __init__(self) -> None:
        """@brief 初始化各聚合存储 / Initialize aggregate stores."""
        self._lock = asyncio.Lock()
        self._resumes: dict[str, ResumeRecord] = {}
        self._proposals: dict[str, ResumeProposalRecord] = {}
        self._conversations: dict[str, ConversationRecord] = {}
        self._messages: dict[str, tuple[ActorScope, MessageRecord]] = {}
        self._runs: dict[str, AgentRunRecord] = {}
        self._sessions: dict[str, InterviewSessionRecord] = {}
        self._reports: dict[str, tuple[ActorScope, dict[str, Any]]] = {}
        self._sources: dict[str, KnowledgeSourceRecord] = {}
        self._spaces: dict[tuple[str, str], EmbeddingSpace] = {}
        self._jobs: dict[str, tuple[ActorScope, Job]] = {}
        self._artifacts: dict[str, tuple[ActorScope, dict[str, Any], bytes, dict[str, Any] | None]] = {}
        self._identity_created_at = utc_now()

    async def get_current_user(self, scope: ActorScope) -> dict[str, Any] | None:
        """Project the development identity assertion as a CurrentUser resource."""
        return {
            "id": scope.actor_id,
            "display_name": "Local Demo User",
            "email": None,
            "locale": "zh-CN",
            "timezone": "Asia/Shanghai",
            "default_workspace_id": scope.workspace_id,
            "created_at": iso_timestamp(self._identity_created_at),
        }

    async def list_workspaces(self, scope: ActorScope) -> list[dict[str, Any]]:
        """Return the single workspace authorized by the development assertion."""
        return [_memory_workspace(scope, self._identity_created_at)]

    async def get_workspace(
        self, scope: ActorScope, workspace_id: str
    ) -> dict[str, Any] | None:
        """Return the asserted workspace only."""
        if workspace_id != scope.workspace_id:
            return None
        return _memory_workspace(scope, self._identity_created_at)

    async def list_workspace_members(
        self, scope: ActorScope, workspace_id: str
    ) -> list[dict[str, Any]]:
        """Return the actor represented by the trusted development assertion."""
        if workspace_id != scope.workspace_id:
            return []
        timestamp = iso_timestamp(self._identity_created_at)
        digest = hashlib.sha256(
            f"{workspace_id}:{scope.actor_id}".encode()
        ).hexdigest()[:32]
        return [
            {
                "id": f"wsm_{digest}",
                "created_at": timestamp,
                "updated_at": timestamp,
                "revision": 1,
                "workspace_id": workspace_id,
                "user_id": scope.actor_id,
                "role": "owner" if scope.actor_id == scope.resource_owner_id else "editor",
                "status": "active",
                "extensions": {"aiws": {"identity_source": "development_mock"}},
            }
        ]

    async def create_resume(self, scope: ActorScope, record: ResumeRecord) -> None:
        """@brief 保存新简历 / Persist a new resume.

        @param scope workspace 范围 / Workspace scope.
        @param record 简历聚合 / Resume aggregate.
        """
        async with self._lock:
            _assert_scope(scope, record.scope)
            self._resumes[record.id] = record

    async def get_resume(self, scope: ActorScope, resume_id: str) -> ResumeRecord | None:
        """@brief 范围内查询简历 / Read a scoped resume.

        @param scope workspace 范围 / Workspace scope.
        @param resume_id 简历 ID / Resume ID.
        @return 简历或 None / Resume or None.
        """
        async with self._lock:
            record = self._resumes.get(resume_id)
            return record if record is not None and _same_scope(scope, record.scope) else None

    async def list_resumes(self, scope: ActorScope) -> list[ResumeRecord]:
        """@brief 列出范围内简历 / List scoped resumes.

        @param scope workspace 范围 / Workspace scope.
        @return 简历列表 / Resume list.
        """
        async with self._lock:
            return [record for record in self._resumes.values() if _same_scope(scope, record.scope)]

    async def save_resume(self, scope: ActorScope, record: ResumeRecord) -> None:
        """@brief 保存简历聚合 / Persist a resume aggregate.

        @param scope workspace 范围 / Workspace scope.
        @param record 简历聚合 / Resume aggregate.
        """
        await self.create_resume(scope, record)

    async def save_resume_and_job(
        self,
        scope: ActorScope,
        record: ResumeRecord,
        job: Job,
    ) -> None:
        """Atomically persist a Resume and queued render Job in memory."""
        async with self._lock:
            _assert_scope(scope, record.scope)
            self._resumes[record.id] = record
            self._jobs[job.id] = (scope, job)

    async def commit_resume_workflow(
        self,
        scope: ActorScope,
        record: ResumeRecord,
        knowledge_source: KnowledgeSourceRecord,
        knowledge_job: Job,
        render_job: Job | None,
        *,
        create_resume: bool,
    ) -> None:
        """Atomically accept Resume, knowledge ingestion, and optional render work in memory."""
        del create_resume
        async with self._lock:
            _assert_scope(scope, record.scope)
            _assert_scope(scope, knowledge_source.scope)
            self._resumes[record.id] = record
            self._sources[knowledge_source.id] = knowledge_source
            self._jobs[knowledge_job.id] = (scope, knowledge_job)
            if render_job is not None:
                self._jobs[render_job.id] = (scope, render_job)

    async def create_proposal(self, scope: ActorScope, record: ResumeProposalRecord) -> None:
        """Persist a tenant-scoped Resume proposal."""
        async with self._lock:
            _assert_scope(scope, record.scope)
            self._proposals[record.id] = record

    async def get_proposal(
        self, scope: ActorScope, proposal_id: str
    ) -> ResumeProposalRecord | None:
        """Read a tenant-scoped Resume proposal."""
        async with self._lock:
            record = self._proposals.get(proposal_id)
            return record if record is not None and _same_scope(scope, record.scope) else None

    async def list_proposals(
        self, scope: ActorScope, resume_id: str
    ) -> list[ResumeProposalRecord]:
        """List scoped proposals for one Resume in newest-first order."""
        async with self._lock:
            records = [
                record
                for record in self._proposals.values()
                if _same_scope(scope, record.scope) and record.resume_id == resume_id
            ]
            return sorted(records, key=lambda record: (record.updated_at, record.id), reverse=True)

    async def save_proposal(self, scope: ActorScope, record: ResumeProposalRecord) -> None:
        """Persist a Resume proposal decision."""
        await self.create_proposal(scope, record)

    async def create_conversation(self, scope: ActorScope, record: ConversationRecord) -> None:
        """@brief 保存新会话 / Persist a new conversation.

        @param scope workspace 范围 / Workspace scope.
        @param record 会话记录 / Conversation record.
        """
        async with self._lock:
            _assert_scope(scope, record.scope)
            self._conversations[record.id] = record

    async def get_conversation(self, scope: ActorScope, conversation_id: str) -> ConversationRecord | None:
        """@brief 范围内查询会话 / Read a scoped conversation.

        @param scope workspace 范围 / Workspace scope.
        @param conversation_id 会话 ID / Conversation ID.
        @return 会话或 None / Conversation or None.
        """
        async with self._lock:
            record = self._conversations.get(conversation_id)
            return record if record is not None and _same_scope(scope, record.scope) else None

    async def create_message(self, scope: ActorScope, record: MessageRecord) -> None:
        """@brief 保存消息 / Persist a message.

        @param scope workspace 范围 / Workspace scope.
        @param record 消息记录 / Message record.
        """
        async with self._lock:
            conversation = self._conversations.get(record.conversation_id)
            if conversation is None or not _same_scope(scope, conversation.scope):
                raise PermissionError("conversation is outside the supplied scope")
            self._messages[record.id] = (scope, record)
            if record.id not in conversation.message_ids:
                conversation.message_ids.append(record.id)

    async def get_message(self, scope: ActorScope, message_id: str) -> MessageRecord | None:
        """@brief 范围内查询消息 / Read a scoped message.

        @param scope workspace 范围 / Workspace scope.
        @param message_id 消息 ID / Message ID.
        @return 消息或 None / Message or None.
        """
        async with self._lock:
            item = self._messages.get(message_id)
            return item[1] if item is not None and _same_scope(scope, item[0]) else None

    async def list_messages(self, scope: ActorScope, conversation_id: str) -> list[MessageRecord]:
        """@brief 列出会话消息 / List conversation messages.

        @param scope workspace 范围 / Workspace scope.
        @param conversation_id 会话 ID / Conversation ID.
        @return 消息列表 / Message list.
        """
        async with self._lock:
            conversation = self._conversations.get(conversation_id)
            if conversation is None or not _same_scope(scope, conversation.scope):
                return []
            return [self._messages[message_id][1] for message_id in conversation.message_ids]

    async def create_run(self, scope: ActorScope, record: AgentRunRecord) -> None:
        """@brief 保存新 Agent Run / Persist a new Agent Run.

        @param scope workspace 范围 / Workspace scope.
        @param record Run 记录 / Run record.
        """
        async with self._lock:
            _assert_scope(scope, record.scope)
            self._runs[record.id] = record

    async def get_run(self, scope: ActorScope, run_id: str) -> AgentRunRecord | None:
        """@brief 范围内查询 Agent Run / Read a scoped Agent Run.

        @param scope workspace 范围 / Workspace scope.
        @param run_id Run ID / Run ID.
        @return Run 或 None / Run or None.
        """
        async with self._lock:
            record = self._runs.get(run_id)
            return record if record is not None and _same_scope(scope, record.scope) else None

    async def save_run(self, scope: ActorScope, record: AgentRunRecord) -> None:
        """@brief 保存 Agent Run / Persist an Agent Run.

        @param scope workspace 范围 / Workspace scope.
        @param record Run 记录 / Run record.
        """
        await self.create_run(scope, record)

    async def create_session(self, scope: ActorScope, record: InterviewSessionRecord) -> None:
        """@brief 保存新面试 Session / Persist a new interview Session.

        @param scope workspace 范围 / Workspace scope.
        @param record Session 记录 / Session record.
        """
        async with self._lock:
            _assert_scope(scope, record.scope)
            self._sessions[record.id] = record

    async def get_session(self, scope: ActorScope, session_id: str) -> InterviewSessionRecord | None:
        """@brief 范围内查询面试 Session / Read a scoped interview Session.

        @param scope workspace 范围 / Workspace scope.
        @param session_id Session ID / Session ID.
        @return Session 或 None / Session or None.
        """
        async with self._lock:
            record = self._sessions.get(session_id)
            return record if record is not None and _same_scope(scope, record.scope) else None

    async def list_sessions(self, scope: ActorScope) -> list[InterviewSessionRecord]:
        """List scoped interview sessions in newest-first order."""
        async with self._lock:
            records = [
                record
                for record in self._sessions.values()
                if _same_scope(scope, record.scope)
            ]
            return sorted(
                records,
                key=lambda record: (record.updated_at, record.id),
                reverse=True,
            )

    async def save_session(self, scope: ActorScope, record: InterviewSessionRecord) -> None:
        """@brief 保存面试 Session / Persist an interview Session.

        @param scope workspace 范围 / Workspace scope.
        @param record Session 记录 / Session record.
        """
        await self.create_session(scope, record)

    async def save_report(self, scope: ActorScope, report: dict[str, Any]) -> None:
        """@brief 保存面试报告 / Persist an interview report.

        @param scope workspace 范围 / Workspace scope.
        @param report 报告对象 / Report object.
        """
        async with self._lock:
            self._reports[str(report["id"])] = (scope, deepcopy(report))

    async def get_report(self, scope: ActorScope, report_id: str) -> dict[str, Any] | None:
        """@brief 范围内查询面试报告 / Read a scoped interview report.

        @param scope workspace 范围 / Workspace scope.
        @param report_id 报告 ID / Report ID.
        @return 报告或 None / Report or None.
        """
        async with self._lock:
            item = self._reports.get(report_id)
            return deepcopy(item[1]) if item is not None and _same_scope(scope, item[0]) else None

    async def create_source(self, scope: ActorScope, record: KnowledgeSourceRecord) -> None:
        """@brief 保存新知识来源 / Persist a new knowledge source.

        @param scope workspace 范围 / Workspace scope.
        @param record 来源记录 / Source record.
        """
        async with self._lock:
            _assert_scope(scope, record.scope)
            self._sources[record.id] = record

    async def get_source(self, scope: ActorScope, source_id: str) -> KnowledgeSourceRecord | None:
        """@brief 范围内查询知识来源 / Read a scoped knowledge source.

        @param scope workspace 范围 / Workspace scope.
        @param source_id 来源 ID / Source ID.
        @return 来源或 None / Source or None.
        """
        async with self._lock:
            record = self._sources.get(source_id)
            return record if record is not None and _same_scope(scope, record.scope) else None

    async def list_sources(self, scope: ActorScope) -> list[KnowledgeSourceRecord]:
        """@brief 列出范围内知识来源 / List scoped knowledge sources.

        @param scope workspace 范围 / Workspace scope.
        @return 来源列表 / Source list.
        """
        async with self._lock:
            return [record for record in self._sources.values() if _same_scope(scope, record.scope)]

    async def save_source(self, scope: ActorScope, record: KnowledgeSourceRecord) -> None:
        """@brief 保存知识来源 / Persist a knowledge source.

        @param scope workspace 范围 / Workspace scope.
        @param record 来源记录 / Source record.
        """
        await self.create_source(scope, record)

    async def save_source_if_revision(
        self,
        scope: ActorScope,
        record: KnowledgeSourceRecord,
        expected_revision: int,
    ) -> bool:
        """Compare-and-set one source while holding the repository lock."""
        async with self._lock:
            _assert_scope(scope, record.scope)
            current = self._sources.get(record.id)
            if (
                current is None
                or not _same_scope(scope, current.scope)
                or current.revision != expected_revision
            ):
                return False
            self._sources[record.id] = record
            return True

    async def save_source_and_job(
        self,
        scope: ActorScope,
        record: KnowledgeSourceRecord,
        job: Job,
    ) -> None:
        """Atomically publish source and ingestion Job state in memory."""
        async with self._lock:
            _assert_scope(scope, record.scope)
            self._sources[record.id] = record
            self._jobs[job.id] = (scope, job)

    async def get_embedding_space(self, scope: ActorScope) -> EmbeddingSpace | None:
        """@brief 查询范围默认 embedding space / Read the scoped default embedding space.

        @param scope workspace 范围 / Workspace scope.
        @return embedding space 或 None / Embedding space or None.
        """
        async with self._lock:
            return self._spaces.get((scope.workspace_id, scope.resource_owner_id))

    async def save_embedding_space(self, scope: ActorScope, space: EmbeddingSpace) -> None:
        """@brief 保存范围默认 embedding space / Persist the scoped default embedding space.

        @param scope workspace 范围 / Workspace scope.
        @param space embedding space / Embedding space.
        """
        async with self._lock:
            key = (scope.workspace_id, scope.resource_owner_id)
            existing = self._spaces.get(key)
            if existing is not None and existing != space:
                raise ValueError("embedding spaces are immutable; create a data migration for a new space")
            self._spaces[key] = space

    async def rank_chunks_by_vector(
        self,
        scope: ActorScope,
        chunk_ids: list[str],
        embedding_space_id: str,
        query_vector: tuple[float, ...],
        limit: int,
    ) -> list[tuple[str, float]]:
        """Rank an authorized chunk subset with normalized cosine similarity."""
        authorized = set(chunk_ids)
        ranked: list[tuple[str, float]] = []
        async with self._lock:
            for source in self._sources.values():
                if not _same_scope(scope, source.scope):
                    continue
                for chunk in source.chunks:
                    if (
                        chunk.id not in authorized
                        or chunk.embedding_space_id != embedding_space_id
                        or len(chunk.vector) != len(query_vector)
                    ):
                        continue
                    dot = sum(
                        left * right
                        for left, right in zip(chunk.vector, query_vector, strict=True)
                    )
                    ranked.append((chunk.id, min(1.0, max(0.0, (dot + 1.0) / 2.0))))
        ranked.sort(key=lambda item: item[1], reverse=True)
        return ranked[:limit]

    async def create_job(self, scope: ActorScope, job: Job) -> None:
        """@brief 保存新 Job / Persist a new Job.

        @param scope workspace 范围 / Workspace scope.
        @param job Job 实体 / Job entity.
        """
        async with self._lock:
            self._jobs[job.id] = (scope, job)

    async def get_job(self, scope: ActorScope, job_id: str) -> Job | None:
        """@brief 范围内查询 Job / Read a scoped Job.

        @param scope workspace 范围 / Workspace scope.
        @param job_id Job ID / Job ID.
        @return Job 或 None / Job or None.
        """
        async with self._lock:
            item = self._jobs.get(job_id)
            return item[1] if item is not None and _same_scope(scope, item[0]) else None

    async def claim_job(
        self,
        scope: ActorScope,
        job_id: str,
        stale_after_seconds: int = 900,
    ) -> Job | None:
        """Atomically claim one queued in-memory Job."""
        async with self._lock:
            item = self._jobs.get(job_id)
            if item is None or not _same_scope(scope, item[0]):
                return None
            job = item[1]
            stale = (
                job.status is JobStatus.RUNNING
                and job.started_at is not None
                and job.started_at <= utc_now() - timedelta(seconds=stale_after_seconds)
            )
            if job.status is not JobStatus.QUEUED and not stale:
                return None
            if stale:
                job.status = JobStatus.QUEUED
                job.phase = "queued"
                job.started_at = None
            job.start()
            return job

    async def save_job(self, scope: ActorScope, job: Job) -> None:
        """@brief 保存 Job / Persist a Job.

        @param scope workspace 范围 / Workspace scope.
        @param job Job 实体 / Job entity.
        """
        await self.create_job(scope, job)

    async def save_artifact(
        self,
        scope: ActorScope,
        artifact: dict[str, Any],
        content: bytes,
        source_map: dict[str, Any] | None,
    ) -> None:
        """@brief 保存渲染产物 / Persist a render artifact.

        @param scope workspace 范围 / Workspace scope.
        @param artifact 公开产物 metadata / Public artifact metadata.
        @param content 二进制内容 / Binary content.
        @param source_map 可选 source map / Optional source map.
        """
        async with self._lock:
            self._artifacts[str(artifact["id"])] = (scope, deepcopy(artifact), content, deepcopy(source_map))

    async def save_artifact_and_job(
        self,
        scope: ActorScope,
        artifact: dict[str, Any],
        content: bytes,
        source_map: dict[str, Any] | None,
        job: Job,
    ) -> None:
        """Atomically publish artifact bytes and successful render Job state in memory."""
        async with self._lock:
            self._artifacts[str(artifact["id"])] = (
                scope,
                deepcopy(artifact),
                content,
                deepcopy(source_map),
            )
            self._jobs[job.id] = (scope, job)

    async def get_artifact(self, scope: ActorScope, artifact_id: str) -> tuple[dict[str, Any], bytes, dict[str, Any] | None] | None:
        """@brief 范围内查询渲染产物 / Read a scoped render artifact.

        @param scope workspace 范围 / Workspace scope.
        @param artifact_id 产物 ID / Artifact ID.
        @return metadata、内容、source map 或 None / Metadata, content, source map, or None.
        """
        async with self._lock:
            item = self._artifacts.get(artifact_id)
            if item is None or not _same_scope(scope, item[0]):
                return None
            return deepcopy(item[1]), item[2], deepcopy(item[3])

    async def list_artifacts(
        self, scope: ActorScope, resume_id: str
    ) -> list[dict[str, Any]]:
        """List scoped render-artifact metadata for one Resume."""
        async with self._lock:
            artifacts = [
                deepcopy(item[1])
                for item in self._artifacts.values()
                if _same_scope(scope, item[0]) and item[1].get("resume_id") == resume_id
            ]
            return sorted(
                artifacts,
                key=lambda artifact: (
                    str(artifact.get("updated_at", "")),
                    str(artifact.get("id", "")),
                ),
                reverse=True,
            )


def _same_scope(left: ActorScope, right: ActorScope) -> bool:
    """@brief 对比资源边界 / Compare resource boundaries.

    @param left 请求范围 / Request scope.
    @param right 已存资源范围 / Stored resource scope.
    @return workspace 和 owner 同时匹配时为真 / True when workspace and owner both match.
    """
    return left.workspace_id == right.workspace_id and left.resource_owner_id == right.resource_owner_id


def _memory_workspace(scope: ActorScope, created_at: Any) -> dict[str, Any]:
    """Build the stable workspace view for the in-memory development adapter."""
    timestamp = iso_timestamp(created_at)
    digest = hashlib.sha256(scope.workspace_id.encode("utf-8")).hexdigest()[:12]
    return {
        "id": scope.workspace_id,
        "created_at": timestamp,
        "updated_at": timestamp,
        "revision": 1,
        "name": "Local Demo Workspace",
        "slug": f"local-{digest}",
        "default_locale": "zh-CN",
        "timezone": "Asia/Shanghai",
        "plan": "free",
        "extensions": {"aiws": {"identity_source": "development_mock"}},
    }


def _assert_scope(expected: ActorScope, actual: ActorScope) -> None:
    """@brief 强制写入范围 / Enforce a write scope.

    @param expected 请求范围 / Request scope.
    @param actual 资源范围 / Resource scope.
    @raise PermissionError 范围不匹配时抛出 / Raised for a scope mismatch.
    """
    if not _same_scope(expected, actual):
        raise PermissionError("resource is outside the supplied scope")
