"""@brief Knowledge durable worker 的 PostgreSQL 两短事务 store / PostgreSQL two-short-transaction store for the durable Knowledge worker.

每次 claim 都递增 Job/source/Connection revision，作为旧 worker 的 fencing token。
外部 I/O 不持有数据库事务；完成事务以 claim 后 revision 精确 CAS，并一次提交领域状态、
统一 Job、索引、outbox 与 audit。
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import sqlalchemy as sa
from pydantic import TypeAdapter, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.application.ports.knowledge_worker import (
    KnowledgeWorkerClaim,
    KnowledgeWorkerTerminalFailure,
    PreparedKnowledgeIndex,
)
from backend.domain.connections import ConnectionProvider, CredentialReference
from backend.domain.knowledge_jobs import (
    ConnectionRevokeSpec,
    KnowledgeDeleteSpec,
    KnowledgeJobKind,
    KnowledgeJobSpec,
    KnowledgeProcessSpec,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceInput,
    KnowledgeSourceType,
    KnowledgeSourceVersionId,
)
from backend.domain.outbox import initial_outbox_lifecycle
from backend.domain.platform import ApiEventId, JobId, JsonValue
from backend.domain.principals import UserId, WorkspaceId
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    AuditEventRecord,
    ConnectionRecord,
    EmbeddingSpaceRecord,
    JobRecord,
    KnowledgeChunkRecord,
    KnowledgeCitationRecord,
    KnowledgeEmbeddingRecord,
    KnowledgeIngestionJobRecord,
    KnowledgeSourceRecord,
    KnowledgeSourceVersionRecord,
    KnowledgeVisibilityPolicyRecord,
    OutboxEventRecord,
)
from workspace_shared.ids import new_opaque_id

_JOB_SPEC_ADAPTER: TypeAdapter[KnowledgeJobSpec] = TypeAdapter(KnowledgeJobSpec)
"""@brief 持久化 Knowledge Job spec codec / Persisted Knowledge Job-spec codec."""

_SOURCE_INPUT_ADAPTER: TypeAdapter[KnowledgeSourceInput] = TypeAdapter(KnowledgeSourceInput)
"""@brief 私有来源判别联合 codec / Private source discriminated-union codec."""

_EVENT_RETENTION = timedelta(days=30)
"""@brief worker 追加事件的 replay retention / Replay retention for worker-appended events."""


class KnowledgeWorkerStoreConflict(RuntimeError):
    """@brief claim fencing token 已被更新 / A claim fencing token has been superseded."""


class PostgresKnowledgeWorkerStore:
    """@brief 统一 Job 驱动的 Knowledge durable worker store / Knowledge durable-worker store driven by unified Jobs."""

    def __init__(self, database: AsyncDatabase) -> None:
        """@brief 绑定 lifespan-owned 数据库 / Bind the lifespan-owned database."""

        self._database = database

    async def claim(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        event_id: ApiEventId,
        job_id: JobId,
    ) -> KnowledgeWorkerClaim | None:
        """@brief 锁定 typed Job/target 并递增 fencing revision / Lock the typed Job and target and advance fencing revisions."""

        async with self._database.new_session() as session:
            async with session.begin():
                await self._scope(session, workspace_id, actor_id)
                job = await self._job_for_update(session, workspace_id, actor_id, job_id)
                if job is None:
                    raise KnowledgeWorkerStoreConflict("Knowledge worker Job is unavailable")
                if job.status in {"succeeded", "failed", "cancelled", "expired"}:
                    return None
                if job.status not in {"queued", "running"}:
                    raise KnowledgeWorkerStoreConflict("Knowledge worker Job has an invalid status")
                kind = KnowledgeJobKind(job.job_type)
                spec = _load_job_spec(job.request_payload)
                _validate_job_binding(job, kind, spec)
                now = datetime.now(UTC)
                if kind is KnowledgeJobKind.CONNECTION_REVOKE:
                    if not isinstance(spec, ConnectionRevokeSpec):
                        raise ValueError("connection-revoke Job contains the wrong spec")
                    connection = await session.scalar(
                        sa.select(ConnectionRecord)
                        .where(
                            ConnectionRecord.workspace_id == str(workspace_id),
                            ConnectionRecord.id == str(spec.connection_id),
                        )
                        .with_for_update()
                    )
                    if connection is None:
                        await self._fail_invalid_target(
                            session,
                            job,
                            event_id,
                            actor_id,
                            "connection.not_found",
                            now,
                        )
                        return None
                    if connection.created_by != str(actor_id):
                        await self._fail_invalid_target(
                            session,
                            job,
                            event_id,
                            actor_id,
                            "connection.revocation_actor_mismatch",
                            now,
                        )
                        return None
                    if connection.status == "revoked":
                        _succeed_job(job, now, result_refs=[])
                        await _append_journals(
                            session,
                            job,
                            event_id,
                            actor_id,
                            action="connection.revoke.replayed",
                            outcome="allowed",
                            details={"connection_id": connection.id},
                        )
                        return None
                    if connection.status != "revoking":
                        await self._fail_invalid_target(
                            session,
                            job,
                            event_id,
                            actor_id,
                            "connection.revocation_state_invalid",
                            now,
                        )
                        return None
                    _start_or_reclaim_job(job, now, phase="revoking")
                    connection.revision += 1
                    connection.updated_at = now
                    return KnowledgeWorkerClaim(
                        event_id,
                        job_id,
                        workspace_id,
                        actor_id,
                        kind,
                        spec,
                        job.revision,
                        None,
                        None,
                        connection_revision=connection.revision,
                        connection_provider=ConnectionProvider(connection.provider),
                        credential_reference=CredentialReference(connection.credential_reference),
                    )

                source_spec = _source_spec(spec)
                source = await session.scalar(
                    sa.select(KnowledgeSourceRecord)
                    .where(
                        KnowledgeSourceRecord.workspace_id == str(workspace_id),
                        KnowledgeSourceRecord.id == str(source_spec.source_id),
                    )
                    .with_for_update()
                )
                if source is None:
                    await self._fail_invalid_target(
                        session,
                        job,
                        event_id,
                        actor_id,
                        "knowledge_source.not_found",
                        now,
                    )
                    return None
                if job.status == "queued" and source.revision != source_spec.source_revision:
                    _succeed_job(job, now, result_refs=[])
                    job.result = {"skipped": True, "reason": "superseded"}
                    await _append_journals(
                        session,
                        job,
                        event_id,
                        actor_id,
                        action="knowledge.job.superseded",
                        outcome="allowed",
                        details={"source_id": source.id},
                    )
                    return None
                if kind is KnowledgeJobKind.KNOWLEDGE_DELETE:
                    if source.ingestion_state == "deleted":
                        _succeed_job(job, now, result_refs=[])
                        await _append_journals(
                            session,
                            job,
                            event_id,
                            actor_id,
                            action="knowledge.delete.replayed",
                            outcome="allowed",
                            details={"source_id": source.id},
                        )
                        return None
                    if source.ingestion_state != "deleting":
                        await self._fail_invalid_target(
                            session,
                            job,
                            event_id,
                            actor_id,
                            "knowledge_source.deletion_state_invalid",
                            now,
                        )
                        return None
                    phase = "deleting"
                else:
                    if source.ingestion_state not in {
                        "queued",
                        "fetching",
                        "parsing",
                        "chunking",
                        "embedding",
                    }:
                        await self._fail_invalid_target(
                            session,
                            job,
                            event_id,
                            actor_id,
                            "knowledge_source.ingestion_state_invalid",
                            now,
                        )
                        return None
                    source.ingestion_state = "fetching"
                    source.last_problem = None
                    phase = "fetching"
                policy = await session.scalar(
                    sa.select(KnowledgeVisibilityPolicyRecord).where(
                        KnowledgeVisibilityPolicyRecord.workspace_id == str(workspace_id),
                        KnowledgeVisibilityPolicyRecord.source_id == source.id,
                        KnowledgeVisibilityPolicyRecord.policy_version
                        == source.current_policy_version,
                    )
                )
                if policy is None:
                    await self._fail_invalid_target(
                        session,
                        job,
                        event_id,
                        actor_id,
                        "knowledge_source.policy_unavailable",
                        now,
                    )
                    return None
                source_input = _load_source_input(source.source_input)
                _start_or_reclaim_job(job, now, phase=phase)
                source.revision += 1
                source.updated_at = now
                return KnowledgeWorkerClaim(
                    event_id,
                    job_id,
                    workspace_id,
                    actor_id,
                    kind,
                    spec,
                    job.revision,
                    source.revision,
                    KnowledgeSourceType(source.source_type),
                    source_input,
                    UserId(source.resource_owner_id),
                    _source_metadata(source.public_config),
                    policy.allow_external_model_processing,
                    tuple(policy.allowed_model_regions),
                )

    async def complete_connection_revocation(self, claim: KnowledgeWorkerClaim) -> None:
        """@brief 原子完成 Connection+Job+journals / Atomically complete Connection, Job, and journals."""

        if not isinstance(claim.spec, ConnectionRevokeSpec):
            raise ValueError("connection completion received the wrong claim")
        async with self._database.new_session() as session:
            async with session.begin():
                await self._scope(session, claim.workspace_id, claim.actor_id)
                job = await self._claimed_job(session, claim)
                if job is None:
                    return
                connection = await session.scalar(
                    sa.select(ConnectionRecord)
                    .where(
                        ConnectionRecord.workspace_id == str(claim.workspace_id),
                        ConnectionRecord.id == str(claim.spec.connection_id),
                    )
                    .with_for_update()
                )
                if (
                    connection is None
                    or connection.status != "revoking"
                    or connection.revision != claim.connection_revision
                ):
                    raise KnowledgeWorkerStoreConflict("Connection revocation target changed")
                now = datetime.now(UTC)
                connection.status = "revoked"
                connection.scopes = []
                connection.problem = None
                connection.updated_at = now
                connection.revision += 1
                _succeed_job(job, now, result_refs=[])
                await _append_journals(
                    session,
                    job,
                    claim.event_id,
                    claim.actor_id,
                    action="connection.revoke.completed",
                    outcome="allowed",
                    details={"connection_id": connection.id},
                )

    async def complete_source_deletion(self, claim: KnowledgeWorkerClaim) -> None:
        """@brief 原子移除 index、擦除私有 locator 并完成 Job / Atomically remove the index, erase private locators, and complete the Job."""

        if not isinstance(claim.spec, KnowledgeDeleteSpec):
            raise ValueError("source deletion completion received the wrong claim")
        async with self._database.new_session() as session:
            async with session.begin():
                await self._scope(session, claim.workspace_id, claim.actor_id)
                job = await self._claimed_job(session, claim)
                if job is None:
                    return
                source = await self._claimed_source(session, claim, claim.spec.source_id)
                if source.ingestion_state != "deleting":
                    raise KnowledgeWorkerStoreConflict("Knowledge source deletion target changed")
                version_ids = sa.select(KnowledgeSourceVersionRecord.id).where(
                    KnowledgeSourceVersionRecord.workspace_id == str(claim.workspace_id),
                    KnowledgeSourceVersionRecord.source_id == source.id,
                )
                chunk_ids = sa.select(KnowledgeChunkRecord.id).where(
                    KnowledgeChunkRecord.workspace_id == str(claim.workspace_id),
                    KnowledgeChunkRecord.source_version_id.in_(version_ids),
                )
                await session.execute(
                    sa.delete(KnowledgeCitationRecord).where(
                        KnowledgeCitationRecord.workspace_id == str(claim.workspace_id),
                        KnowledgeCitationRecord.chunk_id.in_(chunk_ids),
                    )
                )
                await session.execute(
                    sa.delete(KnowledgeChunkRecord).where(
                        KnowledgeChunkRecord.workspace_id == str(claim.workspace_id),
                        KnowledgeChunkRecord.source_version_id.in_(version_ids),
                    )
                )
                await session.execute(
                    sa.update(KnowledgeSourceVersionRecord)
                    .where(
                        KnowledgeSourceVersionRecord.workspace_id == str(claim.workspace_id),
                        KnowledgeSourceVersionRecord.source_id == source.id,
                    )
                    .values(origin={}, parser_metadata={})
                )
                now = datetime.now(UTC)
                source.title = "Deleted knowledge source"
                source.source_input = _tombstone_source_input(source.source_input)
                source.public_config = _tombstone_public_config(source.source_input)
                source.enabled = False
                source.ingestion_state = "deleted"
                source.document_count = 0
                source.chunk_count = 0
                source.last_problem = None
                source.deleted_at = now
                source.updated_at = now
                source.revision += 1
                _succeed_job(job, now, result_refs=[])
                await _append_journals(
                    session,
                    job,
                    claim.event_id,
                    claim.actor_id,
                    action="knowledge.delete.completed",
                    outcome="allowed",
                    details={"source_id": source.id},
                )

    async def complete_processing(
        self,
        claim: KnowledgeWorkerClaim,
        prepared: PreparedKnowledgeIndex,
    ) -> KnowledgeSourceVersionId:
        """@brief 原子发布 immutable version index 与成功 Job / Atomically publish an immutable version index and successful Job."""

        if not isinstance(claim.spec, KnowledgeProcessSpec):
            raise ValueError("Knowledge processing completion received the wrong claim")
        async with self._database.new_session() as session:
            async with session.begin():
                await self._scope(session, claim.workspace_id, claim.actor_id)
                job = await self._claimed_job(session, claim)
                if job is None:
                    current = await session.scalar(
                        sa.select(KnowledgeSourceRecord.current_version_id).where(
                            KnowledgeSourceRecord.workspace_id == str(claim.workspace_id),
                            KnowledgeSourceRecord.id == str(claim.spec.source_id),
                        )
                    )
                    if not isinstance(current, str):
                        raise KnowledgeWorkerStoreConflict("completed Knowledge Job has no version")
                    return KnowledgeSourceVersionId(current)
                source = await self._claimed_source(session, claim, claim.spec.source_id)
                if source.ingestion_state not in {
                    "fetching",
                    "parsing",
                    "chunking",
                    "embedding",
                }:
                    raise KnowledgeWorkerStoreConflict("Knowledge processing target changed")
                now = datetime.now(UTC)
                version, created = await _target_version(
                    session,
                    claim,
                    source,
                    prepared,
                    now,
                )
                space_id = await _ensure_embedding_space(
                    session,
                    claim,
                    prepared,
                    source.resource_owner_id,
                    now,
                )
                source.current_version_id = version.id
                await session.flush()
                if not created:
                    await session.execute(
                        sa.delete(KnowledgeChunkRecord).where(
                            KnowledgeChunkRecord.workspace_id == str(claim.workspace_id),
                            KnowledgeChunkRecord.source_version_id == version.id,
                        )
                    )
                chunk_ids: list[str] = []
                for chunk in prepared.chunks:
                    chunk_id = _stable_row_id("chunk", version.id, str(chunk.ordinal), chunk.text)
                    session.add(
                        KnowledgeChunkRecord(
                            id=chunk_id,
                            workspace_id=str(claim.workspace_id),
                            resource_owner_id=source.resource_owner_id,
                            source_version_id=version.id,
                            ordinal=chunk.ordinal,
                            text_content=chunk.text,
                            content_hash=hashlib.sha256(chunk.text.encode("utf-8")).hexdigest(),
                            origin={
                                "metadata": {
                                    "path": chunk.locator,
                                    "content_type": chunk.content_type,
                                }
                            },
                            token_count=len(chunk.text.split()),
                            created_at=now,
                            updated_at=now,
                            revision=1,
                            extensions={},
                        )
                    )
                    chunk_ids.append(chunk_id)
                await session.flush()
                for chunk_id, chunk in zip(chunk_ids, prepared.chunks, strict=True):
                    session.add(
                        KnowledgeEmbeddingRecord(
                            id=_stable_row_id("embedding", chunk_id, space_id),
                            workspace_id=str(claim.workspace_id),
                            resource_owner_id=source.resource_owner_id,
                            chunk_id=chunk_id,
                            embedding_space_id=space_id,
                            embedding=list(chunk.embedding),
                            created_at=now,
                            updated_at=now,
                            revision=1,
                            extensions={},
                        )
                    )
                version.status = "ready"
                version.parser_metadata = cast(dict[str, Any], dict(prepared.parsed.metadata))
                version.indexed_at = now
                version.updated_at = now
                version.revision += 1
                source.ingestion_state = "ready"
                source.document_count = 1
                source.chunk_count = len(prepared.chunks)
                source.last_success_at = now
                source.last_problem = None
                source.updated_at = now
                source.revision += 1
                await _upsert_ingestion_job(session, claim, version.id, len(prepared.chunks), now)
                result_ref: dict[str, JsonValue] = {
                    "resource_type": "knowledge_source_version",
                    "id": version.id,
                    "revision": version.revision,
                }
                _succeed_job(job, now, result_refs=[result_ref])
                job.completed_units = len(prepared.chunks)
                job.total_units = len(prepared.chunks)
                job.progress_unit = "items"
                job.result = {
                    "source_version_id": version.id,
                    "documents": 1,
                    "chunks": len(prepared.chunks),
                    "embedded_tokens": sum(len(chunk.text.split()) for chunk in prepared.chunks),
                }
                await _append_journals(
                    session,
                    job,
                    claim.event_id,
                    claim.actor_id,
                    action="knowledge.index.completed",
                    outcome="allowed",
                    details={"source_id": source.id, "source_version_id": version.id},
                )
                return KnowledgeSourceVersionId(version.id)

    async def fail(self, claim: KnowledgeWorkerClaim, *, error_code: str) -> None:
        """@brief 最终失败时原子对齐 target、Job 与 journals / Atomically align target, Job, and journals on terminal failure."""

        if not error_code or len(error_code) > 100:
            raise ValueError("Knowledge worker failure code is invalid")
        async with self._database.new_session() as session:
            async with session.begin():
                await self._scope(session, claim.workspace_id, claim.actor_id)
                job = await self._claimed_job(session, claim)
                if job is None:
                    return
                now = datetime.now(UTC)
                problem = _problem(error_code, str(claim.event_id))
                if isinstance(claim.spec, ConnectionRevokeSpec):
                    connection = await session.scalar(
                        sa.select(ConnectionRecord)
                        .where(
                            ConnectionRecord.workspace_id == str(claim.workspace_id),
                            ConnectionRecord.id == str(claim.spec.connection_id),
                        )
                        .with_for_update()
                    )
                    if (
                        connection is not None
                        and connection.status != "revoked"
                        and connection.revision == claim.connection_revision
                    ):
                        connection.status = "failed"
                        connection.problem = problem
                        connection.updated_at = now
                        connection.revision += 1
                    elif connection is not None and connection.status != "revoked":
                        raise KnowledgeWorkerStoreConflict(
                            "Connection failure target fencing token was superseded"
                        )
                else:
                    source_spec = _source_spec(claim.spec)
                    source = await self._claimed_source(session, claim, source_spec.source_id)
                    source.ingestion_state = "failed"
                    source.last_problem = problem
                    source.updated_at = now
                    source.revision += 1
                _fail_job(job, now, problem)
                await _append_journals(
                    session,
                    job,
                    claim.event_id,
                    claim.actor_id,
                    action="knowledge.job.failed",
                    outcome="failed",
                    details={"error_code": error_code},
                )

    async def fail_exhausted(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        event_id: ApiEventId,
        event_type: str,
        job_id: JobId,
    ) -> None:
        """@brief 仅凭可信 header 与持久 Job 原子闭合耗尽工作 / Atomically close exhausted work using trusted headers and the persisted Job only.

        @param workspace_id outbox Workspace / Outbox Workspace.
        @param actor_id outbox 中的原始 Job creator / Original Job creator from the outbox.
        @param event_id 失败事件关联 ID / Failed-event correlation ID.
        @param event_type 已注册的工作事件类型 / Registered work-event type.
        @param job_id outbox subject Job ID / Outbox-subject Job ID.
        @note 不解析 ``request_payload`` 或 event payload。补偿事务失败时异常向上传播，
            dispatcher 因而不会把 source outbox 标为 failed。/ Neither ``request_payload`` nor
            the event payload is parsed. Transaction failures propagate so the dispatcher cannot
            mark the source outbox failed first.
        """
        async with self._database.new_session() as session:
            async with session.begin():
                await self._scope(session, workspace_id, actor_id)
                job = await self._job_for_update(
                    session,
                    workspace_id,
                    actor_id,
                    job_id,
                )
                if job is None or job.status in {
                    "succeeded",
                    "failed",
                    "cancelled",
                    "expired",
                }:
                    return
                if job.status not in {"queued", "running"}:
                    return
                try:
                    kind = KnowledgeJobKind(job.job_type)
                except ValueError:
                    return
                if not _exhaustion_event_matches(event_type, kind):
                    return
                now = datetime.now(UTC)
                problem = _problem(
                    "knowledge.worker_attempts_exhausted",
                    str(event_id),
                )
                if (
                    kind is KnowledgeJobKind.CONNECTION_REVOKE
                    and job.target_resource_type == "connection"
                ):
                    connection = await session.scalar(
                        sa.select(ConnectionRecord)
                        .where(
                            ConnectionRecord.workspace_id == str(workspace_id),
                            ConnectionRecord.created_by == str(actor_id),
                            ConnectionRecord.id == job.target_resource_id,
                        )
                        .with_for_update()
                    )
                    if connection is not None and connection.status == "revoking":
                        connection.status = "failed"
                        connection.problem = problem
                        connection.updated_at = now
                        connection.revision += 1
                elif (
                    kind
                    in {
                        KnowledgeJobKind.KNOWLEDGE_DELETE,
                        KnowledgeJobKind.KNOWLEDGE_INGEST,
                        KnowledgeJobKind.KNOWLEDGE_SYNC,
                    }
                    and job.target_resource_type == "knowledge_source"
                ):
                    source = await session.scalar(
                        sa.select(KnowledgeSourceRecord)
                        .where(
                            KnowledgeSourceRecord.workspace_id == str(workspace_id),
                            KnowledgeSourceRecord.id == job.target_resource_id,
                        )
                        .with_for_update()
                    )
                    active_states = (
                        {"deleting"}
                        if kind is KnowledgeJobKind.KNOWLEDGE_DELETE
                        else {"queued", "fetching", "parsing", "chunking", "embedding"}
                    )
                    if source is not None and source.ingestion_state in active_states:
                        source.ingestion_state = "failed"
                        source.last_problem = problem
                        source.updated_at = now
                        source.revision += 1
                else:
                    return
                _fail_job(job, now, problem)
                await _append_journals(
                    session,
                    job,
                    event_id,
                    actor_id,
                    action="knowledge.job.failed",
                    outcome="failed",
                    details={"error_code": "knowledge.worker_attempts_exhausted"},
                )

    async def _scope(
        self,
        session: AsyncSession,
        workspace_id: WorkspaceId,
        actor_id: UserId,
    ) -> None:
        """@brief 安装事件原始 actor+Workspace scope / Install the event's original actor-and-Workspace scope."""

        await self._database.install_v2_request_scope(
            session,
            actor_id=str(actor_id),
            workspace_id=str(workspace_id),
        )

    @staticmethod
    async def _job_for_update(
        session: AsyncSession,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
    ) -> JobRecord | None:
        """@brief 锁定且验证事件 actor 是 Job creator / Lock a Job and verify the event actor is its creator."""

        return cast(
            JobRecord | None,
            await session.scalar(
                sa.select(JobRecord)
                .where(
                    JobRecord.workspace_id == str(workspace_id),
                    JobRecord.resource_owner_id == str(actor_id),
                    JobRecord.id == str(job_id),
                )
                .with_for_update()
            ),
        )

    async def _claimed_job(
        self,
        session: AsyncSession,
        claim: KnowledgeWorkerClaim,
    ) -> JobRecord | None:
        """@brief 以 revision fencing token 锁定 running Job / Lock a running Job by its revision fencing token."""

        job = await session.scalar(
            sa.select(JobRecord)
            .where(
                JobRecord.workspace_id == str(claim.workspace_id),
                JobRecord.resource_owner_id == str(claim.actor_id),
                JobRecord.id == str(claim.job_id),
            )
            .with_for_update()
        )
        if job is None:
            raise KnowledgeWorkerStoreConflict("claimed Knowledge Job disappeared")
        if job.status in {"succeeded", "failed", "cancelled", "expired"}:
            return None
        if job.status != "running" or job.revision != claim.job_revision:
            raise KnowledgeWorkerStoreConflict("Knowledge Job fencing token was superseded")
        return job

    @staticmethod
    async def _claimed_source(
        session: AsyncSession,
        claim: KnowledgeWorkerClaim,
        source_id: KnowledgeSourceId,
    ) -> KnowledgeSourceRecord:
        """@brief 以 source revision fencing token 锁定来源 / Lock a source by its revision fencing token."""

        source = await session.scalar(
            sa.select(KnowledgeSourceRecord)
            .where(
                KnowledgeSourceRecord.workspace_id == str(claim.workspace_id),
                KnowledgeSourceRecord.id == str(source_id),
            )
            .with_for_update()
        )
        if source is None or source.revision != claim.source_revision:
            raise KnowledgeWorkerStoreConflict("Knowledge source fencing token was superseded")
        return source

    async def _fail_invalid_target(
        self,
        session: AsyncSession,
        job: JobRecord,
        event_id: ApiEventId,
        actor_id: UserId,
        error_code: str,
        now: datetime,
    ) -> None:
        """@brief claim 事务内终结确定性坏引用 / Terminate a deterministic bad reference inside the claim transaction."""

        if job.status == "queued":
            _start_or_reclaim_job(job, now, phase="validating")
        problem = _problem(error_code, str(event_id))
        _fail_job(job, now, problem)
        await _append_journals(
            session,
            job,
            event_id,
            actor_id,
            action="knowledge.job.failed",
            outcome="failed",
            details={"error_code": error_code},
        )


def _load_job_spec(payload: object) -> KnowledgeJobSpec:
    """@brief 从不可信 Job JSONB 读取 typed spec / Read a typed spec from untrusted Job JSONB."""

    if not isinstance(payload, Mapping) or "spec" not in payload:
        raise ValueError("Knowledge Job request payload is invalid")
    try:
        return _JOB_SPEC_ADAPTER.validate_python(payload["spec"])
    except ValidationError as error:
        raise ValueError("Knowledge Job spec violates the V2 domain model") from error


def _exhaustion_event_matches(
    event_type: str,
    kind: KnowledgeJobKind,
) -> bool:
    """@brief 验证工作事件与持久 Job kind 的封闭绑定 / Validate the closed binding between work event and persisted Job kind.

    @param event_type outbox 独立列中的事件类型 / Event type from the outbox's dedicated column.
    @param kind 持久统一 Job kind / Persisted unified Job kind.
    @return 仅精确生产者绑定匹配时为真 / True only for an exact producer binding.
    """
    return kind in {
        "connection.revocation_requested": {KnowledgeJobKind.CONNECTION_REVOKE},
        "knowledge_source.deletion_requested": {KnowledgeJobKind.KNOWLEDGE_DELETE},
        "knowledge_source.job_created": {
            KnowledgeJobKind.KNOWLEDGE_INGEST,
            KnowledgeJobKind.KNOWLEDGE_SYNC,
        },
    }.get(event_type, set())


def _load_source_input(payload: object) -> KnowledgeSourceInput:
    """@brief 从不可信来源 JSONB 读取判别联合 / Read the source discriminated union from untrusted JSONB."""

    try:
        return _SOURCE_INPUT_ADAPTER.validate_python(payload)
    except ValidationError as error:
        raise ValueError("Knowledge source input violates the V2 domain model") from error


def _validate_job_binding(job: JobRecord, kind: KnowledgeJobKind, spec: KnowledgeJobSpec) -> None:
    """@brief 验证 kind、subject 与 spec 不能错配 / Validate that kind, subject, and spec cannot be confused."""

    if kind is KnowledgeJobKind.CONNECTION_REVOKE:
        valid = (
            isinstance(spec, ConnectionRevokeSpec)
            and job.target_resource_type == "connection"
            and job.target_resource_id == str(spec.connection_id)
        )
    else:
        source_spec = _source_spec(spec)
        valid = job.target_resource_type == "knowledge_source" and job.target_resource_id == str(
            source_spec.source_id
        )
    if not valid:
        raise ValueError("Knowledge Job subject and typed spec diverge")


def _source_spec(spec: KnowledgeJobSpec) -> KnowledgeDeleteSpec | KnowledgeProcessSpec:
    """@brief 缩窄 source Job spec / Narrow a source-Job spec."""

    if isinstance(spec, (KnowledgeDeleteSpec, KnowledgeProcessSpec)):
        return spec
    raise ValueError("Connection spec cannot drive a Knowledge source Job")


def _start_or_reclaim_job(job: JobRecord, now: datetime, *, phase: str) -> None:
    """@brief 启动或重 claim running Job 并递增 fencing revision / Start or reclaim a running Job and advance its fencing revision."""

    if job.status == "queued":
        job.started_at = now
    job.status = "running"
    job.phase = phase
    job.completed_units = 0
    job.total_units = None
    job.progress_unit = "unknown"
    job.percent = None
    job.problem = None
    job.result_refs = []
    job.finished_at = None
    job.updated_at = now
    job.revision += 1


def _succeed_job(
    job: JobRecord,
    now: datetime,
    *,
    result_refs: list[dict[str, JsonValue]],
) -> None:
    """@brief 将已启动 Job 置为 succeeded / Transition a started Job to succeeded."""

    if job.started_at is None:
        job.started_at = now
    job.status = "succeeded"
    job.phase = "completed"
    job.problem = None
    job.result_refs = cast(list[dict[str, Any]], result_refs)
    job.finished_at = now
    job.percent = 100.0
    job.updated_at = now
    job.revision += 1


def _fail_job(job: JobRecord, now: datetime, problem: dict[str, Any]) -> None:
    """@brief 将已启动 Job 置为 failed / Transition a started Job to failed."""

    if job.started_at is None:
        job.started_at = now
    job.status = "failed"
    job.phase = "failed"
    job.problem = problem
    job.result_refs = []
    job.finished_at = now
    job.percent = None
    job.updated_at = now
    job.revision += 1


def _problem(error_code: str, request_id: str) -> dict[str, Any]:
    """@brief 构造不含异常正文的契约 ProblemDetails / Build contract ProblemDetails without exception text."""

    return {
        "type_uri": f"https://api.hmalliances.org/problems/{error_code}",
        "title": "Knowledge background processing failed",
        "status": 500,
        "code": error_code,
        "request_id": request_id,
        "retryable": False,
        "detail": None,
        "instance": None,
        "errors": [],
        "extensions": {},
    }


async def _target_version(
    session: AsyncSession,
    claim: KnowledgeWorkerClaim,
    source: KnowledgeSourceRecord,
    prepared: PreparedKnowledgeIndex,
    now: datetime,
) -> tuple[KnowledgeSourceVersionRecord, bool]:
    """@brief 使用初始 pending file version 或分配新 immutable version / Use an initial pending file version or allocate a new immutable version."""

    spec = cast(KnowledgeProcessSpec, claim.spec)
    existing: KnowledgeSourceVersionRecord | None = None
    if spec.version_id is not None:
        existing = await session.scalar(
            sa.select(KnowledgeSourceVersionRecord)
            .where(
                KnowledgeSourceVersionRecord.workspace_id == str(claim.workspace_id),
                KnowledgeSourceVersionRecord.id == str(spec.version_id),
                KnowledgeSourceVersionRecord.source_id == source.id,
            )
            .with_for_update()
        )
        if existing is None:
            raise KnowledgeWorkerTerminalFailure("knowledge.version_unavailable")
    use_existing = (
        claim.kind is KnowledgeJobKind.KNOWLEDGE_INGEST
        and existing is not None
        and existing.status in {"pending", "failed"}
    )
    if use_existing:
        assert existing is not None
        if (
            existing.content_sha256 != prepared.content_sha256
            or existing.size_bytes != prepared.size_bytes
        ):
            raise KnowledgeWorkerTerminalFailure("knowledge.version_content_mismatch")
        existing.status = "indexing"
        return existing, False
    source.version_counter += 1
    identifier = _stable_row_id("knowledge_version", str(claim.job_id), prepared.content_sha256)
    version = KnowledgeSourceVersionRecord(
        id=identifier,
        workspace_id=str(claim.workspace_id),
        resource_owner_id=source.resource_owner_id,
        source_id=source.id,
        version_no=source.version_counter,
        content_hash=prepared.content_sha256,
        content_sha256=prepared.content_sha256,
        size_bytes=prepared.size_bytes,
        status="indexing",
        artifact_type="knowledge_source_version",
        artifact_id=identifier,
        artifact_revision=1,
        origin={"source_type": source.source_type},
        parser_metadata={},
        indexed_at=None,
        created_at=now,
        updated_at=now,
        revision=1,
        extensions={},
    )
    session.add(version)
    return version, True


async def _ensure_embedding_space(
    session: AsyncSession,
    claim: KnowledgeWorkerClaim,
    prepared: PreparedKnowledgeIndex,
    owner_id: str,
    now: datetime,
) -> str:
    """@brief 复用精确 identity space 或插入稳定 ID / Reuse an exact identity space or insert its stable ID."""

    selected = prepared.embedding_space
    existing = await session.scalar(
        sa.select(EmbeddingSpaceRecord).where(
            EmbeddingSpaceRecord.workspace_id == str(claim.workspace_id),
            EmbeddingSpaceRecord.resource_owner_id == owner_id,
            EmbeddingSpaceRecord.provider == selected.provider,
            EmbeddingSpaceRecord.model == selected.model,
            EmbeddingSpaceRecord.model_revision == selected.model_revision,
            EmbeddingSpaceRecord.dimension == selected.dimension,
            EmbeddingSpaceRecord.distance_metric == selected.distance_metric,
            EmbeddingSpaceRecord.normalization == selected.normalization,
        )
    )
    if existing is not None:
        if existing.retired_at is not None:
            raise KnowledgeWorkerTerminalFailure("knowledge.embedding_space_retired")
        return existing.id
    session.add(
        EmbeddingSpaceRecord(
            id=selected.id,
            workspace_id=str(claim.workspace_id),
            resource_owner_id=owner_id,
            provider=selected.provider,
            model=selected.model,
            model_revision=selected.model_revision,
            dimension=selected.dimension,
            distance_metric=selected.distance_metric,
            normalization=selected.normalization,
            retired_at=None,
            created_at=now,
            updated_at=now,
            revision=1,
            extensions={},
        )
    )
    return selected.id


async def _upsert_ingestion_job(
    session: AsyncSession,
    claim: KnowledgeWorkerClaim,
    version_id: str,
    chunk_count: int,
    now: datetime,
) -> None:
    """@brief 记录统一 Job 与 Knowledge index 的 typed association / Record the typed association between a unified Job and Knowledge index."""

    existing = await session.scalar(
        sa.select(KnowledgeIngestionJobRecord).where(
            KnowledgeIngestionJobRecord.workspace_id == str(claim.workspace_id),
            KnowledgeIngestionJobRecord.job_id == str(claim.job_id),
        )
    )
    statistics = {
        "documents": 1,
        "chunks": chunk_count,
        "embedded_tokens": 0,
        "skipped": 0,
    }
    if existing is not None:
        existing.source_version_id = version_id
        existing.statistics = statistics
        existing.updated_at = now
        existing.revision += 1
        return
    source_spec = _source_spec(claim.spec)
    session.add(
        KnowledgeIngestionJobRecord(
            id=_stable_row_id("knowledge_ingestion", str(claim.job_id)),
            workspace_id=str(claim.workspace_id),
            resource_owner_id=str(claim.source_owner_id),
            job_id=str(claim.job_id),
            source_id=str(source_spec.source_id),
            source_version_id=version_id,
            operation=claim.kind.value,
            statistics=statistics,
            created_at=now,
            updated_at=now,
            revision=1,
            extensions={},
        )
    )


async def _append_journals(
    session: AsyncSession,
    job: JobRecord,
    event_id: ApiEventId,
    actor_id: UserId,
    *,
    action: str,
    outcome: str,
    details: dict[str, JsonValue],
) -> None:
    """@brief 在同一事务追加 Job notification 与 audit / Append a Job notification and audit in the same transaction."""

    now = job.updated_at
    lifecycle = initial_outbox_lifecycle("job.updated", occurred_at=now)
    session.add(
        AuditEventRecord(
            id=new_opaque_id("audit"),
            workspace_id=job.workspace_id,
            resource_owner_id=job.resource_owner_id,
            occurred_at=now,
            actor_type="user",
            actor_id=str(actor_id),
            actor_revision=None,
            action=action,
            resource_type=job.target_resource_type,
            resource_id=job.target_resource_id,
            resource_revision=job.target_resource_revision,
            request_id=str(event_id),
            outcome=outcome,
            details=cast(dict[str, Any], details),
            created_at=now,
            updated_at=now,
            revision=1,
            extensions={},
        )
    )
    session.add(
        OutboxEventRecord(
            id=new_opaque_id("evt"),
            workspace_id=job.workspace_id,
            resource_owner_id=job.resource_owner_id,
            aggregate_type="job",
            aggregate_id=job.id,
            subject_revision=job.revision,
            event_type="job.updated",
            sequence=0,
            occurred_at=now,
            payload={
                "actor_id": str(actor_id),
                "subject": {
                    "resource_type": "job",
                    "id": job.id,
                    "revision": job.revision,
                },
                "data": {"status": job.status, **details},
            },
            replay_expires_at=now + _EVENT_RETENTION,
            status=lifecycle.status,
            published_at=lifecycle.published_at,
            created_at=now,
            updated_at=now,
            revision=1,
            extensions={},
        )
    )


def _source_metadata(value: object) -> dict[str, str]:
    """@brief 只投影 worker 所需的公开字符串元数据 / Project only public string metadata required by the worker."""

    if not isinstance(value, Mapping):
        raise ValueError("Knowledge source public config is invalid")
    allowed = {"filename", "media_type", "url", "clone_url", "ref", "resume_id"}
    return {
        key: item
        for key, item in value.items()
        if key in allowed and isinstance(key, str) and isinstance(item, str)
    }


def _tombstone_source_input(value: object) -> dict[str, Any]:
    """@brief 擦除私有正文/URL/remote ID，同时保留合法判别与关系引用 / Erase private bodies, URLs, and remote IDs while retaining a valid discriminator and relational references."""

    if not isinstance(value, Mapping) or not isinstance(value.get("source_type"), str):
        raise ValueError("Knowledge source input cannot be tombstoned")
    payload = dict(value)
    source_type = payload["source_type"]
    if source_type == "manual_note":
        payload["content"] = "[deleted]"
    elif source_type in {"url", "website", "blog_feed"}:
        payload["url"] = "https://deleted.invalid/"
    elif source_type == "git_repository":
        payload.update(
            clone_url="https://deleted.invalid/",
            ref=None,
            include_paths=[],
            exclude_paths=[],
        )
    elif source_type == "cloud_drive":
        payload["remote_id"] = "[deleted]"
    return cast(dict[str, Any], payload)


def _tombstone_public_config(source_input: Mapping[str, Any]) -> dict[str, Any]:
    """@brief 生成与 tombstone input 一致的公开配置 / Build public config matching the tombstoned input."""

    source_type = source_input.get("source_type")
    if source_type == "file":
        return {"filename": "deleted", "media_type": "application/octet-stream"}
    if source_type in {"url", "website", "blog_feed"}:
        return {"url": "https://deleted.invalid/"}
    if source_type == "git_repository":
        return {"clone_url": "https://deleted.invalid/", "ref": None}
    if source_type == "resume":
        resume_id = source_input.get("resume_id")
        if not isinstance(resume_id, str):
            raise ValueError("Resume tombstone lost its relational reference")
        return {"resume_id": resume_id}
    return {}


def _stable_row_id(prefix: str, *parts: str) -> str:
    """@brief 从稳定业务 identity 派生 opaque row ID / Derive an opaque row ID from stable business identity."""

    payload = "\x00".join(parts).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(payload).hexdigest()[:32]}"


__all__ = ["KnowledgeWorkerStoreConflict", "PostgresKnowledgeWorkerStore"]
