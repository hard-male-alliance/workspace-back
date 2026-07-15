"""@brief 五条纵向切片的应用服务 / Application services for the five vertical slices."""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from backend.config import AISettings, NetworkSettings
from backend.domain.agent import AgentRunRecord, AgentRunStatus, ConversationRecord, MessageRecord
from backend.domain.common import DomainError, Job, Problem, iso_timestamp, utc_now
from backend.domain.interview import InterviewSessionRecord, InterviewStatus
from backend.domain.knowledge import EmbeddingSpace, KnowledgeChunk, KnowledgeSourceRecord
from backend.domain.ports import (
    AgentRepository,
    ArtifactRepository,
    InterviewRepository,
    JobRepository,
    KnowledgeRepository,
    ModelProvider,
    Renderer,
    ResumeKnowledgeBridge,
    ResumeRepository,
)
from backend.domain.resume import ResumeRecord, create_empty_document
from backend.infrastructure.concurrency import BackpressureError, BoundedTaskSupervisor
from backend.infrastructure.rendering import artifact_sha256
from backend.infrastructure.telemetry import BufferedTelemetrySink
from workspace_shared.ids import new_opaque_id
from workspace_shared.tenancy import ActorScope

_MAX_AGENT_DELTA_CHARACTERS = 100_000
"""@brief 单个 Agent SSE delta 的契约上限 / Contract limit for one Agent SSE delta."""

_MAX_AGENT_OUTPUT_CHARACTERS = 200_000
"""@brief 单条文本消息的契约上限 / Contract limit for one text message."""

_MAX_AGENT_STREAM_DELTAS = 2_048
"""@brief 单个 Run 可持久化 delta 上限 / Bounded number of persisted deltas per run.

@note 事件存储为 append-only log（仅追加日志）；此上限限制单个 Run 的回放体积、
SSE 轮询成本与恶意 provider 的碎片化放大，而不是依赖全量 delete/reinsert。
"""

_METERING_TOKEN_BYTES = 4
"""@brief v0.1 本地 token 估算的 UTF-8 字节粒度 / UTF-8 byte granularity for the v0.1 local token estimate.

@note 这不是 tokenizer（分词器）或 provider invoice（供应商账单）。以字节累计再向上取整，
使同一输出即使被不同 SSE chunk 边界切分，仍得到稳定、可复算的估算值。
"""

_METERING_TOKENS_PER_MILLION = 1_000_000
"""@brief 配置费率的 token 分母 / Token denominator for configured prices."""

_METERING_ESTIMATOR = "utf8_bytes_div_4_v1"
"""@brief 可审计的本地 token 估算器版本 / Auditable local token-estimator version."""

_METERING_PRICING = "configured_token_rate_v1"
"""@brief 可审计的本地价格表版本 / Auditable local-price-table version."""

_MAX_RESUME_SOURCE_CHARACTERS = 200_000
"""@brief 由 Resume 派生的可索引文本上限 / Maximum indexable text derived from one Resume.

@note 此上限与 mock 知识来源输入上限对齐，避免一份异常 SIR（Semantic Intermediate
Representation，语义中间表示）在异步索引队列中放大内存和向量成本。
"""


@dataclass(slots=True)
class _ScopedLockEntry:
    """@brief 可回收的资源锁条目 / Reclaimable resource-lock entry.

    @param lock 实际异步互斥锁 / Actual asynchronous mutex.
    @param holders_and_waiters 正在持有或等待该锁的协程数 / Number of holders or waiters.
    """

    lock: asyncio.Lock
    holders_and_waiters: int = 0


class ScopedKeyLocks:
    """@brief 按 workspace/owner/key 串行化的轻量锁 / Lightweight locks serialized by workspace/owner/key."""

    def __init__(self) -> None:
        """@brief 初始化锁注册表 / Initialize the lock registry."""
        self._registry_lock = asyncio.Lock()
        self._locks: dict[tuple[str, str, str], _ScopedLockEntry] = {}

    @asynccontextmanager
    async def hold(self, scope: ActorScope, key: str) -> AsyncIterator[None]:
        """@brief 获取资源级锁 / Acquire a resource-level lock.

        @param scope 多租户范围 / Multi-tenant scope.
        @param key 资源稳定 ID / Stable resource ID.
        @return 异步上下文 / Async context.
        """
        lock_key = (scope.workspace_id, scope.resource_owner_id, key)
        async with self._registry_lock:
            entry = self._locks.get(lock_key)
            if entry is None:
                entry = _ScopedLockEntry(asyncio.Lock())
                self._locks[lock_key] = entry
            entry.holders_and_waiters += 1
        try:
            async with entry.lock:
                yield
        finally:
            async with self._registry_lock:
                entry.holders_and_waiters -= 1
                if entry.holders_and_waiters == 0 and self._locks.get(lock_key) is entry:
                    self._locks.pop(lock_key, None)


@dataclass(frozen=True, slots=True)
class ServiceDependencies:
    """@brief 应用服务共享依赖 / Shared application-service dependencies."""

    network: NetworkSettings
    ai: AISettings
    supervisor: BoundedTaskSupervisor
    telemetry: BufferedTelemetrySink


class ResumeApplicationService:
    """@brief 简历创建、操作、编译与下载的应用服务 / Application service for resume CRUD, operations, rendering, and download."""

    def __init__(
        self,
        repository: ResumeRepository,
        jobs: JobRepository,
        artifacts: ArtifactRepository,
        renderer: Renderer,
        knowledge_bridge: ResumeKnowledgeBridge,
        dependencies: ServiceDependencies,
        locks: ScopedKeyLocks,
    ) -> None:
        """@brief 初始化简历服务 / Initialize the resume service.

        @param repository 简历 Repository / Resume repository.
        @param jobs Job Repository / Job repository.
        @param artifacts 产物 Repository / Artifact repository.
        @param renderer 私有 renderer / Private renderer.
        @param knowledge_bridge Resume 派生知识来源的内部桥 / Internal bridge for resume-derived knowledge sources.
        @param dependencies 共享运行时依赖 / Shared runtime dependencies.
        @param locks 资源锁 / Resource locks.
        """
        self._repository = repository
        self._jobs = jobs
        self._artifacts = artifacts
        self._renderer = renderer
        self._knowledge_bridge = knowledge_bridge
        self._dependencies = dependencies
        self._locks = locks

    async def create_resume(
        self,
        scope: ActorScope,
        title: str,
        locale: str,
        template_id: str = "tpl_default_v1",
        template_version: str = "1.0",
        request_id: str | None = None,
    ) -> ResumeRecord:
        """@brief 创建空白 SIR 简历 / Create an empty SIR resume.

        @param scope 多租户范围 / Multi-tenant scope.
        @param title 用户标题 / User-visible title.
        @param locale 语言区域 / Locale.
        @param template_id 模板 ID / Template ID.
        @param template_version 模板版本 / Template version.
        @param request_id 可选请求追踪 ID / Optional request trace ID.
        @return 新建简历聚合 / New resume aggregate.
        """
        resume_id = new_opaque_id("res")
        document = create_empty_document(
            scope,
            resume_id,
            title,
            locale,
            template_id,
            template_version,
            new_opaque_id("sec"),
            new_opaque_id("src"),
        )
        record = ResumeRecord(scope=scope, document=document, revisions={1: deepcopy(document)})
        await self._repository.create_resume(scope, record)
        await self._knowledge_bridge.synchronize_resume(scope, record.snapshot(), request_id)
        self._dependencies.telemetry.record(
            "metric",
            "resume.created",
            1,
            scope,
            request_id,
            {"operation": "create", "outcome": "success"},
            service="backend.worker",
        )
        return record

    async def get_resume(self, scope: ActorScope, resume_id: str, revision: int | None = None) -> ResumeRecord:
        """@brief 获取范围内简历 / Get a scoped resume.

        @param scope 多租户范围 / Multi-tenant scope.
        @param resume_id 简历 ID / Resume ID.
        @param revision 可选历史版本 / Optional historical revision.
        @return 简历聚合 / Resume aggregate.
        @raise DomainError 简历不存在或版本不存在时抛出 / Raised when the resume or revision is absent.
        """
        record = await self._repository.get_resume(scope, resume_id)
        if record is None:
            raise DomainError(Problem("resume.not_found", 404, "Resume was not found"))
        if revision is not None:
            record.snapshot(revision)
        return record

    async def list_resumes(self, scope: ActorScope) -> list[ResumeRecord]:
        """@brief 列出范围内简历 / List scoped resumes.

        @param scope 多租户范围 / Multi-tenant scope.
        @return 简历聚合列表 / Resume aggregate list.
        """
        return await self._repository.list_resumes(scope)

    async def apply_operations(
        self,
        scope: ActorScope,
        resume_id: str,
        batch: dict[str, Any],
        if_match: str | None,
        request_id: str | None,
    ) -> dict[str, Any]:
        """@brief 在资源锁内应用操作批次 / Apply an operation batch under the resource lock.

        @param scope 多租户范围 / Multi-tenant scope.
        @param resume_id 简历 ID / Resume ID.
        @param batch 正式 ResumeOperationBatch / Formal ResumeOperationBatch.
        @param if_match HTTP 强 ETag / HTTP strong ETag.
        @param request_id 请求追踪 ID / Request trace ID.
        @return ResumeOperationBatchResult / ResumeOperationBatchResult.
        @raise DomainError ETag、版本或领域操作无效时抛出 / Raised for invalid ETag, revision, or operation.

        @note ``render_hint`` 的队列满不是回滚已接受编辑的理由：若同步创建的 render job
        遭遇 backpressure（反压），本方法持久化一个 terminal failed job 并把它放进同一份
        idempotent result。这样重试不会重复应用简历操作，也不会在已经提交 revision 后只
        得到一个 503 而失去可观察的 job 状态。
        """
        async with self._locks.hold(scope, resume_id):
            record = await self.get_resume(scope, resume_id)
            normalized = json.dumps(batch, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            cached = record.verify_batch_idempotency(
                str(batch["client_batch_id"]),
                hashlib.sha256(normalized.encode()).hexdigest(),
            )
            if cached is not None:
                return cached
            if if_match is None or if_match != record.etag():
                raise DomainError(
                    Problem(
                        "resume.revision_conflict",
                        412,
                        "Resume revision is stale",
                        extensions={"current_revision": record.revision, "current_etag": record.etag()},
                        retryable=True,
                    )
                )
            previous_revision, new_revision, results, _ = record.apply_operations(
                int(batch["base_revision"]),
                str(batch["conflict_strategy"]),
                list(batch["operations"]),
            )
            await self._repository.save_resume(scope, record)
            if new_revision != previous_revision:
                await self._knowledge_bridge.synchronize_resume(scope, record.snapshot(), request_id)
            render_job: dict[str, Any] | None = None
            if batch.get("render_hint") in {"preview", "final"}:
                detailed_render_job = await self._create_render_job(
                    scope,
                    resume_id,
                    {
                        "resume_revision": new_revision,
                        "mode": batch["render_hint"],
                        "formats": ["pdf"],
                        "include_source_map": True,
                        "include_accessibility_tree": False,
                        "locale": None,
                        "extensions": {},
                    },
                    request_id,
                    raise_on_backpressure=False,
                )
                rendered_job = await self._jobs.get_job(scope, str(detailed_render_job["id"]))
                if rendered_job is None:
                    raise RuntimeError("render job disappeared before its operation-batch response")
                render_job = rendered_job.as_dict()
            result = {
                "resume_id": resume_id,
                "previous_revision": previous_revision,
                "new_revision": new_revision,
                "results": results,
                "normalized_document": record.snapshot(),
                "render_job": render_job,
            }
            record.save_batch_result(
                str(batch["client_batch_id"]),
                hashlib.sha256(normalized.encode()).hexdigest(),
                result,
            )
            await self._repository.save_resume(scope, record)
            self._dependencies.telemetry.record(
                "metric",
                "resume.operations",
                1,
                scope,
                request_id,
                {"operation": "apply", "outcome": "success"},
                service="backend.worker",
            )
            return result

    async def create_render_job(
        self,
        scope: ActorScope,
        resume_id: str,
        request: dict[str, Any],
        request_id: str | None,
    ) -> dict[str, Any]:
        """@brief 创建受控 PDF 编译 Job / Create a controlled PDF-rendering job.

        @param scope 多租户范围 / Multi-tenant scope.
        @param resume_id 简历 ID / Resume ID.
        @param request 正式 RenderJobRequest / Formal RenderJobRequest.
        @param request_id 请求追踪 ID / Request trace ID.
        @return ResumeRenderJob 初始对象 / Initial ResumeRenderJob object.
        """
        return await self._create_render_job(
            scope,
            resume_id,
            request,
            request_id,
            raise_on_backpressure=True,
        )

    async def _create_render_job(
        self,
        scope: ActorScope,
        resume_id: str,
        request: dict[str, Any],
        request_id: str | None,
        *,
        raise_on_backpressure: bool,
    ) -> dict[str, Any]:
        """@brief 创建 render job，并显式决定队列满的调用者语义 / Create a render job and choose caller semantics for a full queue.

        @param scope 多租户范围 / Multi-tenant scope.
        @param resume_id 简历 ID / Resume ID.
        @param request 已校验的 RenderJobRequest / Validated RenderJobRequest.
        @param request_id 请求追踪 ID / Request trace ID.
        @param raise_on_backpressure 直接 Job API 为 ``True``；操作批次联动为 ``False``。
        / ``True`` for the direct Job API; ``False`` for an operation-batch side effect.
        @return 已排队或已失败的 ResumeRenderJob。
        @raise DomainError 队列满且调用方要求直接报告过载时抛出。

        @note 两种路径都会先持久化失败 Job，确保后台队列拒绝不是不可审计的瞬时异常。
        联动路径把失败 Job 作为操作结果的一部分，以保存已经成功持久化的 resume revision
        与其 render 意图之间的可追溯关系。
        """
        record = await self.get_resume(scope, resume_id, int(request["resume_revision"]))
        job = Job(
            id=new_opaque_id("job"),
            job_type="resume.render",
            created_at=utc_now(),
            request_id=request_id,
            extensions={"resume_id": resume_id, "resume_revision": int(request["resume_revision"]), "artifacts": [], "diagnostics": []},
        )
        await self._jobs.create_job(scope, job)
        try:
            self._dependencies.supervisor.submit(
                "render",
                lambda: self._render_job(scope, record.snapshot(int(request["resume_revision"])), job),
                lambda error: self._job_failure(scope, job, error),
                name=f"aiws:render:{job.id}",
            )
        except BackpressureError as error:
            problem = Problem("runtime.overloaded", 503, "Render queue is full", retryable=True)
            job.fail(problem)
            await self._jobs.save_job(scope, job)
            if raise_on_backpressure:
                raise DomainError(problem) from error
        return self._render_job_dict(job)

    async def get_render_job(self, scope: ActorScope, job_id: str) -> dict[str, Any]:
        """@brief 获取 render Job / Get a render job.

        @param scope 多租户范围 / Multi-tenant scope.
        @param job_id Job ID / Job ID.
        @return ResumeRenderJob / ResumeRenderJob.
        @raise DomainError Job 不存在或类型错误时抛出 / Raised when the job is missing or mismatched.
        """
        job = await self._jobs.get_job(scope, job_id)
        if job is None or job.job_type != "resume.render":
            raise DomainError(Problem("resume.render_job_not_found", 404, "Resume render job was not found"))
        return self._render_job_dict(job)

    async def get_artifact(
        self,
        scope: ActorScope,
        artifact_id: str,
    ) -> tuple[dict[str, Any], bytes, dict[str, Any] | None]:
        """@brief 获取范围内产物 / Get a scoped artifact.

        @param scope 多租户范围 / Multi-tenant scope.
        @param artifact_id 产物 ID / Artifact ID.
        @return metadata、内容、source map / Metadata, content, source map.
        @raise DomainError 产物不存在时抛出 / Raised when the artifact is absent.
        """
        artifact = await self._artifacts.get_artifact(scope, artifact_id)
        if artifact is None:
            raise DomainError(Problem("resume.artifact_not_found", 404, "Render artifact was not found"))
        return artifact

    async def _render_job(self, scope: ActorScope, document: dict[str, Any], job: Job) -> None:
        """@brief 执行实际渲染工作 / Execute the actual render work.

        @param scope 多租户范围 / Multi-tenant scope.
        @param document 固定 revision 的 SIR / Fixed-revision SIR.
        @param job 受控 Job / Controlled job.
        """
        job.start()
        await self._jobs.save_job(scope, job)
        pdf, source_map = await self._renderer.render(document)
        artifact_id = new_opaque_id("art")
        source_map["artifact_id"] = artifact_id
        now = iso_timestamp(utc_now())
        artifact = {
            "id": artifact_id,
            "created_at": now,
            "updated_at": now,
            "revision": 1,
            "resume_id": document["id"],
            "resume_revision": document["revision"],
            "format": "pdf",
            "content_type": "application/pdf",
            "size_bytes": len(pdf),
            "sha256": artifact_sha256(pdf),
            "download_url": f"{self._dependencies.network.public_base_url}/api/v1/render-artifacts/{artifact_id}/content",
            "expires_at": None,
            "page_count": 1,
            "source_map_artifact_id": None,
            "extensions": {},
        }
        await self._artifacts.save_artifact(scope, artifact, pdf, source_map)
        job.extensions["artifacts"] = [artifact]
        job.completed_units = 1
        job.total_units = 1
        job.succeed()
        await self._jobs.save_job(scope, job)
        self._dependencies.telemetry.record(
            "metric",
            "resume.render",
            1,
            scope,
            job.request_id,
            {"operation": "render", "outcome": "success", "job_type": "resume.render"},
            service="backend.worker",
        )

    async def _job_failure(self, scope: ActorScope, job: Job, error: BaseException) -> None:
        """@brief 记录后台 Job 失败 / Record a background Job failure.

        @param scope 多租户范围 / Multi-tenant scope.
        @param job Job 实体 / Job entity.
        @param error 原始失败 / Raw failure.
        """
        if isinstance(error, DomainError):
            job.fail(error.problem)
        else:
            job.fail(Problem("resume.render_failed", 500, "Resume rendering failed"))
        await self._jobs.save_job(scope, job)
        self._dependencies.telemetry.record(
            "metric",
            "resume.render",
            1,
            scope,
            job.request_id,
            {"operation": "render", "outcome": "failure", "job_type": "resume.render"},
            service="backend.worker",
        )

    @staticmethod
    def _render_job_dict(job: Job) -> dict[str, Any]:
        """@brief 构建 ResumeRenderJob 视图 / Build a ResumeRenderJob view.

        @param job 基础 Job / Base job.
        @return ResumeRenderJob / ResumeRenderJob.
        """
        payload = job.as_dict()
        payload.update(
            {
                "resume_id": job.extensions["resume_id"],
                "resume_revision": job.extensions["resume_revision"],
                "artifacts": job.extensions["artifacts"],
                "diagnostics": job.extensions["diagnostics"],
            }
        )
        return payload


class AgentApplicationService:
    """@brief 流式 Agent、tool approval 和持久化消息服务 / Streaming Agent, tool-approval, and persisted-message service."""

    def __init__(
        self,
        repository: AgentRepository,
        provider: ModelProvider,
        dependencies: ServiceDependencies,
        locks: ScopedKeyLocks,
    ) -> None:
        """@brief 初始化 Agent 服务 / Initialize the Agent service.

        @param repository Agent Repository / Agent repository.
        @param provider provider 无关模型实现 / Provider-independent model implementation.
        @param dependencies 共享运行时依赖 / Shared runtime dependencies.
        @param locks 资源锁 / Resource locks.
        """
        self._repository = repository
        self._provider = provider
        self._dependencies = dependencies
        self._locks = locks
        self._approval_index: dict[str, tuple[ActorScope, str]] = {}
        self._run_tasks: dict[tuple[str, str, str], asyncio.Task[None]] = {}

    async def create_conversation(
        self,
        scope: ActorScope,
        capability: str,
        title: str | None,
        context_refs: list[dict[str, Any]],
    ) -> ConversationRecord:
        """@brief 创建 Agent 会话 / Create an Agent conversation.

        @param scope 多租户范围 / Multi-tenant scope.
        @param capability 公开能力名 / Public capability name.
        @param title 可选标题 / Optional title.
        @param context_refs 资源引用 / Resource references.
        @return 新会话 / New conversation.
        """
        timestamp = utc_now()
        record = ConversationRecord(scope, new_opaque_id("conv"), timestamp, timestamp, title, capability, context_refs)
        await self._repository.create_conversation(scope, record)
        return record

    async def create_user_message(
        self,
        scope: ActorScope,
        conversation_id: str,
        text: str,
        parent_message_id: str | None = None,
    ) -> MessageRecord:
        """@brief 创建持久化用户消息 / Create a persisted user message.

        @param scope 多租户范围 / Multi-tenant scope.
        @param conversation_id 会话 ID / Conversation ID.
        @param text 用户文本 / User text.
        @param parent_message_id 可选父消息 / Optional parent message.
        @return 新 ChatMessage / New ChatMessage.
        @raise DomainError 会话不存在时抛出 / Raised when the conversation is absent.
        """
        conversation = await self._repository.get_conversation(scope, conversation_id)
        if conversation is None:
            raise DomainError(Problem("agent.conversation_not_found", 404, "Conversation was not found"))
        timestamp = utc_now()
        record = MessageRecord(
            new_opaque_id("msg"),
            conversation_id,
            timestamp,
            timestamp,
            "user",
            "completed",
            [{"part_id": new_opaque_id("part"), "type": "text", "text": text}],
            parent_message_id=parent_message_id,
        )
        await self._repository.create_message(scope, record)
        return record

    async def start_run(
        self,
        scope: ActorScope,
        request: dict[str, Any],
        request_id: str | None,
    ) -> AgentRunRecord:
        """@brief 创建并提交流式 Agent Run / Create and submit a streaming Agent Run.

        @param scope 多租户范围 / Multi-tenant scope.
        @param request 正式 AgentRunRequest / Formal AgentRunRequest.
        @param request_id 请求追踪 ID / Request trace ID.
        @return 初始 Run 记录 / Initial Run record.
        @raise DomainError 会话或输入消息不匹配时抛出 / Raised for mismatched conversation or input message.
        """
        conversation = await self._repository.get_conversation(scope, str(request["conversation_id"]))
        message = await self._repository.get_message(scope, str(request["input_message_id"]))
        if conversation is None or message is None or message.conversation_id != conversation.id:
            raise DomainError(Problem("agent.invalid_input_message", 422, "Input message does not belong to the conversation"))
        timestamp = utc_now()
        run = AgentRunRecord(
            scope,
            new_opaque_id("run"),
            conversation.id,
            message.id,
            timestamp,
            timestamp,
            request,
        )
        await self._repository.create_run(scope, run)
        try:
            task = self._dependencies.supervisor.submit(
                "llm",
                lambda: self._execute_run(scope, run.id, request_id),
                lambda error: self._run_failure(scope, run.id, error),
                name=f"aiws:agent:{run.id}",
            )
            self._remember_run_task(scope, run.id, task)
        except BackpressureError as error:
            run.status = AgentRunStatus.FAILED
            run.problem = Problem("runtime.overloaded", 503, "Agent queue is full", retryable=True)
            await self._repository.save_run(scope, run)
            raise DomainError(run.problem) from error
        return run

    async def get_run(self, scope: ActorScope, run_id: str) -> AgentRunRecord:
        """@brief 获取范围内 Run / Get a scoped Run.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run ID / Run ID.
        @return Run 记录 / Run record.
        @raise DomainError Run 不存在时抛出 / Raised when the Run is absent.
        """
        run = await self._repository.get_run(scope, run_id)
        if run is None:
            raise DomainError(Problem("agent.run_not_found", 404, "Agent run was not found"))
        return run

    async def cancel_run(self, scope: ActorScope, run_id: str) -> AgentRunRecord:
        """@brief 取消 Agent Run / Cancel an Agent Run.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run ID / Run ID.
        @return 更新后的 Run / Updated Run.
        """
        run = await self._mark_run_cancelled(scope, run_id)
        task = self._run_tasks.get(self._run_task_key(scope, run_id))
        if task is not None and not task.done():
            task.cancel()
        return run

    async def decide_tool_approval(
        self,
        scope: ActorScope,
        approval_id: str,
        decision: str,
    ) -> AgentRunRecord:
        """@brief 决定已请求的 mock tool 调用 / Decide a requested mock tool call.

        @param scope 多租户范围 / Multi-tenant scope.
        @param approval_id approval ID / Approval ID.
        @param decision approved 或 rejected / approved or rejected.
        @return 更新后的 Agent Run / Updated Agent Run.
        @raise DomainError approval 不存在、越权或重复决定时抛出 / Raised for unknown, out-of-scope, or repeated decisions.
        """
        indexed = self._approval_index.get(approval_id)
        if indexed is None or indexed[0].workspace_id != scope.workspace_id or indexed[0].resource_owner_id != scope.resource_owner_id:
            raise DomainError(Problem("agent.approval_not_found", 404, "Tool approval was not found"))
        if decision not in {"approved", "rejected"}:
            raise DomainError(Problem("agent.invalid_approval_decision", 422, "Tool approval decision is invalid"))
        run_id = indexed[1]
        async with self._locks.hold(scope, run_id):
            run = await self.get_run(scope, run_id)
            if run.cancelled or self._is_terminal(run):
                raise DomainError(Problem("agent.approval_already_decided", 409, "Tool approval was already decided"))
            approval = run.extensions.get("mock.tool_approval")
            if not isinstance(approval, dict) or approval.get("status") != "pending":
                raise DomainError(Problem("agent.approval_already_decided", 409, "Tool approval was already decided"))
            approval["status"] = decision
            run.status = AgentRunStatus.COMPLETED
            run.phase = "done"
            run.append_event("agent.status", {"phase": "finalizing", "message": None}, None)
            run.append_event(
                "agent.run.completed",
                {"run": run.as_dict(self._stream_url(run.id)), "usage": _public_run_usage(run)},
                None,
            )
            await self._repository.save_run(scope, run)
            return run

    async def stream_events(
        self,
        scope: ActorScope,
        run_id: str,
        last_event_id: str | None,
    ) -> AsyncIterator[dict[str, Any]]:
        """@brief 至少一次回放/流式输出 Agent events / Replay and stream Agent events at-least-once.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run ID / Run ID.
        @param last_event_id 客户端最后已见 ID / Last event ID seen by the client.
        @return 有序 event 异步迭代器 / Ordered async event iterator.
        """
        last_index = -1
        while True:
            run = await self.get_run(scope, run_id)
            if last_event_id is not None and last_index == -1:
                for index, event in enumerate(run.events):
                    if event["event_id"] == last_event_id:
                        last_index = index
                        break
            for event in run.events[last_index + 1 :]:
                yield deepcopy(event)
                last_index += 1
            if run.status in {AgentRunStatus.COMPLETED, AgentRunStatus.CANCELLED, AgentRunStatus.FAILED}:
                return
            await asyncio.sleep(0.025)

    async def _execute_run(
        self,
        scope: ActorScope,
        run_id: str,
        request_id: str | None,
    ) -> None:
        """@brief 在 timeout、取消和有限重试边界内执行 Run / Execute a Run with timeout, cancellation, and bounded retries.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run 稳定 ID；后台任务绝不持有启动时的可变快照 / Stable run ID; workers never retain a mutable start-time snapshot.
        @param request_id 请求追踪 ID / Request trace ID.

        @note 模型流在资源锁外运行。每一个对客户端可见的 delta 都在短锁中重新读取
        Run、更新消息并持久化事件；这样取消命令不能被旧 worker 的最终写回覆盖。
        """
        try:
            initialized = await self._begin_run(scope, run_id, request_id)
            if initialized is None:
                return
            run, input_message, output = initialized
            prompt = _message_plain_text(input_message)
            output_characters = 0
            async with asyncio.timeout(self._latency_budget_ms(run.request) / 1000):
                async for chunk in self._stream_with_retry(prompt, run.request):
                    for delta in _split_agent_delta(chunk):
                        accepted = await self._persist_stream_delta(
                            scope,
                            run_id,
                            output.id,
                            delta,
                            request_id,
                        )
                        if not accepted:
                            return
                        output_characters += len(delta)
            completed = await self._finish_run(
                scope,
                run_id,
                output.id,
                prompt,
                output_characters,
                request_id,
            )
        except asyncio.CancelledError:
            await self._mark_run_cancelled(scope, run_id)
            raise
        if completed:
            live_run = await self.get_run(scope, run_id)
            self._dependencies.telemetry.record(
                "metric",
                "agent.run",
                1,
                scope,
                request_id,
                {"operation": "run", "outcome": "success", "capability": str(live_run.request["capability"])},
                service="backend.worker",
            )

    async def _begin_run(
        self,
        scope: ActorScope,
        run_id: str,
        request_id: str | None,
    ) -> tuple[AgentRunRecord, MessageRecord, MessageRecord] | None:
        """@brief 以新鲜 Run 快照启动流并持久化 started 事件 / Start a stream from a fresh Run snapshot and persist its started event.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run 稳定 ID / Stable run ID.
        @param request_id 请求追踪 ID / Request trace ID.
        @return ``(run, input_message, output_message)``；取消或已启动的 Run 返回 ``None``。
        """
        async with self._locks.hold(scope, run_id):
            run = await self.get_run(scope, run_id)
            if run.cancelled or self._is_terminal(run) or run.status is not AgentRunStatus.QUEUED:
                return None
            input_message = await self._repository.get_message(scope, run.input_message_id)
            if input_message is None:
                raise DomainError(Problem("agent.input_message_not_found", 409, "Input message was not found"))
            output_time = utc_now()
            output = MessageRecord(
                new_opaque_id("msg"),
                run.conversation_id,
                output_time,
                output_time,
                "assistant",
                "streaming",
                [],
                parent_message_id=input_message.id,
                run_id=run.id,
            )
            run.status = AgentRunStatus.RUNNING
            run.phase = "drafting"
            run.output_message_id = output.id
            _set_run_metering(
                run,
                input_utf8_bytes=_utf8_byte_length(_message_plain_text(input_message)),
                output_utf8_bytes=0,
                settings=self._dependencies.ai,
            )
            stream_url = self._stream_url(run.id)
            run.append_event("agent.run.started", {"run": run.as_dict(stream_url)}, request_id)
            await self._repository.create_message(scope, output)
            await self._repository.save_run(scope, run)
            return run, input_message, output

    async def _persist_stream_delta(
        self,
        scope: ActorScope,
        run_id: str,
        output_message_id: str,
        delta: str,
        request_id: str | None,
    ) -> bool:
        """@brief 原子化持久化一个可见 delta / Persist one externally visible delta atomically at application level.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run 稳定 ID / Stable run ID.
        @param output_message_id 输出消息稳定 ID / Stable output message ID.
        @param delta 单个、已受契约大小约束的文本分片 / One contract-bounded text fragment.
        @param request_id 请求追踪 ID / Request trace ID.
        @return 已保存时为 ``True``；若 Run 已取消或终态则为 ``False``。

        @note 锁不覆盖模型网络 I/O。Repository 端口尚未提供跨 Run/message/event 的单事务
        append API，因此这里用最短的应用层临界区防止同进程取消与 worker 写回交叉。
        """
        if not delta:
            return True
        if len(delta) > _MAX_AGENT_DELTA_CHARACTERS:
            raise ValueError("agent delta exceeds its public contract limit")
        async with self._locks.hold(scope, run_id):
            run = await self.get_run(scope, run_id)
            if run.cancelled or self._is_terminal(run):
                return False
            if run.output_message_id != output_message_id:
                return False
            output = await self._repository.get_message(scope, output_message_id)
            if output is None:
                raise RuntimeError("agent output message disappeared while the run was streaming")
            part_id, existing_text = _streaming_text_part(output)
            if len(existing_text) + len(delta) > _MAX_AGENT_OUTPUT_CHARACTERS:
                raise DomainError(
                    Problem("agent.output_too_large", 502, "Model output exceeded the public message limit")
                )
            delta_index = sum(
                event.get("event_type") == "agent.message.delta" for event in run.events
            )
            if delta_index >= _MAX_AGENT_STREAM_DELTAS:
                raise DomainError(
                    Problem("agent.stream_too_fragmented", 502, "Model stream exceeded the event limit")
                )
            output.content = [{"part_id": part_id, "type": "text", "text": existing_text + delta}]
            output.status = "streaming"
            output.updated_at = utc_now()
            _set_run_metering(
                run,
                input_utf8_bytes=_metering_non_negative_int(run.token_usage, "input_utf8_bytes"),
                output_utf8_bytes=_metering_non_negative_int(run.token_usage, "output_utf8_bytes")
                + _utf8_byte_length(delta),
                settings=self._dependencies.ai,
            )
            run.append_event(
                "agent.message.delta",
                {
                    "message_id": output.id,
                    "part_id": part_id,
                    "delta": delta,
                    "index": delta_index,
                },
                request_id,
            )
            await self._repository.create_message(scope, output)
            await self._repository.save_run(scope, run)
            return True

    async def _finish_run(
        self,
        scope: ActorScope,
        run_id: str,
        output_message_id: str,
        prompt: str,
        output_characters: int,
        request_id: str | None,
    ) -> bool:
        """@brief 以新鲜快照完成 Run 或转入 tool approval / Complete a fresh Run snapshot or transition it to tool approval.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run 稳定 ID / Stable run ID.
        @param output_message_id 输出消息稳定 ID / Stable output message ID.
        @param prompt 已授权输入文本 / Authorized input text.
        @param output_characters 已持久化输出字符数 / Persisted output character count.
        @param request_id 请求追踪 ID / Request trace ID.
        @return 正常完成 Run 时为 ``True``；取消、终态或等待 approval 时为 ``False``。
        """
        async with self._locks.hold(scope, run_id):
            run = await self.get_run(scope, run_id)
            if run.cancelled or self._is_terminal(run) or run.output_message_id != output_message_id:
                return False
            output = await self._repository.get_message(scope, output_message_id)
            if output is None:
                raise RuntimeError("agent output message disappeared before finalization")
            output.status = "completed"
            output.updated_at = utc_now()
            _set_run_metering(
                run,
                input_utf8_bytes=_utf8_byte_length(prompt),
                output_utf8_bytes=_utf8_byte_length(_message_plain_text(output)),
                settings=self._dependencies.ai,
            )
            if "structured_json" in run.request.get("output_modes", []):
                run.extensions["mock.structured_output"] = {
                    "schema_version": "mock-v1",
                    "text_length": output_characters,
                }
            extensions = run.request.get("extensions")
            requested_tool = extensions.get("mock.tool_call") if isinstance(extensions, dict) else None
            if isinstance(requested_tool, str) and requested_tool:
                approval_id = new_opaque_id("approval")
                approval = {
                    "approval_id": approval_id,
                    "run_id": run.id,
                    "tool_name": requested_tool,
                    "summary": {
                        "message_key": "agent.mock_tool_call",
                        "fallback_message": "Mock 工具调用需要你的确认。",
                        "params": {},
                    },
                    "risk_level": "medium",
                    "input_preview": {"redacted": True},
                    "expires_at": iso_timestamp(utc_now() + timedelta(minutes=5)),
                    "status": "pending",
                }
                run.extensions["mock.tool_approval"] = approval
                self._approval_index[approval_id] = (scope, run.id)
                run.status = AgentRunStatus.WAITING_FOR_APPROVAL
                run.phase = "applying_tools"
                run.append_event(
                    "agent.tool_approval.required",
                    {"approval": {key: value for key, value in approval.items() if key != "status"}},
                    request_id,
                )
                await self._repository.create_message(scope, output)
                await self._repository.save_run(scope, run)
                return False
            run.status = AgentRunStatus.COMPLETED
            run.phase = "done"
            run.updated_at = utc_now()
            run.append_event("agent.message.completed", {"message": output.as_dict()}, request_id)
            usage = _public_run_usage(run)
            run.append_event(
                "agent.run.completed",
                {"run": run.as_dict(self._stream_url(run.id)), "usage": usage},
                request_id,
            )
            await self._repository.create_message(scope, output)
            await self._repository.save_run(scope, run)
            return True

    async def _mark_run_cancelled(self, scope: ActorScope, run_id: str) -> AgentRunRecord:
        """@brief 持久化不可逆取消状态 / Persist an irreversible cancellation state.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run 稳定 ID / Stable run ID.
        @return 当前持久化后的 Run / The resulting persisted run.
        """
        async with self._locks.hold(scope, run_id):
            run = await self.get_run(scope, run_id)
            if self._is_terminal(run):
                return run
            run.cancelled = True
            run.status = AgentRunStatus.CANCELLED
            run.phase = "done"
            approval = run.extensions.get("mock.tool_approval")
            if isinstance(approval, dict) and approval.get("status") == "pending":
                approval["status"] = "cancelled"
                approval_id = approval.get("approval_id")
                if isinstance(approval_id, str):
                    self._approval_index.pop(approval_id, None)
            await self._set_output_status_locked(scope, run, "cancelled")
            run.append_event(
                "agent.run.completed",
                {"run": run.as_dict(self._stream_url(run.id)), "usage": _public_run_usage(run)},
            )
            await self._repository.save_run(scope, run)
            return run

    async def _set_output_status_locked(
        self,
        scope: ActorScope,
        run: AgentRunRecord,
        status: str,
    ) -> None:
        """@brief 在已持有 Run 锁时结束输出消息 / Finalize an output message while the Run lock is held.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run 已重新读取的 Run 快照 / Freshly read run snapshot.
        @param status 目标消息状态 / Target message status.
        @return 无返回值 / No return value.
        """
        if run.output_message_id is None:
            return
        output = await self._repository.get_message(scope, run.output_message_id)
        if output is None or output.status in {"cancelled", "failed"}:
            return
        output.status = status
        output.updated_at = utc_now()
        await self._repository.create_message(scope, output)

    @staticmethod
    def _is_terminal(run: AgentRunRecord) -> bool:
        """@brief 判断 Run 是否已进入不可逆终态 / Check whether a run is in an irreversible terminal state.

        @param run Run 记录 / Run record.
        @return 已终态时为 ``True`` / ``True`` when terminal.
        """
        return run.status in {
            AgentRunStatus.COMPLETED,
            AgentRunStatus.CANCELLED,
            AgentRunStatus.FAILED,
        }

    @staticmethod
    def _run_task_key(scope: ActorScope, run_id: str) -> tuple[str, str, str]:
        """@brief 构建进程内任务索引键 / Build an in-process task-index key.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run 稳定 ID / Stable run ID.
        @return workspace、owner、run 三元组 / Workspace-owner-run tuple.
        """
        return scope.workspace_id, scope.resource_owner_id, run_id

    def _remember_run_task(
        self,
        scope: ActorScope,
        run_id: str,
        task: asyncio.Task[None],
    ) -> None:
        """@brief 注册本进程受监督任务 / Register a supervised task in this process.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run 稳定 ID / Stable run ID.
        @param task supervisor 创建的后台任务 / Background task created by the supervisor.
        @return 无返回值 / No return value.
        """
        task_key = self._run_task_key(scope, run_id)
        self._run_tasks[task_key] = task
        task.add_done_callback(lambda completed: self._forget_run_task(task_key, completed))

    def _forget_run_task(self, task_key: tuple[str, str, str], completed: asyncio.Task[None]) -> None:
        """@brief 仅移除仍指向该任务的索引 / Remove the task index only when it still points to this task.

        @param task_key 进程内任务索引键 / In-process task-index key.
        @param completed 已完成任务 / Completed task.
        @return 无返回值 / No return value.
        """
        if self._run_tasks.get(task_key) is completed:
            self._run_tasks.pop(task_key, None)

    def _stream_url(self, run_id: str) -> str:
        """@brief 构建公开 SSE URL / Build the public SSE URL.

        @param run_id Run 稳定 ID / Stable run ID.
        @return 公开 SSE URL / Public SSE URL.
        """
        return f"{self._dependencies.network.public_base_url}/api/v1/agent-runs/{run_id}/events"

    @staticmethod
    def _latency_budget_ms(request: dict[str, Any]) -> int:
        """@brief 读取受契约限制的推理时延预算 / Read the contract-bounded inference latency budget.

        @param request 正式 AgentRunRequest / Formal AgentRunRequest.
        @return 100 到 600000 毫秒之间的预算 / Budget between 100 and 600000 milliseconds.
        """
        inference = request.get("inference")
        configured = inference.get("latency_budget_ms") if isinstance(inference, dict) else None
        if isinstance(configured, int) and not isinstance(configured, bool):
            return min(600_000, max(100, configured))
        return 15_000

    async def _stream_with_retry(self, prompt: str, request: dict[str, Any]) -> AsyncIterator[str]:
        """@brief 有界重试的模型流 / Model stream with bounded retries.

        @param prompt 已授权 prompt / Authorized prompt.
        @param request 推理意图 / Inference intent.
        @return 文本分片异步迭代器 / Async iterator of text chunks.
        """
        emitted = False
        for attempt in range(3):
            try:
                async for chunk in self._provider.stream_text(prompt, request):
                    emitted = True
                    yield chunk
                return
            except asyncio.CancelledError:
                raise
            except BaseException:
                if emitted or attempt == 2:
                    raise
                await asyncio.sleep(0.05 * (2**attempt))

    async def _run_failure(self, scope: ActorScope, run_id: str, error: BaseException) -> None:
        """@brief 记录 Agent Run 失败 / Record an Agent Run failure.

        @param scope 多租户范围 / Multi-tenant scope.
        @param run_id Run 稳定 ID / Stable run ID.
        @param error 原始失败 / Raw failure.
        """
        if isinstance(error, asyncio.CancelledError):
            return
        async with self._locks.hold(scope, run_id):
            run = await self.get_run(scope, run_id)
            if run.cancelled or self._is_terminal(run):
                return
            run.status = AgentRunStatus.FAILED
            run.phase = "done"
            run.problem = (
                error.problem
                if isinstance(error, DomainError)
                else Problem("agent.run_failed", 500, "Agent run failed")
            )
            await self._set_output_status_locked(scope, run, "failed")
            run.append_event("agent.run.failed", {"problem": run.problem.as_dict()}, None)
            await self._repository.save_run(scope, run)
        self._dependencies.telemetry.record(
            "metric",
            "agent.run",
            1,
            scope,
            None,
            {"operation": "run", "outcome": "failure", "capability": str(run.request["capability"])},
            service="backend.worker",
        )


def _message_plain_text(message: MessageRecord) -> str:
    """@brief 提取消息文本分片 / Extract text parts from a message.

    @param message ChatMessage 实体 / ChatMessage entity.
    @return 合并后的文本 / Joined text.
    """
    return "".join(str(part.get("text", "")) for part in message.content if part.get("type") == "text")


def _utf8_byte_length(value: str) -> int:
    """@brief 计算文本的 UTF-8 字节数 / Calculate a text value's UTF-8 byte length.

    @param value 待计量文本 / Text to meter.
    @return UTF-8 编码后的字节数 / Number of bytes after UTF-8 encoding.
    """
    return len(value.encode("utf-8"))


def _metering_non_negative_int(usage: dict[str, Any], key: str) -> int:
    """@brief 从已持久化用量中读取非负整数 / Read a non-negative integer from persisted usage.

    @param usage ``AgentRunRecord.token_usage`` 的当前对象。
    @param key 要读取的计数键 / Counter key to read.
    @return 已验证的非负整数；旧/损坏形状回退为零。
    """
    value = usage.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _estimate_tokens_from_utf8_bytes(utf8_bytes: int) -> int:
    """@brief 由累计 UTF-8 字节估算 token 数 / Estimate token count from accumulated UTF-8 bytes.

    @param utf8_bytes 已累计的 UTF-8 字节数 / Accumulated UTF-8 byte count.
    @return 向上取整的本地 token 估算；空文本为零。

    @note 这是一种可复算的 fallback estimator（回退估算器），不是模型 tokenizer 或
    provider 返回的 usage。调用方必须将结果标识为 ``estimated``。
    """
    if utf8_bytes <= 0:
        return 0
    return (utf8_bytes + _METERING_TOKEN_BYTES - 1) // _METERING_TOKEN_BYTES


def _estimated_cost_microusd(token_count: int, rate_microusd_per_million_tokens: int) -> int:
    """@brief 使用配置费率计算四舍五入后的 micro-USD / Calculate rounded micro-USD from a configured rate.

    @param token_count 已估算的 token 数 / Estimated token count.
    @param rate_microusd_per_million_tokens 每百万 token 的 micro-USD 费率。
    @return 就近、半值向上的 micro-USD 成本 / Nearest half-up micro-USD cost.

    @note 全程使用整数，避免浮点货币误差；精度和 rounding policy（舍入策略）会连同
    单价快照一起持久化，不能误当作 provider invoice。
    """
    numerator = token_count * rate_microusd_per_million_tokens
    return (numerator + _METERING_TOKENS_PER_MILLION // 2) // _METERING_TOKENS_PER_MILLION


def _set_run_metering(
    run: AgentRunRecord,
    *,
    input_utf8_bytes: int,
    output_utf8_bytes: int,
    settings: AISettings,
) -> None:
    """@brief 以可审计的本地估算更新 Run token/成本 / Update Run token/cost with an auditable local estimate.

    @param run 待原地更新的 Agent Run。
    @param input_utf8_bytes 已实际提交给本次逻辑请求的输入文本字节数。
    @param output_utf8_bytes 已持久化输出文本的累计字节数。
    @param settings 已验证的 AI 配置，其中包含价格快照来源。
    @return 无返回值。

    @note ``token_usage`` 与 ``cost`` 是 ORM 独立 JSONB 列，不依赖 SSE 事件才得以
    保存；公开 ``AgentRun`` 通过 ``extensions.aiws.metering`` 暴露同一快照。输入仅
    估算应用层可见文本，未把系统提示词、隐式 provider tokenization 或重试账单伪装成
    精确值。
    """
    input_bytes = max(0, input_utf8_bytes)
    output_bytes = max(0, output_utf8_bytes)
    input_tokens = _estimate_tokens_from_utf8_bytes(input_bytes)
    output_tokens = _estimate_tokens_from_utf8_bytes(output_bytes)
    input_rate = settings.metering.input_cost_microusd_per_million_tokens
    output_rate = settings.metering.output_cost_microusd_per_million_tokens
    input_cost = _estimated_cost_microusd(input_tokens, input_rate)
    output_cost = _estimated_cost_microusd(output_tokens, output_rate)
    run.token_usage = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "input_utf8_bytes": input_bytes,
        "output_utf8_bytes": output_bytes,
        "total_utf8_bytes": input_bytes + output_bytes,
        "estimated": True,
        "estimator": _METERING_ESTIMATOR,
    }
    run.cost = {
        "currency": "USD",
        "unit": "microusd",
        "input_cost_microusd": input_cost,
        "output_cost_microusd": output_cost,
        "total_cost_microusd": input_cost + output_cost,
        "estimated": True,
        "pricing": _METERING_PRICING,
        "rounding": "half_up_to_microusd",
        "input_cost_microusd_per_million_tokens": input_rate,
        "output_cost_microusd_per_million_tokens": output_rate,
    }


def _public_run_usage(run: AgentRunRecord) -> dict[str, int]:
    """@brief 构造正式 completed-event 可容纳的 usage 子集 / Build the contract-safe usage subset for a completed event.

    @param run 已具有或尚未具有本地计量快照的 Agent Run。
    @return 仅含正式 ``usage`` schema 允许字段的对象。

    @note 成本没有正式 event 字段，因此完整成本留在随 ``run`` 公开的
    ``extensions.aiws.metering``；不可向 ``usage`` 任意增字段。
    """
    return {
        "input_tokens": _metering_non_negative_int(run.token_usage, "input_tokens"),
        "output_tokens": _metering_non_negative_int(run.token_usage, "output_tokens"),
        "latency_ms": max(0, int((utc_now() - run.created_at).total_seconds() * 1_000)),
    }


def _split_agent_delta(chunk: str) -> Iterator[str]:
    """@brief 将 provider 分片切为契约允许的 delta / Split a provider chunk into contract-valid deltas.

    @param chunk provider 返回的文本分片 / Text fragment returned by the provider.
    @return 至多 100000 字符的 delta 迭代器 / Iterator of deltas with at most 100000 characters.
    @raise TypeError provider 返回非文本分片时抛出 / Raised when a provider yields a non-text fragment.
    """
    if not isinstance(chunk, str):
        raise TypeError("model provider yielded a non-text stream chunk")
    for offset in range(0, len(chunk), _MAX_AGENT_DELTA_CHARACTERS):
        yield chunk[offset : offset + _MAX_AGENT_DELTA_CHARACTERS]


def _streaming_text_part(message: MessageRecord) -> tuple[str, str]:
    """@brief 取得或初始化 streaming 文本 part / Get or initialize a streaming text part.

    @param message 正在流式输出的 assistant 消息 / Streaming assistant message.
    @return ``(part_id, text)`` / ``(part_id, text)``.
    @raise RuntimeError 输出消息的持久化形状被破坏时抛出 / Raised for a corrupt output-message shape.
    """
    if not message.content:
        return new_opaque_id("part"), ""
    if len(message.content) != 1:
        raise RuntimeError("streaming assistant message has more than one content part")
    part = message.content[0]
    part_id = part.get("part_id")
    text = part.get("text")
    if part.get("type") != "text" or not isinstance(part_id, str) or not isinstance(text, str):
        raise RuntimeError("streaming assistant message has an invalid text part")
    return part_id, text


class KnowledgeApplicationService:
    """@brief 知识来源、确定性索引与 deny-priority 检索服务 / Knowledge-source, deterministic-indexing, and deny-priority retrieval service."""

    def __init__(
        self,
        repository: KnowledgeRepository,
        jobs: JobRepository,
        dependencies: ServiceDependencies,
        locks: ScopedKeyLocks,
    ) -> None:
        """@brief 初始化知识库服务 / Initialize the knowledge-base service.

        @param repository 知识 Repository / Knowledge repository.
        @param jobs Job Repository / Job repository.
        @param dependencies 共享运行时依赖 / Shared runtime dependencies.
        @param locks 资源锁 / Resource locks.
        """
        self._repository = repository
        self._jobs = jobs
        self._dependencies = dependencies
        self._locks = locks

    async def synchronize_resume(
        self,
        scope: ActorScope,
        document: dict[str, Any],
        request_id: str | None,
    ) -> None:
        """@brief 将已提交 Resume revision 派生为同租户知识来源 / Derive a submitted Resume revision into a same-tenant knowledge source.

        @param scope 多租户范围 / Multi-tenant scope.
        @param document 已持久化 ResumeDocument SIR 快照 / Persisted ResumeDocument SIR snapshot.
        @param request_id 可选请求追踪 ID / Optional request trace ID.
        @return 无返回值 / No return value.
        @raise DomainError 既有 source ID 指向非本 Resume 的来源时抛出 / Raised when an existing source ID belongs to another resource.

        @note 这是内部派生行为，不新增 HTTP 路由或 DTO（Data Transfer Object，数据传输
        对象）。``revision_mode=latest`` 的 source 在每次 Resume revision 后重新排队；
        旧 job 会根据 source revision 自行判定 superseded（已被替代），不能覆盖新内容。
        """
        resume_id = document.get("id")
        if not isinstance(resume_id, str) or not resume_id:
            raise DomainError(Problem("resume.invalid_document", 422, "Resume ID is invalid"))
        source_id = _resume_knowledge_source_id(document, resume_id)
        timestamp = utc_now()
        config = _resume_source_config(resume_id)
        content = _resume_source_content(document)
        name = _resume_source_name(document)
        async with self._locks.hold(scope, source_id):
            existing = await self._repository.get_source(scope, source_id)
            if existing is None:
                source = KnowledgeSourceRecord(
                    scope=scope,
                    id=source_id,
                    created_at=timestamp,
                    updated_at=timestamp,
                    name=name,
                    source_type="resume",
                    config=config,
                    visibility=_default_visibility(),
                    mock_content=content,
                )
                await self._repository.create_source(scope, source)
            else:
                _validate_resume_source(existing, resume_id)
                source = existing
                source.name = name
                source.config = config
                source.mock_content = content
                source.ingestion_status = "stale" if source.source_version_id is not None else "not_started"
                source.updated_at = timestamp
                source.revision += 1
                await self._repository.save_source(scope, source)
            await self._create_ingestion_job_locked(
                scope,
                source,
                request_id,
                raise_on_backpressure=False,
            )
        self._dependencies.telemetry.record(
            "metric",
            "resume.knowledge_source",
            1,
            scope,
            request_id,
            {"operation": "synchronize", "outcome": "accepted", "job_type": "knowledge.ingest"},
            service="backend.worker",
        )

    async def create_mock_source(
        self,
        scope: ActorScope,
        name: str,
        source_type: str,
        content: str,
        location: str | None,
        visibility: dict[str, Any] | None,
    ) -> KnowledgeSourceRecord:
        """@brief 创建明确标记为 mock 的来源适配器 / Create an explicitly mock source adapter.

        @param scope 多租户范围 / Multi-tenant scope.
        @param name 来源名称 / Source name.
        @param source_type manual_note、url 或 git_repository / manual_note, url, or git_repository.
        @param content 确定性 mock 解析内容 / Deterministic mock-parsed content.
        @param location URL 或 repository URL / URL or repository URL.
        @param visibility 可选可见性策略 / Optional visibility policy.
        @return 知识来源聚合 / Knowledge-source aggregate.
        @raise DomainError source type 或位置无效时抛出 / Raised for an invalid source type or location.
        """
        timestamp = utc_now()
        source_id = new_opaque_id("src")
        policy = deepcopy(visibility) if visibility is not None else _default_visibility()
        config = _mock_source_config(source_type, name, content, location)
        record = KnowledgeSourceRecord(
            scope=scope,
            id=source_id,
            created_at=timestamp,
            updated_at=timestamp,
            name=name,
            source_type=source_type,
            config=config,
            visibility=policy,
            mock_content=content,
        )
        await self._repository.create_source(scope, record)
        return record

    async def list_sources(self, scope: ActorScope) -> list[KnowledgeSourceRecord]:
        """@brief 列出范围内知识来源 / List scoped knowledge sources.

        @param scope 多租户范围 / Multi-tenant scope.
        @return 来源聚合列表 / Source aggregate list.
        """
        return await self._repository.list_sources(scope)

    async def get_source(self, scope: ActorScope, source_id: str) -> KnowledgeSourceRecord:
        """@brief 获取范围内知识来源 / Get a scoped knowledge source.

        @param scope 多租户范围 / Multi-tenant scope.
        @param source_id 来源 ID / Source ID.
        @return 来源聚合 / Source aggregate.
        @raise DomainError 来源不存在时抛出 / Raised when the source is absent.
        """
        source = await self._repository.get_source(scope, source_id)
        if source is None:
            raise DomainError(Problem("knowledge.source_not_found", 404, "Knowledge source was not found"))
        return source

    async def create_ingestion_job(
        self,
        scope: ActorScope,
        source_id: str,
        request_id: str | None,
    ) -> dict[str, Any]:
        """@brief 创建受控知识索引 Job / Create a controlled knowledge-indexing Job.

        @param scope 多租户范围 / Multi-tenant scope.
        @param source_id 来源 ID / Source ID.
        @param request_id 请求追踪 ID / Request trace ID.
        @return KnowledgeIngestionJob 初始表示 / Initial KnowledgeIngestionJob representation.
        """
        async with self._locks.hold(scope, source_id):
            source = await self.get_source(scope, source_id)
            return await self._create_ingestion_job_locked(
                scope,
                source,
                request_id,
                raise_on_backpressure=True,
            )

    async def _create_ingestion_job_locked(
        self,
        scope: ActorScope,
        source: KnowledgeSourceRecord,
        request_id: str | None,
        *,
        raise_on_backpressure: bool,
    ) -> dict[str, Any]:
        """@brief 在来源锁内创建知识索引 Job / Create a knowledge-ingestion Job under the source lock.

        @param scope 多租户范围 / Multi-tenant scope.
        @param source 已锁定的知识来源 / Locked knowledge source.
        @param request_id 请求追踪 ID / Request trace ID.
        @param raise_on_backpressure 直接 mock API 为真；Resume 派生同步为假 / True for the direct mock API and false for resume-derived sync.
        @return 已排队或已失败的 KnowledgeIngestionJob / Queued or failed KnowledgeIngestionJob.
        @raise DomainError 队列满且调用方要求直接报告过载时抛出 / Raised for a full queue when the caller wants a direct overload response.

        @note source revision 在入队时固化。异步 worker 仅在该 revision 仍为最新时写入
        chunk/embedding，因而旧 job 无法把较早的 Resume revision 覆盖回来。
        """
        source.ingestion_status = "queued"
        source.updated_at = utc_now()
        source.revision += 1
        await self._repository.save_source(scope, source)
        expected_source_revision = source.revision
        job = Job(
            new_opaque_id("job"),
            "knowledge.ingest",
            utc_now(),
            request_id,
            extensions={
                "source_id": source.id,
                "source_version_id": None,
                "source_revision": expected_source_revision,
                "stats": _zero_ingestion_stats(),
            },
        )
        await self._jobs.create_job(scope, job)
        try:
            self._dependencies.supervisor.submit(
                "knowledge",
                lambda: self._ingest(scope, source.id, expected_source_revision, job),
                lambda error: self._ingestion_failure(
                    scope,
                    source.id,
                    expected_source_revision,
                    job,
                    error,
                ),
                name=f"aiws:knowledge:{job.id}",
            )
        except BackpressureError as error:
            problem = Problem("runtime.overloaded", 503, "Knowledge queue is full", retryable=True)
            job.fail(problem)
            source.ingestion_status = "failed"
            source.updated_at = utc_now()
            source.revision += 1
            await self._repository.save_source(scope, source)
            await self._jobs.save_job(scope, job)
            if raise_on_backpressure:
                raise DomainError(problem) from error
        return self._ingestion_job_dict(job)

    async def get_ingestion_job(self, scope: ActorScope, job_id: str) -> dict[str, Any]:
        """@brief 获取知识索引 Job / Get a knowledge-indexing Job.

        @param scope 多租户范围 / Multi-tenant scope.
        @param job_id Job ID / Job ID.
        @return KnowledgeIngestionJob / KnowledgeIngestionJob.
        @raise DomainError Job 不存在或类型错误时抛出 / Raised when the job is absent or mismatched.
        """
        job = await self._jobs.get_job(scope, job_id)
        if job is None or not job.job_type.startswith("knowledge."):
            raise DomainError(Problem("knowledge.ingestion_job_not_found", 404, "Knowledge ingestion job was not found"))
        return self._ingestion_job_dict(job)

    async def search(self, scope: ActorScope, request: dict[str, Any]) -> list[dict[str, Any]]:
        """@brief 按 deny-priority 策略检索知识 / Retrieve knowledge using deny-priority policy.

        @param scope 多租户范围 / Multi-tenant scope.
        @param request 正式 KnowledgeSearchRequest / Formal KnowledgeSearchRequest.
        @return KnowledgeSearchResult 列表 / List of KnowledgeSearchResult.
        """
        selection = request["selection"]
        if selection.get("mode") == "none":
            if selection.get("include_source_ids") or selection.get("exclude_source_ids"):
                raise DomainError(
                    Problem(
                        "knowledge.invalid_selection",
                        422,
                        "A none knowledge selection cannot name sources",
                    )
                )
            return []
        candidates = await self._repository.list_sources(scope)
        source_filter = set(selection.get("include_source_ids", []))
        excluded = set(selection.get("exclude_source_ids", []))
        if selection.get("mode") == "explicit":
            candidates = [source for source in candidates if source.id in source_filter]
        elif source_filter:
            candidates = [source for source in candidates if source.id in source_filter]
        query_tokens = set(str(request["query"]).lower().split())
        agent_scope = str(selection.get("agent_scope", "general_chat"))
        results: list[tuple[float, KnowledgeSourceRecord, KnowledgeChunk]] = []
        for source in candidates:
            if (
                source.id in excluded
                or not source.enabled
                or source.ingestion_status != "ready"
                or not _is_allowed(source.visibility, agent_scope, "retrieve")
            ):
                continue
            for chunk in source.chunks:
                score = _lexical_score(query_tokens, chunk.text)
                if score > 0:
                    results.append((score, source, chunk))
        results.sort(key=lambda item: item[0], reverse=True)
        include_quotes = bool(request["include_quotes"])
        return [
            _search_result(score, source, chunk, include_quotes)
            for score, source, chunk in results[: int(request["top_k"])]
        ]

    async def _ingest(
        self,
        scope: ActorScope,
        source_id: str,
        expected_source_revision: int,
        job: Job,
    ) -> None:
        """@brief 运行最新 revision 的确定性解析、chunk 和 embedding / Run deterministic parsing, chunking, and embedding for the latest revision.

        @param scope 多租户范围 / Multi-tenant scope.
        @param source_id 知识来源稳定 ID / Stable knowledge-source ID.
        @param expected_source_revision 入队时固化的来源 revision / Source revision captured at enqueue time.
        @param job Job 实体 / Job entity.

        @note worker 在来源锁内重新读取聚合。若后来 Resume revision 已经替代它，job 被标记
        成功但统计为 ``skipped=1``，且绝不修改来源的当前 chunk/version。
        """
        async with self._locks.hold(scope, source_id):
            job.start()
            await self._jobs.save_job(scope, job)
            source = await self.get_source(scope, source_id)
            if source.revision != expected_source_revision:
                job.extensions["stats"] = {
                    "documents": 0,
                    "chunks": 0,
                    "embedded_tokens": 0,
                    "skipped": 1,
                }
                job.completed_units = 0
                job.total_units = 1
                job.succeed()
                await self._jobs.save_job(scope, job)
                self._dependencies.telemetry.record(
                    "metric",
                    "knowledge.ingest",
                    1,
                    scope,
                    job.request_id,
                    {"operation": "ingest", "outcome": "superseded", "job_type": "knowledge.ingest"},
                    service="backend.worker",
                )
                return
            job.phase = "indexing"
            source.ingestion_status = "chunking"
            await self._repository.save_source(scope, source)
            space = await self._repository.get_embedding_space(scope)
            if space is None:
                space = EmbeddingSpace(
                    new_opaque_id("embsp"),
                    self._dependencies.ai.embedding_provider,
                    self._dependencies.ai.embedding_model,
                    self._dependencies.ai.embedding_model_revision,
                    self._dependencies.ai.embedding_dimension,
                    self._dependencies.ai.embedding_distance_metric,
                    self._dependencies.ai.embedding_normalization,
                    utc_now(),
                )
                await self._repository.save_embedding_space(scope, space)
            if space.dimension != self._dependencies.ai.embedding_dimension:
                raise DomainError(
                    Problem(
                        "knowledge.embedding_space_mismatch",
                        409,
                        "Embedding dimension change requires a data migration",
                    )
                )
            source_version_id = new_opaque_id("srcver")
            parts = _chunk_text(source.mock_content)
            source.chunks = [
                KnowledgeChunk(
                    new_opaque_id("chunk"),
                    source.id,
                    source_version_id,
                    space.id,
                    index,
                    text,
                    _deterministic_vector(text, space.dimension),
                )
                for index, text in enumerate(parts)
            ]
            source.source_version_id = source_version_id
            source.ingestion_status = "ready"
            source.updated_at = utc_now()
            source.revision += 1
            job.extensions["source_version_id"] = source_version_id
            job.extensions["stats"] = {
                "documents": 1,
                "chunks": len(source.chunks),
                "embedded_tokens": sum(len(chunk.text.split()) for chunk in source.chunks),
                "skipped": 0,
            }
            job.completed_units = len(source.chunks)
            job.total_units = len(source.chunks) or 1
            job.succeed()
            await self._repository.save_source(scope, source)
            await self._jobs.save_job(scope, job)
            self._dependencies.telemetry.record(
                "metric",
                "knowledge.ingest",
                1,
                scope,
                job.request_id,
                {"operation": "ingest", "outcome": "success", "job_type": "knowledge.ingest"},
                service="backend.worker",
            )

    async def _ingestion_failure(
        self,
        scope: ActorScope,
        source_id: str,
        expected_source_revision: int,
        job: Job,
        error: BaseException,
    ) -> None:
        """@brief 记录知识索引失败 / Record knowledge-indexing failure.

        @param scope 多租户范围 / Multi-tenant scope.
        @param source_id 知识来源稳定 ID / Stable knowledge-source ID.
        @param expected_source_revision 入队时固化的来源 revision / Source revision captured at enqueue time.
        @param job Job 实体 / Job entity.
        @param error 原始失败 / Raw failure.
        """
        problem = (
            error.problem
            if isinstance(error, DomainError)
            else Problem("knowledge.ingest_failed", 500, "Knowledge ingestion failed")
        )
        job.fail(problem)
        await self._jobs.save_job(scope, job)
        async with self._locks.hold(scope, source_id):
            source = await self._repository.get_source(scope, source_id)
            if source is not None and source.revision == expected_source_revision:
                source.ingestion_status = "failed"
                source.updated_at = utc_now()
                source.revision += 1
                await self._repository.save_source(scope, source)
        self._dependencies.telemetry.record(
            "metric",
            "knowledge.ingest",
            1,
            scope,
            job.request_id,
            {"operation": "ingest", "outcome": "failure", "job_type": "knowledge.ingest"},
            service="backend.worker",
        )

    @staticmethod
    def _ingestion_job_dict(job: Job) -> dict[str, Any]:
        """@brief 构建 KnowledgeIngestionJob 视图 / Build a KnowledgeIngestionJob view.

        @param job 基础 Job / Base job.
        @return KnowledgeIngestionJob / KnowledgeIngestionJob.
        """
        payload = job.as_dict()
        payload.update(
            {
                "source_id": job.extensions["source_id"],
                "source_version_id": job.extensions["source_version_id"],
                "stats": job.extensions["stats"],
            }
        )
        return payload


class InterviewApplicationService:
    """@brief 面试 Session、实时控制和报告应用服务 / Application service for interview sessions, realtime control, and reports."""

    def __init__(
        self,
        repository: InterviewRepository,
        jobs: JobRepository,
        dependencies: ServiceDependencies,
        locks: ScopedKeyLocks,
    ) -> None:
        """@brief 初始化面试服务 / Initialize the interview service.

        @param repository 面试 Repository / Interview repository.
        @param jobs Job Repository / Job repository.
        @param dependencies 共享运行时依赖 / Shared runtime dependencies.
        @param locks 资源锁 / Resource locks.
        """
        self._repository = repository
        self._jobs = jobs
        self._dependencies = dependencies
        self._locks = locks

    async def create_session(self, scope: ActorScope, request: dict[str, Any]) -> InterviewSessionRecord:
        """@brief 创建面试 Session，但不占用媒体连接 / Create an interview Session without reserving media.

        @param scope 多租户范围 / Multi-tenant scope.
        @param request 正式 InterviewSessionCreateRequest / Formal InterviewSessionCreateRequest.
        @return 新 Session / New Session.
        @raise DomainError workspace 或录制同意不合法时抛出 / Raised for invalid workspace or recording consent.
        """
        if request["workspace_id"] != scope.workspace_id:
            raise DomainError(Problem("interview.workspace_mismatch", 403, "Interview workspace is outside the actor scope"))
        recording = request["recording"]
        if (recording.get("record_audio") or recording.get("record_video")) and (
            not recording.get("user_consent_at") or not recording.get("consent_version")
        ):
            raise DomainError(
                Problem("interview.recording_consent_required", 422, "Recording requires explicit consent")
            )
        timestamp = utc_now()
        record = InterviewSessionRecord(scope, new_opaque_id("int"), timestamp, timestamp, deepcopy(request))
        await self._repository.create_session(scope, record)
        return record

    async def get_session(self, scope: ActorScope, session_id: str) -> InterviewSessionRecord:
        """@brief 获取范围内面试 Session / Get a scoped interview Session.

        @param scope 多租户范围 / Multi-tenant scope.
        @param session_id Session ID / Session ID.
        @return Session 记录 / Session record.
        @raise DomainError Session 不存在时抛出 / Raised when the Session is absent.
        """
        session = await self._repository.get_session(scope, session_id)
        if session is None:
            raise DomainError(Problem("interview.session_not_found", 404, "Interview session was not found"))
        return session

    async def create_connection(self, scope: ActorScope, session_id: str, request_id: str | None) -> dict[str, Any]:
        """@brief 创建短期 mock realtime 连接描述 / Create a short-lived mock realtime connection descriptor.

        @param scope 多租户范围 / Multi-tenant scope.
        @param session_id Session ID / Session ID.
        @param request_id 请求追踪 ID / Request trace ID.
        @return RealtimeConnectionDescriptor / RealtimeConnectionDescriptor.
        """
        async with self._locks.hold(scope, session_id):
            session = await self.get_session(scope, session_id)
            if session.status is InterviewStatus.CREATED:
                session.transition(InterviewStatus.PREPARING)
                session.transition(InterviewStatus.READY)
            if session.status is not InterviewStatus.READY:
                raise DomainError(Problem("interview.invalid_state", 409, "Interview is not ready for a connection"))
            session.append_event("interview.session.state", {"status": session.status.value, "reason": None}, request_id)
            await self._repository.save_session(scope, session)
            token = secrets.token_urlsafe(32)
            resume_token = secrets.token_urlsafe(32)
            base_url = self._dependencies.network.public_base_url
            return {
                "session_id": session.id,
                "protocol_version": "1.0",
                "ephemeral_token": token,
                "expires_at": iso_timestamp(utc_now() + timedelta(minutes=5)),
                "signaling_url": f"{base_url}/api/v1/interview-sessions/{session.id}/realtime",
                "event_stream_url": None,
                "ice_servers": [],
                "webrtc": {
                    "enabled": False,
                    "data_channel_label": "aiws-control-v1",
                    "bundle_policy": "max-bundle",
                    "expected_uplink_tracks": ["audio", "video"],
                    "expected_downlink_tracks": ["audio"],
                },
                "fallback": {
                    "websocket_url": f"{base_url.replace('http', 'ws', 1)}/api/v1/interview-sessions/{session.id}/realtime",
                    "binary_frame_protocol": "aiws-media-v1",
                    "max_frame_bytes": 1048576,
                },
                "resume_token": resume_token,
            }

    async def handle_realtime_event(
        self,
        scope: ActorScope,
        session_id: str,
        event: dict[str, Any],
        request_id: str | None,
    ) -> list[dict[str, Any]]:
        """@brief 处理一个已验证的实时控制事件 / Handle one validated realtime control event.

        @param scope 多租户范围 / Multi-tenant scope.
        @param session_id Session ID / Session ID.
        @param event 正式 InterviewRealtimeEvent / Formal InterviewRealtimeEvent.
        @param request_id 请求追踪 ID / Request trace ID.
        @return 服务端待发送事件 / Server events to send.
        """
        async with self._locks.hold(scope, session_id):
            session = await self.get_session(scope, session_id)
            event_type = event.get("event_type")
            responses: list[dict[str, Any]] = []
            if event_type == "interview.client.ready":
                if session.status is InterviewStatus.READY:
                    session.transition(InterviewStatus.CONNECTING)
                    session.transition(InterviewStatus.IN_PROGRESS)
                responses.append(session.append_event("interview.session.state", {"status": session.status.value, "reason": None}, request_id))
            elif event_type == "interview.user.interrupt":
                if session.status is not InterviewStatus.IN_PROGRESS:
                    raise DomainError(Problem("interview.invalid_state", 409, "User interrupt requires an active interview"))
                responses.append(
                    session.append_event(
                        "interview.warning",
                        {
                            "code": "interview.user_interrupted",
                            "message": {
                                "message_key": "interview.user_interrupted",
                                "fallback_message": "数字人输出已因用户打断而取消。",
                                "params": {},
                            },
                            "recoverable": True,
                        },
                        request_id,
                    )
                )
            elif event_type == "interview.session.end_requested":
                await self._end_locked(scope, session, request_id)
                responses.append(session.append_event("interview.session.state", {"status": session.status.value, "reason": None}, request_id))
            elif event_type == "interview.ping":
                ping_payload = event.get("payload", {})
                responses.append(
                    session.append_event(
                        "interview.pong",
                        {"nonce": ping_payload.get("nonce", "unknown"), "sent_at": ping_payload.get("sent_at", iso_timestamp(utc_now()))},
                        request_id,
                    )
                )
            else:
                raise DomainError(Problem("interview.unsupported_event", 422, "Realtime event is unsupported"))
            await self._repository.save_session(scope, session)
            return responses

    async def end_session(self, scope: ActorScope, session_id: str, request_id: str | None) -> dict[str, Any]:
        """@brief 正常结束 Session 并创建报告 Job / End a Session and create a report Job.

        @param scope 多租户范围 / Multi-tenant scope.
        @param session_id Session ID / Session ID.
        @param request_id 请求追踪 ID / Request trace ID.
        @return 通用 Job 视图 / Generic Job view.
        """
        async with self._locks.hold(scope, session_id):
            session = await self.get_session(scope, session_id)
            job = await self._end_locked(scope, session, request_id)
            await self._repository.save_session(scope, session)
            return job.as_dict()

    async def get_report(self, scope: ActorScope, report_id: str) -> dict[str, Any]:
        """@brief 获取范围内面试报告 / Get a scoped interview report.

        @param scope 多租户范围 / Multi-tenant scope.
        @param report_id 报告 ID / Report ID.
        @return InterviewReport / InterviewReport.
        @raise DomainError 报告不存在时抛出 / Raised when the report is absent.
        """
        report = await self._repository.get_report(scope, report_id)
        if report is None:
            raise DomainError(Problem("interview.report_not_found", 404, "Interview report was not found"))
        return report

    async def _end_locked(self, scope: ActorScope, session: InterviewSessionRecord, request_id: str | None) -> Job:
        """@brief 在持锁状态下结束会话 / End a session while holding its lock.

        @param scope 多租户范围 / Multi-tenant scope.
        @param session Session 记录 / Session record.
        @param request_id 请求追踪 ID / Request trace ID.
        @return 报告 Job / Report job.
        @raise DomainError 状态机不允许时抛出 / Raised for an invalid state transition.
        """
        if session.status is not InterviewStatus.IN_PROGRESS:
            raise DomainError(Problem("interview.invalid_state", 409, "Only an active interview can be ended"))
        session.transition(InterviewStatus.ENDING)
        session.transition(InterviewStatus.PROCESSING_REPORT)
        job = Job(new_opaque_id("job"), "interview.report", utc_now(), request_id, extensions={"session_id": session.id})
        await self._jobs.create_job(scope, job)
        try:
            self._dependencies.supervisor.submit(
                "interview",
                lambda: self._build_report(scope, session, job),
                lambda error: self._report_failure(scope, job, error),
                name=f"aiws:interview-report:{job.id}",
            )
        except BackpressureError as error:
            problem = Problem("runtime.overloaded", 503, "Interview queue is full", retryable=True)
            job.fail(problem)
            await self._jobs.save_job(scope, job)
            raise DomainError(problem) from error
        return job

    async def _build_report(self, scope: ActorScope, session: InterviewSessionRecord, job: Job) -> None:
        """@brief 构建确定性且只基于可观察内容的报告 / Build a deterministic report based only on observable content.

        @param scope 多租户范围 / Multi-tenant scope.
        @param session Session 记录 / Session record.
        @param job 报告 Job / Report job.
        """
        job.start()
        await self._jobs.save_job(scope, job)
        report_id = new_opaque_id("rpt")
        report = _mock_report(report_id, session.id)
        await self._repository.save_report(scope, report)
        session.report_id = report_id
        session.transition(InterviewStatus.COMPLETED)
        job.completed_units = 1
        job.total_units = 1
        job.succeed()
        await self._repository.save_session(scope, session)
        await self._jobs.save_job(scope, job)
        self._dependencies.telemetry.record(
            "metric",
            "interview.report",
            1,
            scope,
            job.request_id,
            {"operation": "report", "outcome": "success", "job_type": "interview.report"},
            service="backend.worker",
        )

    async def _report_failure(self, scope: ActorScope, job: Job, error: BaseException) -> None:
        """@brief 记录报告生成失败 / Record report-generation failure.

        @param scope 多租户范围 / Multi-tenant scope.
        @param job Job 实体 / Job entity.
        @param error 原始失败 / Raw failure.
        """
        job.fail(error.problem if isinstance(error, DomainError) else Problem("interview.report_failed", 500, "Interview report generation failed"))
        await self._jobs.save_job(scope, job)
        self._dependencies.telemetry.record(
            "metric",
            "interview.report",
            1,
            scope,
            job.request_id,
            {"operation": "report", "outcome": "failure", "job_type": "interview.report"},
            service="backend.worker",
        )


def _default_visibility() -> dict[str, Any]:
    """@brief 创建 deny-default 可见性策略 / Create a deny-default visibility policy.

    @return 默认拒绝策略 / Default-deny policy.
    """
    return {
        "policy_version": 1,
        "default_effect": "deny",
        "sensitivity": "confidential",
        "agent_grants": [
            {"agent_scope": "resume_assistant", "effect": "allow", "allowed_operations": ["retrieve", "summarize", "derive"]},
            {"agent_scope": "interview_agent", "effect": "allow", "allowed_operations": ["retrieve", "summarize", "derive"]},
            {"agent_scope": "general_chat", "effect": "allow", "allowed_operations": ["retrieve", "summarize"]},
        ],
        "session_override_allowed": True,
        "allow_external_model_processing": False,
        "allowed_model_regions": ["cn", "private_deployment"],
        "retention_days": None,
    }


def _resume_knowledge_source_id(document: dict[str, Any], resume_id: str) -> str:
    """@brief 取得或回填兼容的 Resume 派生 source ID / Get a Resume-derived source ID with a backward-compatible fallback.

    @param document ResumeDocument SIR / ResumeDocument SIR.
    @param resume_id 简历稳定 ID / Stable resume ID.
    @return 同一 Resume 在同一实现版本中稳定的知识来源 ID / Stable knowledge-source ID for this Resume and implementation version.

    @note 新建 Resume 会把随机 source ID 写入既有的 ``knowledge_source_id`` 字段。旧
    Resume 的历史 revision 可以合法地没有该可选字段，因此回退为加盐 SHA-256（Secure
    Hash Algorithm 256）值；不改写历史快照，也不会让一个旧 Resume 反复产生多个来源。
    """
    source_id = document.get("knowledge_source_id")
    if isinstance(source_id, str) and source_id:
        return source_id
    digest = hashlib.sha256(f"aiws:resume-source:v1:{resume_id}".encode()).hexdigest()
    return f"src_{digest}"


def _resume_source_config(resume_id: str) -> dict[str, Any]:
    """@brief 构造正式 ResumeSourceConfig / Build the formal ResumeSourceConfig.

    @param resume_id 简历稳定 ID / Stable resume ID.
    @return 不含私有运行时字段的正式 source config / Formal source config without private runtime fields.
    """
    return {
        "source_type": "resume",
        "resume_id": resume_id,
        "revision_mode": "latest",
    }


def _resume_source_name(document: dict[str, Any]) -> str:
    """@brief 构造受长度约束的 Resume 知识来源标题 / Build a length-bounded Resume knowledge-source title.

    @param document ResumeDocument SIR / ResumeDocument SIR.
    @return 用户可见但不含隐私扩展字段的来源标题 / User-visible source title without private extension fields.
    """
    title = document.get("title")
    normalized_title = title.strip() if isinstance(title, str) else "Untitled resume"
    return f"Resume: {normalized_title or 'Untitled resume'}"[:300]


def _resume_source_content(document: dict[str, Any]) -> str:
    """@brief 从 Resume SIR 提取有界、可检索的纯文本 / Extract bounded, searchable plain text from a Resume SIR.

    @param document ResumeDocument SIR / ResumeDocument SIR.
    @return 不含 ID、时间戳、样式或 extension 的稳定纯文本 / Stable plain text excluding IDs, timestamps, styles, and extensions.

    @note 这不是新的 Resume serialization 格式，也不替代正式 parser。v0.1 的
    deterministic mock ingestion 只消费用户可读的 title/profile/sections，避免把
    opaque ID、内部 style token 或未知 extension 原样纳入检索语料。
    """
    fragments: list[str] = []
    remaining = _MAX_RESUME_SOURCE_CHARACTERS

    def append_text(value: str) -> None:
        """@brief 追加一个受剩余配额约束的文本片段 / Append one text fragment within the remaining budget.

        @param value 待规范化文本 / Text to normalize.
        @return 无返回值 / No return value.
        """
        nonlocal remaining
        if remaining <= 0:
            return
        normalized = " ".join(value.split())
        if not normalized:
            return
        fragment = normalized[:remaining]
        fragments.append(fragment)
        remaining -= len(fragment)

    def visit(value: object) -> None:
        """@brief 深度优先收集用户可读字符串 / Collect user-readable strings in depth-first order.

        @param value SIR 子树 / SIR subtree.
        @return 无返回值 / No return value.
        """
        if remaining <= 0:
            return
        if isinstance(value, str):
            append_text(value)
            return
        if isinstance(value, list):
            for item in value:
                visit(item)
                if remaining <= 0:
                    return
            return
        if not isinstance(value, dict):
            return
        plain_text = value.get("plain_text")
        if isinstance(plain_text, str):
            append_text(plain_text)
            return
        for key in sorted(value):
            if key in {
                "block_id",
                "created_at",
                "extensions",
                "id",
                "item_id",
                "knowledge_source_id",
                "revision",
                "schema_version",
                "section_id",
                "style_intent",
                "template",
                "updated_at",
                "workspace_id",
            }:
                continue
            visit(value[key])
            if remaining <= 0:
                return

    visit(document.get("title"))
    visit(document.get("profile"))
    visit(document.get("sections"))
    return "\n".join(fragments)


def _validate_resume_source(source: KnowledgeSourceRecord, resume_id: str) -> None:
    """@brief 验证 source ID 没有被重用给另一资源 / Verify that a source ID was not reused for another resource.

    @param source 已读取的知识来源 / Loaded knowledge source.
    @param resume_id 当前 Resume 稳定 ID / Current stable Resume ID.
    @return 无返回值 / No return value.
    @raise DomainError source 不是此 Resume 的派生来源时抛出 / Raised when the source is not derived from this Resume.
    """
    if source.source_type == "resume" and source.config.get("resume_id") == resume_id:
        return
    raise DomainError(
        Problem(
            "knowledge.resume_source_conflict",
            409,
            "Resume knowledge source is bound to another resource",
        )
    )


def _mock_source_config(source_type: str, name: str, content: str, location: str | None) -> dict[str, Any]:
    """@brief 构造合法但明确 mock 的来源 config / Build a valid but explicitly mock source config.

    @param source_type 来源类型 / Source type.
    @param name 来源名称 / Source name.
    @param content mock 内容 / Mock content.
    @param location URL 或 repo URL / URL or repo URL.
    @return 正式 KnowledgeSourceConfig 形状 / Formal KnowledgeSourceConfig shape.
    @raise DomainError 不能安全构造时抛出 / Raised when it cannot be safely built.
    """
    if source_type == "manual_note":
        return {"source_type": "manual_note", "title": name, "content": _rich_text(content)}
    if source_type in {"url", "website", "blog_feed"} and location:
        return {"source_type": source_type, "url": location, "crawl_depth": 0, "max_pages": 1, "include_patterns": [], "exclude_patterns": [], "connection_id": None}
    if source_type == "git_repository" and location:
        return {"source_type": "git_repository", "repository_url": location, "default_branch": None, "ref": None, "include_globs": [], "exclude_globs": [], "include_history": False, "connection_id": None}
    raise DomainError(Problem("knowledge.mock_source_unsupported", 422, "Mock source type needs valid location"))


def _zero_ingestion_stats() -> dict[str, int]:
    """@brief 创建零值索引统计 / Create zero ingestion statistics.

    @return 零值统计 / Zero-valued statistics.
    """
    return {"documents": 0, "chunks": 0, "embedded_tokens": 0, "skipped": 0}


def _chunk_text(content: str, max_characters: int = 800) -> list[str]:
    """@brief 稳定切分文本 / Split text deterministically.

    @param content 已解析纯文本 / Parsed plain text.
    @param max_characters 单 chunk 最大字符数 / Maximum characters per chunk.
    @return 非空 chunk 列表 / Non-empty chunk list.
    """
    normalized = content.strip()
    if not normalized:
        return [""]
    return [normalized[offset : offset + max_characters] for offset in range(0, len(normalized), max_characters)]


def _deterministic_vector(text: str, dimension: int) -> tuple[float, ...]:
    """@brief 生成确定性 mock embedding / Generate a deterministic mock embedding.

    @param text 待嵌入文本 / Text to embed.
    @param dimension embedding 维度 / Embedding dimension.
    @return L2 归一化向量 / L2-normalized vector.
    """
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values = [((digest[index % len(digest)] / 255.0) * 2 - 1) for index in range(dimension)]
    norm = sum(value * value for value in values) ** 0.5
    return tuple(value / norm for value in values) if norm else tuple(0.0 for _ in values)


def _is_allowed(policy: dict[str, Any], agent_scope: str, operation: str) -> bool:
    """@brief 按 deny 优先求值可见性 / Evaluate visibility with deny priority.

    @param policy 可见性策略 / Visibility policy.
    @param agent_scope Agent 作用域 / Agent scope.
    @param operation 请求操作 / Requested operation.
    @return 明确允许时为真 / True only when explicitly allowed.
    """
    matching = [grant for grant in policy.get("agent_grants", []) if grant.get("agent_scope") == agent_scope and operation in grant.get("allowed_operations", [])]
    if any(grant.get("effect") == "deny" for grant in matching):
        return False
    if any(grant.get("effect") == "allow" for grant in matching):
        return True
    return policy.get("default_effect") == "allow"


def _lexical_score(query_tokens: set[str], text: str) -> float:
    """@brief 为 mock retrieval 计算可重复词法分数 / Calculate a repeatable lexical score for mock retrieval.

    @param query_tokens 查询词集合 / Query-token set.
    @param text chunk 文本 / Chunk text.
    @return 0 到 1 的分数 / Score from 0 to 1.
    """
    terms = set(text.lower().split())
    return len(query_tokens & terms) / max(len(query_tokens), 1)


def _search_result(
    score: float,
    source: KnowledgeSourceRecord,
    chunk: KnowledgeChunk,
    include_quotes: bool,
) -> dict[str, Any]:
    """@brief 构造公开检索结果 / Construct a public search result.

    @param score 检索分数 / Retrieval score.
    @param source 来源聚合 / Source aggregate.
    @param chunk 命中的 chunk / Matched chunk.
    @param include_quotes 是否包含 quote / Whether to include a quote.
    @return KnowledgeSearchResult / KnowledgeSearchResult.
    """
    return {
        "result_id": new_opaque_id("kres"),
        "citation": {
            "citation_id": new_opaque_id("cite"),
            "source_id": source.id,
            "source_version_id": chunk.source_version_id,
            "title": source.name,
            "uri": source.config.get("url") or source.config.get("repository_url"),
            "locator": {"page": None, "line_start": None, "line_end": None, "time_start_ms": None, "time_end_ms": None, "symbol": None, "path": None},
            "quote": chunk.text[:4000] if include_quotes else None,
            "score": score,
        },
        "text": chunk.text,
        "score": score,
        "metadata": {"embedding_space_id": chunk.embedding_space_id, "ordinal": chunk.ordinal},
    }


def _rich_text(text: str) -> dict[str, Any]:
    """@brief 创建最小 RichText / Create minimal RichText.

    @param text 文本内容 / Text content.
    @return 契约 RichText 对象 / Contract RichText object.
    """
    return {
        "schema_version": "1.0",
        "blocks": [{"block_id": new_opaque_id("blk"), "type": "paragraph", "align": "start", "spans": [{"text": text, "marks": []}]}],
        "plain_text": text,
    }


def _mock_report(report_id: str, session_id: str) -> dict[str, Any]:
    """@brief 创建只基于可观察信息的确定性报告 / Create a deterministic report based only on observable information.

    @param report_id 报告 ID / Report ID.
    @param session_id Session ID / Session ID.
    @return 合法 InterviewReport / Valid InterviewReport.
    """
    timestamp = iso_timestamp(utc_now())
    summary = _rich_text("这是基于已确认转录和可观察沟通行为的 mock 评价，不推断受保护属性或人格。")
    return {
        "id": report_id,
        "created_at": timestamp,
        "updated_at": timestamp,
        "revision": 1,
        "session_id": session_id,
        "report_version": "mock-v1",
        "rubric_ref": {"id": "rubric_mock_v1", "version": "1.0"},
        "overall_score": None,
        "overall_confidence": 0.2,
        "executive_summary": summary,
        "strengths": [],
        "improvements": [_rich_text("请在下一轮练习中使用 STAR 结构，并提供可验证的技术细节。")],
        "rubric_scores": [
            {
                "dimension_id": "rubric_mock_v1",
                "score": 0.0,
                "confidence": 0.2,
                "summary": _rich_text("mock 会话没有足够转录证据，因此分数置信度较低。"),
                "evidence": [],
                "improvement_actions": ["完成一次带转录的模拟回答后重新生成报告。"],
            }
        ],
        "question_evaluations": [],
        "communication_metrics": {"speaking_time_ms": None, "average_answer_length_ms": None, "words_per_minute": None, "filler_word_count": None, "long_pause_count": None, "interruption_count": 0, "notes": ["仅记录可观察的会话控制事件。"]},
        "action_plan": [{"priority": "high", "title": "补充可审计回答", "why": "当前证据不足", "practice": "录制一次 90 秒 STAR 回答", "success_criterion": "回答含情境、行动与量化结果"}],
        "limitations": ["此版本使用确定性 mock 适配器，未进行真实 ASR、TTS、数字人或人格推断。"],
        "transcript_artifact_id": None,
        "recording_artifact_ids": [],
        "extensions": {},
    }
