"""@brief API v2 5.6 通用平台用例 / API v2 section 5.6 common-platform use cases."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from hashlib import sha256

from backend.application.ports.platform import (
    ArtifactContentStore,
    ArtifactDownload,
    ArtifactQuery,
    ByteRangeRequest,
    Clock,
    CollectionPage,
    EventReplayRequest,
    JobCancellationRejected,
    JobCasMismatch,
    JobQuery,
    MutationContext,
    PageRequest,
    PlatformAuthorizationRequest,
    PlatformEventFeed,
    PlatformPermission,
    PlatformResourceTarget,
    PlatformTargetKind,
    PlatformUnitOfWork,
    PlatformUnitOfWorkFactory,
)
from backend.domain.platform import (
    ApiEvent,
    ApiEventId,
    Artifact,
    ArtifactId,
    AuditEvent,
    Job,
    JobId,
    JobTransitionError,
    PdfSourceMap,
)
from backend.domain.principals import TokenPrincipal, WorkspaceAccessContext, WorkspaceId


class PlatformApplicationError(Exception):
    """@brief 可稳定映射为 ProblemDetails 的平台错误 / Platform error mappable to ProblemDetails.

    @param code transport 无关稳定错误码 / Stable transport-independent error code.
    @param detail 可公开且不泄漏租户数据的详情 / Public-safe detail without tenant leakage.
    """

    code: str
    """@brief 稳定错误码 / Stable error code."""

    detail: str
    """@brief 可公开错误详情 / Public-safe error detail."""

    def __init__(self, code: str, detail: str) -> None:
        """@brief 初始化结构化应用错误 / Initialize a structured application error.

        @param code 稳定错误码 / Stable error code.
        @param detail 可公开详情 / Public-safe detail.
        """
        super().__init__(detail)
        self.code = code
        self.detail = detail


class PlatformResourceNotFound(PlatformApplicationError):
    """@brief 资源不存在或不得向调用方暴露 / Resource is absent or must not be disclosed."""

    def __init__(self, resource: str) -> None:
        """@brief 创建不泄漏标识的缺失错误 / Create a non-disclosing not-found error.

        @param resource 稳定资源类型 / Stable resource kind.
        """
        super().__init__(f"{resource}.not_found", f"{resource} was not found")


class PlatformConflict(PlatformApplicationError):
    """@brief 当前领域状态拒绝命令 / Current domain state rejects a command."""


class PlatformPreconditionFailed(PlatformApplicationError):
    """@brief 调用方选中的资源 revision 已失效 / The caller-selected resource revision is stale."""

    def __init__(self) -> None:
        """@brief 创建不泄漏当前 revision 的前置条件失败 / Create a non-disclosing precondition failure."""

        super().__init__("job.precondition_failed", "job changed after its validator was selected")


class PlatformIsolationViolation(RuntimeError):
    """@brief repository 违反 Workspace 隔离后置条件 / Repository violated Workspace isolation."""


class ArtifactContentIntegrityError(PlatformApplicationError):
    """@brief Artifact metadata 或流内容完整性失败 / Artifact metadata or stream integrity failure."""

    def __init__(self) -> None:
        """@brief 创建可重试但不泄漏存储细节的错误 / Create a retryable storage-safe error."""
        super().__init__(
            "artifact.integrity_failed", "artifact content failed integrity validation"
        )


class EventStreamInvariantError(RuntimeError):
    """@brief event adapter 破坏 sequence/event_id 顺序 / Event adapter violated sequence/event-ID order."""


class UtcClock:
    """@brief 使用 UTC 的生产时钟 / Production clock using UTC."""

    def now(self) -> datetime:
        """@brief 返回当前 UTC 时刻 / Return the current UTC instant.

        @return 带时区 UTC 时间 / Timezone-aware UTC time.
        """
        return datetime.now(UTC)


class PlatformApplicationService:
    """@brief 统一 Job/Artifact/Event/Audit 用例协调器 / Unified Job/Artifact/Event/Audit coordinator.

    @param uow_factory 每个查询或命令的独立工作单元 / Independent unit of work per query or command.
    @param content_store Artifact 内容读取端口 / Artifact content-read port.
    @param event_feed 支持重放的 Workspace event feed / Replay-capable Workspace event feed.
    @param clock 可替换时钟 / Replaceable clock.
    """

    def __init__(
        self,
        uow_factory: PlatformUnitOfWorkFactory,
        content_store: ArtifactContentStore,
        event_feed: PlatformEventFeed,
        *,
        clock: Clock | None = None,
    ) -> None:
        """@brief 组装 fail-closed 平台服务 / Assemble the fail-closed platform service.

        @param uow_factory 工作单元工厂 / Unit-of-work factory.
        @param content_store Artifact 内容端口 / Artifact content port.
        @param event_feed SSE event feed / SSE event feed.
        @param clock 可选时钟 / Optional clock.
        """
        self._uow_factory = uow_factory
        self._content_store = content_store
        self._event_feed = event_feed
        self._clock = clock or UtcClock()

    async def list_jobs(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        *,
        query: JobQuery | None = None,
        page: PageRequest | None = None,
    ) -> CollectionPage[Job]:
        """@brief 列出 Workspace Job / List Workspace Jobs.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param query kind/subject 过滤 / Kind/subject filters.
        @param page 解码后的 keyset 分页 / Decoded keyset pagination.
        @return 稳定排序 Job 页面 / Stably ordered Job page.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                PlatformAuthorizationRequest(PlatformPermission.LIST_JOBS),
            )
            result = await uow.repository.list_jobs(
                access,
                query or JobQuery(),
                page or PageRequest(),
            )
            self._require_scoped_items(result.items, workspace_id, "Job")
            return result

    async def get_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        job_id: JobId,
    ) -> Job:
        """@brief 在 Workspace 内读取一个 Job / Read one Job within a Workspace.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param job_id Job 标识 / Job identifier.
        @return 已授权 Job / Authorized Job.
        @raise PlatformResourceNotFound Job 不存在或 scope 错配时抛出 / Raised when absent or
            scoped to another Workspace.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                self._job_request(PlatformPermission.READ_JOB, job_id),
            )
            job = await uow.repository.get_job(access, job_id)
            return self._require_job(job, workspace_id)

    async def get_job_for_cancellation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        job_id: JobId,
    ) -> Job:
        """@brief 以取消权限读取 If-Match 所需 Job 快照 / Read the Job snapshot for If-Match under cancellation permission.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param job_id Job 标识 / Job identifier.
        @return 已通过 CANCEL_JOB 与领域 scope 授权的 Job / Job authorized by CANCEL_JOB and its domain scope.
        @raise PlatformResourceNotFound Job 不存在或 scope 错配时抛出 / Raised when absent or scoped to another Workspace.
        @note 调用方必须把该快照 revision 传回 ``cancel_job``；后者在 mutation transaction
            内再次比较以封闭 TOCTOU。/ The caller must pass this snapshot revision back to
            ``cancel_job``, which compares it again inside the mutation transaction to close TOCTOU.
        """

        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                self._job_request(PlatformPermission.CANCEL_JOB, job_id),
            )
            job = await uow.repository.get_job(access, job_id)
            return self._require_job(job, workspace_id)

    async def cancel_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        job_id: JobId,
        mutation: MutationContext,
        *,
        expected_revision: int | None = None,
    ) -> Job:
        """@brief 以 CAS 取消 queued/running Job / Cancel a queued/running Job using CAS.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param job_id Job 标识 / Job identifier.
        @param mutation 强制 request/trace 审计上下文 / Required request/trace audit context.
        @param expected_revision 可选 HTTP precondition 选中的 revision / Optional revision selected by an HTTP precondition.
        @return cancelled 的新 revision / New cancelled revision.
        @raise PlatformPreconditionFailed 选中 revision 已失效时抛出 / Raised when the selected revision is stale.
        @raise PlatformConflict Job 已终态或发生并发迁移时抛出 / Raised for a terminal or
            concurrently transitioned Job.
        @note POST 的精确重放由通用 durable idempotency 层负责；本状态机不增加 cancelled
            self-loop。/ The durable idempotency layer handles exact POST replay; the state machine
            does not add a cancelled self-loop.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                self._job_request(PlatformPermission.CANCEL_JOB, job_id),
            )
            job = self._require_job(
                await uow.repository.get_job(access, job_id, for_update=True),
                workspace_id,
            )
            if expected_revision is not None and job.meta.revision != expected_revision:
                raise PlatformPreconditionFailed
            previous_revision = job.meta.revision
            cancellation_at = self._clock.now()
            try:
                cancelled = job.cancel(at=cancellation_at)
            except JobTransitionError as exc:
                raise PlatformConflict(
                    "job.not_cancellable",
                    f"job in {exc.current.value} state cannot be cancelled",
                ) from exc
            try:
                await uow.repository.synchronize_cancellation(
                    access,
                    job,
                    at=cancellation_at,
                )
                await uow.repository.save_job(
                    access,
                    cancelled,
                    expected_revision=previous_revision,
                )
            except JobCancellationRejected as exc:
                raise PlatformConflict(exc.code, exc.detail) from exc
            except JobCasMismatch as exc:
                raise PlatformConflict(
                    "job.concurrent_transition",
                    "job changed while cancellation was being applied",
                ) from exc
            await uow.journal.job_cancelled(access, job, cancelled, mutation)
            await uow.commit()
            return cancelled

    async def list_artifacts(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        *,
        query: ArtifactQuery | None = None,
        page: PageRequest | None = None,
    ) -> CollectionPage[Artifact]:
        """@brief 列出 Workspace Artifact / List Workspace Artifacts.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param query kind/subject 过滤 / Kind/subject filters.
        @param page 解码后的 keyset 分页 / Decoded keyset pagination.
        @return 稳定排序 Artifact 页面 / Stably ordered Artifact page.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                PlatformAuthorizationRequest(PlatformPermission.LIST_ARTIFACTS),
            )
            result = await uow.repository.list_artifacts(
                access,
                query or ArtifactQuery(),
                page or PageRequest(),
            )
            self._require_scoped_items(result.items, workspace_id, "Artifact")
            return result

    async def get_artifact(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> Artifact:
        """@brief 在 Workspace 内读取 Artifact / Read an Artifact within a Workspace.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param artifact_id Artifact 标识 / Artifact identifier.
        @return 已授权 Artifact / Authorized Artifact.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                self._artifact_request(PlatformPermission.READ_ARTIFACT, artifact_id),
            )
            artifact = await uow.repository.get_artifact(access, artifact_id)
            return self._require_artifact(artifact, workspace_id)

    async def open_artifact_content(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
        *,
        byte_range: ByteRangeRequest | None = None,
    ) -> ArtifactDownload:
        """@brief 打开支持 ETag 与单 Range 的 Artifact 内容 / Open Artifact content with ETag and Range.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param artifact_id Artifact 标识 / Artifact identifier.
        @param byte_range 可选已解析 Range / Optional parsed Range.
        @return 已做 metadata 交叉校验且流式再验证的下载 / Metadata-cross-checked and streaming-
            validated download.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                self._artifact_request(PlatformPermission.READ_ARTIFACT_CONTENT, artifact_id),
            )
            artifact = self._require_artifact(
                await uow.repository.get_artifact(access, artifact_id),
                workspace_id,
            )
        selected_range = byte_range.resolve(artifact.size_bytes) if byte_range is not None else None
        raw = await self._content_store.open(access, artifact, selected_range)
        if (
            raw.media_type != artifact.media_type
            or raw.total_size_bytes != artifact.size_bytes
            or raw.sha256 != artifact.sha256
            or raw.selected_range != selected_range
        ):
            raise ArtifactContentIntegrityError
        expected_length = artifact.size_bytes if selected_range is None else selected_range.length
        chunks = _validated_content_chunks(
            raw.chunks,
            expected_length=expected_length,
            expected_sha256=artifact.sha256 if selected_range is None else None,
        )
        return ArtifactDownload(
            artifact,
            chunks,
            selected_range,
            f'"sha256-{artifact.sha256}"',
        )

    async def get_pdf_source_map(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> PdfSourceMap:
        """@brief 读取并交叉验证 PDF source map / Read and cross-validate a PDF source map.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param artifact_id Artifact 标识 / Artifact identifier.
        @return 与 Artifact/Resume revision/page count 一致的 source map / Source map consistent
            with Artifact, Resume revision, and page count.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                self._artifact_request(PlatformPermission.READ_ARTIFACT_SOURCE_MAP, artifact_id),
            )
            artifact = self._require_artifact(
                await uow.repository.get_artifact(access, artifact_id),
                workspace_id,
            )
            source_map = await uow.repository.get_pdf_source_map(access, artifact_id)
            if source_map is None:
                raise PlatformResourceNotFound("artifact.source_map")
            source_map.validate_for(artifact)
            return source_map

    async def open_event_stream(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        *,
        after_event_id: ApiEventId | None = None,
    ) -> AsyncIterator[ApiEvent]:
        """@brief 授权并打开可恢复的 Workspace event stream / Authorize and open a resumable event stream.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param after_event_id ``Last-Event-ID`` / ``Last-Event-ID``.
        @return 至少一次、sequence 不倒退的异步事件流 / At-least-once async event stream whose
            sequence never decreases.
        @note feed 在本方法返回前验证 replay window，使 adapter 仍可返回契约 409。
            / The feed validates the replay window before this method returns, preserving HTTP 409.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                PlatformAuthorizationRequest(PlatformPermission.READ_EVENTS),
            )
        events = await self._event_feed.open(access, EventReplayRequest(after_event_id))
        return _ordered_events(events)

    async def list_audit_events(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        *,
        page: PageRequest | None = None,
    ) -> CollectionPage[AuditEvent]:
        """@brief 列出 Workspace 审计事件 / List Workspace audit events.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param page 解码后的 keyset 分页 / Decoded keyset pagination.
        @return 稳定排序审计页 / Stably ordered audit page.
        """
        async with self._uow_factory() as uow:
            access = await self._authorize(
                uow,
                principal,
                workspace_id,
                PlatformAuthorizationRequest(PlatformPermission.LIST_AUDIT_EVENTS),
            )
            result = await uow.repository.list_audit_events(access, page or PageRequest())
            for event in result.items:
                if event.workspace_id != workspace_id:
                    raise PlatformIsolationViolation(
                        "audit repository returned an event from another Workspace"
                    )
            return result

    @staticmethod
    async def _authorize(
        uow: PlatformUnitOfWork,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        request: PlatformAuthorizationRequest,
    ) -> WorkspaceAccessContext:
        """@brief 在读取 tenant 数据前完成精确授权 / Complete exact authorization before tenant reads.

        @param uow 当前工作单元 / Current unit of work.
        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param request 精确 permission/target / Exact permission and target.
        @return 密封 Workspace 证明 / Sealed Workspace proof.
        """
        actor = await uow.authorizer.authenticate(principal)
        return await uow.authorizer.authorize(actor, workspace_id, request)

    @staticmethod
    def _job_request(
        permission: PlatformPermission,
        job_id: JobId,
    ) -> PlatformAuthorizationRequest:
        """@brief 构造 Job 单项授权请求 / Build a Job item-authorization request.

        @param permission Job permission / Job permission.
        @param job_id Job 标识 / Job identifier.
        @return 判别完整的授权请求 / Fully discriminated authorization request.
        """
        return PlatformAuthorizationRequest(
            permission,
            PlatformResourceTarget(PlatformTargetKind.JOB, job_id),
        )

    @staticmethod
    def _artifact_request(
        permission: PlatformPermission,
        artifact_id: ArtifactId,
    ) -> PlatformAuthorizationRequest:
        """@brief 构造 Artifact 单项授权请求 / Build an Artifact item-authorization request.

        @param permission Artifact permission / Artifact permission.
        @param artifact_id Artifact 标识 / Artifact identifier.
        @return 判别完整的授权请求 / Fully discriminated authorization request.
        """
        return PlatformAuthorizationRequest(
            permission,
            PlatformResourceTarget(PlatformTargetKind.ARTIFACT, artifact_id),
        )

    @staticmethod
    def _require_job(job: Job | None, workspace_id: WorkspaceId) -> Job:
        """@brief 要求 Job 存在且属于路径 Workspace / Require a Job in the path Workspace.

        @param job repository 结果 / Repository result.
        @param workspace_id 路径 Workspace / Path Workspace.
        @return 同 Workspace Job / Same-Workspace Job.
        @raise PlatformResourceNotFound 不存在时抛出 / Raised when absent.
        @raise PlatformIsolationViolation repository 泄漏其他 Workspace 时抛出 / Raised for a
            repository isolation defect.
        """
        if job is None:
            raise PlatformResourceNotFound("job")
        if job.workspace_id != workspace_id:
            raise PlatformIsolationViolation("job repository crossed the Workspace boundary")
        return job

    @staticmethod
    def _require_artifact(
        artifact: Artifact | None,
        workspace_id: WorkspaceId,
    ) -> Artifact:
        """@brief 要求 Artifact 存在且属于路径 Workspace / Require an Artifact in the path Workspace.

        @param artifact repository 结果 / Repository result.
        @param workspace_id 路径 Workspace / Path Workspace.
        @return 同 Workspace Artifact / Same-Workspace Artifact.
        """
        if artifact is None:
            raise PlatformResourceNotFound("artifact")
        if artifact.workspace_id != workspace_id:
            raise PlatformIsolationViolation("artifact repository crossed the Workspace boundary")
        return artifact

    @staticmethod
    def _require_scoped_items(
        items: tuple[Job, ...] | tuple[Artifact, ...],
        workspace_id: WorkspaceId,
        label: str,
    ) -> None:
        """@brief 验证列表 repository 的 Workspace 后置条件 / Verify list-repository Workspace postcondition.

        @param items Job 或 Artifact 页面项目 / Job or Artifact page items.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param label 错误标签 / Error label.
        @raise PlatformIsolationViolation 任一项目跨 Workspace 时抛出 / Raised when any item
            crosses the Workspace boundary.
        """
        if any(item.workspace_id != workspace_id for item in items):
            raise PlatformIsolationViolation(
                f"{label} repository returned an item from another Workspace"
            )


async def _validated_content_chunks(
    chunks: AsyncIterator[bytes],
    *,
    expected_length: int,
    expected_sha256: str | None,
) -> AsyncIterator[bytes]:
    """@brief 逐块验证长度，并为完整响应复核 SHA-256 / Validate length and full-response SHA-256.

    @param chunks 底层二进制流 / Underlying binary stream.
    @param expected_length 本响应预期字节数 / Expected bytes in this response.
    @param expected_sha256 完整响应时的摘要；range 时为空 / Digest for a full response; absent for
        a range response.
    @return 验证包装后的相同 bytes / Same bytes through a validating wrapper.
    @raise ArtifactContentIntegrityError 字节类型、长度或摘要不匹配时抛出 / Raised for a type,
        length, or digest mismatch.
    """
    received = 0
    digest = sha256() if expected_sha256 is not None else None
    async for chunk in chunks:
        if not isinstance(chunk, bytes):
            raise ArtifactContentIntegrityError
        received += len(chunk)
        if received > expected_length:
            raise ArtifactContentIntegrityError
        if digest is not None:
            digest.update(chunk)
        yield chunk
    if received != expected_length:
        raise ArtifactContentIntegrityError
    if digest is not None and digest.hexdigest() != expected_sha256:
        raise ArtifactContentIntegrityError


async def _ordered_events(events: AsyncIterator[ApiEvent]) -> AsyncIterator[ApiEvent]:
    """@brief 防御性验证至少一次流的 sequence 顺序 / Defensively validate at-least-once sequence order.

    @param events event adapter 原始流 / Raw event-adapter stream.
    @return sequence 不倒退的相同事件 / Same events with non-decreasing sequence.
    @raise EventStreamInvariantError sequence 倒退或同 sequence 对应不同事件时抛出 / Raised when
        sequence decreases or one sequence identifies different events.
    """
    previous: ApiEvent | None = None
    async for event in events:
        if previous is not None:
            if event.sequence < previous.sequence:
                raise EventStreamInvariantError("event sequence moved backwards")
            if event.sequence == previous.sequence and event != previous:
                raise EventStreamInvariantError(
                    "one event sequence was associated with different payloads"
                )
            if event.sequence > previous.sequence and event.event_id == previous.event_id:
                raise EventStreamInvariantError("one event ID was associated with two sequences")
        previous = event
        yield event


__all__ = [
    "ArtifactContentIntegrityError",
    "EventStreamInvariantError",
    "PlatformApplicationError",
    "PlatformApplicationService",
    "PlatformConflict",
    "PlatformIsolationViolation",
    "PlatformPreconditionFailed",
    "PlatformResourceNotFound",
    "UtcClock",
]
