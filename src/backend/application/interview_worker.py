"""@brief Interview queued 事件到可恢复 worker 的严格适配 / Strict adapter from Interview queued events to a recoverable worker.

统一 outbox 只负责租约、续租和有界退避；本 handler 只消费
``interview.job.queued``，精确交叉验证 envelope 后，把选择权交给持久化
``agent.jobs.job_type``。事件 payload 从不携带可伪造的 Job kind。
"""

from __future__ import annotations

from typing import Protocol

from backend.application.interview_v2 import InterviewApplicationError
from backend.application.ports.outbox_dispatch import (
    OutboxDispatchClaim,
    OutboxHandlerFailure,
)
from backend.domain.interview_v2 import InterviewSessionId
from backend.domain.platform import JobId
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef

INTERVIEW_WORK_EVENT_TYPES = frozenset({"interview.job.queued"})
"""@brief composition 必须独占 claim 的 Interview 工作事件 / Interview work events composition must claim exclusively."""

_QUEUED_PAYLOAD_FIELDS = frozenset({"actor_id", "session_id", "job_id"})
"""@brief queued payload 的封闭字段集 / Closed queued-payload field set."""


class _InterviewQueuedJobWorker(Protocol):
    """@brief handler 所需的最小 worker 形状 / Minimal worker shape required by the handler."""

    async def execute_queued_job(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        job_id: JobId,
        *,
        attempt_count: int,
        maximum_attempts: int,
    ) -> None:
        """@brief 读取持久 Job kind 并幂等执行 / Read persisted Job kind and execute idempotently."""

    async def fail_exhausted(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
    ) -> None:
        """@brief 仅按可信 header 闭合耗尽 Job / Close an exhausted Job using trusted headers only."""


class InterviewJobOutboxHandler:
    """@brief 严格验证 claim 并驱动 end/report Job / Strictly validate a claim and drive an end/report Job."""

    def __init__(
        self,
        worker: _InterviewQueuedJobWorker,
        *,
        maximum_attempts: int = 12,
    ) -> None:
        """@brief 绑定 lifespan-owned worker 与重试上限 / Bind a lifespan-owned worker and retry cap.

        @param worker 按持久 Job kind 分派的两段事务 worker / Two-transaction worker dispatching
            by persisted Job kind.
        @param maximum_attempts 必须与 OutboxDispatchSettings 完全相同 / Must exactly match
            ``OutboxDispatchSettings``.
        """
        if (
            isinstance(maximum_attempts, bool)
            or not isinstance(maximum_attempts, int)
            or not 1 <= maximum_attempts <= 100
        ):
            raise ValueError("Interview outbox attempts must be between 1 and 100")
        self._worker = worker
        self._maximum_attempts = maximum_attempts

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 执行一条严格绑定的 Interview Job 事件 / Execute one strictly bound Interview Job event.

        @param claim 通用 dispatcher 已租约独占的 durable claim / Durable claim exclusively
            leased by the generic dispatcher.
        @raise OutboxHandlerFailure envelope 非法或 worker 请求重放时抛出稳定码 / Raised with
            a stable code for an invalid envelope or a replay request.
        """
        session_id, job_id = _queued_identity(claim)
        try:
            await self._worker.execute_queued_job(
                claim.workspace_id,
                session_id,
                job_id,
                attempt_count=claim.attempt_count,
                maximum_attempts=self._maximum_attempts,
            )
        except InterviewApplicationError as error:
            raise OutboxHandlerFailure(error.code) from error

    async def on_exhausted(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
    ) -> None:
        """@brief 在 source outbox failed 前原子闭合 Interview 工作 / Atomically close Interview work before source-outbox failure.

        @param claim 仍由 dispatcher 租用的最后一次 claim / Final-attempt claim still leased by
            the dispatcher.
        @param error_code dispatcher 即将持久化的脱敏 code / Redacted code about to be persisted
            by the dispatcher.
        @note payload 不可信且完全不读取；不是 Job 的 subject 无法安全定位，按幂等 no-op
            处理。/ The payload is untrusted and never read; a non-Job subject cannot be safely
            located and is treated as an idempotent no-op.
        """
        del error_code
        if (
            claim.event_type not in INTERVIEW_WORK_EVENT_TYPES
            or claim.subject.resource_type != "job"
        ):
            return
        await self._worker.fail_exhausted(
            claim.workspace_id,
            claim.actor_id,
            JobId(claim.subject.id),
        )


def _queued_identity(
    claim: OutboxDispatchClaim,
) -> tuple[InterviewSessionId, JobId]:
    """@brief 交叉验证 event subject 与白名单 payload / Cross-validate event subject and allowlisted payload.

    @param claim 通用 outbox claim / Generic outbox claim.
    @return 强类型 Session/Job identity / Strongly typed Session/Job identity.
    @raise OutboxHandlerFailure 任一 identity 或字段不一致时抛出 / Raised when any identity or
        field is inconsistent.
    """
    payload = claim.payload
    if claim.event_type not in INTERVIEW_WORK_EVENT_TYPES:
        raise OutboxHandlerFailure("interview.event_type_unsupported")
    if (
        claim.subject.resource_type != "job"
        or claim.subject.revision != 1
        or frozenset(payload) != _QUEUED_PAYLOAD_FIELDS
    ):
        raise OutboxHandlerFailure("interview.queued_event_invalid")
    session_id = payload.get("session_id")
    job_id = payload.get("job_id")
    actor_id = payload.get("actor_id")
    if (
        not isinstance(actor_id, str)
        or actor_id != claim.actor_id
        or not isinstance(session_id, str)
        or not isinstance(job_id, str)
        or job_id != claim.subject.id
    ):
        raise OutboxHandlerFailure("interview.queued_event_invalid")
    try:
        ResourceRef("interview_session", session_id)
        ResourceRef("job", job_id, 1)
    except ValueError as error:
        raise OutboxHandlerFailure("interview.queued_event_invalid") from error
    return InterviewSessionId(session_id), JobId(job_id)


__all__ = ["INTERVIEW_WORK_EVENT_TYPES", "InterviewJobOutboxHandler"]
