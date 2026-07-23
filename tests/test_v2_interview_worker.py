"""API V2 Interview unified-outbox handler 与 fail-closed adapters 测试。"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import pytest

from backend.application.interview_v2 import InterviewApplicationError, InterviewWorkerRetry
from backend.application.interview_worker import InterviewJobOutboxHandler
from backend.application.ports.interview_v2 import (
    InterviewWorkerOperationId,
    InterviewWorkerPortFailure,
    ReportGenerationRequest,
)
from backend.application.ports.outbox_dispatch import (
    OutboxDispatchClaim,
    OutboxHandlerFailure,
    OutboxLease,
)
from backend.domain.interview_v2 import InterviewSession, InterviewSessionId
from backend.domain.platform import ApiEventId, JobId, JsonValue
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.interview import (
    ConsentAwareInterviewMediaFinalizer,
    FailClosedInterviewMediaFinalizer,
    FailClosedInterviewReportProvider,
)

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 测试固定时刻 / Fixed test instant."""

WORKSPACE = WorkspaceId("workspace_worker0001")
"""@brief 测试 Workspace / Test Workspace."""

SESSION_ID = InterviewSessionId("session_worker000001")
"""@brief 测试 Session / Test Session."""

JOB_ID = JobId("job_worker000000001")
"""@brief 测试统一 Job / Test unified Job."""


@dataclass(slots=True)
class RecordingQueuedWorker:
    """@brief 记录严格 handler 调用的 worker fake / Worker fake recording strict-handler calls."""

    calls: list[tuple[WorkspaceId, InterviewSessionId, JobId, int, int]] = field(
        default_factory=list
    )
    """@brief 已验证的调用 / Validated invocations."""

    failure: InterviewApplicationError | None = None
    """@brief 可选应用失败 / Optional application failure."""

    exhausted: list[tuple[WorkspaceId, UserId, JobId]] = field(default_factory=list)
    """@brief payload 独立的耗尽补偿调用 / Payload-independent exhaustion calls."""

    async def execute_queued_job(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        job_id: JobId,
        *,
        attempt_count: int,
        maximum_attempts: int,
    ) -> None:
        """@brief 记录调用或抛出分类失败 / Record an invocation or raise a classified failure."""
        self.calls.append(
            (workspace_id, session_id, job_id, attempt_count, maximum_attempts)
        )
        if self.failure is not None:
            raise self.failure

    async def fail_exhausted(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
    ) -> None:
        """@brief 记录只含可信 header 的补偿 / Record compensation containing trusted headers only."""
        self.exhausted.append((workspace_id, actor_id, job_id))


def _claim() -> OutboxDispatchClaim:
    """@brief 构造严格有效的 Interview queued claim / Build a strictly valid Interview queued claim."""
    payload: dict[str, JsonValue] = {
        "actor_id": "user_workeractor001",
        "session_id": str(SESSION_ID),
        "job_id": str(JOB_ID),
    }
    return OutboxDispatchClaim(
        ApiEventId("event_interviewqueued01"),
        WORKSPACE,
        UserId("user_workeractor001"),
        ResourceRef("job", JOB_ID, 1),
        "interview.job.queued",
        payload,
        3,
        OutboxLease("lease-token-" + "a" * 48),
        NOW + timedelta(minutes=2),
    )


@pytest.mark.asyncio
async def test_handler_accepts_only_the_closed_envelope_and_passes_retry_cap() -> None:
    """@brief handler 交叉验证 subject/payload 并透传尝试边界 / Handler cross-validates subject/payload and passes attempt bounds."""
    worker = RecordingQueuedWorker()
    handler = InterviewJobOutboxHandler(worker, maximum_attempts=7)

    await handler.handle(_claim())

    assert worker.calls == [(WORKSPACE, SESSION_ID, JOB_ID, 3, 7)]


@pytest.mark.asyncio
async def test_handler_rejects_event_type_extra_fields_and_subject_mismatch() -> None:
    """@brief 非 allowlist envelope 在 worker 前失败 / Non-allowlisted envelopes fail before the worker."""
    worker = RecordingQueuedWorker()
    handler = InterviewJobOutboxHandler(worker)
    invalid_event = replace(_claim(), event_type="interview.report.queued")
    extra_payload: dict[str, JsonValue] = {
        **dict(_claim().payload),
        "job_kind": "interview.end",
    }
    invalid_payload = replace(_claim(), payload=extra_payload)
    mismatched_actor_payload: dict[str, JsonValue] = {
        **dict(_claim().payload),
        "actor_id": "user_different0001",
    }
    mismatched_actor = replace(_claim(), payload=mismatched_actor_payload)
    invalid_subject = replace(
        _claim(),
        subject=ResourceRef("job", "job_workerother0001", 1),
    )

    with pytest.raises(OutboxHandlerFailure, match=r"interview\.event_type_unsupported"):
        await handler.handle(invalid_event)
    with pytest.raises(OutboxHandlerFailure, match=r"interview\.queued_event_invalid"):
        await handler.handle(invalid_payload)
    with pytest.raises(OutboxHandlerFailure, match=r"interview\.queued_event_invalid"):
        await handler.handle(mismatched_actor)
    with pytest.raises(OutboxHandlerFailure, match=r"interview\.queued_event_invalid"):
        await handler.handle(invalid_subject)
    assert worker.calls == []


@pytest.mark.asyncio
async def test_handler_normalizes_worker_retry_to_stable_outbox_failure() -> None:
    """@brief worker 重放请求只暴露稳定错误码 / Worker replay requests expose only a stable error code."""
    worker = RecordingQueuedWorker(
        failure=InterviewWorkerRetry(
            "interview.media_provider_temporarily_unavailable",
            "private provider text must not escape",
        )
    )
    handler = InterviewJobOutboxHandler(worker)

    with pytest.raises(OutboxHandlerFailure) as captured:
        await handler.handle(_claim())
    assert captured.value.code == "interview.media_provider_temporarily_unavailable"
    assert "private provider text" not in str(captured.value)


@pytest.mark.asyncio
async def test_exhaustion_ignores_malformed_payload_and_uses_header_job() -> None:
    """@brief 最终补偿不读取坏 payload 且容忍不可定位 subject / Final compensation ignores malformed payload and tolerates an unlocatable subject."""
    worker = RecordingQueuedWorker()
    handler = InterviewJobOutboxHandler(worker)
    malformed = replace(_claim(), payload={"secret": "must-not-be-read"})

    await handler.on_exhausted(malformed, error_code="outbox.handler_failed")
    await handler.on_exhausted(
        replace(malformed, subject=ResourceRef("interview_session", SESSION_ID)),
        error_code="outbox.handler_failed",
    )

    assert worker.exhausted == [
        (WORKSPACE, UserId("user_workeractor001"), JOB_ID)
    ]


@pytest.mark.asyncio
async def test_unconfigured_worker_adapters_fail_closed_without_fake_outputs() -> None:
    """@brief 未配置 capability 返回终态分类失败而非空成功 / Unconfigured capabilities return terminal classified failures, never empty success."""
    media = FailClosedInterviewMediaFinalizer()
    report = FailClosedInterviewReportProvider()
    operation_id = InterviewWorkerOperationId("interview.end:job_worker000000001")

    with pytest.raises(InterviewWorkerPortFailure) as media_failure:
        await media.finalize(
            cast(InterviewSession, object()),
            operation_id=operation_id,
        )
    with pytest.raises(InterviewWorkerPortFailure) as report_failure:
        await report.generate(
            cast(ReportGenerationRequest, object()),
            operation_id=operation_id,
        )

    assert media_failure.value.code == "interview.media_finalizer_unconfigured"
    assert not media_failure.value.retryable
    assert report_failure.value.code == "interview.report_provider_unconfigured"
    assert not report_failure.value.retryable


@pytest.mark.asyncio
async def test_consent_aware_media_finalizer_completes_only_without_recording() -> None:
    """@brief 无录制请求可零 Artifact 完成，录制请求仍 fail closed / No-recording sessions may complete with zero Artifacts while recording remains fail-closed."""
    finalizer = ConsentAwareInterviewMediaFinalizer()
    operation_id = InterviewWorkerOperationId("interview.end:job_worker000000001")
    no_recording = cast(
        InterviewSession,
        SimpleNamespace(
            spec=SimpleNamespace(
                recording=SimpleNamespace(record_audio=False, record_video=False)
            )
        ),
    )
    recording = cast(
        InterviewSession,
        SimpleNamespace(
            spec=SimpleNamespace(
                recording=SimpleNamespace(record_audio=True, record_video=False)
            )
        ),
    )

    output = await finalizer.finalize(no_recording, operation_id=operation_id)
    assert output.artifacts == ()
    with pytest.raises(InterviewWorkerPortFailure) as failure:
        await finalizer.finalize(recording, operation_id=operation_id)
    assert failure.value.code == "interview.media_finalizer_unconfigured"
    assert not failure.value.retryable
