"""@brief 跨子领域的纯领域原语 / Pure cross-subdomain domain primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class JobStatus(StrEnum):
    """@brief 长任务的合法状态 / Legal long-running job states."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class Problem:
    """@brief RFC 9457 风格领域错误 / RFC 9457-style domain error."""

    code: str
    status: int
    title: str
    detail: str | None = None
    retryable: bool = False
    violations: list[dict[str, Any]] = field(default_factory=list)
    extensions: dict[str, Any] = field(default_factory=dict)

    def as_dict(self, request_id: str | None = None, instance: str | None = None) -> dict[str, Any]:
        """@brief 变换为公开 ProblemDetails / Convert to public ProblemDetails.

        @param request_id 请求追踪 ID / Request trace ID.
        @param instance 出错资源 URI / Failing resource URI.
        @return 符合契约的错误对象 / Contract-compliant error object.
        """
        return {
            "type": f"urn:aiws:error:{self.code.replace('.', ':')}",
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
            "instance": instance,
            "code": self.code,
            "request_id": request_id,
            "retryable": self.retryable,
            "retry_after_ms": None,
            "violations": self.violations,
            "extensions": self.extensions,
        }


class DomainError(Exception):
    """@brief 显式领域失败 / Explicit domain failure."""

    def __init__(self, problem: Problem) -> None:
        """@brief 初始化领域错误 / Initialize a domain error.

        @param problem 结构化领域问题 / Structured domain problem.
        """
        super().__init__(problem.code)
        self.problem = problem


def utc_now() -> datetime:
    """@brief 取得 UTC 当前时间 / Get the current UTC time.

    @return 带时区的 UTC 时间 / Timezone-aware UTC time.
    """
    return datetime.now(UTC)


def iso_timestamp(value: datetime) -> str:
    """@brief 序列化 RFC 3339 时间 / Serialize an RFC 3339 timestamp.

    @param value UTC 或可转换时间 / UTC or convertible datetime.
    @return UTC 的 RFC 3339 字符串 / RFC 3339 UTC string.
    """
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class Job:
    """@brief 统一长任务实体 / Unified long-running job entity."""

    id: str
    job_type: str
    created_at: datetime
    request_id: str | None
    status: JobStatus = JobStatus.QUEUED
    phase: str = "queued"
    completed_units: int = 0
    total_units: int | None = None
    percent: float | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: Problem | None = None
    extensions: dict[str, Any] = field(default_factory=dict)

    def start(self) -> None:
        """@brief 将 Job 迁移到运行状态 / Transition a job to running.

        @raise DomainError 状态机不允许时抛出 / Raised for an invalid state transition.
        """
        if self.status is not JobStatus.QUEUED:
            raise DomainError(Problem("job.invalid_state", 409, "Job cannot be started"))
        self.status = JobStatus.RUNNING
        self.phase = "processing"
        self.started_at = utc_now()

    def succeed(self) -> None:
        """@brief 将 Job 标记成功 / Mark a job as successful.

        @raise DomainError 状态机不允许时抛出 / Raised for an invalid state transition.
        """
        if self.status not in {JobStatus.QUEUED, JobStatus.RUNNING}:
            raise DomainError(Problem("job.invalid_state", 409, "Job cannot be completed"))
        self.status = JobStatus.SUCCEEDED
        self.phase = "done"
        self.percent = 100.0
        self.finished_at = utc_now()

    def fail(self, problem: Problem) -> None:
        """@brief 将 Job 标记失败 / Mark a job as failed.

        @param problem 失败原因 / Failure reason.
        """
        self.status = JobStatus.FAILED
        self.error = problem
        self.finished_at = utc_now()

    def cancel(self) -> None:
        """@brief 幂等地取消 Job / Idempotently cancel a job."""
        if self.status in {JobStatus.QUEUED, JobStatus.RUNNING}:
            self.status = JobStatus.CANCELLED
            self.finished_at = utc_now()

    def as_dict(self) -> dict[str, Any]:
        """@brief 转换为公开 Job 对象 / Convert to public Job object.

        @return 契约中的 Job 表示 / Contract Job representation.
        """
        return {
            "id": self.id,
            "job_type": self.job_type,
            "status": self.status.value,
            "progress": {
                "phase": self.phase,
                "completed_units": self.completed_units,
                "total_units": self.total_units,
                "percent": self.percent,
                "message": None,
            },
            "created_at": iso_timestamp(self.created_at),
            "started_at": iso_timestamp(self.started_at) if self.started_at else None,
            "finished_at": iso_timestamp(self.finished_at) if self.finished_at else None,
            "expires_at": None,
            "error": self.error.as_dict() if self.error else None,
            "request_id": self.request_id,
            "extensions": self.extensions,
        }
