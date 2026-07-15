"""@brief 面试会话状态机领域模型 / Interview session state-machine domain model."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from backend.domain.common import DomainError, Problem, iso_timestamp, utc_now
from workspace_shared.tenancy import ActorScope


class InterviewStatus(StrEnum):
    """@brief 面试会话合法状态 / Legal interview session states."""

    CREATED = "created"
    PREPARING = "preparing"
    READY = "ready"
    CONNECTING = "connecting"
    IN_PROGRESS = "in_progress"
    ENDING = "ending"
    PROCESSING_REPORT = "processing_report"
    COMPLETED = "completed"
    ABORTED = "aborted"
    FAILED = "failed"
    EXPIRED = "expired"


_ALLOWED_TRANSITIONS: dict[InterviewStatus, set[InterviewStatus]] = {
    InterviewStatus.CREATED: {InterviewStatus.PREPARING, InterviewStatus.ABORTED},
    InterviewStatus.PREPARING: {InterviewStatus.READY, InterviewStatus.FAILED},
    InterviewStatus.READY: {InterviewStatus.CONNECTING, InterviewStatus.EXPIRED, InterviewStatus.ABORTED},
    InterviewStatus.CONNECTING: {InterviewStatus.IN_PROGRESS, InterviewStatus.FAILED, InterviewStatus.ABORTED},
    InterviewStatus.IN_PROGRESS: {InterviewStatus.ENDING, InterviewStatus.FAILED, InterviewStatus.ABORTED},
    InterviewStatus.ENDING: {InterviewStatus.PROCESSING_REPORT, InterviewStatus.FAILED},
    InterviewStatus.PROCESSING_REPORT: {InterviewStatus.COMPLETED, InterviewStatus.FAILED},
    InterviewStatus.COMPLETED: set(),
    InterviewStatus.ABORTED: set(),
    InterviewStatus.FAILED: set(),
    InterviewStatus.EXPIRED: set(),
}


@dataclass(slots=True)
class InterviewSessionRecord:
    """@brief 具有实时事件历史的面试 Session / Interview session with realtime event history."""

    scope: ActorScope
    id: str
    created_at: datetime
    updated_at: datetime
    request: dict[str, Any]
    status: InterviewStatus = InterviewStatus.CREATED
    revision: int = 1
    started_at: datetime | None = None
    ended_at: datetime | None = None
    report_id: str | None = None
    problem: Problem | None = None
    events: list[dict[str, Any]] = field(default_factory=list)

    def transition(self, target: InterviewStatus) -> None:
        """@brief 强制执行面试状态机 / Enforce the interview state machine.

        @param target 目标状态 / Target state.
        @raise DomainError 转移非法时抛出 / Raised for an invalid transition.
        """
        if target not in _ALLOWED_TRANSITIONS[self.status]:
            raise DomainError(
                Problem(
                    "interview.invalid_state",
                    409,
                    "Interview command is invalid in the current state",
                    extensions={"current_status": self.status.value},
                )
            )
        self.status = target
        self.revision += 1
        self.updated_at = utc_now()
        if target is InterviewStatus.IN_PROGRESS:
            self.started_at = self.updated_at
        if target in {InterviewStatus.COMPLETED, InterviewStatus.ABORTED, InterviewStatus.FAILED}:
            self.ended_at = self.updated_at

    def as_dict(self) -> dict[str, Any]:
        """@brief 转换为公开 InterviewSession / Convert to public InterviewSession.

        @return 契约 InterviewSession 表示 / Contract InterviewSession representation.
        """
        request = self.request
        return {
            "id": self.id,
            "created_at": iso_timestamp(self.created_at),
            "updated_at": iso_timestamp(self.updated_at),
            "revision": self.revision,
            "workspace_id": self.scope.workspace_id,
            "scenario_id": request.get("scenario_id"),
            "status": self.status.value,
            "resume_ref": request.get("resume_ref"),
            "job_target": request["job_target"],
            "locale": request["locale"],
            "media": request["media"],
            "recording": request["recording"],
            "started_at": iso_timestamp(self.started_at) if self.started_at else None,
            "ended_at": iso_timestamp(self.ended_at) if self.ended_at else None,
            "report_id": self.report_id,
            "problem": self.problem.as_dict() if self.problem else None,
            "extensions": {},
        }

    def append_event(self, event_type: str, payload: dict[str, Any], trace_id: str | None = None) -> dict[str, Any]:
        """@brief 追加面试实时事件 / Append an interview realtime event.

        @param event_type 事件类型 / Event type.
        @param payload 事件 payload / Event payload.
        @param trace_id 可选追踪 ID / Optional trace ID.
        @return 标准事件 envelope / Standard event envelope.
        """
        from workspace_shared.ids import new_opaque_id

        event = {
            "protocol_version": "1.0",
            "event_id": new_opaque_id("evt"),
            "event_type": event_type,
            "session_id": self.id,
            "sequence": len(self.events),
            "ack_sequence": None,
            "occurred_at": iso_timestamp(utc_now()),
            "trace_id": trace_id,
            "payload": payload,
            "extensions": {},
        }
        self.events.append(event)
        return event
