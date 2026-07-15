"""@brief Agent 会话、消息与流领域模型 / Agent conversation, message, and stream domain models."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from backend.domain.common import Problem, iso_timestamp, utc_now
from workspace_shared.tenancy import ActorScope


class AgentRunStatus(StrEnum):
    """@brief Agent Run 状态 / Agent Run states."""

    QUEUED = "queued"
    RUNNING = "running"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass(slots=True)
class ConversationRecord:
    """@brief Conversation 聚合 / Conversation aggregate."""

    scope: ActorScope
    id: str
    created_at: datetime
    updated_at: datetime
    title: str | None
    capability: str
    context_refs: list[dict[str, Any]]
    revision: int = 1
    status: str = "active"
    message_ids: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        """@brief 转换为公开 Conversation / Convert to public Conversation.

        @return 契约 Conversation 表示 / Contract Conversation representation.
        """
        return {
            "id": self.id,
            "created_at": iso_timestamp(self.created_at),
            "updated_at": iso_timestamp(self.updated_at),
            "revision": self.revision,
            "workspace_id": self.scope.workspace_id,
            "title": self.title,
            "capability": self.capability,
            "status": self.status,
            "context_refs": self.context_refs,
            "extensions": {},
        }


@dataclass(slots=True)
class MessageRecord:
    """@brief ChatMessage 聚合内实体 / Entity inside the ChatMessage aggregate."""

    id: str
    conversation_id: str
    created_at: datetime
    updated_at: datetime
    role: str
    status: str
    content: list[dict[str, Any]]
    revision: int = 1
    parent_message_id: str | None = None
    run_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """@brief 转换为公开 ChatMessage / Convert to public ChatMessage.

        @return 契约 ChatMessage 表示 / Contract ChatMessage representation.
        """
        return {
            "id": self.id,
            "created_at": iso_timestamp(self.created_at),
            "updated_at": iso_timestamp(self.updated_at),
            "revision": self.revision,
            "conversation_id": self.conversation_id,
            "role": self.role,
            "status": self.status,
            "content": self.content,
            "parent_message_id": self.parent_message_id,
            "run_id": self.run_id,
            "extensions": {},
        }


@dataclass(slots=True)
class AgentRunRecord:
    """@brief 可取消且可重放的 Agent Run / Cancellable and replayable Agent Run."""

    scope: ActorScope
    id: str
    conversation_id: str
    input_message_id: str
    created_at: datetime
    updated_at: datetime
    request: dict[str, Any]
    status: AgentRunStatus = AgentRunStatus.QUEUED
    phase: str = "queued"
    revision: int = 1
    output_message_id: str | None = None
    problem: Problem | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    cancelled: bool = False
    extensions: dict[str, Any] = field(default_factory=dict)
    token_usage: dict[str, Any] = field(default_factory=dict)
    cost: dict[str, Any] = field(default_factory=dict)

    def as_dict(self, stream_url: str | None = None) -> dict[str, Any]:
        """@brief 转换为公开 AgentRun / Convert to public AgentRun.

        @param stream_url SSE 事件地址 / SSE event URL.
        @return 契约 AgentRun 表示 / Contract AgentRun representation.
        @note 正式契约尚未冻结顶层计费字段，因此持久化的 ``token_usage`` 与 ``cost``
        通过合法命名空间 ``extensions.aiws.metering`` 公开；它们明确是本地估算，
        不可视为 provider invoice（供应商账单）。
        """
        extensions = deepcopy(self.extensions)
        if self.token_usage or self.cost:
            extensions["aiws.metering"] = {
                "token_usage": deepcopy(self.token_usage),
                "cost": deepcopy(self.cost),
            }
        return {
            "id": self.id,
            "created_at": iso_timestamp(self.created_at),
            "updated_at": iso_timestamp(self.updated_at),
            "revision": self.revision,
            "conversation_id": self.conversation_id,
            "input_message_id": self.input_message_id,
            "output_message_id": self.output_message_id,
            "status": self.status.value,
            "phase": self.phase,
            "stream_url": stream_url,
            "expires_at": None,
            "problem": self.problem.as_dict() if self.problem else None,
            "extensions": extensions,
        }

    def append_event(self, event_type: str, payload: dict[str, Any], trace_id: str | None = None) -> dict[str, Any]:
        """@brief 追加顺序事件 / Append an ordered event.

        @param event_type 稳定事件类型 / Stable event type.
        @param payload 公开事件载荷 / Public event payload.
        @param trace_id 可选追踪 ID / Optional trace ID.
        @return 事件 envelope / Event envelope.
        """
        from workspace_shared.ids import new_opaque_id

        event = {
            "protocol_version": "1.0",
            "event_id": new_opaque_id("evt"),
            "event_type": event_type,
            "run_id": self.id,
            "sequence": len(self.events),
            "ack_sequence": None,
            "occurred_at": iso_timestamp(utc_now()),
            "trace_id": trace_id,
            "payload": payload,
            "extensions": {},
        }
        self.events.append(event)
        self.updated_at = utc_now()
        return event
