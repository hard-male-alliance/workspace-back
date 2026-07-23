"""@brief 统一 outbox 事件的闭合交付语义 / Closed delivery semantics for unified-outbox events.

只有会驱动额外业务副作用的事件才进入 durable worker 状态机；已与业务
事务一起持久化的 SSE 通知不需要第二次“发布”，写入时即为 published。事件类型
采用闭集（closed set），新事件必须先显式选择语义，不存在危险的默认分支。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal


class OutboxEventPurpose(StrEnum):
    """@brief outbox 事件的闭合用途 / Closed purpose of an outbox event."""

    WORK = "work"
    """@brief 必须由 durable worker 执行的副作用 / Side effect requiring a durable worker."""

    NOTIFICATION = "notification"
    """@brief 仅供 replay/SSE 读取的已提交通知 / Committed notification used only for replay/SSE."""


AGENT_WORK_EVENT_TYPES = frozenset(
    {
        "agent.run.queued",
        "agent.tool_decision.recorded",
    }
)
"""@brief Agent durable work 事件闭集 / Closed Agent durable-work event set."""

KNOWLEDGE_WORK_EVENT_TYPES = frozenset(
    {
        "connection.revocation_requested",
        "knowledge_source.deletion_requested",
        "knowledge_source.job_created",
    }
)
"""@brief Knowledge durable work 事件闭集 / Closed Knowledge durable-work event set."""

INTERVIEW_WORK_EVENT_TYPES = frozenset({"interview.job.queued"})
"""@brief Interview durable work 事件闭集 / Closed Interview durable-work event set."""

RESUME_WORK_EVENT_TYPES = frozenset({"resume.job_created"})
"""@brief Resume durable work 事件闭集 / Closed Resume durable-work event set."""

WORK_EVENT_TYPES = frozenset(
    AGENT_WORK_EVENT_TYPES
    | KNOWLEDGE_WORK_EVENT_TYPES
    | INTERVIEW_WORK_EVENT_TYPES
    | RESUME_WORK_EVENT_TYPES
)
"""@brief 所有需要 dispatcher 的事件闭集 / Closed set of all dispatcher-owned events."""

NOTIFICATION_EVENT_TYPES = frozenset(
    {
        "agent.citation.added",
        "agent.message.completed",
        "agent.message.delta",
        "agent.run.cancelled",
        "agent.run.completed",
        "agent.run.failed",
        "agent.run.started",
        "agent.run.updated",
        "agent.status",
        "agent.tool_approval.expired",
        "agent.tool_approval.required",
        "connection.created",
        "job.updated",
        "knowledge_source.created",
        "knowledge_source.updated",
        "knowledge_source.version_created",
        "resume.created",
        "resume.deleted",
        "resume.metadata_updated",
        "resume.operations_applied",
        "resume.proposal_decided",
        "resume.updated",
    }
)
"""@brief 写入即完成的 replay/SSE 通知闭集 / Closed replay/SSE notification set published on insert.

@note ``resume.updated`` 是 API V2 迁移前已保留的历史通知；新 Resume producer 使用
    粒度更细的事件名。/ ``resume.updated`` is a retained pre-V2 notification;
    new Resume producers use more specific event names.
"""

KNOWN_OUTBOX_EVENT_TYPES = frozenset(WORK_EVENT_TYPES | NOTIFICATION_EVENT_TYPES)
"""@brief 统一 outbox 允许的全部事件闭集 / Complete closed set accepted by the unified outbox."""


type InitialOutboxStatus = Literal["pending", "published"]
"""@brief producer 可写入的初始状态 / Initial statuses a producer may write."""


@dataclass(frozen=True, slots=True)
class InitialOutboxLifecycle:
    """@brief producer 使用的强类型初始生命周期 / Typed initial lifecycle used by producers.

    @param status ``pending`` work 或 ``published`` notification / ``pending`` work or a
        ``published`` notification.
    @param published_at 通知的提交时刻；work 必须为空 / Commit instant for a
        notification; necessarily absent for work.
    """

    status: InitialOutboxStatus
    published_at: datetime | None

    def __post_init__(self) -> None:
        """@brief 校验状态与时间的代数数据类型 / Validate the status/timestamp algebraic data type."""

        if (self.status == "published") is not (self.published_at is not None):
            raise ValueError("initial outbox status and published_at must agree")
        if self.published_at is not None and (
            self.published_at.tzinfo is None or self.published_at.utcoffset() is None
        ):
            raise ValueError("initial outbox published_at must be timezone-aware")


class UnknownOutboxEventType(ValueError):
    """@brief producer 未显式声明事件用途 / Producer did not explicitly declare an event purpose."""


def classify_outbox_event(event_type: str) -> OutboxEventPurpose:
    """@brief 在闭集中分类事件 / Classify an event within the closed set.

    @param event_type 稳定事件名 / Stable event name.
    @return work 或 notification 用途 / Work or notification purpose.
    @raise UnknownOutboxEventType 事件未显式分类时抛出 / Raised when the event
        has not been classified explicitly.
    """

    if event_type in WORK_EVENT_TYPES:
        return OutboxEventPurpose.WORK
    if event_type in NOTIFICATION_EVENT_TYPES:
        return OutboxEventPurpose.NOTIFICATION
    raise UnknownOutboxEventType(f"unclassified unified-outbox event type: {event_type}")


def initial_outbox_lifecycle(
    event_type: str,
    *,
    occurred_at: datetime,
) -> InitialOutboxLifecycle:
    """@brief 生成无默认分支的初始生命周期 / Build an initial lifecycle without a default branch.

    @param event_type 已分类事件名 / Classified event name.
    @param occurred_at 业务事务中的发生时刻 / Occurrence instant in the business
        transaction.
    @return work 的 pending 状态或 notification 的 published 状态 / Pending work
        or a published notification.
    @raise UnknownOutboxEventType 事件未分类时抛出 / Raised for an unclassified event.
    @raise ValueError 时间无时区时抛出 / Raised for a naive timestamp.
    """

    if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
        raise ValueError("outbox occurred_at must be timezone-aware")
    purpose = classify_outbox_event(event_type)
    if purpose is OutboxEventPurpose.WORK:
        return InitialOutboxLifecycle("pending", None)
    return InitialOutboxLifecycle("published", occurred_at)


__all__ = [
    "AGENT_WORK_EVENT_TYPES",
    "INTERVIEW_WORK_EVENT_TYPES",
    "KNOWLEDGE_WORK_EVENT_TYPES",
    "KNOWN_OUTBOX_EVENT_TYPES",
    "NOTIFICATION_EVENT_TYPES",
    "RESUME_WORK_EVENT_TYPES",
    "WORK_EVENT_TYPES",
    "InitialOutboxLifecycle",
    "OutboxEventPurpose",
    "UnknownOutboxEventType",
    "classify_outbox_event",
    "initial_outbox_lifecycle",
]
