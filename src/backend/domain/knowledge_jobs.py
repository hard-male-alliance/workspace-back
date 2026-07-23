"""@brief Connection/Knowledge Job 请求与 outbox 事件 / Connection/Knowledge Job requests and outbox events."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import NewType

from backend.domain.connections import ConnectionId, ConnectionStatus, CredentialReference
from backend.domain.knowledge_sources import (
    KnowledgeIngestionStatus,
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
)
from backend.domain.platform import JsonValue, ProblemDetails
from backend.domain.principals import DomainInvariantError, UserId, WorkspaceId
from backend.domain.resources import ResourceRef

KnowledgeOutboxEventId = NewType("KnowledgeOutboxEventId", str)
"""@brief Knowledge outbox event 不透明标识 / Opaque Knowledge outbox-event identifier."""


class KnowledgeJobDomainError(DomainInvariantError):
    """@brief Connection/Knowledge Job 领域不变量错误 / Connection/Knowledge Job invariant error."""


class KnowledgeJobKind(StrEnum):
    """@brief 5.3 创建的开放 Job kind 子集 / Open Job-kind subset created by section 5.3."""

    CONNECTION_REVOKE = "connection.revoke"
    KNOWLEDGE_DELETE = "knowledge.delete"
    KNOWLEDGE_INGEST = "knowledge.ingest"
    KNOWLEDGE_SYNC = "knowledge.sync"


@dataclass(frozen=True, slots=True)
class ConnectionRevokeSpec:
    """@brief Connection credential 撤销 worker 输入 / Connection-credential revocation worker input.

    @param connection_id Connection 标识 / Connection identifier.
    @param credential_reference 仅 worker 可解引用的 server reference / Server reference resolvable only by the worker.
    @param previous_status 排队前可恢复状态 / Restorable status before queueing.
    @param previous_problem FAILED 状态对应的公开安全问题 / Public-safe problem associated
        with a previous FAILED state.
    """

    connection_id: ConnectionId
    credential_reference: CredentialReference = field(repr=False)
    previous_status: ConnectionStatus = ConnectionStatus.REAUTHORIZATION_REQUIRED
    previous_problem: ProblemDetails | None = None

    def __post_init__(self) -> None:
        """@brief 校验撤销补偿快照 / Validate the revocation-compensation snapshot.

        @raise KnowledgeJobDomainError 快照不是可撤销前态时抛出 / Raised unless the snapshot
            is a valid pre-revocation state.
        @note 旧数据缺少新增字段时保守恢复为 reauthorization_required，避免把可能已经失效的
            credential 错误提升为 active。/ Legacy rows lacking the new fields conservatively
            recover to reauthorization_required rather than promoting a possibly invalid credential.
        """
        if self.previous_status not in {
            ConnectionStatus.ACTIVE,
            ConnectionStatus.REAUTHORIZATION_REQUIRED,
            ConnectionStatus.FAILED,
        }:
            raise KnowledgeJobDomainError("connection revocation snapshot has an invalid status")
        if (self.previous_status is ConnectionStatus.FAILED) is (self.previous_problem is None):
            raise KnowledgeJobDomainError(
                "failed connection snapshot requires exactly one public-safe problem"
            )


@dataclass(frozen=True, slots=True)
class KnowledgeDeleteSpec:
    """@brief KnowledgeSource 异步删除 worker 输入 / KnowledgeSource asynchronous-deletion worker input.

    @param source_id 来源 / Source.
    @param source_revision 删除请求冻结的 revision / Source revision frozen by the deletion request.
    @param previous_enabled 删除前是否启用 / Whether the source was enabled before deletion.
    @param previous_ingestion_status 删除前 ingestion 状态 / Ingestion state before deletion.
    @param previous_problem FAILED 前态对应的公开安全问题 / Public-safe problem for a previous
        FAILED state.
    """

    source_id: KnowledgeSourceId
    source_revision: int
    previous_enabled: bool = True
    previous_ingestion_status: KnowledgeIngestionStatus = KnowledgeIngestionStatus.NOT_STARTED
    previous_problem: ProblemDetails | None = None

    def __post_init__(self) -> None:
        """@brief 校验删除 spec / Validate the deletion spec.

        @raise KnowledgeJobDomainError revision 非正时抛出 / Raised for a non-positive revision.
        """
        if self.source_revision < 1:
            raise KnowledgeJobDomainError("knowledge deletion revision must be positive")
        if self.previous_ingestion_status.is_active or self.previous_ingestion_status in {
            KnowledgeIngestionStatus.DELETING,
            KnowledgeIngestionStatus.DELETED,
        }:
            raise KnowledgeJobDomainError("knowledge deletion snapshot must be an idle state")
        if (
            self.previous_ingestion_status is KnowledgeIngestionStatus.FAILED
        ) is (self.previous_problem is None):
            raise KnowledgeJobDomainError(
                "failed knowledge snapshot requires exactly one public-safe problem"
            )


@dataclass(frozen=True, slots=True)
class KnowledgeProcessSpec:
    """@brief ingestion/sync worker 的不可变请求快照 / Immutable ingestion/sync worker request snapshot.

    @param source_id 来源 / Source.
    @param source_revision Job 创建后来源 revision / Source revision after Job creation.
    @param version_id 创建时当前版本 / Current version at creation time.
    @param force 是否显式强制 / Whether processing was explicitly forced.
    @param requested_by 发起用户 / Requesting user.
    @param previous_ingestion_status 排队前可恢复 ingestion 状态 / Restorable ingestion state
        before queueing.
    @param previous_problem FAILED 前态对应的公开安全问题 / Public-safe problem for a previous
        FAILED state.
    """

    source_id: KnowledgeSourceId
    source_revision: int
    version_id: KnowledgeSourceVersionId | None
    force: bool
    requested_by: UserId
    previous_ingestion_status: KnowledgeIngestionStatus = KnowledgeIngestionStatus.NOT_STARTED
    previous_problem: ProblemDetails | None = None

    def __post_init__(self) -> None:
        """@brief 校验处理 spec / Validate the processing spec.

        @raise KnowledgeJobDomainError revision 或 actor 非法时抛出 / Raised for invalid fields.
        """
        if self.source_revision < 1:
            raise KnowledgeJobDomainError("knowledge processing revision must be positive")
        if not self.requested_by:
            raise KnowledgeJobDomainError("knowledge processing requires a requesting user")
        if self.previous_ingestion_status in {
            KnowledgeIngestionStatus.QUEUED,
            KnowledgeIngestionStatus.FETCHING,
            KnowledgeIngestionStatus.PARSING,
            KnowledgeIngestionStatus.CHUNKING,
            KnowledgeIngestionStatus.EMBEDDING,
            KnowledgeIngestionStatus.DELETING,
            KnowledgeIngestionStatus.DELETED,
        }:
            raise KnowledgeJobDomainError("knowledge processing snapshot must be an idle state")
        if (
            self.previous_ingestion_status is KnowledgeIngestionStatus.FAILED
        ) is (self.previous_problem is None):
            raise KnowledgeJobDomainError(
                "failed knowledge snapshot requires exactly one public-safe problem"
            )


type KnowledgeJobSpec = ConnectionRevokeSpec | KnowledgeDeleteSpec | KnowledgeProcessSpec
"""@brief 5.3 Job worker spec 判别联合 / Section-5.3 Job worker-spec union."""


@dataclass(frozen=True, slots=True)
class KnowledgeOutboxEvent:
    """@brief 与业务写入同事务持久化的 secret-free outbox 事件 / Secret-free transactional outbox event.

    @param event_id 事件标识 / Event identifier.
    @param workspace_id Workspace 边界 / Workspace boundary.
    @param event_type 稳定事件类型 / Stable event type.
    @param subject 事件 subject / Event subject.
    @param actor_id 发起用户 / Initiating user.
    @param occurred_at 发生时刻 / Occurrence instant.
    @param data 小型、无 secret 的不可变提示 / Small immutable hint without secrets.
    """

    event_id: KnowledgeOutboxEventId
    workspace_id: WorkspaceId
    event_type: str
    subject: ResourceRef
    actor_id: UserId
    occurred_at: datetime
    data: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        """@brief 校验并浅冻结 outbox envelope / Validate and shallow-freeze the outbox envelope.

        @raise KnowledgeJobDomainError 标识、类型、时间或数据量非法时抛出 / Raised for invalid fields.
        """
        if not self.event_id or not self.workspace_id or not self.actor_id:
            raise KnowledgeJobDomainError("knowledge outbox event requires opaque identifiers")
        if not self.event_type or len(self.event_type) > 100:
            raise KnowledgeJobDomainError("knowledge outbox event type is invalid")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise KnowledgeJobDomainError("knowledge outbox event time must be timezone-aware")
        if len(self.data) > 20:
            raise KnowledgeJobDomainError("knowledge outbox event data cannot exceed 20 entries")
        object.__setattr__(self, "data", MappingProxyType(dict(self.data)))


__all__ = [
    "ConnectionRevokeSpec",
    "KnowledgeDeleteSpec",
    "KnowledgeJobDomainError",
    "KnowledgeJobKind",
    "KnowledgeJobSpec",
    "KnowledgeOutboxEvent",
    "KnowledgeOutboxEventId",
    "KnowledgeProcessSpec",
]
