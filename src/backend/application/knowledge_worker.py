"""@brief Knowledge outbox handler 与两段短事务 worker / Knowledge outbox handler and two-short-transaction worker.

第一段事务只 claim/校验统一 Job 与领域状态；外部 revoke/delete/fetch/parse/embed 全部在
事务外；第二段事务一次提交 source/version/chunks/Job/outbox/audit。Handler 由统一 outbox
租约调度器至少一次调用，因此每个外部操作都使用稳定 event+Job operation ID。
"""

from __future__ import annotations

from collections.abc import Mapping

from backend.application.ports.knowledge_worker import (
    KnowledgeCredentialRevoker,
    KnowledgeIndexBuilder,
    KnowledgeMaterialLoader,
    KnowledgeSourceEraser,
    KnowledgeWorkerClaim,
    KnowledgeWorkerStore,
    KnowledgeWorkerTerminalFailure,
)
from backend.application.ports.outbox_dispatch import (
    OutboxDispatchClaim,
    OutboxHandlerFailure,
)
from backend.domain.knowledge_jobs import KnowledgeJobKind
from backend.domain.platform import JobId

KNOWLEDGE_WORK_EVENT_TYPES = frozenset(
    {
        "connection.revocation_requested",
        "knowledge_source.deletion_requested",
        "knowledge_source.job_created",
    }
)
"""@brief composition 必须穷尽注册的 Knowledge 工作事件 / Knowledge work events composition must register exhaustively."""

_EVENT_PAYLOAD_FIELDS = frozenset({"actor_id", "subject", "data"})
"""@brief Knowledge 工作 envelope 的封闭字段集 / Closed fields of a Knowledge work envelope."""

_SUBJECT_FIELDS = frozenset({"resource_type", "id", "revision"})
"""@brief payload subject 的封闭字段集 / Closed fields of a payload subject."""


class KnowledgeWorkerService:
    """@brief 领域无关 outbox claim 到 Knowledge typed Job 的 adapter / Adapter from a generic outbox claim to a typed Knowledge Job."""

    def __init__(
        self,
        store: KnowledgeWorkerStore,
        credential_revoker: KnowledgeCredentialRevoker,
        source_eraser: KnowledgeSourceEraser,
        material_loader: KnowledgeMaterialLoader,
        index_builder: KnowledgeIndexBuilder,
        *,
        maximum_attempts: int = 12,
    ) -> None:
        """@brief 注入全部显式 worker Ports / Inject every explicit worker port."""

        if not 1 <= maximum_attempts <= 100:
            raise ValueError("Knowledge worker maximum attempts must be between one and 100")
        self._store = store
        self._credential_revoker = credential_revoker
        self._source_eraser = source_eraser
        self._material_loader = material_loader
        self._index_builder = index_builder
        self._maximum_attempts = maximum_attempts

    async def handle(self, dispatch: OutboxDispatchClaim) -> None:
        """@brief 处理一条统一 outbox claim / Handle one unified outbox claim.

        @param dispatch 带真实 actor+Workspace 与租约的 claim / Claim carrying the real
            actor, Workspace, and lease.
        @raise OutboxHandlerFailure 可重试失败时抛出稳定 code / Raised with a stable code
            for a retryable failure.
        """

        if dispatch.event_type not in KNOWLEDGE_WORK_EVENT_TYPES:
            raise OutboxHandlerFailure("knowledge.event_type_unsupported")
        try:
            job_id = _job_id(dispatch)
        except KnowledgeWorkerTerminalFailure as error:
            raise OutboxHandlerFailure(error.code) from error
        claim = await self._store.claim(
            dispatch.workspace_id,
            dispatch.actor_id,
            dispatch.event_id,
            job_id,
        )
        if claim is None:
            return
        try:
            _validate_event_binding(dispatch.event_type, claim.kind)
            await self._execute(claim)
        except KnowledgeWorkerTerminalFailure as error:
            await self._store.fail(claim, error_code=error.code)
        except Exception as error:
            if dispatch.attempt_count >= self._maximum_attempts:
                await self._store.fail(claim, error_code="knowledge.worker_exhausted")
                return
            raise OutboxHandlerFailure("knowledge.worker_retry") from error

    async def on_exhausted(
        self,
        dispatch: OutboxDispatchClaim,
        *,
        error_code: str,
    ) -> None:
        """@brief source outbox failed 前闭合 Knowledge Job/aggregate / Close the Knowledge Job and aggregate before source-outbox failure.

        @param dispatch 仍由通用 dispatcher 租用的最后一次 claim / Final-attempt claim still
            leased by the generic dispatcher.
        @param error_code dispatcher 即将持久化的脱敏错误码 / Redacted error code about to be
            persisted by the dispatcher.
        @note payload 可能损坏，故此路径仅使用独立 header 列；无法定位的 subject 是
            幂等 no-op。/ The payload may be malformed, so this path uses only dedicated header
            columns; an unlocatable subject is an idempotent no-op.
        """
        del error_code
        if (
            dispatch.event_type not in KNOWLEDGE_WORK_EVENT_TYPES
            or dispatch.subject.resource_type != "job"
        ):
            return
        await self._store.fail_exhausted(
            dispatch.workspace_id,
            dispatch.actor_id,
            dispatch.event_id,
            dispatch.event_type,
            JobId(dispatch.subject.id),
        )

    async def _execute(self, claim: KnowledgeWorkerClaim) -> None:
        """@brief 在两个 store 事务之间执行外部工作 / Execute external work between two store transactions."""

        operation_id = f"knowledge:{claim.event_id}:{claim.job_id}"
        if claim.kind is KnowledgeJobKind.CONNECTION_REVOKE:
            await self._credential_revoker.revoke(claim, operation_id=operation_id)
            await self._store.complete_connection_revocation(claim)
            return
        if claim.kind is KnowledgeJobKind.KNOWLEDGE_DELETE:
            await self._source_eraser.erase(claim, operation_id=operation_id)
            await self._store.complete_source_deletion(claim)
            return
        if claim.kind not in {
            KnowledgeJobKind.KNOWLEDGE_INGEST,
            KnowledgeJobKind.KNOWLEDGE_SYNC,
        }:
            raise KnowledgeWorkerTerminalFailure("knowledge.job_kind_unsupported")
        material = await self._material_loader.load(claim)
        prepared = await self._index_builder.build(claim, material)
        await self._store.complete_processing(claim, prepared)


def _job_id(dispatch: OutboxDispatchClaim) -> JobId:
    """@brief 交叉验证 header/envelope 并提取 Job ID / Cross-validate header and envelope and extract the Job ID.

    @param dispatch 通用 outbox claim / Generic outbox claim.
    @return 与 header subject 完全一致的统一 Job ID / Unified Job ID exactly matching the
        header subject.
    @raise KnowledgeWorkerTerminalFailure envelope 任一绑定损坏时抛出 / Raised when any
        envelope binding is malformed.
    """
    payload = dispatch.payload
    subject = payload.get("subject")
    data = payload.get("data")
    expected_data_fields = (
        frozenset({"job_id", "kind", "force"})
        if dispatch.event_type == "knowledge_source.job_created"
        else frozenset({"job_id"})
    )
    if (
        dispatch.subject.resource_type != "job"
        or dispatch.subject.revision != 1
        or frozenset(payload) != _EVENT_PAYLOAD_FIELDS
        or payload.get("actor_id") != dispatch.actor_id
        or not isinstance(subject, Mapping)
        or frozenset(subject) != _SUBJECT_FIELDS
        or subject.get("resource_type") != "job"
        or subject.get("id") != dispatch.subject.id
        or subject.get("revision") != 1
        or not isinstance(data, Mapping)
        or frozenset(data) != expected_data_fields
    ):
        raise KnowledgeWorkerTerminalFailure("knowledge.event_payload_invalid")
    value = data.get("job_id")
    if not isinstance(value, str) or value != dispatch.subject.id:
        raise KnowledgeWorkerTerminalFailure("knowledge.event_payload_invalid")
    return JobId(value)


def _validate_event_binding(event_type: str, kind: KnowledgeJobKind) -> None:
    """@brief 防止一个事件驱动错误的 typed Job / Prevent an event from driving the wrong typed Job."""

    expected = {
        "connection.revocation_requested": {KnowledgeJobKind.CONNECTION_REVOKE},
        "knowledge_source.deletion_requested": {KnowledgeJobKind.KNOWLEDGE_DELETE},
        "knowledge_source.job_created": {
            KnowledgeJobKind.KNOWLEDGE_INGEST,
            KnowledgeJobKind.KNOWLEDGE_SYNC,
        },
    }[event_type]
    if kind not in expected:
        raise KnowledgeWorkerTerminalFailure("knowledge.event_job_mismatch")


__all__ = ["KNOWLEDGE_WORK_EVENT_TYPES", "KnowledgeWorkerService"]
