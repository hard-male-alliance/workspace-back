"""@brief API V2 Agent 领域的内存与 PostgreSQL 持久化 / In-memory and PostgreSQL persistence for API V2 Agent.

本模块将 Conversation、append-only Message、AgentRun、ToolApproval、统一 Job、
transactional outbox 与 AuditEvent 放在一个短事务中。公开请求先经中央
``AccessAuthorizer`` 鉴权；worker 只能从已提交 dispatch 中携带的真实 Run 创建者
快照建立 RLS（Row-Level Security）作用域。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from functools import partial
from types import TracebackType
from typing import Any, Protocol, Self, cast

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import and_, or_, select, tuple_, update
from sqlalchemy.engine import CursorResult, Result
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from backend.application.ports.access import AccessAuthorizer
from backend.application.ports.agent_v2 import (
    AgentCasMismatch,
    AgentContextResolver,
    AgentModelRoute,
    AgentPage,
    AgentPageRequest,
    AgentPermission,
    AgentPermissionGrant,
    AgentPermissionRequest,
    AgentPolicyDenied,
    AgentRunExecutionClaim,
    AgentRunPolicyRequest,
    AgentToolDecisionClaim,
    MessageSequenceReservation,
)
from backend.domain.agent_v2 import (
    AGENT_RUN_JOB_KIND,
    AgentExecutionGrant,
    AgentOutboxRecord,
    AgentRun,
    AgentRunId,
    AgentRunQueuedDispatch,
    AgentRunSpec,
    AgentRunStatus,
    AgentRunView,
    AgentUsage,
    AuthorizedKnowledgeContext,
    Conversation,
    ConversationCapability,
    ConversationId,
    ConversationStatus,
    Message,
    MessageContentPart,
    MessageId,
    MessageRole,
    ToolApproval,
    ToolApprovalId,
    ToolApprovalStatus,
    ToolCallBinding,
    ToolCallId,
    ToolDecisionDispatch,
    ToolRisk,
)
from backend.domain.knowledge_retrieval import (
    InferenceIntent,
    KnowledgeSelection,
    KnowledgeSelectionMode,
    evaluate_visibility,
)
from backend.domain.knowledge_sources import (
    AgentScopeGrant,
    KnowledgeOperation,
    KnowledgeSensitivity,
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    KnowledgeVersionStatus,
    KnowledgeVisibilityPolicy,
    ModelRegion,
    PolicyEffect,
)
from backend.domain.outbox import initial_outbox_lifecycle
from backend.domain.platform import (
    AuditEvent,
    AuditEventId,
    Job,
    JobId,
    JobProgress,
    JobProgressUnit,
    JobStatus,
    JsonValue,
    ProblemDetails,
)
from backend.domain.principals import (
    ResourceMeta,
    TokenPrincipal,
    UserId,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef
from backend.infrastructure.access import (
    InMemoryAccessRepository,
    InMemoryAccessStore,
    PostgresAccessRepository,
)
from backend.infrastructure.agent_resume_proposals import (
    PostgresAgentResumeProposalBoundary,
    UnavailableAgentResumeProposalBoundary,
)
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    AgentRunRecord,
    AuditEventRecord,
    ChatMessageRecord,
    ConversationRecord,
    JobRecord,
    JsonObject,
    KnowledgeSourceRecord,
    KnowledgeSourceVersionRecord,
    KnowledgeVisibilityGrantRecord,
    KnowledgeVisibilityPolicyRecord,
    OutboxEventRecord,
    ResumeDocumentRecord,
    ToolApprovalRecord,
)

_CONTENT_ADAPTER: TypeAdapter[tuple[MessageContentPart, ...]] = TypeAdapter(
    tuple[MessageContentPart, ...]
)
"""@brief Message content JSONB codec / Message content JSONB codec."""

_RUN_SPEC_ADAPTER: TypeAdapter[AgentRunSpec] = TypeAdapter(AgentRunSpec)
"""@brief AgentRunSpec JSONB codec / AgentRunSpec JSONB codec."""

_EXECUTION_GRANT_ADAPTER: TypeAdapter[AgentExecutionGrant] = TypeAdapter(AgentExecutionGrant)
"""@brief AgentExecutionGrant JSONB codec / AgentExecutionGrant JSONB codec."""

_RESOURCE_REFS_ADAPTER: TypeAdapter[tuple[ResourceRef, ...]] = TypeAdapter(tuple[ResourceRef, ...])
"""@brief ResourceRef 列表 JSONB codec / ResourceRef-list JSONB codec."""

_USAGE_ADAPTER: TypeAdapter[AgentUsage] = TypeAdapter(AgentUsage)
"""@brief AgentUsage JSONB codec / AgentUsage JSONB codec."""

_PROBLEM_ADAPTER: TypeAdapter[ProblemDetails] = TypeAdapter(ProblemDetails)
"""@brief ProblemDetails JSONB codec / ProblemDetails JSONB codec."""

_EVENT_RETENTION = timedelta(days=30)
"""@brief Agent outbox 可重放保留期 / Agent-outbox replay retention."""

_PERMISSION_ACTION: Mapping[AgentPermission, WorkspaceAction] = {
    AgentPermission.LIST_CONVERSATIONS: WorkspaceAction.LIST_CONVERSATIONS,
    AgentPermission.CREATE_CONVERSATION: WorkspaceAction.CREATE_CONVERSATION,
    AgentPermission.READ_CONVERSATION: WorkspaceAction.READ_CONVERSATION,
    AgentPermission.UPDATE_CONVERSATION: WorkspaceAction.UPDATE_CONVERSATION,
    AgentPermission.DELETE_CONVERSATION: WorkspaceAction.DELETE_CONVERSATION,
    AgentPermission.LIST_MESSAGES: WorkspaceAction.LIST_MESSAGES,
    AgentPermission.CREATE_MESSAGE: WorkspaceAction.CREATE_MESSAGE,
    AgentPermission.CREATE_AGENT_RUN: WorkspaceAction.CREATE_AGENT_RUN,
    AgentPermission.READ_AGENT_RUN: WorkspaceAction.READ_AGENT_RUN,
    AgentPermission.CANCEL_AGENT_RUN: WorkspaceAction.CANCEL_AGENT_RUN,
    AgentPermission.READ_TOOL_APPROVAL: WorkspaceAction.READ_TOOL_APPROVAL,
    AgentPermission.DECIDE_TOOL_APPROVAL: WorkspaceAction.DECIDE_TOOL_APPROVAL,
}
"""@brief Agent permission 到中央 Workspace action 的穷尽映射 / Exhaustive Agent-permission mapping."""


def _dump_object[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> JsonObject:
    """@brief 将领域值编码为 JSON object / Encode a domain value as a JSON object."""
    payload = adapter.dump_python(value, mode="json")
    if not isinstance(payload, dict):
        raise TypeError("Agent persistence codec must produce an object")
    return cast(JsonObject, payload)


def _dump_array[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> list[JsonObject]:
    """@brief 将领域值编码为 JSON object array / Encode a domain value as a JSON-object array."""
    payload = adapter.dump_python(value, mode="json")
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise TypeError("Agent persistence codec must produce an object array")
    return cast(list[JsonObject], payload)


def _dump_problem(problem: ProblemDetails) -> JsonObject:
    """@brief 将深度冻结 ProblemDetails 编码为可写 JSONB / Encode deeply frozen ProblemDetails as writable JSONB.

    @param problem 已通过领域不变量的公开问题 / Public-safe problem satisfying domain
        invariants.
    @return 不含 ``MappingProxyType`` 的 JSON object / JSON object without
        ``MappingProxyType`` values.
    @note Pydantic 当前不能把领域层用于深度不可变的 mapping proxy 直接序列化为 JSON；
        此边界显式 thaw 后仍由读取侧 TypeAdapter 重新校验并冻结。/ Pydantic cannot
        currently serialize the mapping proxies used for deep domain immutability directly to
        JSON; this boundary explicitly thaws them, while the read-side TypeAdapter validates and
        freezes the value again.
    """
    return {
        "type_uri": problem.type_uri,
        "title": problem.title,
        "status": problem.status,
        "code": problem.code,
        "request_id": problem.request_id,
        "retryable": problem.retryable,
        "detail": problem.detail,
        "instance": problem.instance,
        "errors": [
            {
                "pointer": error.pointer,
                "code": error.code,
                "message_key": error.message_key,
                "params": dict(error.params),
            }
            for error in problem.errors
        ],
        "extensions": {
            key: _thaw_json(value) for key, value in problem.extensions.items()
        },
    }


def _thaw_json(value: JsonValue) -> object:
    """@brief 递归复制不可变 JSON 容器 / Recursively copy immutable JSON containers.

    @param value 深度不可变 JSON 值 / Deeply immutable JSON value.
    @return asyncpg JSON codec 可接受的 dict/list/scalar / Dict, list, or scalar accepted by
        the asyncpg JSON codec.
    """
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _load[ValueT](adapter: TypeAdapter[ValueT], payload: object, label: str) -> ValueT:
    """@brief 从不可信 JSONB 重建领域值 / Rebuild a domain value from untrusted JSONB."""
    try:
        return adapter.validate_python(payload)
    except (ValidationError, ValueError, TypeError) as error:
        raise ValueError(f"persisted {label} violates the API V2 domain model") from error


def _affected_rows(result: Result[Any]) -> int:
    """@brief 返回 DML 受影响行数 / Return the affected-row count of a DML result."""
    if not isinstance(result, CursorResult):
        raise RuntimeError("Agent persistence expected a cursor result")
    return result.rowcount


def _encode_created_position(at: datetime, identifier: str) -> str:
    """@brief 编码 ``(created_at,id)`` keyset / Encode a ``(created_at,id)`` keyset."""
    return json.dumps([at.isoformat(), identifier], separators=(",", ":"))


def _decode_created_position(position: str | None) -> tuple[datetime, str] | None:
    """@brief 解码且严格校验 ``(created_at,id)`` keyset / Decode and validate a created-time keyset."""
    if position is None:
        return None
    try:
        payload = json.loads(position)
        if (
            not isinstance(payload, list)
            or len(payload) != 2
            or not all(isinstance(item, str) for item in payload)
        ):
            raise ValueError
        at = datetime.fromisoformat(payload[0])
        if at.tzinfo is None or not payload[1]:
            raise ValueError
        return at, payload[1]
    except (ValueError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("invalid Agent created-at keyset") from error


def _encode_message_position(sequence: int, identifier: str) -> str:
    """@brief 编码 ``(sequence,id)`` keyset / Encode a ``(sequence,id)`` keyset."""
    return json.dumps([sequence, identifier], separators=(",", ":"))


def _decode_message_position(position: str | None) -> tuple[int, str] | None:
    """@brief 解码且严格校验 Message keyset / Decode and validate a Message keyset."""
    if position is None:
        return None
    try:
        payload = json.loads(position)
        if (
            not isinstance(payload, list)
            or len(payload) != 2
            or isinstance(payload[0], bool)
            or not isinstance(payload[0], int)
            or payload[0] < 1
            or not isinstance(payload[1], str)
            or not payload[1]
        ):
            raise ValueError
        return payload[0], payload[1]
    except (ValueError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("invalid Agent message keyset") from error


class _V2ScopeInstaller(Protocol):
    """@brief 安装 V2 actor/workspace GUC 的 callable / Callable installing V2 actor/workspace GUCs."""

    async def __call__(self, *, actor_id: str, workspace_id: str | None) -> None:
        """@brief 安装事务局部作用域 / Install the transaction-local scope."""


class _TrackingAgentAuthorizer:
    """@brief 密封一个 UoW 的中央授权与真实身份 / Seal central authorization and real identity to one UoW."""

    def __init__(
        self,
        delegate: AccessAuthorizer | None,
        scope_installer: _V2ScopeInstaller | None = None,
        *,
        worker_scope: tuple[WorkspaceId, UserId] | None = None,
    ) -> None:
        """@brief 绑定中央 authorizer 或已验证 worker scope / Bind the central authorizer or a verified worker scope."""
        self._delegate = delegate
        self._scope_installer = scope_installer
        self.actor_id = None if worker_scope is None else worker_scope[1]
        self.workspace_id = None if worker_scope is None else worker_scope[0]
        self._worker = worker_scope is not None

    async def install_worker_scope(self) -> None:
        """@brief 在任何业务读之前安装 worker RLS scope / Install worker RLS scope before every business read."""
        if not self._worker or self.actor_id is None or self.workspace_id is None:
            raise RuntimeError("Agent worker scope is incomplete")
        if self._scope_installer is not None:
            await self._scope_installer(
                actor_id=str(self.actor_id),
                workspace_id=str(self.workspace_id),
            )

    async def authorize(
        self,
        principal: TokenPrincipal,
        request: AgentPermissionRequest,
    ) -> AgentPermissionGrant:
        """@brief 经中央 AccessAuthorizer 签发精确 Agent grant / Issue an exact Agent grant through the central authorizer."""
        if self._worker or self._delegate is None:
            raise PermissionError("worker Agent units of work cannot authorize public requests")
        if self.actor_id is not None and self.actor_id != principal.user_id:
            raise PermissionError("an Agent unit of work cannot switch actors")
        if self.workspace_id is not None and self.workspace_id != request.workspace_id:
            raise PermissionError("an Agent unit of work cannot switch workspaces")
        # TokenPrincipal 已经密码学验证；先安装其真实 local user ID，才能在
        # FORCE-RLS 下读取 identity.users 并完成本地 subject 绑定。
        if self._scope_installer is not None:
            await self._scope_installer(actor_id=str(principal.user_id), workspace_id=None)
        actor = await self._delegate.authenticate(principal)
        if actor.user_id != principal.user_id:
            raise PermissionError("Agent authentication returned a mismatched actor")
        if self._scope_installer is not None:
            await self._scope_installer(
                actor_id=str(actor.user_id),
                workspace_id=str(request.workspace_id),
            )
        await self._delegate.authorize(
            actor,
            request.workspace_id,
            _PERMISSION_ACTION[request.permission],
        )
        self.actor_id = actor.user_id
        self.workspace_id = request.workspace_id
        return AgentPermissionGrant(actor.user_id, request)

    def require_actor(self) -> UserId:
        """@brief 返回 UoW 已固定 actor / Return the actor pinned to the UoW."""
        if self.actor_id is None:
            raise PermissionError("Agent persistence requires an authenticated or dispatch actor")
        return self.actor_id

    def require_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 验证 repository 首参与已授权 Workspace 一致 / Verify the repository workspace matches the authorized scope."""
        if self.workspace_id != workspace_id:
            raise PermissionError("Agent persistence requires prior exact Workspace authorization")

    @property
    def is_worker(self) -> bool:
        """@brief 判断当前 UoW 是否由 durable dispatch 建立 / Report whether a durable dispatch established this UoW."""
        return self._worker


@dataclass(frozen=True, slots=True)
class InMemoryKnowledgePolicyEntry:
    """@brief 内存 Agent policy 的最小 Knowledge 真相 / Minimal Knowledge truth for the in-memory Agent policy."""

    workspace_id: WorkspaceId
    source_id: KnowledgeSourceId
    enabled: bool
    current_version_id: KnowledgeSourceVersionId | None
    ready_version_ids: frozenset[KnowledgeSourceVersionId]
    policy: KnowledgeVisibilityPolicy


@dataclass(slots=True)
class InMemoryAgentPolicyStore:
    """@brief 用于测试和本地运行的跨域策略真相 / Cross-domain policy truth for tests and local execution."""

    contexts: dict[tuple[WorkspaceId, str, str], ResourceRef] = field(default_factory=dict)
    knowledge: dict[tuple[WorkspaceId, KnowledgeSourceId], InMemoryKnowledgePolicyEntry] = field(
        default_factory=dict
    )


class InMemoryAgentContextResolver:
    """@brief 从显式注入真相批量解析 context refs / Resolve context references from explicitly injected truth."""

    def __init__(self, store: InMemoryAgentPolicyStore) -> None:
        """@brief 绑定策略 store / Bind the policy store."""
        self._store = store

    async def resolve(
        self,
        workspace_id: WorkspaceId,
        references: tuple[ResourceRef, ...],
    ) -> tuple[ResourceRef, ...]:
        """@brief 按请求顺序解析精确 revision / Resolve exact revisions in request order."""
        resolved: list[ResourceRef] = []
        for reference in references:
            current = self._store.contexts.get(
                (workspace_id, reference.resource_type, reference.id)
            )
            if current is None or (
                reference.revision is not None and reference.revision != current.revision
            ):
                raise AgentPolicyDenied("agent context is absent, stale, or outside the Workspace")
            resolved.append(current)
        return tuple(resolved)


class InMemoryAgentRunPolicy:
    """@brief 与 PostgreSQL 实现等价的内存 fail-closed Run policy / In-memory fail-closed Run policy equivalent to PostgreSQL."""

    def __init__(
        self,
        store: InMemoryAgentPolicyStore,
        context_resolver: AgentContextResolver,
        model_routes: Sequence[AgentModelRoute],
    ) -> None:
        """@brief 注入 context、Knowledge 与 model 真相 / Inject context, Knowledge, and model truth."""
        self._store = store
        self._contexts = context_resolver
        self._routes = tuple(model_routes)

    async def authorize_run(self, request: AgentRunPolicyRequest) -> AgentExecutionGrant:
        """@brief 授权 session/context/Knowledge/model 交集 / Authorize the session/context/Knowledge/model intersection."""
        route = _select_model_route(self._routes, request.spec.inference)
        contexts = await self._contexts.resolve(
            request.workspace_id,
            request.spec.context_refs,
        )
        entries = {
            source_id: entry
            for (workspace_id, source_id), entry in self._store.knowledge.items()
            if workspace_id == request.workspace_id
        }
        knowledge = _authorize_knowledge_entries(
            request.spec.knowledge, request.spec.inference, route, entries
        )
        return _execution_grant(request, route, contexts, knowledge)


def _select_model_route(
    routes: Sequence[AgentModelRoute],
    inference: InferenceIntent,
) -> AgentModelRoute:
    """@brief 选择严格匹配 region/external 意图的模型路由 / Select a model route strictly compatible with region/external intent."""
    for route in routes:
        if route.data_region is not inference.data_region:
            continue
        if route.external_processing and not inference.allow_external_model_processing:
            continue
        return route
    raise AgentPolicyDenied("no configured model route satisfies the inference intent")


def _authorize_knowledge_entries(
    selection: KnowledgeSelection,
    inference: InferenceIntent,
    route: AgentModelRoute,
    entries: Mapping[KnowledgeSourceId, InMemoryKnowledgePolicyEntry],
) -> tuple[AuthorizedKnowledgeContext, ...]:
    """@brief 以 deny-first 规则授权已批量读取的 Knowledge 条目 / Authorize batch-loaded Knowledge entries deny-first."""
    if selection.mode is KnowledgeSelectionMode.NONE:
        return ()
    included = set(selection.include_source_ids)
    excluded = set(selection.exclude_source_ids)
    if selection.mode is KnowledgeSelectionMode.EXPLICIT:
        selected_ids = list(selection.include_source_ids)
    else:
        selected_ids = sorted(
            (
                source_id
                for source_id, entry in entries.items()
                if source_id not in excluded and (entry.enabled or source_id in included)
            ),
            key=str,
        )
    if len(selected_ids) > 200:
        raise AgentPolicyDenied("Knowledge policy_default selection exceeds 200 sources")
    if included - set(selected_ids):
        raise AgentPolicyDenied("an explicitly included Knowledge source does not exist")
    pins = {pin.source_id: pin.version_id for pin in selection.pinned_versions}
    if set(pins) - set(selected_ids):
        raise AgentPolicyDenied("a Knowledge pin does not belong to the selected source set")
    effective_inference = replace(
        inference,
        data_region=route.data_region,
        allow_external_model_processing=route.external_processing,
    )
    contexts: list[AuthorizedKnowledgeContext] = []
    for source_id in selected_ids:
        entry = entries.get(source_id)
        if entry is None:
            raise AgentPolicyDenied("a selected Knowledge source is unavailable")
        version_id = pins.get(source_id, entry.current_version_id)
        if version_id is None or version_id not in entry.ready_version_ids:
            raise AgentPolicyDenied("a selected Knowledge version is absent or not ready")
        decision = evaluate_visibility(
            source_id=source_id,
            enabled=entry.enabled,
            policy=entry.policy,
            agent_scope=selection.agent_scope,
            operation=KnowledgeOperation.DERIVE,
            inference=effective_inference,
        )
        if decision.effect is not PolicyEffect.ALLOW:
            raise AgentPolicyDenied(decision.reason_codes[0])
        contexts.append(AuthorizedKnowledgeContext(source_id, version_id, decision.policy_version))
    return tuple(contexts)


def _execution_grant(
    request: AgentRunPolicyRequest,
    route: AgentModelRoute,
    contexts: tuple[ResourceRef, ...],
    knowledge: tuple[AuthorizedKnowledgeContext, ...],
) -> AgentExecutionGrant:
    """@brief 构造并交叉验证不可变 execution grant / Build and cross-validate an immutable execution grant."""
    policy_version = max(
        (1, route.model_ref.revision or 1, *(item.policy_version for item in knowledge))
    )
    grant = AgentExecutionGrant(
        session_ref=ResourceRef(
            "conversation",
            request.conversation.meta.id,
            request.conversation.meta.revision,
        ),
        agent_scope=request.spec.knowledge.agent_scope,
        model_ref=route.model_ref,
        model_region=route.data_region,
        external_model_processing=route.external_processing,
        context_refs=contexts,
        knowledge_contexts=knowledge,
        policy_version=policy_version,
    )
    grant.validate_for(request.conversation, request.spec)
    return grant


@dataclass(slots=True)
class InMemoryAgentStore:
    """@brief Agent V2 的共享内存事务真相 / Shared in-memory transactional truth for Agent V2."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    conversations: dict[tuple[WorkspaceId, ConversationId], Conversation] = field(
        default_factory=dict
    )
    messages: dict[tuple[WorkspaceId, ConversationId, MessageId], Message] = field(
        default_factory=dict
    )
    runs: dict[tuple[WorkspaceId, AgentRunId], AgentRun] = field(default_factory=dict)
    approvals: dict[tuple[WorkspaceId, ToolApprovalId], ToolApproval] = field(default_factory=dict)
    jobs: dict[tuple[WorkspaceId, JobId], tuple[Job, UserId]] = field(default_factory=dict)
    outbox: dict[str, AgentOutboxRecord] = field(default_factory=dict)
    published_outbox_ids: set[str] = field(default_factory=set)
    audits: dict[AuditEventId, AuditEvent] = field(default_factory=dict)


class _AgentWorkWorker(Protocol):
    """@brief 内存 dispatcher 所需的窄 Agent worker 形状 / Narrow Agent-worker shape required by the memory dispatcher."""

    async def execute_run(self, dispatch: AgentRunExecutionClaim) -> AgentRunView:
        """@brief 幂等执行一条 queued dispatch / Idempotently execute one queued dispatch."""

    async def execute_approved_tool(self, dispatch: AgentToolDecisionClaim) -> AgentRunView:
        """@brief 幂等执行或终结一条工具决定 / Idempotently execute or terminate a tool decision."""


@dataclass(frozen=True, slots=True)
class InMemoryAgentDispatchResult:
    """@brief 一轮内存 Agent dispatch 计数 / Counters for one in-memory Agent dispatch pass."""

    claimed: int
    """@brief 本轮取得的 Agent 工作事件数 / Agent work events claimed in this pass."""

    completed: int
    """@brief 本轮成功处理并标记 published 的数量 / Events completed and marked published."""


class InMemoryAgentDispatchService:
    """@brief development/test 的有界 Agent outbox dispatcher / Bounded Agent outbox dispatcher for development/test."""

    def __init__(
        self,
        store: InMemoryAgentStore,
        worker: _AgentWorkWorker,
        *,
        batch_size: int = 25,
    ) -> None:
        """@brief 绑定共享 store 与 worker / Bind the shared store and worker.

        @param store 与 Agent UoW 共用的事务内存真相 / Transactional in-memory truth shared
            with Agent UoWs.
        @param worker 两段短事务 worker / Two-short-transaction worker.
        @param batch_size 单轮硬上限 / Hard per-pass bound.
        """
        if isinstance(batch_size, bool) or not 1 <= batch_size <= 100:
            raise ValueError("in-memory Agent dispatch batch size must be between 1 and 100")
        self._store = store
        self._worker = worker
        self._batch_size = batch_size

    async def run_once(self) -> InMemoryAgentDispatchResult:
        """@brief 不跨 worker I/O 持锁地处理一个批次 / Process one batch without holding a lock across worker I/O."""
        async with self._store.lock:
            candidates = tuple(
                sorted(
                    (
                        record
                        for record in self._store.outbox.values()
                        if isinstance(record, (AgentRunQueuedDispatch, ToolDecisionDispatch))
                        and str(record.id) not in self._store.published_outbox_ids
                    ),
                    key=lambda record: (record.occurred_at, str(record.id)),
                )[: self._batch_size]
            )
        completed = 0
        for record in candidates:
            if isinstance(record, AgentRunQueuedDispatch):
                await self._worker.execute_run(record)
            else:
                await self._worker.execute_approved_tool(record)
            async with self._store.lock:
                self._store.published_outbox_ids.add(str(record.id))
            completed += 1
        return InMemoryAgentDispatchResult(len(candidates), completed)


class InMemoryAgentRepository:
    """@brief copy-on-write snapshot 上的 Workspace-first Agent repository / Workspace-first Agent repository over a copy-on-write snapshot."""

    def __init__(
        self,
        authorizer: _TrackingAgentAuthorizer,
        conversations: dict[tuple[WorkspaceId, ConversationId], Conversation],
        messages: dict[tuple[WorkspaceId, ConversationId, MessageId], Message],
        runs: dict[tuple[WorkspaceId, AgentRunId], AgentRun],
        approvals: dict[tuple[WorkspaceId, ToolApprovalId], ToolApproval],
    ) -> None:
        """@brief 绑定事务快照 / Bind transaction snapshots."""
        self._auth = authorizer
        self._conversations = conversations
        self._messages = messages
        self._runs = runs
        self._approvals = approvals

    async def list_conversations(
        self,
        workspace_id: WorkspaceId,
        page: AgentPageRequest,
    ) -> AgentPage[Conversation]:
        """@brief 按 ``(created_at,id)`` 稳定分页 / Page stably by ``(created_at,id)``."""
        self._auth.require_workspace(workspace_id)
        after = _decode_created_position(page.after)
        items = sorted(
            (
                item
                for (owner, _), item in self._conversations.items()
                if owner == workspace_id
                and not item.is_deleted
                and (after is None or (item.meta.created_at, str(item.meta.id)) > after)
            ),
            key=lambda item: (item.meta.created_at, str(item.meta.id)),
        )
        return _memory_page_created(items, page.limit)

    async def get_conversation(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        for_update: bool = False,
        include_deleted: bool = False,
    ) -> Conversation | None:
        """@brief 在 Workspace 内读取 Conversation / Read a Conversation inside a Workspace."""
        del for_update
        self._auth.require_workspace(workspace_id)
        item = self._conversations.get((workspace_id, conversation_id))
        return item if item is not None and (include_deleted or not item.is_deleted) else None

    async def add_conversation(self, conversation: Conversation) -> None:
        """@brief 添加 Conversation / Add a Conversation."""
        self._auth.require_workspace(conversation.workspace_id)
        self._auth.require_actor()
        key = (conversation.workspace_id, conversation.meta.id)
        if key in self._conversations:
            raise AgentCasMismatch
        self._conversations[key] = conversation

    async def save_conversation(
        self,
        conversation: Conversation,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以 revision CAS 保存 Conversation / CAS-save a Conversation by revision."""
        self._auth.require_workspace(conversation.workspace_id)
        key = (conversation.workspace_id, conversation.meta.id)
        current = self._conversations.get(key)
        if (
            current is None
            or current.meta.revision != expected_revision
            or conversation.meta.revision != expected_revision + 1
        ):
            raise AgentCasMismatch
        self._conversations[key] = conversation

    async def has_nonterminal_runs(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> bool:
        """@brief 判断是否存在非终态 Run / Test whether a non-terminal Run exists."""
        self._auth.require_workspace(workspace_id)
        return any(
            owner == workspace_id
            and run.spec.conversation_id == conversation_id
            and not run.is_terminal
            for (owner, _), run in self._runs.items()
        )

    async def list_messages(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[Message]:
        """@brief 按 ``(sequence,id)`` 稳定分页 / Page stably by ``(sequence,id)``."""
        self._auth.require_workspace(workspace_id)
        after = _decode_message_position(page.after)
        items = sorted(
            (
                item
                for (owner, conversation, _), item in self._messages.items()
                if owner == workspace_id
                and conversation == conversation_id
                and (after is None or (item.sequence, str(item.meta.id)) > after)
            ),
            key=lambda item: (item.sequence, str(item.meta.id)),
        )
        window = items[: page.limit + 1]
        selected = tuple(window[: page.limit])
        position = (
            _encode_message_position(selected[-1].sequence, str(selected[-1].meta.id))
            if len(window) > page.limit
            else None
        )
        return AgentPage(selected, position)

    async def get_message(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        message_id: MessageId,
    ) -> Message | None:
        """@brief 以 Workspace/Conversation/Message 三元组读取 / Read by Workspace/Conversation/Message tuple."""
        self._auth.require_workspace(workspace_id)
        return self._messages.get((workspace_id, conversation_id, message_id))

    async def allocate_message_sequence(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        expected_conversation_revision: int | None,
        at: datetime,
    ) -> MessageSequenceReservation:
        """@brief 原子分配 Message sequence 并推进 Conversation revision / Atomically allocate sequence and advance Conversation revision."""
        self._auth.require_workspace(workspace_id)
        key = (workspace_id, conversation_id)
        conversation = self._conversations.get(key)
        if (
            conversation is None
            or not conversation.is_writable
            or (
                expected_conversation_revision is not None
                and conversation.meta.revision != expected_conversation_revision
            )
        ):
            raise AgentCasMismatch
        sequence = (
            max(
                (
                    item.sequence
                    for (owner, parent, _), item in self._messages.items()
                    if owner == workspace_id and parent == conversation_id
                ),
                default=0,
            )
            + 1
        )
        advanced = replace(conversation, meta=conversation.meta.advance(at))
        self._conversations[key] = advanced
        return MessageSequenceReservation(sequence, advanced.meta.revision, at)

    async def add_message(self, message: Message) -> None:
        """@brief 添加 append-only Message / Add an append-only Message."""
        self._auth.require_workspace(message.workspace_id)
        self._auth.require_actor()
        key = (message.workspace_id, message.conversation_id, message.meta.id)
        if key in self._messages or any(
            owner == message.workspace_id
            and conversation == message.conversation_id
            and current.sequence == message.sequence
            for (owner, conversation, _), current in self._messages.items()
        ):
            raise AgentCasMismatch
        self._messages[key] = message

    async def list_runs_for_conversation(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[AgentRun]:
        """@brief 稳定列出 Conversation Runs / Stably list Conversation Runs."""
        self._auth.require_workspace(workspace_id)
        after = _decode_created_position(page.after)
        items = sorted(
            (
                run
                for (owner, _), run in self._runs.items()
                if owner == workspace_id
                and run.spec.conversation_id == conversation_id
                and (after is None or (run.meta.created_at, str(run.meta.id)) > after)
            ),
            key=lambda run: (run.meta.created_at, str(run.meta.id)),
        )
        return _memory_page_created(items, page.limit)

    async def get_run(
        self,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        *,
        for_update: bool = False,
    ) -> AgentRun | None:
        """@brief 在 Workspace 内读取 Run / Read a Run inside a Workspace."""
        del for_update
        self._auth.require_workspace(workspace_id)
        run = self._runs.get((workspace_id, run_id))
        if (
            self._auth.is_worker
            and run is not None
            and run.created_by != self._auth.require_actor()
        ):
            return None
        return run

    async def add_run(self, run: AgentRun) -> None:
        """@brief 添加带冻结执行快照的 Run / Add a Run with frozen execution snapshots."""
        self._auth.require_workspace(run.workspace_id)
        if run.created_by != self._auth.require_actor():
            raise PermissionError("Agent run creator must match the authenticated actor")
        key = (run.workspace_id, run.meta.id)
        if key in self._runs:
            raise AgentCasMismatch
        self._runs[key] = run

    async def save_run(self, run: AgentRun, *, expected_revision: int) -> None:
        """@brief 以 revision CAS 保存 Run / CAS-save a Run by revision."""
        self._auth.require_workspace(run.workspace_id)
        key = (run.workspace_id, run.meta.id)
        current = self._runs.get(key)
        if (
            current is None
            or current.created_by != run.created_by
            or (self._auth.is_worker and current.created_by != self._auth.require_actor())
            or current.meta.revision != expected_revision
            or run.meta.revision != expected_revision + 1
        ):
            raise AgentCasMismatch
        self._runs[key] = run

    async def get_approval(
        self,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
        *,
        for_update: bool = False,
    ) -> ToolApproval | None:
        """@brief 在 Workspace 内读取 ToolApproval / Read a ToolApproval inside a Workspace."""
        del for_update
        self._auth.require_workspace(workspace_id)
        return self._approvals.get((workspace_id, approval_id))

    async def add_approval(self, approval: ToolApproval) -> None:
        """@brief 添加 ToolApproval / Add a ToolApproval."""
        self._auth.require_workspace(approval.workspace_id)
        self._auth.require_actor()
        key = (approval.workspace_id, approval.meta.id)
        if key in self._approvals:
            raise AgentCasMismatch
        self._approvals[key] = approval

    async def save_approval(
        self,
        approval: ToolApproval,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以 revision CAS 一次性保存决定 / CAS-save a one-time decision."""
        self._auth.require_workspace(approval.workspace_id)
        key = (approval.workspace_id, approval.meta.id)
        current = self._approvals.get(key)
        if (
            current is None
            or current.meta.revision != expected_revision
            or approval.meta.revision != expected_revision + 1
        ):
            raise AgentCasMismatch
        self._approvals[key] = approval


def _memory_page_created[ItemT](items: Sequence[ItemT], limit: int) -> AgentPage[ItemT]:
    """@brief 将已按 ResourceMeta 排序的内存项目切成 keyset 页 / Slice ResourceMeta-bearing in-memory items into a keyset page."""
    window = items[: limit + 1]
    selected = tuple(window[:limit])
    if len(window) <= limit:
        return AgentPage(selected, None)
    last = cast(Any, selected[-1]).meta
    return AgentPage(selected, _encode_created_position(last.created_at, str(last.id)))


class _MemoryAgentJobStore:
    """@brief 内存统一 Job store / In-memory unified Job store."""

    def __init__(
        self,
        jobs: dict[tuple[WorkspaceId, JobId], tuple[Job, UserId]],
        authorizer: _TrackingAgentAuthorizer,
    ) -> None:
        """@brief 绑定 Job snapshot 与作用域 / Bind the Job snapshot and scope."""
        self._jobs = jobs
        self._auth = authorizer

    async def add(self, job: Job) -> None:
        """@brief 添加统一 Job / Add a unified Job."""
        self._auth.require_workspace(job.workspace_id)
        actor = self._auth.require_actor()
        key = (job.workspace_id, job.meta.id)
        if key in self._jobs:
            raise AgentCasMismatch
        self._jobs[key] = (job, actor)

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 在 Workspace 内读取统一 Job / Read a unified Job inside a Workspace."""
        del for_update
        self._auth.require_workspace(workspace_id)
        stored = self._jobs.get((workspace_id, job_id))
        if stored is None:
            return None
        if self._auth.is_worker and stored[1] != self._auth.require_actor():
            return None
        return stored[0]

    async def save(self, job: Job, *, expected_revision: int) -> None:
        """@brief 以 revision CAS 统一 Job / CAS-save a unified Job."""
        self._auth.require_workspace(job.workspace_id)
        key = (job.workspace_id, job.meta.id)
        stored = self._jobs.get(key)
        if (
            stored is None
            or stored[0].meta.revision != expected_revision
            or job.meta.revision != expected_revision + 1
            or (self._auth.is_worker and stored[1] != self._auth.require_actor())
        ):
            raise AgentCasMismatch
        self._jobs[key] = (job, stored[1])


class _MemoryAgentOutbox:
    """@brief 与领域写共用 transaction 的内存 outbox / In-memory outbox sharing the domain transaction."""

    def __init__(
        self,
        records: dict[str, AgentOutboxRecord],
        runs: dict[tuple[WorkspaceId, AgentRunId], AgentRun],
        authorizer: _TrackingAgentAuthorizer,
    ) -> None:
        """@brief 绑定记录、Run 真相与作用域 / Bind records, Run truth, and scope."""
        self._records = records
        self._runs = runs
        self._auth = authorizer

    async def add(self, record: AgentOutboxRecord) -> None:
        """@brief 添加封闭且无私有推理的 dispatch / Add a closed dispatch without private reasoning."""
        self._auth.require_workspace(record.workspace_id)
        run = self._runs.get((record.workspace_id, AgentRunId(record.run_ref.id)))
        if run is None or run.created_by != record.actor_id:
            raise PermissionError("Agent outbox actor must match the persisted Run creator")
        if str(record.id) in self._records:
            raise AgentCasMismatch
        self._records[str(record.id)] = record


class _MemoryAgentAuditSink:
    """@brief 内存统一 AuditEvent sink / In-memory unified AuditEvent sink."""

    def __init__(
        self,
        events: dict[AuditEventId, AuditEvent],
        authorizer: _TrackingAgentAuthorizer,
    ) -> None:
        """@brief 绑定 audit snapshot 与作用域 / Bind the audit snapshot and scope."""
        self._events = events
        self._auth = authorizer

    async def add(self, event: AuditEvent) -> None:
        """@brief 添加统一 AuditEvent / Add a unified AuditEvent."""
        self._auth.require_workspace(event.workspace_id)
        if event.actor.id != self._auth.require_actor():
            raise PermissionError("Agent audit actor must match the authenticated actor")
        if event.id in self._events:
            raise AgentCasMismatch
        self._events[event.id] = event


class InMemoryAgentUnitOfWork:
    """@brief Agent/Access 固定顺序加锁的 copy-on-write UoW / Copy-on-write UoW locking Agent and Access in fixed order."""

    def __init__(
        self,
        store: InMemoryAgentStore,
        access_store: InMemoryAccessStore,
        policy_store: InMemoryAgentPolicyStore,
        model_routes: Sequence[AgentModelRoute],
        *,
        worker_scope: tuple[WorkspaceId, UserId] | None = None,
    ) -> None:
        """@brief 绑定共享 stores 与可选 worker scope / Bind shared stores and an optional worker scope."""
        self._store = store
        self._access_store = access_store
        self._policy_store = policy_store
        self._routes = tuple(model_routes)
        self._worker_scope = worker_scope
        self._repository: InMemoryAgentRepository | None = None
        self._authorizer: _TrackingAgentAuthorizer | None = None
        self._policy: InMemoryAgentRunPolicy | None = None
        self._jobs: _MemoryAgentJobStore | None = None
        self._outbox: _MemoryAgentOutbox | None = None
        self._audit: _MemoryAgentAuditSink | None = None
        self._resume_proposals: UnavailableAgentResumeProposalBoundary | None = None
        self._snapshot: tuple[Any, ...] | None = None
        self._entered = False
        self._committed = False
        self._rolled_back = False

    @property
    def repository(self) -> InMemoryAgentRepository:
        """@brief 返回事务 repository / Return the transactional repository."""
        if self._repository is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._repository

    @property
    def authorizer(self) -> _TrackingAgentAuthorizer:
        """@brief 返回中央 authorizer adapter / Return the central-authorizer adapter."""
        if self._authorizer is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._authorizer

    @property
    def policy(self) -> InMemoryAgentRunPolicy:
        """@brief 返回本地 Run policy / Return the local Run policy."""
        if self._policy is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._policy

    @property
    def jobs(self) -> _MemoryAgentJobStore:
        """@brief 返回统一 Job store / Return the unified Job store."""
        if self._jobs is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._jobs

    @property
    def outbox(self) -> _MemoryAgentOutbox:
        """@brief 返回 transactional outbox / Return the transactional outbox."""
        if self._outbox is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._outbox

    @property
    def audit(self) -> _MemoryAgentAuditSink:
        """@brief 返回统一 audit sink / Return the unified audit sink."""
        if self._audit is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._audit

    @property
    def resume_proposals(self) -> UnavailableAgentResumeProposalBoundary:
        """@brief 返回显式 fail-closed 的 memory Proposal 边界 / Return the explicitly fail-closed memory Proposal boundary."""
        if self._resume_proposals is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._resume_proposals

    async def __aenter__(self) -> Self:
        """@brief 按 Access→Agent 固定顺序加锁并建立快照 / Lock Access then Agent and create snapshots."""
        if self._entered:
            raise RuntimeError("Agent unit of work cannot be re-entered")
        await self._access_store.lock.acquire()
        try:
            await self._store.lock.acquire()
        except BaseException:
            self._access_store.lock.release()
            raise
        self._entered = True
        conversations = dict(self._store.conversations)
        messages = dict(self._store.messages)
        runs = dict(self._store.runs)
        approvals = dict(self._store.approvals)
        jobs = dict(self._store.jobs)
        outbox = dict(self._store.outbox)
        audits = dict(self._store.audits)
        self._snapshot = (conversations, messages, runs, approvals, jobs, outbox, audits)
        access = None
        if self._worker_scope is None:
            access_repository = InMemoryAccessRepository(
                users=dict(self._access_store.users),
                workspaces=dict(self._access_store.workspaces),
                memberships=dict(self._access_store.memberships),
                invitations=dict(self._access_store.invitations),
                account_deletions=dict(self._access_store.account_deletions),
            )
            access = AccessAuthorizer(access_repository)
        self._authorizer = _TrackingAgentAuthorizer(
            access,
            worker_scope=self._worker_scope,
        )
        resolver = InMemoryAgentContextResolver(self._policy_store)
        self._policy = InMemoryAgentRunPolicy(self._policy_store, resolver, self._routes)
        self._repository = InMemoryAgentRepository(
            self._authorizer,
            conversations,
            messages,
            runs,
            approvals,
        )
        self._jobs = _MemoryAgentJobStore(jobs, self._authorizer)
        self._outbox = _MemoryAgentOutbox(outbox, runs, self._authorizer)
        self._audit = _MemoryAgentAuditSink(audits, self._authorizer)
        self._resume_proposals = UnavailableAgentResumeProposalBoundary()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 回滚未提交快照并释放锁 / Discard uncommitted snapshots and release locks."""
        del exc, traceback
        if exc_type is not None or not self._committed:
            await self.rollback()
        if self._entered:
            self._store.lock.release()
            self._access_store.lock.release()
        self._entered = False
        self._repository = None
        self._authorizer = None
        self._policy = None
        self._jobs = None
        self._outbox = None
        self._audit = None
        self._resume_proposals = None
        return None

    async def commit(self) -> None:
        """@brief 原子替换全部 Agent/Job/outbox/audit 快照 / Atomically replace all Agent/Job/outbox/audit snapshots."""
        if not self._entered or self._snapshot is None or self._committed or self._rolled_back:
            raise RuntimeError("Agent unit of work cannot commit in its current state")
        (
            self._store.conversations,
            self._store.messages,
            self._store.runs,
            self._store.approvals,
            self._store.jobs,
            self._store.outbox,
            self._store.audits,
        ) = self._snapshot
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等丢弃 copy-on-write 快照 / Idempotently discard copy-on-write snapshots."""
        self._rolled_back = True


class InMemoryAgentUnitOfWorkFactory:
    """@brief 创建公开 Agent 内存 UoW / Create public in-memory Agent UoWs."""

    def __init__(
        self,
        store: InMemoryAgentStore,
        access_store: InMemoryAccessStore,
        *,
        policy_store: InMemoryAgentPolicyStore,
        model_routes: Sequence[AgentModelRoute],
    ) -> None:
        """@brief 注入 stores 与不可变 model routes / Inject stores and immutable model routes."""
        self._store = store
        self._access_store = access_store
        self._policy_store = policy_store
        self._routes = tuple(model_routes)

    def __call__(self) -> InMemoryAgentUnitOfWork:
        """@brief 创建未进入的公开 UoW / Create an unentered public UoW."""
        return InMemoryAgentUnitOfWork(
            self._store,
            self._access_store,
            self._policy_store,
            self._routes,
        )


class InMemoryAgentWorkerUnitOfWorkFactory:
    """@brief 从真实 creator snapshot 创建内存 worker UoW / Create in-memory worker UoWs from real creator snapshots."""

    def __init__(
        self,
        store: InMemoryAgentStore,
        access_store: InMemoryAccessStore,
        *,
        policy_store: InMemoryAgentPolicyStore,
        model_routes: Sequence[AgentModelRoute],
    ) -> None:
        """@brief 注入共享 stores / Inject shared stores."""
        self._store = store
        self._access_store = access_store
        self._policy_store = policy_store
        self._routes = tuple(model_routes)

    def __call__(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
    ) -> InMemoryAgentUnitOfWork:
        """@brief 创建已密封 dispatch scope 的 UoW / Create a UoW sealed to the dispatch scope."""
        return InMemoryAgentUnitOfWork(
            self._store,
            self._access_store,
            self._policy_store,
            self._routes,
            worker_scope=(workspace_id, actor_id),
        )


class PostgresAgentContextResolver:
    """@brief 从各 bounded context 的唯一真相批量解析引用 / Batch-resolve refs from each bounded context's sole truth."""

    def __init__(self, session: AsyncSession) -> None:
        """@brief 绑定当前已授权 transaction / Bind the current authorized transaction."""
        self._session = session

    async def resolve(
        self,
        workspace_id: WorkspaceId,
        references: tuple[ResourceRef, ...],
    ) -> tuple[ResourceRef, ...]:
        """@brief 批量校验 Resume context 的 Workspace 与 revision / Batch-validate Resume-context Workspace and revision.

        @note 当前只有 Resume 聚合根拥有可靠的跨域 revision 真相；未知类型严格
            fail closed / Only the Resume aggregate root currently exposes reliable cross-domain
            revision truth; unknown types fail closed.
        """
        if not references:
            return ()
        if any(reference.resource_type != "resume" for reference in references):
            raise AgentPolicyDenied("an Agent context type has no reliable local resolver")
        identifiers = tuple(reference.id for reference in references)
        result = await self._session.execute(
            select(
                ResumeDocumentRecord.id,
                ResumeDocumentRecord.current_revision_no,
            ).where(
                ResumeDocumentRecord.workspace_id == str(workspace_id),
                ResumeDocumentRecord.id.in_(identifiers),
                ResumeDocumentRecord.deleted_at.is_(None),
            )
        )
        revisions = {identifier: revision for identifier, revision in result.all()}
        resolved: list[ResourceRef] = []
        for reference in references:
            current_revision = revisions.get(reference.id)
            if current_revision is None or (
                reference.revision is not None and reference.revision != current_revision
            ):
                raise AgentPolicyDenied(
                    "an Agent context is absent, stale, or outside the Workspace"
                )
            resolved.append(ResourceRef("resume", reference.id, current_revision))
        return tuple(resolved)


class PostgresAgentRunPolicy:
    """@brief 在同一本地 transaction 中交叉授权 Agent/Knowledge/model / Authorize Agent, Knowledge, and model constraints in one local transaction."""

    def __init__(
        self,
        session: AsyncSession,
        context_resolver: AgentContextResolver,
        model_routes: Sequence[AgentModelRoute],
    ) -> None:
        """@brief 注入事务、context resolver 和配置路由 / Inject transaction, context resolver, and configured routes."""
        self._session = session
        self._contexts = context_resolver
        self._routes = tuple(model_routes)

    async def authorize_run(self, request: AgentRunPolicyRequest) -> AgentExecutionGrant:
        """@brief 批量重新授权冻结 execution grant / Batch-reauthorize and freeze an execution grant."""
        route = _select_model_route(self._routes, request.spec.inference)
        contexts = await self._contexts.resolve(
            request.workspace_id,
            request.spec.context_refs,
        )
        entries = await self._knowledge_entries(
            request.workspace_id,
            request.spec.knowledge,
        )
        knowledge = _authorize_knowledge_entries(
            request.spec.knowledge,
            request.spec.inference,
            route,
            entries,
        )
        return _execution_grant(request, route, contexts, knowledge)

    async def _knowledge_entries(
        self,
        workspace_id: WorkspaceId,
        selection: KnowledgeSelection,
    ) -> Mapping[KnowledgeSourceId, InMemoryKnowledgePolicyEntry]:
        """@brief 以四次有界查询批量读取 source/version/policy/grant / Batch-load source/version/policy/grant in four bounded queries."""
        if selection.mode is KnowledgeSelectionMode.NONE:
            return {}
        included = tuple(str(value) for value in selection.include_source_ids)
        excluded = tuple(str(value) for value in selection.exclude_source_ids)
        source_predicate: Any = KnowledgeSourceRecord.id.in_(included)
        if selection.mode is KnowledgeSelectionMode.POLICY_DEFAULT:
            default_predicate = and_(
                KnowledgeSourceRecord.enabled.is_(True),
                KnowledgeSourceRecord.deleted_at.is_(None),
                KnowledgeSourceRecord.current_version_id.is_not(None),
            )
            source_predicate = (
                or_(default_predicate, KnowledgeSourceRecord.id.in_(included))
                if included
                else default_predicate
            )
        statement = (
            select(
                KnowledgeSourceRecord.id,
                KnowledgeSourceRecord.enabled,
                KnowledgeSourceRecord.current_version_id,
                KnowledgeSourceRecord.current_policy_version,
            )
            .where(
                KnowledgeSourceRecord.workspace_id == str(workspace_id),
                source_predicate,
            )
            .order_by(KnowledgeSourceRecord.id)
            .limit(201)
        )
        if excluded:
            statement = statement.where(KnowledgeSourceRecord.id.not_in(excluded))
        source_rows = (await self._session.execute(statement)).all()
        if len(source_rows) > 200:
            raise AgentPolicyDenied("Knowledge policy_default selection exceeds 200 sources")
        source_ids = tuple(row.id for row in source_rows)
        if not source_ids:
            return {}
        pins = {str(pin.source_id): str(pin.version_id) for pin in selection.pinned_versions}
        desired_versions = {row.id: pins.get(row.id, row.current_version_id) for row in source_rows}
        version_ids = tuple(value for value in desired_versions.values() if value is not None)
        version_rows = (
            await self._session.execute(
                select(
                    KnowledgeSourceVersionRecord.id,
                    KnowledgeSourceVersionRecord.source_id,
                    KnowledgeSourceVersionRecord.status,
                ).where(
                    KnowledgeSourceVersionRecord.workspace_id == str(workspace_id),
                    KnowledgeSourceVersionRecord.source_id.in_(source_ids),
                    KnowledgeSourceVersionRecord.id.in_(version_ids),
                )
            )
        ).all()
        ready_versions: dict[str, frozenset[KnowledgeSourceVersionId]] = {}
        for source_id in source_ids:
            ready_versions[source_id] = frozenset(
                KnowledgeSourceVersionId(row.id)
                for row in version_rows
                if row.source_id == source_id and row.status == KnowledgeVersionStatus.READY.value
            )
        policy_pairs = tuple((row.id, row.current_policy_version) for row in source_rows)
        policies = (
            (
                await self._session.execute(
                    select(KnowledgeVisibilityPolicyRecord).where(
                        KnowledgeVisibilityPolicyRecord.workspace_id == str(workspace_id),
                        tuple_(
                            KnowledgeVisibilityPolicyRecord.source_id,
                            KnowledgeVisibilityPolicyRecord.policy_version,
                        ).in_(policy_pairs),
                    )
                )
            )
            .scalars()
            .all()
        )
        policy_by_source = {row.source_id: row for row in policies}
        policy_ids = tuple(row.id for row in policies)
        grants = (
            (
                await self._session.execute(
                    select(KnowledgeVisibilityGrantRecord)
                    .where(
                        KnowledgeVisibilityGrantRecord.workspace_id == str(workspace_id),
                        KnowledgeVisibilityGrantRecord.policy_id.in_(policy_ids),
                    )
                    .order_by(
                        KnowledgeVisibilityGrantRecord.policy_id,
                        KnowledgeVisibilityGrantRecord.ordinal,
                    )
                )
            )
            .scalars()
            .all()
        )
        grants_by_policy: dict[str, list[AgentScopeGrant]] = {}
        for grant_row in grants:
            grants_by_policy.setdefault(grant_row.policy_id, []).append(
                AgentScopeGrant(
                    grant_row.agent_scope,
                    PolicyEffect(grant_row.effect),
                    tuple(KnowledgeOperation(value) for value in grant_row.allowed_operations),
                )
            )
        entries: dict[KnowledgeSourceId, InMemoryKnowledgePolicyEntry] = {}
        for source_row in source_rows:
            policy_row = policy_by_source.get(source_row.id)
            if policy_row is None:
                raise AgentPolicyDenied("a Knowledge source has no current visibility policy")
            policy = KnowledgeVisibilityPolicy(
                sensitivity=KnowledgeSensitivity(policy_row.sensitivity),
                default_effect=PolicyEffect(policy_row.default_effect),
                agent_grants=tuple(grants_by_policy.get(policy_row.id, ())),
                session_override_allowed=policy_row.session_override_allowed,
                allowed_model_regions=tuple(
                    ModelRegion(value) for value in policy_row.allowed_model_regions
                ),
                allow_external_model_processing=policy_row.allow_external_model_processing,
                retention_days=policy_row.retention_days,
                policy_version=policy_row.policy_version,
            )
            entries[KnowledgeSourceId(source_row.id)] = InMemoryKnowledgePolicyEntry(
                workspace_id=workspace_id,
                source_id=KnowledgeSourceId(source_row.id),
                enabled=source_row.enabled,
                current_version_id=(
                    None
                    if source_row.current_version_id is None
                    else KnowledgeSourceVersionId(source_row.current_version_id)
                ),
                ready_version_ids=ready_versions[source_row.id],
                policy=policy,
            )
        return entries


class _PostgresAgentRepositoryCore:
    """@brief Workspace-first、CAS、row-lock 友好的 PostgreSQL Agent repository / Workspace-first PostgreSQL Agent repository with CAS and row locks."""

    def __init__(
        self,
        session: AsyncSession,
        authorizer: _TrackingAgentAuthorizer,
    ) -> None:
        """@brief 绑定一个短事务 Session 和作用域 / Bind one short-transaction Session and scope."""
        self._session = session
        self._auth = authorizer

    async def list_conversations(
        self,
        workspace_id: WorkspaceId,
        page: AgentPageRequest,
    ) -> AgentPage[Conversation]:
        """@brief 按 ``(created_at,id)`` 稳定列出 Conversation / List Conversations by stable ``(created_at,id)``."""
        self._auth.require_workspace(workspace_id)
        after = _decode_created_position(page.after)
        statement = select(ConversationRecord).where(
            ConversationRecord.workspace_id == str(workspace_id),
            ConversationRecord.deleted_at.is_(None),
        )
        if after is not None:
            statement = statement.where(
                tuple_(ConversationRecord.created_at, ConversationRecord.id) > after
            )
        rows = (
            (
                await self._session.execute(
                    statement.order_by(ConversationRecord.created_at, ConversationRecord.id).limit(
                        page.limit + 1
                    )
                )
            )
            .scalars()
            .all()
        )
        return _postgres_created_page(rows, page.limit, _conversation_from_record)

    async def get_conversation(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        for_update: bool = False,
        include_deleted: bool = False,
    ) -> Conversation | None:
        """@brief 在 Workspace 内精确读取 Conversation / Read an exact Conversation inside a Workspace."""
        self._auth.require_workspace(workspace_id)
        statement = select(ConversationRecord).where(
            ConversationRecord.workspace_id == str(workspace_id),
            ConversationRecord.id == str(conversation_id),
        )
        if not include_deleted:
            statement = statement.where(ConversationRecord.deleted_at.is_(None))
        if for_update:
            statement = statement.with_for_update()
        row = (await self._session.execute(statement)).scalar_one_or_none()
        return None if row is None else _conversation_from_record(row)

    async def add_conversation(self, conversation: Conversation) -> None:
        """@brief 添加 Conversation / Add a Conversation."""
        self._auth.require_workspace(conversation.workspace_id)
        actor = self._auth.require_actor()
        self._session.add(_conversation_record(conversation, actor))

    async def save_conversation(
        self,
        conversation: Conversation,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以受影响行数验证 revision CAS / Verify revision CAS by affected-row count."""
        self._auth.require_workspace(conversation.workspace_id)
        result = await self._session.execute(
            update(ConversationRecord)
            .where(
                ConversationRecord.workspace_id == str(conversation.workspace_id),
                ConversationRecord.id == str(conversation.meta.id),
                ConversationRecord.revision == expected_revision,
            )
            .values(
                title=conversation.title,
                status=conversation.status.value,
                deleted_at=conversation.deleted_at,
                revision=conversation.meta.revision,
                updated_at=conversation.meta.updated_at,
            )
        )
        if _affected_rows(result) != 1:
            raise AgentCasMismatch


class _PostgresAgentJobStore:
    """@brief 直接复用 ``agent.jobs`` 的统一 Job store / Unified Job store directly reusing ``agent.jobs``."""

    def __init__(self, session: AsyncSession, authorizer: _TrackingAgentAuthorizer) -> None:
        """@brief 绑定事务 Session 与作用域 / Bind the transaction Session and scope."""
        self._session = session
        self._auth = authorizer

    async def add(self, job: Job) -> None:
        """@brief 添加 Agent Run 统一 Job / Add a unified Agent Run Job."""
        self._auth.require_workspace(job.workspace_id)
        actor = self._auth.require_actor()
        if job.kind != AGENT_RUN_JOB_KIND:
            raise ValueError("Agent Job store accepts only agent.run jobs")
        self._session.add(_job_record(job, actor))

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 在 Workspace 内读取 Agent Run Job / Read an Agent Run Job inside a Workspace."""
        self._auth.require_workspace(workspace_id)
        statement = select(JobRecord).where(
            JobRecord.workspace_id == str(workspace_id),
            JobRecord.id == str(job_id),
            JobRecord.job_type == AGENT_RUN_JOB_KIND,
        )
        if self._auth.is_worker:
            statement = statement.where(
                JobRecord.resource_owner_id == str(self._auth.require_actor())
            )
        if for_update:
            statement = statement.with_for_update()
        row = (await self._session.execute(statement)).scalar_one_or_none()
        return None if row is None else _job_from_record(row)

    async def save(self, job: Job, *, expected_revision: int) -> None:
        """@brief 以 revision CAS 统一 Job / CAS-save the unified Job."""
        self._auth.require_workspace(job.workspace_id)
        predicates = [
            JobRecord.workspace_id == str(job.workspace_id),
            JobRecord.id == str(job.meta.id),
            JobRecord.job_type == AGENT_RUN_JOB_KIND,
            JobRecord.revision == expected_revision,
        ]
        if self._auth.is_worker:
            predicates.append(JobRecord.resource_owner_id == str(self._auth.require_actor()))
        progress = job.progress
        result = await self._session.execute(
            update(JobRecord)
            .where(*predicates)
            .values(
                status=job.status.value,
                phase="queued" if progress is None else progress.phase,
                completed_units=0 if progress is None else progress.completed,
                total_units=None if progress is None else progress.total,
                progress_unit=(
                    JobProgressUnit.UNKNOWN.value if progress is None else progress.unit.value
                ),
                result_refs=_dump_array(_RESOURCE_REFS_ADAPTER, job.result_refs),
                problem=(
                    None if job.problem is None else _dump_problem(job.problem)
                ),
                started_at=job.started_at,
                finished_at=job.finished_at,
                revision=job.meta.revision,
                updated_at=job.meta.updated_at,
            )
        )
        if _affected_rows(result) != 1:
            raise AgentCasMismatch


class _PostgresAgentOutbox:
    """@brief 直接写入统一 ``agent.outbox_events`` / Write directly to unified ``agent.outbox_events``."""

    def __init__(self, session: AsyncSession, authorizer: _TrackingAgentAuthorizer) -> None:
        """@brief 绑定事务 Session 与作用域 / Bind the transaction Session and scope."""
        self._session = session
        self._auth = authorizer

    async def add(self, record: AgentOutboxRecord) -> None:
        """@brief 序列化封闭 AgentOutboxRecord 并验证 Run creator / Serialize the closed record and verify its Run creator."""
        self._auth.require_workspace(record.workspace_id)
        # Session 全局关闭 autoflush；此处显式 flush 后再以数据库真相校验 creator。
        await self._session.flush()
        creator = await self._session.scalar(
            select(AgentRunRecord.resource_owner_id).where(
                AgentRunRecord.workspace_id == str(record.workspace_id),
                AgentRunRecord.id == record.run_ref.id,
            )
        )
        if creator != str(record.actor_id):
            raise PermissionError("Agent outbox actor must match the persisted Run creator")
        payload = dict(record.as_payload())
        lifecycle = initial_outbox_lifecycle(record.kind, occurred_at=record.occurred_at)
        self._session.add(
            OutboxEventRecord(
                id=str(record.id),
                workspace_id=str(record.workspace_id),
                resource_owner_id=str(record.actor_id),
                aggregate_type=record.run_ref.resource_type,
                aggregate_id=record.run_ref.id,
                subject_revision=record.run_ref.revision,
                event_type=record.kind,
                sequence=0,
                occurred_at=record.occurred_at,
                payload=cast(JsonObject, payload),
                replay_expires_at=record.occurred_at + _EVENT_RETENTION,
                status=lifecycle.status,
                published_at=lifecycle.published_at,
                created_at=record.occurred_at,
                updated_at=record.occurred_at,
                revision=1,
                extensions={},
            )
        )


class _PostgresAgentAuditSink:
    """@brief 与 Agent 领域写共用 transaction 的 AuditEvent sink / AuditEvent sink sharing the Agent transaction."""

    def __init__(self, session: AsyncSession, authorizer: _TrackingAgentAuthorizer) -> None:
        """@brief 绑定事务 Session 与作用域 / Bind the transaction Session and scope."""
        self._session = session
        self._auth = authorizer

    async def add(self, event: AuditEvent) -> None:
        """@brief 添加统一 AuditEvent / Add a unified AuditEvent."""
        self._auth.require_workspace(event.workspace_id)
        actor = self._auth.require_actor()
        if event.actor.id != actor:
            raise PermissionError("Agent audit actor must match the authenticated actor")
        self._session.add(
            AuditEventRecord(
                id=str(event.id),
                workspace_id=str(event.workspace_id),
                resource_owner_id=str(actor),
                occurred_at=event.occurred_at,
                actor_type=event.actor.resource_type,
                actor_id=event.actor.id,
                actor_revision=event.actor.revision,
                action=event.action,
                resource_type=event.target.resource_type,
                resource_id=event.target.id,
                resource_revision=event.target.revision,
                request_id=event.request_id,
                outcome=event.outcome.value,
                details={},
                created_at=event.occurred_at,
                updated_at=event.occurred_at,
                revision=1,
                extensions={},
            )
        )


class PostgresAgentUnitOfWork:
    """@brief Conversation/Message/Run/Approval/Job/outbox/audit 单一 PostgreSQL UoW / Single PostgreSQL UoW for all Agent state and journals."""

    def __init__(
        self,
        database: AsyncDatabase,
        model_routes: Sequence[AgentModelRoute],
        *,
        worker_scope: tuple[WorkspaceId, UserId] | None = None,
    ) -> None:
        """@brief 绑定数据库、model routes 与可选 worker scope / Bind database, model routes, and optional worker scope."""
        self._database = database
        self._routes = tuple(model_routes)
        self._worker_scope = worker_scope
        self._session: AsyncSession | None = None
        self._transaction: AsyncSessionTransaction | None = None
        self._repository: PostgresAgentRepository | None = None
        self._authorizer: _TrackingAgentAuthorizer | None = None
        self._policy: PostgresAgentRunPolicy | None = None
        self._jobs: _PostgresAgentJobStore | None = None
        self._outbox: _PostgresAgentOutbox | None = None
        self._audit: _PostgresAgentAuditSink | None = None
        self._resume_proposals: PostgresAgentResumeProposalBoundary | None = None
        self._committed = False
        self._rolled_back = False

    @property
    def repository(self) -> PostgresAgentRepository:
        """@brief 返回事务 repository / Return the transactional repository."""
        if self._repository is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._repository

    @property
    def authorizer(self) -> _TrackingAgentAuthorizer:
        """@brief 返回中央 authorizer adapter / Return the central-authorizer adapter."""
        if self._authorizer is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._authorizer

    @property
    def policy(self) -> PostgresAgentRunPolicy:
        """@brief 返回本地 Run policy / Return the local Run policy."""
        if self._policy is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._policy

    @property
    def jobs(self) -> _PostgresAgentJobStore:
        """@brief 返回统一 Job store / Return the unified Job store."""
        if self._jobs is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._jobs

    @property
    def outbox(self) -> _PostgresAgentOutbox:
        """@brief 返回 transactional outbox / Return the transactional outbox."""
        if self._outbox is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._outbox

    @property
    def audit(self) -> _PostgresAgentAuditSink:
        """@brief 返回统一 audit sink / Return the unified audit sink."""
        if self._audit is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._audit

    @property
    def resume_proposals(self) -> PostgresAgentResumeProposalBoundary:
        """@brief 返回绑定同一 AsyncSession 的 Resume Proposal 边界 / Return the Resume Proposal boundary bound to the same AsyncSession."""
        if self._resume_proposals is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._resume_proposals

    async def __aenter__(self) -> Self:
        """@brief 建立 Session；worker 在任何业务读前安装 scope / Create the Session and install worker scope before business reads."""
        if self._session is not None:
            raise RuntimeError("Agent unit of work cannot be re-entered")
        self._session = self._database.new_session()
        self._transaction = await self._session.begin()
        scope_installer = partial(self._database.install_v2_request_scope, self._session)
        delegate = None
        if self._worker_scope is None:
            delegate = AccessAuthorizer(PostgresAccessRepository(self._session))
        self._authorizer = _TrackingAgentAuthorizer(
            delegate,
            scope_installer,
            worker_scope=self._worker_scope,
        )
        if self._worker_scope is not None:
            await self._authorizer.install_worker_scope()
        resolver = PostgresAgentContextResolver(self._session)
        self._policy = PostgresAgentRunPolicy(self._session, resolver, self._routes)
        self._repository = PostgresAgentRepository(self._session, self._authorizer)
        self._jobs = _PostgresAgentJobStore(self._session, self._authorizer)
        self._outbox = _PostgresAgentOutbox(self._session, self._authorizer)
        self._audit = _PostgresAgentAuditSink(self._session, self._authorizer)
        self._resume_proposals = PostgresAgentResumeProposalBoundary(
            self._session,
            self._authorizer,
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 回滚未提交 transaction 并关闭 Session / Roll back uncommitted work and close the Session."""
        del exc, traceback
        if self._session is not None:
            if exc_type is not None or not self._committed:
                await self.rollback()
            await self._session.close()
        self._session = None
        self._transaction = None
        self._repository = None
        self._authorizer = None
        self._policy = None
        self._jobs = None
        self._outbox = None
        self._audit = None
        self._resume_proposals = None
        return None

    async def commit(self) -> None:
        """@brief flush 后原子提交全部领域状态与 journals / Flush and atomically commit all domain state and journals."""
        session, transaction = self._require_active()
        if self._committed or self._rolled_back:
            raise RuntimeError("Agent unit of work cannot commit in its current state")
        await session.flush()
        await transaction.commit()
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等回滚活动 transaction / Idempotently roll back the active transaction."""
        if self._transaction is not None and self._transaction.is_active:
            await self._transaction.rollback()
        self._rolled_back = True

    def _require_active(self) -> tuple[AsyncSession, AsyncSessionTransaction]:
        """@brief 要求活动 Session/transaction / Require an active Session/transaction."""
        if self._session is None or self._transaction is None:
            raise RuntimeError("Agent unit of work has not been entered")
        return self._session, self._transaction


class PostgresAgentUnitOfWorkFactory:
    """@brief 创建公开 PostgreSQL Agent UoW / Create public PostgreSQL Agent UoWs."""

    def __init__(self, database: AsyncDatabase, *, model_routes: Sequence[AgentModelRoute]) -> None:
        """@brief 绑定数据库与不可变 model routes / Bind database and immutable model routes."""
        self._database = database
        self._routes = tuple(model_routes)

    def __call__(self) -> PostgresAgentUnitOfWork:
        """@brief 创建未进入的公开 UoW / Create an unentered public UoW."""
        return PostgresAgentUnitOfWork(self._database, self._routes)


class PostgresAgentWorkerUnitOfWorkFactory:
    """@brief 从 durable dispatch 真实身份创建 PostgreSQL worker UoW / Create PostgreSQL worker UoWs from durable-dispatch identity."""

    def __init__(self, database: AsyncDatabase, *, model_routes: Sequence[AgentModelRoute]) -> None:
        """@brief 绑定数据库与 model routes / Bind database and model routes."""
        self._database = database
        self._routes = tuple(model_routes)

    def __call__(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
    ) -> PostgresAgentUnitOfWork:
        """@brief 创建已密封 Run creator scope 的 UoW / Create a UoW sealed to the Run-creator scope."""
        return PostgresAgentUnitOfWork(
            self._database,
            self._routes,
            worker_scope=(workspace_id, actor_id),
        )


def _conversation_record(conversation: Conversation, actor_id: UserId) -> ConversationRecord:
    """@brief 构造 Conversation ORM row / Construct a Conversation ORM row."""
    return ConversationRecord(
        id=str(conversation.meta.id),
        workspace_id=str(conversation.workspace_id),
        resource_owner_id=str(actor_id),
        title=conversation.title,
        capability=conversation.capability.value,
        status=conversation.status.value,
        message_sequence=0,
        deleted_at=conversation.deleted_at,
        created_at=conversation.meta.created_at,
        updated_at=conversation.meta.updated_at,
        revision=conversation.meta.revision,
        extensions={},
    )


def _conversation_from_record(record: ConversationRecord) -> Conversation:
    """@brief 从 ORM row 重建 Conversation / Rebuild a Conversation from an ORM row."""
    return Conversation(
        meta=ResourceMeta(
            ConversationId(record.id),
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        workspace_id=WorkspaceId(record.workspace_id),
        title=record.title,
        capability=ConversationCapability(record.capability),
        status=ConversationStatus(record.status),
        deleted_at=record.deleted_at,
    )


def _message_record(message: Message, actor_id: UserId) -> ChatMessageRecord:
    """@brief 构造 append-only Message ORM row / Construct an append-only Message ORM row."""
    return ChatMessageRecord(
        id=str(message.meta.id),
        workspace_id=str(message.workspace_id),
        resource_owner_id=str(actor_id),
        conversation_id=str(message.conversation_id),
        sequence=message.sequence,
        role=message.role.value,
        content_parts=_dump_array(_CONTENT_ADAPTER, message.content),
        parent_message_id=(
            None if message.parent_message_id is None else str(message.parent_message_id)
        ),
        source_run_id=None if message.source_run_id is None else str(message.source_run_id),
        created_at=message.meta.created_at,
        updated_at=message.meta.updated_at,
        revision=message.meta.revision,
        extensions={},
    )


def _message_from_record(record: ChatMessageRecord) -> Message:
    """@brief 从 ORM row 重建 Message 并重验不变量 / Rebuild a Message and revalidate invariants."""
    return Message(
        meta=ResourceMeta(
            MessageId(record.id),
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        workspace_id=WorkspaceId(record.workspace_id),
        conversation_id=ConversationId(record.conversation_id),
        sequence=record.sequence,
        role=MessageRole(record.role),
        parent_message_id=(
            None if record.parent_message_id is None else MessageId(record.parent_message_id)
        ),
        content=_load(_CONTENT_ADAPTER, record.content_parts, "Agent message content"),
        source_run_id=(None if record.source_run_id is None else AgentRunId(record.source_run_id)),
    )


def _run_record(run: AgentRun) -> AgentRunRecord:
    """@brief 构造不含私有推理字段的 Run ORM row / Construct a Run ORM row without private-reasoning fields."""
    return AgentRunRecord(
        id=str(run.meta.id),
        workspace_id=str(run.workspace_id),
        resource_owner_id=str(run.created_by),
        conversation_id=str(run.spec.conversation_id),
        input_message_id=str(run.spec.input_message_id),
        job_id=str(run.job_id),
        capability=run.spec.capability.value,
        status=run.view.status.value,
        spec=_dump_object(_RUN_SPEC_ADAPTER, run.spec),
        execution_grant=_dump_object(_EXECUTION_GRANT_ADAPTER, run.grant),
        output_message_id=(
            None if run.view.output_message_id is None else str(run.view.output_message_id)
        ),
        proposal_refs=_dump_array(_RESOURCE_REFS_ADAPTER, run.view.proposal_refs),
        pending_approval_id=(
            None if run.view.pending_approval_id is None else str(run.view.pending_approval_id)
        ),
        usage=(None if run.view.usage is None else _dump_object(_USAGE_ADAPTER, run.view.usage)),
        problem=(
            None if run.view.problem is None else _dump_problem(run.view.problem)
        ),
        active_tool_call_id=(
            None if run.active_tool_call_id is None else str(run.active_tool_call_id)
        ),
        created_at=run.meta.created_at,
        updated_at=run.meta.updated_at,
        revision=run.meta.revision,
        extensions={},
    )


def _run_from_record(record: AgentRunRecord) -> AgentRun:
    """@brief 从冻结 spec/grant 重建 Run 聚合 / Rebuild a Run aggregate from frozen spec/grant."""
    spec = _load(_RUN_SPEC_ADAPTER, record.spec, "Agent Run spec")
    grant = _load(
        _EXECUTION_GRANT_ADAPTER,
        record.execution_grant,
        "Agent execution grant",
    )
    usage = None if record.usage is None else _load(_USAGE_ADAPTER, record.usage, "Agent usage")
    problem = (
        None if record.problem is None else _load(_PROBLEM_ADAPTER, record.problem, "Agent problem")
    )
    return AgentRun(
        view=AgentRunView(
            meta=ResourceMeta(
                AgentRunId(record.id),
                record.revision,
                record.created_at,
                record.updated_at,
            ),
            workspace_id=WorkspaceId(record.workspace_id),
            conversation_id=ConversationId(record.conversation_id),
            input_message_id=MessageId(record.input_message_id),
            capability=ConversationCapability(record.capability),
            status=AgentRunStatus(record.status),
            output_message_id=(
                None if record.output_message_id is None else MessageId(record.output_message_id)
            ),
            proposal_refs=_load(
                _RESOURCE_REFS_ADAPTER,
                record.proposal_refs,
                "Agent proposal refs",
            ),
            pending_approval_id=(
                None
                if record.pending_approval_id is None
                else ToolApprovalId(record.pending_approval_id)
            ),
            usage=usage,
            problem=problem,
        ),
        job_id=JobId(record.job_id),
        created_by=UserId(record.resource_owner_id),
        spec=spec,
        grant=grant,
        active_tool_call_id=(
            None if record.active_tool_call_id is None else ToolCallId(record.active_tool_call_id)
        ),
    )


def _approval_record(approval: ToolApproval, actor_id: UserId) -> ToolApprovalRecord:
    """@brief 构造仅持久安全摘要与 invocation ref 的 approval row / Construct an approval row containing only a safe summary and invocation ref."""
    decision = approval.view.decision_by
    invocation = approval.binding.invocation_ref
    return ToolApprovalRecord(
        id=str(approval.meta.id),
        workspace_id=str(approval.workspace_id),
        resource_owner_id=str(actor_id),
        run_id=str(approval.view.run_id),
        tool_call_id=str(approval.binding.tool_call_id),
        tool_name=approval.binding.tool_name,
        summary=approval.binding.summary,
        risk=approval.binding.risk.value,
        invocation_type=invocation.resource_type,
        invocation_id=invocation.id,
        invocation_revision=invocation.revision,
        status=approval.view.status.value,
        decision_by_type=None if decision is None else decision.resource_type,
        decision_by_id=None if decision is None else decision.id,
        decision_by_revision=None if decision is None else decision.revision,
        expires_at=approval.view.expires_at,
        created_at=approval.meta.created_at,
        updated_at=approval.meta.updated_at,
        revision=approval.meta.revision,
        extensions={},
    )


def _approval_from_record(record: ToolApprovalRecord) -> ToolApproval:
    """@brief 从 ORM row 重建精确 ToolApproval 绑定 / Rebuild the exact ToolApproval binding from an ORM row."""
    decision = (
        None
        if record.decision_by_type is None or record.decision_by_id is None
        else ResourceRef(
            record.decision_by_type,
            record.decision_by_id,
            record.decision_by_revision,
        )
    )
    binding = ToolCallBinding(
        tool_call_id=ToolCallId(record.tool_call_id),
        tool_name=record.tool_name,
        summary=record.summary,
        risk=ToolRisk(record.risk),
        expires_at=record.expires_at,
        invocation_ref=ResourceRef(
            record.invocation_type,
            record.invocation_id,
            record.invocation_revision,
        ),
    )
    from backend.domain.agent_v2 import ToolApprovalView

    return ToolApproval(
        view=ToolApprovalView(
            meta=ResourceMeta(
                ToolApprovalId(record.id),
                record.revision,
                record.created_at,
                record.updated_at,
            ),
            workspace_id=WorkspaceId(record.workspace_id),
            run_id=AgentRunId(record.run_id),
            tool_name=record.tool_name,
            summary=record.summary,
            risk=ToolRisk(record.risk),
            status=ToolApprovalStatus(record.status),
            expires_at=record.expires_at,
            decision_by=decision,
        ),
        binding=binding,
    )


def _job_record(job: Job, actor_id: UserId) -> JobRecord:
    """@brief 构造统一 Agent Job ORM row / Construct a unified Agent Job ORM row."""
    progress = job.progress
    return JobRecord(
        id=str(job.meta.id),
        workspace_id=str(job.workspace_id),
        resource_owner_id=str(actor_id),
        job_type=job.kind,
        status=job.status.value,
        phase="queued" if progress is None else progress.phase,
        completed_units=0 if progress is None else progress.completed,
        total_units=None if progress is None else progress.total,
        progress_unit=(JobProgressUnit.UNKNOWN.value if progress is None else progress.unit.value),
        percent=None,
        request_id=None,
        target_resource_type=job.subject.resource_type,
        target_resource_id=job.subject.id,
        target_resource_revision=job.subject.revision,
        result_refs=_dump_array(_RESOURCE_REFS_ADAPTER, job.result_refs),
        problem=(None if job.problem is None else _dump_problem(job.problem)),
        started_at=job.started_at,
        finished_at=job.finished_at,
        expires_at=None,
        request_payload=None,
        created_at=job.meta.created_at,
        updated_at=job.meta.updated_at,
        revision=job.meta.revision,
        extensions={},
    )


def _job_from_record(record: JobRecord) -> Job:
    """@brief 从统一 ORM row 重建 Agent Job / Rebuild an Agent Job from the unified ORM row."""
    progress = None
    if not (
        record.phase == "queued"
        and record.completed_units == 0
        and record.total_units is None
        and record.progress_unit == JobProgressUnit.UNKNOWN.value
    ):
        progress = JobProgress(
            record.phase,
            record.completed_units,
            record.total_units,
            JobProgressUnit(record.progress_unit),
        )
    return Job(
        meta=ResourceMeta(
            JobId(record.id),
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        workspace_id=WorkspaceId(record.workspace_id),
        kind=record.job_type,
        subject=ResourceRef(
            record.target_resource_type,
            record.target_resource_id,
            record.target_resource_revision,
        ),
        status=JobStatus(record.status),
        progress=progress,
        result_refs=_load(
            _RESOURCE_REFS_ADAPTER,
            record.result_refs,
            "Agent Job result refs",
        ),
        problem=(
            None
            if record.problem is None
            else _load(_PROBLEM_ADAPTER, record.problem, "Agent Job problem")
        ),
        started_at=record.started_at,
        finished_at=record.finished_at,
    )


def _postgres_created_page[RecordT, ItemT](
    rows: Sequence[RecordT],
    limit: int,
    project: Any,
) -> AgentPage[ItemT]:
    """@brief 将已排序 ORM rows 投影为 created-at keyset 页 / Project sorted ORM rows into a created-at keyset page."""
    selected_rows = rows[:limit]
    items = tuple(project(row) for row in selected_rows)
    position = (
        _encode_created_position(
            cast(Any, selected_rows[-1]).created_at,
            cast(Any, selected_rows[-1]).id,
        )
        if len(rows) > limit
        else None
    )
    return AgentPage(items, position)


class PostgresAgentRepository(_PostgresAgentRepositoryCore):
    """@brief 完整 PostgreSQL Agent repository；继承聚合根通用查询 / Complete PostgreSQL Agent repository extending aggregate-root primitives."""

    async def has_nonterminal_runs(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> bool:
        """@brief 用 EXISTS 判断非终态 Run / Test non-terminal Run existence with EXISTS."""
        self._auth.require_workspace(workspace_id)
        value = await self._session.scalar(
            select(
                select(AgentRunRecord.id)
                .where(
                    AgentRunRecord.workspace_id == str(workspace_id),
                    AgentRunRecord.conversation_id == str(conversation_id),
                    AgentRunRecord.status.in_(
                        (
                            AgentRunStatus.QUEUED.value,
                            AgentRunStatus.RUNNING.value,
                            AgentRunStatus.WAITING_FOR_APPROVAL.value,
                        )
                    ),
                )
                .exists()
            )
        )
        return bool(value)

    async def list_messages(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[Message]:
        """@brief 按 ``(sequence,id)`` 稳定列出 append-only Message / List append-only Messages by stable ``(sequence,id)``."""
        self._auth.require_workspace(workspace_id)
        after = _decode_message_position(page.after)
        statement = select(ChatMessageRecord).where(
            ChatMessageRecord.workspace_id == str(workspace_id),
            ChatMessageRecord.conversation_id == str(conversation_id),
        )
        if after is not None:
            statement = statement.where(
                tuple_(ChatMessageRecord.sequence, ChatMessageRecord.id) > after
            )
        rows = (
            (
                await self._session.execute(
                    statement.order_by(ChatMessageRecord.sequence, ChatMessageRecord.id).limit(
                        page.limit + 1
                    )
                )
            )
            .scalars()
            .all()
        )
        selected_rows = rows[: page.limit]
        next_position = (
            _encode_message_position(selected_rows[-1].sequence, selected_rows[-1].id)
            if len(rows) > page.limit
            else None
        )
        return AgentPage(
            tuple(_message_from_record(row) for row in selected_rows),
            next_position,
        )

    async def get_message(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        message_id: MessageId,
    ) -> Message | None:
        """@brief 以 Workspace/Conversation/Message 三元组读取 / Read by Workspace/Conversation/Message tuple."""
        self._auth.require_workspace(workspace_id)
        row = (
            await self._session.execute(
                select(ChatMessageRecord).where(
                    ChatMessageRecord.workspace_id == str(workspace_id),
                    ChatMessageRecord.conversation_id == str(conversation_id),
                    ChatMessageRecord.id == str(message_id),
                )
            )
        ).scalar_one_or_none()
        return None if row is None else _message_from_record(row)

    async def allocate_message_sequence(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        expected_conversation_revision: int | None,
        at: datetime,
    ) -> MessageSequenceReservation:
        """@brief 以单条 ``UPDATE ... RETURNING`` 分配 sequence/revision / Allocate sequence/revision with one ``UPDATE ... RETURNING``."""
        self._auth.require_workspace(workspace_id)
        predicates = [
            ConversationRecord.workspace_id == str(workspace_id),
            ConversationRecord.id == str(conversation_id),
            ConversationRecord.status == ConversationStatus.ACTIVE.value,
            ConversationRecord.deleted_at.is_(None),
        ]
        if expected_conversation_revision is not None:
            predicates.append(ConversationRecord.revision == expected_conversation_revision)
        row = (
            await self._session.execute(
                update(ConversationRecord)
                .where(*predicates)
                .values(
                    message_sequence=ConversationRecord.message_sequence + 1,
                    revision=ConversationRecord.revision + 1,
                    updated_at=at,
                )
                .returning(
                    ConversationRecord.message_sequence,
                    ConversationRecord.revision,
                    ConversationRecord.updated_at,
                )
            )
        ).one_or_none()
        if row is None:
            raise AgentCasMismatch
        return MessageSequenceReservation(row.message_sequence, row.revision, row.updated_at)

    async def add_message(self, message: Message) -> None:
        """@brief 添加创建后永不更新的 Message / Add a Message that is never updated."""
        self._auth.require_workspace(message.workspace_id)
        actor = self._auth.require_actor()
        self._session.add(_message_record(message, actor))

    async def list_runs_for_conversation(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[AgentRun]:
        """@brief 按 ``(created_at,id)`` 稳定列出 Run / List Runs by stable ``(created_at,id)``."""
        self._auth.require_workspace(workspace_id)
        after = _decode_created_position(page.after)
        statement = select(AgentRunRecord).where(
            AgentRunRecord.workspace_id == str(workspace_id),
            AgentRunRecord.conversation_id == str(conversation_id),
        )
        if after is not None:
            statement = statement.where(
                tuple_(AgentRunRecord.created_at, AgentRunRecord.id) > after
            )
        rows = (
            (
                await self._session.execute(
                    statement.order_by(AgentRunRecord.created_at, AgentRunRecord.id).limit(
                        page.limit + 1
                    )
                )
            )
            .scalars()
            .all()
        )
        return _postgres_created_page(rows, page.limit, _run_from_record)

    async def get_run(
        self,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        *,
        for_update: bool = False,
    ) -> AgentRun | None:
        """@brief 在 Workspace 内读取 Run，worker 叠加 creator 谓词 / Read a Run in a Workspace, adding a creator predicate for workers."""
        self._auth.require_workspace(workspace_id)
        statement = select(AgentRunRecord).where(
            AgentRunRecord.workspace_id == str(workspace_id),
            AgentRunRecord.id == str(run_id),
        )
        if self._auth.is_worker:
            statement = statement.where(
                AgentRunRecord.resource_owner_id == str(self._auth.require_actor())
            )
        if for_update:
            statement = statement.with_for_update()
        row = (await self._session.execute(statement)).scalar_one_or_none()
        return None if row is None else _run_from_record(row)

    async def add_run(self, run: AgentRun) -> None:
        """@brief 添加完整冻结 spec/grant 的 Run / Add a Run with complete frozen spec/grant."""
        self._auth.require_workspace(run.workspace_id)
        if run.created_by != self._auth.require_actor():
            raise PermissionError("Agent run creator must match the authenticated actor")
        self._session.add(_run_record(run))

    async def save_run(self, run: AgentRun, *, expected_revision: int) -> None:
        """@brief 以 revision CAS 保存 Run 状态 / CAS-save Run state by revision."""
        self._auth.require_workspace(run.workspace_id)
        predicates = [
            AgentRunRecord.workspace_id == str(run.workspace_id),
            AgentRunRecord.id == str(run.meta.id),
            AgentRunRecord.resource_owner_id == str(run.created_by),
            AgentRunRecord.revision == expected_revision,
        ]
        if self._auth.is_worker:
            predicates.append(AgentRunRecord.resource_owner_id == str(self._auth.require_actor()))
        result = await self._session.execute(
            update(AgentRunRecord)
            .where(*predicates)
            .values(
                status=run.view.status.value,
                output_message_id=(
                    None if run.view.output_message_id is None else str(run.view.output_message_id)
                ),
                proposal_refs=_dump_array(_RESOURCE_REFS_ADAPTER, run.view.proposal_refs),
                pending_approval_id=(
                    None
                    if run.view.pending_approval_id is None
                    else str(run.view.pending_approval_id)
                ),
                usage=(
                    None if run.view.usage is None else _dump_object(_USAGE_ADAPTER, run.view.usage)
                ),
                problem=(
                    None
                    if run.view.problem is None
                    else _dump_problem(run.view.problem)
                ),
                active_tool_call_id=(
                    None if run.active_tool_call_id is None else str(run.active_tool_call_id)
                ),
                revision=run.meta.revision,
                updated_at=run.meta.updated_at,
            )
        )
        if _affected_rows(result) != 1:
            raise AgentCasMismatch

    async def get_approval(
        self,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
        *,
        for_update: bool = False,
    ) -> ToolApproval | None:
        """@brief 在 Workspace 内读取 ToolApproval / Read a ToolApproval inside a Workspace."""
        self._auth.require_workspace(workspace_id)
        statement = select(ToolApprovalRecord).where(
            ToolApprovalRecord.workspace_id == str(workspace_id),
            ToolApprovalRecord.id == str(approval_id),
        )
        if self._auth.is_worker:
            statement = statement.where(
                ToolApprovalRecord.resource_owner_id == str(self._auth.require_actor())
            )
        if for_update:
            statement = statement.with_for_update()
        row = (await self._session.execute(statement)).scalar_one_or_none()
        return None if row is None else _approval_from_record(row)

    async def add_approval(self, approval: ToolApproval) -> None:
        """@brief 添加与 Run creator 同 owner 的 ToolApproval / Add a ToolApproval owned by the Run creator."""
        self._auth.require_workspace(approval.workspace_id)
        actor = self._auth.require_actor()
        self._session.add(_approval_record(approval, actor))

    async def save_approval(
        self,
        approval: ToolApproval,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以 revision CAS 保存一次性决定 / CAS-save a one-time decision."""
        self._auth.require_workspace(approval.workspace_id)
        predicates = [
            ToolApprovalRecord.workspace_id == str(approval.workspace_id),
            ToolApprovalRecord.id == str(approval.meta.id),
            ToolApprovalRecord.revision == expected_revision,
        ]
        if self._auth.is_worker:
            predicates.append(
                ToolApprovalRecord.resource_owner_id == str(self._auth.require_actor())
            )
        decision = approval.view.decision_by
        result = await self._session.execute(
            update(ToolApprovalRecord)
            .where(*predicates)
            .values(
                status=approval.view.status.value,
                decision_by_type=None if decision is None else decision.resource_type,
                decision_by_id=None if decision is None else decision.id,
                decision_by_revision=None if decision is None else decision.revision,
                revision=approval.meta.revision,
                updated_at=approval.meta.updated_at,
            )
        )
        if _affected_rows(result) != 1:
            raise AgentCasMismatch


__all__ = [
    "InMemoryAgentContextResolver",
    "InMemoryAgentDispatchResult",
    "InMemoryAgentDispatchService",
    "InMemoryAgentPolicyStore",
    "InMemoryAgentRunPolicy",
    "InMemoryAgentStore",
    "InMemoryAgentUnitOfWork",
    "InMemoryAgentUnitOfWorkFactory",
    "InMemoryAgentWorkerUnitOfWorkFactory",
    "InMemoryKnowledgePolicyEntry",
    "PostgresAgentContextResolver",
    "PostgresAgentRepository",
    "PostgresAgentRunPolicy",
    "PostgresAgentUnitOfWork",
    "PostgresAgentUnitOfWorkFactory",
    "PostgresAgentWorkerUnitOfWorkFactory",
]
