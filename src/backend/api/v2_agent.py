"""@brief API V2 Conversation 与 Agent HTTP 适配器 / API V2 Conversation and Agent HTTP adapter.

本模块只实现 ``contracts/v2`` 5.4 的十二条冻结路由。它负责严格 schema/query/body、
签名 cursor、表示级强 ETag、持久幂等与公开安全投影；权限、CAS、Run/Job/outbox/audit
的原子性仍由应用层与通用幂等事务拥有。私有 provider 状态、工具参数、Job 绑定和
chain-of-thought 没有任何 wire projector。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from functools import wraps
from typing import Concatenate, Protocol, cast

from fastapi import APIRouter, Request
from fastapi.responses import Response

from backend.api.v2_http import list_response
from backend.api.v2_transport import (
    DEFAULT_PAGE_LIMIT,
    JSON_MEDIA_TYPE,
    ContractDefinitionValidator,
    CursorCodec,
    JsonValue,
    OpaquePath,
    PageCursor,
    PageLimit,
    canonical_json_bytes,
    empty_response,
    idempotent_response,
    if_match_header,
    json_response,
    match_etag_revision,
    problem_response,
    replayable_json,
    request_id,
    require_no_body,
    require_query,
    resource_meta,
    resource_response,
    strict_json_object,
    timestamp,
    verified_principal,
)
from backend.application.agent_v2 import (
    AgentApplicationError,
    AgentApplicationService,
    AgentConflict,
    AgentMutationContext,
    AgentPortProtocolError,
    AgentPreconditionFailed,
    AgentResourceNotFound,
    CreateConversationCommand,
    CreateMessageCommand,
    InvalidAgentCommand,
    ToolApprovalDecisionCommand,
)
from backend.application.ports.access import AuthorizationDenied, UnknownPrincipal
from backend.application.ports.agent_v2 import (
    AgentPage,
    AgentPageRequest,
    AgentPolicyDenied,
)
from backend.application.ports.v2_idempotency import (
    ReplayableResponse,
    V2IdempotencyExecutor,
)
from backend.domain.agent_v2 import (
    AgentDomainError,
    AgentOutputMode,
    AgentRunId,
    AgentRunSpec,
    AgentRunTransitionError,
    AgentRunView,
    CitationContentPart,
    Conversation,
    ConversationCapability,
    ConversationId,
    ConversationPatch,
    ConversationStatus,
    ConversationUnavailable,
    Message,
    MessageContentPart,
    MessageId,
    ResumeProposalContentPart,
    TextContentPart,
    ToolApprovalDecisionError,
    ToolApprovalId,
    ToolApprovalView,
    ToolDecision,
)
from backend.domain.common import DomainError, Problem
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeCitation,
    KnowledgeRetrievalError,
    KnowledgeSelection,
    KnowledgeSelectionMode,
    KnowledgeVersionPin,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    ModelRegion,
)
from backend.domain.platform import ProblemDetails, ProblemFieldError
from backend.domain.principals import (
    DomainInvariantError,
    TokenPrincipal,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef

_MAX_SMALL_BODY_BYTES = 64 * 1024
"""@brief 小型 Agent command 原始 body 上限 / Raw-body limit for small Agent commands."""

_MAX_MESSAGE_BODY_BYTES = 20 * 1024 * 1024
"""@brief 最坏 UTF-8 Message command 原始 body 上限 / Worst-case UTF-8 Message-command limit."""

_MAX_RUN_BODY_BYTES = 1024 * 1024
"""@brief AgentRun command 原始 body 上限 / Raw-body limit for AgentRun commands."""

_MAX_AGENT_JSON_DEPTH = 16
"""@brief Agent request JSON 最大嵌套深度 / Maximum Agent-request JSON nesting depth."""

_MAX_MESSAGE_RESPONSE_BYTES = 32 * 1024 * 1024
"""@brief Message 单页或创建响应上限 / Message page or creation response limit."""

_CONVERSATION_SORT = ("created_at", "id")
"""@brief Conversation repository continuation 的稳定顺序 / Stable Conversation continuation order."""

_MESSAGE_SORT = ("sequence", "id")
"""@brief Message repository continuation 的稳定顺序 / Stable Message continuation order."""

_AGENT_BOUNDARY_ERRORS: tuple[type[Exception], ...] = (
    AgentApplicationError,
    AgentDomainError,
    KnowledgeRetrievalError,
    AgentPolicyDenied,
    AuthorizationDenied,
    UnknownPrincipal,
    DomainInvariantError,
)
"""@brief 可稳定映射为公开 Problem 的 Agent boundary 错误 / Stable Agent-boundary errors."""


class V2AgentRuntime(Protocol):
    """@brief 单个 Agent request 所需运行时依赖 / Runtime dependencies for one Agent request."""

    @property
    def agent_v2(self) -> AgentApplicationService:
        """@brief 返回 Agent V2 应用服务 / Return the Agent V2 application service."""

        ...

    @property
    def contracts_v2(self) -> ContractDefinitionValidator:
        """@brief 返回权威 V2 schema validator / Return the authoritative V2 schema validator."""

        ...

    @property
    def v2_cursor(self) -> CursorCodec:
        """@brief 返回签名 cursor codec / Return the signed cursor codec."""

        ...

    @property
    def v2_idempotency(self) -> V2IdempotencyExecutor:
        """@brief 返回持久幂等 executor / Return the durable idempotency executor."""

        ...


type V2AgentRuntimeResolver = Callable[[Request], V2AgentRuntime]
"""@brief 从 request 解析 Agent runtime / Resolve an Agent runtime from a request."""


def v2_agent_runtime_from_request(request: Request) -> V2AgentRuntime:
    """@brief 从 composition container 取得 Agent 依赖 / Read Agent dependencies from composition.

    @param request 当前 HTTP request / Current HTTP request.
    @return Agent runtime / Agent runtime.
    @raise RuntimeError container 尚未安装时抛出 / Raised when the container is unavailable.
    """

    container = getattr(request.app.state, "container", None)
    if container is None:
        raise RuntimeError("backend container is unavailable")
    return cast(V2AgentRuntime, container)


def _translate_http_errors[AdapterT, **ParamT](
    handler: Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
) -> Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]]:
    """@brief 将预期 Agent 错误转成 ProblemDetails / Translate expected Agent errors.

    @param handler 未包装 endpoint / Unwrapped endpoint.
    @return 保留 endpoint 签名的包装器 / Wrapper preserving the endpoint signature.
    """

    @wraps(handler)
    async def wrapped(
        adapter: AdapterT,
        request: Request,
        *args: ParamT.args,
        **kwargs: ParamT.kwargs,
    ) -> Response:
        """@brief 执行 endpoint 并封闭预期失败 / Execute an endpoint and close expected failures."""

        try:
            return await handler(adapter, request, *args, **kwargs)
        except DomainError as error:
            return problem_response(request, error.problem, error=error)
        except _AGENT_BOUNDARY_ERRORS as error:
            return problem_response(request, _agent_problem(error), error=error)

    return cast(
        Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
        wrapped,
    )


class V2AgentHttpAdapter:
    """@brief 把 Agent 5.4 用例适配为冻结 HTTP 路由 / Adapt Agent 5.4 use cases to frozen HTTP routes."""

    def __init__(self, resolve_runtime: V2AgentRuntimeResolver) -> None:
        """@brief 构建并注册全部十二条路由 / Build and register all twelve routes.

        @param resolve_runtime 每个 request 的 runtime resolver / Runtime resolver per request.
        """

        self._resolve_runtime = resolve_runtime
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self) -> None:
        """@brief 注册契约 5.4 的十二条路由 / Register the twelve section-5.4 routes."""

        routes: tuple[
            tuple[
                str,
                str,
                Callable[..., Awaitable[Response]],
                str | None,
                str | None,
            ],
            ...,
        ] = (
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/conversations",
                self.list_conversations,
                None,
                "ConversationList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/conversations",
                self.create_conversation,
                "CreateConversationRequest",
                "Conversation",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}",
                self.get_conversation,
                None,
                "Conversation",
            ),
            (
                "PATCH",
                "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}",
                self.update_conversation,
                "UpdateConversationRequest",
                "Conversation",
            ),
            (
                "DELETE",
                "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}",
                self.delete_conversation,
                None,
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}/messages",
                self.list_messages,
                None,
                "MessageList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}/messages",
                self.create_message,
                "CreateMessageRequest",
                "Message",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/agent-runs",
                self.create_agent_run,
                "CreateAgentRunRequest",
                "AgentRun",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/agent-runs/{run_id}",
                self.get_agent_run,
                None,
                "AgentRun",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/agent-runs/{run_id}/cancellations",
                self.cancel_agent_run,
                None,
                "AgentRun",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/tool-approvals/{approval_id}",
                self.get_tool_approval,
                None,
                "ToolApproval",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/tool-approvals/{approval_id}/decisions",
                self.decide_tool_approval,
                "ToolApprovalDecisionRequest",
                "ToolApproval",
            ),
        )
        for method, path, endpoint, request_definition, response_definition in routes:
            extra: dict[str, JsonValue] = {"x-api-v2-phase": 4}
            if request_definition is not None:
                extra["x-contract-request"] = request_definition
            if response_definition is not None:
                extra["x-contract-response"] = response_definition
            self.router.add_api_route(
                path,
                endpoint,
                methods=[method],
                openapi_extra=extra,
                status_code=_declared_status(method, path),
                response_class=Response,
            )

    @_translate_http_errors
    async def list_conversations(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Conversation / Page through Conversations."""

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        filters: dict[str, JsonValue] = {"collection": "conversations"}
        page = await runtime.agent_v2.list_conversations(
            principal,
            workspace,
            _page_request(
                cursor,
                limit,
                runtime.v2_cursor,
                principal,
                workspace,
                filters,
                _CONVERSATION_SORT,
            ),
        )
        payload = _collection_payload(
            page,
            project=_conversation,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=workspace,
            filters=filters,
            sort=_CONVERSATION_SORT,
        )
        runtime.contracts_v2.validate_definition("ConversationList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def create_conversation(
        self,
        request: Request,
        workspace_id: OpaquePath,
    ) -> Response:
        """@brief 幂等创建 Conversation / Idempotently create a Conversation."""

        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _agent_json(
            request,
            runtime.contracts_v2,
            "CreateConversationRequest",
            maximum_bytes=_MAX_SMALL_BODY_BYTES,
        )

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后创建 Conversation / Create a Conversation after first claim."""

            conversation = await runtime.agent_v2.create_conversation(
                principal,
                workspace,
                CreateConversationCommand(
                    ConversationCapability(_required_string(body, "capability")),
                    _required_nullable_string(body, "title"),
                ),
                AgentMutationContext(request_id(request)),
            )
            payload = _conversation(conversation)
            runtime.contracts_v2.validate_definition("Conversation", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=(
                    f"/api/v2/workspaces/{workspace_id}/conversations/"
                    f"{conversation.meta.id}"
                ),
                etag=True,
            )

        return await _idempotent(
            request,
            runtime,
            principal,
            workspace,
            f"/api/v2/workspaces/{workspace_id}/conversations",
            canonical_json_bytes(body),
            JSON_MEDIA_TYPE,
            None,
            operation,
        )

    @_translate_http_errors
    async def get_conversation(
        self,
        request: Request,
        workspace_id: OpaquePath,
        conversation_id: OpaquePath,
    ) -> Response:
        """@brief 读取单个 Conversation / Read one Conversation."""

        require_query(request)
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        conversation = await runtime.agent_v2.get_conversation(
            principal,
            workspace,
            ConversationId(conversation_id),
        )
        payload = _conversation(conversation)
        runtime.contracts_v2.validate_definition("Conversation", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def update_conversation(
        self,
        request: Request,
        workspace_id: OpaquePath,
        conversation_id: OpaquePath,
    ) -> Response:
        """@brief 使用 Merge Patch 与强 If-Match 修改 Conversation / Update with merge patch and strong If-Match."""

        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _agent_json(
            request,
            runtime.contracts_v2,
            "UpdateConversationRequest",
            maximum_bytes=_MAX_SMALL_BODY_BYTES,
        )
        typed_id = ConversationId(conversation_id)
        current = await runtime.agent_v2.get_conversation_for_update(
            principal,
            workspace,
            typed_id,
        )
        current_payload = _conversation(current)
        runtime.contracts_v2.validate_definition("Conversation", current_payload)
        expected = match_etag_revision(
            if_match_header(request),
            current_payload,
            current.meta.revision,
        )
        updated = await runtime.agent_v2.update_conversation(
            principal,
            workspace,
            typed_id,
            _conversation_patch(body),
            expected_revision=expected,
            context=AgentMutationContext(request_id(request)),
        )
        payload = _conversation(updated)
        runtime.contracts_v2.validate_definition("Conversation", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def delete_conversation(
        self,
        request: Request,
        workspace_id: OpaquePath,
        conversation_id: OpaquePath,
    ) -> Response:
        """@brief 使用强 If-Match 删除 Conversation / Delete a Conversation with strong If-Match."""

        require_query(request)
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        typed_id = ConversationId(conversation_id)
        current = await runtime.agent_v2.get_conversation_for_deletion(
            principal,
            workspace,
            typed_id,
        )
        current_payload = _conversation(current)
        runtime.contracts_v2.validate_definition("Conversation", current_payload)
        expected = match_etag_revision(
            if_match_header(request),
            current_payload,
            current.meta.revision,
        )
        await runtime.agent_v2.delete_conversation(
            principal,
            workspace,
            typed_id,
            expected_revision=expected,
            context=AgentMutationContext(request_id(request)),
        )
        return empty_response(request)

    @_translate_http_errors
    async def list_messages(
        self,
        request: Request,
        workspace_id: OpaquePath,
        conversation_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Conversation Message / Page through Conversation Messages."""

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        filters: dict[str, JsonValue] = {
            "collection": "conversation_messages",
            "conversation_id": conversation_id,
        }
        page = await runtime.agent_v2.list_messages(
            principal,
            workspace,
            ConversationId(conversation_id),
            _page_request(
                cursor,
                limit,
                runtime.v2_cursor,
                principal,
                workspace,
                filters,
                _MESSAGE_SORT,
            ),
        )
        payload = _collection_payload(
            page,
            project=_message,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=workspace,
            filters=filters,
            sort=_MESSAGE_SORT,
        )
        runtime.contracts_v2.validate_definition("MessageList", payload)
        return json_response(
            request,
            payload,
            max_response_bytes=_MAX_MESSAGE_RESPONSE_BYTES,
        )

    @_translate_http_errors
    async def create_message(
        self,
        request: Request,
        workspace_id: OpaquePath,
        conversation_id: OpaquePath,
    ) -> Response:
        """@brief 使用 Conversation If-Match 幂等创建 Message / Idempotently create a Message with Conversation If-Match."""

        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _agent_json(
            request,
            runtime.contracts_v2,
            "CreateMessageRequest",
            maximum_bytes=_MAX_MESSAGE_BODY_BYTES,
        )
        typed_id = ConversationId(conversation_id)
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 在幂等事务内比较 Conversation snapshot 并追加 Message / Compare the Conversation snapshot and append once."""

            current = await runtime.agent_v2.get_conversation_for_message_creation(
                principal,
                workspace,
                typed_id,
            )
            current_payload = _conversation(current)
            runtime.contracts_v2.validate_definition("Conversation", current_payload)
            expected = match_etag_revision(
                raw_if_match,
                current_payload,
                current.meta.revision,
            )
            message = await runtime.agent_v2.create_message(
                principal,
                workspace,
                typed_id,
                _message_command(body),
                expected_conversation_revision=expected,
                context=AgentMutationContext(request_id(request)),
            )
            payload = _message(message)
            runtime.contracts_v2.validate_definition("Message", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=(
                    f"/api/v2/workspaces/{workspace_id}/conversations/"
                    f"{conversation_id}/messages/{message.meta.id}"
                ),
                etag=True,
                max_response_bytes=_MAX_MESSAGE_RESPONSE_BYTES,
            )

        return await _idempotent(
            request,
            runtime,
            principal,
            workspace,
            (
                f"/api/v2/workspaces/{workspace_id}/conversations/"
                f"{conversation_id}/messages"
            ),
            canonical_json_bytes(body),
            JSON_MEDIA_TYPE,
            raw_if_match,
            operation,
        )

    @_translate_http_errors
    async def create_agent_run(
        self,
        request: Request,
        workspace_id: OpaquePath,
    ) -> Response:
        """@brief 原子幂等创建 AgentRun 与统一 Job / Atomically and idempotently create an AgentRun and unified Job."""

        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _agent_json(
            request,
            runtime.contracts_v2,
            "CreateAgentRunRequest",
            maximum_bytes=_MAX_RUN_BODY_BYTES,
        )

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后创建 Run / Create the Run after first claim."""

            run = await runtime.agent_v2.create_agent_run(
                principal,
                workspace,
                _agent_run_spec(body),
                AgentMutationContext(request_id(request)),
            )
            payload = _agent_run(run)
            runtime.contracts_v2.validate_definition("AgentRun", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=f"/api/v2/workspaces/{workspace_id}/agent-runs/{run.meta.id}",
                etag=True,
            )

        return await _idempotent(
            request,
            runtime,
            principal,
            workspace,
            f"/api/v2/workspaces/{workspace_id}/agent-runs",
            canonical_json_bytes(body),
            JSON_MEDIA_TYPE,
            None,
            operation,
        )

    @_translate_http_errors
    async def get_agent_run(
        self,
        request: Request,
        workspace_id: OpaquePath,
        run_id: OpaquePath,
    ) -> Response:
        """@brief 读取单个 AgentRun / Read one AgentRun."""

        require_query(request)
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        run = await runtime.agent_v2.get_agent_run(
            principal,
            workspace,
            AgentRunId(run_id),
        )
        payload = _agent_run(run)
        runtime.contracts_v2.validate_definition("AgentRun", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def cancel_agent_run(
        self,
        request: Request,
        workspace_id: OpaquePath,
        run_id: OpaquePath,
    ) -> Response:
        """@brief 使用强 If-Match 幂等取消 Run 与 Job / Idempotently cancel a Run and Job with strong If-Match."""

        require_query(request)
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        typed_id = AgentRunId(run_id)
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 在幂等事务内比较 Run snapshot 并取消 / Compare the Run snapshot and cancel once."""

            current = await runtime.agent_v2.get_agent_run_for_cancellation(
                principal,
                workspace,
                typed_id,
            )
            current_payload = _agent_run(current)
            runtime.contracts_v2.validate_definition("AgentRun", current_payload)
            expected = match_etag_revision(
                raw_if_match,
                current_payload,
                current.meta.revision,
            )
            cancelled = await runtime.agent_v2.cancel_agent_run(
                principal,
                workspace,
                typed_id,
                expected_revision=expected,
                context=AgentMutationContext(request_id(request)),
            )
            payload = _agent_run(cancelled)
            runtime.contracts_v2.validate_definition("AgentRun", payload)
            return replayable_json(payload, status_code=200, etag=True)

        return await _idempotent(
            request,
            runtime,
            principal,
            workspace,
            f"/api/v2/workspaces/{workspace_id}/agent-runs/{run_id}/cancellations",
            b"",
            None,
            raw_if_match,
            operation,
        )

    @_translate_http_errors
    async def get_tool_approval(
        self,
        request: Request,
        workspace_id: OpaquePath,
        approval_id: OpaquePath,
    ) -> Response:
        """@brief 读取公开安全 ToolApproval / Read a public-safe ToolApproval."""

        require_query(request)
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        approval = await runtime.agent_v2.get_tool_approval(
            principal,
            workspace,
            ToolApprovalId(approval_id),
        )
        payload = _tool_approval(approval)
        runtime.contracts_v2.validate_definition("ToolApproval", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def decide_tool_approval(
        self,
        request: Request,
        workspace_id: OpaquePath,
        approval_id: OpaquePath,
    ) -> Response:
        """@brief 使用强 If-Match 幂等决定 ToolApproval / Idempotently decide a ToolApproval with strong If-Match."""

        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _agent_json(
            request,
            runtime.contracts_v2,
            "ToolApprovalDecisionRequest",
            maximum_bytes=_MAX_SMALL_BODY_BYTES,
        )
        typed_id = ToolApprovalId(approval_id)
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 在幂等事务内比较 approval snapshot 并决定 / Compare the approval snapshot and decide once."""

            current = await runtime.agent_v2.get_tool_approval_for_decision(
                principal,
                workspace,
                typed_id,
            )
            current_payload = _tool_approval(current)
            runtime.contracts_v2.validate_definition("ToolApproval", current_payload)
            expected = match_etag_revision(
                raw_if_match,
                current_payload,
                current.meta.revision,
            )
            decided = await runtime.agent_v2.decide_tool_approval(
                principal,
                workspace,
                typed_id,
                ToolApprovalDecisionCommand(
                    ToolDecision(_required_string(body, "decision"))
                ),
                expected_revision=expected,
                context=AgentMutationContext(request_id(request)),
            )
            payload = _tool_approval(decided)
            runtime.contracts_v2.validate_definition("ToolApproval", payload)
            return replayable_json(payload, status_code=200, etag=True)

        return await _idempotent(
            request,
            runtime,
            principal,
            workspace,
            (
                f"/api/v2/workspaces/{workspace_id}/tool-approvals/"
                f"{approval_id}/decisions"
            ),
            canonical_json_bytes(body),
            JSON_MEDIA_TYPE,
            raw_if_match,
            operation,
        )

    def _request_context(
        self,
        request: Request,
        workspace_id: str,
    ) -> tuple[V2AgentRuntime, TokenPrincipal, WorkspaceId]:
        """@brief 解析 runtime、principal 与路径 Workspace / Resolve runtime, principal, and path Workspace."""

        return (
            self._resolve_runtime(request),
            verified_principal(request),
            WorkspaceId(workspace_id),
        )


def create_v2_agent_router(
    resolve_runtime: V2AgentRuntimeResolver = v2_agent_runtime_from_request,
) -> APIRouter:
    """@brief 创建完整 Agent V2 router / Create the complete Agent V2 router.

    @param resolve_runtime request runtime resolver / Request runtime resolver.
    @return 包含十二条路由的 router / Router containing all twelve routes.
    """

    return V2AgentHttpAdapter(resolve_runtime).router


def _declared_status(method: str, path: str) -> int:
    """@brief 返回 OpenAPI 声明的成功状态 / Return the declared OpenAPI success status."""

    if method == "DELETE":
        return 204
    if method == "POST" and not path.endswith(("/cancellations", "/decisions")):
        return 201
    return 200


async def _agent_json(
    request: Request,
    validator: ContractDefinitionValidator,
    definition: str,
    *,
    maximum_bytes: int,
) -> dict[str, JsonValue]:
    """@brief 严格读取一个 Agent JSON object / Strictly read an Agent JSON object."""

    return await strict_json_object(
        request,
        validator=validator,
        definition=definition,
        max_body_bytes=maximum_bytes,
        max_depth=_MAX_AGENT_JSON_DEPTH,
    )


async def _idempotent(
    request: Request,
    runtime: V2AgentRuntime,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    canonical_path: str,
    canonical_body: bytes,
    content_type: str | None,
    if_match: str | None,
    operation: Callable[[], Awaitable[ReplayableResponse]],
) -> Response:
    """@brief 执行通用原子幂等 Agent command / Execute a generic atomic idempotent Agent command."""

    return await idempotent_response(
        request,
        executor=runtime.v2_idempotency,
        principal=principal,
        workspace_id=workspace_id,
        canonical_path=canonical_path,
        canonical_body=canonical_body,
        content_type=content_type,
        if_match=if_match,
        operation=operation,
        mapped_error_types=_AGENT_BOUNDARY_ERRORS,
        map_error=_agent_problem,
    )


def _page_request(
    cursor: str | None,
    limit: int,
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    filters: Mapping[str, JsonValue],
    sort: Sequence[str],
) -> AgentPageRequest:
    """@brief 解码绑定授权上下文的 repository continuation / Decode an authorization-bound continuation."""

    if cursor is None:
        return AgentPageRequest(limit=limit)
    position = codec.decode(
        cursor,
        principal=principal,
        workspace_id=workspace_id,
        filters=filters,
        sort=sort,
    )
    if not isinstance(position, str) or not position:
        raise DomainError(Problem("http.cursor_invalid", 400, "Pagination cursor is invalid"))
    try:
        return AgentPageRequest(limit=limit, after=position)
    except ValueError as error:
        raise DomainError(
            Problem("http.cursor_invalid", 400, "Pagination cursor is invalid")
        ) from error


def _collection_payload[ItemT](
    page: AgentPage[ItemT],
    *,
    project: Callable[[ItemT], JsonValue],
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    filters: Mapping[str, JsonValue],
    sort: Sequence[str],
) -> dict[str, JsonValue]:
    """@brief 封装应用层已分页结果并签名 continuation / Wrap a page and sign its continuation."""

    next_cursor: str | None = None
    if page.next_position is not None:
        next_cursor = codec.encode(
            page.next_position,
            principal=principal,
            workspace_id=workspace_id,
            filters=filters,
            sort=sort,
        )
    return list_response(
        [project(item) for item in page.items],
        next_cursor=next_cursor,
    )


def _conversation_patch(body: Mapping[str, JsonValue]) -> ConversationPatch:
    """@brief 将 UpdateConversationRequest 转成精确 patch / Convert an update request to an exact patch."""

    return ConversationPatch(
        title_supplied="title" in body,
        title=_optional_nullable_string(body, "title"),
        status=(
            ConversationStatus(_required_string(body, "status"))
            if "status" in body
            else None
        ),
    )


def _message_command(body: Mapping[str, JsonValue]) -> CreateMessageCommand:
    """@brief 将 CreateMessageRequest 转成类型化 command / Convert CreateMessageRequest to a typed command."""

    parent = body.get("parent_message_id")
    parent_id = MessageId(parent) if isinstance(parent, str) else None
    content = tuple(
        TextContentPart(_required_string(part, "text"))
        for part in _required_object_array(body, "content")
    )
    return CreateMessageCommand(parent_id, content)


def _agent_run_spec(body: Mapping[str, JsonValue]) -> AgentRunSpec:
    """@brief 将 CreateAgentRunRequest 转成不可变执行快照 / Convert a run request to an immutable execution snapshot."""

    return AgentRunSpec(
        conversation_id=ConversationId(_required_string(body, "conversation_id")),
        input_message_id=MessageId(_required_string(body, "input_message_id")),
        capability=ConversationCapability(_required_string(body, "capability")),
        context_refs=tuple(
            _resource_ref_input(item)
            for item in _required_object_array(body, "context_refs")
        ),
        knowledge=_knowledge_selection(_required_object(body, "knowledge")),
        inference=_inference_intent(_required_object(body, "inference")),
        output_modes=tuple(
            AgentOutputMode(item) for item in _required_string_array(body, "output_modes")
        ),
        response_locale=_required_string(body, "response_locale"),
    )


def _knowledge_selection(body: Mapping[str, JsonValue]) -> KnowledgeSelection:
    """@brief 投影 KnowledgeSelection 输入 / Project a KnowledgeSelection input."""

    return KnowledgeSelection(
        mode=KnowledgeSelectionMode(_required_string(body, "mode")),
        include_source_ids=tuple(
            KnowledgeSourceId(item)
            for item in _required_string_array(body, "include_source_ids")
        ),
        exclude_source_ids=tuple(
            KnowledgeSourceId(item)
            for item in _required_string_array(body, "exclude_source_ids")
        ),
        pinned_versions=tuple(
            KnowledgeVersionPin(
                KnowledgeSourceId(_required_string(item, "source_id")),
                KnowledgeSourceVersionId(_required_string(item, "version_id")),
            )
            for item in _required_object_array(body, "pinned_versions")
        ),
        agent_scope=_required_string(body, "agent_scope"),
    )


def _inference_intent(body: Mapping[str, JsonValue]) -> InferenceIntent:
    """@brief 投影 InferenceIntent 输入 / Project an InferenceIntent input."""

    latency = body.get("latency_budget_ms")
    return InferenceIntent(
        quality_tier=InferenceQualityTier(_required_string(body, "quality_tier")),
        latency_budget_ms=latency if isinstance(latency, int) and not isinstance(latency, bool) else None,
        cost_tier=InferenceCostTier(_required_string(body, "cost_tier")),
        data_region=ModelRegion(_required_string(body, "data_region")),
        allow_provider_fallback=_required_bool(body, "allow_provider_fallback"),
        allow_external_model_processing=_required_bool(
            body,
            "allow_external_model_processing",
        ),
    )


def _resource_ref_input(body: Mapping[str, JsonValue]) -> ResourceRef:
    """@brief 解析公开 ResourceRef 输入 / Parse a public ResourceRef input."""

    revision = body.get("revision")
    return ResourceRef(
        _required_string(body, "resource_type"),
        _required_string(body, "id"),
        revision if isinstance(revision, int) and not isinstance(revision, bool) else None,
    )


def _conversation(value: Conversation) -> dict[str, JsonValue]:
    """@brief 穷尽投影 Conversation 且排除 deleted_at / Exhaustively project Conversation without deleted_at."""

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "title": value.title,
            "capability": value.capability.value,
            "status": value.status.value,
        }
    )
    return payload


def _message(value: Message) -> dict[str, JsonValue]:
    """@brief 穷尽投影 Message 且排除 sequence/source_run_id / Exhaustively project Message without internal ordering or Run binding."""

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "conversation_id": str(value.conversation_id),
            "role": value.role.value,
            "parent_message_id": (
                str(value.parent_message_id)
                if value.parent_message_id is not None
                else None
            ),
            "content": [_message_part(part) for part in value.content],
        }
    )
    return payload


def _message_part(value: MessageContentPart) -> dict[str, JsonValue]:
    """@brief 穷尽投影封闭 MessageContentPart 联合 / Exhaustively project the closed MessageContentPart union."""

    if isinstance(value, TextContentPart):
        return {"type": "text", "text": value.text}
    if isinstance(value, CitationContentPart):
        return {"type": "citation", "citation": _citation(value.citation)}
    if isinstance(value, ResumeProposalContentPart):
        return {
            "type": "resume_proposal",
            "proposal_ref": _resource_ref(value.proposal_ref),
        }
    raise TypeError("unsupported MessageContentPart")


def _citation(value: KnowledgeCitation) -> dict[str, JsonValue]:
    """@brief 投影公开 KnowledgeCitation / Project a public KnowledgeCitation."""

    return {
        "source_id": str(value.source_id),
        "version_id": str(value.version_id),
        "locator": value.locator,
        "quote": value.quote,
        "score": value.score,
    }


def _agent_run(value: AgentRunView) -> dict[str, JsonValue]:
    """@brief 只投影 AgentRunView，不暴露 Job/spec/grant/tool-call / Project only AgentRunView, excluding private execution state."""

    payload = resource_meta(value.meta)
    usage: dict[str, JsonValue] | None = None
    if value.usage is not None:
        usage = {
            "input_tokens": value.usage.input_tokens,
            "output_tokens": value.usage.output_tokens,
            "cost_micro_usd": value.usage.cost_micro_usd,
        }
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "conversation_id": str(value.conversation_id),
            "input_message_id": str(value.input_message_id),
            "capability": value.capability.value,
            "status": value.status.value,
            "output_message_id": (
                str(value.output_message_id) if value.output_message_id is not None else None
            ),
            "proposal_refs": [_resource_ref(item) for item in value.proposal_refs],
            "pending_approval_id": (
                str(value.pending_approval_id)
                if value.pending_approval_id is not None
                else None
            ),
            "usage": usage,
            "problem": _problem_details(value.problem) if value.problem is not None else None,
        }
    )
    return payload


def _tool_approval(value: ToolApprovalView) -> dict[str, JsonValue]:
    """@brief 只投影 ToolApprovalView，不暴露 invocation binding / Project only ToolApprovalView without the invocation binding."""

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "run_id": str(value.run_id),
            "tool_name": value.tool_name,
            "summary": value.summary,
            "risk": value.risk.value,
            "status": value.status.value,
            "expires_at": timestamp(value.expires_at),
            "decision_by": (
                _resource_ref(value.decision_by) if value.decision_by is not None else None
            ),
        }
    )
    return payload


def _resource_ref(value: ResourceRef) -> dict[str, JsonValue]:
    """@brief 投影 ResourceRef 并省略空 revision / Project ResourceRef and omit an absent revision."""

    payload: dict[str, JsonValue] = {
        "resource_type": value.resource_type,
        "id": value.id,
    }
    if value.revision is not None:
        payload["revision"] = value.revision
    return payload


def _problem_details(value: ProblemDetails) -> dict[str, JsonValue]:
    """@brief 投影 Run 内公开安全 ProblemDetails / Project public-safe ProblemDetails nested in a Run."""

    payload: dict[str, JsonValue] = {
        "type": value.type_uri,
        "title": value.title,
        "status": value.status,
        "code": value.code,
        "request_id": value.request_id,
        "retryable": value.retryable,
        "errors": [_problem_field_error(error) for error in value.errors],
    }
    if value.detail is not None:
        payload["detail"] = value.detail
    if value.instance is not None:
        payload["instance"] = value.instance
    if value.extensions:
        payload["extensions"] = {
            key: _thaw_json(item) for key, item in value.extensions.items()
        }
    return payload


def _problem_field_error(value: ProblemFieldError) -> dict[str, JsonValue]:
    """@brief 投影 ProblemFieldError / Project a ProblemFieldError."""

    payload: dict[str, JsonValue] = {"pointer": value.pointer, "code": value.code}
    if value.message_key is not None:
        payload["message_key"] = value.message_key
    if value.params:
        payload["params"] = dict(value.params)
    return payload


def _thaw_json(value: object) -> JsonValue:
    """@brief 把深度 immutable JSON 复制成 wire JSON / Copy deeply immutable JSON to wire JSON."""

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_thaw_json(item) for item in value]
    raise TypeError("problem extension is not JSON")


def _agent_problem(error: Exception) -> Problem:
    """@brief 把 Agent 应用/领域失败稳定映射成 HTTP Problem / Map Agent application/domain failures to HTTP Problems."""

    if isinstance(error, AgentResourceNotFound):
        return Problem(error.code, 404, "Resource was not found", detail=error.detail)
    if isinstance(error, AgentPreconditionFailed):
        return Problem(
            error.code,
            412,
            "Resource precondition failed",
            detail=error.detail,
        )
    if isinstance(error, AgentConflict):
        return Problem(error.code, 409, "Resource state conflict", detail=error.detail)
    if isinstance(error, AgentPortProtocolError):
        return Problem(
            "agent.internal_protocol_error",
            500,
            "Internal server error",
            detail="The Agent service could not complete the request safely.",
        )
    if isinstance(error, InvalidAgentCommand):
        return Problem(
            error.code,
            422,
            "Command violates a domain constraint",
            detail=error.detail,
        )
    if isinstance(error, AgentPolicyDenied):
        return Problem("authorization.denied", 403, "Action is not permitted")
    if isinstance(error, UnknownPrincipal):
        return Problem("oauth.invalid_token", 401, "Bearer token is invalid")
    if isinstance(error, AuthorizationDenied):
        if error.reason in {
            "authorization.membership_missing",
            "authorization.membership_inactive",
        }:
            return Problem("resource.not_found", 404, "Resource was not found")
        if error.reason == "authorization.scope_missing":
            return Problem(
                "oauth.insufficient_scope",
                403,
                "Token scope is insufficient",
            )
        return Problem("authorization.denied", 403, "Action is not permitted")
    if isinstance(
        error,
        (
            ConversationUnavailable,
            AgentRunTransitionError,
            ToolApprovalDecisionError,
        ),
    ):
        return Problem(
            "agent.state_conflict",
            409,
            "Agent resource state conflict",
            detail="The resource is not in the required state.",
        )
    if isinstance(error, (AgentDomainError, KnowledgeRetrievalError)):
        return Problem(
            "request.domain_constraint",
            422,
            "Request violates a domain constraint",
            detail="The request violates an Agent domain constraint.",
        )
    if isinstance(error, AgentApplicationError):
        return Problem(error.code, 422, "Agent request was rejected", detail=error.detail)
    if isinstance(error, DomainInvariantError):
        return Problem(
            "agent.state_conflict",
            409,
            "Agent resource state conflict",
            detail="The Agent resource does not satisfy the required state.",
        )
    return Problem("agent.internal_error", 500, "Internal server error")


def _required_object(
    body: Mapping[str, JsonValue],
    name: str,
) -> dict[str, JsonValue]:
    """@brief 取得 schema 已验证 object / Read a schema-validated object."""

    value = body[name]
    if not isinstance(value, dict):
        raise RuntimeError(f"validated field {name} must be an object")
    return value


def _required_object_array(
    body: Mapping[str, JsonValue],
    name: str,
) -> tuple[dict[str, JsonValue], ...]:
    """@brief 取得 schema 已验证 object array / Read a schema-validated object array."""

    value = body[name]
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise RuntimeError(f"validated field {name} must be an object array")
    return tuple(cast(dict[str, JsonValue], item) for item in value)


def _required_string(body: Mapping[str, JsonValue], name: str) -> str:
    """@brief 取得 schema 已验证 string / Read a schema-validated string."""

    value = body[name]
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {name} must be a string")
    return value


def _required_nullable_string(
    body: Mapping[str, JsonValue],
    name: str,
) -> str | None:
    """@brief 取得 schema 已验证 nullable string / Read a schema-validated nullable string."""

    value = body[name]
    if value is not None and not isinstance(value, str):
        raise RuntimeError(f"validated field {name} must be a nullable string")
    return value


def _optional_nullable_string(
    body: Mapping[str, JsonValue],
    name: str,
) -> str | None:
    """@brief 取得可省略 nullable string / Read an optional nullable string."""

    value = body.get(name)
    if value is not None and not isinstance(value, str):
        raise RuntimeError(f"validated field {name} must be a nullable string")
    return value


def _required_string_array(
    body: Mapping[str, JsonValue],
    name: str,
) -> tuple[str, ...]:
    """@brief 取得 schema 已验证 string array / Read a schema-validated string array."""

    value = body[name]
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise RuntimeError(f"validated field {name} must be a string array")
    return tuple(cast(str, item) for item in value)


def _required_bool(body: Mapping[str, JsonValue], name: str) -> bool:
    """@brief 取得 schema 已验证 boolean / Read a schema-validated boolean."""

    value = body[name]
    if not isinstance(value, bool):
        raise RuntimeError(f"validated field {name} must be a boolean")
    return value


router_v2_agent = create_v2_agent_router()
"""@brief 使用 production resolver 的 Agent V2 router / Agent V2 router using the production resolver."""


__all__ = [
    "V2AgentHttpAdapter",
    "V2AgentRuntime",
    "V2AgentRuntimeResolver",
    "create_v2_agent_router",
    "router_v2_agent",
    "v2_agent_runtime_from_request",
]
