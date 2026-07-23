"""@brief API V2 Connection、Upload 与 Knowledge HTTP 适配器 / API V2 Knowledge HTTP adapter.

本模块只处理 5.3 冻结的 HTTP 边界：严格 JSON/query/no-body、签名 cursor、强 ETag、
条件写入、持久幂等 receipt 与无 secret 的公开投影。Workspace 授权、CAS、上传核验、
Job/outbox 原子性和执行时重授权由 ``KnowledgeApplicationService`` 负责。
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
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
    idempotent_response,
    if_match_header,
    json_response,
    match_etag_revision,
    prepared_idempotent_response,
    problem_response,
    replayable_json,
    require_idempotency_key,
    require_no_body,
    require_query,
    resource_meta,
    resource_response,
    strict_json_object,
    timestamp,
    verified_principal,
)
from backend.application.knowledge import (
    CreateConnectionAuthorizationSessionCommand,
    CreateConnectionCommand,
    CreateKnowledgeJobCommand,
    CreateKnowledgeSourceCommand,
    InvalidKnowledgeCommand,
    KnowledgeApplicationError,
    KnowledgeApplicationService,
    KnowledgeConflict,
    KnowledgePreconditionFailed,
    KnowledgeResourceNotFound,
    PreparedConnectionCreation,
    PreparedKnowledgeSourceCreation,
    PreparedUploadCompletion,
    PreparedUploadSessionCreation,
    UpdateKnowledgeSourceCommand,
)
from backend.application.ports.access import AuthorizationDenied, UnknownPrincipal
from backend.application.ports.knowledge import KnowledgePage, KnowledgePageRequest
from backend.application.ports.v2_idempotency import (
    IdempotencyPreparationId,
    ReplayableResponse,
    V2PreparedIdempotencyExecutor,
    request_fingerprint,
)
from backend.domain.common import DomainError, Problem
from backend.domain.connections import (
    Connection,
    ConnectionAuthorizationFlow,
    ConnectionAuthorizationSession,
    ConnectionDomainError,
    ConnectionId,
    ConnectionProvider,
    SecretValue,
)
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeAccessEvaluationRequest,
    KnowledgeAccessEvaluationResult,
    KnowledgeRetrievalError,
    KnowledgeSearchRequest,
    KnowledgeSearchResult,
    KnowledgeSelection,
    KnowledgeSelectionMode,
    KnowledgeVersionPin,
    SearchFilters,
    SearchFilterValue,
)
from backend.domain.knowledge_sources import (
    AgentScopeGrant,
    CloudDriveSourceInput,
    FileSourceInput,
    GitSourceInput,
    KnowledgeDomainError,
    KnowledgeOperation,
    KnowledgeSensitivity,
    KnowledgeSource,
    KnowledgeSourceId,
    KnowledgeSourceInput,
    KnowledgeSourceType,
    KnowledgeSourceVersion,
    KnowledgeSourceVersionId,
    KnowledgeVisibilityPolicy,
    ManualSourceInput,
    ModelRegion,
    PolicyEffect,
    ResumeId,
    ResumeSourceInput,
    UrlSourceInput,
)
from backend.domain.platform import Job, ProblemDetails, ProblemFieldError
from backend.domain.principals import (
    DomainInvariantError,
    TokenPrincipal,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef
from backend.domain.upload_sessions import (
    UploadCompletionClaim,
    UploadDeclaration,
    UploadDomainError,
    UploadSessionId,
    UploadSessionView,
)

#: @brief 5.3 请求的冻结原始 body 上限 / Frozen raw-body limit for section 5.3.
_MAX_KNOWLEDGE_BODY_BYTES = 8 * 1024 * 1024
#: @brief 5.3 JSON 容器深度上限 / Maximum JSON container depth for section 5.3.
_MAX_KNOWLEDGE_JSON_DEPTH = 24
#: @brief Connection 集合稳定排序 / Stable Connection collection ordering.
_CONNECTION_SORT = ("id",)
#: @brief KnowledgeSource 集合稳定排序 / Stable KnowledgeSource collection ordering.
_SOURCE_SORT = ("id",)
#: @brief KnowledgeSourceVersion 集合稳定排序 / Stable version collection ordering.
_VERSION_SORT = ("version_number",)
#: @brief Knowledge boundary 可稳定映射的预期异常 / Expected errors with stable HTTP mappings.
_KNOWLEDGE_BOUNDARY_ERRORS: tuple[type[Exception], ...] = (
    KnowledgeApplicationError,
    ConnectionDomainError,
    KnowledgeDomainError,
    KnowledgeRetrievalError,
    UploadDomainError,
    AuthorizationDenied,
    UnknownPrincipal,
    DomainInvariantError,
)


class V2KnowledgeRuntime(Protocol):
    """@brief 单个 5.3 request 所需运行时依赖 / Runtime dependencies for one section-5.3 request."""

    @property
    def knowledge_v2(self) -> KnowledgeApplicationService:
        """@brief 返回 Knowledge 应用服务 / Return the Knowledge application service.

        @return Knowledge application service / Knowledge application service.
        """

        ...

    @property
    def contracts_v2(self) -> ContractDefinitionValidator:
        """@brief 返回权威 V2 schema validator / Return the authoritative V2 schema validator.

        @return definition validator / Definition validator.
        """

        ...

    @property
    def v2_cursor(self) -> CursorCodec:
        """@brief 返回签名 cursor codec / Return the signed cursor codec.

        @return cursor codec / Cursor codec.
        """

        ...

    @property
    def v2_idempotency(self) -> V2PreparedIdempotencyExecutor:
        """@brief 返回持久幂等 executor / Return the durable idempotency executor.

        @return idempotency executor / Idempotency executor.
        """

        ...

    @property
    def sensitive_idempotency_key(self) -> bytes:
        """@brief 返回独立 secret-aware HMAC key / Return the independent secret-aware HMAC key.

        @return 至少 32 字节且不同于 cursor key 的密钥 / A key of at least 32 bytes distinct from the cursor key.
        """

        ...


type V2KnowledgeRuntimeResolver = Callable[[Request], V2KnowledgeRuntime]
"""@brief 从 HTTP request 解析 Knowledge runtime / Resolve a Knowledge runtime from an HTTP request."""


def v2_knowledge_runtime_from_request(request: Request) -> V2KnowledgeRuntime:
    """@brief 从 composition container 取得 5.3 依赖 / Read section-5.3 dependencies from the container.

    @param request 当前 HTTP request / Current HTTP request.
    @return 结构化 Knowledge runtime / Structured Knowledge runtime.
    @raise RuntimeError container 尚未安装时抛出 / Raised when the container is unavailable.
    """

    container = getattr(request.app.state, "container", None)
    if container is None:
        raise RuntimeError("backend container is unavailable")
    return cast(V2KnowledgeRuntime, container)


def _translate_http_errors[AdapterT, **ParamT](
    handler: Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
) -> Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]]:
    """@brief 将预期 5.3 异常转换为 V2 ProblemDetails / Translate expected 5.3 errors.

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
        """@brief 执行 endpoint 并封闭已知失败 / Execute an endpoint and close known failures.

        @param adapter endpoint 所属 adapter / Adapter owning the endpoint.
        @param request 当前 request / Current request.
        @param args endpoint 位置参数 / Positional endpoint arguments.
        @param kwargs endpoint 关键字参数 / Keyword endpoint arguments.
        @return 成功或 ProblemDetails response / Success or Problem Details response.
        """

        try:
            return await handler(adapter, request, *args, **kwargs)
        except DomainError as error:
            return problem_response(request, error.problem, error=error)
        except _KNOWLEDGE_BOUNDARY_ERRORS as error:
            return problem_response(request, _knowledge_problem(error), error=error)

    return cast(
        Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
        wrapped,
    )


class V2KnowledgeHttpAdapter:
    """@brief 把 5.3 应用用例适配为冻结 API V2 路由 / Adapt section-5.3 use cases to frozen API V2 routes.

    @param resolve_runtime 按 request 解析依赖的函数 / Per-request dependency resolver.
    """

    def __init__(self, resolve_runtime: V2KnowledgeRuntimeResolver) -> None:
        """@brief 构建并注册 17 条 Knowledge 路由 / Build and register the 17 Knowledge routes.

        @param resolve_runtime 每个 request 的 runtime resolver / Runtime resolver per request.
        """

        self._resolve_runtime = resolve_runtime
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self) -> None:
        """@brief 注册契约 5.3 的全部路由 / Register every contract section-5.3 route.

        @return 无返回值 / No return value.
        """

        routes: tuple[
            tuple[str, str, Callable[..., Awaitable[Response]], str | None, str | None],
            ...,
        ] = (
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/connections",
                self.list_connections,
                None,
                "ConnectionList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/connection-authorization-sessions",
                self.create_connection_authorization_session,
                "CreateConnectionAuthorizationSessionRequest",
                "ConnectionAuthorizationSession",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/connections",
                self.create_connection,
                "CreateConnectionRequest",
                "Connection",
            ),
            (
                "DELETE",
                "/api/v2/workspaces/{workspace_id}/connections/{connection_id}",
                self.delete_connection,
                None,
                "Job",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources",
                self.list_knowledge_sources,
                None,
                "KnowledgeSourceList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources",
                self.create_knowledge_source,
                "CreateKnowledgeSourceRequest",
                "KnowledgeSource",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}",
                self.get_knowledge_source,
                None,
                "KnowledgeSource",
            ),
            (
                "PATCH",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}",
                self.update_knowledge_source,
                "UpdateKnowledgeSourceRequest",
                "KnowledgeSource",
            ),
            (
                "DELETE",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}",
                self.delete_knowledge_source,
                None,
                "Job",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/versions",
                self.list_knowledge_source_versions,
                None,
                "KnowledgeSourceVersionList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/versions",
                self.create_knowledge_source_version,
                "CreateKnowledgeSourceVersionRequest",
                "KnowledgeSourceVersion",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/upload-sessions",
                self.create_upload_session,
                "CreateUploadSessionRequest",
                "UploadSession",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/upload-sessions/{upload_id}/completions",
                self.complete_upload_session,
                "CompleteUploadSessionRequest",
                "UploadSession",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/ingestion-jobs",
                self.create_ingestion_job,
                "CreateKnowledgeJobRequest",
                "Job",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/sync-jobs",
                self.create_sync_job,
                "CreateKnowledgeJobRequest",
                "Job",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/knowledge-searches",
                self.search_knowledge,
                "KnowledgeSearchRequest",
                "KnowledgeSearchResult",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/knowledge-access-evaluations",
                self.evaluate_knowledge_access,
                "KnowledgeAccessEvaluationRequest",
                "KnowledgeAccessEvaluationResult",
            ),
        )
        for method, path, endpoint, request_definition, response_definition in routes:
            extra: dict[str, JsonValue] = {"x-api-v2-phase": 3}
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
    async def list_connections(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出安全 Connection 投影 / Page through safe Connection projections.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param cursor 可选签名 cursor / Optional signed cursor.
        @param limit 页长 / Page size.
        @return ConnectionList / ConnectionList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        filters: dict[str, JsonValue] = {"collection": "connections"}
        page = await runtime.knowledge_v2.list_connections(
            principal,
            typed_workspace,
            _page_request(
                cursor,
                limit,
                runtime.v2_cursor,
                principal,
                typed_workspace,
                filters,
                _CONNECTION_SORT,
            ),
        )
        payload = _collection_payload(
            page,
            project=_connection,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace,
            filters=filters,
            sort=_CONNECTION_SORT,
        )
        runtime.contracts_v2.validate_definition("ConnectionList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def create_connection_authorization_session(
        self,
        request: Request,
        workspace_id: OpaquePath,
    ) -> Response:
        """@brief 创建不进入通用 receipt 的 provider 授权 session / Create a provider session outside generic receipts.

        @param request 含授权启动 body 的 request / Request carrying the authorization launch body.
        @param workspace_id 路径 Workspace / Path Workspace.
        @return 201 ConnectionAuthorizationSession / 201 ConnectionAuthorizationSession.
        @note ``user_code`` 与 authorization URL 不得进入通用幂等缓存 / ``user_code`` and
            authorization URLs must not enter the generic idempotency cache.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        body = await _knowledge_json(
            request,
            runtime.contracts_v2,
            "CreateConnectionAuthorizationSessionRequest",
        )
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        canonical_body = canonical_json_bytes(body)
        idempotency_key = require_idempotency_key(request)
        session = await runtime.knowledge_v2.create_connection_authorization_session(
            principal,
            typed_workspace,
            CreateConnectionAuthorizationSessionCommand(
                ConnectionProvider(_required_string(body, "provider")),
                ConnectionAuthorizationFlow(_required_string(body, "flow")),
                _string_tuple(body, "requested_scopes"),
                _authorization_idempotency_key_hash(
                    idempotency_key,
                    runtime.sensitive_idempotency_key,
                    principal,
                    typed_workspace,
                ),
                request_fingerprint(
                    canonical_body,
                    content_type=JSON_MEDIA_TYPE,
                    if_match=None,
                ),
            ),
        )
        payload = _connection_authorization_session(session)
        runtime.contracts_v2.validate_definition("ConnectionAuthorizationSession", payload)
        response = resource_response(
            request,
            payload,
            status_code=201,
            location=(
                f"/api/v2/workspaces/{workspace_id}/connection-authorization-sessions/{session.id}"
            ),
        )
        response.headers["Cache-Control"] = "no-store"
        return response

    @_translate_http_errors
    async def create_connection(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief secret-aware 幂等创建 Connection / Create a Connection with secret-aware idempotency.

        @param request 含 API token 与幂等键的 request / Request carrying an API token and idempotency key.
        @param workspace_id 路径 Workspace / Path Workspace.
        @return 201 脱敏 Connection / 201 redacted Connection.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        body = await _knowledge_json(request, runtime.contracts_v2, "CreateConnectionRequest")
        fingerprint = _sensitive_body_fingerprint(
            canonical_json_bytes(body),
            runtime.sensitive_idempotency_key,
        )

        command = CreateConnectionCommand(
            ConnectionProvider(_required_string(body, "provider")),
            _required_string(body, "display_name"),
            SecretValue(_required_string(body, "api_token")),
        )

        async def prepare(
            operation_id: IdempotencyPreparationId,
        ) -> PreparedConnectionCreation:
            """@brief 在事务外验证并暂存 credential / Validate and stage the credential outside a transaction."""

            return await runtime.knowledge_v2.prepare_connection_creation(
                principal,
                typed_workspace,
                command,
                operation_id,
            )

        async def commit(prepared: PreparedConnectionCreation) -> ReplayableResponse:
            """@brief 原子提交 Connection 与 receipt / Atomically commit the Connection and receipt."""

            connection = await runtime.knowledge_v2.commit_connection_creation(
                principal,
                typed_workspace,
                prepared,
            )
            payload = _connection(connection)
            runtime.contracts_v2.validate_definition("Connection", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=f"/api/v2/workspaces/{workspace_id}/connections/{connection.meta.id}",
                etag=True,
            )

        return await prepared_idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace,
            canonical_path=f"/api/v2/workspaces/{workspace_id}/connections",
            canonical_body=fingerprint,
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            prepare=prepare,
            commit=commit,
            mapped_error_types=_KNOWLEDGE_BOUNDARY_ERRORS,
            map_error=_knowledge_problem,
        )

    @_translate_http_errors
    async def delete_connection(
        self,
        request: Request,
        workspace_id: OpaquePath,
        connection_id: OpaquePath,
    ) -> Response:
        """@brief 以 If-Match 条件请求撤销 Connection / Conditionally request Connection revocation with If-Match.

        @param request 含 If-Match 的无 body request / Bodyless request with If-Match.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param connection_id Connection 标识 / Connection identifier.
        @return 202 Job / 202 Job.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        typed_connection = ConnectionId(connection_id)
        supplied_if_match = if_match_header(request)
        current = await runtime.knowledge_v2.get_connection_for_deletion(
            principal,
            typed_workspace,
            typed_connection,
        )
        current_payload = _connection(current)
        runtime.contracts_v2.validate_definition("Connection", current_payload)
        expected_revision = match_etag_revision(
            supplied_if_match,
            current_payload,
            current.meta.revision,
        )
        job = await runtime.knowledge_v2.delete_connection(
            principal,
            typed_workspace,
            typed_connection,
            expected_revision=expected_revision,
        )
        return _job_response(request, runtime, job, workspace_id)

    @_translate_http_errors
    async def list_knowledge_sources(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 KnowledgeSource / Page through KnowledgeSources.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param cursor 可选签名 cursor / Optional signed cursor.
        @param limit 页长 / Page size.
        @return KnowledgeSourceList / KnowledgeSourceList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        filters: dict[str, JsonValue] = {"collection": "knowledge_sources"}
        page = await runtime.knowledge_v2.list_knowledge_sources(
            principal,
            typed_workspace,
            _page_request(
                cursor,
                limit,
                runtime.v2_cursor,
                principal,
                typed_workspace,
                filters,
                _SOURCE_SORT,
            ),
        )
        payload = _collection_payload(
            page,
            project=_knowledge_source,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace,
            filters=filters,
            sort=_SOURCE_SORT,
        )
        runtime.contracts_v2.validate_definition("KnowledgeSourceList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def create_knowledge_source(
        self,
        request: Request,
        workspace_id: OpaquePath,
    ) -> Response:
        """@brief 幂等创建 KnowledgeSource / Idempotently create a KnowledgeSource.

        @param request 含来源 body 与幂等键的 request / Request with source body and idempotency key.
        @param workspace_id 路径 Workspace / Path Workspace.
        @return 201 KnowledgeSource / 201 KnowledgeSource.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        body = await _knowledge_json(
            request,
            runtime.contracts_v2,
            "CreateKnowledgeSourceRequest",
        )

        command = CreateKnowledgeSourceCommand(
            _required_string(body, "name"),
            _source_input(_required_object(body, "input")),
            _visibility(_required_object(body, "visibility")),
        )

        async def prepare(
            operation_id: IdempotencyPreparationId,
        ) -> PreparedKnowledgeSourceCreation:
            """@brief 在事务外执行来源安全检查 / Run source security checks outside a transaction."""

            return await runtime.knowledge_v2.prepare_knowledge_source_creation(
                principal,
                typed_workspace,
                command,
                operation_id,
            )

        async def commit(prepared: PreparedKnowledgeSourceCreation) -> ReplayableResponse:
            """@brief 原子提交来源与 receipt / Atomically commit the source and receipt."""

            source = await runtime.knowledge_v2.commit_knowledge_source_creation(
                principal,
                typed_workspace,
                prepared,
            )
            payload = _knowledge_source(source)
            runtime.contracts_v2.validate_definition("KnowledgeSource", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=(f"/api/v2/workspaces/{workspace_id}/knowledge-sources/{source.meta.id}"),
                etag=True,
            )

        return await prepared_idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace,
            canonical_path=f"/api/v2/workspaces/{workspace_id}/knowledge-sources",
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            prepare=prepare,
            commit=commit,
            mapped_error_types=_KNOWLEDGE_BOUNDARY_ERRORS,
            map_error=_knowledge_problem,
        )

    @_translate_http_errors
    async def get_knowledge_source(
        self,
        request: Request,
        workspace_id: OpaquePath,
        source_id: OpaquePath,
    ) -> Response:
        """@brief 读取一个 KnowledgeSource / Read one KnowledgeSource.

        @param request 已认证无 body request / Authenticated bodyless request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 来源标识 / Source identifier.
        @return 带强 ETag 的 KnowledgeSource / KnowledgeSource with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        source = await runtime.knowledge_v2.get_knowledge_source(
            verified_principal(request),
            WorkspaceId(workspace_id),
            KnowledgeSourceId(source_id),
        )
        payload = _knowledge_source(source)
        runtime.contracts_v2.validate_definition("KnowledgeSource", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def update_knowledge_source(
        self,
        request: Request,
        workspace_id: OpaquePath,
        source_id: OpaquePath,
    ) -> Response:
        """@brief 以 Merge Patch 与强 ETag 修改来源 / Update a source with Merge Patch and a strong ETag.

        @param request 含 merge patch 与 If-Match 的 request / Request with merge patch and If-Match.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 来源标识 / Source identifier.
        @return 更新后的 KnowledgeSource / Updated KnowledgeSource.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        typed_source = KnowledgeSourceId(source_id)
        body = await _knowledge_json(
            request,
            runtime.contracts_v2,
            "UpdateKnowledgeSourceRequest",
        )
        current = await runtime.knowledge_v2.get_knowledge_source(
            principal,
            typed_workspace,
            typed_source,
        )
        current_payload = _knowledge_source(current)
        runtime.contracts_v2.validate_definition("KnowledgeSource", current_payload)
        expected_revision = match_etag_revision(
            if_match_header(request),
            current_payload,
            current.meta.revision,
        )
        visibility_value = body.get("visibility")
        updated = await runtime.knowledge_v2.update_knowledge_source(
            principal,
            typed_workspace,
            typed_source,
            UpdateKnowledgeSourceCommand(
                name=_optional_string(body, "name"),
                visibility=(
                    None
                    if visibility_value is None
                    else _visibility(_object(visibility_value, "visibility"))
                ),
            ),
            expected_revision=expected_revision,
        )
        payload = _knowledge_source(updated)
        runtime.contracts_v2.validate_definition("KnowledgeSource", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def delete_knowledge_source(
        self,
        request: Request,
        workspace_id: OpaquePath,
        source_id: OpaquePath,
    ) -> Response:
        """@brief 以 If-Match 条件请求删除来源 / Conditionally request source deletion with If-Match.

        @param request 含 If-Match 的无 body request / Bodyless request with If-Match.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 来源标识 / Source identifier.
        @return 202 Job / 202 Job.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        typed_source = KnowledgeSourceId(source_id)
        supplied_if_match = if_match_header(request)
        current = await runtime.knowledge_v2.get_knowledge_source_for_deletion(
            principal,
            typed_workspace,
            typed_source,
        )
        current_payload = _knowledge_source(current)
        runtime.contracts_v2.validate_definition("KnowledgeSource", current_payload)
        expected_revision = match_etag_revision(
            supplied_if_match,
            current_payload,
            current.meta.revision,
        )
        job = await runtime.knowledge_v2.delete_knowledge_source(
            principal,
            typed_workspace,
            typed_source,
            expected_revision=expected_revision,
        )
        return _job_response(request, runtime, job, workspace_id)

    @_translate_http_errors
    async def list_knowledge_source_versions(
        self,
        request: Request,
        workspace_id: OpaquePath,
        source_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出一个来源的版本 / Page through one source's versions.

        @param request 已认证无 body request / Authenticated bodyless request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 来源标识 / Source identifier.
        @param cursor 可选签名 cursor / Optional signed cursor.
        @param limit 页长 / Page size.
        @return KnowledgeSourceVersionList / KnowledgeSourceVersionList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        typed_source = KnowledgeSourceId(source_id)
        filters: dict[str, JsonValue] = {
            "collection": "knowledge_source_versions",
            "source_id": source_id,
        }
        page = await runtime.knowledge_v2.list_knowledge_source_versions(
            principal,
            typed_workspace,
            typed_source,
            _page_request(
                cursor,
                limit,
                runtime.v2_cursor,
                principal,
                typed_workspace,
                filters,
                _VERSION_SORT,
            ),
        )
        payload = _collection_payload(
            page,
            project=_knowledge_source_version,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace,
            filters=filters,
            sort=_VERSION_SORT,
        )
        runtime.contracts_v2.validate_definition("KnowledgeSourceVersionList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def create_knowledge_source_version(
        self,
        request: Request,
        workspace_id: OpaquePath,
        source_id: OpaquePath,
    ) -> Response:
        """@brief 幂等创建不可变 KnowledgeSourceVersion / Idempotently create an immutable version.

        @param request 含 upload 引用与幂等键的 request / Request with upload reference and idempotency key.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 来源标识 / Source identifier.
        @return 201 KnowledgeSourceVersion / 201 KnowledgeSourceVersion.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        typed_source = KnowledgeSourceId(source_id)
        body = await _knowledge_json(
            request,
            runtime.contracts_v2,
            "CreateKnowledgeSourceVersionRequest",
        )

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后消费 upload 并创建版本 / Consume the upload and create a version after claim.

            @return 可重放 201 response / Replayable 201 response.
            """

            version = await runtime.knowledge_v2.create_knowledge_source_version(
                principal,
                typed_workspace,
                typed_source,
                UploadSessionId(_required_string(body, "upload_session_id")),
            )
            payload = _knowledge_source_version(version)
            runtime.contracts_v2.validate_definition("KnowledgeSourceVersion", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=(
                    f"/api/v2/workspaces/{workspace_id}/knowledge-sources/"
                    f"{source_id}/versions/{version.meta.id}"
                ),
                etag=True,
            )

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace,
            canonical_path=(
                f"/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/versions"
            ),
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            operation=operation,
            mapped_error_types=_KNOWLEDGE_BOUNDARY_ERRORS,
            map_error=_knowledge_problem,
        )

    @_translate_http_errors
    async def create_upload_session(
        self,
        request: Request,
        workspace_id: OpaquePath,
    ) -> Response:
        """@brief 幂等创建短期直传 session / Idempotently create a short-lived direct-upload session.

        @param request 含上传声明与幂等键的 request / Request with upload declaration and idempotency key.
        @param workspace_id 路径 Workspace / Path Workspace.
        @return 201 UploadSession / 201 UploadSession.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        body = await _knowledge_json(
            request,
            runtime.contracts_v2,
            "CreateUploadSessionRequest",
        )

        declaration = UploadDeclaration(
            _required_string(body, "filename"),
            _required_string(body, "media_type"),
            _required_integer(body, "size_bytes"),
            _required_string(body, "sha256"),
        )

        async def prepare(
            operation_id: IdempotencyPreparationId,
        ) -> PreparedUploadSessionCreation:
            """@brief 在事务外签发稳定直传 grant / Issue a stable direct-upload grant outside a transaction."""

            return await runtime.knowledge_v2.prepare_upload_session_creation(
                principal,
                typed_workspace,
                declaration,
                operation_id,
            )

        async def commit(prepared: PreparedUploadSessionCreation) -> ReplayableResponse:
            """@brief 原子提交 UploadSession 与 receipt / Atomically commit the UploadSession and receipt."""

            upload = await runtime.knowledge_v2.commit_upload_session_creation(
                principal,
                typed_workspace,
                prepared,
            )
            payload = _upload_session(upload)
            runtime.contracts_v2.validate_definition("UploadSession", payload)
            replay = replayable_json(
                payload,
                status_code=201,
                location=f"/api/v2/workspaces/{workspace_id}/upload-sessions/{upload.id}",
                etag=True,
            )
            return ReplayableResponse(
                replay.status_code,
                (*replay.headers, ("Cache-Control", "no-store")),
                replay.json_body,
            )

        return await prepared_idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace,
            canonical_path=f"/api/v2/workspaces/{workspace_id}/upload-sessions",
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            prepare=prepare,
            commit=commit,
            mapped_error_types=_KNOWLEDGE_BOUNDARY_ERRORS,
            map_error=_knowledge_problem,
        )

    @_translate_http_errors
    async def complete_upload_session(
        self,
        request: Request,
        workspace_id: OpaquePath,
        upload_id: OpaquePath,
    ) -> Response:
        """@brief 幂等完成并核验 UploadSession / Idempotently complete and verify an UploadSession.

        @param request 含完成声明与幂等键的 request / Request with completion claim and idempotency key.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param upload_id UploadSession 标识 / UploadSession identifier.
        @return 200 UploadSession / 200 UploadSession.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        typed_upload = UploadSessionId(upload_id)
        body = await _knowledge_json(
            request,
            runtime.contracts_v2,
            "CompleteUploadSessionRequest",
        )

        claim = UploadCompletionClaim(
            _required_integer(body, "size_bytes"),
            _required_string(body, "sha256"),
        )

        async def prepare(
            operation_id: IdempotencyPreparationId,
        ) -> PreparedUploadCompletion:
            """@brief 短事务 claim 后在事务外扫描 / Claim briefly, then scan outside a transaction."""

            return await runtime.knowledge_v2.prepare_upload_completion(
                principal,
                typed_workspace,
                typed_upload,
                claim,
                operation_id,
            )

        async def commit(prepared: PreparedUploadCompletion) -> ReplayableResponse:
            """@brief 原子完成 upload 与 receipt / Atomically complete the upload and receipt."""

            upload = await runtime.knowledge_v2.commit_upload_completion(
                principal,
                typed_workspace,
                prepared,
            )
            payload = _upload_session(upload)
            runtime.contracts_v2.validate_definition("UploadSession", payload)
            replay = replayable_json(payload, status_code=200, etag=True)
            return ReplayableResponse(
                replay.status_code,
                (*replay.headers, ("Cache-Control", "no-store")),
                replay.json_body,
            )

        return await prepared_idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace,
            canonical_path=(
                f"/api/v2/workspaces/{workspace_id}/upload-sessions/{upload_id}/completions"
            ),
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            prepare=prepare,
            commit=commit,
            mapped_error_types=_KNOWLEDGE_BOUNDARY_ERRORS,
            map_error=_knowledge_problem,
        )

    @_translate_http_errors
    async def create_ingestion_job(
        self,
        request: Request,
        workspace_id: OpaquePath,
        source_id: OpaquePath,
    ) -> Response:
        """@brief 幂等创建 knowledge.ingest Job / Idempotently create a knowledge.ingest Job.

        @param request 含 Job body 与幂等键的 request / Request with job body and idempotency key.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 来源标识 / Source identifier.
        @return 202 Job / 202 Job.
        """

        return await self._create_knowledge_job(
            request,
            workspace_id,
            source_id,
            operation="ingestion-jobs",
        )

    @_translate_http_errors
    async def create_sync_job(
        self,
        request: Request,
        workspace_id: OpaquePath,
        source_id: OpaquePath,
    ) -> Response:
        """@brief 幂等创建 knowledge.sync Job / Idempotently create a knowledge.sync Job.

        @param request 含 Job body 与幂等键的 request / Request with job body and idempotency key.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 来源标识 / Source identifier.
        @return 202 Job / 202 Job.
        """

        return await self._create_knowledge_job(
            request,
            workspace_id,
            source_id,
            operation="sync-jobs",
        )

    async def _create_knowledge_job(
        self,
        request: Request,
        workspace_id: str,
        source_id: str,
        *,
        operation: str,
    ) -> Response:
        """@brief 共享 ingestion/sync Job transport / Share ingestion/sync Job transport.

        @param request 当前 request / Current request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 来源标识 / Source identifier.
        @param operation ``ingestion-jobs`` 或 ``sync-jobs`` / ``ingestion-jobs`` or ``sync-jobs``.
        @return 幂等 202 Job / Idempotent 202 Job.
        @raise RuntimeError operation 不是两个冻结值时抛出 / Raised for an unsupported operation.
        """

        if operation not in {"ingestion-jobs", "sync-jobs"}:
            raise RuntimeError("unsupported Knowledge Job operation")
        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace = WorkspaceId(workspace_id)
        typed_source = KnowledgeSourceId(source_id)
        body = await _knowledge_json(
            request,
            runtime.contracts_v2,
            "CreateKnowledgeJobRequest",
        )

        async def execute() -> ReplayableResponse:
            """@brief 首次 claim 后创建对应 Job / Create the selected Job after the first claim.

            @return 可重放 202 Job / Replayable 202 Job.
            """

            command = CreateKnowledgeJobCommand(_optional_boolean(body, "force", default=False))
            if operation == "ingestion-jobs":
                job = await runtime.knowledge_v2.create_ingestion_job(
                    principal,
                    typed_workspace,
                    typed_source,
                    command,
                )
            else:
                job = await runtime.knowledge_v2.create_sync_job(
                    principal,
                    typed_workspace,
                    typed_source,
                    command,
                )
            return _job_replay(runtime, job, workspace_id)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace,
            canonical_path=(
                f"/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/{operation}"
            ),
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            operation=execute,
            mapped_error_types=_KNOWLEDGE_BOUNDARY_ERRORS,
            map_error=_knowledge_problem,
        )

    @_translate_http_errors
    async def search_knowledge(
        self,
        request: Request,
        workspace_id: OpaquePath,
    ) -> Response:
        """@brief 执行已授权 hybrid Knowledge search / Execute an authorized hybrid Knowledge search.

        @param request 含 KnowledgeSearchRequest 的 request / Request carrying KnowledgeSearchRequest.
        @param workspace_id 路径 Workspace / Path Workspace.
        @return KnowledgeSearchResult / KnowledgeSearchResult.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        body = await _knowledge_json(request, runtime.contracts_v2, "KnowledgeSearchRequest")
        result = await runtime.knowledge_v2.search_knowledge(
            verified_principal(request),
            WorkspaceId(workspace_id),
            _knowledge_search_request(body),
        )
        payload = _knowledge_search_result(result)
        runtime.contracts_v2.validate_definition("KnowledgeSearchResult", payload)
        return json_response(request, payload, cache_control="no-store")

    @_translate_http_errors
    async def evaluate_knowledge_access(
        self,
        request: Request,
        workspace_id: OpaquePath,
    ) -> Response:
        """@brief 生成不可作执行授权的访问解释 / Produce access explanations that are not execution grants.

        @param request 含 KnowledgeAccessEvaluationRequest 的 request / Request carrying an access evaluation.
        @param workspace_id 路径 Workspace / Path Workspace.
        @return KnowledgeAccessEvaluationResult / KnowledgeAccessEvaluationResult.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        body = await _knowledge_json(
            request,
            runtime.contracts_v2,
            "KnowledgeAccessEvaluationRequest",
        )
        result = await runtime.knowledge_v2.evaluate_knowledge_access(
            verified_principal(request),
            WorkspaceId(workspace_id),
            _knowledge_access_request(body),
        )
        payload = _knowledge_access_result(result)
        runtime.contracts_v2.validate_definition("KnowledgeAccessEvaluationResult", payload)
        return json_response(request, payload, cache_control="no-store")


def _declared_status(method: str, path: str) -> int:
    """@brief 返回 OpenAPI 冻结成功状态 / Return the frozen OpenAPI success status.

    @param method HTTP method / HTTP method.
    @param path 路由模板 / Route template.
    @return 200、201 或 202 / 200, 201, or 202.
    """

    if method == "DELETE" or path.endswith(("/ingestion-jobs", "/sync-jobs")):
        return 202
    if method == "POST" and not path.endswith(
        ("/completions", "/knowledge-searches", "/knowledge-access-evaluations")
    ):
        return 201
    return 200


async def _knowledge_json(
    request: Request,
    validator: ContractDefinitionValidator,
    definition: str,
) -> dict[str, JsonValue]:
    """@brief 以 8 MiB 上限严格读取 5.3 JSON object / Strictly read a section-5.3 JSON object with an 8 MiB cap.

    @param request 当前 request / Current request.
    @param validator 权威 schema validator / Authoritative schema validator.
    @param definition request definition 名 / Request definition name.
    @return 已校验 object / Validated object.
    """

    return await strict_json_object(
        request,
        validator=validator,
        definition=definition,
        max_body_bytes=_MAX_KNOWLEDGE_BODY_BYTES,
        max_depth=_MAX_KNOWLEDGE_JSON_DEPTH,
    )


def _sensitive_body_fingerprint(canonical_body: bytes, key: bytes) -> bytes:
    """@brief 将含 secret body 变为 keyed HMAC 后再交给通用幂等层 / HMAC a secret-bearing body before generic idempotency.

    @param canonical_body 含 API token 的规范 JSON bytes / Canonical JSON bytes containing an API token.
    @param key 独立 secret-aware HMAC key / Independent secret-aware HMAC key.
    @return ``b"hmac-sha256:" + hex`` / ``b"hmac-sha256:" + hex``.
    @raise RuntimeError key 少于 32 字节时 fail closed / Fails closed when the key is shorter than 32 bytes.
    @note 通用 executor、receipt 与 persistence 永远只能看到返回摘要 / The generic executor,
        receipt, and persistence can observe only the returned digest.
    """

    if not isinstance(key, bytes) or len(key) < 32:
        raise RuntimeError("sensitive idempotency HMAC key must contain at least 32 bytes")
    digest = hmac.new(
        key,
        canonical_body,
        hashlib.sha256,
    ).hexdigest()
    return b"hmac-sha256:" + digest.encode("ascii")


def _authorization_idempotency_key_hash(
    idempotency_key: str,
    secret: bytes,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
) -> str:
    """@brief 生成 actor/workspace/route 绑定的专用 key 摘要 / Hash a dedicated key bound to actor, workspace, and route.

    @param idempotency_key 已校验的原始请求 key / Validated raw request key.
    @param secret 独立服务端 HMAC secret / Independent server-side HMAC secret.
    @param principal 已验签 principal / Verified principal.
    @param workspace_id 路径 Workspace / Path workspace.
    @return 不泄漏原始 key 的 SHA-256 HMAC 十六进制摘要 / SHA-256 HMAC hex digest that does
        not disclose the raw key.
    @raise RuntimeError secret 少于 32 字节时 fail closed / Raised when the secret is shorter than
        32 bytes.

    @note 每段使用长度前缀，避免字符串拼接边界歧义；专用存储以该摘要建立唯一约束，
        authorization URL 与 user code 仍只进入 AEAD 密文 / Every field is length-framed to avoid
        concatenation ambiguity; authorization URLs and user codes still enter only AEAD ciphertext.
    """

    if not isinstance(secret, bytes) or len(secret) < 32:
        raise RuntimeError("sensitive idempotency HMAC key must contain at least 32 bytes")
    fields = (
        b"knowledge.connection-authorization-session.v1",
        str(principal.user_id).encode("utf-8"),
        str(workspace_id).encode("utf-8"),
        idempotency_key.encode("ascii"),
    )
    framed = b"".join(len(field).to_bytes(4, "big") + field for field in fields)
    return hmac.digest(secret, framed, "sha256").hex()


def _page_request(
    cursor: str | None,
    limit: int,
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    filters: Mapping[str, JsonValue],
    sort: Sequence[str],
) -> KnowledgePageRequest:
    """@brief 将授权上下文绑定 cursor 解为 repository continuation / Decode an authorization-bound cursor.

    @param cursor 可选签名 cursor / Optional signed cursor.
    @param limit 页长 / Page size.
    @param codec cursor codec / Cursor codec.
    @param principal 当前 principal / Current principal.
    @param workspace_id 路径 Workspace / Path Workspace.
    @param filters 完整集合上下文 / Complete collection context.
    @param sort 稳定排序 / Stable ordering.
    @return KnowledgePageRequest / KnowledgePageRequest.
    @raise DomainError cursor payload 不是非空 continuation 时抛出 / Raised for an invalid continuation.
    """

    if cursor is None:
        return KnowledgePageRequest(limit=limit)
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
        return KnowledgePageRequest(limit=limit, after=position)
    except ValueError as error:
        raise DomainError(
            Problem("http.cursor_invalid", 400, "Pagination cursor is invalid")
        ) from error


def _collection_payload[ItemT](
    page: KnowledgePage[ItemT],
    *,
    project: Callable[[ItemT], JsonValue],
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    filters: Mapping[str, JsonValue],
    sort: Sequence[str],
) -> dict[str, JsonValue]:
    """@brief 封装应用层已分页结果 / Wrap an application-paginated result.

    @param page 当前应用页 / Current application page.
    @param project 项目公开投影 / Public item projection.
    @param codec cursor codec / Cursor codec.
    @param principal 当前 principal / Current principal.
    @param workspace_id 路径 Workspace / Path Workspace.
    @param filters 完整集合上下文 / Complete collection context.
    @param sort 稳定排序 / Stable ordering.
    @return `{items,page}` / `{items,page}`.
    """

    next_cursor = None
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


def _job_response(
    request: Request,
    runtime: V2KnowledgeRuntime,
    job: Job,
    workspace_id: str,
) -> Response:
    """@brief 校验并返回非 receipt 的 202 Job / Validate and return a non-receipt 202 Job.

    @param request 当前 request / Current request.
    @param runtime 当前 runtime / Current runtime.
    @param job 新建 Job / Newly created Job.
    @param workspace_id canonical path Workspace / Canonical path Workspace.
    @return 带 Location 与强 ETag 的 202 response / 202 response with Location and strong ETag.
    """

    payload = _job(job)
    runtime.contracts_v2.validate_definition("Job", payload)
    return resource_response(
        request,
        payload,
        status_code=202,
        location=f"/api/v2/workspaces/{workspace_id}/jobs/{job.meta.id}",
    )


def _job_replay(runtime: V2KnowledgeRuntime, job: Job, workspace_id: str) -> ReplayableResponse:
    """@brief 校验并构造可重放 202 Job / Validate and build a replayable 202 Job.

    @param runtime 当前 runtime / Current runtime.
    @param job 新建 Job / Newly created Job.
    @param workspace_id canonical path Workspace / Canonical path Workspace.
    @return 可重放 202 response / Replayable 202 response.
    """

    payload = _job(job)
    runtime.contracts_v2.validate_definition("Job", payload)
    return replayable_json(
        payload,
        status_code=202,
        location=f"/api/v2/workspaces/{workspace_id}/jobs/{job.meta.id}",
        etag=True,
    )


def _knowledge_problem(error: BaseException) -> Problem:
    """@brief 将 5.3 应用/领域失败稳定映射为 HTTP Problem / Map section-5.3 failures to HTTP Problems.

    @param error 预期失败 / Expected failure.
    @return transport Problem / Transport Problem.
    """

    if isinstance(error, KnowledgeResourceNotFound):
        return Problem(error.code, 404, "Resource was not found", detail=error.detail)
    if isinstance(error, KnowledgePreconditionFailed):
        return Problem(
            error.code,
            412,
            "Resource precondition failed",
            detail=error.detail,
        )
    if isinstance(error, KnowledgeConflict):
        return Problem(error.code, 409, "Resource state conflict", detail=error.detail)
    if isinstance(error, InvalidKnowledgeCommand):
        return Problem(
            error.code,
            422,
            "Command violates a domain constraint",
            detail=error.detail,
        )
    if isinstance(
        error,
        (ConnectionDomainError, KnowledgeDomainError, KnowledgeRetrievalError, UploadDomainError),
    ):
        return Problem(
            "request.domain_constraint",
            422,
            "Request violates a domain constraint",
            detail=str(error),
        )
    if isinstance(error, UnknownPrincipal):
        return Problem("oauth.invalid_token", 401, "Bearer token is invalid")
    if isinstance(error, AuthorizationDenied):
        if error.reason in {
            "authorization.membership_missing",
            "authorization.membership_inactive",
        }:
            return Problem("resource.not_found", 404, "Resource was not found")
        if error.reason == "authorization.scope_missing":
            return Problem("oauth.insufficient_scope", 403, "Token scope is insufficient")
        return Problem("authorization.denied", 403, "Action is not permitted")
    if isinstance(error, DomainInvariantError):
        return Problem(
            "resource.state_conflict",
            409,
            "Resource state conflict",
            detail="The resource does not satisfy the required Knowledge state.",
        )
    if isinstance(error, KnowledgeApplicationError):
        return Problem(error.code, 409, "Knowledge operation failed", detail=error.detail)
    raise TypeError(f"unsupported Knowledge error: {type(error).__name__}")


def _source_input(body: Mapping[str, JsonValue]) -> KnowledgeSourceInput:
    """@brief 穷尽解析 KnowledgeSourceInput 判别联合 / Exhaustively parse KnowledgeSourceInput.

    @param body 已校验 input object / Validated input object.
    @return 类型化来源输入 / Typed source input.
    @raise RuntimeError validator 接受未知 discriminator 时抛出 / Raised if the validator accepts an unknown discriminator.
    """

    source_type = KnowledgeSourceType(_required_string(body, "source_type"))
    if source_type is KnowledgeSourceType.FILE:
        return FileSourceInput(UploadSessionId(_required_string(body, "upload_session_id")))
    if source_type in {
        KnowledgeSourceType.URL,
        KnowledgeSourceType.WEBSITE,
        KnowledgeSourceType.BLOG_FEED,
    }:
        return UrlSourceInput(source_type, _required_string(body, "url"))
    if source_type is KnowledgeSourceType.GIT_REPOSITORY:
        connection_id = _nullable_string(body, "connection_id")
        return GitSourceInput(
            _required_string(body, "clone_url"),
            _nullable_string(body, "ref"),
            _string_tuple(body, "include_paths"),
            _string_tuple(body, "exclude_paths"),
            ConnectionId(connection_id) if connection_id is not None else None,
        )
    if source_type is KnowledgeSourceType.MANUAL_NOTE:
        return ManualSourceInput(_required_string(body, "content"))
    if source_type is KnowledgeSourceType.RESUME:
        return ResumeSourceInput(ResumeId(_required_string(body, "resume_id")))
    if source_type is KnowledgeSourceType.CLOUD_DRIVE:
        return CloudDriveSourceInput(
            ConnectionId(_required_string(body, "connection_id")),
            _required_string(body, "remote_id"),
        )
    raise RuntimeError("validated KnowledgeSourceInput has an unknown discriminator")


def _visibility(body: Mapping[str, JsonValue]) -> KnowledgeVisibilityPolicy:
    """@brief 解析完整 KnowledgeVisibilityPolicy / Parse a complete KnowledgeVisibilityPolicy.

    @param body 已校验 policy object / Validated policy object.
    @return 类型化 policy / Typed policy.
    """

    retention = body.get("retention_days")
    return KnowledgeVisibilityPolicy(
        KnowledgeSensitivity(_required_string(body, "sensitivity")),
        PolicyEffect(_required_string(body, "default_effect")),
        tuple(
            _agent_grant(_array_object(value, "agent_grants"))
            for value in _required_array(body, "agent_grants")
        ),
        _required_boolean(body, "session_override_allowed"),
        tuple(
            ModelRegion(_array_string(value, "allowed_model_regions"))
            for value in _required_array(body, "allowed_model_regions")
        ),
        _required_boolean(body, "allow_external_model_processing"),
        None if retention is None else _integer(retention, "retention_days"),
        _required_integer(body, "policy_version"),
    )


def _agent_grant(body: Mapping[str, JsonValue]) -> AgentScopeGrant:
    """@brief 解析一个 AgentScopeGrant / Parse one AgentScopeGrant.

    @param body 已校验 grant object / Validated grant object.
    @return 类型化 grant / Typed grant.
    """

    return AgentScopeGrant(
        _required_string(body, "agent_scope"),
        PolicyEffect(_required_string(body, "effect")),
        tuple(
            KnowledgeOperation(_array_string(value, "allowed_operations"))
            for value in _required_array(body, "allowed_operations")
        ),
    )


def _knowledge_search_request(body: Mapping[str, JsonValue]) -> KnowledgeSearchRequest:
    """@brief 解析 KnowledgeSearchRequest / Parse KnowledgeSearchRequest.

    @param body 已校验 request object / Validated request object.
    @return 类型化 search request / Typed search request.
    """

    filters_value = body.get("filters", {})
    filters = _object(filters_value, "filters")
    return KnowledgeSearchRequest(
        _required_string(body, "query"),
        _knowledge_selection(_required_object(body, "selection")),
        _required_integer(body, "top_k"),
        SearchFilters(cast(Mapping[str, SearchFilterValue], deepcopy(filters))),
    )


def _knowledge_selection(body: Mapping[str, JsonValue]) -> KnowledgeSelection:
    """@brief 解析 KnowledgeSelection / Parse KnowledgeSelection.

    @param body 已校验 selection object / Validated selection object.
    @return 类型化 selection / Typed selection.
    """

    return KnowledgeSelection(
        KnowledgeSelectionMode(_required_string(body, "mode")),
        tuple(
            KnowledgeSourceId(_array_string(value, "include_source_ids"))
            for value in _required_array(body, "include_source_ids")
        ),
        tuple(
            KnowledgeSourceId(_array_string(value, "exclude_source_ids"))
            for value in _required_array(body, "exclude_source_ids")
        ),
        tuple(
            _knowledge_version_pin(_array_object(value, "pinned_versions"))
            for value in _required_array(body, "pinned_versions")
        ),
        _required_string(body, "agent_scope"),
    )


def _knowledge_version_pin(body: Mapping[str, JsonValue]) -> KnowledgeVersionPin:
    """@brief 解析 KnowledgeVersionPin / Parse KnowledgeVersionPin.

    @param body 已校验 pin object / Validated pin object.
    @return 类型化 pin / Typed pin.
    """

    return KnowledgeVersionPin(
        KnowledgeSourceId(_required_string(body, "source_id")),
        KnowledgeSourceVersionId(_required_string(body, "version_id")),
    )


def _knowledge_access_request(
    body: Mapping[str, JsonValue],
) -> KnowledgeAccessEvaluationRequest:
    """@brief 解析 KnowledgeAccessEvaluationRequest / Parse KnowledgeAccessEvaluationRequest.

    @param body 已校验 request object / Validated request object.
    @return 类型化 evaluation request / Typed evaluation request.
    """

    return KnowledgeAccessEvaluationRequest(
        tuple(
            KnowledgeSourceId(_array_string(value, "source_ids"))
            for value in _required_array(body, "source_ids")
        ),
        _required_string(body, "agent_scope"),
        KnowledgeOperation(_required_string(body, "operation")),
        _inference_intent(_required_object(body, "inference")),
    )


def _inference_intent(body: Mapping[str, JsonValue]) -> InferenceIntent:
    """@brief 解析 InferenceIntent / Parse InferenceIntent.

    @param body 已校验 inference object / Validated inference object.
    @return 类型化 inference intent / Typed inference intent.
    """

    latency = body.get("latency_budget_ms")
    return InferenceIntent(
        InferenceQualityTier(_required_string(body, "quality_tier")),
        None if latency is None else _integer(latency, "latency_budget_ms"),
        InferenceCostTier(_required_string(body, "cost_tier")),
        ModelRegion(_required_string(body, "data_region")),
        _required_boolean(body, "allow_provider_fallback"),
        _required_boolean(body, "allow_external_model_processing"),
    )


def _connection(value: Connection) -> dict[str, JsonValue]:
    """@brief 穷尽投影无 credential Connection / Exhaustively project a credential-free Connection.

    @param value Connection 领域投影 / Connection domain projection.
    @return Connection JSON / Connection JSON.
    """

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "provider": value.provider.value,
            "auth_method": value.auth_method.value,
            "display_name": value.display_name,
            "status": value.status.value,
            "scopes": list(value.scopes),
            "last_validated_at": (
                timestamp(value.last_validated_at) if value.last_validated_at is not None else None
            ),
            "problem": _problem_details(value.problem) if value.problem is not None else None,
        }
    )
    return payload


def _connection_authorization_session(
    value: ConnectionAuthorizationSession,
) -> dict[str, JsonValue]:
    """@brief 投影客户端授权 session / Project the client authorization session.

    @param value 授权 session / Authorization session.
    @return ConnectionAuthorizationSession JSON / ConnectionAuthorizationSession JSON.
    """

    return {
        "id": str(value.id),
        "provider": value.provider.value,
        "flow": value.flow.value,
        "authorization_url": value.authorization_url,
        "verification_uri": value.verification_uri,
        "user_code": value.user_code,
        "expires_at": timestamp(value.expires_at),
        "poll_interval_ms": value.poll_interval_ms,
    }


def _knowledge_source(value: KnowledgeSource) -> dict[str, JsonValue]:
    """@brief 穷尽投影无私有 input 的 KnowledgeSource / Exhaustively project a source without private input.

    @param value KnowledgeSource aggregate / KnowledgeSource aggregate.
    @return KnowledgeSource JSON / KnowledgeSource JSON.
    """

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "name": value.name,
            "source_type": value.source_type.value,
            "enabled": value.enabled,
            "public_config": _public_source_config(value),
            "visibility": _visibility_payload(value.visibility),
            "ingestion": {
                "status": value.ingestion.status.value,
                "document_count": value.ingestion.document_count,
                "chunk_count": value.ingestion.chunk_count,
                "last_success_at": (
                    timestamp(value.ingestion.last_success_at)
                    if value.ingestion.last_success_at is not None
                    else None
                ),
                "last_problem": (
                    _problem_details(value.ingestion.last_problem)
                    if value.ingestion.last_problem is not None
                    else None
                ),
            },
            "current_version_id": (
                str(value.current_version_id) if value.current_version_id is not None else None
            ),
        }
    )
    return payload


def _public_source_config(value: KnowledgeSource) -> dict[str, JsonValue]:
    """@brief 只投影公开来源配置 allowlist / Project only the public source-config allowlist.

    @param value KnowledgeSource aggregate / KnowledgeSource aggregate.
    @return PublicKnowledgeSourceConfig JSON / PublicKnowledgeSourceConfig JSON.
    @note ``connection_id``、``remote_id``、路径规则与 manual content 永不进入响应 /
        ``connection_id``, ``remote_id``, path rules, and manual content never enter responses.
    """

    config = value.public_config
    payload: dict[str, JsonValue] = {}
    if config.filename is not None:
        payload["filename"] = config.filename
    if config.media_type is not None:
        payload["media_type"] = config.media_type
    if config.url is not None:
        payload["url"] = config.url
    if config.clone_url is not None:
        payload["clone_url"] = config.clone_url
    if config.ref is not None:
        payload["ref"] = config.ref
    if config.resume_id is not None:
        payload["resume_id"] = str(config.resume_id)
    return payload


def _visibility_payload(value: KnowledgeVisibilityPolicy) -> dict[str, JsonValue]:
    """@brief 投影完整 KnowledgeVisibilityPolicy / Project a complete KnowledgeVisibilityPolicy.

    @param value visibility policy / Visibility policy.
    @return KnowledgeVisibilityPolicy JSON / KnowledgeVisibilityPolicy JSON.
    """

    return {
        "sensitivity": value.sensitivity.value,
        "default_effect": value.default_effect.value,
        "agent_grants": [
            {
                "agent_scope": grant.agent_scope,
                "effect": grant.effect.value,
                "allowed_operations": [operation.value for operation in grant.allowed_operations],
            }
            for grant in value.agent_grants
        ],
        "session_override_allowed": value.session_override_allowed,
        "allowed_model_regions": [region.value for region in value.allowed_model_regions],
        "allow_external_model_processing": value.allow_external_model_processing,
        "retention_days": value.retention_days,
        "policy_version": value.policy_version,
    }


def _knowledge_source_version(value: KnowledgeSourceVersion) -> dict[str, JsonValue]:
    """@brief 穷尽投影 KnowledgeSourceVersion / Exhaustively project KnowledgeSourceVersion.

    @param value version aggregate / Version aggregate.
    @return KnowledgeSourceVersion JSON / KnowledgeSourceVersion JSON.
    """

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "source_id": str(value.snapshot.source_id),
            "version_number": value.snapshot.version_number,
            "content_sha256": value.snapshot.content_sha256,
            "size_bytes": value.snapshot.size_bytes,
            "status": value.status.value,
            "indexed_at": timestamp(value.indexed_at) if value.indexed_at is not None else None,
        }
    )
    return payload


def _upload_session(value: UploadSessionView) -> dict[str, JsonValue]:
    """@brief 投影 UploadSession 公开 view / Project an UploadSession public view.

    @param value UploadSession view / UploadSession view.
    @return UploadSession JSON / UploadSession JSON.
    """

    return {
        "id": str(value.id),
        "workspace_id": str(value.workspace_id),
        "status": value.status.value,
        "method": value.method,
        "upload_url": value.upload_url,
        "required_headers": dict(value.required_headers),
        "expires_at": timestamp(value.expires_at),
        "artifact_ref": (
            _resource_ref(value.artifact_ref) if value.artifact_ref is not None else None
        ),
    }


def _knowledge_search_result(value: KnowledgeSearchResult) -> dict[str, JsonValue]:
    """@brief 穷尽投影 KnowledgeSearchResult / Exhaustively project KnowledgeSearchResult.

    @param value search result / Search result.
    @return KnowledgeSearchResult JSON / KnowledgeSearchResult JSON.
    """

    return {
        "query": value.query,
        "citations": [
            {
                "source_id": str(citation.source_id),
                "version_id": str(citation.version_id),
                "locator": citation.locator,
                "quote": citation.quote,
                "score": citation.score,
            }
            for citation in value.citations
        ],
        "policy_version": value.policy_version,
    }


def _knowledge_access_result(
    value: KnowledgeAccessEvaluationResult,
) -> dict[str, JsonValue]:
    """@brief 穷尽投影 KnowledgeAccessEvaluationResult / Exhaustively project an access-evaluation result.

    @param value evaluation result / Evaluation result.
    @return KnowledgeAccessEvaluationResult JSON / KnowledgeAccessEvaluationResult JSON.
    """

    return {
        "evaluated_at": timestamp(value.evaluated_at),
        "decisions": [
            {
                "source_id": str(decision.source_id),
                "effect": decision.effect.value,
                "policy_version": decision.policy_version,
                "reason_codes": list(decision.reason_codes),
            }
            for decision in value.decisions
        ],
    }


def _resource_ref(value: ResourceRef) -> dict[str, JsonValue]:
    """@brief 投影 ResourceRef 并省略空 revision / Project ResourceRef and omit an absent revision.

    @param value 领域资源引用 / Domain resource reference.
    @return ResourceRef JSON / ResourceRef JSON.
    """

    payload: dict[str, JsonValue] = {
        "resource_type": value.resource_type,
        "id": value.id,
    }
    if value.revision is not None:
        payload["revision"] = value.revision
    return payload


def _job(value: Job) -> dict[str, JsonValue]:
    """@brief 穷尽投影统一 Job / Exhaustively project a unified Job.

    @param value Job aggregate / Job aggregate.
    @return Job JSON / Job JSON.
    """

    payload = resource_meta(value.meta)
    progress: dict[str, JsonValue] | None = None
    if value.progress is not None:
        progress = {
            "phase": value.progress.phase,
            "completed": value.progress.completed,
            "total": value.progress.total,
            "unit": value.progress.unit.value,
        }
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "kind": value.kind,
            "subject": _resource_ref(value.subject),
            "status": value.status.value,
            "progress": progress,
            "result_refs": [_resource_ref(reference) for reference in value.result_refs],
            "problem": _problem_details(value.problem) if value.problem is not None else None,
            "started_at": timestamp(value.started_at) if value.started_at is not None else None,
            "finished_at": (
                timestamp(value.finished_at) if value.finished_at is not None else None
            ),
        }
    )
    return payload


def _problem_details(value: ProblemDetails) -> dict[str, JsonValue]:
    """@brief 投影公开安全 ProblemDetails / Project public-safe ProblemDetails.

    @param value immutable ProblemDetails / Immutable ProblemDetails.
    @return ProblemDetails JSON / ProblemDetails JSON.
    """

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
            key: _thaw_json(extension) for key, extension in value.extensions.items()
        }
    return payload


def _problem_field_error(value: ProblemFieldError) -> dict[str, JsonValue]:
    """@brief 投影 ProblemFieldError / Project ProblemFieldError.

    @param value 字段错误 / Field error.
    @return ProblemFieldError JSON / ProblemFieldError JSON.
    """

    payload: dict[str, JsonValue] = {"pointer": value.pointer, "code": value.code}
    if value.message_key is not None:
        payload["message_key"] = value.message_key
    if value.params:
        payload["params"] = dict(value.params)
    return payload


def _thaw_json(value: object) -> JsonValue:
    """@brief 将深度 immutable JSON 复制为 wire JSON / Copy deeply immutable JSON into wire JSON.

    @param value immutable JSON value / Immutable JSON value.
    @return 可序列化 JSON value / Serializable JSON value.
    @raise TypeError 值不属于 JSON 时抛出 / Raised for a non-JSON value.
    """

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_thaw_json(item) for item in value]
    raise TypeError("domain extension contains a non-JSON value")


def _required_string(body: Mapping[str, JsonValue], field: str) -> str:
    """@brief 读取 schema 已保证存在的字符串 / Read a schema-guaranteed string.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @return string value / String value.
    @raise RuntimeError validator 保证被破坏时抛出 / Raised when validator guarantees are broken.
    """

    value = body.get(field)
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a string")
    return value


def _optional_string(body: Mapping[str, JsonValue], field: str) -> str | None:
    """@brief 读取可缺省但不可 null 的字符串 / Read an optional non-null string.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @return 字符串或缺省 None / String or None when absent.
    @raise RuntimeError 显式 null 或类型错误时抛出 / Raised for explicit null or the wrong type.
    """

    if field not in body:
        return None
    return _required_string(body, field)


def _nullable_string(body: Mapping[str, JsonValue], field: str) -> str | None:
    """@brief 读取 required nullable 字符串 / Read a required nullable string.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @return 字符串或 null / String or null.
    @raise RuntimeError 字段缺失或类型错误时抛出 / Raised when absent or wrongly typed.
    """

    if field not in body:
        raise RuntimeError(f"validated field {field} must be present")
    value = body[field]
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a string or null")
    return value


def _required_integer(body: Mapping[str, JsonValue], field: str) -> int:
    """@brief 读取 schema 已保证存在的 integer / Read a schema-guaranteed integer.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @return integer value / Integer value.
    @raise RuntimeError validator 保证被破坏时抛出 / Raised when validator guarantees are broken.
    """

    if field not in body:
        raise RuntimeError(f"validated field {field} must be present")
    return _integer(body[field], field)


def _integer(value: JsonValue, field: str) -> int:
    """@brief 将 JSON 值收窄为非 bool integer / Narrow a JSON value to a non-boolean integer.

    @param value JSON value / JSON value.
    @param field 诊断字段名 / Diagnostic field name.
    @return integer value / Integer value.
    @raise RuntimeError 值不是 integer 时抛出 / Raised when the value is not an integer.
    """

    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"validated field {field} must be an integer")
    return value


def _required_boolean(body: Mapping[str, JsonValue], field: str) -> bool:
    """@brief 读取 schema 已保证存在的 boolean / Read a schema-guaranteed boolean.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @return boolean value / Boolean value.
    @raise RuntimeError validator 保证被破坏时抛出 / Raised when validator guarantees are broken.
    """

    value = body.get(field)
    if not isinstance(value, bool):
        raise RuntimeError(f"validated field {field} must be a boolean")
    return value


def _optional_boolean(
    body: Mapping[str, JsonValue],
    field: str,
    *,
    default: bool,
) -> bool:
    """@brief 读取带显式默认值的 optional boolean / Read an optional boolean with an explicit default.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @param default 缺省值 / Default when absent.
    @return boolean value / Boolean value.
    """

    if field not in body:
        return default
    return _required_boolean(body, field)


def _required_object(
    body: Mapping[str, JsonValue],
    field: str,
) -> Mapping[str, JsonValue]:
    """@brief 读取 schema 已保证存在的 object / Read a schema-guaranteed object.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @return nested object / Nested object.
    @raise RuntimeError validator 保证被破坏时抛出 / Raised when validator guarantees are broken.
    """

    if field not in body:
        raise RuntimeError(f"validated field {field} must be present")
    return _object(body[field], field)


def _object(value: JsonValue, field: str) -> Mapping[str, JsonValue]:
    """@brief 将 JSON 值收窄为 object / Narrow a JSON value to an object.

    @param value JSON value / JSON value.
    @param field 诊断字段名 / Diagnostic field name.
    @return object mapping / Object mapping.
    @raise RuntimeError 值不是 object 时抛出 / Raised when the value is not an object.
    """

    if not isinstance(value, dict):
        raise RuntimeError(f"validated field {field} must be an object")
    return value


def _required_array(body: Mapping[str, JsonValue], field: str) -> list[JsonValue]:
    """@brief 读取 schema 已保证存在的 array / Read a schema-guaranteed array.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @return array values / Array values.
    @raise RuntimeError validator 保证被破坏时抛出 / Raised when validator guarantees are broken.
    """

    value = body.get(field)
    if not isinstance(value, list):
        raise RuntimeError(f"validated field {field} must be an array")
    return value


def _array_string(value: JsonValue, field: str) -> str:
    """@brief 将 array item 收窄为 string / Narrow an array item to a string.

    @param value array item / Array item.
    @param field 诊断字段名 / Diagnostic field name.
    @return string value / String value.
    @raise RuntimeError item 不是 string 时抛出 / Raised when the item is not a string.
    """

    if not isinstance(value, str):
        raise RuntimeError(f"validated array {field} must contain strings")
    return value


def _array_object(value: JsonValue, field: str) -> Mapping[str, JsonValue]:
    """@brief 将 array item 收窄为 object / Narrow an array item to an object.

    @param value array item / Array item.
    @param field 诊断字段名 / Diagnostic field name.
    @return object value / Object value.
    @raise RuntimeError item 不是 object 时抛出 / Raised when the item is not an object.
    """

    return _object(value, field)


def _string_tuple(body: Mapping[str, JsonValue], field: str) -> tuple[str, ...]:
    """@brief 读取 string array 为 immutable tuple / Read a string array into an immutable tuple.

    @param body JSON object / JSON object.
    @param field 字段名 / Field name.
    @return string tuple / String tuple.
    """

    return tuple(_array_string(value, field) for value in _required_array(body, field))


def create_v2_knowledge_router(
    resolve_runtime: V2KnowledgeRuntimeResolver = v2_knowledge_runtime_from_request,
) -> APIRouter:
    """@brief 创建可注入 runtime 的 5.3 router / Create a section-5.3 router with injectable runtime.

    @param resolve_runtime request runtime resolver / Request runtime resolver.
    @return 包含精确 17 条路由的 APIRouter / APIRouter containing exactly 17 routes.
    """

    return V2KnowledgeHttpAdapter(resolve_runtime).router


router_v2_knowledge = create_v2_knowledge_router()
"""@brief 默认 production Knowledge router / Default production Knowledge router."""


__all__ = [
    "V2KnowledgeHttpAdapter",
    "V2KnowledgeRuntime",
    "V2KnowledgeRuntimeResolver",
    "create_v2_knowledge_router",
    "router_v2_knowledge",
    "v2_knowledge_runtime_from_request",
]
