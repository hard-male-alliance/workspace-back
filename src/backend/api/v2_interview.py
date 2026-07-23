"""@brief API V2 Interview HTTP 适配器 / API V2 Interview HTTP adapter.

该模块仅处理 5.5 的严格 JSON/query/body、签名 cursor、强 ETag/If-Match、
持久幂等与穷尽投影。Session consent 与 Realtime credential 使用 no-store；
两个敏感创建 receipt 以 AES-GCM 封装，通用幂等存储只能看到密文。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime
from functools import wraps
from typing import Concatenate, Protocol, cast

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import APIRouter, Request
from fastapi.responses import Response

from backend.api.constants import PROTECTED_RESOURCE_METADATA_URL
from backend.api.v2_http import list_response, validate_idempotency_key
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
    problem_payload,
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
from backend.application.interview_v2 import (
    CreateInterviewReportJobCommand,
    CreateInterviewScenarioCommand,
    CreateInterviewSessionCommand,
    EndInterviewSessionCommand,
    InterviewApplicationError,
    InterviewApplicationService,
    InterviewConflict,
    InterviewMutationContext,
    InterviewPortProtocolError,
    InterviewPreconditionFailed,
    InterviewResourceNotFound,
    InvalidInterviewCommand,
)
from backend.application.ports.access import AuthorizationDenied, UnknownPrincipal
from backend.application.ports.interview_v2 import (
    InterviewPage,
    InterviewPageRequest,
    InterviewPolicyDenied,
)
from backend.application.ports.v2_idempotency import (
    IdempotencyRequest,
    IdempotencyScope,
    ReplayableResponse,
    V2IdempotencyExecutor,
)
from backend.domain.common import DomainError, Problem
from backend.domain.interview_v2 import (
    AvatarOutputMode,
    CreateRealtimeConnectionSpec,
    EndInterviewReason,
    FallbackTransport,
    IceServer,
    InterviewActionPlanItem,
    InterviewAvatarPreferences,
    InterviewCommunicationMetrics,
    InterviewDifficulty,
    InterviewDomainError,
    InterviewEvidence,
    InterviewMediaPreferences,
    InterviewReport,
    InterviewReportId,
    InterviewRichText,
    InterviewRubric,
    InterviewScenario,
    InterviewScenarioId,
    InterviewScenarioPatch,
    InterviewScenarioSpec,
    InterviewSessionId,
    InterviewSessionView,
    InterviewTransitionError,
    JobTarget,
    RealtimeConnection,
    RealtimeTransport,
    RecordingConsent,
    RubricDimension,
    RubricScore,
    ScoreScale,
    TranscriptSegment,
)
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeSelection,
    KnowledgeSelectionMode,
    KnowledgeVersionPin,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    ModelRegion,
)
from backend.domain.platform import (
    Job,
    ProblemDetails,
    ProblemFieldError,
)
from backend.domain.platform import (
    JsonValue as PlatformJsonValue,
)
from backend.domain.principals import DomainInvariantError, TokenPrincipal, WorkspaceId
from backend.domain.resources import ResourceRef

_MAX_INTERVIEW_BODY_BYTES = 2 * 1024 * 1024
"""@brief Interview 请求原始 body 上限 / Interview raw-body limit."""

_MAX_INTERVIEW_JSON_DEPTH = 24
"""@brief Interview JSON 嵌套上限 / Interview JSON nesting limit."""

_SCENARIO_SORT = ("created_at", "id")
"""@brief Scenario 稳定排序 / Stable Scenario ordering."""

_SESSION_SORT = ("created_at", "id")
"""@brief Session 稳定排序 / Stable Session ordering."""

_TRANSCRIPT_SORT = ("sequence", "id")
"""@brief Transcript 稳定排序 / Stable Transcript ordering."""

_PRIVATE_CACHE_CONTROL = "private, no-store"
"""@brief 同意、credential 与证据响应的 cache policy / Cache policy for consent, credentials, and evidence."""

_SENSITIVE_RECEIPT_LABEL = b"api-v2-interview-sensitive-receipt\x00"
"""@brief 敏感 receipt 密钥派生标签 / Sensitive-receipt key-derivation label."""

_INTERVIEW_BOUNDARY_ERRORS: tuple[type[Exception], ...] = (
    InterviewApplicationError,
    InterviewDomainError,
    InterviewPolicyDenied,
    AuthorizationDenied,
    UnknownPrincipal,
    DomainInvariantError,
)
"""@brief Interview boundary 可稳定映射的预期错误 / Expected Interview boundary errors."""


class V2InterviewRuntime(Protocol):
    """@brief 单个 Interview request 的运行时依赖 / Runtime dependencies for one Interview request."""

    @property
    def interview_v2(self) -> InterviewApplicationService:
        """@brief 返回 Interview 应用服务 / Return the Interview application service."""

    @property
    def contracts_v2(self) -> ContractDefinitionValidator:
        """@brief 返回权威 V2 schema validator / Return the authoritative V2 schema validator."""

    @property
    def v2_cursor(self) -> CursorCodec:
        """@brief 返回签名 cursor codec / Return the signed cursor codec."""

    @property
    def v2_idempotency(self) -> V2IdempotencyExecutor:
        """@brief 返回持久幂等 executor / Return the durable idempotency executor."""

    @property
    def sensitive_idempotency_key(self) -> bytes:
        """@brief 返回敏感 receipt 封装密钥材料 / Return key material for sensitive receipt sealing."""


type V2InterviewRuntimeResolver = Callable[[Request], V2InterviewRuntime]
"""@brief 从 request 解析 Interview runtime / Resolve Interview runtime from a request."""


def v2_interview_runtime_from_request(request: Request) -> V2InterviewRuntime:
    """@brief 从 composition container 取得 Interview 依赖 / Read Interview dependencies from the composition container."""
    container = getattr(request.app.state, "container", None)
    if container is None:
        raise RuntimeError("backend container is unavailable")
    return cast(V2InterviewRuntime, container)


def _translate_http_errors[AdapterT, **ParamT](
    handler: Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
) -> Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]]:
    """@brief 将预期 Interview 错误转为 ProblemDetails / Translate expected Interview errors to Problem Details."""

    @wraps(handler)
    async def wrapped(
        adapter: AdapterT,
        request: Request,
        *args: ParamT.args,
        **kwargs: ParamT.kwargs,
    ) -> Response:
        """@brief 执行 endpoint 并封闭已知失败 / Execute an endpoint and close known failures."""
        try:
            return await handler(adapter, request, *args, **kwargs)
        except DomainError as error:
            return problem_response(request, error.problem, error=error)
        except _INTERVIEW_BOUNDARY_ERRORS as error:
            return problem_response(request, _interview_problem(error), error=error)

    return cast(
        Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
        wrapped,
    )


class V2InterviewHttpAdapter:
    """@brief 把 Interview 5.5 用例适配为冻结路由 / Adapt Interview 5.5 use cases to frozen routes."""

    def __init__(self, resolve_runtime: V2InterviewRuntimeResolver) -> None:
        """@brief 构建并注册 12 条路由 / Build and register all twelve routes."""
        self._resolve_runtime = resolve_runtime
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self) -> None:
        """@brief 注册契约 5.5 的 12 条路由 / Register the twelve section-5.5 routes."""
        routes: tuple[tuple[str, str, Callable[..., Awaitable[Response]], str | None, str], ...] = (
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/interview-scenarios",
                self.list_scenarios,
                None,
                "InterviewScenarioList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/interview-scenarios",
                self.create_scenario,
                "CreateInterviewScenarioRequest",
                "InterviewScenario",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/interview-scenarios/{scenario_id}",
                self.get_scenario,
                None,
                "InterviewScenario",
            ),
            (
                "PATCH",
                "/api/v2/workspaces/{workspace_id}/interview-scenarios/{scenario_id}",
                self.patch_scenario,
                "UpdateInterviewScenarioRequest",
                "InterviewScenario",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/interview-sessions",
                self.list_sessions,
                None,
                "InterviewSessionList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/interview-sessions",
                self.create_session,
                "CreateInterviewSessionRequest",
                "InterviewSession",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}",
                self.get_session,
                None,
                "InterviewSession",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/connections",
                self.create_connection,
                "CreateRealtimeConnectionRequest",
                "RealtimeConnection",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/end-requests",
                self.create_end_request,
                "EndInterviewSessionRequest",
                "Job",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/transcript",
                self.get_transcript,
                None,
                "InterviewTranscriptPage",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/report-jobs",
                self.create_report_job,
                "CreateInterviewReportJobRequest",
                "Job",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/interview-reports/{report_id}",
                self.get_report,
                None,
                "InterviewReport",
            ),
        )
        for method, path, endpoint, request_definition, response_definition in routes:
            extra: dict[str, JsonValue] = {
                "x-api-v2-phase": 2,
                "x-contract-response": response_definition,
            }
            if request_definition is not None:
                extra["x-contract-request"] = request_definition
            self.router.add_api_route(
                path,
                endpoint,
                methods=[method],
                openapi_extra=extra,
                status_code=_declared_status(method, path),
                response_class=Response,
            )

    @_translate_http_errors
    async def list_scenarios(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Scenario / Page through Scenarios."""
        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        page = await runtime.interview_v2.list_scenarios(
            principal,
            workspace,
            _page_request(
                cursor,
                limit,
                runtime.v2_cursor,
                principal,
                workspace,
                "interview_scenarios",
                _SCENARIO_SORT,
            ),
        )
        payload = _collection_payload(
            page,
            _scenario,
            runtime.v2_cursor,
            principal,
            workspace,
            "interview_scenarios",
            _SCENARIO_SORT,
        )
        runtime.contracts_v2.validate_definition("InterviewScenarioList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def create_scenario(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief 幂等创建 Scenario / Idempotently create a Scenario."""
        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _interview_json(
            request, runtime.contracts_v2, "CreateInterviewScenarioRequest"
        )

        async def operation() -> ReplayableResponse:
            scenario = await runtime.interview_v2.create_scenario(
                principal,
                workspace,
                CreateInterviewScenarioCommand(_scenario_spec(body)),
                InterviewMutationContext(request_id(request)),
            )
            payload = _scenario(scenario)
            runtime.contracts_v2.validate_definition("InterviewScenario", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=f"/api/v2/workspaces/{workspace_id}/interview-scenarios/{scenario.meta.id}",
                etag=True,
            )

        return await _generic_idempotent(
            request,
            runtime,
            principal,
            workspace,
            f"/api/v2/workspaces/{workspace_id}/interview-scenarios",
            body,
            None,
            operation,
        )

    @_translate_http_errors
    async def get_scenario(
        self, request: Request, workspace_id: OpaquePath, scenario_id: OpaquePath
    ) -> Response:
        """@brief 读取 Scenario / Read a Scenario."""
        require_query(request)
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        scenario = await runtime.interview_v2.get_scenario(
            principal, workspace, InterviewScenarioId(scenario_id)
        )
        payload = _scenario(scenario)
        runtime.contracts_v2.validate_definition("InterviewScenario", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def patch_scenario(
        self, request: Request, workspace_id: OpaquePath, scenario_id: OpaquePath
    ) -> Response:
        """@brief 以强 If-Match 修改 Scenario / Update a Scenario with strong If-Match."""
        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _interview_json(
            request, runtime.contracts_v2, "UpdateInterviewScenarioRequest"
        )
        typed_id = InterviewScenarioId(scenario_id)
        current = await runtime.interview_v2.get_scenario_for_update(principal, workspace, typed_id)
        current_payload = _scenario(current)
        runtime.contracts_v2.validate_definition("InterviewScenario", current_payload)
        expected = match_etag_revision(
            if_match_header(request), current_payload, current.meta.revision
        )
        updated = await runtime.interview_v2.update_scenario(
            principal,
            workspace,
            typed_id,
            _scenario_patch(body),
            expected_revision=expected,
            context=InterviewMutationContext(request_id(request)),
        )
        payload = _scenario(updated)
        runtime.contracts_v2.validate_definition("InterviewScenario", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def list_sessions(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Session / Page through Sessions."""
        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        page = await runtime.interview_v2.list_sessions(
            principal,
            workspace,
            _page_request(
                cursor,
                limit,
                runtime.v2_cursor,
                principal,
                workspace,
                "interview_sessions",
                _SESSION_SORT,
            ),
        )
        payload = _collection_payload(
            page,
            _session,
            runtime.v2_cursor,
            principal,
            workspace,
            "interview_sessions",
            _SESSION_SORT,
        )
        runtime.contracts_v2.validate_definition("InterviewSessionList", payload)
        return json_response(request, payload, cache_control=_PRIVATE_CACHE_CONTROL)

    @_translate_http_errors
    async def create_session(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief 使用加密 receipt 幂等创建 Session / Idempotently create a Session with an encrypted receipt."""
        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _interview_json(request, runtime.contracts_v2, "CreateInterviewSessionRequest")

        async def operation() -> ReplayableResponse:
            session = await runtime.interview_v2.create_session(
                principal,
                workspace,
                _session_command(body),
                InterviewMutationContext(request_id(request)),
            )
            payload = _session(session)
            runtime.contracts_v2.validate_definition("InterviewSession", payload)
            return _private_replayable(
                payload,
                201,
                f"/api/v2/workspaces/{workspace_id}/interview-sessions/{session.meta.id}",
            )

        return await _sensitive_idempotent_response(
            request,
            runtime=runtime,
            principal=principal,
            workspace_id=workspace,
            canonical_path=f"/api/v2/workspaces/{workspace_id}/interview-sessions",
            canonical_body=canonical_json_bytes(body),
            if_match=None,
            operation=operation,
        )

    @_translate_http_errors
    async def get_session(
        self, request: Request, workspace_id: OpaquePath, session_id: OpaquePath
    ) -> Response:
        """@brief 读取 Session / Read a Session."""
        require_query(request)
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        session = await runtime.interview_v2.get_session(
            principal, workspace, InterviewSessionId(session_id)
        )
        payload = _session(session)
        runtime.contracts_v2.validate_definition("InterviewSession", payload)
        response = resource_response(request, payload)
        response.headers["Cache-Control"] = _PRIVATE_CACHE_CONTROL
        return response

    @_translate_http_errors
    async def create_connection(
        self, request: Request, workspace_id: OpaquePath, session_id: OpaquePath
    ) -> Response:
        """@brief 使用加密 receipt 幂等签发 RealtimeConnection / Idempotently issue a RealtimeConnection with an encrypted receipt."""
        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _interview_json(
            request, runtime.contracts_v2, "CreateRealtimeConnectionRequest"
        )

        async def operation() -> ReplayableResponse:
            connection = await runtime.interview_v2.create_realtime_connection(
                principal,
                workspace,
                InterviewSessionId(session_id),
                _connection_spec(body),
                InterviewMutationContext(request_id(request)),
            )
            payload = _connection(connection)
            runtime.contracts_v2.validate_definition("RealtimeConnection", payload)
            return _private_replayable(
                payload,
                201,
                f"/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/connections/{connection.id}",
            )

        return await _sensitive_idempotent_response(
            request,
            runtime=runtime,
            principal=principal,
            workspace_id=workspace,
            canonical_path=f"/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/connections",
            canonical_body=canonical_json_bytes(body),
            if_match=None,
            operation=operation,
        )

    @_translate_http_errors
    async def create_end_request(
        self, request: Request, workspace_id: OpaquePath, session_id: OpaquePath
    ) -> Response:
        """@brief 以 If-Match 幂等创建 end Job / Idempotently create an end Job with If-Match."""
        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _interview_json(request, runtime.contracts_v2, "EndInterviewSessionRequest")
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            current = await runtime.interview_v2.get_session_for_end(
                principal, workspace, InterviewSessionId(session_id)
            )
            current_payload = _session(current)
            runtime.contracts_v2.validate_definition("InterviewSession", current_payload)
            expected = match_etag_revision(raw_if_match, current_payload, current.meta.revision)
            job = await runtime.interview_v2.create_end_request(
                principal,
                workspace,
                InterviewSessionId(session_id),
                EndInterviewSessionCommand(EndInterviewReason(_required_string(body, "reason"))),
                expected_revision=expected,
                context=InterviewMutationContext(request_id(request)),
            )
            return _job_replay(runtime, job, workspace_id)

        return await _generic_idempotent(
            request,
            runtime,
            principal,
            workspace,
            f"/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/end-requests",
            body,
            raw_if_match,
            operation,
        )

    @_translate_http_errors
    async def get_transcript(
        self,
        request: Request,
        workspace_id: OpaquePath,
        session_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页读取 Transcript / Page through a Transcript."""
        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        collection = f"interview_transcript:{session_id}"
        page = await runtime.interview_v2.get_transcript(
            principal,
            workspace,
            InterviewSessionId(session_id),
            _page_request(
                cursor, limit, runtime.v2_cursor, principal, workspace, collection, _TRANSCRIPT_SORT
            ),
        )
        payload = _collection_payload(
            page,
            _transcript_segment,
            runtime.v2_cursor,
            principal,
            workspace,
            collection,
            _TRANSCRIPT_SORT,
        )
        runtime.contracts_v2.validate_definition("InterviewTranscriptPage", payload)
        return json_response(request, payload, cache_control=_PRIVATE_CACHE_CONTROL)

    @_translate_http_errors
    async def create_report_job(
        self, request: Request, workspace_id: OpaquePath, session_id: OpaquePath
    ) -> Response:
        """@brief 幂等创建 Report Job / Idempotently create a Report Job."""
        require_query(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        body = await _interview_json(
            request, runtime.contracts_v2, "CreateInterviewReportJobRequest"
        )

        async def operation() -> ReplayableResponse:
            job = await runtime.interview_v2.create_report_job(
                principal,
                workspace,
                InterviewSessionId(session_id),
                CreateInterviewReportJobCommand(_optional_string(body, "rubric_version")),
                InterviewMutationContext(request_id(request)),
            )
            return _job_replay(runtime, job, workspace_id)

        return await _generic_idempotent(
            request,
            runtime,
            principal,
            workspace,
            f"/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/report-jobs",
            body,
            None,
            operation,
        )

    @_translate_http_errors
    async def get_report(
        self, request: Request, workspace_id: OpaquePath, report_id: OpaquePath
    ) -> Response:
        """@brief 读取 InterviewReport / Read an InterviewReport."""
        require_query(request)
        await require_no_body(request)
        runtime, principal, workspace = self._request_context(request, workspace_id)
        report = await runtime.interview_v2.get_report(
            principal, workspace, cast(InterviewReportId, report_id)
        )
        payload = _report(report)
        runtime.contracts_v2.validate_definition("InterviewReport", payload)
        response = resource_response(request, payload)
        response.headers["Cache-Control"] = _PRIVATE_CACHE_CONTROL
        return response

    def _request_context(
        self, request: Request, workspace_id: str
    ) -> tuple[V2InterviewRuntime, TokenPrincipal, WorkspaceId]:
        """@brief 解析 runtime、principal 与 Workspace / Resolve runtime, principal, and Workspace."""
        return (
            self._resolve_runtime(request),
            verified_principal(request),
            WorkspaceId(workspace_id),
        )


def create_v2_interview_router(
    resolve_runtime: V2InterviewRuntimeResolver = v2_interview_runtime_from_request,
) -> APIRouter:
    """@brief 创建完整 Interview V2 router / Create the complete Interview V2 router."""
    return V2InterviewHttpAdapter(resolve_runtime).router


def _declared_status(method: str, path: str) -> int:
    """@brief 返回 OpenAPI 声明成功状态 / Return the declared success status."""
    if method == "POST" and path.endswith(("end-requests", "report-jobs")):
        return 202
    if method == "POST":
        return 201
    return 200


async def _interview_json(
    request: Request, validator: ContractDefinitionValidator, definition: str
) -> dict[str, JsonValue]:
    """@brief 严格读取 Interview JSON object / Strictly read an Interview JSON object."""
    return await strict_json_object(
        request,
        validator=validator,
        definition=definition,
        max_body_bytes=_MAX_INTERVIEW_BODY_BYTES,
        max_depth=_MAX_INTERVIEW_JSON_DEPTH,
    )


async def _generic_idempotent(
    request: Request,
    runtime: V2InterviewRuntime,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    canonical_path: str,
    body: Mapping[str, JsonValue],
    if_match: str | None,
    operation: Callable[[], Awaitable[ReplayableResponse]],
) -> Response:
    """@brief 执行普通 Interview 幂等命令 / Execute a regular Interview idempotent command."""
    return await idempotent_response(
        request,
        executor=runtime.v2_idempotency,
        principal=principal,
        workspace_id=workspace_id,
        canonical_path=canonical_path,
        canonical_body=canonical_json_bytes(dict(body)),
        content_type=JSON_MEDIA_TYPE,
        if_match=if_match,
        operation=operation,
        mapped_error_types=_INTERVIEW_BOUNDARY_ERRORS,
        map_error=_interview_problem,
    )


async def _sensitive_idempotent_response(
    request: Request,
    *,
    runtime: V2InterviewRuntime,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    canonical_path: str,
    canonical_body: bytes,
    if_match: str | None,
    operation: Callable[[], Awaitable[ReplayableResponse]],
) -> Response:
    """@brief 以 HMAC request fingerprint 与 AES-GCM receipt 执行敏感幂等命令 / Execute sensitive idempotency with an HMAC fingerprint and AES-GCM receipt."""
    keys = request.headers.getlist("Idempotency-Key")
    if len(keys) > 1:
        raise DomainError(
            Problem("http.invalid_idempotency_key", 400, "Idempotency-Key is invalid")
        )
    key = validate_idempotency_key(keys[0] if keys else None)
    secret = runtime.sensitive_idempotency_key
    encryption_key = _receipt_key(secret)
    aad = canonical_json_bytes(
        {
            "user_id": str(principal.user_id),
            "workspace_id": str(workspace_id),
            "method": request.method.upper(),
            "path": canonical_path,
            "key": key,
        }
    )
    request_digest = _sensitive_request_digest(canonical_body, secret)
    idempotency_request = IdempotencyRequest(
        IdempotencyScope(
            principal.user_id, workspace_id, request.method.upper(), canonical_path, key
        ),
        request_digest,
        JSON_MEDIA_TYPE,
        if_match,
    )

    async def captured() -> ReplayableResponse:
        try:
            actual = await operation()
        except DomainError as error:
            actual = _problem_replay(request, error.problem)
        except _INTERVIEW_BOUNDARY_ERRORS as error:
            actual = _problem_replay(request, _interview_problem(error))
        return _seal_replay(actual, encryption_key, aad)

    sealed = await runtime.v2_idempotency.execute(idempotency_request, captured)
    actual = _open_replay(sealed, encryption_key, aad)
    headers = dict(actual.headers)
    headers["X-Request-Id"] = request_id(request)
    headers["Cache-Control"] = _PRIVATE_CACHE_CONTROL
    headers["Pragma"] = "no-cache"
    if actual.status_code == 401:
        headers["WWW-Authenticate"] = (
            f'Bearer resource_metadata="{PROTECTED_RESOURCE_METADATA_URL}"'
        )
    return Response(content=actual.json_body, status_code=actual.status_code, headers=headers)


def _private_replayable(payload: JsonValue, status_code: int, location: str) -> ReplayableResponse:
    """@brief 构造 no-store 且可加密的资源响应 / Build a no-store resource response suitable for sealing."""
    replay = replayable_json(
        payload,
        status_code=status_code,
        location=location,
        etag=True,
    )
    return ReplayableResponse(
        replay.status_code,
        (*replay.headers, ("Cache-Control", _PRIVATE_CACHE_CONTROL)),
        replay.json_body,
    )


def _receipt_key(secret: bytes) -> bytes:
    """@brief 派生 256-bit receipt 密钥 / Derive a 256-bit receipt key."""
    if not isinstance(secret, bytes) or len(secret) < 32:
        raise RuntimeError("sensitive idempotency key must contain at least 32 bytes")
    return hashlib.sha256(_SENSITIVE_RECEIPT_LABEL + secret).digest()


def _sensitive_request_digest(body: bytes, secret: bytes) -> bytes:
    """@brief 以 keyed digest 隐藏低熵 consent 请求 / Hide low-entropy consent input with a keyed digest."""
    key = _receipt_key(secret)
    return b"hmac-sha256:" + hmac.digest(key, body, "sha256").hex().encode("ascii")


def _seal_replay(actual: ReplayableResponse, key: bytes, aad: bytes) -> ReplayableResponse:
    """@brief AES-GCM 封装完整 replay / Seal a complete replay with AES-GCM."""
    plaintext_payload: dict[str, JsonValue] = {
        "status": actual.status_code,
        "headers": [[name, value] for name, value in actual.headers],
        "body": _b64(actual.json_body),
    }
    plaintext = canonical_json_bytes(plaintext_payload)
    nonce = secrets.token_bytes(12)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, aad)
    envelope: dict[str, JsonValue] = {
        "version": 1,
        "nonce": _b64(nonce),
        "ciphertext": _b64(ciphertext),
    }
    return ReplayableResponse(
        200, (("Content-Type", JSON_MEDIA_TYPE),), canonical_json_bytes(envelope)
    )


def _open_replay(sealed: ReplayableResponse, key: bytes, aad: bytes) -> ReplayableResponse:
    """@brief 验证并打开敏感 replay / Authenticate and open a sensitive replay."""
    try:
        envelope = json.loads(sealed.json_body)
        if (
            not isinstance(envelope, dict)
            or set(envelope) != {"version", "nonce", "ciphertext"}
            or envelope["version"] != 1
        ):
            raise ValueError("invalid sensitive receipt envelope")
        nonce = _unb64(envelope["nonce"])
        ciphertext = _unb64(envelope["ciphertext"])
        plaintext = AESGCM(key).decrypt(nonce, ciphertext, aad)
        value = json.loads(plaintext)
        if not isinstance(value, dict) or set(value) != {"status", "headers", "body"}:
            raise ValueError("invalid sensitive receipt payload")
        status = value["status"]
        headers = value["headers"]
        body = _unb64(value["body"])
        if isinstance(status, bool) or not isinstance(status, int) or not isinstance(headers, list):
            raise ValueError("invalid sensitive receipt fields")
        pairs: list[tuple[str, str]] = []
        for pair in headers:
            if (
                not isinstance(pair, list)
                or len(pair) != 2
                or not all(isinstance(item, str) for item in pair)
            ):
                raise ValueError("invalid sensitive receipt headers")
            pairs.append((pair[0], pair[1]))
        return ReplayableResponse(status, tuple(pairs), body)
    except (InvalidTag, UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as error:
        raise RuntimeError("sensitive idempotency receipt authentication failed") from error


def _b64(value: bytes) -> str:
    """@brief 编码无 padding base64url / Encode unpadded base64url."""
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: object) -> bytes:
    """@brief 严格解码 base64url / Strictly decode base64url."""
    if not isinstance(value, str) or not value:
        raise ValueError("invalid base64url value")
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)


def _problem_replay(request: Request, problem: Problem) -> ReplayableResponse:
    """@brief 构造可封装 ProblemDetails / Build sealable Problem Details."""
    return ReplayableResponse(
        problem.status,
        (("Content-Type", "application/problem+json"),),
        canonical_json_bytes(problem_payload(request, problem)),
    )


def _page_request(
    cursor: str | None,
    limit: int,
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    collection: str,
    sort: Sequence[str],
) -> InterviewPageRequest:
    """@brief 解码签名 cursor 为 repository continuation / Decode a signed cursor into a repository continuation."""
    if cursor is None:
        return InterviewPageRequest(limit)
    position = codec.decode(
        cursor,
        principal=principal,
        workspace_id=workspace_id,
        filters={"collection": collection},
        sort=sort,
    )
    if not isinstance(position, str) or not position:
        raise DomainError(Problem("http.cursor_invalid", 400, "Pagination cursor is invalid"))
    return InterviewPageRequest(limit, position)


def _collection_payload[ItemT](
    page: InterviewPage[ItemT],
    project: Callable[[ItemT], JsonValue],
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    collection: str,
    sort: Sequence[str],
) -> dict[str, JsonValue]:
    """@brief 投影 application-paginated 集合 / Project an application-paginated collection."""
    next_cursor = None
    if page.next_position is not None:
        next_cursor = codec.encode(
            page.next_position,
            principal=principal,
            workspace_id=workspace_id,
            filters={"collection": collection},
            sort=sort,
        )
    return list_response([project(item) for item in page.items], next_cursor=next_cursor)


def _job_replay(runtime: V2InterviewRuntime, job: Job, workspace_id: str) -> ReplayableResponse:
    """@brief 校验并构造 202 Job / Validate and build a 202 Job response."""
    payload = _job(job)
    runtime.contracts_v2.validate_definition("Job", payload)
    return replayable_json(
        payload,
        status_code=202,
        location=f"/api/v2/workspaces/{workspace_id}/jobs/{job.meta.id}",
        etag=True,
    )


def _interview_problem(error: BaseException) -> Problem:
    """@brief 将 Interview 预期失败稳定映射到 HTTP / Map expected Interview failures to HTTP."""
    if isinstance(error, InterviewResourceNotFound):
        return Problem(error.code, 404, "Resource was not found", detail=error.detail)
    if isinstance(error, InterviewPreconditionFailed):
        return Problem(error.code, 412, "Resource precondition failed", detail=error.detail)
    if isinstance(error, InvalidInterviewCommand):
        return Problem(error.code, 422, "Command violates a domain constraint", detail=error.detail)
    if isinstance(error, InterviewPortProtocolError):
        return Problem(
            "interview.internal_protocol_error",
            500,
            "Interview service protocol failed",
            retryable=True,
        )
    if isinstance(error, InterviewConflict) or isinstance(error, InterviewTransitionError):
        detail = error.detail if isinstance(error, InterviewApplicationError) else str(error)
        code = (
            error.code
            if isinstance(error, InterviewApplicationError)
            else "interview.state_conflict"
        )
        return Problem(code, 409, "Interview state conflict", detail=detail)
    if isinstance(error, InterviewPolicyDenied) or isinstance(error, AuthorizationDenied):
        return Problem("interview.access_denied", 403, "Interview access was denied")
    if isinstance(error, UnknownPrincipal):
        return Problem("oauth.invalid_token", 401, "Bearer token is invalid")
    if isinstance(error, InterviewDomainError):
        return Problem(
            "interview.invalid_command", 422, "Interview command is invalid", detail=str(error)
        )
    if isinstance(error, DomainInvariantError):
        return Problem("interview.state_conflict", 409, "Interview state conflict")
    if isinstance(error, InterviewApplicationError):
        return Problem(error.code, 409, "Interview operation failed", detail=error.detail)
    raise TypeError(f"unsupported Interview error: {type(error).__name__}")


def _scenario_spec(body: Mapping[str, JsonValue]) -> InterviewScenarioSpec:
    """@brief 解析 Scenario input / Parse Scenario input."""
    return InterviewScenarioSpec(
        _required_string(body, "name"),
        _required_string(body, "description"),
        _required_string(body, "locale"),
        _required_string(body, "interview_type"),
        InterviewDifficulty(_required_string(body, "difficulty")),
        _required_integer(body, "duration_minutes"),
        _required_integer(body, "target_question_count"),
        _string_tuple(body, "focus_areas"),
        _required_boolean(body, "allow_followups"),
        _required_boolean(body, "allow_barge_in"),
        _rubric(_required_object(body, "rubric")),
    )


def _scenario_patch(body: Mapping[str, JsonValue]) -> InterviewScenarioPatch:
    """@brief 解析显式 supplied-field Scenario patch / Parse an explicit supplied-field Scenario patch."""
    values: dict[str, object] = {}
    for name in ("name", "description", "locale", "interview_type"):
        if name in body:
            values[name] = _required_string(body, name)
    for name in ("duration_minutes", "target_question_count"):
        if name in body:
            values[name] = _required_integer(body, name)
    for name in ("allow_followups", "allow_barge_in"):
        if name in body:
            values[name] = _required_boolean(body, name)
    if "focus_areas" in body:
        values["focus_areas"] = _string_tuple(body, "focus_areas")
    if "difficulty" in body:
        values["difficulty"] = InterviewDifficulty(_required_string(body, "difficulty"))
    if "rubric" in body:
        values["rubric"] = _rubric(_required_object(body, "rubric"))
    if "status" in body:
        from backend.domain.interview_v2 import InterviewScenarioStatus

        values["status"] = InterviewScenarioStatus(_required_string(body, "status"))
    return InterviewScenarioPatch(values)


def _session_command(body: Mapping[str, JsonValue]) -> CreateInterviewSessionCommand:
    """@brief 解析 Session create command / Parse a Session-create command."""
    return CreateInterviewSessionCommand(
        InterviewScenarioId(_required_string(body, "scenario_id")),
        _nullable_resource_ref(body, "resume_ref"),
        _job_target(_required_object(body, "job_target")),
        _knowledge_selection(_required_object(body, "knowledge")),
        _required_string(body, "locale"),
        _media(_required_object(body, "media")),
        _recording(_required_object(body, "recording")),
        _inference(_required_object(body, "inference")),
    )


def _connection_spec(body: Mapping[str, JsonValue]) -> CreateRealtimeConnectionSpec:
    """@brief 解析 RealtimeConnection spec / Parse a RealtimeConnection spec."""
    return CreateRealtimeConnectionSpec(
        tuple(RealtimeTransport(value) for value in _string_tuple(body, "supported_transports")),
        _string_tuple(body, "audio_codecs"),
        _string_tuple(body, "video_codecs"),
    )


def _rubric(body: Mapping[str, JsonValue]) -> InterviewRubric:
    """@brief 解析 InterviewRubric / Parse an InterviewRubric."""
    return InterviewRubric(
        _required_string(body, "rubric_id"),
        _required_string(body, "rubric_version"),
        _required_string(body, "name"),
        tuple(
            _rubric_dimension(_array_object(value, "dimensions"))
            for value in _required_array(body, "dimensions")
        ),
        _score_scale(_required_object(body, "overall_scale")),
    )


def _rubric_dimension(body: Mapping[str, JsonValue]) -> RubricDimension:
    """@brief 解析 RubricDimension / Parse a RubricDimension."""
    return RubricDimension(
        _required_string(body, "dimension_id"),
        _required_string(body, "name"),
        _required_string(body, "description"),
        _required_number(body, "weight"),
        _string_tuple(body, "observable_indicators"),
        _score_scale(_required_object(body, "scoring_scale")),
    )


def _score_scale(body: Mapping[str, JsonValue]) -> ScoreScale:
    """@brief 解析 ScoreScale / Parse a ScoreScale."""
    labels_value = body.get("labels", {})
    if not isinstance(labels_value, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in labels_value.items()
    ):
        raise RuntimeError("validated score labels must be a string map")
    return ScoreScale(
        _required_number(body, "minimum"),
        _required_number(body, "maximum"),
        cast(dict[str, str], labels_value),
    )


def _job_target(body: Mapping[str, JsonValue]) -> JobTarget:
    """@brief 解析 JobTarget / Parse a JobTarget."""
    return JobTarget(
        _required_string(body, "title"),
        _nullable_string(body, "company"),
        _nullable_string(body, "location"),
        _nullable_string(body, "description"),
        _nullable_string(body, "source_url"),
        _nullable_string(body, "seniority"),
        _string_tuple(body, "skills"),
    )


def _knowledge_selection(body: Mapping[str, JsonValue]) -> KnowledgeSelection:
    """@brief 解析 KnowledgeSelection / Parse a KnowledgeSelection."""
    return KnowledgeSelection(
        KnowledgeSelectionMode(_required_string(body, "mode")),
        tuple(KnowledgeSourceId(value) for value in _string_tuple(body, "include_source_ids")),
        tuple(KnowledgeSourceId(value) for value in _string_tuple(body, "exclude_source_ids")),
        tuple(
            _knowledge_pin(_array_object(value, "pinned_versions"))
            for value in _required_array(body, "pinned_versions")
        ),
        _required_string(body, "agent_scope"),
    )


def _knowledge_pin(body: Mapping[str, JsonValue]) -> KnowledgeVersionPin:
    """@brief 解析 KnowledgeVersionPin / Parse a KnowledgeVersionPin."""
    return KnowledgeVersionPin(
        KnowledgeSourceId(_required_string(body, "source_id")),
        KnowledgeSourceVersionId(_required_string(body, "version_id")),
    )


def _inference(body: Mapping[str, JsonValue]) -> InferenceIntent:
    """@brief 解析 InferenceIntent / Parse an InferenceIntent."""
    return InferenceIntent(
        InferenceQualityTier(_required_string(body, "quality_tier")),
        _nullable_integer(body, "latency_budget_ms"),
        InferenceCostTier(_required_string(body, "cost_tier")),
        ModelRegion(_required_string(body, "data_region")),
        _required_boolean(body, "allow_provider_fallback"),
        _required_boolean(body, "allow_external_model_processing"),
    )


def _media(body: Mapping[str, JsonValue]) -> InterviewMediaPreferences:
    """@brief 解析 InterviewMediaPreferences / Parse InterviewMediaPreferences."""
    avatar = _required_object(body, "avatar")
    return InterviewMediaPreferences(
        _required_boolean(body, "user_audio"),
        _required_boolean(body, "user_video"),
        _required_boolean(body, "screen_share"),
        _required_integer(body, "max_video_width"),
        _required_integer(body, "max_video_height"),
        _required_integer(body, "max_video_fps"),
        InterviewAvatarPreferences(
            AvatarOutputMode(_required_string(avatar, "output_mode")),
            _nullable_string(avatar, "avatar_id"),
            _nullable_string(avatar, "voice_id"),
            _string_tuple(avatar, "preferred_audio_codecs"),
            _string_tuple(avatar, "preferred_video_codecs"),
            _required_boolean(avatar, "include_visemes"),
            _required_boolean(avatar, "include_expression_cues"),
        ),
        FallbackTransport(_required_string(body, "fallback_transport")),
    )


def _recording(body: Mapping[str, JsonValue]) -> RecordingConsent:
    """@brief 解析 RecordingConsent / Parse RecordingConsent."""
    consented = _nullable_string(body, "consented_at")
    return RecordingConsent(
        _required_boolean(body, "record_audio"),
        _required_boolean(body, "record_video"),
        _required_boolean(body, "store_transcript"),
        _required_integer(body, "retention_days"),
        _parse_timestamp(consented) if consented is not None else None,
        _nullable_string(body, "consent_version"),
    )


def _nullable_resource_ref(body: Mapping[str, JsonValue], field: str) -> ResourceRef | None:
    """@brief 解析 nullable ResourceRef / Parse a nullable ResourceRef."""
    value = _required_value(body, field)
    if value is None:
        return None
    item = _object(value, field)
    return ResourceRef(
        _required_string(item, "resource_type"),
        _required_string(item, "id"),
        _nullable_integer(item, "revision"),
    )


def _scenario(value: InterviewScenario) -> dict[str, JsonValue]:
    """@brief 投影 InterviewScenario / Project an InterviewScenario."""
    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            **_scenario_spec_payload(value.spec),
            "status": value.status.value,
        }
    )
    return payload


def _scenario_spec_payload(value: InterviewScenarioSpec) -> dict[str, JsonValue]:
    """@brief 投影 Scenario input 字段 / Project Scenario-input fields."""
    return {
        "name": value.name,
        "description": value.description,
        "locale": value.locale,
        "interview_type": value.interview_type,
        "difficulty": value.difficulty.value,
        "duration_minutes": value.duration_minutes,
        "target_question_count": value.target_question_count,
        "focus_areas": list(value.focus_areas),
        "allow_followups": value.allow_followups,
        "allow_barge_in": value.allow_barge_in,
        "rubric": _rubric_payload(value.rubric),
    }


def _session(value: InterviewSessionView) -> dict[str, JsonValue]:
    """@brief 投影 InterviewSession 公开 SIR / Project the public InterviewSession SIR."""
    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "scenario_id": str(value.scenario_id),
            "resume_ref": _resource_ref(value.resume_ref) if value.resume_ref is not None else None,
            "job_target": _job_target_payload(value.job_target),
            "status": value.status.value,
            "locale": value.locale,
            "media": _media_payload(value.media),
            "recording": _recording_payload(value.recording),
            "started_at": timestamp(value.started_at) if value.started_at is not None else None,
            "ended_at": timestamp(value.ended_at) if value.ended_at is not None else None,
            "report_id": str(value.report_id) if value.report_id is not None else None,
        }
    )
    return payload


def _connection(value: RealtimeConnection) -> dict[str, JsonValue]:
    """@brief 精确投影一次性 RealtimeConnection / Precisely project a one-time RealtimeConnection."""
    return {
        "id": str(value.id),
        "session_id": str(value.session_id),
        "transport": value.transport.value,
        "signaling_url": value.signaling_url,
        "ephemeral_token": value.ephemeral_token.reveal_to_transport(),
        "ice_servers": [_ice_server(item) for item in value.ice_servers],
        "expires_at": timestamp(value.expires_at),
        "heartbeat_interval_ms": value.heartbeat_interval_ms,
    }


def _ice_server(value: IceServer) -> dict[str, JsonValue]:
    """@brief 只在 no-store response 投影 ICE credential / Project ICE credentials only in a no-store response."""
    return {"urls": list(value.urls), "username": value.username, "credential": value.credential}


def _transcript_segment(value: TranscriptSegment) -> dict[str, JsonValue]:
    """@brief 投影 Transcript segment，不暴露内部 sequence / Project a Transcript segment without internal sequence."""
    return {
        "id": str(value.id),
        "speaker": value.speaker.value,
        "start_ms": value.start_ms,
        "end_ms": value.end_ms,
        "text": value.text,
    }


def _report(value: InterviewReport) -> dict[str, JsonValue]:
    """@brief 穷尽投影 InterviewReport 与证据 / Exhaustively project an InterviewReport and evidence."""
    draft = value.draft
    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "session_id": str(value.session_id),
            "report_version": draft.report_version,
            "rubric_ref": {"id": draft.rubric_id, "version": draft.rubric_version},
            "engine_version": draft.engine_version,
            "overall_score": draft.overall_score,
            "overall_confidence": draft.overall_confidence,
            "executive_summary": _rich_text(draft.executive_summary),
            "rubric_scores": [_rubric_score(item) for item in draft.rubric_scores],
            "strengths": [_rich_text(item) for item in draft.strengths],
            "improvements": [_rich_text(item) for item in draft.improvements],
            "communication_metrics": _metrics(draft.communication_metrics),
            "action_plan": [_action(item) for item in draft.action_plan],
            "limitations": list(draft.limitations),
            "generated_at": timestamp(value.generated_at),
        }
    )
    return payload


def _rubric_payload(value: InterviewRubric) -> dict[str, JsonValue]:
    """@brief 投影 InterviewRubric / Project an InterviewRubric."""
    return {
        "rubric_id": value.rubric_id,
        "rubric_version": value.rubric_version,
        "name": value.name,
        "dimensions": [
            {
                "dimension_id": item.dimension_id,
                "name": item.name,
                "description": item.description,
                "weight": item.weight,
                "observable_indicators": list(item.observable_indicators),
                "scoring_scale": _score_scale_payload(item.scoring_scale),
            }
            for item in value.dimensions
        ],
        "overall_scale": _score_scale_payload(value.overall_scale),
    }


def _score_scale_payload(value: ScoreScale) -> dict[str, JsonValue]:
    """@brief 投影 ScoreScale / Project a ScoreScale."""
    return {"minimum": value.minimum, "maximum": value.maximum, "labels": dict(value.labels)}


def _job_target_payload(value: JobTarget) -> dict[str, JsonValue]:
    """@brief 投影 JobTarget / Project a JobTarget."""
    return {
        "title": value.title,
        "company": value.company,
        "location": value.location,
        "description": value.description,
        "source_url": value.source_url,
        "seniority": value.seniority,
        "skills": list(value.skills),
    }


def _media_payload(value: InterviewMediaPreferences) -> dict[str, JsonValue]:
    """@brief 投影 media preferences / Project media preferences."""
    avatar = value.avatar
    return {
        "user_audio": value.user_audio,
        "user_video": value.user_video,
        "screen_share": value.screen_share,
        "max_video_width": value.max_video_width,
        "max_video_height": value.max_video_height,
        "max_video_fps": value.max_video_fps,
        "avatar": {
            "output_mode": avatar.output_mode.value,
            "avatar_id": avatar.avatar_id,
            "voice_id": avatar.voice_id,
            "preferred_audio_codecs": list(avatar.preferred_audio_codecs),
            "preferred_video_codecs": list(avatar.preferred_video_codecs),
            "include_visemes": avatar.include_visemes,
            "include_expression_cues": avatar.include_expression_cues,
        },
        "fallback_transport": value.fallback_transport.value,
    }


def _recording_payload(value: RecordingConsent) -> dict[str, JsonValue]:
    """@brief 投影精确 consent/retention，不添加派生隐私字段 / Project exact consent/retention without derived privacy fields."""
    return {
        "record_audio": value.record_audio,
        "record_video": value.record_video,
        "store_transcript": value.store_transcript,
        "retention_days": value.retention_days,
        "consented_at": timestamp(value.consented_at) if value.consented_at is not None else None,
        "consent_version": value.consent_version,
    }


def _rubric_score(value: RubricScore) -> dict[str, JsonValue]:
    """@brief 投影 RubricScore 与精确 evidence 时间 / Project a RubricScore with exact evidence timing."""
    return {
        "dimension_id": value.dimension_id,
        "score": value.score,
        "confidence": value.confidence,
        "summary": _rich_text(value.summary),
        "evidence": [_evidence(item) for item in value.evidence],
        "improvement_actions": list(value.improvement_actions),
    }


def _evidence(value: InterviewEvidence) -> dict[str, JsonValue]:
    """@brief 投影 InterviewEvidence / Project InterviewEvidence."""
    return {
        "segment_id": str(value.segment_id),
        "start_ms": value.start_ms,
        "end_ms": value.end_ms,
        "quote": value.quote,
    }


def _rich_text(value: InterviewRichText) -> dict[str, JsonValue]:
    """@brief 投影 InterviewRichText / Project InterviewRichText."""
    return {"plain_text": value.plain_text}


def _metrics(value: InterviewCommunicationMetrics) -> dict[str, JsonValue]:
    """@brief 投影 communication metrics / Project communication metrics."""
    return {
        "speaking_time_ms": value.speaking_time_ms,
        "average_answer_length_ms": value.average_answer_length_ms,
        "words_per_minute": value.words_per_minute,
        "filler_word_count": value.filler_word_count,
        "long_pause_count": value.long_pause_count,
        "interruption_count": value.interruption_count,
        "notes": list(value.notes),
    }


def _action(value: InterviewActionPlanItem) -> dict[str, JsonValue]:
    """@brief 投影 action-plan item / Project an action-plan item."""
    return {
        "priority": value.priority.value,
        "title": value.title,
        "why": value.why,
        "practice": value.practice,
        "success_criterion": value.success_criterion,
    }


def _resource_ref(value: ResourceRef) -> dict[str, JsonValue]:
    """@brief 投影 ResourceRef / Project a ResourceRef."""
    payload: dict[str, JsonValue] = {"resource_type": value.resource_type, "id": value.id}
    if value.revision is not None:
        payload["revision"] = value.revision
    return payload


def _job(value: Job) -> dict[str, JsonValue]:
    """@brief 穷尽投影统一 Job / Exhaustively project a unified Job."""
    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "kind": value.kind,
            "subject": _resource_ref(value.subject),
            "status": value.status.value,
            "progress": (
                {
                    "phase": value.progress.phase,
                    "completed": value.progress.completed,
                    "total": value.progress.total,
                    "unit": value.progress.unit.value,
                }
                if value.progress is not None
                else None
            ),
            "result_refs": [_resource_ref(item) for item in value.result_refs],
            "problem": _problem_details(value.problem) if value.problem is not None else None,
            "started_at": timestamp(value.started_at) if value.started_at is not None else None,
            "finished_at": timestamp(value.finished_at) if value.finished_at is not None else None,
        }
    )
    return payload


def _problem_details(value: ProblemDetails) -> dict[str, JsonValue]:
    """@brief 投影 Job ProblemDetails / Project Job ProblemDetails."""
    payload: dict[str, JsonValue] = {
        "type": value.type_uri,
        "title": value.title,
        "status": value.status,
        "code": value.code,
        "request_id": value.request_id,
        "retryable": value.retryable,
        "errors": [_problem_field_error(item) for item in value.errors],
    }
    if value.detail is not None:
        payload["detail"] = value.detail
    if value.instance is not None:
        payload["instance"] = value.instance
    if value.extensions:
        payload["extensions"] = {key: _thaw_json(item) for key, item in value.extensions.items()}
    return payload


def _problem_field_error(value: ProblemFieldError) -> dict[str, JsonValue]:
    """@brief 投影 ProblemFieldError / Project ProblemFieldError."""
    payload: dict[str, JsonValue] = {"pointer": value.pointer, "code": value.code}
    if value.message_key is not None:
        payload["message_key"] = value.message_key
    if value.params:
        payload["params"] = dict(value.params)
    return payload


def _thaw_json(value: PlatformJsonValue) -> JsonValue:
    """@brief 解冻 platform JSON 值 / Thaw a platform JSON value."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return {key: _thaw_json(item) for key, item in value.items()}


def _required_value(body: Mapping[str, JsonValue], field: str) -> JsonValue:
    if field not in body:
        raise RuntimeError(f"validated field {field} must be present")
    return body[field]


def _required_string(body: Mapping[str, JsonValue], field: str) -> str:
    value = body.get(field)
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a string")
    return value


def _optional_string(body: Mapping[str, JsonValue], field: str) -> str | None:
    value = body.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a string")
    return value


def _nullable_string(body: Mapping[str, JsonValue], field: str) -> str | None:
    value = _required_value(body, field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be nullable string")
    return value


def _required_integer(body: Mapping[str, JsonValue], field: str) -> int:
    value = body.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"validated field {field} must be an integer")
    return value


def _nullable_integer(body: Mapping[str, JsonValue], field: str) -> int | None:
    value = _required_value(body, field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"validated field {field} must be nullable integer")
    return value


def _required_number(body: Mapping[str, JsonValue], field: str) -> float:
    value = body.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(f"validated field {field} must be a number")
    return float(value)


def _required_boolean(body: Mapping[str, JsonValue], field: str) -> bool:
    value = body.get(field)
    if not isinstance(value, bool):
        raise RuntimeError(f"validated field {field} must be boolean")
    return value


def _required_object(body: Mapping[str, JsonValue], field: str) -> dict[str, JsonValue]:
    return _object(body.get(field), field)


def _required_array(body: Mapping[str, JsonValue], field: str) -> list[JsonValue]:
    value = body.get(field)
    if not isinstance(value, list):
        raise RuntimeError(f"validated field {field} must be an array")
    return value


def _string_tuple(body: Mapping[str, JsonValue], field: str) -> tuple[str, ...]:
    values = _required_array(body, field)
    if any(not isinstance(value, str) for value in values):
        raise RuntimeError(f"validated field {field} must contain strings")
    return tuple(cast(list[str], values))


def _object(value: JsonValue, label: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise RuntimeError(f"validated {label} must be an object")
    return value


def _array_object(value: JsonValue, label: str) -> dict[str, JsonValue]:
    return _object(value, label)


def _parse_timestamp(value: str) -> datetime:
    """@brief 解析 schema 已校验的 RFC 3339 timestamp / Parse a schema-validated RFC 3339 timestamp."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


router_v2_interview = create_v2_interview_router()
"""@brief 使用 production resolver 的 Interview V2 router / Interview V2 router using the production resolver."""


__all__ = [
    "V2InterviewHttpAdapter",
    "V2InterviewRuntime",
    "V2InterviewRuntimeResolver",
    "create_v2_interview_router",
    "router_v2_interview",
    "v2_interview_runtime_from_request",
]
