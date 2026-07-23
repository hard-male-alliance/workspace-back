"""@brief API V2 Job、Artifact、Event 与 Audit HTTP 适配器 / API V2 Platform HTTP adapter.

本模块只负责冻结的 HTTP 边界：严格 query/path/header，签名 cursor，强 ETag，
持久幂等取消，单 byte range，canonical JSON 投影与 SSE frame。Workspace 授权、
Job CAS、Artifact 完整性、event replay 和 audit transaction 由 Platform application 层拥有。
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from functools import wraps
from typing import Annotated, Concatenate, Protocol, cast

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import Response, StreamingResponse

from backend.api.v2_http import list_response
from backend.api.v2_transport import (
    DEFAULT_PAGE_LIMIT,
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
    problem_response,
    replayable_json,
    request_id,
    require_no_body,
    require_query,
    resource_meta,
    resource_response,
    timestamp,
    verified_principal,
)
from backend.application.platform import (
    ArtifactContentIntegrityError,
    PlatformApplicationError,
    PlatformApplicationService,
    PlatformConflict,
    PlatformPreconditionFailed,
    PlatformResourceNotFound,
)
from backend.application.ports.access import AuthorizationDenied, UnknownPrincipal
from backend.application.ports.platform import (
    ArtifactQuery,
    ByteRangeRequest,
    CollectionPage,
    EventReplayRequest,
    EventReplayWindowExpired,
    JobQuery,
    MutationContext,
    PageRequest,
    RangeNotSatisfiable,
    SubjectFilter,
)
from backend.application.ports.v2_idempotency import (
    ReplayableResponse,
    V2IdempotencyExecutor,
)
from backend.domain.common import DomainError, Problem
from backend.domain.platform import (
    ApiEvent,
    ApiEventId,
    Artifact,
    ArtifactId,
    ArtifactKind,
    AuditEvent,
    Job,
    JobId,
    PdfSourceMap,
    ProblemDetails,
    ProblemFieldError,
    ResourceRef,
)
from backend.domain.platform import (
    JsonValue as PlatformJsonValue,
)
from backend.domain.principals import (
    DomainInvariantError,
    TokenPrincipal,
    WorkspaceId,
)

#: @brief Job/Artifact 持久层的稳定倒序 / Stable descending Job/Artifact ordering.
_RESOURCE_SORT = ("-created_at", "-id")
#: @brief AuditEvent 持久层的稳定倒序 / Stable descending AuditEvent ordering.
_AUDIT_SORT = ("-occurred_at", "-id")
#: @brief 开放资源名语法 / Open resource-name grammar.
_RESOURCE_NAME = r"^[a-z][a-z0-9_.-]{2,100}$"
#: @brief query OpaqueId 语法 / Query OpaqueId grammar.
_OPAQUE_ID = r"^[A-Za-z][A-Za-z0-9_-]{7,159}$"
#: @brief 单 byte range 语法 / Single byte-range grammar.
_BYTE_RANGE = re.compile(r"bytes=([0-9]*)-([0-9]*)\Z", flags=re.IGNORECASE | re.ASCII)
#: @brief 为 Content-Disposition 选择的保守扩展名 / Conservative extensions for Content-Disposition.
_ARTIFACT_EXTENSIONS: Mapping[ArtifactKind, str] = {
    ArtifactKind.RESUME_PDF: "pdf",
    ArtifactKind.RESUME_JSON: "json",
    ArtifactKind.RESUME_DOCX: "docx",
    ArtifactKind.INTERVIEW_AUDIO: "audio",
    ArtifactKind.INTERVIEW_VIDEO: "video",
    ArtifactKind.INTERVIEW_TRANSCRIPT: "txt",
    ArtifactKind.GENERIC: "bin",
}

ResourceNameQuery = Annotated[
    str | None,
    Query(min_length=3, max_length=101, pattern=_RESOURCE_NAME),
]
"""@brief 可选开放资源名 query / Optional open resource-name query."""

OpaqueIdQuery = Annotated[
    str | None,
    Query(min_length=8, max_length=160, pattern=_OPAQUE_ID),
]
"""@brief 可选 OpaqueId query / Optional OpaqueId query."""

RangeHeader = Annotated[str | None, Header(alias="Range", max_length=128)]
"""@brief 有界单 Range header / Bounded single Range header."""

LastEventIdHeader = Annotated[
    str | None,
    Header(alias="Last-Event-ID", min_length=8, max_length=160),
]
"""@brief SSE 可选 Last-Event-ID header / Optional SSE Last-Event-ID header."""

#: @brief Platform boundary 可稳定映射的预期异常 / Expected Platform boundary errors.
_PLATFORM_BOUNDARY_ERRORS: tuple[type[Exception], ...] = (
    PlatformApplicationError,
    EventReplayWindowExpired,
    AuthorizationDenied,
    UnknownPrincipal,
    DomainInvariantError,
)


class V2PlatformRuntime(Protocol):
    """@brief 单个 Platform request 所需运行时依赖 / Runtime dependencies for one Platform request."""

    @property
    def platform(self) -> PlatformApplicationService:
        """@brief 返回 Platform 应用服务 / Return the Platform application service.

        @return Platform application service / Platform application service.
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
    def v2_idempotency(self) -> V2IdempotencyExecutor:
        """@brief 返回持久幂等 executor / Return the durable idempotency executor.

        @return idempotent-execution port / Idempotent-execution port.
        """

        ...


type V2PlatformRuntimeResolver = Callable[[Request], V2PlatformRuntime]
"""@brief 从 HTTP request 解析 Platform runtime / Resolve Platform runtime from an HTTP request."""


def v2_platform_runtime_from_request(request: Request) -> V2PlatformRuntime:
    """@brief 从 composition container 取得 Platform 依赖 / Read Platform dependencies from the container.

    @param request 当前 HTTP request / Current HTTP request.
    @return 结构化 Platform runtime / Structured Platform runtime.
    @raise RuntimeError container 尚未安装时抛出 / Raised when the container is unavailable.
    """

    container = getattr(request.app.state, "container", None)
    if container is None:
        raise RuntimeError("backend container is unavailable")
    return cast(V2PlatformRuntime, container)


def _translate_http_errors[AdapterT, **ParamT](
    handler: Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
) -> Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]]:
    """@brief 将预期 Platform 异常转换为 V2 ProblemDetails / Translate expected Platform errors.

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
        @param args endpoint 位置参数 / Endpoint positional arguments.
        @param kwargs endpoint 关键字参数 / Endpoint keyword arguments.
        @return 成功或 ProblemDetails response / Success or Problem Details response.
        """

        try:
            return await handler(adapter, request, *args, **kwargs)
        except DomainError as error:
            return problem_response(request, error.problem, error=error)
        except _PLATFORM_BOUNDARY_ERRORS as error:
            return problem_response(request, _platform_problem(error), error=error)

    return cast(
        Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
        wrapped,
    )


class V2PlatformHttpAdapter:
    """@brief 把 Platform 应用用例适配为冻结 API V2 路由 / Adapt Platform use cases to frozen API V2 routes.

    @param resolve_runtime 按 request 解析依赖的函数 / Per-request dependency resolver.
    """

    def __init__(self, resolve_runtime: V2PlatformRuntimeResolver) -> None:
        """@brief 构建并注册全部 Platform V2 路由 / Build and register every Platform V2 route.

        @param resolve_runtime 每个 request 的 runtime resolver / Runtime resolver per request.
        """

        self._resolve_runtime = resolve_runtime
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self) -> None:
        """@brief 注册契约中的九条 Platform 路由 / Register the nine contract Platform routes.

        @return 无返回值 / No return value.
        """

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
                "/api/v2/workspaces/{workspace_id}/jobs",
                self.list_jobs,
                "JobList",
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/jobs/{job_id}",
                self.get_job,
                "Job",
                None,
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/jobs/{job_id}/cancellations",
                self.cancel_job,
                "Job",
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/artifacts",
                self.list_artifacts,
                "ArtifactList",
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/artifacts/{artifact_id}",
                self.get_artifact,
                "Artifact",
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/artifacts/{artifact_id}/content",
                self.get_artifact_content,
                None,
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/artifacts/{artifact_id}/source-map",
                self.get_artifact_source_map,
                "PdfSourceMap",
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/events",
                self.stream_events,
                None,
                "ApiEvent",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/audit-events",
                self.list_audit_events,
                "AuditEventList",
                None,
            ),
        )
        for method, path, endpoint, response_definition, stream_definition in routes:
            extra: dict[str, JsonValue] = {"x-api-v2-phase": 6}
            if response_definition is not None:
                extra["x-contract-response"] = response_definition
            if stream_definition is not None:
                extra["x-contract-stream-item"] = stream_definition
            self.router.add_api_route(
                path,
                endpoint,
                methods=[method],
                openapi_extra=extra,
                status_code=200,
                response_class=Response,
            )

    @_translate_http_errors
    async def list_jobs(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
        kind: ResourceNameQuery = None,
        subject_type: ResourceNameQuery = None,
        subject_id: OpaqueIdQuery = None,
    ) -> Response:
        """@brief 分页与过滤 Workspace Jobs / Page and filter Workspace Jobs.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @param kind 可选 Job kind / Optional Job kind.
        @param subject_type 可选 subject type / Optional subject type.
        @param subject_id 可选 subject ID / Optional subject ID.
        @return JobList / JobList.
        """

        require_query(request, "cursor", "limit", "kind", "subject_type", "subject_id")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        query = _job_query(kind, subject_type, subject_id)
        filters = _collection_filters("jobs", query.cursor_binding)
        page_request = _page_request(
            cursor,
            limit,
            runtime.v2_cursor,
            principal,
            typed_workspace_id,
            filters,
            _RESOURCE_SORT,
        )
        page = await runtime.platform.list_jobs(
            principal,
            typed_workspace_id,
            query=query,
            page=page_request,
        )
        payload = _collection_payload(
            page,
            project=_job,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace_id,
            filters=filters,
            sort=_RESOURCE_SORT,
        )
        runtime.contracts_v2.validate_definition("JobList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def get_job(
        self,
        request: Request,
        workspace_id: OpaquePath,
        job_id: OpaquePath,
    ) -> Response:
        """@brief 读取 Workspace Job / Read a Workspace Job.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param job_id Job 标识 / Job identifier.
        @return 带强 ETag 的 Job / Job with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        value = await runtime.platform.get_job(
            verified_principal(request),
            WorkspaceId(workspace_id),
            JobId(job_id),
        )
        payload = _job(value)
        runtime.contracts_v2.validate_definition("Job", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def cancel_job(
        self,
        request: Request,
        workspace_id: OpaquePath,
        job_id: OpaquePath,
    ) -> Response:
        """@brief 以 ETag、CAS 与幂等 receipt 取消 Job / Cancel a Job with ETag, CAS, and an idempotency receipt.

        @param request 含 If-Match 与 Idempotency-Key 的无 body request / Bodyless request with If-Match and Idempotency-Key.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param job_id Job 标识 / Job identifier.
        @return 取消后的 Job / Cancelled Job.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_job_id = JobId(job_id)
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 内比较 Job ETag 并取消 / Compare the Job ETag and cancel after the first claim.

            @return 可重放 Job response / Replayable Job response.
            """

            current = await runtime.platform.get_job_for_cancellation(
                principal,
                typed_workspace_id,
                typed_job_id,
            )
            current_payload = _job(current)
            runtime.contracts_v2.validate_definition("Job", current_payload)
            match_etag_revision(raw_if_match, current_payload, current.meta.revision)
            value = await runtime.platform.cancel_job(
                principal,
                typed_workspace_id,
                typed_job_id,
                MutationContext(request_id(request), _trace_id(request)),
                expected_revision=current.meta.revision,
            )
            payload = _job(value)
            runtime.contracts_v2.validate_definition("Job", payload)
            return replayable_json(payload, status_code=200, etag=True)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=(f"/api/v2/workspaces/{workspace_id}/jobs/{job_id}/cancellations"),
            canonical_body=b"",
            content_type=None,
            if_match=raw_if_match,
            operation=operation,
            mapped_error_types=_PLATFORM_BOUNDARY_ERRORS,
            map_error=_platform_problem,
        )

    @_translate_http_errors
    async def list_artifacts(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
        kind: ArtifactKind | None = None,
        subject_type: ResourceNameQuery = None,
        subject_id: OpaqueIdQuery = None,
    ) -> Response:
        """@brief 分页与过滤 Workspace Artifacts / Page and filter Workspace Artifacts.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @param kind 可选 Artifact kind / Optional Artifact kind.
        @param subject_type 可选 subject type / Optional subject type.
        @param subject_id 可选 subject ID / Optional subject ID.
        @return ArtifactList / ArtifactList.
        """

        require_query(request, "cursor", "limit", "kind", "subject_type", "subject_id")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        query = _artifact_query(kind, subject_type, subject_id)
        filters = _collection_filters("artifacts", query.cursor_binding)
        page_request = _page_request(
            cursor,
            limit,
            runtime.v2_cursor,
            principal,
            typed_workspace_id,
            filters,
            _RESOURCE_SORT,
        )
        page = await runtime.platform.list_artifacts(
            principal,
            typed_workspace_id,
            query=query,
            page=page_request,
        )
        payload = _collection_payload(
            page,
            project=_artifact,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace_id,
            filters=filters,
            sort=_RESOURCE_SORT,
        )
        runtime.contracts_v2.validate_definition("ArtifactList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def get_artifact(
        self,
        request: Request,
        workspace_id: OpaquePath,
        artifact_id: OpaquePath,
    ) -> Response:
        """@brief 读取 Workspace Artifact metadata / Read Workspace Artifact metadata.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param artifact_id Artifact 标识 / Artifact identifier.
        @return 带强 ETag 的 Artifact / Artifact with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        value = await runtime.platform.get_artifact(
            verified_principal(request),
            WorkspaceId(workspace_id),
            ArtifactId(artifact_id),
        )
        payload = _artifact(value)
        runtime.contracts_v2.validate_definition("Artifact", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def get_artifact_content(
        self,
        request: Request,
        workspace_id: OpaquePath,
        artifact_id: OpaquePath,
        range_header: RangeHeader = None,
    ) -> Response:
        """@brief 以强内容 ETag 与单 Range 流式下载 Artifact / Stream Artifact content with a strong content ETag and one Range.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param artifact_id Artifact 标识 / Artifact identifier.
        @param range_header 可选 Range header / Optional Range header.
        @return 完整 200、部分 206 或结构化 416 / Full 200, partial 206, or structured 416.
        """

        del range_header
        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        try:
            download = await runtime.platform.open_artifact_content(
                verified_principal(request),
                WorkspaceId(workspace_id),
                ArtifactId(artifact_id),
                byte_range=_byte_range(request),
            )
        except RangeNotSatisfiable as error:
            response = problem_response(
                request,
                Problem(
                    "http.range_not_satisfiable",
                    416,
                    "Requested byte range is not satisfiable",
                ),
                error=error,
            )
            response.headers["Accept-Ranges"] = "bytes"
            response.headers["Content-Range"] = f"bytes */{error.total_size_bytes}"
            return response

        headers = {
            "Accept-Ranges": "bytes",
            "Content-Disposition": _content_disposition(download.artifact),
            "Content-Length": str(download.content_length),
            "Content-Type": download.artifact.media_type,
            "ETag": download.etag,
            "X-Request-Id": request_id(request),
        }
        status_code = 200
        if download.selected_range is not None:
            selected = download.selected_range
            status_code = 206
            headers["Content-Range"] = (
                f"bytes {selected.first}-{selected.last_inclusive}/{selected.total_size_bytes}"
            )
        return StreamingResponse(
            download.chunks,
            status_code=status_code,
            headers=headers,
        )

    @_translate_http_errors
    async def get_artifact_source_map(
        self,
        request: Request,
        workspace_id: OpaquePath,
        artifact_id: OpaquePath,
    ) -> Response:
        """@brief 读取并校验 PDF source map / Read and validate a PDF source map.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param artifact_id PDF Artifact 标识 / PDF Artifact identifier.
        @return PdfSourceMap / PdfSourceMap.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        value = await runtime.platform.get_pdf_source_map(
            verified_principal(request),
            WorkspaceId(workspace_id),
            ArtifactId(artifact_id),
        )
        payload = _pdf_source_map(value)
        runtime.contracts_v2.validate_definition("PdfSourceMap", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def stream_events(
        self,
        request: Request,
        workspace_id: OpaquePath,
        last_event_id: LastEventIdHeader = None,
    ) -> Response:
        """@brief 打开支持 Last-Event-ID 的 Workspace SSE / Open Workspace SSE supporting Last-Event-ID.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param last_event_id 可选重放起点 / Optional replay starting point.
        @return text/event-stream response / text/event-stream response.
        """

        del last_event_id
        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        events = await runtime.platform.open_event_stream(
            verified_principal(request),
            WorkspaceId(workspace_id),
            after_event_id=_last_event_id(request),
        )

        async def frames() -> AsyncIterator[bytes]:
            """@brief 把 ApiEvent 投影为 canonical SSE frames / Project ApiEvents into canonical SSE frames.

            @return SSE bytes 异步流 / Async stream of SSE bytes.
            """

            async for event in events:
                payload = _api_event(event)
                runtime.contracts_v2.validate_definition("ApiEvent", payload)
                yield (
                    f"id: {event.event_id}\nevent: {event.type}\ndata: ".encode()
                    + canonical_json_bytes(payload)
                    + b"\n\n"
                )

        return StreamingResponse(
            frames(),
            status_code=200,
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "X-Request-Id": request_id(request),
            },
            media_type="text/event-stream",
        )

    @_translate_http_errors
    async def list_audit_events(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Workspace AuditEvents / Page through Workspace AuditEvents.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @return AuditEventList / AuditEventList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        filters: dict[str, JsonValue] = {"collection": "audit_events"}
        page_request = _page_request(
            cursor,
            limit,
            runtime.v2_cursor,
            principal,
            typed_workspace_id,
            filters,
            _AUDIT_SORT,
        )
        page = await runtime.platform.list_audit_events(
            principal,
            typed_workspace_id,
            page=page_request,
        )
        payload = _collection_payload(
            page,
            project=_audit_event,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace_id,
            filters=filters,
            sort=_AUDIT_SORT,
        )
        runtime.contracts_v2.validate_definition("AuditEventList", payload)
        return json_response(request, payload)


def create_v2_platform_router(
    resolve_runtime: V2PlatformRuntimeResolver = v2_platform_runtime_from_request,
) -> APIRouter:
    """@brief 构建 Platform V2 router / Build the Platform V2 router.

    @param resolve_runtime 每个 request 的依赖 resolver / Per-request dependency resolver.
    @return 可挂载 FastAPI router / Mountable FastAPI router.
    """

    return V2PlatformHttpAdapter(resolve_runtime).router


def _job_query(
    kind: str | None,
    subject_type: str | None,
    subject_id: str | None,
) -> JobQuery:
    """@brief 构建并封闭 Job query 校验 / Build and close Job-query validation.

    @param kind 可选 Job kind / Optional Job kind.
    @param subject_type 可选 subject type / Optional subject type.
    @param subject_id 可选 subject ID / Optional subject ID.
    @return 已验证 JobQuery / Validated JobQuery.
    @raise DomainError query 非法时抛出 / Raised for an invalid query.
    """

    try:
        return JobQuery(kind, SubjectFilter(subject_type, subject_id))
    except ValueError as error:
        raise _invalid_query() from error


def _artifact_query(
    kind: ArtifactKind | None,
    subject_type: str | None,
    subject_id: str | None,
) -> ArtifactQuery:
    """@brief 构建并封闭 Artifact query 校验 / Build and close Artifact-query validation.

    @param kind 可选 Artifact kind / Optional Artifact kind.
    @param subject_type 可选 subject type / Optional subject type.
    @param subject_id 可选 subject ID / Optional subject ID.
    @return 已验证 ArtifactQuery / Validated ArtifactQuery.
    @raise DomainError query 非法时抛出 / Raised for an invalid query.
    """

    try:
        return ArtifactQuery(kind, SubjectFilter(subject_type, subject_id))
    except ValueError as error:
        raise _invalid_query() from error


def _invalid_query() -> DomainError:
    """@brief 构造统一严格 query 错误 / Build the uniform strict-query error.

    @return http.invalid_query DomainError / http.invalid_query DomainError.
    """

    return DomainError(Problem("http.invalid_query", 400, "Query parameters are invalid"))


def _collection_filters(
    collection: str,
    binding: tuple[str | None, str | None, str | None],
) -> dict[str, JsonValue]:
    """@brief 把完整过滤上下文绑定到 cursor / Bind the complete filter context into a cursor.

    @param collection 集合标识 / Collection identity.
    @param binding kind、subject type 与 subject ID / Kind, subject type, and subject ID.
    @return cursor filter object / Cursor filter object.
    """

    kind, subject_type, subject_id = binding
    return {
        "collection": collection,
        "kind": kind,
        "subject_type": subject_type,
        "subject_id": subject_id,
    }


def _page_request(
    cursor: str | None,
    limit: int,
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    filters: Mapping[str, JsonValue],
    sort: Sequence[str],
) -> PageRequest:
    """@brief 将签名 cursor 解码为应用层 continuation / Decode a signed cursor into an application continuation.

    @param cursor 可选签名 cursor / Optional signed cursor.
    @param limit 页长 / Page size.
    @param codec cursor codec / Cursor codec.
    @param principal 当前 principal / Current principal.
    @param workspace_id 路径 Workspace / Path Workspace.
    @param filters 完整集合上下文 / Complete collection context.
    @param sort 稳定排序 / Stable ordering.
    @return application PageRequest / Application PageRequest.
    @raise DomainError continuation 形状非法时抛出 / Raised for an invalid continuation shape.
    """

    if cursor is None:
        return PageRequest(limit=limit)
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
        return PageRequest(limit=limit, after=position)
    except ValueError as error:
        raise DomainError(
            Problem("http.cursor_invalid", 400, "Pagination cursor is invalid")
        ) from error


def _collection_payload[ItemT](
    page: CollectionPage[ItemT],
    *,
    project: Callable[[ItemT], JsonValue],
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    filters: Mapping[str, JsonValue],
    sort: Sequence[str],
) -> dict[str, JsonValue]:
    """@brief 封装 application 已分页结果 / Wrap an application-paginated result.

    @param page 应用层当前页 / Current application-layer page.
    @param project 项目投影 / Item projection.
    @param codec cursor codec / Cursor codec.
    @param principal 当前 principal / Current principal.
    @param workspace_id 路径 Workspace / Path Workspace.
    @param filters 完整集合上下文 / Complete collection context.
    @param sort 稳定排序 / Stable ordering.
    @return 契约集合 payload / Contract collection payload.
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


def _byte_range(request: Request) -> ByteRangeRequest | None:
    """@brief 严格解析唯一单 byte Range / Strictly parse the sole single byte Range.

    @param request 当前 request / Current request.
    @return 未按 Artifact 长度解析的 range 或空 / Unresolved range or absence.
    @raise DomainError Range 重复、多段或语法非法时抛出 / Raised for a duplicate, multipart, or malformed Range.
    """

    values = request.headers.getlist("Range")
    if not values:
        return None
    if len(values) != 1 or len(values[0]) > 128:
        raise _invalid_range()
    matched = _BYTE_RANGE.fullmatch(values[0])
    if matched is None:
        raise _invalid_range()
    first, last = matched.groups()
    if not first and not last:
        raise _invalid_range()
    try:
        if not first:
            return ByteRangeRequest(suffix_length=int(last))
        return ByteRangeRequest(
            first=int(first),
            last_inclusive=int(last) if last else None,
        )
    except ValueError as error:
        raise _invalid_range() from error


def _invalid_range() -> DomainError:
    """@brief 构造稳定 Range 语法错误 / Build the stable Range syntax error.

    @return http.invalid_range DomainError / http.invalid_range DomainError.
    """

    return DomainError(Problem("http.invalid_range", 400, "Range header is invalid"))


def _last_event_id(request: Request) -> ApiEventId | None:
    """@brief 无损读取并验证唯一 Last-Event-ID / Read and validate the sole Last-Event-ID.

    @param request 当前 request / Current request.
    @return 已验证 event ID 或空 / Validated event ID or absence.
    @raise DomainError header 重复或形状非法时抛出 / Raised for a duplicate or malformed header.
    """

    values = request.headers.getlist("Last-Event-ID")
    if not values:
        return None
    if len(values) != 1:
        raise DomainError(Problem("http.invalid_header", 400, "Last-Event-ID is invalid"))
    event_id = ApiEventId(values[0])
    try:
        EventReplayRequest(event_id)
    except ValueError as error:
        raise DomainError(
            Problem("http.invalid_header", 400, "Last-Event-ID is invalid")
        ) from error
    return event_id


def _trace_id(request: Request) -> str | None:
    """@brief 从 middleware 验证的 trace context 读取 trace ID / Read the trace ID from middleware-validated context.

    @param request 当前 request / Current request.
    @return 已验证 trace ID 或空 / Validated trace ID or absence.
    """

    trace = getattr(request.state, "trace_context", None)
    value = getattr(trace, "trace_id", None)
    return value if isinstance(value, str) else None


def _content_disposition(artifact: Artifact) -> str:
    """@brief 构造无注入 Content-Disposition / Build an injection-safe Content-Disposition.

    @param artifact 已验证 Artifact / Validated Artifact.
    @return attachment filename header / Attachment filename header.
    """

    extension = _ARTIFACT_EXTENSIONS[artifact.kind]
    return f'attachment; filename="{artifact.meta.id}.{extension}"'


def _platform_problem(error: BaseException) -> Problem:
    """@brief 将 Platform 应用/领域失败稳定映射到 HTTP Problem / Map Platform application/domain failures to HTTP Problems.

    @param error 预期失败 / Expected failure.
    @return transport problem / Transport problem.
    """

    if isinstance(error, PlatformResourceNotFound):
        return Problem(error.code, 404, "Resource was not found", detail=error.detail)
    if isinstance(error, ArtifactContentIntegrityError):
        return Problem(
            error.code,
            503,
            "Artifact content integrity validation failed",
            detail=error.detail,
            retryable=True,
        )
    if isinstance(error, PlatformConflict):
        return Problem(error.code, 409, "Resource state conflict", detail=error.detail)
    if isinstance(error, PlatformPreconditionFailed):
        return Problem(
            "http.precondition_failed",
            412,
            "Resource precondition failed",
            detail=error.detail,
        )
    if isinstance(error, EventReplayWindowExpired):
        return Problem(
            error.code,
            409,
            "Event replay window has expired",
            detail="Resynchronize authoritative resources before opening a new event stream.",
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
            detail="The resource does not satisfy the required platform state.",
        )
    if isinstance(error, PlatformApplicationError):
        return Problem(error.code, 409, "Platform operation failed", detail=error.detail)
    raise TypeError(f"unsupported Platform error: {type(error).__name__}")


def _resource_ref(value: ResourceRef) -> dict[str, JsonValue]:
    """@brief 投影 ResourceRef，缺省 revision 时省略 / Project ResourceRef, omitting absent revision.

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
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "kind": value.kind,
            "subject": _resource_ref(value.subject),
            "status": value.status.value,
            "progress": _job_progress(value) if value.progress is not None else None,
            "result_refs": [_resource_ref(reference) for reference in value.result_refs],
            "problem": _problem_details(value.problem) if value.problem is not None else None,
            "started_at": timestamp(value.started_at) if value.started_at is not None else None,
            "finished_at": timestamp(value.finished_at) if value.finished_at is not None else None,
        }
    )
    return payload


def _job_progress(value: Job) -> dict[str, JsonValue]:
    """@brief 投影 Job 的非空 progress / Project a Job's non-null progress.

    @param value 含 progress 的 Job / Job containing progress.
    @return JobProgress JSON / JobProgress JSON.
    @raise RuntimeError 调用方违反非空先决条件时抛出 / Raised when the caller violates the non-null precondition.
    """

    progress = value.progress
    if progress is None:
        raise RuntimeError("Job progress projection requires a non-null progress")
    return {
        "phase": progress.phase,
        "completed": progress.completed,
        "total": progress.total,
        "unit": progress.unit.value,
    }


def _problem_details(value: ProblemDetails) -> dict[str, JsonValue]:
    """@brief 投影 Job failure ProblemDetails / Project Job-failure ProblemDetails.

    @param value immutable ProblemDetails / Immutable ProblemDetails.
    @return contract ProblemDetails JSON / Contract ProblemDetails JSON.
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

    @param value immutable field error / Immutable field error.
    @return ProblemFieldError JSON / ProblemFieldError JSON.
    """

    payload: dict[str, JsonValue] = {
        "pointer": value.pointer,
        "code": value.code,
    }
    if value.message_key is not None:
        payload["message_key"] = value.message_key
    if value.params:
        payload["params"] = dict(value.params)
    return payload


def _artifact(value: Artifact) -> dict[str, JsonValue]:
    """@brief 穷尽投影 Artifact metadata / Exhaustively project Artifact metadata.

    @param value Artifact aggregate / Artifact aggregate.
    @return Artifact JSON / Artifact JSON.
    """

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "kind": value.kind.value,
            "subject": _resource_ref(value.subject),
            "media_type": value.media_type,
            "size_bytes": value.size_bytes,
            "sha256": value.sha256,
            "content_url": value.content_url,
            "page_count": value.page_count,
            "expires_at": timestamp(value.expires_at) if value.expires_at is not None else None,
        }
    )
    return payload


def _pdf_source_map(value: PdfSourceMap) -> dict[str, JsonValue]:
    """@brief 穷尽投影 PDF source map / Exhaustively project a PDF source map.

    @param value validated PdfSourceMap / Validated PdfSourceMap.
    @return PdfSourceMap JSON / PdfSourceMap JSON.
    """

    return {
        "artifact_id": str(value.artifact_id),
        "resume_id": value.resume_id,
        "resume_revision": value.resume_revision,
        "nodes": [
            {
                "entity_id": node.entity_id,
                "field_path": list(node.field_path),
                "page": node.page,
                "rects": [
                    {
                        "x": rectangle.x,
                        "y": rectangle.y,
                        "width": rectangle.width,
                        "height": rectangle.height,
                        "unit": rectangle.unit.value,
                    }
                    for rectangle in node.rects
                ],
            }
            for node in value.nodes
        ],
    }


def _api_event(value: ApiEvent) -> dict[str, JsonValue]:
    """@brief 穷尽投影 SSE ApiEvent / Exhaustively project an SSE ApiEvent.

    @param value ApiEvent envelope / ApiEvent envelope.
    @return ApiEvent JSON / ApiEvent JSON.
    """

    payload: dict[str, JsonValue] = {
        "event_id": str(value.event_id),
        "sequence": value.sequence,
        "type": value.type,
        "occurred_at": timestamp(value.occurred_at),
        "subject": _resource_ref(value.subject),
        "data": {key: _thaw_json(item) for key, item in value.data.items()},
    }
    if value.trace_id is not None:
        payload["trace_id"] = value.trace_id
    return payload


def _audit_event(value: AuditEvent) -> dict[str, JsonValue]:
    """@brief 穷尽投影 AuditEvent / Exhaustively project an AuditEvent.

    @param value AuditEvent envelope / AuditEvent envelope.
    @return AuditEvent JSON / AuditEvent JSON.
    """

    return {
        "id": str(value.id),
        "workspace_id": str(value.workspace_id),
        "occurred_at": timestamp(value.occurred_at),
        "actor": _resource_ref(value.actor),
        "action": value.action,
        "target": _resource_ref(value.target),
        "outcome": value.outcome.value,
        "request_id": value.request_id,
    }


def _thaw_json(value: PlatformJsonValue) -> JsonValue:
    """@brief 将领域 immutable JSON 转为 wire JSON / Thaw domain-immutable JSON into wire JSON.

    @param value 不可变领域 JSON / Immutable domain JSON.
    @return 可 canonical 序列化的 JSON / Canonically serializable JSON.
    """

    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


router_v2_platform = create_v2_platform_router()
"""@brief 默认从 composition container 解析依赖的 Platform router / Default container-resolved Platform router."""


__all__ = [
    "V2PlatformRuntime",
    "V2PlatformRuntimeResolver",
    "create_v2_platform_router",
    "router_v2_platform",
    "v2_platform_runtime_from_request",
]
