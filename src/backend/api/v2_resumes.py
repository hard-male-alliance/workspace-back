"""@brief API V2 Resume、revision、Job 与 proposal HTTP 适配器 / API V2 Resume HTTP adapter.

该模块只处理严格 JSON、强 ETag、opaque cursor、幂等 receipt 与契约投影。授权、CAS、
离线 batch ledger、事务与 Workspace 隔离全部由 Resume application service 和 UoW 实现。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from functools import wraps
from typing import Annotated, Concatenate, Protocol, assert_never, cast

from fastapi import APIRouter, Path, Request
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
    require_no_body,
    require_query,
    resource_meta,
    resource_response,
    strict_json_object,
    timestamp,
    verified_principal,
)
from backend.application.ports.access import AuthorizationDenied, UnknownPrincipal
from backend.application.ports.resumes import CollectionPage, PageRequest
from backend.application.ports.v2_idempotency import (
    ReplayableResponse,
    V2IdempotencyExecutor,
)
from backend.application.resumes import (
    CreateRenderJobCommand,
    CreateRestoreJobCommand,
    CreateResumeCommand,
    CreateResumeImportJobCommand,
    InvalidResumeCommand,
    ResumeApplicationError,
    ResumeApplicationService,
    ResumePreconditionFailed,
    ResumeResourceNotFound,
    UpdateResumeMetadataCommand,
)
from backend.domain.common import DomainError, Problem
from backend.domain.platform import Job
from backend.domain.principals import (
    DomainInvariantError,
    TokenPrincipal,
    WorkspaceId,
)
from backend.domain.resume_jobs import RenderFormat, RenderMode
from backend.domain.resume_proposals import (
    ProposalDecision,
    ProposalDecisionCommand,
    ResumeProposal,
)
from backend.domain.resumes import (
    ColorValue,
    ConflictStrategy,
    ContactMethod,
    DateRange,
    EntityKind,
    Measurement,
    MoveResumeEntity,
    PaletteIntent,
    PartialDate,
    RemoveResumeEntity,
    RenderHint,
    ResourceRef,
    ResumeBatchId,
    ResumeBatchKeyReused,
    ResumeConflict,
    ResumeDocument,
    ResumeDomainError,
    ResumeId,
    ResumeItem,
    ResumeItemKind,
    ResumeOperation,
    ResumeOperationBatch,
    ResumeOperationId,
    ResumeOperationOutcome,
    ResumeProfile,
    ResumeProposalId,
    ResumeRevision,
    ResumeRevisionConflict,
    ResumeRevisionSummary,
    ResumeSection,
    ResumeSectionKind,
    ResumeStyleIntent,
    ResumeSummary,
    RichText,
    SectionLayoutIntent,
    SetResumeField,
    SetResumeTemplate,
    TemplateRef,
    TextMark,
    TextMarkKind,
    TypographyIntent,
    UpsertResumeItem,
    UpsertResumeSection,
)

#: @brief Resume command 原始 body 上限 / Raw-body limit for Resume commands.
_MAX_RESUME_BODY_BYTES = 2 * 1024 * 1024
#: @brief Resume command JSON 容器深度上限 / JSON container-depth limit for Resume commands.
_MAX_RESUME_JSON_DEPTH = 24
#: @brief Resume 集合的稳定排序 / Stable ordering of the Resume collection.
_RESUME_SORT = ("id",)
#: @brief Revision 集合的稳定排序 / Stable ordering of the revision collection.
_REVISION_SORT = ("revision",)
#: @brief Proposal 集合的稳定排序 / Stable ordering of the proposal collection.
_PROPOSAL_SORT = ("id",)

PositiveRevision = Annotated[int, Path(ge=1)]
"""@brief 正整数 revision path 类型 / Positive revision path type."""

#: @brief Resume boundary 可稳定映射的预期异常 / Expected Resume boundary errors with stable mappings.
_RESUME_BOUNDARY_ERRORS: tuple[type[Exception], ...] = (
    ResumeApplicationError,
    ResumeDomainError,
    AuthorizationDenied,
    UnknownPrincipal,
    DomainInvariantError,
)


class V2ResumeRuntime(Protocol):
    """@brief 单个 Resume request 所需运行时依赖 / Runtime dependencies for one Resume request."""

    @property
    def resumes_v2(self) -> ResumeApplicationService:
        """@brief 返回 Resume V2 应用服务 / Return the Resume V2 application service.

        @return Resume 应用服务 / Resume application service.
        """
        ...

    @property
    def contracts_v2(self) -> ContractDefinitionValidator:
        """@brief 返回权威 V2 schema 校验器 / Return the authoritative V2 schema validator.

        @return definition 校验器 / Definition validator.
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
        """@brief 返回持久幂等执行器 / Return the durable idempotency executor.

        @return 幂等执行端口 / Idempotent-execution port.
        """
        ...


type V2ResumeRuntimeResolver = Callable[[Request], V2ResumeRuntime]
"""@brief 从 HTTP request 解析 Resume 运行时 / Resolve Resume runtime from an HTTP request."""


def v2_resume_runtime_from_request(request: Request) -> V2ResumeRuntime:
    """@brief 从 composition container 取得 Resume 依赖 / Read Resume dependencies from the container.

    @param request 当前 HTTP request / Current HTTP request.
    @return 结构化 Resume runtime / Structured Resume runtime.
    @raise RuntimeError container 尚未安装时抛出 / Raised when the container is unavailable.
    """

    container = getattr(request.app.state, "container", None)
    if container is None:
        raise RuntimeError("backend container is unavailable")
    return cast(V2ResumeRuntime, container)


def _translate_http_errors[AdapterT, **ParamT](
    handler: Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
) -> Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]]:
    """@brief 将预期 Resume 异常转换为 V2 ProblemDetails / Translate expected Resume errors.

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
        except _RESUME_BOUNDARY_ERRORS as error:
            return problem_response(request, _resume_problem(error), error=error)

    return cast(
        Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
        wrapped,
    )


class V2ResumeHttpAdapter:
    """@brief 把 Resume 应用用例适配为冻结的 API V2 路由 / Adapt Resume use cases to API V2.

    @param resolve_runtime 按 request 解析依赖的函数 / Per-request dependency resolver.
    """

    def __init__(self, resolve_runtime: V2ResumeRuntimeResolver) -> None:
        """@brief 构建并注册全部 Resume V2 路由 / Build and register every Resume V2 route.

        @param resolve_runtime 每个 request 的 runtime resolver / Runtime resolver per request.
        """

        self._resolve_runtime = resolve_runtime
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self) -> None:
        """@brief 注册契约中的十四条 Resume 路由 / Register the fourteen contract Resume routes.

        @return 无返回值 / No return value.
        """

        routes: tuple[
            tuple[str, str, Callable[..., Awaitable[Response]], str | None, str | None], ...
        ] = (
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/resumes",
                self.list_resumes,
                None,
                "ResumeList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/resumes",
                self.create_resume,
                "CreateResumeRequest",
                "ResumeDocument",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/resume-import-jobs",
                self.create_import_job,
                "CreateResumeImportJobRequest",
                "Job",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}",
                self.get_resume,
                None,
                "ResumeDocument",
            ),
            (
                "PATCH",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}",
                self.patch_resume,
                "UpdateResumeMetadataRequest",
                "ResumeDocument",
            ),
            (
                "DELETE",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}",
                self.delete_resume,
                None,
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/revisions",
                self.list_revisions,
                None,
                "ResumeRevisionList",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/revisions/{revision}",
                self.get_revision,
                None,
                "ResumeRevision",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/restore-jobs",
                self.create_restore_job,
                "CreateRestoreJobRequest",
                "Job",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/operations",
                self.apply_operations,
                "ResumeOperationBatch",
                "ResumeOperationResult",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/render-jobs",
                self.create_render_job,
                "CreateRenderJobRequest",
                "Job",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/proposals",
                self.list_proposals,
                None,
                "ResumeProposalList",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/resume-proposals/{proposal_id}",
                self.get_proposal,
                None,
                "ResumeProposal",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/resume-proposals/{proposal_id}/decisions",
                self.decide_proposal,
                "ProposalDecisionRequest",
                "ResumeOperationResult",
            ),
        )
        for method, path, endpoint, request_definition, response_definition in routes:
            extra: dict[str, JsonValue] = {"x-api-v2-phase": 2}
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
    async def list_resumes(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Workspace Resume / Page through Workspace Resumes.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @return ResumeList / ResumeList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        filters: dict[str, JsonValue] = {"collection": "resumes"}
        page_request = _page_request(
            cursor,
            limit,
            runtime.v2_cursor,
            principal,
            typed_workspace_id,
            filters,
            _RESUME_SORT,
        )
        page = await runtime.resumes_v2.list_resumes(
            principal,
            typed_workspace_id,
            page_request,
        )
        payload = _collection_payload(
            page,
            project=_resume_summary,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace_id,
            filters=filters,
            sort=_RESUME_SORT,
        )
        runtime.contracts_v2.validate_definition("ResumeList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def create_resume(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief 幂等创建 Resume / Idempotently create a Resume.

        @param request 含创建 body 与幂等键的 request / Request with body and idempotency key.
        @param workspace_id 路径 Workspace / Path workspace.
        @return 201 ResumeDocument / 201 ResumeDocument.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        body = await _resume_json(request, runtime.contracts_v2, "CreateResumeRequest")

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后创建 Resume / Create a Resume after the first claim.

            @return 可重放 201 response / Replayable 201 response.
            """

            document = await runtime.resumes_v2.create_resume(
                principal,
                typed_workspace_id,
                CreateResumeCommand(
                    _required_string(body, "title"),
                    _required_string(body, "locale"),
                    _template_ref(_required_object(body, "template")),
                    _optional_resume_id(body, "clone_from_resume_id"),
                ),
            )
            payload = _resume_document(document)
            runtime.contracts_v2.validate_definition("ResumeDocument", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=(
                    f"/api/v2/workspaces/{workspace_id}/resumes/{document.meta.id}"
                ),
                etag=True,
            )

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=f"/api/v2/workspaces/{workspace_id}/resumes",
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            operation=operation,
            mapped_error_types=_RESUME_BOUNDARY_ERRORS,
            map_error=_resume_problem,
        )

    @_translate_http_errors
    async def create_import_job(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief 幂等创建 Resume import Job / Idempotently create a Resume import job.

        @param request 含 import body 与幂等键的 request / Request with import body and key.
        @param workspace_id 路径 Workspace / Path workspace.
        @return 202 Job / 202 Job.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        body = await _resume_json(
            request,
            runtime.contracts_v2,
            "CreateResumeImportJobRequest",
        )

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后创建 import Job / Create the import job after the first claim.

            @return 可重放 202 response / Replayable 202 response.
            """

            job = await runtime.resumes_v2.create_import_job(
                principal,
                typed_workspace_id,
                CreateResumeImportJobCommand(
                    _required_string(body, "upload_session_id"),
                    _required_string(body, "title"),
                    _required_string(body, "locale"),
                    _template_ref(_required_object(body, "template")),
                ),
            )
            return _job_replay(runtime, job, workspace_id)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=f"/api/v2/workspaces/{workspace_id}/resume-import-jobs",
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            operation=operation,
            mapped_error_types=_RESUME_BOUNDARY_ERRORS,
            map_error=_resume_problem,
        )

    @_translate_http_errors
    async def get_resume(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
    ) -> Response:
        """@brief 读取 Workspace Resume SIR / Read a Workspace Resume SIR.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @return 带强 ETag 的 ResumeDocument / ResumeDocument with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        document = await runtime.resumes_v2.get_resume(
            verified_principal(request),
            WorkspaceId(workspace_id),
            ResumeId(resume_id),
        )
        payload = _resume_document(document)
        runtime.contracts_v2.validate_definition("ResumeDocument", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def patch_resume(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
    ) -> Response:
        """@brief 条件修改 Resume metadata / Conditionally update Resume metadata.

        @param request 含 Merge Patch 与 If-Match 的 request / Request with Merge Patch and If-Match.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @return 更新后的 ResumeDocument / Updated ResumeDocument.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_resume_id = ResumeId(resume_id)
        body = await _resume_json(
            request,
            runtime.contracts_v2,
            "UpdateResumeMetadataRequest",
        )
        current = await runtime.resumes_v2.get_resume(
            principal,
            typed_workspace_id,
            typed_resume_id,
        )
        expected_revision = match_etag_revision(
            if_match_header(request),
            _resume_document(current),
            current.meta.revision,
        )
        document = await runtime.resumes_v2.update_resume_metadata(
            principal,
            typed_workspace_id,
            typed_resume_id,
            UpdateResumeMetadataCommand(
                title=_optional_string(body, "title"),
                locale=_optional_string(body, "locale"),
            ),
            expected_revision=expected_revision,
        )
        payload = _resume_document(document)
        runtime.contracts_v2.validate_definition("ResumeDocument", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def delete_resume(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
    ) -> Response:
        """@brief 条件删除 Resume / Conditionally delete a Resume.

        @param request 含 If-Match 的无 body request / Bodyless request with If-Match.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @return 204 空 response / Empty 204 response.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_resume_id = ResumeId(resume_id)
        current = await runtime.resumes_v2.get_resume(
            principal,
            typed_workspace_id,
            typed_resume_id,
        )
        expected_revision = match_etag_revision(
            if_match_header(request),
            _resume_document(current),
            current.meta.revision,
        )
        await runtime.resumes_v2.delete_resume(
            principal,
            typed_workspace_id,
            typed_resume_id,
            expected_revision=expected_revision,
        )
        return empty_response(request)

    @_translate_http_errors
    async def list_revisions(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Resume revisions / Page through Resume revisions.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @return ResumeRevisionList / ResumeRevisionList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_resume_id = ResumeId(resume_id)
        filters: dict[str, JsonValue] = {
            "collection": "resume_revisions",
            "resume_id": resume_id,
        }
        page_request = _page_request(
            cursor,
            limit,
            runtime.v2_cursor,
            principal,
            typed_workspace_id,
            filters,
            _REVISION_SORT,
        )
        page = await runtime.resumes_v2.list_revisions(
            principal,
            typed_workspace_id,
            typed_resume_id,
            page_request,
        )
        payload = _collection_payload(
            page,
            project=_revision_summary,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace_id,
            filters=filters,
            sort=_REVISION_SORT,
        )
        runtime.contracts_v2.validate_definition("ResumeRevisionList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def get_revision(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
        revision: PositiveRevision,
    ) -> Response:
        """@brief 读取不可变 Resume revision / Read an immutable Resume revision.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param revision 正整数 revision / Positive revision.
        @return 带强 ETag 的 ResumeRevision / ResumeRevision with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        item = await runtime.resumes_v2.get_revision(
            verified_principal(request),
            WorkspaceId(workspace_id),
            ResumeId(resume_id),
            revision,
        )
        payload = _revision(item)
        runtime.contracts_v2.validate_definition("ResumeRevision", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def create_restore_job(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
    ) -> Response:
        """@brief 条件且幂等地创建 restore Job / Conditionally create an idempotent restore job.

        @param request 含 body、If-Match 与幂等键的 request / Request with body, If-Match, and key.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @return 202 Job / 202 Job.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_resume_id = ResumeId(resume_id)
        body = await _resume_json(request, runtime.contracts_v2, "CreateRestoreJobRequest")
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 内比较 ETag 并创建 Job / Compare ETag and create the job once.

            @return 可重放 202 response / Replayable 202 response.
            """

            current = await runtime.resumes_v2.get_resume(
                principal,
                typed_workspace_id,
                typed_resume_id,
            )
            expected_revision = match_etag_revision(
                raw_if_match,
                _resume_document(current),
                current.meta.revision,
            )
            job = await runtime.resumes_v2.create_restore_job(
                principal,
                typed_workspace_id,
                typed_resume_id,
                CreateRestoreJobCommand(_required_integer(body, "source_revision")),
                expected_revision=expected_revision,
            )
            return _job_replay(runtime, job, workspace_id)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=(
                f"/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/restore-jobs"
            ),
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=raw_if_match,
            operation=operation,
            mapped_error_types=_RESUME_BOUNDARY_ERRORS,
            map_error=_resume_problem,
        )

    @_translate_http_errors
    async def apply_operations(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
    ) -> Response:
        """@brief 条件且幂等地应用离线 operation batch / Apply an offline operation batch.

        @param request 含 batch、If-Match 与幂等键的 request / Request with batch, If-Match, and key.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @return ResumeOperationResult / ResumeOperationResult.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_resume_id = ResumeId(resume_id)
        body = await _resume_json(request, runtime.contracts_v2, "ResumeOperationBatch")
        batch = _operation_batch(body)
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 内执行 ETag/CAS batch / Execute the ETag/CAS batch once.

            @return 可重放 operation result / Replayable operation result.
            """

            current = await runtime.resumes_v2.get_resume(
                principal,
                typed_workspace_id,
                typed_resume_id,
            )
            expected_revision = match_etag_revision(
                raw_if_match,
                _resume_document(current),
                current.meta.revision,
            )
            outcome = await runtime.resumes_v2.apply_operations(
                principal,
                typed_workspace_id,
                typed_resume_id,
                batch,
                expected_revision=expected_revision,
            )
            payload = _operation_outcome(outcome)
            runtime.contracts_v2.validate_definition("ResumeOperationResult", payload)
            return replayable_json(payload, status_code=200, etag=True)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=(
                f"/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/operations"
            ),
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=raw_if_match,
            operation=operation,
            mapped_error_types=_RESUME_BOUNDARY_ERRORS,
            map_error=_resume_problem,
        )

    @_translate_http_errors
    async def create_render_job(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
    ) -> Response:
        """@brief 幂等创建 immutable-revision render Job / Create an idempotent render job.

        @param request 含 render body 与幂等键的 request / Request with render body and key.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @return 202 Job / 202 Job.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        body = await _resume_json(request, runtime.contracts_v2, "CreateRenderJobRequest")

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后创建 render Job / Create the render job after the first claim.

            @return 可重放 202 response / Replayable 202 response.
            """

            job = await runtime.resumes_v2.create_render_job(
                principal,
                typed_workspace_id,
                ResumeId(resume_id),
                CreateRenderJobCommand(
                    _required_integer(body, "resume_revision"),
                    RenderMode(_required_string(body, "mode")),
                    tuple(
                        RenderFormat(_array_string(value, "formats"))
                        for value in _required_array(body, "formats")
                    ),
                ),
            )
            return _job_replay(runtime, job, workspace_id)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=(
                f"/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/render-jobs"
            ),
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            operation=operation,
            mapped_error_types=_RESUME_BOUNDARY_ERRORS,
            map_error=_resume_problem,
        )

    @_translate_http_errors
    async def list_proposals(
        self,
        request: Request,
        workspace_id: OpaquePath,
        resume_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Resume proposals / Page through Resume proposals.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @return ResumeProposalList / ResumeProposalList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        filters: dict[str, JsonValue] = {
            "collection": "resume_proposals",
            "resume_id": resume_id,
        }
        page_request = _page_request(
            cursor,
            limit,
            runtime.v2_cursor,
            principal,
            typed_workspace_id,
            filters,
            _PROPOSAL_SORT,
        )
        page = await runtime.resumes_v2.list_proposals(
            principal,
            typed_workspace_id,
            ResumeId(resume_id),
            page_request,
        )
        payload = _collection_payload(
            page,
            project=_proposal,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace_id,
            filters=filters,
            sort=_PROPOSAL_SORT,
        )
        runtime.contracts_v2.validate_definition("ResumeProposalList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def get_proposal(
        self,
        request: Request,
        workspace_id: OpaquePath,
        proposal_id: OpaquePath,
    ) -> Response:
        """@brief 在 Workspace 内读取 Resume proposal / Read a Resume proposal in a Workspace.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param proposal_id Proposal 标识 / Proposal identifier.
        @return 带强 ETag 的 ResumeProposal / ResumeProposal with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        item = await runtime.resumes_v2.get_proposal(
            verified_principal(request),
            WorkspaceId(workspace_id),
            ResumeProposalId(proposal_id),
        )
        payload = _proposal(item)
        runtime.contracts_v2.validate_definition("ResumeProposal", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def decide_proposal(
        self,
        request: Request,
        workspace_id: OpaquePath,
        proposal_id: OpaquePath,
    ) -> Response:
        """@brief 条件且幂等地决策 proposal / Conditionally decide a proposal idempotently.

        @param request 含 decision、If-Match 与幂等键的 request / Request with decision, ETag, and key.
        @param workspace_id 路径 Workspace / Path workspace.
        @param proposal_id Proposal 标识 / Proposal identifier.
        @return ResumeOperationResult / ResumeOperationResult.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_proposal_id = ResumeProposalId(proposal_id)
        body = await _resume_json(request, runtime.contracts_v2, "ProposalDecisionRequest")
        command = ProposalDecisionCommand(
            ProposalDecision(_required_string(body, "decision")),
            tuple(
                ResumeOperationId(_array_string(value, "accepted_operation_ids"))
                for value in _required_array(body, "accepted_operation_ids")
            ),
        )
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 内比较 proposal ETag 并决策 / Compare proposal ETag and decide once.

            @return 可重放 operation result / Replayable operation result.
            """

            current = await runtime.resumes_v2.get_proposal(
                principal,
                typed_workspace_id,
                typed_proposal_id,
            )
            expected_revision = match_etag_revision(
                raw_if_match,
                _proposal(current),
                current.meta.revision,
            )
            outcome = await runtime.resumes_v2.decide_proposal(
                principal,
                typed_workspace_id,
                typed_proposal_id,
                command,
                expected_revision=expected_revision,
            )
            payload = _operation_outcome(outcome)
            runtime.contracts_v2.validate_definition("ResumeOperationResult", payload)
            return replayable_json(payload, status_code=200, etag=True)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=(
                f"/api/v2/workspaces/{workspace_id}/resume-proposals/{proposal_id}/decisions"
            ),
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=raw_if_match,
            operation=operation,
            mapped_error_types=_RESUME_BOUNDARY_ERRORS,
            map_error=_resume_problem,
        )


def create_v2_resume_router(
    resolve_runtime: V2ResumeRuntimeResolver = v2_resume_runtime_from_request,
) -> APIRouter:
    """@brief 创建完整 Resume V2 router / Create the complete Resume V2 router.

    @param resolve_runtime 每个 request 的依赖 resolver / Per-request dependency resolver.
    @return 可挂载的 FastAPI router / Mountable FastAPI router.
    """

    return V2ResumeHttpAdapter(resolve_runtime).router


def _declared_status(method: str, path: str) -> int:
    """@brief 返回 OpenAPI 的成功状态 / Return the declared OpenAPI success status.

    @param method HTTP method / HTTP method.
    @param path canonical route pattern / Canonical route pattern.
    @return 200、201、202 或 204 / 200, 201, 202, or 204.
    """

    if method == "DELETE":
        return 204
    if method == "POST" and path == "/api/v2/workspaces/{workspace_id}/resumes":
        return 201
    if method == "POST" and path.endswith(
        ("resume-import-jobs", "restore-jobs", "render-jobs")
    ):
        return 202
    return 200


async def _resume_json(
    request: Request,
    validator: ContractDefinitionValidator,
    definition: str,
) -> dict[str, JsonValue]:
    """@brief 以 Resume 专属资源上限读取严格 JSON / Read strict JSON with Resume limits.

    @param request 当前 request / Current request.
    @param validator 权威 V2 validator / Authoritative V2 validator.
    @param definition request definition / Request definition.
    @return 已校验 JSON object / Validated JSON object.
    """

    return await strict_json_object(
        request,
        validator=validator,
        definition=definition,
        max_body_bytes=_MAX_RESUME_BODY_BYTES,
        max_depth=_MAX_RESUME_JSON_DEPTH,
    )


def _page_request(
    cursor: str | None,
    limit: int,
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId,
    filters: Mapping[str, JsonValue],
    sort: Sequence[str],
) -> PageRequest:
    """@brief 将已签名 cursor 解成应用层 continuation / Decode a cursor into application continuation.

    @param cursor 可选签名 cursor / Optional signed cursor.
    @param limit 页长 / Page size.
    @param codec cursor codec / Cursor codec.
    @param principal 当前 principal / Current principal.
    @param workspace_id 路径 Workspace / Path workspace.
    @param filters 完整集合上下文 / Complete collection context.
    @param sort 稳定排序 / Stable ordering.
    @return 应用层 PageRequest / Application PageRequest.
    @raise DomainError continuation 形状无效时抛出 / Raised for an invalid continuation shape.

    @note 不在 transport 层重新排序或切页；``after`` 原样交给 repository。
        / Transport never re-sorts or re-pages; ``after`` is passed through unchanged.
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
    return PageRequest(limit=limit, after=position)


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

    @param page 应用层当前页 / Application-layer current page.
    @param project 项目投影 / Item projection.
    @param codec cursor codec / Cursor codec.
    @param principal 当前 principal / Current principal.
    @param workspace_id 路径 Workspace / Path workspace.
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


def _job_replay(
    runtime: V2ResumeRuntime,
    job: Job,
    workspace_id: str,
) -> ReplayableResponse:
    """@brief 校验并构建可重放 202 Job / Validate and build a replayable 202 Job.

    @param runtime 当前 Resume runtime / Current Resume runtime.
    @param job 新建 queued Job / Newly queued job.
    @param workspace_id canonical path Workspace / Canonical path workspace.
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


def _resume_problem(error: BaseException) -> Problem:
    """@brief 将 Resume 应用/领域失败稳定映射到 HTTP Problem / Map Resume failures to HTTP.

    @param error 预期失败 / Expected failure.
    @return transport problem / Transport problem.
    """

    if isinstance(error, ResumeResourceNotFound):
        return Problem(error.code, 404, "Resource was not found", detail=error.detail)
    if isinstance(error, ResumePreconditionFailed):
        return Problem(error.code, 412, "Resource precondition failed", detail=error.detail)
    if isinstance(error, InvalidResumeCommand):
        return Problem(
            error.code,
            422,
            "Command violates a domain constraint",
            detail=error.detail,
        )
    if isinstance(error, ResumeRevisionConflict):
        return Problem(
            error.code,
            412,
            "Resume revision is stale",
            detail="Refresh the resume or replay operations against the current revision.",
            retryable=True,
            violations=[
                {
                    "pointer": "/base_revision",
                    "code": "stale_revision",
                    "message": {
                        "message_key": "errors.resume.stale_revision",
                        "params": {"current_revision": error.current_revision},
                    },
                }
            ],
            extensions={"org.hmalliances.current_revision": error.current_revision},
        )
    if isinstance(error, ResumeBatchKeyReused) or (
        isinstance(error, ResumeDomainError)
        and error.code
        in {
            "idempotency.key_reused",
            "resume.operation_id_reused",
            "resume.proposal_already_decided",
            "resume.proposal_expired",
        }
    ):
        return Problem(error.code, 409, "Resource state conflict", detail=error.detail)
    if isinstance(error, ResumeDomainError):
        return Problem(
            error.code,
            422,
            "Command violates a domain constraint",
            detail=error.detail,
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
            detail=str(error),
        )
    if isinstance(error, ResumeApplicationError):
        return Problem(error.code, 409, "Application command failed", detail=error.detail)
    raise TypeError(f"unsupported Resume error: {type(error).__name__}")


def _operation_batch(body: Mapping[str, JsonValue]) -> ResumeOperationBatch:
    """@brief 从已校验 body 构建 operation batch / Build an operation batch from validated JSON.

    @param body 已校验 ResumeOperationBatch / Validated ResumeOperationBatch.
    @return 类型化 batch / Typed batch.
    """

    return ResumeOperationBatch(
        ResumeBatchId(_required_string(body, "client_batch_id")),
        _required_integer(body, "base_revision"),
        ConflictStrategy(_required_string(body, "conflict_strategy")),
        tuple(_operation(_array_object(value, "operations")) for value in _required_array(body, "operations")),
        RenderHint(_required_string(body, "render_hint")),
    )


def _operation(body: Mapping[str, JsonValue]) -> ResumeOperation:
    """@brief 穷尽解析六种 Resume operation / Exhaustively parse the six Resume operations.

    @param body 已校验 operation object / Validated operation object.
    @return 类型化 Resume operation / Typed Resume operation.
    @raise RuntimeError validator 违反 discriminated union 时抛出 / Raised for a broken validator guarantee.
    """

    operation_id = ResumeOperationId(_required_string(body, "operation_id"))
    operation_type = _required_string(body, "op")
    if operation_type == "set_field":
        return SetResumeField(
            operation_id,
            _required_string(body, "entity_id"),
            tuple(
                _array_string(value, "field_path")
                for value in _required_array(body, "field_path")
            ),
            deepcopy(_required_value(body, "value")),
        )
    if operation_type == "upsert_section":
        return UpsertResumeSection(
            operation_id,
            _section(_required_object(body, "section")),
            _nullable_string(body, "after_section_id"),
        )
    if operation_type == "upsert_item":
        return UpsertResumeItem(
            operation_id,
            _required_string(body, "section_id"),
            _item(_required_object(body, "item")),
            _nullable_string(body, "after_item_id"),
        )
    if operation_type == "remove_entity":
        return RemoveResumeEntity(
            operation_id,
            EntityKind(_required_string(body, "entity_kind")),
            _required_string(body, "entity_id"),
        )
    if operation_type == "move_entity":
        return MoveResumeEntity(
            operation_id,
            EntityKind(_required_string(body, "entity_kind")),
            _required_string(body, "entity_id"),
            _nullable_string(body, "parent_id"),
            _nullable_string(body, "after_id"),
        )
    if operation_type == "set_template":
        settings = _required_object(body, "settings")
        return SetResumeTemplate(
            operation_id,
            _template_ref(_required_object(body, "template")),
            deepcopy(settings),
        )
    raise RuntimeError("validated ResumeOperation has an unknown discriminator")


def _section(body: Mapping[str, JsonValue]) -> ResumeSection:
    """@brief 解析已校验 ResumeSection / Parse a validated ResumeSection.

    @param body section object / Section object.
    @return 领域 section / Domain section.
    """

    content = body.get("content")
    return ResumeSection(
        _required_string(body, "id"),
        ResumeSectionKind(_required_string(body, "kind")),
        _required_string(body, "title"),
        _required_boolean(body, "visible"),
        None if content is None else _rich_text(_object(content, "content")),
        tuple(_item(_array_object(value, "items")) for value in _required_array(body, "items")),
    )


def _item(body: Mapping[str, JsonValue]) -> ResumeItem:
    """@brief 解析已校验 ResumeItem / Parse a validated ResumeItem.

    @param body item object / Item object.
    @return 领域 item / Domain item.
    """

    date_range = body.get("date_range")
    summary = body.get("summary")
    return ResumeItem(
        id=_required_string(body, "id"),
        kind=ResumeItemKind(_required_string(body, "kind")),
        title=_nullable_string(body, "title"),
        subtitle=_nullable_string(body, "subtitle"),
        organization=_nullable_string(body, "organization"),
        location=_nullable_string(body, "location"),
        date_range=(
            None if date_range is None else _date_range(_object(date_range, "date_range"))
        ),
        summary=None if summary is None else _rich_text(_object(summary, "summary")),
        highlights=tuple(
            _rich_text(_array_object(value, "highlights"))
            for value in _required_array(body, "highlights")
        ),
        skills=tuple(
            _array_string(value, "skills") for value in _required_array(body, "skills")
        ),
        tags=tuple(_array_string(value, "tags") for value in _required_array(body, "tags")),
        visible=_required_boolean(body, "visible"),
        url=_nullable_string(body, "url"),
    )


def _date_range(body: Mapping[str, JsonValue]) -> DateRange:
    """@brief 解析 wire DateRange 的 present sentinel / Parse the DateRange present sentinel.

    @param body DateRange object / DateRange object.
    @return 领域 DateRange / Domain DateRange.
    """

    start = _nullable_string(body, "start")
    end = _nullable_string(body, "end")
    return DateRange(
        PartialDate(start) if start is not None else None,
        PartialDate(end) if end is not None and end != "present" else None,
        present=end == "present",
    )


def _rich_text(body: Mapping[str, JsonValue]) -> RichText:
    """@brief 解析已校验 RichText / Parse validated RichText.

    @param body RichText object / RichText object.
    @return 领域 RichText / Domain RichText.
    """

    return RichText(
        _required_string(body, "text"),
        tuple(
            _text_mark(_array_object(value, "marks"))
            for value in _required_array(body, "marks")
        ),
    )


def _text_mark(body: Mapping[str, JsonValue]) -> TextMark:
    """@brief 解析已校验 TextMark / Parse a validated TextMark.

    @param body TextMark object / TextMark object.
    @return 领域 TextMark / Domain TextMark.
    """

    return TextMark(
        _required_integer(body, "start"),
        _required_integer(body, "end"),
        TextMarkKind(_required_string(body, "kind")),
        _optional_string(body, "href"),
    )


def _template_ref(body: Mapping[str, JsonValue]) -> TemplateRef:
    """@brief 解析 immutable TemplateRef / Parse an immutable TemplateRef.

    @param body TemplateRef object / TemplateRef object.
    @return 领域模板引用 / Domain template reference.
    """

    return TemplateRef(
        _required_string(body, "template_id"),
        _required_string(body, "version"),
    )


def _resume_document(document: ResumeDocument) -> dict[str, JsonValue]:
    """@brief 穷尽投影权威 Resume SIR / Exhaustively project the authoritative Resume SIR.

    @param document Resume document / Resume document.
    @return ResumeDocument JSON / ResumeDocument JSON.
    """

    payload = resource_meta(document.meta)
    payload.update(
        {
            "workspace_id": str(document.workspace_id),
            "title": document.title,
            "locale": document.locale,
            "profile": _profile(document.profile),
            "sections": [_section_payload(section) for section in document.sections],
            "template": _template_ref_payload(document.template),
            "style": _style(document.style),
            "knowledge_source_id": document.knowledge_source_id,
        }
    )
    return payload


def _resume_summary(summary: ResumeSummary) -> dict[str, JsonValue]:
    """@brief 投影 ResumeSummary / Project ResumeSummary.

    @param summary Resume 摘要 / Resume summary.
    @return ResumeSummary JSON / ResumeSummary JSON.
    """

    payload = resource_meta(summary.meta)
    payload.update(
        {
            "workspace_id": str(summary.workspace_id),
            "title": summary.title,
            "locale": summary.locale,
            "template": _template_ref_payload(summary.template),
        }
    )
    return payload


def _profile(profile: ResumeProfile) -> dict[str, JsonValue]:
    """@brief 投影 ResumeProfile / Project ResumeProfile.

    @param profile Resume profile / Resume profile.
    @return ResumeProfile JSON / ResumeProfile JSON.
    """

    return {
        "full_name": profile.full_name,
        "headline": profile.headline,
        "summary": _rich_text_payload(profile.summary) if profile.summary is not None else None,
        "contacts": [_contact(contact) for contact in profile.contacts],
    }


def _contact(contact: ContactMethod) -> dict[str, JsonValue]:
    """@brief 投影 ContactMethod / Project ContactMethod.

    @param contact 联系方式 / Contact method.
    @return ContactMethod JSON / ContactMethod JSON.
    """

    return {
        "id": contact.id,
        "kind": contact.kind.value,
        "label": contact.label,
        "value": contact.value,
        "url": contact.url,
    }


def _section_payload(section: ResumeSection) -> dict[str, JsonValue]:
    """@brief 投影 ResumeSection / Project ResumeSection.

    @param section Resume section / Resume section.
    @return ResumeSection JSON / ResumeSection JSON.
    """

    return {
        "id": section.id,
        "kind": section.kind.value,
        "title": section.title,
        "visible": section.visible,
        "content": _rich_text_payload(section.content) if section.content is not None else None,
        "items": [_item_payload(item) for item in section.items],
    }


def _item_payload(item: ResumeItem) -> dict[str, JsonValue]:
    """@brief 投影 ResumeItem / Project ResumeItem.

    @param item Resume item / Resume item.
    @return ResumeItem JSON / ResumeItem JSON.
    """

    return {
        "id": item.id,
        "kind": item.kind.value,
        "title": item.title,
        "subtitle": item.subtitle,
        "organization": item.organization,
        "location": item.location,
        "date_range": _date_range_payload(item.date_range) if item.date_range else None,
        "summary": _rich_text_payload(item.summary) if item.summary else None,
        "highlights": [_rich_text_payload(value) for value in item.highlights],
        "skills": list(item.skills),
        "tags": list(item.tags),
        "visible": item.visible,
        "url": item.url,
    }


def _date_range_payload(value: DateRange) -> dict[str, JsonValue]:
    """@brief 投影 DateRange present sentinel / Project the DateRange present sentinel.

    @param value 领域日期范围 / Domain date range.
    @return DateRange JSON / DateRange JSON.
    """

    end: str | None = "present" if value.present else None
    if value.end is not None:
        end = value.end.value
    return {
        "start": value.start.value if value.start is not None else None,
        "end": end,
    }


def _rich_text_payload(value: RichText) -> dict[str, JsonValue]:
    """@brief 投影 RichText 与可选 href / Project RichText and optional href.

    @param value 富文本 / Rich text.
    @return RichText JSON / RichText JSON.
    """

    marks: list[JsonValue] = []
    for mark in value.marks:
        payload: dict[str, JsonValue] = {
            "start": mark.start,
            "end": mark.end,
            "kind": mark.kind.value,
        }
        if mark.href is not None:
            payload["href"] = mark.href
        marks.append(payload)
    return {"text": value.text, "marks": marks}


def _style(style: ResumeStyleIntent) -> dict[str, JsonValue]:
    """@brief 穷尽投影 renderer-independent style intent / Exhaustively project style intent.

    @param style Resume style intent / Resume style intent.
    @return ResumeStyleIntent JSON / ResumeStyleIntent JSON.
    """

    return {
        "style_contract_version": style.style_contract_version,
        "page": {
            "size": style.page.size.value,
            "custom_width": (
                _measurement(style.page.custom_width)
                if style.page.custom_width is not None
                else None
            ),
            "custom_height": (
                _measurement(style.page.custom_height)
                if style.page.custom_height is not None
                else None
            ),
            "orientation": style.page.orientation.value,
            "margins": {
                "top": _measurement(style.page.margins.top),
                "right": _measurement(style.page.margins.right),
                "bottom": _measurement(style.page.margins.bottom),
                "left": _measurement(style.page.margins.left),
            },
            "max_pages": style.page.max_pages,
            "show_page_numbers": style.page.show_page_numbers,
        },
        "typography": _typography(style.typography),
        "palette": _palette(style.palette),
        "density": style.density,
        "date_format_token": style.date_format_token,
        "bullet_style_token": style.bullet_style_token,
        "section_layout": [_section_layout(value) for value in style.section_layout],
        "template_settings": cast(JsonValue, deepcopy(dict(style.template_settings))),
        "extensions": cast(JsonValue, deepcopy(dict(style.extensions))),
    }


def _measurement(value: Measurement) -> dict[str, JsonValue]:
    """@brief 投影 Measurement / Project Measurement.

    @param value 测量值 / Measurement.
    @return Measurement JSON / Measurement JSON.
    """

    return {"value": value.value, "unit": value.unit.value}


def _typography(value: TypographyIntent) -> dict[str, JsonValue]:
    """@brief 投影 TypographyIntent / Project TypographyIntent.

    @param value 排版意图 / Typography intent.
    @return TypographyIntent JSON / TypographyIntent JSON.
    """

    return {
        "font_family_token": value.font_family_token,
        "base_size_pt": value.base_size_pt,
        "line_height": value.line_height,
        "heading_scale": value.heading_scale,
        "letter_spacing_em": value.letter_spacing_em,
    }


def _palette(value: PaletteIntent) -> dict[str, JsonValue]:
    """@brief 投影 PaletteIntent / Project PaletteIntent.

    @param value 色板意图 / Palette intent.
    @return PaletteIntent JSON / PaletteIntent JSON.
    """

    return {
        "primary": _color(value.primary),
        "secondary": _color(value.secondary),
        "text": _color(value.text),
        "muted_text": _color(value.muted_text),
        "background": _color(value.background),
    }


def _color(value: ColorValue) -> dict[str, JsonValue]:
    """@brief 投影 ColorValue / Project ColorValue.

    @param value 颜色值 / Color value.
    @return ColorValue JSON / ColorValue JSON.
    """

    return {"space": value.space.value, "value": value.value}


def _section_layout(value: SectionLayoutIntent) -> dict[str, JsonValue]:
    """@brief 投影 SectionLayoutIntent / Project SectionLayoutIntent.

    @param value section 布局意图 / Section layout intent.
    @return SectionLayoutIntent JSON / SectionLayoutIntent JSON.
    """

    return {
        "section_id": value.section_id,
        "zone": value.zone,
        "keep_together": value.keep_together,
        "page_break_before": value.page_break_before,
        "compactness": value.compactness,
        "heading_style_token": value.heading_style_token,
    }


def _template_ref_payload(value: TemplateRef) -> dict[str, JsonValue]:
    """@brief 投影 TemplateRef / Project TemplateRef.

    @param value immutable 模板引用 / Immutable template reference.
    @return TemplateRef JSON / TemplateRef JSON.
    """

    return {"template_id": value.template_id, "version": value.version}


def _resource_ref(value: ResourceRef) -> dict[str, JsonValue]:
    """@brief 投影 ResourceRef，并在缺省时省略 revision / Project ResourceRef, omitting absent revision.

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


def _revision(value: ResumeRevision) -> dict[str, JsonValue]:
    """@brief 投影不可变 ResumeRevision / Project an immutable ResumeRevision.

    @param value Resume revision / Resume revision.
    @return ResumeRevision JSON / ResumeRevision JSON.
    """

    return {
        "resume_id": str(value.resume_id),
        "revision": value.revision,
        "created_at": timestamp(value.created_at),
        "created_by": _resource_ref(ResourceRef("user", str(value.created_by))),
        "document": _resume_document(value.document),
    }


def _revision_summary(value: ResumeRevisionSummary) -> dict[str, JsonValue]:
    """@brief 投影 ResumeRevisionSummary / Project ResumeRevisionSummary.

    @param value Revision 摘要 / Revision summary.
    @return ResumeRevisionSummary JSON / ResumeRevisionSummary JSON.
    """

    return {
        "resume_id": str(value.resume_id),
        "revision": value.revision,
        "created_at": timestamp(value.created_at),
        "created_by": _resource_ref(ResourceRef("user", str(value.created_by))),
    }


def _operation_outcome(value: ResumeOperationOutcome) -> dict[str, JsonValue]:
    """@brief 投影 ResumeOperationResult / Project ResumeOperationResult.

    @param value operation outcome / Operation outcome.
    @return ResumeOperationResult JSON / ResumeOperationResult JSON.
    """

    return {
        "resume": _resume_document(value.resume),
        "applied_operation_ids": [str(item) for item in value.applied_operation_ids],
        "conflicts": [_conflict(item) for item in value.conflicts],
        "render_job_ref": (
            _resource_ref(value.render_job_ref) if value.render_job_ref is not None else None
        ),
    }


def _conflict(value: ResumeConflict) -> dict[str, JsonValue]:
    """@brief 投影 ResumeConflict / Project ResumeConflict.

    @param value operation conflict / Operation conflict.
    @return ResumeConflict JSON / ResumeConflict JSON.
    """

    return {
        "operation_id": str(value.operation_id),
        "code": value.code,
        "entity_id": value.entity_id,
        "field_path": list(value.field_path),
    }


def _operation_payload(value: ResumeOperation) -> dict[str, JsonValue]:
    """@brief 穷尽投影六种 Resume operation / Exhaustively project six Resume operations.

    @param value 类型化 operation / Typed operation.
    @return discriminated ResumeOperation JSON / Discriminated ResumeOperation JSON.
    """

    if isinstance(value, SetResumeField):
        return {
            "operation_id": str(value.operation_id),
            "op": value.op,
            "entity_id": value.entity_id,
            "field_path": list(value.field_path),
            "value": deepcopy(value.value),
        }
    if isinstance(value, UpsertResumeSection):
        return {
            "operation_id": str(value.operation_id),
            "op": value.op,
            "section": _section_payload(value.section),
            "after_section_id": value.after_section_id,
        }
    if isinstance(value, UpsertResumeItem):
        return {
            "operation_id": str(value.operation_id),
            "op": value.op,
            "section_id": value.section_id,
            "item": _item_payload(value.item),
            "after_item_id": value.after_item_id,
        }
    if isinstance(value, RemoveResumeEntity):
        return {
            "operation_id": str(value.operation_id),
            "op": value.op,
            "entity_kind": value.entity_kind.value,
            "entity_id": value.entity_id,
        }
    if isinstance(value, MoveResumeEntity):
        return {
            "operation_id": str(value.operation_id),
            "op": value.op,
            "entity_kind": value.entity_kind.value,
            "entity_id": value.entity_id,
            "parent_id": value.parent_id,
            "after_id": value.after_id,
        }
    if isinstance(value, SetResumeTemplate):
        return {
            "operation_id": str(value.operation_id),
            "op": value.op,
            "template": _template_ref_payload(value.template),
            "settings": cast(JsonValue, deepcopy(dict(value.settings))),
        }
    assert_never(value)


def _proposal(value: ResumeProposal) -> dict[str, JsonValue]:
    """@brief 投影契约可见 ResumeProposal / Project the contract-visible ResumeProposal.

    @param value proposal 聚合 / Proposal aggregate.
    @return ResumeProposal JSON / ResumeProposal JSON.
    """

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "resume_id": str(value.resume_id),
            "base_revision": value.base_revision,
            "title": value.title,
            "status": value.status.value,
            "operations": [_operation_payload(operation) for operation in value.operations],
            "evidence_refs": [_resource_ref(reference) for reference in value.evidence_refs],
        }
    )
    return payload


def _job(value: Job) -> dict[str, JsonValue]:
    """@brief 投影创建时可见的 queued Job / Project a newly visible queued Job.

    @param value queued Resume Job / Queued Resume job.
    @return Job JSON / Job JSON.
    """

    payload = resource_meta(value.meta)
    payload.update(
        {
            "workspace_id": str(value.workspace_id),
            "kind": value.kind,
            "subject": _resource_ref(value.subject),
            "status": value.status.value,
            "progress": None,
            "result_refs": [],
            "problem": None,
            "started_at": None,
            "finished_at": None,
        }
    )
    return payload


def _required_value(body: Mapping[str, JsonValue], field: str) -> JsonValue:
    """@brief 读取可为 null 的必需 JSON 字段 / Read a required JSON field that may be null.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return JSON value / JSON value.
    @raise RuntimeError validator 违反 required 保证时抛出 / Raised if a required field is absent.
    """

    if field not in body:
        raise RuntimeError(f"validated field {field} must be present")
    return body[field]


def _required_string(body: Mapping[str, JsonValue], field: str) -> str:
    """@brief 读取 schema 已保证的必需字符串 / Read a schema-guaranteed required string.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return 字符串值 / String value.
    @raise RuntimeError validator 违反保证时抛出 / Raised if the validator violates its guarantee.
    """

    value = body.get(field)
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a string")
    return value


def _optional_string(body: Mapping[str, JsonValue], field: str) -> str | None:
    """@brief 读取 schema 已保证的可省略字符串 / Read a schema-guaranteed optional string.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return 字符串或字段缺失 / String or absence.
    @raise RuntimeError validator 违反保证时抛出 / Raised if the validator violates its guarantee.
    """

    value = body.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a string")
    return value


def _nullable_string(body: Mapping[str, JsonValue], field: str) -> str | None:
    """@brief 读取必需 nullable 字符串 / Read a required nullable string.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return 字符串或 null / String or null.
    @raise RuntimeError validator 违反 required/type 保证时抛出 / Raised for broken guarantees.
    """

    if field not in body:
        raise RuntimeError(f"validated field {field} must be present")
    value = body[field]
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a nullable string")
    return value


def _required_integer(body: Mapping[str, JsonValue], field: str) -> int:
    """@brief 读取 schema 已保证的整数 / Read a schema-guaranteed integer.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return 整数值 / Integer value.
    @raise RuntimeError validator 违反保证时抛出 / Raised if the validator violates its guarantee.
    """

    value = body.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"validated field {field} must be an integer")
    return value


def _required_boolean(body: Mapping[str, JsonValue], field: str) -> bool:
    """@brief 读取 schema 已保证的 boolean / Read a schema-guaranteed boolean.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return boolean 值 / Boolean value.
    @raise RuntimeError validator 违反保证时抛出 / Raised if the validator violates its guarantee.
    """

    value = body.get(field)
    if not isinstance(value, bool):
        raise RuntimeError(f"validated field {field} must be a boolean")
    return value


def _required_object(
    body: Mapping[str, JsonValue], field: str
) -> dict[str, JsonValue]:
    """@brief 读取 schema 已保证的 object 字段 / Read a schema-guaranteed object field.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return JSON object / JSON object.
    @raise RuntimeError validator 违反保证时抛出 / Raised if the validator violates its guarantee.
    """

    return _object(body.get(field), field)


def _required_array(body: Mapping[str, JsonValue], field: str) -> list[JsonValue]:
    """@brief 读取 schema 已保证的 array 字段 / Read a schema-guaranteed array field.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return JSON array / JSON array.
    @raise RuntimeError validator 违反保证时抛出 / Raised if the validator violates its guarantee.
    """

    value = body.get(field)
    if not isinstance(value, list):
        raise RuntimeError(f"validated field {field} must be an array")
    return value


def _object(value: JsonValue, label: str) -> dict[str, JsonValue]:
    """@brief 收窄已校验 JSON object / Narrow a validated JSON object.

    @param value JSON value / JSON value.
    @param label 诊断标签 / Diagnostic label.
    @return JSON object / JSON object.
    @raise RuntimeError value 不是 object 时抛出 / Raised unless value is an object.
    """

    if not isinstance(value, dict):
        raise RuntimeError(f"validated {label} must be an object")
    return value


def _array_object(value: JsonValue, label: str) -> dict[str, JsonValue]:
    """@brief 收窄 array 中的 JSON object / Narrow a JSON object inside an array.

    @param value array item / Array item.
    @param label 诊断标签 / Diagnostic label.
    @return JSON object / JSON object.
    @raise RuntimeError value 不是 object 时抛出 / Raised unless value is an object.
    """

    return _object(value, label)


def _array_string(value: JsonValue, label: str) -> str:
    """@brief 收窄 array 中的字符串 / Narrow a string inside an array.

    @param value array item / Array item.
    @param label 诊断标签 / Diagnostic label.
    @return 字符串 / String.
    @raise RuntimeError value 不是字符串时抛出 / Raised unless value is a string.
    """

    if not isinstance(value, str):
        raise RuntimeError(f"validated {label} item must be a string")
    return value


def _optional_resume_id(
    body: Mapping[str, JsonValue], field: str
) -> ResumeId | None:
    """@brief 读取可省略 nullable Resume ID / Read an optional nullable Resume ID.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return ResumeId 或 None / ResumeId or None.
    @raise RuntimeError validator 违反保证时抛出 / Raised if the validator violates its guarantee.
    """

    value = body.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a nullable Resume ID")
    return ResumeId(value)


router_v2_resumes = create_v2_resume_router()
"""@brief 默认从 composition container 解析依赖的 Resume router / Default Resume router."""


__all__ = [
    "V2ResumeRuntime",
    "V2ResumeRuntimeResolver",
    "create_v2_resume_router",
    "router_v2_resumes",
    "v2_resume_runtime_from_request",
]
