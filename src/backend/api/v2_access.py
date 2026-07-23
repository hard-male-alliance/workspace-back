"""@brief API V2 用户、Workspace 与 OIDC UserInfo HTTP 适配器 / API V2 access HTTP adapter.

该模块只负责 transport 语义：严格 JSON、Bearer principal 投影、强 ETag、条件写、
opaque cursor、幂等 receipt 和契约表示。授权与状态迁移全部委托给 application 层。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from functools import wraps
from typing import Concatenate, Protocol, cast

from fastapi import APIRouter, Request
from fastapi.responses import Response

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
    keyset_page,
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
from backend.application.access import (
    AccessApplicationError,
    AccessApplicationService,
    AccessConflict,
    AccessPreconditionFailed,
    AccessResourceNotFound,
    CreateInvitationCommand,
    CreateWorkspaceCommand,
    CurrentUserResult,
    InvalidAccessCommand,
    ReauthenticationRequired,
    UpdateMemberCommand,
    UpdateUserCommand,
    UpdateWorkspaceCommand,
    WorkspaceAccessResult,
)
from backend.application.ports.access import AuthorizationDenied, UnknownPrincipal
from backend.application.ports.v2_idempotency import (
    ReplayableResponse,
    V2IdempotencyExecutor,
)
from backend.domain.common import DomainError, Problem
from backend.domain.principals import (
    DomainInvariantError,
    InvitationId,
    MembershipId,
    Scope,
    WorkspaceId,
)
from backend.domain.users import (
    AccountDeletion,
    AccountDeletionId,
    CompletedAccountDeletion,
    FailedAccountDeletion,
)
from backend.domain.workspaces import (
    DataRegion,
    Invitation,
    Membership,
    MemberStatus,
    Workspace,
    WorkspaceRole,
)

#: @brief OIDC profile scope / OIDC profile scope.
_PROFILE_SCOPE = Scope("profile")
#: @brief OIDC email scope / OIDC email scope.
_EMAIL_SCOPE = Scope("email")

#: @brief Access boundary 可稳定映射的预期异常 / Expected Access boundary errors with stable mappings.
_ACCESS_BOUNDARY_ERRORS: tuple[type[Exception], ...] = (
    AccessApplicationError,
    AuthorizationDenied,
    UnknownPrincipal,
    DomainInvariantError,
)


class V2AccessRuntime(Protocol):
    """@brief 单个请求所需的 Phase-1 运行时依赖 / Phase-1 runtime dependencies for one request."""

    @property
    def access(self) -> AccessApplicationService:
        """@brief 返回访问应用服务 / Return the access application service.

        @return Phase-1 应用服务 / Phase-1 application service.
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


type V2AccessRuntimeResolver = Callable[[Request], V2AccessRuntime]
"""@brief 从 HTTP request 解析运行时依赖 / Resolve runtime dependencies from an HTTP request."""


def v2_access_runtime_from_request(request: Request) -> V2AccessRuntime:
    """@brief 从 composition container 取得 Phase-1 依赖 / Read Phase-1 dependencies from the composition container.

    @param request 当前 HTTP request / Current HTTP request.
    @return 结构化 Phase-1 runtime / Structured Phase-1 runtime.
    @raise RuntimeError container 尚未安装时抛出 / Raised when the container is unavailable.
    """

    container = getattr(request.app.state, "container", None)
    if container is None:
        raise RuntimeError("backend container is unavailable")
    return cast(V2AccessRuntime, container)


def _translate_http_errors[AdapterT, **ParamT](
    handler: Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
) -> Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]]:
    """@brief 将预期 boundary 异常转换为 V2 ProblemDetails / Translate expected boundary errors to V2 Problem Details.

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
        except _ACCESS_BOUNDARY_ERRORS as error:
            return problem_response(request, _application_problem(error), error=error)

    return cast(
        Callable[Concatenate[AdapterT, Request, ParamT], Awaitable[Response]],
        wrapped,
    )


class V2AccessHttpAdapter:
    """@brief 把 Phase-1 应用用例适配为冻结的 API V2 路由 / Adapt Phase-1 use cases to frozen API V2 routes.

    @param resolve_runtime 按 request 解析依赖的函数 / Per-request dependency resolver.
    """

    def __init__(self, resolve_runtime: V2AccessRuntimeResolver) -> None:
        """@brief 构建并注册全部 Phase-1 路由 / Build and register every Phase-1 route.

        @param resolve_runtime 每个请求使用的 runtime resolver / Runtime resolver used per request.
        """

        self._resolve_runtime = resolve_runtime
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self) -> None:
        """@brief 注册契约中的 UserInfo 与十九条产品路由 / Register UserInfo and nineteen product routes.

        @return 无返回值 / No return value.
        """

        routes: tuple[
            tuple[str, str, Callable[..., Awaitable[Response]], str | None, str | None], ...
        ] = (
            ("GET", "/userinfo", self.get_userinfo, None, None),
            ("GET", "/api/v2/me", self.get_me, None, "CurrentUser"),
            ("PATCH", "/api/v2/me", self.patch_me, "UpdateUserRequest", "CurrentUser"),
            (
                "POST",
                "/api/v2/me/account-deletion-requests",
                self.create_account_deletion,
                "CreateAccountDeletionRequest",
                "AccountDeletionRequest",
            ),
            (
                "GET",
                "/api/v2/me/account-deletion-requests/{request_id}",
                self.get_account_deletion,
                None,
                "AccountDeletionRequest",
            ),
            (
                "POST",
                "/api/v2/me/account-deletion-requests/{request_id}/cancellations",
                self.cancel_account_deletion,
                None,
                "AccountDeletionRequest",
            ),
            ("GET", "/api/v2/workspaces", self.list_workspaces, None, "WorkspaceList"),
            (
                "POST",
                "/api/v2/workspaces",
                self.create_workspace,
                "CreateWorkspaceRequest",
                "Workspace",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}",
                self.get_workspace,
                None,
                "Workspace",
            ),
            (
                "PATCH",
                "/api/v2/workspaces/{workspace_id}",
                self.patch_workspace,
                "UpdateWorkspaceRequest",
                "Workspace",
            ),
            ("DELETE", "/api/v2/workspaces/{workspace_id}", self.delete_workspace, None, None),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/members",
                self.list_members,
                None,
                "WorkspaceMemberList",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/members/{member_id}",
                self.get_member,
                None,
                "WorkspaceMember",
            ),
            (
                "PATCH",
                "/api/v2/workspaces/{workspace_id}/members/{member_id}",
                self.patch_member,
                "UpdateWorkspaceMemberRequest",
                "WorkspaceMember",
            ),
            (
                "DELETE",
                "/api/v2/workspaces/{workspace_id}/members/{member_id}",
                self.delete_member,
                None,
                None,
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/invitations",
                self.list_invitations,
                None,
                "WorkspaceInvitationList",
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/invitations",
                self.create_invitation,
                "CreateInvitationRequest",
                "WorkspaceInvitation",
            ),
            (
                "GET",
                "/api/v2/workspaces/{workspace_id}/invitations/{invitation_id}",
                self.get_invitation,
                None,
                "WorkspaceInvitation",
            ),
            (
                "DELETE",
                "/api/v2/workspaces/{workspace_id}/invitations/{invitation_id}",
                self.delete_invitation,
                None,
                None,
            ),
            (
                "POST",
                "/api/v2/workspaces/{workspace_id}/invitations/{invitation_id}/acceptances",
                self.accept_invitation,
                None,
                "WorkspaceMember",
            ),
        )
        for method, path, endpoint, request_definition, response_definition in routes:
            extra: dict[str, JsonValue] = {"x-api-v2-phase": 1}
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
    async def get_userinfo(self, request: Request) -> Response:
        """@brief 返回 scope 收窄的标准 OIDC UserInfo / Return scope-narrowed standard OIDC UserInfo.

        @param request 已认证 request / Authenticated request.
        @return UserInfo JSON response / UserInfo JSON response.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        user = await runtime.access.get_userinfo(principal)
        payload: dict[str, JsonValue] = {"sub": str(user.subject)}
        if principal.has_scope(_PROFILE_SCOPE):
            payload.update({"name": user.display_name, "locale": user.locale})
        if principal.has_scope(_EMAIL_SCOPE):
            payload.update({"email": user.email, "email_verified": user.email_verified})
        return json_response(request, payload, cache_control="no-store")

    @_translate_http_errors
    async def get_me(self, request: Request) -> Response:
        """@brief 读取当前用户表示 / Read the current-user representation.

        @param request 已认证 request / Authenticated request.
        @return 带强 ETag 的 CurrentUser / CurrentUser with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        result = await runtime.access.get_current_user(verified_principal(request))
        payload = _current_user(result)
        runtime.contracts_v2.validate_definition("CurrentUser", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def patch_me(self, request: Request) -> Response:
        """@brief 条件修改当前用户偏好 / Conditionally update current-user preferences.

        @param request 含 Merge Patch 与 If-Match 的 request / Request with Merge Patch and If-Match.
        @return 更新后的 CurrentUser / Updated CurrentUser.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        body = await strict_json_object(
            request,
            validator=runtime.contracts_v2,
            definition="UpdateUserRequest",
        )
        current = await runtime.access.get_current_user(principal)
        expected_revision = match_etag_revision(
            if_match_header(request),
            _current_user(current),
            current.user.meta.revision,
        )
        result = await runtime.access.update_current_user(
            principal,
            UpdateUserCommand(
                display_name=_optional_string(body, "display_name"),
                locale=_optional_string(body, "locale"),
                default_workspace_id=(
                    WorkspaceId(cast(str, body["default_workspace_id"]))
                    if body.get("default_workspace_id") is not None
                    else None
                ),
                replace_default_workspace="default_workspace_id" in body,
            ),
            expected_revision=expected_revision,
        )
        payload = _current_user(result)
        runtime.contracts_v2.validate_definition("CurrentUser", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def create_account_deletion(self, request: Request) -> Response:
        """@brief 幂等创建账户删除请求 / Idempotently create an account-deletion request.

        @param request 含确认、reauth flow 与幂等键的 request / Request with confirmation, reauth flow, and idempotency key.
        @return 201 AccountDeletionRequest / 201 AccountDeletionRequest.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        body = await strict_json_object(
            request,
            validator=runtime.contracts_v2,
            definition="CreateAccountDeletionRequest",
        )

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后安排删除 / Schedule deletion after the first claim.

            @return 可重放 201 response / Replayable 201 response.
            """

            deletion = await runtime.access.request_account_deletion(
                principal,
                confirmation=_required_string(body, "confirmation"),
                reauthentication_flow_id=_required_string(body, "reauthentication_flow_id"),
            )
            payload = _account_deletion(deletion)
            runtime.contracts_v2.validate_definition("AccountDeletionRequest", payload)
            location = f"/api/v2/me/account-deletion-requests/{deletion.meta.id}"
            return replayable_json(payload, status_code=201, location=location, etag=True)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=None,
            canonical_path="/api/v2/me/account-deletion-requests",
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            operation=operation,
            mapped_error_types=_ACCESS_BOUNDARY_ERRORS,
            map_error=_application_problem,
        )

    @_translate_http_errors
    async def get_account_deletion(self, request: Request, request_id: OpaquePath) -> Response:
        """@brief 读取自己的账户删除请求 / Read one's own account-deletion request.

        @param request 已认证 request / Authenticated request.
        @param request_id 删除请求标识 / Deletion-request identifier.
        @return 带强 ETag 的 AccountDeletionRequest / AccountDeletionRequest with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        deletion = await runtime.access.get_account_deletion(
            verified_principal(request), AccountDeletionId(request_id)
        )
        payload = _account_deletion(deletion)
        runtime.contracts_v2.validate_definition("AccountDeletionRequest", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def cancel_account_deletion(self, request: Request, request_id: OpaquePath) -> Response:
        """@brief 幂等且有条件地取消账户删除 / Idempotently and conditionally cancel account deletion.

        @param request 含 If-Match 与幂等键的 request / Request with If-Match and idempotency key.
        @param request_id 删除请求标识 / Deletion-request identifier.
        @return 更新后的 AccountDeletionRequest / Updated AccountDeletionRequest.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后执行条件取消 / Execute conditional cancellation after the first claim.

            @return 可重放 200 response / Replayable 200 response.
            """

            current = await runtime.access.get_account_deletion(
                principal, AccountDeletionId(request_id)
            )
            expected_revision = match_etag_revision(
                raw_if_match,
                _account_deletion(current),
                current.meta.revision,
            )
            cancelled = await runtime.access.cancel_account_deletion(
                principal,
                AccountDeletionId(request_id),
                expected_revision=expected_revision,
            )
            payload = _account_deletion(cancelled)
            runtime.contracts_v2.validate_definition("AccountDeletionRequest", payload)
            return replayable_json(payload, status_code=200, etag=True)

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=None,
            canonical_path=(f"/api/v2/me/account-deletion-requests/{request_id}/cancellations"),
            canonical_body=b"",
            content_type=None,
            if_match=raw_if_match,
            operation=operation,
            mapped_error_types=_ACCESS_BOUNDARY_ERRORS,
            map_error=_application_problem,
        )

    @_translate_http_errors
    async def list_workspaces(
        self,
        request: Request,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出当前用户 Workspace / Page through the current user's workspaces.

        @param request 已认证 request / Authenticated request.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @return WorkspaceList / WorkspaceList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        items = await runtime.access.list_workspaces(principal)
        payload = keyset_page(
            items,
            cursor=cursor,
            limit=limit,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=None,
            collection="workspaces",
            key=lambda item: item.workspace.meta,
            project=_workspace_access,
        )
        runtime.contracts_v2.validate_definition("WorkspaceList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def create_workspace(self, request: Request) -> Response:
        """@brief 幂等创建 team Workspace / Idempotently create a team workspace.

        @param request 含创建 body 与幂等键的 request / Request with creation body and idempotency key.
        @return 201 Workspace / 201 Workspace.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        body = await strict_json_object(
            request,
            validator=runtime.contracts_v2,
            definition="CreateWorkspaceRequest",
        )

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后创建 Workspace / Create the workspace after the first claim.

            @return 可重放 201 response / Replayable 201 response.
            """

            workspace = await runtime.access.create_workspace(
                principal,
                CreateWorkspaceCommand(
                    name=_required_string(body, "name"),
                    slug=_required_string(body, "slug"),
                    data_region=DataRegion(_required_string(body, "data_region")),
                ),
            )
            payload = _workspace(workspace)
            runtime.contracts_v2.validate_definition("Workspace", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=f"/api/v2/workspaces/{workspace.meta.id}",
                etag=True,
            )

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=None,
            canonical_path="/api/v2/workspaces",
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            operation=operation,
            mapped_error_types=_ACCESS_BOUNDARY_ERRORS,
            map_error=_application_problem,
        )

    @_translate_http_errors
    async def get_workspace(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief 读取路径 Workspace / Read the path-selected workspace.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace 标识 / Path workspace identifier.
        @return 带强 ETag 的 Workspace / Workspace with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        workspace = await runtime.access.get_workspace(
            verified_principal(request), WorkspaceId(workspace_id)
        )
        payload = _workspace(workspace)
        runtime.contracts_v2.validate_definition("Workspace", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def patch_workspace(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief 条件修改路径 Workspace / Conditionally update the path-selected workspace.

        @param request 含 Merge Patch 与 If-Match 的 request / Request with Merge Patch and If-Match.
        @param workspace_id 路径 Workspace 标识 / Path workspace identifier.
        @return 更新后的 Workspace / Updated Workspace.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        body = await strict_json_object(
            request,
            validator=runtime.contracts_v2,
            definition="UpdateWorkspaceRequest",
        )
        current = await runtime.access.get_workspace(principal, typed_workspace_id)
        expected_revision = match_etag_revision(
            if_match_header(request),
            _workspace(current),
            current.meta.revision,
        )
        workspace = await runtime.access.update_workspace(
            principal,
            typed_workspace_id,
            UpdateWorkspaceCommand(
                name=_optional_string(body, "name"),
                slug=_optional_string(body, "slug"),
            ),
            expected_revision=expected_revision,
        )
        payload = _workspace(workspace)
        runtime.contracts_v2.validate_definition("Workspace", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def delete_workspace(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief 条件删除完整 Workspace / Conditionally delete an entire workspace.

        @param request 含 If-Match 的 request / Request with If-Match.
        @param workspace_id 路径 Workspace 标识 / Path workspace identifier.
        @return 204 空 response / Empty 204 response.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        current = await runtime.access.get_workspace(principal, typed_workspace_id)
        expected_revision = match_etag_revision(
            if_match_header(request),
            _workspace(current),
            current.meta.revision,
        )
        await runtime.access.delete_workspace(
            principal, typed_workspace_id, expected_revision=expected_revision
        )
        return empty_response(request)

    @_translate_http_errors
    async def list_members(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Workspace 成员 / Page through workspace members.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @return WorkspaceMemberList / WorkspaceMemberList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        items = await runtime.access.list_members(principal, typed_workspace_id)
        payload = keyset_page(
            items,
            cursor=cursor,
            limit=limit,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace_id,
            collection="members",
            key=lambda item: item.meta,
            project=_member,
        )
        runtime.contracts_v2.validate_definition("WorkspaceMemberList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def get_member(
        self,
        request: Request,
        workspace_id: OpaquePath,
        member_id: OpaquePath,
    ) -> Response:
        """@brief 在 Workspace 内读取成员 / Read a member within a workspace.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param member_id 成员标识 / Membership identifier.
        @return 带强 ETag 的 WorkspaceMember / WorkspaceMember with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        member = await runtime.access.get_member(
            verified_principal(request), WorkspaceId(workspace_id), MembershipId(member_id)
        )
        payload = _member(member)
        runtime.contracts_v2.validate_definition("WorkspaceMember", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def patch_member(
        self,
        request: Request,
        workspace_id: OpaquePath,
        member_id: OpaquePath,
    ) -> Response:
        """@brief 条件修改 Workspace 成员 / Conditionally update a workspace member.

        @param request 含 Merge Patch 与 If-Match 的 request / Request with Merge Patch and If-Match.
        @param workspace_id 路径 Workspace / Path workspace.
        @param member_id 成员标识 / Membership identifier.
        @return 更新后的 WorkspaceMember / Updated WorkspaceMember.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_member_id = MembershipId(member_id)
        body = await strict_json_object(
            request,
            validator=runtime.contracts_v2,
            definition="UpdateWorkspaceMemberRequest",
        )
        current = await runtime.access.get_member(principal, typed_workspace_id, typed_member_id)
        expected_revision = match_etag_revision(
            if_match_header(request),
            _member(current),
            current.meta.revision,
        )
        member = await runtime.access.update_member(
            principal,
            typed_workspace_id,
            typed_member_id,
            UpdateMemberCommand(
                role=(WorkspaceRole(_required_string(body, "role")) if "role" in body else None),
                status=(
                    MemberStatus(_required_string(body, "status")) if "status" in body else None
                ),
            ),
            expected_revision=expected_revision,
        )
        payload = _member(member)
        runtime.contracts_v2.validate_definition("WorkspaceMember", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def delete_member(
        self,
        request: Request,
        workspace_id: OpaquePath,
        member_id: OpaquePath,
    ) -> Response:
        """@brief 条件移除 Workspace 成员 / Conditionally remove a workspace member.

        @param request 含 If-Match 的 request / Request with If-Match.
        @param workspace_id 路径 Workspace / Path workspace.
        @param member_id 成员标识 / Membership identifier.
        @return 204 空 response / Empty 204 response.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_member_id = MembershipId(member_id)
        current = await runtime.access.get_member(principal, typed_workspace_id, typed_member_id)
        expected_revision = match_etag_revision(
            if_match_header(request),
            _member(current),
            current.meta.revision,
        )
        await runtime.access.remove_member(
            principal,
            typed_workspace_id,
            typed_member_id,
            expected_revision=expected_revision,
        )
        return empty_response(request)

    @_translate_http_errors
    async def list_invitations(
        self,
        request: Request,
        workspace_id: OpaquePath,
        cursor: PageCursor = None,
        limit: PageLimit = DEFAULT_PAGE_LIMIT,
    ) -> Response:
        """@brief 分页列出 Workspace 邀请 / Page through workspace invitations.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param cursor 可选 opaque cursor / Optional opaque cursor.
        @param limit 页长 / Page size.
        @return WorkspaceInvitationList / WorkspaceInvitationList.
        """

        require_query(request, "cursor", "limit")
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        items = await runtime.access.list_invitations(principal, typed_workspace_id)
        payload = keyset_page(
            items,
            cursor=cursor,
            limit=limit,
            codec=runtime.v2_cursor,
            principal=principal,
            workspace_id=typed_workspace_id,
            collection="invitations",
            key=lambda item: item.meta,
            project=_invitation,
        )
        runtime.contracts_v2.validate_definition("WorkspaceInvitationList", payload)
        return json_response(request, payload)

    @_translate_http_errors
    async def create_invitation(self, request: Request, workspace_id: OpaquePath) -> Response:
        """@brief 幂等创建收件人绑定邀请 / Idempotently create a recipient-bound invitation.

        @param request 含创建 body 与幂等键的 request / Request with creation body and idempotency key.
        @param workspace_id 路径 Workspace / Path workspace.
        @return 201 WorkspaceInvitation / 201 WorkspaceInvitation.
        """

        require_query(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        body = await strict_json_object(
            request,
            validator=runtime.contracts_v2,
            definition="CreateInvitationRequest",
        )

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后创建邀请 / Create the invitation after the first claim.

            @return 可重放 201 response / Replayable 201 response.
            """

            invitation = await runtime.access.create_invitation(
                principal,
                typed_workspace_id,
                CreateInvitationCommand(
                    email=_required_string(body, "email"),
                    role=WorkspaceRole(_required_string(body, "role")),
                ),
            )
            payload = _invitation(invitation)
            runtime.contracts_v2.validate_definition("WorkspaceInvitation", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=(f"/api/v2/workspaces/{workspace_id}/invitations/{invitation.meta.id}"),
                etag=True,
            )

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=f"/api/v2/workspaces/{workspace_id}/invitations",
            canonical_body=canonical_json_bytes(body),
            content_type=JSON_MEDIA_TYPE,
            if_match=None,
            operation=operation,
            mapped_error_types=_ACCESS_BOUNDARY_ERRORS,
            map_error=_application_problem,
        )

    @_translate_http_errors
    async def get_invitation(
        self,
        request: Request,
        workspace_id: OpaquePath,
        invitation_id: OpaquePath,
    ) -> Response:
        """@brief 管理员或精确收件人读取邀请 / Let an administrator or exact recipient read an invitation.

        @param request 已认证 request / Authenticated request.
        @param workspace_id 路径 Workspace / Path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @return 带强 ETag 的 WorkspaceInvitation / WorkspaceInvitation with a strong ETag.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_invitation_id = InvitationId(invitation_id)
        try:
            invitation = await runtime.access.get_invitation(
                principal, typed_workspace_id, typed_invitation_id
            )
        except AuthorizationDenied:
            invitation = await runtime.access.get_invitation_for_acceptance(
                principal, typed_workspace_id, typed_invitation_id
            )
        payload = _invitation(invitation)
        runtime.contracts_v2.validate_definition("WorkspaceInvitation", payload)
        return resource_response(request, payload)

    @_translate_http_errors
    async def delete_invitation(
        self,
        request: Request,
        workspace_id: OpaquePath,
        invitation_id: OpaquePath,
    ) -> Response:
        """@brief 条件撤销 Workspace 邀请 / Conditionally revoke a workspace invitation.

        @param request 含 If-Match 的 request / Request with If-Match.
        @param workspace_id 路径 Workspace / Path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @return 204 空 response / Empty 204 response.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_invitation_id = InvitationId(invitation_id)
        current = await runtime.access.get_invitation(
            principal, typed_workspace_id, typed_invitation_id
        )
        expected_revision = match_etag_revision(
            if_match_header(request),
            _invitation(current),
            current.meta.revision,
        )
        await runtime.access.revoke_invitation(
            principal,
            typed_workspace_id,
            typed_invitation_id,
            expected_revision=expected_revision,
        )
        return empty_response(request)

    @_translate_http_errors
    async def accept_invitation(
        self,
        request: Request,
        workspace_id: OpaquePath,
        invitation_id: OpaquePath,
    ) -> Response:
        """@brief 收件人幂等且有条件地接受邀请 / Let the recipient idempotently and conditionally accept an invitation.

        @param request 含 If-Match 与幂等键的 request / Request with If-Match and idempotency key.
        @param workspace_id 路径 Workspace / Path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @return 201 新 WorkspaceMember / 201 newly created WorkspaceMember.
        """

        require_query(request)
        await require_no_body(request)
        runtime = self._resolve_runtime(request)
        principal = verified_principal(request)
        typed_workspace_id = WorkspaceId(workspace_id)
        typed_invitation_id = InvitationId(invitation_id)
        raw_if_match = if_match_header(request)

        async def operation() -> ReplayableResponse:
            """@brief 首次 claim 后校验并接受邀请 / Validate and accept the invitation after the first claim.

            @return 可重放 201 response / Replayable 201 response.
            """

            invitation = await runtime.access.get_invitation_for_acceptance(
                principal, typed_workspace_id, typed_invitation_id
            )
            expected_revision = match_etag_revision(
                raw_if_match,
                _invitation(invitation),
                invitation.meta.revision,
            )
            member = await runtime.access.accept_invitation(
                principal,
                typed_workspace_id,
                typed_invitation_id,
                expected_revision=expected_revision,
            )
            payload = _member(member)
            runtime.contracts_v2.validate_definition("WorkspaceMember", payload)
            return replayable_json(
                payload,
                status_code=201,
                location=f"/api/v2/workspaces/{workspace_id}/members/{member.meta.id}",
                etag=True,
            )

        return await idempotent_response(
            request,
            executor=runtime.v2_idempotency,
            principal=principal,
            workspace_id=typed_workspace_id,
            canonical_path=(
                f"/api/v2/workspaces/{workspace_id}/invitations/{invitation_id}/acceptances"
            ),
            canonical_body=b"",
            content_type=None,
            if_match=raw_if_match,
            operation=operation,
            mapped_error_types=_ACCESS_BOUNDARY_ERRORS,
            map_error=_application_problem,
        )


def create_v2_access_router(
    resolve_runtime: V2AccessRuntimeResolver = v2_access_runtime_from_request,
) -> APIRouter:
    """@brief 创建完整 Phase-1 router / Create the complete Phase-1 router.

    @param resolve_runtime 每个请求的依赖 resolver / Per-request dependency resolver.
    @return 可挂载的 FastAPI router / Mountable FastAPI router.
    """

    return V2AccessHttpAdapter(resolve_runtime).router


def _declared_status(method: str, path: str) -> int:
    """@brief 返回 OpenAPI 的成功状态 / Return the declared OpenAPI success status.

    @param method HTTP method / HTTP method.
    @param path canonical route pattern / Canonical route pattern.
    @return 200、201 或 204 / 200, 201, or 204.
    """

    if method == "DELETE":
        return 204
    if method == "POST" and path in {
        "/api/v2/me/account-deletion-requests",
        "/api/v2/workspaces",
        "/api/v2/workspaces/{workspace_id}/invitations",
        "/api/v2/workspaces/{workspace_id}/invitations/{invitation_id}/acceptances",
    }:
        return 201
    return 200


def _required_string(body: Mapping[str, JsonValue], field: str) -> str:
    """@brief 读取 schema 已保证的必需字符串 / Read a schema-guaranteed required string.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return 字符串值 / String value.
    @raise RuntimeError validator 违反其保证时抛出 / Raised if the validator violates its guarantee.
    """

    value = body.get(field)
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a string")
    return value


def _optional_string(body: Mapping[str, JsonValue], field: str) -> str | None:
    """@brief 读取 schema 已保证的可省略字符串 / Read a schema-guaranteed optional string.

    @param body 已校验 object / Validated object.
    @param field 字段名 / Field name.
    @return 字符串或字段缺失 / String or absent value.
    @raise RuntimeError validator 违反其保证时抛出 / Raised if the validator violates its guarantee.
    """

    value = body.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"validated field {field} must be a string")
    return value


def _application_problem(error: BaseException) -> Problem:
    """@brief 将应用/领域失败稳定映射到 HTTP Problem / Stably map application/domain failures to HTTP Problems.

    @param error 预期失败 / Expected failure.
    @return transport problem / Transport problem.
    """

    if isinstance(error, AccessResourceNotFound):
        return Problem(error.code, 404, "Resource was not found", detail=error.detail)
    if isinstance(error, AccessPreconditionFailed):
        return Problem(error.code, 412, "Resource precondition failed", detail=error.detail)
    if isinstance(error, AccessConflict):
        return Problem(error.code, 409, "Resource state conflict", detail=error.detail)
    if isinstance(error, InvalidAccessCommand):
        return Problem(error.code, 422, "Command violates a domain constraint", detail=error.detail)
    if isinstance(error, ReauthenticationRequired):
        return Problem(
            "identity.reauthentication_required",
            403,
            "Recent reauthentication is required",
        )
    if isinstance(error, UnknownPrincipal):
        return Problem("oauth.invalid_token", 401, "Bearer token is invalid")
    if isinstance(error, AuthorizationDenied):
        if error.reason in {
            "authorization.membership_missing",
            "authorization.membership_inactive",
            "authorization.invitation_recipient_mismatch",
        }:
            return Problem("resource.not_found", 404, "Resource was not found")
        if error.reason == "authorization.scope_missing":
            return Problem("oauth.insufficient_scope", 403, "Token scope is insufficient")
        return Problem("authorization.denied", 403, "Action is not permitted")
    if isinstance(error, DomainInvariantError):
        return Problem("resource.state_conflict", 409, "Resource state conflict", detail=str(error))
    if isinstance(error, AccessApplicationError):
        return Problem(error.code, 409, "Application command failed", detail=error.detail)
    raise TypeError(f"unsupported application error: {type(error).__name__}")


def _current_user(result: CurrentUserResult) -> dict[str, JsonValue]:
    """@brief 投影 CurrentUser / Project CurrentUser.

    @param result 用户与 token scopes / User and token scopes.
    @return CurrentUser JSON / CurrentUser JSON.
    """

    payload = resource_meta(result.user.meta)
    payload.update(
        {
            "subject": str(result.user.subject),
            "email": result.user.email,
            "email_verified": result.user.email_verified,
            "display_name": result.user.display_name,
            "locale": result.user.locale,
            "default_workspace_id": (
                str(result.user.default_workspace_id)
                if result.user.default_workspace_id is not None
                else None
            ),
            "scopes": [cast(JsonValue, str(scope)) for scope in sorted(result.scopes)],
        }
    )
    return payload


def _workspace(workspace: Workspace) -> dict[str, JsonValue]:
    """@brief 投影 Workspace / Project Workspace.

    @param workspace Workspace 聚合 / Workspace aggregate.
    @return Workspace JSON / Workspace JSON.
    """

    payload = resource_meta(workspace.meta)
    payload.update(
        {
            "name": workspace.name,
            "slug": workspace.slug,
            "plan": workspace.plan.value,
            "data_region": workspace.data_region.value,
        }
    )
    return payload


def _workspace_access(item: WorkspaceAccessResult) -> dict[str, JsonValue]:
    """@brief 投影 WorkspaceAccess 列表项 / Project a WorkspaceAccess list item.

    @param item Workspace 与调用者成员关系 / Workspace and caller membership.
    @return WorkspaceAccess JSON / WorkspaceAccess JSON.
    """

    return {
        "workspace": _workspace(item.workspace),
        "member_id": str(item.membership.meta.id),
        "role": item.membership.role.value,
    }


def _member(member: Membership) -> dict[str, JsonValue]:
    """@brief 投影 WorkspaceMember / Project WorkspaceMember.

    @param member 成员关系 / Membership.
    @return WorkspaceMember JSON / WorkspaceMember JSON.
    """

    payload = resource_meta(member.meta)
    payload.update(
        {
            "workspace_id": str(member.workspace_id),
            "user_id": str(member.user_id),
            "display_name": member.display_name,
            "role": member.role.value,
            "status": member.status.value,
        }
    )
    return payload


def _invitation(invitation: Invitation) -> dict[str, JsonValue]:
    """@brief 投影不泄漏完整邮箱的 WorkspaceInvitation / Project WorkspaceInvitation without leaking the full email.

    @param invitation 邀请聚合 / Invitation aggregate.
    @return WorkspaceInvitation JSON / WorkspaceInvitation JSON.
    """

    payload = resource_meta(invitation.meta)
    payload.update(
        {
            "workspace_id": str(invitation.workspace_id),
            "email_hint": _email_hint(invitation.email),
            "role": invitation.role.value,
            "status": invitation.status.value,
            "expires_at": timestamp(invitation.expires_at),
        }
    )
    return payload


def _email_hint(email: str) -> str:
    """@brief 生成最小可识别邮箱提示 / Generate a minimally identifying email hint.

    @param email 完整规范邮箱 / Full canonical email.
    @return 遮罩后的提示 / Masked hint.
    """

    local, separator, domain = email.partition("@")
    if separator != "@" or not local or not domain:
        return "***"
    return f"{local[0]}***@{domain}"


def _account_deletion(deletion: AccountDeletion) -> dict[str, JsonValue]:
    """@brief 投影状态关联的 AccountDeletionRequest / Project a state-correlated AccountDeletionRequest.

    @param deletion 账户删除联合 / Account-deletion union.
    @return AccountDeletionRequest JSON / AccountDeletionRequest JSON.
    """

    payload = resource_meta(deletion.meta)
    completed_at = (
        timestamp(deletion.completed_at) if isinstance(deletion, CompletedAccountDeletion) else None
    )
    problem: JsonValue = None
    if isinstance(deletion, FailedAccountDeletion):
        failure = deletion.failure
        problem = {
            "type": ("https://api.hmalliances.org:8022/problems/" + failure.code.replace(".", "/")),
            "title": "Account deletion failed",
            "status": 500,
            "detail": failure.detail,
            "instance": f"/api/v2/me/account-deletion-requests/{deletion.meta.id}",
            "code": failure.code,
            "request_id": str(deletion.meta.id),
            "retryable": False,
            "errors": [],
            "extensions": {},
        }
    payload.update(
        {
            "status": deletion.status.value,
            "scheduled_for": timestamp(deletion.scheduled_for),
            "completed_at": completed_at,
            "problem": problem,
        }
    )
    return payload


router_v2_access = create_v2_access_router()
"""@brief 默认从 composition container 解析依赖的 Phase-1 router / Default Phase-1 router resolved from the composition container."""


__all__ = [
    "V2AccessRuntime",
    "V2AccessRuntimeResolver",
    "create_v2_access_router",
    "router_v2_access",
    "v2_access_runtime_from_request",
]
