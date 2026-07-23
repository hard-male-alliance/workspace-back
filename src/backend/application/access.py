"""@brief API v2 用户与 Workspace 应用用例 / API v2 user and workspace application use cases."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from backend.application.ports.access import (
    AccessAuthorizer,
    AccessRepository,
    AccessUnitOfWorkFactory,
    AuthorizationDenied,
    Clock,
)
from backend.domain.principals import (
    AuthenticatedActor,
    InvitationId,
    MembershipId,
    ResourceMeta,
    Scope,
    TokenPrincipal,
    UserId,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.users import (
    AccountDeletion,
    AccountDeletionId,
    AccountStatus,
    ScheduledAccountDeletion,
    User,
)
from backend.domain.workspaces import (
    DataRegion,
    InvalidStateTransition,
    Invitation,
    InvitationStatus,
    Membership,
    MemberStatus,
    Workspace,
    WorkspacePlan,
    WorkspaceRole,
)
from workspace_shared.ids import new_opaque_id

OPENID_SCOPE = Scope("openid")
"""@brief 标准 OIDC UserInfo 所需 scope / Scope required by standard OIDC UserInfo."""

PROFILE_SCOPE = Scope("profile")
"""@brief 读取与修改自身资料所需 scope / Scope required for one's own profile."""

WORKSPACE_READ_SCOPE = Scope("workspace.read")
"""@brief 枚举 Workspace 及接受邀请所需 scope / Scope for listing workspaces and accepting invitations."""

WORKSPACE_WRITE_SCOPE = Scope("workspace.write")
"""@brief 创建 Workspace 所需 scope / Scope required to create a workspace."""


class AccessApplicationError(Exception):
    """@brief 可稳定映射到 API problem 的应用错误 / Application error mappable to an API problem.

    @param code 与 transport 无关的稳定错误码 / Stable transport-independent error code.
    @param detail 可公开且不泄漏租户资源的说明 / Safe detail that does not disclose tenant resources.
    """

    code: str
    """@brief 稳定应用错误码 / Stable application error code."""

    detail: str
    """@brief 可公开错误说明 / Safe public error detail."""

    def __init__(self, code: str, detail: str) -> None:
        """@brief 初始化结构化应用错误 / Initialize a structured application error.

        @param code 稳定错误码 / Stable error code.
        @param detail 可公开说明 / Safe public detail.
        """
        super().__init__(detail)
        self.code = code
        self.detail = detail


class AccessResourceNotFound(AccessApplicationError):
    """@brief 资源不存在或不可向调用方暴露 / Resource is absent or must not be disclosed."""

    def __init__(self, resource: str) -> None:
        """@brief 创建不泄漏标识的缺失错误 / Create a non-disclosing missing-resource error.

        @param resource 稳定资源类型名 / Stable resource-kind name.
        """
        super().__init__(f"{resource}.not_found", f"{resource} was not found")


class AccessConflict(AccessApplicationError):
    """@brief 当前领域状态拒绝命令 / Current domain state rejects the command."""


class AccessPreconditionFailed(AccessApplicationError):
    """@brief 强 ETag 所代表的资源版本已过期 / Resource revision represented by a strong ETag is stale."""

    def __init__(self) -> None:
        """@brief 创建统一的 412 应用结果 / Create the uniform application result for HTTP 412."""
        super().__init__("http.precondition_failed", "resource revision precondition failed")


class InvalidAccessCommand(AccessApplicationError):
    """@brief 应用边界收到无意义或不完整命令 / Application boundary received an invalid command."""


class ReauthenticationRequired(AuthorizationDenied):
    """@brief 敏感命令缺少近期重新认证 / Sensitive command lacks recent reauthentication."""

    def __init__(self) -> None:
        """@brief 创建稳定的重新认证拒绝 / Create the stable reauthentication denial."""
        super().__init__("identity.reauthentication_required")


class IdFactory(Protocol):
    """@brief 可注入的不透明标识工厂 / Injectable opaque-identifier factory."""

    def __call__(self, prefix: str) -> str:
        """@brief 生成带领域前缀的标识 / Generate an identifier with a domain prefix.

        @param prefix 稳定领域前缀 / Stable domain prefix.
        @return 新不透明标识 / New opaque identifier.
        """


class ReauthenticationVerifier(Protocol):
    """@brief 验证近期重新认证证明的安全端口 / Security port for recent-reauthentication proof."""

    async def verify_recent(
        self,
        user_id: UserId,
        flow_id: str,
        verified_at: datetime,
    ) -> bool:
        """@brief 验证 flow 与用户绑定且仍在近期窗口 / Verify user binding and recency.

        @param user_id 已认证本地用户 / Authenticated local user.
        @param flow_id 客户端提交的 reauthenticate flow / Submitted reauthenticate flow.
        @param verified_at 应用判定时刻 / Application decision instant.
        @return 证明有效时为真 / True only for a valid proof.
        """


class UtcClock:
    """@brief 使用 UTC 的生产时钟 / Production clock using UTC."""

    def now(self) -> datetime:
        """@brief 返回带时区 UTC 当前时刻 / Return the timezone-aware current UTC instant.

        @return UTC 当前时刻 / Current UTC instant.
        """
        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class CurrentUserResult:
    """@brief 当前用户及本次 token 的实际 scopes / Current user and scopes of this token.

    @param user 本地用户聚合 / Local user aggregate.
    @param scopes 规范排序且去重的 token scopes / Canonically sorted, unique token scopes.
    """

    user: User
    scopes: tuple[Scope, ...]


@dataclass(frozen=True, slots=True)
class WorkspaceAccessResult:
    """@brief Workspace 与调用者成员关系的列表项 / Workspace plus caller membership list item.

    @param workspace Workspace 聚合 / Workspace aggregate.
    @param membership 调用者的活动成员关系 / Caller's active membership.
    """

    workspace: Workspace
    membership: Membership


@dataclass(frozen=True, slots=True)
class UpdateUserCommand:
    """@brief `/me` merge-patch 的显式应用表示 / Explicit application form of the `/me` merge patch.

    @param display_name 可选目标显示名 / Optional target display name.
    @param locale 可选目标 locale / Optional target locale.
    @param default_workspace_id 可为空的目标默认 Workspace / Nullable target default workspace.
    @param replace_default_workspace body 是否出现该字段 / Whether the field appeared in the body.
    """

    display_name: str | None = None
    locale: str | None = None
    default_workspace_id: WorkspaceId | None = None
    replace_default_workspace: bool = False

    def __post_init__(self) -> None:
        """@brief 拒绝空 merge-patch / Reject an empty merge patch.

        @raise InvalidAccessCommand 命令没有任何字段时抛出 / Raised when no field is present.
        """
        if self.display_name is None and self.locale is None and not self.replace_default_workspace:
            raise InvalidAccessCommand("user.patch_empty", "user patch must contain a field")


@dataclass(frozen=True, slots=True)
class CreateWorkspaceCommand:
    """@brief 创建 team Workspace 的应用命令 / Application command creating a team workspace.

    @param name 显示名 / Display name.
    @param slug URL 友好名称 / URL-friendly name.
    @param data_region 数据驻留区域 / Data-residency region.
    """

    name: str
    slug: str
    data_region: DataRegion


@dataclass(frozen=True, slots=True)
class UpdateWorkspaceCommand:
    """@brief Workspace merge-patch 命令 / Workspace merge-patch command.

    @param name 可选目标名称 / Optional target name.
    @param slug 可选目标 slug / Optional target slug.
    """

    name: str | None = None
    slug: str | None = None

    def __post_init__(self) -> None:
        """@brief 拒绝空 Workspace patch / Reject an empty workspace patch.

        @raise InvalidAccessCommand 两字段均缺失时抛出 / Raised when both fields are absent.
        """
        if self.name is None and self.slug is None:
            raise InvalidAccessCommand(
                "workspace.patch_empty", "workspace patch must contain a field"
            )


@dataclass(frozen=True, slots=True)
class UpdateMemberCommand:
    """@brief 成员 merge-patch 命令 / Membership merge-patch command.

    @param role 可选的非 owner 目标角色 / Optional non-owner target role.
    @param status 可选目标状态 / Optional target status.
    """

    role: WorkspaceRole | None = None
    status: MemberStatus | None = None

    def __post_init__(self) -> None:
        """@brief 拒绝空 patch 与普通 owner 授予 / Reject empty patches and ordinary owner grants.

        @raise InvalidAccessCommand 命令不合法时抛出 / Raised for an invalid command.
        """
        if self.role is None and self.status is None:
            raise InvalidAccessCommand("membership.patch_empty", "member patch must contain a field")
        if self.role is WorkspaceRole.OWNER:
            raise InvalidAccessCommand(
                "membership.owner_grant_forbidden",
                "ordinary member patch cannot grant the owner role",
            )


@dataclass(frozen=True, slots=True)
class CreateInvitationCommand:
    """@brief 创建收件人绑定邀请的命令 / Command creating a recipient-bound invitation.

    @param email 完整收件邮箱；仅 adapter 对外投影 hint / Full recipient email; adapters expose a hint.
    @param role 接受后授予的非 owner 角色 / Non-owner role granted on acceptance.
    """

    email: str
    role: WorkspaceRole

    def __post_init__(self) -> None:
        """@brief 拒绝 owner 邀请 / Reject owner invitations.

        @raise InvalidAccessCommand owner 只能通过专用所有权转移流程产生 / Raised because owner
            can only be created by a dedicated ownership-transfer flow.
        """
        if self.role is WorkspaceRole.OWNER:
            raise InvalidAccessCommand(
                "invitation.owner_grant_forbidden", "invitations cannot grant the owner role"
            )


class AccessApplicationService:
    """@brief Phase 1 的事务脚本与领域聚合协调器 / Phase 1 transaction and aggregate coordinator.

    @param uow_factory 每次调用生成独立工作单元 / Factory yielding one unit of work per call.
    @param reauthentication 敏感操作重新认证验证器 / Reauthentication verifier for sensitive work.
    @param clock 可替换时钟 / Replaceable clock.
    @param id_factory 可替换标识工厂 / Replaceable identifier factory.
    @param deletion_cooling_period 账户删除冷静期 / Account-deletion cooling-off period.
    @param invitation_lifetime 邀请有效期 / Invitation lifetime.
    """

    def __init__(
        self,
        uow_factory: AccessUnitOfWorkFactory,
        reauthentication: ReauthenticationVerifier,
        *,
        clock: Clock | None = None,
        id_factory: IdFactory | Callable[[str], str] = new_opaque_id,
        deletion_cooling_period: timedelta = timedelta(days=7),
        invitation_lifetime: timedelta = timedelta(days=7),
    ) -> None:
        """@brief 组装 fail-closed Phase 1 服务 / Assemble the fail-closed Phase 1 service.

        @param uow_factory 工作单元工厂 / Unit-of-work factory.
        @param reauthentication 近期重新认证验证器 / Recent-reauthentication verifier.
        @param clock 可选时钟 / Optional clock.
        @param id_factory 标识工厂 / Identifier factory.
        @param deletion_cooling_period 账户删除冷静期 / Account-deletion cooling period.
        @param invitation_lifetime 邀请有效期 / Invitation lifetime.
        @raise ValueError 时间窗口非正数时抛出 / Raised for non-positive time windows.
        """
        if deletion_cooling_period <= timedelta(0):
            raise ValueError("account-deletion cooling period must be positive")
        if invitation_lifetime <= timedelta(0):
            raise ValueError("invitation lifetime must be positive")
        self._uow_factory = uow_factory
        self._reauthentication = reauthentication
        self._clock = clock or UtcClock()
        self._id_factory = id_factory
        self._deletion_cooling_period = deletion_cooling_period
        self._invitation_lifetime = invitation_lifetime

    async def get_current_user(self, principal: TokenPrincipal) -> CurrentUserResult:
        """@brief 读取当前用户且保留 token scope 投影 / Read current user with token scopes.

        @param principal 已验证 token principal / Verified token principal.
        @return 当前用户结果 / Current-user result.
        """
        async with self._uow_factory() as uow:
            actor = await self._authenticate(uow.repository, principal, PROFILE_SCOPE)
            user = await self._require_user(uow.repository, actor.user_id)
            return self._current_user_result(user, actor)

    async def get_userinfo(self, principal: TokenPrincipal) -> User:
        """@brief 读取标准 UserInfo 的本地用户来源 / Read the local-user source for standard UserInfo.

        @param principal 已验证 token principal / Verified token principal.
        @return 与签名 uid、subject 交叉匹配的用户 / User cross-checked against signed uid and subject.
        @note 是否投影 name/locale 由 HTTP/OIDC adapter 根据 profile scope 决定。
            / The HTTP/OIDC adapter decides name/locale projection from the profile scope.
        """
        async with self._uow_factory() as uow:
            actor = await self._authenticate(uow.repository, principal, OPENID_SCOPE)
            return await self._require_user(uow.repository, actor.user_id)

    async def update_current_user(
        self,
        principal: TokenPrincipal,
        command: UpdateUserCommand,
        *,
        expected_revision: int,
    ) -> CurrentUserResult:
        """@brief 修改自身资料并验证默认 Workspace 可访问 / Update profile and validate default access.

        @param principal 已验证 token principal / Verified token principal.
        @param command 显式 merge-patch 命令 / Explicit merge-patch command.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        @return 已提交的用户结果 / Committed current-user result.
        @raise InvalidAccessCommand 默认 Workspace 不是活动成员关系时抛出 / Raised when the
            default workspace is not backed by an active membership.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            actor = await self._authenticate(repository, principal, PROFILE_SCOPE)
            user = await self._require_user(repository, actor.user_id)
            self._require_revision(user.meta.revision, expected_revision)
            default_workspace_id = user.default_workspace_id
            if command.replace_default_workspace:
                default_workspace_id = command.default_workspace_id
            if default_workspace_id is not None:
                membership = await repository.get_membership(
                    default_workspace_id, actor.user_id
                )
                if membership is None or membership.status is not MemberStatus.ACTIVE:
                    raise InvalidAccessCommand(
                        "user.default_workspace_inaccessible",
                        "default workspace requires an active membership",
                    )
            updated = user.revise_profile(
                display_name=(
                    user.display_name
                    if command.display_name is None
                    else command.display_name
                ),
                locale=user.locale if command.locale is None else command.locale,
                default_workspace_id=default_workspace_id,
                updated_at=self._clock.now(),
            )
            await repository.save_user(updated)
            await uow.commit()
            return self._current_user_result(updated, actor)

    async def request_account_deletion(
        self,
        principal: TokenPrincipal,
        *,
        confirmation: str,
        reauthentication_flow_id: str,
    ) -> AccountDeletion:
        """@brief 经近期重新认证后安排账户删除 / Schedule deletion after recent reauthentication.

        @param principal 已验证 token principal / Verified token principal.
        @param confirmation 明示确认短语 / Explicit confirmation phrase.
        @param reauthentication_flow_id 已完成 reauthenticate flow / Completed reauthenticate flow.
        @return 已提交的 scheduled 请求 / Committed scheduled request.
        @raise InvalidAccessCommand 确认短语不精确时抛出 / Raised for an inexact confirmation.
        @raise ReauthenticationRequired flow 无效、过期或属于他人时抛出 / Raised for an invalid,
            stale, or differently-owned flow.
        """
        if confirmation != "delete_my_account":
            raise InvalidAccessCommand(
                "account_deletion.confirmation_invalid",
                "account deletion confirmation is invalid",
            )
        async with self._uow_factory() as uow:
            repository = uow.repository
            actor = await self._authenticate(repository, principal, PROFILE_SCOPE)
            user = await self._require_user(repository, actor.user_id)
            if user.account_status is not AccountStatus.ACTIVE:
                raise AccessConflict(
                    "account_deletion.already_scheduled",
                    "account deletion is already scheduled",
                )
            now = self._clock.now()
            if not await self._reauthentication.verify_recent(
                actor.user_id, reauthentication_flow_id, now
            ):
                raise ReauthenticationRequired
            request = ScheduledAccountDeletion(
                ResourceMeta(
                    AccountDeletionId(self._id_factory("delreq")),
                    1,
                    now,
                    now,
                ),
                actor.user_id,
                now + self._deletion_cooling_period,
            )
            scheduled_user = user.schedule_deletion(now)
            await repository.add_account_deletion(request)
            await repository.save_user(scheduled_user)
            await uow.commit()
            return request

    async def get_account_deletion(
        self,
        principal: TokenPrincipal,
        request_id: AccountDeletionId,
    ) -> AccountDeletion:
        """@brief 读取自己的账户删除请求 / Read one's own account-deletion request.

        @param principal 已验证 token principal / Verified token principal.
        @param request_id 删除请求标识 / Deletion-request identifier.
        @return 删除请求 / Deletion request.
        @raise AccessResourceNotFound 请求不存在或属于他人时抛出 / Raised when absent or owned by another user.
        """
        async with self._uow_factory() as uow:
            actor = await self._authenticate(uow.repository, principal, PROFILE_SCOPE)
            return await self._require_deletion(uow.repository, actor.user_id, request_id)

    async def cancel_account_deletion(
        self,
        principal: TokenPrincipal,
        request_id: AccountDeletionId,
        *,
        expected_revision: int,
    ) -> AccountDeletion:
        """@brief 只取消仍在冷静期的删除请求 / Cancel only a deletion still cooling off.

        @param principal 已验证 token principal / Verified token principal.
        @param request_id 删除请求标识 / Deletion-request identifier.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        @return 已提交的 cancelled 请求 / Committed cancelled request.
        @raise AccessConflict 请求已离开 scheduled 状态时抛出 / Raised after scheduled state.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            actor = await self._authenticate(repository, principal, PROFILE_SCOPE)
            user = await self._require_user(repository, actor.user_id)
            request = await self._require_deletion(repository, actor.user_id, request_id)
            self._require_revision(request.meta.revision, expected_revision)
            if not isinstance(request, ScheduledAccountDeletion):
                raise AccessConflict(
                    "account_deletion.not_cancellable",
                    "only a scheduled account deletion can be cancelled",
                )
            now = self._clock.now()
            cancelled = request.cancel(now)
            restored_user = user.cancel_scheduled_deletion(now)
            await repository.save_account_deletion(cancelled)
            await repository.save_user(restored_user)
            await uow.commit()
            return cancelled

    async def list_workspaces(
        self, principal: TokenPrincipal
    ) -> Sequence[WorkspaceAccessResult]:
        """@brief 列出调用者的活动 Workspace access / List the caller's active workspace access.

        @param principal 已验证 token principal / Verified token principal.
        @return Workspace access 稳定序列 / Stable workspace-access sequence.
        """
        async with self._uow_factory() as uow:
            actor = await self._authenticate(uow.repository, principal, WORKSPACE_READ_SCOPE)
            items = await uow.repository.list_workspace_access(actor.user_id)
            return tuple(
                WorkspaceAccessResult(workspace, membership)
                for workspace, membership in items
                if membership.status is MemberStatus.ACTIVE
            )

    async def create_workspace(
        self,
        principal: TokenPrincipal,
        command: CreateWorkspaceCommand,
    ) -> Workspace:
        """@brief 原子创建 team Workspace 与唯一初始 owner / Atomically create workspace and owner.

        @param principal 已验证 token principal / Verified token principal.
        @param command 创建参数 / Creation parameters.
        @return 已提交 Workspace / Committed workspace.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            actor = await self._authenticate(repository, principal, WORKSPACE_WRITE_SCOPE)
            user = await self._require_user(repository, actor.user_id)
            now = self._clock.now()
            workspace_id = WorkspaceId(self._id_factory("ws"))
            workspace = Workspace(
                ResourceMeta(workspace_id, 1, now, now),
                command.name,
                command.slug,
                WorkspacePlan.TEAM,
                command.data_region,
            )
            owner = Membership(
                ResourceMeta(MembershipId(self._id_factory("mem")), 1, now, now),
                workspace_id,
                actor.user_id,
                user.display_name,
                WorkspaceRole.OWNER,
                MemberStatus.ACTIVE,
            )
            await repository.add_workspace(workspace, owner)
            await uow.commit()
            return workspace

    async def get_workspace(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
    ) -> Workspace:
        """@brief 读取已授权 Workspace / Read an authorized workspace.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @return Workspace 聚合 / Workspace aggregate.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow.repository, principal, workspace_id, WorkspaceAction.READ
            )
            return await self._require_workspace(uow.repository, workspace_id)

    async def update_workspace(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: UpdateWorkspaceCommand,
        *,
        expected_revision: int,
    ) -> Workspace:
        """@brief 修改 Workspace 名称或 slug / Update a workspace name or slug.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param command merge-patch 命令 / Merge-patch command.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        @return 已提交 Workspace / Committed workspace.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            await self._authorize(repository, principal, workspace_id, WorkspaceAction.UPDATE)
            workspace = await self._require_workspace(repository, workspace_id)
            self._require_revision(workspace.meta.revision, expected_revision)
            updated = workspace.revise(
                name=workspace.name if command.name is None else command.name,
                slug=workspace.slug if command.slug is None else command.slug,
                updated_at=self._clock.now(),
            )
            await repository.save_workspace(updated)
            await uow.commit()
            return updated

    async def delete_workspace(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 由 owner 删除完整 Workspace 租户 / Delete a whole workspace tenant as owner.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            await self._authorize(repository, principal, workspace_id, WorkspaceAction.DELETE)
            workspace = await self._require_workspace(repository, workspace_id)
            self._require_revision(workspace.meta.revision, expected_revision)
            await repository.delete_workspace(workspace_id)
            await uow.commit()

    async def list_members(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
    ) -> Sequence[Membership]:
        """@brief 列出已授权 Workspace 成员 / List members of an authorized workspace.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @return 成员稳定序列 / Stable membership sequence.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow.repository, principal, workspace_id, WorkspaceAction.LIST_MEMBERS
            )
            return tuple(await uow.repository.list_members(workspace_id))

    async def get_member(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        membership_id: MembershipId,
    ) -> Membership:
        """@brief 在租户边界内读取成员 / Read a member within the tenant boundary.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param membership_id 成员关系标识 / Membership identifier.
        @return 成员关系 / Membership.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow.repository, principal, workspace_id, WorkspaceAction.READ_MEMBER
            )
            return await self._require_membership(
                uow.repository, workspace_id, membership_id
            )

    async def update_member(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        membership_id: MembershipId,
        command: UpdateMemberCommand,
        *,
        expected_revision: int,
    ) -> Membership:
        """@brief 在同一写事务中修改成员并保护唯一 owner / Update member while protecting sole owner.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param membership_id 目标成员关系 / Target membership.
        @param command 普通 member patch / Ordinary member patch.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        @return 已提交成员关系 / Committed membership.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            context = await self._authorize(
                repository, principal, workspace_id, WorkspaceAction.UPDATE_MEMBER
            )
            membership = await self._require_membership(
                repository, workspace_id, membership_id
            )
            self._require_revision(membership.meta.revision, expected_revision)
            self._protect_owner_from_admin(context, membership)
            target_role = membership.role if command.role is None else command.role
            target_status = membership.status if command.status is None else command.status
            if target_role is membership.role and target_status is membership.status:
                raise AccessConflict(
                    "membership.patch_noop", "member patch must change role or status"
                )
            owner_count = await repository.count_active_owners(workspace_id)
            updated = membership.revise(
                role=command.role,
                status=command.status,
                active_owner_count=owner_count,
                updated_at=self._clock.now(),
            )
            await repository.save_membership(updated)
            await uow.commit()
            return updated

    async def remove_member(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        membership_id: MembershipId,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 删除成员且不允许失去最后一个活动 owner / Remove a member without losing sole owner.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param membership_id 目标成员关系 / Target membership.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            context = await self._authorize(
                repository, principal, workspace_id, WorkspaceAction.REMOVE_MEMBER
            )
            membership = await self._require_membership(
                repository, workspace_id, membership_id
            )
            self._require_revision(membership.meta.revision, expected_revision)
            self._protect_owner_from_admin(context, membership)
            owner_count = await repository.count_active_owners(workspace_id)
            membership.ensure_removable(active_owner_count=owner_count)
            await repository.delete_membership(membership)
            await uow.commit()

    async def list_invitations(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
    ) -> Sequence[Invitation]:
        """@brief 列出可管理的 Workspace 邀请 / List manageable workspace invitations.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @return 邀请稳定序列 / Stable invitation sequence.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow.repository, principal, workspace_id, WorkspaceAction.LIST_INVITATIONS
            )
            return tuple(await uow.repository.list_invitations(workspace_id))

    async def create_invitation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateInvitationCommand,
    ) -> Invitation:
        """@brief 创建到期且收件人绑定的邀请 / Create an expiring, recipient-bound invitation.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param command 邀请参数 / Invitation parameters.
        @return 已提交 pending 邀请 / Committed pending invitation.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            await self._authorize(
                repository, principal, workspace_id, WorkspaceAction.CREATE_INVITATION
            )
            now = self._clock.now()
            invitation = Invitation(
                ResourceMeta(
                    InvitationId(self._id_factory("inv")),
                    1,
                    now,
                    now,
                ),
                workspace_id,
                command.email.casefold(),
                command.role,
                InvitationStatus.PENDING,
                now + self._invitation_lifetime,
            )
            await repository.add_invitation(invitation)
            await uow.commit()
            return invitation

    async def get_invitation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        invitation_id: InvitationId,
    ) -> Invitation:
        """@brief 在租户边界内读取邀请 / Read an invitation within the tenant boundary.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @return 邀请 / Invitation.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow.repository, principal, workspace_id, WorkspaceAction.READ_INVITATION
            )
            return await self._require_invitation(
                uow.repository, workspace_id, invitation_id
            )

    async def get_invitation_for_acceptance(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        invitation_id: InvitationId,
    ) -> Invitation:
        """@brief 为收件人读取可接受邀请 / Read an invitation for its recipient.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 邀请路径 Workspace / Invitation path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @return 邮箱与当前用户匹配的邀请 / Invitation whose email matches the current user.
        @raise AccessResourceNotFound 邀请不存在或不属于当前收件人时抛出 / Raised when the
            invitation is absent or does not belong to the current recipient.
        @note 此入口不要求已有 Workspace membership；它只暴露单个已知 ID，并以 404
            隐藏收件人不匹配 / This entry point needs no pre-existing membership; it exposes only
            one known identifier and hides recipient mismatches behind 404.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            actor = await self._authenticate(repository, principal, WORKSPACE_READ_SCOPE)
            user = await self._require_user(repository, actor.user_id)
            invitation = await self._require_invitation(
                repository, workspace_id, invitation_id
            )
            if user.email.casefold() != invitation.email.casefold():
                raise AccessResourceNotFound("invitation")
            return invitation

    async def revoke_invitation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        invitation_id: InvitationId,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 撤销一次 pending 邀请 / Revoke one pending invitation.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            await self._authorize(
                repository, principal, workspace_id, WorkspaceAction.REVOKE_INVITATION
            )
            invitation = await self._require_invitation(
                repository, workspace_id, invitation_id
            )
            self._require_revision(invitation.meta.revision, expected_revision)
            if invitation.status is not InvitationStatus.PENDING:
                raise AccessConflict(
                    "invitation.not_revocable", "only a pending invitation can be revoked"
                )
            await repository.save_invitation(invitation.revoke(self._clock.now()))
            await uow.commit()

    async def accept_invitation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        invitation_id: InvitationId,
        *,
        expected_revision: int,
    ) -> Membership:
        """@brief 匹配收件人在单事务中一次性接受邀请 / Recipient accepts once in one transaction.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 邀请路径 Workspace / Invitation path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        @return 已提交活动成员关系 / Committed active membership.
        @note 此特殊入口不要求预先存在 membership，但仍要求 token scope、用户邮箱匹配和唯一约束。
            / This special entry point needs no prior membership, but still enforces token scope,
            email binding, and uniqueness.
        """
        async with self._uow_factory() as uow:
            repository = uow.repository
            actor = await self._authenticate(
                repository, principal, WORKSPACE_READ_SCOPE
            )
            user = await self._require_user(repository, actor.user_id)
            invitation = await self._require_invitation(
                repository, workspace_id, invitation_id
            )
            self._require_revision(invitation.meta.revision, expected_revision)
            existing = await repository.get_membership(workspace_id, actor.user_id)
            if existing is not None:
                raise AccessConflict(
                    "invitation.membership_exists",
                    "the invitation recipient already has a workspace membership",
                )
            now = self._clock.now()
            if invitation.status is not InvitationStatus.PENDING:
                raise AccessConflict(
                    "invitation.not_pending", "only a pending invitation can be accepted"
                )
            if now >= invitation.expires_at:
                raise AccessConflict("invitation.expired", "the invitation has expired")
            if user.email.casefold() != invitation.email.casefold():
                raise AuthorizationDenied("authorization.invitation_recipient_mismatch")
            try:
                accepted = invitation.accept(
                    user_id=actor.user_id,
                    actor_email=user.email,
                    accepted_at=now,
                )
            except InvalidStateTransition as error:
                raise AccessConflict("invitation.not_acceptable", str(error)) from error
            membership = Membership(
                ResourceMeta(MembershipId(self._id_factory("mem")), 1, now, now),
                workspace_id,
                actor.user_id,
                user.display_name,
                invitation.role,
                MemberStatus.ACTIVE,
            )
            await repository.save_invitation(accepted)
            await repository.add_membership(membership)
            await uow.commit()
            return membership

    async def _authenticate(
        self,
        repository: AccessRepository,
        principal: TokenPrincipal,
        required_scope: Scope,
    ) -> AuthenticatedActor:
        """@brief 集中绑定 actor 并检查非 Workspace scope / Bind actor and check non-workspace scope.

        @param repository 当前事务 Repository / Current transaction repository.
        @param principal 已验证 token principal / Verified token principal.
        @param required_scope 此操作精确所需 scope / Exact scope required for the operation.
        @return 已认证 actor / Authenticated actor.
        @raise AuthorizationDenied token 不含 scope 时抛出 / Raised when the token lacks scope.
        """
        actor = await AccessAuthorizer(repository).authenticate(principal)
        if not actor.principal.has_scope(required_scope):
            raise AuthorizationDenied("authorization.scope_missing")
        return actor

    async def _authorize(
        self,
        repository: AccessRepository,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        action: WorkspaceAction,
    ) -> WorkspaceAccessContext:
        """@brief 集中产生精确操作的 Workspace 证明 / Issue exact workspace-operation proof centrally.

        @param repository 当前事务 Repository / Current transaction repository.
        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param action 精确操作 / Exact action.
        @return 密封 Workspace access context / Sealed workspace access context.
        """
        authorizer = AccessAuthorizer(repository)
        actor = await authorizer.authenticate(principal)
        return await authorizer.authorize(actor, workspace_id, action)

    @staticmethod
    async def _require_user(repository: AccessRepository, user_id: UserId) -> User:
        """@brief 读取必然存在的 actor 用户 / Read the actor user that must exist.

        @param repository 当前事务 Repository / Current transaction repository.
        @param user_id 用户标识 / User identifier.
        @return 用户聚合 / User aggregate.
        @raise AccessResourceNotFound 用户并发删除时抛出 / Raised after concurrent deletion.
        """
        user = await repository.get_user(user_id)
        if user is None:
            raise AccessResourceNotFound("user")
        return user

    @staticmethod
    async def _require_workspace(
        repository: AccessRepository, workspace_id: WorkspaceId
    ) -> Workspace:
        """@brief 读取必然存在的路径 Workspace / Read the path workspace that must exist.

        @param repository 当前事务 Repository / Current transaction repository.
        @param workspace_id 路径 Workspace / Path workspace.
        @return Workspace 聚合 / Workspace aggregate.
        """
        workspace = await repository.get_workspace(workspace_id)
        if workspace is None:
            raise AccessResourceNotFound("workspace")
        return workspace

    @staticmethod
    async def _require_membership(
        repository: AccessRepository,
        workspace_id: WorkspaceId,
        membership_id: MembershipId,
    ) -> Membership:
        """@brief 以复合租户键读取成员 / Read membership by composite tenant key.

        @param repository 当前事务 Repository / Current transaction repository.
        @param workspace_id 路径 Workspace / Path workspace.
        @param membership_id 成员标识 / Membership identifier.
        @return 成员关系 / Membership.
        """
        membership = await repository.get_membership_by_id(workspace_id, membership_id)
        if membership is None or membership.workspace_id != workspace_id:
            raise AccessResourceNotFound("membership")
        return membership

    @staticmethod
    async def _require_invitation(
        repository: AccessRepository,
        workspace_id: WorkspaceId,
        invitation_id: InvitationId,
    ) -> Invitation:
        """@brief 以复合租户键读取邀请 / Read invitation by composite tenant key.

        @param repository 当前事务 Repository / Current transaction repository.
        @param workspace_id 路径 Workspace / Path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @return 邀请 / Invitation.
        """
        invitation = await repository.get_invitation(workspace_id, invitation_id)
        if invitation is None or invitation.workspace_id != workspace_id:
            raise AccessResourceNotFound("invitation")
        return invitation

    @staticmethod
    async def _require_deletion(
        repository: AccessRepository,
        user_id: UserId,
        request_id: AccountDeletionId,
    ) -> AccountDeletion:
        """@brief 以所有者复合键读取删除请求 / Read deletion request by owner composite key.

        @param repository 当前事务 Repository / Current transaction repository.
        @param user_id 所属用户 / Owning user.
        @param request_id 请求标识 / Request identifier.
        @return 删除请求 / Deletion request.
        """
        request = await repository.get_account_deletion(user_id, request_id)
        if request is None or request.user_id != user_id:
            raise AccessResourceNotFound("account_deletion")
        return request

    @staticmethod
    def _protect_owner_from_admin(
        context: WorkspaceAccessContext,
        target: Membership,
    ) -> None:
        """@brief 防止 admin 改写 owner / Prevent an admin from rewriting an owner.

        @param context 已授权管理操作 / Authorized management operation.
        @param target 目标成员关系 / Target membership.
        @raise AuthorizationDenied admin 目标是 owner 时抛出 / Raised when an admin targets an owner.
        """
        if target.role is WorkspaceRole.OWNER and context.role is not WorkspaceRole.OWNER:
            raise AuthorizationDenied("authorization.owner_protected")

    @staticmethod
    def _require_revision(actual_revision: int, expected_revision: int) -> None:
        """@brief 在写入前精确检查强 ETag 版本 / Check the strong-ETag revision exactly before writing.

        @param actual_revision 当前事务读取的资源版本 / Resource revision read in this transaction.
        @param expected_revision If-Match 解出的预期版本 / Expected revision decoded from If-Match.
        @raise AccessPreconditionFailed 版本不精确相等时抛出 / Raised unless revisions match exactly.
        """
        if actual_revision != expected_revision:
            raise AccessPreconditionFailed

    @staticmethod
    def _current_user_result(
        user: User, actor: AuthenticatedActor
    ) -> CurrentUserResult:
        """@brief 构建无重复且确定顺序的 CurrentUser 投影 / Build deterministic CurrentUser projection.

        @param user 本地用户 / Local user.
        @param actor token 绑定 actor / Token-bound actor.
        @return 当前用户结果 / Current-user result.
        """
        return CurrentUserResult(user, tuple(sorted(actor.principal.scopes)))


__all__ = [
    "OPENID_SCOPE",
    "PROFILE_SCOPE",
    "WORKSPACE_READ_SCOPE",
    "WORKSPACE_WRITE_SCOPE",
    "AccessApplicationError",
    "AccessApplicationService",
    "AccessConflict",
    "AccessPreconditionFailed",
    "AccessResourceNotFound",
    "CreateInvitationCommand",
    "CreateWorkspaceCommand",
    "CurrentUserResult",
    "IdFactory",
    "InvalidAccessCommand",
    "ReauthenticationRequired",
    "ReauthenticationVerifier",
    "UpdateMemberCommand",
    "UpdateUserCommand",
    "UpdateWorkspaceCommand",
    "UtcClock",
    "WorkspaceAccessResult",
]
