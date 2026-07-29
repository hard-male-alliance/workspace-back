"""@brief API v2 访问端口与集中授权器 / API v2 access ports and central authorizer."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self

from backend.domain.oauth_scopes import OAuthScope
from backend.domain.principals import (
    AuthenticatedActor,
    InvitationId,
    MembershipId,
    Scope,
    TokenPrincipal,
    UserId,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
    _issue_workspace_access_context,
)
from backend.domain.users import AccountDeletion, AccountDeletionId, User
from backend.domain.workspaces import (
    Invitation,
    Membership,
    MemberStatus,
    Workspace,
    WorkspaceRole,
)


class AuthorizationDenied(PermissionError):
    """@brief 默认拒绝的领域无权结果 / Deny-by-default authorization outcome.

    @param reason 不含 transport 状态的稳定原因 / Stable reason without transport status.
    """

    reason: str

    def __init__(self, reason: str) -> None:
        """@brief 初始化拒绝结果 / Initialize a denial.

        @param reason 稳定拒绝原因 / Stable denial reason.
        """
        super().__init__(reason)
        self.reason = reason


class UnknownPrincipal(LookupError):
    """@brief token subject 未绑定活动本地用户 / Token subject is not bound to a local user."""


class Clock(Protocol):
    """@brief 应用层可替换时钟 / Replaceable application clock."""

    def now(self) -> datetime:
        """@brief 读取带时区当前时刻 / Read the timezone-aware current instant.

        @return 带时区当前时间 / Timezone-aware current time.
        """


class AccessRepository(Protocol):
    """@brief Phase 1 用户与 Workspace 持久化端口 / Phase 1 user/workspace persistence port."""

    async def get_user(self, user_id: UserId) -> User | None:
        """@brief 按标识读取用户 / Read a user by identifier.

        @param user_id 用户标识 / User identifier.
        @return 用户或不存在 / User when present.
        """

    async def save_user(self, user: User) -> None:
        """@brief 保存已校验用户 / Persist a validated user.

        @param user 用户聚合 / User aggregate.
        """

    async def list_workspace_access(
        self, user_id: UserId
    ) -> Sequence[tuple[Workspace, Membership]]:
        """@brief 列出用户的活动 Workspace 关系 / List a user's active workspace relations.

        @param user_id 用户标识 / User identifier.
        @return Workspace 与成员关系对 / Workspace-membership pairs.
        """

    async def get_workspace(self, workspace_id: WorkspaceId) -> Workspace | None:
        """@brief 读取 Workspace 根 / Read a workspace root.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @return Workspace 或不存在 / Workspace when present.
        """

    async def add_workspace(self, workspace: Workspace, owner: Membership) -> None:
        """@brief 原子创建 Workspace 与初始 owner / Atomically add workspace and initial owner.

        @param workspace 新 Workspace / New workspace.
        @param owner 初始 owner 成员关系 / Initial owner membership.
        """

    async def save_workspace(self, workspace: Workspace) -> None:
        """@brief 保存 Workspace / Persist a workspace.

        @param workspace Workspace 聚合 / Workspace aggregate.
        """

    async def delete_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 删除已授权 Workspace / Delete an authorized workspace.

        @param workspace_id Workspace 标识 / Workspace identifier.
        """

    async def get_membership(self, workspace_id: WorkspaceId, user_id: UserId) -> Membership | None:
        """@brief 读取用户在路径 Workspace 的关系 / Read a user's membership in path workspace.

        @param workspace_id 路径 Workspace / Path workspace.
        @param user_id 用户标识 / User identifier.
        @return 成员关系或不存在 / Membership when present.
        """

    async def get_membership_by_id(
        self, workspace_id: WorkspaceId, membership_id: MembershipId
    ) -> Membership | None:
        """@brief 在 Workspace 内按标识读取成员 / Read membership by ID within a workspace.

        @param workspace_id 路径 Workspace / Path workspace.
        @param membership_id 成员关系标识 / Membership identifier.
        @return 成员关系或不存在 / Membership when present.
        """

    async def list_members(self, workspace_id: WorkspaceId) -> Sequence[Membership]:
        """@brief 列出 Workspace 成员 / List workspace members.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @return 稳定顺序成员集合 / Stably ordered memberships.
        """

    async def count_active_owners(self, workspace_id: WorkspaceId) -> int:
        """@brief 在写事务中计数活跃 owner / Count active owners inside the write transaction.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @return 活跃 owner 数 / Active-owner count.
        @note Adapter 必须用可防止并发双重降级的锁或隔离级别实现。
            / The adapter must prevent concurrent double-demotion with locking or isolation.
        """

    async def add_membership(self, membership: Membership) -> None:
        """@brief 添加成员关系 / Add a membership.

        @param membership 已校验成员关系 / Validated membership.
        """

    async def save_membership(self, membership: Membership) -> None:
        """@brief 保存成员关系 / Persist a membership.

        @param membership 已校验成员关系 / Validated membership.
        """

    async def delete_membership(self, membership: Membership) -> None:
        """@brief 删除成员关系 / Delete a membership.

        @param membership 已执行 sole-owner 检查的成员关系 / Membership checked for sole owner.
        """

    async def get_invitation(
        self, workspace_id: WorkspaceId, invitation_id: InvitationId
    ) -> Invitation | None:
        """@brief 在 Workspace 内读取邀请 / Read an invitation within a workspace.

        @param workspace_id 路径 Workspace / Path workspace.
        @param invitation_id 邀请标识 / Invitation identifier.
        @return 邀请或不存在 / Invitation when present.
        """

    async def list_invitations(self, workspace_id: WorkspaceId) -> Sequence[Invitation]:
        """@brief 列出 Workspace 邀请 / List workspace invitations.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @return 稳定顺序邀请集合 / Stably ordered invitations.
        """

    async def add_invitation(self, invitation: Invitation) -> None:
        """@brief 添加邀请 / Add an invitation.

        @param invitation pending 邀请 / Pending invitation.
        """

    async def save_invitation(self, invitation: Invitation) -> None:
        """@brief 保存邀请状态 / Persist invitation state.

        @param invitation 已迁移邀请 / Transitioned invitation.
        """

    async def get_account_deletion(
        self, user_id: UserId, request_id: AccountDeletionId
    ) -> AccountDeletion | None:
        """@brief 读取用户自己的删除请求 / Read a user's own deletion request.

        @param user_id 所属用户 / Owning user.
        @param request_id 删除请求标识 / Deletion request identifier.
        @return 删除请求或不存在 / Deletion request when present.
        """

    async def add_account_deletion(self, request: AccountDeletion) -> None:
        """@brief 添加删除请求 / Add an account-deletion request.

        @param request scheduled 删除请求 / Scheduled deletion request.
        """

    async def save_account_deletion(self, request: AccountDeletion) -> None:
        """@brief 保存删除状态 / Persist account-deletion state.

        @param request 已迁移删除请求 / Transitioned deletion request.
        """


class AccessUnitOfWork(Protocol):
    """@brief Phase 1 原子工作单元 / Phase 1 atomic unit of work."""

    @property
    def repository(self) -> AccessRepository:
        """@brief 返回绑定当前事务的 Repository / Return the transaction-bound repository.

        @return 当前事务 Repository / Current transaction repository.
        """

    async def __aenter__(self) -> Self:
        """@brief 开始工作单元 / Begin the unit of work.

        @return 当前工作单元 / Current unit of work.
        """

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 未显式提交或发生异常时回滚 / Roll back when uncommitted or exceptional.

        @param exc_type 异常类型 / Exception type.
        @param exc 异常实例 / Exception instance.
        @param traceback 异常 traceback / Exception traceback.
        @return 不吞掉业务异常 / Does not suppress business exceptions.
        """

    async def commit(self) -> None:
        """@brief 原子提交全部 Phase 1 写入 / Atomically commit all Phase 1 writes."""

    async def rollback(self) -> None:
        """@brief 幂等回滚当前工作单元 / Idempotently roll back the current unit of work."""


class AccessUnitOfWorkFactory(Protocol):
    """@brief 为每个 application command 创建工作单元 / Create a unit of work per command."""

    def __call__(self) -> AccessUnitOfWork:
        """@brief 创建未进入的工作单元 / Create a not-yet-entered unit of work.

        @return 新工作单元 / New unit of work.
        """


@dataclass(frozen=True, slots=True)
class _AuthorizationRule:
    """@brief 一个 scope 与角色的交集规则 / One scope-and-role intersection rule.

    @param scope 必须存在的 token scope / Required token scope.
    @param roles 可执行操作的活动角色 / Active roles allowed to perform the action.
    """

    scope: Scope
    roles: frozenset[WorkspaceRole]


_READ_SCOPE = Scope(OAuthScope.WORKSPACE_READ)
"""@brief Workspace 读取 OAuth scope / Workspace read OAuth scope."""

_WRITE_SCOPE = Scope(OAuthScope.WORKSPACE_WRITE)
"""@brief Workspace 管理 OAuth scope / Workspace administration OAuth scope."""

_RESUME_READ_SCOPE = Scope(OAuthScope.RESUME_READ)
"""@brief Resume 读取 OAuth scope / Resume read OAuth scope."""

_RESUME_WRITE_SCOPE = Scope(OAuthScope.RESUME_WRITE)
"""@brief Resume 写入 OAuth scope / Resume write OAuth scope."""

_RESUME_RENDER_SCOPE = Scope(OAuthScope.RESUME_RENDER)
"""@brief Resume 渲染 OAuth scope / Resume-rendering OAuth scope."""

_INTERVIEW_READ_SCOPE = Scope(OAuthScope.INTERVIEW_READ)
"""@brief Interview 读取 OAuth scope / Interview read OAuth scope."""

_INTERVIEW_WRITE_SCOPE = Scope(OAuthScope.INTERVIEW_WRITE)
"""@brief Interview 写入 OAuth scope / Interview write OAuth scope."""

_ALL_ROLES = frozenset(WorkspaceRole)
"""@brief 所有活动 Workspace 角色 / All active workspace roles."""

_ADMIN_ROLES = frozenset({WorkspaceRole.OWNER, WorkspaceRole.ADMIN})
"""@brief 可管理 Workspace 的角色 / Roles allowed to administer a workspace."""

_OWNER_ONLY = frozenset({WorkspaceRole.OWNER})
"""@brief 仅 owner 的角色集 / Owner-only role set."""

_CONTENT_EDITOR_ROLES = frozenset(
    {WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.EDITOR}
)
"""@brief 可修改 Workspace 内容资源的角色 / Roles allowed to modify workspace content."""

WORKSPACE_AUTHORIZATION_MATRIX: dict[WorkspaceAction, _AuthorizationRule] = {
    WorkspaceAction.READ: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.LIST_MEMBERS: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_MEMBER: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.UPDATE: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.DELETE: _AuthorizationRule(_WRITE_SCOPE, _OWNER_ONLY),
    WorkspaceAction.UPDATE_MEMBER: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.REMOVE_MEMBER: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.LIST_INVITATIONS: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.READ_INVITATION: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.CREATE_INVITATION: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.REVOKE_INVITATION: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.LIST_RESUMES: _AuthorizationRule(_RESUME_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_RESUME: _AuthorizationRule(_RESUME_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.CREATE_RESUME: _AuthorizationRule(_RESUME_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.UPDATE_RESUME: _AuthorizationRule(_RESUME_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.DELETE_RESUME: _AuthorizationRule(_RESUME_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.READ_RESUME_REVISIONS: _AuthorizationRule(_RESUME_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.APPLY_RESUME_OPERATIONS: _AuthorizationRule(_RESUME_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.CREATE_RESUME_IMPORT_JOB: _AuthorizationRule(
        _RESUME_WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.CREATE_RESUME_RESTORE_JOB: _AuthorizationRule(
        _RESUME_WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.CREATE_RESUME_RENDER_JOB: _AuthorizationRule(
        _RESUME_RENDER_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.LIST_RESUME_PROPOSALS: _AuthorizationRule(_RESUME_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_RESUME_PROPOSAL: _AuthorizationRule(_RESUME_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.DECIDE_RESUME_PROPOSAL: _AuthorizationRule(_RESUME_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.LIST_CONNECTIONS: _AuthorizationRule(_READ_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.CREATE_CONNECTION_AUTHORIZATION_SESSION: _AuthorizationRule(
        _WRITE_SCOPE, _ADMIN_ROLES
    ),
    WorkspaceAction.CREATE_CONNECTION: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.DELETE_CONNECTION: _AuthorizationRule(_WRITE_SCOPE, _ADMIN_ROLES),
    WorkspaceAction.LIST_KNOWLEDGE_SOURCES: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_KNOWLEDGE_SOURCE: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.CREATE_KNOWLEDGE_SOURCE: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.UPDATE_KNOWLEDGE_SOURCE: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.DELETE_KNOWLEDGE_SOURCE: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.READ_KNOWLEDGE_VERSIONS: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.CREATE_KNOWLEDGE_VERSION: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.CREATE_UPLOAD_SESSION: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.COMPLETE_UPLOAD_SESSION: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.CREATE_KNOWLEDGE_JOB: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.SEARCH_KNOWLEDGE: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.EVALUATE_KNOWLEDGE_ACCESS: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.LIST_CONVERSATIONS: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.CREATE_CONVERSATION: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.READ_CONVERSATION: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.UPDATE_CONVERSATION: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.DELETE_CONVERSATION: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.LIST_MESSAGES: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.CREATE_MESSAGE: _AuthorizationRule(_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.CREATE_AGENT_RUN: _AuthorizationRule(_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.READ_AGENT_RUN: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.CANCEL_AGENT_RUN: _AuthorizationRule(_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.READ_TOOL_APPROVAL: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.DECIDE_TOOL_APPROVAL: _AuthorizationRule(
        _WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.LIST_INTERVIEW_SCENARIOS: _AuthorizationRule(
        _INTERVIEW_READ_SCOPE, _ALL_ROLES
    ),
    WorkspaceAction.CREATE_INTERVIEW_SCENARIO: _AuthorizationRule(
        _INTERVIEW_WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.READ_INTERVIEW_SCENARIO: _AuthorizationRule(
        _INTERVIEW_READ_SCOPE, _ALL_ROLES
    ),
    WorkspaceAction.UPDATE_INTERVIEW_SCENARIO: _AuthorizationRule(
        _INTERVIEW_WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.LIST_INTERVIEW_SESSIONS: _AuthorizationRule(
        _INTERVIEW_READ_SCOPE, _ALL_ROLES
    ),
    WorkspaceAction.CREATE_INTERVIEW_SESSION: _AuthorizationRule(
        _INTERVIEW_WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.READ_INTERVIEW_SESSION: _AuthorizationRule(
        _INTERVIEW_READ_SCOPE, _ALL_ROLES
    ),
    WorkspaceAction.CREATE_INTERVIEW_CONNECTION: _AuthorizationRule(
        _INTERVIEW_WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.END_INTERVIEW_SESSION: _AuthorizationRule(
        _INTERVIEW_WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.READ_INTERVIEW_TRANSCRIPT: _AuthorizationRule(
        _INTERVIEW_READ_SCOPE, _ALL_ROLES
    ),
    WorkspaceAction.CREATE_INTERVIEW_REPORT_JOB: _AuthorizationRule(
        _INTERVIEW_WRITE_SCOPE, _CONTENT_EDITOR_ROLES
    ),
    WorkspaceAction.READ_INTERVIEW_REPORT: _AuthorizationRule(
        _INTERVIEW_READ_SCOPE, _ALL_ROLES
    ),
    WorkspaceAction.LIST_JOBS: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_JOB: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.CANCEL_JOB: _AuthorizationRule(_WRITE_SCOPE, _CONTENT_EDITOR_ROLES),
    WorkspaceAction.LIST_ARTIFACTS: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_ARTIFACT: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_ARTIFACT_CONTENT: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_ARTIFACT_SOURCE_MAP: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.READ_EVENTS: _AuthorizationRule(_READ_SCOPE, _ALL_ROLES),
    WorkspaceAction.LIST_AUDIT_EVENTS: _AuthorizationRule(_READ_SCOPE, _ADMIN_ROLES),
}
"""@brief 默认拒绝的 scope∩role→action 授权矩阵 / Deny-by-default scope∩role→action matrix."""


class AccessAuthorizer:
    """@brief 集中产生 actor 与 Workspace 授权上下文 / Central actor/workspace authorizer.

    @param repository 当前工作单元的访问 Repository / Access repository for the current unit.
    """

    def __init__(self, repository: AccessRepository) -> None:
        """@brief 绑定事务 Repository / Bind a transaction repository.

        @param repository 当前工作单元 Repository / Current unit-of-work repository.
        """
        self._repository = repository

    async def authenticate(self, principal: TokenPrincipal) -> AuthenticatedActor:
        """@brief 将 token subject 绑定到本地用户 / Bind a token subject to a local user.

        @param principal 已完成密码学验证的 token 投影 / Cryptographically verified token projection.
        @return 已认证本地 actor / Authenticated local actor.
        @raise UnknownPrincipal subject 未绑定本地账户时抛出 / Raised when no local account is bound.
        """
        user = await self._repository.get_user(principal.user_id)
        if user is None or user.subject != principal.subject:
            raise UnknownPrincipal("verified subject is not bound to a local user")
        return AuthenticatedActor(user.meta.id, principal)

    async def authorize(
        self,
        actor: AuthenticatedActor,
        workspace_id: WorkspaceId,
        action: WorkspaceAction,
    ) -> WorkspaceAccessContext:
        """@brief 以 deny-first 顺序授权一次精确操作 / Authorize one exact action deny-first.

        @param actor 已认证本地 actor / Authenticated local actor.
        @param workspace_id URL path 中的 Workspace / Workspace from the URL path.
        @param action 待授权精确操作 / Exact action to authorize.
        @return 仅适用于该 action 的密封上下文 / Sealed context valid only for that action.
        @raise AuthorizationDenied 规则缺失、关系无效、scope 或 role 不满足时抛出 / Raised for
            missing policy, invalid membership, missing scope, or disallowed role.
        """
        rule = WORKSPACE_AUTHORIZATION_MATRIX.get(action)
        if rule is None:
            raise AuthorizationDenied("authorization.policy_missing")
        membership = await self._repository.get_membership(workspace_id, actor.user_id)
        if membership is None or membership.workspace_id != workspace_id:
            raise AuthorizationDenied("authorization.membership_missing")
        if membership.status is not MemberStatus.ACTIVE:
            raise AuthorizationDenied("authorization.membership_inactive")
        if not actor.principal.has_scope(rule.scope):
            raise AuthorizationDenied("authorization.scope_missing")
        if membership.role not in rule.roles:
            raise AuthorizationDenied("authorization.role_denied")
        return _issue_workspace_access_context(
            actor,
            workspace_id,
            membership.meta.id,
            membership.role,
            action,
        )


__all__ = [
    "WORKSPACE_AUTHORIZATION_MATRIX",
    "AccessAuthorizer",
    "AccessRepository",
    "AccessUnitOfWork",
    "AccessUnitOfWorkFactory",
    "AuthorizationDenied",
    "Clock",
    "UnknownPrincipal",
]
