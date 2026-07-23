"""@brief API v2 Phase 1 应用用例测试 / API v2 Phase 1 application-use-case tests."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from types import TracebackType

import pytest

from backend.application.access import (
    AccessApplicationService,
    AccessConflict,
    AccessPreconditionFailed,
    AccessResourceNotFound,
    CreateInvitationCommand,
    CreateWorkspaceCommand,
    InvalidAccessCommand,
    ReauthenticationRequired,
    UpdateMemberCommand,
    UpdateUserCommand,
    UpdateWorkspaceCommand,
)
from backend.application.ports.access import (
    WORKSPACE_AUTHORIZATION_MATRIX,
    AccessRepository,
    AccessUnitOfWork,
    AuthorizationDenied,
)
from backend.domain.principals import (
    ClientId,
    InvitationId,
    MembershipId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.users import AccountDeletion, AccountDeletionId, AccountStatus, User
from backend.domain.workspaces import (
    DataRegion,
    Invitation,
    Membership,
    MemberStatus,
    SoleOwnerViolation,
    Workspace,
    WorkspaceRole,
)

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 测试固定时刻 / Fixed test instant."""

KLEE_ID = UserId("usr_klee")
"""@brief 主测试用户标识 / Primary test-user identifier."""

AMBER_ID = UserId("usr_amber")
"""@brief 邀请收件人标识 / Invitation-recipient identifier."""


class _Clock:
    """@brief 可控测试时钟 / Controllable test clock."""

    current: datetime
    """@brief 当前测试时刻 / Current test instant."""

    def __init__(self) -> None:
        """@brief 初始化固定时刻 / Initialize the fixed instant."""
        self.current = NOW

    def now(self) -> datetime:
        """@brief 返回当前测试时刻 / Return the current test instant.

        @return 带时区测试时刻 / Timezone-aware test instant.
        """
        return self.current


class _Ids:
    """@brief 确定性测试标识工厂 / Deterministic test ID factory."""

    counters: dict[str, int]
    """@brief 各前缀计数器 / Counters by prefix."""

    def __init__(self) -> None:
        """@brief 初始化空计数器 / Initialize empty counters."""
        self.counters = {}

    def __call__(self, prefix: str) -> str:
        """@brief 生成下一个确定性标识 / Generate the next deterministic identifier.

        @param prefix 领域前缀 / Domain prefix.
        @return 测试标识 / Test identifier.
        """
        value = self.counters.get(prefix, 0) + 1
        self.counters[prefix] = value
        return f"{prefix}_{value:04d}"


class _Reauthentication:
    """@brief 用户绑定的测试 reauthentication verifier / User-bound test verifier."""

    valid: set[tuple[UserId, str]]
    """@brief 被允许的用户与 flow 对 / Allowed user-flow pairs."""

    calls: list[tuple[UserId, str, datetime]]
    """@brief 验证调用记录 / Verification call records."""

    def __init__(self) -> None:
        """@brief 初始化 fail-closed verifier / Initialize a fail-closed verifier."""
        self.valid = set()
        self.calls = []

    async def verify_recent(
        self, user_id: UserId, flow_id: str, verified_at: datetime
    ) -> bool:
        """@brief 仅允许显式登记的证明 / Allow only explicitly registered proofs.

        @param user_id 用户标识 / User identifier.
        @param flow_id reauthenticate flow 标识 / Reauthenticate-flow identifier.
        @param verified_at 验证时刻 / Verification instant.
        @return 用户与 flow 对是否登记 / Whether the user-flow pair is registered.
        """
        self.calls.append((user_id, flow_id, verified_at))
        return (user_id, flow_id) in self.valid


class _Repository:
    """@brief 覆盖 Phase 1 端口的内存测试 Repository / In-memory Phase 1 test repository."""

    users: dict[UserId, User]
    """@brief 用户存储 / User store."""

    workspaces: dict[WorkspaceId, Workspace]
    """@brief Workspace 存储 / Workspace store."""

    memberships: dict[tuple[WorkspaceId, MembershipId], Membership]
    """@brief 成员复合键存储 / Membership composite-key store."""

    invitations: dict[tuple[WorkspaceId, InvitationId], Invitation]
    """@brief 邀请复合键存储 / Invitation composite-key store."""

    deletions: dict[tuple[UserId, AccountDeletionId], AccountDeletion]
    """@brief 账户删除复合键存储 / Account-deletion composite-key store."""

    def __init__(self, *users: User) -> None:
        """@brief 初始化测试数据 / Initialize test data.

        @param users 初始用户 / Initial users.
        """
        self.users = {user.meta.id: user for user in users}
        self.workspaces = {}
        self.memberships = {}
        self.invitations = {}
        self.deletions = {}

    async def get_user_by_subject(self, subject: Subject) -> User | None:
        """@brief 按 subject 读取用户 / Read a user by subject.

        @param subject OIDC subject / OIDC subject.
        @return 匹配用户 / Matching user.
        """
        return next((user for user in self.users.values() if user.subject == subject), None)

    async def get_user(self, user_id: UserId) -> User | None:
        """@brief 按标识读取用户 / Read a user by ID.

        @param user_id 用户标识 / User identifier.
        @return 用户或不存在 / User when present.
        """
        return self.users.get(user_id)

    async def save_user(self, user: User) -> None:
        """@brief 保存用户 / Save a user.

        @param user 用户聚合 / User aggregate.
        """
        self.users[user.meta.id] = user

    async def list_workspace_access(
        self, user_id: UserId
    ) -> Sequence[tuple[Workspace, Membership]]:
        """@brief 列出用户 Workspace 关系 / List a user's workspace relations.

        @param user_id 用户标识 / User identifier.
        @return Workspace 与成员关系对 / Workspace-membership pairs.
        """
        return tuple(
            (self.workspaces[membership.workspace_id], membership)
            for membership in self.memberships.values()
            if membership.user_id == user_id
        )

    async def get_workspace(self, workspace_id: WorkspaceId) -> Workspace | None:
        """@brief 读取 Workspace / Read a workspace.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @return Workspace 或不存在 / Workspace when present.
        """
        return self.workspaces.get(workspace_id)

    async def add_workspace(self, workspace: Workspace, owner: Membership) -> None:
        """@brief 原子测试写入 Workspace 与 owner / Atomically add workspace and owner in tests.

        @param workspace 新 Workspace / New workspace.
        @param owner 初始 owner / Initial owner.
        """
        self.workspaces[workspace.meta.id] = workspace
        self.memberships[(owner.workspace_id, owner.meta.id)] = owner

    async def save_workspace(self, workspace: Workspace) -> None:
        """@brief 保存 Workspace / Save a workspace.

        @param workspace Workspace 聚合 / Workspace aggregate.
        """
        self.workspaces[workspace.meta.id] = workspace

    async def delete_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 删除 Workspace 与测试子资源 / Delete a workspace and test children.

        @param workspace_id Workspace 标识 / Workspace identifier.
        """
        del self.workspaces[workspace_id]
        self.memberships = {
            key: value for key, value in self.memberships.items() if key[0] != workspace_id
        }
        self.invitations = {
            key: value for key, value in self.invitations.items() if key[0] != workspace_id
        }

    async def get_membership(
        self, workspace_id: WorkspaceId, user_id: UserId
    ) -> Membership | None:
        """@brief 按 Workspace 与用户读取关系 / Read membership by workspace and user.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @param user_id 用户标识 / User identifier.
        @return 成员关系或不存在 / Membership when present.
        """
        return next(
            (
                membership
                for membership in self.memberships.values()
                if membership.workspace_id == workspace_id and membership.user_id == user_id
            ),
            None,
        )

    async def get_membership_by_id(
        self, workspace_id: WorkspaceId, membership_id: MembershipId
    ) -> Membership | None:
        """@brief 按复合键读取成员 / Read membership by composite key.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @param membership_id 成员关系标识 / Membership identifier.
        @return 成员关系或不存在 / Membership when present.
        """
        return self.memberships.get((workspace_id, membership_id))

    async def list_members(self, workspace_id: WorkspaceId) -> Sequence[Membership]:
        """@brief 列出 Workspace 成员 / List workspace members.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @return 成员序列 / Membership sequence.
        """
        return tuple(
            membership
            for membership in self.memberships.values()
            if membership.workspace_id == workspace_id
        )

    async def count_active_owners(self, workspace_id: WorkspaceId) -> int:
        """@brief 计数活动 owner / Count active owners.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @return 活动 owner 数 / Active-owner count.
        """
        return sum(
            membership.workspace_id == workspace_id
            and membership.role is WorkspaceRole.OWNER
            and membership.status is MemberStatus.ACTIVE
            for membership in self.memberships.values()
        )

    async def add_membership(self, membership: Membership) -> None:
        """@brief 添加唯一用户成员关系 / Add a unique user membership.

        @param membership 新成员关系 / New membership.
        @raise RuntimeError 用户已有关系时模拟数据库唯一约束 / Simulates a database unique constraint.
        """
        if await self.get_membership(membership.workspace_id, membership.user_id) is not None:
            raise RuntimeError("duplicate membership")
        self.memberships[(membership.workspace_id, membership.meta.id)] = membership

    async def save_membership(self, membership: Membership) -> None:
        """@brief 保存成员关系 / Save a membership.

        @param membership 成员关系 / Membership.
        """
        self.memberships[(membership.workspace_id, membership.meta.id)] = membership

    async def delete_membership(self, membership: Membership) -> None:
        """@brief 删除成员关系 / Delete a membership.

        @param membership 成员关系 / Membership.
        """
        del self.memberships[(membership.workspace_id, membership.meta.id)]

    async def get_invitation(
        self, workspace_id: WorkspaceId, invitation_id: InvitationId
    ) -> Invitation | None:
        """@brief 按复合键读取邀请 / Read invitation by composite key.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @param invitation_id 邀请标识 / Invitation identifier.
        @return 邀请或不存在 / Invitation when present.
        """
        return self.invitations.get((workspace_id, invitation_id))

    async def list_invitations(self, workspace_id: WorkspaceId) -> Sequence[Invitation]:
        """@brief 列出 Workspace 邀请 / List workspace invitations.

        @param workspace_id Workspace 标识 / Workspace identifier.
        @return 邀请序列 / Invitation sequence.
        """
        return tuple(
            invitation
            for invitation in self.invitations.values()
            if invitation.workspace_id == workspace_id
        )

    async def add_invitation(self, invitation: Invitation) -> None:
        """@brief 添加邀请 / Add an invitation.

        @param invitation 新邀请 / New invitation.
        """
        self.invitations[(invitation.workspace_id, invitation.meta.id)] = invitation

    async def save_invitation(self, invitation: Invitation) -> None:
        """@brief 保存邀请 / Save an invitation.

        @param invitation 邀请 / Invitation.
        """
        self.invitations[(invitation.workspace_id, invitation.meta.id)] = invitation

    async def get_account_deletion(
        self, user_id: UserId, request_id: AccountDeletionId
    ) -> AccountDeletion | None:
        """@brief 按所有者复合键读取删除请求 / Read deletion by owner composite key.

        @param user_id 用户标识 / User identifier.
        @param request_id 请求标识 / Request identifier.
        @return 删除请求或不存在 / Deletion request when present.
        """
        return self.deletions.get((user_id, request_id))

    async def add_account_deletion(self, request: AccountDeletion) -> None:
        """@brief 添加删除请求 / Add a deletion request.

        @param request 删除请求 / Deletion request.
        """
        self.deletions[(request.user_id, request.meta.id)] = request

    async def save_account_deletion(self, request: AccountDeletion) -> None:
        """@brief 保存删除请求 / Save a deletion request.

        @param request 删除请求 / Deletion request.
        """
        self.deletions[(request.user_id, request.meta.id)] = request


class _UnitOfWork:
    """@brief 记录事务边界的测试工作单元 / Test unit of work recording transaction boundaries."""

    repository: AccessRepository
    """@brief 当前事务 Repository / Current transaction repository."""

    committed: bool
    """@brief 是否显式提交 / Whether explicitly committed."""

    rolled_back: bool
    """@brief 是否执行回滚 / Whether rollback ran."""

    def __init__(self, repository: AccessRepository) -> None:
        """@brief 绑定共享测试 Repository / Bind the shared test repository.

        @param repository 测试 Repository / Test repository.
        """
        self.repository = repository
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self) -> _UnitOfWork:
        """@brief 进入事务 / Enter the transaction.

        @return 当前工作单元 / Current unit of work.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时回滚 / Roll back after error or missing commit.

        @param exc_type 异常类型 / Exception type.
        @param exc 异常实例 / Exception instance.
        @param traceback traceback / Traceback.
        @return 不吞异常 / Does not suppress exceptions.
        """
        if exc is not None or not self.committed:
            await self.rollback()
        return None

    async def commit(self) -> None:
        """@brief 标记事务已提交 / Mark the transaction committed."""
        self.committed = True

    async def rollback(self) -> None:
        """@brief 标记事务已回滚 / Mark the transaction rolled back."""
        self.rolled_back = True


class _UnitOfWorkFactory:
    """@brief 每次调用创建新 UoW 的测试工厂 / Test factory creating a new UoW per call."""

    repository: AccessRepository
    """@brief 所有事务共享的测试 Repository / Test repository shared by transactions."""

    units: list[_UnitOfWork]
    """@brief 已创建工作单元 / Created units of work."""

    def __init__(self, repository: AccessRepository) -> None:
        """@brief 初始化工厂 / Initialize the factory.

        @param repository 测试 Repository / Test repository.
        """
        self.repository = repository
        self.units = []

    def __call__(self) -> AccessUnitOfWork:
        """@brief 创建独立工作单元 / Create an independent unit of work.

        @return 新工作单元 / New unit of work.
        """
        unit = _UnitOfWork(self.repository)
        self.units.append(unit)
        return unit


def _meta[IdT: str](identifier: IdT) -> ResourceMeta[IdT]:
    """@brief 创建 revision-one 测试元数据 / Build revision-one test metadata.

    @param identifier 领域标识 / Domain identifier.
    @return 测试元数据 / Test metadata.
    """
    return ResourceMeta(identifier, 1, NOW, NOW)


def _user(user_id: UserId, name: str, email: str) -> User:
    """@brief 创建无默认 Workspace 的测试用户 / Build a test user without a default workspace.

    @param user_id 用户标识 / User identifier.
    @param name 显示名 / Display name.
    @param email 登录邮箱 / Login email.
    @return 测试用户 / Test user.
    """
    return User(
        _meta(user_id),
        Subject(f"oidc-{user_id}"),
        email,
        True,
        name,
        "zh-CN",
        None,
    )


def _principal(user: User, *scopes: str) -> TokenPrincipal:
    """@brief 为用户创建四字段签名 principal / Build a four-field signed principal for a user.

    @param user 本地用户 / Local user.
    @param scopes token scopes / Token scopes.
    @return 测试 principal / Test principal.
    """
    return TokenPrincipal(
        user.meta.id,
        user.subject,
        ClientId("web-client"),
        frozenset(Scope(scope) for scope in scopes),
    )


def _service(
    repository: _Repository,
    verifier: _Reauthentication | None = None,
) -> tuple[AccessApplicationService, _UnitOfWorkFactory, _Clock, _Reauthentication]:
    """@brief 组装可观测测试服务 / Assemble an observable test service.

    @param repository 测试 Repository / Test repository.
    @param verifier 可选重新认证验证器 / Optional reauthentication verifier.
    @return 服务、UoW 工厂、时钟与 verifier / Service, UoW factory, clock, and verifier.
    """
    clock = _Clock()
    actual_verifier = verifier or _Reauthentication()
    factory = _UnitOfWorkFactory(repository)
    service = AccessApplicationService(
        factory,
        actual_verifier,
        clock=clock,
        id_factory=_Ids(),
        deletion_cooling_period=timedelta(days=14),
        invitation_lifetime=timedelta(days=3),
    )
    return service, factory, clock, actual_verifier


@pytest.mark.asyncio
async def test_me_patch_requires_profile_and_active_default_membership() -> None:
    """@brief `/me` 检查 scope 与活动默认关系 / `/me` checks scope and active default membership."""
    klee = _user(KLEE_ID, "Klee", "klee@example.cn")
    repository = _Repository(klee)
    service, factory, _, _ = _service(repository)

    with pytest.raises(AuthorizationDenied, match="scope_missing"):
        await service.get_current_user(_principal(klee, "workspace.read"))

    missing_workspace = WorkspaceId("ws_missing")
    with pytest.raises(InvalidAccessCommand, match="active membership"):
        await service.update_current_user(
            _principal(klee, "profile"),
            UpdateUserCommand(
                display_name="Spark Knight",
                default_workspace_id=missing_workspace,
                replace_default_workspace=True,
            ),
            expected_revision=1,
        )

    workspace = await _create_workspace_for(service, klee)
    updated = await service.update_current_user(
        _principal(klee, "profile"),
        UpdateUserCommand(
            display_name="Spark Knight",
            default_workspace_id=workspace.meta.id,
            replace_default_workspace=True,
        ),
        expected_revision=1,
    )
    assert updated.user.display_name == "Spark Knight"
    assert updated.user.default_workspace_id == workspace.meta.id
    assert updated.scopes == (Scope("profile"),)
    assert factory.units[-1].committed
    assert len({id(unit) for unit in factory.units}) == len(factory.units)


@pytest.mark.asyncio
async def test_account_deletion_is_reauth_bound_scheduled_and_only_once_cancellable() -> None:
    """@brief 删除请求绑定近期 reauth 且仅 scheduled 可取消 / Deletion is reauth-bound and cancellable only while scheduled."""
    klee = _user(KLEE_ID, "Klee", "klee@example.cn")
    repository = _Repository(klee)
    verifier = _Reauthentication()
    service, _, clock, _ = _service(repository, verifier)
    principal = _principal(klee, "profile")

    with pytest.raises(ReauthenticationRequired):
        await service.request_account_deletion(
            principal,
            confirmation="delete_my_account",
            reauthentication_flow_id="flow_other",
        )

    verifier.valid.add((KLEE_ID, "flow_klee"))
    request = await service.request_account_deletion(
        principal,
        confirmation="delete_my_account",
        reauthentication_flow_id="flow_klee",
    )
    assert request.scheduled_for == NOW + timedelta(days=14)
    assert verifier.calls[-1] == (KLEE_ID, "flow_klee", NOW)
    assert repository.users[KLEE_ID].account_status is AccountStatus.DELETION_SCHEDULED

    clock.current += timedelta(hours=1)
    cancelled = await service.cancel_account_deletion(
        principal, request.meta.id, expected_revision=1
    )
    assert cancelled.status == "cancelled"
    assert repository.users[KLEE_ID].account_status is AccountStatus.ACTIVE
    assert await service.get_account_deletion(principal, request.meta.id) == cancelled
    with pytest.raises(AccessConflict, match="scheduled"):
        await service.cancel_account_deletion(
            principal, request.meta.id, expected_revision=2
        )


@pytest.mark.asyncio
async def test_workspace_member_and_invitation_lifecycle_preserves_invariants() -> None:
    """@brief Workspace 生命周期保持租户、owner 与邀请不变量 / Lifecycle preserves tenant, owner, and invitation invariants."""
    klee = _user(KLEE_ID, "Klee", "klee@example.cn")
    amber = _user(AMBER_ID, "Amber", "amber@example.cn")
    repository = _Repository(klee, amber)
    service, factory, _, _ = _service(repository)
    owner_write = _principal(klee, "workspace.read", "workspace.write")
    recipient_read = _principal(amber, "workspace.read")

    workspace = await service.create_workspace(
        owner_write,
        CreateWorkspaceCommand("Knights Lab", "knights-lab", DataRegion.CN),
    )
    workspace_id = workspace.meta.id
    owner = await repository.get_membership(workspace_id, KLEE_ID)
    assert owner is not None
    assert owner.role is WorkspaceRole.OWNER
    assert factory.units[-1].committed

    listed = await service.list_workspaces(owner_write)
    assert listed[0].workspace == workspace
    assert await service.get_workspace(owner_write, workspace_id) == workspace
    revised = await service.update_workspace(
        owner_write,
        workspace_id,
        UpdateWorkspaceCommand(name="Knights Research Lab"),
        expected_revision=1,
    )
    assert revised.name == "Knights Research Lab"

    with pytest.raises(InvalidAccessCommand, match="cannot grant"):
        UpdateMemberCommand(role=WorkspaceRole.OWNER)
    with pytest.raises(SoleOwnerViolation):
        await service.update_member(
            owner_write,
            workspace_id,
            owner.meta.id,
            UpdateMemberCommand(role=WorkspaceRole.ADMIN),
            expected_revision=1,
        )

    invitation = await service.create_invitation(
        owner_write,
        workspace_id,
        CreateInvitationCommand("AMBER@EXAMPLE.CN", WorkspaceRole.EDITOR),
    )
    assert invitation.email == "amber@example.cn"
    assert invitation.expires_at == NOW + timedelta(days=3)
    assert await service.get_invitation(owner_write, workspace_id, invitation.meta.id) == invitation
    assert (
        await service.get_invitation_for_acceptance(
            recipient_read, workspace_id, invitation.meta.id
        )
        == invitation
    )
    assert list(await service.list_invitations(owner_write, workspace_id)) == [invitation]

    mismatched = await service.create_invitation(
        owner_write,
        workspace_id,
        CreateInvitationCommand("pyro@example.cn", WorkspaceRole.VIEWER),
    )
    with pytest.raises(AccessResourceNotFound):
        await service.get_invitation_for_acceptance(
            recipient_read, workspace_id, mismatched.meta.id
        )
    with pytest.raises(AuthorizationDenied, match="recipient_mismatch"):
        await service.accept_invitation(
            recipient_read, workspace_id, mismatched.meta.id, expected_revision=1
        )

    member = await service.accept_invitation(
        recipient_read, workspace_id, invitation.meta.id, expected_revision=1
    )
    assert member.user_id == AMBER_ID
    assert member.role is WorkspaceRole.EDITOR
    accepted = await repository.get_invitation(workspace_id, invitation.meta.id)
    assert accepted is not None and accepted.status == "accepted"
    with pytest.raises(AccessConflict, match="already has"):
        await service.accept_invitation(
            recipient_read, workspace_id, invitation.meta.id, expected_revision=2
        )

    assert await service.get_member(owner_write, workspace_id, member.meta.id) == member
    assert len(await service.list_members(owner_write, workspace_id)) == 2
    changed = await service.update_member(
        owner_write,
        workspace_id,
        member.meta.id,
        UpdateMemberCommand(role=WorkspaceRole.VIEWER),
        expected_revision=1,
    )
    assert changed.role is WorkspaceRole.VIEWER
    await service.remove_member(
        owner_write, workspace_id, member.meta.id, expected_revision=2
    )
    assert await repository.get_membership(workspace_id, AMBER_ID) is None

    revocable = await service.create_invitation(
        owner_write,
        workspace_id,
        CreateInvitationCommand("amber@example.cn", WorkspaceRole.VIEWER),
    )
    await service.revoke_invitation(
        owner_write, workspace_id, revocable.meta.id, expected_revision=1
    )
    revoked = await repository.get_invitation(workspace_id, revocable.meta.id)
    assert revoked is not None and revoked.status == "revoked"

    await service.delete_workspace(owner_write, workspace_id, expected_revision=2)
    assert workspace_id not in repository.workspaces


@pytest.mark.asyncio
async def test_admin_cannot_modify_owner_even_when_another_owner_exists() -> None:
    """@brief admin 永远不能改写 owner / An admin can never rewrite an owner."""
    klee = _user(KLEE_ID, "Klee", "klee@example.cn")
    amber = _user(AMBER_ID, "Amber", "amber@example.cn")
    repository = _Repository(klee, amber)
    service, _, _, _ = _service(repository)
    workspace = await _create_workspace_for(service, klee)
    owner = await repository.get_membership(workspace.meta.id, KLEE_ID)
    assert owner is not None
    admin = Membership(
        _meta(MembershipId("mem_admin")),
        workspace.meta.id,
        AMBER_ID,
        "Amber",
        WorkspaceRole.ADMIN,
        MemberStatus.ACTIVE,
    )
    repository.memberships[(workspace.meta.id, admin.meta.id)] = admin

    with pytest.raises(AuthorizationDenied, match="owner_protected"):
        await service.update_member(
            _principal(amber, "workspace.write"),
            workspace.meta.id,
            owner.meta.id,
            UpdateMemberCommand(status=MemberStatus.SUSPENDED),
            expected_revision=1,
        )


@pytest.mark.asyncio
async def test_stale_revision_never_writes_or_commits() -> None:
    """@brief stale If-Match 在任何写入前失败且不提交 / Stale If-Match fails before writes or commit."""
    klee = _user(KLEE_ID, "Klee", "klee@example.cn")
    repository = _Repository(klee)
    service, factory, _, _ = _service(repository)
    principal = _principal(klee, "workspace.read", "workspace.write")
    workspace = await service.create_workspace(
        principal,
        CreateWorkspaceCommand("Klee Lab", "klee-lab", DataRegion.CN),
    )

    with pytest.raises(AccessPreconditionFailed) as captured:
        await service.update_workspace(
            principal,
            workspace.meta.id,
            UpdateWorkspaceCommand(name="Stale Write"),
            expected_revision=99,
        )

    assert captured.value.code == "http.precondition_failed"
    assert repository.workspaces[workspace.meta.id] == workspace
    assert not factory.units[-1].committed
    assert factory.units[-1].rolled_back


@pytest.mark.asyncio
async def test_userinfo_requires_exact_openid_scope() -> None:
    """@brief UserInfo 只接受含精确 openid scope 的交叉绑定 principal / UserInfo requires exact openid scope."""
    klee = _user(KLEE_ID, "Klee", "klee@example.cn")
    service, _, _, _ = _service(_Repository(klee))
    with pytest.raises(AuthorizationDenied, match="scope_missing"):
        await service.get_userinfo(_principal(klee, "profile"))
    assert await service.get_userinfo(_principal(klee, "openid")) == klee


def test_every_workspace_action_has_an_explicit_fail_closed_policy() -> None:
    """@brief 授权矩阵必须穷尽 WorkspaceAction / Require an explicit policy for every WorkspaceAction.

    @return 无返回值 / No return value.
    """
    assert set(WORKSPACE_AUTHORIZATION_MATRIX) == set(WorkspaceAction)


async def _create_workspace_for(
    service: AccessApplicationService, user: User
) -> Workspace:
    """@brief 通过真实用例创建测试 Workspace / Create a test workspace through the real use case.

    @param service 被测服务 / Service under test.
    @param user 初始 owner / Initial owner.
    @return 已创建 Workspace / Created workspace.
    """
    return await service.create_workspace(
        _principal(user, "workspace.read", "workspace.write"),
        CreateWorkspaceCommand("Klee Lab", "klee-lab", DataRegion.CN),
    )
