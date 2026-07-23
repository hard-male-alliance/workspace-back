"""@brief API v2 访问领域核心单测 / API v2 access-domain core tests."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from backend.application.ports.access import AccessAuthorizer, AuthorizationDenied
from backend.domain.principals import (
    ClientId,
    DomainInvariantError,
    InvitationId,
    MembershipId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.users import (
    AccountDeletionFailure,
    AccountDeletionId,
    AccountDeletionStatus,
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
    SoleOwnerViolation,
    Workspace,
    WorkspacePlan,
    WorkspaceRole,
)

NOW = datetime(2026, 7, 23, 10, tzinfo=UTC)
"""@brief 测试固定时钟 / Fixed test clock."""

USER_ID = UserId("usr_klee0001")
"""@brief 测试用户标识 / Test user identifier."""

WORKSPACE_ID = WorkspaceId("ws_klee0001")
"""@brief 测试 Workspace 标识 / Test workspace identifier."""


def _meta[IdT: str](identifier: IdT) -> ResourceMeta[IdT]:
    """@brief 创建测试资源元数据 / Build test resource metadata.

    @param identifier 领域专用标识 / Domain-specific identifier.
    @return revision 1 元数据 / Revision-one metadata.
    """
    return ResourceMeta(identifier, 1, NOW, NOW)


def _user() -> User:
    """@brief 创建测试用户 / Build a test user.

    @return 有效用户 / Valid user.
    """
    return User(
        _meta(USER_ID),
        Subject("oidc-klee0001"),
        "klee@example.cn",
        True,
        "Klee",
        "zh-CN",
        WORKSPACE_ID,
    )


def _membership(
    role: WorkspaceRole = WorkspaceRole.OWNER,
    status: MemberStatus = MemberStatus.ACTIVE,
) -> Membership:
    """@brief 创建测试成员关系 / Build a test membership.

    @param role 测试角色 / Test role.
    @param status 测试状态 / Test status.
    @return 有效成员关系 / Valid membership.
    """
    return Membership(
        _meta(MembershipId("mem_klee0001")),
        WORKSPACE_ID,
        USER_ID,
        "Klee",
        role,
        status,
    )


class _AccessReadRepository:
    """@brief authorizer 测试所需最小 Repository / Minimal repository for authorizer tests."""

    def __init__(self, user: User | None, membership: Membership | None) -> None:
        """@brief 初始化测试 Repository / Initialize the test repository.

        @param user subject 查询结果 / Subject lookup result.
        @param membership 成员查询结果 / Membership lookup result.
        """
        self._user = user
        self._membership = membership

    async def get_user(self, user_id: UserId) -> User | None:
        """@brief 返回匹配本地标识的用户 / Return the matching local user.

        @param user_id token 内已签名的本地用户标识 / Signed local user ID from the token.
        @return 用户或不存在 / User when matched.
        """
        if self._user is not None and self._user.meta.id == user_id:
            return self._user
        return None

    async def get_membership(self, workspace_id: WorkspaceId, user_id: UserId) -> Membership | None:
        """@brief 返回精确租户成员关系 / Return an exact tenant membership.

        @param workspace_id 路径 Workspace / Path workspace.
        @param user_id actor 用户 / Actor user.
        @return 成员关系或不存在 / Membership when matched.
        """
        membership = self._membership
        if (
            membership is not None
            and membership.workspace_id == workspace_id
            and membership.user_id == user_id
        ):
            return membership
        return None


def _principal(*scopes: str) -> TokenPrincipal:
    """@brief 创建测试 token principal / Build a test token principal.

    @param scopes token scope 字符串 / Token scope strings.
    @return 已验证 token 投影 / Verified-token projection.
    """
    return TokenPrincipal(
        USER_ID,
        Subject("oidc-klee0001"),
        ClientId("web-client"),
        frozenset(Scope(scope) for scope in scopes),
    )


def test_resource_meta_is_composed_typed_and_monotonic() -> None:
    """@brief 元数据组合保持不可变与单调 revision / Metadata composition stays immutable and monotonic."""
    workspace = Workspace(
        _meta(WORKSPACE_ID),
        "Klee Workspace",
        "klee-workspace",
        WorkspacePlan.PERSONAL,
        DataRegion.CN,
    )
    revised = workspace.revise(name="Klee Lab", slug="klee-lab", updated_at=NOW + timedelta(1))
    assert revised.meta.id == WORKSPACE_ID
    assert revised.meta.revision == 2
    with pytest.raises(FrozenInstanceError):
        workspace.name = "mutated"  # type: ignore[misc]


def test_ordinary_member_patch_cannot_create_owner() -> None:
    """@brief 普通 PATCH 不能把非 owner 提升为 owner / Ordinary PATCH cannot create an owner."""
    member = _membership(WorkspaceRole.EDITOR)
    with pytest.raises(DomainInvariantError, match="cannot grant"):
        member.revise(
            role=WorkspaceRole.OWNER,
            status=None,
            active_owner_count=1,
            updated_at=NOW + timedelta(1),
        )


def test_sole_active_owner_cannot_be_demoted_suspended_or_removed() -> None:
    """@brief 唯一活跃 owner 不可降级、暂停或删除 / Sole active owner is protected."""
    owner = _membership()
    with pytest.raises(SoleOwnerViolation):
        owner.revise(
            role=WorkspaceRole.ADMIN,
            status=None,
            active_owner_count=1,
            updated_at=NOW + timedelta(1),
        )
    with pytest.raises(SoleOwnerViolation):
        owner.revise(
            role=None,
            status=MemberStatus.SUSPENDED,
            active_owner_count=1,
            updated_at=NOW + timedelta(1),
        )
    with pytest.raises(SoleOwnerViolation):
        owner.ensure_removable(active_owner_count=1)


def test_one_of_multiple_owners_may_be_demoted() -> None:
    """@brief 多 owner 时允许安全降级其中一个 / One of multiple owners may be demoted."""
    demoted = _membership().revise(
        role=WorkspaceRole.ADMIN,
        status=None,
        active_owner_count=2,
        updated_at=NOW + timedelta(1),
    )
    assert demoted.role is WorkspaceRole.ADMIN
    assert demoted.meta.revision == 2


def test_invitation_state_machine_is_single_use_recipient_bound_and_expiring() -> None:
    """@brief 邀请仅匹配收件人、单次且到期关闭 / Invitation is recipient-bound, single-use, and expiring."""
    invitation = Invitation(
        _meta(InvitationId("inv_klee0001")),
        WORKSPACE_ID,
        "klee@example.cn",
        WorkspaceRole.EDITOR,
        InvitationStatus.PENDING,
        NOW + timedelta(days=7),
    )
    with pytest.raises(InvalidStateTransition, match="recipient"):
        invitation.accept(
            user_id=USER_ID,
            actor_email="alice@example.cn",
            accepted_at=NOW + timedelta(hours=1),
        )
    accepted = invitation.accept(
        user_id=USER_ID,
        actor_email="KLEE@example.cn",
        accepted_at=NOW + timedelta(hours=1),
    )
    assert accepted.status is InvitationStatus.ACCEPTED
    with pytest.raises(InvalidStateTransition, match="pending"):
        accepted.revoke(NOW + timedelta(hours=2))
    with pytest.raises(DomainInvariantError, match="owner"):
        replace(invitation, role=WorkspaceRole.OWNER)


def test_invitation_cannot_expire_early_or_accept_at_expiry() -> None:
    """@brief 邀请到期边界为半开区间 / Invitation validity uses a half-open interval."""
    expires_at = NOW + timedelta(days=1)
    invitation = Invitation(
        _meta(InvitationId("inv_expire001")),
        WORKSPACE_ID,
        "klee@example.cn",
        WorkspaceRole.VIEWER,
        InvitationStatus.PENDING,
        expires_at,
    )
    with pytest.raises(InvalidStateTransition, match="before"):
        invitation.expire(NOW + timedelta(hours=2))
    with pytest.raises(InvalidStateTransition, match="expired"):
        invitation.accept(user_id=USER_ID, actor_email=invitation.email, accepted_at=expires_at)
    assert invitation.expire(expires_at).status is InvitationStatus.EXPIRED


def test_account_deletion_has_only_contract_state_transitions() -> None:
    """@brief 删除请求只允许 scheduled→running→terminal / Deletion request permits only its state graph."""
    scheduled = ScheduledAccountDeletion(
        _meta(AccountDeletionId("del_klee0001")),
        USER_ID,
        NOW + timedelta(days=14),
    )
    assert scheduled.cancel(NOW + timedelta(days=1)).status is AccountDeletionStatus.CANCELLED
    with pytest.raises(DomainInvariantError, match="before scheduled"):
        scheduled.start(NOW + timedelta(days=13))
    running = scheduled.start(NOW + timedelta(days=14))
    completed = running.complete(NOW + timedelta(days=14, minutes=1))
    assert completed.status is AccountDeletionStatus.COMPLETED
    failed = running.fail(
        AccountDeletionFailure("deletion.retention_failed", "Required retention failed"),
        NOW + timedelta(days=14, minutes=1),
    )
    assert failed.status is AccountDeletionStatus.FAILED
    assert not hasattr(completed, "failure")
    assert not hasattr(failed, "completed_at")


@pytest.mark.asyncio
async def test_authorizer_intersects_scope_role_and_active_membership() -> None:
    """@brief authorizer 计算 scope∩role∩active-membership / Authorizer intersects scope, role, and active membership."""
    repository = cast("object", _AccessReadRepository(_user(), _membership(WorkspaceRole.ADMIN)))
    authorizer = AccessAuthorizer(cast("object", repository))  # type: ignore[arg-type]
    actor = await authorizer.authenticate(_principal("workspace.read", "workspace.write"))
    context = await authorizer.authorize(actor, WORKSPACE_ID, WorkspaceAction.UPDATE_MEMBER)
    assert context.actor == actor
    assert context.workspace_id == WORKSPACE_ID
    assert context.action is WorkspaceAction.UPDATE_MEMBER


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("membership", "scopes", "action", "reason"),
    [
        (None, ("workspace.read",), WorkspaceAction.READ, "membership_missing"),
        (
            _membership(WorkspaceRole.ADMIN, MemberStatus.SUSPENDED),
            ("workspace.write",),
            WorkspaceAction.UPDATE,
            "membership_inactive",
        ),
        (_membership(), ("workspace.read",), WorkspaceAction.DELETE, "scope_missing"),
        (
            _membership(WorkspaceRole.EDITOR),
            ("workspace.write",),
            WorkspaceAction.UPDATE,
            "role_denied",
        ),
    ],
)
async def test_authorizer_is_deny_first(
    membership: Membership | None,
    scopes: tuple[str, ...],
    action: WorkspaceAction,
    reason: str,
) -> None:
    """@brief 缺少任一交集条件均拒绝 / Missing any intersection operand denies access.

    @param membership 测试成员关系 / Test membership.
    @param scopes 测试 token scopes / Test token scopes.
    @param action 测试操作 / Test action.
    @param reason 预期稳定拒绝原因片段 / Expected stable denial fragment.
    """
    repository = cast("object", _AccessReadRepository(_user(), membership))
    authorizer = AccessAuthorizer(cast("object", repository))  # type: ignore[arg-type]
    actor = await authorizer.authenticate(_principal(*scopes))
    with pytest.raises(AuthorizationDenied, match=reason):
        await authorizer.authorize(actor, WORKSPACE_ID, action)


@pytest.mark.asyncio
async def test_unknown_action_defaults_to_denied() -> None:
    """@brief 未配置的新操作默认拒绝 / An unconfigured new action is denied by default."""
    repository = cast("object", _AccessReadRepository(_user(), _membership()))
    authorizer = AccessAuthorizer(cast("object", repository))  # type: ignore[arg-type]
    actor = await authorizer.authenticate(_principal("workspace.read", "workspace.write"))
    unknown = cast(WorkspaceAction, "workspace.future-dangerous-action")
    with pytest.raises(AuthorizationDenied, match="policy_missing"):
        await authorizer.authorize(actor, WORKSPACE_ID, unknown)


def test_workspace_context_constructor_is_sealed() -> None:
    """@brief 调用方不能自行制造 Workspace 上下文 / Callers cannot forge workspace context."""
    actor = cast("object", object())
    with pytest.raises(TypeError, match="only be issued"):
        WorkspaceAccessContext(
            cast("object", actor),  # type: ignore[arg-type]
            WORKSPACE_ID,
            MembershipId("mem_klee0001"),
            WorkspaceRole.OWNER,
            WorkspaceAction.READ,
            _seal=object(),
        )
