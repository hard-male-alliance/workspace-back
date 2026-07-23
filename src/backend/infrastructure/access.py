"""@brief API v2 用户与 Workspace 持久化适配器 / API v2 user/workspace persistence adapters.

内存实现采用 copy-on-write 工作单元，因此未提交写入和异常路径不会污染共享状态。
PostgreSQL 实现把单个 Repository 固定绑定到一个 ``AsyncSession``，并在计数 owner
之前锁定 Workspace 根行，使“最后一个 owner”检查与后续成员更新共享同一串行化点。
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import Protocol, Self

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from backend.application.ports.access import AccessRepository
from backend.domain.principals import (
    InvitationId,
    MembershipId,
    ResourceMeta,
    Subject,
    UserId,
    WorkspaceId,
)
from backend.domain.users import (
    AccountDeletion,
    AccountDeletionFailure,
    AccountDeletionId,
    AccountDeletionStatus,
    AccountStatus,
    CancelledAccountDeletion,
    CompletedAccountDeletion,
    FailedAccountDeletion,
    RunningAccountDeletion,
    ScheduledAccountDeletion,
    User,
)
from backend.domain.workspaces import (
    DataRegion,
    Invitation,
    InvitationStatus,
    Membership,
    MemberStatus,
    Workspace,
    WorkspacePlan,
    WorkspaceRole,
)
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    AccountDeletionRequestRecord,
    UserRecord,
    WorkspaceInvitationRecord,
    WorkspaceMemberRecord,
    WorkspaceRecord,
)


class InMemoryAccessStore:
    """@brief 身份服务与 Access UoW 共享的进程内状态 / Shared in-process identity/access state.

    @param lock 可选共享互斥锁；身份注册必须复用同一把锁 / Optional shared lock; identity
        provisioning must reuse the same lock.
    """

    def __init__(self, *, lock: asyncio.Lock | None = None) -> None:
        """@brief 初始化空状态 / Initialize empty state.

        @param lock 可选共享互斥锁 / Optional shared mutex.
        """
        self.lock = lock or asyncio.Lock()
        self.users: dict[str, User] = {}
        self.workspaces: dict[str, Workspace] = {}
        self.memberships: dict[str, Membership] = {}
        self.invitations: dict[str, Invitation] = {}
        self.account_deletions: dict[str, AccountDeletion] = {}


class _RevisionRecord(Protocol):
    """@brief ResourceMeta 映射需要的 ORM 行形状 / ORM row shape required by ResourceMeta mapping."""

    revision: int
    created_at: datetime
    updated_at: datetime


class InMemoryAccessRepository:
    """@brief 绑定一个 UoW 快照的 Access Repository / Access repository bound to one UoW snapshot.

    @note 构造器只供 ``InMemoryAccessUnitOfWork`` 和原子 identity provision 使用。
        / Construction is reserved for the unit of work and atomic identity provisioning.
    """

    def __init__(
        self,
        *,
        users: dict[str, User],
        workspaces: dict[str, Workspace],
        memberships: dict[str, Membership],
        invitations: dict[str, Invitation],
        account_deletions: dict[str, AccountDeletion],
    ) -> None:
        """@brief 绑定隔离字典 / Bind isolated dictionaries.

        @param users 用户聚合字典 / User aggregate dictionary.
        @param workspaces Workspace 聚合字典 / Workspace aggregate dictionary.
        @param memberships 成员关系字典 / Membership dictionary.
        @param invitations 邀请字典 / Invitation dictionary.
        @param account_deletions 删除请求字典 / Account-deletion dictionary.
        """
        self._users = users
        self._workspaces = workspaces
        self._memberships = memberships
        self._invitations = invitations
        self._account_deletions = account_deletions

    async def get_user(self, user_id: UserId) -> User | None:
        """@brief 按 ID 读取用户 / Read a user by ID.

        @param user_id 用户 ID / User ID.
        @return 用户或不存在 / User when present.
        """
        return self._users.get(str(user_id))

    async def save_user(self, user: User) -> None:
        """@brief 保存用户聚合 / Save a user aggregate.

        @param user 已验证用户 / Validated user.
        """
        self._users[str(user.meta.id)] = user

    async def list_workspace_access(
        self, user_id: UserId
    ) -> Sequence[tuple[Workspace, Membership]]:
        """@brief 列出活动 Workspace 访问关系 / List active Workspace access relations.

        @param user_id 用户 ID / User ID.
        @return 按创建时间和 ID 排序的 Workspace/成员关系 / Workspace-membership pairs ordered
            by creation time and ID.
        """
        pairs = [
            (workspace, membership)
            for membership in self._memberships.values()
            if membership.user_id == user_id and membership.status is MemberStatus.ACTIVE
            if (workspace := self._workspaces.get(str(membership.workspace_id))) is not None
        ]
        return sorted(pairs, key=lambda pair: (pair[0].meta.created_at, pair[0].meta.id))

    async def get_workspace(self, workspace_id: WorkspaceId) -> Workspace | None:
        """@brief 按 ID 读取 Workspace / Read a Workspace by ID.

        @param workspace_id Workspace ID / Workspace ID.
        @return Workspace 或不存在 / Workspace when present.
        """
        return self._workspaces.get(str(workspace_id))

    async def add_workspace(self, workspace: Workspace, owner: Membership) -> None:
        """@brief 添加 Workspace 与初始 owner / Add a Workspace and initial owner.

        @param workspace 新 Workspace / New Workspace.
        @param owner 初始 owner 关系 / Initial owner membership.
        @raise ValueError owner 与 Workspace 不一致时抛出 / Raised for an inconsistent owner.
        """
        if (
            owner.workspace_id != workspace.meta.id
            or owner.role is not WorkspaceRole.OWNER
            or owner.status is not MemberStatus.ACTIVE
        ):
            raise ValueError("new workspace requires its matching active owner")
        self._workspaces[str(workspace.meta.id)] = workspace
        self._memberships[str(owner.meta.id)] = owner

    async def save_workspace(self, workspace: Workspace) -> None:
        """@brief 保存 Workspace / Save a Workspace.

        @param workspace 已验证 Workspace / Validated Workspace.
        """
        self._workspaces[str(workspace.meta.id)] = workspace

    async def delete_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 删除 Workspace 及其局部关系 / Delete a Workspace and local relations.

        @param workspace_id Workspace ID / Workspace ID.
        """
        removed_at = datetime.now(UTC)
        member_user_ids = (
            membership.user_id
            for membership in self._memberships.values()
            if membership.workspace_id == workspace_id
        )
        for user_id in member_user_ids:
            self._clear_default_workspace(user_id, workspace_id, updated_at=removed_at)
        self._workspaces.pop(str(workspace_id), None)
        self._memberships = {
            key: value
            for key, value in self._memberships.items()
            if value.workspace_id != workspace_id
        }
        self._invitations = {
            key: value
            for key, value in self._invitations.items()
            if value.workspace_id != workspace_id
        }

    async def get_membership(self, workspace_id: WorkspaceId, user_id: UserId) -> Membership | None:
        """@brief 按 Workspace 与用户读取成员关系 / Read membership by Workspace and user.

        @param workspace_id Workspace ID / Workspace ID.
        @param user_id 用户 ID / User ID.
        @return 成员关系或不存在 / Membership when present.
        """
        return next(
            (
                item
                for item in self._memberships.values()
                if item.workspace_id == workspace_id and item.user_id == user_id
            ),
            None,
        )

    async def get_membership_by_id(
        self, workspace_id: WorkspaceId, membership_id: MembershipId
    ) -> Membership | None:
        """@brief 在 Workspace 内按 ID 读取成员关系 / Read membership by ID within a Workspace.

        @param workspace_id Workspace ID / Workspace ID.
        @param membership_id 成员关系 ID / Membership ID.
        @return 成员关系或不存在 / Membership when present.
        """
        item = self._memberships.get(str(membership_id))
        return item if item is not None and item.workspace_id == workspace_id else None

    async def list_members(self, workspace_id: WorkspaceId) -> Sequence[Membership]:
        """@brief 列出 Workspace 成员 / List Workspace members.

        @param workspace_id Workspace ID / Workspace ID.
        @return 稳定排序成员 / Stably ordered memberships.
        """
        return sorted(
            (item for item in self._memberships.values() if item.workspace_id == workspace_id),
            key=lambda item: (item.meta.created_at, item.meta.id),
        )

    async def count_active_owners(self, workspace_id: WorkspaceId) -> int:
        """@brief 计数活动 owner / Count active owners.

        @param workspace_id Workspace ID / Workspace ID.
        @return 活动 owner 数 / Active-owner count.
        """
        return sum(
            item.workspace_id == workspace_id
            and item.role is WorkspaceRole.OWNER
            and item.status is MemberStatus.ACTIVE
            for item in self._memberships.values()
        )

    async def add_membership(self, membership: Membership) -> None:
        """@brief 添加成员关系 / Add a membership.

        @param membership 新成员关系 / New membership.
        """
        self._memberships[str(membership.meta.id)] = membership

    async def save_membership(self, membership: Membership) -> None:
        """@brief 保存成员关系 / Save a membership.

        @param membership 已修改成员关系 / Revised membership.
        """
        self._memberships[str(membership.meta.id)] = membership
        if membership.status is MemberStatus.SUSPENDED:
            self._clear_default_workspace(
                membership.user_id,
                membership.workspace_id,
                updated_at=membership.meta.updated_at,
            )

    async def delete_membership(self, membership: Membership) -> None:
        """@brief 删除成员关系 / Delete a membership.

        @param membership 已检查 owner 不变量的关系 / Membership checked for owner invariants.
        """
        self._memberships.pop(str(membership.meta.id), None)
        self._clear_default_workspace(
            membership.user_id,
            membership.workspace_id,
            updated_at=max(membership.meta.updated_at, datetime.now(UTC)),
        )

    def _clear_default_workspace(
        self,
        user_id: UserId,
        workspace_id: WorkspaceId,
        *,
        updated_at: datetime,
    ) -> None:
        """@brief 清理已失效成员的 UI 默认 Workspace / Clear an inactive member's UI default.

        @param user_id 失效成员用户 / Inactive member user.
        @param workspace_id 失去访问权的 Workspace / Workspace whose access was lost.
        @param updated_at 与成员命令共享的修改时刻 / Modification instant shared with the
            membership command.
        """
        user = self._users.get(str(user_id))
        if user is None or user.default_workspace_id != workspace_id:
            return
        self._users[str(user_id)] = user.revise_profile(
            display_name=user.display_name,
            locale=user.locale,
            default_workspace_id=None,
            updated_at=updated_at,
        )

    async def get_invitation(
        self, workspace_id: WorkspaceId, invitation_id: InvitationId
    ) -> Invitation | None:
        """@brief 在 Workspace 内读取邀请 / Read an invitation within a Workspace.

        @param workspace_id Workspace ID / Workspace ID.
        @param invitation_id 邀请 ID / Invitation ID.
        @return 邀请或不存在 / Invitation when present.
        """
        item = self._invitations.get(str(invitation_id))
        return item if item is not None and item.workspace_id == workspace_id else None

    async def list_invitations(self, workspace_id: WorkspaceId) -> Sequence[Invitation]:
        """@brief 列出 Workspace 邀请 / List Workspace invitations.

        @param workspace_id Workspace ID / Workspace ID.
        @return 稳定排序邀请 / Stably ordered invitations.
        """
        return sorted(
            (item for item in self._invitations.values() if item.workspace_id == workspace_id),
            key=lambda item: (item.meta.created_at, item.meta.id),
        )

    async def add_invitation(self, invitation: Invitation) -> None:
        """@brief 添加邀请 / Add an invitation.

        @param invitation pending 邀请 / Pending invitation.
        """
        self._invitations[str(invitation.meta.id)] = invitation

    async def save_invitation(self, invitation: Invitation) -> None:
        """@brief 保存邀请 / Save an invitation.

        @param invitation 已迁移邀请 / Transitioned invitation.
        """
        self._invitations[str(invitation.meta.id)] = invitation

    async def get_account_deletion(
        self, user_id: UserId, request_id: AccountDeletionId
    ) -> AccountDeletion | None:
        """@brief 读取用户自己的账户删除请求 / Read a user's account-deletion request.

        @param user_id 用户 ID / User ID.
        @param request_id 请求 ID / Request ID.
        @return 请求或不存在 / Request when present.
        """
        item = self._account_deletions.get(str(request_id))
        return item if item is not None and item.user_id == user_id else None

    async def add_account_deletion(self, request: AccountDeletion) -> None:
        """@brief 添加账户删除请求 / Add an account-deletion request.

        @param request scheduled 请求 / Scheduled request.
        """
        self._account_deletions[str(request.meta.id)] = request

    async def save_account_deletion(self, request: AccountDeletion) -> None:
        """@brief 保存账户删除请求 / Save an account-deletion request.

        @param request 已迁移请求 / Transitioned request.
        """
        self._account_deletions[str(request.meta.id)] = request


class InMemoryAccessUnitOfWork:
    """@brief copy-on-write 内存工作单元 / Copy-on-write in-memory unit of work."""

    def __init__(self, store: InMemoryAccessStore) -> None:
        """@brief 绑定共享状态 / Bind shared state.

        @param store 共享 Access 状态 / Shared Access state.
        """
        self._store = store
        self._repository: InMemoryAccessRepository | None = None
        self._entered = False
        self._committed = False
        self._rolled_back = False

    @property
    def repository(self) -> AccessRepository:
        """@brief 返回事务绑定 Repository / Return the transaction-bound repository.

        @return 当前快照 Repository / Current snapshot repository.
        @raise RuntimeError 工作单元尚未进入时抛出 / Raised before entering the unit of work.
        """
        if self._repository is None:
            raise RuntimeError("access unit of work has not been entered")
        return self._repository

    async def __aenter__(self) -> Self:
        """@brief 获取锁并复制共享状态 / Acquire the lock and copy shared state.

        @return 当前工作单元 / Current unit of work.
        """
        if self._entered:
            raise RuntimeError("access unit of work cannot be re-entered")
        await self._store.lock.acquire()
        self._entered = True
        self._repository = InMemoryAccessRepository(
            users=dict(self._store.users),
            workspaces=dict(self._store.workspaces),
            memberships=dict(self._store.memberships),
            invitations=dict(self._store.invitations),
            account_deletions=dict(self._store.account_deletions),
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 未提交时丢弃快照并释放锁 / Discard uncommitted state and release the lock.

        @param exc_type 异常类型 / Exception type.
        @param exc 异常 / Exception instance.
        @param traceback traceback / Traceback.
        @return 不吞异常 / Does not suppress exceptions.
        """
        if self._entered:
            if exc_type is not None or not self._committed:
                await self.rollback()
            self._entered = False
            self._repository = None
            self._store.lock.release()
        return None

    async def commit(self) -> None:
        """@brief 原子发布快照 / Atomically publish the snapshot.

        @raise RuntimeError 工作单元未进入或重复提交时抛出 / Raised before entry or on repeat.
        """
        repository = self._require_repository()
        if self._committed:
            raise RuntimeError("access unit of work is already committed")
        if self._rolled_back:
            raise RuntimeError("rolled-back access unit of work cannot commit")
        self._store.users = dict(repository._users)
        self._store.workspaces = dict(repository._workspaces)
        self._store.memberships = dict(repository._memberships)
        self._store.invitations = dict(repository._invitations)
        self._store.account_deletions = dict(repository._account_deletions)
        self._committed = True

    async def rollback(self) -> None:
        """@brief 丢弃未发布快照 / Discard the unpublished snapshot."""
        self._require_repository()
        self._rolled_back = True

    def _require_repository(self) -> InMemoryAccessRepository:
        """@brief 要求工作单元已进入 / Require an entered unit of work.

        @return 当前内存 Repository / Current in-memory repository.
        @raise RuntimeError 尚未进入时抛出 / Raised before entry.
        """
        if self._repository is None:
            raise RuntimeError("access unit of work has not been entered")
        return self._repository


class InMemoryAccessUnitOfWorkFactory:
    """@brief 创建共享同一内存状态的 UoW / Create UoWs sharing one in-memory state."""

    def __init__(self, store: InMemoryAccessStore | None = None) -> None:
        """@brief 绑定或创建状态 / Bind or create state.

        @param store 可选共享状态 / Optional shared state.
        """
        self.store = store or InMemoryAccessStore()

    def __call__(self) -> InMemoryAccessUnitOfWork:
        """@brief 创建一个新工作单元 / Create a new unit of work.

        @return 未进入的工作单元 / Not-yet-entered unit of work.
        """
        return InMemoryAccessUnitOfWork(self.store)


class PostgresAccessRepository:
    """@brief 绑定一个 PostgreSQL 事务的 Access Repository / Transaction-bound PostgreSQL Access repository.

    @param session 当前命令独占的异步 Session / Async session exclusively owned by one command.
    """

    def __init__(self, session: AsyncSession) -> None:
        """@brief 绑定 Session / Bind the session.

        @param session 活动事务 Session / Active transaction session.
        """
        self._session = session
        self._actor_id: str | None = None
        self._workspace_id: str | None = None
        self._read_workspace_revisions: dict[str, int] = {}

    async def get_user(self, user_id: UserId) -> User | None:
        """@brief 按 ID 读取用户 / Read a user by ID.

        @param user_id 用户 ID / User ID.
        @return 用户或不存在 / User when present.
        """
        await self._install_actor(str(user_id))
        record = await self._session.scalar(
            select(UserRecord).where(
                UserRecord.id == str(user_id),
                UserRecord.account_status.in_(("active", "deletion_scheduled")),
            )
        )
        return _user_from_record(record)

    async def save_user(self, user: User) -> None:
        """@brief 以 revision 锁定并保存用户 / Lock and save a user by revision.

        @param user 下一版本用户 / Next user revision.
        """
        await self._install_actor(str(user.meta.id))
        record = await self._locked_record(UserRecord, str(user.meta.id))
        _require_previous_revision(record.revision, user.meta.revision)
        record.display_name = user.display_name
        record.locale = user.locale
        record.default_workspace_id = (
            str(user.default_workspace_id) if user.default_workspace_id is not None else None
        )
        record.account_status = user.account_status.value
        record.updated_at = user.meta.updated_at
        record.revision = user.meta.revision

    async def list_workspace_access(
        self, user_id: UserId
    ) -> Sequence[tuple[Workspace, Membership]]:
        """@brief 列出用户的活动 Workspace 关系 / List a user's active Workspace relations.

        @param user_id 用户 ID / User ID.
        @return Workspace 与成员关系对 / Workspace-membership pairs.
        """
        await self._install_actor(str(user_id))
        rows = (
            await self._session.execute(
                select(WorkspaceRecord, WorkspaceMemberRecord)
                .join(
                    WorkspaceMemberRecord,
                    WorkspaceMemberRecord.workspace_id == WorkspaceRecord.id,
                )
                .where(
                    WorkspaceMemberRecord.user_id == str(user_id),
                    WorkspaceMemberRecord.status == MemberStatus.ACTIVE.value,
                    WorkspaceRecord.deleted_at.is_(None),
                )
                .order_by(WorkspaceRecord.created_at, WorkspaceRecord.id)
            )
        ).all()
        return [
            (_workspace_from_record(workspace), _membership_from_record(member))
            for workspace, member in rows
        ]

    async def get_workspace(self, workspace_id: WorkspaceId) -> Workspace | None:
        """@brief 按 ID 读取未删除 Workspace / Read a non-deleted Workspace by ID.

        @param workspace_id Workspace ID / Workspace ID.
        @return Workspace 或不存在 / Workspace when present.
        """
        await self._install_workspace(str(workspace_id))
        record = await self._session.scalar(
            select(WorkspaceRecord).where(
                WorkspaceRecord.id == str(workspace_id), WorkspaceRecord.deleted_at.is_(None)
            )
        )
        if record is None:
            return None
        self._read_workspace_revisions[record.id] = record.revision
        return _workspace_from_record(record)

    async def add_workspace(self, workspace: Workspace, owner: Membership) -> None:
        """@brief 添加 Workspace 与初始 owner / Add a Workspace and initial owner.

        @param workspace 新 Workspace / New Workspace.
        @param owner 初始 owner / Initial owner.
        """
        if (
            owner.workspace_id != workspace.meta.id
            or owner.role is not WorkspaceRole.OWNER
            or owner.status is not MemberStatus.ACTIVE
        ):
            raise ValueError("new workspace requires its matching active owner")
        await self._install_actor(str(owner.user_id))
        await self._install_workspace(str(workspace.meta.id))
        self._session.add(_workspace_record(workspace, resource_owner_id=str(owner.user_id)))
        self._session.add(_membership_record(owner, resource_owner_id=str(owner.user_id)))

    async def save_workspace(self, workspace: Workspace) -> None:
        """@brief 保存 Workspace / Save a Workspace.

        @param workspace 下一版本 Workspace / Next Workspace revision.
        """
        await self._install_workspace(str(workspace.meta.id))
        record = await self._locked_record(WorkspaceRecord, str(workspace.meta.id))
        _require_previous_revision(record.revision, workspace.meta.revision)
        record.name = workspace.name
        record.slug = workspace.slug
        record.updated_at = workspace.meta.updated_at
        record.revision = workspace.meta.revision

    async def delete_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 软删除 Workspace 根 / Soft-delete a Workspace root.

        @param workspace_id Workspace ID / Workspace ID.
        """
        await self._install_workspace(str(workspace_id))
        record = await self._locked_record(WorkspaceRecord, str(workspace_id))
        expected_revision = self._read_workspace_revisions.get(str(workspace_id))
        if expected_revision is None:
            raise RuntimeError("workspace must be read before deletion")
        _require_same_revision(record.revision, expected_revision)
        now = datetime.now(UTC)
        member_user_ids = await self._session.scalars(
            select(WorkspaceMemberRecord.user_id).where(
                WorkspaceMemberRecord.workspace_id == str(workspace_id)
            )
        )
        for user_id in member_user_ids:
            await self._clear_default_workspace(UserId(user_id), workspace_id)
        record.deleted_at = now
        record.updated_at = now
        record.revision += 1

    async def get_membership(self, workspace_id: WorkspaceId, user_id: UserId) -> Membership | None:
        """@brief 按 Workspace 与用户读取成员关系 / Read membership by Workspace and user.

        @param workspace_id Workspace ID / Workspace ID.
        @param user_id 用户 ID / User ID.
        @return 成员关系或不存在 / Membership when present.
        """
        await self._install_workspace(str(workspace_id))
        record = await self._session.scalar(
            select(WorkspaceMemberRecord).where(
                WorkspaceMemberRecord.workspace_id == str(workspace_id),
                WorkspaceMemberRecord.user_id == str(user_id),
            )
        )
        return _membership_from_record(record) if record is not None else None

    async def get_membership_by_id(
        self, workspace_id: WorkspaceId, membership_id: MembershipId
    ) -> Membership | None:
        """@brief 在 Workspace 内按 ID 读取成员 / Read membership by ID within a Workspace.

        @param workspace_id Workspace ID / Workspace ID.
        @param membership_id 成员关系 ID / Membership ID.
        @return 成员关系或不存在 / Membership when present.
        """
        await self._install_workspace(str(workspace_id))
        record = await self._session.scalar(
            select(WorkspaceMemberRecord).where(
                WorkspaceMemberRecord.workspace_id == str(workspace_id),
                WorkspaceMemberRecord.id == str(membership_id),
            )
        )
        return _membership_from_record(record) if record is not None else None

    async def list_members(self, workspace_id: WorkspaceId) -> Sequence[Membership]:
        """@brief 列出 Workspace 成员 / List Workspace members.

        @param workspace_id Workspace ID / Workspace ID.
        @return 稳定排序成员 / Stably ordered memberships.
        """
        await self._install_workspace(str(workspace_id))
        records = (
            await self._session.scalars(
                select(WorkspaceMemberRecord)
                .where(WorkspaceMemberRecord.workspace_id == str(workspace_id))
                .order_by(WorkspaceMemberRecord.created_at, WorkspaceMemberRecord.id)
            )
        ).all()
        return [_membership_from_record(record) for record in records]

    async def count_active_owners(self, workspace_id: WorkspaceId) -> int:
        """@brief 锁定 Workspace 后计数活动 owner / Lock the Workspace then count active owners.

        @param workspace_id Workspace ID / Workspace ID.
        @return 活动 owner 数 / Active-owner count.

        @note 所有 owner 降级、停用与删除命令必须先调用本方法；Workspace 根行是并发
            命令共享的串行化点 / All owner demotion, suspension, and removal commands must call
            this method first; the Workspace root row is their shared serialization point.
        """
        await self._install_workspace(str(workspace_id))
        await self._session.scalar(
            select(WorkspaceRecord.id)
            .where(WorkspaceRecord.id == str(workspace_id))
            .with_for_update()
        )
        count = await self._session.scalar(
            select(func.count())
            .select_from(WorkspaceMemberRecord)
            .where(
                WorkspaceMemberRecord.workspace_id == str(workspace_id),
                WorkspaceMemberRecord.role == WorkspaceRole.OWNER.value,
                WorkspaceMemberRecord.status == MemberStatus.ACTIVE.value,
            )
        )
        return int(count or 0)

    async def add_membership(self, membership: Membership) -> None:
        """@brief 添加成员关系 / Add a membership.

        @param membership 新成员关系 / New membership.
        """
        await self._install_workspace(str(membership.workspace_id))
        owner_id = await self._workspace_owner_id(membership.workspace_id)
        self._session.add(_membership_record(membership, resource_owner_id=owner_id))

    async def save_membership(self, membership: Membership) -> None:
        """@brief 保存成员关系 / Save a membership.

        @param membership 下一版本成员关系 / Next membership revision.
        """
        await self._install_workspace(str(membership.workspace_id))
        record = await self._locked_record(WorkspaceMemberRecord, str(membership.meta.id))
        _require_previous_revision(record.revision, membership.meta.revision)
        record.role = membership.role.value
        record.status = membership.status.value
        record.updated_at = membership.meta.updated_at
        record.revision = membership.meta.revision
        if membership.status is MemberStatus.SUSPENDED:
            await self._clear_default_workspace(membership.user_id, membership.workspace_id)

    async def delete_membership(self, membership: Membership) -> None:
        """@brief 删除成员关系 / Delete a membership.

        @param membership 已检查 owner 不变量的关系 / Membership checked for owner invariants.
        """
        await self._install_workspace(str(membership.workspace_id))
        record = await self._locked_record(WorkspaceMemberRecord, str(membership.meta.id))
        _require_same_revision(record.revision, membership.meta.revision)
        await self._session.delete(record)
        await self._clear_default_workspace(membership.user_id, membership.workspace_id)

    async def get_invitation(
        self, workspace_id: WorkspaceId, invitation_id: InvitationId
    ) -> Invitation | None:
        """@brief 在 Workspace 内读取邀请 / Read an invitation within a Workspace.

        @param workspace_id Workspace ID / Workspace ID.
        @param invitation_id 邀请 ID / Invitation ID.
        @return 邀请或不存在 / Invitation when present.
        """
        await self._install_workspace(str(workspace_id))
        record = await self._session.scalar(
            select(WorkspaceInvitationRecord).where(
                WorkspaceInvitationRecord.workspace_id == str(workspace_id),
                WorkspaceInvitationRecord.id == str(invitation_id),
            )
        )
        return _invitation_from_record(record) if record is not None else None

    async def list_invitations(self, workspace_id: WorkspaceId) -> Sequence[Invitation]:
        """@brief 列出 Workspace 邀请 / List Workspace invitations.

        @param workspace_id Workspace ID / Workspace ID.
        @return 稳定排序邀请 / Stably ordered invitations.
        """
        await self._install_workspace(str(workspace_id))
        records = (
            await self._session.scalars(
                select(WorkspaceInvitationRecord)
                .where(WorkspaceInvitationRecord.workspace_id == str(workspace_id))
                .order_by(WorkspaceInvitationRecord.created_at, WorkspaceInvitationRecord.id)
            )
        ).all()
        return [_invitation_from_record(record) for record in records]

    async def add_invitation(self, invitation: Invitation) -> None:
        """@brief 添加邀请 / Add an invitation.

        @param invitation pending 邀请 / Pending invitation.
        """
        await self._install_workspace(str(invitation.workspace_id))
        record = _invitation_record(invitation)
        record.invited_by_actor_id = self._actor_id
        self._session.add(record)

    async def save_invitation(self, invitation: Invitation) -> None:
        """@brief 保存邀请状态 / Save invitation state.

        @param invitation 下一版本邀请 / Next invitation revision.
        """
        await self._install_workspace(str(invitation.workspace_id))
        record = await self._locked_record(WorkspaceInvitationRecord, str(invitation.meta.id))
        _require_previous_revision(record.revision, invitation.meta.revision)
        record.status = invitation.status.value
        record.accepted_by_user_id = (
            str(invitation.accepted_by) if invitation.accepted_by is not None else None
        )
        record.resolved_at = invitation.resolved_at
        record.updated_at = invitation.meta.updated_at
        record.revision = invitation.meta.revision

    async def get_account_deletion(
        self, user_id: UserId, request_id: AccountDeletionId
    ) -> AccountDeletion | None:
        """@brief 读取用户自己的删除请求 / Read a user's own deletion request.

        @param user_id 用户 ID / User ID.
        @param request_id 请求 ID / Request ID.
        @return 请求或不存在 / Request when present.
        """
        await self._install_actor(str(user_id))
        record = await self._session.scalar(
            select(AccountDeletionRequestRecord).where(
                AccountDeletionRequestRecord.user_id == str(user_id),
                AccountDeletionRequestRecord.id == str(request_id),
            )
        )
        return _account_deletion_from_record(record) if record is not None else None

    async def add_account_deletion(self, request: AccountDeletion) -> None:
        """@brief 添加删除请求 / Add an account-deletion request.

        @param request scheduled 请求 / Scheduled request.
        """
        await self._install_actor(str(request.user_id))
        self._session.add(_account_deletion_record(request))

    async def save_account_deletion(self, request: AccountDeletion) -> None:
        """@brief 保存删除请求状态 / Save account-deletion state.

        @param request 下一版本请求 / Next request revision.
        """
        await self._install_actor(str(request.user_id))
        record = await self._locked_record(AccountDeletionRequestRecord, str(request.meta.id))
        _require_previous_revision(record.revision, request.meta.revision)
        record.status = request.status.value
        record.completed_at = (
            request.completed_at if isinstance(request, CompletedAccountDeletion) else None
        )
        record.problem = (
            {"code": request.failure.code, "detail": request.failure.detail}
            if isinstance(request, FailedAccountDeletion)
            else None
        )
        record.updated_at = request.meta.updated_at
        record.revision = request.meta.revision

    async def _workspace_owner_id(self, workspace_id: WorkspaceId) -> str:
        """@brief 读取 Workspace 的存储 owner / Read the Workspace storage owner.

        @param workspace_id Workspace ID / Workspace ID.
        @return owner 用户 ID / Owner user ID.
        @raise LookupError Workspace 不存在时抛出 / Raised when the Workspace is absent.
        """
        value = await self._session.scalar(
            select(WorkspaceRecord.resource_owner_id).where(WorkspaceRecord.id == str(workspace_id))
        )
        if not isinstance(value, str):
            raise LookupError("workspace does not exist")
        return value

    async def _install_actor(self, actor_id: str) -> None:
        """@brief 安装并固定签名 token 的本地 actor / Install and pin the signed token's local actor.

        @param actor_id access token 私有 ``uid`` claim / Private ``uid`` claim from the access token.
        @raise PermissionError 同一 UoW 尝试切换 actor 时抛出 / Raised when a UoW tries to switch actors.
        @note 调用方必须只传入完成 JWT 签名验证的 ``uid``；Repository 随后仍交叉验证
            持久化 subject / The caller must pass only a signature-verified ``uid``; the repository
            is subsequently cross-checked against the persisted subject.
        """
        if self._actor_id is not None and self._actor_id != actor_id:
            raise PermissionError("an access unit of work cannot switch actors")
        if self._actor_id is None:
            await self._session.execute(
                text("SELECT set_config('app.actor_id', :actor_id, true)"),
                {"actor_id": actor_id},
            )
            self._actor_id = actor_id

    async def _clear_default_workspace(
        self,
        user_id: UserId,
        workspace_id: WorkspaceId,
    ) -> None:
        """@brief 调用窄化数据库函数清理失效 UI 偏好 / Invoke the narrow DB default cleanup.

        @param user_id 失效成员用户 / Inactive member user.
        @param workspace_id 失去访问权的 Workspace / Workspace whose access was lost.
        @note migration 将该函数限制为“仅在当前 default 等于给定 Workspace 时清 NULL”，
            并由专用 owner policy 执行；它不能修改其他用户资料字段 / The migration limits
            this function to nulling the exact matching default via a dedicated owner policy; it
            cannot mutate any other user profile field.
        """
        await self._session.execute(
            text(
                "SELECT identity.clear_inactive_member_default_workspace(:user_id, :workspace_id)"
            ),
            {"user_id": str(user_id), "workspace_id": str(workspace_id)},
        )

    async def _install_workspace(self, workspace_id: str) -> None:
        """@brief 安装并固定 URL path Workspace / Install and pin the URL-path Workspace.

        @param workspace_id 已完成语法校验的路径 ID / Syntax-validated path ID.
        @raise PermissionError 同一 UoW 尝试切换 Workspace 时抛出 / Raised when a UoW tries to
            switch Workspaces.
        """
        if self._workspace_id is not None and self._workspace_id != workspace_id:
            raise PermissionError("an access unit of work cannot switch workspaces")
        if self._workspace_id is None:
            await self._session.execute(
                text("SELECT set_config('app.workspace_id', :workspace_id, true)"),
                {"workspace_id": workspace_id},
            )
            self._workspace_id = workspace_id

    async def _locked_record[RecordT](self, model: type[RecordT], identifier: str) -> RecordT:
        """@brief 锁定并返回 ORM 行 / Lock and return an ORM row.

        @param model ORM 类型 / ORM model type.
        @param identifier 主键 / Primary key.
        @return 已锁定 ORM 行 / Locked ORM row.
        @raise LookupError 资源不存在时抛出 / Raised when the resource is absent.
        """
        record = await self._session.get(model, identifier, with_for_update=True)
        if record is None:
            raise LookupError("resource does not exist")
        return record


class PostgresAccessUnitOfWork:
    """@brief 一个 PostgreSQL 短事务工作单元 / One PostgreSQL short-transaction unit of work."""

    def __init__(self, database: AsyncDatabase) -> None:
        """@brief 绑定数据库资源 / Bind database resources.

        @param database 共享异步数据库 / Shared async database.
        """
        self._database = database
        self._session: AsyncSession | None = None
        self._transaction: AsyncSessionTransaction | None = None
        self._repository: PostgresAccessRepository | None = None
        self._committed = False
        self._rolled_back = False

    @property
    def repository(self) -> AccessRepository:
        """@brief 返回事务绑定 Repository / Return the transaction-bound repository.

        @return 当前 Repository / Current repository.
        @raise RuntimeError 工作单元未进入时抛出 / Raised before entry.
        """
        if self._repository is None:
            raise RuntimeError("access unit of work has not been entered")
        return self._repository

    async def __aenter__(self) -> Self:
        """@brief 开始独占 Session 与事务 / Start an exclusive session and transaction.

        @return 当前工作单元 / Current unit of work.
        """
        if self._session is not None:
            raise RuntimeError("access unit of work cannot be re-entered")
        self._session = self._database.new_session()
        self._transaction = await self._session.begin()
        self._repository = PostgresAccessRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 回滚未提交事务并关闭 Session / Roll back uncommitted work and close the session.

        @param exc_type 异常类型 / Exception type.
        @param exc 异常 / Exception instance.
        @param traceback traceback / Traceback.
        @return 不吞异常 / Does not suppress exceptions.
        """
        if self._session is not None:
            if exc_type is not None or not self._committed:
                await self.rollback()
            await self._session.close()
        self._session = None
        self._transaction = None
        self._repository = None
        return None

    async def commit(self) -> None:
        """@brief flush 并提交事务 / Flush and commit the transaction."""
        session, transaction = self._require_active()
        if self._committed:
            raise RuntimeError("access unit of work is already committed")
        if self._rolled_back:
            raise RuntimeError("rolled-back access unit of work cannot commit")
        await session.flush()
        await transaction.commit()
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等回滚活动事务 / Idempotently roll back the active transaction."""
        if self._transaction is not None and self._transaction.is_active:
            await self._transaction.rollback()
        self._rolled_back = True

    def _require_active(self) -> tuple[AsyncSession, AsyncSessionTransaction]:
        """@brief 要求活动事务 / Require an active transaction.

        @return Session 与事务 / Session and transaction.
        @raise RuntimeError 工作单元未进入时抛出 / Raised before entry.
        """
        if self._session is None or self._transaction is None:
            raise RuntimeError("access unit of work has not been entered")
        return self._session, self._transaction


class PostgresAccessUnitOfWorkFactory:
    """@brief 创建 PostgreSQL Access UoW / Create PostgreSQL Access UoWs."""

    def __init__(self, database: AsyncDatabase) -> None:
        """@brief 绑定数据库 / Bind the database.

        @param database 共享数据库资源 / Shared database resource.
        """
        self._database = database

    def __call__(self) -> PostgresAccessUnitOfWork:
        """@brief 创建未进入的工作单元 / Create a not-yet-entered unit of work.

        @return 新 PostgreSQL 工作单元 / New PostgreSQL unit of work.
        """
        return PostgresAccessUnitOfWork(self._database)


def _user_from_record(record: UserRecord | None) -> User | None:
    """@brief 将用户 ORM 映射为领域聚合 / Map a user ORM row to a domain aggregate.

    @param record ORM 行或空 / ORM row or null.
    @return 用户或不存在 / User when present.
    """
    if record is None:
        return None
    if record.email is None or record.display_name is None:
        raise ValueError("persisted API v2 user is missing required profile fields")
    return User(
        _meta(record, UserId(record.id)),
        Subject(record.external_subject),
        record.email,
        record.email_verified,
        record.display_name,
        record.locale,
        WorkspaceId(record.default_workspace_id) if record.default_workspace_id else None,
        AccountStatus(record.account_status),
    )


def _workspace_from_record(record: WorkspaceRecord) -> Workspace:
    """@brief 将 Workspace ORM 映射为领域聚合 / Map a Workspace ORM row to a domain aggregate.

    @param record Workspace ORM 行 / Workspace ORM row.
    @return Workspace 聚合 / Workspace aggregate.
    """
    return Workspace(
        _meta(record, WorkspaceId(record.id)),
        record.name,
        record.slug,
        WorkspacePlan(record.plan),
        DataRegion(record.data_region),
    )


def _membership_from_record(record: WorkspaceMemberRecord) -> Membership:
    """@brief 将成员 ORM 快照映射为领域关系 / Map a membership ORM snapshot to the domain.

    @param record 成员 ORM 行 / Membership ORM row.
    @return 成员关系 / Membership aggregate.
    """
    return Membership(
        _meta(record, MembershipId(record.id)),
        WorkspaceId(record.workspace_id),
        UserId(record.user_id),
        record.display_name,
        WorkspaceRole(record.role),
        MemberStatus(record.status),
    )


def _invitation_from_record(record: WorkspaceInvitationRecord) -> Invitation:
    """@brief 将邀请 ORM 映射为领域状态 / Map invitation ORM to domain state.

    @param record 邀请 ORM 行 / Invitation ORM row.
    @return 邀请领域状态 / Invitation domain state.
    """
    status = InvitationStatus(record.status)
    resolved_at = record.resolved_at
    return Invitation(
        _meta(record, InvitationId(record.id)),
        WorkspaceId(record.workspace_id),
        record.email_canonical,
        WorkspaceRole(record.role),
        status,
        record.expires_at,
        UserId(record.accepted_by_user_id) if record.accepted_by_user_id else None,
        resolved_at,
    )


def _account_deletion_from_record(record: AccountDeletionRequestRecord) -> AccountDeletion:
    """@brief 将删除请求 ORM 穷举映射为领域状态 / Exhaustively map deletion ORM state.

    @param record 删除请求 ORM 行 / Account-deletion ORM row.
    @return 判别联合状态 / Discriminated domain state.
    """
    meta = _meta(record, AccountDeletionId(record.id))
    user_id = UserId(record.user_id)
    status = AccountDeletionStatus(record.status)
    if status is AccountDeletionStatus.SCHEDULED:
        return ScheduledAccountDeletion(meta, user_id, record.scheduled_for)
    if status is AccountDeletionStatus.RUNNING:
        return RunningAccountDeletion(meta, user_id, record.scheduled_for, record.updated_at)
    if status is AccountDeletionStatus.COMPLETED:
        if record.completed_at is None:
            raise ValueError("completed account deletion is missing completed_at")
        return CompletedAccountDeletion(meta, user_id, record.scheduled_for, record.completed_at)
    if status is AccountDeletionStatus.CANCELLED:
        return CancelledAccountDeletion(meta, user_id, record.scheduled_for, record.updated_at)
    if record.problem is None:
        raise ValueError("failed account deletion is missing problem")
    code = record.problem.get("code")
    detail = record.problem.get("detail")
    if not isinstance(code, str) or not isinstance(detail, str):
        raise ValueError("failed account deletion has an invalid problem")
    return FailedAccountDeletion(
        meta,
        user_id,
        record.scheduled_for,
        AccountDeletionFailure(code, detail),
        record.updated_at,
    )


def _workspace_record(workspace: Workspace, *, resource_owner_id: str) -> WorkspaceRecord:
    """@brief 从领域 Workspace 构造 ORM 行 / Build an ORM row from a domain Workspace.

    @param workspace Workspace 聚合 / Workspace aggregate.
    @param resource_owner_id 旧存储兼容 owner 列 / Legacy storage-owner column.
    @return 未持久化 ORM 行 / Unpersisted ORM row.
    """
    return WorkspaceRecord(
        id=str(workspace.meta.id),
        resource_owner_id=resource_owner_id,
        name=workspace.name,
        slug=workspace.slug,
        plan=workspace.plan.value,
        data_region=workspace.data_region.value,
        created_at=workspace.meta.created_at,
        updated_at=workspace.meta.updated_at,
        revision=workspace.meta.revision,
        extensions={},
    )


def _membership_record(membership: Membership, *, resource_owner_id: str) -> WorkspaceMemberRecord:
    """@brief 从领域成员关系构造 ORM 行 / Build an ORM row from a membership.

    @param membership 成员关系 / Membership aggregate.
    @param resource_owner_id 旧存储兼容 owner 列 / Legacy storage-owner column.
    @return 未持久化 ORM 行 / Unpersisted ORM row.
    """
    return WorkspaceMemberRecord(
        id=str(membership.meta.id),
        workspace_id=str(membership.workspace_id),
        resource_owner_id=resource_owner_id,
        user_id=str(membership.user_id),
        display_name=membership.display_name,
        role=membership.role.value,
        status=membership.status.value,
        joined_at=membership.meta.created_at,
        created_at=membership.meta.created_at,
        updated_at=membership.meta.updated_at,
        revision=membership.meta.revision,
        extensions={},
    )


def _invitation_record(invitation: Invitation) -> WorkspaceInvitationRecord:
    """@brief 从领域邀请构造 ORM 行 / Build an ORM row from an invitation.

    @param invitation 邀请聚合 / Invitation aggregate.
    @return 未持久化 ORM 行 / Unpersisted ORM row.
    """
    return WorkspaceInvitationRecord(
        id=str(invitation.meta.id),
        workspace_id=str(invitation.workspace_id),
        email_canonical=invitation.email,
        email_hint=_email_hint(invitation.email),
        role=invitation.role.value,
        status=invitation.status.value,
        expires_at=invitation.expires_at,
        accepted_by_user_id=(
            str(invitation.accepted_by) if invitation.accepted_by is not None else None
        ),
        resolved_at=invitation.resolved_at,
        created_at=invitation.meta.created_at,
        updated_at=invitation.meta.updated_at,
        revision=invitation.meta.revision,
        extensions={},
    )


def _account_deletion_record(request: AccountDeletion) -> AccountDeletionRequestRecord:
    """@brief 从领域删除请求构造 ORM 行 / Build an ORM row from an account-deletion state.

    @param request 删除请求 / Account-deletion request.
    @return 未持久化 ORM 行 / Unpersisted ORM row.
    """
    completed_at = request.completed_at if isinstance(request, CompletedAccountDeletion) else None
    problem = (
        {"code": request.failure.code, "detail": request.failure.detail}
        if isinstance(request, FailedAccountDeletion)
        else None
    )
    return AccountDeletionRequestRecord(
        id=str(request.meta.id),
        user_id=str(request.user_id),
        status=request.status.value,
        scheduled_for=request.scheduled_for,
        completed_at=completed_at,
        problem=problem,
        created_at=request.meta.created_at,
        updated_at=request.meta.updated_at,
        revision=request.meta.revision,
        extensions={},
    )


def _meta[IdT: str](record: _RevisionRecord, identifier: IdT) -> ResourceMeta[IdT]:
    """@brief 从 ORM 生命周期列构造 ResourceMeta / Build ResourceMeta from ORM lifecycle columns.

    @param record 具有 revision/timestamps 的 ORM 行 / ORM row with revision and timestamps.
    @param identifier 领域专用 ID / Domain-specific ID.
    @return 领域资源元数据 / Domain resource metadata.
    """
    return ResourceMeta(
        identifier,
        record.revision,
        record.created_at,
        record.updated_at,
    )


def _email_hint(email: str) -> str:
    """@brief 生成不暴露完整 local-part 的邮箱提示 / Make an email hint hiding the local part.

    @param email 规范邮箱 / Canonical email.
    @return 脱敏邮箱提示 / Masked email hint.
    """
    local, separator, domain = email.partition("@")
    return f"{local[:1]}***{separator}{domain}"


def _require_previous_revision(persisted: int, replacement: int) -> None:
    """@brief 验证下一 revision / Validate the next revision.

    @param persisted 已持久化 revision / Persisted revision.
    @param replacement 待写 revision / Replacement revision.
    @raise RuntimeError revision 不是精确递增时抛出 / Raised unless revision increments exactly.
    """
    if replacement != persisted + 1:
        raise RuntimeError("resource revision is stale")


def _require_same_revision(persisted: int, expected: int) -> None:
    """@brief 验证删除前 revision / Validate a revision before deletion.

    @param persisted 已持久化 revision / Persisted revision.
    @param expected 客户端已授权 revision / Authorized revision.
    @raise RuntimeError revision 不同时抛出 / Raised when revisions differ.
    """
    if expected != persisted:
        raise RuntimeError("resource revision is stale")


__all__ = [
    "InMemoryAccessRepository",
    "InMemoryAccessStore",
    "InMemoryAccessUnitOfWork",
    "InMemoryAccessUnitOfWorkFactory",
    "PostgresAccessRepository",
    "PostgresAccessUnitOfWork",
    "PostgresAccessUnitOfWorkFactory",
]
