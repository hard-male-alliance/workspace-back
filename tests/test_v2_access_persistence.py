"""@brief API v2 Access 持久化与首次注册原子性测试 / Access persistence and provisioning tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from backend.domain.identity import IdentityUserRecord
from backend.domain.principals import (
    MembershipId,
    ResourceMeta,
    Subject,
    UserId,
    WorkspaceId,
)
from backend.domain.users import User
from backend.domain.workspaces import (
    DataRegion,
    Membership,
    MemberStatus,
    Workspace,
    WorkspacePlan,
    WorkspaceRole,
)
from backend.infrastructure.access import (
    InMemoryAccessUnitOfWorkFactory,
    PostgresAccessRepository,
)
from backend.infrastructure.hosted_identity import InMemoryHostedIdentityRepository

NOW = datetime(2026, 7, 23, 1, 0, tzinfo=UTC)
"""@brief 测试使用的确定性 UTC 时刻 / Deterministic UTC instant used by tests."""


def _workspace_graph() -> tuple[Workspace, Membership]:
    """@brief 构造最小合法 Workspace 图 / Build a minimal valid Workspace graph.

    @return Workspace 与其唯一 owner / Workspace and its sole owner.
    """
    workspace_id = WorkspaceId("ws_memory_1")
    workspace = Workspace(
        ResourceMeta(workspace_id, 1, NOW, NOW),
        "Klee Lab",
        "klee-lab",
        WorkspacePlan.PERSONAL,
        DataRegion.CN,
    )
    owner = Membership(
        ResourceMeta(MembershipId("member_memory_1"), 1, NOW, NOW),
        workspace_id,
        UserId("user_memory_1"),
        "Klee",
        WorkspaceRole.OWNER,
        MemberStatus.ACTIVE,
    )
    return workspace, owner


@pytest.mark.asyncio
async def test_memory_uow_discards_implicit_and_exceptional_rollbacks() -> None:
    """@brief 未 commit 或异常退出不得泄漏写入 / Uncommitted and exceptional writes do not leak."""
    factory = InMemoryAccessUnitOfWorkFactory()
    workspace, owner = _workspace_graph()

    async with factory() as unit:
        await unit.repository.add_workspace(workspace, owner)

    async with factory() as unit:
        assert await unit.repository.get_workspace(workspace.meta.id) is None
        await unit.repository.add_workspace(workspace, owner)
        await unit.commit()

    async with factory() as unit:
        await unit.repository.delete_workspace(workspace.meta.id)
        await unit.rollback()
        with pytest.raises(RuntimeError, match="rolled-back"):
            await unit.commit()

    with pytest.raises(RuntimeError, match="abort"):
        async with factory() as unit:
            await unit.repository.delete_workspace(workspace.meta.id)
            raise RuntimeError("abort")

    async with factory() as unit:
        assert await unit.repository.get_workspace(workspace.meta.id) == workspace


@pytest.mark.asyncio
async def test_registration_atomically_provisions_personal_workspace() -> None:
    """@brief 首次注册产生可发现的个人 Workspace 与 owner / Registration provisions discoverable ownership."""
    identity = InMemoryHostedIdentityRepository(data_region=DataRegion.CN)
    user = IdentityUserRecord(
        id="usr_registration_1",
        subject="oidc-registration-1",
        email="KLEE@EXAMPLE.COM",
        email_verified=True,
        display_name="Klee",
        locale="zh-CN",
    )

    created = await identity.create_user_with_password(
        user=user,
        password_authenticator_id="authn_registration_1",
        password_verifier="argon2id:test-verifier",
        now=NOW,
    )

    assert created is True
    factory = InMemoryAccessUnitOfWorkFactory(identity.access_store)
    async with factory() as unit:
        access_user = await unit.repository.get_user(UserId(user.id))
        assert access_user is not None
        assert access_user.subject == Subject(user.subject)
        assert access_user.email == "klee@example.com"
        assert access_user.default_workspace_id is not None
        access = await unit.repository.list_workspace_access(UserId(user.id))
        assert len(access) == 1
        workspace, membership = access[0]
        assert workspace.meta.id == access_user.default_workspace_id
        assert workspace.plan is WorkspacePlan.PERSONAL
        assert workspace.data_region is DataRegion.CN
        assert membership.role is WorkspaceRole.OWNER
        assert membership.status is MemberStatus.ACTIVE


@pytest.mark.asyncio
async def test_registration_uniqueness_failure_leaves_access_graph_unchanged() -> None:
    """@brief 重复注册失败不能产生孤儿 Workspace / Duplicate registration cannot create orphan Workspaces."""
    identity = InMemoryHostedIdentityRepository(data_region=DataRegion.GLOBAL)
    user = IdentityUserRecord(
        id="usr_registration_2",
        subject="oidc-registration-2",
        email="klee@example.com",
        email_verified=True,
        display_name="Klee",
        locale="en-SG",
    )
    assert (
        await identity.create_user_with_password(
            user=user,
            password_authenticator_id="authn_registration_2",
            password_verifier="argon2id:test-verifier",
            now=NOW + timedelta(seconds=1),
        )
        is True
    )
    assert (
        await identity.create_user_with_password(
            user=user,
            password_authenticator_id="authn_registration_2",
            password_verifier="argon2id:test-verifier",
            now=NOW + timedelta(seconds=1),
        )
        is False
    )

    factory = InMemoryAccessUnitOfWorkFactory(identity.access_store)
    async with factory() as unit:
        access = await unit.repository.list_workspace_access(UserId(user.id))
        assert len(access) == 1
        assert len(identity.access_store.workspaces) == 1
        assert len(identity.access_store.memberships) == 1


@pytest.mark.asyncio
async def test_suspending_member_clears_matching_default_in_same_uow() -> None:
    """@brief 成员失活与默认 Workspace 清理必须同原子提交 / Suspension clears its matching default atomically."""
    factory = InMemoryAccessUnitOfWorkFactory()
    workspace, owner = _workspace_graph()
    member_user = User(
        ResourceMeta(UserId("user_memory_2"), 1, NOW, NOW),
        Subject("oidc-memory-2"),
        "alice@example.com",
        True,
        "Alice",
        "en-SG",
        workspace.meta.id,
    )
    member = Membership(
        ResourceMeta(MembershipId("member_memory_2"), 1, NOW, NOW),
        workspace.meta.id,
        member_user.meta.id,
        member_user.display_name,
        WorkspaceRole.VIEWER,
        MemberStatus.ACTIVE,
    )
    factory.store.users[str(member_user.meta.id)] = member_user
    factory.store.workspaces[str(workspace.meta.id)] = workspace
    factory.store.memberships[str(owner.meta.id)] = owner
    factory.store.memberships[str(member.meta.id)] = member
    suspended = member.revise(
        role=None,
        status=MemberStatus.SUSPENDED,
        active_owner_count=1,
        updated_at=NOW + timedelta(minutes=1),
    )

    async with factory() as unit:
        await unit.repository.save_membership(suspended)
        await unit.commit()

    async with factory() as unit:
        persisted_user = await unit.repository.get_user(member_user.meta.id)
        assert persisted_user is not None
        assert persisted_user.default_workspace_id is None
        assert persisted_user.meta.revision == 2


@pytest.mark.asyncio
async def test_postgres_owner_count_locks_workspace_serialization_row_first() -> None:
    """@brief owner 计数前必须锁定 Workspace 串行化行 / Owner count first locks its serialization row."""
    session_mock = AsyncMock(spec=AsyncSession)
    session_mock.scalar.side_effect = ["ws_memory_1", 2]
    repository = PostgresAccessRepository(cast("AsyncSession", session_mock))

    count = await repository.count_active_owners(WorkspaceId("ws_memory_1"))

    assert count == 2
    assert "set_config('app.workspace_id'" in str(session_mock.execute.await_args.args[0])
    scalar_calls = session_mock.scalar.await_args_list
    assert "FOR UPDATE" in str(scalar_calls[0].args[0])
    assert "count(" in str(scalar_calls[1].args[0]).lower()
