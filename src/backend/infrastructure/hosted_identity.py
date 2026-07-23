"""Memory and PostgreSQL persistence for hosted identity browser flows."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.domain.identity import (
    IdentityAuthenticatorRecord,
    IdentityBrowserSessionRecord,
    IdentityFlowRecord,
    IdentitySessionRecord,
    IdentityUserRecord,
)
from backend.domain.principals import MembershipId, ResourceMeta, Subject, UserId, WorkspaceId
from backend.domain.users import User
from backend.domain.workspaces import (
    DataRegion,
    Membership,
    MemberStatus,
    Workspace,
    WorkspacePlan,
    WorkspaceRole,
)
from backend.infrastructure.access import InMemoryAccessStore
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    IdentityAuthenticatorRecord as IdentityAuthenticatorOrmRecord,
)
from backend.infrastructure.persistence.models import (
    IdentityBrowserSessionRecord as IdentityBrowserSessionOrmRecord,
)
from backend.infrastructure.persistence.models import IdentityFlowRecord as IdentityFlowOrmRecord
from backend.infrastructure.persistence.models import (
    IdentityFlowStepRecord as IdentityFlowStepOrmRecord,
)
from backend.infrastructure.persistence.models import (
    IdentityLoginSessionRecord as IdentityLoginSessionOrmRecord,
)
from backend.infrastructure.persistence.models import (
    OAuthRefreshTokenFamilyRecord as OAuthRefreshTokenFamilyOrmRecord,
)
from backend.infrastructure.persistence.models import UserRecord as UserOrmRecord
from backend.infrastructure.persistence.models import (
    WorkspaceMemberRecord as WorkspaceMemberOrmRecord,
)
from backend.infrastructure.persistence.models import WorkspaceRecord as WorkspaceOrmRecord
from workspace_shared.ids import new_opaque_id


class InMemoryHostedIdentityRepository:
    """@brief 具有原子个人 Workspace provision 的进程内 identity adapter.

    / Process-local identity adapter with atomic personal-Workspace provisioning.

    @param data_region 新注册 Workspace 的显式数据地域 / Explicit data region for newly
        registered Workspaces.
    @param access_store 与 API v2 Access UoW 共享的可选状态 / Optional state shared with the
        API v2 Access unit of work.
    """

    def __init__(
        self,
        *,
        data_region: DataRegion,
        access_store: InMemoryAccessStore | None = None,
        revoke_token_families: Callable[[str, str, datetime], Awaitable[None]] | None = None,
    ) -> None:
        """@brief 初始化 identity 与 Access 共享原子边界 / Initialize shared identity/access state.

        @param data_region 明确配置的数据驻留地域 / Explicitly configured residency region.
        @param access_store 可选共享 Access 状态 / Optional shared Access state.
        @param revoke_token_families 精确撤销同进程 OAuth family 的协作者 / Collaborator that
            revokes exact process-local OAuth families.
        """
        self._data_region = data_region
        self._access_store = access_store or InMemoryAccessStore()
        self._revoke_token_families = revoke_token_families
        self._browser_sessions: dict[str, IdentityBrowserSessionRecord] = {}
        self._flows: dict[str, IdentityFlowRecord] = {}
        self._users: dict[str, IdentityUserRecord] = {}
        self._user_ids_by_email: dict[str, str] = {}
        self._passwords: dict[str, str] = {}
        self._authenticators: dict[str, IdentityAuthenticatorRecord] = {}
        self._login_sessions: dict[str, IdentitySessionRecord] = {}
        self._processed_steps: dict[tuple[str, str], str] = {}
        self._lock = self._access_store.lock

    @property
    def access_store(self) -> InMemoryAccessStore:
        """@brief 返回与注册共享锁的 Access 状态 / Return Access state sharing registration's lock.

        @return 可传给 ``InMemoryAccessUnitOfWorkFactory`` 的状态 / State suitable for the
            in-memory Access unit-of-work factory.
        """
        return self._access_store

    async def create_browser_session(self, record: IdentityBrowserSessionRecord) -> None:
        async with self._lock:
            if record.id in self._browser_sessions:
                raise RuntimeError("identity browser session id collision")
            self._browser_sessions[record.id] = replace(record)

    async def get_browser_session(self, session_id: str) -> IdentityBrowserSessionRecord | None:
        async with self._lock:
            record = self._browser_sessions.get(session_id)
            return replace(record) if record is not None else None

    async def create_flow(self, record: IdentityFlowRecord) -> None:
        async with self._lock:
            if record.id in self._flows:
                raise RuntimeError("identity flow id collision")
            self._flows[record.id] = replace(record)

    async def get_flow(self, flow_id: str) -> IdentityFlowRecord | None:
        async with self._lock:
            record = self._flows.get(flow_id)
            return replace(record) if record is not None else None

    async def transition_flow(
        self,
        flow_id: str,
        *,
        browser_session_id: str,
        step_id: str,
        expected_step: str,
        allowed_steps: tuple[str, ...],
        status: str,
        state_updates: dict[str, object],
        user_id: str | None = None,
        authorization_resume_uri: str | None = None,
        webauthn_options: dict[str, object] | None = None,
        completed_at: datetime | None = None,
    ) -> IdentityFlowRecord | None:
        async with self._lock:
            flow = self._flows.get(flow_id)
            if flow is None or flow.browser_session_id != browser_session_id:
                return None
            receipt_key = (flow_id, step_id)
            prior_kind = self._processed_steps.get(receipt_key)
            if prior_kind is not None:
                return replace(flow) if prior_kind == expected_step else None
            if expected_step not in flow.allowed_steps or flow.expires_at <= datetime.now(UTC):
                return None
            state = dict(flow.internal_state or {})
            state.update(state_updates)
            updated = replace(
                flow,
                allowed_steps=allowed_steps,
                status=status,
                internal_state=state,
                user_id=user_id if user_id is not None else flow.user_id,
                authorization_resume_uri=authorization_resume_uri,
                webauthn_options=webauthn_options,
                completed_at=completed_at,
            )
            self._flows[flow_id] = updated
            self._processed_steps[receipt_key] = expected_step
            return replace(updated)

    async def processed_step_kind(self, flow_id: str, step_id: str) -> str | None:
        async with self._lock:
            return self._processed_steps.get((flow_id, step_id))

    async def get_user_by_email(self, normalized_email: str) -> IdentityUserRecord | None:
        async with self._lock:
            user_id = self._user_ids_by_email.get(_canonical_email(normalized_email))
            user = self._users.get(user_id or "")
            return replace(user) if user is not None else None

    async def get_identity_user(self, user_id: str) -> IdentityUserRecord | None:
        async with self._lock:
            user = self._users.get(user_id)
            return replace(user) if user is not None else None

    async def create_user_with_password(
        self,
        *,
        user: IdentityUserRecord,
        password_authenticator_id: str,
        password_verifier: str,
        now: datetime,
        passkey: IdentityAuthenticatorRecord | None = None,
    ) -> bool:
        """@brief 原子创建账号、密码、个人 Workspace 与 owner / Atomically provision account and Workspace.

        @param user 待创建 identity 用户 / Identity user to create.
        @param password_authenticator_id 密码验证器 ID / Password-authenticator ID.
        @param password_verifier 不可逆密码 verifier / Irreversible password verifier.
        @param now 单一 provision 时间 / Single provisioning instant.
        @param passkey 可选同时注册的 passkey / Optional passkey registered in the same unit.
        @return 全部创建成功时为真；唯一性冲突时为假 / True when all records were created;
            false on a uniqueness conflict.
        """
        access_user, workspace, owner = _new_personal_access(user, now, self._data_region)
        canonical_email = _canonical_email(user.email)
        async with self._lock:
            if canonical_email in self._user_ids_by_email or user.id in self._users:
                return False
            if passkey is not None:
                credential_id = str(passkey.credential_metadata.get("credential_id", ""))
                if not credential_id or _credential_exists(
                    self._authenticators.values(), credential_id
                ):
                    return False
            self._users[user.id] = replace(user, email=canonical_email)
            self._user_ids_by_email[canonical_email] = user.id
            self._passwords[user.id] = password_verifier
            self._authenticators[password_authenticator_id] = IdentityAuthenticatorRecord(
                id=password_authenticator_id,
                user_id=user.id,
                kind="password",
                display_name="Password",
                verifier=password_verifier,
                credential_metadata={},
                created_at=now,
                last_used_at=None,
            )
            if passkey is not None:
                self._authenticators[passkey.id] = replace(passkey, user_id=user.id)
            self._access_store.users[user.id] = access_user
            self._access_store.workspaces[str(workspace.meta.id)] = workspace
            self._access_store.memberships[str(owner.meta.id)] = owner
            return True

    async def password_verifier(self, user_id: str) -> str | None:
        async with self._lock:
            return self._passwords.get(user_id)

    async def replace_password_and_revoke_sessions(
        self, user_id: str, *, password_verifier: str, now: datetime
    ) -> bool:
        async with self._lock:
            if user_id not in self._users or user_id not in self._passwords:
                return False
            self._passwords[user_id] = password_verifier
            for authenticator_id, authenticator in tuple(self._authenticators.items()):
                if authenticator.user_id == user_id and authenticator.kind == "password":
                    self._authenticators[authenticator_id] = replace(
                        authenticator, verifier=password_verifier, last_used_at=now
                    )
            for session_id, record in tuple(self._login_sessions.items()):
                if record.user_id == user_id and record.revoked_at is None:
                    self._login_sessions[session_id] = replace(record, revoked_at=now)
            return True

    async def create_login_session(self, record: IdentitySessionRecord) -> None:
        async with self._lock:
            if record.id in self._login_sessions:
                raise RuntimeError("identity login session id collision")
            self._login_sessions[record.id] = replace(record)

    async def get_login_session(self, session_id: str) -> IdentitySessionRecord | None:
        async with self._lock:
            record = self._login_sessions.get(session_id)
            return replace(record) if record is not None else None

    async def bind_browser_user(self, browser_session_id: str, user_id: str) -> None:
        async with self._lock:
            record = self._browser_sessions.get(browser_session_id)
            if record is None:
                raise RuntimeError("identity browser session is missing")
            self._browser_sessions[browser_session_id] = replace(record, user_id=user_id)

    async def list_login_sessions(self, user_id: str) -> list[IdentitySessionRecord]:
        async with self._lock:
            return [
                replace(record)
                for record in self._login_sessions.values()
                if record.user_id == user_id and record.revoked_at is None
            ]

    async def revoke_login_session(self, user_id: str, session_id: str, now: datetime) -> bool:
        async with self._lock:
            record = self._login_sessions.get(session_id)
            if record is None or record.user_id != user_id or record.revoked_at is not None:
                return False
            self._login_sessions[session_id] = replace(record, revoked_at=now)
        if self._revoke_token_families is not None:
            await self._revoke_token_families(user_id, session_id, now)
        return True

    async def list_authenticators(self, user_id: str) -> list[IdentityAuthenticatorRecord]:
        async with self._lock:
            return [
                replace(record)
                for record in self._authenticators.values()
                if record.user_id == user_id and record.revoked_at is None
            ]

    async def replace_recovery_codes(
        self,
        user_id: str,
        *,
        authenticator_id: str,
        verifiers: tuple[str, ...],
        now: datetime,
    ) -> None:
        async with self._lock:
            for item_id, record in tuple(self._authenticators.items()):
                if record.user_id == user_id and record.kind == "recovery_code":
                    self._authenticators[item_id] = replace(record, revoked_at=now)
            self._authenticators[authenticator_id] = IdentityAuthenticatorRecord(
                id=authenticator_id,
                user_id=user_id,
                kind="recovery_code",
                display_name="Recovery codes",
                verifier=json.dumps(verifiers),
                credential_metadata={},
                created_at=now,
                last_used_at=None,
            )

    async def revoke_authenticator(
        self, user_id: str, authenticator_id: str, now: datetime
    ) -> bool:
        async with self._lock:
            record = self._authenticators.get(authenticator_id)
            active = [
                item
                for item in self._authenticators.values()
                if item.user_id == user_id and item.revoked_at is None
            ]
            if (
                record is None
                or record.user_id != user_id
                or record.revoked_at is not None
                or len(active) <= 1
            ):
                return False
            self._authenticators[authenticator_id] = replace(record, revoked_at=now)
            if record.kind == "password":
                self._passwords.pop(user_id, None)
            return True

    async def add_passkey(self, record: IdentityAuthenticatorRecord) -> bool:
        async with self._lock:
            credential_id = str(record.credential_metadata.get("credential_id", ""))
            if not credential_id or _credential_exists(
                self._authenticators.values(), credential_id
            ):
                return False
            self._authenticators[record.id] = replace(record)
            return True

    async def get_passkey_by_credential_id(
        self, credential_id: str
    ) -> IdentityAuthenticatorRecord | None:
        async with self._lock:
            for record in self._authenticators.values():
                if (
                    record.kind == "passkey"
                    and record.revoked_at is None
                    and record.credential_metadata.get("credential_id") == credential_id
                ):
                    return replace(record)
            return None

    async def update_passkey_sign_count(
        self, authenticator_id: str, *, expected: int, replacement: int, now: datetime
    ) -> bool:
        async with self._lock:
            record = self._authenticators.get(authenticator_id)
            current_count = (
                record.credential_metadata.get("sign_count", -1) if record is not None else -1
            )
            if record is None or current_count != expected:
                return False
            metadata = dict(record.credential_metadata)
            metadata["sign_count"] = replacement
            self._authenticators[authenticator_id] = replace(
                record, credential_metadata=metadata, last_used_at=now
            )
            return True

    async def consume_recovery_code(self, user_id: str, verifier: str, now: datetime) -> bool:
        async with self._lock:
            for authenticator_id, record in tuple(self._authenticators.items()):
                if (
                    record.user_id != user_id
                    or record.kind != "recovery_code"
                    or record.revoked_at is not None
                ):
                    continue
                values = json.loads(record.verifier)
                if not isinstance(values, list) or verifier not in values:
                    return False
                values.remove(verifier)
                self._authenticators[authenticator_id] = replace(
                    record,
                    verifier=json.dumps(values),
                    last_used_at=now,
                    revoked_at=now if not values else None,
                )
                return True
            return False


class PostgresHostedIdentityRepository:
    """@brief PostgreSQL identity 与个人 Workspace 原子 adapter.

    / PostgreSQL adapter atomically provisioning identity and a personal Workspace.

    @param database 异步 PostgreSQL 资源 / Async PostgreSQL resource.
    @param data_region 新注册 Workspace 的显式地域 / Explicit region for new Workspaces.
    """

    def __init__(self, database: AsyncDatabase, *, data_region: DataRegion) -> None:
        """@brief 绑定数据库与显式地域 / Bind the database and explicit residency region.

        @param database 异步 PostgreSQL 资源 / Async PostgreSQL resource.
        @param data_region 新注册 Workspace 的数据地域 / Data region for new Workspaces.
        """
        self._database = database
        self._data_region = data_region

    async def create_browser_session(self, record: IdentityBrowserSessionRecord) -> None:
        async with self._database.unscoped_transaction() as session:
            session.add(
                IdentityBrowserSessionOrmRecord(
                    id=record.id,
                    authorization_request_id=record.authorization_request_id,
                    browser_secret_hash=record.browser_secret_hash,
                    csrf_token_hash=record.csrf_token_hash,
                    user_id=record.user_id,
                    created_at=record.created_at,
                    last_seen_at=record.last_seen_at,
                    expires_at=record.expires_at,
                )
            )

    async def get_browser_session(self, session_id: str) -> IdentityBrowserSessionRecord | None:
        async with self._database.unscoped_transaction() as session:
            record = await session.scalar(
                select(IdentityBrowserSessionOrmRecord).where(
                    IdentityBrowserSessionOrmRecord.id == session_id
                )
            )
            if record is None:
                return None
            return IdentityBrowserSessionRecord(
                id=record.id,
                authorization_request_id=record.authorization_request_id,
                browser_secret_hash=record.browser_secret_hash,
                csrf_token_hash=record.csrf_token_hash,
                user_id=record.user_id,
                created_at=record.created_at,
                last_seen_at=record.last_seen_at,
                expires_at=record.expires_at,
            )

    async def create_flow(self, record: IdentityFlowRecord) -> None:
        async with self._database.unscoped_transaction() as session:
            session.add(
                IdentityFlowOrmRecord(
                    id=record.id,
                    purpose=record.purpose,
                    status=record.status,
                    allowed_steps=list(record.allowed_steps),
                    authorization_request_id=record.authorization_request_id,
                    browser_session_id=record.browser_session_id,
                    client_id=record.client_id,
                    redirect_uri=record.redirect_uri,
                    code_challenge=record.code_challenge,
                    authorization_resume_uri=record.authorization_resume_uri,
                    webauthn_options=record.webauthn_options,
                    user_id=record.user_id,
                    internal_state=record.internal_state or {},
                    created_at=record.created_at,
                    expires_at=record.expires_at,
                    completed_at=record.completed_at,
                )
            )

    async def get_flow(self, flow_id: str) -> IdentityFlowRecord | None:
        async with self._database.unscoped_transaction() as session:
            record = await session.scalar(
                select(IdentityFlowOrmRecord).where(IdentityFlowOrmRecord.id == flow_id)
            )
            if record is None:
                return None
            return IdentityFlowRecord(
                id=record.id,
                purpose=record.purpose,
                status=record.status,
                allowed_steps=tuple(record.allowed_steps),
                authorization_request_id=record.authorization_request_id,
                browser_session_id=record.browser_session_id,
                client_id=record.client_id,
                redirect_uri=record.redirect_uri,
                code_challenge=record.code_challenge,
                authorization_resume_uri=record.authorization_resume_uri,
                webauthn_options=record.webauthn_options,
                user_id=record.user_id,
                internal_state=record.internal_state,
                created_at=record.created_at,
                expires_at=record.expires_at,
                completed_at=record.completed_at,
            )

    async def transition_flow(
        self,
        flow_id: str,
        *,
        browser_session_id: str,
        step_id: str,
        expected_step: str,
        allowed_steps: tuple[str, ...],
        status: str,
        state_updates: dict[str, object],
        user_id: str | None = None,
        authorization_resume_uri: str | None = None,
        webauthn_options: dict[str, object] | None = None,
        completed_at: datetime | None = None,
    ) -> IdentityFlowRecord | None:
        async with self._database.unscoped_transaction() as session:
            flow = await session.scalar(
                select(IdentityFlowOrmRecord)
                .where(IdentityFlowOrmRecord.id == flow_id)
                .with_for_update()
            )
            if flow is None or flow.browser_session_id != browser_session_id:
                return None
            prior = await session.scalar(
                select(IdentityFlowStepOrmRecord).where(
                    IdentityFlowStepOrmRecord.flow_id == flow_id,
                    IdentityFlowStepOrmRecord.step_id == step_id,
                )
            )
            if prior is not None:
                return _flow_from_orm(flow) if prior.kind == expected_step else None
            if expected_step not in flow.allowed_steps or flow.expires_at <= datetime.now(UTC):
                return None
            state = dict(flow.internal_state)
            state.update(state_updates)
            flow.internal_state = state
            flow.allowed_steps = list(allowed_steps)
            flow.status = status
            if user_id is not None:
                flow.user_id = user_id
            flow.authorization_resume_uri = authorization_resume_uri
            flow.webauthn_options = webauthn_options
            flow.completed_at = completed_at
            session.add(
                IdentityFlowStepOrmRecord(
                    id=new_opaque_id("idstep"),
                    flow_id=flow_id,
                    step_id=step_id,
                    kind=expected_step,
                )
            )
            await session.flush()
            return _flow_from_orm(flow)

    async def processed_step_kind(self, flow_id: str, step_id: str) -> str | None:
        async with self._database.unscoped_transaction() as session:
            value = await session.scalar(
                select(IdentityFlowStepOrmRecord.kind).where(
                    IdentityFlowStepOrmRecord.flow_id == flow_id,
                    IdentityFlowStepOrmRecord.step_id == step_id,
                )
            )
            return value if isinstance(value, str) else None

    async def get_user_by_email(self, normalized_email: str) -> IdentityUserRecord | None:
        """@brief 通过窄函数解析 active 登录用户 / Resolve an active login user via a narrow function.

        @param normalized_email 已规范化邮箱 / Canonical email.
        @return 登录用户或不存在 / Login user when present.
        @note 数据库函数只返回 uid；随后安装 users-self RLS 再读取资料 / The database function
            returns only a uid; users-self RLS is then installed before profile retrieval.
        """
        async with self._database.unscoped_transaction() as session:
            user_id = await session.scalar(
                text("SELECT identity.resolve_login_user_id(:email)"),
                {"email": _canonical_email(normalized_email)},
            )
            if not isinstance(user_id, str):
                return None
            await _install_identity_actor(session, user_id)
            return _user_from_orm(await session.get(UserOrmRecord, user_id))

    async def get_identity_user(self, user_id: str) -> IdentityUserRecord | None:
        """@brief 在 users-self RLS 下读取已知 uid / Read a known uid under users-self RLS.

        @param user_id 由浏览器 session 或签名 token 解析的 uid / UID resolved from a browser
            session or signed token.
        @return identity 用户或不存在 / Identity user when present.
        """
        async with self._database.unscoped_transaction() as session:
            await _install_identity_actor(session, user_id)
            return _user_from_orm(await session.get(UserOrmRecord, user_id))

    async def create_user_with_password(
        self,
        *,
        user: IdentityUserRecord,
        password_authenticator_id: str,
        password_verifier: str,
        now: datetime,
        passkey: IdentityAuthenticatorRecord | None = None,
    ) -> bool:
        """@brief 原子创建账号、验证器、个人 Workspace 与 owner / Atomically provision registration.

        @param user 待创建 identity 用户 / Identity user to create.
        @param password_authenticator_id 密码验证器 ID / Password-authenticator ID.
        @param password_verifier 不可逆密码 verifier / Irreversible password verifier.
        @param now 单一 provision 时间 / Single provisioning instant.
        @param passkey 可选同时注册的 passkey / Optional passkey registered in the same unit.
        @return 全部提交时为真；唯一性冲突时为假 / True when all records commit; false on a
            uniqueness conflict.
        """
        access_user, workspace, owner = _new_personal_access(user, now, self._data_region)
        try:
            async with self._database.unscoped_transaction() as session:
                await session.execute(
                    text(
                        """
                        SELECT
                            set_config('app.actor_id', :actor_id, true),
                            set_config('app.workspace_id', :workspace_id, true),
                            set_config('app.resource_owner_id', :actor_id, true)
                        """
                    ),
                    {"actor_id": user.id, "workspace_id": str(workspace.meta.id)},
                )
                user_record = UserOrmRecord(
                    id=user.id,
                    external_subject=user.subject,
                    display_name=user.display_name,
                    email=user.email,
                    email_canonical=_canonical_email(user.email),
                    email_verified=user.email_verified,
                    account_status="active",
                    default_workspace_id=None,
                    locale=user.locale,
                    created_at=now,
                    updated_at=now,
                    revision=1,
                    extensions={},
                )
                session.add(user_record)
                await session.flush()
                workspace_record = WorkspaceOrmRecord(
                    id=str(workspace.meta.id),
                    resource_owner_id=user.id,
                    name=workspace.name,
                    slug=workspace.slug,
                    plan=workspace.plan.value,
                    data_region=workspace.data_region.value,
                    default_locale=user.locale,
                    created_at=now,
                    updated_at=now,
                    revision=1,
                    extensions={},
                )
                session.add(workspace_record)
                # WorkspaceMemberOrmRecord intentionally has no ORM relationship to the
                # Workspace aggregate.  Flush the tenant root explicitly so SQLAlchemy cannot
                # order the membership INSERT ahead of its foreign-key parent.
                await session.flush()
                session.add(
                    WorkspaceMemberOrmRecord(
                        id=str(owner.meta.id),
                        workspace_id=str(owner.workspace_id),
                        resource_owner_id=user.id,
                        user_id=user.id,
                        display_name=owner.display_name,
                        role=owner.role.value,
                        status=owner.status.value,
                        joined_at=now,
                        created_at=now,
                        updated_at=now,
                        revision=1,
                        extensions={},
                    )
                )
                session.add(
                    IdentityAuthenticatorOrmRecord(
                        id=password_authenticator_id,
                        user_id=user.id,
                        kind="password",
                        display_name="Password",
                        verifier=password_verifier,
                        credential_id=None,
                        created_at=now,
                    )
                )
                if passkey is not None:
                    session.add(_passkey_orm(passkey, user_id=user.id))
                await session.flush()
                user_record.default_workspace_id = str(access_user.default_workspace_id)
        except IntegrityError:
            return False
        return True

    async def password_verifier(self, user_id: str) -> str | None:
        async with self._database.unscoped_transaction() as session:
            value = await session.scalar(
                select(IdentityAuthenticatorOrmRecord.verifier).where(
                    IdentityAuthenticatorOrmRecord.user_id == user_id,
                    IdentityAuthenticatorOrmRecord.kind == "password",
                    IdentityAuthenticatorOrmRecord.revoked_at.is_(None),
                )
            )
            return value if isinstance(value, str) else None

    async def replace_password_and_revoke_sessions(
        self, user_id: str, *, password_verifier: str, now: datetime
    ) -> bool:
        async with self._database.unscoped_transaction() as session:
            authenticator = await session.scalar(
                select(IdentityAuthenticatorOrmRecord)
                .where(
                    IdentityAuthenticatorOrmRecord.user_id == user_id,
                    IdentityAuthenticatorOrmRecord.kind == "password",
                    IdentityAuthenticatorOrmRecord.revoked_at.is_(None),
                )
                .with_for_update()
            )
            if authenticator is None:
                return False
            authenticator.verifier = password_verifier
            authenticator.last_used_at = now
            sessions = (
                await session.scalars(
                    select(IdentityLoginSessionOrmRecord).where(
                        IdentityLoginSessionOrmRecord.user_id == user_id,
                        IdentityLoginSessionOrmRecord.revoked_at.is_(None),
                    )
                )
            ).all()
            for login_session in sessions:
                login_session.revoked_at = now
            families = (
                await session.scalars(
                    select(OAuthRefreshTokenFamilyOrmRecord).where(
                        OAuthRefreshTokenFamilyOrmRecord.user_id == user_id,
                        OAuthRefreshTokenFamilyOrmRecord.revoked_at.is_(None),
                    )
                )
            ).all()
            for family in families:
                family.revoked_at = now
            return True

    async def create_login_session(self, record: IdentitySessionRecord) -> None:
        async with self._database.unscoped_transaction() as session:
            session.add(
                IdentityLoginSessionOrmRecord(
                    id=record.id,
                    user_id=record.user_id,
                    client_id=record.client_id,
                    client_name=record.client_name,
                    device_name=record.device_name,
                    session_secret_hash=record.session_secret_hash,
                    created_at=record.created_at,
                    last_seen_at=record.last_seen_at,
                    idle_expires_at=record.idle_expires_at,
                    absolute_expires_at=record.absolute_expires_at,
                    revoked_at=record.revoked_at,
                )
            )

    async def get_login_session(self, session_id: str) -> IdentitySessionRecord | None:
        async with self._database.unscoped_transaction() as session:
            record = await session.get(IdentityLoginSessionOrmRecord, session_id)
            if record is None:
                return None
            return IdentitySessionRecord(
                id=record.id,
                user_id=record.user_id,
                client_id=record.client_id,
                client_name=record.client_name,
                device_name=record.device_name,
                session_secret_hash=record.session_secret_hash,
                created_at=record.created_at,
                last_seen_at=record.last_seen_at,
                idle_expires_at=record.idle_expires_at,
                absolute_expires_at=record.absolute_expires_at,
                revoked_at=record.revoked_at,
            )

    async def bind_browser_user(self, browser_session_id: str, user_id: str) -> None:
        async with self._database.unscoped_transaction() as session:
            browser = await session.get(IdentityBrowserSessionOrmRecord, browser_session_id)
            if browser is None:
                raise RuntimeError("identity browser session is missing")
            browser.user_id = user_id

    async def list_login_sessions(self, user_id: str) -> list[IdentitySessionRecord]:
        async with self._database.unscoped_transaction() as session:
            records = (
                await session.scalars(
                    select(IdentityLoginSessionOrmRecord).where(
                        IdentityLoginSessionOrmRecord.user_id == user_id,
                        IdentityLoginSessionOrmRecord.revoked_at.is_(None),
                    )
                )
            ).all()
            return [_login_session_from_orm(record) for record in records]

    async def revoke_login_session(self, user_id: str, session_id: str, now: datetime) -> bool:
        async with self._database.unscoped_transaction() as session:
            record = await session.scalar(
                select(IdentityLoginSessionOrmRecord)
                .where(
                    IdentityLoginSessionOrmRecord.id == session_id,
                    IdentityLoginSessionOrmRecord.user_id == user_id,
                )
                .with_for_update()
            )
            if record is None or record.revoked_at is not None:
                return False
            record.revoked_at = now
            families = (
                await session.scalars(
                    select(OAuthRefreshTokenFamilyOrmRecord).where(
                        OAuthRefreshTokenFamilyOrmRecord.user_id == user_id,
                        OAuthRefreshTokenFamilyOrmRecord.login_session_id == session_id,
                        OAuthRefreshTokenFamilyOrmRecord.revoked_at.is_(None),
                    )
                )
            ).all()
            for family in families:
                family.revoked_at = now
            return True

    async def list_authenticators(self, user_id: str) -> list[IdentityAuthenticatorRecord]:
        async with self._database.unscoped_transaction() as session:
            records = (
                await session.scalars(
                    select(IdentityAuthenticatorOrmRecord).where(
                        IdentityAuthenticatorOrmRecord.user_id == user_id,
                        IdentityAuthenticatorOrmRecord.revoked_at.is_(None),
                    )
                )
            ).all()
            return [_authenticator_from_orm(record) for record in records]

    async def replace_recovery_codes(
        self,
        user_id: str,
        *,
        authenticator_id: str,
        verifiers: tuple[str, ...],
        now: datetime,
    ) -> None:
        async with self._database.unscoped_transaction() as session:
            records = (
                await session.scalars(
                    select(IdentityAuthenticatorOrmRecord).where(
                        IdentityAuthenticatorOrmRecord.user_id == user_id,
                        IdentityAuthenticatorOrmRecord.kind == "recovery_code",
                        IdentityAuthenticatorOrmRecord.revoked_at.is_(None),
                    )
                )
            ).all()
            for record in records:
                record.revoked_at = now
            session.add(
                IdentityAuthenticatorOrmRecord(
                    id=authenticator_id,
                    user_id=user_id,
                    kind="recovery_code",
                    display_name="Recovery codes",
                    verifier=json.dumps(verifiers),
                    credential_id=None,
                    created_at=now,
                )
            )

    async def revoke_authenticator(
        self, user_id: str, authenticator_id: str, now: datetime
    ) -> bool:
        async with self._database.unscoped_transaction() as session:
            records = (
                await session.scalars(
                    select(IdentityAuthenticatorOrmRecord)
                    .where(
                        IdentityAuthenticatorOrmRecord.user_id == user_id,
                        IdentityAuthenticatorOrmRecord.revoked_at.is_(None),
                    )
                    .with_for_update()
                )
            ).all()
            target = next((item for item in records if item.id == authenticator_id), None)
            if target is None or len(records) <= 1:
                return False
            target.revoked_at = now
            return True

    async def add_passkey(self, record: IdentityAuthenticatorRecord) -> bool:
        try:
            async with self._database.unscoped_transaction() as session:
                session.add(_passkey_orm(record, user_id=record.user_id))
        except IntegrityError:
            return False
        return True

    async def get_passkey_by_credential_id(
        self, credential_id: str
    ) -> IdentityAuthenticatorRecord | None:
        async with self._database.unscoped_transaction() as session:
            record = await session.scalar(
                select(IdentityAuthenticatorOrmRecord).where(
                    IdentityAuthenticatorOrmRecord.credential_id == credential_id,
                    IdentityAuthenticatorOrmRecord.kind == "passkey",
                    IdentityAuthenticatorOrmRecord.revoked_at.is_(None),
                )
            )
            return _authenticator_from_orm(record) if record is not None else None

    async def update_passkey_sign_count(
        self, authenticator_id: str, *, expected: int, replacement: int, now: datetime
    ) -> bool:
        async with self._database.unscoped_transaction() as session:
            record = await session.scalar(
                select(IdentityAuthenticatorOrmRecord)
                .where(
                    IdentityAuthenticatorOrmRecord.id == authenticator_id,
                    IdentityAuthenticatorOrmRecord.revoked_at.is_(None),
                )
                .with_for_update()
            )
            current_count = (
                record.credential_metadata.get("sign_count", -1) if record is not None else -1
            )
            if record is None or current_count != expected:
                return False
            metadata = dict(record.credential_metadata)
            metadata["sign_count"] = replacement
            record.credential_metadata = metadata
            record.last_used_at = now
            return True

    async def consume_recovery_code(self, user_id: str, verifier: str, now: datetime) -> bool:
        async with self._database.unscoped_transaction() as session:
            record = await session.scalar(
                select(IdentityAuthenticatorOrmRecord)
                .where(
                    IdentityAuthenticatorOrmRecord.user_id == user_id,
                    IdentityAuthenticatorOrmRecord.kind == "recovery_code",
                    IdentityAuthenticatorOrmRecord.revoked_at.is_(None),
                )
                .with_for_update()
            )
            if record is None:
                return False
            values = json.loads(record.verifier)
            if not isinstance(values, list) or verifier not in values:
                return False
            values.remove(verifier)
            record.verifier = json.dumps(values)
            record.last_used_at = now
            if not values:
                record.revoked_at = now
            return True


async def _install_identity_actor(session: AsyncSession, user_id: str) -> None:
    """@brief 为已解析 uid 安装 users-self RLS / Install users-self RLS for a resolved uid.

    @param session 当前短事务 Session / Current short-transaction session.
    @param user_id 由窄函数、浏览器 session 或签名 token 解析的 uid / UID resolved by a narrow
        function, browser session, or signed token.
    """
    await session.execute(
        text("SELECT set_config('app.actor_id', :actor_id, true)"),
        {"actor_id": user_id},
    )


def _flow_from_orm(record: IdentityFlowOrmRecord) -> IdentityFlowRecord:
    return IdentityFlowRecord(
        id=record.id,
        purpose=record.purpose,
        status=record.status,
        allowed_steps=tuple(record.allowed_steps),
        authorization_request_id=record.authorization_request_id,
        browser_session_id=record.browser_session_id,
        client_id=record.client_id,
        redirect_uri=record.redirect_uri,
        code_challenge=record.code_challenge,
        authorization_resume_uri=record.authorization_resume_uri,
        webauthn_options=record.webauthn_options,
        user_id=record.user_id,
        internal_state=record.internal_state,
        created_at=record.created_at,
        expires_at=record.expires_at,
        completed_at=record.completed_at,
    )


def _new_personal_access(
    user: IdentityUserRecord,
    now: datetime,
    data_region: DataRegion,
) -> tuple[User, Workspace, Membership]:
    """@brief 构造首次注册的个人 Workspace 图 / Build a first-registration personal Workspace graph.

    @param user 已验证注册资料 / Validated registration profile.
    @param now 所有资源共享的 provision 时刻 / Provisioning instant shared by all resources.
    @param data_region 构造注入的数据驻留地域 / Constructor-injected data-residency region.
    @return ``(User, Workspace, owner Membership)`` 原子图 / Atomic user/workspace/owner graph.
    """
    workspace_id = WorkspaceId(new_opaque_id("ws"))
    member_id = MembershipId(new_opaque_id("wmem"))
    workspace = Workspace(
        ResourceMeta(workspace_id, 1, now, now),
        user.display_name,
        f"personal-{str(workspace_id).partition('_')[2]}",
        WorkspacePlan.PERSONAL,
        data_region,
    )
    access_user = User(
        ResourceMeta(UserId(user.id), 1, now, now),
        Subject(user.subject),
        _canonical_email(user.email),
        user.email_verified,
        user.display_name,
        user.locale,
        workspace_id,
    )
    owner = Membership(
        ResourceMeta(member_id, 1, now, now),
        workspace_id,
        UserId(user.id),
        user.display_name,
        WorkspaceRole.OWNER,
        MemberStatus.ACTIVE,
    )
    return access_user, workspace, owner


def _canonical_email(email: str) -> str:
    """@brief 生成 identity 等值比较邮箱 / Canonicalize an email for identity equality.

    @param email 已完成语法验证的邮箱 / Syntax-validated email.
    @return 与 PostgreSQL ``lower(btrim())`` 一致的邮箱 / Email matching PostgreSQL
        ``lower(btrim())`` canonicalization.
    """
    return email.strip().lower()


def _user_from_orm(record: UserOrmRecord | None) -> IdentityUserRecord | None:
    if (
        record is None
        or record.account_status not in {"active", "deletion_scheduled"}
        or record.email is None
        or record.display_name is None
    ):
        return None
    return IdentityUserRecord(
        id=record.id,
        subject=record.external_subject,
        email=record.email,
        email_verified=record.email_verified,
        display_name=record.display_name,
        locale=record.locale,
    )


def _login_session_from_orm(record: IdentityLoginSessionOrmRecord) -> IdentitySessionRecord:
    return IdentitySessionRecord(
        id=record.id,
        user_id=record.user_id,
        client_id=record.client_id,
        client_name=record.client_name,
        device_name=record.device_name,
        session_secret_hash=record.session_secret_hash,
        created_at=record.created_at,
        last_seen_at=record.last_seen_at,
        idle_expires_at=record.idle_expires_at,
        absolute_expires_at=record.absolute_expires_at,
        revoked_at=record.revoked_at,
    )


def _authenticator_from_orm(
    record: IdentityAuthenticatorOrmRecord,
) -> IdentityAuthenticatorRecord:
    return IdentityAuthenticatorRecord(
        id=record.id,
        user_id=record.user_id,
        kind=record.kind,
        display_name=record.display_name,
        verifier=record.verifier,
        credential_metadata=record.credential_metadata,
        created_at=record.created_at,
        last_used_at=record.last_used_at,
        revoked_at=record.revoked_at,
    )


def _passkey_orm(
    record: IdentityAuthenticatorRecord, *, user_id: str
) -> IdentityAuthenticatorOrmRecord:
    return IdentityAuthenticatorOrmRecord(
        id=record.id,
        user_id=user_id,
        kind="passkey",
        display_name=record.display_name,
        verifier=record.verifier,
        credential_id=str(record.credential_metadata["credential_id"]),
        credential_metadata=record.credential_metadata,
        created_at=record.created_at,
        last_used_at=record.last_used_at,
        revoked_at=record.revoked_at,
    )


def _credential_exists(records: Iterable[IdentityAuthenticatorRecord], credential_id: str) -> bool:
    return any(
        record.credential_metadata.get("credential_id") == credential_id
        and record.revoked_at is None
        for record in records
    )


__all__ = ["InMemoryHostedIdentityRepository", "PostgresHostedIdentityRepository"]
