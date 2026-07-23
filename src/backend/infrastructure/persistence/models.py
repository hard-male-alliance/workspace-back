"""@brief PostgreSQL ORM 模型与稳定表元数据 / PostgreSQL ORM models and stable metadata.

这些模型是持久化映射，不是领域实体。面向用户的语义文档、Agent 输出和 rubric 等
可演进负载保存在 JSONB（JSON Binary）列中；资源之间的身份、版本、归属、事件与
检索关系仍使用外键及独立表表达，避免把整个系统退化为无约束 JSON 文档库。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from backend.domain.outbox import NOTIFICATION_EVENT_TYPES, WORK_EVENT_TYPES

JsonObject = dict[str, Any]

_OUTBOX_WORK_EVENT_TYPES_SQL = ", ".join(
    f"'{event_type}'" for event_type in sorted(WORK_EVENT_TYPES)
)
"""@brief ORM 约束使用的 work 事件 SQL 闭集 / SQL closed set of work events used by ORM constraints."""

_OUTBOX_NOTIFICATION_EVENT_TYPES_SQL = ", ".join(
    f"'{event_type}'" for event_type in sorted(NOTIFICATION_EVENT_TYPES)
)
"""@brief ORM 约束使用的 notification SQL 闭集 / SQL closed set of notification events used by ORM constraints."""


NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
"""@brief 约束与索引的稳定命名约定 / Stable naming convention for constraints and indexes."""


class Base(DeclarativeBase):
    """@brief 所有持久化映射的 Declarative 基类 / Declarative base for persistence mappings.

    @note Alembic 仅把 ``Base.metadata`` 当作 autogenerate 的目标；初始 migration
    本身是冻结的显式 DDL，不依赖未来模型演进。
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TimestampedRevisionMixin:
    """@brief 资源生命周期与乐观版本字段 / Lifecycle and optimistic-version fields.

    此 mixin 不含主键，允许少数全局 identity 资源与普通 tenant 资源复用相同的
    审计时间和 ``revision`` 语义。
    """

    __abstract__ = True

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    revision: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    extensions: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class TenantScopedMixin(TimestampedRevisionMixin):
    """@brief 多租户资源字段 / Multi-tenant resource fields.

    ``workspace_id`` 是隔离分区，``resource_owner_id`` 指向该资源的个人所有者。
    两者同时参与应用层谓词和 PostgreSQL RLS（Row-Level Security）策略。
    """

    __abstract__ = True

    workspace_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
        nullable=False,
    )
    resource_owner_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )


class WorkspaceScopedMixin(TimestampedRevisionMixin):
    """@brief 仅以 Workspace 隔离的 V2 资源字段 / V2 resource fields isolated only by Workspace.

    @note V2 协作资源属于 Workspace，而不属于创建者个人。新模型不得再把
    ``resource_owner_id`` 混入租户键，否则合法协作者会被错误隔离。
    / V2 collaborative resources belong to the Workspace, not their creator; new models must not
    include ``resource_owner_id`` in the tenant key.
    """

    __abstract__ = True

    workspace_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
        nullable=False,
    )


class UserRecord(Base, TimestampedRevisionMixin):
    """@brief 登录主体的稳定身份记录 / Stable identity record for a login principal.

    用户是全局 identity，不属于某一个 workspace，因此不继承 TenantScopedMixin；
    实际业务资源始终通过 ``resource_owner_id`` 关联到它。
    """

    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("external_subject", name="users_external_subject"),
        CheckConstraint(
            "account_status IN ('active', 'suspended', 'deletion_scheduled', 'deleted')",
            name="users_account_status",
        ),
        CheckConstraint(
            "account_status = 'deleted' OR (email IS NOT NULL "
            "AND email = btrim(email) "
            "AND email_canonical IS NOT NULL "
            "AND email_canonical = lower(email) "
            "AND length(btrim(email)) BETWEEN 3 AND 320 "
            "AND lower(btrim(email)) ~ "
            "'^[^[:space:]@]+@[^[:space:]@]+\\.[^[:space:]@]+$' "
            "AND display_name IS NOT NULL "
            "AND display_name = btrim(display_name) "
            "AND length(display_name) BETWEEN 1 AND 120 "
            "AND locale ~ '^[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*$' "
            "AND locale = btrim(locale) AND length(locale) BETWEEN 2 AND 35 "
            "AND external_subject = btrim(external_subject) "
            "AND length(external_subject) BETWEEN 1 AND 255)",
            name="users_v2_profile",
        ),
        Index(
            "uq_users_email_canonical",
            "email_canonical",
            unique=True,
            postgresql_where=text("email_canonical IS NOT NULL"),
        ),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    external_subject: Mapped[str] = mapped_column(String(320), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(256))
    email: Mapped[str | None] = mapped_column(String(320))
    email_canonical: Mapped[str | None] = mapped_column(String(320))
    email_verified: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    account_status: Mapped[str] = mapped_column(
        String(24), nullable=False, server_default=text("'active'")
    )
    default_workspace_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("identity.workspaces.id", ondelete="SET NULL")
    )
    locale: Mapped[str] = mapped_column(String(35), nullable=False, server_default=text("'zh-CN'"))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OAuthAuthorizationRequestRecord(Base):
    """Short-lived Authorization Code + PKCE transaction state."""

    __tablename__ = "oauth_authorization_requests"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'authenticated', 'consented', 'code_issued', 'expired', 'cancelled')",
            name="oauth_authorization_requests_status",
        ),
        CheckConstraint(
            "code_challenge_method = 'S256'",
            name="oauth_authorization_requests_pkce_s256",
        ),
        Index("ix_oauth_authorization_requests_expires", "expires_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    client_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    redirect_uri: Mapped[str] = mapped_column(Text, nullable=False)
    scope: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(String(512), nullable=False)
    nonce: Mapped[str] = mapped_column(String(512), nullable=False)
    code_challenge: Mapped[str] = mapped_column(String(128), nullable=False)
    code_challenge_method: Mapped[str] = mapped_column(String(8), nullable=False)
    prompt: Mapped[str] = mapped_column(String(128), nullable=False, server_default=text("''"))
    screen_hint: Mapped[str | None] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class IdentityBrowserSessionRecord(Base):
    """Hashed hosted-UI browser binding; no raw Cookie or CSRF value is stored."""

    __tablename__ = "identity_browser_sessions"
    __table_args__ = (
        Index("ix_identity_browser_sessions_expires", "expires_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    authorization_request_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.oauth_authorization_requests.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    browser_secret_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    csrf_token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    user_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class IdentityFlowRecord(Base):
    """Secret-free durable finite-state flow bound to an OAuth browser transaction."""

    __tablename__ = "identity_flows"
    __table_args__ = (
        CheckConstraint(
            "purpose IN ('register', 'login', 'recover', 'reauthenticate')",
            name="identity_flows_purpose",
        ),
        CheckConstraint(
            "status IN ('pending', 'verified', 'completed', 'failed', 'expired')",
            name="identity_flows_status",
        ),
        CheckConstraint(
            "(status = 'completed' AND completed_at IS NOT NULL "
            "AND completed_at >= created_at AND completed_at <= expires_at) OR "
            "(status <> 'completed' AND completed_at IS NULL)",
            name="identity_flows_completion",
        ),
        Index("ix_identity_flows_expires", "expires_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    purpose: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    allowed_steps: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    authorization_request_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.oauth_authorization_requests.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    browser_session_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.identity_browser_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    client_id: Mapped[str] = mapped_column(String(128), nullable=False)
    redirect_uri: Mapped[str] = mapped_column(Text, nullable=False)
    code_challenge: Mapped[str] = mapped_column(String(128), nullable=False)
    authorization_resume_uri: Mapped[str | None] = mapped_column(Text)
    webauthn_options: Mapped[JsonObject | None] = mapped_column(JSONB)
    user_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="SET NULL")
    )
    internal_state: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class IdentityFlowStepRecord(Base):
    """One-time step-id receipt used to make secret-bearing retries idempotent."""

    __tablename__ = "identity_flow_steps"
    __table_args__ = (
        UniqueConstraint("flow_id", "step_id", name="identity_flow_steps_flow_step"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    flow_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.identity_flows.id", ondelete="CASCADE"), nullable=False
    )
    step_id: Mapped[str] = mapped_column(String(160), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class IdentityAuthenticatorRecord(Base):
    """Password/passkey/recovery verifier metadata; never raw credentials."""

    __tablename__ = "identity_authenticators"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('passkey', 'password', 'recovery_code')",
            name="identity_authenticators_kind",
        ),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    verifier: Mapped[str] = mapped_column(Text, nullable=False)
    credential_id: Mapped[str | None] = mapped_column(String(1024), unique=True)
    credential_metadata: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class IdentityLoginSessionRecord(Base):
    """Hashed Authorization Server session cookie with idle and absolute expiry."""

    __tablename__ = "identity_login_sessions"
    __table_args__ = (
        Index("ix_identity_login_sessions_expires", "absolute_expires_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    client_id: Mapped[str] = mapped_column(String(128), nullable=False)
    client_name: Mapped[str] = mapped_column(String(120), nullable=False)
    device_name: Mapped[str | None] = mapped_column(String(200))
    session_secret_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    idle_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    absolute_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OAuthAuthorizationCodeRecord(Base):
    """Hashed, one-time authorization code bound to client, redirect and PKCE challenge."""

    __tablename__ = "oauth_authorization_codes"
    __table_args__ = (
        UniqueConstraint("code_hash", name="oauth_authorization_codes_code_hash"),
        CheckConstraint(
            "consumed_at IS NOT NULL OR login_session_id IS NOT NULL",
            name="oauth_authorization_codes_active_session",
        ),
        Index("ix_oauth_authorization_codes_expires", "expires_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    code_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    authorization_request_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.oauth_authorization_requests.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    subject: Mapped[str] = mapped_column(String(320), nullable=False)
    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="RESTRICT"), nullable=False
    )
    login_session_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("identity.identity_login_sessions.id", ondelete="RESTRICT"),
        index=True,
    )
    client_id: Mapped[str] = mapped_column(String(128), nullable=False)
    redirect_uri: Mapped[str] = mapped_column(Text, nullable=False)
    scope: Mapped[str] = mapped_column(Text, nullable=False)
    nonce: Mapped[str] = mapped_column(String(512), nullable=False)
    code_challenge: Mapped[str] = mapped_column(String(128), nullable=False)
    auth_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OAuthRefreshTokenFamilyRecord(Base):
    """Revocable family shared by every rotated refresh token in one durable grant."""

    __tablename__ = "oauth_refresh_token_families"
    __table_args__ = (
        CheckConstraint(
            "revoked_at IS NOT NULL OR login_session_id IS NOT NULL",
            name="oauth_refresh_token_families_active_session",
        ),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    subject: Mapped[str] = mapped_column(String(320), nullable=False)
    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="RESTRICT"), nullable=False
    )
    client_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    login_session_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("identity.identity_login_sessions.id", ondelete="RESTRICT"),
        index=True,
    )
    scope: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reuse_detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OAuthRefreshTokenRecord(Base):
    """Hashed refresh token that can be consumed exactly once."""

    __tablename__ = "oauth_refresh_tokens"
    __table_args__ = (
        UniqueConstraint("token_hash", name="oauth_refresh_tokens_token_hash"),
        UniqueConstraint("family_id", "sequence", name="oauth_refresh_tokens_family_sequence"),
        Index("ix_oauth_refresh_tokens_expires", "expires_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    family_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.oauth_refresh_token_families.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    replaced_by_token_id: Mapped[str | None] = mapped_column(String(128))


class OAuthRevokedAccessTokenRecord(Base):
    """Hashed JWT identifier retained only until the access token expires."""

    __tablename__ = "oauth_revoked_access_tokens"
    __table_args__ = (
        Index("ix_oauth_revoked_access_tokens_expires", "expires_at"),
        {"schema": "identity"},
    )

    jti_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    revoked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class WorkspaceRecord(Base, TimestampedRevisionMixin):
    """@brief 工作区租户根记录 / Workspace tenant-root record."""

    __tablename__ = "workspaces"
    __table_args__ = (
        CheckConstraint(
            "deleted_at IS NOT NULL OR (name = btrim(name) AND length(name) BETWEEN 1 AND 120)",
            name="workspaces_name",
        ),
        CheckConstraint(
            "slug ~ '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'",
            name="workspaces_slug",
        ),
        CheckConstraint("plan IN ('personal', 'team', 'enterprise')", name="workspaces_plan"),
        CheckConstraint(
            "data_region IN ('cn', 'global', 'private_deployment')",
            name="workspaces_data_region",
        ),
        Index(
            "uq_workspaces_slug",
            "slug",
            unique=True,
            postgresql_where=text("deleted_at IS NULL"),
        ),
        UniqueConstraint(
            "id",
            "resource_owner_id",
            name="uq_workspaces_id_resource_owner",
        ),
        Index(
            "ix_workspaces_resource_owner_id_updated_at",
            "resource_owner_id",
            "updated_at",
        ),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    resource_owner_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="RESTRICT"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    slug: Mapped[str] = mapped_column(String(63), nullable=False)
    plan: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'personal'"))
    data_region: Mapped[str] = mapped_column(String(24), nullable=False)
    default_locale: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=text("'zh-CN'")
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkspaceMemberRecord(Base, TenantScopedMixin):
    """@brief 工作区成员及角色记录 / Workspace membership and role record."""

    __tablename__ = "workspace_members"
    __table_args__ = (
        CheckConstraint(
            "display_name = btrim(display_name) AND length(display_name) BETWEEN 1 AND 120",
            name="workspace_members_display_name",
        ),
        UniqueConstraint("workspace_id", "user_id", name="workspace_members_workspace_user"),
        UniqueConstraint(
            "id",
            "workspace_id",
            "resource_owner_id",
            name="uq_tnt_workspace_members_id_ws_owner",
        ),
        ForeignKeyConstraint(
            ("workspace_id", "resource_owner_id"),
            ("identity.workspaces.id", "identity.workspaces.resource_owner_id"),
            name="fk_tnt_workspace_members_workspace_scope",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "role IN ('owner', 'admin', 'editor', 'viewer')", name="workspace_members_role"
        ),
        CheckConstraint("status IN ('active', 'suspended')", name="workspace_members_status_v2"),
        Index("ix_workspace_members_workspace_id", "workspace_id"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    display_name: Mapped[str] = mapped_column(String(120), nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'active'"))
    invited_by_actor_id: Mapped[str | None] = mapped_column(String(128))
    joined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkspaceInvitationRecord(Base, WorkspaceScopedMixin):
    """@brief 工作区成员邀请及其有限状态机 / Workspace invitation and finite-state machine.

    @note ``email_canonical`` 仅供等值匹配与唯一性约束，API 响应只能暴露脱敏后的
    ``email_hint`` / ``email_canonical`` is private matching state; APIs expose only ``email_hint``.
    """

    __tablename__ = "workspace_invitations"
    __table_args__ = (
        CheckConstraint("role IN ('admin', 'editor', 'viewer')", name="workspace_invitations_role"),
        CheckConstraint(
            "status IN ('pending', 'accepted', 'revoked', 'expired')",
            name="workspace_invitations_status",
        ),
        CheckConstraint(
            "(status = 'pending' AND resolved_at IS NULL "
            "AND accepted_by_user_id IS NULL) OR "
            "(status = 'accepted' AND resolved_at IS NOT NULL "
            "AND accepted_by_user_id IS NOT NULL) OR "
            "(status IN ('revoked', 'expired') AND resolved_at IS NOT NULL "
            "AND accepted_by_user_id IS NULL)",
            name="workspace_invitations_state",
        ),
        Index("ix_workspace_invitations_workspace_created", "workspace_id", "created_at", "id"),
        Index(
            "ix_workspace_invitations_pending_expiry",
            "expires_at",
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "uq_workspace_invitations_pending_email",
            "workspace_id",
            "email_canonical",
            unique=True,
            postgresql_where=text("status = 'pending'"),
        ),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    email_canonical: Mapped[str] = mapped_column(String(320), nullable=False)
    email_hint: Mapped[str] = mapped_column(String(320), nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    invited_by_actor_id: Mapped[str | None] = mapped_column(String(128))
    accepted_by_user_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="RESTRICT")
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AccountDeletionRequestRecord(Base, TimestampedRevisionMixin):
    """@brief 账号删除请求及其执行状态 / Account-deletion request and execution state.

    @note 这是全局用户资源而非 Workspace 资源；数据库 RLS（Row-Level Security）
    以 ``user_id = app.actor_id`` 精确隔离 / This is a global user resource isolated by actor ID.
    """

    __tablename__ = "account_deletion_requests"
    __table_args__ = (
        CheckConstraint(
            "status IN ('scheduled', 'running', 'completed', 'cancelled', 'failed')",
            name="account_deletion_requests_status",
        ),
        CheckConstraint(
            "(status IN ('scheduled', 'running', 'cancelled') AND completed_at IS NULL "
            "AND problem IS NULL) OR (status = 'completed' AND completed_at IS NOT NULL "
            "AND problem IS NULL) OR (status = 'failed' AND completed_at IS NULL "
            "AND problem IS NOT NULL)",
            name="account_deletion_requests_state",
        ),
        Index("ix_account_deletion_requests_user_created", "user_id", "created_at", "id"),
        Index(
            "uq_account_deletion_requests_live_user",
            "user_id",
            unique=True,
            postgresql_where=text("status IN ('scheduled', 'running')"),
        ),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'scheduled'")
    )
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    problem: Mapped[JsonObject | None] = mapped_column(JSONB)


class OAuthUserTokenRevocationRecord(Base):
    """@brief 用户级 access-token 撤销 epoch / User-level access-token revocation epoch.

    @note 该表不枚举 JWT（JSON Web Token），而是以一个单调时间水位撤销已签发的
        全部用户 token / This table revokes all previously issued user tokens through one
        monotonic timestamp rather than enumerating JWTs.
    """

    __tablename__ = "oauth_user_token_revocations"
    __table_args__ = ({"schema": "identity"},)

    user_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.users.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    revoked_before: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class AccountDeletionWorkspaceDispositionRecord(Base):
    """@brief 删除开始时冻结的 Workspace 处置 / Workspace disposition frozen when deletion starts."""

    __tablename__ = "account_deletion_workspace_dispositions"
    __table_args__ = (
        CheckConstraint(
            "disposition IN ('personal', 'shared')",
            name="account_deletion_workspace_disposition_values",
        ),
        {"schema": "identity"},
    )

    request_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.account_deletion_requests.id", ondelete="CASCADE"),
        primary_key=True,
    )
    workspace_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    disposition: Mapped[str] = mapped_column(String(16), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class AccountDeletionErasureItemRecord(Base):
    """@brief 崩溃可恢复的外部擦除 work item / Crash-recoverable external-erasure work item."""

    __tablename__ = "account_deletion_erasure_items"
    __table_args__ = (
        CheckConstraint(
            "resource_kind IN ('upload_object', 'credential_scope') "
            "AND resource_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="account_deletion_erasure_item_resource",
        ),
        CheckConstraint(
            "attempt_count BETWEEN 0 AND 100 AND ("
            "(status = 'pending' AND lease_token_hash IS NULL AND lease_expires_at IS NULL) OR "
            "(status = 'processing' AND lease_token_hash ~ '^[a-f0-9]{64}$' "
            "AND lease_expires_at IS NOT NULL) OR "
            "(status IN ('completed', 'failed') AND lease_token_hash IS NULL "
            "AND lease_expires_at IS NULL))",
            name="account_deletion_erasure_item_lifecycle",
        ),
        CheckConstraint(
            "last_error_code IS NULL OR last_error_code ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="account_deletion_erasure_item_error",
        ),
        Index(
            "ix_account_deletion_erasure_items_due",
            "status",
            "lease_expires_at",
            "created_at",
            "resource_id",
            postgresql_where=text("status IN ('pending', 'processing')"),
        ),
        {"schema": "identity"},
    )

    request_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.account_deletion_requests.id", ondelete="CASCADE"),
        primary_key=True,
    )
    workspace_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    resource_kind: Mapped[str] = mapped_column(String(32), primary_key=True)
    resource_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    lease_token_hash: Mapped[str | None] = mapped_column(String(64))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(101))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class AuditEventRecord(Base, TenantScopedMixin):
    """@brief 面向审计的安全与数据访问事件 / Security and data-access audit event."""

    __tablename__ = "audit_events"
    __table_args__ = (
        CheckConstraint(
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="audit_events_v2_id",
        ),
        CheckConstraint(
            "actor_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND actor_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (actor_revision IS NULL OR actor_revision >= 1)",
            name="audit_events_v2_actor",
        ),
        CheckConstraint(
            "action ~ '^[a-z][a-z0-9_.-]{2,127}$'",
            name="audit_events_v2_action",
        ),
        CheckConstraint(
            "resource_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND resource_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (resource_revision IS NULL OR resource_revision >= 1)",
            name="audit_events_v2_target",
        ),
        CheckConstraint(
            "outcome IN ('allowed', 'denied', 'failed')",
            name="audit_events_v2_outcome",
        ),
        CheckConstraint(
            "request_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="audit_events_v2_request",
        ),
        Index(
            "ix_audit_events_workspace_occurred_id",
            "workspace_id",
            "occurred_at",
            "id",
        ),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    actor_type: Mapped[str] = mapped_column(
        String(101), nullable=False, server_default=text("'user'")
    )
    actor_id: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    actor_revision: Mapped[int | None] = mapped_column(Integer)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(101), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(160), nullable=False)
    resource_revision: Mapped[int | None] = mapped_column(Integer)
    request_id: Mapped[str] = mapped_column(String(160), nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    details: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class IdempotencyRecord(Base, TenantScopedMixin):
    """@brief 幂等请求的持久化去重记录 / Persisted idempotent-request deduplication record."""

    __tablename__ = "idempotency_records"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "actor_id",
            "request_target",
            "idempotency_key",
            name="idempotency_scope_target_key",
        ),
        Index("ix_idempotency_records_expires", "expires_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    request_target: Mapped[str] = mapped_column(String(256), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    response_status: Mapped[int | None] = mapped_column(SmallInteger)
    response_body: Mapped[JsonObject | None] = mapped_column(JSONB)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class JobRecord(Base, TenantScopedMixin):
    """@brief 跨子领域的长任务资源 / Cross-subdomain long-running job resource."""

    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("id", "workspace_id", name="agent_jobs_v2_id_workspace"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
            name="jobs_status",
        ),
        CheckConstraint(
            "job_type NOT LIKE 'resume.%' OR request_payload IS NOT NULL",
            name="jobs_resume_request_payload",
        ),
        CheckConstraint(
            "job_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND target_resource_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND target_resource_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (target_resource_revision IS NULL OR target_resource_revision >= 1)",
            name="jobs_v2_subject",
        ),
        CheckConstraint(
            "phase = btrim(phase) AND length(phase) BETWEEN 1 AND 80 "
            "AND completed_units >= 0 "
            "AND (total_units IS NULL OR (total_units >= 0 AND completed_units <= total_units)) "
            "AND progress_unit IN ('items', 'bytes', 'pages', 'steps', 'unknown')",
            name="jobs_v2_progress",
        ),
        CheckConstraint(
            "jsonb_typeof(result_refs) = 'array' "
            "AND jsonb_array_length(result_refs) <= 50 "
            "AND (status = 'succeeded' OR jsonb_array_length(result_refs) = 0) "
            "AND ((status = 'failed' AND problem IS NOT NULL) "
            "OR (status <> 'failed' AND problem IS NULL))",
            name="jobs_v2_result_problem",
        ),
        CheckConstraint(
            "(status = 'queued' AND started_at IS NULL AND finished_at IS NULL) OR "
            "(status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND started_at IS NOT NULL "
            "AND finished_at IS NOT NULL) OR "
            "(status = 'cancelled' AND finished_at IS NOT NULL) OR "
            "(status = 'expired' AND started_at IS NULL AND finished_at IS NOT NULL)",
            name="jobs_v2_timeline",
        ),
        Index(
            "ix_jobs_workspace_created_id",
            "workspace_id",
            "created_at",
            "id",
        ),
        Index(
            "ix_jobs_workspace_kind_subject_created",
            "workspace_id",
            "job_type",
            "target_resource_type",
            "target_resource_id",
            "created_at",
            "id",
        ),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    job_type: Mapped[str] = mapped_column(String(101), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'queued'"))
    phase: Mapped[str] = mapped_column(String(80), nullable=False, server_default=text("'queued'"))
    completed_units: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    total_units: Mapped[int | None] = mapped_column(Integer)
    progress_unit: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'unknown'")
    )
    percent: Mapped[float | None] = mapped_column(Float)
    request_id: Mapped[str | None] = mapped_column(String(128))
    target_resource_type: Mapped[str] = mapped_column(String(101), nullable=False)
    target_resource_id: Mapped[str] = mapped_column(String(160), nullable=False)
    target_resource_revision: Mapped[int | None] = mapped_column(Integer)
    result_refs: Mapped[list[JsonObject]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb")
    )
    problem: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))
    result: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))
    error: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    request_payload: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))


class OutboxEventRecord(Base, TenantScopedMixin):
    """@brief 事务发件箱事件 / Transactional outbox event.

    业务事务与事件写入共用同一个数据库提交；后台消费者在提交后异步投递，避免
    “资源已保存而消息丢失”的双写失败（dual-write failure）。
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'processing', 'published', 'failed')", name="outbox_status"
        ),
        CheckConstraint(
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND aggregate_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND aggregate_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (subject_revision IS NULL OR subject_revision >= 1) "
            "AND event_type ~ '^[a-z][a-z0-9_.-]{2,127}$' "
            "AND sequence >= 1",
            name="outbox_events_v2_envelope",
        ),
        CheckConstraint(
            "jsonb_typeof(payload) = 'object' "
            "AND jsonb_array_length(jsonb_path_query_array(payload, '$.keyvalue()')) <= 40",
            name="outbox_events_v2_payload",
        ),
        CheckConstraint(
            "trace_id IS NULL OR trace_id ~ '^[a-f0-9]{32}$'",
            name="outbox_events_v2_trace",
        ),
        CheckConstraint(
            "replay_expires_at > occurred_at",
            name="outbox_events_v2_replay_window",
        ),
        CheckConstraint(
            f"(event_type IN ({_OUTBOX_WORK_EVENT_TYPES_SQL}) "
            "AND status IN ('pending', 'processing', 'published', 'failed')) OR "
            f"(event_type IN ({_OUTBOX_NOTIFICATION_EVENT_TYPES_SQL}) "
            "AND status = 'published' AND published_at IS NOT NULL)",
            name="outbox_events_delivery_class",
        ),
        UniqueConstraint(
            "workspace_id",
            "sequence",
            name="outbox_events_workspace_sequence",
        ),
        Index("ix_outbox_events_status_occurred", "status", "occurred_at"),
        Index(
            "ix_outbox_events_workspace_sequence",
            "workspace_id",
            "sequence",
        ),
        Index(
            "ix_outbox_events_replay_expiry",
            "replay_expires_at",
            "workspace_id",
            "sequence",
        ),
        Index(
            "ix_outbox_events_terminal_replay_expiry",
            "replay_expires_at",
            "id",
            postgresql_where=text("status IN ('published', 'failed')"),
        ),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(101), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(160), nullable=False)
    subject_revision: Mapped[int | None] = mapped_column(Integer)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    trace_id: Mapped[str | None] = mapped_column(String(32))
    replay_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))


class WorkspaceEventSequenceRecord(Base):
    """@brief Workspace event stream 的事务计数器 / Transactional counter for a Workspace event stream.

    @note 应用角色不能直接读写；``agent.assign_workspace_event_sequence`` trigger 是唯一
        mutation 入口。/ The application role cannot access this table directly; the trigger is
        its sole mutation entry point.
    """

    __tablename__ = "workspace_event_sequences"
    __table_args__ = (
        CheckConstraint("last_sequence >= 0", name="nonnegative"),
        {"schema": "agent"},
    )

    workspace_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.workspaces.id", ondelete="CASCADE"),
        primary_key=True,
    )
    last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ArtifactRecord(Base, WorkspaceScopedMixin):
    """@brief API V2 唯一 Artifact metadata 真相表 / Sole API V2 Artifact metadata source of truth."""

    __tablename__ = "artifacts"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('resume_pdf', 'resume_json', 'resume_docx', 'interview_audio', "
            "'interview_video', 'interview_transcript', 'generic')",
            name="artifacts_kind",
        ),
        CheckConstraint(
            "subject_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND subject_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (subject_revision IS NULL OR subject_revision >= 1)",
            name="artifacts_subject",
        ),
        CheckConstraint(
            "media_type ~ '^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$'",
            name="artifacts_media_type",
        ),
        CheckConstraint(
            "size_bytes BETWEEN 0 AND 1073741824 AND sha256 ~ '^[a-f0-9]{64}$'",
            name="artifacts_content_identity",
        ),
        CheckConstraint(
            "page_count IS NULL OR page_count >= 1",
            name="artifacts_page_count",
        ),
        CheckConstraint(
            "expires_at IS NULL OR expires_at > created_at",
            name="artifacts_expiration",
        ),
        CheckConstraint(
            "deleted_at IS NULL OR deleted_at >= created_at",
            name="artifacts_deletion",
        ),
        UniqueConstraint("storage_key", name="artifacts_storage_key"),
        UniqueConstraint("id", "workspace_id", name="uq_artifacts_id_workspace"),
        Index(
            "ix_artifacts_workspace_created_id",
            "workspace_id",
            "created_at",
            "id",
        ),
        Index(
            "ix_artifacts_workspace_kind_subject_created",
            "workspace_id",
            "kind",
            "subject_type",
            "subject_id",
            "created_at",
            "id",
        ),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    subject_type: Mapped[str] = mapped_column(String(101), nullable=False)
    subject_id: Mapped[str] = mapped_column(String(160), nullable=False)
    subject_revision: Mapped[int | None] = mapped_column(Integer)
    media_type: Mapped[str] = mapped_column(String(255), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    page_count: Mapped[int | None] = mapped_column(Integer)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ArtifactContentRecord(Base, WorkspaceScopedMixin):
    """@brief Artifact 的可验证 BYTEA 内容对象 / Verifiable BYTEA content object for an Artifact."""

    __tablename__ = "artifact_contents"
    __table_args__ = (
        CheckConstraint(
            "size_bytes BETWEEN 0 AND 1073741824 "
            "AND octet_length(content) = size_bytes "
            "AND sha256 ~ '^[a-f0-9]{64}$'",
            name="artifact_contents_identity",
        ),
        CheckConstraint(
            "media_type ~ '^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$'",
            name="artifact_contents_media_type",
        ),
        ForeignKeyConstraint(
            ["artifact_id", "workspace_id"],
            ["agent.artifacts.id", "agent.artifacts.workspace_id"],
            name="fk_artifact_contents_artifact_scope",
            ondelete="CASCADE",
        ),
        UniqueConstraint("storage_key", name="artifact_contents_storage_key"),
        {"schema": "agent"},
    )

    artifact_id: Mapped[str] = mapped_column(
        String(160),
        primary_key=True,
    )
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    media_type: Mapped[str] = mapped_column(String(255), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class ArtifactPdfSourceMapRecord(Base, WorkspaceScopedMixin):
    """@brief Resume PDF Artifact 的规范 source map / Canonical source map for a Resume PDF Artifact."""

    __tablename__ = "artifact_pdf_source_maps"
    __table_args__ = (
        CheckConstraint(
            "resume_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' AND resume_revision >= 1",
            name="artifact_pdf_source_maps_resume",
        ),
        CheckConstraint(
            "CASE WHEN jsonb_typeof(nodes) = 'array' "
            "THEN jsonb_array_length(nodes) <= 10000 ELSE false END",
            name="artifact_pdf_source_maps_nodes",
        ),
        ForeignKeyConstraint(
            ["artifact_id", "workspace_id"],
            ["agent.artifacts.id", "agent.artifacts.workspace_id"],
            name="fk_artifact_pdf_source_maps_artifact_scope",
            ondelete="CASCADE",
        ),
        {"schema": "agent"},
    )

    artifact_id: Mapped[str] = mapped_column(
        String(160),
        primary_key=True,
    )
    resume_id: Mapped[str] = mapped_column(String(160), nullable=False)
    resume_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    nodes: Mapped[list[JsonObject]] = mapped_column(JSONB, nullable=False)


class ConversationRecord(Base, TenantScopedMixin):
    """@brief Agent 对话会话 / Agent conversation session."""

    __tablename__ = "conversations"
    __table_args__ = (
        CheckConstraint(
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="conversations_v2_id",
        ),
        CheckConstraint(
            "title IS NULL OR length(title) <= 300",
            name="conversations_v2_title",
        ),
        CheckConstraint(
            "capability IN ('general', 'resume_edit', 'knowledge_query', 'interview_coach')",
            name="conversations_v2_capability",
        ),
        CheckConstraint(
            "status IN ('active', 'archived')",
            name="conversations_v2_status",
        ),
        CheckConstraint(
            "message_sequence >= 0 AND (deleted_at IS NULL OR status = 'archived')",
            name="conversations_v2_state",
        ),
        UniqueConstraint("id", "workspace_id", name="conversations_v2_id_workspace"),
        Index(
            "ix_conversations_workspace_created_id",
            "workspace_id",
            "created_at",
            "id",
        ),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    title: Mapped[str | None] = mapped_column(String(300))
    capability: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'active'")
    )
    message_sequence: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ChatMessageRecord(Base, TenantScopedMixin):
    """@brief 对话中的结构化消息 / Structured message within a conversation."""

    __tablename__ = "messages"
    __table_args__ = (
        CheckConstraint(
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' AND sequence >= 1",
            name="messages_v2_identity",
        ),
        CheckConstraint(
            "role IN ('user', 'assistant', 'system_notice')",
            name="messages_v2_role",
        ),
        CheckConstraint(
            "jsonb_typeof(content_parts) = 'array' "
            "AND jsonb_array_length(content_parts) BETWEEN 1 AND 100",
            name="messages_v2_content",
        ),
        CheckConstraint(
            "revision = 1 AND updated_at = created_at",
            name="messages_v2_append_only",
        ),
        CheckConstraint(
            "(role = 'assistant' AND source_run_id IS NOT NULL) OR "
            "(role <> 'assistant' AND source_run_id IS NULL)",
            name="messages_v2_source_run",
        ),
        CheckConstraint(
            "parent_message_id IS NULL OR parent_message_id <> id",
            name="messages_v2_parent",
        ),
        UniqueConstraint(
            "workspace_id",
            "conversation_id",
            "sequence",
            name="messages_v2_conversation_sequence",
        ),
        UniqueConstraint(
            "id",
            "workspace_id",
            "conversation_id",
            name="messages_v2_id_workspace_conversation",
        ),
        ForeignKeyConstraint(
            ["conversation_id", "workspace_id"],
            ["agent.conversations.id", "agent.conversations.workspace_id"],
            name="fk_messages_v2_conversation_workspace",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["parent_message_id", "workspace_id", "conversation_id"],
            ["agent.messages.id", "agent.messages.workspace_id", "agent.messages.conversation_id"],
            name="fk_messages_v2_parent_scope",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["source_run_id", "workspace_id"],
            ["agent.runs.id", "agent.runs.workspace_id"],
            name="fk_messages_v2_source_run_workspace",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        Index(
            "ix_messages_workspace_conversation_sequence_id",
            "workspace_id",
            "conversation_id",
            "sequence",
            "id",
        ),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(String(160), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    content_parts: Mapped[list[JsonObject]] = mapped_column(JSONB, nullable=False)
    parent_message_id: Mapped[str | None] = mapped_column(String(160))
    source_run_id: Mapped[str | None] = mapped_column(String(160))


class AgentRunRecord(Base, TenantScopedMixin):
    """@brief 一次可审计 Agent 执行 / One auditable Agent execution."""

    __tablename__ = "runs"
    __table_args__ = (
        CheckConstraint(
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="agent_runs_v2_id",
        ),
        CheckConstraint(
            "capability IN ('general', 'resume_edit', 'knowledge_query', 'interview_coach')",
            name="agent_runs_v2_capability",
        ),
        CheckConstraint(
            "status IN ('queued', 'running', 'waiting_for_approval', 'succeeded', 'failed', 'cancelled')",
            name="agent_runs_v2_status",
        ),
        CheckConstraint(
            "jsonb_typeof(spec) = 'object' AND jsonb_typeof(execution_grant) = 'object' "
            "AND jsonb_typeof(proposal_refs) = 'array' "
            "AND jsonb_array_length(proposal_refs) <= 100",
            name="agent_runs_v2_snapshots",
        ),
        CheckConstraint(
            "(status = 'waiting_for_approval' AND pending_approval_id IS NOT NULL "
            "AND active_tool_call_id IS NOT NULL AND problem IS NULL) OR "
            "(status <> 'waiting_for_approval' AND pending_approval_id IS NULL "
            "AND active_tool_call_id IS NULL)",
            name="agent_runs_v2_approval_state",
        ),
        CheckConstraint(
            "(status = 'failed' AND problem IS NOT NULL) OR "
            "(status <> 'failed' AND problem IS NULL)",
            name="agent_runs_v2_problem",
        ),
        CheckConstraint(
            "(status = 'succeeded' AND output_message_id IS NOT NULL) OR "
            "(status <> 'succeeded' AND output_message_id IS NULL)",
            name="agent_runs_v2_output",
        ),
        CheckConstraint(
            "status IN ('succeeded', 'failed', 'cancelled') OR "
            "(jsonb_array_length(proposal_refs) = 0 AND usage IS NULL)",
            name="agent_runs_v2_terminal_results",
        ),
        UniqueConstraint("id", "workspace_id", name="agent_runs_v2_id_workspace"),
        ForeignKeyConstraint(
            ["conversation_id", "workspace_id"],
            ["agent.conversations.id", "agent.conversations.workspace_id"],
            name="fk_agent_runs_v2_conversation_workspace",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["input_message_id", "workspace_id", "conversation_id"],
            ["agent.messages.id", "agent.messages.workspace_id", "agent.messages.conversation_id"],
            name="fk_agent_runs_v2_input_message_scope",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["output_message_id", "workspace_id", "conversation_id"],
            ["agent.messages.id", "agent.messages.workspace_id", "agent.messages.conversation_id"],
            name="fk_agent_runs_v2_output_message_scope",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["job_id", "workspace_id"],
            ["agent.jobs.id", "agent.jobs.workspace_id"],
            name="fk_agent_runs_v2_job_workspace",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["pending_approval_id", "workspace_id"],
            ["agent.tool_approvals.id", "agent.tool_approvals.workspace_id"],
            name="fk_agent_runs_v2_pending_approval_workspace",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        Index(
            "ix_agent_runs_workspace_conversation_created_id",
            "workspace_id",
            "conversation_id",
            "created_at",
            "id",
        ),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(String(160), nullable=False)
    input_message_id: Mapped[str] = mapped_column(String(160), nullable=False)
    job_id: Mapped[str] = mapped_column(String(160), nullable=False, unique=True)
    capability: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'queued'"))
    spec: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    execution_grant: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    output_message_id: Mapped[str | None] = mapped_column(String(160))
    proposal_refs: Mapped[list[JsonObject]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb")
    )
    pending_approval_id: Mapped[str | None] = mapped_column(String(160))
    usage: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))
    problem: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))
    active_tool_call_id: Mapped[str | None] = mapped_column(String(160))


class ToolApprovalRecord(Base, TenantScopedMixin):
    """@brief 需要用户决定的 Agent 工具审批 / Agent tool approval requiring a user decision."""

    __tablename__ = "tool_approvals"
    __table_args__ = (
        CheckConstraint(
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND tool_call_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="tool_approvals_v2_identity",
        ),
        CheckConstraint(
            "tool_name ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND length(summary) BETWEEN 1 AND 2000 "
            "AND risk IN ('low', 'medium', 'high')",
            name="tool_approvals_v2_binding",
        ),
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')",
            name="tool_approvals_v2_status",
        ),
        CheckConstraint(
            "(status = 'pending' AND decision_by_type IS NULL AND decision_by_id IS NULL "
            "AND decision_by_revision IS NULL) OR "
            "(status <> 'pending' AND decision_by_type IS NOT NULL AND decision_by_id IS NOT NULL "
            "AND revision >= 2)",
            name="tool_approvals_v2_decision",
        ),
        CheckConstraint(
            "invocation_type = 'tool_invocation' AND expires_at > created_at",
            name="tool_approvals_v2_invocation",
        ),
        UniqueConstraint(
            "workspace_id",
            "run_id",
            "tool_call_id",
            name="tool_approvals_v2_run_call",
        ),
        UniqueConstraint(
            "id",
            "workspace_id",
            name="tool_approvals_v2_id_workspace",
        ),
        ForeignKeyConstraint(
            ["run_id", "workspace_id"],
            ["agent.runs.id", "agent.runs.workspace_id"],
            name="fk_tool_approvals_v2_run_workspace",
            ondelete="CASCADE",
        ),
        Index(
            "ix_tool_approvals_workspace_run_status",
            "workspace_id",
            "run_id",
            "status",
        ),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(160), nullable=False)
    tool_call_id: Mapped[str] = mapped_column(String(160), nullable=False)
    tool_name: Mapped[str] = mapped_column(String(101), nullable=False)
    summary: Mapped[str] = mapped_column(String(2000), nullable=False)
    risk: Mapped[str] = mapped_column(String(16), nullable=False)
    invocation_type: Mapped[str] = mapped_column(String(101), nullable=False)
    invocation_id: Mapped[str] = mapped_column(String(160), nullable=False)
    invocation_revision: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    decision_by_type: Mapped[str | None] = mapped_column(String(101))
    decision_by_id: Mapped[str | None] = mapped_column(String(160))
    decision_by_revision: Mapped[int | None] = mapped_column(Integer)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ResumeTemplateRecord(Base, TenantScopedMixin):
    """@brief 不可变简历模板版本 / Immutable resume-template version."""

    __tablename__ = "template_versions"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "template_id",
            "template_version",
            name="templates_scope_version",
        ),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    template_id: Mapped[str] = mapped_column(String(128), nullable=False)
    template_version: Mapped[str] = mapped_column(String(128), nullable=False)
    manifest: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    renderer_binding: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResumeDocumentRecord(Base, TenantScopedMixin):
    """@brief 简历聚合根元数据 / Resume aggregate-root metadata."""

    __tablename__ = "documents"
    __table_args__ = (
        CheckConstraint(
            "title = btrim(title) AND length(title) BETWEEN 1 AND 300",
            name="resume_documents_v2_title",
        ),
        CheckConstraint(
            "locale ~ '^[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*$'",
            name="resume_documents_v2_locale",
        ),
        CheckConstraint(
            "current_revision_no >= 1 AND revision = current_revision_no",
            name="resume_documents_v2_revision",
        ),
        Index("ix_resume_documents_workspace_updated", "workspace_id", "updated_at"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    template_version_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("resume.template_versions.id", ondelete="RESTRICT")
    )
    template_id: Mapped[str] = mapped_column(String(160), nullable=False)
    template_version: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    locale: Mapped[str] = mapped_column(String(35), nullable=False)
    current_revision_no: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1")
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResumeRevisionRecord(Base, TenantScopedMixin):
    """@brief 不可变的简历语义快照 / Immutable resume semantic snapshot."""

    __tablename__ = "revisions"
    __table_args__ = (
        UniqueConstraint("resume_id", "revision_no", name="resume_revisions_document_revision"),
        Index("ix_resume_revisions_resume_number", "resume_id", "revision_no"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    resume_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    semantic_document: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_by_actor_id: Mapped[str | None] = mapped_column(String(128))
    source: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'user'"))
    change_targets: Mapped[list[JsonObject]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb")
    )


class ResumeOperationBatchRecord(Base, TenantScopedMixin):
    """@brief 简历领域操作批次 / Resume domain-operation batch."""

    __tablename__ = "operation_batches"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "resume_id",
            "client_batch_id",
            name="resume_batches_client_id",
        ),
        UniqueConstraint(
            "workspace_id",
            "resume_id",
            "client_batch_id",
            name="resume_batches_v2_client_id",
        ),
        CheckConstraint(
            "status IN ('received', 'applied', 'conflicted', 'rejected')",
            name="resume_batches_status",
        ),
        CheckConstraint(
            "(request_fingerprint IS NULL AND outcome IS NULL AND expires_at IS NULL) OR "
            "(status = 'applied' AND request_fingerprint IS NOT NULL "
            "AND outcome IS NOT NULL AND expires_at IS NOT NULL)",
            name="resume_batches_v2_receipt",
        ),
        Index(
            "ix_resume_operation_batches_receipt_expiry",
            "expires_at",
            "id",
            postgresql_where=text("request_fingerprint IS NOT NULL"),
        ),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    resume_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    client_batch_id: Mapped[str] = mapped_column(String(160), nullable=False)
    base_revision_no: Mapped[int | None] = mapped_column(Integer)
    applied_revision_no: Mapped[int | None] = mapped_column(Integer)
    conflict_strategy: Mapped[str | None] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'received'")
    )
    idempotency_record_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("identity.idempotency_records.id", ondelete="SET NULL")
    )
    request_fingerprint: Mapped[str | None] = mapped_column(String(64))
    outcome: Mapped[JsonObject | None] = mapped_column(JSONB)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResumeOperationRecord(Base, TenantScopedMixin):
    """@brief 操作批次中的单个稳定 ID 操作 / One stable-ID operation in a resume batch."""

    __tablename__ = "operations"
    __table_args__ = (
        UniqueConstraint("batch_id", "ordinal", name="resume_operations_batch_ordinal"),
        UniqueConstraint("operation_id", name="resume_operations_operation_id"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    batch_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.operation_batches.id", ondelete="CASCADE"), nullable=False
    )
    operation_id: Mapped[str] = mapped_column(String(160), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    applied_revision_no: Mapped[int] = mapped_column(Integer, nullable=False)


class ResumeProposalRecord(Base, TenantScopedMixin):
    """@brief Agent 生成、用户可审批的简历建议 / User-approvable resume proposal produced by an Agent."""

    __tablename__ = "proposals"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'accepted', 'partially_accepted', 'rejected', 'expired')",
            name="resume_proposals_status",
        ),
        CheckConstraint(
            "(status = 'pending' AND decided_by_actor_id IS NULL AND decided_at IS NULL) OR "
            "(status = 'expired' AND decided_by_actor_id IS NULL AND decided_at IS NOT NULL) OR "
            "(status IN ('accepted', 'partially_accepted', 'rejected') "
            "AND decided_by_actor_id IS NOT NULL AND decided_at IS NOT NULL)",
            name="resume_proposals_v2_decision",
        ),
        ForeignKeyConstraint(
            ["agent_run_id", "workspace_id"],
            ["agent.runs.id", "agent.runs.workspace_id"],
            name="fk_resume_proposals_agent_run_workspace",
            ondelete="SET NULL (agent_run_id)",
        ),
        Index("ix_resume_proposals_resume_status", "resume_id", "status"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    resume_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    agent_run_id: Mapped[str | None] = mapped_column(
        String(160)
    )
    base_revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, server_default=text("'pending'")
    )
    decision_payload: Mapped[JsonObject | None] = mapped_column(JSONB)
    decided_by_actor_id: Mapped[str | None] = mapped_column(String(128))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    evidence_refs: Mapped[list[JsonObject]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb")
    )


class ResumeProposalOperationRecord(Base, TenantScopedMixin):
    """@brief 简历建议中的单个可选择操作 / Individually selectable operation in a resume proposal."""

    __tablename__ = "proposal_operations"
    __table_args__ = (
        UniqueConstraint("proposal_id", "ordinal", name="resume_proposal_operations_ordinal"),
        UniqueConstraint(
            "proposal_id",
            "operation_id",
            name="resume_proposal_operations_operation",
        ),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    proposal_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("resume.proposals.id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[str] = mapped_column(String(160), nullable=False)
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    applied_revision_no: Mapped[int | None] = mapped_column(Integer)
    decision: Mapped[str | None] = mapped_column(String(16))


class ResumeRenderJobRecord(Base, TenantScopedMixin):
    """@brief 简历渲染任务与输入/输出的关联 / Association of a resume render job with inputs and outputs."""

    __tablename__ = "render_jobs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["artifact_id", "workspace_id"],
            ["agent.artifacts.id", "agent.artifacts.workspace_id"],
            name="fk_render_jobs_artifact_workspace",
        ),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("agent.jobs.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    resume_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    resume_revision_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.revisions.id", ondelete="RESTRICT"), nullable=False
    )
    artifact_id: Mapped[str | None] = mapped_column(
        String(160), ForeignKey("agent.artifacts.id", ondelete="SET NULL")
    )
    render_profile: Mapped[str] = mapped_column(String(64), nullable=False)
    diagnostics: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class InterviewScenarioRecord(Base, WorkspaceScopedMixin):
    """@brief API V2 面试场景聚合根 / API V2 Interview Scenario aggregate root."""

    __tablename__ = "scenarios"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'active', 'archived')",
            name="interview_scenarios_status",
        ),
        CheckConstraint(
            "jsonb_typeof(spec) = 'object' "
            "AND jsonb_typeof(spec -> 'rubric') = 'object' "
            "AND jsonb_typeof(spec -> 'rubric' -> 'dimensions') = 'array' "
            "AND jsonb_array_length(spec -> 'rubric' -> 'dimensions') BETWEEN 1 AND 50",
            name="interview_scenarios_spec",
        ),
        UniqueConstraint("id", "workspace_id", name="interview_scenarios_id_workspace"),
        Index("ix_interview_scenarios_workspace_created_id", "workspace_id", "created_at", "id"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    spec: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'draft'")
    )


class InterviewSessionRecord(Base, WorkspaceScopedMixin):
    """@brief 含冻结执行快照的 API V2 面试 Session / API V2 Interview Session with frozen execution snapshots."""

    __tablename__ = "sessions"
    __table_args__ = (
        CheckConstraint(
            "status IN ('created', 'connecting', 'active', 'ending', 'completed', "
            "'failed', 'cancelled')",
            name="interview_sessions_status",
        ),
        CheckConstraint(
            "jsonb_typeof(spec) = 'object' AND jsonb_typeof(execution_grant) = 'object'",
            name="interview_sessions_snapshots",
        ),
        CheckConstraint(
            "next_realtime_sequence >= 1 AND next_transcript_sequence >= 1",
            name="interview_sessions_sequences",
        ),
        CheckConstraint(
            "((status = 'ending' AND pending_end_job_id IS NOT NULL AND end_reason IS NOT NULL) "
            "OR (status <> 'ending' AND pending_end_job_id IS NULL AND end_reason IS NULL)) "
            "AND (end_reason IS NULL OR end_reason IN "
            "('completed', 'user_cancelled', 'technical_failure'))",
            name="interview_sessions_end_request",
        ),
        CheckConstraint(
            "((status IN ('completed', 'failed', 'cancelled') AND ended_at IS NOT NULL) "
            "OR (status NOT IN ('completed', 'failed', 'cancelled') AND ended_at IS NULL)) "
            "AND (status <> 'completed' OR started_at IS NOT NULL) "
            "AND (started_at IS NULL OR started_at BETWEEN created_at AND updated_at) "
            "AND (ended_at IS NULL OR ended_at BETWEEN created_at AND updated_at) "
            "AND (started_at IS NULL OR ended_at IS NULL OR ended_at >= started_at) "
            "AND (report_id IS NULL OR status = 'completed')",
            name="interview_sessions_timeline",
        ),
        ForeignKeyConstraint(
            ["scenario_id", "workspace_id"],
            ["interview.scenarios.id", "interview.scenarios.workspace_id"],
            ondelete="RESTRICT",
            name="interview_sessions_scenario_workspace",
        ),
        UniqueConstraint("id", "workspace_id", name="interview_sessions_id_workspace"),
        Index("ix_interview_sessions_workspace_created_id", "workspace_id", "created_at", "id"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    scenario_id: Mapped[str] = mapped_column(String(160), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'created'")
    )
    spec: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    execution_grant: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    report_id: Mapped[str | None] = mapped_column(
        String(160),
        ForeignKey(
            "interview.reports.id",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
    )
    pending_end_job_id: Mapped[str | None] = mapped_column(
        String(160),
        ForeignKey(
            "agent.jobs.id",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
    )
    end_reason: Mapped[str | None] = mapped_column(String(24))
    next_realtime_sequence: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("1")
    )
    next_transcript_sequence: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("1")
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class InterviewRealtimeConnectionRecord(Base, WorkspaceScopedMixin):
    """@brief 无 secret 的短期 realtime connection lease / Secret-free short-lived realtime connection lease."""

    __tablename__ = "realtime_connections"
    __table_args__ = (
        CheckConstraint(
            "audience_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND audience_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (audience_revision IS NULL OR audience_revision >= 1)",
            name="interview_realtime_connections_audience",
        ),
        CheckConstraint(
            "transport IN ('webrtc', 'websocket')",
            name="interview_realtime_connections_transport",
        ),
        CheckConstraint(
            "expires_at > created_at AND expires_at <= created_at + interval '15 minutes'",
            name="interview_realtime_connections_lifetime",
        ),
        ForeignKeyConstraint(
            ["session_id", "workspace_id"],
            ["interview.sessions.id", "interview.sessions.workspace_id"],
            ondelete="CASCADE",
            name="interview_realtime_connections_session_workspace",
        ),
        UniqueConstraint(
            "id", "workspace_id", "session_id", name="interview_connections_scope"
        ),
        Index(
            "ix_interview_realtime_connections_workspace_expiry",
            "workspace_id",
            "expires_at",
            "id",
        ),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(160), nullable=False)
    audience_type: Mapped[str] = mapped_column(String(101), nullable=False)
    audience_id: Mapped[str] = mapped_column(String(160), nullable=False)
    audience_revision: Mapped[int | None] = mapped_column(Integer)
    transport: Mapped[str] = mapped_column(String(16), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class InterviewEventRecord(Base, WorkspaceScopedMixin):
    """@brief 不含候选人正文的 realtime input 幂等账本 / Plaintext-free realtime-input idempotency ledger."""

    __tablename__ = "realtime_inputs"
    __table_args__ = (
        CheckConstraint(
            "sequence >= 1 AND fingerprint_sha256 ~ '^[a-f0-9]{64}$'",
            name="interview_realtime_inputs_envelope",
        ),
        CheckConstraint(
            "revision = 1 AND created_at = updated_at",
            name="interview_realtime_inputs_immutable",
        ),
        ForeignKeyConstraint(
            ["connection_id", "workspace_id", "session_id"],
            [
                "interview.realtime_connections.id",
                "interview.realtime_connections.workspace_id",
                "interview.realtime_connections.session_id",
            ],
            ondelete="RESTRICT",
            name="interview_realtime_inputs_connection_scope",
        ),
        ForeignKeyConstraint(
            ["session_id", "workspace_id"],
            ["interview.sessions.id", "interview.sessions.workspace_id"],
            ondelete="CASCADE",
            name="interview_realtime_inputs_session_workspace",
        ),
        UniqueConstraint(
            "workspace_id",
            "session_id",
            "sequence",
            name="interview_realtime_inputs_session_sequence",
        ),
        UniqueConstraint(
            "id", "workspace_id", "session_id", name="interview_realtime_inputs_scope"
        ),
        Index(
            "ix_interview_realtime_inputs_session_sequence",
            "workspace_id",
            "session_id",
            "sequence",
            "id",
        ),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(160), nullable=False)
    connection_id: Mapped[str] = mapped_column(String(160), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fingerprint_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TranscriptSegmentRecord(Base, WorkspaceScopedMixin):
    """@brief 有来源约束的 append-only Transcript 片段 / Provenance-constrained append-only Transcript segment."""

    __tablename__ = "transcript_segments"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "session_id",
            "sequence",
            name="transcript_segments_session_sequence",
        ),
        UniqueConstraint(
            "id", "workspace_id", "session_id", name="transcript_segments_scope"
        ),
        CheckConstraint(
            "speaker IN ('candidate', 'interviewer', 'system')", name="transcript_segments_speaker"
        ),
        CheckConstraint(
            "start_ms >= 0 AND end_ms >= start_ms AND length(text_content) <= 20000",
            name="transcript_segments_content",
        ),
        CheckConstraint(
            "(source_input_id IS NOT NULL AND source_artifact_id IS NULL "
            "AND source_artifact_revision IS NULL) OR "
            "(source_input_id IS NULL AND source_artifact_id IS NOT NULL "
            "AND source_artifact_revision >= 1)",
            name="transcript_segments_provenance",
        ),
        CheckConstraint(
            "revision = 1 AND created_at = updated_at",
            name="transcript_segments_immutable",
        ),
        ForeignKeyConstraint(
            ["session_id", "workspace_id"],
            ["interview.sessions.id", "interview.sessions.workspace_id"],
            ondelete="CASCADE",
            name="transcript_segments_session_workspace",
        ),
        ForeignKeyConstraint(
            ["source_input_id", "workspace_id", "session_id"],
            [
                "interview.realtime_inputs.id",
                "interview.realtime_inputs.workspace_id",
                "interview.realtime_inputs.session_id",
            ],
            ondelete="RESTRICT",
            name="transcript_segments_input_scope",
        ),
        ForeignKeyConstraint(
            ["source_artifact_id", "workspace_id"],
            ["agent.artifacts.id", "agent.artifacts.workspace_id"],
            ondelete="RESTRICT",
            name="transcript_segments_artifact_workspace",
        ),
        Index(
            "ix_transcript_segments_session_sequence",
            "workspace_id",
            "session_id",
            "sequence",
            "id",
        ),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(160), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    speaker: Mapped[str] = mapped_column(String(16), nullable=False)
    start_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    text_content: Mapped[str] = mapped_column(Text, nullable=False)
    source_input_id: Mapped[str | None] = mapped_column(String(160))
    source_artifact_id: Mapped[str | None] = mapped_column(String(160))
    source_artifact_revision: Mapped[int | None] = mapped_column(Integer)


class InterviewReportRecord(Base, WorkspaceScopedMixin):
    """@brief 创建后不可变的 Interview Report / Immutable Interview Report."""

    __tablename__ = "reports"
    __table_args__ = (
        CheckConstraint(
            "jsonb_typeof(draft) = 'object' AND revision = 1 "
            "AND created_at = updated_at AND generated_at = created_at",
            name="interview_reports_immutable",
        ),
        ForeignKeyConstraint(
            ["session_id", "workspace_id"],
            ["interview.sessions.id", "interview.sessions.workspace_id"],
            ondelete="CASCADE",
            name="interview_reports_session_workspace",
        ),
        UniqueConstraint("workspace_id", "session_id", name="interview_reports_one_per_session"),
        UniqueConstraint("id", "workspace_id", "session_id", name="interview_reports_scope"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(160), nullable=False)
    draft: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class InterviewReportEvidenceRecord(Base):
    """@brief Report 到同 Session Transcript 的引用完整性投影 / Integrity projection from a Report to same-Session Transcript evidence."""

    __tablename__ = "report_evidence"
    __table_args__ = (
        CheckConstraint(
            "start_ms >= 0 AND end_ms >= start_ms",
            name="interview_report_evidence_range",
        ),
        ForeignKeyConstraint(
            ["report_id", "workspace_id", "session_id"],
            [
                "interview.reports.id",
                "interview.reports.workspace_id",
                "interview.reports.session_id",
            ],
            ondelete="CASCADE",
            name="interview_report_evidence_report_scope",
        ),
        ForeignKeyConstraint(
            ["segment_id", "workspace_id", "session_id"],
            [
                "interview.transcript_segments.id",
                "interview.transcript_segments.workspace_id",
                "interview.transcript_segments.session_id",
            ],
            ondelete="RESTRICT",
            name="interview_report_evidence_segment_scope",
        ),
        Index(
            "ix_interview_report_evidence_report_segment",
            "workspace_id",
            "report_id",
            "segment_id",
        ),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    workspace_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
        nullable=False,
    )
    report_id: Mapped[str] = mapped_column(String(160), nullable=False)
    session_id: Mapped[str] = mapped_column(String(160), nullable=False)
    segment_id: Mapped[str] = mapped_column(String(160), nullable=False)
    dimension_id: Mapped[str] = mapped_column(String(160), nullable=False)
    start_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class InterviewReportJobRecord(Base, WorkspaceScopedMixin):
    """@brief 统一 Job 的 Interview typed binding / Interview typed binding for a unified Job."""

    __tablename__ = "session_jobs"
    __table_args__ = (
        CheckConstraint(
            "job_kind IN ('interview.end', 'interview.report')",
            name="interview_session_jobs_kind",
        ),
        CheckConstraint(
            "revision = 1 AND created_at = updated_at",
            name="interview_session_jobs_immutable",
        ),
        ForeignKeyConstraint(
            ["session_id", "workspace_id"],
            ["interview.sessions.id", "interview.sessions.workspace_id"],
            ondelete="CASCADE",
            name="interview_session_jobs_session_workspace",
        ),
        UniqueConstraint("job_id", name="uq_session_jobs_job_id"),
        Index(
            "ix_interview_session_jobs_session_kind",
            "workspace_id",
            "session_id",
            "job_kind",
        ),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    job_id: Mapped[str] = mapped_column(
        String(160),
        ForeignKey(
            "agent.jobs.id",
            ondelete="CASCADE",
            name="fk_session_jobs_job_id_jobs",
        ),
        nullable=False,
    )
    session_id: Mapped[str] = mapped_column(String(160), nullable=False)
    job_kind: Mapped[str] = mapped_column(String(32), nullable=False)


class ConnectionRecord(Base, WorkspaceScopedMixin):
    """@brief 仅保存 secret-manager 引用的 Workspace Connection / Workspace Connection storing only a secret-manager reference.

    @note ``credential_reference`` 是 vault 中的不可解密引用；API token、token 摘要与
        provider credential 均不得进入本表。/ ``credential_reference`` is an opaque vault
        locator; API tokens, token digests, and provider credentials never enter this table.
    """

    __tablename__ = "connections"
    __table_args__ = (
        CheckConstraint(
            "provider ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="knowledge_connections_provider",
        ),
        CheckConstraint(
            "auth_method IN ('oauth', 'device_code', 'api_token')",
            name="knowledge_connections_auth_method",
        ),
        CheckConstraint(
            "status IN ('active', 'reauthorization_required', 'revoking', 'revoked', 'failed')",
            name="knowledge_connections_status",
        ),
        CheckConstraint(
            "display_name = btrim(display_name) AND length(display_name) BETWEEN 1 AND 200",
            name="knowledge_connections_display_name",
        ),
        CheckConstraint(
            "credential_reference ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="knowledge_connections_credential_reference",
        ),
        CheckConstraint(
            "cardinality(scopes) <= 100",
            name="knowledge_connections_scopes",
        ),
        CheckConstraint(
            "(status = 'active' AND problem IS NULL) OR status <> 'active'",
            name="knowledge_connections_active_problem",
        ),
        CheckConstraint(
            "(status = 'failed' AND problem IS NOT NULL) OR status <> 'failed'",
            name="knowledge_connections_failed_problem",
        ),
        UniqueConstraint("id", "workspace_id", name="knowledge_connections_id_workspace"),
        Index(
            "ix_knowledge_connections_workspace_created_id",
            "workspace_id",
            "created_at",
            "id",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    created_by: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="RESTRICT"), nullable=False
    )
    provider: Mapped[str] = mapped_column(String(101), nullable=False)
    auth_method: Mapped[str] = mapped_column(String(16), nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    scopes: Mapped[list[str]] = mapped_column(
        ARRAY(String(200)), nullable=False, default=list, server_default=text("ARRAY[]::varchar[]")
    )
    last_validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    problem: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))
    credential_reference: Mapped[str] = mapped_column(String(160), nullable=False)


class ConnectionAuthorizationRecordModel(Base, WorkspaceScopedMixin):
    """@brief Provider authorization 的一次性私有事务记录 / One-shot private provider-authorization record.

    @note OAuth code、device code 与 provider token 留在 provider secret store；本表只保存
        高熵 state 摘要及 opaque provider-session reference。/ OAuth codes, device codes, and
        provider tokens remain in the provider secret store; this table stores only a high-entropy
        state digest and an opaque provider-session reference.
    """

    __tablename__ = "connection_authorization_sessions"
    __table_args__ = (
        CheckConstraint(
            "provider ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="provider_format",
        ),
        CheckConstraint(
            "flow IN ('browser_redirect', 'device_code')",
            name="flow_kind",
        ),
        CheckConstraint(
            "state IN ('pending', 'completed', 'failed', 'expired')",
            name="state_kind",
        ),
        CheckConstraint(
            "state_sha256 ~ '^[a-f0-9]{64}$' AND "
            "provider_session_reference ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="private_refs",
        ),
        CheckConstraint(
            "cardinality(requested_scopes) <= 100",
            name="scope_count",
        ),
        CheckConstraint(
            "idempotency_key_hash ~ '^[a-f0-9]{64}$' "
            "AND request_fingerprint ~ '^[a-f0-9]{64}$' "
            "AND launch_key_id ~ '^[A-Za-z][A-Za-z0-9_.-]{2,63}$' "
            "AND octet_length(launch_nonce) = 12 "
            "AND octet_length(launch_ciphertext) BETWEEN 17 AND 16384",
            name="sealed_launch",
        ),
        CheckConstraint(
            "expires_at > created_at AND ("
            "(state = 'completed' AND connection_id IS NOT NULL AND problem IS NULL) OR "
            "(state = 'failed' AND connection_id IS NULL AND problem IS NOT NULL) OR "
            "(state IN ('pending', 'expired') AND connection_id IS NULL AND problem IS NULL))",
            name="lifecycle",
        ),
        CheckConstraint(
            "idempotency_expires_at >= created_at + interval '24 hours'",
            name="replay_window",
        ),
        ForeignKeyConstraint(
            ["connection_id", "workspace_id"],
            ["knowledge.connections.id", "knowledge.connections.workspace_id"],
            name="fk_connection_authorizations_connection_workspace",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "workspace_id",
            "created_by",
            "idempotency_key_hash",
            name="knowledge_connection_authorizations_actor_key",
        ),
        Index(
            "ix_knowledge_connection_authorizations_workspace_expiry",
            "workspace_id",
            "expires_at",
            "id",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    created_by: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(101), nullable=False)
    flow: Mapped[str] = mapped_column(String(16), nullable=False)
    launch_key_id: Mapped[str] = mapped_column(String(64), nullable=False)
    launch_nonce: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    launch_ciphertext: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    idempotency_expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    requested_scopes: Mapped[list[str]] = mapped_column(
        ARRAY(String(200)), nullable=False, default=list, server_default=text("ARRAY[]::varchar[]")
    )
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    state_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_session_reference: Mapped[str] = mapped_column(String(160), nullable=False)
    connection_id: Mapped[str | None] = mapped_column(String(160))
    problem: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))


class KnowledgeUploadSessionRecord(Base, WorkspaceScopedMixin):
    """@brief Knowledge 与 Resume 共用的唯一 UploadSession 真相 / Sole UploadSession truth shared by Knowledge and Resume.

    @note ``legacy_payload`` 只标记 0019 从旧最小 Resume claim 行或 V1 file source 原样迁入、
        无法诚实补造 declaration/grant 的记录。新 API V2 行必须具备完整冻结声明与授权。
        / ``legacy_payload`` marks rows migrated from the old minimal Resume claim table or V1 file
        sources where declaration/grant data cannot honestly be fabricated. New API V2 rows always
        contain the complete frozen declaration and grant.
    """

    __tablename__ = "upload_sessions"
    __table_args__ = (
        CheckConstraint(
            "status IN ('created', 'uploaded', 'verifying', 'completed', 'failed', 'expired')",
            name="knowledge_upload_sessions_status",
        ),
        CheckConstraint(
            "revision >= 1 AND expires_at > created_at",
            name="knowledge_upload_sessions_generation",
        ),
        CheckConstraint(
            "legacy_payload OR (filename IS NOT NULL AND media_type IS NOT NULL "
            "AND declared_size_bytes BETWEEN 1 AND 1073741824 "
            "AND declared_sha256 ~ '^[a-f0-9]{64}$' AND upload_url IS NOT NULL "
            "AND jsonb_typeof(required_headers) = 'object')",
            name="knowledge_upload_sessions_declaration",
        ),
        CheckConstraint(
            "legacy_payload OR ("
            "(status IN ('created', 'uploaded', 'expired') AND completion_size_bytes IS NULL "
            "AND completion_sha256 IS NULL AND failure_code IS NULL AND completed_at IS NULL "
            "AND artifact_type IS NULL AND artifact_id IS NULL AND artifact_revision IS NULL) OR "
            "(status = 'verifying' AND completion_size_bytes IS NOT NULL "
            "AND completion_sha256 IS NOT NULL AND failure_code IS NULL AND completed_at IS NULL "
            "AND artifact_type IS NULL AND artifact_id IS NULL AND artifact_revision IS NULL) OR "
            "(status = 'completed' AND completion_size_bytes IS NOT NULL "
            "AND completion_sha256 IS NOT NULL AND failure_code IS NULL AND completed_at IS NOT NULL "
            "AND artifact_type IS NOT NULL AND artifact_id IS NOT NULL) OR "
            "(status = 'failed' AND completion_size_bytes IS NOT NULL "
            "AND completion_sha256 IS NOT NULL AND failure_code IS NOT NULL AND completed_at IS NULL "
            "AND artifact_type IS NULL AND artifact_id IS NULL AND artifact_revision IS NULL))",
            name="knowledge_upload_sessions_lifecycle",
        ),
        CheckConstraint(
            "(claimed_by_type IS NULL AND claimed_by_id IS NULL AND claimed_by_revision IS NULL "
            "AND claimed_by_job_id IS NULL AND consumed_at IS NULL) OR "
            "(status = 'completed' AND claimed_by_type IS NOT NULL AND claimed_by_id IS NOT NULL "
            "AND consumed_at IS NOT NULL AND (claimed_by_revision IS NULL OR claimed_by_revision >= 1) "
            "AND ((claimed_by_type = 'job' AND claimed_by_job_id = claimed_by_id) "
            "OR (claimed_by_type <> 'job' AND claimed_by_job_id IS NULL)))",
            name="knowledge_upload_sessions_claim",
        ),
        CheckConstraint(
            "failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="knowledge_upload_sessions_failure_code",
        ),
        CheckConstraint(
            "((status IN ('verifying', 'completed', 'failed') "
            "AND verification_operation_id IS NOT NULL) OR "
            "(status IN ('created', 'uploaded', 'expired') "
            "AND verification_operation_id IS NULL)) "
            "AND (verification_operation_id IS NULL OR "
            "verification_operation_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,79}$')",
            name="verification_owner",
        ),
        CheckConstraint(
            "artifact_type IS NULL OR artifact_type ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="knowledge_upload_sessions_artifact_type",
        ),
        CheckConstraint(
            "claimed_by_type IS NULL OR claimed_by_type ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="knowledge_upload_sessions_claim_type",
        ),
        ForeignKeyConstraint(
            ["claimed_by_job_id"],
            ["agent.jobs.id"],
            name="fk_upload_sessions_claimed_job",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        UniqueConstraint("claimed_by_job_id", name="knowledge_upload_sessions_claimed_job"),
        UniqueConstraint("id", "workspace_id", name="knowledge_upload_sessions_id_workspace"),
        Index(
            "ix_knowledge_upload_sessions_claimable",
            "workspace_id",
            "expires_at",
            postgresql_where=text("status = 'completed' AND claimed_by_id IS NULL"),
        ),
        Index(
            "ix_knowledge_upload_sessions_workspace_created_id",
            "workspace_id",
            "created_at",
            "id",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    filename: Mapped[str | None] = mapped_column(String(300))
    media_type: Mapped[str | None] = mapped_column(String(200))
    declared_size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    declared_sha256: Mapped[str | None] = mapped_column(String(64))
    upload_url: Mapped[str | None] = mapped_column(Text)
    required_headers: Mapped[JsonObject | None] = mapped_column(JSONB)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completion_size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    completion_sha256: Mapped[str | None] = mapped_column(String(64))
    verification_operation_id: Mapped[str | None] = mapped_column(String(80))
    failure_code: Mapped[str | None] = mapped_column(String(101))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    artifact_type: Mapped[str | None] = mapped_column(String(101))
    artifact_id: Mapped[str | None] = mapped_column(String(160))
    artifact_revision: Mapped[int | None] = mapped_column(Integer)
    claimed_by_type: Mapped[str | None] = mapped_column(String(101))
    claimed_by_id: Mapped[str | None] = mapped_column(String(160))
    claimed_by_revision: Mapped[int | None] = mapped_column(Integer)
    claimed_by_job_id: Mapped[str | None] = mapped_column(String(160))
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    legacy_payload: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )


# 导入别名只指向统一表，不注册任何 Resume 平行表。
ResumeImportUploadSessionRecord = KnowledgeUploadSessionRecord
"""@brief 旧 Resume adapter 的统一 UploadSession 导入别名 / Unified UploadSession import alias for the Resume adapter."""


class KnowledgeSourceRecord(Base, TenantScopedMixin):
    """@brief API V2 KnowledgeSource 聚合根 / API V2 KnowledgeSource aggregate root."""

    __tablename__ = "sources"
    __table_args__ = (
        CheckConstraint(
            "source_type IN ('file', 'url', 'website', 'blog_feed', 'git_repository', "
            "'manual_note', 'resume', 'cloud_drive')",
            name="knowledge_sources_type",
        ),
        CheckConstraint(
            "ingestion_state IN ('not_started', 'queued', 'fetching', 'parsing', 'chunking', "
            "'embedding', 'ready', 'stale', 'failed', 'deleting', 'deleted')",
            name="knowledge_sources_ingestion_state",
        ),
        CheckConstraint(
            "title = btrim(title) AND length(title) BETWEEN 1 AND 300",
            name="knowledge_sources_name",
        ),
        CheckConstraint(
            "version_counter >= 0 AND ((version_counter = 0 AND current_version_id IS NULL) "
            "OR (version_counter > 0 AND current_version_id IS NOT NULL))",
            name="knowledge_sources_version_counter",
        ),
        CheckConstraint(
            "document_count >= 0 AND chunk_count >= 0 AND current_policy_version >= 1",
            name="knowledge_sources_counters",
        ),
        CheckConstraint(
            "(ingestion_state = 'failed' AND last_problem IS NOT NULL) OR "
            "(ingestion_state <> 'failed' AND last_problem IS NULL)",
            name="knowledge_sources_problem",
        ),
        CheckConstraint(
            "ingestion_state NOT IN ('deleting', 'deleted') OR enabled = false",
            name="knowledge_sources_deletion_state",
        ),
        CheckConstraint(
            "jsonb_typeof(source_input) = 'object' AND jsonb_typeof(public_config) = 'object'",
            name="knowledge_sources_config_objects",
        ),
        CheckConstraint(
            "(source_type = 'file' AND upload_session_id IS NOT NULL "
            "AND connection_id IS NULL AND resume_id IS NULL) OR "
            "(source_type = 'resume' AND resume_id IS NOT NULL "
            "AND connection_id IS NULL AND upload_session_id IS NULL) OR "
            "(source_type = 'cloud_drive' AND connection_id IS NOT NULL "
            "AND upload_session_id IS NULL AND resume_id IS NULL) OR "
            "(source_type = 'git_repository' AND upload_session_id IS NULL AND resume_id IS NULL) OR "
            "(source_type IN ('url', 'website', 'blog_feed', 'manual_note') "
            "AND connection_id IS NULL AND upload_session_id IS NULL AND resume_id IS NULL)",
            name="knowledge_sources_input_refs",
        ),
        ForeignKeyConstraint(
            ["connection_id", "workspace_id"],
            ["knowledge.connections.id", "knowledge.connections.workspace_id"],
            name="fk_knowledge_sources_connection_workspace",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["upload_session_id", "workspace_id"],
            ["knowledge.upload_sessions.id", "knowledge.upload_sessions.workspace_id"],
            name="fk_knowledge_sources_upload_workspace",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["current_version_id", "workspace_id"],
            ["knowledge.source_versions.id", "knowledge.source_versions.workspace_id"],
            name="fk_knowledge_sources_current_version_workspace",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        UniqueConstraint("id", "workspace_id", name="knowledge_sources_id_workspace"),
        Index("ix_knowledge_sources_workspace_state", "workspace_id", "ingestion_state"),
        Index(
            "ix_knowledge_sources_workspace_created_id",
            "workspace_id",
            "created_at",
            "id",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    config: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    source_input: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    public_config: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    connection_id: Mapped[str | None] = mapped_column(String(160))
    upload_session_id: Mapped[str | None] = mapped_column(String(160))
    resume_id: Mapped[str | None] = mapped_column(String(160))
    current_policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    current_version_id: Mapped[str | None] = mapped_column(String(160))
    version_counter: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    revision_mode: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'latest'")
    )
    ingestion_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'not_started'")
    )
    document_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_problem: Mapped[JsonObject | None] = mapped_column(JSONB(none_as_null=True))
    sync_schedule: Mapped[JsonObject | None] = mapped_column(JSONB)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeSourceVersionRecord(Base, TenantScopedMixin):
    """@brief 不可变知识来源版本 / Immutable version of a knowledge source."""

    __tablename__ = "source_versions"
    __table_args__ = (
        UniqueConstraint("source_id", "version_no", name="knowledge_source_versions_source_number"),
        UniqueConstraint(
            "workspace_id",
            "source_id",
            "version_no",
            name="knowledge_source_versions_workspace_source_number",
        ),
        UniqueConstraint("id", "workspace_id", name="knowledge_source_versions_id_workspace"),
        CheckConstraint(
            "version_no >= 1 AND size_bytes BETWEEN 0 AND 1073741824 "
            "AND content_sha256 ~ '^[a-f0-9]{64}$'",
            name="knowledge_source_versions_content",
        ),
        CheckConstraint(
            "status IN ('pending', 'indexing', 'ready', 'failed')",
            name="knowledge_source_versions_status",
        ),
        CheckConstraint(
            "(status = 'ready' AND indexed_at IS NOT NULL) OR "
            "(status <> 'ready' AND indexed_at IS NULL)",
            name="knowledge_source_versions_indexed_at",
        ),
        CheckConstraint(
            "artifact_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND artifact_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (artifact_revision IS NULL OR artifact_revision >= 1)",
            name="knowledge_source_versions_artifact",
        ),
        ForeignKeyConstraint(
            ["source_id", "workspace_id"],
            ["knowledge.sources.id", "knowledge.sources.workspace_id"],
            name="fk_knowledge_source_versions_source_workspace",
            ondelete="CASCADE",
        ),
        Index(
            "ix_knowledge_source_versions_source_number",
            "workspace_id",
            "source_id",
            "version_no",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    source_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("knowledge.sources.id", ondelete="CASCADE"), nullable=False
    )
    version_no: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(101), nullable=False)
    artifact_id: Mapped[str] = mapped_column(String(160), nullable=False)
    artifact_revision: Mapped[int | None] = mapped_column(Integer)
    origin: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    parser_metadata: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    indexed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeVisibilityPolicyRecord(Base, TenantScopedMixin):
    """@brief 知识来源的版本化可见性策略 / Versioned visibility policy for a knowledge source."""

    __tablename__ = "visibility_policies"
    __table_args__ = (
        UniqueConstraint("source_id", "policy_version", name="knowledge_visibility_policy_version"),
        CheckConstraint(
            "default_effect IN ('allow', 'deny')", name="knowledge_visibility_default_effect"
        ),
        CheckConstraint(
            "sensitivity IN ('normal', 'confidential', 'highly_confidential')",
            name="knowledge_visibility_sensitivity",
        ),
        CheckConstraint(
            "policy_version >= 1 AND cardinality(allowed_model_regions) BETWEEN 1 AND 3 "
            "AND allowed_model_regions <@ ARRAY['cn', 'global', 'private_deployment']::varchar[] "
            "AND (retention_days IS NULL OR retention_days BETWEEN 1 AND 3650)",
            name="knowledge_visibility_v2_policy",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    source_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("knowledge.sources.id", ondelete="CASCADE"), nullable=False
    )
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    default_effect: Mapped[str] = mapped_column(
        String(8), nullable=False, server_default=text("'deny'")
    )
    sensitivity: Mapped[str] = mapped_column(String(32), nullable=False)
    session_override_allowed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    allow_external_model_processing: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    allowed_model_regions: Mapped[list[str]] = mapped_column(
        ARRAY(String(64)), nullable=False, server_default=text("ARRAY[]::varchar[]")
    )
    retention_days: Mapped[int | None] = mapped_column(Integer)


class KnowledgeVisibilityGrantRecord(Base, TenantScopedMixin):
    """@brief 策略内针对 Agent scope 的显式授权 / Explicit policy grant for an Agent scope."""

    __tablename__ = "visibility_grants"
    __table_args__ = (
        UniqueConstraint("policy_id", "ordinal", name="knowledge_visibility_grants_ordinal"),
        CheckConstraint("effect IN ('allow', 'deny')", name="knowledge_visibility_grants_effect"),
        CheckConstraint(
            "ordinal >= 0 AND agent_scope ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND cardinality(allowed_operations) BETWEEN 1 AND 5 "
            "AND allowed_operations <@ "
            "ARRAY['retrieve', 'quote', 'summarize', 'derive', 'write_back']::varchar[]",
            name="knowledge_visibility_grants_v2_shape",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    policy_id: Mapped[str] = mapped_column(
        String(160),
        ForeignKey("knowledge.visibility_policies.id", ondelete="CASCADE"),
        nullable=False,
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    agent_scope: Mapped[str] = mapped_column(String(128), nullable=False)
    effect: Mapped[str] = mapped_column(String(8), nullable=False)
    allowed_operations: Mapped[list[str]] = mapped_column(
        ARRAY(String(32)), nullable=False, server_default=text("ARRAY[]::varchar[]")
    )


class KnowledgeChunkRecord(Base, TenantScopedMixin):
    """@brief 可检索来源版本的分块文本 / Chunked text for a retrievable source version."""

    __tablename__ = "chunks"
    __table_args__ = (
        UniqueConstraint("source_version_id", "ordinal", name="knowledge_chunks_version_ordinal"),
        Index("ix_knowledge_chunks_version_ordinal", "source_version_id", "ordinal"),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_version_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.source_versions.id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    text_content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    origin: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    token_count: Mapped[int | None] = mapped_column(Integer)


class EmbeddingSpaceRecord(Base, TenantScopedMixin):
    """@brief 不可混用的 embedding 向量空间 / Non-interchangeable embedding vector space.

    任何 provider、model、revision、dimension、distance metric 或 normalization 的
    变化都必须创建新记录；v0.1 的实际向量列固定为 1024 维。
    """

    __tablename__ = "embedding_spaces"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "provider",
            "model",
            "model_revision",
            "dimension",
            "distance_metric",
            "normalization",
            name="embedding_spaces_identity",
        ),
        CheckConstraint("dimension = 1024", name="embedding_spaces_v01_dimension"),
        CheckConstraint(
            "distance_metric IN ('cosine', 'l2', 'inner_product')",
            name="embedding_spaces_distance_metric",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    provider: Mapped[str] = mapped_column(String(128), nullable=False)
    model: Mapped[str] = mapped_column(String(256), nullable=False)
    model_revision: Mapped[str] = mapped_column(String(256), nullable=False)
    dimension: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, server_default=text("1024")
    )
    distance_metric: Mapped[str] = mapped_column(String(32), nullable=False)
    normalization: Mapped[str] = mapped_column(String(64), nullable=False)
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeEmbeddingRecord(Base, TenantScopedMixin):
    """@brief chunk 在指定向量空间中的 embedding / A chunk embedding in a specified vector space."""

    __tablename__ = "embeddings"
    __table_args__ = (
        UniqueConstraint("chunk_id", "embedding_space_id", name="knowledge_embeddings_chunk_space"),
        Index("ix_knowledge_embeddings_space_chunk", "embedding_space_id", "chunk_id"),
        Index(
            "ix_embeddings_embedding_hnsw_cosine",
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    chunk_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.chunks.id", ondelete="CASCADE"), nullable=False
    )
    embedding_space_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("knowledge.embedding_spaces.id", ondelete="RESTRICT"),
        nullable=False,
    )
    embedding: Mapped[list[float]] = mapped_column(Vector(1024), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class KnowledgeCitationRecord(Base, TenantScopedMixin):
    """@brief Agent 输出与知识 chunk 的可审计引用 / Auditable citation from Agent output to a knowledge chunk."""

    __tablename__ = "citations"
    __table_args__ = (
        UniqueConstraint("run_id", "ordinal", name="knowledge_citations_run_ordinal"),
        ForeignKeyConstraint(
            ["run_id", "workspace_id"],
            ["agent.runs.id", "agent.runs.workspace_id"],
            name="fk_knowledge_citations_run_workspace",
            ondelete="CASCADE",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(160), nullable=False
    )
    chunk_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.chunks.id", ondelete="RESTRICT"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    retrieval_score: Mapped[float | None] = mapped_column(Float)
    quote: Mapped[str | None] = mapped_column(Text)
    origin: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)


class KnowledgeIngestionJobRecord(Base, TenantScopedMixin):
    """@brief 知识导入/同步 Job 的子领域关联 / Knowledge ingestion or sync job association."""

    __tablename__ = "ingestion_jobs"
    __table_args__ = ({"schema": "knowledge"},)

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("agent.jobs.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    source_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.sources.id", ondelete="CASCADE"), nullable=False
    )
    source_version_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("knowledge.source_versions.id", ondelete="SET NULL")
    )
    operation: Mapped[str] = mapped_column(String(32), nullable=False)
    statistics: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class KnowledgeAccessSnapshotRecord(Base, TenantScopedMixin):
    """@brief 固化的知识可见性决策快照 / Frozen knowledge-visibility decision snapshot."""

    __tablename__ = "access_snapshots"
    __table_args__ = (
        CheckConstraint(
            "agent_run_id IS NOT NULL OR interview_session_id IS NOT NULL",
            name="knowledge_access_snapshot_subject",
        ),
        ForeignKeyConstraint(
            ["agent_run_id", "workspace_id"],
            ["agent.runs.id", "agent.runs.workspace_id"],
            name="fk_knowledge_access_snapshots_agent_run_workspace",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["interview_session_id", "workspace_id"],
            ["interview.sessions.id", "interview.sessions.workspace_id"],
            name="fk_knowledge_access_snapshots_interview_session_workspace",
            ondelete="CASCADE",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    agent_run_id: Mapped[str | None] = mapped_column(
        String(160)
    )
    interview_session_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("interview.sessions.id", ondelete="CASCADE")
    )
    selection: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    policy_evaluation: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)


class TelemetryRecord(Base):
    """@brief 强约束的统一 telemetry 信封 / Strongly constrained unified telemetry envelope.

    @note 表是物理统一的，但 metric/log/span 的专属字段由互斥 ``CHECK`` 约束保护；
    它不继承业务资源 lifecycle，也不以外键阻塞业务身份删除。
    """

    __tablename__ = "telemetry_records"
    __table_args__ = (
        CheckConstraint("kind IN ('metric', 'log', 'span')", name="telemetry_signal_kind"),
        CheckConstraint("source IN ('backend', 'frontend')", name="telemetry_signal_source"),
        CheckConstraint("jsonb_typeof(attributes) = 'object'", name="telemetry_attributes_object"),
        CheckConstraint(
            "num_nonnulls(workspace_id, resource_owner_id, actor_id) IN (0, 3)",
            name="telemetry_scope_complete",
        ),
        CheckConstraint(
            "(source = 'frontend' AND workspace_id IS NOT NULL AND client_event_id IS NOT NULL) "
            "OR (source = 'backend' AND client_event_id IS NULL)",
            name="telemetry_source_contract",
        ),
        CheckConstraint(
            "(kind = 'metric' AND metric_type IS NOT NULL AND value IS NOT NULL AND unit IS NOT NULL "
            "AND severity_number IS NULL AND severity_text IS NULL AND duration_ms IS NULL "
            "AND span_status IS NULL) OR "
            "(kind = 'log' AND metric_type IS NULL AND value IS NULL AND unit IS NULL "
            "AND severity_number BETWEEN 1 AND 24 AND severity_text IS NOT NULL "
            "AND duration_ms IS NULL AND span_status IS NULL) OR "
            "(kind = 'span' AND metric_type IS NULL AND value IS NULL AND unit IS NULL "
            "AND severity_number IS NULL AND severity_text IS NULL AND duration_ms >= 0 "
            "AND span_status IS NOT NULL AND trace_id IS NOT NULL AND span_id IS NOT NULL)",
            name="telemetry_kind_fields",
        ),
        CheckConstraint(
            "metric_type IS NULL OR metric_type IN ('counter', 'gauge', 'histogram')",
            name="telemetry_metric_type",
        ),
        CheckConstraint(
            "value IS NULL OR value NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)",
            name="telemetry_finite_value",
        ),
        CheckConstraint(
            "duration_ms IS NULL OR duration_ms NOT IN "
            "('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)",
            name="telemetry_finite_duration",
        ),
        CheckConstraint(
            "span_status IS NULL OR span_status IN ('unset', 'ok', 'error')",
            name="telemetry_span_status",
        ),
        CheckConstraint(
            "(trace_id IS NULL AND span_id IS NULL AND parent_span_id IS NULL) OR "
            "(trace_id ~ '^[0-9a-f]{32}$' AND trace_id <> repeat('0', 32) "
            "AND span_id ~ '^[0-9a-f]{16}$' AND span_id <> repeat('0', 16) "
            "AND (parent_span_id IS NULL OR "
            "(parent_span_id ~ '^[0-9a-f]{16}$' AND parent_span_id <> repeat('0', 16))))",
            name="telemetry_trace_context",
        ),
        Index(
            "ix_telemetry_metric_workspace_occurred",
            "workspace_id",
            "occurred_at",
            "service",
            "name",
            postgresql_where=text("kind = 'metric'"),
            postgresql_include=["value", "observed_at", "unit", "metric_type"],
        ),
        Index(
            "ix_telemetry_event_workspace_occurred_observed",
            "workspace_id",
            "occurred_at",
            "observed_at",
            postgresql_where=text("kind IN ('log', 'span')"),
        ),
        Index(
            "ix_telemetry_trace_occurred",
            "trace_id",
            "occurred_at",
            "span_id",
            postgresql_where=text("trace_id IS NOT NULL"),
        ),
        Index("ix_telemetry_observed_at", "observed_at"),
        Index(
            "uq_telemetry_frontend_client_event",
            "workspace_id",
            "resource_owner_id",
            "actor_id",
            "client_event_id",
            unique=True,
            postgresql_where=text("source = 'frontend' AND client_event_id IS NOT NULL"),
        ),
        {"schema": "observability"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    workspace_id: Mapped[str | None] = mapped_column(String(128))
    resource_owner_id: Mapped[str | None] = mapped_column(String(128))
    actor_id: Mapped[str | None] = mapped_column(String(128))
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    source: Mapped[str] = mapped_column(String(16), nullable=False)
    service: Mapped[str] = mapped_column(String(128), nullable=False)
    service_version: Mapped[str | None] = mapped_column(String(128))
    deployment_environment: Mapped[str | None] = mapped_column(String(128))
    service_instance_id: Mapped[str | None] = mapped_column(String(128))
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    metric_type: Mapped[str | None] = mapped_column(String(16))
    value: Mapped[float | None] = mapped_column(Float)
    unit: Mapped[str | None] = mapped_column(String(32))
    severity_number: Mapped[int | None] = mapped_column(SmallInteger)
    severity_text: Mapped[str | None] = mapped_column(String(16))
    duration_ms: Mapped[float | None] = mapped_column(Float)
    span_status: Mapped[str | None] = mapped_column(String(16))
    request_id: Mapped[str | None] = mapped_column(String(128))
    trace_id: Mapped[str | None] = mapped_column(String(32))
    span_id: Mapped[str | None] = mapped_column(String(16))
    parent_span_id: Mapped[str | None] = mapped_column(String(16))
    client_event_id: Mapped[str | None] = mapped_column(String(128))
    attributes: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


__all__ = [
    "AccountDeletionRequestRecord",
    "AgentRunRecord",
    "ArtifactContentRecord",
    "ArtifactPdfSourceMapRecord",
    "ArtifactRecord",
    "AuditEventRecord",
    "Base",
    "ChatMessageRecord",
    "ConnectionAuthorizationRecordModel",
    "ConnectionRecord",
    "ConversationRecord",
    "EmbeddingSpaceRecord",
    "IdempotencyRecord",
    "IdentityAuthenticatorRecord",
    "IdentityBrowserSessionRecord",
    "IdentityFlowRecord",
    "IdentityFlowStepRecord",
    "IdentityLoginSessionRecord",
    "InterviewEventRecord",
    "InterviewReportJobRecord",
    "InterviewReportRecord",
    "InterviewScenarioRecord",
    "InterviewSessionRecord",
    "JobRecord",
    "KnowledgeAccessSnapshotRecord",
    "KnowledgeChunkRecord",
    "KnowledgeCitationRecord",
    "KnowledgeEmbeddingRecord",
    "KnowledgeIngestionJobRecord",
    "KnowledgeSourceRecord",
    "KnowledgeSourceVersionRecord",
    "KnowledgeUploadSessionRecord",
    "KnowledgeVisibilityGrantRecord",
    "KnowledgeVisibilityPolicyRecord",
    "OAuthAuthorizationCodeRecord",
    "OAuthAuthorizationRequestRecord",
    "OAuthRefreshTokenFamilyRecord",
    "OAuthRefreshTokenRecord",
    "OAuthRevokedAccessTokenRecord",
    "OutboxEventRecord",
    "ResumeDocumentRecord",
    "ResumeImportUploadSessionRecord",
    "ResumeOperationBatchRecord",
    "ResumeOperationRecord",
    "ResumeProposalOperationRecord",
    "ResumeProposalRecord",
    "ResumeRenderJobRecord",
    "ResumeRevisionRecord",
    "ResumeTemplateRecord",
    "TelemetryRecord",
    "TenantScopedMixin",
    "TimestampedRevisionMixin",
    "ToolApprovalRecord",
    "TranscriptSegmentRecord",
    "UserRecord",
    "WorkspaceEventSequenceRecord",
    "WorkspaceInvitationRecord",
    "WorkspaceMemberRecord",
    "WorkspaceRecord",
]
