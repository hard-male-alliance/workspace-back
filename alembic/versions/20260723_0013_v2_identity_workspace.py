"""@brief 扩展并回填 API V2 身份与工作区模型 / Expand and backfill API V2 identity and workspace models.

Revision ID: 20260723_0013
Revises: 20260722_0012
Create Date: 2026-07-23
"""

from __future__ import annotations

import json
import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260723_0013"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260722_0012"
"""@brief 线性前驱 revision / Linear predecessor revision."""

branch_labels = None
"""@brief 此迁移不创建分支 / This migration does not create a branch."""

depends_on = None
"""@brief 此迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal[
    "owner_role",
    "app_role",
    "dashboard_role",
    "migrator_role",
]
"""@brief 允许从 Alembic 配置读取的运行时角色 / Runtime roles accepted from Alembic config."""

DataRegion = Literal["cn", "global", "private_deployment"]
"""@brief API V2 允许的数据驻留地域 / API V2 data-residency regions."""

WorkspacePlan = Literal["personal", "team", "enterprise"]
"""@brief API V2 允许的 Workspace 计划 / API V2 Workspace plans."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL 角色标识符白名单 / PostgreSQL role-identifier allowlist."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL 标识符最大字节数 / PostgreSQL identifier byte limit."""

_DATA_REGIONS: frozenset[str] = frozenset({"cn", "global", "private_deployment"})
"""@brief 契约允许的数据驻留地域集合 / Contract-approved data-residency region set."""

_AUDIT_MIGRATION_ID = "api-v2-identity-workspace-0013"
"""@brief 写入 0008 审计账本的稳定迁移标识 / Stable migration ID written to the 0008 ledger."""

_MIGRATION_POLICY = "identity_owner_migration_0013"
"""@brief 事务内 owner 可见性策略名 / Transaction-local owner-visibility policy name."""

_LEGACY_RLS_TABLES = (
    "identity.users",
    "identity.workspaces",
    "identity.workspace_members",
    "identity.identity_flows",
    "identity.api_migration_audits",
)
"""@brief 0013 需要检查或改写的既有 FORCE RLS 表 / Existing forced-RLS tables inspected or changed by 0013."""

_V2_RLS_TABLES = (
    "identity.workspace_invitations",
    "identity.account_deletion_requests",
)
"""@brief 0013 新建且 downgrade 必须检查的 FORCE RLS 表 / New forced-RLS tables inspected by downgrade."""

_LEGACY_TENANT_POLICY_TABLES = (
    "identity.workspace_members",
    "identity.audit_events",
    "identity.idempotency_records",
    "resume.template_versions",
    "resume.documents",
    "resume.revisions",
    "resume.operation_batches",
    "resume.operations",
    "resume.proposals",
    "resume.proposal_operations",
    "resume.render_artifacts",
    "resume.render_jobs",
    "resume.pdf_source_map_entries",
    "resume.artifact_blobs",
    "agent.jobs",
    "agent.outbox_events",
    "agent.conversations",
    "agent.messages",
    "agent.runs",
    "agent.run_events",
    "agent.tool_approvals",
    "interview.scenarios",
    "interview.sessions",
    "interview.events",
    "interview.transcript_segments",
    "interview.reports",
    "interview.report_jobs",
    "interview.recording_artifacts",
    "knowledge.sources",
    "knowledge.source_versions",
    "knowledge.visibility_policies",
    "knowledge.visibility_grants",
    "knowledge.chunks",
    "knowledge.embedding_spaces",
    "knowledge.embeddings",
    "knowledge.citations",
    "knowledge.ingestion_jobs",
    "knowledge.access_snapshots",
)
"""@brief 0013 所知的旧租户策略精确集合 / Exact legacy tenant-policy set known to 0013."""

_COLLABORATIVE_POLICY_TABLES = tuple(
    table
    for table in _LEGACY_TENANT_POLICY_TABLES
    if table != "identity.workspace_members"
)
"""@brief 从个人 owner 隔离迁到 Workspace 协作隔离的表 / Tables migrated from owner to Workspace isolation."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回安全引用的数据库角色 / Return a safely quoted database role.

    @param option Alembic 主配置中的角色选项 / Role option in Alembic main config.
    @return 双引号引用的 PostgreSQL 角色 / Double-quoted PostgreSQL role.
    @raise RuntimeError 配置缺失或不是合法标识符时抛出 / Raised for missing or invalid identifiers.
    """
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option(f"aiws.{option}")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES
    ):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def _configured_data_region(*, required: bool) -> DataRegion | None:
    """@brief 读取显式的数据地域回填选项 / Read the explicit data-region backfill option.

    @param required 是否因存在历史 Workspace 而必须配置 / Whether existing Workspaces require it.
    @return 已验证地域；空库且未配置时返回 ``None`` / Validated region, or ``None`` for an empty DB.
    @raise RuntimeError 非空库缺失选项或取值越界时抛出 / Raised for missing/invalid required input.

    @note 迁移禁止依据部署环境或时区猜测数据驻留地域 / Migration never guesses residency from environment or timezone.
    """
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option("aiws.v2_default_data_region")
    if not value:
        if required:
            raise RuntimeError("non-empty workspace data requires aiws.v2_default_data_region")
        return None
    if value not in _DATA_REGIONS:
        raise RuntimeError("invalid aiws.v2_default_data_region")
    return value  # type: ignore[return-value]


def _configured_workspace_plans(
    workspace_ids: tuple[str, ...],
) -> tuple[tuple[str, WorkspacePlan], ...]:
    """@brief 读取逐 Workspace 的显式计划映射 / Read an explicit plan mapping for every Workspace.

    @param workspace_ids 迁移前实际存在的全部 Workspace ID / Every Workspace ID present before migration.
    @return 按 ID 排序且覆盖精确的计划映射 / Sorted plan mapping with exact coverage.
    @raise RuntimeError 配置缺失、类型错误、包含未知 ID 或遗漏 ID 时抛出 / Raised when the
        mapping is absent, malformed, contains unknown IDs, or omits an ID.
    @note 成员数量不能证明计费 entitlement；迁移禁止据此猜测 personal/team/enterprise。
        / Membership count cannot prove billing entitlement, so migration never infers a plan.
    """
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    raw_value = configuration.get_main_option("aiws.v2_legacy_workspace_plans")
    if raw_value is None:
        raise RuntimeError("missing aiws.v2_legacy_workspace_plans")
    try:
        decoded = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("invalid aiws.v2_legacy_workspace_plans") from error
    if not isinstance(decoded, dict) or any(
        not isinstance(identifier, str)
        or not isinstance(plan, str)
        or plan not in {"personal", "team", "enterprise"}
        for identifier, plan in decoded.items()
    ):
        raise RuntimeError("invalid aiws.v2_legacy_workspace_plans")
    if set(decoded) != set(workspace_ids):
        raise RuntimeError(
            "aiws.v2_legacy_workspace_plans must map every existing Workspace exactly"
        )
    return tuple(
        sorted(
            (identifier, plan)
            for identifier, plan in decoded.items()
        )
    )


def _scalar_count(statement: str) -> int:
    """@brief 执行只返回一行的计数查询 / Execute a scalar count query.

    @param statement 仅由本 revision 常量构造的 SQL / SQL built only from revision constants.
    @return 查询得到的非负行数 / Non-negative row count.
    """
    return int(op.get_bind().execute(sa.text(statement)).scalar_one())


def _install_owner_migration_policies(tables: tuple[str, ...]) -> None:
    """@brief 为当前事务安装显式 owner 全行可见策略 / Install explicit owner visibility for this migration transaction.

    @param tables 仅由 revision 常量给出的限定表名 / Qualified table names from revision constants.
    @return 无返回值 / No return value.
    @note PostgreSQL table owner 在 ``FORCE ROW LEVEL SECURITY`` 下也不能绕过 RLS；这些
        policy 只在 migration 事务内存在，成功路径会显式删除，失败路径由事务回滚。
        / PostgreSQL table owners cannot bypass forced RLS; these policies exist only for the
        migration transaction, are explicitly removed on success, and roll back on failure.
    """
    owner_role = _configured_role("owner_role")
    for table in tables:
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} "
            f"AS PERMISSIVE FOR ALL TO {owner_role} USING (true) WITH CHECK (true)"
        )


def _remove_owner_migration_policies(tables: tuple[str, ...]) -> None:
    """@brief 删除事务内 owner 可见策略 / Remove transaction-local owner visibility policies.

    @param tables 仍然存在的限定表名 / Qualified names of tables that still exist.
    @return 无返回值 / No return value.
    """
    for table in tables:
        op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {table}")


def _preflight() -> tuple[int, DataRegion | None, tuple[tuple[str, WorkspacePlan], ...]]:
    """@brief 在任何 DDL 前验证不可猜测的历史数据 / Validate non-inferable legacy data before DDL.

    @return ``(workspace_count, data_region, workspace_plans)`` 回填输入 / Backfill inputs.
    @raise RuntimeError 邮箱、Owner 或邀请状态无法无损迁移时抛出 / Raised for unsafe legacy state.
    """
    workspace_ids = tuple(
        str(row[0])
        for row in op.get_bind()
        .execute(sa.text("SELECT id FROM identity.workspaces ORDER BY id"))
        .all()
    )
    workspace_count = len(workspace_ids)
    data_region = _configured_data_region(required=workspace_count > 0)
    workspace_plans = _configured_workspace_plans(workspace_ids)

    canonical_collisions = _scalar_count(
        """
        SELECT count(*)
        FROM (
            SELECT lower(btrim(email))
            FROM identity.users
            WHERE email IS NOT NULL AND btrim(email) <> ''
            GROUP BY lower(btrim(email))
            HAVING count(*) > 1
        ) AS collisions
        """
    )
    if canonical_collisions:
        raise RuntimeError("canonical email collisions require operator resolution")

    invalid_active_profiles = _scalar_count(
        """
        SELECT count(*)
        FROM identity.users
        WHERE deleted_at IS NULL
          AND (
              email IS NULL
              OR email <> btrim(email)
              OR length(btrim(email)) < 3
              OR length(btrim(email)) > 320
              OR lower(btrim(email)) !~
                 '^[^[:space:]@]+@[^[:space:]@]+\\.[^[:space:]@]+$'
              OR display_name IS NULL
              OR display_name <> btrim(display_name)
              OR length(btrim(display_name)) < 1
              OR length(btrim(display_name)) > 120
              OR locale !~ '^[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*$'
              OR locale <> btrim(locale)
              OR length(locale) > 35
              OR length(btrim(external_subject)) < 1
              OR length(external_subject) > 255
              OR external_subject <> btrim(external_subject)
          )
        """
    )
    if invalid_active_profiles:
        raise RuntimeError("active users require a contract-valid V2 profile")

    invalid_workspaces = _scalar_count(
        """
        SELECT count(*)
        FROM identity.workspaces
        WHERE deleted_at IS NULL
          AND (length(name) NOT BETWEEN 1 AND 120 OR name <> btrim(name))
        """
    )
    if invalid_workspaces:
        raise RuntimeError("active workspaces require a canonical 1-to-120-character name")

    invalid_member_profiles = _scalar_count(
        """
        SELECT count(*)
        FROM identity.workspace_members AS member
        JOIN identity.users AS member_user ON member_user.id = member.user_id
        WHERE member_user.display_name IS NULL
           OR member_user.display_name <> btrim(member_user.display_name)
           OR length(member_user.display_name) NOT BETWEEN 1 AND 120
        """
    )
    if invalid_member_profiles:
        raise RuntimeError("workspace members require a contract-valid display-name snapshot")

    inconsistent_owners = _scalar_count(
        """
        SELECT count(*)
        FROM identity.workspaces AS workspace
        WHERE workspace.deleted_at IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM identity.workspace_members AS member
            WHERE member.workspace_id = workspace.id
              AND member.resource_owner_id = workspace.resource_owner_id
              AND member.user_id = workspace.resource_owner_id
              AND member.role = 'owner'
              AND member.status = 'active'
        )
        """
    )
    if inconsistent_owners:
        raise RuntimeError("every workspace requires an active resource-owner membership")

    invalid_invitations = _scalar_count(
        """
        SELECT count(*)
        FROM identity.workspace_members AS member
        LEFT JOIN identity.users AS invited_user ON invited_user.id = member.user_id
        WHERE member.status = 'invited'
          AND (
              invited_user.id IS NULL
              OR invited_user.email_verified IS NOT TRUE
              OR invited_user.email IS NULL
              OR invited_user.email <> btrim(invited_user.email)
              OR length(btrim(invited_user.email)) > 320
              OR lower(btrim(invited_user.email)) !~
                 '^[^[:space:]@]+@[^[:space:]@]+\\.[^[:space:]@]+$'
              OR member.role NOT IN ('admin', 'editor', 'viewer')
          )
        """
    )
    if invalid_invitations:
        raise RuntimeError("invited members require a verified valid email and non-owner role")

    duplicate_invitations = _scalar_count(
        """
        SELECT count(*)
        FROM (
            SELECT member.workspace_id, lower(btrim(invited_user.email))
            FROM identity.workspace_members AS member
            JOIN identity.users AS invited_user ON invited_user.id = member.user_id
            WHERE member.status = 'invited'
            GROUP BY member.workspace_id, lower(btrim(invited_user.email))
            HAVING count(*) > 1
        ) AS duplicates
        """
    )
    if duplicate_invitations:
        raise RuntimeError("duplicate pending invitations require operator resolution")
    return workspace_count, data_region, workspace_plans


def _add_and_backfill_columns(
    data_region: DataRegion | None,
    workspace_plans: tuple[tuple[str, WorkspacePlan], ...],
) -> None:
    """@brief 增加 V2 列并按透明规则回填 / Add V2 columns and backfill with transparent rules.

    @param data_region 运维人员显式选择的历史数据地域 / Operator-selected legacy data region.
    @param workspace_plans 运维人员逐 Workspace 确认的计划 / Operator-confirmed plan per Workspace.
    @return 无返回值 / No return value.
    """
    op.drop_constraint("users_email_unique", "users", schema="identity", type_="unique")
    op.add_column("users", sa.Column("email_canonical", sa.String(320)), schema="identity")
    op.add_column(
        "users",
        sa.Column("account_status", sa.String(24), server_default=sa.text("'active'")),
        schema="identity",
    )
    op.add_column(
        "users",
        sa.Column(
            "default_workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="SET NULL"),
        ),
        schema="identity",
    )

    op.alter_column(
        "users",
        "locale",
        existing_type=sa.String(32),
        type_=sa.String(35),
        existing_nullable=False,
        schema="identity",
    )
    op.add_column("workspaces", sa.Column("slug", sa.String(63)), schema="identity")
    op.add_column(
        "workspaces",
        sa.Column("plan", sa.String(16), server_default=sa.text("'personal'")),
        schema="identity",
    )
    op.add_column("workspaces", sa.Column("data_region", sa.String(24)), schema="identity")
    op.add_column(
        "workspace_members",
        sa.Column("display_name", sa.String(120)),
        schema="identity",
    )

    op.execute(
        "UPDATE identity.users SET email_canonical = lower(btrim(email)) "
        "WHERE email IS NOT NULL AND btrim(email) <> ''"
    )
    op.execute(
        "UPDATE identity.users SET account_status = "
        "CASE WHEN deleted_at IS NULL THEN 'active' ELSE 'deleted' END"
    )
    op.execute(
        "UPDATE identity.workspace_members AS member "
        "SET display_name = users.display_name "
        "FROM identity.users AS users WHERE users.id = member.user_id"
    )
    op.execute(
        """
        WITH candidates AS (
            SELECT id,
                   COALESCE(
                       NULLIF(trim(BOTH '-' FROM regexp_replace(lower(name),
                           '[^a-z0-9]+', '-', 'g')), ''),
                       'workspace'
                   ) AS base_slug
            FROM identity.workspaces
        )
        UPDATE identity.workspaces AS workspace
        SET slug = left(candidate.base_slug, 30) || '-' || md5(workspace.id)
        FROM candidates AS candidate
        WHERE candidate.id = workspace.id
        """
    )
    for workspace_id, plan in workspace_plans:
        op.execute(
            sa.text(
                "UPDATE identity.workspaces SET plan = :plan WHERE id = :workspace_id"
            ).bindparams(plan=plan, workspace_id=workspace_id)
        )
    if data_region is not None:
        op.execute(
            sa.text("UPDATE identity.workspaces SET data_region = :data_region").bindparams(
                data_region=data_region
            )
        )
    op.execute(
        """
        WITH ranked_memberships AS (
            SELECT member.user_id,
                   member.workspace_id,
                   row_number() OVER (
                       PARTITION BY member.user_id
                       ORDER BY COALESCE(member.joined_at, member.created_at), member.id
                   ) AS preference
            FROM identity.workspace_members AS member
            JOIN identity.workspaces AS workspace ON workspace.id = member.workspace_id
            WHERE member.status = 'active' AND workspace.deleted_at IS NULL
        )
        UPDATE identity.users AS users
        SET default_workspace_id = ranked.workspace_id
        FROM ranked_memberships AS ranked
        WHERE ranked.user_id = users.id AND ranked.preference = 1
        """
    )

    op.alter_column("users", "account_status", nullable=False, schema="identity")
    op.alter_column("workspaces", "slug", nullable=False, schema="identity")
    op.alter_column("workspaces", "plan", nullable=False, schema="identity")
    op.alter_column("workspaces", "data_region", nullable=False, schema="identity")
    op.alter_column("workspace_members", "display_name", nullable=False, schema="identity")
    op.create_check_constraint(
        "users_account_status",
        "users",
        "account_status IN ('active', 'suspended', 'deletion_scheduled', 'deleted')",
        schema="identity",
    )
    op.create_check_constraint(
        "users_v2_profile",
        "users",
        "account_status = 'deleted' OR (email IS NOT NULL "
        "AND email = btrim(email) "
        "AND email_canonical IS NOT NULL "
        "AND email_canonical = lower(email) "
        "AND length(btrim(email)) BETWEEN 3 AND 320 "
        "AND lower(btrim(email)) ~ "
        "'^[^[:space:]@]+@[^[:space:]@]+\\.[^[:space:]@]+$' "
        "AND display_name IS NOT NULL AND display_name = btrim(display_name) "
        "AND length(display_name) BETWEEN 1 AND 120 "
        "AND locale ~ '^[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*$' "
        "AND locale = btrim(locale) AND length(locale) BETWEEN 2 AND 35 "
        "AND external_subject = btrim(external_subject) "
        "AND length(external_subject) BETWEEN 1 AND 255)",
        schema="identity",
    )
    op.create_check_constraint(
        "workspaces_name",
        "workspaces",
        "deleted_at IS NOT NULL OR "
        "(name = btrim(name) AND length(name) BETWEEN 1 AND 120)",
        schema="identity",
    )
    op.create_check_constraint(
        "workspace_members_display_name",
        "workspace_members",
        "display_name = btrim(display_name) AND length(display_name) BETWEEN 1 AND 120",
        schema="identity",
    )
    op.create_check_constraint(
        "workspaces_slug",
        "workspaces",
        "slug ~ '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'",
        schema="identity",
    )
    op.create_check_constraint(
        "workspaces_plan",
        "workspaces",
        "plan IN ('personal', 'team', 'enterprise')",
        schema="identity",
    )
    op.create_check_constraint(
        "workspaces_data_region",
        "workspaces",
        "data_region IN ('cn', 'global', 'private_deployment')",
        schema="identity",
    )
    op.create_index(
        "uq_users_email_canonical",
        "users",
        ["email_canonical"],
        unique=True,
        schema="identity",
        postgresql_where=sa.text("email_canonical IS NOT NULL"),
    )
    op.create_index("uq_workspaces_slug", "workspaces", ["slug"], unique=True, schema="identity")


def _add_identity_flow_completion_semantics() -> None:
    """@brief 为完成的身份流程记录精确完成时刻 / Record exact identity-flow completion time.

    @return 无返回值 / No return value.
    @note 历史 completed 流程以 created_at 保守回填；这只会缩短而不会延长证明窗口。
        / Historical completed flows use ``created_at`` as a conservative lower bound, which can
        only shorten and never extend the proof window.
    """
    op.add_column(
        "identity_flows",
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        schema="identity",
    )
    op.execute(
        "UPDATE identity.identity_flows SET completed_at = created_at WHERE status = 'completed'"
    )
    op.create_check_constraint(
        "identity_flows_completion",
        "identity_flows",
        "(status = 'completed' AND completed_at IS NOT NULL "
        "AND completed_at >= created_at AND completed_at <= expires_at) OR "
        "(status <> 'completed' AND completed_at IS NULL)",
        schema="identity",
    )


def _create_v2_tables() -> None:
    """@brief 创建邀请与账号删除请求表 / Create invitation and account-deletion-request tables.

    @return 无返回值 / No return value.
    """
    op.create_table(
        "workspace_invitations",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("email_canonical", sa.String(320), nullable=False),
        sa.Column("email_hint", sa.String(320), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("invited_by_actor_id", sa.String(128)),
        sa.Column(
            "accepted_by_user_id",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "role IN ('admin', 'editor', 'viewer')", name="workspace_invitations_role"
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'accepted', 'revoked', 'expired')",
            name="workspace_invitations_status",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND resolved_at IS NULL "
            "AND accepted_by_user_id IS NULL) OR "
            "(status = 'accepted' AND resolved_at IS NOT NULL "
            "AND accepted_by_user_id IS NOT NULL) OR "
            "(status IN ('revoked', 'expired') AND resolved_at IS NOT NULL "
            "AND accepted_by_user_id IS NULL)",
            name="workspace_invitations_state",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_workspace_invitations_workspace_created",
        "workspace_invitations",
        ["workspace_id", "created_at", "id"],
        schema="identity",
    )
    op.create_index(
        "uq_workspace_invitations_pending_email",
        "workspace_invitations",
        ["workspace_id", "email_canonical"],
        unique=True,
        schema="identity",
        postgresql_where=sa.text("status = 'pending'"),
    )

    op.create_table(
        "account_deletion_requests",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column(
            "user_id",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'scheduled'")),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("problem", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "status IN ('scheduled', 'running', 'completed', 'cancelled', 'failed')",
            name="account_deletion_requests_status",
        ),
        sa.CheckConstraint(
            "(status IN ('scheduled', 'running', 'cancelled') AND completed_at IS NULL "
            "AND problem IS NULL) OR (status = 'completed' AND completed_at IS NOT NULL "
            "AND problem IS NULL) OR (status = 'failed' AND completed_at IS NULL "
            "AND problem IS NOT NULL)",
            name="account_deletion_requests_state",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_account_deletion_requests_user_created",
        "account_deletion_requests",
        ["user_id", "created_at", "id"],
        schema="identity",
    )
    op.create_index(
        "uq_account_deletion_requests_live_user",
        "account_deletion_requests",
        ["user_id"],
        unique=True,
        schema="identity",
        postgresql_where=sa.text("status IN ('scheduled', 'running')"),
    )


def _migrate_member_states() -> None:
    """@brief 分离邀请并规范化成员状态 / Separate invitations and normalize membership states.

    @return 无返回值 / No return value.
    """
    op.execute(
        """
        INSERT INTO identity.workspace_invitations (
            id, workspace_id, email_canonical, email_hint,
            role, status, expires_at, resolved_at, invited_by_actor_id,
            created_at, updated_at, revision, extensions
        )
        SELECT 'winv_' || md5(member.id),
               member.workspace_id,
               lower(btrim(invited_user.email)),
               left(invited_user.email, 1) || '***@' || split_part(invited_user.email, '@', 2),
               member.role,
               CASE WHEN now() < member.created_at + interval '7 days'
                    THEN 'pending' ELSE 'expired' END,
               member.created_at + interval '7 days',
               CASE WHEN now() < member.created_at + interval '7 days'
                    THEN NULL ELSE member.created_at + interval '7 days' END,
               member.invited_by_actor_id,
               member.created_at,
               member.updated_at,
               member.revision,
               member.extensions
        FROM identity.workspace_members AS member
        JOIN identity.users AS invited_user ON invited_user.id = member.user_id
        WHERE member.status = 'invited'
        """
    )
    op.execute("DELETE FROM identity.workspace_members WHERE status = 'invited'")
    op.execute(
        "UPDATE identity.workspace_members SET status = 'suspended' WHERE status = 'disabled'"
    )
    op.drop_constraint(
        "workspace_members_status", "workspace_members", schema="identity", type_="check"
    )
    op.create_check_constraint(
        "workspace_members_status_v2",
        "workspace_members",
        "status IN ('active', 'suspended')",
        schema="identity",
    )


def _secure_v2_tables() -> None:
    """@brief 配置最小权限和默认拒绝 RLS / Configure least privilege and default-deny RLS.

    @return 无返回值 / No return value.
    """
    app_role = _configured_role("app_role")
    owner_role = _configured_role("owner_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    for table in ("identity.workspace_invitations", "identity.account_deletion_requests"):
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
    op.execute("DROP POLICY workspace_app_identity_self ON identity.users")
    op.execute(
        f"CREATE POLICY workspace_app_identity_self ON identity.users "
        f"AS PERMISSIVE FOR ALL TO {app_role} "
        "USING (id = current_setting('app.actor_id', true)) "
        "WITH CHECK (id = current_setting('app.actor_id', true))"
    )
    op.execute(
        f"CREATE POLICY identity_owner_narrow_functions ON identity.users "
        f"AS PERMISSIVE FOR SELECT TO {owner_role} USING (true)"
    )
    op.execute(
        f"CREATE POLICY identity_owner_narrow_updates ON identity.users "
        f"AS PERMISSIVE FOR UPDATE TO {owner_role} USING (true) WITH CHECK (true)"
    )
    op.execute(
        """
        CREATE FUNCTION identity.resolve_login_user_id(candidate_email text)
        RETURNS text
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        AS $function$
            SELECT users.id
            FROM identity.users AS users
            WHERE users.email_canonical = lower(btrim(candidate_email))
              AND users.account_status IN ('active', 'deletion_scheduled')
            LIMIT 1
        $function$
        """
    )
    op.execute(
        """
        CREATE FUNCTION identity.clear_inactive_member_default_workspace(
            target_user_id text,
            inactive_workspace_id text
        )
        RETURNS void
        LANGUAGE sql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        AS $function$
            UPDATE identity.users
            SET default_workspace_id = NULL,
                revision = revision + 1,
                updated_at = statement_timestamp()
            WHERE id = target_user_id
              AND default_workspace_id = inactive_workspace_id
        $function$
        """
    )
    for signature in (
        "identity.resolve_login_user_id(text)",
        "identity.clear_inactive_member_default_workspace(text, text)",
    ):
        op.execute(
            f"REVOKE ALL PRIVILEGES ON FUNCTION {signature} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"ALTER FUNCTION {signature} OWNER TO {owner_role}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {app_role}")
    op.execute("DROP POLICY workspace_app_workspace_scope ON identity.workspaces")
    op.execute(
        f"CREATE POLICY workspace_app_workspace_read ON identity.workspaces "
        f"AS PERMISSIVE FOR SELECT TO {app_role} USING (EXISTS ("
        "SELECT 1 FROM identity.workspace_members AS membership "
        "WHERE membership.workspace_id = workspaces.id "
        "AND membership.user_id = current_setting('app.actor_id', true) "
        "AND membership.status = 'active'))"
    )
    op.execute(
        f"CREATE POLICY workspace_app_workspace_insert ON identity.workspaces "
        f"AS PERMISSIVE FOR INSERT TO {app_role} WITH CHECK ("
        "id = current_setting('app.workspace_id', true) "
        "AND resource_owner_id = current_setting('app.actor_id', true))"
    )
    op.execute(
        f"CREATE POLICY workspace_app_workspace_update ON identity.workspaces "
        f"AS PERMISSIVE FOR UPDATE TO {app_role} USING (EXISTS ("
        "SELECT 1 FROM identity.workspace_members AS membership "
        "WHERE membership.workspace_id = workspaces.id "
        "AND membership.user_id = current_setting('app.actor_id', true) "
        "AND membership.status = 'active')) WITH CHECK ("
        "id = current_setting('app.workspace_id', true))"
    )
    op.execute(
        f"CREATE POLICY workspace_app_workspace_delete ON identity.workspaces "
        f"AS PERMISSIVE FOR DELETE TO {app_role} USING (EXISTS ("
        "SELECT 1 FROM identity.workspace_members AS membership "
        "WHERE membership.workspace_id = workspaces.id "
        "AND membership.user_id = current_setting('app.actor_id', true) "
        "AND membership.status = 'active'))"
    )

    op.execute("DROP POLICY workspace_app_tenant_scope ON identity.workspace_members")
    op.execute(
        f"CREATE POLICY workspace_app_membership_self ON identity.workspace_members "
        f"AS PERMISSIVE FOR SELECT TO {app_role} "
        "USING (user_id = current_setting('app.actor_id', true))"
    )
    op.execute(
        f"CREATE POLICY workspace_app_membership_scope ON identity.workspace_members "
        f"AS PERMISSIVE FOR ALL TO {app_role} "
        "USING (workspace_id = current_setting('app.workspace_id', true)) "
        "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )

    for table in _COLLABORATIVE_POLICY_TABLES:
        op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
        op.execute(
            f"CREATE POLICY workspace_app_tenant_scope ON {table} "
            f"AS PERMISSIVE FOR ALL TO {app_role} "
            "USING (workspace_id = current_setting('app.workspace_id', true)) "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )

    invitation_predicate = "workspace_id = current_setting('app.workspace_id', true)"
    op.execute(
        f"CREATE POLICY workspace_app_invitation_scope ON identity.workspace_invitations "
        f"AS PERMISSIVE FOR ALL TO {app_role} USING ({invitation_predicate}) "
        f"WITH CHECK ({invitation_predicate})"
    )
    deletion_predicate = "user_id = current_setting('app.actor_id', true)"
    op.execute(
        f"CREATE POLICY identity_app_account_deletion_self "
        f"ON identity.account_deletion_requests AS PERMISSIVE FOR ALL TO {app_role} "
        f"USING ({deletion_predicate}) WITH CHECK ({deletion_predicate})"
    )


def _write_migration_audit(workspace_count: int, data_region: DataRegion | None) -> None:
    """@brief 为非空业务库写入 0008 追加式账本 / Write the 0008 append-only ledger for non-empty data.

    @param workspace_count 迁移前 Workspace 数量 / Pre-migration Workspace count.
    @param data_region 历史 Workspace 的显式地域 / Explicit region for legacy Workspaces.
    @return 无返回值 / No return value.
    """
    business_rows = (
        _scalar_count(
            "SELECT (SELECT count(*) FROM identity.users) + "
            "(SELECT count(*) FROM identity.workspace_members)"
        )
        + workspace_count
    )
    if business_rows == 0:
        return
    op.execute(
        sa.text(
            """
            INSERT INTO identity.api_migration_audits (
                id, migration_id, phase, event_type,
                source_api_version, target_api_version, details
            ) VALUES (
                'audit_20260723_0013', :migration_id, 1, 'completed',
                'v1', 'v2', jsonb_build_object(
                    'workspace_count', :workspace_count,
                    'data_region', :data_region,
                    'plan_rule', 'explicit per-workspace operator mapping',
                    'default_workspace_rule', 'earliest active non-deleted membership'
                )
            )
            """
        ).bindparams(
            migration_id=_AUDIT_MIGRATION_ID,
            workspace_count=workspace_count,
            data_region=data_region,
        )
    )


def upgrade() -> None:
    """@brief 原子迁移 API V2 身份与 Workspace 状态 / Atomically migrate API V2 identity/workspace state.

    @return 无返回值 / No return value.
    """
    _assert_tenant_policy_coverage(_LEGACY_TENANT_POLICY_TABLES)
    _install_owner_migration_policies(_LEGACY_RLS_TABLES)
    workspace_count, data_region, workspace_plans = _preflight()
    _add_and_backfill_columns(data_region, workspace_plans)
    _add_identity_flow_completion_semantics()
    _create_v2_tables()
    _migrate_member_states()
    _secure_v2_tables()
    _write_migration_audit(workspace_count, data_region)
    _remove_owner_migration_policies(_LEGACY_RLS_TABLES)


def _assert_tenant_policy_coverage(expected_tables: tuple[str, ...]) -> None:
    """@brief 拒绝修改未知或缺失的同名租户策略 / Reject unknown or missing same-name tenant policies.

    @param expected_tables 当前状态应由同名策略覆盖的冻结表集合 / Frozen tables that the
        same-name policy must cover in the current state.
    @return 无返回值 / No return value.
    @raise RuntimeError 实际覆盖范围偏离该 revision 冻结拓扑时抛出 / Raised when actual
        policy coverage differs from this revision's frozen topology.
    """
    rows = op.get_bind().execute(
        sa.text(
            "SELECT schemaname || '.' || tablename "
            "FROM pg_policies WHERE policyname = 'workspace_app_tenant_scope' "
            "ORDER BY schemaname, tablename"
        )
    )
    actual = tuple(str(row[0]) for row in rows)
    expected = tuple(sorted(expected_tables))
    if actual != expected:
        raise RuntimeError(
            "legacy tenant-policy coverage differs from the frozen 0013 topology"
        )


def _restore_v1_rls() -> None:
    """@brief 为空库降级恢复 V1 RLS 谓词 / Restore V1 RLS predicates for an empty downgrade.

    @return 无返回值 / No return value.
    """
    app_role = _configured_role("app_role")
    op.execute("DROP FUNCTION identity.clear_inactive_member_default_workspace(text, text)")
    op.execute("DROP FUNCTION identity.resolve_login_user_id(text)")
    op.execute("DROP POLICY identity_owner_narrow_updates ON identity.users")
    op.execute("DROP POLICY identity_owner_narrow_functions ON identity.users")
    op.execute("DROP POLICY workspace_app_identity_self ON identity.users")
    op.execute(
        f"CREATE POLICY workspace_app_identity_self ON identity.users "
        f"AS PERMISSIVE FOR ALL TO {app_role} USING ("
        "id = current_setting('app.actor_id', true) OR "
        "id = current_setting('app.resource_owner_id', true)) WITH CHECK ("
        "id = current_setting('app.actor_id', true) OR "
        "id = current_setting('app.resource_owner_id', true))"
    )
    for policy in (
        "workspace_app_workspace_read",
        "workspace_app_workspace_insert",
        "workspace_app_workspace_update",
        "workspace_app_workspace_delete",
    ):
        op.execute(f"DROP POLICY {policy} ON identity.workspaces")
    op.execute(
        f"CREATE POLICY workspace_app_workspace_scope ON identity.workspaces "
        f"AS PERMISSIVE FOR ALL TO {app_role} USING ("
        "id = current_setting('app.workspace_id', true) AND "
        "resource_owner_id = current_setting('app.resource_owner_id', true)) WITH CHECK ("
        "id = current_setting('app.workspace_id', true) AND "
        "resource_owner_id = current_setting('app.resource_owner_id', true))"
    )
    op.execute("DROP POLICY workspace_app_membership_self ON identity.workspace_members")
    op.execute("DROP POLICY workspace_app_membership_scope ON identity.workspace_members")
    op.execute(
        f"CREATE POLICY workspace_app_tenant_scope ON identity.workspace_members "
        f"AS PERMISSIVE FOR ALL TO {app_role} USING ("
        "workspace_id = current_setting('app.workspace_id', true) AND "
        "resource_owner_id = current_setting('app.resource_owner_id', true)) WITH CHECK ("
        "workspace_id = current_setting('app.workspace_id', true) AND "
        "resource_owner_id = current_setting('app.resource_owner_id', true))"
    )
    for table in _COLLABORATIVE_POLICY_TABLES:
        op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
        op.execute(
            f"CREATE POLICY workspace_app_tenant_scope ON {table} "
            f"AS PERMISSIVE FOR ALL TO {app_role} USING ("
            "workspace_id = current_setting('app.workspace_id', true) AND "
            "resource_owner_id = current_setting('app.resource_owner_id', true)) "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true) AND "
            "resource_owner_id = current_setting('app.resource_owner_id', true))"
        )


def downgrade() -> None:
    """@brief 仅允许空且无不可逆状态的数据库降级 / Downgrade only empty, reversible databases.

    @return 无返回值 / No return value.
    @raise RuntimeError 新业务状态或追加审计证据存在时拒绝 / Raised for new state or audit evidence.
    """
    _assert_tenant_policy_coverage(_COLLABORATIVE_POLICY_TABLES)
    _install_owner_migration_policies(_LEGACY_RLS_TABLES + _V2_RLS_TABLES)
    irreversible_rows = _scalar_count(
        """
        SELECT
            (SELECT count(*) FROM identity.workspace_invitations) +
            (SELECT count(*) FROM identity.account_deletion_requests) +
            (SELECT count(*) FROM identity.workspaces) +
            (SELECT count(*) FROM identity.workspace_members WHERE status = 'suspended') +
            (SELECT count(*) FROM identity.users
             WHERE account_status <> 'active'
                OR default_workspace_id IS NOT NULL
                OR length(locale) > 32) +
            (SELECT count(*) FROM identity.identity_flows
             WHERE completed_at IS NOT NULL) +
            (SELECT count(*) FROM identity.api_migration_audits
             WHERE migration_id = 'api-v2-identity-workspace-0013')
        """
    )
    if irreversible_rows:
        raise RuntimeError("cannot downgrade non-empty or irreversible API V2 identity state")

    _restore_v1_rls()
    op.drop_constraint(
        "identity_flows_completion", "identity_flows", schema="identity", type_="check"
    )
    op.drop_column("identity_flows", "completed_at", schema="identity")
    op.drop_table("account_deletion_requests", schema="identity")
    op.drop_table("workspace_invitations", schema="identity")
    op.drop_constraint(
        "workspace_members_status_v2", "workspace_members", schema="identity", type_="check"
    )
    op.create_check_constraint(
        "workspace_members_status",
        "workspace_members",
        "status IN ('active', 'invited', 'disabled')",
        schema="identity",
    )
    op.drop_index("uq_workspaces_slug", table_name="workspaces", schema="identity")
    op.drop_index("uq_users_email_canonical", table_name="users", schema="identity")
    op.create_unique_constraint("users_email_unique", "users", ["email"], schema="identity")
    op.drop_constraint("workspaces_data_region", "workspaces", schema="identity", type_="check")
    op.drop_constraint("workspaces_plan", "workspaces", schema="identity", type_="check")
    op.drop_constraint("workspaces_slug", "workspaces", schema="identity", type_="check")
    op.drop_constraint("workspaces_name", "workspaces", schema="identity", type_="check")
    op.drop_constraint(
        "workspace_members_display_name",
        "workspace_members",
        schema="identity",
        type_="check",
    )
    op.drop_constraint("users_account_status", "users", schema="identity", type_="check")
    op.drop_constraint("users_v2_profile", "users", schema="identity", type_="check")
    op.drop_column("workspaces", "data_region", schema="identity")
    op.drop_column("workspaces", "plan", schema="identity")
    op.drop_column("workspaces", "slug", schema="identity")
    op.drop_column("workspace_members", "display_name", schema="identity")
    op.drop_column("users", "default_workspace_id", schema="identity")
    op.drop_column("users", "account_status", schema="identity")
    op.drop_column("users", "email_canonical", schema="identity")
    op.alter_column(
        "users",
        "locale",
        existing_type=sa.String(35),
        type_=sa.String(32),
        existing_nullable=False,
        schema="identity",
    )
    _remove_owner_migration_policies(_LEGACY_RLS_TABLES)
