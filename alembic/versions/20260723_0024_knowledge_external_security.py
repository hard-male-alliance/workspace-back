"""@brief Knowledge 外部 secret、配额与 lexical index / Knowledge external secrets, quota, and lexical index.

Revision ID: 20260723_0024
Revises: 20260723_0023
Create Date: 2026-07-23

两张 secret 表是 OAuth/device transaction 与 Connection credential 的唯一 durable vault；
均只存 AES-256-GCM 密文。Quota 表由一个 owner-owned 窄函数原子访问，避免多 worker
``SUM``/``INSERT`` 竞态。``chunks.search_vector`` 为现有数据自动生成，不复制正文。
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260723_0024"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0023"
"""@brief 统一 outbox lease 前驱 / Unified outbox-lease predecessor."""

branch_labels = None
"""@brief 本迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 本迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal["owner_role", "app_role", "dashboard_role", "migrator_role"]
"""@brief Alembic 接收的 dbctl role 选项 / dbctl role options accepted by Alembic."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role 标识白名单 / PostgreSQL role identifier allowlist."""

_VAULT_TABLES = (
    "knowledge.connection_provider_sessions",
    "knowledge.connection_credentials",
)
"""@brief actor+Workspace scoped secret tables / Actor-and-Workspace-scoped secret tables."""

_QUOTA_POLICY = "knowledge_upload_quota_owner_0024"
"""@brief quota SECURITY DEFINER function 的 owner policy / Owner policy for the quota SECURITY DEFINER function."""

_DOWNGRADE_POLICY = "knowledge_external_downgrade_owner_0024"
"""@brief downgrade 预检期间的 vault owner policy / Vault-owner policy used during downgrade preflight."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回经白名单校验并引用的 role / Return an allowlisted and quoted role."""

    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option(f"aiws.{option}")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > 63
    ):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def _create_provider_session_vault() -> None:
    """@brief 创建 OAuth/device transaction 加密表 / Create the encrypted OAuth/device transaction table."""

    op.create_table(
        "connection_provider_sessions",
        sa.Column("reference", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "created_by",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("provider", sa.String(101), nullable=False),
        sa.Column("flow", sa.String(16), nullable=False),
        sa.Column("state_sha256", sa.String(64), nullable=False),
        sa.Column("key_id", sa.String(64)),
        sa.Column("nonce", sa.LargeBinary()),
        sa.Column("ciphertext", sa.LargeBinary()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "reference ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND provider ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND flow IN ('browser_redirect', 'device_code') "
            "AND state_sha256 ~ '^[a-f0-9]{64}$'",
            name="knowledge_provider_sessions_envelope",
        ),
        sa.CheckConstraint(
            "expires_at > created_at AND updated_at >= created_at "
            "AND ((status = 'pending' AND key_id IS NOT NULL "
            "AND key_id ~ '^[A-Za-z][A-Za-z0-9_.-]{2,63}$' "
            "AND octet_length(nonce) = 12 "
            "AND octet_length(ciphertext) BETWEEN 17 AND 65536) "
            "OR (status IN ('consumed', 'expired', 'failed') "
            "AND key_id IS NULL AND nonce IS NULL AND ciphertext IS NULL))",
            name="knowledge_provider_sessions_lifecycle",
        ),
        sa.UniqueConstraint(
            "reference", "workspace_id", name="knowledge_provider_sessions_reference_workspace"
        ),
        schema="knowledge",
    )
    op.create_index(
        "ix_knowledge_provider_sessions_expiry",
        "connection_provider_sessions",
        ["workspace_id", "created_by", "expires_at", "reference"],
        schema="knowledge",
        postgresql_where=sa.text("status = 'pending'"),
    )


def _create_credential_vault() -> None:
    """@brief 创建可 rotation/orphan-reconcile 的 credential vault / Create the rotation- and orphan-reconciliation-capable credential vault."""

    op.create_table(
        "connection_credentials",
        sa.Column("reference", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "created_by",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # ``connection_id`` intentionally cannot be an FK: prepare stores the credential before
        # the outer idempotent request transaction creates the Connection. Reconciliation below
        # deletes a staged row if that final commit never happens.
        sa.Column("connection_id", sa.String(160), nullable=False),
        sa.Column("provider", sa.String(101), nullable=False),
        sa.Column("auth_method", sa.String(16), nullable=False),
        sa.Column("operation_id", sa.String(160), nullable=False),
        sa.Column("secret_fingerprint", sa.String(64), nullable=False),
        sa.Column("key_id", sa.String(64)),
        sa.Column("nonce", sa.LargeBinary()),
        sa.Column("ciphertext", sa.LargeBinary()),
        sa.Column(
            "scopes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'staged'")),
        sa.Column("validated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("orphan_after", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "reference ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND connection_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND provider ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND auth_method IN ('oauth', 'device_code', 'api_token') "
            "AND length(operation_id) BETWEEN 1 AND 160 "
            "AND operation_id = btrim(operation_id) "
            "AND secret_fingerprint ~ '^[a-f0-9]{64}$'",
            name="knowledge_credentials_envelope",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(scopes) = 'array' AND jsonb_array_length(scopes) <= 100 "
            "AND validated_at >= created_at - interval '5 minutes' "
            "AND orphan_after > created_at AND updated_at >= created_at",
            name="knowledge_credentials_values",
        ),
        sa.CheckConstraint(
            "((status IN ('staged', 'active', 'revoking') "
            "AND key_id IS NOT NULL "
            "AND key_id ~ '^[A-Za-z][A-Za-z0-9_.-]{2,63}$' "
            "AND octet_length(nonce) = 12 "
            "AND octet_length(ciphertext) BETWEEN 17 AND 65536) "
            "OR (status = 'revoked' AND key_id IS NULL "
            "AND nonce IS NULL AND ciphertext IS NULL))",
            name="knowledge_credentials_lifecycle",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "created_by",
            "provider",
            "operation_id",
            name="knowledge_credentials_stable_operation",
        ),
        sa.UniqueConstraint(
            "reference", "workspace_id", name="knowledge_credentials_reference_workspace"
        ),
        schema="knowledge",
    )
    op.create_index(
        "ix_knowledge_credentials_orphans",
        "connection_credentials",
        ["workspace_id", "created_by", "orphan_after", "reference"],
        schema="knowledge",
        postgresql_where=sa.text("status = 'staged'"),
    )


def _create_upload_quota(
    owner_role: str,
    app_role: str,
    dashboard_role: str,
    migrator_role: str,
) -> None:
    """@brief 创建只可经窄函数访问的 quota ledger / Create a quota ledger accessible only through a narrow function."""

    op.create_table(
        "upload_quota_reservations",
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("operation_id", sa.String(160), primary_key=True),
        sa.Column("upload_id", sa.String(160), nullable=False, unique=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "reserved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "length(operation_id) BETWEEN 1 AND 160 AND operation_id = btrim(operation_id) "
            "AND upload_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND size_bytes BETWEEN 1 AND 1073741824",
            name="knowledge_upload_quota_values",
        ),
        schema="knowledge",
    )
    table = "knowledge.upload_quota_reservations"
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {_QUOTA_POLICY} ON {table} AS PERMISSIVE FOR ALL TO {owner_role} "
        "USING (true) WITH CHECK (true)"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {table} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(
        """
        CREATE FUNCTION knowledge.reserve_upload_quota(
            candidate_workspace_id text,
            candidate_upload_id text,
            candidate_operation_id text,
            candidate_size_bytes bigint,
            candidate_maximum_bytes bigint
        ) RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, knowledge
        SET row_security = on
        AS $function$
        DECLARE
            existing_upload_id text;
            existing_size_bytes bigint;
            current_bytes bigint;
        BEGIN
            IF candidate_workspace_id IS NULL OR candidate_workspace_id = ''
               OR candidate_upload_id IS NULL
               OR candidate_upload_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
               OR candidate_operation_id IS NULL
               OR length(candidate_operation_id) NOT BETWEEN 1 AND 160
               OR candidate_operation_id <> btrim(candidate_operation_id)
               OR candidate_size_bytes NOT BETWEEN 1 AND 1073741824
               OR candidate_maximum_bytes < candidate_size_bytes THEN
                RAISE EXCEPTION 'invalid upload quota reservation arguments'
                    USING ERRCODE = '22023';
            END IF;
            PERFORM pg_advisory_xact_lock(hashtextextended(candidate_workspace_id, 240024));
            SELECT reservation.upload_id, reservation.size_bytes
              INTO existing_upload_id, existing_size_bytes
              FROM knowledge.upload_quota_reservations AS reservation
             WHERE reservation.workspace_id = candidate_workspace_id
               AND reservation.operation_id = candidate_operation_id;
            IF FOUND THEN
                IF existing_upload_id <> candidate_upload_id
                   OR existing_size_bytes <> candidate_size_bytes THEN
                    RAISE EXCEPTION 'upload quota operation was reused with different input'
                        USING ERRCODE = '22023';
                END IF;
                RETURN true;
            END IF;
            SELECT COALESCE(sum(reservation.size_bytes), 0)
              INTO current_bytes
              FROM knowledge.upload_quota_reservations AS reservation
             WHERE reservation.workspace_id = candidate_workspace_id;
            IF current_bytes + candidate_size_bytes > candidate_maximum_bytes THEN
                RETURN false;
            END IF;
            INSERT INTO knowledge.upload_quota_reservations (
                workspace_id, operation_id, upload_id, size_bytes, reserved_at
            ) VALUES (
                candidate_workspace_id,
                candidate_operation_id,
                candidate_upload_id,
                candidate_size_bytes,
                statement_timestamp()
            );
            RETURN true;
        END
        $function$
        """
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON FUNCTION "
        "knowledge.reserve_upload_quota(text,text,text,bigint,bigint) "
        f"FROM PUBLIC, {app_role}"
    )
    op.execute(
        "ALTER FUNCTION knowledge.reserve_upload_quota(text,text,text,bigint,bigint) "
        f"OWNER TO {owner_role}"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION "
        "knowledge.reserve_upload_quota(text,text,text,bigint,bigint) "
        f"TO {app_role}"
    )


def _secure_vault_tables(
    *,
    app_role: str,
    dashboard_role: str,
    migrator_role: str,
) -> None:
    """@brief 对 vault 应用 actor+Workspace RLS 和最小角色权限 / Apply actor-and-Workspace RLS and least-role privileges to the vault."""

    predicate = (
        "workspace_id = current_setting('app.workspace_id', true) "
        "AND created_by = current_setting('app.actor_id', true)"
    )
    for table in _VAULT_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")
        for command in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            clause = (
                f"USING ({predicate})"
                if command in {"SELECT", "DELETE"}
                else f"WITH CHECK ({predicate})"
                if command == "INSERT"
                else f"USING ({predicate}) WITH CHECK ({predicate})"
            )
            op.execute(
                f"CREATE POLICY knowledge_vault_{command.lower()}_0024 ON {table} "
                f"AS PERMISSIVE FOR {command} TO {app_role} {clause}"
            )


def _add_lexical_index() -> None:
    """@brief 为现有 chunk 正文添加 generated tsvector+GIN / Add a generated tsvector and GIN index for existing chunk text."""

    op.execute(
        "ALTER TABLE knowledge.chunks ADD COLUMN search_vector tsvector "
        "GENERATED ALWAYS AS (to_tsvector('simple', coalesce(text_content, ''))) STORED"
    )
    op.create_index(
        "ix_knowledge_chunks_search_vector_gin",
        "chunks",
        ["search_vector"],
        schema="knowledge",
        postgresql_using="gin",
    )


def _grant_worker_columns(app_role: str) -> None:
    """@brief 开放 durable Knowledge worker 所需的窄更新列 / Grant the narrow update columns required by the durable Knowledge worker."""

    op.execute(
        f"GRANT UPDATE (source_input, public_config, deleted_at) ON knowledge.sources TO {app_role}"
    )
    op.execute(f"GRANT UPDATE (origin, parser_metadata) ON knowledge.source_versions TO {app_role}")
    op.execute(f"GRANT UPDATE (percent, result) ON agent.jobs TO {app_role}")


def upgrade() -> None:
    """@brief 发布 Knowledge 外部安全基础设施 / Publish Knowledge external-security infrastructure."""

    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _create_provider_session_vault()
    _create_credential_vault()
    _create_upload_quota(owner_role, app_role, dashboard_role, migrator_role)
    _secure_vault_tables(
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    _add_lexical_index()
    _grant_worker_columns(app_role)


def _install_downgrade_visibility(owner_role: str) -> None:
    """@brief 为 vault 安装事务级 owner 可见性 / Install transaction-scoped vault visibility for the owner."""

    for table in _VAULT_TABLES:
        op.execute(
            f"CREATE POLICY {_DOWNGRADE_POLICY} ON {table} AS PERMISSIVE FOR SELECT "
            f"TO {owner_role} USING (true)"
        )


def _preflight_downgrade() -> None:
    """@brief 拒绝丢失任何 vault/quota 业务状态 / Refuse to lose any vault or quota business state."""

    counts = {
        "provider sessions": "knowledge.connection_provider_sessions",
        "connection credentials": "knowledge.connection_credentials",
        "upload quota reservations": "knowledge.upload_quota_reservations",
    }
    populated = [
        label
        for label, table in counts.items()
        if int(op.get_bind().scalar(sa.text(f"SELECT count(*) FROM {table}")) or 0) > 0
    ]
    if populated:
        raise RuntimeError(
            "cannot downgrade non-empty Knowledge external-security state: " + ", ".join(populated)
        )


def downgrade() -> None:
    """@brief 仅空状态安全回退 / Safely downgrade only empty state."""

    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    _install_downgrade_visibility(owner_role)
    _preflight_downgrade()
    op.execute(f"REVOKE UPDATE (percent, result) ON agent.jobs FROM {app_role}")
    op.execute(
        f"REVOKE UPDATE (origin, parser_metadata) ON knowledge.source_versions FROM {app_role}"
    )
    op.execute(
        "REVOKE UPDATE (source_input, public_config, deleted_at) ON knowledge.sources "
        f"FROM {app_role}"
    )
    op.drop_index("ix_knowledge_chunks_search_vector_gin", table_name="chunks", schema="knowledge")
    op.drop_column("chunks", "search_vector", schema="knowledge")
    op.execute("DROP FUNCTION knowledge.reserve_upload_quota(text,text,text,bigint,bigint)")
    op.execute(f"DROP POLICY {_QUOTA_POLICY} ON knowledge.upload_quota_reservations")
    op.drop_table("upload_quota_reservations", schema="knowledge")
    op.drop_table("connection_credentials", schema="knowledge")
    op.drop_table("connection_provider_sessions", schema="knowledge")
