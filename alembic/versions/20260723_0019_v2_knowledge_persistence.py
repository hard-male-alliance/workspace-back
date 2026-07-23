"""@brief 统一 API V2 Connection、Knowledge 与 Upload 持久化 / Unify API V2 Connection, Knowledge, and Upload persistence.

Revision ID: 20260723_0019
Revises: 20260723_0018
Create Date: 2026-07-23

此迁移采用 expand/backfill/constrain/secure 顺序。旧 ``resume.import_upload_sessions``
逐行原样搬入 ``knowledge.upload_sessions`` 后才删除；V1 Knowledge 行通过确定性投影
保留其来源输入、版本、策略、chunk 计数和 worker 引用，不要求空表，也不伪造缺失的
上传声明或签名 URL。
"""

from __future__ import annotations

import hashlib
import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260723_0019"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0018"
"""@brief 线性前驱 revision / Linear predecessor revision."""

branch_labels = None
"""@brief 此迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 此迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal[
    "owner_role",
    "app_role",
    "dashboard_role",
    "migrator_role",
]
"""@brief Alembic 接收的 dbctl role 选项 / dbctl role options accepted by Alembic."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role 标识白名单 / PostgreSQL role identifier allowlist."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL 标识最大字节数 / Maximum PostgreSQL identifier length in bytes."""

_MIGRATION_POLICY = "knowledge_owner_migration_0019"
"""@brief FORCE-RLS 表上的临时 owner policy / Temporary owner policy on FORCE-RLS tables."""

_MIGRATION_AUDIT_ID = "api-v2-knowledge-persistence-0019"
"""@brief 追加式迁移审计标识 / Append-only migration-audit identifier."""

_LEGACY_TABLES = (
    "identity.workspaces",
    "agent.jobs",
    "resume.import_upload_sessions",
    "knowledge.sources",
    "knowledge.source_versions",
    "knowledge.visibility_policies",
    "knowledge.visibility_grants",
    "knowledge.chunks",
)
"""@brief backfill 读取的既有 FORCE-RLS 表 / Existing FORCE-RLS tables read during backfill."""

_KNOWLEDGE_SECURED_TABLES = (
    "knowledge.connections",
    "knowledge.connection_authorization_sessions",
    "knowledge.upload_sessions",
    "knowledge.sources",
    "knowledge.source_versions",
    "knowledge.visibility_policies",
    "knowledge.visibility_grants",
)
"""@brief 0019 收紧的 Workspace 表 / Workspace tables secured by 0019."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回经白名单校验并引用的 role / Return an allowlisted and quoted role.

    @param option dbctl role 配置键 / dbctl role configuration key.
    @return 可安全拼入固定 DDL 的引用 role / Quoted role safe for static DDL.
    @raise RuntimeError 配置缺失或非法时抛出 / Raised for missing or invalid configuration.
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


def _install_migration_visibility(owner_role: str) -> None:
    """@brief 临时允许 schema owner 看见精确 legacy 表集 / Temporarily expose the exact legacy table set to the schema owner.

    @param owner_role 已引用 schema-owner role / Quoted schema-owner role.
    """
    for table in _LEGACY_TABLES:
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )


def _remove_migration_visibility() -> None:
    """@brief 移除临时 owner 可见性 / Remove temporary owner visibility."""
    for table in reversed(_LEGACY_TABLES):
        if table != "resume.import_upload_sessions":
            op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {table}")


def _install_downgrade_visibility(owner_role: str) -> None:
    """@brief 为安全 downgrade 预检临时开放 0019 表 / Temporarily expose 0019 tables for safe downgrade preflight.

    @param owner_role 已引用 schema-owner role / Quoted schema-owner role.
    """
    for table in _KNOWLEDGE_SECURED_TABLES:
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )


def _remove_downgrade_visibility() -> None:
    """@brief 移除 downgrade 临时 owner 可见性 / Remove temporary downgrade owner visibility."""
    for table in reversed(_KNOWLEDGE_SECURED_TABLES):
        op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {table}")


def _count(statement: str) -> int:
    """@brief 执行仅来自本迁移常量的 count / Execute a count supplied only by this migration.

    @param statement 静态 SQL / Static SQL.
    @return 非负 count / Non-negative count.
    """
    value = op.get_bind().scalar(sa.text(statement))
    return int(value or 0)


def _create_connection_tables() -> None:
    """@brief 创建不存 token 的 Connection 与专用授权 receipt 表 / Create token-free Connection and dedicated authorization-receipt tables."""
    op.create_table(
        "connections",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "created_by",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("provider", sa.String(101), nullable=False),
        sa.Column("auth_method", sa.String(16), nullable=False),
        sa.Column("display_name", sa.String(200), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column(
            "scopes",
            postgresql.ARRAY(sa.String(200)),
            nullable=False,
            server_default=sa.text("ARRAY[]::varchar[]"),
        ),
        sa.Column("last_validated_at", sa.DateTime(timezone=True)),
        sa.Column("problem", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("credential_reference", sa.String(160), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "provider ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="ck_connections_knowledge_connections_provider",
        ),
        sa.CheckConstraint(
            "auth_method IN ('oauth', 'device_code', 'api_token')",
            name="ck_connections_knowledge_connections_auth_method",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'reauthorization_required', 'revoking', 'revoked', 'failed')",
            name="ck_connections_knowledge_connections_status",
        ),
        sa.CheckConstraint(
            "display_name = btrim(display_name) AND length(display_name) BETWEEN 1 AND 200",
            name="ck_connections_knowledge_connections_display_name",
        ),
        sa.CheckConstraint(
            "credential_reference ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="ck_connections_knowledge_connections_credential_reference",
        ),
        sa.CheckConstraint(
            "cardinality(scopes) <= 100",
            name="ck_connections_knowledge_connections_scopes",
        ),
        sa.CheckConstraint(
            "(status = 'active' AND problem IS NULL) OR status <> 'active'",
            name="ck_connections_knowledge_connections_active_problem",
        ),
        sa.CheckConstraint(
            "(status = 'failed' AND problem IS NOT NULL) OR status <> 'failed'",
            name="ck_connections_knowledge_connections_failed_problem",
        ),
        sa.UniqueConstraint("id", "workspace_id", name="knowledge_connections_id_workspace"),
        schema="knowledge",
    )
    op.create_index(
        "ix_knowledge_connections_workspace_created_id",
        "connections",
        ["workspace_id", "created_at", "id"],
        schema="knowledge",
    )

    op.create_table(
        "connection_authorization_sessions",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "created_by",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("idempotency_key_hash", sa.String(64), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("provider", sa.String(101), nullable=False),
        sa.Column("flow", sa.String(16), nullable=False),
        sa.Column("launch_key_id", sa.String(64), nullable=False),
        sa.Column("launch_nonce", sa.LargeBinary(), nullable=False),
        sa.Column("launch_ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("idempotency_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "requested_scopes",
            postgresql.ARRAY(sa.String(200)),
            nullable=False,
            server_default=sa.text("ARRAY[]::varchar[]"),
        ),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("state_sha256", sa.String(64), nullable=False),
        sa.Column("provider_session_reference", sa.String(160), nullable=False),
        sa.Column("connection_id", sa.String(160)),
        sa.Column("problem", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "provider ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="ck_connection_authorization_sessions_provider_format",
        ),
        sa.CheckConstraint(
            "flow IN ('browser_redirect', 'device_code')",
            name="ck_connection_authorization_sessions_flow_kind",
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'completed', 'failed', 'expired')",
            name="ck_connection_authorization_sessions_state_kind",
        ),
        sa.CheckConstraint(
            "state_sha256 ~ '^[a-f0-9]{64}$' AND "
            "provider_session_reference ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="ck_connection_authorization_sessions_private_refs",
        ),
        sa.CheckConstraint(
            "cardinality(requested_scopes) <= 100",
            name="ck_connection_authorization_sessions_scope_count",
        ),
        sa.CheckConstraint(
            "idempotency_key_hash ~ '^[a-f0-9]{64}$' "
            "AND request_fingerprint ~ '^[a-f0-9]{64}$' "
            "AND launch_key_id ~ '^[A-Za-z][A-Za-z0-9_.-]{2,63}$' "
            "AND octet_length(launch_nonce) = 12 "
            "AND octet_length(launch_ciphertext) BETWEEN 17 AND 16384",
            name="ck_connection_authorization_sessions_sealed_launch",
        ),
        sa.CheckConstraint(
            "expires_at > created_at AND ("
            "(state = 'completed' AND connection_id IS NOT NULL AND problem IS NULL) OR "
            "(state = 'failed' AND connection_id IS NULL AND problem IS NOT NULL) OR "
            "(state IN ('pending', 'expired') AND connection_id IS NULL AND problem IS NULL))",
            name="ck_connection_authorization_sessions_lifecycle",
        ),
        sa.CheckConstraint(
            "idempotency_expires_at >= created_at + interval '24 hours'",
            name="ck_connection_authorization_sessions_replay_window",
        ),
        sa.ForeignKeyConstraint(
            ["connection_id", "workspace_id"],
            ["knowledge.connections.id", "knowledge.connections.workspace_id"],
            name="fk_connection_authorizations_connection_workspace",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "created_by",
            "idempotency_key_hash",
            name="knowledge_connection_authorizations_actor_key",
        ),
        schema="knowledge",
    )
    op.create_index(
        "ix_knowledge_connection_authorizations_workspace_expiry",
        "connection_authorization_sessions",
        ["workspace_id", "expires_at", "id"],
        schema="knowledge",
    )


def _create_unified_upload_table() -> None:
    """@brief 创建允许诚实 legacy 标记的统一 UploadSession 表 / Create the unified UploadSession table with an honest legacy marker."""
    op.create_table(
        "upload_sessions",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("filename", sa.String(300)),
        sa.Column("media_type", sa.String(200)),
        sa.Column("declared_size_bytes", sa.BigInteger()),
        sa.Column("declared_sha256", sa.String(64)),
        sa.Column("upload_url", sa.Text()),
        sa.Column("required_headers", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completion_size_bytes", sa.BigInteger()),
        sa.Column("completion_sha256", sa.String(64)),
        sa.Column("verification_operation_id", sa.String(80)),
        sa.Column("failure_code", sa.String(101)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("artifact_type", sa.String(101)),
        sa.Column("artifact_id", sa.String(160)),
        sa.Column("artifact_revision", sa.Integer()),
        sa.Column("claimed_by_type", sa.String(101)),
        sa.Column("claimed_by_id", sa.String(160)),
        sa.Column("claimed_by_revision", sa.Integer()),
        sa.Column(
            "claimed_by_job_id",
            sa.String(160),
            sa.ForeignKey(
                "agent.jobs.id",
                ondelete="RESTRICT",
                deferrable=True,
                initially="DEFERRED",
                name="fk_upload_sessions_claimed_job",
            ),
        ),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        sa.Column("legacy_payload", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "status IN ('created', 'uploaded', 'verifying', 'completed', 'failed', 'expired')",
            name="ck_upload_sessions_knowledge_upload_sessions_status",
        ),
        sa.CheckConstraint(
            "revision >= 1 AND expires_at > created_at",
            name="ck_upload_sessions_knowledge_upload_sessions_generation",
        ),
        sa.CheckConstraint(
            "legacy_payload OR (filename IS NOT NULL AND media_type IS NOT NULL "
            "AND declared_size_bytes BETWEEN 1 AND 1073741824 "
            "AND declared_sha256 ~ '^[a-f0-9]{64}$' AND upload_url IS NOT NULL "
            "AND jsonb_typeof(required_headers) = 'object')",
            name="ck_upload_sessions_knowledge_upload_sessions_declaration",
        ),
        sa.CheckConstraint(
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
            name="ck_upload_sessions_knowledge_upload_sessions_lifecycle",
        ),
        sa.CheckConstraint(
            "(claimed_by_type IS NULL AND claimed_by_id IS NULL AND claimed_by_revision IS NULL "
            "AND claimed_by_job_id IS NULL AND consumed_at IS NULL) OR "
            "(status = 'completed' AND claimed_by_type IS NOT NULL AND claimed_by_id IS NOT NULL "
            "AND consumed_at IS NOT NULL AND (claimed_by_revision IS NULL OR claimed_by_revision >= 1) "
            "AND ((claimed_by_type = 'job' AND claimed_by_job_id = claimed_by_id) "
            "OR (claimed_by_type <> 'job' AND claimed_by_job_id IS NULL)))",
            name="ck_upload_sessions_knowledge_upload_sessions_claim",
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="ck_upload_sessions_knowledge_upload_sessions_failure_code",
        ),
        sa.CheckConstraint(
            "artifact_type IS NULL OR artifact_type ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="ck_upload_sessions_knowledge_upload_sessions_artifact_type",
        ),
        sa.CheckConstraint(
            "claimed_by_type IS NULL OR claimed_by_type ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="ck_upload_sessions_knowledge_upload_sessions_claim_type",
        ),
        sa.UniqueConstraint("claimed_by_job_id", name="knowledge_upload_sessions_claimed_job"),
        sa.UniqueConstraint("id", "workspace_id", name="knowledge_upload_sessions_id_workspace"),
        schema="knowledge",
    )
    op.create_index(
        "ix_knowledge_upload_sessions_claimable",
        "upload_sessions",
        ["workspace_id", "expires_at"],
        schema="knowledge",
        postgresql_where=sa.text("status = 'completed' AND claimed_by_id IS NULL"),
    )
    op.create_index(
        "ix_knowledge_upload_sessions_workspace_created_id",
        "upload_sessions",
        ["workspace_id", "created_at", "id"],
        schema="knowledge",
    )


def _migrate_resume_uploads() -> int:
    """@brief 逐字段迁移旧 Resume upload claim 行并删除旧表 / Migrate old Resume upload claims field-for-field and retire the old table.

    @return 迁移行数 / Number of migrated rows.
    @raise RuntimeError 转换后行数或关键状态不一致时抛出 / Raised when row counts or critical state diverge.
    """
    legacy_count = _count("SELECT count(*) FROM resume.import_upload_sessions")
    op.execute(
        """
        INSERT INTO knowledge.upload_sessions (
            id, workspace_id, status, expires_at, completed_at,
            claimed_by_type, claimed_by_id, claimed_by_revision,
            claimed_by_job_id, consumed_at, legacy_payload,
            created_at, updated_at, revision, extensions
        )
        SELECT upload.id,
               upload.workspace_id,
               upload.status,
               upload.expires_at,
               upload.completed_at,
               CASE WHEN upload.claimed_by_job_id IS NULL THEN NULL ELSE 'job' END,
               upload.claimed_by_job_id,
               CASE WHEN upload.claimed_by_job_id IS NULL THEN NULL ELSE job.revision END,
               upload.claimed_by_job_id,
               upload.consumed_at,
               true,
               upload.created_at,
               upload.updated_at,
               upload.revision,
               upload.extensions || jsonb_build_object(
                   '_migration_0019',
                   jsonb_build_object(
                       'source_table', 'resume.import_upload_sessions',
                       'legacy_payload', true
                   )
               )
        FROM resume.import_upload_sessions AS upload
        LEFT JOIN agent.jobs AS job ON job.id = upload.claimed_by_job_id
        """
    )
    migrated_count = _count(
        "SELECT count(*) FROM knowledge.upload_sessions "
        "WHERE extensions #>> '{_migration_0019,source_table}' = "
        "'resume.import_upload_sessions'"
    )
    mismatch_count = _count(
        """
        SELECT count(*)
        FROM resume.import_upload_sessions AS legacy
        LEFT JOIN knowledge.upload_sessions AS unified ON unified.id = legacy.id
        WHERE unified.id IS NULL
           OR unified.workspace_id <> legacy.workspace_id
           OR unified.status <> legacy.status
           OR unified.expires_at <> legacy.expires_at
           OR unified.completed_at IS DISTINCT FROM legacy.completed_at
           OR unified.claimed_by_job_id IS DISTINCT FROM legacy.claimed_by_job_id
           OR unified.consumed_at IS DISTINCT FROM legacy.consumed_at
           OR unified.created_at <> legacy.created_at
           OR unified.updated_at <> legacy.updated_at
           OR unified.revision <> legacy.revision
        """
    )
    if legacy_count != migrated_count or mismatch_count:
        raise RuntimeError("0019 Resume upload conversion did not preserve every legacy row")
    op.drop_table("import_upload_sessions", schema="resume")
    return legacy_count


def _widen_knowledge_ids() -> None:
    """@brief 将 V2-facing Knowledge ID 与引用扩到 160 / Widen V2-facing Knowledge IDs and references to 160."""
    op.alter_column("sources", "id", schema="knowledge", type_=sa.String(160))
    op.alter_column("source_versions", "id", schema="knowledge", type_=sa.String(160))
    op.alter_column("source_versions", "source_id", schema="knowledge", type_=sa.String(160))
    op.alter_column("visibility_policies", "id", schema="knowledge", type_=sa.String(160))
    op.alter_column("visibility_policies", "source_id", schema="knowledge", type_=sa.String(160))
    op.alter_column("visibility_grants", "id", schema="knowledge", type_=sa.String(160))
    op.alter_column("visibility_grants", "policy_id", schema="knowledge", type_=sa.String(160))
    op.alter_column("chunks", "source_version_id", schema="knowledge", type_=sa.String(160))
    op.alter_column("ingestion_jobs", "source_id", schema="knowledge", type_=sa.String(160))
    op.alter_column("ingestion_jobs", "source_version_id", schema="knowledge", type_=sa.String(160))


def _preserve_and_normalize_legacy_roots() -> None:
    """@brief 保留超长/旧枚举值后规范化根字段 / Preserve overlong and legacy enum values before normalizing roots."""
    op.execute(
        """
        UPDATE knowledge.sources
        SET extensions = extensions || jsonb_build_object(
                '_migration_0019',
                COALESCE(extensions -> '_migration_0019', '{}'::jsonb) ||
                jsonb_strip_nulls(jsonb_build_object(
                    'legacy_title', CASE
                        WHEN title <> btrim(title) OR length(title) > 300 OR btrim(title) = ''
                        THEN title ELSE NULL END,
                    'legacy_source_type', source_type,
                    'legacy_ingestion_state', ingestion_state,
                    'legacy_config', config,
                    'legacy_revision_mode', revision_mode,
                    'legacy_sync_schedule', sync_schedule
                ))
            ),
            title = CASE
                WHEN btrim(title) = '' THEN left('Legacy source ' || id, 300)
                ELSE left(btrim(title), 300)
            END,
            source_type = CASE
                WHEN extensions #>> '{runtime,source_type}' IN (
                    'file', 'url', 'website', 'blog_feed', 'git_repository',
                    'manual_note', 'resume', 'cloud_drive'
                ) THEN extensions #>> '{runtime,source_type}'
                ELSE source_type
            END,
            ingestion_state = CASE COALESCE(
                NULLIF(extensions #>> '{runtime,ingestion_status}', ''),
                ingestion_state
            )
                WHEN 'new' THEN 'not_started'
                WHEN 'indexing' THEN 'embedding'
                WHEN 'not_started' THEN 'not_started'
                WHEN 'queued' THEN 'queued'
                WHEN 'fetching' THEN 'fetching'
                WHEN 'parsing' THEN 'parsing'
                WHEN 'chunking' THEN 'chunking'
                WHEN 'embedding' THEN 'embedding'
                WHEN 'ready' THEN 'ready'
                WHEN 'stale' THEN 'stale'
                WHEN 'failed' THEN 'failed'
                WHEN 'deleting' THEN 'deleting'
                WHEN 'deleted' THEN 'deleted'
                ELSE 'not_started'
            END
        """
    )
    op.alter_column("sources", "title", schema="knowledge", type_=sa.String(300))


def _expand_knowledge_tables() -> None:
    """@brief 为根、版本和策略增加 V2 typed state / Add V2 typed state to roots, versions, and policies."""
    op.drop_constraint("knowledge_sources_type", "sources", schema="knowledge", type_="check")
    op.drop_constraint(
        "knowledge_sources_ingestion_state",
        "sources",
        schema="knowledge",
        type_="check",
    )
    _preserve_and_normalize_legacy_roots()

    for column in (
        sa.Column("source_input", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("public_config", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true")),
        sa.Column("connection_id", sa.String(160)),
        sa.Column("upload_session_id", sa.String(160)),
        sa.Column("resume_id", sa.String(160)),
        sa.Column("current_policy_version", sa.Integer()),
        sa.Column("current_version_id", sa.String(160)),
        sa.Column("version_counter", sa.Integer(), server_default=sa.text("0")),
        sa.Column("document_count", sa.Integer(), server_default=sa.text("0")),
        sa.Column("chunk_count", sa.Integer(), server_default=sa.text("0")),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column("last_problem", postgresql.JSONB(astext_type=sa.Text())),
    ):
        op.add_column("sources", column, schema="knowledge")

    for column in (
        sa.Column("content_sha256", sa.String(64)),
        sa.Column("size_bytes", sa.BigInteger()),
        sa.Column("status", sa.String(16)),
        sa.Column("artifact_type", sa.String(101)),
        sa.Column("artifact_id", sa.String(160)),
        sa.Column("artifact_revision", sa.Integer()),
    ):
        op.add_column("source_versions", column, schema="knowledge")

    op.add_column("visibility_grants", sa.Column("ordinal", sa.Integer()), schema="knowledge")


def _backfill_visibility_policies() -> None:
    """@brief 规范化既有 policy 并为缺失来源创建 fail-closed policy / Normalize existing policies and create fail-closed policies for uncovered sources."""
    op.execute(
        """
        UPDATE knowledge.visibility_policies AS policy
        SET extensions = policy.extensions || jsonb_build_object(
                '_migration_0019',
                COALESCE(policy.extensions -> '_migration_0019', '{}'::jsonb) ||
                jsonb_build_object(
                    'legacy_sensitivity', policy.sensitivity,
                    'legacy_allowed_model_regions', to_jsonb(policy.allowed_model_regions),
                    'legacy_retention_days', policy.retention_days
                )
            ),
            sensitivity = CASE
                WHEN policy.sensitivity IN ('normal', 'confidential', 'highly_confidential')
                THEN policy.sensitivity ELSE 'confidential' END,
            allowed_model_regions = CASE
                WHEN cardinality(policy.allowed_model_regions) > 0
                 AND policy.allowed_model_regions <@
                     ARRAY['cn', 'global', 'private_deployment']::varchar[]
                THEN ARRAY(
                    SELECT DISTINCT region
                    FROM unnest(policy.allowed_model_regions) AS region
                    ORDER BY region
                )
                ELSE ARRAY[workspace.data_region]::varchar[]
            END,
            retention_days = CASE
                WHEN policy.retention_days BETWEEN 1 AND 3650 THEN policy.retention_days
                ELSE NULL
            END
        FROM identity.workspaces AS workspace
        WHERE workspace.id = policy.workspace_id
        """
    )
    op.execute(
        """
        INSERT INTO knowledge.visibility_policies (
            id, workspace_id, resource_owner_id, source_id, policy_version,
            default_effect, sensitivity, session_override_allowed,
            allow_external_model_processing, allowed_model_regions, retention_days,
            created_at, updated_at, revision, extensions
        )
        SELECT 'kpol_' || md5('0019:' || source.id),
               source.workspace_id,
               source.resource_owner_id,
               source.id,
               1,
               'deny',
               'confidential',
               false,
               false,
               ARRAY[workspace.data_region]::varchar[],
               NULL,
               source.created_at,
               source.updated_at,
               1,
               jsonb_build_object(
                   '_migration_0019',
                   jsonb_build_object('reason', 'missing_legacy_policy_fail_closed')
               )
        FROM knowledge.sources AS source
        JOIN identity.workspaces AS workspace ON workspace.id = source.workspace_id
        WHERE NOT EXISTS (
            SELECT 1 FROM knowledge.visibility_policies AS policy
            WHERE policy.source_id = source.id
        )
        """
    )

    op.drop_constraint(
        "knowledge_visibility_grants_scope",
        "visibility_grants",
        schema="knowledge",
        type_="unique",
    )
    op.execute(
        """
        WITH ordered AS (
            SELECT id,
                   row_number() OVER (PARTITION BY policy_id ORDER BY created_at, id) - 1 AS ordinal
            FROM knowledge.visibility_grants
        )
        UPDATE knowledge.visibility_grants AS target_grant
        SET ordinal = ordered.ordinal,
            extensions = target_grant.extensions || jsonb_build_object(
                '_migration_0019',
                jsonb_build_object(
                    'legacy_agent_scope', target_grant.agent_scope,
                    'legacy_allowed_operations', to_jsonb(target_grant.allowed_operations)
                )
            ),
            agent_scope = CASE
                WHEN target_grant.agent_scope ~ '^[a-z][a-z0-9_.-]{2,100}$'
                THEN target_grant.agent_scope
                ELSE 'legacy_scope_' || md5(target_grant.id)
            END,
            allowed_operations = CASE
                WHEN cardinality(target_grant.allowed_operations) > 0
                 AND target_grant.allowed_operations <@
                     ARRAY['retrieve', 'quote', 'summarize', 'derive', 'write_back']::varchar[]
                THEN ARRAY(
                    SELECT DISTINCT operation
                    FROM unnest(target_grant.allowed_operations) AS operation
                    ORDER BY operation
                )
                ELSE ARRAY['retrieve']::varchar[]
            END
        FROM ordered
        WHERE ordered.id = target_grant.id
        """
    )
    op.alter_column("visibility_grants", "ordinal", schema="knowledge", nullable=False)
    op.create_unique_constraint(
        "knowledge_visibility_grants_ordinal",
        "visibility_grants",
        ["policy_id", "ordinal"],
        schema="knowledge",
    )

    op.execute(
        """
        UPDATE knowledge.sources AS source
        SET current_policy_version = latest.policy_version
        FROM (
            SELECT source_id, max(policy_version) AS policy_version
            FROM knowledge.visibility_policies
            GROUP BY source_id
        ) AS latest
        WHERE latest.source_id = source.id
        """
    )


def _backfill_version_hashes() -> None:
    """@brief 用真实 SHA-256 确定性规范化 legacy content hash / Deterministically normalize legacy content hashes with real SHA-256."""
    connection = op.get_bind()
    rows = connection.execute(
        sa.text("SELECT id, content_hash FROM knowledge.source_versions ORDER BY id")
    )
    update_statement = sa.text(
        "UPDATE knowledge.source_versions SET content_sha256 = :digest WHERE id = :id"
    )
    valid = re.compile(r"^[a-f0-9]{64}$")
    for row in rows:
        identifier = str(row.id)
        value = str(row.content_hash)
        digest = (
            value
            if valid.fullmatch(value) is not None
            else hashlib.sha256(value.encode()).hexdigest()
        )
        connection.execute(update_statement, {"id": identifier, "digest": digest})


def _backfill_versions() -> None:
    """@brief 把 legacy version 投影为不可变 V2 snapshot / Project legacy versions into immutable V2 snapshots."""
    _backfill_version_hashes()
    op.execute(
        """
        UPDATE knowledge.source_versions
        SET size_bytes = CASE
                WHEN COALESCE(origin ->> 'size_bytes', parser_metadata ->> 'size_bytes', '')
                     ~ '^[0-9]{1,10}$'
                 AND COALESCE(origin ->> 'size_bytes', parser_metadata ->> 'size_bytes')::numeric
                     BETWEEN 0 AND 1073741824
                THEN COALESCE(origin ->> 'size_bytes', parser_metadata ->> 'size_bytes')::bigint
                ELSE 0
            END,
            status = CASE WHEN indexed_at IS NULL THEN 'pending' ELSE 'ready' END,
            artifact_type = 'knowledge_source_version',
            artifact_id = id,
            artifact_revision = revision,
            extensions = extensions || jsonb_build_object(
                '_migration_0019',
                COALESCE(extensions -> '_migration_0019', '{}'::jsonb) ||
                jsonb_build_object(
                    'legacy_content_hash', content_hash,
                    'artifact_basis', 'legacy_source_version_self_reference'
                )
            )
        """
    )


def _create_legacy_file_upload_refs() -> None:
    """@brief 为 V1 file source 创建不可领取的 legacy reference 行 / Create non-claimable legacy reference rows for V1 file sources."""
    op.execute(
        """
        INSERT INTO knowledge.upload_sessions (
            id, workspace_id, status, expires_at, completed_at,
            claimed_by_type, claimed_by_id, claimed_by_revision, consumed_at,
            legacy_payload, created_at, updated_at, revision, extensions
        )
        SELECT 'upload_' || md5('knowledge-file:' || source.id),
               source.workspace_id,
               CASE WHEN current_version.id IS NULL THEN 'expired' ELSE 'completed' END,
               source.created_at + interval '100 years',
               CASE WHEN current_version.id IS NULL THEN NULL ELSE source.updated_at END,
               CASE WHEN current_version.id IS NULL THEN NULL ELSE 'knowledge_source_version' END,
               current_version.id,
               current_version.revision,
               CASE WHEN current_version.id IS NULL THEN NULL ELSE source.updated_at END,
               true,
               source.created_at,
               source.updated_at,
               1,
               jsonb_build_object(
                   '_migration_0019',
                   jsonb_build_object(
                       'source_table', 'knowledge.sources',
                       'source_id', source.id,
                       'reason', 'legacy_file_source_without_upload_declaration'
                   )
               )
        FROM knowledge.sources AS source
        LEFT JOIN LATERAL (
            SELECT version.id, version.revision
            FROM knowledge.source_versions AS version
            WHERE version.source_id = source.id
            ORDER BY version.version_no DESC, version.id DESC
            LIMIT 1
        ) AS current_version ON true
        WHERE source.source_type = 'file'
        ON CONFLICT (id) DO NOTHING
        """
    )


def _backfill_source_inputs_and_state() -> None:
    """@brief 生成公开/私有配置与聚合计数，保留原始 config / Build public/private configs and aggregate counters while retaining original config."""
    _create_legacy_file_upload_refs()
    op.execute(
        """
        WITH version_state AS (
            SELECT source.id AS source_id,
                   version.id AS current_version_id,
                   COALESCE(version.version_no, 0) AS version_counter,
                   version.indexed_at,
                   COALESCE(chunk_count.value, 0) AS chunk_count
            FROM knowledge.sources AS source
            LEFT JOIN LATERAL (
                SELECT item.id, item.version_no, item.indexed_at
                FROM knowledge.source_versions AS item
                WHERE item.source_id = source.id
                ORDER BY item.version_no DESC, item.id DESC
                LIMIT 1
            ) AS version ON true
            LEFT JOIN LATERAL (
                SELECT count(*)::integer AS value
                FROM knowledge.chunks AS chunk
                WHERE chunk.source_version_id = version.id
            ) AS chunk_count ON true
        )
        UPDATE knowledge.sources AS source
        SET enabled = CASE
                WHEN jsonb_typeof(source.extensions #> '{runtime,enabled}') = 'boolean'
                THEN (source.extensions #>> '{runtime,enabled}')::boolean
                ELSE source.ingestion_state NOT IN ('deleting', 'deleted')
            END,
            upload_session_id = CASE WHEN source.source_type = 'file'
                THEN 'upload_' || md5('knowledge-file:' || source.id) ELSE NULL END,
            resume_id = CASE WHEN source.source_type = 'resume'
                THEN COALESCE(NULLIF(source.config ->> 'resume_id', ''), source.id) ELSE NULL END,
            connection_id = NULL,
            source_input = CASE source.source_type
                WHEN 'file' THEN jsonb_build_object(
                    'source_type', 'file',
                    'upload_session_id', 'upload_' || md5('knowledge-file:' || source.id)
                )
                WHEN 'resume' THEN jsonb_build_object(
                    'source_type', 'resume',
                    'resume_id', COALESCE(NULLIF(source.config ->> 'resume_id', ''), source.id)
                )
                WHEN 'manual_note' THEN jsonb_build_object(
                    'source_type', 'manual_note',
                    'content', left(COALESCE(
                        NULLIF(source.config #>> '{content,plain_text}', ''),
                        NULLIF(source.extensions #>> '{runtime,mock_content}', ''),
                        source.title
                    ), 200000)
                )
                WHEN 'git_repository' THEN jsonb_build_object(
                    'source_type', 'git_repository',
                    'clone_url', COALESCE(
                        NULLIF(source.config ->> 'clone_url', ''),
                        NULLIF(source.config ->> 'repository_url', '')
                    ),
                    'ref', COALESCE(source.config -> 'ref', 'null'::jsonb),
                    'include_paths', COALESCE(
                        source.config -> 'include_paths', source.config -> 'include_globs', '[]'::jsonb
                    ),
                    'exclude_paths', COALESCE(
                        source.config -> 'exclude_paths', source.config -> 'exclude_globs', '[]'::jsonb
                    ),
                    'connection_id', 'null'::jsonb
                )
                WHEN 'cloud_drive' THEN jsonb_build_object(
                    'source_type', 'cloud_drive',
                    'connection_id', source.config -> 'connection_id',
                    'remote_id', source.config -> 'remote_id'
                )
                ELSE jsonb_build_object(
                    'source_type', source.source_type,
                    'url', source.config -> 'url'
                )
            END,
            public_config = CASE source.source_type
                WHEN 'file' THEN jsonb_build_object(
                    'filename', left(COALESCE(
                        NULLIF(source.config ->> 'filename', ''), 'legacy-file.bin'
                    ), 300),
                    'media_type', left(COALESCE(
                        NULLIF(source.config ->> 'media_type', ''),
                        NULLIF(source.config ->> 'content_type', ''),
                        'application/octet-stream'
                    ), 200)
                )
                WHEN 'resume' THEN jsonb_build_object(
                    'resume_id', COALESCE(NULLIF(source.config ->> 'resume_id', ''), source.id)
                )
                WHEN 'git_repository' THEN jsonb_strip_nulls(jsonb_build_object(
                    'clone_url', COALESCE(
                        NULLIF(source.config ->> 'clone_url', ''),
                        NULLIF(source.config ->> 'repository_url', '')
                    ),
                    'ref', NULLIF(source.config ->> 'ref', '')
                ))
                WHEN 'url' THEN jsonb_build_object('url', source.config -> 'url')
                WHEN 'website' THEN jsonb_build_object('url', source.config -> 'url')
                WHEN 'blog_feed' THEN jsonb_build_object('url', source.config -> 'url')
                ELSE '{}'::jsonb
            END,
            current_version_id = version_state.current_version_id,
            version_counter = version_state.version_counter,
            document_count = CASE WHEN version_state.current_version_id IS NULL THEN 0 ELSE 1 END,
            chunk_count = version_state.chunk_count,
            last_success_at = CASE WHEN source.ingestion_state = 'ready'
                THEN COALESCE(version_state.indexed_at, source.updated_at) ELSE NULL END,
            last_problem = CASE WHEN source.ingestion_state = 'failed' THEN jsonb_build_object(
                'type_uri', 'https://api.hmalliances.org/problems/knowledge/legacy-ingestion-failed',
                'title', 'Legacy knowledge ingestion failed',
                'status', 500,
                'code', 'knowledge.legacy_ingestion_failed',
                'request_id', 'migration_' || md5(source.id),
                'retryable', true,
                'detail', 'The prior ingestion failed before API V2 migration.',
                'errors', jsonb_build_array(),
                'extensions', jsonb_build_object()
            ) ELSE NULL END
        FROM version_state
        WHERE version_state.source_id = source.id
        """
    )


def _backfill_upload_verification_operations() -> None:
    """@brief 为迁入的活动/终态 upload 生成稳定 saga owner / Generate stable saga owners for migrated active/terminal uploads.

    @note 旧表没有 operation identity；这里使用长度分帧输入的 SHA-256 生成不可碰撞的
        ``prep_`` ID，而不伪造 provider 或客户端数据。/ The legacy tables lacked an operation
        identity; a SHA-256 over length-framed fields supplies a collision-resistant ``prep_`` ID
        without fabricating provider or client data.
    """
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            """
            SELECT workspace_id, id, status
            FROM knowledge.upload_sessions
            WHERE status IN ('verifying', 'completed', 'failed')
              AND verification_operation_id IS NULL
            """
        )
    ).all()
    statement = sa.text(
        """
        UPDATE knowledge.upload_sessions
        SET verification_operation_id = :operation_id
        WHERE workspace_id = :workspace_id AND id = :upload_id
        """
    )
    for workspace_id, upload_id, status in rows:
        framed = b"".join(
            len(value).to_bytes(4, "big") + value
            for value in (
                str(workspace_id).encode(),
                str(upload_id).encode(),
                str(status).encode(),
            )
        )
        connection.execute(
            statement,
            {
                "workspace_id": workspace_id,
                "upload_id": upload_id,
                "operation_id": f"prep_{hashlib.sha256(framed).hexdigest()}",
            },
        )
    # Flush deferred claim-FK events before the constrain phase alters this table.
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    op.execute(
        """
        UPDATE knowledge.sources
        SET enabled = false
        WHERE ingestion_state IN ('deleting', 'deleted')
        """
    )


def _constrain_v2_knowledge() -> None:
    """@brief 在 backfill 验证后安装 V2 relational invariants / Install V2 relational invariants after backfill verification."""
    op.create_check_constraint(
        "ck_upload_sessions_verification_owner",
        "upload_sessions",
        "((status IN ('verifying', 'completed', 'failed') "
        "AND verification_operation_id IS NOT NULL) OR "
        "(status IN ('created', 'uploaded', 'expired') "
        "AND verification_operation_id IS NULL)) "
        "AND (verification_operation_id IS NULL OR "
        "verification_operation_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,79}$')",
        schema="knowledge",
    )
    required_source_columns = (
        "source_input",
        "public_config",
        "enabled",
        "current_policy_version",
        "version_counter",
        "document_count",
        "chunk_count",
    )
    for column in required_source_columns:
        op.alter_column("sources", column, schema="knowledge", nullable=False)
    op.alter_column(
        "sources",
        "ingestion_state",
        schema="knowledge",
        server_default=sa.text("'not_started'"),
    )
    for column in (
        "content_sha256",
        "size_bytes",
        "status",
        "artifact_type",
        "artifact_id",
    ):
        op.alter_column("source_versions", column, schema="knowledge", nullable=False)

    op.create_unique_constraint(
        "knowledge_sources_id_workspace",
        "sources",
        ["id", "workspace_id"],
        schema="knowledge",
    )
    op.create_unique_constraint(
        "knowledge_source_versions_id_workspace",
        "source_versions",
        ["id", "workspace_id"],
        schema="knowledge",
    )
    op.create_unique_constraint(
        "knowledge_source_versions_workspace_source_number",
        "source_versions",
        ["workspace_id", "source_id", "version_no"],
        schema="knowledge",
    )

    source_checks = {
        "knowledge_sources_type": (
            "source_type IN ('file', 'url', 'website', 'blog_feed', 'git_repository', "
            "'manual_note', 'resume', 'cloud_drive')"
        ),
        "knowledge_sources_ingestion_state": (
            "ingestion_state IN ('not_started', 'queued', 'fetching', 'parsing', 'chunking', "
            "'embedding', 'ready', 'stale', 'failed', 'deleting', 'deleted')"
        ),
        "knowledge_sources_name": ("title = btrim(title) AND length(title) BETWEEN 1 AND 300"),
        "knowledge_sources_version_counter": (
            "version_counter >= 0 AND ((version_counter = 0 AND current_version_id IS NULL) "
            "OR (version_counter > 0 AND current_version_id IS NOT NULL))"
        ),
        "knowledge_sources_counters": (
            "document_count >= 0 AND chunk_count >= 0 AND current_policy_version >= 1"
        ),
        "knowledge_sources_problem": (
            "(ingestion_state = 'failed' AND last_problem IS NOT NULL) OR "
            "(ingestion_state <> 'failed' AND last_problem IS NULL)"
        ),
        "knowledge_sources_deletion_state": (
            "ingestion_state NOT IN ('deleting', 'deleted') OR enabled = false"
        ),
        "knowledge_sources_config_objects": (
            "jsonb_typeof(source_input) = 'object' AND jsonb_typeof(public_config) = 'object'"
        ),
        "knowledge_sources_input_refs": (
            "(source_type = 'file' AND upload_session_id IS NOT NULL "
            "AND connection_id IS NULL AND resume_id IS NULL) OR "
            "(source_type = 'resume' AND resume_id IS NOT NULL "
            "AND connection_id IS NULL AND upload_session_id IS NULL) OR "
            "(source_type = 'cloud_drive' AND connection_id IS NOT NULL "
            "AND upload_session_id IS NULL AND resume_id IS NULL) OR "
            "(source_type = 'git_repository' AND upload_session_id IS NULL AND resume_id IS NULL) OR "
            "(source_type IN ('url', 'website', 'blog_feed', 'manual_note') "
            "AND connection_id IS NULL AND upload_session_id IS NULL AND resume_id IS NULL)"
        ),
    }
    for name, condition in source_checks.items():
        op.create_check_constraint(name, "sources", condition, schema="knowledge")

    version_checks = {
        "knowledge_source_versions_content": (
            "version_no >= 1 AND size_bytes BETWEEN 0 AND 1073741824 "
            "AND content_sha256 ~ '^[a-f0-9]{64}$'"
        ),
        "knowledge_source_versions_status": (
            "status IN ('pending', 'indexing', 'ready', 'failed')"
        ),
        "knowledge_source_versions_indexed_at": (
            "(status = 'ready' AND indexed_at IS NOT NULL) OR "
            "(status <> 'ready' AND indexed_at IS NULL)"
        ),
        "knowledge_source_versions_artifact": (
            "artifact_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND artifact_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (artifact_revision IS NULL OR artifact_revision >= 1)"
        ),
    }
    for name, condition in version_checks.items():
        op.create_check_constraint(name, "source_versions", condition, schema="knowledge")

    op.create_check_constraint(
        "knowledge_visibility_sensitivity",
        "visibility_policies",
        "sensitivity IN ('normal', 'confidential', 'highly_confidential')",
        schema="knowledge",
    )
    op.create_check_constraint(
        "knowledge_visibility_v2_policy",
        "visibility_policies",
        "policy_version >= 1 AND cardinality(allowed_model_regions) BETWEEN 1 AND 3 "
        "AND allowed_model_regions <@ ARRAY['cn', 'global', 'private_deployment']::varchar[] "
        "AND (retention_days IS NULL OR retention_days BETWEEN 1 AND 3650)",
        schema="knowledge",
    )
    op.create_check_constraint(
        "knowledge_visibility_grants_v2_shape",
        "visibility_grants",
        "ordinal >= 0 AND agent_scope ~ '^[a-z][a-z0-9_.-]{2,100}$' "
        "AND cardinality(allowed_operations) BETWEEN 1 AND 5 "
        "AND allowed_operations <@ "
        "ARRAY['retrieve', 'quote', 'summarize', 'derive', 'write_back']::varchar[]",
        schema="knowledge",
    )

    op.create_foreign_key(
        "fk_knowledge_sources_connection_workspace",
        "sources",
        "connections",
        ["connection_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="knowledge",
        referent_schema="knowledge",
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_knowledge_sources_upload_workspace",
        "sources",
        "upload_sessions",
        ["upload_session_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="knowledge",
        referent_schema="knowledge",
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_knowledge_sources_current_version_workspace",
        "sources",
        "source_versions",
        ["current_version_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="knowledge",
        referent_schema="knowledge",
        ondelete="RESTRICT",
        deferrable=True,
        initially="DEFERRED",
    )
    op.create_foreign_key(
        "fk_knowledge_source_versions_source_workspace",
        "source_versions",
        "sources",
        ["source_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="knowledge",
        referent_schema="knowledge",
        ondelete="CASCADE",
    )

    op.create_index(
        "ix_knowledge_sources_workspace_created_id",
        "sources",
        ["workspace_id", "created_at", "id"],
        schema="knowledge",
    )
    op.drop_index(
        "ix_source_versions_source_id_version_no",
        table_name="source_versions",
        schema="knowledge",
    )
    op.create_index(
        "ix_knowledge_source_versions_source_number",
        "source_versions",
        ["workspace_id", "source_id", "version_no"],
        schema="knowledge",
    )


def _verify_backfill() -> None:
    """@brief 在安装权限前验证每个 legacy root/version 仍可关联 / Verify every legacy root and version remains associated before securing tables."""
    invalid_sources = _count(
        """
        SELECT count(*) FROM knowledge.sources
        WHERE source_input IS NULL OR public_config IS NULL OR current_policy_version IS NULL
           OR version_counter < 0 OR document_count < 0 OR chunk_count < 0
        """
    )
    invalid_versions = _count(
        """
        SELECT count(*) FROM knowledge.source_versions
        WHERE content_sha256 IS NULL OR size_bytes IS NULL OR status IS NULL
           OR artifact_type IS NULL OR artifact_id IS NULL
        """
    )
    orphan_versions = _count(
        """
        SELECT count(*)
        FROM knowledge.source_versions AS version
        LEFT JOIN knowledge.sources AS source
          ON source.id = version.source_id AND source.workspace_id = version.workspace_id
        WHERE source.id IS NULL
        """
    )
    invalid_upload_operations = _count(
        """
        SELECT count(*) FROM knowledge.upload_sessions
        WHERE (status IN ('verifying', 'completed', 'failed')
               AND verification_operation_id IS NULL)
           OR (status IN ('created', 'uploaded', 'expired')
               AND verification_operation_id IS NOT NULL)
        """
    )
    if invalid_sources or invalid_versions or orphan_versions or invalid_upload_operations:
        raise RuntimeError("0019 Knowledge backfill left invalid or orphaned business rows")


def _secure_knowledge_tables(*, app_role: str, dashboard_role: str, migrator_role: str) -> None:
    """@brief 配置 Workspace RLS 与列级最小写权限 / Configure Workspace RLS and least-privilege column writes.

    @param app_role 应用 role / Application role.
    @param dashboard_role Dashboard role / Dashboard role.
    @param migrator_role 迁移执行 role / Migrator role.
    """
    for table in _KNOWLEDGE_SECURED_TABLES:
        if table in {
            "knowledge.sources",
            "knowledge.source_versions",
            "knowledge.visibility_policies",
            "knowledge.visibility_grants",
        }:
            op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
        else:
            op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
            op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY knowledge_v2_workspace_select ON {table} "
            f"AS PERMISSIVE FOR SELECT TO {app_role} "
            "USING (workspace_id = current_setting('app.workspace_id', true))"
        )
        op.execute(
            f"CREATE POLICY knowledge_v2_workspace_insert ON {table} "
            f"AS PERMISSIVE FOR INSERT TO {app_role} "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )

    updates = {
        "knowledge.connections": (
            "status, scopes, last_validated_at, problem, updated_at, revision"
        ),
        "knowledge.connection_authorization_sessions": (
            "state, connection_id, problem, updated_at, revision"
        ),
        "knowledge.upload_sessions": (
            "status, completion_size_bytes, completion_sha256, verification_operation_id, "
            "failure_code, completed_at, "
            "artifact_type, artifact_id, artifact_revision, claimed_by_type, claimed_by_id, "
            "claimed_by_revision, claimed_by_job_id, consumed_at, updated_at, revision"
        ),
        "knowledge.sources": (
            "title, enabled, current_policy_version, current_version_id, version_counter, "
            "ingestion_state, document_count, chunk_count, last_success_at, last_problem, "
            "updated_at, revision"
        ),
        "knowledge.source_versions": "status, indexed_at, updated_at, revision",
    }
    for table, columns in updates.items():
        op.execute(f"GRANT UPDATE ({columns}) ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY knowledge_v2_workspace_update ON {table} "
            f"AS PERMISSIVE FOR UPDATE TO {app_role} "
            "USING (workspace_id = current_setting('app.workspace_id', true)) "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )


def _write_migration_audit(*, resume_uploads: int, sources: int, versions: int) -> None:
    """@brief 非空转换写入 append-only evidence / Write append-only evidence for a non-empty conversion.

    @param resume_uploads 搬迁 upload 数 / Migrated upload count.
    @param sources 转换 source 数 / Converted source count.
    @param versions 转换 version 数 / Converted version count.
    """
    if not (resume_uploads or sources or versions):
        return
    op.execute(
        sa.text(
            """
            INSERT INTO identity.api_migration_audits (
                id, migration_id, phase, event_type,
                source_api_version, target_api_version, details
            ) VALUES (
                :id, :migration_id, 5, 'completed', 'v1', 'v2',
                CAST(:details AS jsonb)
            )
            """
        ).bindparams(
            id="migration_0019_knowledge_persistence",
            migration_id=_MIGRATION_AUDIT_ID,
            details=(
                '{"resume_upload_rows":'
                f'{resume_uploads},"knowledge_source_rows":{sources},'
                f'"knowledge_version_rows":{versions}}}'
            ),
        )
    )


def upgrade() -> None:
    """@brief 发布 API V2 Knowledge persistence 并保留非空数据 / Publish API V2 Knowledge persistence while preserving non-empty data."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _install_migration_visibility(owner_role)
    source_count = _count("SELECT count(*) FROM knowledge.sources")
    version_count = _count("SELECT count(*) FROM knowledge.source_versions")
    _create_connection_tables()
    _create_unified_upload_table()
    resume_upload_count = _migrate_resume_uploads()
    _widen_knowledge_ids()
    _expand_knowledge_tables()
    _backfill_visibility_policies()
    _backfill_versions()
    _backfill_source_inputs_and_state()
    _backfill_upload_verification_operations()
    _verify_backfill()
    _constrain_v2_knowledge()
    _secure_knowledge_tables(
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    _write_migration_audit(
        resume_uploads=resume_upload_count,
        sources=source_count,
        versions=version_count,
    )
    _remove_migration_visibility()


def _preflight_downgrade() -> None:
    """@brief 仅允许无业务状态回退 / Permit downgrade only without business state."""
    counts = {
        "connections": _count("SELECT count(*) FROM knowledge.connections"),
        "authorization sessions": _count(
            "SELECT count(*) FROM knowledge.connection_authorization_sessions"
        ),
        "upload sessions": _count("SELECT count(*) FROM knowledge.upload_sessions"),
        "knowledge sources": _count("SELECT count(*) FROM knowledge.sources"),
        "knowledge versions": _count("SELECT count(*) FROM knowledge.source_versions"),
        "migration evidence": _count(
            "SELECT count(*) FROM identity.api_migration_audits "
            f"WHERE migration_id = '{_MIGRATION_AUDIT_ID}'"
        ),
    }
    if any(counts.values()):
        populated = ", ".join(name for name, value in counts.items() if value)
        raise RuntimeError(f"cannot downgrade non-empty API V2 Knowledge state: {populated}")


def _restore_empty_resume_upload_table(app_role: str) -> None:
    """@brief 空库 downgrade 时恢复旧 Resume upload 表形状 / Restore the old Resume upload shape for an empty downgrade.

    @param app_role 应用 role / Application role.
    """
    op.create_table(
        "import_upload_sessions",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "claimed_by_job_id",
            sa.String(160),
            sa.ForeignKey(
                "agent.jobs.id",
                ondelete="RESTRICT",
                deferrable=True,
                initially="DEFERRED",
            ),
            unique=True,
        ),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "status IN ('created', 'uploaded', 'verifying', 'completed', 'failed', 'expired')",
            name="resume_import_upload_sessions_status",
        ),
        sa.CheckConstraint(
            "(status = 'completed' AND completed_at IS NOT NULL "
            "AND ((claimed_by_job_id IS NULL AND consumed_at IS NULL) "
            "OR (claimed_by_job_id IS NOT NULL AND consumed_at IS NOT NULL))) "
            "OR (status <> 'completed' AND claimed_by_job_id IS NULL AND consumed_at IS NULL)",
            name="resume_import_upload_sessions_lifecycle",
        ),
        schema="resume",
    )
    op.create_index(
        "ix_resume_import_upload_sessions_claimable",
        "import_upload_sessions",
        ["workspace_id", "expires_at"],
        schema="resume",
        postgresql_where=sa.text("status = 'completed' AND claimed_by_job_id IS NULL"),
    )
    op.execute("ALTER TABLE resume.import_upload_sessions ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE resume.import_upload_sessions FORCE ROW LEVEL SECURITY")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON resume.import_upload_sessions TO {app_role}")
    op.execute(
        f"CREATE POLICY resume_v2_upload_workspace_scope "
        f"ON resume.import_upload_sessions AS PERMISSIVE FOR ALL TO {app_role} "
        "USING (workspace_id = current_setting('app.workspace_id', true)) "
        "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )


def _drop_v2_security(app_role: str) -> None:
    """@brief 删除 0019 policy 并恢复旧 Knowledge 广泛策略 / Drop 0019 policies and restore prior Knowledge policies.

    @param app_role 应用 role / Application role.
    """
    for table in reversed(_KNOWLEDGE_SECURED_TABLES):
        for command in ("update", "insert", "select"):
            op.execute(f"DROP POLICY IF EXISTS knowledge_v2_workspace_{command} ON {table}")
        if table in {
            "knowledge.sources",
            "knowledge.source_versions",
            "knowledge.visibility_policies",
            "knowledge.visibility_grants",
        }:
            op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")
            op.execute(
                f"CREATE POLICY workspace_app_tenant_scope ON {table} "
                f"AS PERMISSIVE FOR ALL TO {app_role} "
                "USING (workspace_id = current_setting('app.workspace_id', true)) "
                "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
            )


def _drop_v2_knowledge_shape() -> None:
    """@brief 空库中移除 V2 Knowledge 扩展列与约束 / Remove V2 Knowledge columns and constraints from an empty database."""
    for constraint in ("fk_knowledge_source_versions_source_workspace",):
        op.drop_constraint(constraint, "source_versions", schema="knowledge", type_="foreignkey")
    for constraint in (
        "fk_knowledge_sources_current_version_workspace",
        "fk_knowledge_sources_upload_workspace",
        "fk_knowledge_sources_connection_workspace",
    ):
        op.drop_constraint(constraint, "sources", schema="knowledge", type_="foreignkey")
    op.drop_index(
        "ix_knowledge_source_versions_source_number",
        table_name="source_versions",
        schema="knowledge",
    )
    op.create_index(
        "ix_source_versions_source_id_version_no",
        "source_versions",
        ["source_id", "version_no"],
        schema="knowledge",
    )
    op.drop_index(
        "ix_knowledge_sources_workspace_created_id",
        table_name="sources",
        schema="knowledge",
    )
    for constraint in (
        "knowledge_source_versions_artifact",
        "knowledge_source_versions_indexed_at",
        "knowledge_source_versions_status",
        "knowledge_source_versions_content",
    ):
        op.drop_constraint(constraint, "source_versions", schema="knowledge", type_="check")
    for constraint in (
        "knowledge_sources_input_refs",
        "knowledge_sources_config_objects",
        "knowledge_sources_deletion_state",
        "knowledge_sources_problem",
        "knowledge_sources_counters",
        "knowledge_sources_version_counter",
        "knowledge_sources_name",
        "knowledge_sources_ingestion_state",
        "knowledge_sources_type",
    ):
        op.drop_constraint(constraint, "sources", schema="knowledge", type_="check")
    op.drop_constraint(
        "knowledge_source_versions_workspace_source_number",
        "source_versions",
        schema="knowledge",
        type_="unique",
    )
    op.drop_constraint(
        "knowledge_source_versions_id_workspace",
        "source_versions",
        schema="knowledge",
        type_="unique",
    )
    op.drop_constraint(
        "knowledge_sources_id_workspace", "sources", schema="knowledge", type_="unique"
    )
    op.drop_constraint(
        "knowledge_visibility_grants_v2_shape",
        "visibility_grants",
        schema="knowledge",
        type_="check",
    )
    op.drop_constraint(
        "knowledge_visibility_v2_policy",
        "visibility_policies",
        schema="knowledge",
        type_="check",
    )
    op.drop_constraint(
        "knowledge_visibility_sensitivity",
        "visibility_policies",
        schema="knowledge",
        type_="check",
    )
    op.drop_constraint(
        "knowledge_visibility_grants_ordinal",
        "visibility_grants",
        schema="knowledge",
        type_="unique",
    )
    op.drop_column("visibility_grants", "ordinal", schema="knowledge")
    op.create_unique_constraint(
        "knowledge_visibility_grants_scope",
        "visibility_grants",
        ["policy_id", "agent_scope"],
        schema="knowledge",
    )
    for column in (
        "artifact_revision",
        "artifact_id",
        "artifact_type",
        "status",
        "size_bytes",
        "content_sha256",
    ):
        op.drop_column("source_versions", column, schema="knowledge")
    for column in (
        "last_problem",
        "last_success_at",
        "chunk_count",
        "document_count",
        "version_counter",
        "current_version_id",
        "current_policy_version",
        "resume_id",
        "upload_session_id",
        "connection_id",
        "enabled",
        "public_config",
        "source_input",
    ):
        op.drop_column("sources", column, schema="knowledge")
    op.alter_column(
        "sources", "ingestion_state", schema="knowledge", server_default=sa.text("'new'")
    )
    op.alter_column("sources", "title", schema="knowledge", type_=sa.String(512))
    op.create_check_constraint(
        "knowledge_sources_type",
        "sources",
        "source_type IN ('resume', 'file', 'url', 'git_repository', 'manual_note')",
        schema="knowledge",
    )
    op.create_check_constraint(
        "knowledge_sources_ingestion_state",
        "sources",
        "ingestion_state IN ('new', 'queued', 'indexing', 'ready', 'stale', 'deleted', 'failed')",
        schema="knowledge",
    )
    op.alter_column("ingestion_jobs", "source_version_id", schema="knowledge", type_=sa.String(128))
    op.alter_column("ingestion_jobs", "source_id", schema="knowledge", type_=sa.String(128))
    op.alter_column("chunks", "source_version_id", schema="knowledge", type_=sa.String(128))
    op.alter_column("visibility_grants", "policy_id", schema="knowledge", type_=sa.String(128))
    op.alter_column("visibility_grants", "id", schema="knowledge", type_=sa.String(128))
    op.alter_column("visibility_policies", "source_id", schema="knowledge", type_=sa.String(128))
    op.alter_column("visibility_policies", "id", schema="knowledge", type_=sa.String(128))
    op.alter_column("source_versions", "source_id", schema="knowledge", type_=sa.String(128))
    op.alter_column("source_versions", "id", schema="knowledge", type_=sa.String(128))
    op.alter_column("sources", "id", schema="knowledge", type_=sa.String(128))


def downgrade() -> None:
    """@brief 只在无业务/审计状态时回退 / Downgrade only without business or audit state."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    _install_downgrade_visibility(owner_role)
    _preflight_downgrade()
    _remove_downgrade_visibility()
    _drop_v2_security(app_role)
    _drop_v2_knowledge_shape()
    op.drop_table("connection_authorization_sessions", schema="knowledge")
    op.drop_table("connections", schema="knowledge")
    op.drop_table("upload_sessions", schema="knowledge")
    _restore_empty_resume_upload_table(app_role)
