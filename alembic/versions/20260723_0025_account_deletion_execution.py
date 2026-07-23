"""@brief 可恢复账户删除、用户 token epoch 与外部擦除清单 / Recoverable account deletion, user token epochs, and external-erasure manifests.

Revision ID: 20260723_0025
Revises: 20260723_0024

账户删除不是一次大而脆弱的 ``DELETE``。本 revision 冻结 Workspace 处置决策、立即撤销
认证能力、为对象存储建立 durable work items，再以 token+revision CAS 完成直接标识符
擦除和共享数据脱钩。稳定 user tombstone 仍属于假名化数据（pseudonymous data），不能被
误称为完全匿名；accepted invitation 与审计引用按保留义务继续指向该 tombstone。
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260723_0025"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0024"
"""@brief 前驱 Knowledge external-security revision / Preceding Knowledge external-security revision."""

branch_labels = None
"""@brief 无分支标签 / No branch labels."""

depends_on = None
"""@brief 无额外依赖 / No additional dependencies."""

_ROLE_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
"""@brief dbctl role identifier 语法 / dbctl role-identifier grammar."""

_DELETION_POLICY = "account_deletion_owner_0025"
"""@brief Workspace 表上的 owner-only 删除策略 / Owner-only deletion policy on Workspace tables."""

_USER_POLICY = "account_deletion_user_owner_0025"
"""@brief 目标用户表上的 owner-only 删除策略 / Owner-only deletion policy on target-user tables."""

_ACTOR_REFERENCE_POLICY = "account_deletion_actor_reference_owner_0025"
"""@brief 已离开 Workspace 的 actor 引用清理策略 / Actor-reference cleanup policy after leaving a Workspace."""

_DELETED_WORKSPACE_SLUG = "deleted-workspace"
"""@brief 不含用户标识且不参与 live 路由唯一性的 tombstone slug / Non-identifying tombstone slug excluded from live routing uniqueness."""

_SLUG_MIGRATION_POLICY = "account_deletion_slug_migration_owner_0025"
"""@brief FORCE RLS 下仅迁移旧 deleted slug 的临时 owner policy / Temporary owner policy for legacy deleted slugs under FORCE RLS."""

_TOKEN_FUNCTIONS = (
    "identity.revoke_user_access_tokens(text,timestamp with time zone)",
    "identity.user_access_tokens_revoked(text,timestamp with time zone)",
)
"""@brief 用户 access-token epoch 窄函数 / Narrow user access-token epoch functions."""

_EXECUTION_FUNCTIONS = (
    "identity.claim_due_account_deletions(text,timestamp with time zone,integer,integer,integer)",
    "identity.claim_account_deletion_erasure_items(text,text,integer,text,timestamp with time zone,integer,integer)",
    "identity.complete_account_deletion_erasure_item(text,text,integer,text,text,text,text)",
    "identity.retry_account_deletion_erasure_item(text,text,integer,text,text,text,text,text,boolean)",
    "identity.account_deletion_external_state(text,text,integer)",
    "identity.release_account_deletion_progress(text,text,integer)",
    "identity.erase_account_for_deletion(text,text,integer,timestamp with time zone)",
    "identity.finalize_account_deletion(text,text,integer,text,text,text,timestamp with time zone)",
)
"""@brief 账户删除执行窄函数 / Narrow account-deletion execution functions."""

_USER_ID_TABLES = (
    "identity.api_v2_idempotency_records",
    "identity.identity_authenticators",
    "identity.identity_login_sessions",
    "identity.identity_browser_sessions",
    "identity.identity_flows",
    "identity.oauth_authorization_codes",
    "identity.oauth_refresh_token_families",
)
"""@brief 通过 ``user_id`` 精确限制的身份表 / Identity tables restricted by ``user_id``."""

_WORKSPACE_TABLES = (
    "identity.workspace_members",
    "identity.workspace_invitations",
    "identity.audit_events",
    "identity.idempotency_records",
    "agent.artifact_contents",
    "agent.artifact_pdf_source_maps",
    "agent.artifacts",
    "agent.conversations",
    "agent.jobs",
    "agent.messages",
    "agent.outbox_events",
    "agent.runs",
    "agent.tool_approvals",
    "agent.workspace_event_sequences",
    "resume.documents",
    "resume.operation_batches",
    "resume.operations",
    "resume.proposal_operations",
    "resume.proposals",
    "resume.render_jobs",
    "resume.revisions",
    "resume.template_versions",
    "interview.realtime_connections",
    "interview.realtime_inputs",
    "interview.report_evidence",
    "interview.reports",
    "interview.scenarios",
    "interview.session_jobs",
    "interview.sessions",
    "interview.transcript_segments",
    "knowledge.access_snapshots",
    "knowledge.chunks",
    "knowledge.citations",
    "knowledge.connection_authorization_sessions",
    "knowledge.connections",
    "knowledge.connection_credentials",
    "knowledge.embedding_spaces",
    "knowledge.embeddings",
    "knowledge.ingestion_jobs",
    "knowledge.source_versions",
    "knowledge.sources",
    "knowledge.connection_provider_sessions",
    "knowledge.upload_quota_reservations",
    "knowledge.upload_sessions",
    "knowledge.visibility_grants",
    "knowledge.visibility_policies",
)
"""@brief 擦除函数按精确 Workspace GUC 访问的表 / Tables accessed by the eraser under an exact Workspace GUC."""


def _configured_role(option: str) -> str:
    """@brief 读取并验证 dbctl role / Read and validate one dbctl role.

    @param option Alembic main-option 名 / Alembic main-option name.
    @return 可安全插入 DDL 的 role / Role safe for DDL interpolation.
    @raise RuntimeError role 缺失或非法时抛出 / Raised for a missing or invalid role.
    """

    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic configuration is unavailable")
    value = configuration.get_main_option(f"aiws.{option}")
    if value is None or _ROLE_PATTERN.fullmatch(value) is None:
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return value


def _preflight() -> None:
    """@brief 在 DDL 前拒绝无法诚实推断的历史删除状态 / Reject non-inferable historical deletion states before DDL.

    @return 无返回值 / No return value.
    @raise RuntimeError 发现旧 worker 声称 running/completed 或状态错配时抛出 / Raised when
        an old worker claims running/completed work or user state is inconsistent.
    """

    bind = op.get_bind()
    historical_execution = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM identity.account_deletion_requests "
                "WHERE status IN ('running', 'completed', 'failed')"
            )
        ).scalar_one()
    )
    inconsistent = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM identity.account_deletion_requests AS request "
                "JOIN identity.users AS users ON users.id = request.user_id "
                "WHERE (request.status = 'scheduled' "
                "AND users.account_status <> 'deletion_scheduled') "
                "OR (request.status = 'completed' AND users.account_status <> 'deleted')"
            )
        ).scalar_one()
    )
    if historical_execution:
        raise RuntimeError(
            "0025 cannot infer ownership or erasure evidence for historical account deletion "
            "execution states; resolve them before migration"
        )
    if inconsistent:
        raise RuntimeError("0025 found account deletion requests inconsistent with user state")


def _scope_workspace_slug_uniqueness_to_live_rows(owner_role: str) -> None:
    """@brief 将 slug 唯一性限定到 live Workspace 并迁移旧 tombstone / Scope slug uniqueness to live Workspaces and migrate old tombstones.

    @param owner_role 执行迁移的非登录 owner role / Non-login owner role running migrations.
    @return 无返回值 / No return value.

    @note 已删除 Workspace 不再参与路由，因此继续为其占用全局 slug 命名空间既泄漏旧标识，
        又会让用户可预占的 ``deleted-*`` 名称阻塞账户擦除。DDL 与数据更新位于同一迁移事务，
        不会暴露无唯一索引的中间状态。/ Deleted Workspaces are no longer routable, so keeping
        them in the global slug namespace both leaks an old identifier and lets a user-controlled
        ``deleted-*`` slug block erasure. DDL and normalization share one migration transaction.
    """

    op.drop_index("uq_workspaces_slug", table_name="workspaces", schema="identity")
    op.execute(
        f"CREATE POLICY {_SLUG_MIGRATION_POLICY} ON identity.workspaces "
        f"AS PERMISSIVE FOR ALL TO {owner_role} "
        "USING (deleted_at IS NOT NULL) WITH CHECK (deleted_at IS NOT NULL)"
    )
    op.execute(
        sa.text(
            "UPDATE identity.workspaces "
            "SET slug = :tombstone_slug, "
            "updated_at = GREATEST(updated_at, statement_timestamp()), "
            "revision = revision + 1 "
            "WHERE deleted_at IS NOT NULL AND slug <> :tombstone_slug"
        ).bindparams(tombstone_slug=_DELETED_WORKSPACE_SLUG)
    )
    op.execute(f"DROP POLICY {_SLUG_MIGRATION_POLICY} ON identity.workspaces")
    op.create_index(
        "uq_workspaces_slug",
        "workspaces",
        ["slug"],
        unique=True,
        schema="identity",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )


def _restore_global_workspace_slug_uniqueness(owner_role: str) -> None:
    """@brief 在已证明 slug 全局唯一后恢复旧索引 / Restore the legacy index after proving global slug uniqueness.

    @param owner_role 执行迁移的非登录 owner role / Non-login owner role running migrations.
    @return 无返回值 / No return value.
    @raise RuntimeError deleted tombstone 已产生重复时拒绝有损回退 / Raised when duplicate
        deleted tombstones make the legacy global invariant impossible.
    """

    op.execute(
        f"CREATE POLICY {_SLUG_MIGRATION_POLICY} ON identity.workspaces "
        f"AS PERMISSIVE FOR SELECT TO {owner_role} USING (true)"
    )
    duplicate_slugs = bool(
        op.get_bind().execute(
            sa.text(
                "SELECT EXISTS ("
                "SELECT workspace.slug FROM identity.workspaces AS workspace "
                "GROUP BY workspace.slug HAVING count(*) > 1)"
            )
        ).scalar_one()
    )
    if duplicate_slugs:
        raise RuntimeError(
            "cannot restore global Workspace slug uniqueness after deleted tombstones overlap"
        )
    op.execute(f"DROP POLICY {_SLUG_MIGRATION_POLICY} ON identity.workspaces")
    op.drop_index("uq_workspaces_slug", table_name="workspaces", schema="identity")
    op.create_index(
        "uq_workspaces_slug",
        "workspaces",
        ["slug"],
        unique=True,
        schema="identity",
    )


def _expand_requests() -> None:
    """@brief 为 request 添加租约、尝试与证据字段 / Add lease, attempt, and evidence fields to requests.

    @return 无返回值 / No return value.
    """

    op.drop_constraint(
        "account_deletion_requests_state",
        "account_deletion_requests",
        schema="identity",
        type_="check",
    )
    op.add_column(
        "account_deletion_requests",
        sa.Column("started_at", sa.DateTime(timezone=True)),
        schema="identity",
    )
    op.add_column(
        "account_deletion_requests",
        sa.Column("claim_token_hash", sa.String(64)),
        schema="identity",
    )
    op.add_column(
        "account_deletion_requests",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        schema="identity",
    )
    op.add_column(
        "account_deletion_requests",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        schema="identity",
    )
    op.add_column(
        "account_deletion_requests",
        sa.Column("erasure_evidence", postgresql.JSONB(astext_type=sa.Text())),
        schema="identity",
    )
    op.create_check_constraint(
        "account_deletion_requests_execution",
        "account_deletion_requests",
        "attempt_count BETWEEN 0 AND 100 AND ("
        "(status = 'scheduled' AND started_at IS NULL AND completed_at IS NULL "
        "AND problem IS NULL AND claim_token_hash IS NULL AND lease_expires_at IS NULL "
        "AND erasure_evidence IS NULL) OR "
        "(status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL "
        "AND problem IS NULL AND claim_token_hash ~ '^[a-f0-9]{64}$' "
        "AND lease_expires_at > started_at AND erasure_evidence IS NULL) OR "
        "(status = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL "
        "AND completed_at >= started_at AND problem IS NULL AND claim_token_hash IS NULL "
        "AND lease_expires_at IS NULL AND jsonb_typeof(erasure_evidence) = 'object') OR "
        "(status = 'cancelled' AND started_at IS NULL AND completed_at IS NULL "
        "AND problem IS NULL AND claim_token_hash IS NULL AND lease_expires_at IS NULL "
        "AND erasure_evidence IS NULL) OR "
        "(status = 'failed' AND started_at IS NOT NULL AND completed_at IS NULL "
        "AND problem IS NOT NULL AND claim_token_hash IS NULL AND lease_expires_at IS NULL "
        "AND erasure_evidence IS NULL))",
        schema="identity",
    )
    op.create_index(
        "ix_account_deletion_requests_due_execution",
        "account_deletion_requests",
        ["scheduled_for", "lease_expires_at", "created_at", "id"],
        schema="identity",
        postgresql_where=sa.text("status IN ('scheduled', 'running')"),
    )


def _create_execution_tables() -> None:
    """@brief 创建 token epoch、Workspace 决策与外部 work items / Create token epochs, Workspace decisions, and external work items.

    @return 无返回值 / No return value.
    """

    op.create_table(
        "oauth_user_token_revocations",
        sa.Column(
            "user_id",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("revoked_before", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        schema="identity",
    )
    op.create_table(
        "account_deletion_workspace_dispositions",
        sa.Column(
            "request_id",
            sa.String(128),
            sa.ForeignKey("identity.account_deletion_requests.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("disposition", sa.String(16), nullable=False),
        sa.Column(
            "decided_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "disposition IN ('personal', 'shared')",
            name="account_deletion_workspace_disposition_values",
        ),
        schema="identity",
    )
    op.create_table(
        "account_deletion_erasure_items",
        sa.Column(
            "request_id",
            sa.String(128),
            sa.ForeignKey("identity.account_deletion_requests.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("resource_kind", sa.String(32), primary_key=True),
        sa.Column("resource_id", sa.String(160), primary_key=True),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("lease_token_hash", sa.String(64)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("last_error_code", sa.String(101)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "resource_kind IN ('upload_object', 'credential_scope') "
            "AND resource_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
            name="account_deletion_erasure_item_resource",
        ),
        sa.CheckConstraint(
            "attempt_count BETWEEN 0 AND 100 AND ("
            "(status = 'pending' AND lease_token_hash IS NULL AND lease_expires_at IS NULL) OR "
            "(status = 'processing' AND lease_token_hash ~ '^[a-f0-9]{64}$' "
            "AND lease_expires_at IS NOT NULL) OR "
            "(status IN ('completed', 'failed') AND lease_token_hash IS NULL "
            "AND lease_expires_at IS NULL))",
            name="account_deletion_erasure_item_lifecycle",
        ),
        sa.CheckConstraint(
            "last_error_code IS NULL OR last_error_code ~ '^[a-z][a-z0-9_.-]{2,100}$'",
            name="account_deletion_erasure_item_error",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_account_deletion_erasure_items_due",
        "account_deletion_erasure_items",
        ["status", "lease_expires_at", "created_at", "resource_id"],
        schema="identity",
        postgresql_where=sa.text("status IN ('pending', 'processing')"),
    )


def _install_owner_policies(owner_role: str) -> None:
    """@brief 为 owner 函数安装精确 runtime RLS 策略 / Install exact runtime RLS policies for owner functions.

    @param owner_role 无登录 table/function owner / Non-login table/function owner.
    @return 无返回值 / No return value.
    """

    for table in (
        "identity.account_deletion_requests",
        "identity.oauth_user_token_revocations",
        "identity.account_deletion_workspace_dispositions",
        "identity.account_deletion_erasure_items",
    ):
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY {_DELETION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )
    for table in _WORKSPACE_TABLES:
        op.execute(
            f"CREATE POLICY {_DELETION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} "
            "USING (workspace_id = current_setting('app.deletion_workspace_id', true)) "
            "WITH CHECK (workspace_id = current_setting('app.deletion_workspace_id', true))"
        )
    op.execute(
        f"CREATE POLICY {_DELETION_POLICY} ON identity.workspaces AS PERMISSIVE FOR ALL "
        f"TO {owner_role} USING (id = current_setting('app.deletion_workspace_id', true)) "
        "WITH CHECK (id = current_setting('app.deletion_workspace_id', true))"
    )
    op.execute(
        f"CREATE POLICY {_USER_POLICY} ON identity.users AS PERMISSIVE FOR ALL "
        f"TO {owner_role} USING (id = current_setting('app.deletion_user_id', true)) "
        "WITH CHECK (id = current_setting('app.deletion_user_id', true))"
    )
    for table in _USER_ID_TABLES:
        op.execute(
            f"CREATE POLICY {_USER_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (user_id = current_setting('app.deletion_user_id', true)) "
            "WITH CHECK (user_id = current_setting('app.deletion_user_id', true))"
        )
    op.execute(
        f"CREATE POLICY {_USER_POLICY} ON identity.workspace_members "
        f"AS PERMISSIVE FOR ALL TO {owner_role} "
        "USING (user_id = current_setting('app.deletion_user_id', true)) "
        "WITH CHECK (user_id = current_setting('app.deletion_user_id', true))"
    )
    op.execute(
        f"CREATE POLICY {_ACTOR_REFERENCE_POLICY} ON identity.workspace_members "
        f"AS PERMISSIVE FOR UPDATE TO {owner_role} "
        "USING (invited_by_actor_id = current_setting('app.deletion_user_id', true)) "
        "WITH CHECK (true)"
    )
    op.execute(
        f"CREATE POLICY {_ACTOR_REFERENCE_POLICY} ON identity.workspace_invitations "
        f"AS PERMISSIVE FOR UPDATE TO {owner_role} "
        "USING (invited_by_actor_id = current_setting('app.deletion_user_id', true) "
        "OR accepted_by_user_id = current_setting('app.deletion_user_id', true)) "
        "WITH CHECK (true)"
    )
    op.execute(
        f"CREATE POLICY {_ACTOR_REFERENCE_POLICY} ON identity.audit_events "
        f"AS PERMISSIVE FOR UPDATE TO {owner_role} "
        "USING (actor_id = current_setting('app.deletion_user_id', true)) "
        "WITH CHECK (actor_id = current_setting('app.deletion_user_id', true))"
    )
    op.execute(
        f"CREATE POLICY {_ACTOR_REFERENCE_POLICY} ON identity.idempotency_records "
        f"AS PERMISSIVE FOR DELETE TO {owner_role} "
        "USING (actor_id = current_setting('app.deletion_user_id', true))"
    )
    for table in (
        "knowledge.connections",
        "knowledge.connection_authorization_sessions",
        "knowledge.connection_credentials",
        "knowledge.connection_provider_sessions",
    ):
        op.execute(
            f"CREATE POLICY {_ACTOR_REFERENCE_POLICY} ON {table} "
            f"AS PERMISSIVE FOR ALL TO {owner_role} "
            "USING (created_by = current_setting('app.deletion_user_id', true)) "
            "WITH CHECK (created_by = current_setting('app.deletion_user_id', true))"
        )


def _create_token_functions() -> None:
    """@brief 创建用户级 access-token epoch 函数 / Create user-level access-token epoch functions.

    @return 无返回值 / No return value.
    """

    op.execute(
        """
        CREATE FUNCTION identity.revoke_user_access_tokens(
            candidate_user_id text,
            candidate_revoked_before timestamp with time zone
        ) RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            effective_revoked_before timestamp with time zone;
        BEGIN
            IF candidate_user_id IS NULL
               OR candidate_user_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,127}$'
               OR candidate_revoked_before IS NULL THEN
                RAISE EXCEPTION 'invalid user access-token revocation arguments'
                    USING ERRCODE = '22023';
            END IF;
            effective_revoked_before := LEAST(
                candidate_revoked_before,
                statement_timestamp()
            );
            PERFORM set_config('app.deletion_user_id', candidate_user_id, true);
            IF NOT EXISTS (
                SELECT 1 FROM identity.users AS target_user
                WHERE target_user.id = candidate_user_id
            ) THEN
                RETURN false;
            END IF;
            INSERT INTO identity.oauth_user_token_revocations (
                user_id, revoked_before, updated_at
            ) VALUES (
                candidate_user_id, effective_revoked_before, statement_timestamp()
            )
            ON CONFLICT (user_id) DO UPDATE
            SET revoked_before = GREATEST(
                    identity.oauth_user_token_revocations.revoked_before,
                    EXCLUDED.revoked_before
                ),
                updated_at = statement_timestamp();
            RETURN true;
        END
        $function$
        """
    )
    op.execute(
        """
        CREATE FUNCTION identity.user_access_tokens_revoked(
            candidate_user_id text,
            candidate_issued_at timestamp with time zone
        ) RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        BEGIN
            IF candidate_user_id IS NULL
               OR candidate_user_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,127}$'
               OR candidate_issued_at IS NULL THEN
                RETURN false;
            END IF;
            RETURN EXISTS (
                SELECT 1
                FROM identity.oauth_user_token_revocations AS revocation
                WHERE revocation.user_id = candidate_user_id
                  AND candidate_issued_at <= revocation.revoked_before
            );
        END
        $function$
        """
    )


def _create_claim_function() -> None:
    """@brief 创建冻结处置并立即撤销认证的 claim 函数 / Create the claim function that freezes disposition and revokes authentication.

    @return 无返回值 / No return value.
    """

    op.execute(
        """
        CREATE FUNCTION identity.claim_due_account_deletions(
            candidate_claim_token_hash text,
            candidate_now timestamp with time zone,
            candidate_lease_seconds integer,
            batch_limit integer,
            maximum_attempts integer
        ) RETURNS TABLE (
            request_id text,
            user_id text,
            expected_revision integer,
            claimed_at timestamp with time zone,
            lease_expires_at timestamp with time zone
        )
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity, knowledge
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
            candidate record;
            disposition record;
            claimed_revision integer;
        BEGIN
            IF candidate_claim_token_hash IS NULL
               OR candidate_claim_token_hash !~ '^[a-f0-9]{64}$'
               OR candidate_now IS NULL
               OR candidate_lease_seconds NOT BETWEEN 60 AND 3600
               OR batch_limit NOT BETWEEN 1 AND 100
               OR maximum_attempts NOT BETWEEN 1 AND 100 THEN
                RAISE EXCEPTION 'invalid account deletion claim arguments'
                    USING ERRCODE = '22023';
            END IF;
            -- Lease ownership and due-ness are database-time facts.  A stale or
            -- malicious application clock must never extend an expired claim.
            effective_now := statement_timestamp();

            UPDATE identity.account_deletion_requests AS exhausted
            SET status = 'failed',
                started_at = COALESCE(exhausted.started_at, effective_now),
                claim_token_hash = NULL,
                lease_expires_at = NULL,
                problem = jsonb_build_object(
                    'code', 'account_deletion.retry_exhausted',
                    'detail', 'Account deletion requires operator intervention.'
                ),
                updated_at = effective_now,
                revision = exhausted.revision + 1
            WHERE exhausted.status = 'running'
              AND exhausted.lease_expires_at <= effective_now
              AND exhausted.attempt_count >= maximum_attempts;

            FOR candidate IN
                SELECT request.id AS request_id, request.user_id
                FROM identity.account_deletion_requests AS request
                WHERE request.attempt_count < maximum_attempts
                  AND (
                    (request.status = 'scheduled'
                     AND request.scheduled_for <= effective_now)
                    OR (request.status = 'running'
                        AND request.lease_expires_at <= effective_now)
                  )
                ORDER BY COALESCE(request.lease_expires_at, request.scheduled_for),
                         request.created_at,
                         request.id
                LIMIT batch_limit
                FOR UPDATE SKIP LOCKED
            LOOP
                PERFORM set_config('app.deletion_user_id', candidate.user_id, true);
                PERFORM 1
                FROM identity.users AS target_user
                WHERE target_user.id = candidate.user_id
                FOR UPDATE;
                IF NOT FOUND THEN
                    CONTINUE;
                END IF;

                IF NOT EXISTS (
                    SELECT 1
                    FROM identity.account_deletion_workspace_dispositions AS frozen
                    WHERE frozen.request_id = candidate.request_id
                ) THEN
                    FOR disposition IN
                        SELECT member.workspace_id
                        FROM identity.workspace_members AS member
                        WHERE member.user_id = candidate.user_id
                          AND member.status = 'active'
                        ORDER BY member.workspace_id
                    LOOP
                        PERFORM set_config(
                            'app.deletion_workspace_id', disposition.workspace_id, true
                        );
                        PERFORM 1
                        FROM identity.workspaces AS workspace
                        WHERE workspace.id = disposition.workspace_id
                          AND workspace.deleted_at IS NULL
                        FOR UPDATE;
                        IF NOT FOUND THEN
                            CONTINUE;
                        END IF;
                        INSERT INTO identity.account_deletion_workspace_dispositions (
                            request_id, workspace_id, disposition, decided_at
                        ) VALUES (
                            candidate.request_id,
                            disposition.workspace_id,
                            CASE WHEN EXISTS (
                                SELECT 1
                                FROM identity.workspace_members AS collaborator
                                WHERE collaborator.workspace_id = disposition.workspace_id
                                  AND collaborator.user_id <> candidate.user_id
                                  AND collaborator.status = 'active'
                            ) THEN 'shared' ELSE 'personal' END,
                            effective_now
                        )
                        ON CONFLICT ON CONSTRAINT
                            account_deletion_workspace_dispositions_pkey DO NOTHING;
                    END LOOP;
                END IF;

                FOR disposition IN
                    SELECT frozen.workspace_id, frozen.disposition
                    FROM identity.account_deletion_workspace_dispositions AS frozen
                    WHERE frozen.request_id = candidate.request_id
                    ORDER BY frozen.workspace_id
                LOOP
                    PERFORM set_config(
                        'app.deletion_workspace_id', disposition.workspace_id, true
                    );
                    UPDATE identity.workspace_members AS member
                    SET status = 'suspended',
                        display_name = 'Deleted user',
                        invited_by_actor_id = NULL,
                        updated_at = effective_now,
                        revision = member.revision + 1,
                        extensions = '{}'::jsonb
                    WHERE member.workspace_id = disposition.workspace_id
                      AND member.user_id = candidate.user_id
                      AND (
                        member.status <> 'suspended'
                        OR member.display_name <> 'Deleted user'
                        OR member.invited_by_actor_id IS NOT NULL
                        OR member.extensions <> '{}'::jsonb
                      );
                    IF disposition.disposition = 'personal' THEN
                        UPDATE identity.workspaces AS workspace
                        SET deleted_at = COALESCE(workspace.deleted_at, effective_now),
                            updated_at = effective_now,
                            revision = workspace.revision + 1
                        WHERE workspace.id = disposition.workspace_id
                          AND workspace.deleted_at IS NULL;
                        INSERT INTO identity.account_deletion_erasure_items (
                            request_id, workspace_id, resource_kind, resource_id,
                            status, attempt_count, created_at, updated_at
                        )
                        SELECT candidate.request_id,
                               disposition.workspace_id,
                               'upload_object',
                               upload.id,
                               'pending',
                               0,
                               effective_now,
                               effective_now
                        FROM knowledge.upload_sessions AS upload
                        WHERE upload.workspace_id = disposition.workspace_id
                        ON CONFLICT ON CONSTRAINT
                            account_deletion_erasure_items_pkey DO NOTHING;
                    END IF;
                    INSERT INTO identity.account_deletion_erasure_items (
                        request_id, workspace_id, resource_kind, resource_id,
                        status, attempt_count, created_at, updated_at
                    ) VALUES (
                        candidate.request_id,
                        disposition.workspace_id,
                        'credential_scope',
                        candidate.user_id,
                        'pending',
                        0,
                        effective_now,
                        effective_now
                    )
                    ON CONFLICT ON CONSTRAINT
                        account_deletion_erasure_items_pkey DO NOTHING;
                END LOOP;

                FOR disposition IN
                    SELECT connection.workspace_id
                    FROM knowledge.connections AS connection
                    WHERE connection.created_by = candidate.user_id
                    UNION
                    SELECT authorization_session.workspace_id
                    FROM knowledge.connection_authorization_sessions AS authorization_session
                    WHERE authorization_session.created_by = candidate.user_id
                    UNION
                    SELECT credential.workspace_id
                    FROM knowledge.connection_credentials AS credential
                    WHERE credential.created_by = candidate.user_id
                    UNION
                    SELECT provider_session.workspace_id
                    FROM knowledge.connection_provider_sessions AS provider_session
                    WHERE provider_session.created_by = candidate.user_id
                LOOP
                    INSERT INTO identity.account_deletion_erasure_items (
                        request_id, workspace_id, resource_kind, resource_id,
                        status, attempt_count, created_at, updated_at
                    ) VALUES (
                        candidate.request_id,
                        disposition.workspace_id,
                        'credential_scope',
                        candidate.user_id,
                        'pending',
                        0,
                        effective_now,
                        effective_now
                    )
                    ON CONFLICT ON CONSTRAINT
                        account_deletion_erasure_items_pkey DO NOTHING;
                END LOOP;

                PERFORM set_config('app.deletion_user_id', candidate.user_id, true);
                UPDATE identity.identity_login_sessions AS login_session
                SET revoked_at = COALESCE(login_session.revoked_at, effective_now)
                WHERE login_session.user_id = candidate.user_id
                  AND login_session.revoked_at IS NULL;
                UPDATE identity.oauth_authorization_codes AS authorization_code
                SET consumed_at = COALESCE(authorization_code.consumed_at, effective_now),
                    subject = 'deleted:' || candidate.user_id
                WHERE authorization_code.user_id = candidate.user_id;
                UPDATE identity.oauth_refresh_token_families AS token_family
                SET revoked_at = COALESCE(token_family.revoked_at, effective_now),
                    subject = 'deleted:' || candidate.user_id
                WHERE token_family.user_id = candidate.user_id;
                DELETE FROM identity.identity_authenticators AS authenticator
                WHERE authenticator.user_id = candidate.user_id;
                UPDATE identity.identity_flows AS identity_flow
                SET internal_state = '{}'::jsonb,
                    webauthn_options = NULL,
                    authorization_resume_uri = NULL
                WHERE identity_flow.user_id = candidate.user_id;
                UPDATE identity.identity_browser_sessions AS browser_session
                SET browser_secret_hash = repeat('0', 64),
                    csrf_token_hash = repeat('0', 64),
                    last_seen_at = effective_now,
                    expires_at = LEAST(browser_session.expires_at, effective_now)
                WHERE browser_session.user_id = candidate.user_id;
                UPDATE identity.users AS target_user
                SET account_status = CASE
                        WHEN target_user.account_status = 'deleted' THEN 'deleted'
                        ELSE 'suspended'
                    END,
                    default_workspace_id = NULL,
                    updated_at = effective_now,
                    revision = target_user.revision + 1
                WHERE target_user.id = candidate.user_id;
                PERFORM identity.revoke_user_access_tokens(candidate.user_id, effective_now);

                UPDATE identity.account_deletion_requests AS request
                SET status = 'running',
                    started_at = COALESCE(request.started_at, effective_now),
                    completed_at = NULL,
                    problem = NULL,
                    claim_token_hash = candidate_claim_token_hash,
                    lease_expires_at = effective_now
                        + make_interval(secs => candidate_lease_seconds),
                    attempt_count = request.attempt_count + 1,
                    updated_at = effective_now,
                    revision = request.revision + 1
                WHERE request.id = candidate.request_id
                RETURNING request.revision INTO claimed_revision;

                request_id := candidate.request_id;
                user_id := candidate.user_id;
                expected_revision := claimed_revision;
                claimed_at := effective_now;
                lease_expires_at := effective_now
                    + make_interval(secs => candidate_lease_seconds);
                RETURN NEXT;
            END LOOP;
        END
        $function$
        """
    )


def _create_erasure_item_functions() -> None:
    """@brief 创建外部擦除 work-item 租约协议 / Create the external-erasure work-item lease protocol.

    @return 无返回值 / No return value.
    """

    op.execute(
        """
        CREATE FUNCTION identity.claim_account_deletion_erasure_items(
            candidate_request_id text,
            candidate_account_claim_hash text,
            candidate_revision integer,
            candidate_item_lease_hash text,
            candidate_now timestamp with time zone,
            candidate_lease_seconds integer,
            batch_limit integer
        ) RETURNS TABLE (
            workspace_id text,
            resource_kind text,
            resource_id text,
            item_attempt integer
        )
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
            account_lease_expires_at timestamp with time zone;
        BEGIN
            IF candidate_request_id IS NULL
               OR candidate_request_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,127}$'
               OR candidate_account_claim_hash !~ '^[a-f0-9]{64}$'
               OR candidate_revision < 2
               OR candidate_item_lease_hash !~ '^[a-f0-9]{64}$'
               OR candidate_now IS NULL
               OR candidate_lease_seconds NOT BETWEEN 30 AND 600
               OR batch_limit NOT BETWEEN 1 AND 100 THEN
                RAISE EXCEPTION 'invalid account deletion erasure-item claim arguments'
                    USING ERRCODE = '22023';
            END IF;
            -- Use the database clock for both takeover and the newly issued
            -- item lease.  Using LEAST(client_now, database_now) here allowed a
            -- caller with an old timestamp to operate under an expired account
            -- lease and immediately issue an already-expired item lease.
            effective_now := statement_timestamp();
            SELECT request.lease_expires_at
            INTO account_lease_expires_at
            FROM identity.account_deletion_requests AS request
            WHERE request.id = candidate_request_id
              AND request.status = 'running'
              AND request.claim_token_hash = candidate_account_claim_hash
              AND request.revision = candidate_revision
              AND request.lease_expires_at > effective_now
            FOR UPDATE;
            IF NOT FOUND THEN
                RETURN;
            END IF;

            UPDATE identity.account_deletion_erasure_items AS exhausted
            SET status = 'failed',
                lease_token_hash = NULL,
                lease_expires_at = NULL,
                last_error_code = 'account_deletion.retry_exhausted',
                updated_at = effective_now
            WHERE exhausted.request_id = candidate_request_id
              AND exhausted.status IN ('pending', 'processing')
              AND exhausted.attempt_count >= 100;

            RETURN QUERY
            WITH candidates AS MATERIALIZED (
                SELECT item.request_id,
                       item.workspace_id,
                       item.resource_kind,
                       item.resource_id
                FROM identity.account_deletion_erasure_items AS item
                WHERE item.request_id = candidate_request_id
                  AND item.attempt_count < 100
                  AND (
                    item.status = 'pending'
                    OR (item.status = 'processing'
                        AND item.lease_expires_at <= effective_now)
                  )
                ORDER BY item.created_at,
                         item.workspace_id,
                         item.resource_kind,
                         item.resource_id
                LIMIT batch_limit
                FOR UPDATE SKIP LOCKED
            )
            UPDATE identity.account_deletion_erasure_items AS item
            SET status = 'processing',
                attempt_count = item.attempt_count + 1,
                lease_token_hash = candidate_item_lease_hash,
                lease_expires_at = LEAST(
                    account_lease_expires_at,
                    effective_now + make_interval(secs => candidate_lease_seconds)
                ),
                last_error_code = NULL,
                updated_at = effective_now
            FROM candidates
            WHERE item.request_id = candidates.request_id
              AND item.workspace_id = candidates.workspace_id
              AND item.resource_kind = candidates.resource_kind
              AND item.resource_id = candidates.resource_id
            RETURNING item.workspace_id::text,
                      item.resource_kind::text,
                      item.resource_id::text,
                      item.attempt_count;
        END
        $function$
        """
    )


def _create_database_erasure_function() -> None:
    """@brief 创建最终数据库擦除与共享数据脱钩函数 / Create final database erasure and shared-data detachment.

    @return 无返回值 / No return value.
    """

    op.execute(
        """
        CREATE FUNCTION identity.erase_account_for_deletion(
            candidate_request_id text,
            candidate_claim_token_hash text,
            candidate_revision integer,
            candidate_erased_at timestamp with time zone
        ) RETURNS TABLE (
            sessions_revoked boolean,
            oauth_grants_revoked boolean,
            credentials_revoked boolean,
            external_connections_unlinked boolean,
            identity_direct_identifiers_erased boolean,
            memberships_anonymized boolean,
            personal_workspaces_erased boolean,
            shared_workspaces_detached boolean,
            invitation_references_preserved boolean,
            failure_code text,
            failure_detail text
        )
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity, agent, resume, interview, knowledge, observability
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
            target_user_id text;
            remaining_external_items bigint;
            failed_external_items bigint;
            frozen record;
            credential_scope record;
            successor_member_id text;
        BEGIN
            IF candidate_request_id IS NULL
               OR candidate_request_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,127}$'
               OR candidate_claim_token_hash !~ '^[a-f0-9]{64}$'
               OR candidate_revision < 2
               OR candidate_erased_at IS NULL THEN
                RAISE EXCEPTION 'invalid account deletion erasure arguments'
                    USING ERRCODE = '22023';
            END IF;
            -- Erasure authority comes from the live database lease, not a
            -- caller-selected historical instant.
            effective_now := statement_timestamp();
            SELECT request.user_id
            INTO target_user_id
            FROM identity.account_deletion_requests AS request
            WHERE request.id = candidate_request_id
              AND request.status = 'running'
              AND request.claim_token_hash = candidate_claim_token_hash
              AND request.revision = candidate_revision
              AND request.lease_expires_at > effective_now
            FOR UPDATE;
            IF NOT FOUND THEN
                RETURN;
            END IF;
            SELECT count(*) FILTER (
                       WHERE item.status IN ('pending', 'processing')
                   ),
                   count(*) FILTER (WHERE item.status = 'failed')
            INTO remaining_external_items, failed_external_items
            FROM identity.account_deletion_erasure_items AS item
            WHERE item.request_id = candidate_request_id;
            IF failed_external_items > 0 THEN
                RETURN QUERY SELECT
                    false, false, false, false, false, false, false, false, false,
                    'account_deletion.external_erasure_failed'::text,
                    'External erasure requires operator intervention.'::text;
                RETURN;
            END IF;
            IF remaining_external_items > 0 THEN
                RAISE EXCEPTION 'account deletion external erasure is incomplete'
                    USING ERRCODE = '55P03';
            END IF;

            PERFORM set_config('app.deletion_user_id', target_user_id, true);
            SET CONSTRAINTS ALL DEFERRED;

            FOR credential_scope IN
                SELECT DISTINCT item.workspace_id
                FROM identity.account_deletion_erasure_items AS item
                WHERE item.request_id = candidate_request_id
                  AND item.resource_kind = 'credential_scope'
                  AND item.status = 'completed'
                ORDER BY item.workspace_id
            LOOP
                PERFORM set_config(
                    'app.deletion_workspace_id', credential_scope.workspace_id, true
                );
                DELETE FROM knowledge.connection_authorization_sessions AS authorization_session
                WHERE authorization_session.workspace_id = credential_scope.workspace_id
                  AND authorization_session.created_by = target_user_id;
                UPDATE knowledge.connections AS connection
                SET status = 'revoked',
                    display_name = 'Deleted connection',
                    scopes = ARRAY[]::varchar[],
                    last_validated_at = NULL,
                    problem = NULL,
                    updated_at = effective_now,
                    revision = connection.revision + 1,
                    extensions = '{}'::jsonb
                WHERE connection.workspace_id = credential_scope.workspace_id
                  AND connection.created_by = target_user_id
                  AND (
                    connection.status <> 'revoked'
                    OR connection.display_name <> 'Deleted connection'
                    OR cardinality(connection.scopes) <> 0
                    OR connection.last_validated_at IS NOT NULL
                    OR connection.problem IS NOT NULL
                    OR connection.extensions <> '{}'::jsonb
                  );
                UPDATE knowledge.connection_credentials AS credential
                SET status = 'revoked',
                    key_id = NULL,
                    nonce = NULL,
                    ciphertext = NULL,
                    scopes = '[]'::jsonb,
                    updated_at = effective_now
                WHERE credential.workspace_id = credential_scope.workspace_id
                  AND credential.created_by = target_user_id;
                UPDATE knowledge.connection_provider_sessions AS provider_session
                SET status = CASE
                        WHEN provider_session.status = 'pending' THEN 'failed'
                        ELSE provider_session.status
                    END,
                    key_id = NULL,
                    nonce = NULL,
                    ciphertext = NULL,
                    updated_at = effective_now
                WHERE provider_session.workspace_id = credential_scope.workspace_id
                  AND provider_session.created_by = target_user_id;
            END LOOP;

            FOR frozen IN
                SELECT disposition.workspace_id, disposition.disposition
                FROM identity.account_deletion_workspace_dispositions AS disposition
                WHERE disposition.request_id = candidate_request_id
                ORDER BY disposition.workspace_id
            LOOP
                PERFORM set_config('app.deletion_workspace_id', frozen.workspace_id, true);
                IF frozen.disposition = 'personal' THEN
                    DELETE FROM knowledge.access_snapshots AS snapshot
                    WHERE snapshot.workspace_id = frozen.workspace_id;
                    DELETE FROM knowledge.citations AS citation
                    WHERE citation.workspace_id = frozen.workspace_id;
                    UPDATE interview.sessions AS interview_session
                    SET report_id = NULL
                    WHERE interview_session.workspace_id = frozen.workspace_id
                      AND interview_session.report_id IS NOT NULL;
                    -- Report evidence and transcript provenance use immediate
                    -- RESTRICT edges.  Delete leaves before their report,
                    -- transcript, input, and connection parents.
                    DELETE FROM interview.report_evidence AS evidence
                    WHERE evidence.workspace_id = frozen.workspace_id;
                    DELETE FROM interview.reports AS report
                    WHERE report.workspace_id = frozen.workspace_id;
                    DELETE FROM interview.transcript_segments AS segment
                    WHERE segment.workspace_id = frozen.workspace_id;
                    DELETE FROM interview.realtime_inputs AS realtime_input
                    WHERE realtime_input.workspace_id = frozen.workspace_id;
                    DELETE FROM interview.realtime_connections AS realtime_connection
                    WHERE realtime_connection.workspace_id = frozen.workspace_id;
                    DELETE FROM interview.sessions AS interview_session
                    WHERE interview_session.workspace_id = frozen.workspace_id;
                    DELETE FROM interview.scenarios AS scenario
                    WHERE scenario.workspace_id = frozen.workspace_id;
                    DELETE FROM resume.documents AS document
                    WHERE document.workspace_id = frozen.workspace_id;
                    DELETE FROM resume.template_versions AS template
                    WHERE template.workspace_id = frozen.workspace_id;
                    -- A source may point at its current version while every
                    -- version points back to the source with CASCADE.  Clear the
                    -- nullable selector before deleting the aggregate root so
                    -- ON DELETE RESTRICT never observes the cycle.
                    UPDATE knowledge.sources AS source
                    SET current_version_id = NULL,
                        version_counter = 0
                    WHERE source.workspace_id = frozen.workspace_id
                      AND source.current_version_id IS NOT NULL;
                    DELETE FROM knowledge.sources AS source
                    WHERE source.workspace_id = frozen.workspace_id;
                    DELETE FROM knowledge.connection_authorization_sessions AS authorization_session
                    WHERE authorization_session.workspace_id = frozen.workspace_id;
                    DELETE FROM knowledge.connections AS connection
                    WHERE connection.workspace_id = frozen.workspace_id;
                    DELETE FROM knowledge.upload_sessions AS upload
                    WHERE upload.workspace_id = frozen.workspace_id;
                    DELETE FROM knowledge.embedding_spaces AS embedding_space
                    WHERE embedding_space.workspace_id = frozen.workspace_id;
                    DELETE FROM knowledge.connection_credentials AS credential
                    WHERE credential.workspace_id = frozen.workspace_id;
                    DELETE FROM knowledge.connection_provider_sessions AS provider_session
                    WHERE provider_session.workspace_id = frozen.workspace_id;
                    DELETE FROM knowledge.upload_quota_reservations AS reservation
                    WHERE reservation.workspace_id = frozen.workspace_id;

                    -- Agent has two intentional reference cycles:
                    -- messages.source_run_id -> runs and runs.input/output_message_id
                    -- -> messages, plus waiting runs -> tool approvals -> runs.  PostgreSQL
                    -- ON DELETE RESTRICT is immediate even when the FK itself is marked
                    -- DEFERRABLE, so SET CONSTRAINTS cannot make either cycle deletable.
                    -- Break only the soon-to-be-erased references inside this transaction,
                    -- then delete roots in dependency order.
                    UPDATE agent.messages AS message
                    SET role = 'system_notice',
                        source_run_id = NULL
                    WHERE message.workspace_id = frozen.workspace_id
                      AND message.source_run_id IS NOT NULL;
                    UPDATE agent.runs AS run
                    SET status = CASE
                            WHEN run.status = 'waiting_for_approval' THEN 'running'
                            ELSE run.status
                        END,
                        pending_approval_id = NULL,
                        active_tool_call_id = NULL
                    WHERE run.workspace_id = frozen.workspace_id
                      AND (
                        run.pending_approval_id IS NOT NULL
                        OR run.active_tool_call_id IS NOT NULL
                      );
                    DELETE FROM agent.tool_approvals AS approval
                    WHERE approval.workspace_id = frozen.workspace_id;
                    DELETE FROM agent.runs AS run
                    WHERE run.workspace_id = frozen.workspace_id;
                    DELETE FROM agent.messages AS message
                    WHERE message.workspace_id = frozen.workspace_id;
                    DELETE FROM agent.conversations AS conversation
                    WHERE conversation.workspace_id = frozen.workspace_id;
                    DELETE FROM agent.jobs AS job
                    WHERE job.workspace_id = frozen.workspace_id;
                    DELETE FROM agent.artifacts AS artifact
                    WHERE artifact.workspace_id = frozen.workspace_id;
                    DELETE FROM agent.outbox_events AS outbox_event
                    WHERE outbox_event.workspace_id = frozen.workspace_id;
                    DELETE FROM agent.workspace_event_sequences AS event_sequence
                    WHERE event_sequence.workspace_id = frozen.workspace_id;
                    DELETE FROM identity.idempotency_records AS idempotency_record
                    WHERE idempotency_record.workspace_id = frozen.workspace_id;
                    DELETE FROM identity.audit_events AS audit_event
                    WHERE audit_event.workspace_id = frozen.workspace_id;
                    DELETE FROM identity.workspace_invitations AS invitation
                    WHERE invitation.workspace_id = frozen.workspace_id
                      AND invitation.status <> 'accepted';
                    DELETE FROM identity.workspace_members AS member
                    WHERE member.workspace_id = frozen.workspace_id;
                    UPDATE identity.workspaces AS workspace
                    SET name = 'Deleted workspace',
                        slug = 'deleted-workspace',
                        deleted_at = COALESCE(workspace.deleted_at, effective_now),
                        updated_at = effective_now,
                        revision = workspace.revision + 1,
                        extensions = '{}'::jsonb
                    WHERE workspace.id = frozen.workspace_id
                      AND (
                        workspace.name <> 'Deleted workspace'
                        OR workspace.slug <> 'deleted-workspace'
                        OR workspace.deleted_at IS NULL
                        OR workspace.extensions <> '{}'::jsonb
                      );
                ELSE
                    IF NOT EXISTS (
                        SELECT 1
                        FROM identity.workspace_members AS collaborator
                        WHERE collaborator.workspace_id = frozen.workspace_id
                          AND collaborator.user_id <> target_user_id
                          AND collaborator.status = 'active'
                    ) THEN
                        RAISE EXCEPTION 'shared workspace has no active successor'
                            USING ERRCODE = '55P03';
                    END IF;
                    UPDATE identity.workspace_members AS target_member
                    SET display_name = 'Deleted user',
                        role = 'viewer',
                        status = 'suspended',
                        invited_by_actor_id = NULL,
                        updated_at = effective_now,
                        revision = target_member.revision + 1,
                        extensions = '{}'::jsonb
                    WHERE target_member.workspace_id = frozen.workspace_id
                      AND target_member.user_id = target_user_id;
                    IF NOT EXISTS (
                        SELECT 1
                        FROM identity.workspace_members AS active_owner
                        WHERE active_owner.workspace_id = frozen.workspace_id
                          AND active_owner.status = 'active'
                          AND active_owner.role = 'owner'
                    ) THEN
                        SELECT collaborator.id
                        INTO successor_member_id
                        FROM identity.workspace_members AS collaborator
                        WHERE collaborator.workspace_id = frozen.workspace_id
                          AND collaborator.user_id <> target_user_id
                          AND collaborator.status = 'active'
                        ORDER BY CASE collaborator.role
                                    WHEN 'admin' THEN 0
                                    WHEN 'editor' THEN 1
                                    ELSE 2
                                 END,
                                 COALESCE(collaborator.joined_at, collaborator.created_at),
                                 collaborator.id
                        LIMIT 1
                        FOR UPDATE;
                        UPDATE identity.workspace_members AS successor
                        SET role = 'owner',
                            updated_at = effective_now,
                            revision = successor.revision + 1
                        WHERE successor.id = successor_member_id;
                    END IF;
                    UPDATE identity.audit_events AS audit_event
                    SET details = '{}'::jsonb,
                        updated_at = effective_now,
                        revision = audit_event.revision + 1,
                        extensions = '{}'::jsonb
                    WHERE audit_event.workspace_id = frozen.workspace_id
                      AND audit_event.actor_id = target_user_id;
                    DELETE FROM identity.idempotency_records AS idempotency_record
                    WHERE idempotency_record.workspace_id = frozen.workspace_id
                      AND idempotency_record.actor_id = target_user_id;
                END IF;
            END LOOP;

            PERFORM set_config('app.deletion_user_id', target_user_id, true);
            UPDATE identity.workspace_members AS member
            SET display_name = 'Deleted user',
                role = 'viewer',
                status = 'suspended',
                invited_by_actor_id = NULL,
                updated_at = effective_now,
                revision = member.revision + 1,
                extensions = '{}'::jsonb
            WHERE member.user_id = target_user_id
              AND (
                member.display_name <> 'Deleted user'
                OR member.role <> 'viewer'
                OR member.status <> 'suspended'
                OR member.invited_by_actor_id IS NOT NULL
                OR member.extensions <> '{}'::jsonb
              );
            UPDATE identity.workspace_members AS invited_member
            SET invited_by_actor_id = NULL,
                updated_at = effective_now,
                revision = invited_member.revision + 1
            WHERE invited_member.invited_by_actor_id = target_user_id;
            UPDATE identity.workspace_invitations AS invitation
            SET email_canonical = CASE
                    WHEN invitation.accepted_by_user_id = target_user_id
                    THEN 'deleted+' || substr(md5(invitation.id || target_user_id), 1, 24)
                         || '@invalid.example'
                    ELSE invitation.email_canonical
                END,
                email_hint = CASE
                    WHEN invitation.accepted_by_user_id = target_user_id
                    THEN 'd***@invalid.example'
                    ELSE invitation.email_hint
                END,
                invited_by_actor_id = CASE
                    WHEN invitation.invited_by_actor_id = target_user_id THEN NULL
                    ELSE invitation.invited_by_actor_id
                END,
                updated_at = effective_now,
                revision = invitation.revision + 1,
                extensions = CASE
                    WHEN invitation.accepted_by_user_id = target_user_id
                    THEN '{}'::jsonb
                    ELSE invitation.extensions
                END
            WHERE (
                invitation.accepted_by_user_id = target_user_id
                OR invitation.invited_by_actor_id = target_user_id
              ) AND (
                invitation.accepted_by_user_id = target_user_id
                AND (
                    invitation.email_canonical NOT LIKE 'deleted+%@invalid.example'
                    OR invitation.email_hint <> 'd***@invalid.example'
                    OR invitation.extensions <> '{}'::jsonb
                )
                OR invitation.invited_by_actor_id = target_user_id
              );
            UPDATE identity.audit_events AS audit_event
            SET details = '{}'::jsonb,
                updated_at = effective_now,
                revision = audit_event.revision + 1,
                extensions = '{}'::jsonb
            WHERE audit_event.actor_id = target_user_id
              AND (audit_event.details <> '{}'::jsonb OR audit_event.extensions <> '{}'::jsonb);
            DELETE FROM identity.idempotency_records AS idempotency_record
            WHERE idempotency_record.actor_id = target_user_id;
            DELETE FROM identity.api_v2_idempotency_records AS idempotency_receipt
            WHERE idempotency_receipt.user_id = target_user_id;
            UPDATE observability.telemetry_records AS telemetry
            SET attributes = '{}'::jsonb
            WHERE telemetry.actor_id = target_user_id;

            UPDATE identity.identity_login_sessions AS login_session
            SET revoked_at = COALESCE(login_session.revoked_at, effective_now)
            WHERE login_session.user_id = target_user_id;
            UPDATE identity.oauth_authorization_codes AS authorization_code
            SET consumed_at = COALESCE(authorization_code.consumed_at, effective_now),
                subject = 'deleted:' || target_user_id
            WHERE authorization_code.user_id = target_user_id;
            UPDATE identity.oauth_refresh_token_families AS token_family
            SET revoked_at = COALESCE(token_family.revoked_at, effective_now),
                subject = 'deleted:' || target_user_id
            WHERE token_family.user_id = target_user_id;
            DELETE FROM identity.identity_authenticators AS authenticator
            WHERE authenticator.user_id = target_user_id;
            UPDATE identity.identity_flows AS identity_flow
            SET internal_state = '{}'::jsonb,
                webauthn_options = NULL,
                authorization_resume_uri = NULL
            WHERE identity_flow.user_id = target_user_id;
            UPDATE identity.identity_browser_sessions AS browser_session
            SET browser_secret_hash = repeat('0', 64),
                csrf_token_hash = repeat('0', 64),
                last_seen_at = LEAST(browser_session.last_seen_at, effective_now),
                expires_at = LEAST(browser_session.expires_at, effective_now)
            WHERE browser_session.user_id = target_user_id;
            PERFORM identity.revoke_user_access_tokens(target_user_id, effective_now);
            UPDATE identity.users AS target_user
            SET external_subject = 'deleted:' || target_user.id,
                display_name = NULL,
                email = NULL,
                email_canonical = NULL,
                email_verified = false,
                account_status = 'deleted',
                default_workspace_id = NULL,
                locale = 'und',
                deleted_at = COALESCE(target_user.deleted_at, effective_now),
                updated_at = effective_now,
                revision = target_user.revision + 1,
                extensions = '{}'::jsonb
            WHERE target_user.id = target_user_id
              AND (
                target_user.account_status <> 'deleted'
                OR target_user.email IS NOT NULL
                OR target_user.email_canonical IS NOT NULL
                OR target_user.display_name IS NOT NULL
                OR target_user.external_subject <> 'deleted:' || target_user.id
                OR target_user.extensions <> '{}'::jsonb
              );

            SELECT NOT EXISTS (
                       SELECT 1 FROM identity.identity_login_sessions AS login_session
                       WHERE login_session.user_id = target_user_id
                         AND login_session.revoked_at IS NULL
                   ),
                   NOT EXISTS (
                       SELECT 1 FROM identity.oauth_authorization_codes AS authorization_code
                       WHERE authorization_code.user_id = target_user_id
                         AND authorization_code.consumed_at IS NULL
                   ) AND NOT EXISTS (
                       SELECT 1 FROM identity.oauth_refresh_token_families AS token_family
                       WHERE token_family.user_id = target_user_id
                         AND token_family.revoked_at IS NULL
                   ) AND EXISTS (
                       SELECT 1 FROM identity.oauth_user_token_revocations AS revocation
                       WHERE revocation.user_id = target_user_id
                   ),
                   NOT EXISTS (
                       SELECT 1 FROM identity.identity_authenticators AS authenticator
                       WHERE authenticator.user_id = target_user_id
                   ),
                   NOT EXISTS (
                       SELECT 1 FROM knowledge.connections AS connection
                       WHERE connection.created_by = target_user_id
                         AND connection.status <> 'revoked'
                   ) AND NOT EXISTS (
                       SELECT 1
                       FROM knowledge.connection_authorization_sessions AS authorization_session
                       WHERE authorization_session.created_by = target_user_id
                   ) AND NOT EXISTS (
                       SELECT 1 FROM knowledge.connection_credentials AS credential
                       WHERE credential.created_by = target_user_id
                         AND (credential.status <> 'revoked'
                              OR credential.ciphertext IS NOT NULL)
                   ) AND NOT EXISTS (
                       SELECT 1 FROM knowledge.connection_provider_sessions AS provider_session
                       WHERE provider_session.created_by = target_user_id
                         AND (provider_session.status = 'pending'
                              OR provider_session.ciphertext IS NOT NULL)
                   ),
                   EXISTS (
                       SELECT 1 FROM identity.users AS target_user
                       WHERE target_user.id = target_user_id
                         AND target_user.account_status = 'deleted'
                         AND target_user.external_subject = 'deleted:' || target_user.id
                         AND target_user.display_name IS NULL
                         AND target_user.email IS NULL
                         AND target_user.email_canonical IS NULL
                         AND target_user.email_verified IS false
                         AND target_user.default_workspace_id IS NULL
                   ) AND NOT EXISTS (
                       SELECT 1
                       FROM identity.api_v2_idempotency_records AS idempotency_receipt
                       WHERE idempotency_receipt.user_id = target_user_id
                   ),
                   NOT EXISTS (
                       SELECT 1 FROM identity.workspace_members AS member
                       WHERE member.user_id = target_user_id
                         AND (member.status <> 'suspended'
                              OR member.display_name <> 'Deleted user'
                              OR member.invited_by_actor_id IS NOT NULL
                              OR member.extensions <> '{}'::jsonb)
                   )
            INTO sessions_revoked,
                 oauth_grants_revoked,
                 credentials_revoked,
                 external_connections_unlinked,
                 identity_direct_identifiers_erased,
                 memberships_anonymized;

            personal_workspaces_erased := true;
            FOR frozen IN
                SELECT disposition.workspace_id
                FROM identity.account_deletion_workspace_dispositions AS disposition
                WHERE disposition.request_id = candidate_request_id
                  AND disposition.disposition = 'personal'
                ORDER BY disposition.workspace_id
            LOOP
                PERFORM set_config('app.deletion_workspace_id', frozen.workspace_id, true);
                IF EXISTS (
                    SELECT 1
                    FROM identity.workspaces AS workspace
                    WHERE workspace.id = frozen.workspace_id
                      AND (
                        workspace.deleted_at IS NULL
                        OR workspace.name <> 'Deleted workspace'
                        OR workspace.slug <> 'deleted-workspace'
                      )
                ) OR EXISTS (
                    SELECT 1 FROM agent.jobs AS job
                    WHERE job.workspace_id = frozen.workspace_id
                ) OR EXISTS (
                    SELECT 1 FROM agent.artifacts AS artifact
                    WHERE artifact.workspace_id = frozen.workspace_id
                ) OR EXISTS (
                    SELECT 1 FROM resume.documents AS document
                    WHERE document.workspace_id = frozen.workspace_id
                ) OR EXISTS (
                    SELECT 1 FROM interview.sessions AS interview_session
                    WHERE interview_session.workspace_id = frozen.workspace_id
                ) OR EXISTS (
                    SELECT 1 FROM knowledge.sources AS source
                    WHERE source.workspace_id = frozen.workspace_id
                ) OR EXISTS (
                    SELECT 1 FROM knowledge.upload_sessions AS upload
                    WHERE upload.workspace_id = frozen.workspace_id
                ) THEN
                    personal_workspaces_erased := false;
                END IF;
            END LOOP;
            shared_workspaces_detached := NOT EXISTS (
                SELECT 1
                FROM identity.account_deletion_workspace_dispositions AS disposition
                JOIN identity.workspace_members AS member
                  ON member.workspace_id = disposition.workspace_id
                 AND member.user_id = target_user_id
                WHERE disposition.request_id = candidate_request_id
                  AND disposition.disposition = 'shared'
                  AND member.status <> 'suspended'
            );
            invitation_references_preserved := NOT EXISTS (
                SELECT 1
                FROM identity.workspace_invitations AS invitation
                WHERE invitation.accepted_by_user_id = target_user_id
                  AND (
                    invitation.status <> 'accepted'
                    OR invitation.email_canonical NOT LIKE 'deleted+%@invalid.example'
                    OR invitation.email_hint <> 'd***@invalid.example'
                  )
            );
            failure_code := NULL;
            failure_detail := NULL;
            RETURN NEXT;
        END
        $function$
        """
    )


def _create_finalize_function() -> None:
    """@brief 创建 token+revision CAS finalize 函数 / Create the token-and-revision CAS finalizer.

    @return 无返回值 / No return value.
    """

    op.execute(
        """
        CREATE FUNCTION identity.finalize_account_deletion(
            candidate_request_id text,
            candidate_claim_token_hash text,
            candidate_revision integer,
            candidate_outcome text,
            candidate_failure_code text,
            candidate_failure_detail text,
            candidate_finalized_at timestamp with time zone
        ) RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
            target_user_id text;
            request_started_at timestamp with time zone;
            affected_count integer;
        BEGIN
            IF candidate_request_id IS NULL
               OR candidate_request_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,127}$'
               OR candidate_claim_token_hash !~ '^[a-f0-9]{64}$'
               OR candidate_revision < 2
               OR candidate_outcome NOT IN ('completed', 'failed')
               OR candidate_finalized_at IS NULL
               OR (
                    candidate_outcome = 'completed'
                    AND (candidate_failure_code IS NOT NULL
                         OR candidate_failure_detail IS NOT NULL)
               )
               OR (
                    candidate_outcome = 'failed'
                    AND (
                        candidate_failure_code IS NULL
                        OR candidate_failure_code !~ '^[a-z][a-z0-9_.-]{2,100}$'
                        OR candidate_failure_detail IS NULL
                        OR length(candidate_failure_detail) NOT BETWEEN 1 AND 1000
                    )
               ) THEN
                RAISE EXCEPTION 'invalid account deletion finalize arguments'
                    USING ERRCODE = '22023';
            END IF;
            -- Finalization is a live CAS.  Comparing the lease against a stale
            -- client timestamp would let an expired worker publish completion.
            effective_now := statement_timestamp();
            SELECT request.user_id, request.started_at
            INTO target_user_id, request_started_at
            FROM identity.account_deletion_requests AS request
            WHERE request.id = candidate_request_id
              AND request.status = 'running'
              AND request.claim_token_hash = candidate_claim_token_hash
              AND request.revision = candidate_revision
              AND request.lease_expires_at > effective_now
            FOR UPDATE;
            IF NOT FOUND THEN
                RETURN false;
            END IF;
            effective_now := GREATEST(effective_now, request_started_at);
            IF candidate_outcome = 'completed' THEN
                PERFORM set_config('app.deletion_user_id', target_user_id, true);
                IF NOT EXISTS (
                    SELECT 1 FROM identity.users AS target_user
                    WHERE target_user.id = target_user_id
                      AND target_user.account_status = 'deleted'
                      AND target_user.email IS NULL
                      AND target_user.email_canonical IS NULL
                ) OR EXISTS (
                    SELECT 1
                    FROM identity.account_deletion_erasure_items AS item
                    WHERE item.request_id = candidate_request_id
                      AND item.status <> 'completed'
                ) THEN
                    RAISE EXCEPTION 'account deletion completion evidence is incomplete'
                        USING ERRCODE = '23514';
                END IF;
                UPDATE identity.account_deletion_requests AS request
                SET status = 'completed',
                    completed_at = effective_now,
                    problem = NULL,
                    claim_token_hash = NULL,
                    lease_expires_at = NULL,
                    erasure_evidence = jsonb_build_object(
                        'sessions_revoked', true,
                        'oauth_grants_revoked', true,
                        'credentials_revoked', true,
                        'external_connections_unlinked', true,
                        'identity_direct_identifiers_erased', true,
                        'memberships_anonymized', true,
                        'personal_workspaces_erased', true,
                        'shared_workspaces_detached', true,
                        'invitation_references_preserved', true,
                        'completed_at', effective_now
                    ),
                    updated_at = effective_now,
                    revision = request.revision + 1
                WHERE request.id = candidate_request_id
                  AND request.status = 'running'
                  AND request.claim_token_hash = candidate_claim_token_hash
                  AND request.revision = candidate_revision;
            ELSE
                UPDATE identity.account_deletion_requests AS request
                SET status = 'failed',
                    completed_at = NULL,
                    problem = jsonb_build_object(
                        'code', candidate_failure_code,
                        'detail', candidate_failure_detail
                    ),
                    claim_token_hash = NULL,
                    lease_expires_at = NULL,
                    erasure_evidence = NULL,
                    updated_at = effective_now,
                    revision = request.revision + 1
                WHERE request.id = candidate_request_id
                  AND request.status = 'running'
                  AND request.claim_token_hash = candidate_claim_token_hash
                  AND request.revision = candidate_revision;
            END IF;
            GET DIAGNOSTICS affected_count = ROW_COUNT;
            RETURN affected_count = 1;
        END
        $function$
        """
    )


def _secure_runtime_surface(
    *,
    owner_role: str,
    app_role: str,
    dashboard_role: str,
    migrator_role: str,
) -> None:
    """@brief 只向 app 开放窄函数而非执行表 / Expose narrow functions, not executor tables, to app.

    @param owner_role SECURITY DEFINER owner / SECURITY DEFINER owner.
    @param app_role 后台 worker 运行角色 / Backend worker runtime role.
    @param dashboard_role 无执行权 Dashboard 角色 / Dashboard role without execution rights.
    @param migrator_role 无运行权迁移角色 / Migrator role without runtime rights.
    @return 无返回值 / No return value.
    """

    for table in (
        "identity.oauth_user_token_revocations",
        "identity.account_deletion_workspace_dispositions",
        "identity.account_deletion_erasure_items",
    ):
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
    for signature in (*_TOKEN_FUNCTIONS, *_EXECUTION_FUNCTIONS):
        op.execute(
            f"REVOKE ALL PRIVILEGES ON FUNCTION {signature} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"ALTER FUNCTION {signature} OWNER TO {owner_role}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {app_role}")


def upgrade() -> None:
    """@brief 发布可恢复账户删除与用户 token epoch / Publish recoverable deletion and user token epochs.

    @return 无返回值 / No return value.
    """

    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _preflight()
    _scope_workspace_slug_uniqueness_to_live_rows(owner_role)
    _expand_requests()
    _create_execution_tables()
    _install_owner_policies(owner_role)
    _create_token_functions()
    _create_claim_function()
    _create_erasure_item_functions()
    _create_item_completion_functions()
    _create_database_erasure_function()
    _create_finalize_function()
    _secure_runtime_surface(
        owner_role=owner_role,
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )


def downgrade() -> None:
    """@brief 仅在从未执行删除或 epoch 撤销时回退 / Downgrade only before deletion execution or epoch revocation.

    @return 无返回值 / No return value.
    @raise RuntimeError 存在不可逆证据时拒绝 / Raised when irreversible evidence exists.
    """

    owner_role = _configured_role("owner_role")
    bind = op.get_bind()
    irreversible = sum(
        int(bind.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one())
        for table in (
            "identity.oauth_user_token_revocations",
            "identity.account_deletion_workspace_dispositions",
            "identity.account_deletion_erasure_items",
        )
    )
    executed_requests = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM identity.account_deletion_requests "
                "WHERE attempt_count <> 0 OR started_at IS NOT NULL "
                "OR claim_token_hash IS NOT NULL OR lease_expires_at IS NOT NULL "
                "OR erasure_evidence IS NOT NULL"
            )
        ).scalar_one()
    )
    if irreversible or executed_requests:
        raise RuntimeError("cannot downgrade account deletion execution evidence")

    for signature in reversed(_EXECUTION_FUNCTIONS):
        op.execute(f"DROP FUNCTION {signature}")
    for signature in reversed(_TOKEN_FUNCTIONS):
        op.execute(f"DROP FUNCTION {signature}")
    for table in (
        "knowledge.connection_provider_sessions",
        "knowledge.connection_credentials",
        "knowledge.connection_authorization_sessions",
        "knowledge.connections",
        "identity.idempotency_records",
        "identity.audit_events",
        "identity.workspace_invitations",
        "identity.workspace_members",
    ):
        op.execute(f"DROP POLICY {_ACTOR_REFERENCE_POLICY} ON {table}")
    op.execute(f"DROP POLICY {_USER_POLICY} ON identity.workspace_members")
    for table in reversed(_USER_ID_TABLES):
        op.execute(f"DROP POLICY {_USER_POLICY} ON {table}")
    op.execute(f"DROP POLICY {_USER_POLICY} ON identity.users")
    op.execute(f"DROP POLICY {_DELETION_POLICY} ON identity.workspaces")
    for table in reversed(_WORKSPACE_TABLES):
        op.execute(f"DROP POLICY {_DELETION_POLICY} ON {table}")
    for table in (
        "identity.account_deletion_erasure_items",
        "identity.account_deletion_workspace_dispositions",
        "identity.oauth_user_token_revocations",
        "identity.account_deletion_requests",
    ):
        op.execute(f"DROP POLICY {_DELETION_POLICY} ON {table}")

    _restore_global_workspace_slug_uniqueness(owner_role)

    op.drop_index(
        "ix_account_deletion_erasure_items_due",
        table_name="account_deletion_erasure_items",
        schema="identity",
    )
    op.drop_table("account_deletion_erasure_items", schema="identity")
    op.drop_table("account_deletion_workspace_dispositions", schema="identity")
    op.drop_table("oauth_user_token_revocations", schema="identity")
    op.drop_index(
        "ix_account_deletion_requests_due_execution",
        table_name="account_deletion_requests",
        schema="identity",
    )
    op.drop_constraint(
        "account_deletion_requests_execution",
        "account_deletion_requests",
        schema="identity",
        type_="check",
    )
    for column in (
        "erasure_evidence",
        "attempt_count",
        "lease_expires_at",
        "claim_token_hash",
        "started_at",
    ):
        op.drop_column("account_deletion_requests", column, schema="identity")
    op.create_check_constraint(
        "account_deletion_requests_state",
        "account_deletion_requests",
        "(status IN ('scheduled', 'running', 'cancelled') AND completed_at IS NULL "
        "AND problem IS NULL) OR (status = 'completed' AND completed_at IS NOT NULL "
        "AND problem IS NULL) OR (status = 'failed' AND completed_at IS NULL "
        "AND problem IS NOT NULL)",
        schema="identity",
    )


def _create_item_completion_functions() -> None:
    """@brief 创建 item 完成、重试与状态查询函数 / Create item completion, retry, and state-query functions.

    @return 无返回值 / No return value.
    """

    op.execute(
        """
        CREATE FUNCTION identity.complete_account_deletion_erasure_item(
            candidate_request_id text,
            candidate_account_claim_hash text,
            candidate_revision integer,
            candidate_workspace_id text,
            candidate_resource_kind text,
            candidate_resource_id text,
            candidate_item_lease_hash text
        ) RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            affected_count integer;
        BEGIN
            IF candidate_account_claim_hash !~ '^[a-f0-9]{64}$'
               OR candidate_item_lease_hash !~ '^[a-f0-9]{64}$'
               OR candidate_revision < 2 THEN
                RETURN false;
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM identity.account_deletion_requests AS request
                WHERE request.id = candidate_request_id
                  AND request.status = 'running'
                  AND request.claim_token_hash = candidate_account_claim_hash
                  AND request.revision = candidate_revision
                  AND request.lease_expires_at > statement_timestamp()
            ) THEN
                RETURN false;
            END IF;
            UPDATE identity.account_deletion_erasure_items AS item
            SET status = 'completed',
                lease_token_hash = NULL,
                lease_expires_at = NULL,
                last_error_code = NULL,
                updated_at = statement_timestamp()
            WHERE item.request_id = candidate_request_id
              AND item.workspace_id = candidate_workspace_id
              AND item.resource_kind = candidate_resource_kind
              AND item.resource_id = candidate_resource_id
              AND item.status = 'processing'
              AND item.lease_token_hash = candidate_item_lease_hash
              AND item.lease_expires_at > statement_timestamp();
            GET DIAGNOSTICS affected_count = ROW_COUNT;
            RETURN affected_count = 1;
        END
        $function$
        """
    )
    op.execute(
        """
        CREATE FUNCTION identity.retry_account_deletion_erasure_item(
            candidate_request_id text,
            candidate_account_claim_hash text,
            candidate_revision integer,
            candidate_workspace_id text,
            candidate_resource_kind text,
            candidate_resource_id text,
            candidate_item_lease_hash text,
            candidate_error_code text,
            permanent_failure boolean
        ) RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            affected_count integer;
        BEGIN
            IF candidate_account_claim_hash !~ '^[a-f0-9]{64}$'
               OR candidate_item_lease_hash !~ '^[a-f0-9]{64}$'
               OR candidate_revision < 2
               OR candidate_error_code !~ '^[a-z][a-z0-9_.-]{2,100}$'
               OR permanent_failure IS NULL THEN
                RETURN false;
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM identity.account_deletion_requests AS request
                WHERE request.id = candidate_request_id
                  AND request.status = 'running'
                  AND request.claim_token_hash = candidate_account_claim_hash
                  AND request.revision = candidate_revision
                  AND request.lease_expires_at > statement_timestamp()
            ) THEN
                RETURN false;
            END IF;
            UPDATE identity.account_deletion_erasure_items AS item
            SET status = CASE
                    WHEN permanent_failure OR item.attempt_count >= 100
                    THEN 'failed'
                    ELSE 'pending'
                END,
                lease_token_hash = NULL,
                lease_expires_at = NULL,
                last_error_code = candidate_error_code,
                updated_at = statement_timestamp()
            WHERE item.request_id = candidate_request_id
              AND item.workspace_id = candidate_workspace_id
              AND item.resource_kind = candidate_resource_kind
              AND item.resource_id = candidate_resource_id
              AND item.status = 'processing'
              AND item.lease_token_hash = candidate_item_lease_hash
              AND item.lease_expires_at > statement_timestamp();
            GET DIAGNOSTICS affected_count = ROW_COUNT;
            RETURN affected_count = 1;
        END
        $function$
        """
    )
    op.execute(
        """
        CREATE FUNCTION identity.account_deletion_external_state(
            candidate_request_id text,
            candidate_account_claim_hash text,
            candidate_revision integer
        ) RETURNS TABLE (
            recipient_email text,
            pending_items bigint,
            failed_items bigint
        )
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            target_user_id text;
        BEGIN
            SELECT request.user_id
            INTO target_user_id
            FROM identity.account_deletion_requests AS request
            WHERE request.id = candidate_request_id
              AND request.status = 'running'
              AND request.claim_token_hash = candidate_account_claim_hash
              AND request.revision = candidate_revision
              AND request.lease_expires_at > statement_timestamp();
            IF NOT FOUND THEN
                RETURN;
            END IF;
            PERFORM set_config('app.deletion_user_id', target_user_id, true);
            RETURN QUERY
            SELECT target_user.email::text,
                   count(*) FILTER (
                       WHERE item.status IN ('pending', 'processing')
                   )::bigint,
                   count(*) FILTER (WHERE item.status = 'failed')::bigint
            FROM identity.users AS target_user
            LEFT JOIN identity.account_deletion_erasure_items AS item
              ON item.request_id = candidate_request_id
            WHERE target_user.id = target_user_id
            GROUP BY target_user.email;
        END
        $function$
        """
    )
    op.execute(
        """
        CREATE FUNCTION identity.release_account_deletion_progress(
            candidate_request_id text,
            candidate_account_claim_hash text,
            candidate_revision integer
        ) RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
            affected_count integer;
        BEGIN
            IF candidate_request_id IS NULL
               OR candidate_request_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,127}$'
               OR candidate_account_claim_hash !~ '^[a-f0-9]{64}$'
               OR candidate_revision < 2 THEN
                RETURN false;
            END IF;
            effective_now := statement_timestamp();
            UPDATE identity.account_deletion_requests AS request
            SET attempt_count = request.attempt_count - 1,
                lease_expires_at = GREATEST(
                    effective_now,
                    request.started_at + interval '1 microsecond'
                ),
                updated_at = effective_now
            WHERE request.id = candidate_request_id
              AND request.status = 'running'
              AND request.claim_token_hash = candidate_account_claim_hash
              AND request.revision = candidate_revision
              AND request.lease_expires_at > effective_now
              AND request.attempt_count > 0
              AND EXISTS (
                  SELECT 1
                  FROM identity.account_deletion_erasure_items AS progressed
                  WHERE progressed.request_id = candidate_request_id
                    AND progressed.updated_at >= request.updated_at
                    AND (
                        progressed.status = 'completed'
                        OR (
                            progressed.status = 'pending'
                            AND progressed.last_error_code =
                                'account_deletion.credential_batch_incomplete'
                        )
                    )
              )
              AND EXISTS (
                  SELECT 1
                  FROM identity.account_deletion_erasure_items AS item
                  WHERE item.request_id = candidate_request_id
                    AND item.status IN ('pending', 'processing')
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM identity.account_deletion_erasure_items AS item
                  WHERE item.request_id = candidate_request_id
                    AND item.status = 'failed'
              );
            GET DIAGNOSTICS affected_count = ROW_COUNT;
            RETURN affected_count = 1;
        END
        $function$
        """
    )
