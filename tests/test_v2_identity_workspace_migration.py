"""@brief API V2 身份与 Workspace 迁移门禁 / API V2 identity/workspace migration gates."""

from __future__ import annotations

from pathlib import Path

from backend.infrastructure.persistence.models import (
    AccountDeletionRequestRecord,
    IdentityFlowRecord,
    UserRecord,
    WorkspaceInvitationRecord,
    WorkspaceMemberRecord,
    WorkspaceRecord,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260723_0013_v2_identity_workspace.py"
"""@brief API V2 身份与 Workspace migration / API V2 identity/workspace migration."""


def test_revision_is_linear_and_region_backfill_is_explicit() -> None:
    """@brief revision 必须线性且非空库地域必须显式配置 / Revision is linear and region is explicit.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert 'revision = "20260723_0013"' in source
    assert 'down_revision = "20260722_0012"' in source
    assert "aiws.v2_default_data_region" in source
    assert "non-empty workspace data requires aiws.v2_default_data_region" in source
    assert "aiws.v2_legacy_workspace_plans" in source
    assert "must map every existing Workspace exactly" in source
    assert (
        '_DATA_REGIONS: frozenset[str] = frozenset({"cn", "global", "private_deployment"})'
        in source
    )


def test_preflight_rejects_ambiguous_legacy_identity_state() -> None:
    """@brief 迁移不得猜测邮箱、Owner 或邀请身份 / Migration rejects ambiguous identity state.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert "GROUP BY lower(btrim(email))" in source
    assert "canonical email collisions require operator resolution" in source
    assert "active users require a contract-valid V2 profile" in source
    assert "email <> btrim(email)" in source
    assert "length(external_subject) > 255" in source
    assert "active workspaces require a canonical 1-to-120-character name" in source
    assert "workspace members require a contract-valid display-name snapshot" in source
    assert "every workspace requires an active resource-owner membership" in source
    assert "invited_user.email_verified IS NOT TRUE" in source
    assert "member.role NOT IN ('admin', 'editor', 'viewer')" in source
    assert "invited members require a verified valid email and non-owner role" in source
    assert "duplicate pending invitations require operator resolution" in source


def test_backfill_rules_are_deterministic_and_audited() -> None:
    """@brief slug、套餐和默认 Workspace 规则必须确定且可审计 / Backfills are deterministic and audited.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert "left(candidate.base_slug, 30) || '-' || md5(workspace.id)" in source
    assert "UPDATE identity.workspaces SET plan = :plan WHERE id = :workspace_id" in source
    assert "COALESCE(member.joined_at, member.created_at), member.id" in source
    assert "api-v2-identity-workspace-0013" in source
    assert "INSERT INTO identity.api_migration_audits" in source
    assert "explicit per-workspace operator mapping" in source
    assert 'op.drop_constraint("users_email_unique"' in source
    assert 'op.create_unique_constraint("users_email_unique"' in source


def test_invited_members_become_independent_invitations_before_member_cleanup() -> None:
    """@brief invited 成员先无损复制再删除，disabled 规范为 suspended / Invitations copy before cleanup.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    invitation_insert = "INSERT INTO identity.workspace_invitations"
    invited_delete = "DELETE FROM identity.workspace_members WHERE status = 'invited'"
    disabled_update = (
        "UPDATE identity.workspace_members SET status = 'suspended' WHERE status = 'disabled'"
    )
    assert source.index(invitation_insert) < source.index(invited_delete)
    assert source.index(invited_delete) < source.index(disabled_update)
    assert "status IN ('active', 'suspended')" in source
    assert "uq_workspace_invitations_pending_email" in source


def test_new_tables_are_least_privilege_and_default_deny() -> None:
    """@brief 新表必须 FORCE RLS 且只授予应用最小读写权限 / New tables force RLS and least privilege.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert "REVOKE ALL PRIVILEGES ON TABLE" in source
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE" in source
    assert "ENABLE ROW LEVEL SECURITY" in source
    assert "FORCE ROW LEVEL SECURITY" in source
    assert "workspace_app_invitation_scope" in source
    assert "workspace_id = current_setting('app.workspace_id', true)" in source
    assert "identity_app_account_deletion_self" in source
    assert "user_id = current_setting('app.actor_id', true)" in source
    assert (
        "invitation_predicate = \"workspace_id = current_setting('app.workspace_id', true)\""
        in source
    )
    assert "identity.resolve_login_user_id(candidate_email text)" in source
    assert "identity.clear_inactive_member_default_workspace(" in source
    assert "SECURITY DEFINER" in source
    assert "SET search_path = pg_catalog, identity" in source
    assert "REVOKE ALL PRIVILEGES ON FUNCTION" in source
    assert "cannot downgrade non-empty or irreversible API V2 identity state" in source
    assert "_LEGACY_TENANT_POLICY_TABLES" in source
    assert "legacy tenant-policy coverage differs from the frozen 0013 topology" in source
    assert "FOR scoped_table IN" not in source


def test_orm_identity_and_workspace_columns_match_v2_contract() -> None:
    """@brief ORM 必须映射 V2 用户、Workspace 与成员状态 / ORM maps V2 user/workspace/member state.

    @return 无返回值 / No return value.
    """
    assert {"email_canonical", "account_status", "default_workspace_id"} <= set(
        UserRecord.__table__.c.keys()
    )
    assert {"slug", "plan", "data_region"} <= set(WorkspaceRecord.__table__.c.keys())
    assert "display_name" in WorkspaceMemberRecord.__table__.c
    assert "ix_workspace_invitations_workspace_id" not in {
        index.name for index in WorkspaceInvitationRecord.__table__.indexes
    }
    assert UserRecord.__table__.c.locale.type.length == 35
    user_checks = " ".join(
        str(constraint.sqltext)
        for constraint in UserRecord.__table__.constraints
        if hasattr(constraint, "sqltext")
    )
    assert "length(external_subject) BETWEEN 1 AND 255" in user_checks
    assert "email = btrim(email)" in user_checks
    workspace_checks = " ".join(
        str(constraint.sqltext)
        for constraint in WorkspaceRecord.__table__.constraints
        if hasattr(constraint, "sqltext")
    )
    assert "length(name) BETWEEN 1 AND 120" in workspace_checks
    member_checks = " ".join(
        str(constraint.sqltext)
        for constraint in WorkspaceMemberRecord.__table__.constraints
        if hasattr(constraint, "sqltext")
    )
    assert "'active', 'suspended'" in member_checks
    assert "invited" not in member_checks
    assert "disabled" not in member_checks


def test_reauthentication_flows_record_a_conservative_completion_instant() -> None:
    """@brief 重新认证窗口必须基于完成时刻且历史回填不延长窗口 / Reauth uses completion time.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert "UPDATE identity.identity_flows SET completed_at = created_at" in source
    assert "identity_flows_completion" in source
    assert "completed_at >= created_at AND completed_at <= expires_at" in source
    assert "completed_at" in IdentityFlowRecord.__table__.c
    assert "WHERE completed_at IS NOT NULL" in source
    flow_checks = " ".join(
        str(constraint.sqltext)
        for constraint in IdentityFlowRecord.__table__.constraints
        if hasattr(constraint, "sqltext")
    )
    assert "status = 'completed' AND completed_at IS NOT NULL" in flow_checks


def test_orm_new_resources_encode_state_and_partial_uniqueness() -> None:
    """@brief ORM 用约束与部分唯一索引表达业务状态 / ORM encodes state and partial uniqueness.

    @return 无返回值 / No return value.
    """
    invitation = WorkspaceInvitationRecord.__table__
    deletion = AccountDeletionRequestRecord.__table__
    assert invitation.schema == "identity"
    assert deletion.schema == "identity"
    assert {"email_canonical", "email_hint", "expires_at", "resolved_at"} <= set(
        invitation.c.keys()
    )
    accepted_by_fk = next(iter(invitation.c.accepted_by_user_id.foreign_keys))
    assert accepted_by_fk.ondelete == "RESTRICT"
    assert "resource_owner_id" not in invitation.c
    assert {"user_id", "scheduled_for", "completed_at", "problem"} <= set(deletion.c.keys())
    invitation_indexes = {index.name: index for index in invitation.indexes}
    deletion_indexes = {index.name: index for index in deletion.indexes}
    assert invitation_indexes["uq_workspace_invitations_pending_email"].unique
    assert (
        str(
            invitation_indexes["uq_workspace_invitations_pending_email"].dialect_options[
                "postgresql"
            ]["where"]
        )
        == "status = 'pending'"
    )
    assert deletion_indexes["uq_account_deletion_requests_live_user"].unique
    assert "scheduled" in str(
        deletion_indexes["uq_account_deletion_requests_live_user"].dialect_options["postgresql"][
            "where"
        ]
    )
