"""@brief API V2 维护 migration 安全门禁 / API V2 maintenance migration safety gates."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from backend.infrastructure import maintenance
from backend.infrastructure.persistence.models import WorkspaceInvitationRecord
from backend.infrastructure.v2_idempotency import api_v2_idempotency_records

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260723_0015_v2_maintenance.py"
"""@brief API V2 maintenance migration / API V2 maintenance migration."""


def _load_migration() -> ModuleType:
    """@brief 隔离加载 0015 / Load revision 0015 in isolation.

    @return 新加载 migration 模块 / Newly loaded migration module.
    """
    specification = importlib.util.spec_from_file_location(
        "test_20260723_0015_v2_maintenance",
        MIGRATION,
    )
    if specification is None or specification.loader is None:
        raise AssertionError("无法加载 20260723_0015 migration")
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_revision_linearly_follows_v2_idempotency() -> None:
    """@brief 0015 必须线性承接 0014 / Revision 0015 linearly follows revision 0014."""
    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    revision_script = scripts.get_revision("20260723_0015")

    assert revision_script is not None
    assert revision_script.down_revision == "20260723_0014"
    heads = scripts.get_heads()
    lineage = {item.revision for item in scripts.walk_revisions()}
    assert len(heads) == 1
    assert "20260723_0015" in lineage


def test_invitation_expiry_is_bounded_locked_and_advances_all_resource_fields() -> None:
    """@brief 邀请到期必须跳锁、有界并推进完整版本 / Expiry is skip-locked, bounded, and version-complete."""
    source = MIGRATION.read_text(encoding="utf-8")
    function = source[source.index("CREATE FUNCTION identity.expire_due_workspace_invitations") :]
    function = function[: function.index("CREATE FUNCTION identity.maintain_api_v2")]

    assert "FOR UPDATE SKIP LOCKED" in function
    assert "LIMIT batch_limit" in function
    assert "batch_limit > 1000" in function
    assert "invitation.status = 'pending'" in function
    assert "invitation.expires_at <= effective_now" in function
    assert "invitation.updated_at <= effective_now" in function
    assert "status = 'expired'" in function
    assert "revision = invitation.revision + 1" in function
    assert "updated_at = effective_now" in function
    assert "resolved_at = effective_now" in function
    assert "accepted_by_user_id" not in function


def test_receipt_cleanup_never_claims_or_deletes_pending_and_exposes_stranded() -> None:
    """@brief receipt 函数只删 completed 且显式统计 stranded pending / Receipt cleanup deletes only completed and reports stranded pending."""
    source = MIGRATION.read_text(encoding="utf-8")
    function = source[source.index("CREATE FUNCTION identity.maintain_api_v2") :]
    function = function[: function.index("def _secure_functions")]

    assert "FOR UPDATE SKIP LOCKED" in function
    assert "LIMIT batch_limit" in function
    assert "receipt.status = 'completed'" in function
    assert "DELETE FROM identity.api_v2_idempotency_records" in function
    assert "receipt.status = 'pending'" in function
    assert "LEAST(count(*), batch_limit)::bigint" in function
    assert "min(stranded.expires_at)" in function
    assert "LIMIT batch_limit + 1" in function
    assert "count(*) > batch_limit" in function
    assert "UPDATE identity.api_v2_idempotency_records" not in function
    assert "status = 'pending'" not in function.split(
        "DELETE FROM identity.api_v2_idempotency_records", maxsplit=1
    )[0]


def test_security_definers_are_owner_owned_fixed_path_and_execute_only() -> None:
    """@brief 窄函数固定路径、owner-owned 且 app 仅获 EXECUTE / Narrow functions fix search path, are owner-owned, and grant app only EXECUTE."""
    source = MIGRATION.read_text(encoding="utf-8")

    assert source.count("\n        SECURITY DEFINER\n") == 2
    assert source.count("SET search_path = pg_catalog, identity") == 2
    assert source.count("SET row_security = on") == 2
    assert "ALTER FUNCTION {signature} OWNER TO {owner_role}" in source
    assert "FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}" in source
    assert "GRANT EXECUTE ON FUNCTION {signature} TO {app_role}" in source
    assert "GRANT SELECT" not in source
    assert "GRANT UPDATE" not in source
    assert "GRANT DELETE" not in source


def test_force_rls_owner_policies_are_due_state_only() -> None:
    """@brief owner policy 只暴露到期状态且不形成全表 bypass / Owner policies expose due states only, never a table-wide bypass."""
    source = MIGRATION.read_text(encoding="utf-8")

    assert "maintenance_owner_due_invitation_select" in source
    assert "maintenance_owner_due_invitation_update" in source
    assert "maintenance_owner_due_idempotency_select" in source
    assert "maintenance_owner_completed_idempotency_delete" in source
    assert "maintenance_owner_completed_idempotency_lock" in source
    assert "expires_at <= statement_timestamp()" in source
    assert "WITH CHECK (false)" in source
    assert "USING (true)" not in source
    assert "WITH CHECK (true)" not in source
    assert "BYPASSRLS" not in source
    assert "DISABLE ROW LEVEL SECURITY" not in source


def test_candidate_clock_cannot_advance_beyond_database_time() -> None:
    """@brief caller 不能用未来时间提前过期或清理 / A caller cannot expire or purge early with a future clock."""
    source = MIGRATION.read_text(encoding="utf-8")
    assert source.count("LEAST(candidate_now, statement_timestamp())") == 2


def test_partial_indexes_are_synchronized_with_runtime_metadata() -> None:
    """@brief 维护 partial indexes 必须存在于运行时 metadata / Maintenance partial indexes stay synchronized with runtime metadata."""
    invitation_indexes = {
        index.name: index for index in WorkspaceInvitationRecord.__table__.indexes
    }
    receipt_indexes = {index.name: index for index in api_v2_idempotency_records.indexes}

    assert "ix_workspace_invitations_pending_expiry" in invitation_indexes
    assert "ix_workspace_invitations_workspace_id" not in invitation_indexes
    assert (
        str(
            invitation_indexes["ix_workspace_invitations_pending_expiry"].dialect_options[
                "postgresql"
            ]["where"]
        )
        == "status = 'pending'"
    )
    assert "ix_api_v2_idempotency_completed_expiry" in receipt_indexes
    assert "ix_api_v2_idempotency_pending_expiry" in receipt_indexes
    assert (
        str(
            receipt_indexes["ix_api_v2_idempotency_pending_expiry"].dialect_options["postgresql"][
                "where"
            ]
        )
        == "status = 'pending'"
    )


def test_account_deletion_is_not_falsely_implemented() -> None:
    """@brief 0015 不伪装执行账户删除 / Revision 0015 does not pretend to execute account deletion."""
    source = MIGRATION.read_text(encoding="utf-8")
    infrastructure = (
        PROJECT_ROOT / "src" / "backend" / "infrastructure" / "maintenance.py"
    ).read_text(encoding="utf-8")

    for forbidden in (
        "UPDATE identity.account_deletion_requests",
        "DELETE FROM identity.users",
        "DELETE FROM identity.oauth",
    ):
        assert forbidden not in source
        assert forbidden not in infrastructure


def test_runtime_adapter_calls_only_narrow_functions_without_fabricated_scope() -> None:
    """@brief PG adapter 只走窄函数且不伪造租户 GUC / PG adapter calls only narrow functions without fake tenant GUCs."""
    source = Path(maintenance.__file__).read_text(encoding="utf-8")

    assert "identity.expire_due_workspace_invitations" in source
    assert "identity.maintain_api_v2_idempotency_receipts" in source
    assert "unscoped_transaction" in source
    assert "set_config(" not in source
    assert "ActorScope(" not in source


def test_upgrade_rejects_unsafe_owner_role_before_any_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 非法 owner role 在任何 DDL 前失败 / An unsafe owner role fails before any DDL.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
    """
    migration = _load_migration()
    configuration = Mock()
    configuration.get_main_option.side_effect = {
        "aiws.owner_role": 'owner"; DROP SCHEMA identity CASCADE; --',
        "aiws.app_role": "app",
        "aiws.dashboard_role": "dashboard",
        "aiws.migrator_role": "migrator",
    }.get
    operation = Mock()
    operation.get_context.return_value = SimpleNamespace(config=configuration)
    monkeypatch.setattr(migration, "op", operation)

    with pytest.raises(RuntimeError, match="missing or invalid dbctl role option: owner_role"):
        migration.upgrade()
    operation.create_index.assert_not_called()
    operation.execute.assert_not_called()
