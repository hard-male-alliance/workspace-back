"""@brief API V2 幂等 migration 门禁 / API V2 idempotency migration gates."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import LargeBinary

from backend.infrastructure.v2_idempotency import api_v2_idempotency_records

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260723_0014_v2_idempotency.py"
"""@brief API V2 幂等 migration / API V2 idempotency migration."""


def _load_migration() -> ModuleType:
    """@brief 从固定路径隔离加载 0014 / Load revision 0014 in isolation from its fixed path.

    @return 新加载 migration 模块 / Newly loaded migration module.
    """
    specification = importlib.util.spec_from_file_location(
        "test_20260723_0014_v2_idempotency",
        MIGRATION,
    )
    if specification is None or specification.loader is None:
        raise AssertionError("无法加载 20260723_0014 migration")
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_revision_is_linear_after_identity_workspace_expansion() -> None:
    """@brief 0014 必须线性承接 0013 / Revision 0014 linearly follows revision 0013.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert 'revision = "20260723_0014"' in source
    assert 'down_revision = "20260723_0013"' in source

    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    revision_script = scripts.get_revision("20260723_0014")
    assert revision_script is not None
    assert revision_script.down_revision == "20260723_0013"


def test_new_table_has_v2_scope_without_legacy_owner_axis() -> None:
    """@brief 表 scope 精确使用 principal/workspace/method/path/key / Table uses the exact V2 scope.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    table = api_v2_idempotency_records

    assert table.schema == "identity"
    assert {
        "user_id",
        "workspace_id",
        "method",
        "canonical_path",
        "idempotency_key",
        "request_fingerprint",
    } <= set(table.c.keys())
    assert "resource_owner_id" not in table.c
    assert table.c.workspace_id.nullable
    assert isinstance(table.c.response_body.type, LargeBinary)
    assert "postgresql_nulls_not_distinct=True" in source
    assert "uq_api_v2_idempotency_scope" in source
    user_fk = next(iter(table.c.user_id.foreign_keys))
    workspace_fk = next(iter(table.c.workspace_id.foreign_keys))
    assert user_fk.target_fullname == "identity.users.id"
    assert user_fk.ondelete == "CASCADE"
    assert workspace_fk.target_fullname == "identity.workspaces.id"
    assert workspace_fk.ondelete == "CASCADE"
    unique_scope = next(
        constraint
        for constraint in table.constraints
        if constraint.name == "uq_api_v2_idempotency_scope"
    )
    assert unique_scope.dialect_options["postgresql"]["nulls_not_distinct"] is True


def test_receipt_schema_preserves_exact_json_bytes_and_state_machine() -> None:
    """@brief receipt 用 BYTEA 保留 body 且约束 pending/completed / Receipt uses BYTEA and constrains states.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert 'sa.Column("response_body", sa.LargeBinary())' in source
    assert "status = 'pending'" in source
    assert "status = 'completed'" in source
    assert "claim_token_hash ~ '^[0-9a-f]{64}$'" in source
    assert "response_status BETWEEN 100 AND 599" in source
    assert "jsonb_typeof(response_headers) = 'array'" in source
    assert "expires_at >= created_at + interval '24 hours'" in source
    assert "status = 'completed'" in source
    assert "ix_api_v2_idempotency_completed_expiry" in source
    table = api_v2_idempotency_records
    check_names = {
        constraint.name
        for constraint in table.constraints
        if constraint.name is not None and constraint.name.startswith("ck_api_v2_idempotency_")
    }
    assert check_names == {
        "ck_api_v2_idempotency_method",
        "ck_api_v2_idempotency_canonical_path",
        "ck_api_v2_idempotency_key",
        "ck_api_v2_idempotency_fingerprint",
        "ck_api_v2_idempotency_retention",
        "ck_api_v2_idempotency_state",
    }
    assert table.c.created_at.server_default is not None
    assert table.c.updated_at.server_default is not None
    assert {
        "ix_api_v2_idempotency_completed_expiry",
        "ix_api_v2_idempotency_pending_expiry",
    } <= {index.name for index in table.indexes}


def test_rls_is_actor_self_and_null_safe_workspace_exact() -> None:
    """@brief FORCE RLS 同时比较 actor 与 NULL-safe Workspace / Forced RLS compares actor and null-safe Workspace.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert "ENABLE ROW LEVEL SECURITY" in source
    assert "FORCE ROW LEVEL SECURITY" in source
    assert "api_v2_idempotency_actor_scope" in source
    assert "user_id = current_setting('app.actor_id', true)" in source
    assert "workspace_id IS NOT DISTINCT FROM" in source
    assert "NULLIF(current_setting('app.workspace_id', true), '')" in source
    assert "REVOKE ALL PRIVILEGES ON TABLE" in source
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE" in source


def test_downgrade_refuses_to_destroy_pending_or_completed_receipts() -> None:
    """@brief 非空 receipt 表禁止 destructive downgrade / A non-empty receipt table blocks destructive downgrade.

    @return 无返回值 / No return value.
    """
    source = MIGRATION.read_text(encoding="utf-8")
    assert "SELECT count(*) FROM identity.api_v2_idempotency_records" in source
    assert "cannot downgrade non-empty API V2 idempotency receipts" in source
    assert source.index("if row_count:") < source.index(
        'op.drop_table("api_v2_idempotency_records"'
    )


def test_upgrade_rejects_unsafe_dynamic_role_before_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief role 注入在任何 DDL 前失败 / Role injection fails before any DDL.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """
    migration = _load_migration()
    configuration = Mock()
    configuration.get_main_option.side_effect = {
        "aiws.app_role": 'app"; DROP SCHEMA identity CASCADE; --',
        "aiws.dashboard_role": "dashboard",
        "aiws.migrator_role": "migrator",
    }.get
    operation = Mock()
    operation.get_context.return_value = SimpleNamespace(config=configuration)
    monkeypatch.setattr(migration, "op", operation)

    with pytest.raises(RuntimeError, match="missing or invalid dbctl role option: app_role"):
        migration.upgrade()
    operation.create_table.assert_not_called()
    operation.execute.assert_not_called()


def test_runtime_adapter_never_uses_legacy_identity_autocreation() -> None:
    """@brief V2 adapter 不调用 legacy 身份机会式创建 / V2 adapter never invokes legacy identity autocreation.

    @return 无返回值 / No return value.
    """
    source = (PROJECT_ROOT / "src" / "backend" / "infrastructure" / "v2_idempotency.py").read_text(
        encoding="utf-8"
    )
    assert "_ensure_scope_identities" not in source
    assert "ActorScope(" not in source
    assert "api_v2_idempotency_records" in source
