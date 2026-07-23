"""@brief Alembic 版本表权限迁移测试 / Alembic version-table privilege migration tests."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Literal
from unittest.mock import Mock

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260721_0007_protect_alembic_version_table.py"
"""@brief 版本表权限修复 migration / Version-table privilege repair migration."""

MigrationAction = Literal["upgrade", "downgrade"]
"""@brief 测试允许调用的迁移动作 / Migration actions callable by the tests."""


def _load_migration() -> ModuleType:
    """@brief 从固定路径隔离加载 revision 模块 / Load the revision module in isolation from its fixed path.

    @return 新加载的 migration 模块 / Newly loaded migration module.
    """
    specification = importlib.util.spec_from_file_location(
        "test_20260721_0007_protect_alembic_version_table",
        MIGRATION,
    )
    if specification is None or specification.loader is None:
        raise AssertionError("无法加载 20260721_0007 migration")
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def _execute_and_capture(
    migration: ModuleType,
    action: MigrationAction,
    role_options: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[str, ...]:
    """@brief 用内存 Alembic 配置执行并捕获 SQL / Execute with in-memory Alembic config and capture SQL.

    @param migration 已隔离加载的 revision 模块 / Isolated revision module.
    @param action upgrade 或 downgrade / Upgrade or downgrade action.
    @param role_options ``aiws.*`` 动态 role 配置 / Dynamic ``aiws.*`` role options.
    @param monkeypatch pytest monkeypatch fixture / Pytest monkeypatch fixture.
    @return migration 传给 Alembic 的 SQL 语句 / SQL statements passed to Alembic.
    """
    configuration = Mock()
    configuration.get_main_option.side_effect = role_options.get
    operation = Mock()
    operation.get_context.return_value = SimpleNamespace(config=configuration)
    monkeypatch.setattr(migration, "op", operation)
    getattr(migration, action)()
    return tuple(call.args[0] for call in operation.execute.call_args_list)


def test_revision_extends_0006_in_the_single_linear_history() -> None:
    """@brief 0007 必须线性承接 0006 / Revision 0007 linearly extends revision 0006.

    @return 无返回值 / No return value.
    """
    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    revision_script = scripts.get_revision("20260721_0007")

    assert scripts.get_heads() == ["20260723_0028"]
    assert revision_script is not None
    assert revision_script.down_revision == "20260721_0006"
    assert revision_script.branch_labels == set()
    assert revision_script.dependencies is None


def test_upgrade_revokes_every_direct_non_owner_grant_using_dynamic_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief upgrade 安全引用动态角色并仅撤销控制表权限 / Upgrade safely quotes dynamic roles and revokes only control-table grants.

    @param monkeypatch pytest monkeypatch fixture / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """
    migration = _load_migration()
    statements = _execute_and_capture(
        migration,
        "upgrade",
        {
            "aiws.app_role": "Tenant_App",
            "aiws.dashboard_role": "Tenant_Dashboard",
            "aiws.migrator_role": "Tenant_Migrator",
        },
        monkeypatch,
    )
    assert statements == (
        "REVOKE ALL PRIVILEGES ON TABLE identity.alembic_version "
        'FROM PUBLIC, "Tenant_App", "Tenant_Dashboard", "Tenant_Migrator"',
    )


def test_upgrade_rejects_unsafe_role_before_executing_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 非法动态 role 必须在 SQL 执行前失败 / An unsafe dynamic role fails before SQL execution.

    @param monkeypatch pytest monkeypatch fixture / Pytest monkeypatch fixture.
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
    operation.execute.assert_not_called()


def test_downgrade_restores_only_0006_app_dml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief downgrade 只恢复 app 的四项历史 DML / Downgrade restores only the app's four historical DML privileges.

    @param monkeypatch pytest monkeypatch fixture / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """
    migration = _load_migration()
    statements = _execute_and_capture(
        migration,
        "downgrade",
        {"aiws.app_role": "Restored_App"},
        monkeypatch,
    )
    assert statements == (
        'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE identity.alembic_version TO "Restored_App"',
    )


def test_revision_emits_privilege_ddl_only_and_never_touches_business_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 两个方向都只能发出版本表权限 DDL / Both directions emit only version-table privilege DDL.

    @param monkeypatch pytest monkeypatch fixture / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """
    migration = _load_migration()
    role_options = {
        "aiws.app_role": "app",
        "aiws.dashboard_role": "dashboard",
        "aiws.migrator_role": "migrator",
    }
    statements = _execute_and_capture(migration, "upgrade", role_options, monkeypatch)
    statements += _execute_and_capture(migration, "downgrade", role_options, monkeypatch)

    assert len(statements) == 2
    assert all(" ON TABLE identity.alembic_version " in statement for statement in statements)
    assert all(statement.startswith(("REVOKE ", "GRANT ")) for statement in statements)
    assert all(
        forbidden not in statement
        for statement in statements
        for forbidden in ("INSERT INTO ", "UPDATE identity.", "DELETE FROM ", "TRUNCATE ")
    )
