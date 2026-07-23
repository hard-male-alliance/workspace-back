"""@brief Generic Job 取消补偿 migration 门禁 / Generic Job-cancellation compensation migration gates."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = (
    PROJECT_ROOT
    / "alembic"
    / "versions"
    / "20260723_0026_job_cancellation_compensation.py"
)
"""@brief Job cancellation compensation migration / Job cancellation compensation migration."""


def _load_migration() -> ModuleType:
    """@brief 隔离加载 0026 / Load revision 0026 in isolation.

    @return 新加载 migration module / Newly loaded migration module.
    """

    specification = importlib.util.spec_from_file_location(
        "test_20260723_0026_job_cancellation_compensation",
        MIGRATION,
    )
    if specification is None or specification.loader is None:
        raise AssertionError("无法加载 20260723_0026 migration")
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_0026_linearly_precedes_outbox_lifecycle() -> None:
    """@brief 0026 线性承接 0025 并由 0027 继承 / 0026 follows 0025 linearly and is succeeded by 0027."""

    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    script = scripts.get_revision("20260723_0026")

    assert scripts.get_heads() == ["20260723_0027"]
    assert script is not None
    assert script.down_revision == "20260723_0025"
    assert script.branch_labels == set()
    assert script.dependencies is None


def test_upgrade_freezes_one_snapshot_and_rejects_ambiguous_bindings() -> None:
    """@brief preflight 冻结三表并拒绝重复/错配 target / Preflight freezes all three tables and rejects duplicate or mismatched targets."""

    source = MIGRATION.read_text(encoding="utf-8")

    assert "LOCK TABLE agent.jobs, knowledge.connections, knowledge.sources" in source
    assert "IN SHARE ROW EXCLUSIVE MODE" in source
    assert "LEFT JOIN knowledge.connections AS connection" in source
    assert "LEFT JOIN knowledge.sources AS source" in source
    assert "connection.workspace_id = job.workspace_id" in source
    assert "source.workspace_id = job.workspace_id" in source
    assert "job.request_payload -> 'subject' ->> 'resource_type'" in source
    assert "job.request_payload -> 'subject' ->> 'id'" in source
    assert "job.request_payload -> 'subject' -> 'revision'" in source
    assert "job.request_payload -> 'spec' -> 'source_revision'" in source
    assert "job.request_payload -> 'spec' ->> 'credential_reference'" in source
    assert "HAVING count(*) > 1" in source
    assert "unique-active-work binding" in source


def test_backfill_is_fail_closed_and_never_promotes_delete_permissions() -> None:
    """@brief 不可知前态只回填安全下界 / Unknown prior state is backfilled only to a safe lower bound."""

    source = MIGRATION.read_text(encoding="utf-8")

    assert "'previous_status', 'reauthorization_required'" in source
    assert "source.current_version_id IS NOT NULL" in source
    assert "source.last_success_at IS NOT NULL" in source
    assert "THEN 'stale'" in source
    assert "ELSE 'not_started'" in source
    assert "jsonb_build_object('previous_enabled', false)" in source
    assert "jsonb_build_object('previous_enabled', true)" not in source
    assert "jsonb_build_object('previous_problem', NULL)" in source


def test_existing_snapshots_are_preserved_and_invalid_explicit_state_fails() -> None:
    """@brief 仅补 absent key，显式损坏状态不得被默认值掩盖 / Only absent keys are filled; explicit corrupt state cannot be masked by defaults."""

    source = MIGRATION.read_text(encoding="utf-8")

    for member in (
        "previous_status",
        "previous_enabled",
        "previous_ingestion_status",
        "previous_problem",
    ):
        assert f"NOT (job.request_payload -> 'spec' ? '{member}')" in source
    assert "NOT IN ('active', 'reauthorization_required', 'failed')" in source
    assert "NOT IN ('not_started', 'ready', 'stale', 'failed')" in source
    assert "found corrupt or contradictory explicit cancellation snapshots" in source
    assert "(job.request_payload -> 'spec') || candidate.added_spec" in source


def test_marker_makes_downgrade_exact_and_refuses_changed_evidence() -> None:
    """@brief downgrade 只移除 marker 证明由 0026 新增的成员 / Downgrade removes only members proven to have been inserted by 0026."""

    source = MIGRATION.read_text(encoding="utf-8")
    downgrade = source[source.index("def _preflight_downgrade_markers") :]

    assert '"_migration_0026"' in source
    assert '"generic_job_cancellation_compensation"' in source
    assert "'added_spec', candidate.added_spec" in source
    assert "jsonb_object_keys(" in downgrade
    assert "IS DISTINCT FROM" in downgrade
    assert "cannot downgrade 0026" in downgrade
    assert "(job.request_payload -> 'spec') - migrated.added_fields" in downgrade
    assert "job.request_payload - '{_MARKER}'" in downgrade
    assert "ORDER BY added.key" in downgrade
    assert "SET request_payload" in downgrade
    assert "SET updated_at" not in downgrade
    assert "SET revision" not in downgrade


def test_temporary_rls_visibility_is_bounded_and_removed() -> None:
    """@brief 临时 owner policy 只覆盖活动/marked 行且迁移后删除 / Temporary owner policies cover only active/marked rows and are removed."""

    source = MIGRATION.read_text(encoding="utf-8")

    assert "status IN ('queued', 'running')" in source
    assert "request_payload ? '{_MARKER}'" in source
    assert 'visible = f"({marked}) OR ({restored})"' in source
    assert "USING ({visible}) WITH CHECK ({visible})" in source
    assert "NOT (request_payload ? '{_MARKER}')" in source
    assert "USING (status = 'revoking')" in source
    assert "USING (ingestion_state IN (" in source
    assert "DROP POLICY {_MIGRATION_POLICY} ON knowledge.sources" in source
    assert "DROP POLICY {_MIGRATION_POLICY} ON knowledge.connections" in source
    assert "BYPASSRLS" not in source
    assert "DISABLE ROW LEVEL SECURITY" not in source


def test_unsafe_owner_role_fails_before_lock_or_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief owner role 注入在任何 DDL/DML 前失败 / Owner-role injection fails before any DDL or DML.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
    """

    migration = _load_migration()
    configuration = Mock()
    configuration.get_main_option.return_value = 'owner"; DROP SCHEMA agent CASCADE; --'
    operation = Mock()
    operation.get_context.return_value = SimpleNamespace(config=configuration)
    monkeypatch.setattr(migration, "op", operation)

    with pytest.raises(RuntimeError, match="missing or invalid dbctl role option: owner_role"):
        migration.upgrade()
    operation.execute.assert_not_called()
    operation.get_bind.assert_not_called()
