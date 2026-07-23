"""@brief 统一 outbox 闭合生命周期与 0027 migration 门禁 / Unified-outbox lifecycle and 0027 migration gates."""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from backend.domain.outbox import (
    AGENT_WORK_EVENT_TYPES,
    INTERVIEW_WORK_EVENT_TYPES,
    KNOWLEDGE_WORK_EVENT_TYPES,
    KNOWN_OUTBOX_EVENT_TYPES,
    NOTIFICATION_EVENT_TYPES,
    RESUME_WORK_EVENT_TYPES,
    WORK_EVENT_TYPES,
    OutboxEventPurpose,
    UnknownOutboxEventType,
    classify_outbox_event,
    initial_outbox_lifecycle,
)
from backend.infrastructure.persistence.models import OutboxEventRecord

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = (
    PROJECT_ROOT
    / "alembic"
    / "versions"
    / "20260723_0027_outbox_notification_lifecycle.py"
)
"""@brief outbox lifecycle migration / Outbox-lifecycle migration."""

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 确定性 UTC 时刻 / Deterministic UTC instant."""


def _load_migration() -> ModuleType:
    """@brief 隔离加载 0027 / Load revision 0027 in isolation.

    @return migration module / Migration module.
    """

    specification = importlib.util.spec_from_file_location(
        "test_20260723_0027_outbox_notification_lifecycle",
        MIGRATION,
    )
    if specification is None or specification.loader is None:
        raise AssertionError("无法加载 20260723_0027 migration")
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_event_taxonomy_is_disjoint_complete_and_matches_worker_domains() -> None:
    """@brief work/notification 互斥且 work 由四个领域完整组成 / Taxonomy is disjoint and work is exactly the four domain sets."""

    assert not WORK_EVENT_TYPES & NOTIFICATION_EVENT_TYPES
    assert KNOWN_OUTBOX_EVENT_TYPES == WORK_EVENT_TYPES | NOTIFICATION_EVENT_TYPES
    assert WORK_EVENT_TYPES == (
        AGENT_WORK_EVENT_TYPES
        | KNOWLEDGE_WORK_EVENT_TYPES
        | INTERVIEW_WORK_EVENT_TYPES
        | RESUME_WORK_EVENT_TYPES
    )
    assert "job.updated" in NOTIFICATION_EVENT_TYPES
    assert "resume.job_created" in WORK_EVENT_TYPES


def test_initial_lifecycle_publishes_notifications_and_retains_work() -> None:
    """@brief notification 写入即 published，work 保持 pending / Notifications publish on insert while work remains pending."""

    notification = initial_outbox_lifecycle("job.updated", occurred_at=NOW)
    work = initial_outbox_lifecycle("agent.run.queued", occurred_at=NOW)

    assert classify_outbox_event("job.updated") is OutboxEventPurpose.NOTIFICATION
    assert notification.status == "published"
    assert notification.published_at == NOW
    assert classify_outbox_event("agent.run.queued") is OutboxEventPurpose.WORK
    assert work.status == "pending"
    assert work.published_at is None
    with pytest.raises(UnknownOutboxEventType, match="unclassified"):
        initial_outbox_lifecycle("plugin.magic", occurred_at=NOW)
    with pytest.raises(ValueError, match="timezone-aware"):
        initial_outbox_lifecycle("job.updated", occurred_at=NOW.replace(tzinfo=None))


def test_orm_metadata_exposes_closed_delivery_constraint_and_retention_index() -> None:
    """@brief 运行时 metadata 与 0027 约束/索引同步 / Runtime metadata mirrors the 0027 constraint and index."""

    constraint_names = {constraint.name for constraint in OutboxEventRecord.__table__.constraints}
    index_names = {index.name for index in OutboxEventRecord.__table__.indexes}

    assert any(
        name is not None and name.endswith("outbox_events_delivery_class")
        for name in constraint_names
    )
    assert "ix_outbox_events_terminal_replay_expiry" in index_names


def test_0027_is_single_linear_head_and_taxonomy_matches_runtime() -> None:
    """@brief 0027 线性承接 0026 且 migration/runtime 事件闭集一致 / 0027 linearly follows 0026 with matching migration/runtime sets."""

    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    script = scripts.get_revision("20260723_0027")
    migration = _load_migration()

    assert scripts.get_heads() == ["20260723_0028"]
    assert script is not None
    assert script.down_revision == "20260723_0026"
    assert set(migration._WORK_EVENT_TYPES) == WORK_EVENT_TYPES
    assert set(migration._NOTIFICATION_EVENT_TYPES) == NOTIFICATION_EVENT_TYPES


def test_migration_fails_closed_then_backfills_with_exact_marker() -> None:
    """@brief migration 预检未知/暧昧行并为精确回退留 marker / Migration rejects unknown/ambiguous rows and marks exact rollback evidence."""

    source = MIGRATION.read_text(encoding="utf-8")

    assert "event_type NOT IN ({_KNOWN_SQL})" in source
    assert "status IN ('processing', 'failed')" in source
    assert "attempt_count <> 0" in source
    assert "0027 found unclassified outbox event types" in source
    assert "WHERE event.event_type IN ({_NOTIFICATION_SQL})" in source
    assert "AND event.status = 'pending'" in source
    assert "published_at = event.occurred_at" in source
    assert '"_migration_0027"' in source
    assert "previous_updated_at" in source
    assert "extensions = event.extensions - '{_MIGRATION_MARKER}'" in source


def test_retention_is_bounded_terminal_only_and_clock_capped() -> None:
    """@brief retention 只删过期终态、严格有界且不信任 caller 时钟 / Retention is terminal-only, strictly bounded, and clock-capped."""

    source = MIGRATION.read_text(encoding="utf-8")
    function = source[source.index("CREATE FUNCTION agent.purge_expired_outbox_events") :]
    function = function[: function.index("def _secure_retention_function")]

    assert "event.status IN ('published', 'failed')" in function
    assert "event.status IN ('pending', 'processing')" not in function
    assert "event.replay_expires_at <= effective_now" in function
    assert "LIMIT candidate_batch_size" in function
    assert "candidate_batch_size NOT BETWEEN 1 AND 1000" in function
    assert "LEAST(candidate_now, statement_timestamp())" in function
    assert "SECURITY DEFINER" in function
    assert "SET search_path = pg_catalog, agent" in function
    assert "SET row_security = on" in function


def test_claim_recovery_replays_exhausted_processing_without_overflowing_attempts() -> None:
    """@brief 已耗尽 processing 租约过期后可重放补偿且不再加 attempt / Exhausted processing can replay compensation without another increment."""

    source = MIGRATION.read_text(encoding="utf-8")
    replacement = source[source.index("def _replace_claim_function") :]
    replacement = replacement[: replacement.index("def _secure_retention_function")]

    assert "event.status = 'pending'" in replacement
    assert "event.attempt_count < candidate_maximum_attempts" in replacement
    assert "event.status = 'processing' AND event.lease_expires_at <= effective_now" in replacement
    assert "event.attempt_count >= candidate_maximum_attempts" in replacement
    assert "THEN event.attempt_count ELSE event.attempt_count + 1 END" in replacement
    assert "_replace_claim_function(recover_exhausted_processing=True)" in source
    assert "_replace_claim_function(recover_exhausted_processing=False)" in source


def test_retention_security_is_execute_only_and_owner_rls_is_due_terminal_only() -> None:
    """@brief app 只获窄函数 EXECUTE，owner RLS 仅见过期终态 / App gets execute only and owner RLS sees only expired terminal rows."""

    source = MIGRATION.read_text(encoding="utf-8")

    assert "ALTER FUNCTION {_FUNCTION_SIGNATURE} OWNER TO {owner_role}" in source
    assert "FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}" in source
    assert "GRANT EXECUTE ON FUNCTION {_FUNCTION_SIGNATURE} TO {app_role}" in source
    assert "GRANT DELETE ON TABLE agent.outbox_events" not in source
    assert "status IN ('published', 'failed')" in source
    assert "replay_expires_at <= statement_timestamp()" in source
    assert "USING (true)" in source  # migration-only policy
    runtime_policies = source[source.index("def _install_retention_policies") :]
    runtime_policies = runtime_policies[: runtime_policies.index("def _create_retention_function")]
    assert "USING (true)" not in runtime_policies
    assert "BYPASSRLS" not in source
    assert "DISABLE ROW LEVEL SECURITY" not in source


def test_unsafe_role_fails_before_any_ddl(monkeypatch: pytest.MonkeyPatch) -> None:
    """@brief 非法 role 在任何 DDL 前失败 / An unsafe role fails before any DDL.

    @param monkeypatch pytest monkeypatch fixture / Pytest monkeypatch fixture.
    """

    migration = _load_migration()
    configuration = Mock()
    configuration.get_main_option.side_effect = {
        "aiws.owner_role": 'owner"; DROP SCHEMA agent CASCADE; --',
        "aiws.app_role": "app",
        "aiws.dashboard_role": "dashboard",
        "aiws.migrator_role": "migrator",
    }.get
    operation = Mock()
    operation.get_context.return_value = SimpleNamespace(config=configuration)
    monkeypatch.setattr(migration, "op", operation)

    with pytest.raises(RuntimeError, match="missing or invalid dbctl role option: owner_role"):
        migration.upgrade()
    operation.execute.assert_not_called()
    operation.create_check_constraint.assert_not_called()
