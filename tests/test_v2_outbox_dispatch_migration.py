"""@brief 统一 outbox 租约 migration 安全门禁 / Unified outbox-lease migration safety gates."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from backend.infrastructure import outbox_dispatch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = (
    PROJECT_ROOT
    / "alembic"
    / "versions"
    / "20260723_0023_v2_outbox_dispatch_leases.py"
)
"""@brief outbox dispatch lease migration / Outbox dispatch lease migration."""


def _load_migration() -> ModuleType:
    """@brief 隔离加载 0023 / Load revision 0023 in isolation."""
    specification = importlib.util.spec_from_file_location(
        "test_20260723_0023_v2_outbox_dispatch_leases",
        MIGRATION,
    )
    if specification is None or specification.loader is None:
        raise AssertionError("无法加载 20260723_0023 migration")
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_revision_follows_interview_persistence_and_recovers_legacy_processing() -> None:
    """@brief 0023 线性承接 Interview 并恢复无租约 processing / 0023 follows Interview and recovers unleased processing."""
    migration = _load_migration()
    source = MIGRATION.read_text(encoding="utf-8")

    assert migration.revision == "20260723_0023"
    assert migration.down_revision == "20260723_0022"
    assert "WHEN status = 'processing' THEN 'pending'" in source
    assert "lease_token_hash" in source
    assert "lease_expires_at" in source
    assert "next_attempt_at" in source
    assert "last_error_code" in source


def test_claim_is_skip_locked_bounded_expiring_and_attempt_limited() -> None:
    """@brief claim 必须跳锁、有界、可恢复且限次 / Claim is skip-locked, bounded, recoverable, and attempt-limited."""
    source = MIGRATION.read_text(encoding="utf-8")
    claim = source[source.index("CREATE FUNCTION agent.claim_outbox_events") :]
    claim = claim[: claim.index("def _create_transition_functions")]

    assert "FOR UPDATE SKIP LOCKED" in claim
    assert "LIMIT candidate_batch_size" in claim
    assert "candidate_batch_size NOT BETWEEN 1 AND 100" in claim
    assert "candidate_lease_seconds NOT BETWEEN 5 AND 900" in claim
    assert "event.attempt_count < candidate_maximum_attempts" in claim
    assert "event.status = 'processing' AND event.lease_expires_at <= effective_now" in claim
    assert "LEAST(candidate_now, statement_timestamp())" in claim
    assert "attempt_count = event.attempt_count + 1" in claim
    assert "candidate_event_types text[] DEFAULT NULL" in claim
    assert "cardinality(candidate_event_types) NOT BETWEEN 1 AND 32" in claim
    assert "event.event_type = ANY(candidate_event_types)" in claim


def test_token_cas_functions_are_fixed_path_owner_owned_and_execute_only() -> None:
    """@brief 所有状态推进都以 token CAS 走 owner-owned 窄函数 / Every transition uses owner-owned token-CAS functions."""
    source = MIGRATION.read_text(encoding="utf-8")

    assert source.count("\n        SECURITY DEFINER\n") == 4
    assert source.count("SET search_path = pg_catalog, agent") == 4
    assert source.count("SET row_security = on") == 4
    assert source.count("event.lease_token_hash = candidate_lease_token_hash") == 3
    assert "ALTER FUNCTION {signature} OWNER TO {owner_role}" in source
    assert "FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}" in source
    assert "GRANT EXECUTE ON FUNCTION {signature} TO {app_role}" in source
    assert "REVOKE UPDATE ON TABLE agent.outbox_events FROM {app_role}" in source


def test_runtime_owner_policies_are_state_bounded() -> None:
    """@brief runtime owner policy 不得全表 bypass RLS / Runtime owner policy cannot bypass the whole table."""
    source = MIGRATION.read_text(encoding="utf-8")
    runtime = source[source.index("def _install_runtime_owner_policies") :]
    runtime = runtime[: runtime.index("def _backfill_and_constrain")]

    assert "FOR SELECT" in runtime
    assert "USING (status IN ('pending', 'processing') OR" in runtime
    assert "status IN ('published', 'failed') AND updated_at = statement_timestamp()" in runtime
    assert "FOR UPDATE" in runtime
    assert "WITH CHECK (status IN ('pending', 'processing', 'published', 'failed'))" in runtime
    assert "USING (true)" not in runtime
    assert "BYPASSRLS" not in source
    assert "DISABLE ROW LEVEL SECURITY" not in source


def test_downgrade_refuses_active_leases_or_retry_evidence() -> None:
    """@brief downgrade 不得丢失活动 claim 或重试证据 / Downgrade cannot discard active claims or retry evidence."""
    source = MIGRATION.read_text(encoding="utf-8")
    downgrade = source[source.index("def downgrade()") :]

    assert "status = 'processing'" in downgrade
    assert "lease_token_hash IS NOT NULL" in downgrade
    assert "lease_expires_at IS NOT NULL" in downgrade
    assert "last_error_code IS NOT NULL" in downgrade
    assert "cannot downgrade 0023" in downgrade


def test_runtime_adapter_uses_only_unscoped_narrow_functions_and_hashes_leases() -> None:
    """@brief adapter 不伪造租户 scope 且不存原始 token / Adapter fabricates no tenant scope and stores no raw token."""
    source = Path(outbox_dispatch.__file__).read_text(encoding="utf-8")

    for function in (
        "agent.claim_outbox_events",
        "agent.renew_outbox_event_lease",
        "agent.complete_outbox_event",
        "agent.retry_outbox_event",
    ):
        assert function in source
    assert "unscoped_transaction" in source
    assert "sha256(" in source
    assert "CAST(:event_types AS text[])" in source
    assert "event_types: frozenset[str]" in source
    assert "set_config(" not in source
    assert "install_v2_request_scope" not in source


def test_runtime_adapter_requires_a_bounded_nonempty_consumer_allowlist() -> None:
    """@brief 每个 repository 实例必须声明有界消费归属 / Every repository instance declares bounded consumer ownership."""
    database = Mock()
    with pytest.raises(ValueError, match="between 1 and 32"):
        outbox_dispatch.PostgresOutboxClaimRepository(
            database,
            event_types=frozenset(),
        )
    with pytest.raises(ValueError, match="invalid event type"):
        outbox_dispatch.PostgresOutboxClaimRepository(
            database,
            event_types=frozenset({"Knowledge Bad Event"}),
        )


def test_upgrade_rejects_unsafe_owner_role_before_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 非法 owner role 在任何 DDL 前失败 / Unsafe owner role fails before any DDL.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
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
    operation.add_column.assert_not_called()
