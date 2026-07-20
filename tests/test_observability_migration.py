"""@brief observability 0006 迁移契约静态测试 / Static contract tests for observability migration 0006."""

from __future__ import annotations

from pathlib import Path

from backend.infrastructure.persistence.models import TelemetryRecord

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = (
    PROJECT_ROOT
    / "alembic"
    / "versions"
    / "20260721_0006_observability_signal_envelope.py"
)
"""@brief 强类型 observability migration / Typed observability migration."""


def test_migration_contains_lossless_semantic_validation_and_safe_backfill() -> None:
    """@brief migration 必须校验 kind/latency 并清洗 legacy attributes / Migration validates kinds/latency and sanitizes attributes."""
    source = MIGRATION.read_text(encoding="utf-8")
    assert "target_metric_count <> legacy_metric_count" in source
    assert "target_log_count <> legacy_log_count + legacy_span_count" in source
    assert "target_span_count <> 0" in source
    assert "legacy.value / 1000.0" in source
    assert "telemetry latency conversion mismatch" in source
    assert "jsonb_each" in source
    assert "safe_attributes - 'status_code'" in source
    assert "prompt" not in source


def test_migration_exposes_fixed_dashboard_projection_and_minimal_grants() -> None:
    """@brief migration 固定 dashboard_signals 且撤销默认 INSERT / Migration fixes the Dashboard projection and revokes default INSERT."""
    source = MIGRATION.read_text(encoding="utf-8")
    for column in (
        "workspace_id",
        "occurred_at",
        "observed_at",
        "source",
        "service",
        "kind",
        "name",
        "metric_type",
        "value",
        "unit",
        "severity_number",
        "severity_text",
        "duration_ms",
        "span_status",
        "request_id",
        "trace_id",
        "span_id",
        "attributes",
    ):
        assert column in source
    assert "WITH (security_barrier = true)" in source
    assert "REVOKE INSERT ON TABLES" in source
    assert "GRANT INSERT ON observability.telemetry_records" in source
    assert "GRANT SELECT ON observability.dashboard_signals" in source


def test_migration_indexes_match_dashboard_and_retention_queries() -> None:
    """@brief 索引必须以 workspace/time 或 observed_at 起始 / Indexes lead with workspace/time or observed_at."""
    source = MIGRATION.read_text(encoding="utf-8")
    assert '["workspace_id", "occurred_at", "service", "name"]' in source
    assert '["workspace_id", "occurred_at", "observed_at"]' in source
    assert 'postgresql_where=sa.text("trace_id IS NOT NULL")' in source
    assert '"ix_telemetry_observed_at"' in source
    assert (
        '["workspace_id", "resource_owner_id", "actor_id", "client_event_id"]'
        in source
    )


def test_frontend_idempotency_index_uses_complete_actor_scope() -> None:
    """@brief ORM 幂等索引必须隔离完整 ActorScope / ORM idempotency index isolates the full ActorScope."""
    index = next(
        candidate
        for candidate in TelemetryRecord.__table__.indexes
        if candidate.name == "uq_telemetry_frontend_client_event"
    )
    assert tuple(column.name for column in index.columns) == (
        "workspace_id",
        "resource_owner_id",
        "actor_id",
        "client_event_id",
    )


def test_shadow_table_uses_a_collision_free_primary_key_name() -> None:
    """@brief shadow PK 在旧表存续时不得抢占同名 backing index / Shadow PK avoids the live table's backing-index name."""
    source = MIGRATION.read_text(encoding="utf-8")
    temporary_name = 'name="pk_telemetry_records_v2"'
    stable_rename = "RENAME CONSTRAINT pk_telemetry_records_v2 TO pk_telemetry_records"
    assert temporary_name in source
    assert stable_rename in source
    assert source.index(temporary_name) < source.index("op.drop_table") < source.index(stable_rename)


def test_migration_freezes_legacy_writers_before_backfill() -> None:
    """@brief 旧表必须在回填前停写以免切表丢行 / Legacy writes are frozen before backfill to prevent cutover loss."""

    source = MIGRATION.read_text(encoding="utf-8")
    upgrade_source = source[source.index("def upgrade()") :]
    timeout = "SET LOCAL lock_timeout = '30s'"
    lock = "LOCK TABLE observability.telemetry_records IN SHARE ROW EXCLUSIVE MODE"
    assert upgrade_source.index(timeout) < upgrade_source.index(lock)
    assert upgrade_source.index(lock) < upgrade_source.index("_create_v2_table()")
    assert upgrade_source.index(lock) < upgrade_source.index("_backfill_v2()")
