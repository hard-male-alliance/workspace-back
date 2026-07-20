"""@brief Dashboard PostgreSQL 查询契约测试 / Dashboard PostgreSQL query-contract tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dashboard.application.errors import DashboardReadStoreUnavailable
from dashboard.domain.model import SignalKind
from dashboard.infrastructure import postgres


def _normalized(statement: str) -> str:
    """@brief 将 SQL 空白规范化便于断言 / Normalize SQL whitespace for assertions.

    @param statement SQL 文本 / SQL text.
    @return 单空格 SQL / Single-space SQL.
    """

    return " ".join(statement.split())


def test_overview_aggregates_the_complete_occurrence_window_in_sql() -> None:
    """@brief Overview 不得 raw LIMIT 后在 Python 聚合 / Overview never aggregates in Python after a raw-row limit."""

    sql = _normalized(postgres._OVERVIEW_SQL)
    assert "observability.dashboard_signals" in sql
    assert "occurred_at >= :start_at" in sql
    assert "occurred_at < :end_at" in sql
    assert "max(observed_at) AS latest_observed_at" in sql
    assert "max(occurred_at) AS latest_occurred_at" not in sql
    assert (
        "max( GREATEST( EXTRACT(EPOCH FROM (observed_at - occurred_at)), 0.0 ) ) "
        "AS max_collection_lag_seconds"
    ) in sql
    assert "GROUP BY service" in sql
    assert "LIMIT" not in sql
    assert "aiws.http.server.request.count" in sql
    assert "aiws.http.server.error.count" in sql
    assert "http.server.request.duration" in sql
    assert "value * 1000.0" in sql
    assert "aiws.runtime.supervisor.utilization" in sql
    assert "aiws.telemetry.queue.utilization" in sql


def test_trends_use_postgresql_date_bin_without_raw_limit() -> None:
    """@brief 趋势必须由 PostgreSQL date_bin 完整分桶 / Trends must be completely bucketed by PostgreSQL ``date_bin``."""

    statements = {signal: _normalized(sql) for signal, sql in postgres._TREND_SQL.items()}
    assert set(statements) == set(SignalKind)
    for sql in statements.values():
        assert "date_bin(" in sql
        assert "make_interval(secs => :bucket_seconds)" in sql
        assert "occurred_at" in sql
        assert "GROUP BY bucket_start, service" in sql
        assert "LIMIT" not in sql

    assert "percentile_cont" not in statements[SignalKind.TRAFFIC]
    assert "http.server.request.duration" not in statements[SignalKind.TRAFFIC]
    assert "percentile_cont" not in statements[SignalKind.ERRORS]
    assert "value * 1000.0" in statements[SignalKind.LATENCY]
    assert "aiws.http.server.request.count" not in statements[SignalKind.LATENCY]
    assert "percentile_cont" not in statements[SignalKind.SATURATION]
    assert "aiws.telemetry.queue.utilization" in statements[SignalKind.SATURATION]


def test_only_diagnostic_events_have_a_hard_limit() -> None:
    """@brief 仅有界诊断与系统快照读取可使用 LIMIT / Only bounded diagnostics and system snapshots use ``LIMIT``."""

    sql = _normalized(postgres._EVENT_SQL)
    assert "kind IN ('log', 'span')" in sql
    assert "source = 'frontend' AND kind = 'metric'" in sql
    assert "'event'" not in sql
    assert "occurred_at DESC, observed_at DESC" in sql
    assert "severity_number" in sql
    assert "severity_text" in sql
    assert "span_id" in sql
    assert "duration_ms" in sql
    assert "span_status" in sql
    assert "value" in sql
    assert "unit" in sql
    assert "LIMIT :limit" in sql

    system_sql = _normalized(postgres._SYSTEM_HEALTH_SQL)
    assert "workspace_id IS NULL" in system_sql
    assert "aiws.telemetry.health.snapshot" in system_sql
    assert "LIMIT 1" in system_sql


def test_canonical_event_row_preserves_occurrence_and_collection_times() -> None:
    """@brief event row 应保留发生、采集与 trace/span 关联 / Event rows preserve occurrence, collection, and trace/span correlation."""

    occurred_at = datetime(2026, 7, 21, 7, 59, tzinfo=UTC)
    observed_at = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    row = postgres._event_row(
        {
            "occurred_at": occurred_at,
            "observed_at": observed_at,
            "source": "frontend",
            "service": "web",
            "kind": "span",
            "name": "ui.navigation",
            "severity_number": 9,
            "severity_text": "INFO",
            "value": None,
            "unit": None,
            "duration_ms": 12.5,
            "span_status": "ok",
            "request_id": "req",
            "trace_id": "trace",
            "span_id": "span",
            "attributes": {"route": "/jobs"},
        }
    )
    assert row.occurred_at == occurred_at
    assert row.observed_at == observed_at
    assert row.source == "frontend"
    assert row.severity_number == 9
    assert row.span_id == "span"
    assert row.duration_ms == 12.5
    assert row.span_status == "ok"


def test_event_row_rejects_naive_timestamps() -> None:
    """@brief 读模型拒绝无时区时间避免窗口歧义 / Read models reject naive timestamps to avoid window ambiguity."""

    with pytest.raises(DashboardReadStoreUnavailable, match="时区"):
        postgres._event_row(
            {
                "occurred_at": datetime(2026, 7, 21, 7, 59),
                "observed_at": datetime(2026, 7, 21, 8, 0, tzinfo=UTC),
                "source": "backend",
                "service": "backend",
                "kind": "log",
                "name": "test",
                "severity_number": None,
                "severity_text": None,
                "value": None,
                "unit": None,
                "duration_ms": None,
                "span_status": None,
                "request_id": None,
                "trace_id": None,
                "span_id": None,
                "attributes": {},
            }
        )


def test_system_health_row_requires_complete_non_negative_counters() -> None:
    """@brief 系统健康快照只接受完整非负计数 / System-health snapshots require complete non-negative counters."""

    occurred_at = datetime(2026, 7, 21, 7, 59, tzinfo=UTC)
    observed_at = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    row = postgres._system_health_row(
        {
            "occurred_at": occurred_at,
            "observed_at": observed_at,
            "severity_number": 13,
            "severity_text": "WARNING",
            "attributes": {
                "accepted_count": 100,
                "dropped_count": 2,
                "write_failure_count": 1,
                "output_dropped_count": 3,
            },
        }
    )
    assert row.accepted_count == 100
    assert row.dropped_count == 2
    assert row.write_failure_count == 1
    assert row.output_dropped_count == 3

    with pytest.raises(DashboardReadStoreUnavailable, match="dropped_count"):
        postgres._system_health_row(
            {
                "occurred_at": occurred_at,
                "observed_at": observed_at,
                "severity_number": 13,
                "severity_text": "WARNING",
                "attributes": {
                    "accepted_count": 100,
                    "dropped_count": -1,
                    "write_failure_count": 0,
                    "output_dropped_count": 0,
                },
            }
        )
