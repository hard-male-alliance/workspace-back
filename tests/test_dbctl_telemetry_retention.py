"""@brief dbctl 遥测保留期清理的纯单元测试 / Pure unit tests for dbctl telemetry-retention pruning."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from conftest import PROJECT_ROOT
from dbctl.cli import main
from dbctl.composition import DbctlComposition
from dbctl.errors import TelemetryRetentionExecutionError
from dbctl.retention import (
    PsycopgTelemetryRetentionRunner,
    TelemetryPruneExecutor,
    TelemetryPruneRequest,
    TelemetryPruneResult,
)


@dataclass
class RecordingTelemetryRunner:
    """@brief 不连接 PostgreSQL 的遥测清理 fake runner / Telemetry-pruning fake runner with no PostgreSQL connection.

    @param batch_results 依次返回的批次删除数 / Batch deletion counts returned in order.
    @param has_more 最后探测是否仍有过期记录 / Whether the final probe finds stale records.
    @param delete_calls 已记录的删除调用 / Recorded deletion calls.
    @param probe_calls 已记录的剩余状态探测 / Recorded remaining-state probes.
    """

    batch_results: list[int]
    has_more: bool
    delete_calls: list[tuple[datetime, int, int, int]] = field(default_factory=list)
    probe_calls: list[tuple[datetime, int, int]] = field(default_factory=list)

    def delete_batch(
        self,
        *,
        cutoff: datetime,
        batch_size: int,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> int:
        """@brief 记录并返回一个预设删除数 / Record and return one configured deletion count.

        @param cutoff 过期边界 / Staleness boundary.
        @param batch_size 本批上限 / This batch's cap.
        @param statement_timeout_ms 语句超时 / Statement timeout.
        @param lock_timeout_ms 锁等待超时 / Lock-wait timeout.
        @return 下一个预设删除数 / Next configured deletion count.
        """
        self.delete_calls.append((cutoff, batch_size, statement_timeout_ms, lock_timeout_ms))
        return self.batch_results.pop(0) if self.batch_results else 0

    def has_stale(
        self,
        *,
        cutoff: datetime,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> bool:
        """@brief 记录并返回预设的剩余状态 / Record and return configured stale state.

        @param cutoff 过期边界 / Staleness boundary.
        @param statement_timeout_ms 语句超时 / Statement timeout.
        @param lock_timeout_ms 锁等待超时 / Lock-wait timeout.
        @return 预设的剩余状态 / Configured stale-row state.
        """
        self.probe_calls.append((cutoff, statement_timeout_ms, lock_timeout_ms))
        return self.has_more


@dataclass
class FakeCursor:
    """@brief 最小 DB-API cursor fake / Minimal DB-API cursor fake.

    @param rows ``fetchall``/``fetchone`` 返回的行 / Rows returned by ``fetchall``/``fetchone``.
    """

    rows: list[tuple[object, ...]]
    rowcount: int = 0

    def fetchall(self) -> list[tuple[object, ...]]:
        """@brief 返回所有 fake 行 / Return all fake rows.

        @return 行副本 / Row copy.
        """
        return list(self.rows)

    def fetchone(self) -> tuple[object, ...] | None:
        """@brief 返回第一行 fake 结果 / Return the first fake result row.

        @return 第一行或 ``None`` / First row or ``None``.
        """
        return self.rows[0] if self.rows else None


@dataclass
class RecordingConnection:
    """@brief 记录参数化数据库调用的 fake 连接 / Fake connection recording parameterized database calls.

    @param deleted_rows DELETE RETURNING 应返回的行数 / Rows returned by DELETE RETURNING.
    @param has_more EXISTS 查询应返回的状态 / Value returned by the EXISTS query.
    @param calls 依序记录的 SQL 与绑定参数 / SQL and bound parameters recorded in order.
    @param transaction_count 已开启短事务数 / Number of short transactions opened.
    @param closed 是否已关闭 / Whether the connection was closed.
    """

    deleted_rows: int = 0
    has_more: bool = False
    calls: list[tuple[str, tuple[object, ...] | None]] = field(default_factory=list)
    transaction_count: int = 0
    closed: bool = False

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """@brief 模拟一个短事务上下文 / Simulate one short transaction context.

        @return 空上下文 / Empty context.
        """
        self.transaction_count += 1
        yield

    def execute(
        self,
        sql: str,
        parameters: tuple[object, ...] | None = None,
    ) -> FakeCursor:
        """@brief 记录 SQL 与绑定参数并返回固定 cursor / Record SQL/bindings and return a fixed cursor.

        @param sql runner 传入的 SQL 文本 / SQL text submitted by runner.
        @param parameters DB-API 绑定参数 / DB-API bound parameters.
        @return 对应语句的 fake cursor / Fake cursor for the statement.
        """
        self.calls.append((sql, parameters))
        if "DELETE FROM" in sql:
            return FakeCursor([], rowcount=self.deleted_rows)
        if "SELECT EXISTS" in sql:
            return FakeCursor([(self.has_more,)])
        return FakeCursor([])

    def close(self) -> None:
        """@brief 标记连接已关闭 / Mark the connection as closed.

        @return 无返回值 / No return value.
        """
        self.closed = True


class FailingConnection:
    """@brief 会在执行 SQL 时抛出含 secret 的异常的 fake / Fake that raises a secret-bearing error on SQL execution."""

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """@brief 模拟短事务 / Simulate a short transaction.

        @return 空上下文 / Empty context.
        """
        yield

    def execute(self, _: str, __: tuple[object, ...] | None = None) -> FakeCursor:
        """@brief 抛出不应透传的底层异常 / Raise an underlying error that must not be exposed.

        @param _ SQL 文本 / SQL text.
        @param __ 绑定参数 / Bound parameters.
        @return 永不返回 / Never returns.
        """
        raise RuntimeError("postgresql://workspace_migrator:password-sentinel@unsafe.example/secret")

    def close(self) -> None:
        """@brief 模拟关闭 / Simulate close.

        @return 无返回值 / No return value.
        """


def _fixed_now() -> datetime:
    """@brief 返回可预测的 UTC 当前时间 / Return a deterministic UTC current time.

    @return 固定 UTC 时间 / Fixed UTC time.
    """
    return datetime(2026, 7, 15, 12, 0, tzinfo=UTC)


def test_prune_dry_run_is_default_and_never_calls_runner() -> None:
    """@brief dry-run 必须不连接、不删除且仍给出可审阅边界 / Dry-run must neither connect nor delete while providing an auditable boundary."""

    runner = RecordingTelemetryRunner(batch_results=[1], has_more=True)
    result = TelemetryPruneExecutor(runner, clock=_fixed_now).execute(
        TelemetryPruneRequest(retention_days=30)
    )

    assert result.applied is False
    assert result.disabled is False
    assert result.cutoff == datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
    assert result.deleted_count == 0
    assert result.has_more is None
    assert runner.delete_calls == []
    assert runner.probe_calls == []
    assert "不会连接数据库" in result.render_operator_summary()


def test_retention_zero_disables_apply_without_database_access() -> None:
    """@brief retention_days=0 必须安全停用，即使请求 --apply / retention_days=0 must safely disable work even with --apply."""

    runner = RecordingTelemetryRunner(batch_results=[1], has_more=True)
    result = TelemetryPruneExecutor(runner, clock=_fixed_now).execute(
        TelemetryPruneRequest(retention_days=0, apply=True)
    )

    assert result.disabled is True
    assert result.applied is False
    assert result.cutoff is None
    assert runner.delete_calls == []
    assert runner.probe_calls == []
    assert "未连接数据库" in result.render_operator_summary()


def test_apply_uses_bounded_batches_and_reports_remaining_state() -> None:
    """@brief apply 仅按上限分批删除并有界探测剩余状态 / Apply uses bounded batches and a bounded remaining-state probe."""

    runner = RecordingTelemetryRunner(batch_results=[3, 3, 1], has_more=True)
    result = TelemetryPruneExecutor(runner, clock=_fixed_now).execute(
        TelemetryPruneRequest(
            retention_days=7,
            batch_size=3,
            max_batches=5,
            statement_timeout_ms=1_234,
            apply=True,
        )
    )

    assert result.applied is True
    assert result.deleted_count == 7
    assert result.batch_count == 3
    assert result.has_more is True
    assert result.reached_batch_limit is False
    assert all(call[1:] == (3, 1_234, 500) for call in runner.delete_calls)
    assert runner.probe_calls == [
        (datetime(2026, 7, 8, 12, 0, tzinfo=UTC), 1_234, 500)
    ]
    assert "删除 7 条；仍有过期记录" in result.render_operator_summary()


def test_apply_stops_at_configured_batch_budget() -> None:
    """@brief max_batches 必须成为硬边界 / max_batches must be a hard boundary."""

    runner = RecordingTelemetryRunner(batch_results=[2, 2, 2], has_more=True)
    result = TelemetryPruneExecutor(runner, clock=_fixed_now).execute(
        TelemetryPruneRequest(
            retention_days=1,
            batch_size=2,
            max_batches=2,
            apply=True,
        )
    )

    assert result.deleted_count == 4
    assert result.batch_count == 2
    assert result.has_more is True
    assert result.reached_batch_limit is True
    assert len(runner.delete_calls) == 2


def test_psycopg_runner_sets_local_owner_and_binds_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief PostgreSQL runner 必须每批 SET LOCAL ROLE 且仅绑定运行时值 / PostgreSQL runner must SET LOCAL ROLE per batch and bind runtime values only.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
    """

    connection = RecordingConnection(deleted_rows=2)
    runner = PsycopgTelemetryRetentionRunner(
        "postgresql://workspace_migrator:password-sentinel@db.example.test/ai_job_workspace",
        owner_role="workspace_owner",
    )
    monkeypatch.setattr(runner, "_connect", lambda: connection)
    cutoff = datetime(2026, 6, 1, tzinfo=UTC)

    deleted = runner.delete_batch(
        cutoff=cutoff,
        batch_size=4,
        statement_timeout_ms=1_234,
        lock_timeout_ms=500,
    )

    assert deleted == 2
    assert connection.transaction_count == 1
    assert connection.closed is True
    assert connection.calls[0] == ('SET LOCAL ROLE "workspace_owner";', None)
    assert connection.calls[1] == (
        "SELECT set_config('statement_timeout', %s, true);",
        ("1234ms",),
    )
    assert connection.calls[2] == (
        "SELECT set_config('lock_timeout', %s, true);",
        ("500ms",),
    )
    delete_sql, delete_parameters = connection.calls[3]
    assert "LIMIT %s" in delete_sql
    assert cutoff.isoformat() not in delete_sql
    assert delete_parameters == (cutoff, 4)
    assert "password-sentinel" not in delete_sql


def test_psycopg_runner_hides_driver_secret_and_raw_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief runner 不得把驱动异常、DSN 或 SQL 原文传给 CLI / Runner must not expose driver errors, DSN, or raw SQL to CLI.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
    """

    runner = PsycopgTelemetryRetentionRunner(
        "postgresql://workspace_migrator:password-sentinel@db.example.test/ai_job_workspace",
        owner_role="workspace_owner",
    )
    monkeypatch.setattr(runner, "_connect", FailingConnection)

    with pytest.raises(TelemetryRetentionExecutionError) as error_info:
        runner.delete_batch(
            cutoff=datetime(2026, 6, 1, tzinfo=UTC),
            batch_size=1,
            statement_timeout_ms=1_000,
            lock_timeout_ms=500,
        )

    displayed = str(error_info.value)
    assert "password-sentinel" not in displayed
    assert "postgresql://" not in displayed
    assert "DELETE" not in displayed


def test_composition_consumes_root_retention_and_cli_defaults_to_dry_run(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief composition 从根配置读取 retention_days，CLI 默认不需要 migrator DSN / Composition reads root retention_days and CLI default needs no migrator DSN.

    @param capsys pytest 标准流捕获夹具 / pytest standard-stream capture fixture.
    """

    composition = DbctlComposition.from_config_path(PROJECT_ROOT / "config.jsonc", environ={})
    result = composition.prune_telemetry()
    assert result.retention_days == 30
    assert result.applied is False

    exit_code = main(["--config", str(PROJECT_ROOT / "config.jsonc"), "prune-telemetry"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "dry-run" in captured.out
    assert "数据库凭证" not in captured.out


def test_cli_apply_is_the_only_deletion_opt_in(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief CLI 只有 --apply 才应向 composition 传递删除授权 / Only --apply must authorize deletion in CLI composition.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
    @param capsys pytest 标准流捕获夹具 / pytest standard-stream capture fixture.
    """

    observed_arguments: list[tuple[bool, int, int, int, int]] = []

    def fake_prune(
        _: DbctlComposition,
        *,
        apply: bool,
        batch_size: int,
        max_batches: int,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> TelemetryPruneResult:
        """@brief 记录 CLI 向 composition 传入的受限参数 / Record bounded arguments passed by CLI to composition.

        @param _ composition 实例 / Composition instance.
        @param apply 删除授权标志 / Deletion authorization flag.
        @param batch_size 单批上限 / Per-batch cap.
        @param max_batches 批次上限 / Batch cap.
        @param statement_timeout_ms 语句超时 / Statement timeout.
        @param lock_timeout_ms 锁等待超时 / Lock-wait timeout.
        @return 可安全打印的伪执行结果 / Safely printable fake execution result.
        """
        observed_arguments.append(
            (apply, batch_size, max_batches, statement_timeout_ms, lock_timeout_ms)
        )
        return TelemetryPruneResult(
            retention_days=30,
            cutoff=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
            applied=apply,
            disabled=False,
            batch_count=1 if apply else 0,
            batch_size=batch_size,
            max_batches=max_batches,
            deleted_count=2 if apply else 0,
            has_more=True if apply else None,
        )

    monkeypatch.setattr(DbctlComposition, "prune_telemetry", fake_prune)
    exit_code = main(
        [
            "--config",
            str(PROJECT_ROOT / "config.jsonc"),
            "prune-telemetry",
            "--apply",
            "--batch-size",
            "7",
            "--max-batches",
            "2",
            "--statement-timeout-ms",
            "2500",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert observed_arguments == [(True, 7, 2, 2_500, 500)]
    assert "删除 2 条；仍有过期记录" in captured.out
