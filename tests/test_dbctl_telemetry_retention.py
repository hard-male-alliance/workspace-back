"""@brief dbctl 遥测保留领域、用例与 Psycopg 边界测试 / dbctl telemetry-retention domain, use-case, and Psycopg-boundary tests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from conftest import PROJECT_ROOT
from dbctl.application.errors import RetentionExecutionError
from dbctl.application.prune_telemetry import (
    DeleteTelemetryBatch,
    PruneApplied,
    PruneMode,
    PrunePreview,
    PruneRequest,
    PruneTelemetryService,
    RetentionDisabled,
    StaleTelemetryProbe,
)
from dbctl.composition import compose_dbctl
from dbctl.domain.retention import PruneLimits, RetentionPolicy
from dbctl.infrastructure.postgres.telemetry import PsycopgTelemetryRetentionAdapter
from dbctl.interfaces.cli import main
from dbctl.interfaces.presenters import render_prune_outcome


@dataclass
class RecordingTelemetryPort:
    """@brief 不连接 PostgreSQL 的遥测 fake port / Telemetry fake port without PostgreSQL.

    @param batch_results 依次返回删除数 / Deletion counts returned in order.
    @param has_more 最后探测结果 / Final probe result.
    @param delete_commands 已记录删除命令 / Recorded delete commands.
    @param probes 已记录探测 / Recorded probes.
    """

    batch_results: list[int]
    has_more: bool
    delete_commands: list[DeleteTelemetryBatch] = field(default_factory=list)
    probes: list[StaleTelemetryProbe] = field(default_factory=list)

    def delete_batch(self, command: DeleteTelemetryBatch) -> int:
        """@brief 记录并返回预设删除数 / Record and return a configured deletion count.

        @param command 单批命令 / One-batch command.
        @return 下一个预设计数 / Next configured count.
        """

        self.delete_commands.append(command)
        return self.batch_results.pop(0) if self.batch_results else 0

    def has_stale(self, probe: StaleTelemetryProbe) -> bool:
        """@brief 记录并返回预设剩余状态 / Record and return the configured remaining state.

        @param probe 剩余状态探测 / Remaining-state probe.
        @return 预设布尔值 / Configured boolean.
        """

        self.probes.append(probe)
        return self.has_more


@dataclass
class FakeCursor:
    """@brief 最小 Psycopg cursor fake / Minimal Psycopg cursor fake.

    @param rows fetchone 返回行 / Rows returned by fetchone.
    @param rowcount DML 影响行数 / DML affected-row count.
    """

    rows: list[tuple[object, ...]]
    rowcount: int = 0

    def fetchone(self) -> tuple[object, ...] | None:
        """@brief 返回第一行 / Return the first row.

        @return 第一行或 None / First row or None.
        """

        return self.rows[0] if self.rows else None


@dataclass
class RecordingConnection:
    """@brief 记录参数化 SQL 的连接 fake / Connection fake recording parameterized SQL.

    @param deleted_rows DELETE rowcount / DELETE rowcount.
    @param has_more EXISTS 结果 / EXISTS result.
    @param calls SQL 与参数 / SQL and parameters.
    @param transaction_count 短事务数量 / Short-transaction count.
    @param closed 是否关闭 / Whether closed.
    """

    deleted_rows: int = 0
    has_more: bool = False
    calls: list[tuple[str, tuple[object, ...] | None]] = field(default_factory=list)
    transaction_count: int = 0
    closed: bool = False

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """@brief 模拟一个短事务 / Simulate one short transaction.

        @return 空上下文 / Empty context.
        """

        self.transaction_count += 1
        yield

    def execute(
        self,
        sql: str,
        parameters: tuple[object, ...] | None = None,
    ) -> FakeCursor:
        """@brief 记录 SQL 并返回对应 cursor / Record SQL and return the corresponding cursor.

        @param sql SQL 文本 / SQL text.
        @param parameters 参数绑定 / Bound parameters.
        @return 固定 cursor / Fixed cursor.
        """

        self.calls.append((sql, parameters))
        if "DELETE FROM" in sql:
            return FakeCursor([], rowcount=self.deleted_rows)
        if "SELECT EXISTS" in sql:
            return FakeCursor([(self.has_more,)])
        return FakeCursor([])

    def close(self) -> None:
        """@brief 标记连接关闭 / Mark the connection closed.

        @return 无返回值 / No return value.
        """

        self.closed = True


class FailingConnection(RecordingConnection):
    """@brief 会抛出含 DSN 异常的连接 fake / Connection fake raising a DSN-bearing error."""

    def execute(
        self,
        _sql: str,
        _parameters: tuple[object, ...] | None = None,
    ) -> FakeCursor:
        """@brief 抛出必须被隐藏的底层错误 / Raise an underlying error that must be hidden.

        @param _sql SQL 文本 / SQL text.
        @param _parameters 参数 / Parameters.
        @return 永不返回 / Never returns.
        """

        raise RuntimeError(
            "postgresql://workspace_migrator:password-sentinel@unsafe.example/secret"
        )


def _fixed_now() -> datetime:
    """@brief 返回固定 UTC 时间 / Return a fixed UTC time.

    @return 2026-07-15 UTC / 2026-07-15 UTC.
    """

    return datetime(2026, 7, 15, 12, 0, tzinfo=UTC)


def _adapter(config_path: Path) -> PsycopgTelemetryRetentionAdapter:
    """@brief 从强类型配置构建 telemetry adapter / Build a telemetry adapter from typed config.

    @param config_path 已初始化配置 / Initialized config.
    @return Psycopg adapter / Psycopg adapter.
    """

    settings = compose_dbctl(
        config_path,
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    ).settings
    return PsycopgTelemetryRetentionAdapter(
        settings.connections.migrator,
        owner_role=settings.blueprint.roles.owner,
        observability_schema=settings.blueprint.observability_schema,
    )


def test_prune_dry_run_is_disconnected_and_typed() -> None:
    """@brief dry-run 返回专属类型且不调用 port / Dry-run returns its own type without calling the port.

    @return 无返回值 / No return value.
    """

    port = RecordingTelemetryPort(batch_results=[1], has_more=True)
    outcome = PruneTelemetryService(port, clock=_fixed_now).execute(
        PruneRequest(policy=RetentionPolicy(30), limits=PruneLimits())
    )

    assert isinstance(outcome, PrunePreview)
    assert outcome.cutoff == datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
    assert port.delete_commands == []
    assert port.probes == []
    assert "不会连接数据库" in render_prune_outcome(outcome)


def test_retention_zero_disables_apply_without_port_access() -> None:
    """@brief days=0 即使 APPLY 也显式停用 / days=0 explicitly disables even APPLY.

    @return 无返回值 / No return value.
    """

    port = RecordingTelemetryPort(batch_results=[1], has_more=True)
    outcome = PruneTelemetryService(port, clock=_fixed_now).execute(
        PruneRequest(
            policy=RetentionPolicy(0),
            limits=PruneLimits(),
            mode=PruneMode.APPLY,
        )
    )

    assert isinstance(outcome, RetentionDisabled)
    assert port.delete_commands == []
    assert port.probes == []
    assert "未连接数据库" in render_prune_outcome(outcome)


def test_apply_uses_bounded_batches_and_one_fixed_cutoff() -> None:
    """@brief APPLY 使用硬边界和单一 cutoff / APPLY uses hard bounds and one fixed cutoff.

    @return 无返回值 / No return value.
    """

    port = RecordingTelemetryPort(batch_results=[3, 3, 1], has_more=True)
    limits = PruneLimits(batch_size=3, max_batches=5, statement_timeout_ms=1_234)
    outcome = PruneTelemetryService(port, clock=_fixed_now).execute(
        PruneRequest(
            policy=RetentionPolicy(7),
            limits=limits,
            mode=PruneMode.APPLY,
        )
    )

    assert isinstance(outcome, PruneApplied)
    assert outcome.deleted_count == 7
    assert outcome.batch_count == 3
    assert outcome.has_more is True
    assert outcome.reached_batch_limit is False
    assert all(
        command.cutoff == datetime(2026, 7, 8, 12, 0, tzinfo=UTC)
        for command in port.delete_commands
    )
    assert all(command.limits is limits for command in port.delete_commands)
    assert port.probes == [StaleTelemetryProbe(outcome.cutoff, limits)]
    assert "删除 7 条；仍有过期记录" in render_prune_outcome(outcome)


def test_apply_stops_at_configured_batch_budget() -> None:
    """@brief max_batches 是不可突破的硬预算 / max_batches is an inviolable hard budget.

    @return 无返回值 / No return value.
    """

    port = RecordingTelemetryPort(batch_results=[2, 2, 2], has_more=True)
    outcome = PruneTelemetryService(port, clock=_fixed_now).execute(
        PruneRequest(
            policy=RetentionPolicy(1),
            limits=PruneLimits(batch_size=2, max_batches=2),
            mode=PruneMode.APPLY,
        )
    )

    assert isinstance(outcome, PruneApplied)
    assert outcome.deleted_count == 4
    assert outcome.batch_count == 2
    assert outcome.reached_batch_limit is True
    assert len(port.delete_commands) == 2


def test_psycopg_adapter_sets_local_owner_and_binds_values(
    monkeypatch: pytest.MonkeyPatch,
    dbctl_config_path: Path,
) -> None:
    """@brief adapter 每批 SET LOCAL ROLE 且绑定运行时值 / Adapter SET LOCAL ROLE per batch and binds runtime values.

    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    connection = RecordingConnection(deleted_rows=2)
    adapter = _adapter(dbctl_config_path)
    monkeypatch.setattr(adapter, "_connect_safely", lambda: connection)
    cutoff = datetime(2026, 6, 1, tzinfo=UTC)

    deleted = adapter.delete_batch(
        DeleteTelemetryBatch(
            cutoff=cutoff,
            limits=PruneLimits(
                batch_size=4,
                statement_timeout_ms=1_234,
                lock_timeout_ms=500,
            ),
        )
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
    assert "ORDER BY observed_at ASC, ctid ASC" in delete_sql
    assert "FOR UPDATE SKIP LOCKED" in delete_sql
    assert "LIMIT %s" in delete_sql
    assert delete_parameters == (cutoff, 4)
    assert "password" not in delete_sql


def test_psycopg_adapter_hides_driver_secret_and_cause(
    monkeypatch: pytest.MonkeyPatch,
    dbctl_config_path: Path,
) -> None:
    """@brief adapter 隐藏驱动错误与异常链 / Adapter hides driver errors and exception chains.

    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    adapter = _adapter(dbctl_config_path)
    monkeypatch.setattr(adapter, "_connect_safely", FailingConnection)

    with pytest.raises(RetentionExecutionError) as error_info:
        adapter.delete_batch(
            DeleteTelemetryBatch(
                cutoff=datetime(2026, 6, 1, tzinfo=UTC),
                limits=PruneLimits(batch_size=1, statement_timeout_ms=1_000),
            )
        )

    displayed = str(error_info.value)
    assert "password-sentinel" not in displayed
    assert "postgresql://" not in displayed
    assert "DELETE" not in displayed
    assert error_info.value.__cause__ is None


def test_cli_consumes_root_retention_and_defaults_to_dry_run(
    capsys: pytest.CaptureFixture[str],
    dbctl_config_path: Path,
) -> None:
    """@brief CLI 读取根 retention 且默认不连接 / CLI reads root retention and is disconnected by default.

    @param capsys pytest 输出夹具 / pytest output fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    exit_code = main(
        [
            "--config",
            str(dbctl_config_path),
            "--dbinit",
            str(PROJECT_ROOT / "dbinit.jsonc"),
            "prune-telemetry",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "dry-run" in captured.out
    assert "保留 30 天" in captured.out


def test_cli_apply_is_the_only_deletion_opt_in(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    dbctl_config_path: Path,
) -> None:
    """@brief 只有 --apply 构造 APPLY request / Only --apply constructs an APPLY request.

    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @param capsys pytest 输出夹具 / pytest output fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    observed: list[PruneRequest] = []

    def fake_execute(
        _service: PruneTelemetryService,
        request: PruneRequest,
    ) -> PruneApplied:
        """@brief 捕获 CLI 构造的强类型请求 / Capture the typed request constructed by CLI.

        @param _service 用例实例 / Use-case instance.
        @param request 强类型请求 / Typed request.
        @return 可展示执行结果 / Displayable applied result.
        """

        observed.append(request)
        return PruneApplied(
            policy=request.policy,
            limits=request.limits,
            cutoff=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
            batch_count=1,
            deleted_count=2,
            has_more=True,
        )

    monkeypatch.setattr(PruneTelemetryService, "execute", fake_execute)
    exit_code = main(
        [
            "--config",
            str(dbctl_config_path),
            "--dbinit",
            str(PROJECT_ROOT / "dbinit.jsonc"),
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
    assert len(observed) == 1
    assert observed[0].mode is PruneMode.APPLY
    assert observed[0].limits == PruneLimits(
        batch_size=7,
        max_batches=2,
        statement_timeout_ms=2_500,
        lock_timeout_ms=500,
    )
    assert "删除 2 条；仍有过期记录" in captured.out
