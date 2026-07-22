"""@brief dbctl 操作员交互与安全诊断测试 / dbctl operator UX and safe-diagnostics tests."""

from __future__ import annotations

import hashlib
import stat
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from types import TracebackType
from typing import Any, Final, Self

import pytest

from conftest import PROJECT_ROOT
from dbctl.application import migrate as migration_module
from dbctl.application.errors import (
    ApplicationError,
    BootstrapExecutionError,
    DbctlConfigurationError,
    RetentionExecutionError,
    add_safe_diagnostic_note,
    redact_sensitive_text,
    safe_diagnostic_notes,
    safe_external_cause,
)
from dbctl.application.migrate import MigrationRevision
from dbctl.application.progress import OperationName, ProgressState, ProgressUpdate
from dbctl.application.provision import (
    BootstrapAccessMode,
    BootstrapPlan,
    BootstrapService,
    BootstrapStage,
    StageCondition,
    build_bootstrap_plan,
)
from dbctl.application.prune_telemetry import (
    DeleteTelemetryBatch,
    PruneApplied,
    PruneMode,
    PruneRequest,
    PruneTelemetryService,
    StaleTelemetryProbe,
)
from dbctl.composition import compose_bootstrap
from dbctl.domain.errors import DomainError
from dbctl.domain.names import DatabaseName
from dbctl.domain.retention import PruneLimits, RetentionPolicy
from dbctl.infrastructure.configuration import DbctlConfigStore
from dbctl.infrastructure.postgres.psql import LocalPsqlBootstrapRunnerFactory
from dbctl.interfaces import cli as cli_module
from dbctl.interfaces.cli import main, run
from dbctl.interfaces.console import OperatorConsole
from workspace_shared.jsonc import load_jsonc

_FIXED_NOW = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
"""@brief 确定性遥测清理时钟 / Deterministic telemetry-pruning clock."""

_LEGACY_BOOTSTRAP_DRY_RUN_SHA256: Final[str] = (
    "136ec45a9bdb93176b2c6716a2dc2fd93c692253baeeaeb92ba9fdf82dade35d"
)
"""@brief 基线 dry-run stdout 的逐字节兼容指纹 / Byte-exact compatibility fingerprint for dry-run stdout."""


@dataclass
class OperatorBootstrapRunner:
    """@brief 不连接数据库的完整 bootstrap runner / Full bootstrap runner without database I/O.

    @param access_mode 已解析的实际访问方式 / Resolved administrative-access mode.
    @param database_present 模拟数据库存在状态 / Simulated database-presence state.
    @param stages 已执行阶段 / Executed stages.
    @param enter_failure 可选上下文进入故障 / Optional context-entry failure.
    @param exit_failure 可选上下文退出故障 / Optional context-exit failure.
    @param stage_failure_at 可选的失败阶段序号 / Optional one-based failing stage number.
    @param stage_failure 可选阶段故障 / Optional stage failure.
    @param attempted_stage_count 已尝试的阶段数 / Number of attempted stages.
    """

    access_mode: BootstrapAccessMode = BootstrapAccessMode.PROMPT
    database_present: bool = False
    stages: list[BootstrapStage] = field(default_factory=list)
    enter_failure: BaseException | None = None
    exit_failure: BaseException | None = None
    stage_failure_at: int | None = None
    stage_failure: BaseException | None = None
    attempted_stage_count: int = 0

    def __enter__(self) -> Self:
        """@brief 进入 fake runner 生命周期 / Enter the fake runner lifecycle.

        @return 当前 runner / This runner.
        """

        if self.enter_failure is not None:
            raise self.enter_failure
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """@brief 退出 fake runner 生命周期 / Exit the fake runner lifecycle.

        @param _exception_type 未使用异常类型 / Unused exception type.
        @param _exception 未使用异常 / Unused exception.
        @param _traceback 未使用 traceback / Unused traceback.
        @return 无返回值 / No return value.
        """

        if self.exit_failure is not None:
            raise self.exit_failure

    def database_exists(self, _database: DatabaseName) -> bool:
        """@brief 返回模拟数据库存在状态 / Return simulated database presence.

        @param _database 已验证数据库名 / Validated database name.
        @return 当前存在状态 / Current presence state.
        """

        return self.database_present

    def execute_stage(self, stage: BootstrapStage) -> None:
        """@brief 记录一个已提交阶段 / Record one committed stage.

        @param stage 当前 bootstrap 阶段 / Current bootstrap stage.
        @return 无返回值 / No return value.
        """

        self.attempted_stage_count += 1
        if self.stage_failure_at == self.attempted_stage_count:
            if self.stage_failure is None:
                raise AssertionError("stage_failure_at 需要配套 stage_failure")
            raise self.stage_failure
        self.stages.append(stage)
        if stage.condition is StageCondition.DATABASE_ABSENT:
            self.database_present = True


@dataclass
class OperatorBootstrapFactory:
    """@brief 可注入 open 故障的 bootstrap factory / Bootstrap factory with injectable open failure.

    @param runner 将返回的内存 runner / In-memory runner to return.
    @param open_failure 可选 factory.open 故障 / Optional factory.open failure.
    """

    runner: OperatorBootstrapRunner
    open_failure: BaseException | None = None

    def open(
        self,
        _plan: BootstrapPlan,
        _access_mode: BootstrapAccessMode,
    ) -> OperatorBootstrapRunner:
        """@brief 返回 runner 或传播预设故障 / Return the runner or propagate the configured failure.

        @param _plan 未使用的自足计划 / Unused self-contained plan.
        @param _access_mode 未使用的访问模式 / Unused access mode.
        @return 配置的内存 runner / Configured in-memory runner.
        """

        if self.open_failure is not None:
            raise self.open_failure
        return self.runner


def _operator_bootstrap_plan(config_path: Path) -> BootstrapPlan:
    """@brief 从隔离配置构建完整 bootstrap 计划 / Build a complete plan from isolated configuration.

    @param config_path 已初始化的私密配置 / Initialized private configuration.
    @return 已验证且不执行 I/O 的 bootstrap 计划 / Validated bootstrap plan without execution I/O.
    """

    settings, _service = compose_bootstrap(
        config_path,
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
        environ={},
    )
    return build_bootstrap_plan(settings)


def _render_operator_failure(error: BaseException) -> str:
    """@brief 经正式终端边界渲染测试故障 / Render a test failure through the real console boundary.

    @param error 待渲染异常 / Exception to render.
    @return 无颜色的安全诊断 / Safe diagnostic without color.
    """

    stderr = StringIO()
    OperatorConsole(stderr, no_color=True).failure("bootstrap", error, exit_code=2)
    return stderr.getvalue()


@dataclass
class RecordingProgressSink:
    """@brief 记录应用进度事件的测试 sink / Test sink recording application progress updates.

    @param updates 按发布顺序记录的事件 / Updates in publication order.
    """

    updates: list[ProgressUpdate] = field(default_factory=list)

    def publish(self, update: ProgressUpdate) -> None:
        """@brief 追加一条进度事件 / Append one progress update.

        @param update 已验证事件 / Validated update.
        @return 无返回值 / No return value.
        """

        self.updates.append(update)


class ExplodingProgressSink:
    """@brief 模拟已关闭 stderr 的进度 sink / Progress sink simulating a closed stderr."""

    def publish(self, _update: ProgressUpdate) -> None:
        """@brief 每次发布均模拟 BrokenPipe / Simulate a broken pipe for every update.

        @param _update 未使用进度 / Unused progress update.
        @return 永不返回 / Never returns.
        @raise BrokenPipeError 固定输出故障 / Fixed output failure.
        """

        raise BrokenPipeError("stderr pipe closed")


class TerminalBuffer(StringIO):
    """@brief 被 Rich 识别为真实 TTY 的内存流 / In-memory stream recognized by Rich as a TTY."""

    def isatty(self) -> bool:
        """@brief 声明流是 TTY / Report that the stream is a TTY.

        @return 恒为真 / Always true.
        """

        return True

    def fileno(self) -> int:
        """@brief 返回 stderr 风格文件描述符 / Return a stderr-like file descriptor.

        @return 固定描述符 2 / Fixed descriptor two.
        """

        return 2


@dataclass
class FailingSecondBatchPort:
    """@brief 首批提交后令第二批失败的遥测端口 / Telemetry port failing after one committed batch.

    @param calls 已尝试删除的批次数 / Number of attempted deletion batches.
    """

    calls: int = 0

    def delete_batch(self, command: DeleteTelemetryBatch) -> int:
        """@brief 首批返回满批计数，第二批抛错 / Return one full batch, then fail.

        @param command 固定 cutoff 与护栏 / Fixed cutoff and guardrails.
        @return 首批已提交行数 / Rows committed by the first batch.
        @raise RetentionExecutionError 第二批固定失败 / The second batch always fails.
        """

        self.calls += 1
        if self.calls == 1:
            return command.limits.batch_size
        raise RetentionExecutionError("模拟第二批删除失败。")

    def has_stale(self, _probe: StaleTelemetryProbe) -> bool:
        """@brief 防止失败后错误探测 / Guard against probing after failure.

        @param _probe 未使用探测 / Unused probe.
        @return 永不返回 / Never returns.
        @raise AssertionError 若用例违反 fail-fast 契约 / If the use case violates fail-fast semantics.
        """

        raise AssertionError("批次失败后不得执行剩余状态探测")


@dataclass
class CommittedTelemetryPort:
    """@brief 提交一个非满批次的遥测端口 / Telemetry port committing one partial batch.

    @param committed_rows 已提交记录数 / Committed row count.
    """

    committed_rows: int = 0

    def delete_batch(self, _command: DeleteTelemetryBatch) -> int:
        """@brief 提交一条记录 / Commit one record.

        @param _command 删除命令 / Deletion command.
        @return 固定返回一 / Always one.
        """

        self.committed_rows += 1
        return 1

    def has_stale(self, _probe: StaleTelemetryProbe) -> bool:
        """@brief 报告没有剩余过期记录 / Report no remaining stale rows.

        @param _probe 剩余状态探测 / Remaining-state probe.
        @return 恒为假 / Always false.
        """

        return False


@pytest.mark.parametrize(
    ("arguments", "expected"),
    (
        (["--help"], "推荐工作流"),
        (["bootstrap", "--help"], "成功后运行"),
        (["migrate", "--help"], "受控变更窗口"),
        (["prune-telemetry", "--help"], "运维影响"),
        (["shell", "--help"], "临时 0600 PGPASSFILE"),
    ),
)
def test_help_exposes_workflows_without_configuration_io(
    arguments: list[str],
    expected: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief 帮助直接给出安全工作流且不读取配置 / Help exposes safe workflows without config I/O.

    @param arguments help 参数 / Help arguments.
    @param expected 每个帮助页的关键提示 / Key guidance expected on each help page.
    @param capsys pytest 输出夹具 / Pytest output fixture.
    @return 无返回值 / No return value.
    """

    with pytest.raises(SystemExit) as exit_info:
        main(arguments)
    captured = capsys.readouterr()
    assert exit_info.value.code == 0
    assert expected in captured.out
    assert "示例" in captured.out or "推荐工作流" in captured.out
    assert captured.err == ""


def test_run_routes_argument_errors_to_injected_stderr() -> None:
    """@brief run 将参数错误写入注入流 / run routes argument errors to its injected stream.

    @return 无返回值 / No return value.
    """

    stdout = StringIO()
    stderr = StringIO()

    with pytest.raises(SystemExit) as exit_info:
        run(["migrat"], stdout=stdout, stderr=stderr)

    assert exit_info.value.code == 2
    assert stdout.getvalue() == ""
    assert "usage: dbctl" in stderr.getvalue()
    assert "migrate" in stderr.getvalue()


def test_bootstrap_dry_run_reports_local_initialization_side_effects(tmp_path: Path) -> None:
    """@brief dry-run 明示本地配置写入、凭据生成与权限 / Dry-run reports local writes, credentials, and mode.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @return 无返回值 / No return value.
    """

    config_path = tmp_path / "config.jsonc"
    stdout = StringIO()
    stderr = StringIO()
    exit_code = run(
        [
            "bootstrap",
            "--dry-run",
            "--no-color",
            "--config",
            str(config_path),
            "--dbinit",
            str(PROJECT_ROOT / "dbinit.jsonc"),
        ],
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 0
    assert config_path.is_file()
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
    assert "不执行任何 SQL" in stdout.getvalue()
    assert (
        hashlib.sha256(stdout.getvalue().encode()).hexdigest() == _LEGACY_BOOTSTRAP_DRY_RUN_SHA256
    )
    assert "本地配置仍可初始化" in stderr.getvalue()
    assert "私密运行配置已创建" in stderr.getvalue()
    assert "生成凭据=3 组" in stderr.getvalue()
    assert "权限=0600" in stderr.getvalue()
    assert "\x1b" not in stdout.getvalue() + stderr.getvalue()


def test_bootstrap_cli_reports_each_committed_stage(
    dbctl_config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief bootstrap 从命令意图报告到每个已提交阶段 / Bootstrap reports intent through committed stages.

    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    runner = OperatorBootstrapRunner()

    def open_without_postgres(
        _factory: LocalPsqlBootstrapRunnerFactory,
        _plan: BootstrapPlan,
        access_mode: BootstrapAccessMode,
    ) -> OperatorBootstrapRunner:
        """@brief 返回无 I/O runner 并核验显式模式 / Return a no-I/O runner and verify explicit mode.

        @param _factory 被替换 factory / Patched factory.
        @param _plan 自足 bootstrap 计划 / Self-contained bootstrap plan.
        @param access_mode CLI 解析后的访问模式 / Access mode parsed by the CLI.
        @return 共享 fake runner / Shared fake runner.
        """

        assert access_mode is BootstrapAccessMode.PROMPT
        return runner

    monkeypatch.setattr(LocalPsqlBootstrapRunnerFactory, "open", open_without_postgres)
    stdout = StringIO()
    stderr = StringIO()
    exit_code = run(
        [
            "bootstrap",
            "--access-mode",
            "prompt",
            "--no-color",
            "--config",
            str(dbctl_config_path),
            "--dbinit",
            str(PROJECT_ROOT / "dbinit.jsonc"),
        ],
        stdout=stdout,
        stderr=stderr,
    )

    progress = stderr.getvalue()
    assert exit_code == 0
    assert len(runner.stages) == 5
    assert stdout.getvalue() == (
        "dbctl bootstrap 完成：目标数据库已创建；执行了 126 条计划 SQL。\n"
    )
    assert "实际模式=prompt" in progress
    assert "bootstrap [1/5] · 开始" in progress
    assert "bootstrap [5/5] · 完成" in progress
    assert "本阶段已提交" in progress
    assert "\x1b" not in stdout.getvalue() + progress


def test_bootstrap_factory_open_failure_reports_no_committed_stage_safely(
    dbctl_config_path: Path,
) -> None:
    """@brief factory.open 故障明确报告零阶段且保持异常链 / Open failure reports zero stages and preserves its chain.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    private_message = "factory-open-secret-sentinel"
    private_note = "factory-open-note-sentinel"
    root_cause = ValueError("factory-open-cause-secret-sentinel")
    failure = RuntimeError(private_message)
    failure.add_note(private_note)
    failure.__cause__ = root_cause
    progress = RecordingProgressSink()
    service = BootstrapService(
        OperatorBootstrapFactory(OperatorBootstrapRunner(), open_failure=failure),
        progress=progress,
    )

    with pytest.raises(RuntimeError) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    diagnostic = _render_operator_failure(error_info.value)
    assert error_info.value is failure
    assert error_info.value.__cause__ is root_cause
    assert len(failures) == 1
    assert failures[0].message == "创建 PostgreSQL bootstrap runner 失败"
    assert failures[0].detail is not None
    assert "尚未调用任何计划阶段" in failures[0].detail
    assert "创建 PostgreSQL bootstrap runner 失败" in notes
    assert private_message not in notes + diagnostic
    assert private_note not in notes + diagnostic
    assert "factory-open-cause-secret-sentinel" not in notes + diagnostic


def test_bootstrap_context_enter_failure_reports_no_committed_stage_safely(
    dbctl_config_path: Path,
) -> None:
    """@brief __enter__ 故障在阶段执行前产生单一安全失败 / Entry failure emits one safe failure before stages.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    private_message = "context-enter-secret-sentinel"
    private_note = "context-enter-note-sentinel"
    failure = RuntimeError(private_message)
    failure.add_note(private_note)
    runner = OperatorBootstrapRunner(enter_failure=failure)
    progress = RecordingProgressSink()
    service = BootstrapService(OperatorBootstrapFactory(runner), progress=progress)

    with pytest.raises(RuntimeError) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    diagnostic = _render_operator_failure(error_info.value)
    assert error_info.value is failure
    assert runner.attempted_stage_count == 0
    assert len(failures) == 1
    assert failures[0].message == "进入 PostgreSQL bootstrap runner 失败"
    assert failures[0].detail is not None
    assert "未有数据库阶段被本用例报告为已提交" in failures[0].detail
    assert "进入 PostgreSQL bootstrap runner 失败" in notes
    assert private_message not in notes + diagnostic
    assert private_note not in notes + diagnostic


def test_bootstrap_access_mode_property_failure_reports_no_committed_stage_safely(
    dbctl_config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief access_mode descriptor 故障也属于进入前生命周期 / Access lookup fails before entry safely.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    runner = OperatorBootstrapRunner()
    failure = RuntimeError("access-mode-property-secret-sentinel")

    def fail_access_mode(_runner: OperatorBootstrapRunner) -> BootstrapAccessMode:
        """@brief 模拟不可信 runner descriptor 故障 / Simulate an untrusted runner descriptor failure.

        @param _runner 未使用 runner / Unused runner.
        @return 永不返回 / Never returns.
        @raise RuntimeError 固定测试故障 / Fixed test failure.
        """

        raise failure

    monkeypatch.setattr(OperatorBootstrapRunner, "access_mode", property(fail_access_mode))
    progress = RecordingProgressSink()
    service = BootstrapService(OperatorBootstrapFactory(runner), progress=progress)

    with pytest.raises(RuntimeError) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    diagnostic = _render_operator_failure(error_info.value)
    assert error_info.value is failure
    assert runner.attempted_stage_count == 0
    assert len(failures) == 1
    assert failures[0].message == "读取 PostgreSQL 管理访问方式失败"
    assert "尚未调用任何计划阶段" in notes
    assert "access-mode-property-secret-sentinel" not in notes + diagnostic


def test_bootstrap_rejects_unresolved_runner_access_mode(
    dbctl_config_path: Path,
) -> None:
    """@brief runner 不得把 auto 当作已解析访问模式 / A runner must not report auto as resolved access.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    runner = OperatorBootstrapRunner(access_mode=BootstrapAccessMode.AUTO)
    progress = RecordingProgressSink()
    service = BootstrapService(OperatorBootstrapFactory(runner), progress=progress)

    with pytest.raises(BootstrapExecutionError) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    assert runner.attempted_stage_count == 0
    assert len(failures) == 1
    assert failures[0].message == "读取 PostgreSQL 管理访问方式失败"
    assert "尚未调用任何计划阶段" in notes


def test_bootstrap_recovers_stage_failure_suppressed_by_runner(
    dbctl_config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 违约 runner 不得抑制阶段失败 / A contract-breaking runner cannot suppress a stage failure.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    stage_failure = RuntimeError("suppressed-stage-secret-sentinel")
    runner = OperatorBootstrapRunner(stage_failure_at=2, stage_failure=stage_failure)

    def suppress_failure(
        _runner: OperatorBootstrapRunner,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> bool:
        """@brief 模拟违反端口契约的 __exit__ / Simulate a contract-breaking ``__exit__``.

        @param _runner 未使用 runner / Unused runner.
        @param _exception_type 未使用异常类型 / Unused exception type.
        @param _exception 未使用异常 / Unused exception.
        @param _traceback 未使用 traceback / Unused traceback.
        @return 恒为真以错误抑制异常 / Always true to suppress the failure incorrectly.
        """

        return True

    monkeypatch.setattr(OperatorBootstrapRunner, "__exit__", suppress_failure)
    progress = RecordingProgressSink()
    service = BootstrapService(OperatorBootstrapFactory(runner), progress=progress)

    with pytest.raises(RuntimeError) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    assert error_info.value is stage_failure
    assert len(failures) == 2
    assert failures[-1].message == "PostgreSQL bootstrap runner 错误抑制了阶段失败"
    assert "runner 违反端口契约" in notes


def test_bootstrap_interrupt_reports_partial_state_conservatively(
    dbctl_config_path: Path,
) -> None:
    """@brief 阶段中断保留先前计数并声明当前状态未知 / Interrupt preserves prior counts conservatively.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    interrupt = KeyboardInterrupt()
    runner = OperatorBootstrapRunner(stage_failure_at=2, stage_failure=interrupt)
    progress = RecordingProgressSink()
    service = BootstrapService(OperatorBootstrapFactory(runner), progress=progress)

    with pytest.raises(KeyboardInterrupt) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    assert error_info.value is interrupt
    assert len(runner.stages) == 1
    assert len(failures) == 1
    assert failures[0].current == 2
    assert failures[0].detail is not None
    assert "此前已完成 1 个阶段" in failures[0].detail
    assert "当前阶段完成状态未知" in failures[0].detail
    assert "后续阶段未执行" in notes


def test_bootstrap_interrupt_cannot_be_suppressed_by_runner(
    dbctl_config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 违约 runner 不能把阶段中断变成部分成功 / A runner cannot suppress an interrupt into success.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    interrupt = KeyboardInterrupt()
    runner = OperatorBootstrapRunner(stage_failure_at=2, stage_failure=interrupt)

    def suppress_interrupt(
        _runner: OperatorBootstrapRunner,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> bool:
        """@brief 模拟错误抑制中断的 ``__exit__`` / Simulate ``__exit__`` suppressing an interrupt.

        @param _runner 未使用 runner / Unused runner.
        @param _exception_type 未使用异常类型 / Unused exception type.
        @param _exception 未使用异常 / Unused exception.
        @param _traceback 未使用 traceback / Unused traceback.
        @return 恒为真 / Always true.
        """

        return True

    monkeypatch.setattr(OperatorBootstrapRunner, "__exit__", suppress_interrupt)
    progress = RecordingProgressSink()
    service = BootstrapService(OperatorBootstrapFactory(runner), progress=progress)

    with pytest.raises(KeyboardInterrupt) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    assert error_info.value is interrupt
    assert len(failures) == 2
    assert failures[-1].message == "PostgreSQL bootstrap runner 错误抑制了阶段失败"


def test_cancelled_console_shows_only_dbctl_authored_impact_notes() -> None:
    """@brief 取消摘要展示安全影响但隐藏原生 Python note / Cancellation shows only safe impact notes.

    @return 无返回值 / No return value.
    """

    native_secret = "cancel-native-note-secret-sentinel"
    interrupt = KeyboardInterrupt()
    interrupt.add_note(native_secret)
    add_safe_diagnostic_note(interrupt, "运维影响：当前阶段完成状态未知。")
    stderr = StringIO()

    OperatorConsole(stderr, no_color=True).cancelled("bootstrap", interrupt)

    diagnostic = stderr.getvalue()
    assert "退出码 130" in diagnostic
    assert "当前阶段完成状态未知" in diagnostic
    assert native_secret not in diagnostic


def test_bootstrap_context_exit_failure_reports_all_committed_stages_safely(
    dbctl_config_path: Path,
) -> None:
    """@brief __exit__ 故障不抹去全部已提交阶段 / Exit failure retains the complete committed-stage impact.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    private_message = "context-exit-secret-sentinel"
    private_note = "context-exit-note-sentinel"
    failure = RuntimeError(private_message)
    failure.add_note(private_note)
    runner = OperatorBootstrapRunner(exit_failure=failure)
    progress = RecordingProgressSink()
    service = BootstrapService(OperatorBootstrapFactory(runner), progress=progress)

    with pytest.raises(RuntimeError) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    diagnostic = _render_operator_failure(error_info.value)
    statement_count = sum(len(stage.statements) for stage in plan.stages)
    assert error_info.value is failure
    assert len(runner.stages) == len(plan.stages)
    assert len(failures) == 1
    assert failures[0].message == "退出 PostgreSQL bootstrap runner 失败"
    assert failures[0].detail is not None
    assert f"已提交 {len(plan.stages)} 个阶段" in failures[0].detail
    assert f"执行 {statement_count} 条计划 SQL" in failures[0].detail
    assert "所有需执行阶段均已报告提交" in notes
    assert "runner 清理状态未确认" in notes
    assert private_message not in notes + diagnostic
    assert private_note not in notes + diagnostic


def test_bootstrap_stage_and_exit_failures_are_reported_once_each(
    dbctl_config_path: Path,
) -> None:
    """@brief 阶段与退出双故障各报告一次并保留 context / Stage and exit failures each report once with context intact.

    @param dbctl_config_path 隔离私密配置 / Isolated private configuration.
    @return 无返回值 / No return value.
    """

    plan = _operator_bootstrap_plan(dbctl_config_path)
    stage_failure = RuntimeError("stage-failure-secret-sentinel")
    stage_failure.add_note("stage-failure-note-sentinel")
    exit_failure = RuntimeError("dual-exit-secret-sentinel")
    exit_failure.add_note("dual-exit-note-sentinel")
    runner = OperatorBootstrapRunner(
        exit_failure=exit_failure,
        stage_failure_at=2,
        stage_failure=stage_failure,
    )
    progress = RecordingProgressSink()
    service = BootstrapService(OperatorBootstrapFactory(runner), progress=progress)

    with pytest.raises(RuntimeError) as error_info:
        service.execute(plan)

    failures = [update for update in progress.updates if update.state is ProgressState.FAILED]
    exit_notes = "\n".join(safe_diagnostic_notes(error_info.value))
    stage_notes = "\n".join(safe_diagnostic_notes(stage_failure))
    diagnostic = _render_operator_failure(error_info.value)
    assert error_info.value is exit_failure
    assert error_info.value.__context__ is stage_failure
    assert len(failures) == 2
    assert sum(update.message == plan.stages[1].label for update in failures) == 1
    assert failures[0].current == 2
    assert failures[0].total == len(plan.stages)
    assert failures[1].message == ("传播阶段失败时退出 PostgreSQL bootstrap runner 又失败")
    assert failures[1].detail is not None
    assert "阶段失败已经单独报告" in failures[1].detail
    assert "此前已完成 1 个阶段" in exit_notes
    assert f"执行 {len(plan.stages[0].statements)} 条计划 SQL" in exit_notes
    assert plan.stages[1].label in stage_notes
    for private_value in (
        "stage-failure-secret-sentinel",
        "stage-failure-note-sentinel",
        "dual-exit-secret-sentinel",
        "dual-exit-note-sentinel",
    ):
        assert private_value not in exit_notes + stage_notes + diagnostic


def test_migration_failure_prints_real_but_secret_safe_traceback(
    dbctl_config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief migration 展示底层栈帧但隐藏 DSN 与 locals / Migration shows frames but hides DSNs and locals.

    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    root = load_jsonc(dbctl_config_path)
    private_dsn = str(root["database"]["migrator_dsn"])
    locals_only_secret = private_dsn + "-locals-only"
    opaque_driver_secret = "opaque-driver-row-data-sentinel"

    def fail_with_private_dsn(configuration: Any, _revision: str) -> None:
        """@brief 模拟回显私密 DSN 的 Alembic 故障 / Simulate an Alembic failure echoing a private DSN.

        @param configuration Alembic 内存配置 / Alembic in-memory configuration.
        @param _revision 未使用 revision / Unused revision.
        @return 永不返回 / Never returns.
        """

        local_value = locals_only_secret
        assert local_value
        raise ValueError(
            f"{configuration.attributes['aiws.migration_dsn']}; {opaque_driver_secret}"
        )

    monkeypatch.setattr("alembic.command.upgrade", fail_with_private_dsn)
    stdout = StringIO()
    stderr = StringIO()
    exit_code = run(
        [
            "migrate",
            "--revision",
            "head",
            "--no-color",
            "--config",
            str(dbctl_config_path),
            "--dbinit",
            str(PROJECT_ROOT / "dbinit.jsonc"),
        ],
        stdout=stdout,
        stderr=stderr,
    )

    diagnostic = stderr.getvalue()
    assert exit_code == 2
    assert stdout.getvalue() == ""
    assert "Traceback（已脱敏；未读取 locals/源码行/外部异常正文）" in diagnostic
    assert 'File "dbctl/infrastructure/alembic.py"' in diagnostic
    assert "<external frame hidden>" in diagnostic
    assert "ExternalDiagnosticError" in diagnostic
    assert "MigrationExecutionError" in diagnostic
    assert "运维影响：migration 未报告完成" in diagnostic
    assert private_dsn not in diagnostic
    assert locals_only_secret not in diagnostic
    assert opaque_driver_secret not in diagnostic
    assert "postgresql://" not in diagnostic


def test_unexpected_failure_keeps_type_and_frames_but_hides_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 未预期异常保留类型与栈而隐藏原始正文 / Unexpected failures retain type and frames, not messages.

    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    private_value = "unexpected-secret-sentinel"
    untrusted_note = "opaque-note-secret-sentinel"

    def fail_composition(*_arguments: object, **_keywords: object) -> object:
        """@brief 在组合根注入未知故障 / Inject an unknown failure at the composition root.

        @param _arguments 未使用位置参数 / Unused positional arguments.
        @param _keywords 未使用关键字参数 / Unused keyword arguments.
        @return 永不返回 / Never returns.
        """

        sensitive_runtime_value = private_value
        failure = RuntimeError(sensitive_runtime_value)
        failure.add_note(untrusted_note)
        raise failure

    monkeypatch.setattr(cli_module, "compose_prune_telemetry", fail_composition)
    stdout = StringIO()
    stderr = StringIO()
    exit_code = run(
        ["prune-telemetry", "--quiet", "--no-color"],
        stdout=stdout,
        stderr=stderr,
    )

    diagnostic = stderr.getvalue()
    assert exit_code == 1
    assert stdout.getvalue() == ""
    assert "未预期的 builtins.RuntimeError" in diagnostic
    assert 'File "dbctl/interfaces/cli.py"' in diagnostic
    assert "<external frame hidden>" in diagnostic
    assert "builtins.RuntimeError: <原始异常消息已隐藏" in diagnostic
    assert private_value not in diagnostic
    assert untrusted_note not in diagnostic


def test_safe_traceback_omits_source_lines_that_can_contain_literal_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief traceback 不回显可能嵌入 secret 的源码行 / Tracebacks omit secret-bearing source lines.

    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    def fail_from_literal_source(*_arguments: object, **_keywords: object) -> object:
        """@brief 在源码行放置不可推断的 secret / Place an opaque secret in source text.

        @param _arguments 未使用位置参数 / Unused positional arguments.
        @param _keywords 未使用关键字参数 / Unused keyword arguments.
        @return 永不返回 / Never returns.
        """

        raise RuntimeError("literal-source-secret-sentinel")

    monkeypatch.setattr(cli_module, "compose_prune_telemetry", fail_from_literal_source)
    stderr = StringIO()

    exit_code = run(
        ["prune-telemetry", "--quiet", "--no-color"],
        stdout=StringIO(),
        stderr=stderr,
    )

    diagnostic = stderr.getvalue()
    assert exit_code == 1
    assert 'File "dbctl/interfaces/cli.py"' in diagnostic
    assert "<external frame hidden>" in diagnostic
    assert "literal-source-secret-sentinel" not in diagnostic


def test_safe_traceback_never_calls_unknown_exception_hooks(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief 安全回溯不执行未知异常的文本或反射钩子 / Safe tracebacks never execute unknown error hooks.

    @param capsys pytest 标准流捕获器 / Pytest standard-stream capture.
    @return 无返回值 / No return value.
    """

    hook_secret = "exception-hook-secret-sentinel"
    calls = {
        "str": 0,
        "dir": 0,
        "dict": 0,
        "class": 0,
        "meta_get": 0,
        "meta_hash": 0,
        "meta_subclass": 0,
    }

    class HookedErrorMeta(type):
        """@brief 任意元类反射都会产生副作用 / Metaclass reflection has observable side effects."""

        def __getattribute__(cls, name: str) -> object:
            """@brief 记录禁止的元类属性读取 / Record forbidden metaclass attribute reads.

            @param name 属性名 / Attribute name.
            @return 标准 type 查找结果 / Standard ``type`` lookup result.
            """

            if name in {"__mro__", "__bases__", "__class__"}:
                calls["meta_get"] += 1
                print(hook_secret, file=sys.stderr)
            return type.__getattribute__(cls, name)

        def __hash__(cls) -> int:
            """@brief 记录禁止的动态类型哈希 / Record forbidden dynamic-type hashing.

            @return 标准类型哈希 / Standard type hash.
            """

            calls["meta_hash"] += 1
            print(hook_secret, file=sys.stderr)
            return type.__hash__(cls)

        def __subclasscheck__(cls, subclass: type[object]) -> bool:
            """@brief 记录禁止的动态 subclass hook / Record a forbidden subclass hook.

            @param subclass 候选子类 / Candidate subclass.
            @return 标准 subclass 判断 / Standard subclass decision.
            """

            calls["meta_subclass"] += 1
            print(hook_secret, file=sys.stderr)
            return type.__subclasscheck__(cls, subclass)

    class HookedRuntimeError(RuntimeError, metaclass=HookedErrorMeta):
        """@brief 任意钩子会产生副作用的未知异常 / Unknown error with side-effecting hooks."""

        def __str__(self) -> str:
            """@brief 记录不应发生的正文读取 / Record a forbidden message read.

            @return 含 secret 的未知正文 / Untrusted text containing a secret.
            """

            calls["str"] += 1
            print(hook_secret, file=sys.stderr)
            return hook_secret

        def __dir__(self) -> list[str]:
            """@brief 记录不应发生的属性探测 / Record forbidden attribute discovery.

            @return 含 secret 的伪造属性 / Forged attributes containing a secret.
            """

            calls["dir"] += 1
            print(hook_secret, file=sys.stderr)
            return [hook_secret]

        @property
        def __dict__(self) -> dict[str, object]:
            """@brief 记录不应发生的 namespace descriptor 读取 / Record forbidden namespace access.

            @return 含 secret 的伪 namespace / Forged namespace containing a secret.
            """

            calls["dict"] += 1
            print(hook_secret, file=sys.stderr)
            return {"_dbctl_safe_diagnostic_notes": hook_secret}

        @property
        def __class__(self) -> type[object]:
            """@brief 记录不应发生的伪类型读取 / Record forbidden forged-type access.

            @return 伪造 OSError 类型 / Forged ``OSError`` type.
            """

            calls["class"] += 1
            print(hook_secret, file=sys.stderr)
            return OSError

    failure = HookedRuntimeError(hook_secret)
    add_safe_diagnostic_note(failure, "dbctl 构造的可信诊断。")
    stderr = StringIO()

    OperatorConsole(stderr, no_color=True).failure("migrate", failure, exit_code=1)

    ambient = capsys.readouterr()
    assert calls == {
        "str": 0,
        "dir": 0,
        "dict": 0,
        "class": 0,
        "meta_get": 0,
        "meta_hash": 0,
        "meta_subclass": 0,
    }
    assert ambient.out == ""
    assert ambient.err == ""
    assert hook_secret not in stderr.getvalue()
    assert "builtins.RuntimeError" in stderr.getvalue()
    assert "dbctl 构造的可信诊断" in stderr.getvalue()


def test_external_application_error_subclass_cannot_approve_its_message() -> None:
    """@brief 外部子类不能借内部基类放行正文 / An external subclass cannot approve its own text.

    @return 无返回值 / No return value.
    """

    private_message = "forged-application-error-secret-sentinel"

    class ForgedApplicationError(ApplicationError):
        """@brief 模拟第三方伪装内部错误 / Simulate a third party impersonating an internal error."""

    try:
        raise ForgedApplicationError(private_message)
    except ForgedApplicationError as failure:
        stderr = StringIO()
        OperatorConsole(stderr, no_color=True).failure("migrate", failure, exit_code=1)

    diagnostic = stderr.getvalue()
    assert "builtins.RuntimeError" in diagnostic
    assert "原始异常消息已隐藏" in diagnostic
    assert private_message not in diagnostic


def test_configuration_domain_bridge_never_formats_untrusted_subclass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief 配置边界不把未知 DomainError 正文升级为可信 / Config does not promote unknown text.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @param capsys pytest 标准流捕获器 / Pytest standard-stream capture.
    @return 无返回值 / No return value.
    """

    private_message = "forged-domain-error-secret-sentinel"
    calls = {"str": 0}

    class ForgedDomainError(DomainError):
        """@brief 带副作用正文的第三方领域错误 / Third-party domain error with side effects."""

        def __str__(self) -> str:
            """@brief 记录禁止的正文读取 / Record a forbidden message read.

            @return 含 secret 的正文 / Text containing a secret.
            """

            calls["str"] += 1
            print(private_message, file=sys.stderr)
            return private_message

    def fail_load(_store: DbctlConfigStore, *, initialize: bool) -> object:
        """@brief 在配置装配内部注入未知领域错误 / Inject an unknown domain error during assembly.

        @param _store 未使用配置存储 / Unused configuration store.
        @param initialize 未使用副作用模式 / Unused side-effect mode.
        @return 永不返回 / Never returns.
        """

        del initialize
        raise ForgedDomainError(private_message)

    monkeypatch.setattr(DbctlConfigStore, "_load", fail_load)
    store = DbctlConfigStore(tmp_path / "config.jsonc", tmp_path / "dbinit.jsonc")

    with pytest.raises(DbctlConfigurationError) as error_info:
        store.load()

    diagnostic = _render_operator_failure(error_info.value)
    ambient = capsys.readouterr()
    assert calls == {"str": 0}
    assert ambient.out == ""
    assert ambient.err == ""
    assert "数据库配置违反领域约束" in diagnostic
    assert private_message not in diagnostic


def test_safe_traceback_hides_untrusted_frame_and_type_metadata() -> None:
    """@brief 回溯隐藏可伪造的文件、函数与动态类名 / Tracebacks hide forged frame and class metadata.

    @return 无返回值 / No return value.
    """

    metadata_secret = "opaque-frame-and-type-secret-sentinel"
    hostile_type = type(metadata_secret, (RuntimeError,), {"__module__": metadata_secret})
    namespace = {"failure_type": hostile_type}
    source = "raise failure_type('message-secret-sentinel')"
    forged_filename = f"/tmp/external.py\nFORGED-SUCCESS {metadata_secret}"
    try:
        exec(compile(source, forged_filename, "exec"), namespace)
    except RuntimeError as failure:
        stderr = StringIO()
        OperatorConsole(stderr, no_color=True).failure("migrate", failure, exit_code=1)
    else:  # pragma: no cover - compile/exec 必须抛出上述异常。
        pytest.fail("forged traceback fixture did not raise")

    diagnostic = stderr.getvalue()
    assert "<external frame hidden>" in diagnostic
    assert "builtins.RuntimeError" in diagnostic
    assert metadata_secret not in diagnostic
    assert "FORGED-SUCCESS" not in diagnostic
    assert "message-secret-sentinel" not in diagnostic


def test_safe_traceback_rejects_lexically_forged_dbctl_frame() -> None:
    """@brief 仅伪造 package 路径不能获得 frame 信任 / A forged package path cannot gain frame trust.

    @return 无返回值 / No return value.
    """

    function_secret = "token_SUPERSECRET_FRAME_SENTINEL"
    filename = PROJECT_ROOT / "src" / "dbctl" / "interfaces" / "console.py"
    namespace = {"__name__": "dbctl.interfaces.console"}
    source = (
        f"def {function_secret}():\n"
        "    raise RuntimeError('forged-message-secret-sentinel')\n"
        f"{function_secret}()\n"
    )
    try:
        exec(compile(source, str(filename), "exec"), namespace)
    except RuntimeError as failure:
        stderr = StringIO()
        OperatorConsole(stderr, no_color=True).failure("migrate", failure, exit_code=1)
    else:  # pragma: no cover - compile/exec 必须抛出上述异常。
        pytest.fail("lexically forged traceback fixture did not raise")

    diagnostic = stderr.getvalue()
    assert "<external frame hidden>" in diagnostic
    assert function_secret not in diagnostic
    assert "forged-message-secret-sentinel" not in diagnostic


def test_external_diagnostic_ignores_hostile_descriptors() -> None:
    """@brief 外部诊断不反射读取任意 descriptor / External diagnostics ignore arbitrary descriptors.

    @return 无返回值 / No return value.
    """

    descriptor_secret = "descriptor-secret-sentinel"
    calls = {"errno": 0, "sqlstate": 0, "lineno": 0, "colno": 0}

    class HookedOSError(OSError):
        """@brief 伪造诊断 descriptor 的 OSError / OSError with forged diagnostic descriptors."""

        @property
        def errno(self) -> int:
            """@brief 禁止访问的伪 errno / Forged errno that must not be accessed.

            @return 伪造值 / Forged value.
            """

            calls["errno"] += 1
            return 999

        @property
        def sqlstate(self) -> str:
            """@brief 禁止访问的伪 SQLSTATE / Forged SQLSTATE that must not be accessed.

            @return 含 secret 的伪造值 / Forged value containing a secret.
            """

            calls["sqlstate"] += 1
            return descriptor_secret

        @property
        def lineno(self) -> int:
            """@brief 禁止访问的伪行号 / Forged line number that must not be accessed.

            @return 伪造值 / Forged value.
            """

            calls["lineno"] += 1
            return 999

        @property
        def colno(self) -> int:
            """@brief 禁止访问的伪列号 / Forged column number that must not be accessed.

            @return 伪造值 / Forged value.
            """

            calls["colno"] += 1
            return 999

    cause = safe_external_cause(
        HookedOSError(2, descriptor_secret),
        operation="诊断受控外部操作",
    )

    assert calls == {"errno": 0, "sqlstate": 0, "lineno": 0, "colno": 0}
    assert "builtins.OSError" in str(cause)
    assert "errno=2" in str(cause)
    assert descriptor_secret not in str(cause)


def test_external_diagnostic_bounds_untrusted_errno_before_formatting() -> None:
    """@brief 超大 errno 不得使安全转换遮蔽原错误 / Huge errno cannot break safe conversion.

    @return 无返回值 / No return value.
    """

    cause = safe_external_cause(
        OSError(10**5_000, "oversized-errno-secret-sentinel"),
        operation="诊断受控外部操作",
    )

    rendered = str(cause)
    assert "builtins.OSError" in rendered
    assert "errno=" not in rendered
    assert "oversized-errno-secret-sentinel" not in rendered


def test_native_python_notes_and_untrusted_application_origin_are_hidden() -> None:
    """@brief 外部来源内部类型正文及 Python notes 都隐藏 / External-origin internal text and notes are hidden.

    @return 无返回值 / No return value.
    """

    private_note = "application-note-secret-sentinel"
    stderr = StringIO()
    console = OperatorConsole(stderr, no_color=True)
    try:
        failure = ApplicationError("可安全展示的应用错误。")
        failure.add_note(private_note)
        raise failure
    except ApplicationError as error:
        console.failure("migrate", error, exit_code=2)

    diagnostic = stderr.getvalue()
    assert "可安全展示的应用错误" not in diagnostic
    assert "原始异常消息已隐藏" in diagnostic
    assert private_note not in diagnostic


def test_application_error_message_uses_immutable_construction_snapshot() -> None:
    """@brief 应用错误 args 后改写不能污染可信正文 / Mutated application args cannot replace trusted text.

    @return 无返回值 / No return value.
    """

    mutated_secret = "mutated-application-args-secret-sentinel"
    stderr = StringIO()
    try:
        MigrationRevision("invalid revision")
    except DbctlConfigurationError as failure:
        failure.args = (mutated_secret,)
        try:
            raise failure
        except DbctlConfigurationError as reraised:
            OperatorConsole(stderr, no_color=True).failure("migrate", reraised, exit_code=2)

    diagnostic = stderr.getvalue()
    assert "Alembic revision 只能包含" in diagnostic
    assert mutated_secret not in diagnostic


def test_domain_error_message_uses_immutable_construction_snapshot() -> None:
    """@brief 领域错误 args 后改写不能污染可信正文 / Mutated domain args cannot replace trusted text.

    @return 无返回值 / No return value.
    """

    mutated_secret = "mutated-domain-args-secret-sentinel"
    stderr = StringIO()
    try:
        DatabaseName("invalid-name")
    except DomainError as failure:
        failure.args = (mutated_secret,)
        try:
            raise failure
        except DomainError as reraised:
            OperatorConsole(stderr, no_color=True).failure("bootstrap", reraised, exit_code=2)

    diagnostic = stderr.getvalue()
    assert "数据库名只能包含 ASCII" in diagnostic
    assert mutated_secret not in diagnostic


def test_missing_application_snapshot_fails_closed() -> None:
    """@brief 丢失构造快照时固定隐藏当前 args / Missing snapshots hide current args fail-closed.

    @return 无返回值 / No return value.
    """

    mutable_secret = "missing-snapshot-secret-sentinel"
    stderr = StringIO()
    try:
        MigrationRevision("invalid revision")
    except DbctlConfigurationError as failure:
        failure.__dict__.pop("_dbctl_safe_diagnostic_notes", None)
        failure.args = (mutable_secret,)
        try:
            raise failure
        except DbctlConfigurationError as reraised:
            OperatorConsole(stderr, no_color=True).failure("migrate", reraised, exit_code=2)

    diagnostic = stderr.getvalue()
    assert "原始异常消息已隐藏" in diagnostic
    assert mutable_secret not in diagnostic


def test_safe_external_proxy_keeps_origin_grant_when_note_is_added() -> None:
    """@brief 安全注释不会清除代理正文来源授权 / Adding a safe note preserves proxy origin approval.

    @return 无返回值 / No return value.
    """

    raw_secret = "safe-proxy-raw-secret-sentinel"
    try:
        raise OSError(5, raw_secret)
    except OSError as raw_error:
        proxy = safe_external_cause(raw_error, operation="读取受控运行状态失败")
    add_safe_diagnostic_note(proxy, "dbctl 构造的代理诊断。")
    stderr = StringIO()

    OperatorConsole(stderr, no_color=True).failure("bootstrap", proxy, exit_code=2)

    diagnostic = stderr.getvalue()
    assert "读取受控运行状态失败" in diagnostic
    assert "dbctl 构造的代理诊断" in diagnostic
    assert raw_secret not in diagnostic


def test_safe_traceback_bounds_a_self_referential_cause_chain() -> None:
    """@brief 自环 cause 退化为单一安全错误且不会死循环 / A self-cause falls back without looping.

    @return 无返回值 / No return value.
    """

    stderr = StringIO()
    try:
        MigrationRevision("invalid revision")
    except DbctlConfigurationError as failure:
        failure.__cause__ = failure
        try:
            raise failure
        except DbctlConfigurationError as reraised:
            OperatorConsole(stderr, no_color=True).failure("migrate", reraised, exit_code=2)

    diagnostic = stderr.getvalue()
    assert diagnostic.count("DbctlConfigurationError:") == 2
    assert "Alembic revision 只能包含" in diagnostic


def test_invalid_trusted_module_path_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 可信模块路径异常不能遮蔽原始诊断 / Invalid trusted module paths fail closed.

    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    try:
        MigrationRevision("invalid revision")
    except DbctlConfigurationError as failure:
        monkeypatch.setattr(migration_module, "__file__", "\x00")
        stderr = StringIO()
        OperatorConsole(stderr, no_color=True).failure("migrate", failure, exit_code=2)

    diagnostic = stderr.getvalue()
    assert "原始异常消息已隐藏" in diagnostic
    assert "Alembic revision 只能包含" not in diagnostic


def test_external_exception_cannot_spoof_dbctl_safe_notes() -> None:
    """@brief 外部异常不能用同名属性伪造安全注释 / External errors cannot spoof safe notes by name.

    @return 无返回值 / No return value.
    """

    forged_note = "forged-safe-note-secret-sentinel"
    failure = ApplicationError("可安全展示的应用错误。")
    failure.__dict__["_dbctl_safe_diagnostic_notes"] = (forged_note,)
    add_safe_diagnostic_note(failure, "dbctl 构造的可信诊断。")
    stderr = StringIO()

    OperatorConsole(stderr, no_color=True).failure("migrate", failure, exit_code=2)

    diagnostic = stderr.getvalue()
    assert "dbctl 构造的可信诊断" in diagnostic
    assert forged_note not in diagnostic


def test_empty_no_color_environment_disables_all_ansi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief NO_COLOR 只要存在即禁用全部 ANSI / Presence of NO_COLOR disables all ANSI.

    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("NO_COLOR", "")
    stderr = TerminalBuffer()
    console = OperatorConsole(stderr)
    console.publish(
        ProgressUpdate(
            operation=OperationName.MIGRATION,
            state=ProgressState.STARTED,
            message="执行带样式进度",
        )
    )

    assert "执行带样式进度" in stderr.getvalue()
    assert "\x1b" not in stderr.getvalue()


def test_terminal_redaction_removes_assignment_and_unicode_control_spoofing() -> None:
    """@brief 终端脱敏移除常见赋值与 Unicode 控制欺骗 / Redaction removes assignments and Unicode spoofing.

    @return 无返回值 / No return value.
    """

    password_secret = "password-assignment-sentinel"
    json_secret = "json-password-sentinel"
    canonicalization_secret = "canonicalization-secret-sentinel"
    uri_secret = "zero-width-uri-secret-sentinel"
    newline_secret = "newline-keyword-secret-sentinel"
    newline_uri_secret = "newline-uri-secret-sentinel"
    fullwidth_secret = "fullwidth-keyword-secret-sentinel"
    escaped_double_secret = "escaped-double-quote-secret-sentinel"
    escaped_single_secret = "escaped-single-quote-secret-sentinel"
    escaped_unquoted_secret = "escaped-unquoted-secret-sentinel"
    c1_secret = "c1-control-secret-sentinel"
    generic_escape_secret = "generic-escape-secret-sentinel"
    backspace_secret = "backspace-secret-sentinel"
    carriage_return_secret = "carriage-return-secret-sentinel"
    payload = (
        "\x1b[31mERROR\x1b[0m\x9b31m\x7f\u202e "
        f'PGPASSWORD={password_secret} "password": "{json_secret}" '
        f"pass\u200bword={canonicalization_secret} "
        f"postgre\u200bsql://operator:{uri_secret}@db.example.test/aiws"
        f" pass\nword={newline_secret} "
        f"postgre\nsql://operator:{newline_uri_secret}@db.example.test/aiws "
        f"ｐａｓｓｗｏｒｄ＝{fullwidth_secret}"
        f' password="prefix\\"{escaped_double_secret}"'
        f" PASSWORD E'prefix\\'{escaped_single_secret}'"
        f" password=prefix\\ {escaped_unquoted_secret}"
        f" pass\x9b31mword={c1_secret}"
        f" pass\x1b(Bword={generic_escape_secret}"
        f" passX\bword={backspace_secret}"
        f" visible-prefix\rpassword={carriage_return_secret}"
    )

    rendered = redact_sensitive_text(payload)

    assert password_secret not in rendered
    assert json_secret not in rendered
    assert canonicalization_secret not in rendered
    assert uri_secret not in rendered
    assert newline_secret not in rendered
    assert newline_uri_secret not in rendered
    assert fullwidth_secret not in rendered
    assert escaped_double_secret not in rendered
    assert escaped_single_secret not in rendered
    assert escaped_unquoted_secret not in rendered
    assert c1_secret not in rendered
    assert generic_escape_secret not in rendered
    assert backspace_secret not in rendered
    assert carriage_return_secret not in rendered
    assert "<redacted>" in rendered
    assert all(control not in rendered for control in ("\x1b", "\x9b", "\x7f", "\u202e"))


def test_quiet_keeps_primary_result_and_suppresses_normal_progress(
    dbctl_config_path: Path,
) -> None:
    """@brief quiet 只抑制正常进度而保留主结果 / Quiet suppresses normal progress but keeps results.

    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    stdout = StringIO()
    stderr = StringIO()
    exit_code = run(
        [
            "prune-telemetry",
            "--quiet",
            "--no-color",
            "--config",
            str(dbctl_config_path),
            "--dbinit",
            str(PROJECT_ROOT / "dbinit.jsonc"),
        ],
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 0
    assert "dry-run" in stdout.getvalue()
    assert stderr.getvalue() == ""


def test_prune_failure_reports_committed_batches_and_fixed_cutoff() -> None:
    """@brief 清理失败明确报告已提交工作与未执行范围 / Prune failure reports committed work and remaining scope.

    @return 无返回值 / No return value.
    """

    port = FailingSecondBatchPort()
    progress = RecordingProgressSink()
    service = PruneTelemetryService(port, clock=lambda: _FIXED_NOW, progress=progress)
    request = PruneRequest(
        policy=RetentionPolicy(7),
        limits=PruneLimits(batch_size=2, max_batches=3),
        mode=PruneMode.APPLY,
    )

    with pytest.raises(RetentionExecutionError) as error_info:
        service.execute(request)

    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    failure = progress.updates[-1]
    assert port.calls == 2
    assert "此前已提交 1 个短事务、删除 2 条" in notes
    assert "固定 cutoff=2026-07-15T12:00:00+00:00" in notes
    assert failure.state is ProgressState.FAILED
    assert failure.current == 2
    assert failure.total == 3
    assert failure.detail is not None
    assert "此前已提交 1 个短事务、删除 2 条" in failure.detail


def test_progress_output_failure_cannot_change_committed_prune_result() -> None:
    """@brief 进度输出故障不能把已提交清理变成失败 / Progress failure cannot change a committed prune.

    @return 无返回值 / No return value.
    """

    port = CommittedTelemetryPort()
    service = PruneTelemetryService(
        port,
        clock=lambda: _FIXED_NOW,
        progress=ExplodingProgressSink(),
    )
    outcome = service.execute(
        PruneRequest(
            policy=RetentionPolicy(7),
            limits=PruneLimits(batch_size=2, max_batches=3),
            mode=PruneMode.APPLY,
        )
    )

    assert isinstance(outcome, PruneApplied)
    assert outcome.deleted_count == 1
    assert outcome.batch_count == 1
    assert outcome.has_more is False
    assert port.committed_rows == 1
