"""@brief dbctl 操作员交互与安全诊断测试 / dbctl operator UX and safe-diagnostics tests."""

from __future__ import annotations

import stat
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import pytest

from conftest import PROJECT_ROOT
from dbctl.application.errors import (
    ApplicationError,
    RetentionExecutionError,
    redact_sensitive_text,
    safe_diagnostic_notes,
)
from dbctl.application.progress import OperationName, ProgressState, ProgressUpdate
from dbctl.application.provision import (
    BootstrapAccessMode,
    BootstrapPlan,
    BootstrapStage,
    StageCondition,
)
from dbctl.application.prune_telemetry import (
    DeleteTelemetryBatch,
    PruneApplied,
    PruneMode,
    PruneRequest,
    PruneTelemetryService,
    StaleTelemetryProbe,
)
from dbctl.domain.names import DatabaseName
from dbctl.domain.retention import PruneLimits, RetentionPolicy
from dbctl.infrastructure.postgres.psql import LocalPsqlBootstrapRunnerFactory
from dbctl.interfaces import cli as cli_module
from dbctl.interfaces.cli import main, run
from dbctl.interfaces.console import OperatorConsole
from workspace_shared.jsonc import load_jsonc

_FIXED_NOW = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
"""@brief 确定性遥测清理时钟 / Deterministic telemetry-pruning clock."""


@dataclass
class OperatorBootstrapRunner:
    """@brief 不连接数据库的完整 bootstrap runner / Full bootstrap runner without database I/O.

    @param access_mode 已解析的实际访问方式 / Resolved administrative-access mode.
    @param database_present 模拟数据库存在状态 / Simulated database-presence state.
    @param stages 已执行阶段 / Executed stages.
    """

    access_mode: BootstrapAccessMode = BootstrapAccessMode.PROMPT
    database_present: bool = False
    stages: list[BootstrapStage] = field(default_factory=list)

    def __enter__(self) -> Self:
        """@brief 进入 fake runner 生命周期 / Enter the fake runner lifecycle.

        @return 当前 runner / This runner.
        """

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

        self.stages.append(stage)
        if stage.condition is StageCondition.DATABASE_ABSENT:
            self.database_present = True


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
    assert "本地配置初始化可能已完成" in stdout.getvalue()
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
    assert "Traceback（已脱敏；未捕获 locals）" in diagnostic
    assert "fail_with_private_dsn" in diagnostic
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
    assert "fail_composition" in diagnostic
    assert "builtins.RuntimeError: <原始异常消息已隐藏" in diagnostic
    assert private_value not in diagnostic
    assert untrusted_note not in diagnostic


def test_native_python_notes_are_hidden_even_for_application_errors() -> None:
    """@brief 预期异常也不信任任意 Python notes / Expected errors also distrust arbitrary Python notes.

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
    assert "可安全展示的应用错误" in diagnostic
    assert private_note not in diagnostic


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
    payload = (
        "\x1b[31mERROR\x1b[0m\x9b31m\x7f\u202e "
        f"PGPASSWORD={password_secret} \"password\": \"{json_secret}\""
    )

    rendered = redact_sensitive_text(payload)

    assert password_secret not in rendered
    assert json_secret not in rendered
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
