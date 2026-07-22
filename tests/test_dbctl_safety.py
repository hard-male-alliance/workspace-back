"""@brief dbctl bootstrap 计划、编排与 psql 边界测试 / dbctl bootstrap planning, orchestration, and psql-boundary tests."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import pytest

from conftest import PROJECT_ROOT
from dbctl.application.errors import (
    BootstrapExecutionError,
    DbctlConfigurationError,
    ExternalDiagnosticError,
)
from dbctl.application.provision import (
    BootstrapAccessMode,
    BootstrapPlan,
    BootstrapService,
    BootstrapStage,
    ExecutionTarget,
    StageCondition,
    TransactionMode,
    build_bootstrap_plan,
)
from dbctl.composition import compose_bootstrap
from dbctl.domain.names import DatabaseName
from dbctl.domain.roles import Secret
from dbctl.infrastructure.postgres.psql import LocalPsqlBootstrapRunnerFactory
from dbctl.interfaces.cli import main
from dbctl.interfaces.console import render_bootstrap_plan


@dataclass
class RecordingBootstrapRunner:
    """@brief 不接触 PostgreSQL 的 stage runner / Stage runner that never touches PostgreSQL.

    @param database_present 初始数据库存在状态 / Initial database-presence state.
    @param stages 实际执行的 stage / Stages actually executed.
    """

    database_present: bool = False
    stages: list[BootstrapStage] = field(default_factory=list)
    access_mode: BootstrapAccessMode = BootstrapAccessMode.PROMPT

    def __enter__(self) -> Self:
        """@brief 进入 fake 生命周期 / Enter the fake lifecycle.

        @return 当前 fake / This fake.
        """

        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """@brief 离开 fake 生命周期 / Leave the fake lifecycle.

        @param _exception_type 未使用异常类型 / Unused exception type.
        @param _exception 未使用异常对象 / Unused exception value.
        @param _traceback 未使用 traceback / Unused traceback.
        @return 无返回值 / No return value.
        """

    def database_exists(self, _database: DatabaseName) -> bool:
        """@brief 返回内存存在状态 / Return the in-memory presence state.

        @param _database 强类型数据库名 / Strongly typed database name.
        @return 当前存在状态 / Current presence state.
        """

        return self.database_present

    def execute_stage(self, stage: BootstrapStage) -> None:
        """@brief 记录 stage 并模拟 CREATE DATABASE / Record a stage and simulate CREATE DATABASE.

        @param stage 应用层有序批次 / Application-layer ordered batch.
        @return 无返回值 / No return value.
        """

        self.stages.append(stage)
        if stage.condition is StageCondition.DATABASE_ABSENT:
            self.database_present = True


@dataclass
class RecordingBootstrapFactory:
    """@brief 始终返回同一 fake runner 的 factory / Factory always returning one fake runner.

    @param runner 被复用的内存 runner / In-memory runner to reuse.
    """

    runner: RecordingBootstrapRunner

    def open(
        self,
        _plan: BootstrapPlan,
        _access_mode: BootstrapAccessMode,
    ) -> RecordingBootstrapRunner:
        """@brief 返回受应用服务管理的 runner / Return the application-owned runner.

        @param _plan 自足计划 / Self-contained plan.
        @param _access_mode 访问模式 / Access mode.
        @return 同一 fake runner / The same fake runner.
        """

        return self.runner


def _plan_and_secret(config_path: Path) -> tuple[BootstrapPlan, str]:
    """@brief 从隔离配置构建计划与泄漏哨兵 / Build a plan and leak sentinel from isolated config.

    @param config_path 已初始化私密配置 / Initialized private config.
    @return bootstrap plan 与 app 密码 / Bootstrap plan and app password.
    """

    settings, _bootstrap = compose_bootstrap(
        config_path,
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
        environ={},
    )
    return (
        build_bootstrap_plan(settings),
        settings.connections.application.password.reveal(),
    )


def test_bootstrap_plan_is_least_privilege_staged_and_secret_free(
    dbctl_config_path: Path,
) -> None:
    """@brief 计划表达事务、最小权限与完全脱敏 / Plan expresses transactions, least privilege, and full redaction.

    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    plan, secret = _plan_and_secret(dbctl_config_path)
    statements = tuple(statement for stage in plan.stages for statement in stage.statements)
    all_sql = "\n".join(statement.sql for statement in statements)
    dry_run = render_bootstrap_plan(plan)

    assert plan.database.value == "ai_job_workspace"
    assert len(plan.stages) == 5
    assert sum(len(stage.statements) for stage in plan.stages) == 126
    create_stage = next(
        stage for stage in plan.stages if stage.condition is StageCondition.DATABASE_ABSENT
    )
    assert create_stage.target is ExecutionTarget.MAINTENANCE
    assert create_stage.transaction_mode is TransactionMode.AUTOCOMMIT
    assert all(
        stage.transaction_mode is TransactionMode.TRANSACTIONAL
        for stage in plan.stages
        if stage is not create_stage
    )
    assert 'CREATE DATABASE "ai_job_workspace" OWNER "workspace_owner";' in all_sql
    assert '"workspace_owner" NOLOGIN NOINHERIT NOSUPERUSER' in all_sql
    for role in ("workspace_migrator", "workspace_app", "workspace_dashboard"):
        assert f'"{role}" LOGIN NOINHERIT NOSUPERUSER' in all_sql
    assert (
        'GRANT "workspace_owner" TO "workspace_migrator" '
        "WITH INHERIT FALSE, SET TRUE, ADMIN FALSE;" in all_sql
    )
    assert 'REVOKE "workspace_owner" FROM "workspace_app", "workspace_dashboard";' in all_sql
    assert "WITH RECURSIVE membership_path" in all_sql
    assert "identity" in all_sql and "alembic_version" in all_sql
    assert "REVOKE ALL ON TABLE" in all_sql
    assert "CREATE EXTENSION IF NOT EXISTS vector;" in all_sql
    assert "GRANT CREATE ON SCHEMA" not in all_sql
    assert "ALTER SYSTEM" not in all_sql
    assert "pg_hba.conf" not in all_sql

    password_statements = tuple(statement for statement in statements if statement.parameters)
    assert len(password_statements) == 3
    assert (
        sum(
            isinstance(statement.parameters[0], Secret)
            and statement.parameters[0].reveal() == secret
            for statement in password_statements
        )
        == 1
    )
    assert all(secret not in repr(statement) for statement in password_statements)
    assert secret not in dry_run
    assert "<redacted>" in dry_run
    assert "不修改 pg_hba.conf" in dry_run


def test_cli_dry_run_never_invokes_bootstrap_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    dbctl_config_path: Path,
) -> None:
    """@brief dry-run 不得创建数据库会话或执行 SQL / Dry-run never opens a database session or executes SQL.

    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @param capsys pytest 输出夹具 / pytest output fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    secret = "cli-password-sentinel-must-never-print"
    monkeypatch.setenv("PGPASSWORD", secret)

    def unexpected_execution(*_arguments: object, **_keywords: object) -> object:
        """@brief 若错误执行立即失败 / Fail immediately on unintended execution.

        @param _arguments 未使用位置参数 / Unused positional arguments.
        @param _keywords 未使用关键字参数 / Unused keyword arguments.
        @return 永不返回 / Never returns.
        """

        raise AssertionError("dry-run tried to execute bootstrap")

    monkeypatch.setattr(BootstrapService, "execute", unexpected_execution)
    exit_code = main(
        [
            "--config",
            str(dbctl_config_path),
            "--dbinit",
            str(PROJECT_ROOT / "dbinit.jsonc"),
            "bootstrap",
            "--dry-run",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert secret not in captured.out + captured.err
    assert "不执行任何 SQL" in captured.out


def test_bootstrap_service_skips_create_on_repeat_without_external_io(
    dbctl_config_path: Path,
) -> None:
    """@brief 重复执行只跳过条件 stage / Repeated execution skips only the conditional stage.

    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    plan, _secret = _plan_and_secret(dbctl_config_path)
    runner = RecordingBootstrapRunner()
    service = BootstrapService(RecordingBootstrapFactory(runner))

    first = service.execute(plan)
    second = service.execute(plan)

    assert first.database_created is True
    assert first.executed_stage_count == 5
    assert first.executed_statement_count == 126
    assert second.database_created is False
    assert second.executed_stage_count == 4
    assert second.skipped_stage_count == 1
    assert second.executed_statement_count == 125
    assert sum(stage.condition is StageCondition.DATABASE_ABSENT for stage in runner.stages) == 1


def test_sudo_runner_batches_stage_into_one_transactional_psql_process(
    monkeypatch: pytest.MonkeyPatch,
    dbctl_config_path: Path,
) -> None:
    """@brief 一个事务 stage 只启动一个 psql / One transactional stage starts one psql process.

    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    observed: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(command: list[str], **keywords: Any) -> subprocess.CompletedProcess[str]:
        """@brief 记录批处理 subprocess / Record the batched subprocess.

        @param command 无 shell argv / Shell-free argv.
        @param keywords subprocess 参数 / Subprocess arguments.
        @return 成功结果 / Successful result.
        """

        observed.append((command, keywords))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    plan, _secret = _plan_and_secret(dbctl_config_path)
    factory = LocalPsqlBootstrapRunnerFactory(
        platform_name="posix",
        executable_finder=lambda executable: f"/usr/bin/{executable}",
        environ={"PGHOST": "attacker.example", "PGPASSWORD": "must-be-removed"},
    )
    stage = plan.stages[0]
    with factory.open(plan, BootstrapAccessMode.SUDO) as runner:
        runner.execute_stage(stage)

    assert len(observed) == 1
    command, keywords = observed[0]
    assert command[:5] == ["/usr/bin/sudo", "-u", "postgres", "--", "psql"]
    assert "--file=-" in command
    assert "--single-transaction" in command
    assert keywords["input"].count("ALTER ROLE") >= 4
    assert all(not name.startswith("PG") for name in keywords["env"])


def test_psql_failure_never_exposes_arbitrary_stderr(
    monkeypatch: pytest.MonkeyPatch,
    dbctl_config_path: Path,
) -> None:
    """@brief psql 任意 stderr 不进入安全异常链 / Arbitrary psql stderr never enters the safe exception chain.

    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    opaque_server_value = "opaque-trigger-row-sentinel"

    def fail_with_server_stderr(
        command: list[str],
        **_keywords: Any,
    ) -> subprocess.CompletedProcess[str]:
        """@brief 模拟服务器回显任意行数据 / Simulate a server echoing arbitrary row data.

        @param command psql argv / psql arguments.
        @param _keywords 未使用 subprocess 参数 / Unused subprocess options.
        @return 固定非零进程结果 / Fixed non-zero process result.
        """

        return subprocess.CompletedProcess(
            command,
            9,
            stdout="",
            stderr=f"trigger failure: {opaque_server_value}",
        )

    monkeypatch.setattr(subprocess, "run", fail_with_server_stderr)
    plan, _secret = _plan_and_secret(dbctl_config_path)
    factory = LocalPsqlBootstrapRunnerFactory(
        platform_name="posix",
        executable_finder=lambda executable: f"/usr/bin/{executable}",
        environ={},
    )

    with factory.open(plan, BootstrapAccessMode.SUDO) as runner:
        with pytest.raises(BootstrapExecutionError) as error_info:
            runner.execute_stage(plan.stages[0])

    cause = error_info.value.__cause__
    assert isinstance(cause, ExternalDiagnosticError)
    assert "退出码=9" in str(cause)
    assert opaque_server_value not in str(error_info.value)
    assert opaque_server_value not in str(cause)


def test_complete_bootstrap_uses_six_processes_for_126_logical_statements(
    monkeypatch: pytest.MonkeyPatch,
    dbctl_config_path: Path,
) -> None:
    """@brief 完整新库计划从 124 次进程降为 6 次 / A complete fresh plan reduces about 124 processes to six.

    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    commands: list[list[str]] = []

    def fake_run(command: list[str], **_keywords: Any) -> subprocess.CompletedProcess[str]:
        """@brief 模拟全部 psql 调用并报告数据库不存在 / Simulate all psql calls and report database absence.

        @param command psql argv / psql argv.
        @param _keywords subprocess 参数 / Subprocess arguments.
        @return 查询为 false、其他调用成功 / False for the probe and success otherwise.
        """

        commands.append(command)
        stdout = "f\n" if "-A" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    plan, _secret = _plan_and_secret(dbctl_config_path)
    service = BootstrapService(
        LocalPsqlBootstrapRunnerFactory(
            platform_name="posix",
            executable_finder=lambda executable: f"/usr/bin/{executable}",
            environ={},
        )
    )

    result = service.execute(plan, access_mode=BootstrapAccessMode.SUDO)

    assert result.database_created is True
    assert result.executed_statement_count == 126
    assert len(commands) == 6
    assert sum("--single-transaction" in command for command in commands) == 4
    assert sum("-A" in command for command in commands) == 1


@pytest.mark.parametrize("platform_name", ("nt", "posix"))
def test_prompt_runner_prompts_once_and_uses_exact_pgpass(
    platform_name: str,
    monkeypatch: pytest.MonkeyPatch,
    dbctl_config_path: Path,
) -> None:
    """@brief 无 sudo 时只提示一次并精确绑定 pgpass / Without sudo, prompt once and bind pgpass exactly.

    @param platform_name 模拟平台 / Simulated platform.
    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    secret = "bootstrap-admin-password-sentinel"
    prompt_count = 0
    observed: list[tuple[list[str], dict[str, Any]]] = []

    def prompt(_message: str) -> str:
        """@brief 返回测试密码 / Return the test password.

        @param _message 安全提示 / Safe prompt.
        @return 测试密码 / Test password.
        """

        nonlocal prompt_count
        prompt_count += 1
        return secret

    def fake_run(command: list[str], **keywords: Any) -> subprocess.CompletedProcess[str]:
        """@brief 捕获 prompt 命令并报告数据库不存在 / Capture prompt command and report absence.

        @param command 无 shell argv / Shell-free argv.
        @param keywords subprocess 参数 / Subprocess arguments.
        @return 数据库不存在的结果 / Database-absent result.
        """

        observed.append((command, keywords))
        return subprocess.CompletedProcess(command, 0, stdout="f\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    plan, _sentinel = _plan_and_secret(dbctl_config_path)
    factory = LocalPsqlBootstrapRunnerFactory(
        platform_name=platform_name,
        executable_finder=lambda _executable: None,
        password_prompt=prompt,
        environ={"PGPASSWORD": "inherited-secret", "PGSERVICE": "unsafe"},
    )
    runner = factory.open(plan, BootstrapAccessMode.AUTO)
    with runner:
        assert runner.access_mode is BootstrapAccessMode.PROMPT
        assert runner.database_exists(plan.database) is False
        assert runner.database_exists(plan.database) is False
        command, keywords = observed[0]
        password_file = Path(keywords["env"]["PGPASSFILE"])
        pgpass = password_file.read_text(encoding="utf-8")
        assert pgpass == f"127.0.0.1:5432:postgres:postgres:{secret}\n"
        assert "--host=127.0.0.1" in command
        assert "--port=5432" in command
        assert "--username=postgres" in command
        assert "PGPASSWORD" not in keywords["env"]
        assert "PGSERVICE" not in keywords["env"]
        assert secret not in repr(command)
        assert prompt_count == 1
    assert not password_file.exists()


def test_sudo_mode_fails_closed_for_windows_and_remote_targets(
    dbctl_config_path: Path,
) -> None:
    """@brief sudo 在缺失或远程 target 上 fail closed / Sudo fails closed when unavailable or remote.

    @param dbctl_config_path 隔离私密配置 / Isolated private config.
    @return 无返回值 / No return value.
    """

    plan, _secret = _plan_and_secret(dbctl_config_path)
    windows = LocalPsqlBootstrapRunnerFactory(
        platform_name="nt",
        executable_finder=lambda _executable: None,
    )
    with pytest.raises(DbctlConfigurationError, match="未找到兼容的 sudo"):
        windows.open(plan, BootstrapAccessMode.SUDO)

    remote_plan = BootstrapPlan(
        database=plan.database,
        access=plan.access.__class__(
            maintenance_target=plan.access.maintenance_target.__class__(
                host="db.example.test",
                port=plan.access.maintenance_target.port,
                database=plan.access.maintenance_target.database,
            ),
            local_postgres_user=plan.access.local_postgres_user,
            bootstrap_database_user=plan.access.bootstrap_database_user,
        ),
        database_target=plan.database_target.__class__(
            host="db.example.test",
            port=plan.database_target.port,
            database=plan.database_target.database,
        ),
        stages=plan.stages,
    )
    posix = LocalPsqlBootstrapRunnerFactory(
        platform_name="posix",
        executable_finder=lambda executable: f"/usr/bin/{executable}",
    )
    with pytest.raises(DbctlConfigurationError, match="loopback"):
        posix.open(remote_plan, BootstrapAccessMode.SUDO)
