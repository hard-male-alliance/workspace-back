"""@brief dbctl 纯计划、dry-run 与幂等执行编排的安全测试 / Safety tests for dbctl pure planning, dry-run, and idempotent execution orchestration."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from conftest import PROJECT_ROOT
from dbctl.bootstrap import (
    BootstrapExecutor,
    BootstrapPlan,
    ExecutionTarget,
    SqlStatement,
)
from dbctl.cli import main
from dbctl.composition import DbctlComposition
from dbctl.errors import DbctlConfigurationError
from dbctl.runners import BootstrapAccessMode, LocalPsqlBootstrapRunner


@dataclass
class RecordingBootstrapRunner:
    """@brief 不接触 PostgreSQL 的 bootstrap runner / Bootstrap runner that never touches PostgreSQL.

    @param database_present 初始时目标数据库是否存在 / Whether target database initially exists.
    @param calls 按执行顺序记录的 SQL 调用 / SQL calls recorded in execution order.
    """

    database_present: bool = False
    calls: list[tuple[ExecutionTarget, SqlStatement]] = field(default_factory=list)

    def database_exists(self, _: str) -> bool:
        """@brief 返回内存中的数据库存在状态 / Return in-memory database existence state.

        @param _ 已验证的数据库名 / Validated database name.
        @return 数据库目前存在时为真 / True when database currently exists.
        """

        return self.database_present

    def execute(self, target: ExecutionTarget, statement: SqlStatement) -> None:
        """@brief 仅记录语句并模拟 CREATE DATABASE 成功 / Record a statement only and simulate successful CREATE DATABASE.

        @param target maintenance 或 database 目标 / Maintenance or database target.
        @param statement 已计划 SQL / Planned SQL.
        @return 无返回值 / No return value.
        """

        self.calls.append((target, statement))
        if statement.sql.startswith("CREATE DATABASE "):
            self.database_present = True


def _composition_with_secret() -> tuple[DbctlComposition, str]:
    """@brief 构造只使用内存环境的 dbctl composition / Construct a dbctl composition using only an in-memory environment.

    @return composition 与用于泄漏检测的秘密 / Composition and secret used for leakage detection.
    """

    secret = "password-sentinel-must-never-print"
    composition = DbctlComposition.from_config_path(
        PROJECT_ROOT / "config.jsonc",
        environ={
            "AIWS_APP_DATABASE_DSN": (
                f"postgresql://workspace_app:{secret}@db.example.test:5432/ai_job_workspace"
            )
        },
    )
    return composition, secret


def test_dbctl_bootstrap_plan_is_least_privilege_and_secret_free_when_displayed() -> None:
    """@brief 计划应定义四类最小权限角色并在展示层彻底脱敏 / Plan must define four least-privilege roles and fully redact display output."""

    composition, secret = _composition_with_secret()
    plan = composition.build_bootstrap_plan()
    all_statements = (
        *plan.pre_database_statements,
        plan.create_database,
        *plan.maintenance_statements,
        *plan.database_statements,
    )
    all_sql = "\n".join(statement.sql for statement in all_statements)
    dry_run = plan.render_dry_run()

    assert plan.database_name == "ai_job_workspace"
    assert 'CREATE DATABASE "ai_job_workspace" OWNER "workspace_owner";' in all_sql
    assert '"workspace_owner" NOLOGIN NOINHERIT NOSUPERUSER' in all_sql
    for role in ("workspace_migrator", "workspace_app", "workspace_dashboard"):
        assert f'"{role}" LOGIN NOINHERIT NOSUPERUSER' in all_sql
    assert 'GRANT "workspace_owner" TO "workspace_migrator";' in all_sql
    assert "CREATE EXTENSION IF NOT EXISTS vector;" in all_sql
    for schema in ("identity", "resume", "agent", "interview", "knowledge", "observability"):
        assert f'CREATE SCHEMA IF NOT EXISTS "{schema}" AUTHORIZATION "workspace_owner";' in all_sql
    assert "GRANT CREATE ON SCHEMA" not in all_sql
    assert (
        'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "observability" TO "workspace_app";'
        not in all_sql
    )
    assert (
        'GRANT SELECT ON TABLE "observability"."dashboard_metric_samples" TO "workspace_dashboard";'
        in all_sql
    )
    assert "ALTER SYSTEM" not in all_sql
    assert "pg_hba.conf" not in all_sql

    password_statements = [statement for statement in all_statements if statement.parameters]
    assert len(password_statements) == 3
    assert all(statement.parameters != (secret,) for statement in password_statements)
    assert all(secret not in repr(statement) for statement in password_statements)
    assert secret not in dry_run
    assert "<redacted>" in dry_run
    assert "不修改 pg_hba.conf" in dry_run
    assert "不创建 PostgreSQL superuser" in dry_run


def test_dbctl_dry_run_never_invokes_bootstrap_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief CLI --dry-run 不得创建 runner、连接数据库或执行 SQL / CLI --dry-run must not create a runner, connect to a database, or execute SQL.

    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @param capsys pytest 标准流捕获夹具 / pytest standard-stream capture fixture.
    """

    secret = "cli-password-sentinel-must-never-print"
    monkeypatch.setenv(
        "AIWS_APP_DATABASE_DSN",
        f"postgresql://workspace_app:{secret}@db.example.test:5432/ai_job_workspace",
    )

    def unexpected_execution(_: DbctlComposition, __: BootstrapPlan) -> Any:
        """@brief 若 dry-run 错误执行则立即失败 / Fail immediately if dry-run wrongly executes.

        @param _ dbctl composition / dbctl composition.
        @param __ bootstrap 计划 / Bootstrap plan.
        @return 永不返回 / Never returns.
        """

        raise AssertionError("dry-run tried to execute bootstrap")

    monkeypatch.setattr(DbctlComposition, "execute_bootstrap", unexpected_execution)
    exit_code = main(
        [
            "--config",
            str(PROJECT_ROOT / "config.jsonc"),
            "bootstrap",
            "--dry-run",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert secret not in captured.out
    assert secret not in captured.err
    assert "不执行任何 SQL" in captured.out
    assert "<redacted>" in captured.out


def test_bootstrap_executor_skips_existing_database_on_repeat_without_external_io() -> None:
    """@brief 同一计划第二次执行不应再次发送 CREATE DATABASE / A second execution of the same plan must not send CREATE DATABASE again.

    该测试只使用 RecordingBootstrapRunner；它验证幂等编排而不启动数据库、sudo 或
    psql。
    """

    composition, _ = _composition_with_secret()
    plan = composition.build_bootstrap_plan()
    runner = RecordingBootstrapRunner()
    executor = BootstrapExecutor(runner)

    first_result = executor.apply(plan)
    first_create_count = sum(
        statement.sql.startswith("CREATE DATABASE ") for _, statement in runner.calls
    )
    second_result = executor.apply(plan)
    final_create_count = sum(
        statement.sql.startswith("CREATE DATABASE ") for _, statement in runner.calls
    )

    expected_without_create = (
        len(plan.pre_database_statements)
        + len(plan.maintenance_statements)
        + len(plan.database_statements)
    )
    assert first_result.database_created is True
    assert first_result.executed_statement_count == expected_without_create + 1
    assert second_result.database_created is False
    assert second_result.executed_statement_count == expected_without_create
    assert first_create_count == 1
    assert final_create_count == 1
    assert runner.calls[0][0] is ExecutionTarget.MAINTENANCE
    assert any(target is ExecutionTarget.DATABASE for target, _ in runner.calls)


def test_bootstrap_runner_uses_terminal_sudo_psql_on_supported_posix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 支持 sudo 的 POSIX 可显式使用 sudo psql / Supported POSIX platforms can explicitly use sudo psql.

    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @return 无返回值 / No return value.
    """
    observed_commands: list[list[str]] = []

    def fake_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        """@brief 记录本地命令并返回数据库不存在 / Record the local command and report an absent database.

        @param command 无 shell argv / Shell-free argv.
        @param _ subprocess 其余受控参数 / Remaining controlled subprocess parameters.
        @return 成功的 psql 结果 / Successful psql result.
        """
        observed_commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="f\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    composition, _ = _composition_with_secret()
    runner = composition._bootstrap_runner(access_mode=BootstrapAccessMode.SUDO)
    assert isinstance(runner, LocalPsqlBootstrapRunner)
    assert runner.database_exists("ai_job_workspace") is False
    assert observed_commands[0][:5] == ["sudo", "-u", "postgres", "--", "psql"]
    assert "shell" not in observed_commands[0]


@pytest.mark.parametrize("platform_name", ("nt", "posix"))
def test_bootstrap_runner_prompts_once_without_sudo(
    platform_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief Windows 与无 sudo 平台应提示一次密码且不把密码放进 argv/environment / Windows and sudo-less platforms prompt once without exposing the password.

    @param platform_name 模拟平台 / Simulated platform.
    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @return 无返回值 / No return value.
    """
    secret = "bootstrap-admin-password-sentinel"
    prompt_count = 0
    observed: list[tuple[list[str], dict[str, Any]]] = []

    def prompt(_: str) -> str:
        """@brief 返回一次测试密码 / Return one test password.

        @param _ 安全提示文本 / Safe prompt text.
        @return 测试密码 / Test password.
        """
        nonlocal prompt_count
        prompt_count += 1
        return secret

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """@brief 捕获 prompt 模式命令 / Capture a prompt-mode command.

        @param command 无 shell argv / Shell-free argv.
        @param kwargs subprocess 参数 / Subprocess arguments.
        @return 数据库不存在的成功结果 / Successful absent-database result.
        """
        observed.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="f\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    runner = LocalPsqlBootstrapRunner(
        platform_name=platform_name,
        executable_finder=lambda _: None,
        password_prompt=prompt,
        environ={"PGPASSWORD": "inherited-password-must-be-removed"},
    ).with_target_database("ai_job_workspace")
    try:
        assert runner.access_mode is BootstrapAccessMode.PROMPT
        assert runner.database_exists("ai_job_workspace") is False
        assert runner.database_exists("ai_job_workspace") is False
        command, arguments = observed[0]
        assert command[0] == "psql"
        assert "sudo" not in command
        assert "--username=postgres" in command
        assert secret not in repr(command)
        child_environment = arguments["env"]
        assert isinstance(child_environment, dict)
        assert "PGPASSWORD" not in child_environment
        password_file = child_environment["PGPASSFILE"]
        assert isinstance(password_file, str)
        assert prompt_count == 1
    finally:
        runner.close()
    assert not Path(password_file).exists()


def test_explicit_sudo_mode_fails_closed_on_windows() -> None:
    """@brief Windows 显式 sudo 模式必须 fail closed / Explicit sudo mode fails closed on Windows.

    @return 无返回值 / No return value.
    """
    with pytest.raises(DbctlConfigurationError, match="未找到兼容的 sudo"):
        LocalPsqlBootstrapRunner(
            platform_name="nt",
            executable_finder=lambda _: None,
            access_mode=BootstrapAccessMode.SUDO,
        )
