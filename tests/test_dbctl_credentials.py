"""@brief dbctl 配置凭证到 migrate/shell 的端到端单元测试 / Config credential flow tests."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest

from conftest import PROJECT_ROOT
from dbctl.cli import main
from dbctl.composition import DbctlComposition
from dbctl.config import DbctlConfigurationService
from dbctl.domain import DatabaseLogin, LoginRole
from dbctl.errors import MigrationExecutionError
from dbctl.migration import AlembicMigrationRunner
from workspace_shared.jsonc import load_jsonc


def _initialized_config(
    tmp_path: Path,
    *,
    app_password: str = "generated-app-password",
    migrator_password: str | None = None,
) -> Path:
    """@brief 创建可独立修改的私密测试配置 / Create an isolated private test config.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @param app_password application role 密码 / Application-role password.
    @param migrator_password 可选 migrator role 密码 / Optional migrator-role password.
    @return 已初始化配置路径 / Initialized config path.
    """
    config_path = tmp_path / "config.jsonc"
    config_path.write_text((PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8"))
    DbctlConfigurationService(config_path, PROJECT_ROOT / "dbinit.jsonc").initialize()
    root = load_jsonc(config_path)
    root["database"]["application_dsn"] = (
        f"postgresql://workspace_app:{quote(app_password, safe='')}@127.0.0.1:5432/ai_job_workspace"
    )
    if migrator_password is not None:
        root["database"]["migrator_dsn"] = (
            "postgresql://workspace_migrator:"
            f"{quote(migrator_password, safe='')}@127.0.0.1:5432/ai_job_workspace"
        )
    config_path.write_text(json.dumps(root), encoding="utf-8")
    return config_path


def test_shell_uses_config_password_via_temporary_pgpass_without_prompt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief shell 应自动使用 config 密码并清理 pgpass / Shell uses config password and cleans pgpass.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / Pytest patch fixture.
    """
    secret = "application:@/\\雪-password"
    config_path = _initialized_config(tmp_path, app_password=secret)
    inherited = {
        "PGPASSWORD": "must-not-win",
        "PGPASSFILE": str(tmp_path / "stale-pgpass"),
        "PATH": os.environ.get("PATH", ""),
    }
    composition = DbctlComposition.from_config_path(
        config_path,
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
        environ=inherited,
    )
    prepared = composition.prepare_shell(LoginRole.APP)
    observed_password_file: Path | None = None

    def fake_run(command: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """@brief 在 psql 读取期间检查临时凭证 / Inspect credentials while psql would read them.

        @param command psql argv / psql argv.
        @param kwargs subprocess 参数 / Subprocess keyword arguments.
        @return 固定非零退出结果 / Fixed non-zero completion.
        """
        nonlocal observed_password_file
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        assert "PGPASSWORD" not in environment
        observed_password_file = Path(environment["PGPASSFILE"])
        assert observed_password_file.is_file()
        assert os.stat(observed_password_file).st_mode & 0o777 == 0o600
        pgpass = observed_password_file.read_text(encoding="utf-8")
        assert "workspace_app" in pgpass
        assert "application\\:@/\\\\雪-password" in pgpass
        assert "--no-password" in command
        assert all(secret not in argument for argument in command)
        return subprocess.CompletedProcess(command, 37)

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert secret not in repr(prepared)
    assert composition.run_prepared_shell(prepared) == 37
    assert observed_password_file is not None
    assert not observed_password_file.exists()
    assert inherited["PGPASSWORD"] == "must-not-win"
    assert inherited["PGPASSFILE"] == str(tmp_path / "stale-pgpass")


@pytest.mark.parametrize(
    ("role", "expected_name"),
    (
        (LoginRole.APP, "workspace_app"),
        (LoginRole.MIGRATOR, "workspace_migrator"),
        (LoginRole.DASHBOARD, "workspace_dashboard"),
    ),
)
def test_shell_role_selection_always_resolves_a_complete_config_login(
    role: LoginRole,
    expected_name: str,
    tmp_path: Path,
) -> None:
    """@brief 三种 shell 身份都必须完整来自 config / Every shell identity comes from config.

    @param role 待选择的登录用途 / Login purpose to select.
    @param expected_name 预期 PostgreSQL role 名 / Expected PostgreSQL role name.
    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    """
    config_path = _initialized_config(tmp_path)
    composition = DbctlComposition.from_config_path(
        config_path,
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )

    prepared = composition.prepare_shell(role)

    assert prepared.login.role is role
    assert prepared.login.role_name == expected_name
    assert prepared.login.password
    assert prepared.login.password not in repr(prepared)
    assert all(prepared.login.password not in argument for argument in prepared.argv)


def test_migration_transports_percent_encoded_config_dsn_outside_configparser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 特殊字符 DSN 应通过 attributes 原样进入 Alembic / Encoded DSN bypasses ConfigParser.

    @param monkeypatch pytest 替换夹具 / Pytest patch fixture.
    """
    secret = "migrator:@/%雪"
    dsn = (
        f"postgresql://workspace_migrator:{quote(secret, safe='')}@127.0.0.1:5432/ai_job_workspace"
    )
    login = DatabaseLogin(
        role=LoginRole.MIGRATOR,
        role_name="workspace_migrator",
        dsn=dsn,
        safe_conninfo=(
            "user='workspace_migrator' host='127.0.0.1' port='5432' dbname='ai_job_workspace'"
        ),
        password=secret,
    )

    def fake_upgrade(config: Any, revision: str) -> None:
        """@brief 验证 Alembic 收到的内存配置 / Verify Alembic's in-memory configuration.

        @param config Alembic Config / Alembic Config.
        @param revision 目标 revision / Target revision.
        """
        assert revision == "head"
        assert config.attributes["aiws.migration_dsn"] == dsn
        assert not config.get_main_option("sqlalchemy.url")

    monkeypatch.setattr("alembic.command.upgrade", fake_upgrade)
    runner = AlembicMigrationRunner(
        login,
        PROJECT_ROOT / "alembic",
        "workspace_owner",
        "workspace_app",
        "workspace_dashboard",
    )
    runner.upgrade()
    assert secret not in repr(runner)


def test_migration_failure_never_displays_config_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief Alembic 底层异常不得把 config secret 带到 CLI / Migration errors redact config secrets.

    @param monkeypatch pytest 替换夹具 / Pytest patch fixture.
    """
    secret = "migration-secret:@/%雪"
    dsn = (
        f"postgresql://workspace_migrator:{quote(secret, safe='')}@127.0.0.1:5432/ai_job_workspace"
    )
    login = DatabaseLogin(
        role=LoginRole.MIGRATOR,
        role_name="workspace_migrator",
        dsn=dsn,
        safe_conninfo="user='workspace_migrator' dbname='ai_job_workspace'",
        password=secret,
    )

    def fail_with_dsn(_: Any, __: str) -> None:
        """@brief 模拟会回显 DSN 的底层失败 / Simulate a lower-level failure echoing the DSN.

        @param _ Alembic Config / Alembic Config.
        @param __ revision / Migration revision.
        """
        raise ValueError(dsn)

    monkeypatch.setattr("alembic.command.upgrade", fail_with_dsn)
    runner = AlembicMigrationRunner(
        login,
        PROJECT_ROOT / "alembic",
        "workspace_owner",
        "workspace_app",
        "workspace_dashboard",
    )

    with pytest.raises(MigrationExecutionError) as error_info:
        runner.upgrade()
    displayed = str(error_info.value)
    assert secret not in displayed
    assert dsn not in displayed
    assert "postgresql://" not in displayed


def test_composition_selects_migrator_role_and_password_from_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief migrate 自动选择 config migrator 身份 / Migrate selects config migrator identity.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / Pytest patch fixture.
    """
    secret = "migrator-config:@/%雪"
    config_path = _initialized_config(tmp_path, migrator_password=secret)
    observed: list[DatabaseLogin] = []

    def fake_upgrade(runner: AlembicMigrationRunner, revision: str = "head") -> None:
        """@brief 捕获 composition 交给 migration adapter 的身份 / Capture composed login.

        @param runner Alembic runner / Alembic runner.
        @param revision 目标 revision / Target revision.
        """
        assert revision == "head"
        observed.append(runner._migrator)

    monkeypatch.setattr(AlembicMigrationRunner, "upgrade", fake_upgrade)
    composition = DbctlComposition.from_config_path(
        config_path,
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    composition.execute_migration()

    assert len(observed) == 1
    assert observed[0].role is LoginRole.MIGRATOR
    assert observed[0].role_name == "workspace_migrator"
    assert observed[0].password == secret


@pytest.mark.parametrize("command", ("migrate", "shell"))
def test_non_bootstrap_commands_never_create_missing_config(
    command: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief migrate/shell 缺配置时必须失败且无写入 / Non-bootstrap commands never initialize config.

    @param command dbctl 子命令 / dbctl subcommand.
    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @param capsys pytest 输出捕获 / Pytest output capture.
    """
    config_path = tmp_path / "config.jsonc"
    exit_code = main(
        [
            "--config",
            str(config_path),
            "--dbinit",
            str(PROJECT_ROOT / "dbinit.jsonc"),
            command,
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "配置文件不存在" in captured.err
    assert not config_path.exists()
