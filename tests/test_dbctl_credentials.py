"""@brief dbctl 配置凭证到 migration/shell 的边界测试 / dbctl config-credential boundary tests for migration and shell."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest

from conftest import PROJECT_ROOT
from dbctl.application.errors import (
    ExternalDiagnosticError,
    MigrationExecutionError,
    ShellExecutionError,
    safe_diagnostic_notes,
)
from dbctl.application.migrate import MigrationRevision
from dbctl.composition import compose_migration, compose_shell
from dbctl.domain.database import LoginDatabase
from dbctl.domain.roles import LoginRole
from dbctl.infrastructure.alembic import AlembicMigrationAdapter
from dbctl.infrastructure.configuration import DbctlConfigStore
from dbctl.infrastructure.postgres.shell import PsqlShellAdapter
from dbctl.interfaces.cli import main
from workspace_shared.jsonc import load_jsonc


class FailingCleanupLease:
    """@brief close 固定失败的临时凭据租约 / Temporary credential lease whose close always fails.

    @param path 供子进程环境引用的测试路径 / Test path referenced by the child environment.
    """

    def __init__(self, path: Path) -> None:
        """@brief 保存测试路径 / Retain the test path.

        @param path 临时凭据路径 / Temporary credential path.
        """

        self.path = path

    def close(self) -> None:
        """@brief 模拟凭据文件清理失败 / Simulate credential-file cleanup failure.

        @return 永不返回 / Never returns.
        @raise OSError 固定清理错误 / Fixed cleanup failure.
        """

        raise OSError("opaque-cleanup-error-sentinel")


def _initialized_config(
    tmp_path: Path,
    *,
    app_password: str = "generated-app-password",
    migrator_password: str | None = None,
) -> Path:
    """@brief 创建可独立修改的私密测试配置 / Create an independently mutable private test config.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param app_password application role 密码 / Application-role password.
    @param migrator_password 可选 migrator role 密码 / Optional migrator-role password.
    @return 已初始化配置路径 / Initialized config path.
    """

    config_path = tmp_path / "config.jsonc"
    config_path.write_text((PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8"))
    DbctlConfigStore(config_path, PROJECT_ROOT / "dbinit.jsonc").initialize()
    root = load_jsonc(config_path)
    root["database"]["application_dsn"] = (
        f"postgresql://workspace_app:{quote(app_password, safe='')}@127.0.0.1:5432/ai_job_workspace"
    )
    if migrator_password is not None:
        root["database"]["migrator_dsn"] = (
            f"postgresql://workspace_migrator:{quote(migrator_password, safe='')}"
            "@127.0.0.1:5432/ai_job_workspace"
        )
    config_path.write_text(json.dumps(root), encoding="utf-8")
    return config_path


def test_shell_uses_exact_config_pgpass_without_prompt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief shell 使用精确配置凭证并清理租约 / Shell uses exact config credentials and cleans its lease.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @return 无返回值 / No return value.
    """

    secret = "application:@/\\雪-password"
    config_path = _initialized_config(tmp_path, app_password=secret)
    inherited = {
        "PGPASSWORD": "must-not-win",
        "PGPASSFILE": str(tmp_path / "stale-pgpass"),
        "PGHOST": "attacker.example",
        "PATH": os.environ.get("PATH", ""),
    }
    settings, shell = compose_shell(
        config_path,
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
        environ=inherited,
    )
    login = settings.connections.application
    observed_password_file: Path | None = None

    def fake_run(command: tuple[str, ...], **keywords: Any) -> subprocess.CompletedProcess[str]:
        """@brief 在 psql 读取期间检查临时凭证 / Inspect temporary credentials while psql reads them.

        @param command psql argv / psql argv.
        @param keywords subprocess 参数 / Subprocess keyword arguments.
        @return 固定非零退出结果 / Fixed non-zero result.
        """

        nonlocal observed_password_file
        environment = keywords["env"]
        assert isinstance(environment, dict)
        assert "PGPASSWORD" not in environment
        assert "PGHOST" not in environment
        observed_password_file = Path(environment["PGPASSFILE"])
        assert observed_password_file.is_file()
        assert os.stat(observed_password_file).st_mode & 0o777 == 0o600
        assert observed_password_file.read_text(encoding="utf-8") == (
            "127.0.0.1:5432:ai_job_workspace:workspace_app:application\\:@/\\\\雪-password\n"
        )
        assert "--no-password" in command
        assert all(secret not in argument for argument in command)
        return subprocess.CompletedProcess(command, 37)

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert secret not in repr(login)
    assert shell.execute(login) == 37
    assert observed_password_file is not None
    assert not observed_password_file.exists()
    assert inherited["PGPASSWORD"] == "must-not-win"
    assert inherited["PGPASSFILE"] == str(tmp_path / "stale-pgpass")


def test_shell_reports_secondary_pgpass_cleanup_risk_without_masking_primary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief shell 主故障保留且凭据清理风险可见 / Shell keeps the primary failure and reports cleanup risk.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / Pytest monkeypatch fixture.
    @return 无返回值 / No return value.
    """

    settings, _shell = compose_shell(
        _initialized_config(tmp_path),
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    lease = FailingCleanupLease(tmp_path / "leased-pgpass")

    def fake_lease(**_keywords: object) -> FailingCleanupLease:
        """@brief 返回清理失败租约 / Return the cleanup-failing lease.

        @param _keywords 未使用租约参数 / Unused lease arguments.
        @return 共享测试租约 / Shared test lease.
        """

        return lease

    def fail_to_start(*_arguments: object, **_keywords: object) -> object:
        """@brief 模拟 psql 启动失败 / Simulate failure to launch psql.

        @param _arguments 未使用位置参数 / Unused positional arguments.
        @param _keywords 未使用关键字参数 / Unused keyword arguments.
        @return 永不返回 / Never returns.
        @raise OSError 固定启动错误 / Fixed launch failure.
        """

        raise OSError("opaque-process-error-sentinel")

    monkeypatch.setattr(
        "dbctl.infrastructure.postgres.shell.create_pgpass_lease",
        fake_lease,
    )
    monkeypatch.setattr(subprocess, "run", fail_to_start)

    with pytest.raises(ShellExecutionError) as error_info:
        PsqlShellAdapter({}).launch(settings.connections.application)

    assert str(error_info.value) == "无法启动本地 PostgreSQL psql。"
    assert isinstance(error_info.value.__cause__, ExternalDiagnosticError)
    notes = "\n".join(safe_diagnostic_notes(error_info.value))
    assert "临时 PGPASSFILE 清理也失败" in notes
    assert "手工删除残留凭据文件" in notes
    assert "opaque-process-error-sentinel" not in str(error_info.value.__cause__)
    assert "opaque-cleanup-error-sentinel" not in notes


@pytest.mark.parametrize(
    ("role", "expected_name"),
    (
        (LoginRole.APP, "workspace_app"),
        (LoginRole.MIGRATOR, "workspace_migrator"),
        (LoginRole.DASHBOARD, "workspace_dashboard"),
    ),
)
def test_shell_role_selection_resolves_complete_typed_login(
    role: LoginRole,
    expected_name: str,
    tmp_path: Path,
) -> None:
    """@brief 三种 shell 身份都完整来自同一连接目录 / All shell identities come from one connection catalog.

    @param role 待选择登录用途 / Login purpose to select.
    @param expected_name 预期 PostgreSQL role / Expected PostgreSQL role.
    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """

    settings, _shell = compose_shell(
        _initialized_config(tmp_path),
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    login = settings.connections.login_for(role)

    assert login.role is role
    assert login.role_name.value == expected_name
    assert login.password.reveal()
    assert login.password.reveal() not in repr(login)
    assert login.target == settings.connections.target


def test_migration_transports_encoded_dsn_only_via_memory_attributes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 特殊字符 DSN 只经 attributes 进入 Alembic / Encoded DSN reaches Alembic only through attributes.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @return 无返回值 / No return value.
    """

    secret = "migrator:@/%雪"
    settings, _migration = compose_migration(
        _initialized_config(tmp_path, migrator_password=secret),
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    login = settings.connections.migrator

    def fake_upgrade(configuration: Any, revision: str) -> None:
        """@brief 验证 Alembic 内存配置 / Verify Alembic in-memory configuration.

        @param configuration Alembic Config / Alembic Config.
        @param revision 目标 revision / Target revision.
        @return 无返回值 / No return value.
        """

        assert revision == "head"
        assert configuration.attributes["aiws.migration_dsn"] == login.dsn.reveal()
        assert not configuration.get_main_option("sqlalchemy.url")
        assert configuration.get_main_option("aiws.owner_role") == "workspace_owner"
        assert configuration.get_main_option("aiws.migrator_role") == "workspace_migrator"
        assert configuration.get_main_option("aiws.app_role") == "workspace_app"
        assert configuration.get_main_option("aiws.dashboard_role") == "workspace_dashboard"

    monkeypatch.setattr("alembic.command.upgrade", fake_upgrade)
    AlembicMigrationAdapter().upgrade(
        login,
        MigrationRevision(),
        settings.blueprint,
    )


def test_migration_failure_uses_secret_safe_diagnostic_cause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief Alembic 异常链保留栈但不保留 DSN / Alembic cause chains retain frames without DSNs.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @return 无返回值 / No return value.
    """

    secret = "migration-secret:@/%雪"
    settings, _migration = compose_migration(
        _initialized_config(tmp_path, migrator_password=secret),
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    login = settings.connections.migrator

    def fail_with_dsn(_configuration: Any, _revision: str) -> None:
        """@brief 模拟回显 DSN 的底层失败 / Simulate a lower-level DSN-bearing failure.

        @param _configuration Alembic Config / Alembic Config.
        @param _revision revision / Revision.
        @return 永不返回 / Never returns.
        """

        raise ValueError(login.dsn.reveal())

    monkeypatch.setattr("alembic.command.upgrade", fail_with_dsn)
    with pytest.raises(MigrationExecutionError) as error_info:
        AlembicMigrationAdapter().upgrade(
            login,
            MigrationRevision(),
            settings.blueprint,
        )
    assert secret not in str(error_info.value)
    assert "postgresql://" not in str(error_info.value)
    cause = error_info.value.__cause__
    assert isinstance(cause, ExternalDiagnosticError)
    assert cause.__traceback__ is not None
    assert secret not in str(cause)
    assert "postgresql://" not in str(cause)


def test_composition_passes_only_migrator_and_nonsecret_blueprint_to_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief migration 端口只接收 migrator 与非秘密 blueprint / Migration port receives only migrator and a non-secret blueprint.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @return 无返回值 / No return value.
    """

    secret = "migrator-config:@/%雪"

    def reject_unrelated_adapter(*_arguments: object, **_keywords: object) -> object:
        """@brief 禁止 migration 组合构造无关 adapter / Reject unrelated adapter construction.

        @param _arguments 未使用位置参数 / Unused positional arguments.
        @param _keywords 未使用关键字参数 / Unused keyword arguments.
        @return 永不返回 / Never returns.
        @raise AssertionError 若组合根越过命令边界 / If composition crosses the command boundary.
        """

        raise AssertionError("migration composition constructed an unrelated adapter")

    monkeypatch.setattr(
        "dbctl.composition.LocalPsqlBootstrapRunnerFactory",
        reject_unrelated_adapter,
    )
    monkeypatch.setattr(
        "dbctl.composition.PsycopgTelemetryRetentionAdapter",
        reject_unrelated_adapter,
    )
    monkeypatch.setattr(
        "dbctl.composition.PsqlShellAdapter",
        reject_unrelated_adapter,
    )
    settings, migration = compose_migration(
        _initialized_config(tmp_path, migrator_password=secret),
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    observed: list[tuple[LoginDatabase, object]] = []

    def fake_upgrade(
        _adapter: AlembicMigrationAdapter,
        login: LoginDatabase,
        revision: MigrationRevision,
        blueprint: object,
    ) -> None:
        """@brief 捕获应用服务传给 adapter 的 authority / Capture authority passed to the adapter.

        @param _adapter adapter 实例 / Adapter instance.
        @param login 强类型登录 / Typed login.
        @param revision 迁移 revision / Migration revision.
        @param blueprint 非秘密数据库 blueprint / Non-secret database blueprint.
        @return 无返回值 / No return value.
        """

        assert revision.value == "head"
        observed.append((login, blueprint))

    monkeypatch.setattr(AlembicMigrationAdapter, "upgrade", fake_upgrade)
    migration.execute(
        settings.connections.migrator,
        MigrationRevision(),
        settings,
    )

    assert observed == [(settings.connections.migrator, settings.blueprint)]
    assert observed[0][0].password.reveal() == secret


@pytest.mark.parametrize("command", ("migrate", "shell"))
def test_non_bootstrap_commands_never_create_missing_config(
    command: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief 非 bootstrap 命令缺配置时只读失败 / Non-bootstrap commands fail read-only when config is missing.

    @param command dbctl 子命令 / dbctl subcommand.
    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param capsys pytest 输出夹具 / pytest output fixture.
    @return 无返回值 / No return value.
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
