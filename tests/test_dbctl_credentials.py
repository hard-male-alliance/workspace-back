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
from dbctl.application.errors import MigrationExecutionError
from dbctl.application.migrate import MigrationRevision
from dbctl.composition import compose_dbctl
from dbctl.domain.database import LoginDatabase
from dbctl.domain.roles import LoginRole
from dbctl.infrastructure.alembic import AlembicMigrationAdapter
from dbctl.infrastructure.configuration import DbctlConfigStore
from dbctl.interfaces.cli import main
from workspace_shared.jsonc import load_jsonc


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
    application = compose_dbctl(
        config_path,
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
        environ=inherited,
    )
    login = application.settings.connections.application
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
    assert application.shell.execute(login) == 37
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

    application = compose_dbctl(
        _initialized_config(tmp_path),
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    login = application.settings.connections.login_for(role)

    assert login.role is role
    assert login.role_name.value == expected_name
    assert login.password.reveal()
    assert login.password.reveal() not in repr(login)
    assert login.target == application.settings.connections.target


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
    application = compose_dbctl(
        _initialized_config(tmp_path, migrator_password=secret),
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    login = application.settings.connections.migrator

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
        application.settings.blueprint,
    )


def test_migration_failure_never_displays_or_chains_config_dsn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief Alembic 异常不进入显示文本或 cause chain / Alembic errors enter neither display text nor cause chains.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 替换夹具 / pytest patch fixture.
    @return 无返回值 / No return value.
    """

    secret = "migration-secret:@/%雪"
    application = compose_dbctl(
        _initialized_config(tmp_path, migrator_password=secret),
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    )
    login = application.settings.connections.migrator

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
            application.settings.blueprint,
        )
    assert secret not in str(error_info.value)
    assert "postgresql://" not in str(error_info.value)
    assert error_info.value.__cause__ is None


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
    application = compose_dbctl(
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
    application.migration.execute(
        application.settings.connections.migrator,
        MigrationRevision(),
        application.settings,
    )

    assert observed == [(application.settings.connections.migrator, application.settings.blueprint)]
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
