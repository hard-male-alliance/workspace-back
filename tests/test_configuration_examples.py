"""@brief 根配置示例完整性测试 / Root configuration-example completeness tests."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pytest

from backend.config import BackendSettings
from dashboard.application.errors import DashboardConfigurationError
from dashboard.infrastructure.config import DashboardQuerySettings, DashboardSettings
from dbctl.application.errors import DbctlConfigurationError
from dbctl.infrastructure.configuration import DbctlConfigStore
from dbctl.infrastructure.postgres.conninfo import parse_postgres_dsn
from workspace_shared.jsonc import ConfigurationError, load_jsonc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根路径 / Repository-root path."""


def test_public_runtime_example_loads_in_product_applications() -> None:
    """@brief 无密钥运行配置示例应被产品应用接受 / Product applications accept the secret-free runtime example.

    @return 无返回值；任一配置服务拒绝示例即令测试失败。
    """
    path = PROJECT_ROOT / "example.jsonc"
    root = load_jsonc(path)

    backend = BackendSettings.from_file(path)
    dashboard = DashboardSettings.from_root_mapping(root)

    assert backend.environment == "development"
    assert dashboard.default_workspace_id == "ws_local_demo"
    assert dashboard.database.mode == "memory"


def test_dbctl_creates_private_config_and_loads_separate_dbinit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief dbctl 应从公开模板生成私密配置并独立读取 dbinit / dbctl generates private config and reads dbinit separately.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 工作目录隔离夹具 / Pytest working-directory isolation fixture.
    @return 无返回值 / No return value.
    """
    example = (PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8")
    (tmp_path / "example.jsonc").write_text(example, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.jsonc"
    dbctl = DbctlConfigStore(
        dbinit_path=PROJECT_ROOT / "dbinit.jsonc",
    ).initialize()

    generated = load_jsonc(config_path)
    assert "database_role_passwords" not in generated
    database = generated["database"]
    expected_users = {
        "application_dsn": "workspace_app",
        "migrator_dsn": "workspace_migrator",
        "dashboard_dsn": "workspace_dashboard",
    }
    parsed = {field_name: parse_postgres_dsn(database[field_name]) for field_name in expected_users}
    assert {field_name: dsn.user.value for field_name, dsn in parsed.items()} == expected_users
    passwords = [dsn.password.reveal() for dsn in parsed.values()]
    assert all(isinstance(password, str) and len(password) >= 32 for password in passwords)
    assert len(set(passwords)) == 3
    backend = BackendSettings.from_file(config_path)
    dashboard = DashboardSettings.from_root_mapping(generated)
    assert backend.database.application_dsn is not None
    assert parse_postgres_dsn(backend.database.application_dsn).user.value == "workspace_app"
    assert dashboard.database.dsn is not None
    assert parse_postgres_dsn(dashboard.database.dsn).user.value == "workspace_dashboard"
    assert os.stat(config_path).st_mode & 0o777 == 0o600
    assert dbctl.blueprint.observability_schema.value == "observability"


def test_dbctl_rejects_legacy_password_mapping_without_rewriting_config(tmp_path: Path) -> None:
    """@brief 旧密码表不得被静默迁移或改写 / Legacy password tables fail without rewriting config.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """
    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    database = root["database"]
    for field_name in ("application_dsn", "migrator_dsn", "dashboard_dsn"):
        database.pop(field_name)
    legacy_passwords = {
        "migrator": "legacy-migrator-password",
        "app": "legacy-application-password",
        "dashboard": "legacy-dashboard-password",
    }
    root["database_role_passwords"] = legacy_passwords
    config_path = tmp_path / "config.jsonc"
    config_path.write_text(json.dumps(root), encoding="utf-8")
    config_path.chmod(0o600)

    original = config_path.read_text(encoding="utf-8")
    with pytest.raises(DbctlConfigurationError, match="migrator_dsn"):
        DbctlConfigStore(config_path, PROJECT_ROOT / "dbinit.jsonc").load()
    assert config_path.read_text(encoding="utf-8") == original


def test_backend_postgresql_mode_requires_application_dsn_in_config(tmp_path: Path) -> None:
    """@brief Backend PostgreSQL 模式必须从 config.jsonc 得到 DSN / Backend PostgreSQL mode requires its config.jsonc DSN.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """
    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["database"]["mode"] = "postgresql"
    path = tmp_path / "config.jsonc"
    path.write_text(json.dumps(root), encoding="utf-8")

    with pytest.raises(ConfigurationError, match=r"database\.application_dsn"):
        BackendSettings.from_file(path)


def test_backend_requires_explicit_logging_routes(tmp_path: Path) -> None:
    """@brief 日志分流不得依赖隐藏的旧配置默认值 / Log routing must not rely on hidden legacy defaults.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @return 无返回值 / No return value.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["logging"].pop("routes")
    path = tmp_path / "config.jsonc"
    path.write_text(json.dumps(root), encoding="utf-8")

    with pytest.raises(ConfigurationError, match=r"logging\.routes"):
        BackendSettings.from_file(path)


def test_backend_logging_shutdown_budget_has_a_hard_cap(tmp_path: Path) -> None:
    """@brief 配置不得把日志关闭重新变成无界等待 / Configuration cannot restore unbounded logging shutdown.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["logging"]["shutdown_timeout_ms"] = 60_001
    path = tmp_path / "config.jsonc"
    path.write_text(json.dumps(root), encoding="utf-8")

    with pytest.raises(ConfigurationError, match=r"logging\.shutdown_timeout_ms"):
        BackendSettings.from_file(path)


def test_backend_rejects_duplicate_stream_routes(tmp_path: Path) -> None:
    """@brief 一个 stream 只能由一个 worker 拥有 / One worker exclusively owns a stream.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["logging"]["routes"].append({"sink": "stdout", "levels": ["INFO"]})
    path = tmp_path / "config.jsonc"
    path.write_text(json.dumps(root), encoding="utf-8")

    with pytest.raises(ConfigurationError, match=r"duplicates the stdout sink"):
        BackendSettings.from_file(path)


def test_backend_rejects_multiple_rotators_for_one_file(tmp_path: Path) -> None:
    """@brief 同一路径只能由一个轮转 worker 拥有 / One rotating worker exclusively owns a file path.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    file_route = next(route for route in root["logging"]["routes"] if route["sink"] == "file")
    root["logging"]["routes"].append(dict(file_route))
    path = tmp_path / "config.jsonc"
    path.write_text(json.dumps(root), encoding="utf-8")

    with pytest.raises(ConfigurationError, match=r"distinct file sink"):
        BackendSettings.from_file(path)


def test_dashboard_postgresql_mode_requires_dashboard_dsn_in_config() -> None:
    """@brief Dashboard PostgreSQL 模式必须从 config.jsonc 得到 DSN / Dashboard PostgreSQL mode requires its config.jsonc DSN.

    @return 无返回值 / No return value.
    """
    with pytest.raises(DashboardConfigurationError, match=r"database\.dashboard_dsn"):
        DashboardSettings.from_root_mapping(
            {
                "environment": "development",
                "workspace": {"default_workspace_id": "ws-test"},
                "database": {"mode": "postgresql", "dashboard_dsn": None},
                "dashboard": {"access": {"mode": "operator_token"}},
            }
        )


def test_dashboard_config_service_fails_when_root_config_is_missing(tmp_path: Path) -> None:
    """@brief Dashboard 默认不得绕过根配置 / Dashboard must not bypass the root config by default.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """
    with pytest.raises(DashboardConfigurationError, match="找不到配置文件"):
        DashboardSettings.from_file(tmp_path / "missing.jsonc")


def test_dashboard_query_configuration_cannot_raise_absolute_budgets() -> None:
    """@brief 配置文件不得绕过查询的代码级上限 / Configuration cannot bypass absolute query budgets."""

    with pytest.raises(DashboardConfigurationError, match="31 天"):
        DashboardQuerySettings(max_window=timedelta(days=32))
    with pytest.raises(DashboardConfigurationError, match="60000"):
        DashboardQuerySettings(statement_timeout_ms=60_001)
    with pytest.raises(DashboardConfigurationError, match="1 秒到 1 天"):
        DashboardQuerySettings(freshness_target=timedelta(days=1, seconds=1))
    with pytest.raises(DashboardConfigurationError, match="2000"):
        DashboardQuerySettings(target_points=2_001)
    with pytest.raises(DashboardConfigurationError, match="1000"):
        DashboardQuerySettings(max_event_limit=1_001)
