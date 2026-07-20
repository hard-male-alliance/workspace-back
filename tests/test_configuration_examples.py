"""@brief 根配置示例完整性测试 / Root configuration-example completeness tests."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from backend.config import BackendSettings
from dashboard.config import DashboardConfigService, DashboardSettings
from dashboard.errors import DashboardConfigurationError
from dbctl.config import DbctlConfigurationService
from dbctl.connection import parse_postgres_dsn
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
    assert dashboard.observability_view == "observability.dashboard_metric_samples"


def test_dbctl_creates_private_config_and_loads_separate_dbinit(tmp_path: Path) -> None:
    """@brief dbctl 应从公开模板生成私密配置并独立读取 dbinit / dbctl generates private config and reads dbinit separately.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """
    example = (PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8")
    (tmp_path / "example.jsonc").write_text(example, encoding="utf-8")
    config_path = tmp_path / "config.jsonc"
    dbctl = DbctlConfigurationService(
        config_path,
        PROJECT_ROOT / "dbinit.jsonc",
    ).load()

    generated = load_jsonc(config_path)
    assert "database_role_passwords" not in generated
    database = generated["database"]
    expected_users = {
        "application_dsn": "workspace_app",
        "migrator_dsn": "workspace_migrator",
        "dashboard_dsn": "workspace_dashboard",
    }
    parsed = {field_name: parse_postgres_dsn(database[field_name]) for field_name in expected_users}
    assert {field_name: dsn.user for field_name, dsn in parsed.items()} == expected_users
    passwords = [dsn.password for dsn in parsed.values()]
    assert all(isinstance(password, str) and len(password) >= 32 for password in passwords)
    assert len(set(passwords)) == 3
    backend = BackendSettings.from_file(config_path)
    dashboard = DashboardSettings.from_root_mapping(generated)
    assert backend.database.application_dsn is not None
    assert parse_postgres_dsn(backend.database.application_dsn).user == "workspace_app"
    assert dashboard.dashboard_dsn is not None
    assert parse_postgres_dsn(dashboard.dashboard_dsn).user == "workspace_dashboard"
    assert os.stat(config_path).st_mode & 0o777 == 0o600
    assert dbctl.administration.observability_schema == "observability"


def test_dbctl_migrates_legacy_password_mapping_without_rotation(tmp_path: Path) -> None:
    """@brief 旧密码映射应迁移为真实 DSN 且不轮换 / Legacy passwords migrate to actual DSNs without rotation.

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

    DbctlConfigurationService(config_path, PROJECT_ROOT / "dbinit.jsonc").load()

    migrated = load_jsonc(config_path)
    assert "database_role_passwords" not in migrated
    assert (
        parse_postgres_dsn(migrated["database"]["migrator_dsn"]).password
        == legacy_passwords["migrator"]
    )
    assert (
        parse_postgres_dsn(migrated["database"]["application_dsn"]).password
        == legacy_passwords["app"]
    )
    assert (
        parse_postgres_dsn(migrated["database"]["dashboard_dsn"]).password
        == legacy_passwords["dashboard"]
    )


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


def test_dashboard_postgresql_mode_requires_dashboard_dsn_in_config() -> None:
    """@brief Dashboard PostgreSQL 模式必须从 config.jsonc 得到 DSN / Dashboard PostgreSQL mode requires its config.jsonc DSN.

    @return 无返回值 / No return value.
    """
    with pytest.raises(DashboardConfigurationError, match=r"database\.dashboard_dsn"):
        DashboardSettings.from_root_mapping(
            {
                "environment": "development",
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
        DashboardConfigService(tmp_path / "missing.jsonc").load()
