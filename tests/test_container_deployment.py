"""@brief Docker 部署配置的回归测试 / Regression tests for Docker deployment configuration."""

from __future__ import annotations

import stat
from pathlib import Path

import pytest

from backend.config import BackendSettings
from dashboard.infrastructure.config import DashboardSettings
from dbctl.config import DatabaseRole, DbctlConfigurationService
from dbctl.container_entrypoint import build_runtime_config, write_runtime_files
from workspace_shared.jsonc import ConfigurationError


def _container_environment() -> dict[str, str]:
    """@brief 构造不含真实 secret 的容器测试环境 / Build a container test environment without real secrets.

    @return 独立数据库凭证与生产身份设置 / Distinct database credentials and production identity settings.
    """

    return {
        "AIWS_ENVIRONMENT": "production",
        "AIWS_PUBLIC_BASE_URL": "https://workspace.example.test",
        "AIWS_IDENTITY_MODE": "trusted_proxy_hmac",
        "AIWS_TRUSTED_PROXY_HMAC_SECRET": "test-only-hmac-secret-with-at-least-32-bytes",
        "AIWS_TRUSTED_PROXY_CIDRS": '["172.30.0.0/24"]',
        "AIWS_DB_HOST": "postgres",
        "AIWS_DB_PORT": "5432",
        "AIWS_DB_MIGRATOR_PASSWORD": "migrator:@/test",
        "AIWS_DB_APP_PASSWORD": "application:@/test",
        "AIWS_DB_DASHBOARD_PASSWORD": "dashboard:@/test",
    }


def test_generated_container_config_is_valid_for_every_process(tmp_path: Path) -> None:
    """@brief 生成配置应同时满足 backend、dashboard 与 dbctl / Generated config satisfies backend, dashboard, and dbctl.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @return 无返回值 / No return value.
    """

    config_path = tmp_path / "config.jsonc"
    dbinit_path = tmp_path / "dbinit.jsonc"
    write_runtime_files(config_path, dbinit_path, _container_environment())

    backend = BackendSettings.from_file(config_path)
    dashboard = DashboardSettings.from_file(config_path)
    dbctl = DbctlConfigurationService(config_path, dbinit_path).load()

    assert backend.environment == "production"
    assert backend.database.mode == "postgresql"
    assert backend.network.bind_host == "0.0.0.0"
    assert backend.security.identity_mode == "trusted_proxy_hmac"
    assert dashboard.database.mode == "postgresql"
    assert dashboard.api.host == "0.0.0.0"
    assert dashboard.access.mode == "operator_token"
    assert dbctl.role_passwords[DatabaseRole.APP] == "application:@/test"
    assert dbctl.role_passwords[DatabaseRole.MIGRATOR] == "migrator:@/test"
    assert dbctl.role_passwords[DatabaseRole.DASHBOARD] == "dashboard:@/test"
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(dbinit_path.stat().st_mode) == 0o644


def test_container_config_rejects_reused_database_passwords() -> None:
    """@brief 数据库角色不得共享密码 / Database roles must not share passwords."""

    environ = _container_environment()
    environ["AIWS_DB_APP_PASSWORD"] = environ["AIWS_DB_MIGRATOR_PASSWORD"]

    with pytest.raises(ConfigurationError, match="passwords must be distinct"):
        build_runtime_config(environ)


def test_default_container_config_uses_development_mocks() -> None:
    """@brief 默认 Compose 配置只启用开发 mock / Default Compose config uses development mocks only."""

    environ = _container_environment()
    del environ["AIWS_ENVIRONMENT"]
    del environ["AIWS_IDENTITY_MODE"]
    del environ["AIWS_TRUSTED_PROXY_HMAC_SECRET"]

    config = build_runtime_config(environ)

    assert config["environment"] == "development"
    assert config["security"]["identity_mode"] == "development_mock"
    assert config["ai"]["provider"] == "mock"
    assert config["resume_rendering"]["adapter"] == "mock"


def test_production_container_requires_hmac_secret() -> None:
    """@brief 生产容器缺少 HMAC secret 时必须 fail closed / Production container fails closed without an HMAC secret."""

    environ = _container_environment()
    del environ["AIWS_TRUSTED_PROXY_HMAC_SECRET"]

    with pytest.raises(ConfigurationError, match="AIWS_TRUSTED_PROXY_HMAC_SECRET"):
        build_runtime_config(environ)
