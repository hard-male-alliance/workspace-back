"""@brief dbctl 强类型领域与配置边界测试 / dbctl typed-domain and configuration-boundary tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from conftest import PROJECT_ROOT
from dbctl.application.errors import DbctlConfigurationError, ExternalDiagnosticError
from dbctl.domain.errors import InvalidRetentionPolicyError
from dbctl.domain.retention import PruneLimits
from dbctl.domain.roles import Secret
from dbctl.infrastructure.configuration import DbctlConfigStore
from dbctl.infrastructure.postgres.conninfo import parse_postgres_dsn
from workspace_shared.jsonc import load_jsonc


def _private_config(tmp_path: Path) -> Path:
    """@brief 初始化一份隔离私密配置 / Initialize one isolated private config.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return config.jsonc 路径 / config.jsonc path.
    """

    path = tmp_path / "config.jsonc"
    path.write_text((PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8"))
    DbctlConfigStore(path, PROJECT_ROOT / "dbinit.jsonc").initialize()
    return path


def test_connection_catalog_binds_every_login_to_one_exact_target(tmp_path: Path) -> None:
    """@brief 三个登录必须绑定同一 host/port/database / All logins bind to one host/port/database.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """

    settings = DbctlConfigStore(
        _private_config(tmp_path),
        PROJECT_ROOT / "dbinit.jsonc",
    ).load()
    target = settings.connections.target

    assert (target.host, target.port, target.database.value) == (
        "127.0.0.1",
        5432,
        "ai_job_workspace",
    )
    assert settings.connections.migrator.target == target
    assert settings.connections.application.target == target
    assert settings.connections.dashboard.target == target
    assert (
        len(
            {
                settings.connections.migrator.password.reveal(),
                settings.connections.application.password.reveal(),
                settings.connections.dashboard.password.reveal(),
            }
        )
        == 3
    )


def test_config_rejects_dsn_target_drift_without_secret_cause(tmp_path: Path) -> None:
    """@brief DSN endpoint 漂移在配置边界 fail closed / DSN endpoint drift fails at the config boundary.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """

    path = _private_config(tmp_path)
    root = load_jsonc(path)
    root["database"]["application_dsn"] = (
        "postgresql://workspace_app:secret-sentinel@db.example.test:5432/ai_job_workspace"
    )
    path.write_text(json.dumps(root), encoding="utf-8")

    with pytest.raises(DbctlConfigurationError, match="host、port 或 database") as error_info:
        DbctlConfigStore(path, PROJECT_ROOT / "dbinit.jsonc").load()
    assert "secret-sentinel" not in str(error_info.value)
    assert error_info.value.__cause__ is None


@pytest.mark.parametrize("option", ("hostaddr", "options", "passfile", "service"))
def test_dsn_parser_rejects_implicit_routing_and_credential_options(option: str) -> None:
    """@brief DSN 不能携带绕过强类型 target 的 libpq 选项 / DSNs reject options bypassing the typed target.

    @param option 被拒绝 libpq option / Rejected libpq option.
    @return 无返回值 / No return value.
    """

    dsn = (
        "postgresql://workspace_app:secret-sentinel@127.0.0.1:5432/ai_job_workspace"
        f"?{option}=unsafe-value"
    )
    with pytest.raises(DbctlConfigurationError) as error_info:
        parse_postgres_dsn(dsn)
    assert "secret-sentinel" not in str(error_info.value)
    assert isinstance(error_info.value.__cause__, ExternalDiagnosticError)
    assert "secret-sentinel" not in str(error_info.value.__cause__)
    assert "postgresql://" not in str(error_info.value.__cause__)


def test_dbinit_rejects_fake_schema_customization_before_writing_config(
    tmp_path: Path,
) -> None:
    """@brief migration 固定 schema 不再伪装可配置 / Migration-fixed schemas are no longer fake-configurable.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """

    dbinit = load_jsonc(PROJECT_ROOT / "dbinit.jsonc")
    dbinit["database_administration"]["schemas"][-1] = "telemetry"
    dbinit_path = tmp_path / "dbinit.jsonc"
    dbinit_path.write_text(json.dumps(dbinit), encoding="utf-8")
    config_path = tmp_path / "config.jsonc"

    with pytest.raises(DbctlConfigurationError, match="canonical catalog"):
        DbctlConfigStore(config_path, dbinit_path).initialize()
    assert not config_path.exists()


def test_dbinit_rejects_bootstrap_user_colliding_with_managed_role(
    tmp_path: Path,
) -> None:
    """@brief 管理身份不能与受管 role 重名 / Administrative identity cannot collide with a managed role.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """

    dbinit = load_jsonc(PROJECT_ROOT / "dbinit.jsonc")
    dbinit["database_administration"]["bootstrap_database_user"] = "workspace_owner"
    dbinit_path = tmp_path / "dbinit.jsonc"
    dbinit_path.write_text(json.dumps(dbinit), encoding="utf-8")

    with pytest.raises(DbctlConfigurationError, match="受管业务角色"):
        DbctlConfigStore(tmp_path / "config.jsonc", dbinit_path).initialize()


def test_secret_repr_and_retention_cross_bounds_make_illegal_states_unrepresentable() -> None:
    """@brief secret 与 timeout 非法状态在构造时失败 / Secret and timeout illegal states fail at construction.

    @return 无返回值 / No return value.
    """

    secret = Secret("domain-secret-sentinel")
    assert "domain-secret-sentinel" not in repr(secret)
    assert str(secret) == "<redacted>"
    with pytest.raises(InvalidRetentionPolicyError, match="lock_timeout_ms"):
        PruneLimits(statement_timeout_ms=100, lock_timeout_ms=500)
