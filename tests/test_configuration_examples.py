"""@brief 根配置示例完整性测试 / Root configuration-example completeness tests."""

from __future__ import annotations

import os
from pathlib import Path

from backend.config import BackendSettings
from dashboard.config import DashboardSettings
from dbctl.config import DbctlConfigurationService
from workspace_shared.jsonc import load_jsonc

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
    passwords = generated["database_role_passwords"]
    assert set(passwords) == {"migrator", "app", "dashboard"}
    assert all(isinstance(password, str) and len(password) >= 32 for password in passwords.values())
    assert os.stat(config_path).st_mode & 0o777 == 0o600
    assert dbctl.administration.observability_schema == "observability"
