"""@brief 根配置示例完整性测试 / Root configuration-example completeness tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.config import BackendSettings
from dashboard.config import DashboardSettings
from dbctl.config import DbctlConfigurationService
from workspace_shared.jsonc import load_jsonc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根路径 / Repository-root path."""


@pytest.mark.parametrize("file_name", ("config.jsonc", "example.jsonc"))
def test_root_configuration_examples_load_in_every_application(file_name: str) -> None:
    """@brief 三个独立应用均应接受完整根配置 / Every independent application accepts each complete root configuration.

    @param file_name 待校验的根 JSONC 文件名 / Root JSONC filename to validate.
    @return 无返回值；任一配置服务拒绝示例即令测试失败。

    @note 该回归测试保护“一个共享事实来源、三个独立配置服务”的架构边界；它不让
    backend 配置对象替 dashboard 或 dbctl 代为验证设置。
    """
    path = PROJECT_ROOT / file_name
    root = load_jsonc(path)

    backend = BackendSettings.from_file(path)
    dashboard = DashboardSettings.from_root_mapping(root)
    dbctl = DbctlConfigurationService(path).load()

    assert backend.environment == "development"
    assert dashboard.observability_view == "observability.dashboard_metric_samples"
    assert dbctl.administration.observability_schema == "observability"
