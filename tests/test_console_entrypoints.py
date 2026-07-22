"""@brief 已发布命令行入口的打包契约测试 / Packaging-contract tests for published CLI entrypoints."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from backend.app import config_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根目录 / Repository root."""


def test_canonical_console_commands_are_declared_without_aliases() -> None:
    """@brief 只发布五个简洁且无重复语义的规范命令 / Publish only five concise canonical commands.

    @return 无返回值 / No return value.
    """

    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = project["project"]["scripts"]

    assert scripts["backend"] == "backend.__main__:main"
    assert scripts["dashboard"] == "dashboard.__main__:main"
    assert scripts["dashboard-api"] == "dashboard.interfaces.api:main"
    assert scripts["dashboard-gui"] == "dashboard.interfaces.gui:main"
    assert scripts["dbctl"] == "dbctl.__main__:main"
    assert set(scripts) == {
        "backend",
        "dashboard",
        "dashboard-api",
        "dashboard-gui",
        "dbctl",
    }


def test_backend_default_config_is_runtime_relative(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief 安装态 backend 不得把配置定位到虚拟环境 / Installed backend must not locate config inside its virtual environment.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @param tmp_path 临时运行目录 / Temporary runtime directory.
    @return 无返回值 / No return value.
    """

    monkeypatch.chdir(tmp_path)
    assert config_path().resolve() == tmp_path / "config.jsonc"
