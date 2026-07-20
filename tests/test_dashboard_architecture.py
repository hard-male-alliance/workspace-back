"""@brief Dashboard 分层依赖方向测试 / Dashboard layered-dependency direction tests."""

from __future__ import annotations

import ast
from pathlib import Path

from conftest import PROJECT_ROOT

_DASHBOARD_ROOT = PROJECT_ROOT / "src" / "dashboard"
"""@brief Dashboard 源码根 / Dashboard source root."""

_FORBIDDEN_CORE_PREFIXES = (
    "dashboard.infrastructure",
    "dashboard.interfaces",
    "fastapi",
    "matplotlib",
    "plotly",
    "PyQt6",
    "sqlalchemy",
)
"""@brief domain/application 禁止依赖的外层模块 / Outer modules forbidden from domain/application."""


def test_domain_and_application_do_not_import_outer_adapters() -> None:
    """@brief 核心层只能依赖向内层 / Core layers may only depend inward."""

    violations: list[str] = []
    for layer in ("domain", "application"):
        for path in (_DASHBOARD_ROOT / layer).glob("*.py"):
            for imported in _imports(path):
                if imported.startswith(_FORBIDDEN_CORE_PREFIXES):
                    violations.append(f"{path.relative_to(PROJECT_ROOT)} -> {imported}")
    assert violations == []


def test_legacy_flat_dashboard_implementation_is_removed() -> None:
    """@brief 单一分层实现不得与旧 flat 模块并存 / The single layered implementation must not coexist with legacy flat modules."""

    legacy = {
        "access.py",
        "api.py",
        "cli.py",
        "composition.py",
        "config.py",
        "errors.py",
        "gui.py",
        "models.py",
        "ports.py",
        "repositories.py",
        "service.py",
    }
    assert not legacy.intersection(path.name for path in _DASHBOARD_ROOT.glob("*.py"))


def _imports(path: Path) -> tuple[str, ...]:
    """@brief 提取 Python 文件 import 根路径 / Extract import paths from a Python file.

    @param path Python 文件 / Python file.
    @return import 路径 / Import paths.
    """

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.append(node.module)
    return tuple(imports)
