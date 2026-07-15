"""@brief 三个可执行应用的静态模块边界测试 / Static module-boundary tests for the three executable applications."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Repository project root."""

_APPLICATION_PACKAGES = frozenset({"backend", "dashboard", "dbctl"})
"""@brief 彼此必须隔离的 executable package 集 / Executable packages that must remain isolated."""


def _imported_application_packages(source: Path) -> set[str]:
    """@brief 解析一个模块直接导入的 executable package / Parse executable packages directly imported by one module.

    @param source 待检查的 Python 源文件 / Python source file to inspect.
    @return 直接 import 或 from-import 的顶层应用 package 名。
    """
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    imported: set[str] = set()
    for node in ast.walk(tree):
        module_name: str | None = None
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.partition(".")[0])
            continue
        if isinstance(node, ast.ImportFrom) and node.level == 0:
            module_name = node.module
        if module_name:
            imported.add(module_name.partition(".")[0])
    return imported & _APPLICATION_PACKAGES


@pytest.mark.parametrize("package", sorted(_APPLICATION_PACKAGES))
def test_executable_application_packages_never_import_each_other(package: str) -> None:
    """@brief backend、dashboard、dbctl 只能依赖自身和纯共享层 / Each executable app may depend only on itself and the pure shared layer.

    @param package 当前被验证的应用 package / Application package under verification.
    @return 无返回值。

    @note 这是模块化单体（modular monolith）的硬边界：三者不能借 import、内部 HTTP 或
    隐式 composition root 形成分布式单体。业务规则应停留在 backend application/domain，
    而共享范围仅限 ``workspace_shared`` 的纯契约与无业务语义工具。
    """
    package_root = _PROJECT_ROOT / "src" / package
    violations: list[str] = []
    for source in package_root.rglob("*.py"):
        foreign_imports = _imported_application_packages(source) - {package}
        if foreign_imports:
            violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {sorted(foreign_imports)}")
    assert not violations, "跨可执行应用 import 破坏模块边界：\n" + "\n".join(violations)


def test_backend_domain_never_imports_infrastructure() -> None:
    """@brief 领域层不得倒置依赖基础设施 / Domain layer must never depend on infrastructure.

    @return 无返回值。

    @note domain（领域层）只表达业务状态、类型和 ports（端口）。数据库、HTTP、
    subprocess、日志和 provider 细节只能由 infrastructure（基础设施层）实现，防止
    测试替身与生产适配器反向污染业务规则。
    """
    domain_root = _PROJECT_ROOT / "src" / "backend" / "domain"
    violations: list[str] = []
    for source in domain_root.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.level == 0 and (
                node.module == "backend.infrastructure"
                or (
                    isinstance(node.module, str)
                    and node.module.startswith("backend.infrastructure.")
                )
            ):
                violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {node.module}")
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "backend.infrastructure" or alias.name.startswith(
                        "backend.infrastructure."
                    ):
                        violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {alias.name}")
    assert not violations, "domain 反向依赖 infrastructure：\n" + "\n".join(violations)
