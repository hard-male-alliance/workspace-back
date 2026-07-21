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


def test_packaged_alembic_runtime_never_imports_executable_applications() -> None:
    """@brief dbctl 的 Alembic 资源不得反向导入应用 / Alembic runtime stays app-independent.

    @return 无返回值 / No return value.
    @note migration upgrade 使用显式 revision，不需要 backend ORM metadata；开发期 autogenerate
    应使用单独入口，不能把安装态 dbctl 与 backend infrastructure 粘合。
    / Explicit revisions need no backend ORM metadata. Development autogenerate belongs in a separate
    authoring entry point and must not couple installed dbctl runtime to backend infrastructure.
    """
    violations: list[str] = []
    for source in (_PROJECT_ROOT / "alembic").rglob("*.py"):
        foreign_imports = _imported_application_packages(source)
        if foreign_imports:
            violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {sorted(foreign_imports)}")
    assert not violations, "Alembic runtime 反向依赖 executable app：\n" + "\n".join(violations)


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
            if (
                isinstance(node, ast.ImportFrom)
                and node.level == 0
                and (
                    node.module == "backend.infrastructure"
                    or (
                        isinstance(node.module, str)
                        and node.module.startswith("backend.infrastructure.")
                    )
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


def test_backend_application_never_imports_infrastructure() -> None:
    """@brief 应用层通过端口依赖基础设施 / Application layer depends on infrastructure through ports.

    @return 无返回值 / No return value.

    @note composition root 可以把 adapter 注入 use case；application 本身不得认识具体
    pipeline、repository、HTTP 或 logging 实现，否则分层架构只是目录装饰。
    / The composition root may inject adapters, while application code must not know concrete
    pipeline, repository, HTTP, or logging implementations.
    """
    application_root = _PROJECT_ROOT / "src" / "backend" / "application"
    violations: list[str] = []
    for source in application_root.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            imported: str | None = None
            if isinstance(node, ast.ImportFrom) and node.level == 0:
                imported = node.module
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "backend.infrastructure" or alias.name.startswith(
                        "backend.infrastructure."
                    ):
                        violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {alias.name}")
                continue
            if imported == "backend.infrastructure" or (
                isinstance(imported, str) and imported.startswith("backend.infrastructure.")
            ):
                violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {imported}")
    assert not violations, "application 反向依赖 infrastructure：\n" + "\n".join(violations)


@pytest.mark.parametrize(
    ("layer", "forbidden_layers", "forbidden_libraries"),
    (
        (
            "domain",
            frozenset({"application", "infrastructure", "interfaces"}),
            frozenset({"fastapi", "matplotlib", "plotly", "PyQt6", "rich", "sqlalchemy"}),
        ),
        (
            "application",
            frozenset({"infrastructure", "interfaces"}),
            frozenset({"fastapi", "matplotlib", "plotly", "PyQt6", "rich", "sqlalchemy"}),
        ),
    ),
)
def test_dashboard_core_layers_only_depend_inward(
    layer: str,
    forbidden_layers: frozenset[str],
    forbidden_libraries: frozenset[str],
) -> None:
    """@brief Dashboard 核心层只能向内依赖，且不得认识呈现/数据库框架。

    @param layer 被检查的 Dashboard 层 / Dashboard layer under inspection.
    @param forbidden_layers 违反单向依赖的内部层名 / Internal layers that violate dependency direction.
    @param forbidden_libraries 核心层禁止导入的适配器库 / Adapter libraries forbidden in the core.
    @return 无返回值 / No return value.
    """

    layer_root = _PROJECT_ROOT / "src" / "dashboard" / layer
    violations: list[str] = []
    for source in layer_root.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            imported: str | None = None
            if isinstance(node, ast.ImportFrom):
                imported = node.module
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    root_name = alias.name.partition(".")[0]
                    if root_name in forbidden_libraries:
                        violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {alias.name}")
                continue
            if not imported:
                continue
            root_name = imported.partition(".")[0]
            if root_name in forbidden_libraries:
                violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {imported}")
            absolute_parts = imported.split(".")
            if absolute_parts[:1] == ["dashboard"] and len(absolute_parts) > 1:
                target_layer = absolute_parts[1]
            elif node.level > 0:
                target_layer = absolute_parts[0]
            else:
                target_layer = ""
            if target_layer in forbidden_layers:
                violations.append(f"{source.relative_to(_PROJECT_ROOT)} -> {imported}")

    assert not violations, f"dashboard.{layer} 依赖方向错误：\n" + "\n".join(violations)
