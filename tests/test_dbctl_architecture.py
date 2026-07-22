"""@brief dbctl 分层架构静态护栏 / Static guardrails for the dbctl layered architecture."""

from __future__ import annotations

import ast
import sys
from importlib.util import resolve_name
from pathlib import Path

from conftest import PROJECT_ROOT

_SOURCE_ROOT = PROJECT_ROOT / "src"
"""@brief src-layout 源码根目录 / Source root of the src layout."""

_DBCTL_ROOT = _SOURCE_ROOT / "dbctl"
"""@brief dbctl package 根目录 / Root directory of the dbctl package."""

_LAYERS = ("domain", "application", "infrastructure", "interfaces")
"""@brief dbctl 必须具备的四个架构层 / Four architectural layers required in dbctl."""

_LEGACY_FLAT_MODULES = frozenset(
    {
        "bootstrap.py",
        "cli.py",
        "config.py",
        "connection.py",
        "container_entrypoint.py",
        "credentials.py",
        "domain.py",
        "errors.py",
        "identifiers.py",
        "migration.py",
        "package_resources.py",
        "retention.py",
        "runners.py",
        "shell.py",
    }
)
"""@brief 已被分层实现取代的平铺模块 / Flat modules superseded by the layered implementation."""

_OUTER_LAYER_PREFIXES = ("dbctl.infrastructure", "dbctl.interfaces")
"""@brief 核心层禁止反向导入的外层前缀 / Outer-layer prefixes forbidden in core layers."""


def test_dbctl_uses_complete_src_layout_layer_packages() -> None:
    """@brief 四层都必须是 src 下的真实 Python package / Every layer is a real Python package under src.

    @return 无返回值 / No return value.
    """

    missing = [
        str((_DBCTL_ROOT / layer / "__init__.py").relative_to(PROJECT_ROOT))
        for layer in _LAYERS
        if not (_DBCTL_ROOT / layer / "__init__.py").is_file()
    ]
    assert missing == [], "dbctl 缺少分层 package：\n" + "\n".join(missing)


def test_domain_imports_only_domain_and_standard_library() -> None:
    """@brief 领域层只能依赖自身与标准库 / Domain depends only on itself and the standard library.

    @return 无返回值 / No return value.
    """

    violations = _invalid_imports(
        layer="domain",
        allowed_dbctl_prefixes=("dbctl.domain",),
    )
    assert violations == [], "domain 存在向外依赖：\n" + "\n".join(violations)


def test_application_imports_only_domain_application_and_standard_library() -> None:
    """@brief 应用层只依赖领域、同层模块与标准库 / Application depends only on domain, peers, and stdlib.

    @return 无返回值 / No return value.
    """

    violations = _invalid_imports(
        layer="application",
        allowed_dbctl_prefixes=("dbctl.domain", "dbctl.application"),
    )
    assert violations == [], "application 存在向外依赖：\n" + "\n".join(violations)


def test_core_layers_never_import_infrastructure_or_interfaces() -> None:
    """@brief 内层不得反向认识 adapter / Inner layers must not know outer adapters.

    @return 无返回值 / No return value.
    """

    violations: list[str] = []
    for layer in ("domain", "application"):
        for source in _python_sources(layer):
            for imported in _resolved_imports(source):
                if any(
                    imported == prefix or imported.startswith(f"{prefix}.")
                    for prefix in _OUTER_LAYER_PREFIXES
                ):
                    violations.append(f"{source.relative_to(PROJECT_ROOT)} -> {imported}")
    assert violations == [], "核心层发生依赖倒置：\n" + "\n".join(violations)


def test_legacy_flat_dbctl_modules_are_removed() -> None:
    """@brief 分层实现不得与旧平铺实现并存 / Layered and legacy flat implementations must not coexist.

    @return 无返回值 / No return value.
    """

    remaining = sorted(
        path.name for path in _DBCTL_ROOT.glob("*.py") if path.name in _LEGACY_FLAT_MODULES
    )
    assert remaining == [], "仍存在旧 dbctl 平铺模块：\n" + "\n".join(remaining)


def test_core_packages_do_not_reexport_barrel_apis() -> None:
    """@brief 核心层 package 仅作标记，不集中转发 API / Core packages are markers, not API barrels.

    @return 无返回值 / No return value.
    """

    violations: list[str] = []
    for layer in ("application", "domain"):
        source = _DBCTL_ROOT / layer / "__init__.py"
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        if any(isinstance(node, (ast.Import, ast.ImportFrom)) for node in ast.walk(tree)):
            violations.append(str(source.relative_to(PROJECT_ROOT)))
    assert violations == [], "核心层不得维护集中导出面：\n" + "\n".join(violations)


def test_application_ports_are_colocated_with_their_use_cases() -> None:
    """@brief 禁止恢复与用例割裂的集中 ports 模块 / Keep ports colocated with use cases.

    @return 无返回值 / No return value.
    """

    assert not (_DBCTL_ROOT / "application" / "ports.py").exists()


def _invalid_imports(
    *,
    layer: str,
    allowed_dbctl_prefixes: tuple[str, ...],
) -> list[str]:
    """@brief 找出一个核心层的非标准库或越层导入 / Find non-stdlib or outward imports in a core layer.

    @param layer 被检查的核心层 / Core layer under inspection.
    @param allowed_dbctl_prefixes 允许的 dbctl 内部 package 前缀 / Allowed internal dbctl package prefixes.
    @return 文件与非法 import 的稳定排序描述 / Sorted descriptions of files and invalid imports.
    """

    violations: list[str] = []
    for source in _python_sources(layer):
        for imported in _resolved_imports(source):
            top_level = imported.partition(".")[0]
            is_allowed_internal = any(
                imported == prefix or imported.startswith(f"{prefix}.")
                for prefix in allowed_dbctl_prefixes
            )
            if top_level not in sys.stdlib_module_names and not is_allowed_internal:
                violations.append(f"{source.relative_to(PROJECT_ROOT)} -> {imported}")
    return sorted(violations)


def _python_sources(layer: str) -> tuple[Path, ...]:
    """@brief 返回一个架构层中的全部 Python 源码 / Return every Python source in one layer.

    @param layer dbctl 下的层目录名 / Layer-directory name under dbctl.
    @return 按路径排序的非空源码 tuple / Non-empty tuple of sources sorted by path.
    """

    sources = tuple(sorted((_DBCTL_ROOT / layer).rglob("*.py")))
    assert sources, f"dbctl.{layer} 不得为空"
    return sources


def _resolved_imports(source: Path) -> tuple[str, ...]:
    """@brief 将绝对与相对 import 解析为规范绝对路径 / Resolve absolute and relative imports canonically.

    @param source 待解析的 Python 源文件 / Python source file to parse.
    @return 包含被导入符号的绝对路径 / Absolute paths including imported symbols.

    @note 保留 from-import 的符号名可阻止 ``from dbctl import infrastructure`` 绕过
    package 前缀检查。/ Keeping imported symbols prevents package-level imports from bypassing
    the layer-prefix checks.
    """

    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    package = ".".join(source.relative_to(_SOURCE_ROOT).parent.parts)
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        module = node.module or ""
        if node.level:
            module = resolve_name(f"{'.' * node.level}{module}", package)
        for alias in node.names:
            imports.append(module if alias.name == "*" else f"{module}.{alias.name}")
    return tuple(imports)
