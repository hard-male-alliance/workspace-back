"""@brief dbctl 安装资源的唯一定位器 / Canonical locator for installed dbctl resources."""

from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources import as_file, files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Final, Literal

from dbctl.application.errors import DbctlConfigurationError

type DefaultTextResource = Literal["example.jsonc", "dbinit.jsonc"]
"""@brief 可读取的固定文本资源名 / Fixed text-resource names that may be read."""

_PACKAGE_NAME: Final = "dbctl"
"""@brief importlib.resources 使用的包名 / Package name used by importlib.resources."""

_RESOURCE_DIRECTORY: Final = "resources"
"""@brief wheel 内固定资源目录 / Fixed resource directory inside the wheel."""

_ALEMBIC_DIRECTORY: Final = "alembic"
"""@brief wheel 内 Alembic 脚本目录 / Alembic script directory inside the wheel."""


def read_default_text(name: DefaultTextResource) -> str:
    """@brief 从 wheel 或已验证源码 checkout 读取默认文本 / Read default text from the wheel or a verified checkout.

    @param name 固定白名单中的资源名 / Resource name from the fixed allowlist.
    @return UTF-8 资源文本 / UTF-8 resource text.
    @raise DbctlConfigurationError 资源缺失或不可读时抛出 / Raised when the resource is missing or unreadable.
    """

    packaged = _packaged_resource(name)
    if packaged.is_file():
        try:
            return packaged.read_text(encoding="utf-8")
        except OSError as error:
            raise DbctlConfigurationError(f"内置 {name} 资源无法读取。") from error
    source_path = _source_checkout_resource(name)
    if source_path is not None and source_path.is_file():
        try:
            return source_path.read_text(encoding="utf-8")
        except OSError as error:
            raise DbctlConfigurationError(f"源码树 {name} 资源无法读取。") from error
    raise DbctlConfigurationError(f"未找到内置 {name} 资源。")


@contextmanager
def alembic_script_location() -> Iterator[Path]:
    """@brief 在上下文内物化完整 Alembic 脚本目录 / Materialize the complete Alembic scripts for one context.

    @return 上下文存续期间有效的脚本路径 / Script path valid for the context lifetime.
    @raise DbctlConfigurationError wheel 与源码 checkout 都不完整时抛出。
    / Raised when both the wheel and source checkout are incomplete.
    """

    packaged = _packaged_resource(_ALEMBIC_DIRECTORY)
    if packaged.is_dir():
        with as_file(packaged) as materialized:
            _validate_alembic_directory(materialized)
            yield materialized
            return
    source_path = _source_checkout_resource(_ALEMBIC_DIRECTORY)
    if source_path is None:
        raise DbctlConfigurationError("未找到内置 Alembic 脚本目录。")
    _validate_alembic_directory(source_path)
    yield source_path


def _packaged_resource(name: str) -> Traversable:
    """@brief 返回尚未物化的包资源 / Return an unmaterialized package resource.

    @param name 本模块提供的固定资源名 / Fixed resource name supplied by this module.
    @return importlib Traversable / importlib Traversable.
    """

    return files(_PACKAGE_NAME).joinpath(_RESOURCE_DIRECTORY, name)


def _source_checkout_resource(name: str) -> Path | None:
    """@brief 从固定祖先验证并定位源码资源 / Validate the fixed ancestor and locate a checkout resource.

    @param name 本模块提供的固定资源名 / Fixed resource name supplied by this module.
    @return checkout 资源路径；安装态返回 None / Checkout resource path, or None when installed.
    """

    repository_root = Path(__file__).resolve().parents[3]
    package_marker = repository_root / "src" / "dbctl" / "__init__.py"
    project_marker = repository_root / "pyproject.toml"
    if not package_marker.is_file() or not project_marker.is_file():
        return None
    return repository_root / name


def _validate_alembic_directory(path: Path) -> None:
    """@brief 验证可执行的最小 Alembic 资源结构 / Validate the minimum executable Alembic resource structure.

    @param path 候选脚本目录 / Candidate script directory.
    @return 无返回值 / No return value.
    @raise DbctlConfigurationError 缺少环境、模板或 revision 时抛出。
    / Raised when the environment, template, or revisions are missing.
    """

    required_files = ("env.py", "script.py.mako")
    if not path.is_dir() or not all((path / name).is_file() for name in required_files):
        raise DbctlConfigurationError("内置 Alembic 脚本目录不完整。")
    versions = path / "versions"
    if not versions.is_dir() or not any(versions.glob("*.py")):
        raise DbctlConfigurationError("内置 Alembic migration 版本目录为空。")


__all__ = ["DefaultTextResource", "alembic_script_location", "read_default_text"]
