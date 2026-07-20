"""@brief dbctl 安装包资源定位 / Installed-package resource location for dbctl."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources import as_file, files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Final, Literal

from .errors import DbctlConfigurationError

DefaultTextResource = Literal["example.jsonc", "dbinit.jsonc"]
"""@brief 可从安装包读取的默认文本资源名 / Default text resources readable from the package."""

_PACKAGE_NAME: Final[str] = "dbctl"
"""@brief importlib.resources 使用的包名 / Package name used by ``importlib.resources``."""

_RESOURCE_DIRECTORY: Final[str] = "resources"
"""@brief wheel 内资源目录名 / Resource-directory name inside the wheel."""

_ALEMBIC_DIRECTORY: Final[str] = "alembic"
"""@brief Alembic 脚本资源目录名 / Alembic script-resource directory name."""


def read_default_text(name: DefaultTextResource) -> str:
    """@brief 读取 wheel 或安全源码回退中的默认文本 / Read default text from the wheel or safe source fallback.

    @param name 固定白名单中的资源名 / Resource name from the fixed allowlist.
    @return UTF-8 资源文本 / UTF-8 resource text.
    @raise DbctlConfigurationError 资源缺失或不可读时抛出 / Raised when the resource is absent or unreadable.
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
    """@brief 物化 Alembic 脚本目录 / Materialize the Alembic script directory.

    @return 上下文存续期间有效的文件系统路径 / Filesystem path valid for the context lifetime.
    @raise DbctlConfigurationError wheel 与安全源码回退都缺少完整脚本时抛出。
    / Raised when neither the wheel nor the safe source fallback contains complete scripts.

    @note ``importlib.resources.as_file`` 同时支持普通 wheel 安装与 zip importer；调用方不得
    在上下文退出后缓存返回路径。/ ``importlib.resources.as_file`` supports both ordinary
    wheel installs and zip importers; callers must not cache the returned path after context exit.
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
    """@brief 返回 dbctl 包内固定资源 / Return a fixed resource inside the dbctl package.

    @param name 由本模块提供的固定资源名 / Fixed resource name supplied by this module.
    @return 尚未物化的 Traversable / Unmaterialized ``Traversable``.
    """

    return files(_PACKAGE_NAME).joinpath(_RESOURCE_DIRECTORY, name)


def _source_checkout_resource(name: str) -> Path | None:
    """@brief 在可验证源码树中定位资源 / Locate a resource in a verified source checkout.

    @param name 由本模块提供的固定资源名 / Fixed resource name supplied by this module.
    @return 固定仓库根资源路径；安装态或标记不完整时返回 ``None``。
    / Fixed repository-root resource path, or ``None`` outside a verified checkout.

    @note 回退只接受同时含 pyproject 和当前 dbctl 源包的固定祖先，绝不搜索 CWD、父目录链
    或用户输入路径。/ The fallback accepts only the fixed ancestor containing both pyproject and
    this dbctl source package; it never searches CWD, arbitrary parents, or user-provided paths.
    """

    repository_root = Path(__file__).resolve().parents[2]
    package_marker = repository_root / "src" / "dbctl" / "__init__.py"
    project_marker = repository_root / "pyproject.toml"
    if not package_marker.is_file() or not project_marker.is_file():
        return None
    return repository_root / name


def _validate_alembic_directory(path: Path) -> None:
    """@brief 校验最小 Alembic 资源结构 / Validate the minimum Alembic resource structure.

    @param path 候选脚本目录 / Candidate script directory.
    @return 无返回值 / No return value.
    @raise DbctlConfigurationError 缺少 env.py、模板或版本目录时抛出。
    / Raised when ``env.py``, the template, or the versions directory is missing.
    """

    required_file_names = ("env.py", "script.py.mako")
    """@brief Alembic 执行所需固定文件 / Fixed files required for Alembic execution."""
    if not path.is_dir() or not all((path / name).is_file() for name in required_file_names):
        raise DbctlConfigurationError("内置 Alembic 脚本目录不完整。")
    versions = path / "versions"
    if not versions.is_dir() or not any(versions.glob("*.py")):
        raise DbctlConfigurationError("内置 Alembic migration 版本目录为空。")


__all__ = ["DefaultTextResource", "alembic_script_location", "read_default_text"]
