"""@brief Backend 安装包资源定位 / Installed-package resource lookup for the backend."""

from __future__ import annotations

from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Final

_PACKAGE_NAME: Final[str] = "backend"
"""@brief importlib.resources 使用的包名 / Package name used by ``importlib.resources``."""

_RESOURCE_DIRECTORY: Final[str] = "resources"
"""@brief wheel 内资源目录 / Resource directory inside the wheel."""

_V1_CONTRACT_SCHEMA: Final[str] = "ai-job-workspace.contract.schema.json"
"""@brief 运行时权威 JSON Schema 文件 / Authoritative runtime JSON Schema file."""


def read_contract_schema_text(version: str = "v1") -> str:
    """@brief 读取 wheel 或已验证源码树中的 contract / Read the contract from the wheel or verified source tree.

    @return UTF-8 JSON Schema 文本 / UTF-8 JSON Schema text.
    @raise RuntimeError contract 缺失或不可读时抛出 / Raised when the contract is missing or unreadable.
    """

    if version not in {"v1", "v2"}:
        raise ValueError("contract version must be v1 or v2")
    packaged = _packaged_contract(version)
    if packaged.is_file():
        try:
            return packaged.read_text(encoding="utf-8")
        except OSError as error:
            raise RuntimeError("packaged contract schema cannot be read") from error
    source_path = _source_contract(version)
    if source_path is not None and source_path.is_file():
        try:
            return source_path.read_text(encoding="utf-8")
        except OSError as error:
            raise RuntimeError("source contract schema cannot be read") from error
    raise RuntimeError("contract schema resource is missing")


def _packaged_contract(version: str) -> Traversable:
    """@brief 返回尚未物化的包内 contract / Return the unmaterialized packaged contract.

    @return importlib Traversable 资源 / Importlib ``Traversable`` resource.
    """

    filename = _V1_CONTRACT_SCHEMA if version == "v1" else "api-v2.schema.jsonc"
    return files(_PACKAGE_NAME).joinpath(_RESOURCE_DIRECTORY, filename)


def _source_contract(version: str) -> Path | None:
    """@brief 在双标记验证的源码树定位 contract / Locate the contract in a two-marker verified source tree.

    @return 源码 contract 路径；安装态无源码树时为 None / Source contract path, or ``None`` outside a checkout.

    @note 不搜索当前工作目录或任意父目录，避免运行位置控制资源选择。
    / The current directory and arbitrary parents are never searched, preventing runtime location from selecting a resource.
    """

    repository_root = Path(__file__).resolve().parents[2]
    if not (repository_root / "pyproject.toml").is_file():
        return None
    if not (repository_root / "src" / "backend" / "__init__.py").is_file():
        return None
    if version == "v1":
        return repository_root / "workspace-shared-docs" / "contracts" / "v1" / _V1_CONTRACT_SCHEMA
    return repository_root / "workspace-shared-docs" / "contracts" / "v2" / "schema.jsonc"


__all__ = ["read_contract_schema_text"]
