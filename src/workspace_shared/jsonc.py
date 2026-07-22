"""@brief JSONC 配置读取工具 / JSONC configuration loading utilities."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import json5


class ConfigurationError(ValueError):
    """@brief 配置无效异常 / Invalid configuration exception.

    @note 不在异常文本中包含 secret，避免日志泄漏。
    """


def load_jsonc(path: Path) -> dict[str, Any]:
    """@brief 读取 JSONC 配置 / Load a JSONC configuration file.

    @param path 配置文件路径 / Configuration file path.
    @return 顶层对象 / Top-level object.
    @raise ConfigurationError 文件不存在、无法解析或根不是对象时抛出 / Raised for missing, malformed, or non-object input.
    """
    if not path.is_file():
        raise ConfigurationError(f"configuration file does not exist: {path}")
    try:
        value = json5.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ConfigurationError(f"could not parse configuration: {path}") from error
    if not isinstance(value, Mapping):
        raise ConfigurationError("configuration root must be an object")
    return dict(value)


def require_mapping(value: object, name: str) -> dict[str, Any]:
    """@brief 校验对象配置节 / Validate an object configuration section.

    @param value 候选配置值 / Candidate configuration value.
    @param name 人类可读配置节名称 / Human-readable section name.
    @return 可变字典副本 / Mutable dictionary copy.
    @raise ConfigurationError 值不是对象时抛出 / Raised when the value is not an object.
    """
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"configuration section {name!r} must be an object")
    return dict(value)


def redact_secret(value: str | None) -> str | None:
    """@brief 脱敏 secret / Redact a secret for diagnostics.

    @param value 原始 secret / Raw secret.
    @return 安全的展示值 / Safe display value.
    """
    if value is None:
        return None
    return "<redacted>"
