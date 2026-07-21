"""@brief Docker 运行配置投影与进程入口 / Docker runtime-config projection and process entrypoint."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import json5

from workspace_shared.jsonc import ConfigurationError, require_mapping

_DEFAULT_SOURCE_CONFIG_PATH: Final[Path] = Path("/var/lib/aiws-config/config.jsonc")
"""@brief dbctl 持久配置默认路径 / Default path of the persistent dbctl-owned configuration."""

_DEFAULT_RUNTIME_CONFIG_PATH: Final[Path] = Path("/tmp/aiws/config.jsonc")
"""@brief 容器运行副本默认路径 / Default path of the container runtime projection."""


def build_runtime_config(
    source_config_path: Path,
    environ: Mapping[str, str],
) -> dict[str, Any]:
    """@brief 从 dbctl 配置投影容器运行设置 / Project container settings from the dbctl-owned config.

    @param source_config_path 已由 dbctl bootstrap 创建的持久配置。
    / Persistent configuration created by dbctl bootstrap.
    @param environ 容器非数据库状态的环境覆盖 / Environment overrides for non-database container state.
    @return 保留 dbctl DSN、适配容器边界的配置 / Config preserving dbctl DSNs and adapting container boundaries.
    @raise ConfigurationError 源配置缺失、无效或生产 secret 缺失时抛出。
    / Raised when the source config is absent or invalid, or a production secret is missing.
    """

    if not source_config_path.is_file():
        raise ConfigurationError("run dbctl bootstrap before starting container services")
    try:
        parsed = json5.loads(source_config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise ConfigurationError("dbctl-generated source configuration is invalid") from error
    if not isinstance(parsed, Mapping):
        raise ConfigurationError("dbctl-generated source configuration root must be an object")
    root = dict(parsed)

    environment = _optional_text(environ, "AIWS_ENVIRONMENT") or str(
        root.get("environment", "development")
    )
    root["environment"] = environment

    database = require_mapping(root.get("database"), "database")
    for field_name in ("application_dsn", "migrator_dsn", "dashboard_dsn"):
        value = database.get(field_name)
        if not isinstance(value, str) or not value:
            raise ConfigurationError(f"database.{field_name} must be created by dbctl bootstrap")
    database["mode"] = "postgresql"
    root["database"] = database

    network = require_mapping(root.get("network"), "network")
    network.update(
        {
            "bind_host": "0.0.0.0",
            "bind_port": 8000,
            "public_base_url": _optional_text(environ, "AIWS_PUBLIC_BASE_URL")
            or str(network.get("public_base_url", "http://127.0.0.1:8000")),
            "cors_allowed_origins": _optional_json_string_list(
                environ,
                "AIWS_CORS_ALLOWED_ORIGINS",
                network.get("cors_allowed_origins", []),
            ),
            "trusted_proxy_cidrs": _optional_json_string_list(
                environ,
                "AIWS_TRUSTED_PROXY_CIDRS",
                network.get("trusted_proxy_cidrs", ["172.30.0.0/24"]),
            ),
            "outbound_proxy_url": _optional_text(environ, "AIWS_OUTBOUND_PROXY_URL"),
        }
    )
    root["network"] = network

    knowledge = require_mapping(root.get("knowledge"), "knowledge")
    knowledge["blob_directory"] = "/var/lib/aiws/knowledge-blobs"
    root["knowledge"] = knowledge

    renderer = require_mapping(root.get("resume_rendering"), "resume_rendering")
    renderer["artifact_directory"] = "/var/lib/aiws/artifacts"
    root["resume_rendering"] = renderer

    ai = require_mapping(root.get("ai"), "ai")
    for environment_name, field_name in (
        ("AIWS_AI_PROVIDER", "provider"),
        ("AIWS_AI_MODEL", "model"),
        ("AIWS_AI_BASE_URL", "base_url"),
        ("AIWS_AI_DATA_REGION", "data_region"),
    ):
        value = _optional_text(environ, environment_name)
        if value is not None:
            ai[field_name] = value
    root["ai"] = ai

    logging = require_mapping(root.get("logging"), "logging")
    logging["routes"] = [
        {"sink": "stdout", "levels": ["DEBUG", "INFO"]},
        {"sink": "stderr", "levels": ["WARNING", "ERROR", "CRITICAL"]},
    ]
    root["logging"] = logging

    security = require_mapping(root.get("security"), "security")
    identity_mode = _optional_text(environ, "AIWS_IDENTITY_MODE")
    if identity_mode is not None:
        security["identity_mode"] = identity_mode
    if security.get("identity_mode") == "trusted_proxy_hmac":
        _required_text(environ, "AIWS_TRUSTED_PROXY_HMAC_SECRET")
    root["security"] = security

    dashboard = require_mapping(root.get("dashboard"), "dashboard")
    dashboard_api = require_mapping(dashboard.get("api"), "dashboard.api")
    dashboard_api.update({"host": "0.0.0.0", "port": 8010})
    dashboard["api"] = dashboard_api
    dashboard_access = require_mapping(dashboard.get("access"), "dashboard.access")
    dashboard_access["mode"] = "operator_token"
    dashboard["access"] = dashboard_access
    root["dashboard"] = dashboard
    return root


def write_runtime_config(
    source_config_path: Path,
    runtime_config_path: Path,
    environ: Mapping[str, str],
) -> None:
    """@brief 原子写入临时运行投影 / Atomically write the ephemeral runtime projection.

    @param source_config_path dbctl 持久配置 / Persistent dbctl-owned configuration.
    @param runtime_config_path 临时运行配置目标 / Ephemeral runtime-config destination.
    @param environ 非数据库环境覆盖 / Non-database environment overrides.
    @return 无返回值 / No return value.
    """

    payload = json.dumps(
        build_runtime_config(source_config_path, environ),
        ensure_ascii=False,
        indent=2,
    ) + "\n"
    _atomic_write(runtime_config_path, payload, 0o600)


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 投影配置后以目标进程替换入口 / Project config and replace the entrypoint with the target process.

    @param argv 待执行命令；None 时读取 sys.argv / Command to execute; reads ``sys.argv`` when None.
    @return 仅参数或配置错误时返回 / Returns only for argument or configuration errors.
    """

    command = tuple(sys.argv[1:] if argv is None else argv)
    if not command:
        print("container entrypoint requires a command", file=sys.stderr)
        return 2
    source_config_path = Path(
        os.environ.get("AIWS_SOURCE_CONFIG", str(_DEFAULT_SOURCE_CONFIG_PATH))
    )
    runtime_config_path = Path(os.environ.get("AIWS_CONFIG", str(_DEFAULT_RUNTIME_CONFIG_PATH)))
    try:
        write_runtime_config(source_config_path, runtime_config_path, os.environ)
    except (ConfigurationError, OSError, ValueError):
        print(
            "container entrypoint could not read the dbctl-generated configuration; "
            "run dbctl bootstrap first",
            file=sys.stderr,
        )
        return 2
    os.execvpe(command[0], command, os.environ)
    return 0


def _optional_json_string_list(
    environ: Mapping[str, str],
    name: str,
    default: object,
) -> list[str]:
    """@brief 读取可选 JSON 字符串数组 / Read an optional JSON string array.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名 / Environment-variable name.
    @param default 环境变量缺失时的配置值 / Config value used when the variable is absent.
    @return 非空字符串列表 / List of non-empty strings.
    """

    raw_value = environ.get(name)
    parsed = default
    if raw_value:
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            raise ConfigurationError(f"{name} must be a JSON string array") from None
    if not isinstance(parsed, list) or not all(
        isinstance(item, str) and item for item in parsed
    ):
        raise ConfigurationError(f"{name} must be a JSON string array")
    return parsed


def _required_text(environ: Mapping[str, str], name: str) -> str:
    """@brief 读取必填且不回显的环境变量 / Read a required variable without echoing it.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名称 / Environment-variable name.
    @return 非空原值 / Non-empty original value.
    """

    value = environ.get(name)
    if not value:
        raise ConfigurationError(f"required environment variable {name} is missing")
    return value


def _optional_text(environ: Mapping[str, str], name: str) -> str | None:
    """@brief 读取可选环境变量 / Read an optional environment variable.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名称 / Environment-variable name.
    @return 非空值或 None / Non-empty value or None.
    """

    value = environ.get(name)
    return value if value else None


def _atomic_write(path: Path, content: str, mode: int) -> None:
    """@brief 在同目录原子写入文件 / Atomically write a file in its destination directory.

    @param path 目标路径 / Destination path.
    @param content UTF-8 文本 / UTF-8 text.
    @param mode 最终 POSIX 权限 / Final POSIX permissions.
    @return 无返回值 / No return value.
    """

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            os.fchmod(temporary.fileno(), mode)
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
