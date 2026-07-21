"""@brief Docker 运行配置生成与进程入口 / Docker runtime-config rendering and process entrypoint."""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping, Sequence
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Final
from urllib.parse import quote

import json5

from dbctl.package_resources import read_default_text
from workspace_shared.jsonc import ConfigurationError, require_mapping

_DEFAULT_CONFIG_PATH: Final[Path] = Path("/tmp/aiws/config.jsonc")
"""@brief 生成配置的默认临时路径 / Default path for the generated runtime configuration."""

_DEFAULT_DBINIT_PATH: Final[Path] = Path("/tmp/aiws/dbinit.jsonc")
"""@brief 容器内 dbinit 资源的默认临时路径 / Default container path for the dbinit resource."""

_HOSTNAME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)(?:\.(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?))*$"
)
"""@brief 容器数据库 DNS hostname 白名单 / Allow-list pattern for container database hostnames."""

_DATABASE_NAME: Final[str] = "ai_job_workspace"
"""@brief dbinit 声明固定的数据库名称 / Database name fixed by the dbinit declaration."""


def build_runtime_config(environ: Mapping[str, str]) -> dict[str, Any]:
    """@brief 从非密钥模板与环境变量构建容器配置 / Build container config from the non-secret template and environment.

    @param environ 容器环境变量映射 / Container environment mapping.
    @return 可序列化且包含运行时 DSN 的配置 / Serializable config containing runtime DSNs.
    @raise ConfigurationError 必填值缺失、非法或数据库密码重复时抛出。
    / Raised when required values are absent, invalid, or database passwords are duplicated.
    """

    parsed = json5.loads(read_default_text("example.jsonc"))
    if not isinstance(parsed, Mapping):
        raise ConfigurationError("packaged example configuration root must be an object")
    root = dict(parsed)

    environment = _optional_text(environ, "AIWS_ENVIRONMENT") or "development"
    root["environment"] = environment

    database = require_mapping(root.get("database"), "database")
    app_password = _required_text(environ, "AIWS_DB_APP_PASSWORD")
    migrator_password = _required_text(environ, "AIWS_DB_MIGRATOR_PASSWORD")
    dashboard_password = _required_text(environ, "AIWS_DB_DASHBOARD_PASSWORD")
    if len({app_password, migrator_password, dashboard_password}) != 3:
        raise ConfigurationError("database role passwords must be distinct")
    database_host = _database_host(environ.get("AIWS_DB_HOST", "postgres"))
    database_port = _port(environ.get("AIWS_DB_PORT", "5432"), "AIWS_DB_PORT")
    database.update(
        {
            "mode": "postgresql",
            "application_dsn": _database_dsn(
                "workspace_app", app_password, database_host, database_port
            ),
            "migrator_dsn": _database_dsn(
                "workspace_migrator", migrator_password, database_host, database_port
            ),
            "dashboard_dsn": _database_dsn(
                "workspace_dashboard", dashboard_password, database_host, database_port
            ),
        }
    )
    root["database"] = database

    network = require_mapping(root.get("network"), "network")
    network.update(
        {
            "bind_host": "0.0.0.0",
            "bind_port": 8000,
            "public_base_url": environ.get(
                "AIWS_PUBLIC_BASE_URL", "http://127.0.0.1:8000"
            ),
            "cors_allowed_origins": _json_string_list(
                environ.get("AIWS_CORS_ALLOWED_ORIGINS", "[]"),
                "AIWS_CORS_ALLOWED_ORIGINS",
            ),
            "trusted_proxy_cidrs": _json_string_list(
                environ.get("AIWS_TRUSTED_PROXY_CIDRS", '["172.30.0.0/24"]'),
                "AIWS_TRUSTED_PROXY_CIDRS",
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
    ai.update(
        {
            "provider": environ.get("AIWS_AI_PROVIDER", "mock"),
            "model": environ.get("AIWS_AI_MODEL", "mock-deterministic-v1"),
            "base_url": _optional_text(environ, "AIWS_AI_BASE_URL"),
            "data_region": environ.get("AIWS_AI_DATA_REGION", "private_deployment"),
        }
    )
    root["ai"] = ai

    logging = require_mapping(root.get("logging"), "logging")
    logging["routes"] = [
        {"sink": "stdout", "levels": ["DEBUG", "INFO"]},
        {"sink": "stderr", "levels": ["WARNING", "ERROR", "CRITICAL"]},
    ]
    root["logging"] = logging

    identity_mode = _optional_text(environ, "AIWS_IDENTITY_MODE")
    if identity_mode is None:
        identity_mode = (
            "development_mock"
            if environment in {"development", "test"}
            else "trusted_proxy_hmac"
        )
    if identity_mode == "trusted_proxy_hmac":
        _required_text(environ, "AIWS_TRUSTED_PROXY_HMAC_SECRET")
    security = require_mapping(root.get("security"), "security")
    security["identity_mode"] = identity_mode
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


def write_runtime_files(
    config_path: Path,
    dbinit_path: Path,
    environ: Mapping[str, str],
) -> None:
    """@brief 原子写入权限受限配置与非密钥 dbinit / Atomically write private config and non-secret dbinit.

    @param config_path 运行时配置目标 / Runtime-config destination.
    @param dbinit_path dbinit 资源目标 / Dbinit-resource destination.
    @param environ 容器环境变量映射 / Container environment mapping.
    @return 无返回值 / No return value.
    """

    config_payload = json.dumps(
        build_runtime_config(environ),
        ensure_ascii=False,
        indent=2,
    ) + "\n"
    _atomic_write(config_path, config_payload, 0o600)
    _atomic_write(dbinit_path, read_default_text("dbinit.jsonc"), 0o644)


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 生成配置后以目标进程替换入口 / Render config and replace the entrypoint with the target process.

    @param argv 待执行命令；None 时读取 sys.argv / Command to execute; reads ``sys.argv`` when None.
    @return 仅参数或配置错误时返回；正常路径由 exec 替换 / Returns only for argument/config errors; exec replaces the normal path.
    """

    command = tuple(sys.argv[1:] if argv is None else argv)
    if not command:
        print("container entrypoint requires a command", file=sys.stderr)
        return 2
    config_path = Path(os.environ.get("AIWS_CONFIG", str(_DEFAULT_CONFIG_PATH)))
    dbinit_path = Path(os.environ.get("AIWS_DBINIT", str(_DEFAULT_DBINIT_PATH)))
    config_mode = os.environ.get("AIWS_CONFIG_MODE", "generated")
    try:
        if config_mode == "generated":
            write_runtime_files(config_path, dbinit_path, os.environ)
        elif config_mode == "mounted":
            if not config_path.is_file():
                raise ConfigurationError("mounted AIWS_CONFIG file does not exist")
            _atomic_write(dbinit_path, read_default_text("dbinit.jsonc"), 0o644)
        else:
            raise ConfigurationError("AIWS_CONFIG_MODE must be generated or mounted")
    except (ConfigurationError, OSError, ValueError):
        print("container entrypoint could not prepare runtime configuration", file=sys.stderr)
        return 2
    os.execvpe(command[0], command, os.environ)
    return 0


def _database_dsn(role: str, password: str, host: str, port: int) -> str:
    """@brief 构造百分号编码的内部 PostgreSQL DSN / Build a percent-encoded internal PostgreSQL DSN.

    @param role 固定数据库登录角色 / Fixed database login role.
    @param password 不记录的角色密码 / Role password that must never be logged.
    @param host 已验证数据库 hostname/IP / Validated database hostname or IP.
    @param port 已验证数据库端口 / Validated database port.
    @return PostgreSQL URI / PostgreSQL URI.
    """

    return (
        f"postgresql://{quote(role, safe='')}:{quote(password, safe='')}@"
        f"{host}:{port}/{_DATABASE_NAME}"
    )


def _database_host(value: str) -> str:
    """@brief 校验并格式化数据库 hostname/IP / Validate and format a database hostname or IP.

    @param value 候选 hostname/IP / Candidate hostname or IP.
    @return URI authority 可用的 hostname/IP / Hostname or IP suitable for a URI authority.
    @raise ConfigurationError hostname/IP 非法时抛出 / Raised for an invalid hostname or IP.
    """

    candidate = value.strip()
    try:
        address = ip_address(candidate.strip("[]"))
    except ValueError:
        if not _HOSTNAME_PATTERN.fullmatch(candidate):
            raise ConfigurationError("AIWS_DB_HOST is invalid") from None
        return candidate
    return f"[{address}]" if address.version == 6 else str(address)


def _port(value: str, name: str) -> int:
    """@brief 解析 TCP 端口 / Parse a TCP port.

    @param value 候选十进制端口 / Candidate decimal port.
    @param name 安全错误标签 / Safe error label.
    @return 1..65535 端口 / Port from 1 through 65535.
    @raise ConfigurationError 端口非法时抛出 / Raised for an invalid port.
    """

    try:
        port = int(value, 10)
    except ValueError:
        raise ConfigurationError(f"{name} must be an integer") from None
    if not 1 <= port <= 65535:
        raise ConfigurationError(f"{name} must be between 1 and 65535")
    return port


def _json_string_list(value: str, name: str) -> list[str]:
    """@brief 解析环境中的 JSON 字符串数组 / Parse a JSON string array from the environment.

    @param value JSON 文本 / JSON text.
    @param name 安全错误标签 / Safe error label.
    @return 非空字符串列表 / List of non-empty strings.
    @raise ConfigurationError JSON 形状非法时抛出 / Raised for an invalid JSON shape.
    """

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        raise ConfigurationError(f"{name} must be a JSON string array") from None
    if not isinstance(parsed, list) or not all(
        isinstance(item, str) and item for item in parsed
    ):
        raise ConfigurationError(f"{name} must be a JSON string array")
    return parsed


def _required_text(environ: Mapping[str, str], name: str) -> str:
    """@brief 读取必填且不回显的环境变量 / Read a required environment variable without echoing it.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名称 / Environment-variable name.
    @return 非空原值 / Non-empty original value.
    @raise ConfigurationError 变量缺失或为空时抛出 / Raised when absent or empty.
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
