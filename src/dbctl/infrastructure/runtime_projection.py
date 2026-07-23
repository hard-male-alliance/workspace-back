"""@brief Docker 运行配置投影基础设施 / Docker runtime-configuration projection infrastructure."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

import json5

from workspace_shared.jsonc import ConfigurationError, require_mapping

from .private_files import atomic_write_private_text

_PRIVATE_DIRECTORY_MODE: Final[int] = 0o700
"""@brief 运行投影父目录的创建权限 / Creation mode for the runtime projection directory."""

_API_V2_PRODUCTION_ORIGIN: Final[str] = "https://api.hmalliances.org:8022"
"""@brief API Standard V2 冻结的生产公开 Origin / Production public origin frozen by API Standard V2."""


def build_runtime_config(
    source_config_path: Path,
    environ: Mapping[str, str],
) -> dict[str, Any]:
    """@brief 从 dbctl 配置投影容器运行设置 / Project container settings from dbctl configuration.

    @param source_config_path 已由 dbctl bootstrap 创建的持久配置。
    / Persistent configuration created by dbctl bootstrap.
    @param environ 容器非数据库状态的环境覆盖 / Environment overrides for non-database state.
    @return 保留 dbctl DSN、适配容器边界的配置。
    / Configuration preserving dbctl DSNs while adapting container boundaries.
    @raise ConfigurationError 源配置缺失、无效或生产 secret 缺失时抛出。
    / Raised when the source configuration is missing or invalid, or a production secret is absent.
    """

    if not source_config_path.is_file():
        raise ConfigurationError("run dbctl bootstrap before starting container services")
    try:
        parsed = json5.loads(
            source_config_path.read_text(encoding="utf-8"),
            allow_duplicate_keys=False,
        )
    except (OSError, UnicodeError, ValueError) as error:
        raise ConfigurationError("dbctl-generated source configuration is invalid") from error
    if not isinstance(parsed, Mapping):
        raise ConfigurationError("dbctl-generated source configuration root must be an object")
    root = dict(parsed)

    environment = _optional_text(environ, "AIWS_ENVIRONMENT") or str(
        root.get("environment", "development")
    )
    root["environment"] = environment

    api = require_mapping(root.get("api"), "api")
    if environment not in {"development", "test"}:
        api["legacy_v1_enabled"] = False
    root["api"] = api

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
            "bind_port": 9000,
            "public_base_url": (
                _API_V2_PRODUCTION_ORIGIN
                if environment == "production"
                else _optional_text(environ, "AIWS_PUBLIC_BASE_URL")
                or str(network.get("public_base_url", "http://127.0.0.1:9000"))
            ),
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
    connections = require_mapping(
        knowledge.get("connections"),
        "knowledge.connections",
    )
    configured_providers = _optional_json_object_list(
        environ,
        "AIWS_KNOWLEDGE_CONNECTION_PROVIDERS",
        connections.get("providers", []),
    )
    connections["providers"] = configured_providers
    if environment not in {"development", "test"} or configured_providers:
        connections.update(
            {
                "provider_session_keyring": {
                    "active_key_id": _required_text(
                        environ,
                        "AIWS_KNOWLEDGE_PROVIDER_SESSION_ACTIVE_KEY_ID",
                    ),
                    "keys": _required_json_string_mapping(
                        environ,
                        "AIWS_KNOWLEDGE_PROVIDER_SESSION_KEYS",
                    ),
                },
                "credential_keyring": {
                    "active_key_id": _required_text(
                        environ,
                        "AIWS_KNOWLEDGE_CREDENTIAL_ACTIVE_KEY_ID",
                    ),
                    "keys": _required_json_string_mapping(
                        environ,
                        "AIWS_KNOWLEDGE_CREDENTIAL_KEYS",
                    ),
                },
                "credential_fingerprint_hmac_key": _required_text(
                    environ,
                    "AIWS_KNOWLEDGE_CREDENTIAL_FINGERPRINT_HMAC_KEY",
                ),
                "credential_reference_hmac_key": _required_text(
                    environ,
                    "AIWS_KNOWLEDGE_CREDENTIAL_REFERENCE_HMAC_KEY",
                ),
            }
        )
    knowledge["connections"] = connections

    uploads = require_mapping(knowledge.get("uploads"), "knowledge.uploads")
    storage = require_mapping(uploads.get("storage"), "knowledge.uploads.storage")
    if environment in {"development", "test"}:
        local = require_mapping(
            storage.get("local"),
            "knowledge.uploads.storage.local",
        )
        local.update(
            {
                "directory": "/var/lib/aiws/knowledge-blobs",
                "signing_hmac_key": _required_text(
                    environ,
                    "AIWS_KNOWLEDGE_LOCAL_UPLOAD_SIGNING_HMAC_KEY",
                ),
            }
        )
        storage.update({"mode": "local", "local": local, "s3": None})
    else:
        storage.update(
            {
                "mode": "s3",
                "local": None,
                "s3": {
                    "endpoint": _required_text(environ, "AIWS_KNOWLEDGE_S3_ENDPOINT"),
                    "region": _required_text(environ, "AIWS_KNOWLEDGE_S3_REGION"),
                    "bucket": _required_text(environ, "AIWS_KNOWLEDGE_S3_BUCKET"),
                    "access_key_id": _required_text(
                        environ,
                        "AIWS_KNOWLEDGE_S3_ACCESS_KEY_ID",
                    ),
                    "secret_access_key": _required_text(
                        environ,
                        "AIWS_KNOWLEDGE_S3_SECRET_ACCESS_KEY",
                    ),
                    "session_token": _optional_text(
                        environ,
                        "AIWS_KNOWLEDGE_S3_SESSION_TOKEN",
                    ),
                    "object_prefix": _optional_text(
                        environ,
                        "AIWS_KNOWLEDGE_S3_OBJECT_PREFIX",
                    )
                    or "aiws-uploads",
                    "connect_timeout_ms": 3_000,
                    "read_timeout_ms": 300_000,
                },
            }
        )
    uploads["storage"] = storage
    malware_mode = _optional_text(environ, "AIWS_KNOWLEDGE_MALWARE_MODE") or (
        "dev" if environment in {"development", "test"} else "reject"
    )
    malware: dict[str, Any] = {"mode": malware_mode, "clamav": None}
    if malware_mode == "clamav":
        malware["clamav"] = {
            "host": _required_text(environ, "AIWS_KNOWLEDGE_CLAMAV_HOST"),
            "port": _optional_integer(environ, "AIWS_KNOWLEDGE_CLAMAV_PORT", 3310),
            "connect_timeout_ms": 3_000,
            "read_timeout_ms": 300_000,
        }
    uploads["malware"] = malware
    knowledge["uploads"] = uploads

    source_network = require_mapping(
        knowledge.get("source_network"),
        "knowledge.source_network",
    )
    if environment not in {"development", "test"}:
        source_network["allowed_host_patterns"] = _required_json_string_list(
            environ,
            "AIWS_KNOWLEDGE_SOURCE_ALLOWED_HOST_PATTERNS",
        )
    else:
        source_network["allowed_host_patterns"] = _optional_json_string_list(
            environ,
            "AIWS_KNOWLEDGE_SOURCE_ALLOWED_HOST_PATTERNS",
            source_network.get("allowed_host_patterns", []),
        )
    knowledge["source_network"] = source_network
    root["knowledge"] = knowledge

    interview = require_mapping(root.get("interview"), "interview")
    interview_realtime = require_mapping(
        interview.get("realtime"),
        "interview.realtime",
    )
    if environment not in {"development", "test"}:
        interview_realtime["signing_keyring"] = {
            "active_key_id": _required_text(
                environ,
                "AIWS_INTERVIEW_REALTIME_ACTIVE_KEY_ID",
            ),
            "keys": _required_json_string_mapping(
                environ,
                "AIWS_INTERVIEW_REALTIME_SIGNING_KEYS",
            ),
        }
        interview_realtime["signaling_url"] = _required_text(
            environ,
            "AIWS_INTERVIEW_SIGNALING_URL",
        )
    else:
        source_signing_keyring = require_mapping(
            interview_realtime.get("signing_keyring"),
            "interview.realtime.signing_keyring",
        )
        configured_active_key_id = source_signing_keyring.get("active_key_id")
        active_key_id = _optional_text(
            environ,
            "AIWS_INTERVIEW_REALTIME_ACTIVE_KEY_ID",
        ) or (
            configured_active_key_id
            if isinstance(configured_active_key_id, str)
            else None
        )
        signing_keys = _optional_json_string_mapping(
            environ,
            "AIWS_INTERVIEW_REALTIME_SIGNING_KEYS",
            source_signing_keyring.get("keys", {}),
        )
        if active_key_id is not None or signing_keys:
            interview_realtime["signing_keyring"] = {
                "active_key_id": active_key_id,
                "keys": signing_keys,
            }
        signaling_url = _optional_text(environ, "AIWS_INTERVIEW_SIGNALING_URL")
        if signaling_url is not None:
            interview_realtime["signaling_url"] = signaling_url
    interview_realtime["ice_urls"] = _optional_json_string_list(
        environ,
        "AIWS_INTERVIEW_ICE_URLS",
        interview_realtime.get("ice_urls", []),
    )
    interview["realtime"] = interview_realtime
    root["interview"] = interview

    renderer = require_mapping(root.get("resume_rendering"), "resume_rendering")
    renderer["artifact_directory"] = "/var/lib/aiws/artifacts"
    renderer_adapter = (
        _required_text(environ, "AIWS_RESUME_RENDERER_ADAPTER")
        if environment not in {"development", "test"}
        else _optional_text(environ, "AIWS_RESUME_RENDERER_ADAPTER")
    )
    if renderer_adapter is not None:
        renderer["adapter"] = renderer_adapter
    xelatex_command = _optional_text(environ, "AIWS_RESUME_XELATEX_COMMAND")
    if xelatex_command is not None:
        renderer["xelatex_command"] = xelatex_command
    root["resume_rendering"] = renderer

    ai = require_mapping(root.get("ai"), "ai")
    ai_fields = (
        ("AIWS_AI_PROVIDER", "provider"),
        ("AIWS_AI_MODEL", "model"),
        ("AIWS_AI_BASE_URL", "base_url"),
        ("AIWS_AI_DATA_REGION", "data_region"),
        ("AIWS_AI_EMBEDDING_PROVIDER", "embedding_provider"),
        ("AIWS_AI_EMBEDDING_MODEL", "embedding_model"),
        ("AIWS_AI_EMBEDDING_MODEL_REVISION", "embedding_model_revision"),
    )
    for environment_name, field_name in ai_fields:
        value = (
            _required_text(environ, environment_name)
            if environment not in {"development", "test"}
            else _optional_text(environ, environment_name)
        )
        if value is not None:
            ai[field_name] = value
    if environment not in {"development", "test"}:
        ai["embedding_dimension"] = _required_positive_integer(
            environ, "AIWS_AI_EMBEDDING_DIMENSION"
        )
    else:
        ai["embedding_dimension"] = _optional_integer(
            environ,
            "AIWS_AI_EMBEDDING_DIMENSION",
            int(ai.get("embedding_dimension", 0)),
        )
    api_key = (
        _required_text(environ, "AIWS_LLM_API_KEY")
        if environment not in {"development", "test"}
        else _optional_text(environ, "AIWS_LLM_API_KEY")
    )
    if api_key is not None:
        ai["api_key"] = api_key
    root["ai"] = ai

    logging = require_mapping(root.get("logging"), "logging")
    logging["routes"] = [
        {"sink": "stdout", "levels": ["DEBUG", "INFO"]},
        {"sink": "stderr", "levels": ["WARNING", "ERROR", "CRITICAL"]},
    ]
    root["logging"] = logging

    security = require_mapping(root.get("security"), "security")
    identity_mode = _optional_text(environ, "AIWS_IDENTITY_MODE")
    if environment not in {"development", "test"}:
        if identity_mode not in {None, "disabled"}:
            raise ConfigurationError(
                "AIWS_IDENTITY_MODE must be disabled for the API V2 deployment"
            )
        if _optional_text(environ, "AIWS_TRUSTED_PROXY_HMAC_SECRET") is not None:
            raise ConfigurationError(
                "AIWS_TRUSTED_PROXY_HMAC_SECRET is not consumed by the API V2 deployment"
            )
        security["identity_mode"] = "disabled"
        security["trusted_proxy_hmac_secret"] = None
        security["cursor_hmac_secret"] = _required_text(
            environ, "AIWS_CURSOR_HMAC_SECRET"
        )
        security["sensitive_idempotency_hmac_secret"] = _required_text(
            environ, "AIWS_SENSITIVE_IDEMPOTENCY_HMAC_SECRET"
        )
    else:
        if identity_mode is not None:
            security["identity_mode"] = identity_mode
        if security.get("identity_mode") == "trusted_proxy_hmac":
            security["trusted_proxy_hmac_secret"] = _required_text(
                environ, "AIWS_TRUSTED_PROXY_HMAC_SECRET"
            )
    root["security"] = security

    hosted_identity = require_mapping(root.get("hosted_identity"), "hosted_identity")
    password_breach = require_mapping(
        hosted_identity.get("password_breach"),
        "hosted_identity.password_breach",
    )
    identity_email = require_mapping(hosted_identity.get("email"), "hosted_identity.email")
    email_outbox = require_mapping(
        identity_email.get("outbox"),
        "hosted_identity.email.outbox",
    )
    if environment not in {"development", "test"}:
        password_breach["mode"] = "pwned_passwords"
        identity_email.update(
            {
                "mode": "smtp",
                "from_address": _required_text(
                    environ,
                    "AIWS_IDENTITY_EMAIL_FROM_ADDRESS",
                ),
                "smtp_host": _required_text(
                    environ,
                    "AIWS_IDENTITY_EMAIL_SMTP_HOST",
                ),
            }
        )
        smtp_username = _optional_text(environ, "AIWS_IDENTITY_EMAIL_SMTP_USERNAME")
        smtp_password = _optional_text(environ, "AIWS_IDENTITY_EMAIL_SMTP_PASSWORD")
        if smtp_username is not None or smtp_password is not None:
            identity_email["smtp_username"] = smtp_username
            identity_email["smtp_password"] = smtp_password
        email_outbox.update(
            {
                "active_key_id": _required_text(
                    environ,
                    "AIWS_IDENTITY_EMAIL_ACTIVE_KEY_ID",
                ),
                "encryption_keys": _required_json_string_mapping(
                    environ,
                    "AIWS_IDENTITY_EMAIL_ENCRYPTION_KEYS",
                ),
                "rate_limit_hmac_key": _required_text(
                    environ,
                    "AIWS_IDENTITY_EMAIL_RATE_LIMIT_HMAC_KEY",
                ),
            }
        )
    identity_email["outbox"] = email_outbox
    hosted_identity["password_breach"] = password_breach
    hosted_identity["email"] = identity_email
    root["hosted_identity"] = hosted_identity

    dashboard = require_mapping(root.get("dashboard"), "dashboard")
    dashboard_api = require_mapping(dashboard.get("api"), "dashboard.api")
    dashboard_api.update({"host": "0.0.0.0", "port": 8010})
    dashboard["api"] = dashboard_api
    dashboard_access = require_mapping(dashboard.get("access"), "dashboard.access")
    dashboard_access["mode"] = "operator_token"
    dashboard_token = _optional_text(environ, "AIWS_DASHBOARD_OPERATOR_TOKEN")
    if dashboard_token is not None:
        dashboard_access["token"] = dashboard_token
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
    @param runtime_config_path 临时运行配置目标 / Ephemeral runtime-configuration destination.
    @param environ 非数据库环境覆盖 / Non-database environment overrides.
    @return 无返回值 / No return value.
    """

    payload = (
        json.dumps(
            build_runtime_config(source_config_path, environ),
            ensure_ascii=False,
            indent=2,
        )
        + "\n"
    )
    runtime_config_path.parent.mkdir(
        parents=True,
        exist_ok=True,
        mode=_PRIVATE_DIRECTORY_MODE,
    )
    atomic_write_private_text(runtime_config_path, payload)


def _optional_json_string_list(
    environ: Mapping[str, str],
    name: str,
    default: object,
) -> list[str]:
    """@brief 读取可选 JSON 字符串数组 / Read an optional JSON string array.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名 / Environment-variable name.
    @param default 环境变量缺失时的配置值 / Configuration value used when the variable is absent.
    @return 非空字符串列表 / List of non-empty strings.
    @raise ConfigurationError 变量或默认值不是字符串数组时抛出。
    / Raised when the variable or default value is not an array of strings.
    """

    raw_value = environ.get(name)
    parsed = default
    if raw_value:
        try:
            parsed = json.loads(raw_value, object_pairs_hook=_unique_json_object)
        except (json.JSONDecodeError, ValueError):
            raise ConfigurationError(f"{name} must be a JSON string array") from None
    if not isinstance(parsed, list) or not all(isinstance(item, str) and item for item in parsed):
        raise ConfigurationError(f"{name} must be a JSON string array")
    return parsed


def _required_json_string_list(
    environ: Mapping[str, str],
    name: str,
) -> list[str]:
    """@brief 读取必填 JSON 字符串数组 / Read a required JSON string array.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名 / Environment-variable name.
    @return 非空字符串列表 / Non-empty string list.
    @raise ConfigurationError 变量缺失或 shape 非法 / Missing variable or invalid shape.
    """

    raw_value = environ.get(name)
    if not raw_value:
        raise ConfigurationError(f"required environment variable {name} is missing")
    values = _optional_json_string_list(environ, name, [])
    if not values:
        raise ConfigurationError(f"{name} must be a non-empty JSON string array")
    return values


def _optional_json_object_list(
    environ: Mapping[str, str],
    name: str,
    default: object,
) -> list[dict[str, Any]]:
    """@brief 读取可选 JSON object 数组 / Read an optional JSON object array.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名 / Environment-variable name.
    @param default 环境变量缺失时的配置值 / Default configuration value.
    @return object 的新列表 / New list of objects.
    @raise ConfigurationError JSON 或 shape 非法 / Invalid JSON or shape.

    @note 这里只投影 shape；provider 的完整 allowlist/endpoint/scope 语义由 backend
        配置服务统一验证 / This projects shape only; the backend configuration service owns
        provider allowlist, endpoint, and scope semantics.
    """

    raw_value = environ.get(name)
    parsed = default
    if raw_value:
        try:
            parsed = json.loads(raw_value, object_pairs_hook=_unique_json_object)
        except (json.JSONDecodeError, ValueError):
            raise ConfigurationError(f"{name} must be a JSON object array") from None
    if not isinstance(parsed, list) or not all(isinstance(item, dict) for item in parsed):
        raise ConfigurationError(f"{name} must be a JSON object array")
    return [dict(item) for item in parsed]


def _optional_integer(environ: Mapping[str, str], name: str, default: int) -> int:
    """@brief 读取不回显的可选十进制整数 / Read an optional decimal integer without echoing it.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名 / Environment-variable name.
    @param default 缺失时默认值 / Default used when absent.
    @return 解析后的整数 / Parsed integer.
    @raise ConfigurationError 值不是十进制整数 / Value is not a decimal integer.
    """

    value = environ.get(name)
    if not value:
        return default
    try:
        return int(value, 10)
    except ValueError:
        raise ConfigurationError(f"{name} must be a decimal integer") from None


def _required_positive_integer(environ: Mapping[str, str], name: str) -> int:
    """@brief 读取必填正十进制整数 / Read a required positive decimal integer.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名 / Environment-variable name.
    @return 正整数 / Positive integer.
    @raise ConfigurationError 变量缺失、非整数或非正时抛出 / Raised when missing, non-integral, or non-positive.
    """

    value = _required_text(environ, name)
    try:
        parsed = int(value, 10)
    except ValueError:
        raise ConfigurationError(f"{name} must be a positive decimal integer") from None
    if parsed < 1:
        raise ConfigurationError(f"{name} must be a positive decimal integer")
    return parsed


def _required_text(environ: Mapping[str, str], name: str) -> str:
    """@brief 读取必填且不回显的环境变量 / Read a required variable without echoing it.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名称 / Environment-variable name.
    @return 非空原值 / Non-empty original value.
    @raise ConfigurationError 变量缺失或为空时抛出 / Raised when the variable is missing or empty.
    """

    value = environ.get(name)
    if not value:
        raise ConfigurationError(f"required environment variable {name} is missing")
    return value


def _required_json_string_mapping(
    environ: Mapping[str, str],
    name: str,
) -> dict[str, str]:
    """@brief 读取必填 JSON 字符串映射 / Read a required JSON string mapping.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名 / Environment-variable name.
    @return 至少一个非空 key/value 的新映射 / New mapping with at least one non-empty pair.
    @raise ConfigurationError 缺失、JSON 非法或 shape 非法 / Missing, malformed, or invalid shape.

    @note 错误绝不回显 secret value / Errors never echo secret values.
    """

    raw_value = environ.get(name)
    if not raw_value:
        raise ConfigurationError(f"required environment variable {name} is missing")
    parsed = _optional_json_string_mapping(environ, name, {})
    if not parsed:
        raise ConfigurationError(f"{name} must be a JSON string mapping")
    return parsed


def _optional_json_string_mapping(
    environ: Mapping[str, str],
    name: str,
    default: object,
) -> dict[str, str]:
    """@brief 读取可选 JSON 字符串映射 / Read an optional JSON string mapping.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名 / Environment-variable name.
    @param default 变量缺失时的配置值 / Configuration value used when absent.
    @return 可为空的非空 key/value 映射 / Possibly empty mapping of non-empty strings.
    @raise ConfigurationError JSON 或 shape 非法 / Invalid JSON or shape.

    @note 错误绝不回显 secret value / Errors never echo secret values.
    """

    raw_value = environ.get(name)
    parsed = default
    if raw_value:
        try:
            parsed = json.loads(raw_value, object_pairs_hook=_unique_json_object)
        except (json.JSONDecodeError, ValueError):
            raise ConfigurationError(f"{name} must be a JSON string mapping") from None
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str)
        and bool(key)
        and isinstance(value, str)
        and bool(value)
        for key, value in parsed.items()
    ):
        raise ConfigurationError(f"{name} must be a JSON string mapping")
    return dict(parsed)


def _optional_text(environ: Mapping[str, str], name: str) -> str | None:
    """@brief 读取可选环境变量 / Read an optional environment variable.

    @param environ 环境变量映射 / Environment mapping.
    @param name 环境变量名称 / Environment-variable name.
    @return 非空值或 ``None`` / Non-empty value or ``None``.
    """

    value = environ.get(name)
    return value if value else None


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@brief 构造无重复字段的环境 JSON object / Build a duplicate-free environment JSON object.

    @param pairs JSON decoder 保序 pair / Ordered pairs from the JSON decoder.
    @return 唯一字段 mapping / Unique-key mapping.
    @raise ValueError 任意字段重复 / Any field is duplicated.
    """

    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result
