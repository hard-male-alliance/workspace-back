"""@brief dbctl 独立配置服务 / Independent configuration service for dbctl."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

import json5

from .errors import DbctlConfigurationError
from .identifiers import validate_postgres_identifier

_ENVIRONMENT_VARIABLE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LOCAL_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
_RESERVED_DATABASE_NAMES: Final[frozenset[str]] = frozenset({"postgres", "template0", "template1"})
_RESERVED_ROLE_NAMES: Final[frozenset[str]] = frozenset(
    {"postgres", "public", "current_user", "current_role", "session_user"}
)
_DEFAULT_BOOTSTRAP_SCHEMAS: Final[tuple[str, ...]] = (
    "identity",
    "resume",
    "agent",
    "interview",
    "knowledge",
    "observability",
)


class DatabaseRole(StrEnum):
    """@brief dbctl 管理的角色类别 / Role categories managed by dbctl."""

    OWNER = "owner"
    MIGRATOR = "migrator"
    APP = "app"
    DASHBOARD = "dashboard"


@dataclass(frozen=True, slots=True)
class ObservabilityRetentionSettings:
    """@brief dbctl 遥测保留设置 / Telemetry-retention settings owned by dbctl.

    @param retention_days 遥测记录保留天数；``0`` 表示显式禁用清理。
    / Number of days to retain telemetry records; ``0`` explicitly disables pruning.

    @note 该对象只描述保留边界，不包含数据库凭证、批量大小或执行开关。后两者由
    ``workspace-dbctl prune-telemetry`` 的受限运维参数提供，避免把一次性维护强度
    偷偷藏入应用请求路径。
    / This object describes only the retention boundary. Database credentials, batch size, and
    execution switches are supplied by constrained ``workspace-dbctl prune-telemetry`` operator
    arguments, keeping one-off maintenance intensity out of the application request path.
    """

    retention_days: int

    def __post_init__(self) -> None:
        """@brief 校验保留天数 / Validate telemetry retention days.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 值不是非负整数（或错误地传入布尔值）时抛出。
        / Raised when the value is not a non-negative integer (including a boolean).
        """
        if (
            not isinstance(self.retention_days, int)
            or isinstance(self.retention_days, bool)
            or self.retention_days < 0
        ):
            raise DbctlConfigurationError("observability.retention_days 必须是非负整数。")


@dataclass(frozen=True, slots=True)
class DatabaseConnectionSettings:
    """@brief PostgreSQL DSN 环境变量设置 / PostgreSQL DSN environment-variable settings.

    @param mode 运行时数据库模式；dbctl 不会据此决定是否执行 bootstrap。
    / Runtime database mode; dbctl does not use it to decide whether bootstrap may run.
    @param application_dsn_env 后端应用 DSN 的环境变量名 / Environment variable for application DSN.
    @param migrator_dsn_env Alembic 迁移 DSN 的环境变量名 / Environment variable for migrator DSN.
    @param admin_dsn_env 管理员 DSN 的环境变量名 / Environment variable for administrator DSN.
    @param dashboard_dsn_env Dashboard 只读 DSN 的环境变量名；未配置时 shell 不提供该身份。
    / Environment variable for Dashboard read-only DSN; no shell is offered for it when absent.
    """

    mode: str
    application_dsn_env: str
    migrator_dsn_env: str
    admin_dsn_env: str
    dashboard_dsn_env: str = "AIWS_DASHBOARD_DATABASE_DSN"

    def __post_init__(self) -> None:
        """@brief 校验 DSN 环境变量名 / Validate DSN environment-variable names.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 字段为空或不是合法环境变量名时抛出。
        / Raised when a field is empty or not a valid environment-variable name.
        """
        if not isinstance(self.mode, str) or not self.mode.strip():
            raise DbctlConfigurationError("database.mode 必须是非空字符串。")
        object.__setattr__(self, "mode", self.mode.strip())
        for field_name in (
            "application_dsn_env",
            "migrator_dsn_env",
            "admin_dsn_env",
            "dashboard_dsn_env",
        ):
            value = getattr(self, field_name)
            _validate_environment_variable_name(value, field_name)

    def dsn_environment_for(self, role: DatabaseRole) -> str:
        """@brief 获取 shell 身份对应的 DSN 环境变量 / Get the DSN environment variable for a shell role.

        @param role 请求 ``psql`` shell 的角色类别 / Requested ``psql`` shell role.
        @return 对应 DSN 环境变量名称 / Corresponding DSN environment-variable name.
        @raise DbctlConfigurationError 请求不可登录的 owner 身份时抛出。
        / Raised when the non-login owner identity is requested.
        """
        if role is DatabaseRole.APP:
            return self.application_dsn_env
        if role is DatabaseRole.MIGRATOR:
            return self.migrator_dsn_env
        if role is DatabaseRole.DASHBOARD:
            return self.dashboard_dsn_env
        raise DbctlConfigurationError("workspace_owner 是 NOLOGIN 角色，不能启动 psql shell。")


@dataclass(frozen=True, slots=True)
class DatabaseAdministrationSettings:
    """@brief PostgreSQL bootstrap 的非敏感管理设置 / Non-secret administration settings for bootstrap.

    @param database_name 要创建或管理的项目数据库 / Project database to create or manage.
    @param owner_role 不可登录的对象所有者角色 / Non-login object-owner role.
    @param migrator_role 可 ``SET ROLE`` 为 owner 的迁移角色 / Migrator role that may ``SET ROLE`` to owner.
    @param app_role 后端运行时 DML 角色 / Backend runtime DML role.
    @param dashboard_role observability 只读角色 / Observability read-only role.
    @param schemas 由 bootstrap 拥有和授权的 schema 列表 / Schemas owned and granted by bootstrap.
    @param observability_schema Dashboard 可读取的 observability schema。
    / Observability schema that Dashboard may read.
    @param local_postgres_user 仅 ``--local-postgres`` 模式传给 ``sudo -u`` 的本机账户。
    / Local account passed to ``sudo -u`` only in ``--local-postgres`` mode.
    @param maintenance_database 管理员连接用于创建目标数据库的 maintenance database。
    / Maintenance database used by the administrator connection to create the target database.
    @param password_env_by_role 可选角色密码环境变量映射；配置中只允许变量名，不允许密码值。
    / Optional role-password environment mapping; configuration permits variable names, never password values.
    """

    database_name: str
    owner_role: str
    migrator_role: str
    app_role: str
    dashboard_role: str
    schemas: tuple[str, ...] = _DEFAULT_BOOTSTRAP_SCHEMAS
    observability_schema: str = "observability"
    local_postgres_user: str = "postgres"
    maintenance_database: str = "postgres"
    password_env_by_role: Mapping[DatabaseRole, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """@brief 校验权限边界和管理名称 / Validate privilege boundaries and administration names.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 管理名称冲突、使用保留名称或密码配置不安全时抛出。
        / Raised when administration names collide, reserved names are used, or password configuration is unsafe.
        """
        database_name = validate_postgres_identifier(self.database_name, kind="数据库名")
        maintenance_database = validate_postgres_identifier(
            self.maintenance_database, kind="maintenance 数据库名"
        )
        if database_name.casefold() in _RESERVED_DATABASE_NAMES:
            raise DbctlConfigurationError("目标数据库名不能是 PostgreSQL 系统数据库。")
        if database_name == maintenance_database:
            raise DbctlConfigurationError("目标数据库与 maintenance 数据库不能相同。")

        role_values = {
            DatabaseRole.OWNER: validate_postgres_identifier(self.owner_role, kind="owner role"),
            DatabaseRole.MIGRATOR: validate_postgres_identifier(
                self.migrator_role, kind="migrator role"
            ),
            DatabaseRole.APP: validate_postgres_identifier(self.app_role, kind="app role"),
            DatabaseRole.DASHBOARD: validate_postgres_identifier(
                self.dashboard_role, kind="dashboard role"
            ),
        }
        if len(set(role_values.values())) != len(role_values):
            raise DbctlConfigurationError("owner、migrator、app 和 dashboard role 必须互不相同。")
        for role_name in role_values.values():
            normalized = role_name.casefold()
            if normalized in _RESERVED_ROLE_NAMES or normalized.startswith("pg_"):
                raise DbctlConfigurationError("bootstrap role 不能使用 PostgreSQL 系统或伪角色名称。")

        if not isinstance(self.schemas, tuple) or not self.schemas:
            raise DbctlConfigurationError("database_administration.schemas 必须是非空列表。")
        schemas = tuple(validate_postgres_identifier(value, kind="schema 名") for value in self.schemas)
        if len(set(schemas)) != len(schemas):
            raise DbctlConfigurationError("database_administration.schemas 不能包含重复 schema。")
        for schema in schemas:
            normalized = schema.casefold()
            if normalized == "information_schema" or normalized.startswith("pg_"):
                raise DbctlConfigurationError("bootstrap schema 不能使用 PostgreSQL 系统 schema 名称。")

        observability_schema = validate_postgres_identifier(
            self.observability_schema, kind="observability schema 名"
        )
        if observability_schema not in schemas:
            raise DbctlConfigurationError("observability schema 必须包含在 schemas 中。")

        if not isinstance(self.local_postgres_user, str) or not _LOCAL_ACCOUNT_PATTERN.fullmatch(
            self.local_postgres_user
        ):
            raise DbctlConfigurationError("local_postgres_user 必须是安全的本机 Unix 账户名。")

        normalized_password_envs: dict[DatabaseRole, str] = {}
        for raw_role, variable_name in self.password_env_by_role.items():
            try:
                role = DatabaseRole(raw_role)
            except ValueError as error:
                raise DbctlConfigurationError("password_env_by_role 包含未知角色。") from error
            if role is DatabaseRole.OWNER:
                raise DbctlConfigurationError("NOLOGIN owner role 不能配置密码。")
            _validate_environment_variable_name(variable_name, "角色密码环境变量")
            normalized_password_envs[role] = variable_name

        object.__setattr__(self, "database_name", database_name)
        object.__setattr__(self, "maintenance_database", maintenance_database)
        object.__setattr__(self, "owner_role", role_values[DatabaseRole.OWNER])
        object.__setattr__(self, "migrator_role", role_values[DatabaseRole.MIGRATOR])
        object.__setattr__(self, "app_role", role_values[DatabaseRole.APP])
        object.__setattr__(self, "dashboard_role", role_values[DatabaseRole.DASHBOARD])
        object.__setattr__(self, "schemas", schemas)
        object.__setattr__(self, "observability_schema", observability_schema)
        object.__setattr__(self, "password_env_by_role", MappingProxyType(normalized_password_envs))

    def role_name(self, role: DatabaseRole) -> str:
        """@brief 获取角色类别对应的 PostgreSQL 名称 / Get the PostgreSQL name for a role category.

        @param role dbctl 管理的角色类别 / Role category managed by dbctl.
        @return 已校验的 PostgreSQL role 名称 / Validated PostgreSQL role name.
        """
        names = {
            DatabaseRole.OWNER: self.owner_role,
            DatabaseRole.MIGRATOR: self.migrator_role,
            DatabaseRole.APP: self.app_role,
            DatabaseRole.DASHBOARD: self.dashboard_role,
        }
        return names[role]


@dataclass(frozen=True, slots=True)
class DbctlSettings:
    """@brief dbctl composition root 所需的完整配置 / Complete configuration required by dbctl composition root.

    @param database 非敏感的 DSN 环境变量设置 / Non-secret DSN environment-variable settings.
    @param administration 非敏感的 role、database、schema 管理设置。
    / Non-secret role, database, and schema administration settings.
    @param observability 仅供受控运维清理读取的遥测保留设置。
    / Telemetry-retention settings read only by controlled operator pruning.
    """

    database: DatabaseConnectionSettings
    administration: DatabaseAdministrationSettings
    observability: ObservabilityRetentionSettings

    def require_admin_dsn(self, environ: Mapping[str, str]) -> str:
        """@brief 从环境读取管理员 DSN / Read the administrator DSN from the environment.

        @param environ 环境变量映射 / Environment-variable mapping.
        @return 非空管理员 DSN / Non-empty administrator DSN.
        @raise DbctlConfigurationError 管理员 DSN 未设置时抛出；错误不回显 DSN。
        / Raised when the administrator DSN is absent; the error never echoes the DSN.
        """
        return _require_secret_environment_value(environ, self.database.admin_dsn_env)

    def require_migrator_dsn(self, environ: Mapping[str, str]) -> str:
        """@brief 从环境读取迁移 DSN / Read the migrator DSN from the environment.

        @param environ 环境变量映射 / Environment-variable mapping.
        @return 非空 migrator DSN / Non-empty migrator DSN.
        @raise DbctlConfigurationError 迁移 DSN 未设置时抛出，且不回显 DSN。
        / Raised when migrator DSN is absent without echoing it.
        """
        return _require_secret_environment_value(environ, self.database.migrator_dsn_env)

    def require_shell_dsn(self, role: DatabaseRole, environ: Mapping[str, str]) -> str:
        """@brief 从环境读取 shell 身份对应的 DSN / Read the DSN for a shell identity.

        @param role 请求 shell 的可登录角色 / Login role requested for shell.
        @param environ 环境变量映射 / Environment-variable mapping.
        @return 非空的角色 DSN / Non-empty role DSN.
        @raise DbctlConfigurationError DSN 缺失或请求 owner shell 时抛出。
        / Raised when the DSN is missing or an owner shell is requested.
        """
        return _require_secret_environment_value(environ, self.database.dsn_environment_for(role))


class DbctlConfigurationService:
    """@brief 读取并验证根 JSONC 的 dbctl 配置 / Load and validate dbctl configuration from root JSONC.

    dbctl 不导入 backend 或 dashboard 的配置对象。它只读取同一事实来源（source of
    truth）中的 ``database``、``database_administration`` 与
    ``observability.retention_days``，并在此边界完成自己的类型与安全校验。
    / dbctl does not import backend or dashboard configuration objects. It reads only
    ``database``, ``database_administration``, and ``observability.retention_days`` from the
    shared source of truth and performs its own type and safety validation at this boundary.
    """

    def __init__(self, config_path: Path | str = Path("config.jsonc")) -> None:
        """@brief 初始化配置服务 / Initialize the configuration service.

        @param config_path 根 JSONC 配置文件路径 / Root JSONC configuration file path.
        """
        self._config_path = Path(config_path)

    @property
    def config_path(self) -> Path:
        """@brief 返回配置路径 / Return the configuration path.

        @return 未解析为绝对路径的调用方配置路径 / Caller-configured path without forced absolute resolution.
        """
        return self._config_path

    def load(self) -> DbctlSettings:
        """@brief 加载 dbctl 设置 / Load dbctl settings.

        @return 已完整验证的 DbctlSettings / Fully validated DbctlSettings.
        @raise DbctlConfigurationError 文件缺失、JSONC 无效或所需配置节不合规时抛出。
        / Raised for a missing file, invalid JSONC, or malformed required configuration sections.
        """
        root = self._load_root_mapping()
        database = _require_mapping(root.get("database"), "database")
        administration = _require_mapping(
            root.get("database_administration"), "database_administration"
        )
        observability = _require_mapping(root.get("observability"), "observability")
        return DbctlSettings(
            database=DatabaseConnectionSettings(
                mode=_required_text(database, "mode"),
                application_dsn_env=_text_with_default(
                    database, "application_dsn_env", "AIWS_APP_DATABASE_DSN"
                ),
                migrator_dsn_env=_text_with_default(
                    database, "migrator_dsn_env", "AIWS_MIGRATOR_DATABASE_DSN"
                ),
                admin_dsn_env=_text_with_default(
                    database, "admin_dsn_env", "AIWS_ADMIN_DATABASE_DSN"
                ),
                dashboard_dsn_env=_text_with_default(
                    database, "dashboard_dsn_env", "AIWS_DASHBOARD_DATABASE_DSN"
                ),
            ),
            administration=DatabaseAdministrationSettings(
                database_name=_required_text(administration, "database_name"),
                owner_role=_required_text(administration, "owner_role"),
                migrator_role=_required_text(administration, "migrator_role"),
                app_role=_required_text(administration, "app_role"),
                dashboard_role=_required_text(administration, "dashboard_role"),
                schemas=_parse_schemas(administration),
                observability_schema=_text_with_default(
                    administration, "observability_schema", "observability"
                ),
                local_postgres_user=_text_with_default(
                    administration, "local_postgres_user", "postgres"
                ),
                maintenance_database=_text_with_default(
                    administration, "maintenance_database", "postgres"
                ),
                password_env_by_role=_parse_password_environment_mapping(administration),
            ),
            observability=ObservabilityRetentionSettings(
                retention_days=_require_non_negative_int(observability, "retention_days"),
            ),
        )

    def _load_root_mapping(self) -> dict[str, Any]:
        """@brief 读取 JSONC 根对象 / Read the JSONC root object.

        @return 可变顶层配置字典 / Mutable top-level configuration dictionary.
        @raise DbctlConfigurationError 文件或语法不可用时抛出，且不展示文件内容。
        / Raised when the file or syntax is unavailable, without displaying its content.
        """
        if not self._config_path.is_file():
            raise DbctlConfigurationError(f"dbctl 配置文件不存在：{self._config_path}")
        try:
            parsed = json5.loads(self._config_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise DbctlConfigurationError("dbctl 配置文件无法解析。") from error
        if not isinstance(parsed, Mapping):
            raise DbctlConfigurationError("dbctl 配置根必须是对象。")
        return dict(parsed)


def _validate_environment_variable_name(value: object, field_name: str) -> None:
    """@brief 校验环境变量名 / Validate an environment-variable name.

    @param value 候选变量名 / Candidate variable name.
    @param field_name 出错字段名称 / Field name for diagnostics.
    @return 无返回值 / No return value.
    @raise DbctlConfigurationError 变量名无效时抛出。
    / Raised when the variable name is invalid.
    """
    if not isinstance(value, str) or not _ENVIRONMENT_VARIABLE_PATTERN.fullmatch(value):
        raise DbctlConfigurationError(f"{field_name} 必须是合法环境变量名。")


def _require_secret_environment_value(environ: Mapping[str, str], variable_name: str) -> str:
    """@brief 安全读取必填 secret 环境变量 / Safely read a required secret environment variable.

    @param environ 环境变量映射 / Environment-variable mapping.
    @param variable_name 要读取的变量名 / Variable name to read.
    @return 非空 secret 值 / Non-empty secret value.
    @raise DbctlConfigurationError 值不存在或为空时抛出，且不回显值。
    / Raised when the value is absent or empty, without echoing it.
    """
    value = environ.get(variable_name)
    if not isinstance(value, str) or not value:
        raise DbctlConfigurationError(f"必填数据库凭证环境变量未设置：{variable_name}")
    return value


def _require_mapping(value: object, section_name: str) -> dict[str, Any]:
    """@brief 校验 JSONC 对象配置节 / Validate a JSONC object configuration section.

    @param value 候选对象 / Candidate object.
    @param section_name 配置节名称 / Configuration section name.
    @return 可变字典副本 / Mutable dictionary copy.
    @raise DbctlConfigurationError 值不是对象时抛出。
    / Raised when the value is not an object.
    """
    if not isinstance(value, Mapping):
        raise DbctlConfigurationError(f"{section_name} 必须是对象。")
    return dict(value)


def _required_text(mapping: Mapping[str, Any], key: str) -> str:
    """@brief 读取必填文本字段 / Read a required text field.

    @param mapping 配置对象 / Configuration object.
    @param key 字段名 / Field name.
    @return 修剪后的非空文本 / Trimmed non-empty text.
    @raise DbctlConfigurationError 字段缺失、不是文本或为空时抛出。
    / Raised when the field is absent, not text, or empty.
    """
    return _text_with_default(mapping, key, None)


def _require_non_negative_int(mapping: Mapping[str, Any], key: str) -> int:
    """@brief 读取非负整数配置 / Read a non-negative integer configuration value.

    @param mapping 配置对象 / Configuration object.
    @param key 字段名 / Field name.
    @return 已校验的非负整数 / Validated non-negative integer.
    @raise DbctlConfigurationError 字段缺失、不是整数、是布尔值或小于零时抛出。
    / Raised when the field is absent, non-integer, boolean, or negative.
    """
    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise DbctlConfigurationError(f"{key} 必须是非负整数。")
    return value


def _text_with_default(mapping: Mapping[str, Any], key: str, default: str | None) -> str:
    """@brief 读取带默认值的文本字段 / Read a text field with a default.

    @param mapping 配置对象 / Configuration object.
    @param key 字段名 / Field name.
    @param default 缺失时默认值；``None`` 表示必填 / Default for absence; ``None`` means required.
    @return 修剪后的非空文本 / Trimmed non-empty text.
    @raise DbctlConfigurationError 字段值不是非空文本时抛出。
    / Raised when the field value is not non-empty text.
    """
    value = mapping.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise DbctlConfigurationError(f"{key} 必须是非空字符串。")
    return value.strip()


def _parse_schemas(administration: Mapping[str, Any]) -> tuple[str, ...]:
    """@brief 解析 bootstrap schema 列表 / Parse bootstrap schema list.

    @param administration ``database_administration`` 配置节。
    / ``database_administration`` configuration section.
    @return 不包含重复项的 schema 元组（后续由 settings 再验证）。
    / Schema tuple without interpretation; settings performs final validation.
    @raise DbctlConfigurationError ``schemas`` 不是非空字符串列表时抛出。
    / Raised when ``schemas`` is not a non-empty list of strings.
    """
    raw_schemas = administration.get("schemas")
    if raw_schemas is None:
        observability_schema = _text_with_default(
            administration, "observability_schema", "observability"
        )
        if "application_schema" in administration or "schema_name" in administration:
            application_schema = _text_with_default(
                administration,
                "application_schema",
                administration.get("schema_name", "workspace"),
            )
            return tuple(dict.fromkeys((application_schema, observability_schema)))
        return tuple(
            observability_schema if schema == "observability" else schema
            for schema in _DEFAULT_BOOTSTRAP_SCHEMAS
        )
    if not isinstance(raw_schemas, list) or not raw_schemas:
        raise DbctlConfigurationError("database_administration.schemas 必须是非空数组。")
    schemas: list[str] = []
    for value in raw_schemas:
        if not isinstance(value, str) or not value.strip():
            raise DbctlConfigurationError("database_administration.schemas 必须只包含非空字符串。")
        schemas.append(value.strip())
    return tuple(schemas)


def _parse_password_environment_mapping(
    administration: Mapping[str, Any],
) -> Mapping[DatabaseRole, str]:
    """@brief 解析可选角色密码环境变量 / Parse optional role-password environment variables.

    @param administration ``database_administration`` 配置节。
    / ``database_administration`` configuration section.
    @return 仅含可登录角色的密码变量映射 / Password-variable mapping for login roles only.
    @raise DbctlConfigurationError 映射结构、角色名或重复配置无效时抛出。
    / Raised when mapping structure, role names, or duplicate configuration is invalid.
    """
    result: dict[DatabaseRole, str] = {}
    raw_mapping = administration.get("role_password_envs", {})
    if not isinstance(raw_mapping, Mapping):
        raise DbctlConfigurationError("role_password_envs 必须是对象。")
    for raw_role, raw_variable_name in raw_mapping.items():
        try:
            role = DatabaseRole(raw_role)
        except ValueError as error:
            raise DbctlConfigurationError("role_password_envs 包含未知角色。") from error
        if role is DatabaseRole.OWNER:
            raise DbctlConfigurationError("role_password_envs 不能为 owner 配置密码。")
        _validate_environment_variable_name(raw_variable_name, "role_password_envs")
        result[role] = raw_variable_name

    aliases = {
        "migrator_password_env": DatabaseRole.MIGRATOR,
        "app_password_env": DatabaseRole.APP,
        "dashboard_password_env": DatabaseRole.DASHBOARD,
    }
    for key, role in aliases.items():
        if key not in administration:
            continue
        if role in result:
            raise DbctlConfigurationError(f"{key} 与 role_password_envs 重复配置。")
        variable_name = administration[key]
        _validate_environment_variable_name(variable_name, key)
        result[role] = variable_name
    return result


def read_optional_environment_value(variable_name: str, environ: Mapping[str, str] | None = None) -> str | None:
    """@brief 读取可选环境变量 / Read an optional environment variable.

    @param variable_name 已验证的环境变量名 / Validated environment-variable name.
    @param environ 可选环境映射；默认读取当前进程环境 / Optional environment mapping; defaults to process environment.
    @return 非空值或 ``None`` / Non-empty value or ``None``.

    @note 此函数不记录返回值，适合读取 DSN 或密码等 secret。
    / This function does not log its return value and is suitable for DSNs or passwords.
    """
    _validate_environment_variable_name(variable_name, "环境变量")
    source = os.environ if environ is None else environ
    value = source.get(variable_name)
    return value if isinstance(value, str) and value else None
