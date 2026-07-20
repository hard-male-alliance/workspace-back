"""@brief dbctl 独立配置服务 / Independent configuration service for dbctl."""

from __future__ import annotations

import json
import re
import secrets
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final
from urllib.parse import quote

import json5

from .connection import parse_postgres_dsn
from .errors import DbctlConfigurationError
from .identifiers import validate_postgres_identifier
from .package_resources import read_default_text

_LOCAL_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
_DATABASE_HOST_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._:-]+$")
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

_DEFAULT_CONFIG_PATH: Final[Path] = Path("config.jsonc")
"""@brief 默认私密配置路径 / Default private-configuration path."""

_DEFAULT_DBINIT_NAME: Final[str] = "dbinit.jsonc"
"""@brief 默认数据库声明文件名 / Default database-declaration file name."""

_DEFAULT_EXAMPLE_NAME: Final[str] = "example.jsonc"
"""@brief 默认公开配置模板名 / Default public configuration-template name."""


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
    ``dbctl prune-telemetry`` 的受限运维参数提供，避免把一次性维护强度
    偷偷藏入应用请求路径。
    / This object describes only the retention boundary. Database credentials, batch size, and
    execution switches are supplied by constrained ``dbctl prune-telemetry`` operator
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
    """@brief PostgreSQL DSN 设置 / PostgreSQL DSN settings.

    @param mode 运行时数据库模式；dbctl 不会据此决定是否执行 bootstrap。
    / Runtime database mode; dbctl does not use it to decide whether bootstrap may run.
    """

    mode: str
    application_dsn: str = field(repr=False)
    migrator_dsn: str = field(repr=False)
    dashboard_dsn: str = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 DSN 环境变量名 / Validate DSN environment-variable names.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 字段为空或不是合法环境变量名时抛出。
        / Raised when a field is empty or not a valid environment-variable name.
        """
        if not isinstance(self.mode, str) or not self.mode.strip():
            raise DbctlConfigurationError("database.mode 必须是非空字符串。")
        object.__setattr__(self, "mode", self.mode.strip())
        for field_name in ("application_dsn", "migrator_dsn", "dashboard_dsn"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise DbctlConfigurationError(f"database.{field_name} 必须是非空字符串。")
            object.__setattr__(self, field_name, value.strip())

    def dsn_for(self, role: DatabaseRole) -> str:
        """@brief 获取登录角色的直接 DSN / Get the direct DSN for a login role.

        @param role 请求连接的角色类别 / Requested connection role.
        @return config.jsonc 中对应的 DSN / Corresponding DSN from config.jsonc.
        @raise DbctlConfigurationError 请求 NOLOGIN owner 时抛出 / Raised for the NOLOGIN owner.
        """
        if role is DatabaseRole.APP:
            return self.application_dsn
        if role is DatabaseRole.MIGRATOR:
            return self.migrator_dsn
        if role is DatabaseRole.DASHBOARD:
            return self.dashboard_dsn
        raise DbctlConfigurationError("workspace_owner 是 NOLOGIN 角色，不能启动 psql shell。")


@dataclass(frozen=True, slots=True)
class DatabaseEndpointSettings:
    """@brief 生成本地 DSN 所需的数据库端点 / Database endpoint used to generate local DSNs."""

    host: str
    port: int

    def __post_init__(self) -> None:
        """@brief 校验主机与端口 / Validate host and port.

        @return 无返回值 / No return value.
        """
        if not isinstance(self.host, str) or not _DATABASE_HOST_PATTERN.fullmatch(
            self.host.strip()
        ):
            raise DbctlConfigurationError(
                "database_connection.host 必须是合法的 DNS、IPv4 或 IPv6 主机。"
            )
        if (
            not isinstance(self.port, int)
            or isinstance(self.port, bool)
            or not 1 <= self.port <= 65535
        ):
            raise DbctlConfigurationError("database_connection.port 必须是 1 到 65535 的整数。")
        object.__setattr__(self, "host", self.host.strip())


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
    @param local_postgres_user sudo 模式传给 ``sudo -u`` 的本机账户。
    / Local account passed to ``sudo -u`` in sudo mode.
    @param bootstrap_database_user 无 sudo 平台通过终端密码连接的 PostgreSQL 管理角色。
    / PostgreSQL administrative role used for terminal-password connections without sudo.
    @param maintenance_database 管理员连接用于创建目标数据库的 maintenance database。
    / Maintenance database used by the administrator connection to create the target database.
    """

    database_name: str
    owner_role: str
    migrator_role: str
    app_role: str
    dashboard_role: str
    schemas: tuple[str, ...] = _DEFAULT_BOOTSTRAP_SCHEMAS
    observability_schema: str = "observability"
    local_postgres_user: str = "postgres"
    bootstrap_database_user: str = "postgres"
    maintenance_database: str = "postgres"

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
                raise DbctlConfigurationError(
                    "bootstrap role 不能使用 PostgreSQL 系统或伪角色名称。"
                )

        if not isinstance(self.schemas, tuple) or not self.schemas:
            raise DbctlConfigurationError("database_administration.schemas 必须是非空列表。")
        schemas = tuple(
            validate_postgres_identifier(value, kind="schema 名") for value in self.schemas
        )
        if len(set(schemas)) != len(schemas):
            raise DbctlConfigurationError("database_administration.schemas 不能包含重复 schema。")
        for schema in schemas:
            normalized = schema.casefold()
            if normalized == "information_schema" or normalized.startswith("pg_"):
                raise DbctlConfigurationError(
                    "bootstrap schema 不能使用 PostgreSQL 系统 schema 名称。"
                )

        observability_schema = validate_postgres_identifier(
            self.observability_schema, kind="observability schema 名"
        )
        if observability_schema not in schemas:
            raise DbctlConfigurationError("observability schema 必须包含在 schemas 中。")

        if not isinstance(self.local_postgres_user, str) or not _LOCAL_ACCOUNT_PATTERN.fullmatch(
            self.local_postgres_user
        ):
            raise DbctlConfigurationError("local_postgres_user 必须是安全的本机 Unix 账户名。")
        bootstrap_database_user = validate_postgres_identifier(
            self.bootstrap_database_user,
            kind="bootstrap 数据库用户",
        )

        object.__setattr__(self, "database_name", database_name)
        object.__setattr__(self, "maintenance_database", maintenance_database)
        object.__setattr__(self, "owner_role", role_values[DatabaseRole.OWNER])
        object.__setattr__(self, "migrator_role", role_values[DatabaseRole.MIGRATOR])
        object.__setattr__(self, "app_role", role_values[DatabaseRole.APP])
        object.__setattr__(self, "dashboard_role", role_values[DatabaseRole.DASHBOARD])
        object.__setattr__(self, "schemas", schemas)
        object.__setattr__(self, "observability_schema", observability_schema)
        object.__setattr__(self, "bootstrap_database_user", bootstrap_database_user)

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

    @param database 私密 config.jsonc 中的直接 DSN / Direct DSNs in private config.jsonc.
    @param administration 非敏感的 role、database、schema 管理设置。
    / Non-secret role, database, and schema administration settings.
    @param observability 仅供受控运维清理读取的遥测保留设置。
    / Telemetry-retention settings read only by controlled operator pruning.
    """

    database: DatabaseConnectionSettings
    administration: DatabaseAdministrationSettings
    observability: ObservabilityRetentionSettings
    role_passwords: Mapping[DatabaseRole, str] = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 冻结本地生成的登录角色密码 / Freeze locally generated login-role passwords.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 密码映射包含 owner、未知角色或空密码时抛出。
        / Raised when the password map contains owner, an unknown role, or an empty password.
        """
        normalized: dict[DatabaseRole, str] = {}
        for raw_role, password in self.role_passwords.items():
            try:
                role = DatabaseRole(raw_role)
            except ValueError as error:
                raise DbctlConfigurationError("数据库凭证包含未知角色。") from error
            if role is DatabaseRole.OWNER:
                raise DbctlConfigurationError("NOLOGIN owner role 不能配置密码。")
            if not isinstance(password, str) or not password or "\x00" in password:
                raise DbctlConfigurationError("登录 role 密码必须是非空且不含 NUL 的字符串。")
            normalized[role] = password
        required_roles = {DatabaseRole.MIGRATOR, DatabaseRole.APP, DatabaseRole.DASHBOARD}
        if set(normalized) != required_roles:
            raise DbctlConfigurationError("数据库凭证必须完整包含三个登录角色。")
        object.__setattr__(self, "role_passwords", MappingProxyType(normalized))

    def require_migrator_dsn(self) -> str:
        """@brief 获取迁移 DSN / Get the migrator DSN.

        @return 非空 migrator DSN / Non-empty migrator DSN.
        @raise DbctlConfigurationError 迁移 DSN 未设置时抛出，且不回显 DSN。
        / Raised when migrator DSN is absent without echoing it.
        """
        return self.database.migrator_dsn

    def require_shell_dsn(self, role: DatabaseRole) -> str:
        """@brief 从环境读取 shell 身份对应的 DSN / Read the DSN for a shell identity.

        @param role 请求 shell 的可登录角色 / Login role requested for shell.
        @return 非空的角色 DSN / Non-empty role DSN.
        @raise DbctlConfigurationError DSN 缺失或请求 owner shell 时抛出。
        / Raised when the DSN is missing or an owner shell is requested.
        """
        return self.database.dsn_for(role)


class DbctlConfigurationService:
    """@brief 读取私密运行配置与独立 dbinit 声明 / Load private runtime config and the separate dbinit declaration.

    默认 ``config.jsonc`` 是被 Git 忽略的本地运行配置，首次缺失时由 ``example.jsonc``
    生成并写入随机登录角色密码；``dbinit.jsonc`` 是可提交、无密钥的数据库目标状态。
    显式路径缺失时不会回退内置资源。/ Default ``config.jsonc`` is Git-ignored local runtime
    configuration. When absent it is generated from ``example.jsonc`` and populated with random
    login-role passwords; ``dbinit.jsonc`` is the committable, secret-free desired database state.
    Missing explicit paths never fall back to bundled resources.
    """

    def __init__(
        self,
        config_path: Path | str | None = None,
        dbinit_path: Path | str | None = None,
    ) -> None:
        """@brief 初始化配置服务 / Initialize the configuration service.

        @param config_path 私密运行配置路径；``None`` 表示默认路径并允许内置模板回退。
        / Private runtime-configuration path; ``None`` selects the default and permits bundled-template fallback.
        @param dbinit_path 可提交数据库初始化声明路径；默认与 config 同目录的 dbinit.jsonc。
        / Committable database-initialization declaration; defaults to dbinit.jsonc beside config.
        """
        uses_default_config = config_path is None
        """@brief 调用方是否省略配置路径 / Whether the caller omitted the config path."""
        self._config_path = _DEFAULT_CONFIG_PATH if config_path is None else Path(config_path)
        self._dbinit_path = (
            self._config_path.parent / _DEFAULT_DBINIT_NAME
            if dbinit_path is None
            else Path(dbinit_path)
        )
        self._allow_packaged_config_fallback = uses_default_config
        self._allow_packaged_dbinit_fallback = uses_default_config and dbinit_path is None

    @property
    def config_path(self) -> Path:
        """@brief 返回配置路径 / Return the configuration path.

        @return 未解析为绝对路径的调用方配置路径 / Caller-configured path without forced absolute resolution.
        """
        return self._config_path

    @property
    def dbinit_path(self) -> Path:
        """@brief 返回数据库初始化声明路径 / Return the database-initialization declaration path.

        @return 调用方配置的 dbinit.jsonc 路径 / Caller-configured dbinit.jsonc path.
        """
        return self._dbinit_path

    def load(self) -> DbctlSettings:
        """@brief 加载 dbctl 设置 / Load dbctl settings.

        @return 已完整验证的 DbctlSettings / Fully validated DbctlSettings.
        @raise DbctlConfigurationError 文件缺失、JSONC 无效或所需配置节不合规时抛出。
        / Raised for a missing file, invalid JSONC, or malformed required configuration sections.
        """
        dbinit = self._load_dbinit_mapping()
        administration = _require_mapping(
            dbinit.get("database_administration"), "database_administration"
        )
        endpoint_mapping = _require_mapping(
            dbinit.get("database_connection"), "database_connection"
        )
        administration_settings = DatabaseAdministrationSettings(
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
            bootstrap_database_user=_text_with_default(
                administration, "bootstrap_database_user", "postgres"
            ),
            maintenance_database=_text_with_default(
                administration, "maintenance_database", "postgres"
            ),
        )
        endpoint = DatabaseEndpointSettings(
            host=_required_text(endpoint_mapping, "host"),
            port=_require_port(endpoint_mapping, "port"),
        )
        root = self._load_or_create_private_root_mapping(administration_settings, endpoint)
        database = _require_mapping(root.get("database"), "database")
        observability = _require_mapping(root.get("observability"), "observability")
        connection_settings = DatabaseConnectionSettings(
            mode=_required_text(database, "mode"),
            application_dsn=_required_text(database, "application_dsn"),
            migrator_dsn=_required_text(database, "migrator_dsn"),
            dashboard_dsn=_required_text(database, "dashboard_dsn"),
        )
        return DbctlSettings(
            database=connection_settings,
            administration=administration_settings,
            observability=ObservabilityRetentionSettings(
                retention_days=_require_non_negative_int(observability, "retention_days"),
            ),
            role_passwords=_passwords_from_dsns(connection_settings, administration_settings),
        )

    def _load_dbinit_mapping(self) -> dict[str, Any]:
        """@brief 读取显式 dbinit 或默认内置声明 / Load explicit dbinit or the bundled default declaration.

        @return 可变 dbinit 根对象 / Mutable dbinit root mapping.
        @raise DbctlConfigurationError 显式路径缺失或默认资源损坏时抛出。
        / Raised when an explicit path is missing or the default resource is invalid.
        """

        if self._dbinit_path.is_file() or not self._allow_packaged_dbinit_fallback:
            return self._load_mapping_file(self._dbinit_path, "dbinit")
        return self._load_mapping_text(read_default_text("dbinit.jsonc"), "dbinit")

    def _load_or_create_private_root_mapping(
        self,
        administration: DatabaseAdministrationSettings,
        endpoint: DatabaseEndpointSettings,
    ) -> dict[str, Any]:
        """@brief 创建或读取私密运行配置并确保登录密码存在 / Create or load private config and ensure login passwords exist.

        @return 含三个生成密码的可变根对象 / Mutable root containing three generated passwords.
        @raise DbctlConfigurationError 模板缺失、文件不可写或配置无效时抛出。
        / Raised when the template is absent, the file is unwritable, or configuration is invalid.
        """
        if not self._config_path.is_file():
            if not self._allow_packaged_config_fallback:
                raise DbctlConfigurationError(f"config.jsonc 不存在：{self._config_path}")
            template_path = self._config_path.parent / _DEFAULT_EXAMPLE_NAME
            template_text: str
            """@brief 即将落盘的公开模板文本 / Public template text to persist."""
            if template_path.is_file():
                try:
                    template_text = template_path.read_text(encoding="utf-8")
                except OSError as error:
                    raise DbctlConfigurationError("无法读取 example.jsonc 初始化模板。") from error
            else:
                template_text = read_default_text("example.jsonc")
            self._write_private_text(template_text)
        root = self._load_mapping_file(self._config_path, "dbctl 配置")
        changed = _ensure_database_dsns(root, administration, endpoint)
        if changed:
            self._write_private_root(root)
        else:
            try:
                self._config_path.chmod(0o600)
            except OSError as error:
                raise DbctlConfigurationError("无法收紧 config.jsonc 文件权限。") from error
        return root

    def _write_private_root(self, root: Mapping[str, Any]) -> None:
        """@brief 原子写入仅本机可读的私密配置 / Atomically write owner-only private configuration.

        @param root 待序列化的完整配置根对象 / Complete configuration root to serialize.
        @return 无返回值 / No return value.
        """
        self._write_private_text(json.dumps(root, ensure_ascii=False, indent=2) + "\n")

    def _write_private_text(self, content: str) -> None:
        """@brief 原子写入权限为 0600 的配置文本 / Atomically write configuration text with mode 0600.

        @param content 待写入 UTF-8 文本 / UTF-8 text to write.
        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 文件无法安全写入时抛出 / Raised when the file cannot be written safely.
        """

        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._config_path.parent,
                prefix=f".{self._config_path.name}.",
                delete=False,
            ) as temporary_file:
                temporary_file.write(content)
                temporary_path = Path(temporary_file.name)
            temporary_path.chmod(0o600)
            temporary_path.replace(self._config_path)
        except OSError as error:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise DbctlConfigurationError("无法写入私密 config.jsonc。") from error

    @staticmethod
    def _load_mapping_file(path: Path, label: str) -> dict[str, Any]:
        """@brief 加载一个 JSONC 根对象 / Load one JSONC root mapping.

        @param path 待读取文件 / File to read.
        @param label 安全错误标签 / Safe error label.
        @return 可变顶层对象 / Mutable root mapping.
        """
        if not path.is_file():
            raise DbctlConfigurationError(f"{label}文件不存在：{path}")
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as error:
            raise DbctlConfigurationError(f"{label}文件无法解析。") from error
        return DbctlConfigurationService._load_mapping_text(content, label)

    @staticmethod
    def _load_mapping_text(content: str, label: str) -> dict[str, Any]:
        """@brief 解析一个 JSONC 根对象文本 / Parse one JSONC root mapping from text.

        @param content UTF-8 JSONC 文本 / UTF-8 JSONC text.
        @param label 安全错误标签 / Safe error label.
        @return 可变顶层对象 / Mutable root mapping.
        """

        try:
            parsed = json5.loads(content)
        except ValueError as error:
            raise DbctlConfigurationError(f"{label}文件无法解析。") from error
        if not isinstance(parsed, Mapping):
            raise DbctlConfigurationError(f"{label}根必须是对象。")
        return dict(parsed)


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


def _ensure_database_dsns(
    root: dict[str, Any],
    administration: DatabaseAdministrationSettings,
    endpoint: DatabaseEndpointSettings,
) -> bool:
    """@brief 确保私密配置直接包含三个角色 DSN / Ensure private config directly contains three role DSNs.

    @param root 可变 config.jsonc 根对象 / Mutable config.jsonc root object.
    @param administration 角色与数据库声明 / Role and database declaration.
    @param endpoint DSN 连接端点 / DSN connection endpoint.
    @return 配置是否发生变化 / Whether the configuration changed.
    @note 旧版 database_role_passwords 会被原地迁移并删除，密码不会轮换。
    / Legacy database_role_passwords is migrated in place and removed without rotating passwords.
    """
    raw_database = root.get("database")
    if not isinstance(raw_database, dict):
        raise DbctlConfigurationError("database 必须是对象。")
    legacy_credentials = root.get("database_role_passwords", {})
    if not isinstance(legacy_credentials, Mapping):
        raise DbctlConfigurationError("database_role_passwords 必须是对象。")

    fields = {
        DatabaseRole.MIGRATOR: "migrator_dsn",
        DatabaseRole.APP: "application_dsn",
        DatabaseRole.DASHBOARD: "dashboard_dsn",
    }
    passwords: dict[DatabaseRole, str] = {}
    changed = False
    for role, field_name in fields.items():
        raw_dsn = raw_database.get(field_name)
        if isinstance(raw_dsn, str) and raw_dsn.strip():
            parsed = parse_postgres_dsn(raw_dsn)
            if parsed.user != administration.role_name(role) or not parsed.password:
                raise DbctlConfigurationError(
                    f"database.{field_name} 必须包含 dbinit.jsonc 声明的角色和非空密码。"
                )
            passwords[role] = parsed.password
            continue
        legacy_password = legacy_credentials.get(role.value)
        if legacy_password is not None and (
            not isinstance(legacy_password, str) or not legacy_password or "\x00" in legacy_password
        ):
            raise DbctlConfigurationError(
                f"database_role_passwords.{role.value} 必须是非空字符串。"
            )
        password = legacy_password or secrets.token_urlsafe(32)
        passwords[role] = password
        raw_database[field_name] = _build_postgres_dsn(
            endpoint=endpoint,
            database_name=administration.database_name,
            role_name=administration.role_name(role),
            password=password,
        )
        changed = True

    if len(set(passwords.values())) != len(passwords):
        raise DbctlConfigurationError("三个登录 role 必须使用互不相同的密码。")
    if "database_role_passwords" in root:
        del root["database_role_passwords"]
        changed = True
    return changed


def _build_postgres_dsn(
    *,
    endpoint: DatabaseEndpointSettings,
    database_name: str,
    role_name: str,
    password: str,
) -> str:
    """@brief 构造包含本地生成凭证的 PostgreSQL URI / Build a PostgreSQL URI with generated credentials.

    @param endpoint 数据库主机与端口 / Database host and port.
    @param database_name 目标数据库名 / Target database name.
    @param role_name 登录角色名 / Login role name.
    @param password 随机生成密码 / Randomly generated password.
    @return 百分号编码的 PostgreSQL URI / Percent-encoded PostgreSQL URI.
    """
    host = (
        f"[{endpoint.host}]"
        if ":" in endpoint.host and not endpoint.host.startswith("[")
        else endpoint.host
    )
    return (
        f"postgresql://{quote(role_name, safe='')}:{quote(password, safe='')}@"
        f"{host}:{endpoint.port}/{quote(database_name, safe='')}"
    )


def _passwords_from_dsns(
    database: DatabaseConnectionSettings,
    administration: DatabaseAdministrationSettings,
) -> dict[DatabaseRole, str]:
    """@brief 从实际 DSN 派生 bootstrap 密码 / Derive bootstrap passwords from actual DSNs.

    @param database 三个运行时 DSN / Three runtime DSNs.
    @param administration 角色声明 / Role declaration.
    @return 登录角色到密码的映射 / Login-role-to-password mapping.
    """
    result: dict[DatabaseRole, str] = {}
    for role in (DatabaseRole.MIGRATOR, DatabaseRole.APP, DatabaseRole.DASHBOARD):
        parsed = parse_postgres_dsn(database.dsn_for(role))
        if parsed.user != administration.role_name(role) or not parsed.password:
            raise DbctlConfigurationError("config.jsonc DSN 与 dbinit.jsonc 角色声明不一致。")
        result[role] = parsed.password
    return result


def _require_port(mapping: Mapping[str, Any], key: str) -> int:
    """@brief 读取 TCP 端口 / Read a TCP port.

    @param mapping 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @return 1 到 65535 的端口 / Port from 1 through 65535.
    """
    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 65535:
        raise DbctlConfigurationError(f"{key} 必须是 1 到 65535 的整数。")
    return value
