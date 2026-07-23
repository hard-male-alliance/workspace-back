"""@brief dbctl 数据库聚合与连接目录 / dbctl database aggregate and connection catalog."""

import re
from dataclasses import dataclass, field
from typing import Final, Literal, assert_never, overload

from .errors import InvalidDatabaseModelError
from .names import DatabaseName, RoleName, SchemaName
from .retention import RetentionPolicy
from .roles import LoginRole, RoleSet, Secret

_DATABASE_HOST_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._:-]+$")
"""@brief PostgreSQL 主机文本的保守白名单 / Conservative PostgreSQL host-text allow-list."""

_LOCAL_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
"""@brief sudo 本机账户名白名单 / Allow-list for local sudo account names."""

_RESERVED_DATABASE_NAMES: Final[frozenset[str]] = frozenset({"postgres", "template0", "template1"})
"""@brief 禁止作为业务数据库的系统数据库名 / System names forbidden for project databases."""

IDENTITY_SCHEMA: Final[SchemaName] = SchemaName("identity")
"""@brief 身份与迁移元数据 schema / Identity and migration-metadata schema."""

OBSERVABILITY_SCHEMA: Final[SchemaName] = SchemaName("observability")
"""@brief 遥测 schema / Telemetry schema."""

CANONICAL_SCHEMA_CATALOG: Final[tuple[SchemaName, ...]] = (
    IDENTITY_SCHEMA,
    SchemaName("resume"),
    SchemaName("agent"),
    SchemaName("interview"),
    SchemaName("knowledge"),
    OBSERVABILITY_SCHEMA,
)
"""@brief 应用唯一受支持的固定 schema 目录 / The application's sole canonical schema catalog."""

type DataRegion = Literal["cn", "global", "private_deployment"]
"""@brief 迁移允许的数据驻留地域 / Data-residency regions accepted by migrations."""

type WorkspacePlan = Literal["personal", "team", "enterprise"]
"""@brief 迁移允许的 Workspace 计划 / Workspace plans accepted by migrations."""

type LegacyWorkspacePlans = tuple[tuple[str, WorkspacePlan], ...]
"""@brief 按 Workspace ID 排序的显式旧数据计划映射 / Explicit legacy plan mapping sorted by Workspace ID."""


@dataclass(frozen=True, slots=True)
class DatabaseTarget:
    """@brief 一个完整且不可拆分的 PostgreSQL 目标 / A complete, indivisible PostgreSQL target.

    @param host PostgreSQL DNS 名或 IP 文本 / PostgreSQL DNS name or IP text.
    @param port TCP 端口 / TCP port.
    @param database 目标数据库 / Target database.
    """

    host: str
    port: int
    database: DatabaseName

    def __post_init__(self) -> None:
        """@brief 校验目标端点 / Validate the target endpoint.

        @return 无返回值 / No return value.
        @raise InvalidDatabaseModelError 主机、端口或数据库对象无效时抛出。
        / Raised when host, port, or database object is invalid.
        """
        if not isinstance(self.host, str) or not _DATABASE_HOST_PATTERN.fullmatch(
            self.host.strip()
        ):
            raise InvalidDatabaseModelError("数据库 host 必须是合法的 DNS、IPv4 或 IPv6 文本。")
        if (
            not isinstance(self.port, int)
            or isinstance(self.port, bool)
            or not 1 <= self.port <= 65_535
        ):
            raise InvalidDatabaseModelError("数据库 port 必须是 1 到 65535 的整数。")
        if not isinstance(self.database, DatabaseName):
            raise InvalidDatabaseModelError("数据库 target 必须持有 DatabaseName。")
        object.__setattr__(self, "host", self.host.strip())


@dataclass(frozen=True, slots=True)
class DatabaseLogin[R: LoginRole]:
    """@brief 用类型参数固定用途的数据库登录身份 / Purpose-typed database login identity.

    @param role 登录职责；泛型参数使调用边界不能交换职责 / Login purpose fixed by the type argument.
    @param role_name DSN 中的实际 PostgreSQL 用户 / PostgreSQL user encoded by the DSN.
    @param target DSN 指向的完整数据库目标 / Complete target encoded by the DSN.
    @param dsn 仅交给数据库驱动的原始 DSN / Raw DSN exposed only to a database driver.
    @param safe_conninfo 已移除密码的 libpq conninfo / Password-free libpq conninfo.
    @param password 与 role 绑定的密码 / Password bound to the role.
    """

    role: R
    role_name: RoleName
    target: DatabaseTarget
    dsn: Secret[str] = field(repr=False)
    safe_conninfo: str = field(repr=False)
    password: Secret[str] = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验登录身份的结构完整性 / Validate login-identity structural integrity.

        @return 无返回值 / No return value.
        @raise InvalidDatabaseModelError 任一字段不符合其值对象契约时抛出。
        / Raised when any field violates its value-object contract.
        """
        if not isinstance(self.role, LoginRole):
            raise InvalidDatabaseModelError("DatabaseLogin.role 必须是 LoginRole。")
        if not isinstance(self.role_name, RoleName):
            raise InvalidDatabaseModelError("DatabaseLogin.role_name 必须是 RoleName。")
        if not isinstance(self.target, DatabaseTarget):
            raise InvalidDatabaseModelError("DatabaseLogin.target 必须是 DatabaseTarget。")
        if not isinstance(self.dsn, Secret) or not isinstance(self.dsn.reveal(), str):
            raise InvalidDatabaseModelError("DatabaseLogin.dsn 必须是文本 Secret。")
        if not isinstance(self.password, Secret) or not isinstance(self.password.reveal(), str):
            raise InvalidDatabaseModelError("DatabaseLogin.password 必须是文本 Secret。")
        if (
            not isinstance(self.safe_conninfo, str)
            or not self.safe_conninfo
            or "\x00" in self.safe_conninfo
        ):
            raise InvalidDatabaseModelError("DatabaseLogin.safe_conninfo 无效。")


type MigratorLogin = DatabaseLogin[Literal[LoginRole.MIGRATOR]]
"""@brief 迁移专用登录类型 / Login type dedicated to migrations."""

type AppLogin = DatabaseLogin[Literal[LoginRole.APP]]
"""@brief 应用运行时登录类型 / Runtime application login type."""

type DashboardLogin = DatabaseLogin[Literal[LoginRole.DASHBOARD]]
"""@brief Dashboard 只读登录类型 / Dashboard read-only login type."""

type LoginDatabase = MigratorLogin | AppLogin | DashboardLogin
"""@brief 所有允许建立连接的类型并集 / Union of all login-capable database identities."""


@dataclass(frozen=True, slots=True)
class ConnectionCatalog:
    """@brief 同一目标上的三种强类型登录目录 / Three purpose-typed logins for one target.

    @param target 所有登录必须共享的数据库目标 / Database target shared by every login.
    @param migrator 迁移登录 / Migration login.
    @param application 应用运行时登录 / Runtime application login.
    @param dashboard Dashboard 只读登录 / Dashboard read-only login.
    """

    target: DatabaseTarget
    migrator: MigratorLogin = field(repr=False)
    application: AppLogin = field(repr=False)
    dashboard: DashboardLogin = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 拒绝用途、目标或密码漂移 / Reject purpose, target, or password drift.

        @return 无返回值 / No return value.
        @raise InvalidDatabaseModelError 登录目录不一致时抛出。
        / Raised when the login catalog is inconsistent.
        """
        expected = (
            (self.migrator, LoginRole.MIGRATOR),
            (self.application, LoginRole.APP),
            (self.dashboard, LoginRole.DASHBOARD),
        )
        if not isinstance(self.target, DatabaseTarget):
            raise InvalidDatabaseModelError("ConnectionCatalog.target 必须是 DatabaseTarget。")
        for login, role in expected:
            if not isinstance(login, DatabaseLogin) or login.role is not role:
                raise InvalidDatabaseModelError(f"{role.value} 登录身份用途不匹配。")
            if login.target != self.target:
                raise InvalidDatabaseModelError("所有登录身份必须指向同一 DatabaseTarget。")
        passwords = tuple(login.password.reveal() for login, _ in expected)
        if len(set(passwords)) != len(passwords):
            raise InvalidDatabaseModelError("三个登录角色必须使用互不相同的密码。")

    @overload
    def login_for(self, role: Literal[LoginRole.MIGRATOR]) -> MigratorLogin: ...

    @overload
    def login_for(self, role: Literal[LoginRole.APP]) -> AppLogin: ...

    @overload
    def login_for(self, role: Literal[LoginRole.DASHBOARD]) -> DashboardLogin: ...

    @overload
    def login_for(self, role: LoginRole) -> LoginDatabase: ...

    def login_for(self, role: LoginRole) -> LoginDatabase:
        """@brief 按职责返回强类型登录 / Return a purpose-typed login by responsibility.

        @param role 请求的登录职责 / Requested login purpose.
        @return 与职责绑定的登录值对象 / Login value object bound to that purpose.
        """
        if role is LoginRole.MIGRATOR:
            return self.migrator
        if role is LoginRole.APP:
            return self.application
        if role is LoginRole.DASHBOARD:
            return self.dashboard
        assert_never(role)


@dataclass(frozen=True, slots=True)
class DatabaseBlueprint:
    """@brief 项目数据库的期望结构与权限身份 / Desired project database shape and identities.

    @param database 项目数据库名 / Project database name.
    @param roles 四职责角色集合 / Four-responsibility role set.
    @param v2_default_data_region 旧 Workspace 的显式数据地域 / Explicit data region for legacy Workspaces.
    @param v2_legacy_workspace_plans 每个旧 Workspace 的显式计划 / Explicit plan for every legacy Workspace.
    @param schemas 固定 schema 目录；不支持运行时扩展 / Fixed schema catalog; runtime extension is unsupported.
    """

    database: DatabaseName
    roles: RoleSet
    v2_default_data_region: DataRegion
    v2_legacy_workspace_plans: LegacyWorkspacePlans
    schemas: tuple[SchemaName, ...] = CANONICAL_SCHEMA_CATALOG

    def __post_init__(self) -> None:
        """@brief 保证 blueprint 采用唯一规范目录 / Require the sole canonical catalog.

        @return 无返回值 / No return value.
        @raise InvalidDatabaseModelError 目标为系统数据库或 schema 目录漂移时抛出。
        / Raised for a system database target or schema-catalog drift.
        """
        if not isinstance(self.database, DatabaseName):
            raise InvalidDatabaseModelError("DatabaseBlueprint.database 必须是 DatabaseName。")
        if self.database.value.casefold() in _RESERVED_DATABASE_NAMES:
            raise InvalidDatabaseModelError("项目数据库不能使用 PostgreSQL 系统数据库名。")
        if not isinstance(self.roles, RoleSet):
            raise InvalidDatabaseModelError("DatabaseBlueprint.roles 必须是 RoleSet。")
        if self.v2_default_data_region not in {"cn", "global", "private_deployment"}:
            raise InvalidDatabaseModelError("V2 默认数据地域不合法。")
        if (
            not isinstance(self.v2_legacy_workspace_plans, tuple)
            or any(
                not isinstance(workspace_id, str)
                or not workspace_id
                or workspace_id.strip() != workspace_id
                or plan not in {"personal", "team", "enterprise"}
                for workspace_id, plan in self.v2_legacy_workspace_plans
            )
            or tuple(sorted(self.v2_legacy_workspace_plans))
            != self.v2_legacy_workspace_plans
            or len({workspace_id for workspace_id, _ in self.v2_legacy_workspace_plans})
            != len(self.v2_legacy_workspace_plans)
        ):
            raise InvalidDatabaseModelError("V2 旧 Workspace plan 映射不合法。")
        if not isinstance(self.schemas, tuple) or self.schemas != CANONICAL_SCHEMA_CATALOG:
            raise InvalidDatabaseModelError("项目必须使用固定的 canonical schema 目录。")

    @property
    def observability_schema(self) -> SchemaName:
        """@brief 返回固定的 observability schema / Return the fixed observability schema.

        @return OBSERVABILITY_SCHEMA 值对象 / OBSERVABILITY_SCHEMA value object.
        """
        return OBSERVABILITY_SCHEMA


@dataclass(frozen=True, slots=True)
class BootstrapAccess:
    """@brief bootstrap 管理连接的非秘密访问参数 / Non-secret bootstrap access parameters.

    @param maintenance_target 创建项目数据库所用目标 / Target used to create the project database.
    @param local_postgres_user sudo 模式使用的本机账户 / Local account used by sudo mode.
    @param bootstrap_database_user 无 sudo 时使用的 PostgreSQL 管理角色 / PostgreSQL admin role without sudo.
    """

    maintenance_target: DatabaseTarget
    local_postgres_user: str = "postgres"
    bootstrap_database_user: RoleName = field(default_factory=lambda: RoleName("postgres"))

    def __post_init__(self) -> None:
        """@brief 校验管理访问边界 / Validate administrative access boundaries.

        @return 无返回值 / No return value.
        @raise InvalidDatabaseModelError target、账户或角色不合法时抛出。
        / Raised when target, account, or role is invalid.
        """
        if not isinstance(self.maintenance_target, DatabaseTarget):
            raise InvalidDatabaseModelError("maintenance_target 必须是 DatabaseTarget。")
        if not isinstance(self.local_postgres_user, str) or not _LOCAL_ACCOUNT_PATTERN.fullmatch(
            self.local_postgres_user
        ):
            raise InvalidDatabaseModelError("local_postgres_user 必须是安全的本机 Unix 账户名。")
        if not isinstance(self.bootstrap_database_user, RoleName):
            raise InvalidDatabaseModelError("bootstrap_database_user 必须是 RoleName。")


@dataclass(frozen=True, slots=True)
class DbctlSettings:
    """@brief dbctl 用例所需的完整领域设置 / Complete domain settings required by dbctl use cases.

    @param blueprint 项目数据库目标状态 / Desired project database state.
    @param access bootstrap 管理访问设置 / Bootstrap administrative access settings.
    @param connections 项目数据库强类型连接目录 / Purpose-typed project connection catalog.
    @param retention 遥测保留策略 / Telemetry-retention policy.
    """

    blueprint: DatabaseBlueprint
    access: BootstrapAccess
    connections: ConnectionCatalog = field(repr=False)
    retention: RetentionPolicy

    def __post_init__(self) -> None:
        """@brief 在聚合根处阻止跨对象配置漂移 / Prevent cross-object drift at the aggregate root.

        @return 无返回值 / No return value.
        @raise InvalidDatabaseModelError 目标、角色或 maintenance 数据库不一致时抛出。
        / Raised when targets, roles, or the maintenance database are inconsistent.
        """
        if not isinstance(self.blueprint, DatabaseBlueprint):
            raise InvalidDatabaseModelError("settings.blueprint 必须是 DatabaseBlueprint。")
        if not isinstance(self.access, BootstrapAccess):
            raise InvalidDatabaseModelError("settings.access 必须是 BootstrapAccess。")
        if not isinstance(self.connections, ConnectionCatalog):
            raise InvalidDatabaseModelError("settings.connections 必须是 ConnectionCatalog。")
        if not isinstance(self.retention, RetentionPolicy):
            raise InvalidDatabaseModelError("settings.retention 必须是 RetentionPolicy。")
        if self.connections.target.database != self.blueprint.database:
            raise InvalidDatabaseModelError("连接 DSN 的 dbname 必须与 blueprint 数据库一致。")
        if (
            self.access.maintenance_target.host,
            self.access.maintenance_target.port,
        ) != (self.connections.target.host, self.connections.target.port):
            raise InvalidDatabaseModelError("maintenance 与项目连接必须指向同一 PostgreSQL 实例。")
        if self.access.maintenance_target.database == self.blueprint.database:
            raise InvalidDatabaseModelError("maintenance 数据库与项目数据库不能相同。")
        if self.access.bootstrap_database_user in (
            self.blueprint.roles.owner,
            self.blueprint.roles.migrator,
            self.blueprint.roles.application,
            self.blueprint.roles.dashboard,
        ):
            raise InvalidDatabaseModelError("bootstrap 管理角色不能与任一受管业务角色重名。")
        expected_names = (
            (self.connections.migrator, self.blueprint.roles.migrator),
            (self.connections.application, self.blueprint.roles.application),
            (self.connections.dashboard, self.blueprint.roles.dashboard),
        )
        if any(login.role_name != expected_name for login, expected_name in expected_names):
            raise InvalidDatabaseModelError("连接 DSN 的 user 必须与 blueprint 角色名一致。")
