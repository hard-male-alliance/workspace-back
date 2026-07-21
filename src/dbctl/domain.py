"""@brief dbctl 数据库身份领域模型 / Database-identity domain model for dbctl."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from .errors import DbctlConfigurationError


class DatabaseRole(StrEnum):
    """@brief bootstrap 管理的全部数据库角色 / All database roles managed by bootstrap."""

    OWNER = "owner"
    MIGRATOR = "migrator"
    APP = "app"
    DASHBOARD = "dashboard"


class LoginRole(StrEnum):
    """@brief 可建立数据库连接的角色 / Database roles that may establish a connection.

    ``OWNER`` 不属于本类型，因此 shell 和 migration API 无法接收 NOLOGIN owner。
    / ``OWNER`` is absent by construction, so shell and migration APIs cannot accept it.
    """

    MIGRATOR = "migrator"
    APP = "app"
    DASHBOARD = "dashboard"

    def as_database_role(self) -> DatabaseRole:
        """@brief 转换为 bootstrap 角色类别 / Convert to the bootstrap role category.

        @return 同名 DatabaseRole / DatabaseRole with the same semantic identity.
        """
        return DatabaseRole(self.value)


@dataclass(frozen=True, slots=True)
class DatabaseLogin:
    """@brief 从私密配置解析出的完整登录身份 / Complete login identity parsed from private config.

    @param role 连接用途，而非用户任意输入的名称 / Connection purpose, not an arbitrary name.
    @param role_name config DSN 中的实际 PostgreSQL 用户名 / PostgreSQL username in the config DSN.
    @param dsn 原始 DSN，仅供数据库驱动使用 / Original DSN, only for database drivers.
    @param safe_conninfo 已剥离 secret 的 libpq conninfo / Secret-free libpq conninfo.
    @param password config DSN 中的密码 / Password from the config DSN.
    """

    role: LoginRole
    role_name: str
    dsn: str = field(repr=False)
    safe_conninfo: str = field(repr=False)
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 保证登录身份完整且不会意外暴露 secret / Keep login identity complete and secret-safe.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 任一连接字段为空或密码包含 NUL 时抛出。
        / Raised when a connection field is empty or the password contains NUL.
        """
        if not isinstance(self.role, LoginRole):
            raise DbctlConfigurationError("数据库登录身份的 role 无效。")
        for label, value in (
            ("role_name", self.role_name),
            ("dsn", self.dsn),
            ("safe_conninfo", self.safe_conninfo),
            ("password", self.password),
        ):
            if not isinstance(value, str) or not value or "\x00" in value:
                raise DbctlConfigurationError(f"数据库登录身份的 {label} 无效。")
