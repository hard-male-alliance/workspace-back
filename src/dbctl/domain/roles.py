"""@brief PostgreSQL 角色与秘密值模型 / PostgreSQL role and secret-value models."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Final, assert_never

from .errors import DomainError, InvalidRoleSetError
from .names import RoleName

_RESERVED_ROLE_NAMES: Final[frozenset[str]] = frozenset(
    {"postgres", "public", "current_user", "current_role", "session_user"}
)
"""@brief 禁止作为业务角色的系统或伪角色名 / Reserved system and pseudo-role names."""


class DatabaseRole(StrEnum):
    """@brief bootstrap 管理的全部数据库角色 / All database roles managed by bootstrap."""

    OWNER = "owner"
    MIGRATOR = "migrator"
    APP = "app"
    DASHBOARD = "dashboard"


class LoginRole(StrEnum):
    """@brief 可建立数据库连接的角色 / Database roles permitted to log in."""

    MIGRATOR = "migrator"
    APP = "app"
    DASHBOARD = "dashboard"

    def as_database_role(self) -> DatabaseRole:
        """@brief 转换为完整数据库角色类别 / Convert to the full database-role category.

        @return 同一业务身份的 DatabaseRole / DatabaseRole for the same business identity.
        """
        return DatabaseRole(self.value)


@dataclass(frozen=True, slots=True, repr=False)
class Secret[T]:
    """@brief 避免秘密值意外进入日志的轻量封装 / Lightweight accidental-leak guard.

    @param value 仅在受控适配器边界揭示的秘密值 / Secret revealed only at a controlled adapter boundary.
    @note 本类型降低 ``repr`` 和 ``str`` 泄漏风险，但不提供内存加密。
    / This type reduces ``repr`` and ``str`` leakage; it does not encrypt process memory.
    """

    _value: T

    def __init__(self, value: T) -> None:
        """@brief 保存秘密并拒绝明显无效的文本 / Store a secret and reject invalid text.

        @param value 待保护的值 / Value to protect.
        @return 无返回值 / No return value.
        @raise DomainError 文本 secret 为空或包含 NUL 时抛出。
        / Raised when a textual secret is empty or contains NUL.
        """
        if isinstance(value, str) and (not value or "\x00" in value):
            raise DomainError("文本 secret 不能为空或包含 NUL 字符。")
        object.__setattr__(self, "_value", value)

    def reveal(self) -> T:
        """@brief 在受控边界显式揭示秘密 / Explicitly reveal the secret at a controlled boundary.

        @return 原始秘密值 / Original secret value.
        """
        return self._value

    def __repr__(self) -> str:
        """@brief 返回不含秘密的调试表示 / Return a secret-free debug representation.

        @return 固定脱敏文本 / Fixed redacted text.
        """
        return "Secret(<redacted>)"

    def __str__(self) -> str:
        """@brief 返回不含秘密的显示文本 / Return secret-free display text.

        @return 固定脱敏文本 / Fixed redacted text.
        """
        return "<redacted>"


@dataclass(frozen=True, slots=True)
class RoleSet:
    """@brief 四种隔离职责的完整角色集合 / Complete four-role separation-of-duties set.

    @param owner 不可登录的对象所有者 / Non-login object owner.
    @param migrator 可显式切换为 owner 的迁移身份 / Migration identity allowed to set owner.
    @param application 运行时应用 DML 身份 / Runtime application DML identity.
    @param dashboard observability 只读身份 / Read-only observability identity.
    """

    owner: RoleName
    migrator: RoleName
    application: RoleName
    dashboard: RoleName

    def __post_init__(self) -> None:
        """@brief 校验唯一性与系统角色隔离 / Validate uniqueness and system-role isolation.

        @return 无返回值 / No return value.
        @raise InvalidRoleSetError 角色重复或使用系统名称时抛出。
        / Raised when roles collide or use a system name.
        """
        names = (self.owner, self.migrator, self.application, self.dashboard)
        if any(not isinstance(name, RoleName) for name in names):
            raise InvalidRoleSetError("RoleSet 的成员必须是 RoleName。")
        values = tuple(name.value for name in names)
        if len(set(values)) != len(values):
            raise InvalidRoleSetError("owner、migrator、application 与 dashboard 必须互不相同。")
        if any(
            value.casefold() in _RESERVED_ROLE_NAMES or value.casefold().startswith("pg_")
            for value in values
        ):
            raise InvalidRoleSetError("业务数据库角色不能使用 PostgreSQL 系统或伪角色名称。")

    def name_for(self, role: DatabaseRole | LoginRole) -> RoleName:
        """@brief 返回领域角色对应的 PostgreSQL 名称 / Return the PostgreSQL name for a role.

        @param role 数据库职责类别 / Database responsibility category.
        @return 与职责绑定的 RoleName / RoleName bound to the responsibility.
        """
        database_role = role.as_database_role() if isinstance(role, LoginRole) else role
        if database_role is DatabaseRole.OWNER:
            return self.owner
        if database_role is DatabaseRole.MIGRATOR:
            return self.migrator
        if database_role is DatabaseRole.APP:
            return self.application
        if database_role is DatabaseRole.DASHBOARD:
            return self.dashboard
        assert_never(database_role)
