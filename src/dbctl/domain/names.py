"""@brief PostgreSQL 名称值对象 / PostgreSQL name value objects."""

import re
from dataclasses import dataclass
from typing import Final

from .errors import InvalidNameError

_POSTGRES_IDENTIFIER_MAX_BYTES: Final[int] = 63
"""@brief PostgreSQL 标识符字节上限 / PostgreSQL identifier byte limit."""

_PORTABLE_IDENTIFIER_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief 跨工具一致的可移植标识符白名单 / Portable identifier allow-list."""


def _validate_identifier(value: str, *, kind: str) -> str:
    """@brief 校验并返回 PostgreSQL 标识符 / Validate and return a PostgreSQL identifier.

    @param value 待校验名称 / Candidate name.
    @param kind 错误消息中的名称种类 / Name kind used in diagnostics.
    @return 原样保留的合法名称 / The valid name unchanged.
    @raise InvalidNameError 名称不是可移植 PostgreSQL 标识符时抛出。
    / Raised when the name is not a portable PostgreSQL identifier.
    """
    if not isinstance(value, str):
        raise InvalidNameError(f"{kind}必须是字符串。")
    if not value or not value.strip():
        raise InvalidNameError(f"{kind}不能为空。")
    if "\x00" in value:
        raise InvalidNameError(f"{kind}不能包含 NUL 字符。")
    if not _PORTABLE_IDENTIFIER_PATTERN.fullmatch(value):
        raise InvalidNameError(f"{kind}只能包含 ASCII 字母、数字和下划线，且不能以数字开头。")
    if len(value.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES:
        raise InvalidNameError(
            f"{kind}超过 PostgreSQL {_POSTGRES_IDENTIFIER_MAX_BYTES} 字节标识符限制。"
        )
    return value


@dataclass(frozen=True, slots=True)
class DatabaseName:
    """@brief 已验证的 PostgreSQL 数据库名 / Validated PostgreSQL database name.

    @param value 可移植且可安全引用的数据库标识符 / Portable database identifier.
    """

    value: str

    def __post_init__(self) -> None:
        """@brief 建立数据库名不变量 / Establish database-name invariants.

        @return 无返回值 / No return value.
        """
        object.__setattr__(self, "value", _validate_identifier(self.value, kind="数据库名"))

    def __str__(self) -> str:
        """@brief 返回数据库名文本 / Return the database-name text.

        @return PostgreSQL 数据库标识符 / PostgreSQL database identifier.
        """
        return self.value


@dataclass(frozen=True, slots=True)
class RoleName:
    """@brief 已验证的 PostgreSQL 角色名 / Validated PostgreSQL role name.

    @param value 可移植且可安全引用的角色标识符 / Portable role identifier.
    """

    value: str

    def __post_init__(self) -> None:
        """@brief 建立角色名不变量 / Establish role-name invariants.

        @return 无返回值 / No return value.
        """
        object.__setattr__(self, "value", _validate_identifier(self.value, kind="数据库 role"))

    def __str__(self) -> str:
        """@brief 返回角色名文本 / Return the role-name text.

        @return PostgreSQL 角色标识符 / PostgreSQL role identifier.
        """
        return self.value


@dataclass(frozen=True, slots=True)
class SchemaName:
    """@brief 已验证的 PostgreSQL schema 名 / Validated PostgreSQL schema name.

    @param value 可移植且可安全引用的 schema 标识符 / Portable schema identifier.
    """

    value: str

    def __post_init__(self) -> None:
        """@brief 建立 schema 名不变量 / Establish schema-name invariants.

        @return 无返回值 / No return value.
        """
        object.__setattr__(self, "value", _validate_identifier(self.value, kind="schema 名"))

    def __str__(self) -> str:
        """@brief 返回 schema 名文本 / Return the schema-name text.

        @return PostgreSQL schema 标识符 / PostgreSQL schema identifier.
        """
        return self.value
