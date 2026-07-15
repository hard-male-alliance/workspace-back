"""@brief PostgreSQL 标识符与字面量安全处理 / Safe PostgreSQL identifier and literal handling."""

from __future__ import annotations

from typing import Final

from .errors import UnsafeIdentifierError

_POSTGRES_IDENTIFIER_MAX_BYTES: Final[int] = 63


def validate_postgres_identifier(value: str, *, kind: str = "标识符") -> str:
    """@brief 校验 PostgreSQL 标识符 / Validate a PostgreSQL identifier.

    @param value 待校验的名称 / Candidate name.
    @param kind 错误信息中使用的名称类别 / Name category used in diagnostics.
    @return 已校验的原始名称 / Validated original name.
    @raise UnsafeIdentifierError 名称为空、含 NUL 或超过 PostgreSQL 的 63 字节限制时抛出。
    / Raised when the name is empty, contains NUL, or exceeds PostgreSQL's 63-byte limit.

    @note 允许合法的带引号标识符字符；调用方必须继续通过
    :func:`quote_postgres_identifier` 输出 SQL。
    / Valid quoted-identifier characters are allowed; callers must still emit SQL
    through :func:`quote_postgres_identifier`.
    """
    if not isinstance(value, str):
        raise UnsafeIdentifierError(f"{kind}必须是字符串。")
    if not value or not value.strip():
        raise UnsafeIdentifierError(f"{kind}不能为空。")
    if "\x00" in value:
        raise UnsafeIdentifierError(f"{kind}不能包含 NUL 字符。")
    if len(value.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES:
        raise UnsafeIdentifierError(
            f"{kind}超过 PostgreSQL {_POSTGRES_IDENTIFIER_MAX_BYTES} 字节标识符限制。"
        )
    return value


def quote_postgres_identifier(value: str, *, kind: str = "标识符") -> str:
    """@brief 安全引用 PostgreSQL 标识符 / Safely quote a PostgreSQL identifier.

    @param value 已配置的标识符 / Configured identifier.
    @param kind 错误信息中使用的名称类别 / Name category used in diagnostics.
    @return 双引号包裹且内部双引号已转义的 SQL 标识符 / SQL identifier with escaped double quotes.
    @raise UnsafeIdentifierError 输入不满足 PostgreSQL 标识符约束时抛出。
    / Raised when input violates PostgreSQL identifier constraints.
    """
    identifier = validate_postgres_identifier(value, kind=kind)
    return '"' + identifier.replace('"', '""') + '"'


def quote_postgres_literal(value: str) -> str:
    """@brief 安全引用 PostgreSQL 文本字面量 / Safely quote a PostgreSQL text literal.

    @param value 待写入 SQL 的文本值 / Text value to place in SQL.
    @return 使用显式转义字符串（escape string）语法的 SQL 字面量。
    / SQL literal using explicit escape-string syntax.
    @raise UnsafeIdentifierError 值不是字符串或包含 NUL 时抛出。
    / Raised when the value is not a string or contains NUL.

    @note 此函数主要服务于本地 ``psql`` runner 的 stdin 脚本渲染；管理员
    DSN runner 会优先使用 psycopg 的安全字面量渲染。
    / This is principally for rendering stdin scripts in the local ``psql`` runner;
    the administrator-DSN runner prefers psycopg's safe literal rendering.
    """
    if not isinstance(value, str):
        raise UnsafeIdentifierError("PostgreSQL 文本字面量必须是字符串。")
    if "\x00" in value:
        raise UnsafeIdentifierError("PostgreSQL 文本字面量不能包含 NUL 字符。")
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return "E'" + escaped + "'"
