"""@brief PostgreSQL DSN 的规范解析与脱敏 / Canonical parsing and redaction of PostgreSQL DSNs."""

from __future__ import annotations

from dataclasses import dataclass, field

from psycopg.conninfo import conninfo_to_dict, make_conninfo

from .errors import DbctlConfigurationError


@dataclass(frozen=True, slots=True)
class ParsedPostgresDsn:
    """@brief 已剥离密码的 PostgreSQL 连接信息 / PostgreSQL connection info with password removed.

    @param safe_conninfo 可安全传给 psql 的 libpq conninfo / Password-free conninfo safe for psql.
    @param user DSN 声明的数据库用户名 / Database user declared by the DSN.
    @param password 原始 DSN 密码，仅留在内存中 / Original password retained only in memory.
    """

    safe_conninfo: str = field(repr=False)
    user: str | None
    password: str | None = field(repr=False)


def parse_postgres_dsn(dsn: str) -> ParsedPostgresDsn:
    """@brief 用项目必需的 psycopg 解析并剥离密码 / Parse and redact with required psycopg.

    @param dsn PostgreSQL URI 或 libpq conninfo / PostgreSQL URI or libpq conninfo.
    @return 不含 password/sslpassword 的连接信息与内存密码 / Redacted conninfo and memory password.
    @raise DbctlConfigurationError DSN 无效时抛出，且不回显输入 / Raised without echoing invalid input.

    @note dbctl 配置边界进一步要求 URI，以与 SQLAlchemy/Alembic 驱动契约一致；本函数保留
    libpq conninfo 支持供纯 PostgreSQL 工具代码使用。
    / The dbctl config boundary additionally requires a URI for SQLAlchemy/Alembic consistency;
    this parser retains libpq conninfo support for PostgreSQL-only utility code.
    """
    if not isinstance(dsn, str) or not dsn.strip():
        raise DbctlConfigurationError("PostgreSQL DSN 必须是非空字符串。")
    try:
        parameters = {
            key: str(value) for key, value in conninfo_to_dict(dsn).items() if value is not None
        }
        password = parameters.pop("password", None)
        parameters.pop("sslpassword", None)
        user = parameters.get("user")
        safe_conninfo = make_conninfo(**parameters)
    except Exception as error:
        raise DbctlConfigurationError("PostgreSQL DSN 格式无效。") from error
    return ParsedPostgresDsn(
        safe_conninfo=safe_conninfo,
        user=user if user else None,
        password=password if password else None,
    )
