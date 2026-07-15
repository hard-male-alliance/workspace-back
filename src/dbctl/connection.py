"""@brief PostgreSQL DSN 的脱敏解析 / Redacted parsing of PostgreSQL DSNs."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, unquote, urlsplit

from .errors import DbctlConfigurationError


@dataclass(frozen=True, slots=True)
class ParsedPostgresDsn:
    """@brief 已去除密码的 PostgreSQL 连接信息 / PostgreSQL connection information with password removed.

    @param safe_conninfo 可安全传给 ``psql --dbname`` 的 conninfo；不含 password。
    / Conninfo safe for ``psql --dbname``; it contains no password.
    @param user DSN 声明的数据库用户名；未声明时为 ``None``。
    / Database user declared by the DSN, or ``None`` when unspecified.
    @param password 原始 DSN 密码，仅留在内存中且 repr 隐藏。
    / Original DSN password, retained only in memory and hidden from repr.
    """

    safe_conninfo: str = field(repr=False)
    user: str | None
    password: str | None = field(repr=False)


def parse_postgres_dsn(dsn: str) -> ParsedPostgresDsn:
    """@brief 解析 PostgreSQL DSN 并剥离 password / Parse a PostgreSQL DSN and remove password.

    @param dsn 原始 PostgreSQL URI 或 libpq conninfo / Raw PostgreSQL URI or libpq conninfo.
    @return 不含 password 的连接信息与仅内存保存的密码 / Password-free connection information and in-memory password.
    @raise DbctlConfigurationError DSN 无效时抛出，错误信息不回显原始 DSN。
    / Raised when the DSN is invalid; the error does not echo the raw DSN.
    @note psycopg 不可用时会使用内置的受限 URI/libpq conninfo 解析器；实际管理员
    连接仍由 bootstrap runner 单独要求 psycopg。
    / When psycopg is unavailable, a constrained built-in URI/libpq conninfo parser is used;
    actual administrator connections still require psycopg separately in the bootstrap runner.
    """
    if not isinstance(dsn, str) or not dsn.strip():
        raise DbctlConfigurationError("PostgreSQL DSN 必须是非空字符串。")
    try:
        from psycopg.conninfo import conninfo_to_dict
    except ImportError:
        return _parse_postgres_dsn_without_psycopg(dsn)

    try:
        parameters = {
            key: str(value)
            for key, value in conninfo_to_dict(dsn).items()
            if value is not None
        }
        password = _pop_password_parameter(parameters)
        _remove_non_login_password_parameters(parameters)
        user = parameters.get("user")
        safe_conninfo = _make_safe_conninfo(parameters)
    except Exception as error:
        raise DbctlConfigurationError("PostgreSQL DSN 格式无效。") from error

    normalized_user = user if isinstance(user, str) and user else None
    normalized_password = password if isinstance(password, str) and password else None
    return ParsedPostgresDsn(
        safe_conninfo=safe_conninfo,
        user=normalized_user,
        password=normalized_password,
    )


_CONNINFO_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _parse_postgres_dsn_without_psycopg(dsn: str) -> ParsedPostgresDsn:
    """@brief 在无 psycopg 时解析常见 PostgreSQL DSN / Parse common PostgreSQL DSNs without psycopg.

    @param dsn 原始 URI 或 libpq conninfo / Raw URI or libpq conninfo.
    @return 已剥离 password 的连接信息 / Connection information with password stripped.
    @raise DbctlConfigurationError URI 或 conninfo 不合法时抛出，且不回显原文。
    / Raised for invalid URI or conninfo without echoing original text.
    """
    if "://" in dsn:
        parameters = _parse_postgres_uri(dsn)
    else:
        parameters = _parse_libpq_conninfo(dsn)
    password = _pop_password_parameter(parameters)
    user = parameters.get("user")
    return ParsedPostgresDsn(
        safe_conninfo=_make_safe_conninfo(parameters),
        user=user if isinstance(user, str) and user else None,
        password=password if isinstance(password, str) and password else None,
    )


def _parse_postgres_uri(dsn: str) -> dict[str, str]:
    """@brief 解析 PostgreSQL URI / Parse a PostgreSQL URI.

    @param dsn 原始 PostgreSQL URI / Raw PostgreSQL URI.
    @return libpq 连接参数字典 / libpq connection-parameter dictionary.
    @raise DbctlConfigurationError URI 结构或端口不合法时抛出。
    / Raised when URI structure or port is invalid.
    """
    try:
        parsed = urlsplit(dsn)
        if parsed.scheme.casefold() not in {"postgres", "postgresql"}:
            raise ValueError("unsupported scheme")
        port = parsed.port
    except (ValueError, UnicodeError) as error:
        raise DbctlConfigurationError("PostgreSQL DSN 格式无效。") from error
    if parsed.fragment:
        raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")

    parameters: dict[str, str] = {}
    if parsed.hostname:
        parameters["host"] = parsed.hostname
    if port is not None:
        parameters["port"] = str(port)
    if parsed.username is not None:
        parameters["user"] = unquote(parsed.username)
    if parsed.password is not None:
        parameters["password"] = unquote(parsed.password)
    database_name = unquote(parsed.path[1:]) if parsed.path.startswith("/") else ""
    if database_name:
        parameters["dbname"] = database_name
    try:
        query_pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as error:
        raise DbctlConfigurationError("PostgreSQL DSN 格式无效。") from error
    for key, value in query_pairs:
        if not _CONNINFO_KEY_PATTERN.fullmatch(key):
            raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")
        parameters[key.casefold()] = value
    return parameters


def _parse_libpq_conninfo(dsn: str) -> dict[str, str]:
    """@brief 解析 libpq key=value conninfo / Parse libpq key=value conninfo.

    @param dsn 原始 libpq conninfo / Raw libpq conninfo.
    @return 连接参数字典 / Connection-parameter dictionary.
    @raise DbctlConfigurationError conninfo 语法不合法时抛出。
    / Raised when conninfo syntax is invalid.
    """
    parameters: dict[str, str] = {}
    index = 0
    length = len(dsn)
    while index < length:
        while index < length and dsn[index].isspace():
            index += 1
        if index == length:
            break
        key_start = index
        while index < length and (dsn[index].isalnum() or dsn[index] == "_"):
            index += 1
        key = dsn[key_start:index]
        if not key or not _CONNINFO_KEY_PATTERN.fullmatch(key):
            raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")
        if index >= length or dsn[index] != "=":
            raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")
        index += 1
        while index < length and dsn[index].isspace():
            index += 1
        value, index = _parse_conninfo_value(dsn, index)
        parameters[key.casefold()] = value
    if not parameters:
        raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")
    return parameters


def _parse_conninfo_value(dsn: str, index: int) -> tuple[str, int]:
    """@brief 解析一个 libpq conninfo 值 / Parse one libpq conninfo value.

    @param dsn 原始 conninfo / Raw conninfo.
    @param index 值开始位置 / Start position of the value.
    @return ``(value, next_index)`` / ``(value, next_index)``.
    @raise DbctlConfigurationError 引号或转义不闭合时抛出。
    / Raised when quotes or escapes are unterminated.
    """
    if index >= len(dsn):
        return "", index
    quoted = dsn[index] == "'"
    if quoted:
        index += 1
    value: list[str] = []
    while index < len(dsn):
        character = dsn[index]
        if character == "\\":
            index += 1
            if index >= len(dsn):
                raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")
            value.append(dsn[index])
            index += 1
            continue
        if quoted:
            if character == "'":
                return "".join(value), index + 1
            value.append(character)
            index += 1
            continue
        if character.isspace():
            return "".join(value), index
        value.append(character)
        index += 1
    if quoted:
        raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")
    return "".join(value), index


def _make_safe_conninfo(parameters: dict[str, str]) -> str:
    """@brief 构造不含 password 的 libpq conninfo / Build password-free libpq conninfo.

    @param parameters 已解析且已剥离 password 的参数 / Parsed parameters with password stripped.
    @return 可安全放入 ``psql --dbname`` 的 conninfo / Conninfo safe for ``psql --dbname``.
    """
    parts: list[str] = []
    for key, value in parameters.items():
        if _is_password_parameter(key):
            continue
        if not _CONNINFO_KEY_PATTERN.fullmatch(key):
            raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")
        if not isinstance(value, str):
            raise DbctlConfigurationError("PostgreSQL DSN 格式无效。")
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        parts.append(f"{key}='{escaped}'")
    return " ".join(parts)


def _pop_password_parameter(parameters: dict[str, str]) -> str | None:
    """@brief 从连接参数中移除大小写无关的 password / Remove case-insensitive password from connection parameters.

    @param parameters 可变连接参数字典 / Mutable connection-parameter dictionary.
    @return 被移除的密码或 ``None`` / Removed password or ``None``.
    """
    for key in tuple(parameters):
        if key.casefold() == "password":
            value = parameters.pop(key)
            return value if isinstance(value, str) else None
    return None


def _remove_non_login_password_parameters(parameters: dict[str, str]) -> None:
    """@brief 移除不应进入 psql argv 的其他密码参数 / Remove other password parameters that must not enter psql argv.

    @param parameters 可变连接参数字典 / Mutable connection-parameter dictionary.
    @return 无返回值 / No return value.
    """
    for key in tuple(parameters):
        if _is_password_parameter(key):
            parameters.pop(key)


def _is_password_parameter(key: str) -> bool:
    """@brief 判断 libpq 参数是否可承载密码 / Determine whether a libpq parameter can carry a password.

    @param key libpq 连接参数名 / libpq connection parameter name.
    @return 参数名代表 password/sslpassword 时为 ``True``。
    / ``True`` when the parameter name represents password/sslpassword.
    """
    return key.casefold() in {"password", "sslpassword"}
