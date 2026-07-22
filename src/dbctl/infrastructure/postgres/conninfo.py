"""@brief PostgreSQL 连接身份的唯一解析边界 / Canonical PostgreSQL connection-identity boundary."""

from dataclasses import dataclass
from typing import Final
from urllib.parse import quote

from psycopg.conninfo import conninfo_to_dict, make_conninfo

from dbctl.application.errors import DbctlConfigurationError, safe_external_cause
from dbctl.domain.database import DatabaseLogin, DatabaseTarget
from dbctl.domain.names import DatabaseName, RoleName
from dbctl.domain.roles import LoginRole, Secret

_FORBIDDEN_CONNECTION_OPTIONS: Final[frozenset[str]] = frozenset(
    {"hostaddr", "options", "passfile", "service", "sslpassword"}
)
"""@brief 会绕过目标或凭证不变量的 libpq 选项 / libpq options bypassing target or credential invariants."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ParsedPostgresDsn:
    """@brief 已规范解析且默认不可泄密的 PostgreSQL DSN / Canonically parsed, secret-safe PostgreSQL DSN.

    @param target DSN 指向的精确数据库目标 / Exact database target addressed by the DSN.
    @param user DSN 中的 PostgreSQL role / PostgreSQL role carried by the DSN.
    @param dsn 仅供数据库 adapter 显式解封的原始 DSN / Raw DSN revealed only by database adapters.
    @param safe_conninfo 删除 password/sslpassword 后的 libpq conninfo。
    / libpq conninfo with password and sslpassword removed.
    @param password DSN 中的登录密码 / Login password carried by the DSN.
    """

    target: DatabaseTarget
    user: RoleName
    dsn: Secret[str]
    safe_conninfo: str
    password: Secret[str]


def parse_postgres_dsn(dsn: str) -> ParsedPostgresDsn:
    """@brief 解析完整且目标明确的 PostgreSQL URI / Parse a complete, target-explicit PostgreSQL URI.

    @param dsn PostgreSQL URI；必须显式包含 user、password、host、port 与 database。
    / PostgreSQL URI explicitly containing user, password, host, port, and database.
    @return 默认不可打印的结构化连接身份 / Structured connection identity that is secret-safe by default.
    @raise DbctlConfigurationError URI 缺字段、使用其他 scheme 或 libpq 无法解析时抛出。
    / Raised when fields are missing, the scheme differs, or libpq cannot parse the URI.

    @note 强制显式 target 可防止 libpq 环境默认值把 bootstrap、migration 与清理命令
    静默导向不同实例。/ Requiring an explicit target prevents libpq environment defaults from
    silently routing bootstrap, migration, and pruning to different instances.
    """

    if not isinstance(dsn, str) or not dsn.strip():
        raise DbctlConfigurationError("PostgreSQL DSN 必须是非空字符串。")
    if not dsn.casefold().startswith("postgresql://"):
        raise DbctlConfigurationError(
            "数据库 DSN 必须使用 postgresql:// URI，不能使用其他驱动或 libpq key=value。"
        )
    try:
        parameters = {
            key: str(value) for key, value in conninfo_to_dict(dsn).items() if value is not None
        }
        if _FORBIDDEN_CONNECTION_OPTIONS.intersection(parameters):
            raise ValueError("forbidden libpq routing or credential option")
        raw_password = parameters.pop("password", None)
        parameters.pop("sslpassword", None)
        raw_user = parameters.get("user")
        raw_host = parameters.get("host")
        raw_port = parameters.get("port")
        raw_database = parameters.get("dbname")
        if (
            raw_user is None
            or raw_password is None
            or raw_host is None
            or raw_port is None
            or raw_database is None
            or not all((raw_user, raw_password, raw_host, raw_port, raw_database))
            or "\r" in raw_password
            or "\n" in raw_password
        ):
            raise ValueError("incomplete PostgreSQL target")
        port = int(raw_port)
        target = DatabaseTarget(
            host=raw_host,
            port=port,
            database=DatabaseName(raw_database),
        )
        user = RoleName(raw_user)
        safe_conninfo = make_conninfo(**parameters)
    except (TypeError, ValueError) as error:
        raise DbctlConfigurationError(
            "PostgreSQL DSN 必须显式包含合法的 user、password、host、port 与 database。"
        ) from safe_external_cause(
            error,
            operation="解析并验证 PostgreSQL DSN",
        )
    except Exception as error:
        raise DbctlConfigurationError("PostgreSQL DSN 格式无效。") from safe_external_cause(
            error,
            operation="调用 libpq conninfo parser",
        )
    return ParsedPostgresDsn(
        target=target,
        user=user,
        dsn=Secret(dsn),
        safe_conninfo=safe_conninfo,
        password=Secret(raw_password),
    )


def parse_database_login[R: LoginRole](
    dsn: str,
    *,
    role: R,
    expected_role_name: RoleName,
    expected_target: DatabaseTarget,
) -> DatabaseLogin[R]:
    """@brief 将 DSN 绑定为一种用途唯一的强类型登录 / Bind a DSN to one purpose-specific typed login.

    @param dsn 私密运行配置中的 PostgreSQL URI / PostgreSQL URI from private runtime configuration.
    @param role 编译期和运行期共同约束的登录用途 / Login purpose constrained statically and at runtime.
    @param expected_role_name dbinit 目标状态声明的 role 名 / Role name declared by the dbinit target state.
    @param expected_target dbinit 声明的唯一数据库目标 / Sole database target declared by dbinit.
    @return 已验证用途、role 和 target 的登录身份 / Login identity validated for purpose, role, and target.
    @raise DbctlConfigurationError DSN role 或 target 与 dbinit 不一致时抛出。
    / Raised when the DSN role or target differs from dbinit.
    """

    parsed = parse_postgres_dsn(dsn)
    if parsed.user != expected_role_name:
        raise DbctlConfigurationError("数据库 DSN 用户名与 dbinit 声明的 role 不一致。")
    if parsed.target != expected_target:
        raise DbctlConfigurationError("数据库 DSN 的 host、port 或 database 与 dbinit 目标不一致。")
    return DatabaseLogin(
        role=role,
        role_name=parsed.user,
        target=parsed.target,
        dsn=parsed.dsn,
        safe_conninfo=parsed.safe_conninfo,
        password=parsed.password,
    )


def build_postgres_dsn(
    *,
    target: DatabaseTarget,
    role_name: RoleName,
    password: Secret[str],
) -> str:
    """@brief 构造与数据库目标严格绑定的 PostgreSQL URI / Build a PostgreSQL URI bound to an exact target.

    @param target 显式 host、port 与 database / Explicit host, port, and database.
    @param role_name PostgreSQL 登录 role / PostgreSQL login role.
    @param password 只在本函数内解封的随机密码 / Random password revealed only inside this function.
    @return 百分号编码且字段完整的 PostgreSQL URI / Complete percent-encoded PostgreSQL URI.
    """

    host = (
        f"[{target.host}]"
        if ":" in target.host and not target.host.startswith("[")
        else target.host
    )
    return (
        f"postgresql://{quote(role_name.value, safe='')}:"
        f"{quote(password.reveal(), safe='')}@{host}:{target.port}/"
        f"{quote(target.database.value, safe='')}"
    )


__all__ = [
    "ParsedPostgresDsn",
    "build_postgres_dsn",
    "parse_database_login",
    "parse_postgres_dsn",
]
