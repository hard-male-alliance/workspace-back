"""@brief PostgreSQL 客户端进程环境边界 / PostgreSQL client-process environment boundary."""

from collections.abc import Mapping


def sanitized_libpq_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """@brief 移除所有可改变 libpq 连接的 PG* 环境变量 / Remove every PG* libpq override.

    @param environ 调用进程环境 / Calling-process environment.
    @return 不含 PostgreSQL 隐式路由或凭证覆盖的副本。
    / Copy without implicit PostgreSQL routing or credential overrides.

    @note adapter 随后只添加本次租约的 ``PGPASSFILE``；host、port、database 与 user
    均来自 argv/conninfo 中已验证的领域目标。/ Adapters subsequently add only the current
    lease's ``PGPASSFILE``; host, port, database, and user come from the validated domain target.
    """

    return {name: value for name, value in environ.items() if not name.upper().startswith("PG")}


__all__ = ["sanitized_libpq_environment"]
