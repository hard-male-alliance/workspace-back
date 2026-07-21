"""@brief Alembic 异步迁移环境 / Alembic asynchronous migration environment.

Alembic 的 migration machinery 仍是同步 API；此文件只在 AsyncConnection 上通过
``run_sync`` 建立清晰的同步边界，绝不把它误当成“全异步 migration API”。
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from alembic import context
from sqlalchemy import Connection, pool, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

config = context.config
target_metadata = None

_VERSION_TABLE_SCHEMA = "identity"
"""@brief Alembic 版本表的 owner 管控 schema / Owner-controlled schema for Alembic versions."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief 经 dbctl 验证的 role 标识符模式 / dbctl-validated role identifier pattern."""

_MIGRATION_LOCK_KEYS = (1_094_532_435, 1)
"""@brief 本应用 migration 的事务级 advisory lock 键 / Transaction advisory-lock keys."""


def _migration_url() -> str:
    """@brief 获取迁移专用 DSN / Get the migration-specific DSN.

    @return 规范化后的 ``postgresql+asyncpg`` DSN。
    @raise RuntimeError 未配置 migrator DSN 时抛出。

    @note 只接受 dbctl 通过 Alembic attributes 注入的 config.jsonc 凭证；不读取环境变量，
    也不经过 ConfigParser 的百分号插值。
    """
    raw_url = config.attributes.get("aiws.migration_dsn")
    if not isinstance(raw_url, str) or not raw_url:
        raise RuntimeError("missing dbctl-provided config migration identity")
    url: URL = make_url(raw_url)
    if url.get_backend_name() != "postgresql":
        raise RuntimeError("dbctl migration identity is not PostgreSQL")
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+asyncpg")
    if url.drivername != "postgresql+asyncpg":
        raise RuntimeError("dbctl migration identity requires the asyncpg driver")
    return url.render_as_string(hide_password=False)


def _owner_role_identifier() -> str:
    """@brief 返回经 dbctl 校验的 owner SQL identifier / Return the dbctl-validated owner identifier.

    @return 双引号引用的 PostgreSQL role identifier。
    @raise RuntimeError dbctl 未在内存 Alembic Config 提供 owner role 时抛出。

    @note 迁移绝不以 migrator 身份在 ``public`` 创建 ``alembic_version``。角色值只由
    dbctl 的严格标识符校验路径传入；直接调用 Alembic 若缺少该配置会 fail closed。
    """
    role = config.get_main_option("aiws.owner_role")
    if not role:
        raise RuntimeError("missing dbctl-provided aiws.owner_role for Alembic migration")
    if not _ROLE_IDENTIFIER_PATTERN.fullmatch(role):
        raise RuntimeError("invalid dbctl-provided aiws.owner_role for Alembic migration")
    return '"' + role.replace('"', '""') + '"'


def run_migrations_offline() -> None:
    """@brief 生成不连接数据库的 SQL / Generate SQL without connecting to the database.

    @return 无返回值。
    """
    context.configure(
        url=_migration_url(),
        target_metadata=target_metadata,
        version_table_schema=_VERSION_TABLE_SCHEMA,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.execute(f"SET LOCAL ROLE {_owner_role_identifier()}")
        context.execute(f"CREATE SCHEMA IF NOT EXISTS {_VERSION_TABLE_SCHEMA}")
        context.run_migrations()


def _run_sync_migrations(connection: Connection) -> None:
    """@brief 在同步 Connection 上运行 Alembic 核心 / Run Alembic core on a sync Connection.

    @param connection 由 ``AsyncConnection.run_sync`` 适配得到的同步连接。
    @return 无返回值。

    @note 这是唯一允许 Alembic 同步 machinery 运行的边界。
    """
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        version_table_schema=_VERSION_TABLE_SCHEMA,
        include_schemas=True,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        connection.execute(text("SET LOCAL lock_timeout = '30s'"))
        connection.execute(
            text(
                "SELECT pg_advisory_xact_lock("
                f"{_MIGRATION_LOCK_KEYS[0]}, {_MIGRATION_LOCK_KEYS[1]})"
            )
        )
        connection.execute(text(f"SET LOCAL ROLE {_owner_role_identifier()}"))
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_VERSION_TABLE_SCHEMA}"))
        context.run_migrations()


async def _run_async_migrations() -> None:
    """@brief 创建临时 async Engine 并桥接同步迁移 / Create a temporary async engine and bridge sync migrations.

    @return 无返回值。

    @note ``NullPool`` 防止迁移进程留下长期运行时连接池。
    """
    connectable: AsyncEngine = create_async_engine(_migration_url(), poolclass=pool.NullPool)
    try:
        async with connectable.connect() as connection:
            await connection.run_sync(_run_sync_migrations)
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    """@brief 以异步驱动执行在线迁移 / Run online migrations using an async driver.

    @return 无返回值。

    @note 编程式调用可在 ``config.attributes['connection']`` 注入同步连接；标准
    CLI 路径则由 ``asyncio.run`` 驱动临时异步 Engine。
    """
    supplied_connection: Any = config.attributes.get("connection")
    if supplied_connection is not None:
        _run_sync_migrations(supplied_connection)
        return
    asyncio.run(_run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
