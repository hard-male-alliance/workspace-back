"""@brief PostgreSQL 异步连接与短事务边界 / Async PostgreSQL connection and short transaction boundaries.

本模块刻意不调用 Alembic。数据库结构变更必须由 dbctl 或运维流程显式执行，
避免应用启动时发生隐式 DDL（Data Definition Language）。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import URL, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from workspace_shared.tenancy import ActorScope


@dataclass(frozen=True, slots=True)
class AsyncDatabaseOptions:
    """@brief 异步 PostgreSQL 连接池配置 / Async PostgreSQL pool configuration.

    @param dsn PostgreSQL DSN；可省略 ``+asyncpg`` 驱动后缀。
    @param pool_size 常驻连接数（pool size）。
    @param max_overflow 忙碌时可临时创建的额外连接数。
    @param pool_timeout_s 等待空闲连接的最长秒数。
    @param connect_timeout_s 新建连接的最长秒数。
    @param statement_timeout_ms 每个后端短事务中 SQL 的最长执行时间。
    @param lock_timeout_ms 每个后端短事务中等待锁的最长时间。
    @param echo 是否输出 SQLAlchemy SQL 日志，仅供本地诊断。
    """

    dsn: str
    pool_size: int = 10
    max_overflow: int = 10
    pool_timeout_s: float = 30.0
    connect_timeout_s: float = 3.0
    statement_timeout_ms: int = 30_000
    lock_timeout_ms: int = 3_000
    echo: bool = False

    def __post_init__(self) -> None:
        """@brief 校验连接池参数 / Validate pool options.

        @raise ValueError 参数为空、驱动不兼容或数值非法时抛出。
        """
        if not self.dsn.strip():
            raise ValueError("database dsn must not be empty")
        if self.pool_size < 1:
            raise ValueError("pool_size must be at least one")
        if self.max_overflow < 0:
            raise ValueError("max_overflow must not be negative")
        if self.pool_timeout_s <= 0 or self.connect_timeout_s <= 0:
            raise ValueError("database timeouts must be positive")
        if (
            isinstance(self.statement_timeout_ms, bool)
            or not isinstance(self.statement_timeout_ms, int)
            or self.statement_timeout_ms <= 0
            or isinstance(self.lock_timeout_ms, bool)
            or not isinstance(self.lock_timeout_ms, int)
            or self.lock_timeout_ms <= 0
            or self.lock_timeout_ms > self.statement_timeout_ms
        ):
            raise ValueError("database lock and statement timeouts must be positive and ordered")


def normalize_asyncpg_dsn(dsn: str) -> str:
    """@brief 规范化为 SQLAlchemy asyncpg DSN / Normalize a SQLAlchemy asyncpg DSN.

    @param dsn 运行时配置提供的 PostgreSQL DSN。
    @return 带 ``postgresql+asyncpg`` 方言的安全 DSN 字符串。
    @raise ValueError DSN 不是 PostgreSQL 或指定了不兼容驱动时抛出。

    @note 不记录 DSN，防止密码进入日志。
    """
    url: URL = make_url(dsn)
    if url.get_backend_name() != "postgresql":
        raise ValueError("the persistence layer requires a PostgreSQL DSN")
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+asyncpg")
    if url.drivername != "postgresql+asyncpg":
        raise ValueError("the runtime persistence layer requires the asyncpg driver")
    return url.render_as_string(hide_password=False)


def create_session_factory(options: AsyncDatabaseOptions) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """@brief 创建异步 Engine 与 Session 工厂 / Create async engine and session factory.

    @param options 已校验的连接池配置。
    @return ``(AsyncEngine, async_sessionmaker)`` 二元组。

    @note 工厂每次调用都会产生独立的 ``AsyncSession``；绝不共享可变 Session。
    """
    engine = create_async_engine(
        normalize_asyncpg_dsn(options.dsn),
        echo=options.echo,
        pool_pre_ping=True,
        pool_size=options.pool_size,
        max_overflow=options.max_overflow,
        pool_timeout=options.pool_timeout_s,
        connect_args={"timeout": options.connect_timeout_s},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    return engine, sessions


async def install_tenant_scope(
    session: AsyncSession,
    scope: ActorScope,
    *,
    statement_timeout_ms: int = 30_000,
    lock_timeout_ms: int = 3_000,
) -> None:
    """@brief 在当前事务安装 RLS 范围 / Install Row-Level Security scope in this transaction.

    @param session 当前短生命周期异步 Session。
    @param scope 不可为空的 actor、workspace 与资源所有者范围。
    @param statement_timeout_ms 当前事务 SQL 时间上限 / Per-transaction SQL time limit.
    @param lock_timeout_ms 当前事务锁等待上限 / Per-transaction lock-wait limit.
    @return 无返回值。

    @note ``is_local=true`` 使 GUC（Grand Unified Configuration）随事务结束清除，
    防止连接池复用时泄漏上一个租户的上下文。
    """
    await session.execute(
        text(
            """
            SELECT
                set_config('app.actor_id', :actor_id, true),
                set_config('app.workspace_id', :workspace_id, true),
                set_config('app.resource_owner_id', :resource_owner_id, true),
                set_config('statement_timeout', CAST(:statement_timeout_ms AS text), true),
                set_config('lock_timeout', CAST(:lock_timeout_ms AS text), true)
            """
        ),
        {
            "actor_id": scope.actor_id,
            "workspace_id": scope.workspace_id,
            "resource_owner_id": scope.resource_owner_id,
            "statement_timeout_ms": str(statement_timeout_ms),
            "lock_timeout_ms": str(lock_timeout_ms),
        },
    )


class AsyncDatabase:
    """@brief 后端 PostgreSQL 资源所有者 / Backend PostgreSQL resource owner.

    @param options 异步连接池配置。

    @note 此类只管理连接池和短事务（short transaction），不执行 migration；每个
    协程任务必须分别进入 ``transaction`` 或 ``read_session``，不能跨任务共享
    ``AsyncSession``。
    """

    def __init__(self, options: AsyncDatabaseOptions) -> None:
        """@brief 创建连接池与 Session 工厂 / Create pool and session factory.

        @param options 已校验的连接池配置。
        @return 新建的 AsyncDatabase 实例。
        """
        self._engine, self._session_factory = create_session_factory(options)
        self._statement_timeout_ms = options.statement_timeout_ms
        self._lock_timeout_ms = options.lock_timeout_ms

    @property
    def engine(self) -> AsyncEngine:
        """@brief 返回受管理的异步 Engine / Return the managed async engine.

        @return 仅供健康检查或受控基础设施使用的 Engine。
        """
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """@brief 返回 Session 工厂 / Return the session factory.

        @return 创建独立 AsyncSession 的工厂。

        @note 应用代码优先使用 ``transaction`` 或 ``read_session``，从而确保 RLS
        范围已设置。
        """
        return self._session_factory

    @asynccontextmanager
    async def transaction(self, scope: ActorScope) -> AsyncIterator[AsyncSession]:
        """@brief 打开一个租户受限的读写短事务 / Open a tenant-scoped read-write transaction.

        @param scope 该操作的 actor/workspace/resource-owner 边界。
        @return 在上下文内有效的 AsyncSession；正常退出时提交，异常时回滚。

        @note 一个 ``asyncio.Task`` 应只使用一次此上下文；不要把 yield 出去的
        Session 传给并发子任务。
        """
        async with self._session_factory() as session:
            async with session.begin():
                await install_tenant_scope(
                    session,
                    scope,
                    statement_timeout_ms=self._statement_timeout_ms,
                    lock_timeout_ms=self._lock_timeout_ms,
                )
                yield session

    @asynccontextmanager
    async def read_session(self, scope: ActorScope) -> AsyncIterator[AsyncSession]:
        """@brief 打开一个租户受限的只读短事务 / Open a tenant-scoped read-only transaction.

        @param scope 该读取的 actor/workspace/resource-owner 边界。
        @return 在上下文内有效的只读 AsyncSession。

        @note PostgreSQL 的 ``SET TRANSACTION READ ONLY`` 是第二道保护；RLS 仍由
        migration 创建的策略执行。
        """
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                await install_tenant_scope(
                    session,
                    scope,
                    statement_timeout_ms=self._statement_timeout_ms,
                    lock_timeout_ms=self._lock_timeout_ms,
                )
                yield session

    async def aclose(self) -> None:
        """@brief 关闭连接池 / Dispose the connection pool.

        @return 无返回值。

        @note 应由后端 composition root 在 graceful shutdown 中调用。
        """
        await self._engine.dispose()


__all__ = [
    "AsyncDatabase",
    "AsyncDatabaseOptions",
    "create_session_factory",
    "install_tenant_scope",
    "normalize_asyncpg_dsn",
]
