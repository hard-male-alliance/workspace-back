"""@brief PostgreSQL 异步连接与短事务边界 / Async PostgreSQL connection and short transaction boundaries.

本模块刻意不调用 Alembic。数据库结构变更必须由 dbctl 或运维流程显式执行，
避免应用启动时发生隐式 DDL（Data Definition Language）。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from sqlalchemy import URL, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
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


@dataclass(frozen=True, slots=True)
class _AtomicEnvelopeBinding:
    """@brief 当前任务独占的外层连接 / Outer connection exclusively owned by one task.

    @param connection 外层事务连接 / Outer transaction connection.
    @param owner_task 创建信封的 asyncio Task / Asyncio task that created the envelope.
    """

    connection: AsyncConnection
    owner_task: object


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


def create_session_factory(
    options: AsyncDatabaseOptions,
) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
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
        self._envelope_binding: ContextVar[_AtomicEnvelopeBinding | None] = ContextVar(
            f"aiws_database_envelope_{id(self)}",
            default=None,
        )

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

    def new_session(self) -> AsyncSession:
        """@brief 创建独立或加入请求信封的 Session / Create an independent or envelope-joined Session.

        @return 未进入上下文的异步 Session / An async Session not yet entered as a context.

        @note 普通调用绑定 Engine 并拥有自己的事务；原子请求信封内则显式绑定同一
            ``AsyncConnection``，使用 ``create_savepoint`` 加入外部事务。每个 UoW 仍有
            独立 Session，避免跨协程并发共享可变 ``AsyncSession``。
            / Ordinary calls bind the Engine and own their transaction. Inside an atomic request
            envelope, the Session explicitly joins the same connection using ``create_savepoint``.
            Each UoW still gets a distinct Session, avoiding concurrent sharing of mutable state.
        """
        binding = self._envelope_binding.get()
        if binding is None:
            return self._session_factory()
        if asyncio.current_task() is not binding.owner_task:
            raise RuntimeError("atomic database envelope cannot be used by a child task")
        return AsyncSession(
            bind=binding.connection,
            expire_on_commit=False,
            autoflush=False,
            join_transaction_mode="create_savepoint",
        )

    @property
    def in_atomic_envelope(self) -> bool:
        """@brief 判断当前调用链是否处于原子信封 / Report whether this call path is enveloped.

        @return 当前上下文绑定外部事务时为真 / True when an external transaction is bound.
        """
        return self._envelope_binding.get() is not None

    @asynccontextmanager
    async def atomic_envelope(
        self,
        *,
        connection: AsyncConnection | None = None,
    ) -> AsyncIterator[None]:
        """@brief 打开可供多个短 UoW 加入的原子请求信封 / Open an atomic request envelope.

        @param connection 可选、已由 session-level coordinator 独占且当前无事务的连接 /
            Optional transaction-free connection exclusively held by a session-level coordinator.
        @return 上下文正常退出才提交的外部事务 / External transaction committed only on
            normal context exit.
        @raise RuntimeError 同一调用链嵌套信封时抛出 / Raised for a nested envelope in the
            same call path.

        @note 信封只允许串行调用 UoW；不得把继承该 ``ContextVar`` 的子任务并发运行。
            外层连接事务使幂等 claim、业务状态、transactional outbox 与 response receipt
            要么全部提交，要么全部回滚。内部 UoW 的 commit 仅释放其 SAVEPOINT。
            / UoWs inside the envelope must run serially, never in concurrently spawned child
            tasks inheriting this context. The outer connection transaction commits or rolls back
            the idempotency claim, domain state, transactional outbox, and response receipt as one.
        """
        if self._envelope_binding.get() is not None:
            raise RuntimeError("atomic database envelopes cannot be nested")
        owner_task = asyncio.current_task()
        if owner_task is None:
            raise RuntimeError("atomic database envelope requires an asyncio task")
        if connection is not None:
            async with self._atomic_envelope_on_connection(connection, owner_task):
                yield
            return
        async with self._engine.connect() as owned_connection:
            async with self._atomic_envelope_on_connection(owned_connection, owner_task):
                yield

    @asynccontextmanager
    async def coordination_lock(
        self,
        lock_key: int,
    ) -> AsyncIterator[AsyncConnection | None]:
        """@brief 尝试持有无事务的 PostgreSQL session advisory lock / Try a transaction-free PostgreSQL session advisory lock.

        @param lock_key 有符号 64 位 advisory-lock key / Signed 64-bit advisory-lock key.
        @return 成功时返回独占连接；竞争失败时返回 ``None`` / Exclusively held connection on
            success, or ``None`` on contention.
        @raise RuntimeError PostgreSQL 返回非布尔结果或释放失败时抛出 / Raised for an invalid
            PostgreSQL result or failed unlock.

        @note 获取与释放各自结束 SQLAlchemy 的隐式短事务；yield 期间连接持有的是
            session-level lock，不存在活动事务、行锁或 MVCC snapshot。该能力要求
            session-pooling/direct PostgreSQL，不能放在 transaction-pooling proxy 后面 /
            Acquisition and release each close SQLAlchemy's implicit short transaction. During
            ``yield`` only the session-level lock remains, requiring direct/session-pooled PostgreSQL.
        """

        if isinstance(lock_key, bool) or not -(2**63) <= lock_key < 2**63:
            raise ValueError("coordination lock key must be a signed 64-bit integer")
        async with self._engine.connect() as connection:
            acquired = await connection.scalar(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": lock_key},
            )
            await connection.commit()
            if not isinstance(acquired, bool):
                raise RuntimeError("PostgreSQL returned an invalid session-lock result")
            if not acquired:
                yield None
                return
            try:
                yield connection
            finally:
                if connection.in_transaction():
                    await connection.rollback()
                released = await connection.scalar(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": lock_key},
                )
                await connection.commit()
                if released is not True:
                    raise RuntimeError("PostgreSQL failed to release a session advisory lock")

    @asynccontextmanager
    async def _atomic_envelope_on_connection(
        self,
        connection: AsyncConnection,
        owner_task: object,
    ) -> AsyncIterator[None]:
        """@brief 在指定空闲连接上绑定原子信封 / Bind an atomic envelope to a supplied idle connection.

        @param connection 当前无活动事务的独占连接 / Exclusive connection without an active transaction.
        @param owner_task 拥有调用链的 asyncio Task / Owning asyncio task.
        @return 正常退出才提交的原子信封 / Atomic envelope committed only on normal exit.
        @raise RuntimeError 连接仍处于事务中时抛出 / Raised when the connection still has an
            active transaction.
        """

        if connection.in_transaction():
            raise RuntimeError("atomic envelope requires a transaction-free connection")
        async with connection.begin():
            token = self._envelope_binding.set(_AtomicEnvelopeBinding(connection, owner_task))
            try:
                yield
            finally:
                self._envelope_binding.reset(token)

    async def install_v2_request_scope(
        self,
        session: AsyncSession,
        *,
        actor_id: str,
        workspace_id: str | None,
    ) -> None:
        """@brief 安装 API V2 RLS 与事务超时 / Install API V2 RLS scope and timeouts.

        @param session 当前事务绑定 Session / Session bound to the current transaction.
        @param actor_id 已验证 access token 的本地用户 ID / Local user ID from a verified token.
        @param workspace_id URL 路径 Workspace；用户级操作为空 / URL-path Workspace, or null
            for a user-scoped operation.
        @return 无返回值 / No return value.
        @raise ValueError actor 或 Workspace 不是规范非空值时抛出 / Raised for a
            non-canonical actor or Workspace value.

        @note API V2 不伪造历史 ``resource_owner_id``。``SET LOCAL`` 等价的
            ``set_config(..., true)`` 随最外层事务结束清除，适用于独立事务和原子信封。
            / API V2 never fabricates the legacy resource-owner axis. Transaction-local settings
            are cleared with either an independent transaction or the outer atomic envelope.
        """
        if not actor_id or actor_id != actor_id.strip():
            raise ValueError("API V2 actor_id must be a canonical non-empty value")
        if workspace_id is not None and (not workspace_id or workspace_id != workspace_id.strip()):
            raise ValueError("API V2 workspace_id must be a canonical non-empty value")
        await session.execute(
            text(
                """
                SELECT
                    set_config('app.actor_id', :actor_id, true),
                    set_config('app.workspace_id', :workspace_id, true),
                    set_config('statement_timeout', CAST(:statement_timeout_ms AS text), true),
                    set_config('lock_timeout', CAST(:lock_timeout_ms AS text), true)
                """
            ),
            {
                "actor_id": actor_id,
                "workspace_id": "" if workspace_id is None else workspace_id,
                "statement_timeout_ms": str(self._statement_timeout_ms),
                "lock_timeout_ms": str(self._lock_timeout_ms),
            },
        )

    @asynccontextmanager
    async def transaction(self, scope: ActorScope) -> AsyncIterator[AsyncSession]:
        """@brief 打开一个租户受限的读写短事务 / Open a tenant-scoped read-write transaction.

        @param scope 该操作的 actor/workspace/resource-owner 边界。
        @return 在上下文内有效的 AsyncSession；正常退出时提交，异常时回滚。

        @note 一个 ``asyncio.Task`` 应只使用一次此上下文；不要把 yield 出去的
        Session 传给并发子任务。
        """
        async with self.new_session() as session:
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
        async with self.new_session() as session:
            async with session.begin():
                if not self.in_atomic_envelope:
                    await session.execute(text("SET TRANSACTION READ ONLY"))
                await install_tenant_scope(
                    session,
                    scope,
                    statement_timeout_ms=self._statement_timeout_ms,
                    lock_timeout_ms=self._lock_timeout_ms,
                )
                yield session

    @asynccontextmanager
    async def unscoped_transaction(self) -> AsyncIterator[AsyncSession]:
        """@brief 打开仅用于全局基础设施记录的短事务 / Open a short transaction for global infrastructure records.

        @return 安装 timeout、但不伪造租户 GUC 的 Session / Session with timeouts and no fabricated tenant GUCs.

        @note 仅允许由具有显式全 NULL-scope RLS policy 的基础设施表使用；业务
        Repository 不得通过此接口绕过 ActorScope。
        """
        async with self.new_session() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        SELECT
                            set_config(
                                'statement_timeout', CAST(:statement_timeout_ms AS text), true
                            ),
                            set_config('lock_timeout', CAST(:lock_timeout_ms AS text), true)
                        """
                    ),
                    {
                        "statement_timeout_ms": str(self._statement_timeout_ms),
                        "lock_timeout_ms": str(self._lock_timeout_ms),
                    },
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
