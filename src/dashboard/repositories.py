"""Dashboard 的内存与 PostgreSQL 可观测性仓库适配器。"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import URL, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from .errors import DashboardDataError, DashboardValidationError
from .models import MetricKind, MetricQuery, MetricSample
from .ports import AsyncRowFetcher


class MemoryObservabilityRepository:
    """@brief 用于开发、GUI 演示与确定性测试的内存仓库（memory repository）。

    @param samples: 可选的初始样本序列；仓库会复制它而不会保留调用方的可变容器。

    @note 此实现是 Dashboard 的默认仓库，绝不尝试连接数据库。
    """

    def __init__(self, samples: Iterable[MetricSample] = ()) -> None:
        """@brief 创建线程安全的内存可观测性仓库。

        @param samples: 初始指标样本。
        @return: 新建的 MemoryObservabilityRepository 实例。
        """

        self._samples = list(samples)
        if not all(isinstance(sample, MetricSample) for sample in self._samples):
            raise DashboardValidationError("内存仓库只接受 MetricSample 样本。")
        self._lock = asyncio.Lock()

    async def append(self, sample: MetricSample) -> None:
        """@brief 追加一个样本，主要供本地演示或测试注入。

        @param sample: 已通过 MetricSample 校验的样本。
        @return: 无返回值。
        """

        if not isinstance(sample, MetricSample):
            raise DashboardValidationError("内存仓库只接受 MetricSample 样本。")
        async with self._lock:
            self._samples.append(sample)

    async def extend(self, samples: Iterable[MetricSample]) -> None:
        """@brief 原子地追加一批样本（batch append）。

        @param samples: 待追加的 MetricSample 可迭代对象。
        @return: 无返回值；遇到非法样本时不写入任何样本。
        """

        batch = list(samples)
        if not all(isinstance(sample, MetricSample) for sample in batch):
            raise DashboardValidationError("内存仓库只接受 MetricSample 样本。")
        async with self._lock:
            self._samples.extend(batch)

    async def replace(self, samples: Iterable[MetricSample]) -> None:
        """@brief 用一批样本替换内存内容，便于获得确定性测试状态。

        @param samples: 新的完整 MetricSample 序列。
        @return: 无返回值；遇到非法样本时保留旧内容。
        """

        replacement = list(samples)
        if not all(isinstance(sample, MetricSample) for sample in replacement):
            raise DashboardValidationError("内存仓库只接受 MetricSample 样本。")
        async with self._lock:
            self._samples = replacement

    async def list_observations(self, query: MetricQuery) -> Sequence[MetricSample]:
        """@brief 按工作区、时间窗口与服务名读取有序样本。

        @param query: 含必填 workspace scope 的有界读取条件。
        @return: 按 observed_at、service、metric 排序且受 max_samples 限制的样本元组。
        """

        async with self._lock:
            matching = tuple(
                sample
                for sample in self._samples
                if sample.workspace_id == query.scope.workspace_id
                and query.start_at <= sample.observed_at < query.end_at
                and (query.service is None or sample.service == query.service)
            )

        ordered = sorted(
            matching,
            key=lambda sample: (sample.observed_at, sample.service, sample.metric.value),
        )
        return tuple(ordered[: query.max_samples])

    async def aclose(self) -> None:
        """@brief 释放仓库资源（resource cleanup）的无操作实现。

        @return: 无返回值。

        @note 内存仓库没有外部连接；保留此方法让组合根可以统一关闭资源。
        """


class SqlAlchemyAsyncRowFetcher:
    """@brief 由 Dashboard 自己拥有的异步 PostgreSQL 行读取器（row fetcher）。

    @param dsn: dashboard role 的 PostgreSQL DSN；只保存在进程内，绝不写日志。
    @param pool_size: 常驻只读连接数。
    @param max_overflow: 高峰时允许额外创建的连接数。
    @param connect_timeout_ms: 建立新连接的超时。
    @param query_timeout_ms: 每个事务本地的 PostgreSQL statement timeout。

    @note 该适配器不 import backend persistence。它只接受 SELECT，且每次查询均在
    ``READ ONLY`` 短事务中完成；由 PostgresObservabilityRepository 在拥有它时关闭。
    """

    def __init__(
        self,
        dsn: str,
        *,
        pool_size: int,
        max_overflow: int,
        connect_timeout_ms: int,
        query_timeout_ms: int,
    ) -> None:
        """@brief 创建惰性异步连接池（lazy async engine）。

        @param dsn: PostgreSQL DSN。
        @param pool_size: 连接池常驻连接数。
        @param max_overflow: 连接池临时连接数。
        @param connect_timeout_ms: 连接超时，单位毫秒。
        @param query_timeout_ms: 查询超时，单位毫秒。
        @return: 新建的 SqlAlchemyAsyncRowFetcher；构造时不会连接数据库。
        @raise DashboardValidationError: DSN 或池参数不安全/不兼容时抛出。
        """

        if not isinstance(dsn, str) or not dsn.strip():
            raise DashboardValidationError("Dashboard PostgreSQL DSN 不能为空。")
        if isinstance(pool_size, bool) or not isinstance(pool_size, int) or pool_size < 1:
            raise DashboardValidationError("Dashboard PostgreSQL pool_size 必须是正整数。")
        if isinstance(max_overflow, bool) or not isinstance(max_overflow, int) or max_overflow < 0:
            raise DashboardValidationError("Dashboard PostgreSQL max_overflow 必须是非负整数。")
        if (
            isinstance(connect_timeout_ms, bool)
            or not isinstance(connect_timeout_ms, int)
            or connect_timeout_ms < 1
        ):
            raise DashboardValidationError("Dashboard PostgreSQL connect timeout 必须是正整数。")
        if (
            isinstance(query_timeout_ms, bool)
            or not isinstance(query_timeout_ms, int)
            or query_timeout_ms < 1
        ):
            raise DashboardValidationError("Dashboard PostgreSQL query timeout 必须是正整数。")

        self._engine = create_async_engine(
            _normalize_asyncpg_dsn(dsn),
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            connect_args={"timeout": connect_timeout_ms / 1_000},
        )
        self._query_timeout_ms = query_timeout_ms
        self._closed = False

    async def __call__(
        self,
        statement: str,
        parameters: Mapping[str, object],
    ) -> Sequence[Mapping[str, Any]]:
        """@brief 在只读短事务内执行受控的参数化 SELECT。

        @param statement: 仅支持命名绑定参数的 SELECT 文本。
        @param parameters: SQLAlchemy 绑定参数；绝不插值到 SQL 字符串。
        @return: 按列名读取的普通字典行序列。
        @raise DashboardDataError: 连接、SQL 或结果读取失败时抛出且不泄漏 DSN。
        """

        if self._closed:
            raise DashboardDataError("Dashboard PostgreSQL 读取器已经关闭。")
        if not isinstance(statement, str) or not statement.lstrip().casefold().startswith("select"):
            raise DashboardDataError("Dashboard PostgreSQL 读取器只允许 SELECT 查询。")
        if not isinstance(parameters, Mapping):
            raise DashboardDataError("Dashboard PostgreSQL 查询参数必须是 Mapping。")

        try:
            async with self._engine.connect() as connection:
                async with connection.begin():
                    await connection.execute(text("SET TRANSACTION READ ONLY"))
                    await connection.execute(
                        text(
                            "SELECT set_config('statement_timeout', :timeout_ms, true)"
                        ),
                        {"timeout_ms": str(self._query_timeout_ms)},
                    )
                    result = await connection.execute(text(statement), dict(parameters))
                    return tuple(dict(row) for row in result.mappings().all())
        except SQLAlchemyError as error:
            raise DashboardDataError("无法读取 Dashboard PostgreSQL 可观测性视图。") from error

    async def aclose(self) -> None:
        """@brief 释放异步连接池（dispose async engine）。

        @return: 无返回值；重复关闭是幂等（idempotent）的。
        """

        if self._closed:
            return
        self._closed = True
        await self._engine.dispose()


class PostgresObservabilityRepository:
    """@brief 通过稳定 PostgreSQL 视图读取指标的仓库（PostgreSQL repository）。

    @param row_fetcher: 注入的异步字典行读取器，不绑定任意数据库客户端库。
    @param relation: 只读视图的限定名，默认是 observability.dashboard_metric_samples。

    @note 视图需提供 workspace_id、observed_at、service、metric_name、value 与可选
    dimensions 列。所有用户值均使用参数绑定；relation 在构造时被严格验证和引用。
    """

    DEFAULT_RELATION = "observability.dashboard_metric_samples"
    _IDENTIFIER_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(
        self,
        row_fetcher: AsyncRowFetcher,
        relation: str = DEFAULT_RELATION,
        *,
        owns_row_fetcher: bool = False,
    ) -> None:
        """@brief 创建仅可读取的 PostgreSQL 仓库适配器。

        @param row_fetcher: 执行参数化只读 SQL 的驱动适配函数。
        @param relation: 稳定 observability 视图的 schema.table 名称。
        @param owns_row_fetcher: 为 True 时，aclose 会关闭具有 aclose 的读取器资源。
        @return: 新建的 PostgresObservabilityRepository 实例。
        """

        if not isinstance(row_fetcher, AsyncRowFetcher):
            raise DashboardValidationError("row_fetcher 必须实现 AsyncRowFetcher 协议。")
        self._row_fetcher = row_fetcher
        self._relation = self._quote_relation(relation)
        self._owns_row_fetcher = owns_row_fetcher
        self._closed = False

    async def list_observations(self, query: MetricQuery) -> Sequence[MetricSample]:
        """@brief 从稳定视图按 workspace scope 读取并转换指标样本。

        @param query: 含必填工作区过滤、时间窗口、服务过滤和上限的查询。
        @return: 可供 DashboardService 聚合的 MetricSample 元组。
        """

        if self._closed:
            raise DashboardDataError("Dashboard PostgreSQL 仓库已经关闭。")
        service_predicate = ""
        parameters: dict[str, object] = {
            "workspace_id": query.scope.workspace_id,
            "start_at": query.start_at,
            "end_at": query.end_at,
            "limit": query.max_samples,
        }
        if query.service is not None:
            service_predicate = "\n              AND service = :service"
            parameters["service"] = query.service
        statement = f"""
            SELECT workspace_id, observed_at, service, metric_name, value, dimensions
            FROM {self._relation}
            WHERE workspace_id = :workspace_id
              AND observed_at >= :start_at
              AND observed_at < :end_at{service_predicate}
            ORDER BY observed_at ASC, service ASC, metric_name ASC
            LIMIT :limit
        """
        rows = await self._row_fetcher(statement, parameters)
        return tuple(self._sample_from_row(row) for row in rows)

    async def aclose(self) -> None:
        """@brief 关闭仓库及其可选行读取器资源（resource cleanup）。

        @return: 无返回值。

        @note 注入 row_fetcher 默认仍由调用方管理；只在 composition root 显式传入
        ``owns_row_fetcher=True`` 时释放异步连接池。
        """

        if self._closed:
            return
        self._closed = True
        if not self._owns_row_fetcher:
            return
        close = getattr(self._row_fetcher, "aclose", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    @classmethod
    def _quote_relation(cls, relation: str) -> str:
        parts = relation.split(".")
        if len(parts) != 2 or not all(cls._IDENTIFIER_PART.fullmatch(part) for part in parts):
            raise DashboardValidationError(
                "relation 必须是安全的两段式 schema.table PostgreSQL 标识符。"
            )
        return ".".join(f'"{part}"' for part in parts)

    @staticmethod
    def _sample_from_row(row: Mapping[str, Any]) -> MetricSample:
        if not isinstance(row, Mapping):
            raise DashboardDataError("数据库行必须是按列名访问的 Mapping。")
        try:
            observed_at = PostgresObservabilityRepository._parse_datetime(row["observed_at"])
            dimensions = PostgresObservabilityRepository._parse_dimensions(row.get("dimensions"))
            return MetricSample(
                workspace_id=str(row["workspace_id"]),
                observed_at=observed_at,
                service=str(row["service"]),
                metric=MetricKind(str(row["metric_name"])),
                value=float(row["value"]),
                dimensions=dimensions,
            )
        except (KeyError, TypeError, ValueError, DashboardValidationError) as error:
            raise DashboardDataError(f"无法将数据库行转换为指标样本：{error}") from error

    @staticmethod
    def _parse_datetime(value: object) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise DashboardDataError("数据库 observed_at 必须带时区。")
            return value.astimezone(UTC)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise DashboardDataError("数据库 observed_at 必须带时区。")
            return parsed.astimezone(UTC)
        raise DashboardDataError("数据库 observed_at 必须是 datetime 或 RFC 3339 字符串。")

    @staticmethod
    def _parse_dimensions(value: object) -> Mapping[str, str]:
        if value is None:
            return {}
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as error:
                raise DashboardDataError("数据库 dimensions 不是有效 JSON。") from error
        if not isinstance(value, Mapping):
            raise DashboardDataError("数据库 dimensions 必须是对象或 NULL。")
        return {str(key): str(item) for key, item in value.items()}


def _normalize_asyncpg_dsn(dsn: str) -> str:
    """@brief 规范化 Dashboard 只读 DSN 为 SQLAlchemy asyncpg URL。

    @param dsn: 从环境变量读取的 PostgreSQL DSN。
    @return: 使用 ``postgresql+asyncpg`` 方言的 URL 字符串。
    @raise DashboardValidationError: DSN 不是 PostgreSQL 或显式指定不兼容驱动时抛出。

    @note 调用方不得记录返回值，因为其可能包含凭证。
    """

    try:
        url: URL = make_url(dsn)
    except (SQLAlchemyError, ValueError) as error:
        raise DashboardValidationError("Dashboard PostgreSQL DSN 格式无效。") from error
    if url.get_backend_name() != "postgresql":
        raise DashboardValidationError("Dashboard 只支持 PostgreSQL DSN。")
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+asyncpg")
    if url.drivername != "postgresql+asyncpg":
        raise DashboardValidationError("Dashboard PostgreSQL 读取器要求 asyncpg 驱动。")
    return url.render_as_string(hide_password=False)


__all__ = [
    "MemoryObservabilityRepository",
    "PostgresObservabilityRepository",
    "SqlAlchemyAsyncRowFetcher",
]
