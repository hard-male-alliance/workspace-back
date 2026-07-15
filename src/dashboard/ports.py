"""Dashboard 应用层依赖的最小端口（ports）。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from .models import MetricQuery, MetricSample


@runtime_checkable
class ObservabilityRepository(Protocol):
    """@brief 读取可观测性读模型的仓库端口（repository port）。

    该协议故意只包含 Dashboard 真正需要的一个有界读取操作。每次读取都接受
    MetricQuery，因此实现层无法遗漏 workspace scope。
    """

    async def list_observations(self, query: MetricQuery) -> Sequence[MetricSample]:
        """@brief 读取一个工作区、一个时间窗口内的指标样本。

        @param query: 含 workspace scope、半开时间窗口和返回上限的查询。
        @return: 不超过 query.max_samples 的指标样本序列。
        """


@runtime_checkable
class AsyncRowFetcher(Protocol):
    """@brief 数据库行读取器（async row fetcher）的极小适配协议。

    Dashboard 不直接依赖 psycopg、SQLAlchemy 或 asyncpg。部署组合根可将任意
    驱动包装为此协议，从而保持 Dashboard 对数据库实现无感。
    """

    async def __call__(
        self,
        statement: str,
        parameters: Mapping[str, object],
    ) -> Sequence[Mapping[str, Any]]:
        """@brief 执行只读参数化 SQL 并返回字典行。

        @param statement: 使用命名参数的只读 PostgreSQL SQL。
        @param parameters: 必须由数据库驱动绑定、不得字符串拼接的参数。
        @return: 数据库行组成的序列，每行可按列名访问。
        """


__all__ = ["AsyncRowFetcher", "ObservabilityRepository"]
