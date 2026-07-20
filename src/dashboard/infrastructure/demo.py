"""@brief 明确仅供本地演示的空读存储 / Explicit empty read store for local demonstrations only."""

from __future__ import annotations

from collections.abc import Sequence

from dashboard.application.ports import (
    DiagnosticEventRow,
    EventReadRequest,
    OverviewReadRequest,
    ServiceSignalRow,
    SystemHealthReadRequest,
    SystemHealthRow,
    TrendReadRequest,
    TrendSignalRow,
)


class DemoObservabilityReadStore:
    """@brief memory 配置下的显式无数据演示适配器 / Explicit no-data demo adapter for memory configuration.

    @note 本适配器不声称跨进程持久化；生产模式必须使用 PostgreSQL。
    / This adapter makes no cross-process persistence claim; production must use PostgreSQL.
    """

    async def fetch_overview(self, request: OverviewReadRequest) -> Sequence[ServiceSignalRow]:
        """@brief 返回空 Overview / Return an empty overview.

        @param request 被保留用于端口一致性 / Request retained for port consistency.
        @return 空元组 / Empty tuple.
        """

        del request
        return ()

    async def fetch_trends(self, request: TrendReadRequest) -> Sequence[TrendSignalRow]:
        """@brief 返回空趋势 / Return empty trends.

        @param request 被保留用于端口一致性 / Request retained for port consistency.
        @return 空元组 / Empty tuple.
        """

        del request
        return ()

    async def fetch_recent_events(
        self,
        request: EventReadRequest,
    ) -> Sequence[DiagnosticEventRow]:
        """@brief 返回空诊断事件 / Return empty diagnostic events.

        @param request 被保留用于端口一致性 / Request retained for port consistency.
        @return 空元组 / Empty tuple.
        """

        del request
        return ()

    async def fetch_system_health(
        self,
        request: SystemHealthReadRequest,
    ) -> SystemHealthRow | None:
        """@brief demo 模式明确没有系统健康事实 / Demo mode explicitly has no system-health facts.

        @param request 被保留用于端口一致性 / Request retained for port consistency.
        @return 总为 None / Always ``None``.
        """

        del request
        return None


__all__ = ["DemoObservabilityReadStore"]
