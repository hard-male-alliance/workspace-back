"""CLI、API 与 GUI 共同调用的 Dashboard 应用服务（application service）。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from math import floor

from .config import DashboardSettings
from .errors import DashboardValidationError
from .models import (
    DashboardOverview,
    DashboardScope,
    HealthStatus,
    MetricKind,
    MetricQuery,
    MetricSample,
    ServiceSummary,
)
from .ports import ObservabilityRepository

Clock = Callable[[], datetime]


class DashboardService:
    """@brief 聚合可观测性读模型的应用服务（application service）。

    @param repository: 实现 ObservabilityRepository 的注入仓库。
    @param settings: Dashboard 查询与健康阈值配置快照。
    @param clock: 可注入时钟；默认使用 UTC 当前时间，便于确定性测试。

    @note 本类不知晓 CLI、FastAPI 或 PyQt6，从而避免三套业务逻辑分叉。
    """

    def __init__(
        self,
        repository: ObservabilityRepository,
        settings: DashboardSettings,
        *,
        clock: Clock | None = None,
    ) -> None:
        """@brief 创建 Dashboard 应用服务。

        @param repository: 只读可观测性仓库端口。
        @param settings: 已校验的 DashboardSettings。
        @param clock: 返回带时区 datetime 的可选时钟函数。
        @return: 新建的 DashboardService 实例。
        """

        if not isinstance(repository, ObservabilityRepository):
            raise DashboardValidationError("repository 必须实现 ObservabilityRepository 协议。")
        if not isinstance(settings, DashboardSettings):
            raise DashboardValidationError("settings 必须是 DashboardSettings。")
        self._repository = repository
        self._settings = settings
        self._clock = clock or _utc_now

    @property
    def settings(self) -> DashboardSettings:
        """@brief 返回该服务绑定的不可变配置快照。

        @return: DashboardSettings；调用方不能通过该对象修改运行中配置。
        """

        return self._settings

    async def overview(
        self,
        scope: DashboardScope,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        service: str | None = None,
        max_samples: int | None = None,
    ) -> DashboardOverview:
        """@brief 获取一个工作区在有界时间窗口内的完整运维概览。

        @param scope: 必填工作区范围；不会提供无范围的跨租户读取。
        @param start_at: 可选窗口起点；省略时使用 end_at - 默认窗口。
        @param end_at: 可选窗口终点；省略时使用注入时钟的当前 UTC 时间。
        @param service: 可选稳定服务名过滤。
        @param max_samples: 可选更严格上限；不能超过配置的 max_samples。
        @return: 由每服务摘要与工作区汇总组成的 DashboardOverview。
        """

        query = self._make_query(
            scope,
            start_at=start_at,
            end_at=end_at,
            service=service,
            max_samples=max_samples,
        )
        samples = await self._repository.list_observations(query)
        if len(samples) > query.max_samples:
            raise DashboardValidationError("仓库返回的指标样本超过查询上限。")
        return self._aggregate(query, samples)

    async def list_services(
        self,
        scope: DashboardScope,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        max_samples: int | None = None,
    ) -> tuple[ServiceSummary, ...]:
        """@brief 返回一个工作区内所有服务的健康摘要。

        @param scope: 必填工作区范围。
        @param start_at: 可选窗口起点。
        @param end_at: 可选窗口终点。
        @param max_samples: 可选样本读取上限。
        @return: 按服务名称稳定排序的 ServiceSummary 元组。
        """

        overview = await self.overview(
            scope,
            start_at=start_at,
            end_at=end_at,
            max_samples=max_samples,
        )
        return overview.services

    def _make_query(
        self,
        scope: DashboardScope,
        *,
        start_at: datetime | None,
        end_at: datetime | None,
        service: str | None,
        max_samples: int | None,
    ) -> MetricQuery:
        if not isinstance(scope, DashboardScope):
            raise DashboardValidationError("scope 必须是 DashboardScope。")

        end = _normalize_datetime(end_at or self._clock(), "end_at")
        start = _normalize_datetime(
            start_at or end - self._settings.default_window,
            "start_at",
        )
        if end - start > self._settings.max_window:
            raise DashboardValidationError(
                "查询窗口超过 dashboard.query.max_window_hours 的配置上限。"
            )

        limit = self._settings.max_samples if max_samples is None else max_samples
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise DashboardValidationError("max_samples 必须是正整数。")
        if limit > self._settings.max_samples:
            raise DashboardValidationError(
                "max_samples 不能超过 dashboard.query.max_samples 的配置上限。"
            )
        return MetricQuery(
            scope=scope,
            start_at=start,
            end_at=end,
            service=service,
            max_samples=limit,
        )

    def _aggregate(
        self,
        query: MetricQuery,
        samples: Sequence[MetricSample],
    ) -> DashboardOverview:
        grouped: dict[str, list[MetricSample]] = defaultdict(list)
        for sample in samples:
            if not isinstance(sample, MetricSample):
                raise DashboardValidationError("仓库返回了非 MetricSample 的对象。")
            if sample.workspace_id != query.scope.workspace_id:
                raise DashboardValidationError("仓库返回了查询范围外的 workspace_id。")
            if not query.start_at <= sample.observed_at < query.end_at:
                raise DashboardValidationError("仓库返回了查询时间窗口外的指标样本。")
            if query.service is not None and sample.service != query.service:
                raise DashboardValidationError("仓库返回了查询服务范围外的指标样本。")
            grouped[sample.service].append(sample)

        summaries = tuple(
            self._summarize_service(service, service_samples)
            for service, service_samples in sorted(grouped.items())
        )
        request_count = sum(summary.request_count for summary in summaries)
        error_count = sum(summary.error_count for summary in summaries)
        error_rate = _error_rate(request_count, error_count)
        availability = None if error_rate is None else 1 - error_rate
        health = _overall_health(summary.health for summary in summaries)

        return DashboardOverview(
            scope=query.scope,
            start_at=query.start_at,
            end_at=query.end_at,
            generated_at=_normalize_datetime(self._clock(), "clock"),
            services=summaries,
            health=health,
            request_count=request_count,
            error_count=error_count,
            error_rate=error_rate,
            availability=availability,
        )

    def _summarize_service(
        self,
        service: str,
        samples: Sequence[MetricSample],
    ) -> ServiceSummary:
        request_count = sum(
            sample.value for sample in samples if sample.metric is MetricKind.REQUESTS
        )
        error_count = sum(sample.value for sample in samples if sample.metric is MetricKind.ERRORS)
        latencies = tuple(
            sample.value for sample in samples if sample.metric is MetricKind.LATENCY_MS
        )
        saturations = tuple(
            sample.value for sample in samples if sample.metric is MetricKind.SATURATION
        )
        error_rate = _error_rate(request_count, error_count)
        availability = None if error_rate is None else 1 - error_rate
        latency_p50 = _percentile(latencies, 0.50)
        latency_p95 = _percentile(latencies, 0.95)
        latency_p99 = _percentile(latencies, 0.99)
        saturation = sum(saturations) / len(saturations) if saturations else None
        health = self._health(error_rate, latency_p95, saturation)

        return ServiceSummary(
            service=service,
            request_count=request_count,
            error_count=error_count,
            error_rate=error_rate,
            availability=availability,
            latency_p50_ms=latency_p50,
            latency_p95_ms=latency_p95,
            latency_p99_ms=latency_p99,
            saturation=saturation,
            health=health,
            sample_count=len(samples),
        )

    def _health(
        self,
        error_rate: float | None,
        latency_p95_ms: float | None,
        saturation: float | None,
    ) -> HealthStatus:
        if error_rate is None and latency_p95_ms is None and saturation is None:
            return HealthStatus.NO_DATA

        policy = self._settings.health_policy
        critical = (
            (error_rate is not None and error_rate >= policy.critical_error_rate)
            or (latency_p95_ms is not None and latency_p95_ms >= policy.critical_latency_ms)
            or (saturation is not None and saturation >= policy.critical_saturation)
        )
        if critical:
            return HealthStatus.CRITICAL

        degraded = (
            (error_rate is not None and error_rate >= policy.warning_error_rate)
            or (latency_p95_ms is not None and latency_p95_ms >= policy.warning_latency_ms)
            or (saturation is not None and saturation >= policy.warning_saturation)
        )
        return HealthStatus.DEGRADED if degraded else HealthStatus.HEALTHY


def _utc_now() -> datetime:
    """返回当前 UTC 时刻。"""

    return datetime.now(UTC)


def _normalize_datetime(value: datetime, name: str) -> datetime:
    """确保内部时间统一为携带 UTC 时区的 datetime。"""

    if not isinstance(value, datetime) or value.tzinfo is None:
        raise DashboardValidationError(f"{name} 必须是携带时区的 datetime。")
    return value.astimezone(UTC)


def _error_rate(request_count: float, error_count: float) -> float | None:
    """在没有请求时保留未知语义，而不是把它伪装成零错误。"""

    if request_count <= 0:
        return None
    return min(error_count / request_count, 1.0)


def _percentile(values: Iterable[float], quantile: float) -> float | None:
    """使用线性插值计算确定性百分位数。"""

    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * quantile
    lower = floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _overall_health(statuses: Iterable[HealthStatus]) -> HealthStatus:
    """按严重程度折叠多个服务状态。"""

    status_set = set(statuses)
    if HealthStatus.CRITICAL in status_set:
        return HealthStatus.CRITICAL
    if HealthStatus.DEGRADED in status_set:
        return HealthStatus.DEGRADED
    if HealthStatus.HEALTHY in status_set:
        return HealthStatus.HEALTHY
    return HealthStatus.NO_DATA


__all__ = ["DashboardService"]
