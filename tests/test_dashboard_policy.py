"""@brief Dashboard SLO、新鲜度与查询边界策略测试 / Dashboard SLO, freshness, and query-boundary policy tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dashboard.application.errors import DashboardQueryError
from dashboard.application.ports import (
    ServiceSignalRow,
    SystemHealthReadRequest,
    SystemHealthRow,
)
from dashboard.application.service import DashboardQueryPolicy, DashboardQueryService
from dashboard.domain.model import (
    DashboardDomainError,
    FreshnessMode,
    HealthStatus,
    NoDataReason,
    OperatorPrincipal,
    ServiceLevelObjective,
    SignalKind,
    TimeWindow,
    WorkspaceScope,
)
from dashboard.domain.policy import assess_health
from dashboard.infrastructure.demo import DemoObservabilityReadStore


class StaleReadStore(DemoObservabilityReadStore):
    """@brief 返回过期聚合的测试存储 / Test store returning a stale aggregate."""

    def __init__(self, observed_at: datetime) -> None:
        """@brief 保存过期观测时刻 / Store the stale observation time.

        @param observed_at 过期观测时刻 / Stale observation time.
        @return 新测试存储 / New test store.
        """

        self.observed_at = observed_at

    async def fetch_overview(self, request: object) -> tuple[ServiceSignalRow, ...]:
        """@brief 返回看似健康但已经过期的聚合 / Return an apparently healthy but stale aggregate.

        @param request 未使用请求 / Unused request.
        @return 单服务聚合 / Single-service aggregate.
        """

        del request
        return (
            ServiceSignalRow(
                service="backend",
                request_count=100.0,
                error_count=0.0,
                latency_p50_ms=20.0,
                latency_p95_ms=40.0,
                latency_p99_ms=80.0,
                saturation_mean=0.1,
                saturation_max=0.2,
                sample_count=8,
                max_collection_lag_seconds=1.0,
                latest_observed_at=self.observed_at,
            ),
        )


class MixedFreshnessReadStore(DemoObservabilityReadStore):
    """@brief 返回一新一旧两个服务的测试存储 / Test store returning one fresh and one stale service."""

    def __init__(self, now: datetime) -> None:
        """@brief 保存报告时钟 / Store the report clock.

        @param now 报告生成时刻 / Report-generation time.
        """

        self.now = now

    async def fetch_overview(self, request: object) -> tuple[ServiceSignalRow, ...]:
        """@brief 返回会暴露 aggregate freshness 假绿的两行 / Return two rows exposing aggregate-freshness false green.

        @param request 未使用请求 / Unused request.
        @return 新旧服务聚合 / Fresh and stale service aggregates.
        """

        del request

        def row(service: str, observed_at: datetime) -> ServiceSignalRow:
            """@brief 构造一个健康服务聚合 / Build one healthy service aggregate.

            @param service 稳定服务名 / Stable service name.
            @param observed_at 最近观测时刻 / Latest observation time.
            @return 服务聚合行 / Service aggregate row.
            """

            return ServiceSignalRow(
                service=service,
                request_count=100.0,
                error_count=0.0,
                latency_p50_ms=20.0,
                latency_p95_ms=40.0,
                latency_p99_ms=80.0,
                saturation_mean=0.1,
                saturation_max=0.2,
                sample_count=8,
                max_collection_lag_seconds=1.0,
                latest_observed_at=observed_at,
            )

        return (
            row("backend.fresh", self.now - timedelta(seconds=10)),
            row("backend.stale", self.now - timedelta(minutes=10)),
        )


class HistoricalReadStore(DemoObservabilityReadStore):
    """@brief 返回带确定采集延迟的历史聚合 / Return a historical aggregate with deterministic collection lag."""

    def __init__(self, observed_at: datetime, max_collection_lag_seconds: float) -> None:
        """@brief 保存采集时刻与窗口最坏延迟 / Store collection time and worst window lag.

        @param observed_at 指标持久化时刻 / Metric persistence time.
        @param max_collection_lag_seconds 同一行上的最坏采集延迟 / Worst same-row collection lag.
        @return 新测试存储 / New test store.
        """

        self.observed_at = observed_at
        self.max_collection_lag_seconds = max_collection_lag_seconds

    async def fetch_overview(self, request: object) -> tuple[ServiceSignalRow, ...]:
        """@brief 返回历史窗口内的健康聚合 / Return a healthy aggregate in a historical window.

        @param request 未使用请求 / Unused request.
        @return 单服务聚合 / Single-service aggregate.
        """

        del request
        return (
            ServiceSignalRow(
                service="backend",
                request_count=100.0,
                error_count=0.0,
                latency_p50_ms=20.0,
                latency_p95_ms=40.0,
                latency_p99_ms=80.0,
                saturation_mean=0.1,
                saturation_max=0.2,
                sample_count=8,
                max_collection_lag_seconds=self.max_collection_lag_seconds,
                latest_observed_at=self.observed_at,
            ),
        )


class SaturationOnlyReadStore(DemoObservabilityReadStore):
    """@brief 返回没有流量但有资源饱和度的聚合 / Return saturation telemetry without traffic."""

    def __init__(self, observed_at: datetime) -> None:
        """@brief 保存观测时刻 / Store the observation timestamp.

        @param observed_at 最近观测时刻 / Latest observation timestamp.
        @return 新测试存储 / New test store.
        """

        self.observed_at = observed_at

    async def fetch_overview(self, request: object) -> tuple[ServiceSignalRow, ...]:
        """@brief 返回临界饱和度样本 / Return a critical saturation sample.

        @param request 未使用请求 / Unused request.
        @return 单服务资源聚合 / Single-service resource aggregate.
        """

        del request
        return (
            ServiceSignalRow(
                service="backend",
                request_count=0.0,
                error_count=0.0,
                latency_p50_ms=None,
                latency_p95_ms=None,
                latency_p99_ms=None,
                saturation_mean=0.97,
                saturation_max=0.97,
                sample_count=1,
                max_collection_lag_seconds=1.0,
                latest_observed_at=self.observed_at,
            ),
        )


class StaleSystemHealthReadStore(DemoObservabilityReadStore):
    """@brief 返回陈旧系统快照的测试存储 / Test store returning a stale system snapshot."""

    def __init__(self, observed_at: datetime) -> None:
        """@brief 保存陈旧观测时刻 / Store the stale observation timestamp.

        @param observed_at 陈旧观测时刻 / Stale observation timestamp.
        @return 新测试存储 / New test store.
        """

        self.observed_at = observed_at

    async def fetch_system_health(
        self,
        request: SystemHealthReadRequest,
    ) -> SystemHealthRow:
        """@brief 返回表面健康但陈旧的快照 / Return an apparently healthy but stale snapshot.

        @param request 未使用查询 / Unused query.
        @return 陈旧系统健康行 / Stale system-health row.
        """

        del request
        return SystemHealthRow(
            occurred_at=self.observed_at - timedelta(seconds=1),
            observed_at=self.observed_at,
            severity_number=9,
            severity_text="INFO",
            accepted_count=100,
            dropped_count=0,
            write_failure_count=0,
            output_dropped_count=0,
        )


def _objective() -> ServiceLevelObjective:
    """@brief 返回 30 天 99% 可用性目标 / Return a 30-day 99% availability objective.

    @return SLO / Service-level objective.
    """

    return ServiceLevelObjective(availability_target=0.99, period=timedelta(days=30))


def _policy() -> DashboardQueryPolicy:
    """@brief 返回确定性查询策略 / Return a deterministic query policy.

    @return 查询策略 / Query policy.
    """

    return DashboardQueryPolicy(
        default_window=timedelta(hours=1),
        max_window=timedelta(days=7),
        freshness_target=timedelta(minutes=2),
        target_points=120,
        max_event_limit=500,
        objective=_objective(),
    )


def test_latency_objective_rejects_unsupported_percentile() -> None:
    """@brief 固定 p95 读模型不得接受伪可配置百分位 / The fixed-p95 read model must reject a falsely configurable percentile."""

    with pytest.raises(DashboardDomainError, match="只支持 p95"):
        ServiceLevelObjective(latency_target=0.99)


def test_budget_remaining_is_a_period_scaled_steady_traffic_estimate() -> None:
    """@brief 窗口燃烧速度不得直接冒充整周期预算消耗 / Window burn velocity must not masquerade as whole-period budget consumption."""

    result = assess_health(
        request_count=1_000,
        error_count=5,
        latency_p95_ms=100,
        saturation_max=0.2,
        objective=_objective(),
        window_duration=timedelta(hours=1),
    )
    assert result.burn_rate == pytest.approx(0.5)
    assert result.budget_remaining_ratio == pytest.approx(1.0 - 0.5 / (30 * 24))
    assert result.budget_remaining_ratio != pytest.approx(0.5)


@pytest.mark.asyncio
async def test_stale_telemetry_cannot_produce_a_green_overview() -> None:
    """@brief 过期遥测即使历史值健康也不能产生假绿 / Stale telemetry cannot produce a false green even when historical values look healthy."""

    now = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    service = DashboardQueryService(
        StaleReadStore(now - timedelta(minutes=10)),
        _policy(),
        clock=lambda: now,
    )
    report = await service.overview(
        OperatorPrincipal("operator"),
        WorkspaceScope("workspace"),
    )
    assert report.freshness.stale is True
    assert report.no_data_reason is NoDataReason.STALE_TELEMETRY
    assert report.health is HealthStatus.NO_DATA


@pytest.mark.asyncio
async def test_fresh_service_cannot_hide_another_stale_service() -> None:
    """@brief 聚合实时新鲜度必须采用最旧的服务最新点 / Aggregate live freshness must use the oldest per-service latest point."""

    now = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    service = DashboardQueryService(
        MixedFreshnessReadStore(now),
        _policy(),
        clock=lambda: now,
    )
    report = await service.overview(
        OperatorPrincipal("operator"),
        WorkspaceScope("workspace"),
    )

    assert report.freshness.lag_seconds == 600.0
    assert report.freshness.stale is True
    assert report.no_data_reason is NoDataReason.STALE_TELEMETRY
    assert report.health is HealthStatus.NO_DATA


@pytest.mark.asyncio
async def test_historical_window_measures_collection_lag_instead_of_age_from_now() -> None:
    """@brief 历史查询以采集延迟判断质量而非距今时长 / Historical queries assess collection lag, not age from now."""

    now = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    occurred_at = now - timedelta(days=1, minutes=1)
    observed_at = occurred_at + timedelta(seconds=5)
    service = DashboardQueryService(
        HistoricalReadStore(observed_at, 5.0),
        _policy(),
        clock=lambda: now,
    )
    report = await service.overview(
        OperatorPrincipal("operator"),
        WorkspaceScope("workspace"),
        window=TimeWindow(now - timedelta(days=1, hours=1), now - timedelta(days=1)),
    )

    assert report.freshness.mode is FreshnessMode.HISTORICAL
    assert report.freshness.lag_seconds == 5.0
    assert report.freshness.stale is False
    assert report.health is HealthStatus.HEALTHY
    assert report.no_data_reason is None


@pytest.mark.asyncio
async def test_historical_window_uses_worst_same_row_collection_lag() -> None:
    """@brief 较新的快样本不得掩盖窗口内较早慢样本 / A newer fast sample must not hide an older slow sample in the window."""

    now = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    latest_observed_at = now - timedelta(days=1, seconds=1)
    service = DashboardQueryService(
        HistoricalReadStore(latest_observed_at, 10 * 60.0),
        _policy(),
        clock=lambda: now,
    )
    report = await service.overview(
        OperatorPrincipal("operator"),
        WorkspaceScope("workspace"),
        window=TimeWindow(now - timedelta(days=1, hours=1), now - timedelta(days=1)),
    )

    assert report.freshness.mode is FreshnessMode.HISTORICAL
    assert report.freshness.lag_seconds == 600.0
    assert report.freshness.stale is True
    assert report.no_data_reason is NoDataReason.STALE_TELEMETRY
    assert report.health is HealthStatus.NO_DATA


@pytest.mark.asyncio
async def test_saturation_without_http_traffic_remains_actionable() -> None:
    """@brief 无 HTTP 流量时高饱和度仍必须告警 / High saturation remains actionable without HTTP traffic."""

    now = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    service = DashboardQueryService(
        SaturationOnlyReadStore(now - timedelta(seconds=10)),
        _policy(),
        clock=lambda: now,
    )
    report = await service.overview(
        OperatorPrincipal("operator"),
        WorkspaceScope("workspace"),
    )

    assert report.request_count == 0
    assert report.health is HealthStatus.CRITICAL
    assert report.no_data_reason is None
    assert report.services[0].reasons == ("saturation_high",)


@pytest.mark.asyncio
async def test_stale_system_health_snapshot_cannot_produce_a_false_green() -> None:
    """@brief 陈旧 worker 快照不得显示系统健康 / A stale worker snapshot cannot appear healthy."""

    now = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    service = DashboardQueryService(
        StaleSystemHealthReadStore(now - timedelta(minutes=10)),
        _policy(),
        clock=lambda: now,
    )
    report = await service.system_health(OperatorPrincipal("operator"))

    assert report.freshness.stale is True
    assert report.health is HealthStatus.NO_DATA
    assert report.no_data_reason is NoDataReason.STALE_TELEMETRY


@pytest.mark.asyncio
async def test_automatic_bucket_uses_ceiling_to_bound_point_count() -> None:
    """@brief 自动桶宽使用 ceiling 保证不超目标点数 / Automatic bucket width uses a ceiling to bound point count."""

    store = DemoObservabilityReadStore()
    service = DashboardQueryService(store, _policy())
    window = TimeWindow(
        datetime(2026, 7, 21, 8, 0, tzinfo=UTC),
        datetime(2026, 7, 21, 8, 2, 1, tzinfo=UTC),
    )
    report = await service.trends(
        OperatorPrincipal("operator"),
        WorkspaceScope("workspace"),
        SignalKind.TRAFFIC,
        window=window,
    )
    assert report.bucket_seconds == 5
    assert window.duration.total_seconds() / report.bucket_seconds <= 120


@pytest.mark.parametrize(
    "overrides",
    (
        {"max_window": timedelta(days=32)},
        {"target_points": 2_001},
        {"max_event_limit": 1_001},
    ),
)
def test_query_policy_rejects_configuration_above_code_level_ceiling(
    overrides: dict[str, object],
) -> None:
    """@brief 配置不得抬高代码级查询预算 / Configuration cannot raise code-level query budgets.

    @param overrides 越界策略字段 / Out-of-bounds policy fields.
    """

    values: dict[str, object] = {
        "default_window": timedelta(hours=1),
        "max_window": timedelta(days=7),
        "freshness_target": timedelta(minutes=2),
        "target_points": 120,
        "max_event_limit": 500,
        "objective": _objective(),
        **overrides,
    }
    with pytest.raises(DashboardQueryError):
        DashboardQueryPolicy(**values)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_explicit_bucket_cannot_exceed_one_day() -> None:
    """@brief 显式 SQL 桶宽受代码级一天上限约束 / Explicit SQL buckets have a one-day code-level ceiling."""

    service = DashboardQueryService(DemoObservabilityReadStore(), _policy())
    with pytest.raises(DashboardQueryError, match="86400"):
        await service.trends(
            OperatorPrincipal("operator"),
            WorkspaceScope("workspace"),
            SignalKind.TRAFFIC,
            bucket_seconds=86_401,
        )


@pytest.mark.parametrize(
    "workspace_id",
    ("w" * 129, "workspace\nforged"),
)
def test_workspace_scope_rejects_oversized_or_control_text(workspace_id: str) -> None:
    """@brief workspace filter 必须有界且不含控制字符 / Workspace filters are bounded and control-character free.

    @param workspace_id 非法工作区 / Invalid workspace identifier.
    """

    with pytest.raises(DashboardDomainError):
        WorkspaceScope(workspace_id)


@pytest.mark.parametrize("service_name", ("s" * 129, "backend\rforged"))
@pytest.mark.asyncio
async def test_service_filter_rejects_oversized_or_control_text(service_name: str) -> None:
    """@brief service filter 必须有界且不含控制字符 / Service filters are bounded and control-character free.

    @param service_name 非法服务名 / Invalid service name.
    """

    service = DashboardQueryService(DemoObservabilityReadStore(), _policy())
    with pytest.raises(DashboardQueryError):
        await service.overview(
            OperatorPrincipal("operator"),
            WorkspaceScope("workspace"),
            service=service_name,
        )
