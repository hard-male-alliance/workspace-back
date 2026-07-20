"""@brief Dashboard 查询用例编排 / Dashboard query-use-case orchestration."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import ceil

from dashboard.domain.model import (
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

from .dto import (
    DashboardOverview,
    DiagnosticEvent,
    EventReport,
    FreshnessSnapshot,
    ServiceOverview,
    SloSnapshot,
    SystemHealthReport,
    TrendPoint,
    TrendReport,
)
from .errors import DashboardQueryError
from .ports import (
    EventReadRequest,
    ObservabilityReadStore,
    OverviewReadRequest,
    SystemHealthReadRequest,
    TrendReadRequest,
)

Clock = Callable[[], datetime]
"""@brief 可注入 UTC 时钟 / Injectable UTC clock."""

_NICE_BUCKET_SECONDS = (1, 5, 10, 30, 60, 300, 900, 3_600, 21_600, 86_400)
"""@brief 自动分辨率允许的稳定桶宽 / Stable bucket widths accepted by automatic resolution."""

MAX_QUERY_WINDOW = timedelta(days=31)
"""@brief 任意配置不可突破的查询窗口上限 / Absolute query-window ceiling no configuration may exceed."""

MAX_TARGET_POINTS = 2_000
"""@brief 单个趋势、服务组合的最大目标点数 / Maximum target points per trend and service."""

MAX_EVENT_LIMIT = 1_000
"""@brief 单次事件读取绝对上限 / Absolute event-read ceiling."""

MAX_BUCKET_SECONDS = 86_400
"""@brief PostgreSQL ``make_interval`` 接受的最大显式桶宽 / Maximum explicit bucket passed to PostgreSQL."""


@dataclass(frozen=True, slots=True)
class DashboardQueryPolicy:
    """@brief 查询安全边界与领域策略 / Query safety bounds and domain policy.

    @param default_window 默认查询窗口 / Default query window.
    @param max_window 最大查询窗口 / Maximum query window.
    @param freshness_target 数据新鲜度目标 / Data-freshness objective.
    @param target_points 自动分桶目标点数 / Target number of automatic buckets.
    @param max_event_limit 最大诊断事件数 / Maximum diagnostic-event limit.
    @param objective 服务级别目标 / Service-level objective.
    """

    default_window: timedelta
    max_window: timedelta
    freshness_target: timedelta
    target_points: int
    max_event_limit: int
    objective: ServiceLevelObjective

    def __post_init__(self) -> None:
        """@brief 校验查询策略 / Validate the query policy.

        @return 无返回值 / No return value.
        """

        if self.default_window <= timedelta(0) or self.max_window < self.default_window:
            raise DashboardQueryError("Dashboard 时间窗口策略无效。")
        if self.max_window > MAX_QUERY_WINDOW:
            raise DashboardQueryError("Dashboard max_window 超过 31 天代码级上限。")
        if self.freshness_target <= timedelta(0):
            raise DashboardQueryError("freshness_target 必须大于零。")
        if not 10 <= self.target_points <= MAX_TARGET_POINTS:
            raise DashboardQueryError(
                f"target_points 必须在 10 到 {MAX_TARGET_POINTS} 之间。"
            )
        if not 1 <= self.max_event_limit <= MAX_EVENT_LIMIT:
            raise DashboardQueryError("Dashboard 点数或事件上限无效。")


class DashboardQueryService:
    """@brief CLI、API 与 GUI 共享的查询服务 / Query service shared by CLI, API, and GUI.

    @param store 面向用例的可观测性读存储 / Use-case-oriented observability read store.
    @param policy 查询与 SLO 策略 / Query and SLO policy.
    @param clock 可注入时钟 / Injectable clock.
    """

    def __init__(
        self,
        store: ObservabilityReadStore,
        policy: DashboardQueryPolicy,
        *,
        clock: Clock | None = None,
    ) -> None:
        """@brief 创建查询服务 / Create the query service.

        @param store 可观测性读存储 / Observability read store.
        @param policy 查询策略 / Query policy.
        @param clock 可选 UTC 时钟 / Optional UTC clock.
        @return 新查询服务 / New query service.
        """

        self._store = store
        self._policy = policy
        self._clock = clock or _utc_now

    @property
    def policy(self) -> DashboardQueryPolicy:
        """@brief 返回不可变查询策略 / Return the immutable query policy.

        @return 当前查询策略 / Current query policy.
        """

        return self._policy

    async def overview(
        self,
        principal: OperatorPrincipal,
        scope: WorkspaceScope,
        *,
        window: TimeWindow | None = None,
        service: str | None = None,
    ) -> DashboardOverview:
        """@brief 查询准确的完整窗口 Overview / Query an accurate complete-window overview.

        @param principal 已认证运维主体 / Authenticated operator principal.
        @param scope 单工作区范围 / Single-workspace scope.
        @param window 可选窗口 / Optional window.
        @param service 可选服务过滤 / Optional service filter.
        @return 含 SLO、预算、新鲜度与服务明细的 Overview / Overview with SLO, budget, freshness, and services.
        """

        resolved_window = self._resolve_window(window)
        normalized_service = _normalize_service(service)
        rows = await self._store.fetch_overview(
            OverviewReadRequest(scope, resolved_window, normalized_service)
        )
        generated_at = self._now()
        services: list[ServiceOverview] = []
        for row in rows:
            assessment = assess_health(
                request_count=row.request_count,
                error_count=row.error_count,
                latency_p95_ms=row.latency_p95_ms,
                saturation_max=row.saturation_max,
                objective=self._policy.objective,
                window_duration=resolved_window.duration,
            )
            services.append(
                ServiceOverview(
                    service=row.service,
                    request_count=row.request_count,
                    error_count=row.error_count,
                    error_rate=assessment.error_rate,
                    latency_p50_ms=row.latency_p50_ms,
                    latency_p95_ms=row.latency_p95_ms,
                    latency_p99_ms=row.latency_p99_ms,
                    saturation_mean=row.saturation_mean,
                    saturation_max=row.saturation_max,
                    health=assessment.health,
                    reasons=assessment.reasons,
                    sample_count=row.sample_count,
                    latest_observed_at=row.latest_observed_at,
                )
            )

        ordered_services = tuple(sorted(services, key=lambda item: item.service))
        request_count = sum(item.request_count for item in ordered_services)
        error_count = sum(item.error_count for item in ordered_services)
        latest_observed = min(
            (item.latest_observed_at for item in ordered_services), default=None
        )
        max_collection_lag_seconds = max(
            (row.max_collection_lag_seconds for row in rows), default=None
        )
        freshness = _freshness(
            generated_at,
            resolved_window,
            latest_observed,
            max_collection_lag_seconds,
            self._policy.freshness_target,
        )
        aggregate = assess_health(
            request_count=request_count,
            error_count=error_count,
            latency_p95_ms=max(
                (item.latency_p95_ms for item in ordered_services if item.latency_p95_ms is not None),
                default=None,
            ),
            saturation_max=max(
                (item.saturation_max for item in ordered_services if item.saturation_max is not None),
                default=None,
            ),
            objective=self._policy.objective,
            window_duration=resolved_window.duration,
        )
        no_data_reason: NoDataReason | None = None
        has_saturation = any(
            item.saturation_max is not None for item in ordered_services
        )
        if request_count <= 0 and not has_saturation:
            no_data_reason = NoDataReason.FILTERED if normalized_service else NoDataReason.NO_TRAFFIC
        elif freshness.stale:
            no_data_reason = NoDataReason.STALE_TELEMETRY
        objective = self._policy.objective
        return DashboardOverview(
            principal=principal,
            scope=scope,
            window=resolved_window,
            generated_at=generated_at,
            health=(
                HealthStatus.NO_DATA
                if freshness.stale
                else _overall_health(item.health for item in ordered_services)
                if ordered_services
                else aggregate.health
            ),
            services=ordered_services,
            request_count=request_count,
            error_count=error_count,
            slo=SloSnapshot(
                availability_target=objective.availability_target,
                latency_target=objective.latency_target,
                latency_threshold_ms=objective.latency_threshold_ms,
                error_rate=aggregate.error_rate,
                burn_rate=aggregate.burn_rate,
                budget_remaining_ratio=aggregate.budget_remaining_ratio,
            ),
            freshness=freshness,
            no_data_reason=no_data_reason,
        )

    async def trends(
        self,
        principal: OperatorPrincipal,
        scope: WorkspaceScope,
        signal: SignalKind,
        *,
        window: TimeWindow | None = None,
        service: str | None = None,
        bucket_seconds: int | None = None,
    ) -> TrendReport:
        """@brief 查询由 SQL `date_bin` 生成的趋势 / Query trends generated by SQL ``date_bin``.

        @param principal 已认证运维主体 / Authenticated operator principal.
        @param scope 工作区范围 / Workspace scope.
        @param signal 目标黄金信号 / Target golden signal.
        @param window 可选窗口 / Optional window.
        @param service 可选服务过滤 / Optional service filter.
        @param bucket_seconds 可选显式桶宽 / Optional explicit bucket width.
        @return 有硬分辨率边界的趋势报告 / Trend report with hard resolution bounds.
        """

        if not isinstance(signal, SignalKind):
            raise DashboardQueryError("signal 必须是 SignalKind。")
        resolved_window = self._resolve_window(window)
        bucket = self._resolve_bucket(resolved_window, bucket_seconds)
        normalized_service = _normalize_service(service)
        rows = await self._store.fetch_trends(
            TrendReadRequest(scope, resolved_window, bucket, signal, normalized_service)
        )
        points = tuple(
            TrendPoint(
                bucket_start=row.bucket_start,
                service=row.service,
                request_count=row.request_count,
                error_count=row.error_count,
                error_rate=(
                    min(max(row.error_count / row.request_count, 0.0), 1.0)
                    if row.request_count > 0
                    else None
                ),
                latency_p50_ms=row.latency_p50_ms,
                latency_p95_ms=row.latency_p95_ms,
                latency_p99_ms=row.latency_p99_ms,
                saturation_mean=row.saturation_mean,
                saturation_max=row.saturation_max,
            )
            for row in rows
        )
        reason = None
        if not points:
            reason = NoDataReason.FILTERED if normalized_service else NoDataReason.NO_TRAFFIC
        return TrendReport(principal, scope, resolved_window, signal, bucket, points, reason)

    async def recent_events(
        self,
        principal: OperatorPrincipal,
        scope: WorkspaceScope,
        *,
        window: TimeWindow | None = None,
        service: str | None = None,
        limit: int = 100,
    ) -> EventReport:
        """@brief 查询有界诊断事件 / Query bounded diagnostic events.

        @param principal 已认证运维主体 / Authenticated operator principal.
        @param scope 工作区范围 / Workspace scope.
        @param window 可选窗口 / Optional window.
        @param service 可选服务过滤 / Optional service filter.
        @param limit 最大事件数 / Maximum event count.
        @return 逆时间排序的事件报告 / Reverse-chronological event report.
        """

        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= self._policy.max_event_limit:
            raise DashboardQueryError(
                f"limit 必须在 1 到 {self._policy.max_event_limit} 之间。"
            )
        resolved_window = self._resolve_window(window)
        normalized_service = _normalize_service(service)
        rows = await self._store.fetch_recent_events(
            EventReadRequest(scope, resolved_window, normalized_service, limit)
        )
        events = tuple(
            DiagnosticEvent(
                occurred_at=row.occurred_at,
                observed_at=row.observed_at,
                source=row.source,
                service=row.service,
                kind=row.kind,
                name=row.name,
                severity_number=row.severity_number,
                severity_text=row.severity_text,
                value=row.value,
                unit=row.unit,
                duration_ms=row.duration_ms,
                span_status=row.span_status,
                request_id=row.request_id,
                trace_id=row.trace_id,
                span_id=row.span_id,
                attributes=row.attributes,
            )
            for row in rows
        )
        reason = None
        if not events:
            reason = NoDataReason.FILTERED if normalized_service else NoDataReason.NO_TRAFFIC
        return EventReport(principal, scope, resolved_window, events, reason)

    async def system_health(
        self,
        principal: OperatorPrincipal,
        *,
        window: TimeWindow | None = None,
    ) -> SystemHealthReport:
        """@brief 查询不归因于 workspace 的 worker 管线健康 / Query worker-pipeline health not attributed to a workspace.

        @param principal 已认证 operator / Authenticated operator.
        @param window 可选窗口 / Optional window.
        @return 最新全局 self-health 报告 / Latest global self-health report.
        """

        resolved_window = self._resolve_window(window)
        row = await self._store.fetch_system_health(SystemHealthReadRequest(resolved_window))
        generated_at = self._now()
        if row is None:
            return SystemHealthReport(
                principal=principal,
                window=resolved_window,
                generated_at=generated_at,
                health=HealthStatus.NO_DATA,
                freshness=FreshnessSnapshot(
                    None,
                    None,
                    False,
                    _freshness_mode(generated_at, resolved_window, self._policy.freshness_target),
                ),
                accepted_count=None,
                dropped_count=None,
                write_failure_count=None,
                output_dropped_count=None,
                severity_text=None,
                no_data_reason=NoDataReason.NO_SYSTEM_HEALTH,
            )
        severity = row.severity_text.upper()
        snapshot_health = (
            HealthStatus.CRITICAL
            if row.severity_number >= 17
            else HealthStatus.DEGRADED
            if row.severity_number >= 13
            else HealthStatus.HEALTHY
        )
        freshness = _freshness(
            generated_at,
            resolved_window,
            row.observed_at,
            max(0.0, (row.observed_at - row.occurred_at).total_seconds()),
            self._policy.freshness_target,
        )
        return SystemHealthReport(
            principal=principal,
            window=resolved_window,
            generated_at=generated_at,
            health=HealthStatus.NO_DATA if freshness.stale else snapshot_health,
            freshness=freshness,
            accepted_count=row.accepted_count,
            dropped_count=row.dropped_count,
            write_failure_count=row.write_failure_count,
            output_dropped_count=row.output_dropped_count,
            severity_text=severity,
            no_data_reason=(
                NoDataReason.STALE_TELEMETRY if freshness.stale else None
            ),
        )

    def _resolve_window(self, window: TimeWindow | None) -> TimeWindow:
        """@brief 应用默认值并强制最大窗口 / Apply defaults and enforce the maximum window.

        @param window 可选调用方窗口 / Optional caller window.
        @return 有界窗口 / Bounded window.
        """

        resolved = window or TimeWindow.ending_at(self._now(), self._policy.default_window)
        if resolved.duration > self._policy.max_window:
            raise DashboardQueryError("查询窗口超过配置上限。")
        return resolved

    def _resolve_bucket(self, window: TimeWindow, requested: int | None) -> int:
        """@brief 选择可预测的 nice-duration 桶宽 / Select a predictable nice-duration bucket width.

        @param window 查询窗口 / Query window.
        @param requested 可选显式秒数 / Optional explicit seconds.
        @return 支持的桶宽秒数 / Supported bucket width in seconds.
        """

        minimum = max(1, ceil(window.duration.total_seconds() / self._policy.target_points))
        if requested is not None:
            if (
                isinstance(requested, bool)
                or not isinstance(requested, int)
                or not minimum <= requested <= MAX_BUCKET_SECONDS
            ):
                raise DashboardQueryError(
                    f"bucket_seconds 必须在 {minimum} 到 {MAX_BUCKET_SECONDS} 之间。"
                )
            return requested
        return next((value for value in _NICE_BUCKET_SECONDS if value >= minimum), _NICE_BUCKET_SECONDS[-1])

    def _now(self) -> datetime:
        """@brief 读取并校验 UTC 时钟 / Read and validate the UTC clock.

        @return UTC 当前时刻 / Current UTC time.
        """

        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise DashboardQueryError("Dashboard clock 必须返回带时区 datetime。")
        return now.astimezone(UTC)


def _utc_now() -> datetime:
    """@brief 返回当前 UTC 时刻 / Return the current UTC time.

    @return 当前 UTC datetime / Current UTC datetime.
    """

    return datetime.now(UTC)


def _normalize_service(service: str | None) -> str | None:
    """@brief 规范化可选服务过滤 / Normalize an optional service filter.

    @param service 可选服务名 / Optional service name.
    @return 修剪后的服务名或 None / Stripped service name or ``None``.
    """

    if service is None:
        return None
    if not isinstance(service, str) or not service.strip():
        raise DashboardQueryError("service 必须是非空字符串。")
    normalized = service.strip()
    if len(normalized) > 128 or not normalized.isprintable():
        raise DashboardQueryError("service 必须不超过 128 字符且不含控制字符。")
    return normalized


def _freshness(
    generated_at: datetime,
    window: TimeWindow,
    latest_observed_at: datetime | None,
    max_collection_lag_seconds: float | None,
    target: timedelta,
) -> FreshnessSnapshot:
    """@brief 区分实时新鲜度与历史采集延迟 / Distinguish live freshness from historical collection lag.

    @param generated_at 报告生成时刻 / Report generation time.
    @param window 查询窗口 / Query window.
    @param latest_observed_at 最近采集时刻 / Latest collection time.
    @param max_collection_lag_seconds 窗口内同一信号行的最坏采集延迟 / Worst collection lag from one signal row in the window.
    @param target 新鲜度目标 / Freshness objective.
    @return 新鲜度快照 / Freshness snapshot.
    """

    mode = _freshness_mode(generated_at, window, target)
    if latest_observed_at is None:
        return FreshnessSnapshot(None, None, False, mode)
    observed = latest_observed_at.astimezone(UTC)
    if mode is FreshnessMode.LIVE:
        lag = max(0.0, (generated_at - observed).total_seconds())
    elif max_collection_lag_seconds is None:
        lag = None
    else:
        lag = max(0.0, max_collection_lag_seconds)
    return FreshnessSnapshot(
        observed,
        lag,
        lag is not None and lag > target.total_seconds(),
        mode,
    )


def _freshness_mode(
    generated_at: datetime,
    window: TimeWindow,
    target: timedelta,
) -> FreshnessMode:
    """@brief 判断窗口是实时还是历史 / Determine whether a window is live or historical.

    @param generated_at 报告生成时刻 / Report generation time.
    @param window 查询窗口 / Query window.
    @param target 新鲜度容差 / Freshness tolerance.
    @return 新鲜度模式 / Freshness mode.
    """

    return (
        FreshnessMode.HISTORICAL
        if window.end_at < generated_at - target
        else FreshnessMode.LIVE
    )


def _overall_health(statuses: Iterable[HealthStatus]) -> HealthStatus:
    """@brief 折叠服务健康等级 / Fold service health levels.

    @param statuses 服务健康等级 / Service health levels.
    @return 最严重健康等级 / Most severe health level.
    """

    values = set(statuses)
    if HealthStatus.CRITICAL in values:
        return HealthStatus.CRITICAL
    if HealthStatus.DEGRADED in values:
        return HealthStatus.DEGRADED
    if HealthStatus.HEALTHY in values:
        return HealthStatus.HEALTHY
    return HealthStatus.NO_DATA


__all__ = [
    "MAX_BUCKET_SECONDS",
    "MAX_EVENT_LIMIT",
    "MAX_QUERY_WINDOW",
    "MAX_TARGET_POINTS",
    "Clock",
    "DashboardQueryPolicy",
    "DashboardQueryService",
]
