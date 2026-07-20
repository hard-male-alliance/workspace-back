"""@brief 入口共享的不可变 Dashboard DTO / Immutable Dashboard DTOs shared by entry adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType

from dashboard.domain.model import (
    FreshnessMode,
    HealthStatus,
    NoDataReason,
    OperatorPrincipal,
    SignalKind,
    TimeWindow,
    WorkspaceScope,
)


@dataclass(frozen=True, slots=True)
class SloSnapshot:
    """@brief 当前窗口的 SLO 与错误预算快照 / Current-window SLO and error-budget snapshot.

    @param availability_target 可用性目标 / Availability target.
    @param latency_target 延迟目标比例 / Latency target proportion.
    @param latency_threshold_ms 延迟阈值毫秒 / Latency threshold in milliseconds.
    @param error_rate 当前窗口错误率 / Current-window error ratio.
    @param burn_rate 错误预算燃烧率 / Error-budget burn rate.
    @param budget_remaining_ratio 假设当前流量与错误率稳定延续时的 SLO 周期剩余预算估算 / SLO-period budget estimate assuming current traffic and error ratio remain steady.
    """

    availability_target: float
    latency_target: float
    latency_threshold_ms: float
    error_rate: float | None
    burn_rate: float | None
    budget_remaining_ratio: float | None


@dataclass(frozen=True, slots=True)
class FreshnessSnapshot:
    """@brief 遥测新鲜度快照 / Telemetry freshness snapshot.

    @param last_observed_at 最近观测时刻 / Most recent observation time.
    @param lag_seconds 实时窗口距生成时刻的延迟，或历史窗口的最坏采集延迟 / Lag from generation time for live windows, or worst collection lag for historical windows.
    @param stale 是否超过新鲜度目标 / Whether the freshness objective was exceeded.
    @param mode 实时 wall-clock 或历史采集延迟语义 / Live wall-clock or historical collection-lag semantics.
    """

    last_observed_at: datetime | None
    lag_seconds: float | None
    stale: bool
    mode: FreshnessMode


@dataclass(frozen=True, slots=True)
class ServiceOverview:
    """@brief 单服务完整窗口摘要 / Complete-window summary for one service.

    @param service 稳定服务名 / Stable service name.
    @param request_count 请求总数 / Total requests.
    @param error_count 错误总数 / Total errors.
    @param error_rate 错误比例 / Error ratio.
    @param latency_p50_ms p50 延迟 / p50 latency.
    @param latency_p95_ms p95 延迟 / p95 latency.
    @param latency_p99_ms p99 延迟 / p99 latency.
    @param saturation_mean 平均饱和度 / Mean saturation.
    @param saturation_max 最大饱和度 / Maximum saturation.
    @param health 健康等级 / Health level.
    @param reasons 健康判断原因 / Health-assessment reasons.
    @param sample_count 聚合前样本数 / Number of source observations aggregated by SQL.
    @param latest_observed_at 最近观测时刻 / Latest observation time.
    """

    service: str
    request_count: float
    error_count: float
    error_rate: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    saturation_mean: float | None
    saturation_max: float | None
    health: HealthStatus
    reasons: tuple[str, ...]
    sample_count: int
    latest_observed_at: datetime


@dataclass(frozen=True, slots=True)
class DashboardOverview:
    """@brief Overview 与 services 视图共享的应用 DTO / Application DTO shared by overview and services views.

    @param principal 已认证运维主体 / Authenticated operator principal.
    @param scope 工作区范围 / Workspace scope.
    @param window 查询窗口 / Query window.
    @param generated_at 生成时刻 / Generation time.
    @param health 汇总健康状态 / Aggregate health state.
    @param services 服务摘要 / Service summaries.
    @param request_count 汇总请求数 / Aggregate request count.
    @param error_count 汇总错误数 / Aggregate error count.
    @param slo SLO 与错误预算快照 / SLO and error-budget snapshot.
    @param freshness 遥测新鲜度 / Telemetry freshness.
    @param no_data_reason 可解释空状态 / Explainable empty-state reason.
    """

    principal: OperatorPrincipal
    scope: WorkspaceScope
    window: TimeWindow
    generated_at: datetime
    health: HealthStatus
    services: tuple[ServiceOverview, ...]
    request_count: float
    error_count: float
    slo: SloSnapshot
    freshness: FreshnessSnapshot
    no_data_reason: NoDataReason | None


@dataclass(frozen=True, slots=True)
class TrendPoint:
    """@brief SQL 时间桶聚合点 / SQL time-bucket aggregate point.

    @param bucket_start UTC 桶起点 / UTC bucket start.
    @param service 稳定服务名 / Stable service name.
    @param request_count 桶内请求数 / Requests in the bucket.
    @param error_count 桶内错误数 / Errors in the bucket.
    @param error_rate 桶内错误率 / Error ratio in the bucket.
    @param latency_p50_ms 桶内 p50 延迟 / Bucket p50 latency.
    @param latency_p95_ms 桶内 p95 延迟 / Bucket p95 latency.
    @param latency_p99_ms 桶内 p99 延迟 / Bucket p99 latency.
    @param saturation_mean 桶内平均饱和度 / Bucket mean saturation.
    @param saturation_max 桶内最大饱和度 / Bucket maximum saturation.
    """

    bucket_start: datetime
    service: str
    request_count: float
    error_count: float
    error_rate: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    saturation_mean: float | None
    saturation_max: float | None


@dataclass(frozen=True, slots=True)
class TrendReport:
    """@brief 一个黄金信号趋势视图 / One golden-signal trend view.

    @param principal 已认证运维主体 / Authenticated operator principal.
    @param scope 工作区范围 / Workspace scope.
    @param window 查询窗口 / Query window.
    @param signal 用户选择的信号 / User-selected signal.
    @param bucket_seconds SQL 分桶宽度秒数 / SQL bucket width in seconds.
    @param points 时间序列点 / Time-series points.
    @param no_data_reason 可解释空状态 / Explainable empty-state reason.
    """

    principal: OperatorPrincipal
    scope: WorkspaceScope
    window: TimeWindow
    signal: SignalKind
    bucket_seconds: int
    points: tuple[TrendPoint, ...]
    no_data_reason: NoDataReason | None


@dataclass(frozen=True, slots=True)
class DiagnosticEvent:
    """@brief 已脱敏诊断事件 / Sanitized diagnostic event.

    @param occurred_at 业务发生时刻 / Business occurrence time.
    @param observed_at 采集入库时刻 / Collection time.
    @param source 信号来源 / Signal source.
    @param service 稳定服务名 / Stable service name.
    @param kind log 或 span / ``log`` or ``span``.
    @param name 稳定事件名 / Stable event name.
    @param severity_number 可选 OpenTelemetry 严重度数值 / Optional OpenTelemetry severity number.
    @param severity_text 可选严重度文本 / Optional severity text.
    @param value metric 数值 / Metric value.
    @param unit metric 规范单位 / Metric canonical unit.
    @param duration_ms span 时长毫秒 / Span duration in milliseconds.
    @param span_status span 状态 / Span status.
    @param request_id 可选请求关联标识 / Optional request correlation identifier.
    @param trace_id 可选 trace 标识 / Optional trace identifier.
    @param span_id 可选 span 标识 / Optional span identifier.
    @param attributes 低基数脱敏属性 / Low-cardinality sanitized attributes.
    """

    occurred_at: datetime
    observed_at: datetime
    source: str
    service: str
    kind: str
    name: str
    severity_number: int | None
    severity_text: str | None
    value: float | None
    unit: str | None
    duration_ms: float | None
    span_status: str | None
    request_id: str | None
    trace_id: str | None
    span_id: str | None
    attributes: Mapping[str, object]

    def __post_init__(self) -> None:
        """@brief 冻结属性映射 / Freeze the attribute mapping.

        @return 无返回值 / No return value.
        """

        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


@dataclass(frozen=True, slots=True)
class EventReport:
    """@brief 最近诊断事件报告 / Recent diagnostic-event report.

    @param principal 已认证运维主体 / Authenticated operator principal.
    @param scope 工作区范围 / Workspace scope.
    @param window 查询窗口 / Query window.
    @param events 逆时间排序的事件 / Reverse-chronological events.
    @param no_data_reason 可解释空状态 / Explainable empty-state reason.
    """

    principal: OperatorPrincipal
    scope: WorkspaceScope
    window: TimeWindow
    events: tuple[DiagnosticEvent, ...]
    no_data_reason: NoDataReason | None


@dataclass(frozen=True, slots=True)
class SystemHealthReport:
    """@brief operator-only worker 管线健康报告 / Operator-only worker-pipeline health report.

    @param principal 已认证运维主体 / Authenticated operator principal.
    @param window 查询窗口 / Query window.
    @param generated_at 报告生成时刻 / Report generation time.
    @param health 当前系统健康等级 / Current system health status.
    @param freshness 实时或历史采集新鲜度 / Live or historical collection freshness.
    @param accepted_count 累计准入信号数 / Cumulative admitted signals.
    @param dropped_count 累计管线丢弃数 / Cumulative pipeline drops.
    @param write_failure_count 累计持久化失败信号数 / Cumulative persistence failures.
    @param output_dropped_count 累计日志输出丢弃数 / Cumulative logging-output drops.
    @param severity_text 最新快照严重度 / Latest snapshot severity.
    @param no_data_reason 可解释空态 / Explainable empty state.
    """

    principal: OperatorPrincipal
    window: TimeWindow
    generated_at: datetime
    health: HealthStatus
    freshness: FreshnessSnapshot
    accepted_count: int | None
    dropped_count: int | None
    write_failure_count: int | None
    output_dropped_count: int | None
    severity_text: str | None
    no_data_reason: NoDataReason | None


__all__ = [
    "DashboardOverview",
    "DiagnosticEvent",
    "EventReport",
    "FreshnessSnapshot",
    "ServiceOverview",
    "SloSnapshot",
    "SystemHealthReport",
    "TrendPoint",
    "TrendReport",
]
