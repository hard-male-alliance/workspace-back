"""@brief Dashboard 应用层读取端口 / Dashboard application-layer read ports."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from dashboard.domain.model import SignalKind, TimeWindow, WorkspaceScope


@dataclass(frozen=True, slots=True)
class OverviewReadRequest:
    """@brief SQL 完整窗口聚合请求 / SQL complete-window aggregation request.

    @param scope 工作区范围 / Workspace scope.
    @param window 半开时间窗口 / Half-open time window.
    @param service 可选服务过滤 / Optional service filter.
    """

    scope: WorkspaceScope
    window: TimeWindow
    service: str | None = None


@dataclass(frozen=True, slots=True)
class ServiceSignalRow:
    """@brief PostgreSQL 聚合后的服务信号行 / Service-signal row aggregated by PostgreSQL.

    @param max_collection_lag_seconds 窗口内同一信号行的最坏采集延迟 / Worst collection lag from one signal row in the window.
    @param latest_observed_at 窗口内最近采集时刻 / Latest collection time in the window.
    """

    service: str
    request_count: float
    error_count: float
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    saturation_mean: float | None
    saturation_max: float | None
    sample_count: int
    max_collection_lag_seconds: float
    latest_observed_at: datetime


@dataclass(frozen=True, slots=True)
class TrendReadRequest:
    """@brief SQL `date_bin` 趋势读取请求 / SQL ``date_bin`` trend-read request.

    @param scope 工作区范围 / Workspace scope.
    @param window 半开时间窗口 / Half-open time window.
    @param bucket_seconds 分桶宽度秒数 / Bucket width in seconds.
    @param signal 只选择当前视图所需指标 / Select only metrics needed by the active view.
    @param service 可选服务过滤 / Optional service filter.
    """

    scope: WorkspaceScope
    window: TimeWindow
    bucket_seconds: int
    signal: SignalKind
    service: str | None = None


@dataclass(frozen=True, slots=True)
class TrendSignalRow:
    """@brief PostgreSQL 分桶后的趋势行 / Trend row bucketed by PostgreSQL."""

    bucket_start: datetime
    service: str
    request_count: float
    error_count: float
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    saturation_mean: float | None
    saturation_max: float | None


@dataclass(frozen=True, slots=True)
class EventReadRequest:
    """@brief 最近诊断事件读取请求 / Recent diagnostic-event read request.

    @param scope 工作区范围 / Workspace scope.
    @param window 半开时间窗口 / Half-open time window.
    @param service 可选服务过滤 / Optional service filter.
    @param limit 最大事件数 / Maximum event count.
    """

    scope: WorkspaceScope
    window: TimeWindow
    service: str | None
    limit: int


@dataclass(frozen=True, slots=True)
class DiagnosticEventRow:
    """@brief 读存储返回的脱敏诊断事件行 / Sanitized diagnostic-event row returned by the read store."""

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


@dataclass(frozen=True, slots=True)
class SystemHealthReadRequest:
    """@brief 无租户归因的系统健康读取请求 / System-health request without tenant attribution.

    @param window 半开时间窗口 / Half-open time window.
    """

    window: TimeWindow


@dataclass(frozen=True, slots=True)
class SystemHealthRow:
    """@brief 最新 worker 管线健康快照 / Latest worker-pipeline health snapshot."""

    occurred_at: datetime
    observed_at: datetime
    severity_number: int
    severity_text: str
    accepted_count: int
    dropped_count: int
    write_failure_count: int
    output_dropped_count: int


class ObservabilityReadStore(Protocol):
    """@brief 面向 Dashboard 用例的只读存储 / Use-case-oriented read-only Dashboard store."""

    async def fetch_overview(self, request: OverviewReadRequest) -> Sequence[ServiceSignalRow]:
        """@brief 在数据库内完成完整窗口聚合 / Aggregate the complete window in the database.

        @param request 有界聚合请求 / Bounded aggregation request.
        @return 每个服务一行的完整聚合 / Complete aggregate with one row per service.
        """

        ...

    async def fetch_trends(self, request: TrendReadRequest) -> Sequence[TrendSignalRow]:
        """@brief 在数据库内完成时间分桶 / Bucket the time series in the database.

        @param request 有界趋势请求 / Bounded trend request.
        @return 按时间与服务排序的分桶行 / Bucket rows ordered by time and service.
        """

        ...

    async def fetch_recent_events(
        self,
        request: EventReadRequest,
    ) -> Sequence[DiagnosticEventRow]:
        """@brief 读取有硬上限的最近诊断事件 / Read hard-bounded recent diagnostic events.

        @param request 含 limit 的事件请求 / Event request containing a limit.
        @return 逆时间排序的事件行 / Reverse-chronological event rows.
        """

        ...

    async def fetch_system_health(
        self,
        request: SystemHealthReadRequest,
    ) -> SystemHealthRow | None:
        """@brief 读取 operator-only 全局管线健康 / Read operator-only global pipeline health.

        @param request 无 workspace 的有界请求 / Bounded request without a workspace.
        @return 最新快照或 None / Latest snapshot or ``None``.
        """

        ...


__all__ = [
    "DiagnosticEventRow",
    "EventReadRequest",
    "ObservabilityReadStore",
    "OverviewReadRequest",
    "ServiceSignalRow",
    "SystemHealthReadRequest",
    "SystemHealthRow",
    "TrendReadRequest",
    "TrendSignalRow",
]
