"""@brief Dashboard 的纯领域值对象 / Pure Dashboard domain value objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from math import isfinite


class DashboardDomainError(ValueError):
    """@brief 领域不变量被破坏 / A Dashboard domain invariant was violated.

    @note 本异常不包含数据库、HTTP 或 GUI 细节 / This exception contains no database, HTTP, or GUI details.
    """


class SignalKind(StrEnum):
    """@brief 用户可查询的黄金信号视图 / User-queryable golden-signal views."""

    TRAFFIC = "traffic"
    LATENCY = "latency"
    ERRORS = "errors"
    SATURATION = "saturation"


class HealthStatus(StrEnum):
    """@brief 面向运维者的健康等级 / Operator-facing health levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    NO_DATA = "no_data"


class NoDataReason(StrEnum):
    """@brief 缺少可用读模型时的可解释原因 / Explainable reasons for missing read-model data."""

    NO_TRAFFIC = "no_traffic"
    FILTERED = "filtered"
    STALE_TELEMETRY = "stale_telemetry"
    NO_SYSTEM_HEALTH = "no_system_health"


class FreshnessMode(StrEnum):
    """@brief 遥测新鲜度的时间语义 / Time semantics used for telemetry freshness."""

    LIVE = "live"
    HISTORICAL = "historical"


@dataclass(frozen=True, slots=True)
class OperatorPrincipal:
    """@brief 已认证运维主体 / Authenticated operator principal.

    @param operator_id 由可信入口确定的稳定主体标识 / Stable principal identifier resolved by a trusted boundary.
    """

    operator_id: str

    def __post_init__(self) -> None:
        """@brief 规范化运维主体标识 / Normalize the operator identifier.

        @return 无返回值 / No return value.
        """

        if not isinstance(self.operator_id, str) or not self.operator_id.strip():
            raise DashboardDomainError("operator_id 必须是非空字符串。")
        object.__setattr__(self, "operator_id", self.operator_id.strip())


@dataclass(frozen=True, slots=True)
class WorkspaceScope:
    """@brief 单工作区数据范围 / Single-workspace data scope.

    @param workspace_id 必须参与每个数据库谓词的租户标识 / Tenant identifier required in every database predicate.
    """

    workspace_id: str

    def __post_init__(self) -> None:
        """@brief 规范化租户范围 / Normalize the tenant scope.

        @return 无返回值 / No return value.
        """

        if not isinstance(self.workspace_id, str) or not self.workspace_id.strip():
            raise DashboardDomainError("workspace_id 必须是非空字符串。")
        normalized = self.workspace_id.strip()
        if len(normalized) > 128 or not normalized.isprintable():
            raise DashboardDomainError("workspace_id 必须不超过 128 字符且不含控制字符。")
        object.__setattr__(self, "workspace_id", normalized)


@dataclass(frozen=True, slots=True)
class TimeWindow:
    """@brief UTC 半开查询窗口 / UTC half-open query window.

    @param start_at 含时区的窗口起点 / Timezone-aware inclusive window start.
    @param end_at 含时区的窗口终点 / Timezone-aware exclusive window end.
    """

    start_at: datetime
    end_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验并统一窗口到 UTC / Validate and normalize the window to UTC.

        @return 无返回值 / No return value.
        """

        if not isinstance(self.start_at, datetime) or not isinstance(self.end_at, datetime):
            raise DashboardDomainError("时间窗口边界必须是 datetime。")
        if self.start_at.tzinfo is None or self.end_at.tzinfo is None:
            raise DashboardDomainError("时间窗口边界必须携带时区。")
        start_at = self.start_at.astimezone(UTC)
        end_at = self.end_at.astimezone(UTC)
        if start_at >= end_at:
            raise DashboardDomainError("时间窗口起点必须早于终点。")
        object.__setattr__(self, "start_at", start_at)
        object.__setattr__(self, "end_at", end_at)

    @property
    def duration(self) -> timedelta:
        """@brief 返回窗口长度 / Return the window duration.

        @return 正的时间跨度 / Positive window duration.
        """

        return self.end_at - self.start_at

    @classmethod
    def ending_at(cls, end_at: datetime, duration: timedelta) -> TimeWindow:
        """@brief 从终点和长度创建窗口 / Build a window from its end and duration.

        @param end_at 含时区的窗口终点 / Timezone-aware window end.
        @param duration 正的窗口长度 / Positive window duration.
        @return 规范化的半开窗口 / Normalized half-open window.
        """

        if not isinstance(duration, timedelta) or duration <= timedelta(0):
            raise DashboardDomainError("duration 必须是正 timedelta。")
        return cls(end_at - duration, end_at)


@dataclass(frozen=True, slots=True)
class ServiceLevelObjective:
    """@brief Dashboard 用于解释可靠性的 SLO / SLO used to interpret reliability.

    @param availability_target 成功请求目标比例 / Target proportion of successful requests.
    @param latency_target 达到延迟阈值的请求目标比例 / Target proportion of requests meeting the latency threshold.
    @param latency_threshold_ms 延迟 SLI 阈值，单位毫秒 / Latency SLI threshold in milliseconds.
    @param period 误差预算评估周期 / Error-budget evaluation period.
    """

    availability_target: float = 0.99
    latency_target: float = 0.95
    latency_threshold_ms: float = 1_000.0
    period: timedelta = timedelta(days=30)

    def __post_init__(self) -> None:
        """@brief 校验 SLO 数值范围 / Validate SLO numeric ranges.

        @return 无返回值 / No return value.
        """

        for field_name in ("availability_target", "latency_target"):
            value = float(getattr(self, field_name))
            if not isfinite(value) or not 0 < value < 1:
                raise DashboardDomainError(f"{field_name} 必须位于 0 与 1 之间。")
            object.__setattr__(self, field_name, value)
        if self.latency_target != 0.95:
            raise DashboardDomainError("当前 latency SLI 只支持 p95，latency_target 必须为 0.95。")
        threshold = float(self.latency_threshold_ms)
        if not isfinite(threshold) or threshold <= 0:
            raise DashboardDomainError("latency_threshold_ms 必须是正有限数值。")
        if not isinstance(self.period, timedelta) or self.period <= timedelta(0):
            raise DashboardDomainError("SLO period 必须是正 timedelta。")
        object.__setattr__(self, "latency_threshold_ms", threshold)


__all__ = [
    "DashboardDomainError",
    "FreshnessMode",
    "HealthStatus",
    "NoDataReason",
    "OperatorPrincipal",
    "ServiceLevelObjective",
    "SignalKind",
    "TimeWindow",
    "WorkspaceScope",
]
