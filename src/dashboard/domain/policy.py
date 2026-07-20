"""@brief SLO 与健康判断的纯策略 / Pure SLO and health-assessment policies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .model import DashboardDomainError, HealthStatus, ServiceLevelObjective


@dataclass(frozen=True, slots=True)
class ReliabilityAssessment:
    """@brief 一次可解释的可靠性判断 / One explainable reliability assessment.

    @param health 健康等级 / Health level.
    @param error_rate 可用性 SLI 的错误比例 / Error ratio for the availability SLI.
    @param burn_rate 相对错误预算的燃烧速率 / Burn rate relative to the error budget.
    @param budget_remaining_ratio 假设窗口流量与错误率稳定延续时的周期剩余预算估算 / Period budget remaining estimate assuming steady traffic and error ratio.
    @param reasons 触发当前等级的稳定原因 / Stable reasons producing the current level.
    """

    health: HealthStatus
    error_rate: float | None
    burn_rate: float | None
    budget_remaining_ratio: float | None
    reasons: tuple[str, ...]


def assess_health(
    *,
    request_count: float,
    error_count: float,
    latency_p95_ms: float | None,
    saturation_max: float | None,
    objective: ServiceLevelObjective,
    window_duration: timedelta,
) -> ReliabilityAssessment:
    """@brief 将完整窗口信号映射为可解释 SLO 状态 / Map complete-window signals to an explainable SLO state.

    @param request_count 窗口请求总数 / Total requests in the window.
    @param error_count 窗口错误请求数 / Erroneous requests in the window.
    @param latency_p95_ms 窗口 p95 延迟 / Window p95 latency.
    @param saturation_max 窗口最大饱和度 / Maximum window saturation.
    @param objective 服务级别目标 / Service-level objective.
    @param window_duration 参与周期预算估算的窗口长度 / Window duration used by the period-budget estimate.
    @return 可靠性判断及原因 / Reliability assessment and reasons.

    @note 错误预算燃烧率 ``r=e/(1-S)``；剩余预算采用 steady-traffic estimate
    ``R=max(0,1-r*min(W/P,1))``。其中 ``e`` 为错误率，``S`` 为可用性目标，``W`` 为
    查询窗口，``P`` 为 SLO 周期。/ Error-budget burn rate is ``r=e/(1-S)``; remaining budget
    uses the steady-traffic estimate ``R=max(0,1-r*min(W/P,1))``, where ``e`` is error ratio,
    ``S`` the availability target, ``W`` the query window, and ``P`` the SLO period.
    """

    if not isinstance(window_duration, timedelta) or window_duration <= timedelta(0):
        raise DashboardDomainError("window_duration 必须是正 timedelta。")
    if request_count <= 0:
        if saturation_max is not None:
            if saturation_max >= 0.95:
                health = HealthStatus.CRITICAL
                resource_reasons = ("saturation_high",)
            elif saturation_max >= 0.80:
                health = HealthStatus.DEGRADED
                resource_reasons = ("saturation_high",)
            else:
                health = HealthStatus.HEALTHY
                resource_reasons = ("saturation_within_capacity",)
            return ReliabilityAssessment(
                health=health,
                error_rate=None,
                burn_rate=None,
                budget_remaining_ratio=None,
                reasons=resource_reasons,
            )
        return ReliabilityAssessment(
            health=HealthStatus.NO_DATA,
            error_rate=None,
            burn_rate=None,
            budget_remaining_ratio=None,
            reasons=("no_traffic",),
        )

    error_rate = error_count / request_count
    reasons: list[str] = []
    if error_rate < 0 or error_rate > 1:
        reasons.append("telemetry_inconsistent")
        error_rate = min(max(error_rate, 0.0), 1.0)
    error_budget = 1.0 - objective.availability_target
    burn_rate = error_rate / error_budget
    window_fraction = min(window_duration / objective.period, 1.0)
    budget_remaining = max(0.0, 1.0 - burn_rate * window_fraction)

    critical = burn_rate >= 10.0 or (
        saturation_max is not None and saturation_max >= 0.95
    )
    degraded = burn_rate >= 1.0 or (
        latency_p95_ms is not None and latency_p95_ms > objective.latency_threshold_ms
    ) or (saturation_max is not None and saturation_max >= 0.80)

    if burn_rate >= 1.0:
        reasons.append("availability_error_budget_burning")
    if latency_p95_ms is not None and latency_p95_ms > objective.latency_threshold_ms:
        reasons.append("latency_objective_missed")
    if saturation_max is not None and saturation_max >= 0.80:
        reasons.append("saturation_high")
    if not reasons:
        reasons.append("within_objective")

    if critical:
        health = HealthStatus.CRITICAL
    elif degraded:
        health = HealthStatus.DEGRADED
    else:
        health = HealthStatus.HEALTHY
    return ReliabilityAssessment(
        health=health,
        error_rate=error_rate,
        burn_rate=burn_rate,
        budget_remaining_ratio=budget_remaining,
        reasons=tuple(reasons),
    )


def percentile_health(
    latency_p95_ms: float | None,
    objective: ServiceLevelObjective,
) -> bool | None:
    """@brief 判断 p95 是否满足延迟阈值 / Determine whether p95 meets the latency threshold.

    @param latency_p95_ms 可选 p95 延迟 / Optional p95 latency.
    @param objective 服务级别目标 / Service-level objective.
    @return 无数据时为 None，否则返回是否满足阈值 / ``None`` without data, otherwise whether the threshold is met.
    """

    if latency_p95_ms is None:
        return None
    return latency_p95_ms <= objective.latency_threshold_ms


__all__ = ["ReliabilityAssessment", "assess_health", "percentile_health"]
