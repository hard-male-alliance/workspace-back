"""@brief Dashboard DTO 的稳定呈现模型 / Stable presentation models for Dashboard DTOs."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from dashboard.application.dto import (
    DashboardOverview,
    DiagnosticEvent,
    EventReport,
    ServiceOverview,
    SystemHealthReport,
    TrendPoint,
    TrendReport,
)

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
"""@brief JSON 可表达值 / JSON-representable value."""


def overview_payload(report: DashboardOverview) -> dict[str, JsonValue]:
    """@brief 将 Overview DTO 转为稳定 JSON 模型 / Convert an overview DTO to a stable JSON model.

    @param report Overview 应用 DTO / Overview application DTO.
    @return 可供 CLI、API 与报告复用的对象 / Object reusable by CLI, API, and reports.
    """

    return {
        "principal": {"operator_id": report.principal.operator_id},
        "scope": {"workspace_id": report.scope.workspace_id},
        "window": _window(report.window.start_at, report.window.end_at),
        "generated_at": _timestamp(report.generated_at),
        "health": report.health.value,
        "request_count": report.request_count,
        "error_count": report.error_count,
        "slo": {
            "availability_target": report.slo.availability_target,
            "latency_target": report.slo.latency_target,
            "latency_threshold_ms": report.slo.latency_threshold_ms,
            "error_rate": report.slo.error_rate,
            "error_budget_burn_rate": report.slo.burn_rate,
            "estimated_error_budget_remaining_ratio": report.slo.budget_remaining_ratio,
        },
        "freshness": {
            "last_observed_at": _optional_timestamp(report.freshness.last_observed_at),
            "lag_seconds": report.freshness.lag_seconds,
            "stale": report.freshness.stale,
            "mode": report.freshness.mode.value,
        },
        "no_data_reason": _enum_value(report.no_data_reason),
        "services": [service_payload(item) for item in report.services],
    }


def service_payload(service: ServiceOverview) -> dict[str, JsonValue]:
    """@brief 将服务摘要转为稳定 JSON 模型 / Convert a service summary to a stable JSON model.

    @param service 服务摘要 DTO / Service-summary DTO.
    @return JSON 对象 / JSON object.
    """

    return {
        "service": service.service,
        "health": service.health.value,
        "request_count": service.request_count,
        "error_count": service.error_count,
        "error_rate": service.error_rate,
        "latency_p50_ms": service.latency_p50_ms,
        "latency_p95_ms": service.latency_p95_ms,
        "latency_p99_ms": service.latency_p99_ms,
        "saturation_mean": service.saturation_mean,
        "saturation_max": service.saturation_max,
        "sample_count": service.sample_count,
        "latest_observed_at": _timestamp(service.latest_observed_at),
        "reasons": list(service.reasons),
    }


def trend_report_payload(report: TrendReport) -> dict[str, JsonValue]:
    """@brief 将趋势 DTO 转为稳定 JSON 模型 / Convert a trend DTO to a stable JSON model.

    @param report 趋势应用 DTO / Trend application DTO.
    @return JSON 对象 / JSON object.
    """

    return {
        "principal": {"operator_id": report.principal.operator_id},
        "scope": {"workspace_id": report.scope.workspace_id},
        "window": _window(report.window.start_at, report.window.end_at),
        "signal": report.signal.value,
        "bucket_seconds": report.bucket_seconds,
        "no_data_reason": _enum_value(report.no_data_reason),
        "points": [_trend_point_payload(point) for point in report.points],
    }


def event_report_payload(report: EventReport) -> dict[str, JsonValue]:
    """@brief 将诊断事件 DTO 转为稳定 JSON 模型 / Convert diagnostic-event DTOs to a stable JSON model.

    @param report 事件应用 DTO / Event application DTO.
    @return JSON 对象 / JSON object.
    """

    return {
        "principal": {"operator_id": report.principal.operator_id},
        "scope": {"workspace_id": report.scope.workspace_id},
        "window": _window(report.window.start_at, report.window.end_at),
        "no_data_reason": _enum_value(report.no_data_reason),
        "events": [_event_payload(event) for event in report.events],
    }


def system_health_payload(report: SystemHealthReport) -> dict[str, JsonValue]:
    """@brief 序列化 operator-only 系统健康 / Serialize operator-only system health.

    @param report 系统健康应用 DTO / System-health application DTO.
    @return 稳定 JSON 对象 / Stable JSON object.
    """

    return {
        "principal": {"operator_id": report.principal.operator_id},
        "scope": {"kind": "system"},
        "window": _window(report.window.start_at, report.window.end_at),
        "generated_at": _timestamp(report.generated_at),
        "health": report.health.value,
        "freshness": {
            "last_observed_at": _optional_timestamp(report.freshness.last_observed_at),
            "lag_seconds": report.freshness.lag_seconds,
            "stale": report.freshness.stale,
            "mode": report.freshness.mode.value,
        },
        "accepted_count": report.accepted_count,
        "dropped_count": report.dropped_count,
        "write_failure_count": report.write_failure_count,
        "output_dropped_count": report.output_dropped_count,
        "severity_text": report.severity_text,
        "no_data_reason": _enum_value(report.no_data_reason),
    }


def _trend_point_payload(point: TrendPoint) -> dict[str, JsonValue]:
    """@brief 序列化一个趋势点 / Serialize one trend point.

    @param point 趋势点 / Trend point.
    @return JSON 对象 / JSON object.
    """

    return {
        "bucket_start": _timestamp(point.bucket_start),
        "service": point.service,
        "request_count": point.request_count,
        "error_count": point.error_count,
        "error_rate": point.error_rate,
        "latency_p50_ms": point.latency_p50_ms,
        "latency_p95_ms": point.latency_p95_ms,
        "latency_p99_ms": point.latency_p99_ms,
        "saturation_mean": point.saturation_mean,
        "saturation_max": point.saturation_max,
    }


def _event_payload(event: DiagnosticEvent) -> dict[str, JsonValue]:
    """@brief 序列化一个诊断事件 / Serialize one diagnostic event.

    @param event 诊断事件 / Diagnostic event.
    @return JSON 对象 / JSON object.
    """

    return {
        "occurred_at": _timestamp(event.occurred_at),
        "observed_at": _timestamp(event.observed_at),
        "source": event.source,
        "service": event.service,
        "kind": event.kind,
        "name": event.name,
        "severity_number": event.severity_number,
        "severity_text": event.severity_text,
        "value": event.value,
        "unit": event.unit,
        "duration_ms": event.duration_ms,
        "span_status": event.span_status,
        "request_id": event.request_id,
        "trace_id": event.trace_id,
        "span_id": event.span_id,
        "attributes": _json_mapping(event.attributes),
    }


def _window(start_at: datetime, end_at: datetime) -> dict[str, JsonValue]:
    """@brief 序列化半开窗口 / Serialize a half-open window.

    @param start_at 窗口起点 / Window start.
    @param end_at 窗口终点 / Window end.
    @return JSON 对象 / JSON object.
    """

    return {"start_at": _timestamp(start_at), "end_at": _timestamp(end_at)}


def _json_mapping(value: object) -> dict[str, JsonValue]:
    """@brief 防御性规范化 JSON 属性 / Defensively normalize JSON attributes.

    @param value JSON-like 值 / JSON-like value.
    @return 字符串键 JSON 对象 / String-keyed JSON object.
    """

    if not isinstance(value, Mapping):
        return {}
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: object) -> JsonValue:
    """@brief 将未知属性收敛为安全 JSON 值 / Coerce an unknown attribute to a safe JSON value.

    @param value 未知值 / Unknown value.
    @return JSON 值 / JSON value.
    """

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


def _timestamp(value: datetime) -> str:
    """@brief 序列化 UTC 时间 / Serialize a UTC timestamp.

    @param value 带时区时间 / Timezone-aware time.
    @return RFC 3339 文本 / RFC 3339 text.
    """

    return value.isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    """@brief 序列化可空时间 / Serialize a nullable timestamp.

    @param value 可空时间 / Nullable time.
    @return RFC 3339 文本或 None / RFC 3339 text or ``None``.
    """

    return None if value is None else _timestamp(value)


def _enum_value(value: object) -> str | None:
    """@brief 序列化可空字符串枚举 / Serialize a nullable string enum.

    @param value 可空 StrEnum / Nullable ``StrEnum``.
    @return 枚举值或 None / Enum value or ``None``.
    """

    if value is None:
        return None
    rendered = getattr(value, "value", None)
    return rendered if isinstance(rendered, str) else str(value)


__all__ = [
    "JsonValue",
    "event_report_payload",
    "overview_payload",
    "service_payload",
    "system_health_payload",
    "trend_report_payload",
]
