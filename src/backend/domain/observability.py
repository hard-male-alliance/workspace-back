"""@brief 可观测性信号领域模型 / Observability signal domain model.

该模块用判别联合表达 metric、log 与 span 的互斥语义。所有信号组合一个不可变
``SignalEnvelope``，从类型层消除旧式“任意 kind 加一组可空字段”的非法状态。
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from types import MappingProxyType
from typing import ClassVar

from workspace_shared.ids import new_opaque_id
from workspace_shared.tenancy import ActorScope

type AttributeValue = str | int | float | bool
"""@brief 允许持久化的低基数属性值 / Persistable low-cardinality attribute value."""

_STABLE_NAME = re.compile(r"[a-z][a-z0-9_.-]{0,127}")
"""@brief 稳定服务与事件名格式 / Stable service and event-name format."""

_TRACE_ID = re.compile(r"[0-9a-f]{32}")
"""@brief W3C trace-id 格式 / W3C trace-id format."""

_SPAN_ID = re.compile(r"[0-9a-f]{16}")
"""@brief W3C parent-id/span-id 格式 / W3C parent-id and span-id format."""

_ALLOWED_ATTRIBUTE_KEYS = frozenset(
    {
        "accepted_count",
        "capability",
        "close_code",
        "dropped_count",
        "error_code",
        "event_type",
        "job_type",
        "http.request.method",
        "http.response.status_code",
        "http.route",
        "level",
        "method",
        "metric_name",
        "operation",
        "output_dropped_count",
        "outcome",
        "provider",
        "release",
        "route",
        "stack_fingerprint",
        "status_class",
        "transport",
        "url.scheme",
        "write_failure_count",
    }
)
"""@brief 可查询低基数属性白名单 / Queryable low-cardinality attribute allowlist."""


class SignalKind(StrEnum):
    """@brief telemetry 信号种类 / Telemetry signal kinds."""

    METRIC = "metric"
    LOG = "log"
    SPAN = "span"


class SignalSource(StrEnum):
    """@brief telemetry 可信来源 / Trusted telemetry producers."""

    BACKEND = "backend"
    FRONTEND = "frontend"


class MetricType(StrEnum):
    """@brief metric 仪器语义 / Metric instrument semantics."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


class SpanStatus(StrEnum):
    """@brief OpenTelemetry span 状态 / OpenTelemetry span status."""

    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


class SeverityNumber(IntEnum):
    """@brief OpenTelemetry 日志严重度基准值 / OpenTelemetry log severity base numbers.

    @note OpenTelemetry 为每档预留四个数字；本系统使用各档的起始值，以稳定映射
    Python 日志等级：TRACE=1、DEBUG=5、INFO=9、WARN=13、ERROR=17、FATAL=21。
    """

    TRACE = 1
    DEBUG = 5
    INFO = 9
    WARN = 13
    ERROR = 17
    FATAL = 21


@dataclass(frozen=True, slots=True)
class ResourceMetadata:
    """@brief 产生信号的资源元数据 / Resource metadata producing a signal.

    @param service 稳定服务名 / Stable service name.
    @param service_version 服务发布版本 / Service release version.
    @param deployment_environment 部署环境 / Deployment environment.
    @param service_instance_id 进程实例标识 / Process instance identifier.
    """

    service: str
    service_version: str | None = None
    deployment_environment: str | None = None
    service_instance_id: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验资源字段边界 / Validate resource-field bounds.

        @raise ValueError 服务名或资源标签不安全时抛出 / Raised for unsafe resource labels.
        """
        _require_stable_name(self.service, "service")
        for label, value in (
            ("service_version", self.service_version),
            ("deployment_environment", self.deployment_environment),
            ("service_instance_id", self.service_instance_id),
        ):
            if value is not None and (not value or len(value) > 128 or _has_control(value)):
                raise ValueError(f"{label} must be a non-empty safe string of at most 128 chars")


@dataclass(frozen=True, slots=True)
class SignalEnvelope:
    """@brief 三类信号共享的不可变信封 / Immutable envelope shared by all signal kinds.

    @param event_id 服务端生成的幂等事件 ID / Server-generated idempotent event ID.
    @param source 信号来源 / Signal producer.
    @param occurred_at 事件发生时间 / Event occurrence time.
    @param observed_at 服务端接收时间 / Server observation time.
    @param resource 资源元数据 / Resource metadata.
    @param name 稳定事件或仪器名 / Stable event or instrument name.
    @param scope 可选租户范围 / Optional tenant scope.
    @param request_id 请求关联 ID / Request correlation ID.
    @param trace_id W3C trace ID / W3C trace ID.
    @param span_id W3C span ID / W3C span ID.
    @param parent_span_id W3C parent span ID / W3C parent span ID.
    @param client_event_id 前端幂等 ID / Frontend idempotency ID.
    @param attributes 低基数结构化属性 / Low-cardinality structured attributes.
    """

    source: SignalSource
    resource: ResourceMetadata
    name: str
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: new_opaque_id("tel"))
    scope: ActorScope | None = None
    request_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    client_event_id: str | None = None
    attributes: Mapping[str, AttributeValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """@brief 强制信封、关联和低基数约束 / Enforce envelope, correlation and cardinality constraints.

        @raise ValueError 任一信封字段不满足持久化契约时抛出。
        / Raised when any envelope field violates the persistence contract.
        """
        _require_short_safe(self.event_id, "event_id", 128)
        _require_stable_name(self.name, "name")
        _require_aware(self.occurred_at, "occurred_at")
        _require_aware(self.observed_at, "observed_at")
        for label, value in (("request_id", self.request_id), ("client_event_id", self.client_event_id)):
            if value is not None:
                _require_short_safe(value, label, 128)
        _validate_trace_context(self.trace_id, self.span_id, self.parent_span_id)
        if self.source is SignalSource.FRONTEND:
            if self.scope is None or self.client_event_id is None:
                raise ValueError("frontend signals require scope and client_event_id")
        elif self.client_event_id is not None:
            raise ValueError("backend signals cannot carry client_event_id")
        sanitized = _validated_attributes(self.attributes)
        object.__setattr__(self, "attributes", MappingProxyType(sanitized))


@dataclass(frozen=True, slots=True)
class MetricPoint:
    """@brief 一个 counter/gauge/histogram 观测点 / A counter, gauge, or histogram observation.

    @param envelope 公共信封 / Common envelope.
    @param metric_type 仪器类型 / Instrument type.
    @param value 有限数值 / Finite numeric value.
    @param unit UCUM 风格单位 / UCUM-style unit.
    """

    kind: ClassVar[SignalKind] = SignalKind.METRIC
    envelope: SignalEnvelope
    metric_type: MetricType
    value: float
    unit: str

    def __post_init__(self) -> None:
        """@brief 校验 metric 数值与单位 / Validate metric value and unit.

        @raise ValueError 数值非有限或单位不安全时抛出 / Raised for invalid values or units.
        """
        if isinstance(self.value, bool) or not math.isfinite(float(self.value)):
            raise ValueError("metric value must be finite")
        _require_short_safe(self.unit, "unit", 32)


@dataclass(frozen=True, slots=True)
class LogEvent:
    """@brief 不含自由文本的稳定日志事件 / Stable log event without free-form text.

    @param envelope 公共信封；name 即稳定事件名 / Envelope whose name is the stable event name.
    @param severity_number OpenTelemetry 严重度数字 / OpenTelemetry severity number.
    @param severity_text 规范化严重度文本 / Normalized severity text.
    """

    kind: ClassVar[SignalKind] = SignalKind.LOG
    envelope: SignalEnvelope
    severity_number: SeverityNumber
    severity_text: str

    def __post_init__(self) -> None:
        """@brief 校验日志严重度文本 / Validate log severity text.

        @raise ValueError 严重度文本不安全时抛出 / Raised for unsafe severity text.
        """
        _require_short_safe(self.severity_text, "severity_text", 16)


@dataclass(frozen=True, slots=True)
class SpanEvent:
    """@brief 一个已结束的 server/internal span / A completed server or internal span.

    @param envelope 含合法 trace/span ID 的公共信封 / Envelope with valid trace/span IDs.
    @param duration_ms 非负持续时间（毫秒）/ Non-negative duration in milliseconds.
    @param status OpenTelemetry span 状态 / OpenTelemetry span status.
    """

    kind: ClassVar[SignalKind] = SignalKind.SPAN
    envelope: SignalEnvelope
    duration_ms: float
    status: SpanStatus

    def __post_init__(self) -> None:
        """@brief 校验 span 因果与耗时 / Validate span causality and duration.

        @raise ValueError 缺少 trace/span ID 或耗时非法时抛出。
        / Raised for missing trace/span IDs or invalid duration.
        """
        if self.envelope.trace_id is None or self.envelope.span_id is None:
            raise ValueError("span signals require trace_id and span_id")
        if (
            isinstance(self.duration_ms, bool)
            or not math.isfinite(float(self.duration_ms))
            or self.duration_ms < 0
        ):
            raise ValueError("span duration_ms must be finite and non-negative")


type TelemetrySignal = MetricPoint | LogEvent | SpanEvent
"""@brief 可持久化 telemetry 判别联合 / Persistable telemetry discriminated union."""


def severity_from_logging_level(level: int) -> SeverityNumber:
    """@brief 将 Python 日志等级映射到 OpenTelemetry / Map Python logging level to OpenTelemetry.

    @param level Python ``LogRecord.levelno`` / Python ``LogRecord.levelno``.
    @return 对应的严重度基准值 / Corresponding base severity number.
    """
    if level >= 50:
        return SeverityNumber.FATAL
    if level >= 40:
        return SeverityNumber.ERROR
    if level >= 30:
        return SeverityNumber.WARN
    if level >= 20:
        return SeverityNumber.INFO
    if level >= 10:
        return SeverityNumber.DEBUG
    return SeverityNumber.TRACE


def _validated_attributes(
    attributes: Mapping[str, AttributeValue],
) -> dict[str, AttributeValue]:
    """@brief 复制并校验低基数 attributes / Copy and validate low-cardinality attributes.

    @param attributes 候选属性 / Candidate attributes.
    @return 与调用方隔离的已校验副本 / Validated copy isolated from the caller.
    @raise ValueError 键不在白名单或值越界时抛出 / Raised for unknown or unsafe values.
    """
    if len(attributes) > 16:
        raise ValueError("telemetry attributes cannot contain more than 16 entries")
    sanitized: dict[str, AttributeValue] = {}
    for key, value in attributes.items():
        if key not in _ALLOWED_ATTRIBUTE_KEYS:
            raise ValueError(f"telemetry attribute key is not allowed: {key}")
        if isinstance(value, str):
            if not value or len(value) > 256 or _has_control(value):
                raise ValueError(f"telemetry string attribute is unsafe: {key}")
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"telemetry numeric attribute must be finite: {key}")
        elif not isinstance(value, (bool, int, float)):
            raise ValueError(f"telemetry attribute has unsupported type: {key}")
        sanitized[key] = value
    return sanitized


def _validate_trace_context(
    trace_id: str | None,
    span_id: str | None,
    parent_span_id: str | None,
) -> None:
    """@brief 校验 W3C trace/span 关联 / Validate W3C trace/span correlation.

    @param trace_id trace ID / Trace ID.
    @param span_id 当前 span ID / Current span ID.
    @param parent_span_id 父 span ID / Parent span ID.
    @raise ValueError ID 非法、全零或缺少配对字段时抛出 / Raised for malformed correlation.
    """
    if (trace_id is None) != (span_id is None):
        raise ValueError("trace_id and span_id must be supplied together")
    if trace_id is not None and (
        _TRACE_ID.fullmatch(trace_id) is None or trace_id == "0" * 32
    ):
        raise ValueError("trace_id must be a non-zero lowercase 32-hex value")
    if span_id is not None and (_SPAN_ID.fullmatch(span_id) is None or span_id == "0" * 16):
        raise ValueError("span_id must be a non-zero lowercase 16-hex value")
    if parent_span_id is not None:
        if trace_id is None or _SPAN_ID.fullmatch(parent_span_id) is None or parent_span_id == "0" * 16:
            raise ValueError("parent_span_id requires a valid trace context")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 要求时区感知时间 / Require a timezone-aware timestamp.

    @param value 候选时间 / Candidate timestamp.
    @param label 错误字段名 / Error field label.
    @raise ValueError 时间缺少时区时抛出 / Raised for a naive timestamp.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")


def _require_stable_name(value: str, label: str) -> None:
    """@brief 要求稳定低基数名称 / Require a stable low-cardinality name.

    @param value 候选名称 / Candidate name.
    @param label 错误字段名 / Error field label.
    @raise ValueError 名称不符合约束时抛出 / Raised for an invalid name.
    """
    if _STABLE_NAME.fullmatch(value) is None:
        raise ValueError(f"{label} must match {_STABLE_NAME.pattern!r}")


def _require_short_safe(value: str, label: str, maximum: int) -> None:
    """@brief 要求有界无控制字符字符串 / Require a bounded string without control characters.

    @param value 候选字符串 / Candidate string.
    @param label 错误字段名 / Error field label.
    @param maximum 最大字符数 / Maximum number of characters.
    @raise ValueError 字符串为空、过长或含控制字符时抛出。
    / Raised for empty, oversized, or control-bearing strings.
    """
    if not value or len(value) > maximum or _has_control(value):
        raise ValueError(f"{label} must be a non-empty safe string of at most {maximum} chars")


def _has_control(value: str) -> bool:
    """@brief 检测日志注入控制字符 / Detect log-injection control characters.

    @param value 待检查字符串 / String to inspect.
    @return 存在控制字符时为真 / True when control characters are present.
    """
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


__all__ = [
    "AttributeValue",
    "LogEvent",
    "MetricPoint",
    "MetricType",
    "ResourceMetadata",
    "SeverityNumber",
    "SignalEnvelope",
    "SignalKind",
    "SignalSource",
    "SpanEvent",
    "SpanStatus",
    "TelemetrySignal",
    "severity_from_logging_level",
]
