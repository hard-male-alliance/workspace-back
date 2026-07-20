"""@brief 前端诊断信号的应用层准入与映射 / Application admission and mapping for frontend diagnostics."""

from __future__ import annotations

import asyncio
import math
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Literal

from backend.config import DiagnosticsSettings
from backend.domain.observability import AttributeValue, MetricType, SeverityNumber, SignalSource
from backend.domain.ports import ObservabilityRecorder
from workspace_shared.tenancy import ActorScope


@dataclass(frozen=True, slots=True)
class ClientErrorDiagnostic:
    """@brief 已验证的浏览器错误诊断 / Validated browser error diagnostic."""

    client_event_id: str
    occurred_at: datetime
    route: str
    release: str
    error_code: str
    stack_fingerprint: str | None


@dataclass(frozen=True, slots=True)
class ClientPerformanceDiagnostic:
    """@brief 已验证的浏览器性能诊断 / Validated browser performance diagnostic."""

    client_event_id: str
    occurred_at: datetime
    route: str
    release: str
    metric_name: str
    value: float
    unit: Literal["ms", "1"]


@dataclass(frozen=True, slots=True)
class ClientNetworkDiagnostic:
    """@brief 已验证的浏览器网络诊断 / Validated browser network diagnostic."""

    client_event_id: str
    occurred_at: datetime
    route: str
    release: str
    operation: str
    duration_ms: float
    status_code: int


type ClientDiagnostic = (
    ClientErrorDiagnostic | ClientPerformanceDiagnostic | ClientNetworkDiagnostic
)
"""@brief 浏览器诊断判别联合 / Browser-diagnostic discriminated union."""


@dataclass(slots=True)
class _TokenBucket:
    """@brief 单 ActorScope token bucket 状态 / Token-bucket state for one ActorScope."""

    tokens: float
    updated_at: float


class DiagnosticRateLimiter:
    """@brief 有界 ActorScope token bucket 注册表 / Bounded ActorScope token-bucket registry."""

    def __init__(self, settings: DiagnosticsSettings) -> None:
        """@brief 初始化准入预算 / Initialize admission budgets.

        @param settings 已验证诊断预算 / Validated diagnostics budgets.
        """
        self._capacity = float(settings.rate_limit_capacity)
        self._refill_per_second = settings.rate_limit_refill_per_minute / 60.0
        self._max_buckets = settings.max_actor_buckets
        self._buckets: OrderedDict[tuple[str, str, str], _TokenBucket] = OrderedDict()
        self._lock = asyncio.Lock()

    async def consume(self, scope: ActorScope, cost: int) -> int | None:
        """@brief 消耗事件 token 或返回重试秒数 / Consume event tokens or return retry seconds.

        @param scope 已认证 ActorScope / Authenticated ActorScope.
        @param cost 本批事件数 / Number of events in this batch.
        @return 成功时 None；超限时正整数 Retry-After / None on success, positive Retry-After on limit.
        """
        now = monotonic()
        key = (scope.workspace_id, scope.resource_owner_id, scope.actor_id)
        async with self._lock:
            bucket = self._buckets.pop(key, None)
            if bucket is None:
                if len(self._buckets) >= self._max_buckets:
                    self._buckets.popitem(last=False)
                bucket = _TokenBucket(self._capacity, now)
            else:
                elapsed = max(0.0, now - bucket.updated_at)
                bucket.tokens = min(
                    self._capacity, bucket.tokens + elapsed * self._refill_per_second
                )
                bucket.updated_at = now
            self._buckets[key] = bucket
            if cost <= bucket.tokens:
                bucket.tokens -= cost
                return None
            deficit = cost - bucket.tokens
            return max(1, math.ceil(deficit / self._refill_per_second))


class DiagnosticIngestionService:
    """@brief 将客户端 DTO 映射为服务端控制的 frontend signals / Map client DTOs to server-controlled frontend signals."""

    def __init__(
        self,
        settings: DiagnosticsSettings,
        telemetry: ObservabilityRecorder,
        limiter: DiagnosticRateLimiter,
    ) -> None:
        """@brief 初始化诊断用例 / Initialize the diagnostics use case.

        @param settings 载荷与时钟预算 / Payload and clock budgets.
        @param telemetry 强类型信号管线 / Strongly typed signal pipeline.
        @param limiter ActorScope 限流器 / ActorScope rate limiter.
        """
        self._settings = settings
        self._telemetry = telemetry
        self._limiter = limiter

    async def retry_after(self, scope: ActorScope, event_count: int) -> int | None:
        """@brief 执行一批事件的限流准入 / Rate-limit one event batch.

        @param scope 已认证 ActorScope / Authenticated ActorScope.
        @param event_count 批内事件数 / Event count in the batch.
        @return 成功时 None；超限时 Retry-After 秒数 / None on success or Retry-After seconds.
        """
        return await self._limiter.consume(scope, event_count)

    def ingest(
        self,
        scope: ActorScope,
        request_id: str,
        events: tuple[ClientDiagnostic, ...],
    ) -> tuple[int, int]:
        """@brief 验证客户端时间并非阻塞提交信号 / Validate client time and non-blockingly submit signals.

        @param scope 服务端身份边界 / Server-derived identity scope.
        @param request_id 服务端请求 ID / Server request ID.
        @param events 已经过 DTO 白名单的事件 / DTO-allowlisted events.
        @return ``(accepted, dropped)`` / ``(accepted, dropped)``.
        @raise ValueError 事件时间超出可信窗口时抛出 / Raised when event time is outside the trust window.
        """
        now = datetime.now(UTC)
        oldest = now - timedelta(seconds=self._settings.max_event_age_seconds)
        newest = now + timedelta(seconds=self._settings.max_future_skew_seconds)
        for event in events:
            if event.occurred_at < oldest or event.occurred_at > newest:
                raise ValueError("diagnostic occurred_at is outside the accepted clock window")
        accepted = 0
        for event in events:
            accepted += int(self._record(scope, request_id, event))
        return accepted, len(events) - accepted

    def observe_ingestion(
        self,
        scope: ActorScope,
        request_id: str,
        *,
        payload_bytes: int,
        accepted: int,
        dropped: int,
        rate_limited: int,
        duration_seconds: float,
    ) -> None:
        """@brief 非阻塞记录诊断准入自身指标 / Non-blockingly record diagnostic-admission metrics.

        @param scope 服务端认证的完整 ActorScope / Complete server-authenticated ActorScope.
        @param request_id 服务端请求 ID / Server request ID.
        @param payload_bytes 已读取的原始 JSON 字节数 / Raw JSON bytes read.
        @param accepted 已进入 telemetry 队列的客户端事件数 / Client events admitted to telemetry.
        @param dropped 因 telemetry 背压或关闭而丢弃的客户端事件数 / Client events dropped by telemetry pressure or closure.
        @param rate_limited 被 token bucket 拒绝的客户端事件数 / Client events rejected by the token bucket.
        @param duration_seconds 从入口到准入决定的单调时钟耗时 / Monotonic duration from ingress to admission decision.
        @return 无返回值 / No return value.

        @note 只使用固定 instrument 与 ``outcome`` 枚举；不记录 route、release、actor ID
        等高基数维度。所有调用都走有界 ``ObservabilityRecorder``，不会执行持久化 I/O。
        / Only fixed instruments and outcome values are used; all writes go through the bounded
        recorder and never perform persistence I/O on the request path.
        """
        outcome: Literal["accepted", "partial", "dropped", "rate_limited"]
        if rate_limited:
            outcome = "rate_limited"
        elif accepted and dropped:
            outcome = "partial"
        elif accepted:
            outcome = "accepted"
        else:
            outcome = "dropped"
        attributes: dict[str, AttributeValue] = {
            "operation": "ingest",
            "outcome": outcome,
        }
        self._telemetry.record_metric(
            "aiws.diagnostics.ingest.payload.size",
            payload_bytes,
            scope,
            request_id,
            attributes,
            service="backend.api",
            metric_type=MetricType.HISTOGRAM,
            unit="By",
        )
        self._telemetry.record_metric(
            "aiws.diagnostics.ingest.batch.count",
            1,
            scope,
            request_id,
            attributes,
            service="backend.api",
            metric_type=MetricType.COUNTER,
            unit="{batch}",
        )
        self._telemetry.record_metric(
            "aiws.diagnostics.ingest.duration",
            max(0.0, duration_seconds),
            scope,
            request_id,
            attributes,
            service="backend.api",
            metric_type=MetricType.HISTOGRAM,
            unit="s",
        )
        for event_outcome, event_count in (
            ("accepted", accepted),
            ("dropped", dropped),
            ("rate_limited", rate_limited),
        ):
            if event_count <= 0:
                continue
            self._telemetry.record_metric(
                "aiws.diagnostics.ingest.event.count",
                event_count,
                scope,
                request_id,
                {"operation": "ingest", "outcome": event_outcome},
                service="backend.api",
                metric_type=MetricType.COUNTER,
                unit="{event}",
            )

    def _record(
        self,
        scope: ActorScope,
        request_id: str,
        event: ClientDiagnostic,
    ) -> bool:
        """@brief 将一个诊断映射为固定服务端信号 / Map one diagnostic to a fixed server signal.

        @return 成功进入管线时为真 / True when admitted to the pipeline.
        """
        common = {
            "event_type": (
                "error"
                if isinstance(event, ClientErrorDiagnostic)
                else "performance"
                if isinstance(event, ClientPerformanceDiagnostic)
                else "network"
            ),
            "route": event.route,
            "release": event.release,
        }
        if isinstance(event, ClientErrorDiagnostic):
            attributes: dict[str, AttributeValue] = {
                **common,
                "error_code": event.error_code,
            }
            if event.stack_fingerprint is not None:
                attributes["stack_fingerprint"] = event.stack_fingerprint
            return self._telemetry.record_log(
                "aiws.frontend.error",
                SeverityNumber.ERROR,
                "ERROR",
                scope,
                request_id,
                attributes,
                service="frontend.browser",
                source=SignalSource.FRONTEND,
                client_event_id=event.client_event_id,
                occurred_at=event.occurred_at,
            )
        if isinstance(event, ClientPerformanceDiagnostic):
            value = event.value / 1_000 if event.unit == "ms" else event.value
            unit = "s" if event.unit == "ms" else "1"
            return self._telemetry.record_metric(
                "aiws.frontend.web_vital",
                value,
                scope,
                request_id,
                {**common, "metric_name": event.metric_name},
                service="frontend.browser",
                metric_type=MetricType.HISTOGRAM,
                unit=unit,
                source=SignalSource.FRONTEND,
                client_event_id=event.client_event_id,
                occurred_at=event.occurred_at,
            )
        outcome = "success" if 100 <= event.status_code < 400 else "failure"
        return self._telemetry.record_metric(
            "aiws.frontend.network.request.duration",
            event.duration_ms / 1_000,
            scope,
            request_id,
            {
                **common,
                "operation": event.operation,
                "outcome": outcome,
                "status_class": "network_error" if event.status_code == 0 else f"{event.status_code // 100}xx",
            },
            service="frontend.browser",
            metric_type=MetricType.HISTOGRAM,
            unit="s",
            source=SignalSource.FRONTEND,
            client_event_id=event.client_event_id,
            occurred_at=event.occurred_at,
        )


__all__ = [
    "ClientDiagnostic",
    "ClientErrorDiagnostic",
    "ClientNetworkDiagnostic",
    "ClientPerformanceDiagnostic",
    "DiagnosticIngestionService",
    "DiagnosticRateLimiter",
]
