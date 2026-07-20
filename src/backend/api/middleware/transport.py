"""@brief ASGI transport 终态观测适配器 / ASGI transport terminal-observation adapter."""

from __future__ import annotations

import logging
import threading
from asyncio import CancelledError
from datetime import UTC, datetime
from time import perf_counter
from typing import cast

from fastapi import Request
from starlette.requests import ClientDisconnect, HTTPConnection
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from backend.api.middleware.context import (
    _install_transport_context,
    _with_correlation_headers,
)
from backend.composition import BackendContainer
from backend.domain.observability import MetricType, SpanStatus
from backend.infrastructure.observability.context import (
    ObservabilityContext,
    ServerTraceContext,
    bind_observability_context,
)
from workspace_shared.tenancy import ActorScope

logger = logging.getLogger("backend.app")
"""@brief HTTP 边界稳定事件 logger / Stable-event logger for the HTTP boundary."""


class TransportTelemetryMiddleware:
    """@brief 在 ASGI 终态记录 HTTP 流与 WebSocket 生命周期 / Observe HTTP streams and WebSocket lifecycles at ASGI termination.

    @note HTTP 只有在最后一个 ``http.response.body`` 成功发送后才结束；建流时刻只是
    TTFB（Time to First Byte），不得伪装成 ``http.server.request.duration``。
    """

    def __init__(self, app: ASGIApp) -> None:
        """@brief 包装下游 ASGI 应用 / Wrap the downstream ASGI application.

        @param app 下游应用 / Downstream application.
        """

        self._app = app
        self._active_lock = threading.Lock()
        self._active_http_requests = 0
        self._active_websocket_connections = 0

    def _change_active(self, scope: Scope, transport: str, delta: int) -> None:
        """@brief 原子更新并持久化 worker-local active gauge / Atomically update and persist a worker-local active gauge.

        @param scope 当前 ASGI scope / Current ASGI scope.
        @param transport ``http`` 或 ``websocket`` / ``http`` or ``websocket``.
        @param delta 只能为 +1 或 -1 / Must be +1 or -1.
        """

        with self._active_lock:
            if transport == "http":
                self._active_http_requests = max(0, self._active_http_requests + delta)
                value = self._active_http_requests
            else:
                self._active_websocket_connections = max(
                    0, self._active_websocket_connections + delta
                )
                value = self._active_websocket_connections
        _record_active_transport_gauge(scope, transport, value)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """@brief 根据 transport 执行终态观测 / Observe a terminal state according to transport.

        @param scope ASGI connection scope / ASGI connection scope.
        @param receive ASGI receive callable / ASGI receive callable.
        @param send ASGI send callable / ASGI send callable.
        """

        if scope["type"] == "http":
            _install_transport_context(scope)
            track_active = scope.get("path") != "/_internal/healthz"
            if track_active:
                self._change_active(scope, "http", 1)
            try:
                await self._observe_http(scope, receive, send)
            finally:
                if track_active:
                    self._change_active(scope, "http", -1)
            return
        if scope["type"] == "websocket":
            _install_transport_context(scope)
            await self._observe_websocket(scope, receive, send)
            return
        await self._app(scope, receive, send)

    async def _observe_http(self, scope: Scope, receive: Receive, send: Send) -> None:
        """@brief 在最后 body 或失败时结束 HTTP span / End an HTTP span at the final body or failure.

        @param scope HTTP scope / HTTP scope.
        @param receive ASGI receive callable / ASGI receive callable.
        @param send 上游 send callable / Upstream send callable.
        """

        started = perf_counter()
        wall_start = datetime.now(UTC)
        status_code = 500
        completed = False
        peer_disconnected = False
        send_disconnected = False

        async def observe_receive() -> Message:
            """@brief 捕获 ASGI 2.3 流取消使用的 peer disconnect / Capture peer disconnect used to cancel ASGI 2.3 streams.

            @return 下一个请求消息 / Next request message.
            """

            nonlocal peer_disconnected
            message = await receive()
            if message["type"] == "http.disconnect":
                peer_disconnected = True
            return message

        async def observe_send(message: Message) -> None:
            """@brief 观察 response-start 与最终 body / Observe response start and final body.

            @param message 下游 ASGI 消息 / Downstream ASGI message.
            """

            nonlocal completed, send_disconnected, status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                message = _with_correlation_headers(scope, message)
            try:
                await send(message)
            except OSError:
                send_disconnected = True
                raise
            if (
                message["type"] == "http.response.body"
                and not bool(message.get("more_body", False))
                and not completed
            ):
                completed = True
                _observe_http_scope_completion(
                    scope,
                    499 if peer_disconnected else status_code,
                    started,
                    wall_start,
                )

        try:
            await self._app(scope, observe_receive, observe_send)
        except CancelledError:
            if not completed:
                completed = True
                _observe_http_scope_completion(scope, 499, started, wall_start)
            raise
        except ClientDisconnect:
            if not completed:
                completed = True
                _observe_http_scope_completion(scope, 499, started, wall_start)
            raise
        except OSError:
            if not completed:
                completed = True
                _observe_http_scope_completion(
                    scope, 499 if send_disconnected else 500, started, wall_start
                )
            raise
        except BaseException:
            if not completed:
                completed = True
                _observe_http_scope_completion(scope, 500, started, wall_start)
            raise
        if not completed:
            _observe_http_scope_completion(
                scope,
                499 if peer_disconnected or send_disconnected else status_code,
                started,
                wall_start,
            )

    async def _observe_websocket(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        """@brief 记录 WebSocket 接受、断开、耗时和服务端失败 / Record WebSocket acceptance, disconnect, duration, and server failure.

        @param scope WebSocket scope / WebSocket scope.
        @param receive 下游 receive callable / Downstream receive callable.
        @param send 上游 send callable / Upstream send callable.
        """

        started = perf_counter()
        wall_start = datetime.now(UTC)
        accepted = False
        close_code: int | None = None
        server_error = False
        send_disconnected = False

        async def observe_receive() -> Message:
            """@brief 捕获 peer disconnect code / Capture the peer disconnect code.

            @return 下一个 ASGI 消息 / Next ASGI message.
            """

            nonlocal close_code
            message = await receive()
            if message["type"] == "websocket.disconnect":
                close_code = int(message.get("code", 1005))
            return message

        async def observe_send(message: Message) -> None:
            """@brief 捕获 accept 与 application close code / Capture accept and application close code.

            @param message 下游 ASGI 消息 / Downstream ASGI message.
            """

            nonlocal accepted, close_code, send_disconnected
            if message["type"] == "websocket.close":
                close_code = int(message.get("code", 1000))
            try:
                await send(message)
            except OSError:
                send_disconnected = True
                raise
            if message["type"] == "websocket.accept" and not accepted:
                accepted = True
                self._change_active(scope, "websocket", 1)

        try:
            await self._app(scope, observe_receive, observe_send)
        except CancelledError:
            close_code = close_code or 1001
            raise
        except OSError:
            if send_disconnected:
                close_code = 1006
            else:
                server_error = True
                close_code = 1011
            raise
        except BaseException:
            server_error = True
            close_code = 1011
            raise
        finally:
            effective_close_code = 1006 if send_disconnected else close_code or 1000
            try:
                _observe_websocket_scope_completion(
                    scope,
                    accepted,
                    effective_close_code,
                    server_error,
                    started,
                    wall_start,
                )
            finally:
                if accepted:
                    self._change_active(scope, "websocket", -1)


def _record_active_transport_gauge(
    scope: Scope,
    transport: str,
    value: int,
) -> None:
    """@brief 持久化 worker-local 活跃请求/连接数 / Persist worker-local active request/connection count.

    @param scope 当前 ASGI scope / Current ASGI scope.
    @param transport ``http`` 或 ``websocket`` / ``http`` or ``websocket``.
    @param value 当前非负活跃数 / Current non-negative active count.
    """

    app = scope.get("app")
    container = getattr(getattr(app, "state", None), "container", None)
    if container is None:
        return
    telemetry = cast(BackendContainer, container).telemetry
    is_http = transport == "http"
    telemetry.record_metric(
        "aiws.http.server.active_requests"
        if is_http
        else "aiws.websocket.server.active_connections",
        value,
        None,
        None,
        {"operation": "active", "transport": transport},
        service="backend.api",
        metric_type=MetricType.GAUGE,
        unit="{request}" if is_http else "{connection}",
    )


def _record_http_signals(
    container: BackendContainer,
    scope: ActorScope | None,
    request: Request,
    status_code: int,
    latency_ms: float,
    trace: ServerTraceContext,
    started_at: datetime,
) -> None:
    """@brief 写入 Google SRE 黄金信号与 server span / Write Google SRE signals and a server span.

    @param container 当前 worker 容器 / Current worker container.
    @param scope 已认证租户范围；认证前失败可为空 / Authenticated scope; nullable before identity.
    @param request HTTP 请求 / HTTP request.
    @param status_code 最终 HTTP 状态码 / Final HTTP status code.
    @param latency_ms 请求端到端耗时毫秒 / End-to-end request duration in milliseconds.
    @param trace 当前 W3C server span / Current W3C server span.
    @param started_at span UTC 开始时间 / Span UTC start time.
    @return 无返回值 / No return value.
    @note route 使用 Starlette 匹配后的模板，绝不记录原始 URL、query 或异常文本。
    """
    outcome = (
        "server_error"
        if status_code >= 500
        else "client_error"
        if status_code >= 400
        else "success"
    )
    route = _route_template(request)
    attributes: dict[str, str | int | float | bool] = {
        "http.request.method": request.method.upper(),
        "http.response.status_code": status_code,
        "http.route": route,
        "outcome": outcome,
        "url.scheme": request.url.scheme,
    }
    telemetry = container.telemetry
    telemetry.record_metric(
        "aiws.http.server.request.count",
        1,
        scope,
        request.state.request_id,
        attributes,
        service="backend.api",
        metric_type=MetricType.COUNTER,
        unit="{request}",
    )
    telemetry.record_metric(
        "http.server.request.duration",
        max(0.0, latency_ms) / 1_000,
        scope,
        request.state.request_id,
        attributes,
        service="backend.api",
        metric_type=MetricType.HISTOGRAM,
        unit="s",
    )
    telemetry.record_metric(
        "aiws.runtime.supervisor.utilization",
        container.supervisor.saturation,
        scope,
        request.state.request_id,
        {"method": request.method.upper(), "outcome": outcome, "route": route},
        service="backend.api",
        metric_type=MetricType.GAUGE,
        unit="1",
    )
    telemetry.record_metric(
        "aiws.telemetry.queue.utilization",
        telemetry.queue_utilization,
        scope,
        request.state.request_id,
        {"operation": "export", "outcome": "success"},
        service="backend.observability",
        metric_type=MetricType.GAUGE,
        unit="1",
    )
    if status_code >= 500:
        telemetry.record_metric(
            "aiws.http.server.error.count",
            1,
            scope,
            request.state.request_id,
            attributes,
            service="backend.api",
            metric_type=MetricType.COUNTER,
            unit="{error}",
        )
    telemetry.record_span(
        "http.server.request",
        max(0.0, latency_ms),
        SpanStatus.ERROR if status_code >= 500 else SpanStatus.UNSET,
        scope,
        request.state.request_id,
        attributes,
        trace_id=trace.trace_id,
        span_id=trace.span_id,
        parent_span_id=trace.parent_span_id,
        service="backend.api",
        occurred_at=started_at,
    )
    telemetry.record_health_snapshot(
        output_dropped_count=container.logging_runtime.dropped_output_count,
    )


def _route_template(request: Request) -> str:
    """@brief 提取低基数路由模板 / Extract the low-cardinality route template.

    @param request 已完成路由匹配的请求 / Request after routing.
    @return FastAPI route template；未匹配时为固定值 / Route template or a fixed unmatched value.
    """
    return _route_template_from_scope(request.scope)


def _route_template_from_scope(scope: Scope) -> str:
    """@brief 从 ASGI scope 提取低基数路由模板 / Extract a low-cardinality route template from an ASGI scope.

    @param scope 已路由 HTTP/WebSocket scope / Routed HTTP/WebSocket scope.
    @return route template 或固定 fallback / Route template or fixed fallback.
    """

    route = scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str) and path:
        return path
    state = scope.get("state")
    if isinstance(state, dict) and state.get("route_fallback") == "pre_auth":
        return "pre_auth"
    if scope["type"] == "http" and scope.get("method") == "OPTIONS":
        headers = HTTPConnection(scope).headers
        if "origin" in headers and "access-control-request-method" in headers:
            return "cors_preflight"
    return "unmatched"


def _log_http_completion(request: Request, status_code: int, latency_ms: float) -> None:
    """@brief 按状态类别发出稳定完成日志 / Emit a stable completion log by status class.

    @param request 当前请求 / Current request.
    @param status_code 最终状态码 / Final status code.
    @param latency_ms 端到端耗时 / End-to-end latency in milliseconds.
    """
    level = (
        logging.ERROR
        if status_code >= 500
        else logging.WARNING
        if status_code >= 400
        else logging.INFO
    )
    logger.log(
        level,
        "backend.http.request.completed",
        extra={
            "event_name": "backend.http.request.completed",
            "telemetry_attributes": {
                "method": request.method.upper(),
                "outcome": (
                    "server_error"
                    if status_code >= 500
                    else "client_error"
                    if status_code >= 400
                    else "success"
                ),
                "route": _route_template(request),
                "status_class": f"{status_code // 100}xx",
            },
            "duration_ms": max(0.0, latency_ms),
        },
    )


def log_http_start(request: Request, trace: ServerTraceContext) -> None:
    """@brief 在身份验证前记录全局请求开始 / Record a global request start before identity verification.

    @param request 当前请求 / Current request.
    @param trace 当前 server trace / Current server trace.
    """
    with bind_observability_context(ObservabilityContext(None, request.state.request_id, trace)):
        logger.debug(
            "backend.http.request.started",
            extra={
                "event_name": "backend.http.request.started",
                "telemetry_attributes": {
                    "method": request.method.upper(),
                    "operation": "request",
                    "outcome": "accepted",
                },
            },
        )


def _observe_http_completion(
    container: BackendContainer,
    scope: ActorScope | None,
    request: Request,
    status_code: int,
    trace: ServerTraceContext,
    monotonic_start: float,
    wall_start: datetime,
) -> None:
    """@brief 统一记录所有请求终态 / Uniformly observe every request terminal state.

    @param container 当前容器 / Current container.
    @param scope 已验证 scope；认证前失败为空 / Verified scope; null for pre-auth failures.
    @param request 当前请求 / Current request.
    @param status_code 最终状态码 / Final status code.
    @param trace 当前 server trace / Current server trace.
    @param monotonic_start 单调时钟开始值 / Monotonic start value.
    @param wall_start UTC span 开始时间 / UTC span start time.
    """
    latency_ms = (perf_counter() - monotonic_start) * 1_000
    with bind_observability_context(ObservabilityContext(scope, request.state.request_id, trace)):
        _record_http_signals(
            container,
            scope,
            request,
            status_code,
            latency_ms,
            trace,
            wall_start,
        )
        _log_http_completion(request, status_code, latency_ms)


def _observe_http_scope_completion(
    scope: Scope,
    status_code: int,
    monotonic_start: float,
    wall_start: datetime,
) -> None:
    """@brief 从完整 ASGI scope 记录 HTTP 终态 / Observe an HTTP terminal state from the completed ASGI scope.

    @param scope 已完成路由匹配的 HTTP scope / HTTP scope after route matching.
    @param status_code 最终状态；流失败强制为 500 / Final status, forced to 500 for stream failures.
    @param monotonic_start 单调时钟起点 / Monotonic-clock start.
    @param wall_start UTC span 起点 / UTC span start.
    """

    app = scope.get("app")
    container = getattr(getattr(app, "state", None), "container", None)
    if container is None or scope.get("path") == "/_internal/healthz":
        return
    state = scope.get("state")
    if not isinstance(state, dict):
        return
    trace = state.get("trace_context")
    request_id = state.get("request_id")
    if not isinstance(trace, ServerTraceContext) or not isinstance(request_id, str):
        return
    actor_scope = state.get("actor_scope")
    resolved_scope = actor_scope if isinstance(actor_scope, ActorScope) else None
    request = Request(scope)
    _observe_http_completion(
        cast(BackendContainer, container),
        resolved_scope,
        request,
        status_code,
        trace,
        monotonic_start,
        wall_start,
    )


def _observe_websocket_scope_completion(
    scope: Scope,
    accepted: bool,
    close_code: int,
    server_error: bool,
    monotonic_start: float,
    wall_start: datetime,
) -> None:
    """@brief 持久化一个 WebSocket 连接终态 / Persist one WebSocket connection terminal state.

    @param scope 已完成路由的 WebSocket scope / Routed WebSocket scope.
    @param accepted 是否完成握手 / Whether the handshake was accepted.
    @param close_code 最终 WebSocket close code / Final WebSocket close code.
    @param server_error 是否由未处理服务端异常终止 / Whether an unhandled server error terminated it.
    @param monotonic_start 单调时钟起点 / Monotonic-clock start.
    @param wall_start UTC span 起点 / UTC span start.
    """

    app = scope.get("app")
    container = getattr(getattr(app, "state", None), "container", None)
    state = scope.get("state")
    if container is None or not isinstance(state, dict):
        return
    trace = state.get("trace_context")
    request_id = state.get("request_id")
    if not isinstance(trace, ServerTraceContext) or not isinstance(request_id, str):
        return
    actor_scope = state.get("actor_scope")
    resolved_scope = actor_scope if isinstance(actor_scope, ActorScope) else None
    latency_ms = max(0.0, (perf_counter() - monotonic_start) * 1_000)
    outcome, is_server_error = _websocket_outcome(
        accepted,
        close_code,
        server_error,
    )
    attributes: dict[str, str | int | float | bool] = {
        "close_code": close_code,
        "outcome": outcome,
        "route": _route_template_from_scope(scope),
        "status_class": outcome,
        "transport": "websocket",
    }
    resolved_container = cast(BackendContainer, container)
    telemetry = resolved_container.telemetry
    with bind_observability_context(ObservabilityContext(resolved_scope, request_id, trace)):
        telemetry.record_metric(
            "aiws.websocket.server.connection.count",
            1,
            resolved_scope,
            request_id,
            attributes,
            service="backend.api",
            metric_type=MetricType.COUNTER,
            unit="{connection}",
        )
        telemetry.record_metric(
            "aiws.websocket.server.connection.duration",
            latency_ms / 1_000,
            resolved_scope,
            request_id,
            attributes,
            service="backend.api",
            metric_type=MetricType.HISTOGRAM,
            unit="s",
        )
        if is_server_error:
            telemetry.record_metric(
                "aiws.websocket.server.error.count",
                1,
                resolved_scope,
                request_id,
                attributes,
                service="backend.api",
                metric_type=MetricType.COUNTER,
                unit="{error}",
            )
        telemetry.record_span(
            "websocket.server.connection",
            latency_ms,
            SpanStatus.ERROR if is_server_error else SpanStatus.UNSET,
            resolved_scope,
            request_id,
            attributes,
            trace_id=trace.trace_id,
            span_id=trace.span_id,
            parent_span_id=trace.parent_span_id,
            service="backend.api",
            occurred_at=wall_start,
        )
        logger.log(
            logging.ERROR if is_server_error else logging.INFO,
            "backend.websocket.connection.completed",
            extra={
                "event_name": "backend.websocket.connection.completed",
                "telemetry_attributes": attributes,
                "duration_ms": latency_ms,
            },
        )
        telemetry.record_health_snapshot(
            output_dropped_count=resolved_container.logging_runtime.dropped_output_count,
        )


def _websocket_outcome(
    accepted: bool,
    close_code: int,
    unhandled_server_error: bool,
) -> tuple[str, bool]:
    """@brief 将标准 WebSocket close code 归为稳定结果 / Classify standard WebSocket close codes into stable outcomes.

    @param accepted 是否完成握手 / Whether the handshake was accepted.
    @param close_code RFC 6455/IANA close code / RFC 6455/IANA close code.
    @param unhandled_server_error 是否有未处理服务端异常 / Whether an unhandled server exception occurred.
    @return ``(outcome, is_server_error)`` / ``(outcome, is_server_error)``.
    """

    if unhandled_server_error or close_code in {1011, 1012, 1013, 1014}:
        return "server_error", True
    if accepted and close_code in {1000, 1001}:
        return "success", False
    return "client_error", False
