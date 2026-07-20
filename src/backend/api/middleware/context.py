"""@brief ASGI 请求关联上下文边界 / ASGI request-correlation context boundary."""

from __future__ import annotations

import re
from typing import cast

from fastapi import Request
from fastapi.responses import Response
from starlette.requests import HTTPConnection
from starlette.types import Message, Scope

from backend.infrastructure.observability.context import (
    ServerTraceContext,
    new_server_trace_context,
)
from workspace_shared.ids import new_opaque_id

_REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
"""@brief 可安全持久化的 request ID 格式 / Request-ID format safe for persistence."""


def _install_transport_context(scope: Scope) -> None:
    """@brief 在任何业务中间件前安装关联上下文 / Install correlation context before business middleware.

    @param scope HTTP 或 WebSocket scope / HTTP or WebSocket scope.
    """

    state = scope.setdefault("state", {})
    connection = HTTPConnection(scope)
    trace_values = connection.headers.getlist("traceparent")
    state["trace_context"] = new_server_trace_context(
        trace_values[0] if len(trace_values) == 1 else None
    )
    try:
        state["request_id"] = _request_id_from_headers(connection)
        state["request_id_invalid"] = False
    except ValueError:
        state["request_id"] = new_opaque_id("req")
        state["request_id_invalid"] = True


def _with_correlation_headers(scope: Scope, message: Message) -> Message:
    """@brief 在最外层响应上规范化关联 headers / Normalize correlation headers on the outermost response.

    @param scope HTTP scope / HTTP scope.
    @param message ``http.response.start`` 消息 / ``http.response.start`` message.
    @return 带唯一 request ID 与 traceparent 的副本 / Copy with one request ID and traceparent.
    """

    state = scope.get("state")
    if not isinstance(state, dict):
        return message
    request_id = state.get("request_id")
    trace = state.get("trace_context")
    if not isinstance(request_id, str) or not isinstance(trace, ServerTraceContext):
        return message
    headers = [
        (name, value)
        for name, value in message.get("headers", [])
        if name.lower() not in {b"x-request-id", b"traceparent"}
    ]
    headers.extend(
        (
            (b"x-request-id", request_id.encode("ascii")),
            (b"traceparent", trace.traceparent.encode("ascii")),
        )
    )
    updated = dict(message)
    updated["headers"] = headers
    return cast(Message, updated)


def correlate_http_response(
    response: Response,
    request: Request,
    trace: ServerTraceContext,
) -> Response:
    """@brief 对所有 return 分支统一附加关联 header / Uniformly attach correlation headers to every return branch.

    @param response 待返回响应 / Response to return.
    @param request 当前请求 / Current request.
    @param trace 当前 trace / Current trace.
    @return 原响应对象 / Original response object.
    """

    response.headers["X-Request-Id"] = request.state.request_id
    response.headers["traceparent"] = trace.traceparent
    return response


def _request_id_from_headers(request: HTTPConnection) -> str:
    """@brief 验证入口关联 ID 或生成新 ID / Validate an ingress correlation ID or generate a new one.

    @param request 当前 HTTP 请求 / Current HTTP request.
    @return 可写入 telemetry 与 PostgreSQL ``VARCHAR(128)`` 的 request ID。
    @raise ValueError 客户端提供了重复、过长或含控制字符的值时抛出。

    @note request ID 不是身份凭据，但其会进入持久化 telemetry。边界校验避免一个
    任意长 header 将普通业务请求放大成数据库列溢出或日志污染。
    """
    values = request.headers.getlist("X-Request-Id")
    if not values:
        return new_opaque_id("req")
    if len(values) != 1 or not _REQUEST_ID_PATTERN.fullmatch(values[0]):
        raise ValueError("invalid request ID")
    return values[0]
