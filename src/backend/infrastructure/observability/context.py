"""@brief 请求级 telemetry 与 W3C trace 上下文 / Request telemetry and W3C trace context."""

from __future__ import annotations

import re
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass

from workspace_shared.tenancy import ActorScope

_TRACEPARENT = re.compile(
    r"00-(?P<trace_id>[0-9a-f]{32})-(?P<parent_id>[0-9a-f]{16})-(?P<flags>[0-9a-f]{2})"
)
"""@brief 当前支持的 W3C traceparent v00 格式 / Supported W3C traceparent v00 format."""


@dataclass(frozen=True, slots=True)
class ServerTraceContext:
    """@brief 当前 HTTP server span 的 W3C 上下文 / W3C context for the current HTTP server span.

    @param trace_id 继承或新建的 trace ID / Inherited or newly generated trace ID.
    @param span_id 服务端新建的 span ID / Newly generated server span ID.
    @param parent_span_id 合法上游 parent ID / Valid upstream parent ID.
    @param trace_flags 传播的 W3C flags / Propagated W3C flags.
    """

    trace_id: str
    span_id: str
    parent_span_id: str | None
    trace_flags: str

    @property
    def traceparent(self) -> str:
        """@brief 序列化当前 server span / Serialize the current server span.

        @return 可放入响应的 W3C traceparent / W3C traceparent suitable for a response.
        """
        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags}"


@dataclass(frozen=True, slots=True)
class ObservabilityContext:
    """@brief 进入日志与 telemetry 的请求上下文 / Request context injected into logs and telemetry.

    @param scope 已验证租户范围；系统事件可为空 / Verified tenant scope; nullable for system events.
    @param request_id 后端请求关联 ID / Backend request correlation ID.
    @param trace 当前 server trace 上下文 / Current server trace context.
    """

    scope: ActorScope | None
    request_id: str | None
    trace: ServerTraceContext | None


_context: ContextVar[ObservabilityContext | None] = ContextVar(
    "aiws_observability_context",
    default=None,
)
"""@brief 当前 asyncio 上下文 / Current asyncio-local observability context."""


@contextmanager
def bind_observability_context(context: ObservabilityContext) -> Iterator[None]:
    """@brief 在当前任务绑定请求上下文 / Bind request context in the current task.

    @param context 已验证上下文 / Validated context.
    @return 离开后自动复位的上下文 / Context automatically reset on exit.
    """
    token: Token[ObservabilityContext | None] = _context.set(context)
    try:
        yield
    finally:
        _context.reset(token)


def current_observability_context() -> ObservabilityContext:
    """@brief 返回当前 telemetry 上下文 / Return the current telemetry context.

    @return 当前上下文；无请求时字段为空 / Current context with empty fields outside requests.
    """
    return _context.get() or ObservabilityContext(scope=None, request_id=None, trace=None)


def new_server_trace_context(traceparent: str | None) -> ServerTraceContext:
    """@brief 延续合法 W3C traceparent 或开启新 trace / Continue a valid traceparent or start a new trace.

    @param traceparent 未受信任的入口 header / Untrusted ingress header.
    @return 新建 server span 的合法上下文 / Valid context for a newly created server span.

    @note 非法、全零或不支持版本的 header 按 W3C 规则忽略，而非回显或报错。
    """
    matched = _TRACEPARENT.fullmatch(traceparent) if traceparent is not None else None
    trace_id = secrets.token_hex(16)
    parent_span_id: str | None = None
    trace_flags = "01"
    if matched is not None:
        candidate_trace = matched.group("trace_id")
        candidate_parent = matched.group("parent_id")
        if candidate_trace != "0" * 32 and candidate_parent != "0" * 16:
            trace_id = candidate_trace
            parent_span_id = candidate_parent
            trace_flags = matched.group("flags")
    return ServerTraceContext(
        trace_id=trace_id,
        span_id=secrets.token_hex(8),
        parent_span_id=parent_span_id,
        trace_flags=trace_flags,
    )


__all__ = [
    "ObservabilityContext",
    "ServerTraceContext",
    "bind_observability_context",
    "current_observability_context",
    "new_server_trace_context",
]
