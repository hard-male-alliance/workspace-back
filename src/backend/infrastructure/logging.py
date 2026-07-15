"""@brief 低基数结构化日志与可观测性桥接 / Low-cardinality structured logging and observability bridge."""

from __future__ import annotations

import logging
from typing import Any

from backend.config import LoggingSettings
from backend.infrastructure.telemetry import BufferedTelemetrySink, current_telemetry_scope


class StructuredTelemetryHandler(logging.Handler):
    """@brief 将稳定日志事件写入有界 telemetry sink / Write stable log events into the bounded telemetry sink.

    @note 不序列化 `record.getMessage()`、异常栈、prompt 或 URL，防止高基数/敏感数据泄漏。
    """

    def __init__(self, sink: BufferedTelemetrySink) -> None:
        """@brief 初始化 handler / Initialize the handler.

        @param sink 有界 telemetry sink / Bounded telemetry sink.
        """
        super().__init__()
        self._sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        """@brief 非阻塞地发出稳定结构化事件 / Non-blockingly emit a stable structured event.

        @param record Python 日志记录 / Python log record.
        """
        scope = current_telemetry_scope()
        request_id = _safe_string(getattr(record, "request_id", None))
        if scope is None:
            return
        self._sink.record(
            "log",
            record.name,
            None,
            scope,
            request_id,
            {"operation": "log", "outcome": "failure" if record.levelno >= logging.ERROR else "success", "level": record.levelname},
            service="backend",
        )


def configure_logging(settings: LoggingSettings, sink: BufferedTelemetrySink) -> logging.Handler | None:
    """@brief 配置后端命名空间日志 / Configure backend-namespace logging.

    @param settings 日志设置 / Logging settings.
    @param sink 有界 telemetry sink / Bounded telemetry sink.
    @return 已安装 handler；关闭时应移除 / Installed handler, to remove during shutdown.
    """
    logger = logging.getLogger("backend")
    logger.setLevel(settings.level)
    logger.propagate = True
    if not settings.persist_structured_events:
        return None
    handler = StructuredTelemetryHandler(sink)
    handler.setLevel(settings.level)
    logger.addHandler(handler)
    return handler


def remove_logging_handler(handler: logging.Handler | None) -> None:
    """@brief 移除 telemetry logging handler / Remove the telemetry logging handler.

    @param handler 待移除 handler / Handler to remove.
    """
    if handler is None:
        return
    logger = logging.getLogger("backend")
    logger.removeHandler(handler)
    handler.close()


def _safe_string(value: Any) -> str | None:
    """@brief 仅接受短稳定字符串 / Accept only short stable strings.

    @param value 候选值 / Candidate value.
    @return 合法短字符串或 None / Valid short string or None.
    """
    return value if isinstance(value, str) and 0 < len(value) <= 128 else None
