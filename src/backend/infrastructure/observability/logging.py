"""@brief 有界 JSON 日志路由与 telemetry fan-out / Bounded JSON log routing and telemetry fan-out."""

from __future__ import annotations

import json
import logging
import math
import os
import queue
import sys
import threading
from collections.abc import Callable
from copy import copy
from dataclasses import dataclass
from datetime import UTC, datetime
from io import TextIOWrapper
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from time import monotonic
from typing import Any, TextIO, cast

from backend.config import LoggingRouteSettings, LoggingSettings
from backend.domain.observability import severity_from_logging_level
from backend.infrastructure.observability.context import current_observability_context
from backend.infrastructure.observability.pipeline import ObservabilityPipeline
from workspace_shared.ids import new_opaque_id
from workspace_shared.tenancy import ActorScope


class ExactLevelFilter(logging.Filter):
    """@brief 仅允许显式等级集合 / Allow only an explicit set of levels."""

    def __init__(self, levels: tuple[str, ...]) -> None:
        """@brief 初始化精确等级过滤器 / Initialize an exact-level filter.

        @param levels 标准日志等级名 / Standard logging level names.
        """
        super().__init__()
        level_names = logging.getLevelNamesMapping()
        self._levels = frozenset(level_names[level] for level in levels)

    def filter(self, record: logging.LogRecord) -> bool:
        """@brief 判断记录是否属于 route / Decide whether a record belongs to the route.

        @param record Python 日志记录 / Python log record.
        @return 等级精确匹配时为真 / True on an exact level match.
        """
        return record.levelno in self._levels


class EventContextFilter(logging.Filter):
    """@brief 在生产线程捕获 event/request/trace 上下文 / Capture event, request, and trace context in the producer thread."""

    def filter(self, record: logging.LogRecord) -> bool:
        """@brief 为输出和 DB fan-out 注入同一个事件 ID / Inject one event ID for output and DB fan-out.

        @param record 可变 Python 日志记录 / Mutable Python log record.
        @return 总为真 / Always true.
        """
        if not isinstance(getattr(record, "event_id", None), str):
            record.event_id = new_opaque_id("log")
        context = current_observability_context()
        if not hasattr(record, "request_id"):
            record.request_id = context.request_id
        if context.trace is not None:
            record.trace_id = context.trace.trace_id
            record.span_id = context.trace.span_id
            record.parent_span_id = context.trace.parent_span_id
        if context.scope is not None:
            record.actor_id = context.scope.actor_id
            record.workspace_id = context.scope.workspace_id
            record.resource_owner_id = context.scope.resource_owner_id
        return True


class JsonLineFormatter(logging.Formatter):
    """@brief 将日志编码为单行 JSON / Encode logs as one-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        """@brief 格式化稳定且可关联的日志对象 / Format a stable, correlatable log object.

        @param record Python 日志记录 / Python log record.
        @return UTF-8 JSONL 行 / UTF-8 JSONL line.
        """
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event_id": getattr(record, "event_id", None),
            "event_name": _event_name(record),
            "message": record.getMessage(),
        }
        for key in (
            "request_id",
            "trace_id",
            "span_id",
            "parent_span_id",
            "workspace_id",
            "resource_owner_id",
            "actor_id",
        ):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        duration_ms = getattr(record, "duration_ms", None)
        if (
            isinstance(duration_ms, (int, float))
            and not isinstance(duration_ms, bool)
            and math.isfinite(float(duration_ms))
            and duration_ms >= 0
        ):
            payload["duration_ms"] = float(duration_ms)
        attributes = getattr(record, "telemetry_attributes", None)
        if isinstance(attributes, dict) and attributes:
            payload["attributes"] = attributes
        exception_type = getattr(record, "exception_type", None)
        if isinstance(exception_type, str) and exception_type:
            payload["exception_type"] = exception_type
        elif record.exc_info is not None and record.exc_info[0] is not None:
            payload["exception_type"] = record.exc_info[0].__name__
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


class NonBlockingQueueHandler(QueueHandler):
    """@brief 队列满时计数并丢弃的 QueueHandler / QueueHandler that counts and drops on pressure."""

    def __init__(self, target: queue.Queue[logging.LogRecord]) -> None:
        """@brief 绑定有界线程队列 / Bind a bounded thread queue.

        @param target 日志 listener 队列 / Logging-listener queue.
        """
        super().__init__(target)
        self._state_lock = threading.Lock()
        self._accepting = True
        self._dropped_count = 0
        self._failure_callback: Callable[[], None] | None = None

    @property
    def dropped_count(self) -> int:
        """@brief 返回线程安全的累计丢弃数 / Return the thread-safe cumulative drop count.

        @return 输出队列丢弃的记录数 / Records dropped by the output queue.
        """

        with self._state_lock:
            return self._dropped_count

    def enqueue(self, record: logging.LogRecord) -> None:
        """@brief 非阻塞放入队列 / Non-blockingly enqueue a record.

        @param record 已准备日志记录 / Prepared log record.
        """
        with self._state_lock:
            if not self._accepting:
                self._dropped_count += 1
                return
            try:
                self.queue.put_nowait(record)
            except queue.Full:
                self._dropped_count += 1

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """@brief 复制日志但绝不格式化异常文本 / Copy a record without ever formatting exception text.

        @param record producer 线程中的原始记录 / Original producer-thread record.
        @return 可安全交给 listener 的脱敏副本 / Sanitized copy safe for the listener.

        @note 标准 ``QueueHandler.prepare`` 会先调用 formatter，将 traceback 和异常消息
        合并到 ``msg`` 后才清空 ``exc_info``；那会绕过 JSON formatter 的脱敏边界。
        / The standard implementation formats the traceback into ``msg`` before clearing
        ``exc_info``, which would bypass the JSON formatter's redaction boundary.
        """

        prepared = copy(record)
        prepared.message = record.getMessage()
        prepared.msg = prepared.message
        prepared.args = None
        if record.exc_info is not None and record.exc_info[0] is not None:
            prepared.exception_type = record.exc_info[0].__name__
        prepared.exc_info = None
        prepared.exc_text = None
        prepared.stack_info = None
        return prepared

    def bind_failure_callback(self, callback: Callable[[], None]) -> None:
        """@brief 绑定 producer 端脱敏失败回调 / Bind a sanitized producer-side failure callback.

        @param callback 不读取当前异常或日志正文的回调 / Callback that reads neither the active exception nor log body.
        """

        self._failure_callback = callback

    def handleError(self, record: logging.LogRecord) -> None:
        """@brief 脱敏处理 prepare/enqueue 异常 / Sanitize prepare and enqueue failures.

        @param record 失败记录；其正文绝不再格式化 / Failed record whose body is never formatted again.

        @note 标准 ``QueueHandler.handleError`` 在开发模式会把当前 traceback
        写入 STDERR，可能泄漏 formatter 边界本应隔离的异常文本。
        / The standard implementation may print the active traceback to STDERR and
        leak exception text that belongs behind the formatter boundary.
        """

        del record
        self.note_output_drop()
        callback = self._failure_callback
        if callback is None:
            return
        try:
            callback()
        except Exception:
            return

    def stop_accepting(self) -> None:
        """@brief 与在途 producer 建立屏障并拒绝后续记录 / Fence in-flight producers and reject later records.

        @return 无返回值 / No return value.

        @note close 在插入 listener sentinel 前调用本方法；同一锁保证 producer 不可能在
        腾出队列槽与插入 sentinel 之间重新填满该槽。
        """

        with self._state_lock:
            self._accepting = False

    def note_shutdown_drop(self) -> None:
        """@brief 计入为保证有界关闭而移除的记录 / Count a record removed for bounded shutdown.

        @return 无返回值 / No return value.
        """

        with self._state_lock:
            self._dropped_count += 1

    def note_output_drop(self) -> None:
        """@brief 计入 sink 故障造成的输出丢弃 / Count an output lost to a sink failure.

        @return 无返回值 / No return value.
        """

        with self._state_lock:
            self._dropped_count += 1


OutputFailureCallback = Callable[[logging.Handler], None]
"""@brief 不得抛异常的输出失败回调 / Non-throwing output-failure callback."""


class _FailureAwareOutput:
    """@brief 抑制 logging 内建 traceback 并报告稳定失败 / Suppress logging tracebacks and report stable failures."""

    _failure_callback: OutputFailureCallback | None = None

    def bind_failure_callback(self, callback: OutputFailureCallback) -> None:
        """@brief 绑定脱敏失败回调 / Bind a sanitized failure callback.

        @param callback 不读取当前异常的回调 / Callback that never reads the active exception.
        """

        self._failure_callback = callback

    def handleError(self, record: logging.LogRecord) -> None:
        """@brief 吞掉 sink 异常且不向 STDERR 泄漏 traceback / Swallow sink failures without leaking tracebacks to STDERR.

        @param record 触发失败的日志记录 / Record whose output failed.
        """

        callback = self._failure_callback
        if callback is None:
            return
        try:
            callback(cast(logging.Handler, self))
        except Exception:
            return


class IsolatedStreamHandler(_FailureAwareOutput, logging.StreamHandler[TextIO]):
    """@brief 单个流失败不会杀死 listener 的输出 / Stream output whose failure cannot kill the listener."""


class PrivateRotatingFileHandler(_FailureAwareOutput, RotatingFileHandler):
    """@brief 每次创建都强制私有权限的轮转文件 / Rotating file that enforces private permissions on every creation.

    @note 标准 ``RotatingFileHandler`` 在 rollover 后会按进程 umask 创建新文件；本实现
    通过带 ``0600`` mode 的原子 ``os.open`` 覆盖首次打开与每次重新打开。
    """

    def _open(self) -> TextIOWrapper[Any]:
        """@brief 以 append 和 0600 打开当前日志文件 / Open the current log file for append with mode 0600.

        @return 由 logging handler 拥有的 UTF-8 文本流 / UTF-8 text stream owned by the logging handler.
        """

        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        descriptor = os.open(self.baseFilename, flags, 0o600)
        try:
            os.chmod(self.baseFilename, 0o600)
            return cast(
                "TextIOWrapper[Any]",
                os.fdopen(
                    descriptor,
                    self.mode,
                    encoding=self.encoding,
                    errors=self.errors,
                ),
            )
        except BaseException:
            os.close(descriptor)
            raise


class StructuredTelemetryHandler(logging.Handler):
    """@brief 将日志事件异步 fan-out 到 telemetry 管线 / Asynchronously fan out log events to telemetry."""

    def __init__(self, pipeline: ObservabilityPipeline) -> None:
        """@brief 初始化 DB bridge / Initialize the database bridge.

        @param pipeline 独立有界 observability 管线 / Independent bounded observability pipeline.
        """
        super().__init__(logging.DEBUG)
        self._pipeline = pipeline

    def emit(self, record: logging.LogRecord) -> None:
        """@brief 提交不含 message/stack 的稳定 LogEvent / Submit a stable LogEvent without message or stack.

        @param record Python 日志记录 / Python log record.
        """
        scope = _scope_from_record(record)
        raw_attributes = getattr(record, "telemetry_attributes", {})
        attributes: dict[str, str | int | float | bool] = (
            dict(raw_attributes) if isinstance(raw_attributes, dict) else {}
        )
        attributes.setdefault("level", record.levelname)
        attributes.setdefault("outcome", "failure" if record.levelno >= logging.ERROR else "success")
        self._pipeline.record_log(
            _event_name(record),
            severity_from_logging_level(record.levelno),
            record.levelname,
            scope,
            _safe_string(getattr(record, "request_id", None), 128),
            attributes,
            service="backend",
            event_id=_safe_string(getattr(record, "event_id", None), 128),
            trace_id=_safe_string(getattr(record, "trace_id", None), 32),
            span_id=_safe_string(getattr(record, "span_id", None), 16),
            parent_span_id=_safe_string(
                getattr(record, "parent_span_id", None), 16
            ),
        )


class _OutputFailureReporter:
    """@brief 跨 route 限频提交脱敏输出故障 / Rate-limit sanitized output failures across routes."""

    def __init__(self, pipeline: ObservabilityPipeline) -> None:
        """@brief 绑定独立 telemetry 管线 / Bind the independent telemetry pipeline.

        @param pipeline 不经普通 logger 的事件管线 / Event pipeline that bypasses the normal logger.
        """

        self._pipeline = pipeline
        self._lock = threading.Lock()
        self._last_report_at: dict[str, float] = {}

    def record(self, name: str) -> None:
        """@brief 每类故障至多每 30 秒提交一次 / Submit each failure kind at most once per 30 seconds.

        @param name 不含路径或异常文本的稳定事件名 / Stable event name without paths or exception text.
        """

        now = monotonic()
        with self._lock:
            last_report_at = self._last_report_at.get(name, float("-inf"))
            if now - last_report_at < 30.0:
                return
            self._last_report_at[name] = now
        try:
            self._pipeline.record_log(
                name,
                severity_from_logging_level(logging.ERROR),
                "ERROR",
                None,
                None,
                {"operation": "log_output", "outcome": "failure"},
                service="backend.observability",
            )
        except Exception:
            return

    def record_output_failure(self) -> None:
        """@brief 提交通用输出失败事件 / Submit the generic output-failure event."""

        self.record("aiws.logging.output.failed")


class _BoundedQueueListener(QueueListener):
    """@brief 单 sink 的有界可回收 listener / Bounded, reapable listener for exactly one sink.

    @note 每个 route 必须拥有独立队列和 worker；禁止在此处挂多个
    handler，从结构上消除 sink 间的队头阻塞（head-of-line blocking）。
    / Every route owns an independent queue and worker. Multiple handlers are rejected
    here so one blocked sink can never stall another sink by construction.
    """

    def __init__(
        self,
        target: queue.Queue[logging.LogRecord],
        drop_counter: NonBlockingQueueHandler,
        *handlers: logging.Handler,
        reporter: _OutputFailureReporter | None = None,
        respect_handler_level: bool = False,
    ) -> None:
        """@brief 绑定一个已隔离 sink 的队列 / Bind one isolated sink queue.

        @param target 日志记录队列 / Log-record queue.
        @param drop_counter 关闭丢弃计数所有者 / Owner of the shutdown-drop counter.
        @param handlers 零个或一个下游输出 / Zero or one downstream output.
        @param reporter 可选的共享脱敏失败报告器 / Optional shared sanitized failure reporter.
        @param respect_handler_level 是否尊重 handler level / Whether handler levels are respected.
        @raise ValueError 挂载多个 sink 时拒绝 / Raised when multiple sinks are attached.
        """

        if len(handlers) > 1:
            raise ValueError("an isolated logging listener accepts at most one output sink")
        super().__init__(target, *handlers, respect_handler_level=respect_handler_level)
        self._drop_counter = drop_counter
        self._reporter = reporter
        for handler in handlers:
            if isinstance(handler, _FailureAwareOutput):
                handler.bind_failure_callback(self._note_sink_failure)

    def _monitor(self) -> None:
        """@brief 在 sink 自有 worker 内输出并关闭 / Emit and close the sink inside its owning worker.

        @note 把 ``handler.close()`` 也放在独立 worker，使阻塞的 flush/close
        与阻塞的 emit 一样只影响当前 route。
        / Running handler close in the isolated worker gives blocking flush/close the
        same fault containment as blocking emit.
        """

        target = cast(queue.Queue[logging.LogRecord | None], self.queue)
        try:
            while True:
                record = target.get()
                try:
                    if record is None:
                        return
                    self.handle(record)
                finally:
                    target.task_done()
        finally:
            for handler in self.handlers:
                if not _close_output_handler(handler):
                    self._note_sink_failure(handler)

    def handle(self, record: logging.LogRecord) -> None:
        """@brief 逐 sink 隔离输出异常 / Isolate output failures per sink.

        @param record 已从有界队列取出的记录 / Record dequeued from the bounded queue.
        """

        prepared = self.prepare(record)
        for handler in self.handlers:
            if self.respect_handler_level and prepared.levelno < handler.level:
                continue
            try:
                handler.handle(prepared)
            except Exception:
                self._note_sink_failure(handler)

    def _note_sink_failure(self, handler: logging.Handler) -> None:
        """@brief 计数并限频持久化脱敏 sink 失败 / Count and rate-limit a sanitized sink-failure event.

        @param handler 失败输出；其路径与异常均不记录 / Failed output; its path and exception are not recorded.
        """

        del handler
        self._drop_counter.note_output_drop()
        reporter = self._reporter
        if reporter is not None:
            reporter.record_output_failure()

    def note_shutdown_timeout(self) -> None:
        """@brief 记录 listener 未在预算内停止 / Record that the listener exceeded its shutdown budget."""

        self._drop_counter.note_output_drop()
        reporter = self._reporter
        if reporter is not None:
            reporter.record("aiws.logging.output.shutdown_timeout")

    def enqueue_sentinel(self) -> None:
        """@brief 无竞态地加入停止哨兵 / Enqueue the stop sentinel without a producer race.

        @note 调用前 producer 已由 ``stop_accepting`` 栅栏拒绝。若队列仍满，移除最旧
        记录并显式计数；listener 只会继续腾空队列，故循环必然放入 sentinel。
        """

        target = cast(queue.Queue[logging.LogRecord | None], self.queue)
        while True:
            try:
                target.put_nowait(None)
                return
            except queue.Full:
                try:
                    target.get_nowait()
                except queue.Empty:
                    continue
                target.task_done()
                self._drop_counter.note_shutdown_drop()

    def stop_bounded(self, timeout_seconds: float) -> bool:
        """@brief 在明确预算内等待 listener / Wait for the listener within an explicit budget.

        @param timeout_seconds 非负等待秒数 / Non-negative wait duration in seconds.
        @return listener 已停止时为真 / True when the listener has stopped.
        """

        self.enqueue_sentinel()
        return self.wait_stopped(timeout_seconds)

    def wait_stopped(self, timeout_seconds: float) -> bool:
        """@brief 等待已请求停止的 worker / Wait for a worker whose stop was requested.

        @param timeout_seconds 非负等待秒数 / Non-negative wait duration in seconds.
        @return worker 已停止时为真 / True when the worker has stopped.
        """

        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout_seconds)
        if thread.is_alive():
            return False
        self._thread = None
        return True

    def finish_stop(self) -> None:
        """@brief 在 daemon reaper 中等待最终退出 / Await final exit in a daemon reaper."""

        thread = self._thread
        if thread is not None:
            thread.join()
            self._thread = None


@dataclass(frozen=True, slots=True)
class _OutputWorker:
    """@brief 一条 route 的队列、worker 与 sink 所有权 / Queue, worker, and sink ownership for one route."""

    queue_handler: NonBlockingQueueHandler
    listener: _BoundedQueueListener
    output_handler: logging.Handler


class LoggingRuntime:
    """@brief 一次日志配置的资源所有者 / Resource owner for one logging configuration."""

    def __init__(
        self,
        logger: logging.Logger,
        output_workers: tuple[_OutputWorker, ...],
        database_handler: StructuredTelemetryHandler | None,
        shutdown_timeout_ms: int,
    ) -> None:
        """@brief 保存需要对称关闭的日志资源 / Retain logging resources for symmetric shutdown."""
        self._logger = logger
        self._output_workers = output_workers
        self._database_handler = database_handler
        self._shutdown_timeout_ms = shutdown_timeout_ms
        self._closed = False

    @property
    def dropped_output_count(self) -> int:
        """@brief 返回输出队列丢弃数 / Return output-queue drop count.

        @return 丢弃的输出日志数 / Dropped output-log count.
        """
        return sum(worker.queue_handler.dropped_count for worker in self._output_workers)

    def close(self) -> None:
        """@brief 停止 listener 并释放 handlers / Stop the listener and release handlers."""
        global _active_runtime
        with _lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            for worker in self._output_workers:
                self._logger.removeHandler(worker.queue_handler)
            if self._database_handler is not None:
                self._logger.removeHandler(self._database_handler)
            for worker in self._output_workers:
                worker.queue_handler.stop_accepting()
            for worker in self._output_workers:
                worker.listener.enqueue_sentinel()

            deadline = monotonic() + self._shutdown_timeout_ms / 1_000
            stopped_workers: list[bool] = []
            for worker in self._output_workers:
                remaining = max(0.0, deadline - monotonic())
                stopped_workers.append(worker.listener.wait_stopped(remaining))

            for worker in self._output_workers:
                worker.queue_handler.close()
            if self._database_handler is not None:
                self._database_handler.close()
            for index, (worker, stopped) in enumerate(
                zip(self._output_workers, stopped_workers, strict=True)
            ):
                if stopped:
                    continue
                worker.listener.note_shutdown_timeout()
                threading.Thread(
                    target=_reap_listener,
                    args=(worker.listener,),
                    name=f"aiws:logging-reaper-{index}",
                    daemon=True,
                ).start()
            if _active_runtime is self:
                _active_runtime = None


def _close_output_handler(handler: logging.Handler) -> bool:
    """@brief 尽力关闭一个输出 / Best-effort close one output.

    @param handler 由 runtime 拥有的输出 / Output owned by the runtime.
    @return 关闭未抛异常时为真 / True when close did not raise.
    """

    try:
        handler.close()
    except Exception:
        return False
    return True


def _reap_listener(
    listener: _BoundedQueueListener,
) -> None:
    """@brief 后台等待阻塞 sink 完成自有回收 / Await self-owned cleanup after a blocked sink recovers.

    @param listener 超出关闭预算的 listener / Listener that exceeded its shutdown budget.
    """

    listener.finish_stop()


_lifecycle_lock = threading.RLock()
"""@brief 串行化进程级 logger 重配置 / Serialize process-wide logger reconfiguration."""

_active_runtime: LoggingRuntime | None = None
"""@brief 当前 backend logger 配置所有者 / Current backend logger configuration owner."""


def configure_logging(
    settings: LoggingSettings,
    pipeline: ObservabilityPipeline,
    base_directory: Path,
) -> LoggingRuntime:
    """@brief 配置 backend 命名空间的有界精确路由 / Configure bounded exact routes for backend loggers.

    @param settings 已验证日志 routes / Validated logging routes.
    @param pipeline DB 异步 fan-out 管线 / Asynchronous database fan-out pipeline.
    @param base_directory 相对文件 route 的根目录 / Root for relative file routes.
    @return 必须在 shutdown 关闭的资源对象 / Resource object to close during shutdown.
    """
    global _active_runtime
    with _lifecycle_lock:
        if _active_runtime is not None:
            _active_runtime.close()
        formatter = JsonLineFormatter()
        reporter = _OutputFailureReporter(pipeline)
        output_workers: list[_OutputWorker] = []
        try:
            for route in settings.routes:
                output_handler = _output_handler(route, formatter, base_directory)
                records: queue.Queue[logging.LogRecord] = queue.Queue(
                    maxsize=settings.queue_capacity
                )
                queue_handler = NonBlockingQueueHandler(records)
                queue_handler.bind_failure_callback(reporter.record_output_failure)
                queue_handler.setLevel(logging.DEBUG)
                queue_handler.addFilter(ExactLevelFilter(route.levels))
                queue_handler.addFilter(EventContextFilter())
                listener = _BoundedQueueListener(
                    records,
                    queue_handler,
                    output_handler,
                    reporter=reporter,
                    respect_handler_level=True,
                )
                output_workers.append(
                    _OutputWorker(queue_handler, listener, output_handler)
                )
        except BaseException:
            for worker in output_workers:
                worker.queue_handler.close()
                _close_output_handler(worker.output_handler)
            raise
        workers = tuple(output_workers)
        database_handler: StructuredTelemetryHandler | None = None
        if settings.persist_structured_events:
            database_handler = StructuredTelemetryHandler(pipeline)
            database_handler.addFilter(EventContextFilter())
        logger = logging.getLogger("backend")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        started_workers: list[_OutputWorker] = []
        try:
            for worker in workers:
                worker.listener.start()
                started_workers.append(worker)
        except BaseException:
            for worker in started_workers:
                worker.queue_handler.stop_accepting()
                worker.listener.enqueue_sentinel()
            cleanup_deadline = monotonic() + 1.0
            for index, worker in enumerate(started_workers):
                remaining = max(0.0, cleanup_deadline - monotonic())
                if worker.listener.wait_stopped(remaining):
                    continue
                worker.listener.note_shutdown_timeout()
                threading.Thread(
                    target=_reap_listener,
                    args=(worker.listener,),
                    name=f"aiws:logging-setup-reaper-{index}",
                    daemon=True,
                ).start()
            for index, worker in enumerate(workers):
                worker.queue_handler.close()
                if index >= len(started_workers):
                    _close_output_handler(worker.output_handler)
            if database_handler is not None:
                database_handler.close()
            raise
        for worker in workers:
            logger.addHandler(worker.queue_handler)
        if database_handler is not None:
            logger.addHandler(database_handler)
        runtime = LoggingRuntime(
            logger,
            workers,
            database_handler,
            settings.shutdown_timeout_ms,
        )
        _active_runtime = runtime
        return runtime


def _output_handler(
    route: LoggingRouteSettings,
    formatter: logging.Formatter,
    base_directory: Path,
) -> logging.Handler:
    """@brief 构造一条 stdout/stderr/file 输出 route / Build one stdout, stderr, or file route.

    @param route 已验证 route / Validated route.
    @param formatter JSONL formatter / JSONL formatter.
    @param base_directory 相对路径根目录 / Relative-path root.
    @return 配置完成的 handler / Configured handler.
    """
    if route.sink == "stdout":
        handler: logging.Handler = IsolatedStreamHandler(_stream("stdout"))
    elif route.sink == "stderr":
        handler = IsolatedStreamHandler(_stream("stderr"))
    else:
        assert route.path is not None
        assert route.max_bytes is not None
        assert route.backup_count is not None
        path = route.path if route.path.is_absolute() else base_directory / route.path
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = PrivateRotatingFileHandler(
            path,
            maxBytes=route.max_bytes,
            backupCount=route.backup_count,
            encoding="utf-8",
        )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    return handler


def _stream(name: str) -> TextIO:
    """@brief 解析标准输出流 / Resolve a standard output stream.

    @param name ``stdout`` 或 ``stderr`` / ``stdout`` or ``stderr``.
    @return 对应文本流 / Corresponding text stream.
    """
    return sys.stdout if name == "stdout" else sys.stderr


def _event_name(record: logging.LogRecord) -> str:
    """@brief 提取稳定事件名 / Extract a stable event name.

    @param record Python 日志记录 / Python log record.
    @return 显式事件名或安全 fallback / Explicit event name or safe fallback.
    """
    candidate = getattr(record, "event_name", None)
    if isinstance(candidate, str) and candidate:
        return candidate
    return "backend.log.event"


def _scope_from_record(record: logging.LogRecord) -> ActorScope | None:
    """@brief 从 producer 捕获字段恢复租户 scope / Restore tenant scope from producer-captured fields.

    @param record Python 日志记录 / Python log record.
    @return 完整 scope；缺任一字段时为 None / Complete scope, or None when any part is absent.
    """
    actor_id = _safe_string(getattr(record, "actor_id", None), 128)
    workspace_id = _safe_string(getattr(record, "workspace_id", None), 128)
    owner_id = _safe_string(getattr(record, "resource_owner_id", None), 128)
    if actor_id is None or workspace_id is None or owner_id is None:
        return None
    return ActorScope(actor_id, workspace_id, owner_id)


def _safe_string(value: Any, maximum: int) -> str | None:
    """@brief 接受有界无控制字符字符串 / Accept a bounded string without controls.

    @param value 候选值 / Candidate value.
    @param maximum 最大字符数 / Maximum number of characters.
    @return 合法字符串或 None / Safe string or None.
    """
    if not isinstance(value, str) or not value or len(value) > maximum:
        return None
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return None
    return value


__all__ = [
    "ExactLevelFilter",
    "IsolatedStreamHandler",
    "JsonLineFormatter",
    "LoggingRuntime",
    "NonBlockingQueueHandler",
    "PrivateRotatingFileHandler",
    "StructuredTelemetryHandler",
    "configure_logging",
]
