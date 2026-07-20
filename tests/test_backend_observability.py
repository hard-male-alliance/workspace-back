"""@brief 后端 observability 领域、并发与日志生命周期测试 / Backend observability domain, concurrency, and logging lifecycle tests."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import queue
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from typing import Any, cast

import pytest

from backend.api.middleware.transport import (
    TransportTelemetryMiddleware,
    _websocket_outcome,
)
from backend.application.diagnostics import (
    ClientErrorDiagnostic,
    DiagnosticIngestionService,
    DiagnosticRateLimiter,
)
from backend.composition import _close_runtime_resources, build_container
from backend.config import (
    BackendSettings,
    DiagnosticsSettings,
    LoggingRouteSettings,
    LoggingSettings,
)
from backend.domain.observability import (
    LogEvent,
    MetricPoint,
    MetricType,
    ResourceMetadata,
    SignalEnvelope,
    SignalSource,
    TelemetrySignal,
)
from backend.infrastructure.observability.logging import (
    JsonLineFormatter,
    NonBlockingQueueHandler,
    _BoundedQueueListener,
    configure_logging,
)
from backend.infrastructure.observability.pipeline import (
    InMemoryTelemetryWriter,
    ObservabilityPipeline,
)
from workspace_shared.tenancy import ActorScope

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root directory."""


def _pipeline(
    writer: InMemoryTelemetryWriter | None = None,
) -> tuple[ObservabilityPipeline, InMemoryTelemetryWriter]:
    """@brief 构造测试管线 / Build a test observability pipeline.

    @param writer 可注入内存 writer / Optional in-memory writer.
    @return ``(pipeline, writer)`` / ``(pipeline, writer)``.
    """
    resolved_writer = writer or InMemoryTelemetryWriter()
    return (
        ObservabilityPipeline(
            resolved_writer,
            ResourceMetadata("backend.test", "test", "test", "worker-1"),
            queue_capacity=16,
            batch_size=8,
            flush_interval_ms=10,
            drop_policy="drop_newest",
            shutdown_flush_timeout_ms=500,
        ),
        resolved_writer,
    )


def _diagnostics_settings() -> DiagnosticsSettings:
    """@brief 返回确定性诊断预算 / Return deterministic diagnostics budgets."""
    return DiagnosticsSettings(
        max_body_bytes=65_536,
        max_batch_size=50,
        max_event_age_seconds=3_600,
        max_future_skew_seconds=60,
        rate_limit_capacity=50,
        rate_limit_refill_per_minute=50,
        max_actor_buckets=100,
    )


class _FailingTelemetryWriter:
    """@brief 总是失败的 writer 测试替身 / Writer test double that always fails."""

    async def write_batch(self, records: list[TelemetrySignal]) -> None:
        """@brief 模拟持久化故障 / Simulate a persistence failure.

        @param records 待失败批次 / Batch to fail.
        @raise OSError 始终抛出 / Always raised.
        """
        raise OSError("secret database detail must not be emitted")


class _CancellationResistantWriter:
    """@brief 延迟响应 cancellation 的 writer / Writer that delays cancellation completion."""

    def __init__(self) -> None:
        """@brief 初始化同步事件 / Initialize synchronization events."""

        self.entered = asyncio.Event()
        self.finished = asyncio.Event()

    async def write_batch(self, records: list[TelemetrySignal]) -> None:
        """@brief 模拟耗时 cancellation cleanup / Simulate slow cancellation cleanup.

        @param records 待写批次 / Batch to write.
        @raise asyncio.CancelledError 清理结束后继续传播取消 / Re-propagated after cleanup.
        """

        del records
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.15)
            self.finished.set()
            raise


def test_signal_envelope_rejects_unknown_or_high_cardinality_attributes() -> None:
    """@brief 领域模型必须拒绝未知/自由文本属性 / Domain model rejects unknown or free-form attributes."""
    with pytest.raises(ValueError, match="not allowed"):
        SignalEnvelope(
            SignalSource.BACKEND,
            ResourceMetadata("backend.test"),
            "aiws.test.metric",
            attributes={"prompt": "secret"},
        )
    with pytest.raises(ValueError, match="unsafe"):
        SignalEnvelope(
            SignalSource.BACKEND,
            ResourceMetadata("backend.test"),
            "aiws.test.metric",
            attributes={"route": "x" * 257},
        )


def test_json_log_formatter_preserves_finite_request_duration() -> None:
    """@brief JSONL 输出应保留可用的 HTTP 耗时 / JSONL output preserves a usable HTTP duration."""

    record = logging.LogRecord(
        "backend.http",
        logging.INFO,
        __file__,
        1,
        "backend.http.request.completed",
        (),
        None,
    )
    record.duration_ms = 12.5
    payload = json.loads(JsonLineFormatter().format(record))

    assert payload["duration_ms"] == 12.5


@pytest.mark.parametrize(
    ("accepted", "close_code", "unhandled", "expected"),
    (
        (False, 1000, False, ("client_error", False)),
        (True, 1000, False, ("success", False)),
        (True, 1001, False, ("success", False)),
        (True, 1005, False, ("client_error", False)),
        (True, 1006, False, ("client_error", False)),
        (True, 1011, False, ("server_error", True)),
        (True, 1014, False, ("server_error", True)),
        (True, 4000, False, ("client_error", False)),
        (True, 1000, True, ("server_error", True)),
    ),
)
def test_websocket_close_taxonomy_is_explicit_and_bounded(
    accepted: bool,
    close_code: int,
    unhandled: bool,
    expected: tuple[str, bool],
) -> None:
    """@brief WebSocket code 不得因默认分支产生假成功 / WebSocket codes cannot become false success through a default branch."""

    assert _websocket_outcome(accepted, close_code, unhandled) == expected


@pytest.mark.asyncio
async def test_http_stream_duration_finishes_only_after_final_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief HTTP duration 必须等待最后一个 ASGI body / HTTP duration must await the final ASGI body.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    """

    response_started = asyncio.Event()
    release_body = asyncio.Event()
    observed_statuses: list[int] = []
    active_samples: list[tuple[str, int]] = []

    async def streaming_app(
        _scope: object,
        _receive: object,
        send: object,
    ) -> None:
        """@brief 发送 start 后等待测试释放最终 body / Send start, then await release of the final body."""

        typed_send = cast(Any, send)
        await typed_send({"type": "http.response.start", "status": 200, "headers": []})
        response_started.set()
        await release_body.wait()
        await typed_send({"type": "http.response.body", "body": b"done"})

    def observe(
        _scope: object,
        status_code: int,
        _started: float,
        _wall_start: datetime,
    ) -> None:
        """@brief 捕获最终状态 / Capture the terminal status."""

        observed_statuses.append(status_code)

    def observe_active(
        _scope: object,
        transport: str,
        value: int,
    ) -> None:
        """@brief 捕获 worker-local active gauge / Capture the worker-local active gauge."""

        active_samples.append((transport, value))

    async def receive() -> dict[str, object]:
        """@brief 返回测试 request body 终态 / Return the test request-body terminal state."""

        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_message: object) -> None:
        """@brief 接收上游响应消息 / Receive an upstream response message."""

    monkeypatch.setattr("backend.api.middleware.transport._observe_http_scope_completion", observe)
    monkeypatch.setattr(
        "backend.api.middleware.transport._record_active_transport_gauge",
        observe_active,
    )
    middleware = TransportTelemetryMiddleware(cast(Any, streaming_app))
    scope = cast(
        Any,
        {
            "type": "http",
            "method": "GET",
            "path": "/stream",
            "raw_path": b"/stream",
            "query_string": b"",
            "headers": [],
            "scheme": "http",
            "server": ("test", 80),
            "client": ("test", 1234),
            "root_path": "",
            "http_version": "1.1",
        },
    )
    task = asyncio.create_task(middleware(scope, receive, send))
    await response_started.wait()
    assert observed_statuses == []
    assert active_samples == [("http", 1)]
    release_body.set()
    await task
    assert observed_statuses == [200]
    assert active_samples == [("http", 1), ("http", 0)]


@pytest.mark.asyncio
async def test_http_stream_failure_after_response_start_finishes_as_server_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 建流后的生成器失败必须结束为 500 / A generator failure after response start must terminate as 500.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    """

    observed_statuses: list[int] = []

    async def failing_stream(
        _scope: object,
        _receive: object,
        send: object,
    ) -> None:
        """@brief 发送 200 start 后模拟流失败 / Simulate stream failure after a 200 start.

        @raise RuntimeError 始终失败 / Always raised.
        """

        await cast(Any, send)({"type": "http.response.start", "status": 200, "headers": []})
        raise RuntimeError("stream failed")

    def observe(
        _scope: object,
        status_code: int,
        _started: float,
        _wall_start: datetime,
    ) -> None:
        """@brief 捕获最终状态 / Capture the terminal status."""

        observed_statuses.append(status_code)

    async def receive() -> dict[str, object]:
        """@brief 返回测试 request body 终态 / Return the test request-body terminal state."""

        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_message: object) -> None:
        """@brief 接收上游响应消息 / Receive an upstream response message."""

    monkeypatch.setattr("backend.api.middleware.transport._observe_http_scope_completion", observe)
    middleware = TransportTelemetryMiddleware(cast(Any, failing_stream))
    scope = cast(
        Any,
        {
            "type": "http",
            "method": "GET",
            "path": "/stream",
            "raw_path": b"/stream",
            "query_string": b"",
            "headers": [],
            "scheme": "http",
            "server": ("test", 80),
            "client": ("test", 1234),
            "root_path": "",
            "http_version": "1.1",
        },
    )
    with pytest.raises(RuntimeError, match="stream failed"):
        await middleware(scope, receive, send)
    assert observed_statuses == [500]


@pytest.mark.asyncio
async def test_http_disconnect_that_cancels_stream_finishes_as_499(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief ASGI 2.3 正常取消流也必须识别 peer disconnect / ASGI 2.3 normal stream cancellation must still detect peer disconnect.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    """

    observed_statuses: list[int] = []

    async def cancelled_stream(
        _scope: object,
        receive: object,
        send: object,
    ) -> None:
        """@brief 发送 start 后因 disconnect 正常返回 / Return normally after a disconnect following response start."""

        await cast(Any, send)({"type": "http.response.start", "status": 200, "headers": []})
        assert (await cast(Any, receive)())["type"] == "http.disconnect"

    def observe(
        _scope: object,
        status_code: int,
        _started: float,
        _wall_start: datetime,
    ) -> None:
        """@brief 捕获最终状态 / Capture the terminal status."""

        observed_statuses.append(status_code)

    async def receive() -> dict[str, object]:
        """@brief 模拟 peer disconnect / Simulate peer disconnect."""

        return {"type": "http.disconnect"}

    async def send(_message: object) -> None:
        """@brief 接收 response start / Receive response start."""

    monkeypatch.setattr("backend.api.middleware.transport._observe_http_scope_completion", observe)
    middleware = TransportTelemetryMiddleware(cast(Any, cancelled_stream))
    scope = cast(
        Any,
        {
            "type": "http",
            "method": "GET",
            "path": "/stream",
            "raw_path": b"/stream",
            "query_string": b"",
            "headers": [],
            "scheme": "http",
            "server": ("test", 80),
            "client": ("test", 1234),
            "root_path": "",
            "http_version": "1.1",
        },
    )
    await middleware(scope, receive, send)
    assert observed_statuses == [499]


@pytest.mark.asyncio
async def test_websocket_send_disconnect_swallowed_downstream_finishes_as_1006(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 下游吞掉 send 异常时 outer middleware 仍保留 1006 / The outer middleware retains 1006 when downstream swallows a send failure.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    """

    observed: list[tuple[bool, int, bool]] = []

    async def websocket_app(
        _scope: object,
        _receive: object,
        send: object,
    ) -> None:
        """@brief 模拟 Starlette 将 OSError 转换并由 route 吞掉 / Simulate a converted send error swallowed by the route."""

        typed_send = cast(Any, send)
        await typed_send({"type": "websocket.accept"})
        try:
            await typed_send({"type": "websocket.send", "text": "payload"})
        except OSError:
            return

    def observe(
        _scope: object,
        accepted: bool,
        close_code: int,
        server_error: bool,
        _started: float,
        _wall_start: datetime,
    ) -> None:
        """@brief 捕获 WebSocket 终态 / Capture the WebSocket terminal state."""

        observed.append((accepted, close_code, server_error))

    async def receive() -> dict[str, object]:
        """@brief 返回 connect 消息 / Return a connect message."""

        return {"type": "websocket.connect"}

    async def send(message: dict[str, object]) -> None:
        """@brief 只让 data send 模拟断连 / Simulate disconnect only for a data send.

        @param message 上游 WebSocket 消息 / Upstream WebSocket message.
        @raise OSError data send 时抛出 / Raised for a data send.
        """

        if message["type"] == "websocket.send":
            raise OSError("peer disconnected")

    monkeypatch.setattr(
        "backend.api.middleware.transport._observe_websocket_scope_completion", observe
    )
    middleware = TransportTelemetryMiddleware(cast(Any, websocket_app))
    scope = cast(
        Any,
        {
            "type": "websocket",
            "path": "/ws",
            "raw_path": b"/ws",
            "query_string": b"",
            "headers": [],
            "scheme": "ws",
            "server": ("test", 80),
            "client": ("test", 1234),
            "root_path": "",
            "subprotocols": [],
        },
    )
    await middleware(scope, receive, cast(Any, send))
    assert observed == [(True, 1006, False)]


@pytest.mark.asyncio
async def test_queued_exception_output_keeps_type_but_never_traceback_or_message_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief 日志入队不得把异常 secret 合并到 message / Queue preparation must not merge exception secrets into the message.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @param tmp_path 临时文件根目录 / Temporary file root.
    @return 无返回值 / No return value.
    """

    output = io.StringIO()
    monkeypatch.setattr(sys, "stderr", output)
    pipeline, _writer = _pipeline()
    pipeline.start()
    runtime = configure_logging(
        LoggingSettings(
            queue_capacity=8,
            routes=(LoggingRouteSettings("stderr", ("ERROR",)),),
            persist_structured_events=False,
        ),
        pipeline,
        tmp_path,
    )
    try:
        try:
            raise RuntimeError("TOP_SECRET_TOKEN")
        except RuntimeError:
            logging.getLogger("backend.security").error(
                "backend.http.unexpected_error",
                extra={"event_name": "backend.http.unexpected_error"},
                exc_info=True,
            )
    finally:
        runtime.close()
        await pipeline.close()

    rendered = output.getvalue()
    payload = json.loads(rendered)
    assert payload["message"] == "backend.http.unexpected_error"
    assert payload["exception_type"] == "RuntimeError"
    assert "TOP_SECRET_TOKEN" not in rendered
    assert "Traceback" not in rendered


@pytest.mark.asyncio
async def test_queue_prepare_failure_is_sanitized_counted_and_persisted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief producer 端 prepare 失败不得走标准 traceback 路径 / Producer-side prepare failures never use the standard traceback path.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @param tmp_path 临时文件根目录 / Temporary file root.
    """

    class SecretBearingMessage:
        """@brief 格式化时抛出含 secret 异常的消息 / Message raising a secret-bearing formatting exception."""

        def __str__(self) -> str:
            """@brief 模拟不可格式化的正文 / Simulate an unformattable body.

            @raise OSError 始终抛出 / Always raised.
            """

            raise OSError("TOP_SECRET_QUEUE_PREPARE")

    emergency_stderr = io.StringIO()
    normal_stdout = io.StringIO()
    monkeypatch.setattr(sys, "stderr", emergency_stderr)
    monkeypatch.setattr(sys, "stdout", normal_stdout)
    pipeline, writer = _pipeline()
    pipeline.start()
    runtime = configure_logging(
        LoggingSettings(
            queue_capacity=8,
            routes=(LoggingRouteSettings("stdout", ("INFO",)),),
            persist_structured_events=False,
        ),
        pipeline,
        tmp_path,
    )
    try:
        logging.getLogger("backend.output").info(SecretBearingMessage())
    finally:
        runtime.close()
        await pipeline.close()

    rendered = emergency_stderr.getvalue() + normal_stdout.getvalue()
    assert "TOP_SECRET_QUEUE_PREPARE" not in rendered
    assert "Traceback" not in rendered
    assert runtime.dropped_output_count == 1
    assert any(
        isinstance(signal, LogEvent) and signal.envelope.name == "aiws.logging.output.failed"
        for signal in writer.snapshot()
    )


@pytest.mark.asyncio
async def test_unstarted_pipeline_drops_without_touching_asyncio_queue() -> None:
    """@brief enabled 但未 start 的管线必须 fail closed / An enabled but unstarted pipeline fails closed."""
    pipeline, writer = _pipeline()
    accepted = pipeline.record_metric(
        "aiws.test.count",
        1,
        None,
        None,
        {"operation": "test", "outcome": "success"},
        metric_type=MetricType.COUNTER,
    )
    assert accepted is False
    assert pipeline.stats.accepted == 0
    assert pipeline.stats.dropped == 1
    assert writer.snapshot() == ()
    await pipeline.close()


@pytest.mark.asyncio
async def test_writer_failure_isolated_and_visible_without_recursive_logging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief writer 故障不影响业务且产生脱敏 emergency STDERR / Writer failure is isolated and emits sanitized emergency STDERR.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    """
    emergency = io.StringIO()
    monkeypatch.setattr(sys, "__stderr__", emergency)
    pipeline = ObservabilityPipeline(
        _FailingTelemetryWriter(),
        ResourceMetadata("backend.test"),
        queue_capacity=4,
        batch_size=4,
        flush_interval_ms=5,
        drop_policy="drop_newest",
        shutdown_flush_timeout_ms=200,
    )
    pipeline.start()
    assert pipeline.record_metric(
        "aiws.test.count",
        1,
        None,
        None,
        {"operation": "test", "outcome": "success"},
    )
    for _attempt in range(50):
        if pipeline.stats.write_failures == 1:
            break
        await asyncio.sleep(0.01)
    await pipeline.close()
    rendered = emergency.getvalue()
    assert pipeline.stats.write_failures == 1
    assert '"event_name":"aiws.telemetry.write_failed"' in rendered
    assert '"failed_batch_size":1' in rendered
    assert "secret database detail" not in rendered


@pytest.mark.asyncio
async def test_pipeline_close_deadline_does_not_wait_for_nonconforming_writer_cleanup() -> None:
    """@brief close 方法上限不等待违反端口契约的 writer / The close-method bound does not await a nonconforming writer."""

    writer = _CancellationResistantWriter()
    pipeline = ObservabilityPipeline(
        writer,
        ResourceMetadata("backend.test"),
        queue_capacity=4,
        batch_size=1,
        flush_interval_ms=1,
        drop_policy="drop_newest",
        shutdown_flush_timeout_ms=20,
    )
    pipeline.start()
    assert pipeline.record_metric(
        "aiws.test.count",
        1,
        None,
        None,
        {"operation": "test", "outcome": "success"},
    )
    await asyncio.wait_for(writer.entered.wait(), timeout=0.2)
    started = asyncio.get_running_loop().time()
    await pipeline.close()
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.10
    await asyncio.wait_for(writer.finished.wait(), timeout=0.5)
    assert pipeline.stats.write_failures == 1


@pytest.mark.asyncio
async def test_pipeline_persists_sparse_self_health_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 进程计数应全局持久化且仅为新增损失告警 / Process counters are global and warn only for new loss."""

    monkeypatch.setattr(
        "backend.infrastructure.observability.pipeline._HEALTH_SNAPSHOT_ACCEPTED_INTERVAL",
        2,
    )
    pipeline = ObservabilityPipeline(
        writer := InMemoryTelemetryWriter(),
        ResourceMetadata("backend.test"),
        queue_capacity=32,
        batch_size=32,
        flush_interval_ms=10,
        drop_policy="drop_newest",
        shutdown_flush_timeout_ms=500,
    )
    pipeline.start()
    tenant_scope = ActorScope("usr_test", "ws_test", "usr_test")

    assert pipeline.record_metric(
        "aiws.test.count",
        1,
        tenant_scope,
        "req-tenant",
        {"operation": "test", "outcome": "success"},
    )
    assert pipeline.record_health_snapshot(output_dropped_count=0)
    assert not pipeline.record_health_snapshot(output_dropped_count=0)
    assert pipeline.record_health_snapshot(output_dropped_count=2)
    assert pipeline.record_metric(
        "aiws.test.count",
        1,
        tenant_scope,
        "req-tenant",
        {"operation": "test", "outcome": "success"},
    )
    assert pipeline.record_health_snapshot(output_dropped_count=2)
    await pipeline.close()

    snapshots = [
        signal
        for signal in writer.snapshot()
        if isinstance(signal, LogEvent) and signal.envelope.name == "aiws.telemetry.health.snapshot"
    ]
    assert len(snapshots) == 3
    assert all(snapshot.envelope.scope is None for snapshot in snapshots)
    assert all(snapshot.envelope.request_id is None for snapshot in snapshots)
    assert snapshots[0].envelope.attributes["outcome"] == "success"
    assert snapshots[1].envelope.attributes["output_dropped_count"] == 2
    assert snapshots[1].severity_text == "WARNING"
    assert snapshots[2].envelope.attributes["output_dropped_count"] == 2
    assert snapshots[2].severity_text == "INFO"


@pytest.mark.asyncio
async def test_worker_thread_logging_uses_owner_loop_and_preserves_event_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief worker 线程日志经线程安全 handoff 入 DB 且共用 event ID / Worker-thread logs use a safe handoff and preserve the event ID.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @param tmp_path 临时文件根目录 / Temporary file root.
    """
    output = io.StringIO()
    monkeypatch.setattr("sys.stdout", output)
    pipeline, writer = _pipeline()
    pipeline.start()
    settings = LoggingSettings(
        queue_capacity=16,
        routes=(LoggingRouteSettings("stdout", ("WARNING",)),),
        persist_structured_events=True,
    )
    runtime = configure_logging(settings, pipeline, tmp_path)

    def produce() -> None:
        """@brief 在非 event-loop 线程发出日志 / Emit a log outside the event-loop thread."""
        logging.getLogger("backend.worker").warning(
            "backend.worker.retrying",
            extra={
                "event_name": "backend.worker.retrying",
                "telemetry_attributes": {"operation": "retry", "outcome": "accepted"},
            },
        )

    await asyncio.to_thread(produce)
    for _attempt in range(50):
        if writer.records:
            break
        await asyncio.sleep(0.01)
    runtime.close()
    await pipeline.close()

    assert len(writer.records) == 1
    signal = writer.records[0]
    assert isinstance(signal, LogEvent)
    rendered = output.getvalue()
    assert signal.envelope.event_id in rendered
    assert signal.envelope.name == "backend.worker.retrying"


@pytest.mark.asyncio
async def test_every_standard_level_fans_out_to_exact_stream_file_and_database_routes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief 五个标准等级按精确矩阵分流且共用一次 DB fan-out / Five standard levels follow the exact routing matrix with one DB fan-out.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @param tmp_path 临时文件根目录 / Temporary file root.
    @return 无返回值 / No return value.
    """

    stdout = io.StringIO()
    stderr = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    pipeline, writer = _pipeline()
    pipeline.start()
    levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    settings = LoggingSettings(
        queue_capacity=32,
        routes=(
            LoggingRouteSettings("stdout", ("DEBUG", "INFO")),
            LoggingRouteSettings("stderr", ("WARNING", "ERROR", "CRITICAL")),
            LoggingRouteSettings(
                "file",
                levels,
                Path("matrix/backend.jsonl"),
                64_000,
                2,
            ),
        ),
        persist_structured_events=True,
    )
    runtime = configure_logging(settings, pipeline, tmp_path)
    logger = logging.getLogger("backend.routing")
    for level_name in levels:
        event_name = f"backend.routing.{level_name.lower()}"
        logger.log(
            logging.getLevelNamesMapping()[level_name],
            event_name,
            extra={
                "event_name": event_name,
                "telemetry_attributes": {
                    "operation": "route",
                    "outcome": "failure" if level_name in {"ERROR", "CRITICAL"} else "success",
                },
            },
        )
    for _attempt in range(50):
        if len(writer.records) == len(levels):
            break
        await asyncio.sleep(0.01)
    runtime.close()
    await pipeline.close()

    def routed_levels(value: str) -> list[str]:
        """@brief 解析一组 JSONL 的等级 / Parse levels from JSON Lines.

        @param value JSONL 文本 / JSON Lines text.
        @return 顺序等级列表 / Ordered level names.
        """

        return [json.loads(line)["level"] for line in value.splitlines() if line]

    file_text = (tmp_path / "matrix" / "backend.jsonl").read_text(encoding="utf-8")
    assert routed_levels(stdout.getvalue()) == ["DEBUG", "INFO"]
    assert routed_levels(stderr.getvalue()) == ["WARNING", "ERROR", "CRITICAL"]
    assert routed_levels(file_text) == list(levels)
    assert [
        signal.severity_text for signal in writer.records if isinstance(signal, LogEvent)
    ] == list(levels)


@pytest.mark.asyncio
async def test_repeated_logging_configuration_replaces_prior_lifecycle(tmp_path: Path) -> None:
    """@brief 重复 configure 不得叠加 backend handlers / Reconfiguration must not stack backend handlers.

    @param tmp_path 临时文件根目录 / Temporary file root.
    """
    first_pipeline, _first_writer = _pipeline()
    second_pipeline, _second_writer = _pipeline()
    first_pipeline.start()
    second_pipeline.start()
    settings = LoggingSettings(
        queue_capacity=8,
        routes=(LoggingRouteSettings("stderr", ("ERROR",)),),
        persist_structured_events=True,
    )
    first = configure_logging(settings, first_pipeline, tmp_path)
    second = configure_logging(settings, second_pipeline, tmp_path)
    backend_logger = logging.getLogger("backend")
    assert len(backend_logger.handlers) == 2
    first.close()
    assert len(backend_logger.handlers) == 2
    second.close()
    assert backend_logger.handlers == []
    await first_pipeline.close()
    await second_pipeline.close()


def test_logging_shutdown_fences_producers_before_forcing_sentinel() -> None:
    """@brief 满队列关闭必须保证 sentinel 且计入被移除记录 / Full-queue shutdown guarantees a sentinel and counts the removed record."""

    records: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=1)
    queue_handler = NonBlockingQueueHandler(records)
    first = logging.LogRecord("backend.test", logging.INFO, __file__, 1, "first", (), None)
    late = logging.LogRecord("backend.test", logging.INFO, __file__, 1, "late", (), None)
    records.put_nowait(first)
    queue_handler.stop_accepting()
    queue_handler.enqueue(late)
    listener = _BoundedQueueListener(records, queue_handler)

    listener.enqueue_sentinel()

    assert records.get_nowait() is None
    records.task_done()
    assert queue_handler.dropped_count == 2


@pytest.mark.asyncio
async def test_logging_shutdown_is_bounded_when_one_sink_blocks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief 阻塞 sink 不得无限拖住应用关闭 / A blocked sink cannot hold application shutdown forever.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @param tmp_path 临时文件根目录 / Temporary file root.
    """

    entered = threading.Event()
    release = threading.Event()
    was_closed = threading.Event()
    survivor_received_follow_up = threading.Event()
    survivor_was_closed = threading.Event()
    survivor_records: list[str] = []
    survivor_lock = threading.Lock()

    class BlockingHandler(logging.Handler):
        """@brief 在测试栅栏上阻塞的输出 / Output blocked on a test fence."""

        def emit(self, record: logging.LogRecord) -> None:
            """@brief 等待测试释放 / Wait for the test to release the sink.

            @param record 待输出记录 / Record to output.
            """

            del record
            entered.set()
            release.wait()

        def close(self) -> None:
            """@brief 标记 reaper 已释放输出 / Mark that the reaper released the output."""

            was_closed.set()
            super().close()

    class RecordingHandler(logging.Handler):
        """@brief 记录未被兄弟 sink 阻塞的输出 / Record output that is not blocked by its sibling sink."""

        def emit(self, record: logging.LogRecord) -> None:
            """@brief 记录已完成输出 / Record a completed output.

            @param record 待输出记录 / Record to output.
            """

            message = record.getMessage()
            with survivor_lock:
                survivor_records.append(message)
            if message == "backend.logging.after_block":
                survivor_received_follow_up.set()

        def close(self) -> None:
            """@brief 记录独立 worker 已正常关闭 / Record normal closure of the independent worker."""

            survivor_was_closed.set()
            super().close()

    handler = BlockingHandler()
    survivor = RecordingHandler()
    injected_handlers = iter((handler, survivor))

    def output_handler(*_args: object) -> logging.Handler:
        """@brief 依次注入阻塞与健康输出 / Inject blocked and healthy outputs in order."""

        return next(injected_handlers)

    monkeypatch.setattr(
        "backend.infrastructure.observability.logging._output_handler",
        output_handler,
    )
    pipeline, _writer = _pipeline()
    pipeline.start()
    runtime = configure_logging(
        LoggingSettings(
            queue_capacity=8,
            routes=(
                LoggingRouteSettings("stdout", ("INFO",)),
                LoggingRouteSettings("stderr", ("INFO",)),
            ),
            persist_structured_events=False,
            shutdown_timeout_ms=20,
        ),
        pipeline,
        tmp_path,
    )
    try:
        logging.getLogger("backend.blocked").info("backend.logging.blocked")
        assert entered.wait(1.0)
        logging.getLogger("backend.blocked").info("backend.logging.after_block")
        assert survivor_received_follow_up.wait(1.0)
        started = monotonic()
        runtime.close()
        elapsed = monotonic() - started
        assert elapsed < 0.2
        assert runtime.dropped_output_count >= 1
        assert survivor_was_closed.wait(1.0)
        with survivor_lock:
            assert survivor_records == [
                "backend.logging.blocked",
                "backend.logging.after_block",
            ]
    finally:
        release.set()
        assert was_closed.wait(1.0)
        await asyncio.sleep(0)
        await pipeline.close()


@pytest.mark.asyncio
async def test_blocking_sink_close_is_isolated_and_reaped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief sink close 阻塞也不得占用关闭主线程 / A blocking sink close cannot occupy the shutdown thread.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @param tmp_path 临时文件根目录 / Temporary file root.
    """

    blocking_delivered = threading.Event()
    close_entered = threading.Event()
    release_close = threading.Event()
    close_finished = threading.Event()
    healthy_delivered = threading.Event()
    healthy_closed = threading.Event()

    class CloseBlockingHandler(logging.Handler):
        """@brief 只在 close 阶段阻塞的 sink / Sink that blocks only during close."""

        def emit(self, record: logging.LogRecord) -> None:
            """@brief 确认正常输出 / Confirm normal emission.

            @param record 待输出记录 / Record to output.
            """

            del record
            blocking_delivered.set()

        def close(self) -> None:
            """@brief 在测试栅栏上阻塞关闭 / Block close on the test fence."""

            close_entered.set()
            release_close.wait()
            close_finished.set()
            super().close()

    class HealthyHandler(logging.Handler):
        """@brief 验证兄弟 route 独立关闭的 sink / Sink proving independent sibling shutdown."""

        def emit(self, record: logging.LogRecord) -> None:
            """@brief 确认正常输出 / Confirm normal emission.

            @param record 待输出记录 / Record to output.
            """

            del record
            healthy_delivered.set()

        def close(self) -> None:
            """@brief 确认本 route 已完成关闭 / Confirm this route completed shutdown."""

            healthy_closed.set()
            super().close()

    injected_handlers = iter((CloseBlockingHandler(), HealthyHandler()))

    def output_handler(*_args: object) -> logging.Handler:
        """@brief 注入 close 阻塞与健康 sink / Inject close-blocked and healthy sinks."""

        return next(injected_handlers)

    monkeypatch.setattr(
        "backend.infrastructure.observability.logging._output_handler",
        output_handler,
    )
    pipeline, _writer = _pipeline()
    pipeline.start()
    runtime = configure_logging(
        LoggingSettings(
            queue_capacity=8,
            routes=(
                LoggingRouteSettings("stdout", ("INFO",)),
                LoggingRouteSettings("stderr", ("INFO",)),
            ),
            persist_structured_events=False,
            shutdown_timeout_ms=20,
        ),
        pipeline,
        tmp_path,
    )
    try:
        logging.getLogger("backend.close").info("backend.logging.close_probe")
        assert blocking_delivered.wait(1.0)
        assert healthy_delivered.wait(1.0)
        started = monotonic()
        runtime.close()
        assert monotonic() - started < 0.2
        assert close_entered.wait(1.0)
        assert healthy_closed.wait(1.0)
        assert runtime.dropped_output_count >= 1
    finally:
        release_close.set()
        assert close_finished.wait(1.0)
        await asyncio.sleep(0)
        await pipeline.close()


@pytest.mark.asyncio
async def test_stream_failure_is_isolated_sanitized_and_persisted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief 一个流失败不影响其它 route 且不泄漏异常 / One failed stream cannot break other routes or leak its exception.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @param tmp_path 临时文件根目录 / Temporary file root.
    """

    class FailingStream(io.StringIO):
        """@brief 写入时抛含 secret 异常的流 / Stream raising a secret-bearing exception on write."""

        def write(self, value: str) -> int:
            """@brief 拒绝写入 / Reject a write.

            @param value 待写文本 / Text to write.
            @raise OSError 始终失败 / Always raised.
            """

            del value
            raise OSError("TOP_SECRET_SINK_DETAIL")

    failing = FailingStream()
    surviving = io.StringIO()
    monkeypatch.setattr(sys, "stdout", failing)
    monkeypatch.setattr(sys, "stderr", surviving)
    pipeline, writer = _pipeline()
    pipeline.start()
    runtime = configure_logging(
        LoggingSettings(
            queue_capacity=8,
            routes=(
                LoggingRouteSettings("stdout", ("ERROR",)),
                LoggingRouteSettings("stderr", ("ERROR",)),
            ),
            persist_structured_events=False,
        ),
        pipeline,
        tmp_path,
    )
    logging.getLogger("backend.output").error(
        "backend.output.probe",
        extra={"event_name": "backend.output.probe"},
    )
    runtime.close()
    await asyncio.sleep(0)
    await pipeline.close()

    rendered = surviving.getvalue()
    assert "backend.output.probe" in rendered
    assert "TOP_SECRET_SINK_DETAIL" not in rendered
    assert "Traceback" not in rendered
    assert runtime.dropped_output_count >= 1
    assert any(
        isinstance(signal, LogEvent) and signal.envelope.name == "aiws.logging.output.failed"
        for signal in writer.snapshot()
    )


@pytest.mark.asyncio
async def test_rotated_log_files_remain_private(tmp_path: Path) -> None:
    """@brief 首个及 rollover 后文件都必须保持 0600 / Initial and post-rollover files remain mode 0600.

    @param tmp_path 临时文件根目录 / Temporary file root.
    @return 无返回值 / No return value.
    """

    pipeline, _writer = _pipeline()
    pipeline.start()
    settings = LoggingSettings(
        queue_capacity=32,
        routes=(
            LoggingRouteSettings(
                "file",
                ("INFO",),
                Path("private/backend.jsonl"),
                128,
                2,
            ),
        ),
        persist_structured_events=False,
    )
    runtime = None
    try:
        runtime = configure_logging(settings, pipeline, tmp_path)
        for index in range(8):
            logging.getLogger("backend.rotation").info(
                "backend.rotation.sample",
                extra={
                    "event_name": "backend.rotation.sample",
                    "telemetry_attributes": {"operation": "rotation", "outcome": "success"},
                    "sample_index": index,
                },
            )
    finally:
        if runtime is not None:
            runtime.close()
        await pipeline.close()

    files = tuple((tmp_path / "private").glob("backend.jsonl*"))
    assert len(files) >= 2
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in files)


def test_diagnostics_validates_entire_batch_before_emitting() -> None:
    """@brief 第 N 条时间非法时前 N-1 条不得部分提交 / An invalid Nth timestamp must not partially emit the prefix."""
    pipeline, writer = _pipeline()
    settings = _diagnostics_settings()
    service = DiagnosticIngestionService(settings, pipeline, DiagnosticRateLimiter(settings))
    scope = ActorScope("usr_test", "ws_test", "usr_test")
    now = datetime.now(UTC)
    valid = ClientErrorDiagnostic(
        "evt-valid",
        now,
        "/resumes/{resume_id}",
        "1.0.0",
        "ui.render.failed",
        None,
    )
    stale = ClientErrorDiagnostic(
        "evt-stale",
        now - timedelta(days=2),
        "/resumes/{resume_id}",
        "1.0.0",
        "ui.render.failed",
        None,
    )
    with pytest.raises(ValueError, match="clock window"):
        service.ingest(scope, "req-test", (valid, stale))
    assert pipeline.stats.accepted == 0
    assert writer.snapshot() == ()


@pytest.mark.asyncio
async def test_diagnostics_self_monitoring_is_scoped_and_low_cardinality() -> None:
    """@brief 准入自身指标必须完整、按 ActorScope 隔离且维度有界 / Admission self-metrics are complete, scoped, and bounded."""
    pipeline, writer = _pipeline()
    pipeline.start()
    settings = _diagnostics_settings()
    service = DiagnosticIngestionService(settings, pipeline, DiagnosticRateLimiter(settings))
    scope = ActorScope("usr_test", "ws_test", "owner_test")

    service.observe_ingestion(
        scope,
        "req-partial",
        payload_bytes=321,
        accepted=2,
        dropped=1,
        rate_limited=0,
        duration_seconds=0.125,
    )
    service.observe_ingestion(
        scope,
        "req-rate-limited",
        payload_bytes=123,
        accepted=0,
        dropped=0,
        rate_limited=3,
        duration_seconds=0.25,
    )
    await pipeline.close()

    metrics = [signal for signal in writer.snapshot() if isinstance(signal, MetricPoint)]
    assert len(metrics) == 9
    assert all(metric.envelope.scope == scope for metric in metrics)
    assert all(metric.envelope.resource.service == "backend.api" for metric in metrics)
    assert all(set(metric.envelope.attributes) == {"operation", "outcome"} for metric in metrics)
    assert all(metric.envelope.attributes["operation"] == "ingest" for metric in metrics)

    partial_metrics = [metric for metric in metrics if metric.envelope.request_id == "req-partial"]
    partial_by_name = {
        metric.envelope.name: metric
        for metric in partial_metrics
        if metric.envelope.name != "aiws.diagnostics.ingest.event.count"
    }
    assert partial_by_name["aiws.diagnostics.ingest.payload.size"].value == 321
    assert partial_by_name["aiws.diagnostics.ingest.payload.size"].unit == "By"
    assert partial_by_name["aiws.diagnostics.ingest.batch.count"].value == 1
    assert (
        partial_by_name["aiws.diagnostics.ingest.batch.count"].envelope.attributes["outcome"]
        == "partial"
    )
    assert partial_by_name["aiws.diagnostics.ingest.duration"].value == pytest.approx(0.125)
    event_counts = {
        metric.envelope.attributes["outcome"]: metric.value
        for metric in partial_metrics
        if metric.envelope.name == "aiws.diagnostics.ingest.event.count"
    }
    assert event_counts == {"accepted": 2.0, "dropped": 1.0}

    rate_limited_metrics = [
        metric for metric in metrics if metric.envelope.request_id == "req-rate-limited"
    ]
    rate_limited_events = [
        metric
        for metric in rate_limited_metrics
        if metric.envelope.name == "aiws.diagnostics.ingest.event.count"
    ]
    assert len(rate_limited_events) == 1
    assert rate_limited_events[0].value == 3
    assert rate_limited_events[0].envelope.attributes["outcome"] == "rate_limited"
    assert all(
        metric.envelope.attributes["outcome"] == "rate_limited" for metric in rate_limited_metrics
    )


def test_frontend_source_requires_scope_and_client_event_id() -> None:
    """@brief frontend 信号不能伪造为空 scope / Frontend signals cannot omit scope or client event ID."""
    with pytest.raises(ValueError, match="require scope"):
        MetricPoint(
            SignalEnvelope(
                SignalSource.FRONTEND,
                ResourceMetadata("frontend.browser"),
                "aiws.frontend.web_vital",
            ),
            MetricType.HISTOGRAM,
            1.0,
            "s",
        )


@pytest.mark.asyncio
async def test_logging_configuration_failure_closes_started_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief configure_logging 失败必须关闭已启动 worker / A configure_logging failure closes the started worker.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    """
    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")
    closed = asyncio.Event()
    original_close = ObservabilityPipeline.close

    async def tracked_close(pipeline: ObservabilityPipeline) -> None:
        """@brief 记录 composition 的对称关闭 / Track symmetric cleanup by composition."""
        await original_close(pipeline)
        closed.set()

    def fail_configuration(*_args: object, **_kwargs: object) -> None:
        """@brief 模拟日志文件/handler 初始化失败 / Simulate log-file or handler initialization failure."""
        raise OSError("logging route unavailable")

    monkeypatch.setattr(ObservabilityPipeline, "close", tracked_close)
    monkeypatch.setattr("backend.composition.configure_logging", fail_configuration)
    with pytest.raises(OSError, match="logging route unavailable"):
        async with build_container(settings, PROJECT_ROOT):
            pytest.fail("container must not yield after logging setup failure")
    assert closed.is_set()


@pytest.mark.asyncio
async def test_runtime_cleanup_attempts_every_resource_after_prior_failures() -> None:
    """@brief 前置 close 失败不得跳过日志、遥测或数据库资源 / An earlier close failure must not skip logging, telemetry, or databases."""

    calls: list[str] = []

    class Provider:
        """@brief 关闭时失败的 provider / Provider failing during close."""

        async def aclose(self) -> None:
            """@brief 记录并抛出首个失败 / Record and raise the first failure."""

            calls.append("provider")
            raise RuntimeError("provider close failed")

    class Logging:
        """@brief 同步日志资源替身 / Synchronous logging-resource double."""

        @property
        def dropped_output_count(self) -> int:
            """@brief 返回无丢弃计数 / Return a zero dropped-output count."""

            return 0

        def close(self) -> None:
            """@brief 记录日志关闭 / Record logging close."""

            calls.append("logging")

    class Telemetry:
        """@brief 异步遥测资源替身 / Asynchronous telemetry-resource double."""

        async def close(self) -> None:
            """@brief 记录遥测关闭 / Record telemetry close."""

            calls.append("telemetry")

    class Database:
        """@brief 异步数据库资源替身 / Asynchronous database-resource double."""

        def __init__(self, name: str) -> None:
            """@brief 保存资源名 / Store the resource name.

            @param name 记录名 / Recorded name.
            """

            self.name = name

        async def aclose(self) -> None:
            """@brief 记录数据库关闭 / Record database close."""

            calls.append(self.name)

    with pytest.raises(RuntimeError, match="provider close failed"):
        await _close_runtime_resources(
            Provider(),
            Logging(),
            Telemetry(),
            Database("database"),
            Database("telemetry_database"),
        )
    assert calls == ["provider", "logging", "telemetry", "database", "telemetry_database"]


@pytest.mark.asyncio
async def test_runtime_cleanup_persists_final_global_pipeline_health() -> None:
    """@brief 日志 listener 关闭后的最终丢弃计数必须进入全局快照 / Final listener losses enter a global snapshot."""

    pipeline, writer = _pipeline()
    pipeline.start()

    class Logging:
        """@brief 具有最终丢弃计数的日志资源替身 / Logging-resource double with a final drop count."""

        @property
        def dropped_output_count(self) -> int:
            """@brief 返回 listener 关闭后的累计丢弃 / Return losses after listener shutdown."""

            return 3

        def close(self) -> None:
            """@brief 模拟 listener 已停止 / Simulate a stopped listener."""

    await _close_runtime_resources(None, Logging(), pipeline, None, None)

    snapshots = [
        signal
        for signal in writer.snapshot()
        if isinstance(signal, LogEvent) and signal.envelope.name == "aiws.telemetry.health.snapshot"
    ]
    assert len(snapshots) == 1
    assert snapshots[0].envelope.scope is None
    assert snapshots[0].envelope.request_id is None
    assert snapshots[0].envelope.attributes["output_dropped_count"] == 3
    assert snapshots[0].severity_text == "WARNING"
