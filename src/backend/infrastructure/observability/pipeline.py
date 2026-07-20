"""@brief 独立有界 observability 写入管线 / Independent bounded observability pipeline."""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from time import monotonic

from backend.domain.observability import (
    AttributeValue,
    LogEvent,
    MetricPoint,
    MetricType,
    ResourceMetadata,
    SeverityNumber,
    SignalEnvelope,
    SignalSource,
    SpanEvent,
    SpanStatus,
    TelemetrySignal,
)
from backend.domain.ports import TelemetryWriter
from backend.infrastructure.observability.context import current_observability_context
from workspace_shared.ids import new_opaque_id
from workspace_shared.tenancy import ActorScope

_EMERGENCY_INTERVAL_SECONDS = 60.0
"""@brief telemetry writer 故障 STDERR 最小间隔 / Minimum interval for writer-failure STDERR notices."""

_HEALTH_SNAPSHOT_ACCEPTED_INTERVAL = 1_000
"""@brief 无损期间持久化一次健康快照的信号间隔 / Signal interval for a persisted healthy snapshot."""


@dataclass(frozen=True, slots=True)
class PipelineStats:
    """@brief observability 管线的单调计数快照 / Monotonic observability-pipeline snapshot.

    @param accepted 已进入有界队列的信号数 / Signals admitted to the bounded queue.
    @param dropped 因关闭、无效或背压丢弃的信号数 / Signals dropped by close, validation, or pressure.
    @param write_failures 持久化失败的信号数 / Signals whose persistence failed.
    @param queue_size 当前排队数 / Current queued signals.
    @param queue_capacity 队列容量 / Queue capacity.
    @param queue_utilization 当前队列利用率 / Current queue utilization.
    """

    accepted: int
    dropped: int
    write_failures: int
    queue_size: int
    queue_capacity: int
    queue_utilization: float


class ObservabilityPipeline:
    """@brief 与业务 supervisor 隔离的非阻塞批量管线 / Non-blocking batch pipeline isolated from the business supervisor.

    @note worker 由本对象直接拥有；持久化失败只更新内部计数，绝不调用 logging，
    从而避免 telemetry writer → logger → telemetry writer 的递归故障。
    """

    def __init__(
        self,
        writer: TelemetryWriter,
        resource: ResourceMetadata,
        queue_capacity: int,
        batch_size: int,
        flush_interval_ms: int,
        drop_policy: str,
        shutdown_flush_timeout_ms: int,
        enabled: bool = True,
    ) -> None:
        """@brief 初始化有界管线 / Initialize the bounded pipeline.

        @param writer 批量持久化端口 / Batch persistence port.
        @param resource 默认资源元数据 / Default resource metadata.
        @param queue_capacity 最大待写信号数 / Maximum queued signals.
        @param batch_size 单批最大信号数 / Maximum batch size.
        @param flush_interval_ms 最大聚合等待时间 / Maximum batching delay.
        @param drop_policy ``drop_newest`` 或 ``drop_oldest`` / ``drop_newest`` or ``drop_oldest``.
        @param shutdown_flush_timeout_ms 关闭时最长刷新时间 / Maximum shutdown flush duration.
        @param enabled 是否接受信号 / Whether signal admission is enabled.
        """
        if queue_capacity < 1 or batch_size < 1 or flush_interval_ms < 1:
            raise ValueError("observability queue and batching limits must be positive")
        if shutdown_flush_timeout_ms < 1:
            raise ValueError("observability shutdown flush timeout must be positive")
        if drop_policy not in {"drop_newest", "drop_oldest"}:
            raise ValueError("unsupported observability drop policy")
        self._writer = writer
        self._resource = resource
        self._queue: asyncio.Queue[TelemetrySignal] = asyncio.Queue(maxsize=queue_capacity)
        self._batch_size = batch_size
        self._flush_interval_seconds = flush_interval_ms / 1_000
        self._shutdown_flush_timeout_seconds = shutdown_flush_timeout_ms / 1_000
        self._drop_policy = drop_policy
        self._enabled = enabled
        self._closing = False
        self._worker: asyncio.Task[None] | None = None
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._owner_thread_id: int | None = None
        self._state_lock = threading.Lock()
        self._accepted = 0
        self._dropped = 0
        self._write_failures = 0
        self._last_emergency_at = float("-inf")
        self._suppressed_emergencies = 0
        self._last_health_snapshot: tuple[int, int, int, int] | None = None

    @property
    def dropped_count(self) -> int:
        """@brief 返回累计丢弃数 / Return cumulative dropped signals.

        @return 累计丢弃数 / Cumulative dropped count.
        """
        with self._state_lock:
            return self._dropped

    @property
    def queue_utilization(self) -> float:
        """@brief 返回当前队列利用率 / Return current queue utilization.

        @return ``qsize / capacity``，范围为 0..1 / ``qsize / capacity`` in 0..1.
        """
        return self._queue.qsize() / self._queue.maxsize

    @property
    def stats(self) -> PipelineStats:
        """@brief 返回一致的轻量统计快照 / Return a coherent lightweight stats snapshot.

        @return 当前管线统计 / Current pipeline statistics.
        """
        with self._state_lock:
            accepted = self._accepted
            dropped = self._dropped
            write_failures = self._write_failures
        return PipelineStats(
            accepted=accepted,
            dropped=dropped,
            write_failures=write_failures,
            queue_size=self._queue.qsize(),
            queue_capacity=self._queue.maxsize,
            queue_utilization=self.queue_utilization,
        )

    def start(self) -> None:
        """@brief 启动自有 flush worker / Start the owned flush worker.

        @raise RuntimeError 重复启动时抛出 / Raised when started more than once.
        """
        if self._worker is not None:
            raise RuntimeError("observability pipeline is already started")
        if self._enabled and not self._closing:
            self._owner_loop = asyncio.get_running_loop()
            self._owner_thread_id = threading.get_ident()
            self._worker = asyncio.create_task(
                self._flush_loop(), name="aiws:observability-flush"
            )

    def emit(self, signal: TelemetrySignal) -> bool:
        """@brief 非阻塞提交一个已验证信号 / Non-blockingly submit one validated signal.

        @param signal 强类型 telemetry 信号 / Strongly typed telemetry signal.
        @return owner 线程中表示已入队；外部线程中仅表示已成功调度到 owner loop。
        / On the owner thread, true means enqueued; on foreign threads, it only means scheduled.
        """
        with self._state_lock:
            unavailable = not self._enabled or self._closing or self._owner_loop is None
            owner_loop = self._owner_loop
            owner_thread_id = self._owner_thread_id
        if unavailable:
            self._increment_dropped()
            return False
        if owner_loop is not None and owner_thread_id != threading.get_ident():
            try:
                owner_loop.call_soon_threadsafe(self._enqueue_on_owner, signal)
            except RuntimeError:
                self._increment_dropped()
                return False
            return True
        return self._enqueue_on_owner(signal)

    def _enqueue_on_owner(self, signal: TelemetrySignal) -> bool:
        """@brief 仅在 owner loop 操作 asyncio.Queue / Operate the asyncio.Queue only on its owner loop.

        @param signal 已验证信号 / Validated signal.
        @return 成功入队时为真 / True when enqueued.
        """
        with self._state_lock:
            unavailable = not self._enabled or self._closing or self._owner_loop is None
        if unavailable:
            self._increment_dropped()
            return False
        try:
            self._queue.put_nowait(signal)
        except asyncio.QueueFull:
            self._increment_dropped()
            if self._drop_policy != "drop_oldest":
                return False
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self._queue.put_nowait(signal)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                return False
        with self._state_lock:
            self._accepted += 1
        return True

    def record_metric(
        self,
        name: str,
        value: float,
        scope: ActorScope | None,
        request_id: str | None,
        attributes: dict[str, AttributeValue],
        *,
        service: str = "backend.worker",
        metric_type: MetricType = MetricType.COUNTER,
        unit: str = "{event}",
        source: SignalSource = SignalSource.BACKEND,
        client_event_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> bool:
        """@brief 构造并提交 metric / Construct and submit a metric.

        @param name 稳定仪器名 / Stable instrument name.
        @param value 有限数值 / Finite value.
        @param scope 可空租户范围 / Optional tenant scope.
        @param request_id 请求关联 ID / Request correlation ID.
        @param attributes 低基数属性 / Low-cardinality attributes.
        @param service 稳定服务名 / Stable service name.
        @param metric_type 仪器类型 / Instrument type.
        @param unit 规范单位 / Canonical unit.
        @param source 可信信号来源 / Trusted signal producer.
        @param client_event_id 前端幂等 ID / Frontend idempotency ID.
        @param occurred_at 事件时间；默认当前 UTC / Event time; defaults to current UTC.
        @return 成功进入队列时为真 / True when admitted to the queue.
        """
        try:
            envelope = self._envelope(
                name,
                scope,
                request_id,
                attributes,
                service=service,
                source=source,
                client_event_id=client_event_id,
                occurred_at=occurred_at,
            )
            signal = MetricPoint(envelope, metric_type, float(value), unit)
        except (TypeError, ValueError):
            self._increment_dropped()
            return False
        return self.emit(signal)

    def record_log(
        self,
        name: str,
        severity_number: SeverityNumber,
        severity_text: str,
        scope: ActorScope | None,
        request_id: str | None,
        attributes: dict[str, AttributeValue],
        *,
        service: str = "backend",
        source: SignalSource = SignalSource.BACKEND,
        client_event_id: str | None = None,
        occurred_at: datetime | None = None,
        event_id: str | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
        parent_span_id: str | None = None,
    ) -> bool:
        """@brief 构造并提交稳定日志事件 / Construct and submit a stable log event.

        @return 成功进入队列时为真 / True when admitted to the queue.
        """
        try:
            envelope = self._envelope(
                name,
                scope,
                request_id,
                attributes,
                service=service,
                source=source,
                client_event_id=client_event_id,
                occurred_at=occurred_at,
                event_id=event_id,
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
            )
            signal = LogEvent(envelope, severity_number, severity_text)
        except (TypeError, ValueError):
            self._increment_dropped()
            return False
        return self.emit(signal)

    def record_span(
        self,
        name: str,
        duration_ms: float,
        status: SpanStatus,
        scope: ActorScope | None,
        request_id: str | None,
        attributes: dict[str, AttributeValue],
        *,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        service: str = "backend.api",
        occurred_at: datetime | None = None,
    ) -> bool:
        """@brief 构造并提交已结束 span / Construct and submit a completed span.

        @return 成功进入队列时为真 / True when admitted to the queue.
        """
        try:
            envelope = self._envelope(
                name,
                scope,
                request_id,
                attributes,
                service=service,
                source=SignalSource.BACKEND,
                client_event_id=None,
                occurred_at=occurred_at,
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
            )
            signal = SpanEvent(envelope, float(duration_ms), status)
        except (TypeError, ValueError):
            self._increment_dropped()
            return False
        return self.emit(signal)

    async def close(self) -> None:
        """@brief 在有界时间内刷新并关闭 worker / Flush and close the worker within a bound.

        @return 无返回值 / No return value.

        @note 配置上限约束本方法；完整进程的退出上限还依赖 ``TelemetryWriter`` 传播
        cancellation 的端口契约，因为 Python coroutine 不可被强杀。/ The configured bound
        constrains this method. A whole-process exit bound additionally relies on the
        ``TelemetryWriter`` cancellation contract because Python coroutines cannot be forcibly killed.
        """
        with self._state_lock:
            self._closing = True
        worker = self._worker
        if worker is None:
            self._discard_queued()
            return
        try:
            await asyncio.wait_for(
                asyncio.shield(worker), timeout=self._shutdown_flush_timeout_seconds
            )
        except TimeoutError:
            worker.cancel()
            worker.add_done_callback(_consume_detached_worker_result)
            self._discard_queued()
        finally:
            self._worker = None

    def record_health_snapshot(
        self,
        *,
        output_dropped_count: int,
        force: bool = False,
    ) -> bool:
        """@brief 稀疏持久化管线与日志丢失状态 / Sparsely persist pipeline and logging-loss health.

        @param output_dropped_count 日志输出队列累计丢弃数 / Cumulative logging-output drops.
        @param force 即使未到稀疏采样间隔也提交最终快照 / Submit a final snapshot even before the sparse interval.
        @return 新快照进入队列时为真 / True when a new snapshot enters the queue.

        @note 首次调用、任一丢失计数变化或每新增 1000 个 accepted signal 时记录一次；
        满队列时不递归制造新的 drop。/ A snapshot is recorded initially, whenever any loss
        counter changes, or after 1,000 more accepted signals; a full queue is never amplified
        by recursively attempting self-observation.

        @note 这些计数属于进程而非租户，因此快照强制使用 global scope 且不携带触发请求
        ID，禁止把跨租户累计值错误归因给最后一个请求。/ These counters belong to the
        process rather than a tenant, so snapshots always use global scope and omit the triggering
        request ID; process-wide activity must never be attributed to the last request's tenant.
        """

        if (
            isinstance(output_dropped_count, bool)
            or not isinstance(output_dropped_count, int)
            or output_dropped_count < 0
        ):
            raise ValueError("output_dropped_count must be a non-negative integer")
        if not isinstance(force, bool):
            raise ValueError("force must be a boolean")
        stats = self.stats
        current = (
            stats.accepted,
            stats.dropped,
            stats.write_failures,
            output_dropped_count,
        )
        with self._state_lock:
            previous = self._last_health_snapshot
        has_new_loss = any(current[1:]) if previous is None else any(
            candidate > prior
            for candidate, prior in zip(current[1:], previous[1:], strict=True)
        )
        if previous is not None and not force:
            loss_unchanged = current[1:] == previous[1:]
            accepted_delta = current[0] - previous[0]
            if loss_unchanged and accepted_delta < _HEALTH_SNAPSHOT_ACCEPTED_INTERVAL:
                return False
        if self._queue.full():
            return False
        admitted = self.record_log(
            "aiws.telemetry.health.snapshot",
            SeverityNumber.WARN if has_new_loss else SeverityNumber.INFO,
            "WARNING" if has_new_loss else "INFO",
            None,
            None,
            {
                "accepted_count": current[0],
                "dropped_count": current[1],
                "write_failure_count": current[2],
                "output_dropped_count": current[3],
                "operation": "export",
                "outcome": "failure" if has_new_loss else "success",
            },
            service="backend.observability",
        )
        if admitted:
            with self._state_lock:
                self._last_health_snapshot = current
        return admitted

    async def _flush_loop(self) -> None:
        """@brief 周期批量写入直到关闭且队列为空 / Batch-write until closing and drained."""
        while not self._closing or not self._queue.empty():
            batch = await self._take_batch()
            if not batch:
                continue
            try:
                await self._writer.write_batch(batch)
            except asyncio.CancelledError:
                self._increment_write_failures(len(batch))
                self._increment_dropped(len(batch))
                raise
            except BaseException:
                self._increment_write_failures(len(batch))
                self._increment_dropped(len(batch))
                self._emit_emergency_write_failure(len(batch))
            finally:
                for _signal in batch:
                    self._queue.task_done()

    async def _take_batch(self) -> list[TelemetrySignal]:
        """@brief 取得一个受时间约束的批次 / Take one time-bounded batch.

        @return 可写批次；周期内无信号时为空 / Writable batch, empty when the interval elapses.
        """
        try:
            async with asyncio.timeout(self._flush_interval_seconds):
                first = await self._queue.get()
        except TimeoutError:
            return []
        batch = [first]
        while len(batch) < self._batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    def _envelope(
        self,
        name: str,
        scope: ActorScope | None,
        request_id: str | None,
        attributes: dict[str, AttributeValue],
        *,
        service: str,
        source: SignalSource,
        client_event_id: str | None,
        occurred_at: datetime | None,
        trace_id: str | None = None,
        span_id: str | None = None,
        parent_span_id: str | None = None,
        event_id: str | None = None,
    ) -> SignalEnvelope:
        """@brief 从显式参数与当前上下文构造信封 / Build an envelope from arguments and context.

        @return 已验证信封 / Validated envelope.
        """
        context = current_observability_context()
        trace = context.trace
        resolved_trace_id = trace_id or (trace.trace_id if trace is not None else None)
        resolved_span_id = span_id or (trace.span_id if trace is not None else None)
        resolved_parent = parent_span_id
        if trace_id is None and trace is not None:
            resolved_parent = trace.parent_span_id
        return SignalEnvelope(
            source=source,
            resource=replace(self._resource, service=service),
            name=name,
            occurred_at=occurred_at or datetime.now(UTC),
            event_id=event_id or new_opaque_id("tel"),
            scope=scope,
            request_id=request_id,
            trace_id=resolved_trace_id,
            span_id=resolved_span_id,
            parent_span_id=resolved_parent,
            client_event_id=client_event_id,
            attributes=attributes,
        )

    def _discard_queued(self) -> None:
        """@brief 丢弃剩余队列并更新计数 / Discard the remaining queue and update counters."""
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._queue.task_done()
            self._increment_dropped()

    def _increment_dropped(self, count: int = 1) -> None:
        """@brief 线程安全地累计丢弃数 / Thread-safely increment dropped signals.

        @param count 增量 / Increment.
        """
        with self._state_lock:
            self._dropped += count

    def _increment_write_failures(self, count: int) -> None:
        """@brief 线程安全地累计写失败数 / Thread-safely increment write failures.

        @param count 增量 / Increment.
        """
        with self._state_lock:
            self._write_failures += count

    def _emit_emergency_write_failure(self, batch_size: int) -> None:
        """@brief 绕过 logging 限频输出 writer 故障 / Rate-limit a writer failure directly outside logging.

        @param batch_size 本次失败批大小 / Size of the failed batch.

        @note 输出不包含异常文本、DSN 或 payload；写 STDERR 自身失败也必须吞掉，避免
        observability 故障改变业务控制流或递归回到 pipeline。
        """
        now = monotonic()
        if now - self._last_emergency_at < _EMERGENCY_INTERVAL_SECONDS:
            self._suppressed_emergencies += 1
            return
        suppressed = self._suppressed_emergencies
        self._last_emergency_at = now
        self._suppressed_emergencies = 0
        payload = {
            "event_name": "aiws.telemetry.write_failed",
            "failed_batch_size": batch_size,
            "suppressed_since_last": suppressed,
        }
        try:
            stream = sys.__stderr__
            if stream is None:
                return
            stream.write(json.dumps(payload, separators=(",", ":")) + "\n")
            stream.flush()
        except BaseException:
            return


class InMemoryTelemetryWriter:
    """@brief 测试用确定性内存 writer / Deterministic in-memory writer for tests.

    @note MOCK — 不用于生产持久化 / MOCK — not for production persistence.
    """

    def __init__(self) -> None:
        """@brief 初始化记录列表 / Initialize the record list."""
        self.records: list[TelemetrySignal] = []

    async def write_batch(self, records: list[TelemetrySignal]) -> None:
        """@brief 追加一个批次 / Append one batch.

        @param records 强类型信号 / Strongly typed signals.
        """
        self.records.extend(records)

    def snapshot(self) -> tuple[TelemetrySignal, ...]:
        """@brief 返回稳定快照 / Return a stable snapshot.

        @return 不可变信号元组 / Immutable signal tuple.
        """
        return tuple(self.records)


def _consume_detached_worker_result(worker: asyncio.Task[None]) -> None:
    """@brief 回收超过 close deadline 的非合规 worker 结果 / Consume a nonconforming worker result beyond the close deadline.

    @param worker 已取消但不得继续阻塞关闭的任务 / Cancelled task that must not keep shutdown blocked.
    @return 无返回值 / No return value.

    @note Python coroutine 无法被强制终止；若 writer 违反端口契约并吞掉 cancellation，
    任务只能在后台自行结束。回调取得异常以避免 ``Task exception was never retrieved``；
    此时仅 ``ObservabilityPipeline.close`` 返回时间受配置约束，不能宣称进程退出有硬上限。
    """

    try:
        worker.exception()
    except asyncio.CancelledError:
        return


__all__ = ["InMemoryTelemetryWriter", "ObservabilityPipeline", "PipelineStats"]
