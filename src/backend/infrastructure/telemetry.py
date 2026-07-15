"""@brief 有界批量业务 telemetry 管线 / Bounded batched business telemetry pipeline."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from datetime import UTC, datetime

from backend.domain.ports import TelemetryWriter
from backend.infrastructure.concurrency import BoundedTaskSupervisor
from workspace_shared.observability import TelemetryRecord
from workspace_shared.tenancy import ActorScope

_ALLOWED_ATTRIBUTE_KEYS = frozenset({"operation", "outcome", "status_code", "provider", "capability", "job_type", "transport", "level"})
_telemetry_scope: ContextVar[ActorScope | None] = ContextVar("aiws_telemetry_scope", default=None)


@contextmanager
def bind_telemetry_scope(scope: ActorScope) -> Iterator[None]:
    """@brief 在当前任务绑定 telemetry 租户范围 / Bind a telemetry tenant scope in the current task.

    @param scope actor/workspace/resource-owner 范围 / Actor/workspace/resource-owner scope.
    @return 用完即复位的上下文 / Context reset after use.
    @note asyncio 子任务在创建时复制 ContextVar，故受监督的后台工作保留其请求范围。
    """
    token: Token[ActorScope | None] = _telemetry_scope.set(scope)
    try:
        yield
    finally:
        _telemetry_scope.reset(token)


def current_telemetry_scope() -> ActorScope | None:
    """@brief 返回当前任务的 telemetry 范围 / Return the current task's telemetry scope.

    @return 已绑定范围；无用户/租户上下文时为 None / Bound scope, or None without a user/tenant context.
    """
    return _telemetry_scope.get()


class BufferedTelemetrySink:
    """@brief 不阻塞业务请求的 telemetry sink / Telemetry sink that never blocks business requests.

    @note sink 自身不记录 telemetry，避免递归；满队列按明确策略采样/丢弃。
    """

    def __init__(
        self,
        writer: TelemetryWriter,
        queue_capacity: int,
        batch_size: int,
        flush_interval_ms: int,
        drop_policy: str,
        enabled: bool = True,
    ) -> None:
        """@brief 初始化有界 sink / Initialize the bounded sink.

        @param writer 持久化写入端口 / Persistence writer port.
        @param queue_capacity 最大未写记录数 / Maximum unwritten records.
        @param batch_size 批大小 / Batch size.
        @param flush_interval_ms 刷新周期 / Flush interval.
        @param drop_policy drop_newest 或 drop_oldest / drop_newest or drop_oldest.
        """
        self._writer = writer
        self._queue: asyncio.Queue[TelemetryRecord] = asyncio.Queue(maxsize=queue_capacity)
        self._batch_size = batch_size
        self._flush_interval_seconds = flush_interval_ms / 1000
        self._drop_policy = drop_policy
        self._closed = not enabled
        self._dropped = 0

    @property
    def dropped_count(self) -> int:
        """@brief 返回已丢弃数量 / Return the number of dropped records.

        @return 丢弃计数 / Drop count.
        """
        return self._dropped

    def record(
        self,
        kind: str,
        name: str,
        value: float | None,
        scope: ActorScope,
        request_id: str | None,
        attributes: dict[str, str | int | float | bool],
        *,
        service: str = "backend",
    ) -> None:
        """@brief 非阻塞提交低基数 telemetry / Non-blockingly submit low-cardinality telemetry.

        @param kind metric、log 或 span / metric, log, or span.
        @param name 稳定名称 / Stable name.
        @param value 数值值 / Numeric value.
        @param scope 必填多租户范围 / Required multi-tenant scope.
        @param request_id 可选请求 ID / Optional request ID.
        @param attributes 已白名单的低基数标签 / Whitelisted low-cardinality labels.
        @param service 稳定服务名 / Stable service name.
        """
        if (
            self._closed
            or not isinstance(scope, ActorScope)
            or not service
            or not set(attributes).issubset(_ALLOWED_ATTRIBUTE_KEYS)
        ):
            self._dropped += 1
            return
        record = TelemetryRecord(
            occurred_at=datetime.now(UTC),
            kind=kind,  # type: ignore[arg-type]
            actor_id=scope.actor_id,
            workspace_id=scope.workspace_id,
            resource_owner_id=scope.resource_owner_id,
            service=service,
            name=name,
            value=value,
            request_id=request_id,
            attributes=attributes,
        )
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._drop_policy == "drop_oldest":
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                    self._queue.put_nowait(record)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass

    def start(self, supervisor: BoundedTaskSupervisor) -> None:
        """@brief 在 lifespan TaskGroup 中启动 worker / Start the worker in the lifespan TaskGroup.

        @param supervisor 应用任务监督器 / Application task supervisor.
        """
        if not self._closed:
            supervisor.submit("telemetry", self._flush_loop, name="aiws:telemetry-flush")

    async def close(self) -> None:
        """@brief 请求 telemetry worker 停止 / Request that the telemetry worker stop."""
        self._closed = True

    async def _flush_loop(self) -> None:
        """@brief 批量刷新队列 / Batch-flush the queue.

        @note 写入失败被隔离，绝不拖垮请求或再次产生 telemetry。
        """
        while not self._closed or not self._queue.empty():
            batch = await self._take_batch()
            if not batch:
                continue
            try:
                await self._writer.write_batch(batch)
            except asyncio.CancelledError:
                raise
            except BaseException:
                self._dropped += len(batch)

    async def _take_batch(self) -> list[TelemetryRecord]:
        """@brief 取得一个受时间限制的批次 / Take one time-bounded batch.

        @return 可写批次；超时时为空 / Writable batch, empty on timeout.
        """
        try:
            async with asyncio.timeout(self._flush_interval_seconds):
                first = await self._queue.get()
        except TimeoutError:
            return []
        batch = [first]
        self._queue.task_done()
        while len(batch) < self._batch_size:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            batch.append(item)
            self._queue.task_done()
        return batch


class InMemoryTelemetryWriter:
    """@brief 确定性内存 telemetry writer / Deterministic in-memory telemetry writer.

    @note MOCK — 非生产持久化实现 / MOCK — non-production persistence implementation.
    """

    def __init__(self) -> None:
        """@brief 初始化内存记录列表 / Initialize the in-memory record list."""
        self.records: list[TelemetryRecord] = []

    async def write_batch(self, records: list[TelemetryRecord]) -> None:
        """@brief 追加一个记录批次 / Append a record batch.

        @param records 已过滤记录 / Filtered records.
        """
        self.records.extend(records)

    def snapshot(self) -> tuple[TelemetryRecord, ...]:
        """@brief 返回稳定快照 / Return a stable snapshot.

        @return telemetry 记录元组 / Tuple of telemetry records.
        """
        return tuple(self.records)


def iter_low_cardinality_attributes(
    attributes: dict[str, str | int | float | bool],
) -> Iterable[tuple[str, str | int | float | bool]]:
    """@brief 过滤低基数 attributes / Filter low-cardinality attributes.

    @param attributes 候选 attributes / Candidate attributes.
    @return 白名单键值对 / Whitelisted key-value pairs.
    """
    return ((key, value) for key, value in attributes.items() if key in _ALLOWED_ATTRIBUTE_KEYS)
