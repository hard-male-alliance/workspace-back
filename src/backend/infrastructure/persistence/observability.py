"""@brief PostgreSQL observability 批量 writer / PostgreSQL observability batch writer."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.domain.observability import LogEvent, MetricPoint, SpanEvent, TelemetrySignal
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import TelemetryRecord
from workspace_shared.tenancy import ActorScope


class PostgresTelemetryWriter:
    """@brief 使用独立小连接池批量写统一信号表 / Batch-write the unified signal table through an isolated small pool.

    @note writer 不调用 logger、不创建 identity 记录，也不产生自身 SQL span，因此数据库
    故障不会递归进入同一 observability 管线。
    """

    def __init__(self, database: AsyncDatabase) -> None:
        """@brief 绑定独立 telemetry 数据库资源 / Bind the isolated telemetry database resource.

        @param database 仅由 observability 使用的数据库连接池 / Database pool used only by observability.
        """
        self._database = database

    async def write_batch(self, records: list[TelemetrySignal]) -> None:
        """@brief 按 scope 批量 INSERT 并幂等忽略冲突 / Batch INSERT by scope and idempotently ignore conflicts.

        @param records 强类型信号批次 / Batch of strongly typed signals.
        @return 无返回值 / No return value.

        @note scope 分组仅用于安装 PostgreSQL RLS（Row-Level Security）事务上下文；
        全局 backend 事件使用全 NULL scope 并由专用 INSERT policy 接受。
        """
        grouped: dict[ActorScope | None, list[TelemetrySignal]] = defaultdict(list)
        for signal in records:
            grouped[signal.envelope.scope].append(signal)
        for scope, scoped_records in grouped.items():
            async with self._transaction(scope) as session:
                statement = insert(TelemetryRecord).values(
                    [_mapping(signal) for signal in scoped_records]
                )
                await session.execute(statement.on_conflict_do_nothing())

    @asynccontextmanager
    async def _transaction(self, scope: ActorScope | None) -> AsyncIterator[AsyncSession]:
        """@brief 打开 scoped 或 global 短事务 / Open a scoped or global short transaction.

        @param scope 可空租户范围 / Optional tenant scope.
        @return 当前批次 Session / Session for the current batch.
        """
        if scope is not None:
            async with self._database.transaction(scope) as session:
                yield session
            return
        async with self._database.unscoped_transaction() as session:
            yield session


def _mapping(signal: TelemetrySignal) -> dict[str, Any]:
    """@brief 将判别联合映射为互斥列 / Map the discriminated union to mutually exclusive columns.

    @param signal 强类型 telemetry 信号 / Strongly typed telemetry signal.
    @return 可用于 SQLAlchemy bulk INSERT 的字典 / Mapping for SQLAlchemy bulk INSERT.
    """
    envelope = signal.envelope
    scope = envelope.scope
    mapping: dict[str, Any] = {
        "id": envelope.event_id,
        "workspace_id": scope.workspace_id if scope is not None else None,
        "resource_owner_id": scope.resource_owner_id if scope is not None else None,
        "actor_id": scope.actor_id if scope is not None else None,
        "occurred_at": envelope.occurred_at,
        "observed_at": envelope.observed_at,
        "kind": signal.kind.value,
        "source": envelope.source.value,
        "service": envelope.resource.service,
        "service_version": envelope.resource.service_version,
        "deployment_environment": envelope.resource.deployment_environment,
        "service_instance_id": envelope.resource.service_instance_id,
        "name": envelope.name,
        "metric_type": None,
        "value": None,
        "unit": None,
        "severity_number": None,
        "severity_text": None,
        "duration_ms": None,
        "span_status": None,
        "request_id": envelope.request_id,
        "trace_id": envelope.trace_id,
        "span_id": envelope.span_id,
        "parent_span_id": envelope.parent_span_id,
        "client_event_id": envelope.client_event_id,
        "attributes": dict(envelope.attributes),
    }
    if isinstance(signal, MetricPoint):
        mapping.update(
            metric_type=signal.metric_type.value,
            value=signal.value,
            unit=signal.unit,
        )
    elif isinstance(signal, LogEvent):
        mapping.update(
            severity_number=int(signal.severity_number),
            severity_text=signal.severity_text,
        )
    elif isinstance(signal, SpanEvent):
        mapping.update(duration_ms=signal.duration_ms, span_status=signal.status.value)
    return mapping


__all__ = ["PostgresTelemetryWriter"]
