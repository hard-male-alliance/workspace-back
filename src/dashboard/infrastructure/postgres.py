"""@brief PostgreSQL Dashboard 聚合读存储 / PostgreSQL Dashboard aggregate read store."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import URL, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dashboard.application.errors import DashboardConfigurationError, DashboardReadStoreUnavailable
from dashboard.application.ports import (
    DiagnosticEventRow,
    EventReadRequest,
    OverviewReadRequest,
    ServiceSignalRow,
    SystemHealthReadRequest,
    SystemHealthRow,
    TrendReadRequest,
    TrendSignalRow,
)
from dashboard.domain.model import SignalKind

_DASHBOARD_RELATION = "observability.dashboard_signals"
"""@brief migration 提供的固定只读投影视图 / Fixed read-only projection supplied by migration."""

_OVERVIEW_SQL = """
SELECT
    service,
    COALESCE(sum(value) FILTER (
        WHERE name = 'aiws.http.server.request.count'
    ), 0) AS request_count,
    COALESCE(sum(value) FILTER (
        WHERE name = 'aiws.http.server.error.count'
    ), 0) AS error_count,
    percentile_cont(ARRAY[0.50, 0.95, 0.99]) WITHIN GROUP (ORDER BY value * 1000.0)
        FILTER (WHERE name = 'http.server.request.duration') AS latency_percentiles,
    avg(value) FILTER (
        WHERE name IN (
            'aiws.runtime.supervisor.utilization',
            'aiws.telemetry.queue.utilization'
        )
    ) AS saturation_mean,
    max(value) FILTER (
        WHERE name IN (
            'aiws.runtime.supervisor.utilization',
            'aiws.telemetry.queue.utilization'
        )
    ) AS saturation_max,
    count(*) FILTER (WHERE kind = 'metric') AS sample_count,
    max(
        GREATEST(
            EXTRACT(EPOCH FROM (observed_at - occurred_at)),
            0.0
        )
    ) AS max_collection_lag_seconds,
    max(observed_at) AS latest_observed_at
FROM observability.dashboard_signals
WHERE workspace_id = :workspace_id
  AND occurred_at >= :start_at
  AND occurred_at < :end_at
  AND kind = 'metric'
  AND name IN (
      'aiws.http.server.request.count',
      'aiws.http.server.error.count',
      'http.server.request.duration',
      'aiws.runtime.supervisor.utilization',
      'aiws.telemetry.queue.utilization'
  )
  {service_predicate}
GROUP BY service
ORDER BY service ASC
"""
"""@brief 完整窗口服务聚合 SQL / Complete-window service aggregation SQL."""

_TREND_SQL_TEMPLATE = """
SELECT
    date_bin(
        make_interval(secs => :bucket_seconds),
        occurred_at,
        TIMESTAMPTZ '1970-01-01 00:00:00+00'
    ) AS bucket_start,
    service,
    {signal_aggregates}
FROM observability.dashboard_signals
WHERE workspace_id = :workspace_id
  AND occurred_at >= :start_at
  AND occurred_at < :end_at
  AND kind = 'metric'
  AND name IN ({metric_names})
  {service_predicate}
GROUP BY bucket_start, service
ORDER BY bucket_start ASC, service ASC
"""
"""@brief 数据库侧 date_bin 趋势模板 / Database-side ``date_bin`` trend template."""

_TREND_AGGREGATES = {
    SignalKind.TRAFFIC: """
        COALESCE(sum(value), 0) AS request_count,
        0.0::double precision AS error_count,
        NULL::double precision[] AS latency_percentiles,
        NULL::double precision AS saturation_mean,
        NULL::double precision AS saturation_max
    """,
    SignalKind.ERRORS: """
        COALESCE(sum(value) FILTER (
            WHERE name = 'aiws.http.server.request.count'
        ), 0) AS request_count,
        COALESCE(sum(value) FILTER (
            WHERE name = 'aiws.http.server.error.count'
        ), 0) AS error_count,
        NULL::double precision[] AS latency_percentiles,
        NULL::double precision AS saturation_mean,
        NULL::double precision AS saturation_max
    """,
    SignalKind.LATENCY: """
        0.0::double precision AS request_count,
        0.0::double precision AS error_count,
        percentile_cont(ARRAY[0.50, 0.95, 0.99])
            WITHIN GROUP (ORDER BY value * 1000.0) AS latency_percentiles,
        NULL::double precision AS saturation_mean,
        NULL::double precision AS saturation_max
    """,
    SignalKind.SATURATION: """
        0.0::double precision AS request_count,
        0.0::double precision AS error_count,
        NULL::double precision[] AS latency_percentiles,
        avg(value) AS saturation_mean,
        max(value) AS saturation_max
    """,
}
"""@brief 各视图所需的最小聚合投影 / Minimal aggregate projection required by each view."""

_TREND_METRIC_NAMES = {
    SignalKind.TRAFFIC: "'aiws.http.server.request.count'",
    SignalKind.ERRORS: (
        "'aiws.http.server.request.count', 'aiws.http.server.error.count'"
    ),
    SignalKind.LATENCY: "'http.server.request.duration'",
    SignalKind.SATURATION: (
        "'aiws.runtime.supervisor.utilization', "
        "'aiws.telemetry.queue.utilization'"
    ),
}
"""@brief 各视图允许扫描的固定指标名 / Fixed metric names each view may scan."""

_TREND_SQL = {
    signal: _TREND_SQL_TEMPLATE.format(
        signal_aggregates=aggregates,
        metric_names=_TREND_METRIC_NAMES[signal],
        service_predicate="{service_predicate}",
    )
    for signal, aggregates in _TREND_AGGREGATES.items()
}
"""@brief 由模块常量生成的四条固定趋势 SQL / Four fixed trend SQL statements generated from module constants."""

_EVENT_SQL = """
SELECT
    occurred_at,
    observed_at,
    source,
    service,
    kind,
    name,
    severity_number,
    severity_text,
    value,
    unit,
    duration_ms,
    span_status,
    request_id,
    trace_id,
    span_id,
    attributes
FROM observability.dashboard_signals
WHERE workspace_id = :workspace_id
  AND occurred_at >= :start_at
  AND occurred_at < :end_at
  AND (kind IN ('log', 'span') OR (source = 'frontend' AND kind = 'metric'))
  {service_predicate}
ORDER BY occurred_at DESC, observed_at DESC, service ASC, name ASC
LIMIT :limit
"""
"""@brief 有硬上限的诊断事件 SQL / Hard-bounded diagnostic-event SQL."""

_SYSTEM_HEALTH_SQL = """
SELECT
    occurred_at,
    observed_at,
    severity_number,
    severity_text,
    attributes
FROM observability.dashboard_signals
WHERE workspace_id IS NULL
  AND occurred_at >= :start_at
  AND occurred_at < :end_at
  AND kind = 'log'
  AND name = 'aiws.telemetry.health.snapshot'
ORDER BY observed_at DESC, occurred_at DESC
LIMIT 1
"""
"""@brief operator-only 最新全局遥测健康 SQL / Operator-only latest global telemetry-health SQL."""


class PostgresObservabilityReadStore:
    """@brief 面向用例的 PostgreSQL 聚合读存储 / Use-case-oriented PostgreSQL aggregate read store.

    @param engine Dashboard 自有异步 engine / Dashboard-owned async engine.
    @param statement_timeout_ms 每条查询的事务本地超时 / Transaction-local timeout for each query.
    @param owns_engine 是否在 aclose 时释放 engine / Whether ``aclose`` disposes the engine.

    @note 所有业务值均参数绑定；固定 relation 不来自用户配置。Overview 与 trend 均先在 SQL
    完整聚合，绝不执行 raw-row LIMIT 后的 Python 聚合。
    / All business values are bound parameters and the fixed relation is not configurable. Overview
    and trends are fully aggregated in SQL and never aggregate a raw-row ``LIMIT`` in Python.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        statement_timeout_ms: int,
        owns_engine: bool = False,
    ) -> None:
        """@brief 创建 PostgreSQL 读存储 / Create the PostgreSQL read store.

        @param engine 异步 SQLAlchemy engine / Async SQLAlchemy engine.
        @param statement_timeout_ms 正的查询超时 / Positive query timeout.
        @param owns_engine 是否拥有 engine / Whether the store owns the engine.
        @return 新读存储 / New read store.
        """

        if isinstance(statement_timeout_ms, bool) or statement_timeout_ms < 1:
            raise DashboardConfigurationError("statement_timeout_ms 必须是正整数。")
        self._engine = engine
        self._statement_timeout_ms = statement_timeout_ms
        self._owns_engine = owns_engine
        self._closed = False

    @classmethod
    def from_dsn(
        cls,
        dsn: str,
        *,
        pool_size: int,
        max_overflow: int,
        connect_timeout_ms: int,
        statement_timeout_ms: int,
    ) -> PostgresObservabilityReadStore:
        """@brief 从独立 dashboard DSN 创建读存储 / Build a read store from the dedicated Dashboard DSN.

        @param dsn dashboard role PostgreSQL DSN / Dashboard-role PostgreSQL DSN.
        @param pool_size 常驻连接数 / Persistent pool size.
        @param max_overflow 临时连接数 / Temporary overflow connections.
        @param connect_timeout_ms 建连与 pool checkout 超时 / Connect and pool-checkout timeout.
        @param statement_timeout_ms 查询超时 / Query timeout.
        @return 拥有其 engine 的读存储 / Read store owning its engine.
        """

        if pool_size < 1 or max_overflow < 0 or connect_timeout_ms < 1:
            raise DashboardConfigurationError("Dashboard PostgreSQL pool 参数无效。")
        engine = create_async_engine(
            _normalize_asyncpg_dsn(dsn),
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=connect_timeout_ms / 1_000,
            connect_args={"timeout": connect_timeout_ms / 1_000},
        )
        return cls(engine, statement_timeout_ms=statement_timeout_ms, owns_engine=True)

    async def fetch_overview(self, request: OverviewReadRequest) -> Sequence[ServiceSignalRow]:
        """@brief 在 PostgreSQL 聚合完整窗口 / Aggregate the complete window in PostgreSQL.

        @param request 有界 Overview 请求 / Bounded overview request.
        @return 每服务一行的完整聚合 / Complete aggregate with one row per service.
        """

        statement, parameters = _statement_and_parameters(
            _OVERVIEW_SQL,
            request.scope.workspace_id,
            request.window.start_at,
            request.window.end_at,
            request.service,
        )
        rows = await self._fetch(statement, parameters)
        return tuple(_overview_row(row) for row in rows)

    async def fetch_trends(self, request: TrendReadRequest) -> Sequence[TrendSignalRow]:
        """@brief 在 PostgreSQL 执行 `date_bin` 聚合 / Run ``date_bin`` aggregation in PostgreSQL.

        @param request 有界趋势请求 / Bounded trend request.
        @return 时间桶聚合行 / Time-bucket aggregate rows.
        """

        statement, parameters = _statement_and_parameters(
            _TREND_SQL[request.signal],
            request.scope.workspace_id,
            request.window.start_at,
            request.window.end_at,
            request.service,
        )
        parameters["bucket_seconds"] = request.bucket_seconds
        rows = await self._fetch(statement, parameters)
        return tuple(_trend_row(row) for row in rows)

    async def fetch_recent_events(
        self,
        request: EventReadRequest,
    ) -> Sequence[DiagnosticEventRow]:
        """@brief 读取最近诊断事件 / Read recent diagnostic events.

        @param request 有硬 limit 的事件请求 / Event request with a hard limit.
        @return 逆时间排序事件 / Reverse-chronological events.
        """

        statement, parameters = _statement_and_parameters(
            _EVENT_SQL,
            request.scope.workspace_id,
            request.window.start_at,
            request.window.end_at,
            request.service,
        )
        parameters["limit"] = request.limit
        rows = await self._fetch(statement, parameters)
        return tuple(_event_row(row) for row in rows)

    async def fetch_system_health(
        self,
        request: SystemHealthReadRequest,
    ) -> SystemHealthRow | None:
        """@brief 读取无 workspace 归因的最新管线快照 / Read the latest pipeline snapshot without workspace attribution.

        @param request 有界系统窗口 / Bounded system window.
        @return 最新强类型快照或 None / Latest typed snapshot or ``None``.
        """

        rows = await self._fetch(
            _SYSTEM_HEALTH_SQL,
            {
                "start_at": request.window.start_at,
                "end_at": request.window.end_at,
            },
        )
        return None if not rows else _system_health_row(rows[0])

    async def aclose(self) -> None:
        """@brief 幂等关闭自有连接池 / Idempotently close the owned connection pool.

        @return 无返回值 / No return value.
        """

        if self._closed:
            return
        self._closed = True
        if self._owns_engine:
            await self._engine.dispose()

    async def _fetch(
        self,
        statement: str,
        parameters: Mapping[str, object],
    ) -> tuple[Mapping[str, Any], ...]:
        """@brief 在只读短事务执行参数化查询 / Execute a parameterized query in a short read-only transaction.

        @param statement 仅由本模块定义的 SQL / SQL defined only by this module.
        @param parameters 绑定参数 / Bound parameters.
        @return 普通字典行 / Plain mapping rows.
        """

        if self._closed:
            raise DashboardReadStoreUnavailable("Dashboard PostgreSQL 读存储已关闭。")
        try:
            async with self._engine.connect() as connection:
                async with connection.begin():
                    await connection.execute(text("SET TRANSACTION READ ONLY"))
                    await connection.execute(
                        text("SELECT set_config('statement_timeout', :timeout, true)"),
                        {"timeout": f"{self._statement_timeout_ms}ms"},
                    )
                    result = await connection.execute(text(statement), dict(parameters))
                    return tuple(dict(row) for row in result.mappings().all())
        except SQLAlchemyError as error:
            raise DashboardReadStoreUnavailable(
                "Dashboard PostgreSQL 读模型当前不可用。"
            ) from error


def _statement_and_parameters(
    template: str,
    workspace_id: str,
    start_at: datetime,
    end_at: datetime,
    service: str | None,
) -> tuple[str, dict[str, object]]:
    """@brief 为固定模板添加可选服务谓词 / Add an optional service predicate to a fixed template.

    @param template 模块内固定 SQL 模板 / Module-owned fixed SQL template.
    @param workspace_id 工作区 ID / Workspace ID.
    @param start_at 窗口起点 / Window start.
    @param end_at 窗口终点 / Window end.
    @param service 可选服务过滤 / Optional service filter.
    @return SQL 与参数字典 / SQL and parameter mapping.

    @note 插入的仅是模块内常量谓词，用户值始终绑定 / Only a module constant predicate is inserted; user values are always bound.
    """

    service_predicate = ""
    parameters: dict[str, object] = {
        "workspace_id": workspace_id,
        "start_at": start_at,
        "end_at": end_at,
    }
    if service is not None:
        service_predicate = "AND service = :service"
        parameters["service"] = service
    return template.format(service_predicate=service_predicate), parameters


def _overview_row(row: Mapping[str, Any]) -> ServiceSignalRow:
    """@brief 转换 Overview 数据库行 / Convert an overview database row.

    @param row 数据库映射行 / Database mapping row.
    @return 强类型服务信号行 / Strongly typed service-signal row.
    """

    percentiles = _percentiles(row.get("latency_percentiles"))
    return ServiceSignalRow(
        service=_required_string(row, "service"),
        request_count=_number_or_zero(row.get("request_count")),
        error_count=_number_or_zero(row.get("error_count")),
        latency_p50_ms=percentiles[0],
        latency_p95_ms=percentiles[1],
        latency_p99_ms=percentiles[2],
        saturation_mean=_optional_number(row.get("saturation_mean")),
        saturation_max=_optional_number(row.get("saturation_max")),
        sample_count=int(row.get("sample_count", 0)),
        max_collection_lag_seconds=_number_or_zero(
            row.get("max_collection_lag_seconds")
        ),
        latest_observed_at=_aware_datetime(row.get("latest_observed_at")),
    )


def _trend_row(row: Mapping[str, Any]) -> TrendSignalRow:
    """@brief 转换趋势数据库行 / Convert a trend database row.

    @param row 数据库映射行 / Database mapping row.
    @return 强类型趋势行 / Strongly typed trend row.
    """

    percentiles = _percentiles(row.get("latency_percentiles"))
    return TrendSignalRow(
        bucket_start=_aware_datetime(row.get("bucket_start")),
        service=_required_string(row, "service"),
        request_count=_number_or_zero(row.get("request_count")),
        error_count=_number_or_zero(row.get("error_count")),
        latency_p50_ms=percentiles[0],
        latency_p95_ms=percentiles[1],
        latency_p99_ms=percentiles[2],
        saturation_mean=_optional_number(row.get("saturation_mean")),
        saturation_max=_optional_number(row.get("saturation_max")),
    )


def _event_row(row: Mapping[str, Any]) -> DiagnosticEventRow:
    """@brief 转换诊断事件数据库行 / Convert a diagnostic-event database row.

    @param row 数据库映射行 / Database mapping row.
    @return 强类型诊断事件 / Strongly typed diagnostic event.
    """

    return DiagnosticEventRow(
        occurred_at=_aware_datetime(row.get("occurred_at")),
        observed_at=_aware_datetime(row.get("observed_at")),
        source=_required_string(row, "source"),
        service=_required_string(row, "service"),
        kind=_required_string(row, "kind"),
        name=_required_string(row, "name"),
        severity_number=_optional_int(row.get("severity_number")),
        severity_text=_optional_string(row.get("severity_text")),
        value=_optional_number(row.get("value")),
        unit=_optional_string(row.get("unit")),
        duration_ms=_optional_number(row.get("duration_ms")),
        span_status=_optional_string(row.get("span_status")),
        request_id=_optional_string(row.get("request_id")),
        trace_id=_optional_string(row.get("trace_id")),
        span_id=_optional_string(row.get("span_id")),
        attributes=_attributes(row.get("attributes")),
    )


def _system_health_row(row: Mapping[str, Any]) -> SystemHealthRow:
    """@brief 校验全局 self-health 数据库行 / Validate a global self-health database row.

    @param row 数据库映射 / Database mapping.
    @return 强类型系统健康行 / Typed system-health row.
    """

    attributes = _attributes(row.get("attributes"))
    return SystemHealthRow(
        occurred_at=_aware_datetime(row.get("occurred_at")),
        observed_at=_aware_datetime(row.get("observed_at")),
        severity_number=_required_non_negative_int(row.get("severity_number"), "severity_number"),
        severity_text=_required_string(row, "severity_text"),
        accepted_count=_required_non_negative_int(attributes.get("accepted_count"), "accepted_count"),
        dropped_count=_required_non_negative_int(attributes.get("dropped_count"), "dropped_count"),
        write_failure_count=_required_non_negative_int(
            attributes.get("write_failure_count"), "write_failure_count"
        ),
        output_dropped_count=_required_non_negative_int(
            attributes.get("output_dropped_count"), "output_dropped_count"
        ),
    )


def _percentiles(value: object) -> tuple[float | None, float | None, float | None]:
    """@brief 解析固定三分位数组 / Parse the fixed three-percentile array.

    @param value PostgreSQL array 或 None / PostgreSQL array or ``None``.
    @return p50、p95、p99 / p50, p95, and p99.
    """

    if value is None:
        return (None, None, None)
    if not isinstance(value, (list, tuple)) or len(value) != 3:
        raise DashboardReadStoreUnavailable("Dashboard latency percentile 数据形状无效。")
    return tuple(_optional_number(item) for item in value)  # type: ignore[return-value]


def _aware_datetime(value: object) -> datetime:
    """@brief 解析带时区数据库时间 / Parse a timezone-aware database timestamp.

    @param value datetime 或 RFC 3339 文本 / ``datetime`` or RFC 3339 text.
    @return UTC datetime / UTC datetime.
    """

    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise DashboardReadStoreUnavailable("Dashboard 时间格式无效。") from error
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise DashboardReadStoreUnavailable("Dashboard 时间必须携带时区。")
    return value.astimezone(UTC)


def _required_string(row: Mapping[str, Any], key: str) -> str:
    """@brief 读取非空数据库文本 / Read non-empty database text.

    @param row 数据库行 / Database row.
    @param key 列名 / Column name.
    @return 非空文本 / Non-empty text.
    """

    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise DashboardReadStoreUnavailable(f"Dashboard 列 {key} 无效。")
    return value


def _optional_string(value: object) -> str | None:
    """@brief 读取可空数据库文本 / Read nullable database text.

    @param value 数据库值 / Database value.
    @return 文本或 None / Text or ``None``.
    """

    return value if isinstance(value, str) and value else None


def _optional_number(value: object) -> float | None:
    """@brief 读取可空数据库数值 / Read a nullable database number.

    @param value 数据库值 / Database value.
    @return float 或 None / ``float`` or ``None``.
    """

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise DashboardReadStoreUnavailable("Dashboard 数值列无效。")
    return float(value)


def _optional_int(value: object) -> int | None:
    """@brief 读取可空数据库整数 / Read a nullable database integer.

    @param value 数据库值 / Database value.
    @return int 或 None / ``int`` or ``None``.
    """

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise DashboardReadStoreUnavailable("Dashboard 整数列无效。")
    return value


def _required_non_negative_int(value: object, label: str) -> int:
    """@brief 读取 JSON/数据库非负整数 / Read a non-negative integer from JSON or a database row.

    @param value 候选值 / Candidate value.
    @param label 安全字段名 / Safe field label.
    @return 非负整数 / Non-negative integer.
    """

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DashboardReadStoreUnavailable(f"Dashboard {label} 不是非负整数。")
    return value


def _number_or_zero(value: object) -> float:
    """@brief 将可空聚合数值归零 / Normalize a nullable aggregate number to zero.

    @param value 数据库值 / Database value.
    @return 浮点数 / Floating-point number.
    """

    return _optional_number(value) or 0.0


def _attributes(value: object) -> Mapping[str, object]:
    """@brief 解析 JSONB 低基数属性 / Parse JSONB low-cardinality attributes.

    @param value Mapping、JSON 文本或 None / Mapping, JSON text, or ``None``.
    @return 普通字符串键字典 / Plain string-keyed dictionary.
    """

    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise DashboardReadStoreUnavailable("Dashboard attributes JSON 无效。") from error
    if not isinstance(value, Mapping):
        raise DashboardReadStoreUnavailable("Dashboard attributes 必须是对象。")
    return {str(key): item for key, item in value.items()}


def _normalize_asyncpg_dsn(dsn: str) -> str:
    """@brief 规范化为 SQLAlchemy asyncpg DSN / Normalize to a SQLAlchemy asyncpg DSN.

    @param dsn dashboard role PostgreSQL DSN / Dashboard-role PostgreSQL DSN.
    @return asyncpg URL / Asyncpg URL.

    @note 返回值可能含凭证，禁止记录 / The result may contain credentials and must not be logged.
    """

    if not isinstance(dsn, str) or not dsn.strip():
        raise DashboardConfigurationError("Dashboard PostgreSQL DSN 不能为空。")
    try:
        url: URL = make_url(dsn)
    except (SQLAlchemyError, ValueError) as error:
        raise DashboardConfigurationError("Dashboard PostgreSQL DSN 格式无效。") from error
    if url.get_backend_name() != "postgresql":
        raise DashboardConfigurationError("Dashboard 只支持 PostgreSQL。")
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+asyncpg")
    if url.drivername != "postgresql+asyncpg":
        raise DashboardConfigurationError("Dashboard PostgreSQL 必须使用 asyncpg。")
    return url.render_as_string(hide_password=False)


__all__ = ["PostgresObservabilityReadStore"]
