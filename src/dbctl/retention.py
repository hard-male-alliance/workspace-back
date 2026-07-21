"""@brief dbctl 遥测保留期清理 / dbctl telemetry-retention pruning.

该模块只由 ``dbctl prune-telemetry`` 的显式运维命令调用。它绝不被
backend、Dashboard 或任何 HTTP/WebSocket 请求路径导入；因此保留期删除不会把大范围
I/O、锁竞争或数据库管理权限带入面向用户的工作负载。
/ This module is invoked only by the explicit ``dbctl prune-telemetry`` operator
command. It is never imported by backend, Dashboard, or any HTTP/WebSocket request path, so
retention deletion cannot bring wide I/O, lock contention, or database-maintenance privileges
into user-facing workloads.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Protocol

from .errors import (
    DbctlConfigurationError,
    DbctlDependencyError,
    TelemetryRetentionExecutionError,
)
from .identifiers import quote_postgres_identifier

DEFAULT_TELEMETRY_PRUNE_BATCH_SIZE: Final[int] = 1_000
"""@brief 默认单批删除上限 / Default maximum rows deleted in one batch."""

MAX_TELEMETRY_PRUNE_BATCH_SIZE: Final[int] = 10_000
"""@brief 单批删除的硬上限 / Hard upper bound for one deletion batch."""

DEFAULT_TELEMETRY_PRUNE_MAX_BATCHES: Final[int] = 10
"""@brief 单次命令默认最多执行的批次数 / Default maximum batches per invocation."""

MAX_TELEMETRY_PRUNE_MAX_BATCHES: Final[int] = 100
"""@brief 单次命令最多执行的批次数 / Hard maximum batches per invocation."""

DEFAULT_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS: Final[int] = 5_000
"""@brief 每个数据库语句的默认超时（毫秒）/ Default per-statement timeout in milliseconds."""

MAX_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS: Final[int] = 60_000
"""@brief 运维清理允许设置的最大语句超时（毫秒）/ Maximum configurable statement timeout."""

DEFAULT_TELEMETRY_PRUNE_LOCK_TIMEOUT_MS: Final[int] = 500
"""@brief 清理事务默认锁等待上限（毫秒）/ Default prune-transaction lock timeout."""

MAX_TELEMETRY_PRUNE_LOCK_TIMEOUT_MS: Final[int] = 5_000
"""@brief 清理事务锁等待硬上限（毫秒）/ Hard prune-transaction lock-timeout ceiling."""

_TELEMETRY_TABLE_NAME: Final[str] = "telemetry_records"
"""@brief 固定的遥测表名 / Fixed telemetry relation name."""


def _utc_now() -> datetime:
    """@brief 获取当前 UTC 时间 / Get current UTC time.

    @return 带 UTC 时区的当前时间 / Current time with UTC timezone.
    """
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class TelemetryPruneRequest:
    """@brief 一次受限遥测清理请求 / One bounded telemetry-pruning request.

    @param retention_days 从根 ``observability.retention_days`` 读取的保留天数。
    / Retention days loaded from root ``observability.retention_days``.
    @param batch_size 一批最多删除的记录数 / Maximum records deleted in one batch.
    @param max_batches 本次命令最多执行的批次数 / Maximum batches for this invocation.
    @param statement_timeout_ms 每个 SQL 语句的事务本地超时 / Transaction-local timeout for each SQL statement.
    @param lock_timeout_ms 每个删除事务的锁等待上限 / Lock-wait timeout for each delete transaction.
    @param apply ``True`` 才允许连接数据库并删除；``False`` 是 dry-run。
    / Only ``True`` permits database connection and deletion; ``False`` is dry-run.

    @note ``retention_days == 0`` 是显式的停用开关：即便传入 ``apply=True`` 也不连接
    数据库。这避免把“暂不保留 telemetry”误解释为“立即删除全部 telemetry”。
    / ``retention_days == 0`` is an explicit disable switch: even ``apply=True`` never connects to
    the database. This prevents interpreting “do not retain telemetry” as “delete all telemetry now”.
    """

    retention_days: int
    batch_size: int = DEFAULT_TELEMETRY_PRUNE_BATCH_SIZE
    max_batches: int = DEFAULT_TELEMETRY_PRUNE_MAX_BATCHES
    statement_timeout_ms: int = DEFAULT_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS
    lock_timeout_ms: int = DEFAULT_TELEMETRY_PRUNE_LOCK_TIMEOUT_MS
    apply: bool = False

    def __post_init__(self) -> None:
        """@brief 校验有界运维参数 / Validate bounded operator parameters.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 参数类型或安全边界无效时抛出。
        / Raised when parameter types or safety bounds are invalid.
        """
        _require_non_negative_integer(self.retention_days, "observability.retention_days")
        _require_bounded_integer(
            self.batch_size,
            "batch_size",
            lower=1,
            upper=MAX_TELEMETRY_PRUNE_BATCH_SIZE,
        )
        _require_bounded_integer(
            self.max_batches,
            "max_batches",
            lower=1,
            upper=MAX_TELEMETRY_PRUNE_MAX_BATCHES,
        )
        _require_bounded_integer(
            self.statement_timeout_ms,
            "statement_timeout_ms",
            lower=1,
            upper=MAX_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS,
        )
        _require_bounded_integer(
            self.lock_timeout_ms,
            "lock_timeout_ms",
            lower=1,
            upper=MAX_TELEMETRY_PRUNE_LOCK_TIMEOUT_MS,
        )
        if self.lock_timeout_ms > self.statement_timeout_ms:
            raise DbctlConfigurationError("lock_timeout_ms 不能大于 statement_timeout_ms。")
        if not isinstance(self.apply, bool):
            raise DbctlConfigurationError("apply 必须是布尔值。")

    @property
    def disabled(self) -> bool:
        """@brief 判断保留清理是否被配置停用 / Determine whether retention pruning is disabled.

        @return ``retention_days`` 为零时返回 ``True`` / ``True`` when retention days is zero.
        """
        return self.retention_days == 0


@dataclass(frozen=True, slots=True)
class TelemetryPruneResult:
    """@brief 不含 SQL 或凭证的遥测清理摘要 / Secret- and SQL-free telemetry-pruning summary.

    @param retention_days 本次使用的保留天数 / Retention days used by this invocation.
    @param cutoff 仅删除早于该 UTC 时刻的记录；停用时为 ``None``。
    / Only rows older than this UTC moment are deleted; ``None`` when disabled.
    @param applied 是否实际连接并执行删除 / Whether deletion was actually connected and executed.
    @param disabled 是否因 ``retention_days == 0`` 被停用 / Whether disabled by ``retention_days == 0``.
    @param batch_count 已提交的删除批次数 / Number of committed deletion batches.
    @param batch_size 单个删除事务的行数上限 / Row cap for one deletion transaction.
    @param max_batches 调用方允许的最大批次数 / Maximum batches allowed by the caller.
    @param deleted_count 本次已删除记录总数 / Total records deleted in this invocation.
    @param has_more 截止时是否仍有过期记录；dry-run 或停用时为 ``None``。
    / Whether any stale row remains at the cutoff; ``None`` for dry-run or disabled mode.
    """

    retention_days: int
    cutoff: datetime | None
    applied: bool
    disabled: bool
    batch_count: int
    batch_size: int
    max_batches: int
    deleted_count: int
    has_more: bool | None

    @property
    def reached_batch_limit(self) -> bool:
        """@brief 判断实际运行是否耗尽了批次预算 / Determine whether execution exhausted its batch budget.

        @return 已实际执行、未停用且批次数达到上限时为 ``True``。
        / ``True`` when execution ran, was not disabled, and used every allowed batch.
        """
        return self.applied and not self.disabled and self.batch_count >= self.max_batches

    def render_operator_summary(self) -> str:
        """@brief 渲染可安全显示的运维摘要 / Render a safely displayable operator summary.

        @return 不包含 DSN、密码、原始 SQL 或数据库异常的中文摘要。
        / Chinese summary containing no DSN, password, raw SQL, or database exception.
        """
        if self.disabled:
            return (
                "dbctl prune-telemetry：observability.retention_days=0，清理已停用；未连接数据库。"
            )
        if not self.applied:
            if self.cutoff is None:
                raise RuntimeError("未停用的 dry-run 必须包含 UTC cutoff。")
            return (
                "dbctl prune-telemetry dry-run：不会连接数据库或执行删除；"
                f"保留 {self.retention_days} 天，删除早于 {self.cutoff.isoformat()} 的记录；"
                f"最多 {self.max_batches} 批、"
                f"每批上限 {self.batch_size} 条，语句超时由 --statement-timeout-ms 控制。"
            )
        has_more = self.has_more
        if has_more is None or self.cutoff is None:
            raise RuntimeError("已执行的遥测清理结果必须包含 has_more 与 UTC cutoff。")
        limit_note = "；已达到本次批次上限" if self.reached_batch_limit else ""
        remaining_note = "；仍有过期记录待下轮处理" if has_more else "；过期记录已清空"
        return (
            "dbctl prune-telemetry 完成："
            f"删除 {self.deleted_count} 条{remaining_note}；"
            f"cutoff 为 {self.cutoff.isoformat()}；"
            f"已提交 {self.batch_count}/{self.max_batches} 个短事务{limit_note}。"
        )


class TelemetryRetentionRunner(Protocol):
    """@brief 受控遥测删除 I/O 端口 / Controlled telemetry-deletion I/O port.

    实现必须让每个 ``delete_batch`` 与 ``has_stale`` 调用使用独立短事务，并在
    同一事务内以 migrator 连接显式 ``SET LOCAL ROLE`` 为配置化 owner。协议使纯单元
    测试可注入 fake runner，而无需 PostgreSQL、subprocess 或真实凭证。
    / Implementations must use a separate short transaction for each ``delete_batch`` and
    ``has_stale`` call, explicitly ``SET LOCAL ROLE`` to the configured owner over a migrator
    connection in that transaction. The protocol lets unit tests inject a fake runner without
    PostgreSQL, subprocesses, or real credentials.
    """

    def delete_batch(
        self,
        *,
        cutoff: datetime,
        batch_size: int,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> int:
        """@brief 删除一个有界的过期遥测批次 / Delete one bounded batch of stale telemetry.

        @param cutoff 仅删除早于该 UTC 时刻的记录 / Delete only rows older than this UTC moment.
        @param batch_size 单批最大记录数 / Maximum records in one batch.
        @param statement_timeout_ms 语句超时 / Statement timeout.
        @param lock_timeout_ms 锁等待超时 / Lock-wait timeout.
        @return 本事务已删除的记录数 / Records deleted in this transaction.
        """

    def has_stale(
        self,
        *,
        cutoff: datetime,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> bool:
        """@brief 判断是否仍有过期遥测 / Determine whether stale telemetry remains.

        @param cutoff 过期边界 UTC 时刻 / Staleness boundary UTC moment.
        @param statement_timeout_ms 语句超时 / Statement timeout.
        @param lock_timeout_ms 锁等待超时 / Lock-wait timeout.
        @return 存在至少一条过期记录时为真 / True when at least one stale row remains.
        """


class TelemetryPruneExecutor:
    """@brief 编排 dry-run 与有界批量清理 / Orchestrate dry-run and bounded batch pruning.

    @param runner 可选的 I/O runner；dry-run/停用路径不需要它。
    / Optional I/O runner; dry-run and disabled paths do not need one.
    @param clock 返回当前带时区 UTC 时间的函数 / Function returning current timezone-aware UTC time.
    """

    def __init__(
        self,
        runner: TelemetryRetentionRunner | None,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        """@brief 初始化清理编排器 / Initialize the pruning orchestrator.

        @param runner 生产 Psycopg runner 或测试 fake runner / Production Psycopg runner or test fake.
        @param clock 可注入的 UTC 时钟 / Injectable UTC clock.
        @return 无返回值 / No return value.
        """
        self._runner = runner
        self._clock = clock

    def execute(self, request: TelemetryPruneRequest) -> TelemetryPruneResult:
        """@brief 执行或预览一轮遥测清理 / Execute or preview one telemetry-pruning run.

        @param request 已验证的有界清理请求 / Validated bounded pruning request.
        @return 无敏感信息的执行结果 / Execution result without sensitive information.
        @raise DbctlConfigurationError 实际执行却没有 I/O runner 或时钟不合规时抛出。
        / Raised when execution lacks an I/O runner or the clock is invalid.
        @raise TelemetryRetentionExecutionError runner 返回违反有界契约的结果时抛出。
        / Raised when the runner returns a result violating boundedness.
        """
        if request.disabled:
            return TelemetryPruneResult(
                retention_days=request.retention_days,
                cutoff=None,
                applied=False,
                disabled=True,
                batch_count=0,
                batch_size=request.batch_size,
                max_batches=request.max_batches,
                deleted_count=0,
                has_more=None,
            )

        cutoff = _utc_cutoff(self._clock(), request.retention_days)
        if not request.apply:
            return TelemetryPruneResult(
                retention_days=request.retention_days,
                cutoff=cutoff,
                applied=False,
                disabled=False,
                batch_count=0,
                batch_size=request.batch_size,
                max_batches=request.max_batches,
                deleted_count=0,
                has_more=None,
            )
        if self._runner is None:
            raise DbctlConfigurationError("遥测清理执行缺少受控数据库 runner。")

        deleted_count = 0
        batch_count = 0
        for _ in range(request.max_batches):
            deleted_in_batch = self._runner.delete_batch(
                cutoff=cutoff,
                batch_size=request.batch_size,
                statement_timeout_ms=request.statement_timeout_ms,
                lock_timeout_ms=request.lock_timeout_ms,
            )
            if not isinstance(deleted_in_batch, int) or isinstance(deleted_in_batch, bool):
                raise TelemetryRetentionExecutionError("遥测清理批次返回了无效删除计数。")
            if deleted_in_batch < 0 or deleted_in_batch > request.batch_size:
                raise TelemetryRetentionExecutionError("遥测清理批次违反了受限删除上限。")
            batch_count += 1
            deleted_count += deleted_in_batch
            if deleted_in_batch < request.batch_size:
                break

        has_more = self._runner.has_stale(
            cutoff=cutoff,
            statement_timeout_ms=request.statement_timeout_ms,
            lock_timeout_ms=request.lock_timeout_ms,
        )
        if not isinstance(has_more, bool):
            raise TelemetryRetentionExecutionError("遥测清理剩余状态无效。")
        return TelemetryPruneResult(
            retention_days=request.retention_days,
            cutoff=cutoff,
            applied=True,
            disabled=False,
            batch_count=batch_count,
            batch_size=request.batch_size,
            max_batches=request.max_batches,
            deleted_count=deleted_count,
            has_more=has_more,
        )


class PsycopgTelemetryRetentionRunner:
    """@brief 使用 migrator DSN 的 PostgreSQL 遥测清理 runner / PostgreSQL telemetry pruner using a migrator DSN.

    每一个公开操作都会建立一个新连接，并在独立的短事务内先执行 ``SET LOCAL ROLE``，
    再设置事务本地 ``statement_timeout``。migrator 必须是配置 owner 的 ``NOINHERIT``
    成员；owner 对当前遥测表的 ``FOR ALL`` RLS policy 由 migration ``20260721_0006`` 建立。
    / Each public operation opens a new connection and, in a separate short transaction, first
    executes ``SET LOCAL ROLE`` and then sets transaction-local ``statement_timeout``. The migrator
    must be a ``NOINHERIT`` member of the configured owner; migration ``20260721_0006`` establishes
    the owner's ``FOR ALL`` RLS policy for the current telemetry table.
    """

    def __init__(
        self,
        migrator_dsn: str,
        *,
        owner_role: str,
        observability_schema: str = "observability",
    ) -> None:
        """@brief 初始化受控 PostgreSQL 清理 runner / Initialize controlled PostgreSQL pruning runner.

        @param migrator_dsn 仅内存保存的迁移 DSN / Migrator DSN retained only in memory.
        @param owner_role 经 dbctl 配置校验的对象 owner role / dbctl-configured object-owner role.
        @param observability_schema 经 dbctl 配置校验的 observability schema。
        / dbctl-configured observability schema.
        @return 无返回值 / No return value.
        @raise DbctlConfigurationError DSN、role 或 schema 为空/无效时抛出。
        / Raised when DSN, role, or schema is empty or invalid.
        """
        if not isinstance(migrator_dsn, str) or not migrator_dsn:
            raise DbctlConfigurationError("迁移 PostgreSQL DSN 必须是非空字符串。")
        self._migrator_dsn = migrator_dsn
        owner_identifier = quote_postgres_identifier(owner_role, kind="owner role")
        schema_identifier = quote_postgres_identifier(
            observability_schema,
            kind="observability schema",
        )
        table_identifier = quote_postgres_identifier(_TELEMETRY_TABLE_NAME, kind="遥测表名")
        relation = f"{schema_identifier}.{table_identifier}"
        self._set_local_role_sql = f"SET LOCAL ROLE {owner_identifier};"
        self._delete_batch_sql = (
            "WITH stale_rows AS ("
            f" SELECT ctid FROM {relation}"
            " WHERE observed_at < %s"
            " ORDER BY observed_at ASC"
            " LIMIT %s"
            " FOR UPDATE SKIP LOCKED"
            ")"
            f" DELETE FROM {relation} AS telemetry"
            " USING stale_rows"
            " WHERE telemetry.ctid = stale_rows.ctid;"
        )
        self._has_stale_sql = (
            f"SELECT EXISTS (SELECT 1 FROM {relation} WHERE observed_at < %s LIMIT 1);"
        )

    def delete_batch(
        self,
        *,
        cutoff: datetime,
        batch_size: int,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> int:
        """@brief 用一个短事务删除一批过期记录 / Delete a stale-record batch in one short transaction.

        @param cutoff 过期边界 UTC 时刻 / Staleness boundary UTC moment.
        @param batch_size 单批上限 / Per-batch row cap.
        @param statement_timeout_ms 事务本地语句超时 / Transaction-local statement timeout.
        @param lock_timeout_ms 事务本地锁等待上限 / Transaction-local lock-wait timeout.
        @return 已提交删除的记录数 / Number of records deleted and committed.
        @raise TelemetryRetentionExecutionError PostgreSQL 操作失败时抛出，且不泄露原始错误。
        / Raised when PostgreSQL operation fails, without exposing the original error.
        """
        _validate_runtime_arguments(cutoff, batch_size, statement_timeout_ms, lock_timeout_ms)
        connection: Any | None = None
        try:
            connection = self._connect()
            with connection.transaction():
                self._prepare_short_transaction(
                    connection,
                    statement_timeout_ms,
                    lock_timeout_ms,
                )
                cursor = connection.execute(self._delete_batch_sql, (cutoff, batch_size))
                deleted = getattr(cursor, "rowcount", None)
                if not isinstance(deleted, int) or isinstance(deleted, bool) or deleted < 0:
                    raise TelemetryRetentionExecutionError("遥测保留期删除计数不可用。")
                return deleted
        except DbctlConfigurationError, DbctlDependencyError:
            raise
        except Exception as error:
            raise TelemetryRetentionExecutionError(
                "遥测保留期删除失败；请确认已迁移到包含 owner maintenance policy 的版本。"
            ) from error
        finally:
            _close_connection_quietly(connection)

    def has_stale(
        self,
        *,
        cutoff: datetime,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> bool:
        """@brief 在一个短事务中判断是否仍有过期记录 / Check for stale rows in one short transaction.

        @param cutoff 过期边界 UTC 时刻 / Staleness boundary UTC moment.
        @param statement_timeout_ms 事务本地语句超时 / Transaction-local statement timeout.
        @param lock_timeout_ms 事务本地锁等待上限 / Transaction-local lock-wait timeout.
        @return 仍有过期记录时为真 / True when stale records remain.
        @raise TelemetryRetentionExecutionError PostgreSQL 操作失败时抛出，且不泄露原始错误。
        / Raised when PostgreSQL operation fails, without exposing the original error.
        """
        _validate_runtime_arguments(cutoff, 1, statement_timeout_ms, lock_timeout_ms)
        connection: Any | None = None
        try:
            connection = self._connect()
            with connection.transaction():
                self._prepare_short_transaction(
                    connection,
                    statement_timeout_ms,
                    lock_timeout_ms,
                )
                row = connection.execute(self._has_stale_sql, (cutoff,)).fetchone()
                if row is None or not isinstance(row[0], bool):
                    raise TelemetryRetentionExecutionError("遥测保留期剩余状态不可用。")
                return row[0]
        except DbctlConfigurationError, DbctlDependencyError, TelemetryRetentionExecutionError:
            raise
        except Exception as error:
            raise TelemetryRetentionExecutionError(
                "遥测保留期计数失败；请确认已迁移到包含 owner maintenance policy 的版本。"
            ) from error
        finally:
            _close_connection_quietly(connection)

    def _connect(self) -> Any:
        """@brief 建立非 autocommit migrator 连接 / Open a non-autocommit migrator connection.

        @return psycopg connection object / psycopg connection object.
        @raise DbctlDependencyError psycopg 不可用时抛出。
        / Raised when psycopg is unavailable.
        @raise TelemetryRetentionExecutionError 无法建立连接时抛出，不泄露 DSN。
        / Raised when a connection cannot be established, without exposing the DSN.
        """
        try:
            import psycopg
        except ImportError as error:
            raise DbctlDependencyError("执行遥测保留期清理需要 psycopg 依赖。") from error
        try:
            return psycopg.connect(self._migrator_dsn, autocommit=False)
        except Exception as error:
            raise TelemetryRetentionExecutionError(
                "无法连接遥测清理数据库；连接详情已隐藏。"
            ) from error

    def _prepare_short_transaction(
        self,
        connection: Any,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> None:
        """@brief 在当前事务中切换 owner 并设置局部超时 / Set owner role and local timeout in current transaction.

        @param connection 当前 psycopg 连接 / Current psycopg connection.
        @param statement_timeout_ms 已校验的超时毫秒数 / Validated timeout in milliseconds.
        @param lock_timeout_ms 已校验的锁等待超时毫秒数 / Validated lock-wait timeout.
        @return 无返回值 / No return value.

        @note role 名是经 identifier 校验后引用的 PostgreSQL 标识符；所有值参数（包括
        超时）均通过 DB-API 参数绑定传递。
        / The role is a quoted, identifier-validated PostgreSQL identifier; every value parameter
        (including timeout) is passed via DB-API parameter binding.
        """
        connection.execute(self._set_local_role_sql)
        connection.execute(
            "SELECT set_config('statement_timeout', %s, true);",
            (f"{statement_timeout_ms}ms",),
        )
        connection.execute(
            "SELECT set_config('lock_timeout', %s, true);",
            (f"{lock_timeout_ms}ms",),
        )


def _utc_cutoff(now: datetime, retention_days: int) -> datetime:
    """@brief 从 UTC 时钟与保留天数计算过期边界 / Compute stale cutoff from UTC clock and retention days.

    @param now 当前带时区的时间 / Current timezone-aware time.
    @param retention_days 需保留的天数 / Number of days to retain.
    @return UTC 过期边界 / UTC stale cutoff.
    @raise DbctlConfigurationError 时钟不是带时区 datetime 或天数无效时抛出。
    / Raised when the clock is not timezone-aware datetime or days are invalid.
    """
    _require_non_negative_integer(retention_days, "observability.retention_days")
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise DbctlConfigurationError("遥测清理时钟必须返回带时区的 datetime。")
    return now.astimezone(UTC) - timedelta(days=retention_days)


def _validate_runtime_arguments(
    cutoff: datetime,
    batch_size: int,
    statement_timeout_ms: int,
    lock_timeout_ms: int,
) -> None:
    """@brief 校验 runner 的运行时输入 / Validate runner runtime inputs.

    @param cutoff 过期边界 / Staleness boundary.
    @param batch_size 删除批次大小 / Deletion batch size.
    @param statement_timeout_ms 语句超时毫秒数 / Statement timeout milliseconds.
    @param lock_timeout_ms 锁等待超时毫秒数 / Lock-wait timeout milliseconds.
    @return 无返回值 / No return value.
    @raise DbctlConfigurationError 输入未通过受限运行边界时抛出。
    / Raised when input fails bounded runtime constraints.
    """
    if not isinstance(cutoff, datetime) or cutoff.tzinfo is None:
        raise DbctlConfigurationError("遥测清理 cutoff 必须是带时区的 datetime。")
    _require_bounded_integer(
        batch_size,
        "batch_size",
        lower=1,
        upper=MAX_TELEMETRY_PRUNE_BATCH_SIZE,
    )
    _require_bounded_integer(
        statement_timeout_ms,
        "statement_timeout_ms",
        lower=1,
        upper=MAX_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS,
    )
    _require_bounded_integer(
        lock_timeout_ms,
        "lock_timeout_ms",
        lower=1,
        upper=MAX_TELEMETRY_PRUNE_LOCK_TIMEOUT_MS,
    )
    if lock_timeout_ms > statement_timeout_ms:
        raise DbctlConfigurationError("lock_timeout_ms 不能大于 statement_timeout_ms。")


def _require_non_negative_integer(value: object, name: str) -> None:
    """@brief 验证非负整数 / Validate a non-negative integer.

    @param value 候选值 / Candidate value.
    @param name 面向运维者的安全字段名 / Safe operator-facing field name.
    @return 无返回值 / No return value.
    @raise DbctlConfigurationError 候选值不是非负整数时抛出。
    / Raised when the candidate is not a non-negative integer.
    """
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise DbctlConfigurationError(f"{name} 必须是非负整数。")


def _require_bounded_integer(value: object, name: str, *, lower: int, upper: int) -> None:
    """@brief 验证闭区间内整数 / Validate an integer inside an inclusive range.

    @param value 候选值 / Candidate value.
    @param name 面向运维者的安全字段名 / Safe operator-facing field name.
    @param lower 允许的最小值 / Inclusive minimum.
    @param upper 允许的最大值 / Inclusive maximum.
    @return 无返回值 / No return value.
    @raise DbctlConfigurationError 候选值不是区间内整数时抛出。
    / Raised when the candidate is not an integer in the range.
    """
    if not isinstance(value, int) or isinstance(value, bool) or value < lower or value > upper:
        raise DbctlConfigurationError(f"{name} 必须介于 {lower} 与 {upper} 之间。")


def _close_connection_quietly(connection: Any | None) -> None:
    """@brief 无泄露地关闭连接 / Close a connection without surfacing connection internals.

    @param connection 可选 psycopg 连接 / Optional psycopg connection.
    @return 无返回值 / No return value.

    @note 清理阶段的 close 失败不覆盖主操作异常，也不打印可能包含连接信息的底层错误。
    / A close failure never replaces the primary operation error and never prints a lower-level
    error that could contain connection information.
    """
    if connection is None:
        return
    try:
        connection.close()
    except Exception:
        return
