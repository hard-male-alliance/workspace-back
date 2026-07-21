"""@brief Psycopg 遥测保留适配器 / Psycopg telemetry-retention adapter."""

from __future__ import annotations

from typing import Any, Final

from psycopg import Connection, connect

from dbctl.application.errors import RetentionExecutionError
from dbctl.application.prune_telemetry import DeleteTelemetryBatch, StaleTelemetryProbe
from dbctl.domain.database import MigratorLogin
from dbctl.domain.names import RoleName, SchemaName

_TELEMETRY_RELATION: Final[str] = "telemetry_records"
"""@brief 固定遥测 relation 名 / Fixed telemetry relation name."""

_CONNECT_TIMEOUT_SECONDS: Final[int] = 5
"""@brief 遥测维护连接超时 / Telemetry-maintenance connection timeout."""


class PsycopgTelemetryRetentionAdapter:
    """@brief 每次操作使用独立短事务的遥测删除端口 / Telemetry port using one short transaction per operation."""

    def __init__(
        self,
        login: MigratorLogin,
        *,
        owner_role: RoleName,
        observability_schema: SchemaName,
    ) -> None:
        """@brief 绑定 migrator、owner 与固定 relation / Bind migrator, owner, and the fixed relation.

        @param login 与目标数据库绑定的 migrator 登录 / Migrator login bound to the target database.
        @param owner_role 每个事务显式切换到的对象 owner / Object owner assumed in each transaction.
        @param observability_schema 固定 observability schema / Fixed observability schema.
        """

        self._login = login
        self._set_local_role_sql = f"SET LOCAL ROLE {_quote_identifier(owner_role.value)};"
        relation = (
            f"{_quote_identifier(observability_schema.value)}."
            f"{_quote_identifier(_TELEMETRY_RELATION)}"
        )
        self._delete_sql = (
            "WITH stale_rows AS ("
            f" SELECT ctid FROM {relation}"
            " WHERE observed_at < %s"
            " ORDER BY observed_at ASC, ctid ASC"
            " LIMIT %s"
            " FOR UPDATE SKIP LOCKED"
            ")"
            f" DELETE FROM {relation} AS telemetry"
            " USING stale_rows"
            " WHERE telemetry.ctid = stale_rows.ctid;"
        )
        self._probe_sql = (
            f"SELECT EXISTS (SELECT 1 FROM {relation} WHERE observed_at < %s LIMIT 1);"
        )

    def delete_batch(self, command: DeleteTelemetryBatch) -> int:
        """@brief 在一个短事务内删除一批 / Delete one batch in one short transaction.

        @param command 固定 cutoff 与资源护栏 / Fixed cutoff and resource guardrails.
        @return 已提交的删除行数 / Committed deleted-row count.
        @raise RetentionExecutionError 数据库失败或 rowcount 无效时抛出且隐藏详情。
        / Raised for database failure or invalid rowcount with details hidden.
        """

        connection = self._connect_safely()
        try:
            with connection.transaction():
                self._prepare_transaction(
                    connection,
                    statement_timeout_ms=command.limits.statement_timeout_ms,
                    lock_timeout_ms=command.limits.lock_timeout_ms,
                )
                cursor = connection.execute(
                    self._delete_sql,
                    (command.cutoff, command.limits.batch_size),
                )
                rowcount = cursor.rowcount
            if not isinstance(rowcount, int) or isinstance(rowcount, bool):
                raise RetentionExecutionError("PostgreSQL 未返回有效遥测删除计数。")
            return rowcount
        except Exception:
            raise RetentionExecutionError("遥测清理数据库操作失败；底层详情已隐藏。") from None
        finally:
            _close_quietly(connection)

    def has_stale(self, probe: StaleTelemetryProbe) -> bool:
        """@brief 在独立短事务中探测剩余过期记录 / Probe remaining stale records in a separate short transaction.

        @param probe 固定 cutoff 与查询护栏 / Fixed cutoff and query guardrails.
        @return 至少存在一条过期记录时为真 / True when at least one stale record exists.
        @raise RetentionExecutionError 数据库结果无效时抛出且隐藏详情。
        / Raised when the database result is invalid, with details hidden.
        """

        connection = self._connect_safely()
        try:
            with connection.transaction():
                self._prepare_transaction(
                    connection,
                    statement_timeout_ms=probe.limits.statement_timeout_ms,
                    lock_timeout_ms=probe.limits.lock_timeout_ms,
                )
                row = connection.execute(self._probe_sql, (probe.cutoff,)).fetchone()
            if row is None or len(row) != 1 or not isinstance(row[0], bool):
                raise RetentionExecutionError("PostgreSQL 返回了无效遥测剩余状态。")
            return row[0]
        except Exception:
            raise RetentionExecutionError("遥测清理数据库操作失败；底层详情已隐藏。") from None
        finally:
            _close_quietly(connection)

    def _connect_safely(self) -> Connection[Any]:
        """@brief 建立带硬连接超时的 migrator 连接 / Open a migrator connection with a hard timeout.

        @return 新 Psycopg connection / New Psycopg connection.
        @raise RetentionExecutionError 连接失败时抛出且隐藏 DSN / Raised on connection failure with DSN hidden.
        """

        try:
            return connect(
                self._login.dsn.reveal(),
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
            )
        except Exception:
            raise RetentionExecutionError("无法连接 PostgreSQL 执行遥测清理。") from None

    def _prepare_transaction(
        self,
        connection: Connection[Any],
        *,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> None:
        """@brief 设置 owner 身份与事务本地超时 / Set owner identity and transaction-local timeouts.

        @param connection 当前短事务连接 / Current short-transaction connection.
        @param statement_timeout_ms 语句超时毫秒 / Statement timeout in milliseconds.
        @param lock_timeout_ms 锁等待超时毫秒 / Lock timeout in milliseconds.
        @return 无返回值 / No return value.
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


def _quote_identifier(value: str) -> str:
    """@brief 引用已验证的 PostgreSQL 标识符 / Quote a validated PostgreSQL identifier.

    @param value RoleName/SchemaName 验证后的文本 / Text validated by RoleName or SchemaName.
    @return 双引号标识符 / Double-quoted identifier.
    """

    return '"' + value.replace('"', '""') + '"'


def _close_quietly(connection: Connection[Any]) -> None:
    """@brief 不让 close 异常泄密或遮蔽主结果 / Prevent close errors from leaking or masking the primary result.

    @param connection 待关闭短生命周期连接 / Short-lived connection to close.
    @return 无返回值 / No return value.
    """

    try:
        connection.close()
    except Exception:
        pass


__all__ = ["PsycopgTelemetryRetentionAdapter"]
