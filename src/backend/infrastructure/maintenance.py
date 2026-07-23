"""@brief API V2 维护的内存与 PostgreSQL adapter / In-memory and PostgreSQL V2 maintenance adapters.

PostgreSQL adapter 只调用 migration 安装的窄 ``SECURITY DEFINER`` 函数，不设置虚构 actor
或 Workspace GUC，也不获得业务表的额外直连权限。内存 adapter 复用真实 Access 与 V2
idempotency store，使本地运行与生产状态机保持同一语义。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text

from backend.application.ports.maintenance import IdempotencyMaintenanceResult
from backend.application.ports.v2_idempotency import IdempotencyScope, IdempotencyStatus
from backend.domain.workspaces import InvitationStatus
from backend.infrastructure.access import InMemoryAccessStore
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.v2_idempotency import InMemoryV2IdempotencyStore

_MAXIMUM_BATCH_SIZE = 1_000
"""@brief 数据库函数与 adapter 共享的批量硬上限 / Shared database/adapter hard batch limit."""

_EXPIRE_INVITATIONS_SQL = text(
    "SELECT identity.expire_due_workspace_invitations(:candidate_now, :batch_size)"
)
"""@brief 到期邀请窄函数调用 / Narrow due-invitation function call."""

_MAINTAIN_IDEMPOTENCY_SQL = text(
    "SELECT purged_completed_receipts, stranded_pending_receipts, "
    "has_more_stranded_pending_receipts, "
    "oldest_stranded_expires_at "
    "FROM identity.maintain_api_v2_idempotency_receipts(:candidate_now, :batch_size)"
)
"""@brief receipt 清理与观测窄函数调用 / Narrow receipt cleanup and observation call."""

_PURGE_OUTBOX_SQL = text(
    "SELECT agent.purge_expired_outbox_events(:candidate_now, :batch_size)"
)
"""@brief terminal outbox replay-retention 清理窄函数 / Narrow terminal-outbox retention purge call."""


class InMemoryMaintenanceRepository:
    """@brief 复用真实内存 store 的维护 adapter / Maintenance adapter over the real memory stores.

    @param access_store identity/access 共享内存状态 / Shared identity/access memory state.
    @param idempotency_store V2 幂等内存状态 / V2 idempotency memory state.

    @note 两个上游 store 当前只把锁与字典暴露为实现属性；本 adapter 与它们同属
        infrastructure 层，并集中封装这项耦合，避免 application/composition 触碰内部状态。
    """

    def __init__(
        self,
        access_store: InMemoryAccessStore,
        idempotency_store: InMemoryV2IdempotencyStore,
    ) -> None:
        """@brief 绑定共享内存状态 / Bind shared in-memory state.

        @param access_store Access UoW 使用的同一 store / Same store used by Access UoWs.
        @param idempotency_store HTTP executor 使用的同一 V2 store / Same V2 store used by the
            HTTP executor.
        """
        self._access_store = access_store
        self._idempotency_store = idempotency_store

    async def expire_due_invitations(self, *, now: datetime, batch_size: int) -> int:
        """@brief 原子推进一批到期邀请 / Atomically advance one batch of due invitations.

        @param now 带时区截止时刻 / Timezone-aware cutoff.
        @param batch_size 最大推进数 / Maximum rows to advance.
        @return 实际推进数 / Actual number advanced.
        """
        _validate_inputs(now, batch_size)
        async with self._access_store.lock:
            candidates = sorted(
                (
                    invitation
                    for invitation in self._access_store.invitations.values()
                    if invitation.status is InvitationStatus.PENDING
                    and invitation.expires_at <= now
                ),
                key=lambda invitation: (invitation.expires_at, str(invitation.meta.id)),
            )[:batch_size]
            for invitation in candidates:
                self._access_store.invitations[str(invitation.meta.id)] = invitation.expire(now)
            return len(candidates)

    async def maintain_idempotency_receipts(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> IdempotencyMaintenanceResult:
        """@brief 清理 completed 并保留/统计 pending / Purge completed and retain/count pending.

        @param now 带时区截止时刻 / Timezone-aware cutoff.
        @param batch_size 最大 completed 清理数 / Maximum completed receipts to purge.
        @return 清理与 stranded pending 统计 / Purge and stranded-pending statistics.
        """
        _validate_inputs(now, batch_size)
        async with self._idempotency_store._lock:
            expired_completed = sorted(
                (
                    scope
                    for scope, record in self._idempotency_store._records.items()
                    if record.status is IdempotencyStatus.COMPLETED and record.expires_at <= now
                ),
                key=lambda scope: (
                    self._idempotency_store._records[scope].expires_at,
                    _scope_sort_key(scope),
                ),
            )[:batch_size]
            for scope in expired_completed:
                del self._idempotency_store._records[scope]

            stranded_expiries = sorted(
                record.expires_at
                for record in self._idempotency_store._records.values()
                if record.status is IdempotencyStatus.PENDING and record.expires_at <= now
            )[: batch_size + 1]
            observed_expiries = stranded_expiries[:batch_size]
            return IdempotencyMaintenanceResult(
                purged_completed_receipts=len(expired_completed),
                stranded_pending_receipts=len(observed_expiries),
                has_more_stranded_pending_receipts=len(stranded_expiries) > batch_size,
                oldest_stranded_expires_at=(
                    observed_expiries[0] if observed_expiries else None
                ),
            )

    async def purge_expired_outbox_events(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> int:
        """@brief 内存模式无持久化统一 outbox，返回零 / Return zero because memory mode has no durable unified outbox.

        @param now 带时区截止时刻 / Timezone-aware cutoff.
        @param batch_size 有界批量 / Bounded batch size.
        @return 始终为零 / Always zero.
        """

        _validate_inputs(now, batch_size)
        return 0


class PostgresMaintenanceRepository:
    """@brief 仅调用 owner-owned 窄函数的 PostgreSQL adapter / PostgreSQL adapter calling narrow owner-owned functions only.

    @param database composition 管理的数据库 / Database managed by composition.
    """

    def __init__(self, database: AsyncDatabase) -> None:
        """@brief 绑定数据库资源 / Bind the database resource.

        @param database 应用生命周期数据库 / Application-lifetime database.
        """
        self._database = database

    async def expire_due_invitations(self, *, now: datetime, batch_size: int) -> int:
        """@brief 调用邀请到期窄函数 / Call the narrow invitation-expiry function.

        @param now 带时区候选截止时刻；数据库会截断到自身 statement time / Timezone-aware
            candidate cutoff capped by database statement time.
        @param batch_size 最大推进数 / Maximum rows to advance.
        @return 实际推进数 / Actual number advanced.
        """
        _validate_inputs(now, batch_size)
        async with self._database.unscoped_transaction() as session:
            result = await session.execute(
                _EXPIRE_INVITATIONS_SQL,
                {"candidate_now": now, "batch_size": batch_size},
            )
            return _database_count(result.scalar_one(), "expired invitation")

    async def maintain_idempotency_receipts(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> IdempotencyMaintenanceResult:
        """@brief 调用 receipt 清理/观测窄函数 / Call the narrow receipt cleanup/observation function.

        @param now 带时区候选截止时刻；数据库会截断到自身 statement time / Timezone-aware
            candidate cutoff capped by database statement time.
        @param batch_size 最大 completed 清理数 / Maximum completed receipts to purge.
        @return 清理与 stranded pending 统计 / Purge and stranded-pending statistics.
        """
        _validate_inputs(now, batch_size)
        async with self._database.unscoped_transaction() as session:
            result = await session.execute(
                _MAINTAIN_IDEMPOTENCY_SQL,
                {"candidate_now": now, "batch_size": batch_size},
            )
            row = result.one()
            purged = _database_count(row[0], "purged receipt")
            stranded = _database_count(row[1], "stranded pending receipt")
            has_more = row[2]
            if not isinstance(has_more, bool):
                raise RuntimeError("maintenance function returned an invalid stranded overflow")
            oldest = row[3]
            if oldest is not None and not isinstance(oldest, datetime):
                raise RuntimeError("maintenance function returned an invalid stranded timestamp")
            return IdempotencyMaintenanceResult(purged, stranded, has_more, oldest)

    async def purge_expired_outbox_events(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> int:
        """@brief 调用仅删除过期终态 outbox 的窄函数 / Call the narrow expired-terminal-outbox purge function.

        @param now 带时区候选截止时刻；数据库会截断到 statement time / Timezone-aware
            candidate cutoff capped by database statement time.
        @param batch_size 最大删除数 / Maximum rows deleted.
        @return 实际删除数 / Actual number deleted.
        """

        _validate_inputs(now, batch_size)
        async with self._database.unscoped_transaction() as session:
            result = await session.execute(
                _PURGE_OUTBOX_SQL,
                {"candidate_now": now, "batch_size": batch_size},
            )
            return _database_count(result.scalar_one(), "purged outbox event")


def _validate_inputs(now: datetime, batch_size: int) -> None:
    """@brief 校验 adapter 输入 / Validate adapter inputs.

    @param now 带时区截止时刻 / Timezone-aware cutoff.
    @param batch_size 1..1000 批量 / Batch size between one and one thousand.
    @raise ValueError 时间或批量非法时抛出 / Raised for an invalid timestamp or batch size.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("maintenance cutoff must be timezone-aware")
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or not 1 <= batch_size <= _MAXIMUM_BATCH_SIZE
    ):
        raise ValueError("maintenance batch size must be an integer between 1 and 1000")


def _scope_sort_key(scope: IdempotencyScope) -> tuple[str, str, str, str, str]:
    """@brief 为内存清理生成稳定 scope 次序 / Build a stable scope order for memory cleanup.

    @param scope V2 幂等完整 scope / Complete V2 idempotency scope.
    @return 不含秘密的确定性排序元组 / Deterministic non-secret sort tuple.
    """
    return (
        str(scope.user_id),
        "" if scope.workspace_id is None else str(scope.workspace_id),
        scope.method,
        scope.canonical_path,
        scope.key,
    )


def _database_count(value: object, name: str) -> int:
    """@brief 防御性解析数据库计数 / Defensively parse a database count.

    @param value driver 返回值 / Driver-returned value.
    @param name 错误上下文 / Error context.
    @return 非负整数 / Non-negative integer.
    @raise RuntimeError 数据库函数返回错误形状时抛出 / Raised for an invalid database result.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"maintenance function returned an invalid {name} count")
    return value


__all__ = ["InMemoryMaintenanceRepository", "PostgresMaintenanceRepository"]
