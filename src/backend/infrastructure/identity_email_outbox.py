"""@brief PostgreSQL 身份邮件 transactional outbox / PostgreSQL identity-email transactional outbox.

验证码与收件地址只以 AES-256-GCM 密文进入数据库。路由请求与身份 flow 共用
``AsyncDatabase.atomic_envelope``；worker 使用 ``FOR UPDATE SKIP LOCKED`` 的短租约，
网络 I/O 永不占用数据库事务或行锁。
"""

from __future__ import annotations

import asyncio
import hmac
import json
import random
import secrets
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, cast

import sqlalchemy as sa
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from backend.domain.ports import (
    IdentityEmailEnqueueError,
    IdentityEmailRateLimitExceeded,
)
from backend.infrastructure.identity_email import IdentityEmailTransport
from backend.infrastructure.persistence.database import AsyncDatabase
from workspace_shared.ids import new_opaque_id

type IdentityEmailKind = Literal["verification_code", "recovery_notification"]
"""@brief 身份邮件模板类型 / Identity-email template kinds."""

type DeliveryFailureDisposition = Literal["retried", "dead", "lost"]
"""@brief worker 失败确认结果 / Worker failure acknowledgement outcomes."""

_AAD_VERSION = 1
"""@brief 当前 authenticated additional data 格式版本 / Current AAD format version."""

_metadata = sa.MetaData(schema="identity")
"""@brief 不污染 ORM registry 的独立 Core metadata / Standalone Core metadata."""

identity_email_rate_limits = sa.Table(
    "identity_email_rate_limits",
    _metadata,
    sa.Column("dimension_kind", sa.String(16), primary_key=True),
    sa.Column("dimension_digest", sa.LargeBinary(), primary_key=True),
    sa.Column("window_started_at", sa.DateTime(timezone=True), primary_key=True),
    sa.Column("request_count", sa.Integer(), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
)
"""@brief 原子小时窗口频控 Core table / Atomic hourly rate-limit Core table."""

identity_email_outbox = sa.Table(
    "identity_email_outbox",
    _metadata,
    sa.Column("id", sa.String(160), primary_key=True),
    sa.Column("message_kind", sa.String(32), nullable=False),
    sa.Column("recipient_digest", sa.LargeBinary(), nullable=False),
    sa.Column("key_id", sa.String(64), nullable=False),
    sa.Column("aad_version", sa.SmallInteger(), nullable=False),
    sa.Column("nonce", sa.LargeBinary()),
    sa.Column("ciphertext", sa.LargeBinary()),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("attempts", sa.Integer(), nullable=False),
    sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("lease_owner", sa.String(160)),
    sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
    sa.Column("last_failure_code", sa.String(64)),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("sent_at", sa.DateTime(timezone=True)),
    sa.Column("dead_at", sa.DateTime(timezone=True)),
    sa.Column("payload_cleared_at", sa.DateTime(timezone=True)),
    sa.Column("retain_until", sa.DateTime(timezone=True)),
)
"""@brief 加密 durable outbox Core table / Encrypted durable outbox Core table."""


class IdentityEmailPayloadError(RuntimeError):
    """@brief outbox 密文或 AAD 无法可信解码 / Outbox ciphertext or AAD cannot be trusted."""


@dataclass(frozen=True, slots=True)
class EncryptedIdentityEmail:
    """@brief 一封待持久化的 AEAD payload / AEAD payload ready for persistence.

    @param key_id 加密 key 标识 / Encryption-key identifier.
    @param aad_version AAD schema 版本 / AAD schema version.
    @param nonce 每消息唯一的 96-bit nonce / Per-message unique 96-bit nonce.
    @param ciphertext 含 128-bit tag 的密文 / Ciphertext including the 128-bit tag.
    """

    key_id: str
    aad_version: int
    nonce: bytes
    ciphertext: bytes


@dataclass(frozen=True, slots=True)
class ClaimedIdentityEmail:
    """@brief worker 已租约的一行加密邮件 / Encrypted email row leased by a worker."""

    id: str
    message_kind: IdentityEmailKind
    key_id: str
    aad_version: int
    nonce: bytes
    ciphertext: bytes
    attempts: int


@dataclass(frozen=True, slots=True)
class IdentityEmailMessage:
    """@brief 仅在单次 worker 调用内存在的明文邮件 / Plaintext email scoped to one worker call."""

    kind: IdentityEmailKind
    recipient: str
    code: str | None


@dataclass(frozen=True, slots=True)
class IdentityEmailRetentionResult:
    """@brief 单轮有界 retention 结果 / One bounded retention result."""

    outbox_rows: int
    rate_limit_rows: int


@dataclass(frozen=True, slots=True)
class IdentityEmailWorkerResult:
    """@brief 单轮 worker 可观测计数 / Observable counts for one worker pass."""

    claimed: int
    sent: int
    retried: int
    dead: int
    lost_leases: int
    purged_outbox_rows: int
    purged_rate_limit_rows: int


class IdentityEmailKeyring:
    """@brief 支持 key rotation 的 AES-256-GCM keyring / Rotation-aware AES-256-GCM keyring."""

    def __init__(self, active_key_id: str, keys: Mapping[str, bytes]) -> None:
        """@brief 验证并复制 key material / Validate and copy key material.

        @param active_key_id 新 payload 使用的 key / Key used for new payloads.
        @param keys 当前 key 与尚未排空旧 key 的映射 / Active and draining historical keys.
        @raise ValueError key ID 非规范或 key 不是 256 bits / Non-canonical ID or non-256-bit key.
        """

        copied = dict(keys)
        if active_key_id not in copied:
            raise ValueError("active identity email encryption key is unavailable")
        if any(not _valid_key_id(key_id) for key_id in copied):
            raise ValueError("identity email encryption key IDs must be canonical")
        if any(len(key) != 32 for key in copied.values()):
            raise ValueError("identity email encryption keys must contain exactly 256 bits")
        self._active_key_id = active_key_id
        self._keys = copied

    def encrypt(
        self,
        outbox_id: str,
        kind: IdentityEmailKind,
        *,
        recipient: str,
        code: str | None,
    ) -> EncryptedIdentityEmail:
        """@brief 以随机 nonce 和 row-bound AAD 加密邮件 / Encrypt with a random nonce and row-bound AAD.

        @param outbox_id 密文不可移植的目标 row ID / Target row ID binding the ciphertext.
        @param kind 邮件模板类型 / Message template kind.
        @param recipient 收件地址 / Recipient address.
        @param code 验证码；恢复通知必须为空 / Verification code, absent for recovery notices.
        @return key metadata、nonce 与 ciphertext / Key metadata, nonce, and ciphertext.
        """

        payload = _plaintext_payload(kind, recipient=recipient, code=code)
        nonce = secrets.token_bytes(12)
        aad = _associated_data(outbox_id, kind, self._active_key_id, _AAD_VERSION)
        ciphertext = AESGCM(self._keys[self._active_key_id]).encrypt(nonce, payload, aad)
        return EncryptedIdentityEmail(
            self._active_key_id,
            _AAD_VERSION,
            nonce,
            ciphertext,
        )

    def decrypt(self, claimed: ClaimedIdentityEmail) -> IdentityEmailMessage:
        """@brief 验证 AAD/tag 后严格解码邮件 / Authenticate AAD/tag before strict payload decoding.

        @param claimed 带 row metadata 的租约 payload / Leased payload with row metadata.
        @return 仅供当前发送调用的明文 / Plaintext scoped to the current delivery call.
        @raise IdentityEmailPayloadError key、版本、tag 或 JSON schema 不可信 / Untrusted metadata or payload.
        """

        key = self._keys.get(claimed.key_id)
        if key is None or claimed.aad_version != _AAD_VERSION:
            raise IdentityEmailPayloadError("identity email payload key or AAD version is unavailable")
        aad = _associated_data(
            claimed.id,
            claimed.message_kind,
            claimed.key_id,
            claimed.aad_version,
        )
        try:
            plaintext = AESGCM(key).decrypt(claimed.nonce, claimed.ciphertext, aad)
        except (InvalidTag, ValueError) as error:
            raise IdentityEmailPayloadError("identity email payload authentication failed") from error
        return _decode_plaintext(claimed.message_kind, plaintext)


class PostgresIdentityEmailOutbox:
    """@brief 原子 enqueue、租约和 retention adapter / Atomic enqueue, leasing, and retention adapter."""

    def __init__(
        self,
        database: AsyncDatabase,
        keyring: IdentityEmailKeyring,
        *,
        rate_limit_hmac_key: bytes,
        lease_duration: timedelta,
        retention: timedelta,
    ) -> None:
        """@brief 绑定共享数据库和独立 key domains / Bind shared storage and separate key domains.

        @param database 与 hosted identity repository 共用的数据库 / Database shared with identity storage.
        @param keyring AES-256-GCM keyring / AES-256-GCM keyring.
        @param rate_limit_hmac_key 稳定、独立的 256-bit 摘要 key / Stable independent 256-bit digest key.
        @param lease_duration worker crash recovery 租约 / Worker crash-recovery lease.
        @param retention 清密文后审计行寿命 / Audit-row lifetime after ciphertext clearing.
        @raise ValueError key 或时间边界非法 / Invalid key or duration boundary.
        """

        if len(rate_limit_hmac_key) != 32:
            raise ValueError("identity email rate-limit HMAC key must contain exactly 256 bits")
        if lease_duration <= timedelta(0) or retention < timedelta(days=1):
            raise ValueError("identity email lease and retention durations are invalid")
        self._database = database
        self._keyring = keyring
        self._rate_limit_hmac_key = bytes(rate_limit_hmac_key)
        self._lease_duration = lease_duration
        self._retention = retention

    @asynccontextmanager
    async def atomic(self) -> AsyncIterator[None]:
        """@brief 打开或加入 identity/outbox 原子信封 / Open or join the identity/outbox atomic envelope.

        @return 正常退出才提交的异步上下文 / Async context committed only on normal exit.
        """

        if self._database.in_atomic_envelope:
            yield
            return
        async with self._database.atomic_envelope():
            yield

    async def send_verification_code(
        self,
        recipient: str,
        code: str,
        *,
        browser_session_id: str,
        network_identifier: str,
        limit_per_hour: int,
    ) -> None:
        """@brief 原子消费额度并写入验证码密文 / Atomically consume budgets and persist encrypted code.

        @raise IdentityEmailRateLimitExceeded 任一维已达上限 / Any dimension reached its limit.
        @raise IdentityEmailEnqueueError 加密或数据库写入失败 / Encryption or database write failed.
        """

        try:
            await self._enqueue(
                "verification_code",
                recipient=recipient,
                code=code,
                rate_dimensions=(
                    ("account", recipient),
                    ("device", browser_session_id),
                    ("network", network_identifier),
                ),
                limit_per_hour=limit_per_hour,
            )
        except IdentityEmailRateLimitExceeded:
            raise
        except Exception as error:
            raise IdentityEmailEnqueueError("identity email enqueue failed") from error

    async def send_recovery_notification(self, recipient: str) -> None:
        """@brief 写入恢复通知密文 / Persist an encrypted recovery notification.

        @param recipient 已恢复账户地址 / Recovered account address.
        @raise IdentityEmailEnqueueError 加密或数据库写入失败 / Encryption or database write failed.
        """

        try:
            await self._enqueue(
                "recovery_notification",
                recipient=recipient,
                code=None,
                rate_dimensions=(),
                limit_per_hour=None,
            )
        except Exception as error:
            raise IdentityEmailEnqueueError("identity email enqueue failed") from error

    async def _enqueue(
        self,
        kind: IdentityEmailKind,
        *,
        recipient: str,
        code: str | None,
        rate_dimensions: tuple[tuple[str, str], ...],
        limit_per_hour: int | None,
    ) -> None:
        """@brief 在当前 envelope 的 SAVEPOINT 中频控并 INSERT / Rate limit and insert in the envelope savepoint."""

        outbox_id = new_opaque_id("emailmsg")
        encrypted = self._keyring.encrypt(
            outbox_id,
            kind,
            recipient=recipient,
            code=code,
        )
        async with self._database.unscoped_transaction() as session:
            if rate_dimensions:
                if limit_per_hour is None or limit_per_hour < 1:
                    raise ValueError("identity email rate limit must be positive")
                for dimension_kind, value in rate_dimensions:
                    await self._consume_budget(
                        session,
                        dimension_kind=dimension_kind,
                        value=value,
                        limit_per_hour=limit_per_hour,
                    )
            now = sa.func.transaction_timestamp()
            await session.execute(
                sa.insert(identity_email_outbox).values(
                    id=outbox_id,
                    message_kind=kind,
                    recipient_digest=self._digest("recipient", recipient),
                    key_id=encrypted.key_id,
                    aad_version=encrypted.aad_version,
                    nonce=encrypted.nonce,
                    ciphertext=encrypted.ciphertext,
                    status="pending",
                    attempts=0,
                    available_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )

    async def _consume_budget(
        self,
        session: AsyncSession,
        *,
        dimension_kind: str,
        value: str,
        limit_per_hour: int,
    ) -> None:
        """@brief 以 UPSERT 原子增加一个小时窗口 / Atomically increment one hourly window with UPSERT."""

        window = sa.func.date_trunc("hour", sa.func.transaction_timestamp())
        insert_statement = pg_insert(identity_email_rate_limits).values(
            dimension_kind=dimension_kind,
            dimension_digest=self._digest(dimension_kind, value),
            window_started_at=window,
            request_count=1,
            updated_at=sa.func.transaction_timestamp(),
        )
        statement = insert_statement.on_conflict_do_update(
            index_elements=(
                identity_email_rate_limits.c.dimension_kind,
                identity_email_rate_limits.c.dimension_digest,
                identity_email_rate_limits.c.window_started_at,
            ),
            set_={
                "request_count": identity_email_rate_limits.c.request_count + 1,
                "updated_at": sa.func.transaction_timestamp(),
            },
            where=identity_email_rate_limits.c.request_count < limit_per_hour,
        ).returning(identity_email_rate_limits.c.request_count)
        if (await session.execute(statement)).scalar_one_or_none() is None:
            raise IdentityEmailRateLimitExceeded

    def _digest(self, domain: str, value: str) -> bytes:
        """@brief 生成不可逆、域分离的 HMAC 摘要 / Produce a domain-separated irreversible HMAC digest."""

        normalized = value.strip().casefold() if domain in {"account", "recipient"} else value
        material = f"aiws.identity-email.v1\0{domain}\0{normalized}".encode()
        return hmac.digest(self._rate_limit_hmac_key, material, "sha256")

    def decrypt_claimed(self, claimed: ClaimedIdentityEmail) -> IdentityEmailMessage:
        """@brief 为 worker 解密一行精确租约 / Decrypt one exact lease for the worker.

        @param claimed 已从本 adapter 租约的 row / Row leased from this adapter.
        @return 经 AEAD 验证的短生命周期明文 / Short-lived AEAD-authenticated plaintext.
        @raise IdentityEmailPayloadError payload 不可信 / Payload cannot be trusted.
        """

        return self._keyring.decrypt(claimed)

    async def erase_recipient(self, recipient: str, *, limit: int = 1_000) -> bool:
        """@brief 有界擦除一个收件人的待发、终态邮件与账号频控 / Boundedly erase one recipient's queued, terminal, and account-budget state.

        @param recipient 删除执行器仍持有的规范化邮箱 / Canonical address still held by the
            deletion executor.
        @param limit 单事务锁定并删除的最大邮件数 / Maximum messages locked and deleted in one
            transaction.
        @return 本轮后已无邮件且不存在在途租约时为真 / True when no messages or in-flight
            lease remain after this pass.
        @note 未过期 ``leased`` 行可能已被 worker 解密，必须等待租约结束后再完成账号删除；
            直接删行不能撤回已经开始的 SMTP side effect。/ An unexpired leased row may already
            have been decrypted by a worker, so account deletion waits for its lease rather than
            pretending that deleting the row can recall an in-flight SMTP side effect.
        """

        if (
            not isinstance(recipient, str)
            or not recipient
            or recipient.strip() != recipient
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 1_000
        ):
            raise ValueError("identity email recipient erasure arguments are invalid")
        recipient_digest = self._digest("recipient", recipient)
        account_digest = self._digest("account", recipient)
        table = identity_email_outbox
        async with self._database.unscoped_transaction() as session:
            rows = (
                await session.execute(
                    sa.select(
                        table.c.id,
                        table.c.status,
                        sa.and_(
                            table.c.status == "leased",
                            table.c.lease_expires_at > sa.func.transaction_timestamp(),
                        ).label("active_lease"),
                    )
                    .where(table.c.recipient_digest == recipient_digest)
                    .order_by(table.c.created_at, table.c.id)
                    .limit(limit + 1)
                    .with_for_update()
                )
            ).all()
            active_lease = any(bool(row[2]) for row in rows)
            if active_lease:
                return False
            identifiers = tuple(str(row[0]) for row in rows[:limit])
            if identifiers:
                await session.execute(sa.delete(table).where(table.c.id.in_(identifiers)))
            has_more = len(rows) > limit
            if not has_more:
                await session.execute(
                    sa.delete(identity_email_rate_limits).where(
                        identity_email_rate_limits.c.dimension_kind == "account",
                        identity_email_rate_limits.c.dimension_digest == account_digest,
                    )
                )
            return not has_more

    async def claim(self, *, worker_id: str, batch_size: int) -> tuple[ClaimedIdentityEmail, ...]:
        """@brief 以 SKIP LOCKED 租约一批 due row / Lease a due batch with SKIP LOCKED.

        @param worker_id 当前进程唯一 lease owner / Process-unique lease owner.
        @param batch_size 1..1000 的有界 batch / Bounded batch in the range 1..1000.
        @return 已增加 attempts 的加密 rows / Encrypted rows whose attempt count was advanced.
        """

        if not worker_id or len(worker_id) > 160 or not 1 <= batch_size <= 1_000:
            raise ValueError("identity email worker ID or batch size is invalid")
        table = identity_email_outbox
        now = sa.func.transaction_timestamp()
        due_at = sa.case(
            (table.c.status == "pending", table.c.available_at),
            else_=table.c.lease_expires_at,
        )
        candidates = (
            sa.select(table.c.id)
            .where(
                sa.or_(
                    sa.and_(table.c.status == "pending", table.c.available_at <= now),
                    sa.and_(table.c.status == "leased", table.c.lease_expires_at <= now),
                )
            )
            .order_by(due_at, table.c.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
            .cte("identity_email_due")
        )
        statement = (
            sa.update(table)
            .where(table.c.id.in_(sa.select(candidates.c.id)))
            .values(
                status="leased",
                attempts=table.c.attempts + 1,
                lease_owner=worker_id,
                lease_expires_at=now + self._lease_duration,
                updated_at=now,
            )
            .returning(
                table.c.id,
                table.c.message_kind,
                table.c.key_id,
                table.c.aad_version,
                table.c.nonce,
                table.c.ciphertext,
                table.c.attempts,
            )
        )
        async with self._database.unscoped_transaction() as session:
            rows = (await session.execute(statement)).mappings().all()
        return tuple(sorted((_claimed_from_row(row) for row in rows), key=lambda row: row.id))

    async def acknowledge_success(self, claimed: ClaimedIdentityEmail, *, worker_id: str) -> bool:
        """@brief 标记发送成功并立即清除密文 / Mark delivery sent and immediately clear ciphertext.

        @return 精确租约仍属于 worker 时为真 / True when the exact lease still belongs to the worker.
        """

        table = identity_email_outbox
        now = sa.func.transaction_timestamp()
        statement = (
            sa.update(table)
            .where(
                table.c.id == claimed.id,
                table.c.status == "leased",
                table.c.lease_owner == worker_id,
                table.c.attempts == claimed.attempts,
            )
            .values(
                status="sent",
                nonce=None,
                ciphertext=None,
                lease_owner=None,
                lease_expires_at=None,
                last_failure_code=None,
                updated_at=now,
                sent_at=now,
                payload_cleared_at=now,
                retain_until=now + self._retention,
            )
            .returning(table.c.id)
        )
        async with self._database.unscoped_transaction() as session:
            return (await session.execute(statement)).scalar_one_or_none() is not None

    async def acknowledge_failure(
        self,
        claimed: ClaimedIdentityEmail,
        *,
        worker_id: str,
        failure_code: str,
        max_attempts: int,
        retry_after: timedelta,
        permanent: bool = False,
    ) -> DeliveryFailureDisposition:
        """@brief 安排安全重试或 dead-letter 并清密文 / Schedule retry or dead-letter and clear ciphertext.

        @return ``retried``、``dead`` 或租约已丢失的 ``lost`` / Retry, dead-letter, or lost lease.
        """

        if not _valid_failure_code(failure_code) or max_attempts < 1 or retry_after < timedelta(0):
            raise ValueError("identity email failure acknowledgement is invalid")
        table = identity_email_outbox
        now = sa.func.transaction_timestamp()
        terminal = permanent or claimed.attempts >= max_attempts
        values: dict[str, object] = {
            "status": "dead" if terminal else "pending",
            "lease_owner": None,
            "lease_expires_at": None,
            "last_failure_code": failure_code,
            "updated_at": now,
        }
        if terminal:
            values.update(
                nonce=None,
                ciphertext=None,
                dead_at=now,
                payload_cleared_at=now,
                retain_until=now + self._retention,
            )
        else:
            values["available_at"] = now + retry_after
        statement = (
            sa.update(table)
            .where(
                table.c.id == claimed.id,
                table.c.status == "leased",
                table.c.lease_owner == worker_id,
                table.c.attempts == claimed.attempts,
            )
            .values(**values)
            .returning(table.c.id)
        )
        async with self._database.unscoped_transaction() as session:
            acknowledged = (await session.execute(statement)).scalar_one_or_none()
        if acknowledged is None:
            return "lost"
        return "dead" if terminal else "retried"

    async def purge_retained(self, *, batch_size: int) -> IdentityEmailRetentionResult:
        """@brief 有界删除过期审计行与旧频控窗口 / Bounded purge of expired audit rows and stale windows."""

        if not 1 <= batch_size <= 1_000:
            raise ValueError("identity email retention batch must be between 1 and 1000")
        table = identity_email_outbox
        now = sa.func.transaction_timestamp()
        terminal = (
            sa.select(table.c.id)
            .where(
                table.c.status.in_(("sent", "dead")),
                table.c.retain_until <= now,
            )
            .order_by(table.c.retain_until, table.c.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
            .cte("identity_email_expired")
        )
        delete_outbox = (
            sa.delete(table)
            .where(table.c.id.in_(sa.select(terminal.c.id)))
            .returning(table.c.id)
        )
        rate = identity_email_rate_limits
        stale_rate = (
            sa.select(
                rate.c.dimension_kind,
                rate.c.dimension_digest,
                rate.c.window_started_at,
            )
            .where(
                rate.c.window_started_at
                < sa.func.date_trunc("hour", now) - timedelta(hours=1)
            )
            .order_by(rate.c.window_started_at, rate.c.dimension_kind)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
            .cte("identity_email_stale_rate")
        )
        delete_rate = (
            sa.delete(rate)
            .where(
                sa.tuple_(
                    rate.c.dimension_kind,
                    rate.c.dimension_digest,
                    rate.c.window_started_at,
                ).in_(
                    sa.select(
                        stale_rate.c.dimension_kind,
                        stale_rate.c.dimension_digest,
                        stale_rate.c.window_started_at,
                    )
                )
            )
            .returning(rate.c.dimension_kind)
        )
        async with self._database.unscoped_transaction() as session:
            outbox_count = len((await session.execute(delete_outbox)).all())
            rate_count = len((await session.execute(delete_rate)).all())
        return IdentityEmailRetentionResult(outbox_count, rate_count)


class IdentityEmailOutboxWorker:
    """@brief SMTP 网络 I/O 与 retry policy 的单轮 worker / One-pass SMTP worker with retry policy."""

    def __init__(
        self,
        outbox: PostgresIdentityEmailOutbox,
        transport: IdentityEmailTransport,
        *,
        worker_id: str,
        batch_size: int,
        max_attempts: int,
        retry_base: timedelta,
        retry_cap: timedelta,
        jitter: Callable[[float, float], float] | None = None,
    ) -> None:
        """@brief 绑定 transport 与有界 retry 参数 / Bind transport and bounded retry parameters."""

        if (
            not worker_id
            or len(worker_id) > 160
            or not 1 <= batch_size <= 1_000
            or not 1 <= max_attempts <= 100
            or retry_base <= timedelta(0)
            or retry_cap < retry_base
        ):
            raise ValueError("identity email worker settings are invalid")
        self._outbox = outbox
        self._transport = transport
        self._worker_id = worker_id
        self._batch_size = batch_size
        self._max_attempts = max_attempts
        self._retry_base = retry_base
        self._retry_cap = retry_cap
        self._jitter = jitter or random.SystemRandom().uniform

    async def run_once(self) -> IdentityEmailWorkerResult:
        """@brief 租约、发送、确认并有界清理一轮 / Lease, deliver, acknowledge, and purge one batch.

        @return 不含 PII 或 secret 的聚合计数 / Aggregate counts without PII or secrets.
        """

        claimed_rows = await self._outbox.claim(
            worker_id=self._worker_id,
            batch_size=self._batch_size,
        )
        sent = retried = dead = lost = 0
        for claimed in claimed_rows:
            disposition: DeliveryFailureDisposition | None = None
            try:
                message = self._outbox.decrypt_claimed(claimed)
                if message.kind == "verification_code":
                    assert message.code is not None
                    await self._transport.send_verification_code(message.recipient, message.code)
                else:
                    await self._transport.send_recovery_notification(message.recipient)
                if await self._outbox.acknowledge_success(claimed, worker_id=self._worker_id):
                    sent += 1
                else:
                    lost += 1
            except asyncio.CancelledError:
                raise
            except IdentityEmailPayloadError:
                disposition = await self._outbox.acknowledge_failure(
                    claimed,
                    worker_id=self._worker_id,
                    failure_code="payload_invalid",
                    max_attempts=self._max_attempts,
                    retry_after=timedelta(0),
                    permanent=True,
                )
            except Exception:
                disposition = await self._outbox.acknowledge_failure(
                    claimed,
                    worker_id=self._worker_id,
                    failure_code="transport_unavailable",
                    max_attempts=self._max_attempts,
                    retry_after=identity_email_retry_delay(
                        claimed.attempts,
                        base=self._retry_base,
                        cap=self._retry_cap,
                        jitter=self._jitter,
                    ),
                )
            if disposition == "retried":
                retried += 1
            elif disposition == "dead":
                dead += 1
            elif disposition == "lost":
                lost += 1
        retention = await self._outbox.purge_retained(batch_size=self._batch_size)
        return IdentityEmailWorkerResult(
            claimed=len(claimed_rows),
            sent=sent,
            retried=retried,
            dead=dead,
            lost_leases=lost,
            purged_outbox_rows=retention.outbox_rows,
            purged_rate_limit_rows=retention.rate_limit_rows,
        )


def identity_email_retry_delay(
    attempt: int,
    *,
    base: timedelta,
    cap: timedelta,
    jitter: Callable[[float, float], float],
) -> timedelta:
    """@brief 计算 capped exponential equal-jitter delay / Compute capped exponential equal-jitter delay.

    @param attempt 已执行且从 1 开始的 attempt / Completed one-based attempt number.
    @param base 首次 retry 的 ceiling / First-retry ceiling.
    @param cap 任意 retry 的硬 ceiling / Hard ceiling for every retry.
    @param jitter 可注入的均匀采样器 / Injectable uniform sampler.
    @return ``[ceiling/2, ceiling]`` 内的 delay / Delay within the equal-jitter interval.
    """

    if attempt < 1 or base <= timedelta(0) or cap < base:
        raise ValueError("identity email retry parameters are invalid")
    exponent = min(attempt - 1, 30)
    ceiling = min(cap.total_seconds(), base.total_seconds() * (2**exponent))
    sampled = jitter(ceiling / 2, ceiling)
    return timedelta(seconds=max(ceiling / 2, min(ceiling, sampled)))


def _plaintext_payload(
    kind: IdentityEmailKind,
    *,
    recipient: str,
    code: str | None,
) -> bytes:
    """@brief 编码严格、版本内隐的明文 schema / Encode the strict version-local plaintext schema."""

    if not recipient or recipient != recipient.strip() or len(recipient) > 320:
        raise ValueError("identity email recipient is invalid")
    if kind == "verification_code":
        if code is None or len(code) != 6 or not code.isascii() or not code.isdigit():
            raise ValueError("identity email verification code is invalid")
        payload: dict[str, str] = {"recipient": recipient, "code": code}
    else:
        if code is not None:
            raise ValueError("recovery notification must not contain a verification code")
        payload = {"recipient": recipient}
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _decode_plaintext(kind: IdentityEmailKind, plaintext: bytes) -> IdentityEmailMessage:
    """@brief 拒绝任何 schema drift 后返回明文 / Reject schema drift before returning plaintext."""

    try:
        value = json.loads(plaintext)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise IdentityEmailPayloadError("identity email payload is not valid JSON") from error
    if not isinstance(value, dict):
        raise IdentityEmailPayloadError("identity email payload must be an object")
    if kind == "verification_code":
        if set(value) != {"recipient", "code"}:
            raise IdentityEmailPayloadError("identity email verification payload schema is invalid")
        recipient = value.get("recipient")
        code = value.get("code")
        if (
            not isinstance(recipient, str)
            or not isinstance(code, str)
            or len(code) != 6
            or not code.isascii()
            or not code.isdigit()
        ):
            raise IdentityEmailPayloadError("identity email verification payload values are invalid")
        return IdentityEmailMessage(kind, recipient, code)
    if set(value) != {"recipient"} or not isinstance(value.get("recipient"), str):
        raise IdentityEmailPayloadError("identity email recovery payload schema is invalid")
    return IdentityEmailMessage(kind, cast(str, value["recipient"]), None)


def _associated_data(
    outbox_id: str,
    kind: IdentityEmailKind,
    key_id: str,
    version: int,
) -> bytes:
    """@brief 将密文绑定到 row、kind、key 与版本 / Bind ciphertext to row, kind, key, and version."""

    metadata = json.dumps(
        {"id": outbox_id, "key_id": key_id, "kind": kind, "version": version},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return b"aiws.identity-email.outbox\0" + metadata


def _claimed_from_row(row: RowMapping) -> ClaimedIdentityEmail:
    """@brief 从数据库 row 构造严格租约值 / Build a strict claimed value from a database row."""

    kind = str(row["message_kind"])
    if kind not in {"verification_code", "recovery_notification"}:
        raise IdentityEmailPayloadError("identity email row has an unknown message kind")
    nonce = row["nonce"]
    ciphertext = row["ciphertext"]
    if not isinstance(nonce, bytes | bytearray) or not isinstance(ciphertext, bytes | bytearray):
        raise IdentityEmailPayloadError("identity email row has no active ciphertext")
    return ClaimedIdentityEmail(
        id=str(row["id"]),
        message_kind=cast(IdentityEmailKind, kind),
        key_id=str(row["key_id"]),
        aad_version=int(row["aad_version"]),
        nonce=bytes(nonce),
        ciphertext=bytes(ciphertext),
        attempts=int(row["attempts"]),
    )


def _valid_key_id(value: str) -> bool:
    """@brief 校验 key ID 的 portable ASCII 子集 / Validate the portable ASCII key-ID subset."""

    return bool(value) and len(value) <= 64 and all(
        character.isascii() and (character.isalnum() or character in "._-")
        for character in value
    )


def _valid_failure_code(value: str) -> bool:
    """@brief 校验不含异常文本的安全失败码 / Validate a safe failure code without exception text."""

    return bool(value) and len(value) <= 64 and all(
        character.isascii() and (character.islower() or character.isdigit() or character == "_")
        for character in value
    )


__all__ = [
    "ClaimedIdentityEmail",
    "EncryptedIdentityEmail",
    "IdentityEmailKeyring",
    "IdentityEmailMessage",
    "IdentityEmailOutboxWorker",
    "IdentityEmailPayloadError",
    "IdentityEmailRetentionResult",
    "IdentityEmailWorkerResult",
    "PostgresIdentityEmailOutbox",
    "identity_email_outbox",
    "identity_email_rate_limits",
    "identity_email_retry_delay",
]
