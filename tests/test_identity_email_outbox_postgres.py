"""@brief 身份邮件 0020 migration 与真实 PostgreSQL adapter 测试 / Real PostgreSQL identity-email migration and adapter tests."""

from __future__ import annotations

import asyncio
import getpass
import shutil
import socket
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg.rows import dict_row

from backend.domain.ports import IdentityEmailRateLimitExceeded
from backend.infrastructure.identity_email_outbox import (
    IdentityEmailKeyring,
    IdentityEmailOutboxWorker,
    PostgresIdentityEmailOutbox,
)
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根目录 / Repository root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260723_0020_identity_email_outbox.py"
"""@brief 本测试拥有的 migration 文件 / Migration file owned by this test."""


@dataclass(frozen=True, slots=True)
class _PostgresHarness:
    """@brief 隔离 PostgreSQL 0020 测试环境 / Isolated PostgreSQL 0020 test harness."""

    port: int
    socket_dir: Path
    superuser: str

    @property
    def migration_dsn(self) -> str:
        """@brief 返回 migrator asyncpg DSN / Return the migrator asyncpg DSN."""

        return f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/aiws"

    @property
    def app_dsn(self) -> str:
        """@brief 返回 application asyncpg DSN / Return the application asyncpg DSN."""

        return f"postgresql://aiws_app@127.0.0.1:{self.port}/aiws"

    def super_dsn_for(self, database: str = "aiws") -> str:
        """@brief 返回超级用户 DSN / Return a superuser DSN.

        @param database 目标数据库 / Target database.
        @return psycopg DSN / psycopg DSN.
        """

        return f"postgresql://{self.superuser}@127.0.0.1:{self.port}/{database}"

    def psql(self, binary: Path, sql: str, *, database: str = "aiws") -> None:
        """@brief 通过 psql 执行固定 fixture SQL / Execute fixed fixture SQL through psql."""

        subprocess.run(
            [
                str(binary),
                "-h",
                str(self.socket_dir),
                "-p",
                str(self.port),
                "-d",
                database,
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=sql,
            text=True,
            check=True,
            capture_output=True,
        )

    def rows(self, statement: str) -> list[dict[str, Any]]:
        """@brief 以超级用户读取验证 rows / Read verification rows as the superuser."""

        with psycopg.connect(self.super_dsn_for(), row_factory=dict_row) as connection:
            return [dict(row) for row in connection.execute(statement).fetchall()]

    def execute(self, statement: str) -> None:
        """@brief 以超级用户推进测试时钟状态 / Advance test state as the superuser."""

        with psycopg.connect(self.super_dsn_for()) as connection:
            connection.execute(statement)


def _postgres_binary(name: str) -> Path | None:
    """@brief 定位 PATH 或 Debian versioned PostgreSQL binary / Locate a PostgreSQL binary."""

    direct = shutil.which(name)
    if direct is not None:
        return Path(direct)
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return candidates[0] if candidates else None


def _migration_config(dsn: str) -> Config:
    """@brief 构建显式 migration 配置 / Build explicit migration configuration."""

    configuration = Config(str(PROJECT_ROOT / "alembic.ini"))
    configuration.attributes["aiws.migration_dsn"] = dsn
    for key, value in {
        "owner_role": "aiws_owner",
        "app_role": "aiws_app",
        "dashboard_role": "aiws_dashboard",
        "migrator_role": "aiws_migrator",
        "v2_legacy_workspace_plans": "{}",
    }.items():
        configuration.set_main_option(f"aiws.{key}", value)
    return configuration


@pytest.fixture(scope="module")
def email_postgres(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_PostgresHarness]:
    """@brief 启动临时 PostgreSQL 并真实执行 0020 / Start PostgreSQL and execute 0020 for real."""

    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("identity-email-postgres")
    data = root / "data"
    socket_dir = root / "socket"
    socket_dir.mkdir()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])
    try:
        subprocess.run(
            [
                str(initdb),
                "-D",
                str(data),
                "-A",
                "trust",
                "--no-locale",
                "--encoding=UTF8",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        pytest.skip(f"initdb cannot initialize an unprivileged cluster: {error.stderr}")
    subprocess.run(
        [
            str(pg_ctl),
            "-D",
            str(data),
            "-o",
            f"-p {port} -k {socket_dir}",
            "-l",
            str(root / "postgres.log"),
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    harness = _PostgresHarness(port, socket_dir, getpass.getuser())
    try:
        harness.psql(
            psql,
            """
            CREATE ROLE aiws_owner NOLOGIN;
            CREATE ROLE aiws_migrator LOGIN;
            CREATE ROLE aiws_app LOGIN;
            CREATE ROLE aiws_dashboard LOGIN;
            GRANT aiws_owner TO aiws_migrator;
            CREATE DATABASE aiws OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws TO aiws_owner;
            """,
            database="postgres",
        )
        configuration = _migration_config(harness.migration_dsn)
        command.stamp(configuration, "20260723_0019")
        command.upgrade(configuration, "20260723_0020")
        harness.psql(psql, "GRANT USAGE ON SCHEMA identity TO aiws_app;")
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


def test_0020_is_linear_and_installs_least_privilege_constraints(
    email_postgres: _PostgresHarness,
) -> None:
    """@brief 0020 线性接在 0019 且拒绝 dashboard 读取 / 0020 follows 0019 and denies dashboard reads."""

    source = MIGRATION.read_text(encoding="utf-8")
    assert 'down_revision = "20260723_0019"' in source
    privileges = email_postgres.rows(
        """
        SELECT
          has_table_privilege('aiws_app', 'identity.identity_email_outbox', 'SELECT') AS app_read,
          has_table_privilege('aiws_app', 'identity.identity_email_outbox', 'UPDATE') AS app_update,
          has_table_privilege('aiws_dashboard', 'identity.identity_email_outbox', 'SELECT') AS dashboard_read
        """
    )[0]
    assert privileges == {"app_read": True, "app_update": True, "dashboard_read": False}
    constraints = {
        row["conname"]
        for row in email_postgres.rows(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'identity.identity_email_outbox'::regclass
            """
        )
    }
    assert "ck_identity_email_outbox_state" in constraints
    assert "ck_identity_email_outbox_encryption_metadata" in constraints


class _RecordingTransport:
    """@brief 记录 worker 解密后的测试 transport / Test transport recording worker-decrypted messages."""

    def __init__(self) -> None:
        """@brief 初始化空记录 / Initialize empty records."""

        self.codes: list[tuple[str, str]] = []
        self.recovery: list[str] = []

    async def send_verification_code(self, recipient: str, code: str) -> None:
        """@brief 记录验证码 / Record a verification code."""

        self.codes.append((recipient, code))

    async def send_recovery_notification(self, recipient: str) -> None:
        """@brief 记录恢复通知 / Record a recovery notice."""

        self.recovery.append(recipient)


class _FailingTransport:
    """@brief 模拟不泄露异常文本的 SMTP 故障 / Simulate an SMTP failure whose text must not persist."""

    async def send_verification_code(self, recipient: str, code: str) -> None:
        """@brief 拒绝验证码投递 / Reject verification delivery."""

        raise RuntimeError(f"secret SMTP detail for {recipient} and {code}")

    async def send_recovery_notification(self, recipient: str) -> None:
        """@brief 拒绝恢复通知 / Reject recovery notification."""

        raise RuntimeError(f"secret SMTP detail for {recipient}")


def _outbox(database: AsyncDatabase) -> PostgresIdentityEmailOutbox:
    """@brief 构造固定 key 的测试 adapter / Build a test adapter with fixed keys."""

    return PostgresIdentityEmailOutbox(
        database,
        IdentityEmailKeyring("key-2026-07", {"key-2026-07": bytes(range(32))}),
        rate_limit_hmac_key=bytes(reversed(range(32))),
        lease_duration=timedelta(seconds=30),
        retention=timedelta(days=30),
    )


async def test_real_postgres_atomic_envelope_rolls_back_budget_and_ciphertext(
    email_postgres: _PostgresHarness,
) -> None:
    """@brief 外层业务失败同时回滚频控与 outbox / Outer business failure rolls back budgets and outbox together."""

    database = AsyncDatabase(AsyncDatabaseOptions(email_postgres.app_dsn))
    outbox = _outbox(database)
    before = email_postgres.rows(
        "SELECT "
        "(SELECT count(*) FROM identity.identity_email_outbox) AS outbox_count, "
        "(SELECT count(*) FROM identity.identity_email_rate_limits) AS rate_count"
    )[0]
    try:
        with pytest.raises(RuntimeError, match="simulated identity transition failure"):
            async with outbox.atomic():
                await outbox.send_verification_code(
                    "rollback@example.test",
                    "271828",
                    browser_session_id="idsess_rollback",
                    network_identifier="198.51.100.8",
                    limit_per_hour=2,
                )
                raise RuntimeError("simulated identity transition failure")
        after = email_postgres.rows(
            "SELECT "
            "(SELECT count(*) FROM identity.identity_email_outbox) AS outbox_count, "
            "(SELECT count(*) FROM identity.identity_email_rate_limits) AS rate_count"
        )[0]
        assert after == before
    finally:
        await database.aclose()


async def test_real_postgres_enforces_concurrent_budgets_and_clears_sent_ciphertext(
    email_postgres: _PostgresHarness,
) -> None:
    """@brief 多 worker 竞争只接纳上限内请求，发送后清密文 / Concurrent requests honor the cap and clear ciphertext after send."""

    database = AsyncDatabase(
        AsyncDatabaseOptions(email_postgres.app_dsn, pool_size=8, max_overflow=0)
    )
    outbox = _outbox(database)

    async def enqueue(index: int) -> bool:
        """@brief 尝试一个共享三维额度的 enqueue / Attempt one enqueue sharing all three dimensions."""

        try:
            async with outbox.atomic():
                await outbox.send_verification_code(
                    "klee@example.test",
                    f"{index:06d}",
                    browser_session_id="idsess_same_device",
                    network_identifier="203.0.113.9",
                    limit_per_hour=3,
                )
        except IdentityEmailRateLimitExceeded:
            return False
        return True

    try:
        accepted = await asyncio.gather(*(enqueue(index) for index in range(8)))
        assert accepted.count(True) == 3
        assert accepted.count(False) == 5

        rows = email_postgres.rows(
            "SELECT status, request_count, ciphertext, recipient_digest "
            "FROM identity.identity_email_outbox "
            "JOIN identity.identity_email_rate_limits ON dimension_kind = 'account' "
            "ORDER BY identity_email_outbox.id"
        )
        assert len(rows) == 3
        assert {row["request_count"] for row in rows} == {3}
        assert all(len(bytes(row["recipient_digest"])) == 32 for row in rows)
        assert all(b"klee@example.test" not in bytes(row["ciphertext"]) for row in rows)

        transport = _RecordingTransport()
        worker = IdentityEmailOutboxWorker(
            outbox,
            transport,
            worker_id="worker-send",
            batch_size=10,
            max_attempts=3,
            retry_base=timedelta(seconds=1),
            retry_cap=timedelta(seconds=10),
        )
        result = await worker.run_once()
        assert result.claimed == result.sent == 3
        assert len(transport.codes) == 3
        terminal = email_postgres.rows(
            "SELECT status, nonce, ciphertext, sent_at, payload_cleared_at, retain_until "
            "FROM identity.identity_email_outbox"
        )
        assert all(row["status"] == "sent" for row in terminal)
        assert all(row["nonce"] is None and row["ciphertext"] is None for row in terminal)
        assert all(row["sent_at"] == row["payload_cleared_at"] for row in terminal)
        assert all(row["retain_until"] > row["sent_at"] for row in terminal)
    finally:
        await database.aclose()


async def test_real_postgres_lease_recovery_dead_letter_and_retention(
    email_postgres: _PostgresHarness,
) -> None:
    """@brief crash lease 可恢复，dead-letter 清密文且 retention 有界删除 / Crash leases recover and dead letters clear then expire."""

    database = AsyncDatabase(AsyncDatabaseOptions(email_postgres.app_dsn))
    outbox = _outbox(database)
    try:
        async with outbox.atomic():
            await outbox.send_recovery_notification("lease@example.test")
        first = await outbox.claim(worker_id="worker-a", batch_size=1)
        assert len(first) == 1 and first[0].attempts == 1
        assert await outbox.claim(worker_id="worker-b", batch_size=1) == ()

        email_postgres.execute(
            "UPDATE identity.identity_email_outbox "
            "SET lease_expires_at = transaction_timestamp() - interval '1 second' "
            "WHERE id = '" + first[0].id + "'"
        )
        recovered = await outbox.claim(worker_id="worker-b", batch_size=1)
        assert len(recovered) == 1 and recovered[0].attempts == 2
        disposition = await outbox.acknowledge_failure(
            recovered[0],
            worker_id="worker-b",
            failure_code="transport_unavailable",
            max_attempts=2,
            retry_after=timedelta(seconds=1),
        )
        assert disposition == "dead"
        dead = email_postgres.rows(
            "SELECT status, nonce, ciphertext, dead_at, payload_cleared_at, recipient_digest "
            f"FROM identity.identity_email_outbox WHERE id = '{first[0].id}'"
        )[0]
        assert dead["status"] == "dead"
        assert dead["nonce"] is None and dead["ciphertext"] is None
        assert dead["dead_at"] == dead["payload_cleared_at"]
        assert len(bytes(dead["recipient_digest"])) == 32

        email_postgres.execute(
            "UPDATE identity.identity_email_outbox "
            "SET retain_until = transaction_timestamp() - interval '1 second' "
            f"WHERE id = '{first[0].id}'"
        )
        purged = await outbox.purge_retained(batch_size=10)
        assert purged.outbox_rows >= 1
        assert email_postgres.rows(
            f"SELECT id FROM identity.identity_email_outbox WHERE id = '{first[0].id}'"
        ) == []
    finally:
        await database.aclose()


async def test_real_postgres_worker_retries_with_ciphertext_then_dead_letters_safely(
    email_postgres: _PostgresHarness,
) -> None:
    """@brief transient failure 保留密文重试，耗尽后只保留安全码 / Transient failure retries encrypted then retains only a safe code."""

    database = AsyncDatabase(AsyncDatabaseOptions(email_postgres.app_dsn))
    outbox = _outbox(database)
    worker = IdentityEmailOutboxWorker(
        outbox,
        _FailingTransport(),
        worker_id="worker-failing",
        batch_size=1,
        max_attempts=2,
        retry_base=timedelta(seconds=1),
        retry_cap=timedelta(seconds=2),
        jitter=lambda _lower, ceiling: ceiling,
    )
    try:
        async with outbox.atomic():
            await outbox.send_recovery_notification("retry@example.test")
        first = await worker.run_once()
        assert first.retried == 1 and first.dead == 0
        pending = email_postgres.rows(
            "SELECT id, status, attempts, available_at, updated_at, ciphertext, "
            "last_failure_code FROM identity.identity_email_outbox "
            "WHERE status = 'pending' ORDER BY created_at DESC LIMIT 1"
        )[0]
        assert pending["attempts"] == 1
        assert pending["available_at"] > pending["updated_at"]
        assert pending["ciphertext"] is not None
        assert pending["last_failure_code"] == "transport_unavailable"
        assert "secret SMTP detail" not in str(pending)

        email_postgres.execute(
            "UPDATE identity.identity_email_outbox "
            "SET available_at = transaction_timestamp() - interval '1 second' "
            f"WHERE id = '{pending['id']}'"
        )
        second = await worker.run_once()
        assert second.dead == 1 and second.retried == 0
        terminal = email_postgres.rows(
            "SELECT status, attempts, nonce, ciphertext, last_failure_code "
            f"FROM identity.identity_email_outbox WHERE id = '{pending['id']}'"
        )[0]
        assert terminal == {
            "status": "dead",
            "attempts": 2,
            "nonce": None,
            "ciphertext": None,
            "last_failure_code": "transport_unavailable",
        }
    finally:
        await database.aclose()


def test_0020_downgrade_refuses_to_discard_security_state(
    email_postgres: _PostgresHarness,
) -> None:
    """@brief downgrade 不得丢弃频控或投递证据 / Downgrade must not discard rate or delivery evidence."""

    email_postgres.execute(
        """
        INSERT INTO identity.identity_email_rate_limits (
          dimension_kind, dimension_digest, window_started_at, request_count, updated_at
        ) VALUES (
          'network', decode(repeat('ab', 32), 'hex'),
          date_trunc('hour', transaction_timestamp()), 1, transaction_timestamp()
        ) ON CONFLICT DO NOTHING
        """
    )
    with pytest.raises(RuntimeError, match="cannot downgrade non-empty identity email state"):
        command.downgrade(_migration_config(email_postgres.migration_dsn), "20260723_0019")
    assert email_postgres.rows("SELECT version_num FROM identity.alembic_version") == [
        {"version_num": "20260723_0020"}
    ]
