"""@brief 0027 outbox 生命周期的真实 PostgreSQL 回归 / Real-PostgreSQL regressions for the 0027 outbox lifecycle."""

from __future__ import annotations

import asyncio
import getpass
import shutil
import socket
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg.rows import dict_row

from backend.application.maintenance import MaintenanceBatchSizes, V2MaintenanceService
from backend.infrastructure.maintenance import PostgresMaintenanceRepository
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

USER_ID = "user_outboxlife001"
"""@brief 真库测试用户 / Real-database test user."""

WORKSPACE_ID = "workspace_outboxlife01"
"""@brief 真库测试 Workspace / Real-database test Workspace."""

_PRE_0027_FIXTURE = f"""
BEGIN;
SELECT set_config('app.actor_id', '{USER_ID}', true),
       set_config('app.workspace_id', '{WORKSPACE_ID}', true);
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    '{USER_ID}', 'outbox-lifecycle-subject', 'Outbox Owner',
    'outbox-lifecycle@example.com', true, 'outbox-lifecycle@example.com',
    'en', 'active', '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z', 1, '{{}}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    '{WORKSPACE_ID}', '{USER_ID}', 'Outbox Lifecycle', 'en',
    'outbox-lifecycle', 'team', 'global', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{{}}'
);
INSERT INTO agent.outbox_events (
    id, workspace_id, resource_owner_id, aggregate_type, aggregate_id,
    subject_revision, event_type, sequence, occurred_at, payload, trace_id,
    replay_expires_at, status, published_at, attempt_count,
    created_at, updated_at, revision, extensions
) VALUES
(
    'event_workpending01', '{WORKSPACE_ID}', '{USER_ID}', 'agent_run',
    'agent_run_outbox01', 1, 'agent.run.queued', 0, '2019-01-01T00:00:00Z',
    '{{}}', NULL, '2020-01-01T00:00:00Z', 'pending', NULL, 0,
    '2019-01-01T00:00:00Z', '2019-01-02T00:00:00Z', 1, '{{}}'
),
(
    'event_notifypending1', '{WORKSPACE_ID}', '{USER_ID}', 'job',
    'job_outboxnotify01', 1, 'job.updated', 0, '2026-01-01T00:00:00Z',
    '{{"status":"queued"}}', NULL, '2100-01-01T00:00:00Z', 'pending', NULL, 0,
    '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z', 1, '{{"keep":true}}'
),
(
    'event_notifypublish1', '{WORKSPACE_ID}', '{USER_ID}', 'resume',
    'resume_outbox0001', 1, 'resume.created', 0, '2026-02-01T00:00:00Z',
    '{{}}', NULL, '2100-01-01T00:00:00Z', 'published',
    '2026-02-01T00:00:00Z', 0, '2026-02-01T00:00:00Z',
    '2026-02-01T00:00:00Z', 1, '{{"keep":true}}'
),
(
    'event_unknown000001', '{WORKSPACE_ID}', '{USER_ID}', 'job',
    'job_unknown000001', 1, 'plugin.magic', 0, '2026-03-01T00:00:00Z',
    '{{}}', NULL, '2100-01-01T00:00:00Z', 'pending', NULL, 0,
    '2026-03-01T00:00:00Z', '2026-03-01T00:00:00Z', 1, '{{}}'
);
COMMIT;
"""
"""@brief 0026 上的 work、notification 与未知事件 fixture / Work, notification, and unknown-event fixture at 0026."""

_TERMINAL_FIXTURE = f"""
BEGIN;
SELECT set_config('app.actor_id', '{USER_ID}', true),
       set_config('app.workspace_id', '{WORKSPACE_ID}', true);
INSERT INTO agent.outbox_events (
    id, workspace_id, resource_owner_id, aggregate_type, aggregate_id,
    subject_revision, event_type, sequence, occurred_at, payload, trace_id,
    replay_expires_at, status, published_at, attempt_count,
    created_at, updated_at, revision, extensions
) VALUES
(
    'event_expiredpublish', '{WORKSPACE_ID}', '{USER_ID}', 'job',
    'job_expiredpublish', 1, 'job.updated', 0, '2018-01-01T00:00:00Z',
    '{{}}', NULL, '2019-01-01T00:00:00Z', 'published',
    '2018-01-01T00:00:00Z', 0, '2018-01-01T00:00:00Z',
    '2018-01-01T00:00:00Z', 1, '{{}}'
),
(
    'event_expiredfailed1', '{WORKSPACE_ID}', '{USER_ID}', 'agent_run',
    'agent_run_failed01', 1, 'agent.run.queued', 0, '2018-01-01T00:00:01Z',
    '{{}}', NULL, '2019-01-01T00:00:01Z', 'failed', NULL, 3,
    '2018-01-01T00:00:01Z', '2018-01-01T00:00:01Z', 1, '{{}}'
),
(
    'event_freshpublish01', '{WORKSPACE_ID}', '{USER_ID}', 'job',
    'job_freshpublish01', 1, 'job.updated', 0, '2026-01-01T00:00:01Z',
    '{{}}', NULL, '2100-01-01T00:00:01Z', 'published',
    '2026-01-01T00:00:01Z', 0, '2026-01-01T00:00:01Z',
    '2026-01-01T00:00:01Z', 1, '{{}}'
);
COMMIT;
"""
"""@brief 0027 retention 的 expired/fresh 终态 fixture / Expired and fresh terminal fixture for 0027 retention."""

_EXHAUSTED_CRASH_FIXTURE = f"""
BEGIN;
SELECT set_config('app.actor_id', '{USER_ID}', true),
       set_config('app.workspace_id', '{WORKSPACE_ID}', true);
INSERT INTO agent.outbox_events (
    id, workspace_id, resource_owner_id, aggregate_type, aggregate_id,
    subject_revision, event_type, sequence, occurred_at, payload, trace_id,
    replay_expires_at, status, published_at, attempt_count, lease_token_hash,
    lease_expires_at, created_at, updated_at, revision, extensions
) VALUES (
    'event_exhaustedcrash', '{WORKSPACE_ID}', '{USER_ID}', 'job',
    'job_exhaustedcrash', 1, 'resume.job_created', 0, '2026-01-01T00:00:02Z',
    '{{}}', NULL, '2100-01-01T00:00:02Z', 'processing', NULL, 3,
    repeat('a', 64), '2019-01-01T00:00:02Z', '2026-01-01T00:00:02Z',
    '2026-01-01T00:00:02Z', 1, '{{}}'
);
COMMIT;
"""
"""@brief 补偿已提交但 outbox CAS 前崩溃的耗尽租约 / Exhausted lease crashed after compensation but before outbox CAS."""


@dataclass(frozen=True, slots=True)
class _PostgresHarness:
    """@brief 隔离 PostgreSQL migration 测试环境 / Isolated PostgreSQL migration-test environment.

    @param port 随机 TCP 端口 / Random TCP port.
    @param socket_dir Unix socket 目录 / Unix-socket directory.
    @param superuser initdb 超级用户 / Initdb superuser.
    @param psql_binary psql 绝对路径 / Absolute path to psql.
    """

    port: int
    socket_dir: Path
    superuser: str
    psql_binary: Path

    @property
    def migration_dsn(self) -> str:
        """@brief 返回 migrator DSN / Return the migrator DSN."""

        return f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/aiws"

    @property
    def app_dsn(self) -> str:
        """@brief 返回 runtime app DSN / Return the runtime-app DSN."""

        return f"postgresql://aiws_app@127.0.0.1:{self.port}/aiws"

    @property
    def super_dsn(self) -> str:
        """@brief 返回超级用户 DSN / Return the superuser DSN."""

        return f"postgresql://{self.superuser}@127.0.0.1:{self.port}/aiws"

    def psql(self, sql: str, *, database: str = "aiws") -> None:
        """@brief 用超级用户执行固定 fixture SQL / Execute static fixture SQL as superuser.

        @param sql 测试模块内 SQL / Test-module-owned SQL.
        @param database 目标数据库 / Target database.
        """

        subprocess.run(
            [
                str(self.psql_binary),
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
        """@brief 以超级用户读取验证行 / Read verification rows as superuser.

        @param statement 测试模块内查询 / Test-module-owned query.
        @return dict 行列表 / List of dictionary rows.
        """

        with psycopg.connect(self.super_dsn, row_factory=dict_row) as connection:
            return [dict(row) for row in connection.execute(statement).fetchall()]


def _postgres_binary(name: str) -> Path | None:
    """@brief 定位 PATH 或 Debian versioned PostgreSQL binary / Locate a PATH or Debian-versioned PostgreSQL binary.

    @param name binary 名称 / Binary name.
    @return 可执行路径或空 / Executable path or none.
    """

    direct = shutil.which(name)
    if direct is not None:
        return Path(direct)
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return candidates[0] if candidates else None


def _migration_config(dsn: str) -> Config:
    """@brief 构建不依赖环境变量的 Alembic 配置 / Build Alembic configuration without environment variables.

    @param dsn migrator DSN / Migrator DSN.
    @return 完整 role 配置 / Complete role configuration.
    """

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
def outbox_postgres(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_PostgresHarness]:
    """@brief 启动真实 PG，证明未知类型回滚后完成 0027 / Start real PG and complete 0027 after proving unknown-type rollback.

    @param tmp_path_factory pytest 临时目录工厂 / Pytest temporary-path factory.
    @return 已升级 0027 的 harness / Harness upgraded to 0027.
    """

    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("outbox-lifecycle-postgres")
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
    harness = _PostgresHarness(port, socket_dir, getpass.getuser(), psql)
    try:
        harness.psql(
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
        try:
            harness.psql("CREATE EXTENSION vector;")
        except subprocess.CalledProcessError:
            pytest.skip("the PostgreSQL vector extension is unavailable")
        configuration = _migration_config(harness.migration_dsn)
        command.upgrade(configuration, "20260723_0026")
        harness.psql(_PRE_0027_FIXTURE)
        with pytest.raises(RuntimeError, match="unclassified outbox event types"):
            command.upgrade(configuration, "20260723_0027")
        assert harness.rows("SELECT version_num FROM identity.alembic_version") == [
            {"version_num": "20260723_0026"}
        ]
        harness.psql("DELETE FROM agent.outbox_events WHERE id = 'event_unknown000001';")
        command.upgrade(configuration, "20260723_0027")
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


@pytest.mark.asyncio
async def test_0027_real_postgres_backfill_retention_constraint_and_exact_downgrade(
    outbox_postgres: _PostgresHarness,
) -> None:
    """@brief 真库验证精确回填、terminal-only 清理、闭集约束与降级 / Verify exact backfill, terminal-only purge, closed constraint, and downgrade."""

    rows = {
        row["id"]: row
        for row in outbox_postgres.rows(
            "SELECT id, status, published_at, occurred_at, updated_at, extensions "
            "FROM agent.outbox_events ORDER BY id"
        )
    }
    backfilled = rows["event_notifypending1"]
    assert backfilled["status"] == "published"
    assert backfilled["published_at"] == backfilled["occurred_at"]
    assert backfilled["extensions"]["keep"] is True
    assert datetime.fromisoformat(
        backfilled["extensions"]["_migration_0027"]["previous_updated_at"]
    ) == datetime(2026, 1, 2, tzinfo=UTC)
    assert rows["event_notifypublish1"]["extensions"] == {"keep": True}
    assert rows["event_workpending01"]["status"] == "pending"

    with psycopg.connect(outbox_postgres.super_dsn) as connection:
        connection.execute(
            "SELECT set_config('app.actor_id', %s, true), "
            "set_config('app.workspace_id', %s, true)",
            (USER_ID, WORKSPACE_ID),
        )
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                """
                INSERT INTO agent.outbox_events (
                    id, workspace_id, resource_owner_id, aggregate_type, aggregate_id,
                    subject_revision, event_type, sequence, occurred_at, payload,
                    replay_expires_at, status, created_at, updated_at, revision, extensions
                ) VALUES (
                    'event_unknownafter1', %s, %s, 'job', 'job_unknownafter01', 1,
                    'plugin.magic', 0, now(), '{}'::jsonb, now() + interval '1 day',
                    'pending', now(), now(), 1, '{}'::jsonb
                )
                """,
                (WORKSPACE_ID, USER_ID),
            )
        connection.rollback()

    outbox_postgres.psql(_TERMINAL_FIXTURE)
    outbox_postgres.psql(_EXHAUSTED_CRASH_FIXTURE)
    replacement_token_hash = "b" * 64
    with psycopg.connect(outbox_postgres.app_dsn, row_factory=dict_row) as connection:
        recovered = connection.execute(
            "SELECT * FROM agent.claim_outbox_events(%s, now(), 30, 10, 3, %s)",
            (replacement_token_hash, ["resume.job_created"]),
        ).fetchall()
        assert [dict(row)["event_id"] for row in recovered] == ["event_exhaustedcrash"]
        assert dict(recovered[0])["attempt_count"] == 3
        finalized = connection.execute(
            "SELECT agent.retry_outbox_event(%s, %s, %s, now(), 3)",
            (
                "event_exhaustedcrash",
                replacement_token_hash,
                "resume.worker_exhausted",
            ),
        ).fetchone()
        assert finalized is not None
        assert dict(finalized)["retry_outbox_event"] is True
        connection.commit()
    assert outbox_postgres.rows(
        "SELECT status, attempt_count FROM agent.outbox_events "
        "WHERE id = 'event_exhaustedcrash'"
    ) == [{"status": "failed", "attempt_count": 3}]

    database = AsyncDatabase(
        AsyncDatabaseOptions(outbox_postgres.app_dsn, pool_size=1, max_overflow=0)
    )
    try:
        result = await V2MaintenanceService(
            PostgresMaintenanceRepository(database),
            batch_sizes=MaintenanceBatchSizes(
                invitations=10,
                idempotency_receipts=10,
                outbox_events=10,
            ),
            clock=lambda: datetime.now(UTC),
        ).run_once()
    finally:
        await database.aclose()
    assert result.purged_outbox_events == 2
    survivors = {
        row["id"]
        for row in outbox_postgres.rows(
            "SELECT id FROM agent.outbox_events ORDER BY id"
        )
    }
    assert "event_expiredpublish" not in survivors
    assert "event_expiredfailed1" not in survivors
    assert "event_freshpublish01" in survivors
    assert "event_workpending01" in survivors

    configuration = _migration_config(outbox_postgres.migration_dsn)
    await asyncio.to_thread(command.downgrade, configuration, "20260723_0026")
    restored = outbox_postgres.rows(
        "SELECT status, published_at, updated_at, extensions "
        "FROM agent.outbox_events WHERE id = 'event_notifypending1'"
    )[0]
    assert restored == {
        "status": "pending",
        "published_at": None,
        "updated_at": datetime(2026, 1, 2, tzinfo=UTC),
        "extensions": {"keep": True},
    }
