"""@brief Knowledge external-security 与 outbox lease 的真实 PostgreSQL 回归 / Real-PostgreSQL regressions for Knowledge external security and outbox leases."""

from __future__ import annotations

import asyncio
import getpass
import hashlib
import shutil
import socket
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg.rows import dict_row

from backend.application.ports.knowledge_worker import (
    PreparedEmbeddingSpace,
    PreparedKnowledgeChunk,
    PreparedKnowledgeIndex,
)
from backend.application.ports.v2_idempotency import IdempotencyPreparationId
from backend.domain.connections import (
    ConnectionAuthorizationFlow,
    ConnectionId,
    ConnectionOwnership,
    ConnectionProvider,
    ProviderSessionReference,
    SecretValue,
)
from backend.domain.knowledge import (
    KnowledgeContentType,
    KnowledgeDocumentPart,
    ParsedKnowledgeDocument,
)
from backend.domain.platform import ApiEventId, JobId
from backend.domain.principals import UserId, WorkspaceId
from backend.infrastructure.knowledge_connections import (
    ConnectionSecretKeyring,
    ConnectionVaultKey,
    PostgresConnectionCredentialVault,
    ProviderSessionSecret,
)
from backend.infrastructure.knowledge_worker import PostgresKnowledgeWorkerStore
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

WORKSPACE_ID = "workspace_external01"
"""@brief 外部安全测试 Workspace / Workspace used by external-security tests."""

USER_ID = "user_external0001"
"""@brief 外部安全测试 actor / Actor used by external-security tests."""

_BASE_FIXTURE_SQL = f"""
BEGIN;
SELECT set_config('app.actor_id', '{USER_ID}', true),
       set_config('app.workspace_id', '{WORKSPACE_ID}', true);
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    '{USER_ID}', 'external-security-subject', 'External Owner',
    'external-security@example.com', true, 'external-security@example.com',
    'en', 'active', '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z', 1, '{{}}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    '{WORKSPACE_ID}', '{USER_ID}', 'External Security', 'en',
    'external-security', 'team', 'global', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{{}}'
);
INSERT INTO agent.outbox_events (
    id, workspace_id, resource_owner_id, aggregate_type, aggregate_id,
    subject_revision, event_type, sequence, occurred_at, payload, trace_id,
    replay_expires_at, status, published_at, attempt_count,
    created_at, updated_at, revision, extensions
) VALUES (
    'event_stranded001', '{WORKSPACE_ID}', '{USER_ID}', 'knowledge_source',
    'source_stranded01', 1, 'knowledge_source.job_created', 999,
    '2026-07-02T00:00:00Z', '{{"job_id":"job_stranded001"}}', NULL,
    '2027-07-02T00:00:00Z', 'processing', NULL, 0,
    '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', 1, '{{}}'
);
COMMIT;
"""
"""@brief 0022 状态的身份、Workspace 与 stranded outbox fixture / Identity, Workspace, and stranded-outbox fixture at revision 0022."""


@dataclass(frozen=True, slots=True)
class _PostgresHarness:
    """@brief 隔离 PostgreSQL 迁移测试环境 / Isolated PostgreSQL migration harness."""

    port: int
    socket_dir: Path
    superuser: str
    psql_binary: Path

    @property
    def migration_dsn(self) -> str:
        """@brief 返回 migrator asyncpg DSN / Return the migrator asyncpg DSN."""

        return f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/aiws"

    @property
    def app_dsn(self) -> str:
        """@brief 返回 app DSN / Return the app DSN."""

        return f"postgresql://aiws_app@127.0.0.1:{self.port}/aiws"

    @property
    def app_async_dsn(self) -> str:
        """@brief 返回 SQLAlchemy asyncpg app DSN / Return the SQLAlchemy asyncpg app DSN."""

        return f"postgresql+asyncpg://aiws_app@127.0.0.1:{self.port}/aiws"

    @property
    def superuser_dsn(self) -> str:
        """@brief 返回 cluster superuser DSN / Return the cluster-superuser DSN."""

        return f"postgresql://{self.superuser}@127.0.0.1:{self.port}/aiws"

    def psql(self, sql: str, *, database: str = "aiws") -> None:
        """@brief 用 socket 执行 fixture SQL / Execute fixture SQL through the socket."""

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

    def execute(self, statement: str) -> None:
        """@brief 以 superuser 提交一段 SQL / Commit one SQL statement as the superuser."""

        with psycopg.connect(self.superuser_dsn) as connection:
            connection.execute(statement)

    def rows(self, statement: str) -> list[dict[str, Any]]:
        """@brief 以 superuser 读取验证行 / Read verification rows as the superuser."""

        with psycopg.connect(self.superuser_dsn, row_factory=dict_row) as connection:
            return [dict(row) for row in connection.execute(statement).fetchall()]


def _postgres_binary(name: str) -> Path | None:
    """@brief 定位 PostgreSQL binary / Locate a PostgreSQL binary."""

    direct = shutil.which(name)
    if direct is not None:
        return Path(direct)
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return candidates[0] if candidates else None


def _migration_config(dsn: str) -> Config:
    """@brief 构建显式 Alembic 配置 / Build an explicit Alembic configuration."""

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
def external_postgres(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[_PostgresHarness]:
    """@brief 启动临时 PostgreSQL 并执行 0022→0024 非空迁移 / Start PostgreSQL and execute a non-empty 0022-to-0024 migration."""

    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("knowledge-external-postgres")
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
        command.upgrade(configuration, "20260723_0022")
        harness.psql(_BASE_FIXTURE_SQL)
        command.upgrade(configuration, "20260723_0024")
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


async def _insert_pending_events(connection: asyncpg.Connection[Any], *, count: int) -> None:
    """@brief 通过 app RLS 插入待投递事件 / Insert pending events through app RLS."""

    async with connection.transaction():
        await connection.execute(
            "SELECT set_config('app.actor_id', $1, true), set_config('app.workspace_id', $2, true)",
            USER_ID,
            WORKSPACE_ID,
        )
        for index in range(count):
            await connection.execute(
                """
                INSERT INTO agent.outbox_events (
                    id, workspace_id, resource_owner_id, aggregate_type, aggregate_id,
                    subject_revision, event_type, sequence, occurred_at, payload, trace_id,
                    replay_expires_at, status, created_at, updated_at, revision, extensions
                ) VALUES (
                    $1, $2, $3, 'knowledge_source', $4, 1,
                    'knowledge_source.job_created', 999, now(), $5::jsonb, NULL,
                    now() + interval '30 days', 'pending', now(), now(), 1, '{}'::jsonb
                )
                """,
                f"event_dispatch{index:04d}",
                WORKSPACE_ID,
                USER_ID,
                f"source_dispatch{index:04d}",
                f'{{"job_id":"job_dispatch{index:04d}"}}',
            )


async def _claim(
    dsn: str,
    token_hash: str,
    *,
    batch_size: int,
) -> list[asyncpg.Record]:
    """@brief 在独立连接 claim 一批 outbox 事件 / Claim an outbox batch on an independent connection."""

    connection = await asyncpg.connect(dsn)
    try:
        return list(
            await connection.fetch(
                "SELECT * FROM agent.claim_outbox_events($1, now(), 30, $2, 3)",
                token_hash,
                batch_size,
            )
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_0023_real_postgres_recovers_claims_and_token_cas(
    external_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 PG 验证 backfill、并发 claim、过期回收与 CAS / Real PG verifies backfill, concurrent claims, expiry recovery, and CAS."""

    stranded = external_postgres.rows(
        "SELECT status, attempt_count, lease_token_hash, lease_expires_at, next_attempt_at "
        "FROM agent.outbox_events WHERE id = 'event_stranded001'"
    )
    assert stranded == [
        {
            "status": "pending",
            "attempt_count": 0,
            "lease_token_hash": None,
            "lease_expires_at": None,
            "next_attempt_at": datetime(2026, 7, 2, tzinfo=UTC),
        }
    ]

    insert_connection = await asyncpg.connect(external_postgres.app_dsn)
    try:
        await _insert_pending_events(insert_connection, count=8)
    finally:
        await insert_connection.close()

    first_hash = "a" * 64
    second_hash = "b" * 64
    first, second = await asyncio.gather(
        _claim(external_postgres.app_dsn, first_hash, batch_size=4),
        _claim(external_postgres.app_dsn, second_hash, batch_size=4),
    )
    first_ids = {str(row["event_id"]) for row in first}
    second_ids = {str(row["event_id"]) for row in second}
    assert len(first_ids) == len(second_ids) == 4
    assert first_ids.isdisjoint(second_ids)

    transition = await asyncpg.connect(external_postgres.app_dsn)
    try:
        renewable = str(first[0]["event_id"])
        assert not await transition.fetchval(
            "SELECT agent.complete_outbox_event($1, $2, now())",
            renewable,
            "f" * 64,
        )
        assert await transition.fetchval(
            "SELECT agent.renew_outbox_event_lease($1, $2, now(), 30)",
            renewable,
            first_hash,
        )
        assert await transition.fetchval(
            "SELECT agent.complete_outbox_event($1, $2, now())",
            renewable,
            first_hash,
        )

        exhausted = str(second[0]["event_id"])
        assert await transition.fetchval(
            "SELECT agent.retry_outbox_event($1, $2, 'provider.unavailable', "
            "now() + interval '1 minute', 1)",
            exhausted,
            second_hash,
        )

        expired = str(first[1]["event_id"])
        external_postgres.execute(
            "UPDATE agent.outbox_events SET lease_expires_at = now() - interval '1 second' "
            f"WHERE id = '{expired}'"
        )
        replacement_hash = "c" * 64
        reclaimed = await _claim(
            external_postgres.app_dsn,
            replacement_hash,
            batch_size=1,
        )
        assert [str(row["event_id"]) for row in reclaimed] == [expired]
        assert int(reclaimed[0]["attempt_count"]) == 2
        assert not await transition.fetchval(
            "SELECT agent.complete_outbox_event($1, $2, now())",
            expired,
            first_hash,
        )
        assert await transition.fetchval(
            "SELECT agent.complete_outbox_event($1, $2, now())",
            expired,
            replacement_hash,
        )
    finally:
        await transition.close()

    states = external_postgres.rows(
        "SELECT id, status, lease_token_hash, lease_expires_at, last_error_code "
        "FROM agent.outbox_events WHERE id IN "
        f"('{renewable}', '{exhausted}', '{expired}') ORDER BY id"
    )
    assert {row["status"] for row in states} == {"published", "failed"}
    failed = next(row for row in states if row["id"] == exhausted)
    assert failed["last_error_code"] == "provider.unavailable"
    assert all(row["lease_token_hash"] is None for row in states)
    assert all(row["lease_expires_at"] is None for row in states)


@pytest.mark.asyncio
async def test_0024_quota_function_serializes_workspace_reservations(
    external_postgres: _PostgresHarness,
) -> None:
    """@brief 同 Workspace 并发 reservation 不得越过总配额 / Concurrent same-Workspace reservations cannot exceed quota."""

    async def reserve(upload_id: str, operation_id: str) -> bool:
        """@brief 在独立事务保留 60 bytes / Reserve 60 bytes in an independent transaction."""

        connection = await asyncpg.connect(external_postgres.app_dsn)
        try:
            return bool(
                await connection.fetchval(
                    "SELECT knowledge.reserve_upload_quota($1, $2, $3, 60, 100)",
                    WORKSPACE_ID,
                    upload_id,
                    operation_id,
                )
            )
        finally:
            await connection.close()

    outcomes = await asyncio.gather(
        reserve("upload_quota0001", "operation-quota-1"),
        reserve("upload_quota0002", "operation-quota-2"),
    )
    assert sorted(outcomes) == [False, True]
    reservations = external_postgres.rows(
        "SELECT upload_id, operation_id, size_bytes "
        "FROM knowledge.upload_quota_reservations "
        f"WHERE workspace_id = '{WORKSPACE_ID}'"
    )
    assert len(reservations) == 1
    replay = await reserve(
        str(reservations[0]["upload_id"]),
        str(reservations[0]["operation_id"]),
    )
    assert replay


@pytest.mark.asyncio
async def test_0024_vault_enforces_actor_scope_and_erases_all_private_material(
    external_postgres: _PostgresHarness,
) -> None:
    """@brief vault 精确隔离 actor 并分批擦除 credential/session / Vault scopes exact actors and batch-erases credentials and sessions."""

    database = AsyncDatabase(
        AsyncDatabaseOptions(
            dsn=external_postgres.app_async_dsn,
            pool_size=2,
            max_overflow=0,
        )
    )
    with pytest.raises(ValueError, match="must not reuse AES key material"):
        PostgresConnectionCredentialVault(
            database,
            ConnectionSecretKeyring(
                "session-v1",
                (ConnectionVaultKey("session-v1", b"x" * 32),),
            ),
            ConnectionSecretKeyring(
                "credential-v1",
                (ConnectionVaultKey("credential-v1", b"x" * 32),),
            ),
            fingerprint_key=b"f" * 32,
            reference_key=b"r" * 32,
        )
    vault = PostgresConnectionCredentialVault(
        database,
        ConnectionSecretKeyring(
            "session-v1",
            (ConnectionVaultKey("session-v1", b"s" * 32),),
        ),
        ConnectionSecretKeyring(
            "credential-v1",
            (ConnectionVaultKey("credential-v1", b"k" * 32),),
        ),
        fingerprint_key=b"f" * 32,
        reference_key=b"r" * 32,
    )
    ownership = ConnectionOwnership(WorkspaceId(WORKSPACE_ID), UserId(USER_ID))
    provider = ConnectionProvider("test_provider")
    now = datetime.now(UTC)
    try:
        for index in range(2):
            await vault.save_provider_session(
                ProviderSessionSecret(
                    ProviderSessionReference(f"provider_session_{index:08d}"),
                    ownership,
                    provider,
                    ConnectionAuthorizationFlow.BROWSER_REDIRECT,
                    now + timedelta(minutes=10),
                    {"code_verifier": f"verifier-{index}"},
                ),
                state=SecretValue(f"state-{index}"),
            )
            await vault.stage_api_token(
                ownership,
                ConnectionId(f"connection_{index:08d}"),
                provider,
                SecretValue(f"plain-secret-{index}"),
                ("files.read",),
                operation_id=IdempotencyPreparationId(f"prepare-{index}"),
                validated_at=now,
            )

        key_domains = external_postgres.rows(
            "SELECT 'credential' AS kind, key_id FROM knowledge.connection_credentials "
            f"WHERE workspace_id = '{WORKSPACE_ID}' UNION ALL "
            "SELECT 'provider_session' AS kind, key_id "
            "FROM knowledge.connection_provider_sessions "
            f"WHERE workspace_id = '{WORKSPACE_ID}' ORDER BY kind, key_id"
        )
        assert key_domains == [
            {"kind": "credential", "key_id": "credential-v1"},
            {"kind": "credential", "key_id": "credential-v1"},
            {"kind": "provider_session", "key_id": "session-v1"},
            {"kind": "provider_session", "key_id": "session-v1"},
        ]

        scoped = await asyncpg.connect(external_postgres.app_dsn)
        try:
            async with scoped.transaction():
                await scoped.execute(
                    "SELECT set_config('app.actor_id', $1, true), "
                    "set_config('app.workspace_id', $2, true)",
                    "user_someone_else",
                    WORKSPACE_ID,
                )
                assert (
                    await scoped.fetchval("SELECT count(*) FROM knowledge.connection_credentials")
                    == 0
                )
                assert (
                    await scoped.fetchval(
                        "SELECT count(*) FROM knowledge.connection_provider_sessions"
                    )
                    == 0
                )
        finally:
            await scoped.close()

        encrypted = external_postgres.rows(
            "SELECT ciphertext FROM knowledge.connection_credentials ORDER BY reference"
        )
        assert len(encrypted) == 2
        assert all(row["ciphertext"] is not None for row in encrypted)
        assert b"plain-secret" not in b"".join(bytes(row["ciphertext"]) for row in encrypted)

        credentials = await vault.erase_created_by(ownership, limit=2)
        assert credentials.credentials_cleared == 2
        assert credentials.provider_sessions_cleared == 0
        assert credentials.has_more

        sessions = await vault.erase_created_by(ownership, limit=2)
        assert sessions.credentials_cleared == 0
        assert sessions.provider_sessions_cleared == 2
        assert not sessions.has_more

        replay = await vault.erase_created_by(ownership, limit=2)
        assert replay.credentials_cleared == replay.provider_sessions_cleared == 0
        assert not replay.has_more
    finally:
        await database.aclose()

    assert external_postgres.rows(
        "SELECT count(*) AS count FROM knowledge.connection_credentials "
        "WHERE status <> 'revoked' OR key_id IS NOT NULL OR nonce IS NOT NULL "
        "OR ciphertext IS NOT NULL"
    ) == [{"count": 0}]
    assert external_postgres.rows(
        "SELECT count(*) AS count FROM knowledge.connection_provider_sessions "
        "WHERE status = 'pending' OR key_id IS NOT NULL OR nonce IS NOT NULL "
        "OR ciphertext IS NOT NULL"
    ) == [{"count": 0}]


def test_0024_generated_lexical_index_covers_existing_chunk_shape(
    external_postgres: _PostgresHarness,
) -> None:
    """@brief generated tsvector 与 GIN index 可真实检索正文 / Generated tsvector and GIN index retrieve real chunk text."""

    external_postgres.execute(
        f"""
        INSERT INTO knowledge.sources (
            id, workspace_id, resource_owner_id, source_type, title, config,
            source_input, public_config, enabled, current_policy_version,
            version_counter, revision_mode, ingestion_state, document_count,
            chunk_count, created_at, updated_at, revision, extensions
        ) VALUES (
            'source_lexical001', '{WORKSPACE_ID}', '{USER_ID}', 'manual_note',
            'Lexical source', '{{}}'::jsonb, '{{"text":"source"}}'::jsonb,
            '{{}}'::jsonb, true, 1, 0, 'latest', 'not_started', 0, 0,
            now(), now(), 1, '{{}}'::jsonb
        );
        INSERT INTO knowledge.source_versions (
            id, workspace_id, resource_owner_id, source_id, version_no,
            content_hash, content_sha256, size_bytes, status, artifact_type,
            artifact_id, artifact_revision, origin, parser_metadata, indexed_at,
            created_at, updated_at, revision, extensions
        ) VALUES (
            'version_lexical01', '{WORKSPACE_ID}', '{USER_ID}',
            'source_lexical001', 1, repeat('a', 64), repeat('a', 64), 29,
            'ready', 'knowledge_snapshot', 'artifact_lexical01', 1,
            '{{}}'::jsonb, '{{}}'::jsonb, now(), now(), now(), 1, '{{}}'::jsonb
        );
        INSERT INTO knowledge.chunks (
            id, workspace_id, resource_owner_id, source_version_id, ordinal,
            text_content, content_hash, origin, token_count,
            created_at, updated_at, revision, extensions
        ) VALUES (
            'chunk_lexical0001', '{WORKSPACE_ID}', '{USER_ID}',
            'version_lexical01', 0, 'distributed systems consensus', repeat('b', 64),
            '{{}}'::jsonb, 3, now(), now(), 1, '{{}}'::jsonb
        );
        UPDATE knowledge.sources
        SET current_version_id = 'version_lexical01', version_counter = 1,
            ingestion_state = 'ready', document_count = 1, chunk_count = 1,
            last_success_at = now(), updated_at = now(), revision = 2
        WHERE id = 'source_lexical001';
        """
    )
    assert external_postgres.rows(
        "SELECT search_vector @@ plainto_tsquery('simple', 'consensus') AS matches "
        "FROM knowledge.chunks WHERE id = 'chunk_lexical0001'"
    ) == [{"matches": True}]
    assert external_postgres.rows(
        "SELECT indexname FROM pg_indexes "
        "WHERE schemaname = 'knowledge' "
        "AND indexname = 'ix_knowledge_chunks_search_vector_gin'"
    ) == [{"indexname": "ix_knowledge_chunks_search_vector_gin"}]


@pytest.mark.asyncio
async def test_knowledge_worker_commits_index_and_privacy_deletion_atomically(
    external_postgres: _PostgresHarness,
) -> None:
    """@brief durable worker 真实提交 index、Job、journals 与隐私删除 / Durable worker really commits index, Job, journals, and privacy deletion."""

    source_id = "source_worker0001"
    ingest_job_id = "job_worker_ingest01"
    external_postgres.execute(
        f"""
        INSERT INTO knowledge.sources (
            id, workspace_id, resource_owner_id, source_type, title, config,
            source_input, public_config, enabled, current_policy_version,
            version_counter, revision_mode, ingestion_state, document_count,
            chunk_count, created_at, updated_at, revision, extensions
        ) VALUES (
            '{source_id}', '{WORKSPACE_ID}', '{USER_ID}', 'manual_note',
            'Worker source', '{{}}'::jsonb,
            '{{"source_type":"manual_note","content":"durable worker content"}}'::jsonb,
            '{{}}'::jsonb, true, 1, 0, 'latest', 'queued', 0, 0,
            now(), now(), 1, '{{}}'::jsonb
        );
        INSERT INTO knowledge.visibility_policies (
            id, workspace_id, resource_owner_id, source_id, policy_version,
            default_effect, sensitivity, session_override_allowed,
            allow_external_model_processing, allowed_model_regions, retention_days,
            created_at, updated_at, revision, extensions
        ) VALUES (
            'policy_worker0001', '{WORKSPACE_ID}', '{USER_ID}', '{source_id}', 1,
            'allow', 'normal', false, false, ARRAY['global']::varchar[], 365,
            now(), now(), 1, '{{}}'::jsonb
        );
        INSERT INTO agent.jobs (
            id, workspace_id, resource_owner_id, job_type, status, phase,
            completed_units, progress_unit, target_resource_type, target_resource_id,
            target_resource_revision, result_refs, request_payload,
            created_at, updated_at, revision, extensions
        ) VALUES (
            '{ingest_job_id}', '{WORKSPACE_ID}', '{USER_ID}', 'knowledge.ingest',
            'queued', 'queued', 0, 'unknown', 'knowledge_source', '{source_id}', 1,
            '[]'::jsonb,
            jsonb_build_object('spec', jsonb_build_object(
                'source_id', '{source_id}', 'source_revision', 1,
                'version_id', NULL, 'force', false, 'requested_by', '{USER_ID}'
            )), now(), now(), 1, '{{}}'::jsonb
        );
        """
    )
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            dsn=external_postgres.app_async_dsn,
            pool_size=2,
            max_overflow=0,
        )
    )
    store = PostgresKnowledgeWorkerStore(database)
    material = b"durable worker content"
    unit_embedding = (1.0, *(0.0 for _ in range(1_023)))
    prepared = PreparedKnowledgeIndex(
        hashlib.sha256(material).hexdigest(),
        len(material),
        ParsedKnowledgeDocument(
            (
                KnowledgeDocumentPart(
                    material.decode(),
                    KnowledgeContentType.GENERAL,
                    {"path": "manual/1"},
                ),
            ),
            {"parser": "test"},
        ),
        (
            PreparedKnowledgeChunk(
                0,
                material.decode(),
                "manual/1#chunk=0",
                KnowledgeContentType.GENERAL.value,
                unit_embedding,
            ),
        ),
        PreparedEmbeddingSpace(
            "embsp_worker0001",
            "test_provider",
            "test-model",
            "2026-07-23",
            1_024,
            "cosine",
            "l2",
        ),
    )
    try:
        claim = await store.claim(
            WorkspaceId(WORKSPACE_ID),
            UserId(USER_ID),
            ApiEventId("event_worker_ingest1"),
            JobId(ingest_job_id),
        )
        assert claim is not None
        version_id = await store.complete_processing(claim, prepared)
        assert await store.complete_processing(claim, prepared) == version_id

        indexed = external_postgres.rows(
            "SELECT source.ingestion_state, source.revision AS source_revision, "
            "source.chunk_count, job.status AS job_status, job.revision AS job_revision, "
            "job.percent, job.result ->> 'source_version_id' AS result_version "
            "FROM knowledge.sources AS source JOIN agent.jobs AS job "
            f"ON job.id = '{ingest_job_id}' WHERE source.id = '{source_id}'"
        )
        assert indexed == [
            {
                "ingestion_state": "ready",
                "source_revision": 3,
                "chunk_count": 1,
                "job_status": "succeeded",
                "job_revision": 3,
                "percent": 100.0,
                "result_version": str(version_id),
            }
        ]
        assert external_postgres.rows(
            "SELECT count(*) AS count FROM knowledge.chunks "
            f"WHERE source_version_id = '{version_id}'"
        ) == [{"count": 1}]
        assert external_postgres.rows("SELECT count(*) AS count FROM knowledge.embeddings") == [
            {"count": 1}
        ]
        assert external_postgres.rows(
            "SELECT updated_at >= created_at AS timestamps_valid "
            "FROM knowledge.source_versions "
            f"WHERE id = '{version_id}'"
        ) == [{"timestamps_valid": True}]
        assert external_postgres.rows(
            "SELECT count(*) AS count FROM identity.audit_events "
            f"WHERE resource_id = '{source_id}' AND action = 'knowledge.index.completed'"
        ) == [{"count": 1}]

        delete_job_id = "job_worker_delete01"
        external_postgres.execute(
            f"""
            UPDATE knowledge.sources
            SET ingestion_state = 'deleting', enabled = false,
                updated_at = now(), revision = revision + 1
            WHERE id = '{source_id}';
            INSERT INTO agent.jobs (
                id, workspace_id, resource_owner_id, job_type, status, phase,
                completed_units, progress_unit, target_resource_type, target_resource_id,
                target_resource_revision, result_refs, request_payload,
                created_at, updated_at, revision, extensions
            ) VALUES (
                '{delete_job_id}', '{WORKSPACE_ID}', '{USER_ID}', 'knowledge.delete',
                'queued', 'queued', 0, 'unknown', 'knowledge_source', '{source_id}', 4,
                '[]'::jsonb,
                jsonb_build_object('spec', jsonb_build_object(
                    'source_id', '{source_id}', 'source_revision', 4
                )), now(), now(), 1, '{{}}'::jsonb
            );
            """
        )
        delete_claim = await store.claim(
            WorkspaceId(WORKSPACE_ID),
            UserId(USER_ID),
            ApiEventId("event_worker_delete1"),
            JobId(delete_job_id),
        )
        assert delete_claim is not None
        await store.complete_source_deletion(delete_claim)
        await store.complete_source_deletion(delete_claim)
    finally:
        await database.aclose()

    deleted = external_postgres.rows(
        "SELECT ingestion_state, enabled, document_count, chunk_count, deleted_at IS NOT NULL "
        "AS has_deleted_at, source_input ->> 'content' AS content, public_config "
        f"FROM knowledge.sources WHERE id = '{source_id}'"
    )
    assert deleted == [
        {
            "ingestion_state": "deleted",
            "enabled": False,
            "document_count": 0,
            "chunk_count": 0,
            "has_deleted_at": True,
            "content": "[deleted]",
            "public_config": {},
        }
    ]
    assert external_postgres.rows(
        f"SELECT count(*) AS count FROM knowledge.chunks WHERE source_version_id = '{version_id}'"
    ) == [{"count": 0}]
    assert external_postgres.rows(
        f"SELECT origin, parser_metadata FROM knowledge.source_versions WHERE id = '{version_id}'"
    ) == [{"origin": {}, "parser_metadata": {}}]


@pytest.mark.asyncio
async def test_knowledge_exhaustion_ignores_corrupt_specs_and_closes_source_job_atomically(
    external_postgres: _PostgresHarness,
) -> None:
    """@brief claim 前 spec 损坏仍由可信 Job target 原子闭合 / A corrupt pre-claim spec is still closed atomically from the trusted Job target."""
    source_id = "source_exhausted001"
    job_id = "job_exhaustedknow01"
    event_id = "event_exhaustedknow1"
    external_postgres.execute(
        f"""
        INSERT INTO knowledge.sources (
            id, workspace_id, resource_owner_id, source_type, title, config,
            source_input, public_config, enabled, current_policy_version,
            version_counter, revision_mode, ingestion_state, document_count,
            chunk_count, created_at, updated_at, revision, extensions
        ) VALUES (
            '{source_id}', '{WORKSPACE_ID}', '{USER_ID}', 'manual_note',
            'Exhausted source', '{{}}'::jsonb,
            '{{"source_type":"manual_note","content":"private text"}}'::jsonb,
            '{{}}'::jsonb, true, 1, 0, 'latest', 'queued', 0, 0,
            now(), now(), 1, '{{}}'::jsonb
        );
        INSERT INTO agent.jobs (
            id, workspace_id, resource_owner_id, job_type, status, phase,
            completed_units, progress_unit, target_resource_type, target_resource_id,
            target_resource_revision, result_refs, request_payload,
            created_at, updated_at, revision, extensions
        ) VALUES (
            '{job_id}', '{WORKSPACE_ID}', '{USER_ID}', 'knowledge.ingest',
            'queued', 'queued', 0, 'unknown', 'knowledge_source', '{source_id}', 1,
            '[]'::jsonb, '{{}}'::jsonb, now(), now(), 1, '{{}}'::jsonb
        );
        """
    )
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            dsn=external_postgres.app_async_dsn,
            pool_size=2,
            max_overflow=0,
        )
    )
    store = PostgresKnowledgeWorkerStore(database)
    try:
        await store.fail_exhausted(
            WorkspaceId(WORKSPACE_ID),
            UserId(USER_ID),
            ApiEventId(event_id),
            "knowledge_source.job_created",
            JobId(job_id),
        )
        await store.fail_exhausted(
            WorkspaceId(WORKSPACE_ID),
            UserId(USER_ID),
            ApiEventId(event_id),
            "knowledge_source.job_created",
            JobId(job_id),
        )
    finally:
        await database.aclose()

    assert external_postgres.rows(
        "SELECT source.ingestion_state, source.last_problem ->> 'code' AS source_code, "
        "job.status, job.problem ->> 'code' AS job_code "
        "FROM knowledge.sources AS source JOIN agent.jobs AS job "
        f"ON job.id = '{job_id}' WHERE source.id = '{source_id}'"
    ) == [
        {
            "ingestion_state": "failed",
            "source_code": "knowledge.worker_attempts_exhausted",
            "status": "failed",
            "job_code": "knowledge.worker_attempts_exhausted",
        }
    ]
