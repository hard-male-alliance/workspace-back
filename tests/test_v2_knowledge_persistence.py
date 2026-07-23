"""@brief API V2 Knowledge persistence、加密与非空迁移回归 / API V2 Knowledge persistence, encryption, and non-empty migration regressions."""

from __future__ import annotations

import getpass
import shutil
import socket
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg.rows import dict_row

from backend.domain.connections import (
    ConnectionAuthorizationFlow,
    ConnectionAuthorizationIdempotency,
    ConnectionAuthorizationRecord,
    ConnectionAuthorizationSession,
    ConnectionAuthorizationSessionId,
    ConnectionAuthorizationState,
    ConnectionOwnership,
    ConnectionProvider,
    ProviderSessionReference,
)
from backend.domain.knowledge_sources import KnowledgeSourceId
from backend.domain.principals import (
    ClientId,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.upload_sessions import (
    UploadCompletionClaim,
    UploadDeclaration,
    UploadGrant,
    UploadSession,
    UploadSessionId,
    UploadVerificationId,
)
from backend.infrastructure.knowledge import (
    AesGcmAuthorizationLaunchCipher,
    PostgresKnowledgeUnitOfWorkFactory,
)
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions
from backend.infrastructure.persistence.models import (
    ConnectionAuthorizationRecordModel,
    ConnectionRecord,
    KnowledgeUploadSessionRecord,
    ResumeImportUploadSessionRecord,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260723_0019_v2_knowledge_persistence.py"
"""@brief 0019 migration 文件 / 0019 migration file."""

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 固定测试时刻 / Fixed test instant."""

_LEGACY_FIXTURE_SQL = r"""
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    'user_legacy0001', 'legacy-subject', 'Legacy Owner', 'legacy@example.com',
    true, 'legacy@example.com', 'en', 'active', '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 3, '{"legacy_user":true}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    'workspace_legacy0001', 'user_legacy0001', 'Legacy Workspace', 'en',
    'legacy-workspace', 'team', 'global', '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 2, '{"legacy_workspace":true}'
);
INSERT INTO identity.workspace_members (
    id, workspace_id, resource_owner_id, user_id, display_name, role, status,
    joined_at, created_at, updated_at, revision, extensions
) VALUES (
    'membership_legacy01', 'workspace_legacy0001', 'user_legacy0001',
    'user_legacy0001', 'Legacy Owner', 'owner', 'active',
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 1, '{}'
);
INSERT INTO agent.jobs (
    id, workspace_id, resource_owner_id, job_type, status, phase,
    completed_units, target_resource_type, target_resource_id,
    request_payload, created_at, updated_at, revision, extensions
) VALUES (
    'job_legacyimport1', 'workspace_legacy0001', 'user_legacy0001',
    'resume.import', 'queued', 'queued', 0, 'resume', 'resume_legacy0001', '{}',
    '2026-07-05T00:00:00Z', '2026-07-05T00:00:00Z', 1, '{"keep":"job"}'
);
INSERT INTO resume.import_upload_sessions (
    id, workspace_id, status, completed_at, expires_at, claimed_by_job_id,
    consumed_at, created_at, updated_at, revision, extensions
) VALUES (
    'upload_legacy0001', 'workspace_legacy0001', 'completed',
    '2026-07-06T00:00:00Z', '2027-07-06T00:00:00Z', 'job_legacyimport1',
    '2026-07-06T01:00:00Z', '2026-07-05T00:00:00Z',
    '2026-07-06T01:00:00Z', 4, '{"keep":"upload"}'
);
INSERT INTO knowledge.sources (
    id, workspace_id, resource_owner_id, source_type, title, config,
    revision_mode, ingestion_state, sync_schedule, deleted_at,
    created_at, updated_at, revision, extensions
) VALUES (
    'source_legacy0001', 'workspace_legacy0001', 'user_legacy0001', 'file',
    'Legacy File', '{"filename":"legacy.pdf","media_type":"application/pdf"}',
    'latest', 'ready', NULL, NULL, '2026-07-03T00:00:00Z',
    '2026-07-07T00:00:00Z', 4, '{"keep":"source"}'
);
INSERT INTO knowledge.source_versions (
    id, workspace_id, resource_owner_id, source_id, version_no, content_hash,
    origin, parser_metadata, indexed_at, created_at, updated_at, revision, extensions
) VALUES (
    'version_legacy001', 'workspace_legacy0001', 'user_legacy0001',
    'source_legacy0001', 1, repeat('a', 64), '{"size_bytes":123}',
    '{"pages":2}', '2026-07-07T00:00:00Z', '2026-07-04T00:00:00Z',
    '2026-07-07T00:00:00Z', 2, '{"keep":"version"}'
);
INSERT INTO knowledge.visibility_policies (
    id, workspace_id, resource_owner_id, source_id, policy_version,
    default_effect, sensitivity, session_override_allowed,
    allow_external_model_processing, allowed_model_regions, retention_days,
    created_at, updated_at, revision, extensions
) VALUES (
    'policy_legacy0001', 'workspace_legacy0001', 'user_legacy0001',
    'source_legacy0001', 1, 'allow', 'normal', false, false,
    ARRAY['global'], 365, '2026-07-03T00:00:00Z',
    '2026-07-03T00:00:00Z', 1, '{"keep":"policy"}'
);
INSERT INTO knowledge.visibility_grants (
    id, workspace_id, resource_owner_id, policy_id, agent_scope, effect,
    allowed_operations, created_at, updated_at, revision, extensions
) VALUES (
    'grant_legacy00001', 'workspace_legacy0001', 'user_legacy0001',
    'policy_legacy0001', 'agent.default', 'allow', ARRAY['retrieve'],
    '2026-07-03T00:00:00Z', '2026-07-03T00:00:00Z', 1, '{"keep":"grant"}'
);
INSERT INTO knowledge.chunks (
    id, workspace_id, resource_owner_id, source_version_id, ordinal,
    text_content, content_hash, origin, token_count,
    created_at, updated_at, revision, extensions
) VALUES (
    'chunk_legacy00001', 'workspace_legacy0001', 'user_legacy0001',
    'version_legacy001', 0, 'hello', repeat('b', 64), '{}', 1,
    '2026-07-05T00:00:00Z', '2026-07-05T00:00:00Z', 1, '{}'
);
"""
"""@brief 0018 状态的非空 Knowledge/Upload fixture / Non-empty Knowledge/Upload fixture at revision 0018."""


def _authorization_record(
    *,
    workspace_id: WorkspaceId | None = None,
    user_id: UserId | None = None,
    session_id: ConnectionAuthorizationSessionId | None = None,
) -> ConnectionAuthorizationRecord:
    """@brief 构造含敏感 browser URL 的授权记录 / Build an authorization record containing a sensitive browser URL."""
    workspace_id = workspace_id or WorkspaceId("workspace_00000001")
    user_id = user_id or UserId("user_00000001")
    session_id = session_id or ConnectionAuthorizationSessionId("connection_auth_00000001")
    return ConnectionAuthorizationRecord(
        ConnectionAuthorizationSession(
            session_id,
            ConnectionProvider("google_drive"),
            ConnectionAuthorizationFlow.BROWSER_REDIRECT,
            NOW + timedelta(minutes=10),
            "https://provider.example/authorize?secret=one-time",
        ),
        ConnectionOwnership(workspace_id, user_id),
        ("files.read",),
        ConnectionAuthorizationState.PENDING,
        "a" * 64,
        ProviderSessionReference("provider_session_00000001"),
        ConnectionAuthorizationIdempotency(
            "b" * 64,
            "c" * 64,
            NOW + timedelta(days=2),
        ),
        NOW,
    )


def test_authorization_launch_cipher_round_trips_and_authenticates_scope() -> None:
    """@brief AEAD 可重放但拒绝 scope/ciphertext 篡改 / AEAD supports replay while rejecting scope or ciphertext tampering."""
    record = _authorization_record()
    cipher = AesGcmAuthorizationLaunchCipher(
        {"key_2026": bytes(range(32))}, active_key_id="key_2026"
    )
    encrypted = cipher.encrypt(record)
    assert b"secret=one-time" not in encrypted.ciphertext
    restored = cipher.decrypt(
        encrypted,
        workspace_id=str(record.ownership.workspace_id),
        created_by=str(record.ownership.created_by),
        session_id=str(record.session.id),
        provider=record.session.provider.value,
        flow=record.session.flow.value,
        expires_at=record.session.expires_at,
    )
    assert restored == record.session
    with pytest.raises(ValueError, match="authentication failed"):
        cipher.decrypt(
            encrypted,
            workspace_id="workspace_attacker1",
            created_by=str(record.ownership.created_by),
            session_id=str(record.session.id),
            provider=record.session.provider.value,
            flow=record.session.flow.value,
            expires_at=record.session.expires_at,
        )
    with pytest.raises(ValueError, match="valid AES-256 keys"):
        AesGcmAuthorizationLaunchCipher({"x": bytes(32)}, active_key_id="x")


def test_knowledge_orm_has_one_upload_truth_and_no_raw_connection_secret() -> None:
    """@brief ORM 只有统一 upload table 且 Connection 不存 raw token / ORM has one upload truth and no raw Connection token."""
    assert ResumeImportUploadSessionRecord is KnowledgeUploadSessionRecord
    assert KnowledgeUploadSessionRecord.__table__.fullname == "knowledge.upload_sessions"
    assert "verification_operation_id" in KnowledgeUploadSessionRecord.__table__.c
    assert "resume.import_upload_sessions" not in {
        table.fullname for table in KnowledgeUploadSessionRecord.metadata.tables.values()
    }
    connection_columns = set(ConnectionRecord.__table__.c.keys())
    assert "credential_reference" in connection_columns
    assert not {"token", "api_token", "token_hash", "credential"} & connection_columns
    authorization_columns = set(ConnectionAuthorizationRecordModel.__table__.c.keys())
    assert {"launch_key_id", "launch_nonce", "launch_ciphertext"} <= authorization_columns
    assert not {"authorization_url", "verification_uri", "user_code"} & authorization_columns


def test_0019_is_linear_and_contains_data_safe_conversion_gates() -> None:
    """@brief 0019 线性且先搬迁验证再删除旧表 / 0019 is linear and migrates/verifies before retiring the old table."""
    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    script = scripts.get_revision("20260723_0019")
    assert script is not None
    assert script.down_revision == "20260723_0018"
    assert len(script.nextrev) <= 1
    source = MIGRATION.read_text(encoding="utf-8")
    assert source.index("INSERT INTO knowledge.upload_sessions") < source.index(
        'op.drop_table("import_upload_sessions", schema="resume")'
    )
    assert "0019 Resume upload conversion did not preserve every legacy row" in source
    assert "_verify_backfill()" in source
    assert "_backfill_upload_verification_operations()" in source
    assert "ALTER TABLE" in source and "FORCE ROW LEVEL SECURITY" in source


@dataclass(frozen=True, slots=True)
class _PostgresHarness:
    """@brief 隔离 PostgreSQL 迁移测试环境 / Isolated PostgreSQL migration harness."""

    port: int
    socket_dir: Path
    superuser: str

    @property
    def migration_dsn(self) -> str:
        """@brief 返回 migrator asyncpg DSN / Return the migrator asyncpg DSN."""
        return f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/aiws"

    @property
    def app_dsn(self) -> str:
        """@brief 返回应用 asyncpg DSN / Return the application asyncpg DSN."""
        return f"postgresql+asyncpg://aiws_app@127.0.0.1:{self.port}/aiws"

    @property
    def app_psycopg_dsn(self) -> str:
        """@brief 返回应用 psycopg DSN / Return the application psycopg DSN."""
        return f"postgresql://aiws_app@127.0.0.1:{self.port}/aiws"

    def psql(self, binary: Path, sql: str, *, database: str = "aiws") -> None:
        """@brief 用 socket 执行 fixture SQL / Execute fixture SQL through the socket."""
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
        """@brief 以 cluster superuser 读取验证行 / Read verification rows as the cluster superuser."""
        dsn = f"postgresql://{self.superuser}@127.0.0.1:{self.port}/aiws"
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
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
def knowledge_postgres(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[_PostgresHarness]:
    """@brief 启动临时 PostgreSQL 并执行 0018→0019 非空迁移 / Start PostgreSQL and execute a non-empty 0018-to-0019 migration."""
    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("knowledge-postgres")
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
            CREATE DATABASE aiws_empty OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_empty TO aiws_owner;
            """,
            database="postgres",
        )
        try:
            harness.psql(psql, "CREATE EXTENSION vector;")
            harness.psql(psql, "CREATE EXTENSION vector;", database="aiws_empty")
        except subprocess.CalledProcessError:
            pytest.skip("the PostgreSQL vector extension is unavailable")
        configuration = _migration_config(harness.migration_dsn)
        command.upgrade(configuration, "20260723_0018")
        harness.psql(psql, _LEGACY_FIXTURE_SQL)
        command.upgrade(configuration, "20260723_0019")
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


def test_0019_real_postgres_preserves_nonempty_knowledge_and_uploads(
    knowledge_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 PostgreSQL 保留非空行、引用、扩展与 claim / Real PostgreSQL preserves non-empty rows, references, extensions, and claims."""
    assert knowledge_postgres.rows("SELECT version_num FROM identity.alembic_version") == [
        {"version_num": "20260723_0019"}
    ]
    assert knowledge_postgres.rows(
        "SELECT to_regclass('resume.import_upload_sessions') AS legacy"
    ) == [{"legacy": None}]
    upload = knowledge_postgres.rows(
        """
        SELECT status, claimed_by_type, claimed_by_id, claimed_by_revision,
               claimed_by_job_id, revision, verification_operation_id,
               extensions ->> 'keep' AS kept
        FROM knowledge.upload_sessions WHERE id = 'upload_legacy0001'
        """
    )[0]
    assert upload["status"] == "completed"
    assert upload["claimed_by_type"] == "job"
    assert upload["claimed_by_id"] == "job_legacyimport1"
    assert upload["claimed_by_job_id"] == "job_legacyimport1"
    assert upload["claimed_by_revision"] == 1
    assert upload["revision"] == 4
    assert cast(str, upload["verification_operation_id"]).startswith("prep_")
    assert upload["kept"] == "upload"

    source = knowledge_postgres.rows(
        """
        SELECT source_input, public_config, current_version_id, version_counter,
               document_count, chunk_count, extensions ->> 'keep' AS kept
        FROM knowledge.sources WHERE id = 'source_legacy0001'
        """
    )[0]
    assert source["source_input"]["source_type"] == "file"
    assert source["public_config"] == {
        "filename": "legacy.pdf",
        "media_type": "application/pdf",
    }
    assert source["current_version_id"] == "version_legacy001"
    assert source["version_counter"] == 1
    assert source["document_count"] == 1
    assert source["chunk_count"] == 1
    assert source["kept"] == "source"

    version = knowledge_postgres.rows(
        """
        SELECT content_sha256, size_bytes, status, artifact_type, artifact_id,
               extensions ->> 'keep' AS kept
        FROM knowledge.source_versions WHERE id = 'version_legacy001'
        """
    )[0]
    assert version == {
        "content_sha256": "a" * 64,
        "size_bytes": 123,
        "status": "ready",
        "artifact_type": "knowledge_source_version",
        "artifact_id": "version_legacy001",
        "kept": "version",
    }
    evidence = knowledge_postgres.rows(
        """
        SELECT count(*) AS count FROM identity.api_migration_audits
        WHERE migration_id = 'api-v2-knowledge-persistence-0019'
        """
    )
    assert evidence == [{"count": 1}]


def test_0019_real_postgres_installs_workspace_rls_and_removes_owner_policy(
    knowledge_postgres: _PostgresHarness,
) -> None:
    """@brief 新表 FORCE RLS 且临时 owner policy 已移除 / New tables use FORCE RLS and temporary owner policies are gone."""
    secured = knowledge_postgres.rows(
        """
        SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'knowledge'
          AND c.relname IN ('connections', 'connection_authorization_sessions',
                            'upload_sessions', 'sources', 'source_versions',
                            'visibility_policies', 'visibility_grants')
        ORDER BY c.relname
        """
    )
    assert len(secured) == 7
    assert all(row["relrowsecurity"] and row["relforcerowsecurity"] for row in secured)
    assert knowledge_postgres.rows(
        """
        SELECT count(*) AS count FROM pg_policies
        WHERE policyname = 'knowledge_owner_migration_0019'
        """
    ) == [{"count": 0}]

    with psycopg.connect(knowledge_postgres.app_psycopg_dsn) as connection:
        with connection.transaction():
            connection.execute(
                "SELECT set_config('app.actor_id', %s, true), "
                "set_config('app.workspace_id', %s, true)",
                ("user_legacy0001", "workspace_legacy0001"),
            )
            assert connection.execute("SELECT count(*) FROM knowledge.sources").fetchone() == (1,)
        with connection.transaction():
            connection.execute(
                "SELECT set_config('app.actor_id', %s, true), "
                "set_config('app.workspace_id', %s, true)",
                ("user_legacy0001", "workspace_other0001"),
            )
            assert connection.execute("SELECT count(*) FROM knowledge.sources").fetchone() == (0,)


@pytest.mark.asyncio
async def test_real_postgres_adapter_rehydrates_sources_and_round_trips_sealed_state(
    knowledge_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 adapter 经授权读写来源、AEAD session 与统一 upload / Real adapter authorizes and round-trips sources, AEAD sessions, and unified uploads."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            knowledge_postgres.app_dsn,
            pool_size=1,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=1_000,
        )
    )
    cipher = AesGcmAuthorizationLaunchCipher(
        {"key_2026": bytes(range(32))}, active_key_id="key_2026"
    )
    factory = PostgresKnowledgeUnitOfWorkFactory(database, launch_cipher=cipher)
    workspace_id = WorkspaceId("workspace_legacy0001")
    user_id = UserId("user_legacy0001")
    principal = TokenPrincipal(
        user_id,
        Subject("legacy-subject"),
        ClientId("client_legacy0001"),
        frozenset({Scope("workspace.read"), Scope("workspace.write")}),
    )
    authorization = _authorization_record(
        workspace_id=workspace_id,
        user_id=user_id,
        session_id=ConnectionAuthorizationSessionId("connection_auth_adapter1"),
    )
    digest = "d" * 64
    upload = UploadSession.create(
        upload_id=UploadSessionId("upload_adapter0001"),
        workspace_id=workspace_id,
        declaration=UploadDeclaration("adapter.pdf", "application/pdf", 128, digest),
        grant=UploadGrant(
            "https://storage.example/upload/adapter?signature=one-time",
            {"content-type": "application/pdf"},
        ),
        created_at=NOW,
        expires_at=NOW + timedelta(hours=1),
    )
    try:
        async with factory() as unit:
            actor = await unit.authorizer.authenticate(principal)
            await unit.authorizer.authorize(
                actor, workspace_id, WorkspaceAction.READ_KNOWLEDGE_SOURCE
            )
            source = await unit.repository.get_source(
                workspace_id, KnowledgeSourceId("source_legacy0001")
            )
            assert source is not None
            assert str(source.current_version_id) == "version_legacy001"
            assert source.visibility.agent_grants[0].agent_scope == "agent.default"

        async with factory() as unit:
            actor = await unit.authorizer.authenticate(principal)
            await unit.authorizer.authorize(
                actor,
                workspace_id,
                WorkspaceAction.CREATE_CONNECTION_AUTHORIZATION_SESSION,
            )
            await unit.repository.add_authorization_record(authorization)
            await unit.commit()

        raw_authorization = knowledge_postgres.rows(
            "SELECT launch_key_id, launch_nonce, launch_ciphertext "
            "FROM knowledge.connection_authorization_sessions "
            "WHERE id = 'connection_auth_adapter1'"
        )[0]
        assert raw_authorization["launch_key_id"] == "key_2026"
        assert len(raw_authorization["launch_nonce"]) == 12
        assert b"secret=one-time" not in raw_authorization["launch_ciphertext"]

        async with factory() as unit:
            actor = await unit.authorizer.authenticate(principal)
            await unit.authorizer.authorize(
                actor,
                workspace_id,
                WorkspaceAction.CREATE_CONNECTION_AUTHORIZATION_SESSION,
            )
            replayed = await unit.repository.get_authorization_record_by_idempotency(
                workspace_id,
                user_id,
                authorization.idempotency.key_hash,
                for_update=True,
            )
            assert replayed == authorization

        async with factory() as unit:
            actor = await unit.authorizer.authenticate(principal)
            await unit.authorizer.authorize(
                actor, workspace_id, WorkspaceAction.CREATE_UPLOAD_SESSION
            )
            await unit.repository.add_upload(upload)
            await unit.commit()

        claim = UploadCompletionClaim(128, digest)
        operation_id = UploadVerificationId(f"prep_{'e' * 64}")
        async with factory() as unit:
            actor = await unit.authorizer.authenticate(principal)
            await unit.authorizer.authorize(
                actor, workspace_id, WorkspaceAction.COMPLETE_UPLOAD_SESSION
            )
            stored = await unit.repository.get_upload(workspace_id, upload.view.id, for_update=True)
            assert stored is not None
            verifying = stored.begin_completion(claim, operation_id, at=NOW + timedelta(minutes=1))
            await unit.repository.save_upload(verifying, expected_generation=stored.generation)
            await unit.commit()

        async with factory() as unit:
            actor = await unit.authorizer.authenticate(principal)
            await unit.authorizer.authorize(
                actor, workspace_id, WorkspaceAction.COMPLETE_UPLOAD_SESSION
            )
            restored = await unit.repository.get_upload(workspace_id, upload.view.id)
            assert restored is not None
            assert restored.verification_operation_id == operation_id
            assert restored.completion_claim == claim
            assert restored.generation == 2
    finally:
        await database.aclose()


def test_0019_empty_upgrade_can_safely_restore_0018_shape(
    knowledge_postgres: _PostgresHarness,
) -> None:
    """@brief 空库可回退且不伪造 migration evidence / An empty database can downgrade without fabricated migration evidence."""
    migration_dsn = (
        f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{knowledge_postgres.port}/aiws_empty"
    )
    configuration = _migration_config(migration_dsn)
    command.upgrade(configuration, "20260723_0019")

    empty_dsn = (
        f"postgresql://{knowledge_postgres.superuser}@127.0.0.1:"
        f"{knowledge_postgres.port}/aiws_empty"
    )
    with psycopg.connect(empty_dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM identity.api_migration_audits "
            "WHERE migration_id = 'api-v2-knowledge-persistence-0019'"
        ).fetchone() == (0,)

    command.downgrade(configuration, "20260723_0018")
    with psycopg.connect(empty_dsn) as connection:
        assert connection.execute(
            "SELECT version_num FROM identity.alembic_version"
        ).fetchone() == ("20260723_0018",)
        assert connection.execute(
            "SELECT to_regclass('knowledge.upload_sessions'), "
            "to_regclass('knowledge.connection_authorization_sessions'), "
            "to_regclass('resume.import_upload_sessions')"
        ).fetchone() == (None, None, "resume.import_upload_sessions")
