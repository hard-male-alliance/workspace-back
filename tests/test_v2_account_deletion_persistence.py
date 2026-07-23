"""@brief API V2 账户删除的真实 PostgreSQL 执行证据 / Real-PostgreSQL API V2 account-deletion evidence."""

from __future__ import annotations

import getpass
import shutil
import socket
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg.rows import dict_row

from backend.application.account_deletion import AccountDeletionExecutionService
from backend.domain.connections import ConnectionOwnership
from backend.domain.upload_sessions import UploadSessionId
from backend.infrastructure.account_deletion import PostgresAccountDeletionExecutionPort
from backend.infrastructure.identity_email_outbox import (
    IdentityEmailKeyring,
    PostgresIdentityEmailOutbox,
)
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260723_0025_account_deletion_execution.py"
"""@brief 本测试负责的 migration / Migration owned by this test."""


@dataclass(frozen=True, slots=True)
class _PostgresHarness:
    """@brief 隔离的账户删除 PostgreSQL 集群 / Isolated account-deletion PostgreSQL cluster.

    @param port TCP 端口 / TCP port.
    @param socket_dir Unix socket 目录 / Unix-socket directory.
    @param superuser initdb 超级用户 / initdb superuser.
    """

    port: int
    socket_dir: Path
    superuser: str

    @property
    def migration_dsn(self) -> str:
        """@brief 返回 Alembic asyncpg DSN / Return the Alembic asyncpg DSN."""

        return f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/aiws"

    @property
    def app_dsn(self) -> str:
        """@brief 返回 runtime asyncpg DSN / Return the runtime asyncpg DSN."""

        return f"postgresql+asyncpg://aiws_app@127.0.0.1:{self.port}/aiws"

    @property
    def app_sync_dsn(self) -> str:
        """@brief 返回 runtime psycopg DSN / Return the runtime psycopg DSN."""

        return f"postgresql://aiws_app@127.0.0.1:{self.port}/aiws"

    def super_dsn(self, database: str = "aiws") -> str:
        """@brief 返回超级用户 DSN / Return a superuser DSN.

        @param database 数据库名 / Database name.
        @return psycopg DSN / psycopg DSN.
        """

        return f"postgresql://{self.superuser}@127.0.0.1:{self.port}/{database}"

    def psql(self, binary: Path, sql: str, *, database: str = "aiws") -> None:
        """@brief 通过本地 socket 执行 fixture SQL / Execute fixture SQL over the local socket.

        @param binary psql 路径 / psql path.
        @param sql SQL 文本 / SQL text.
        @param database 数据库名 / Database name.
        """

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

    def execute(
        self,
        statement: str,
        parameters: tuple[object, ...] | None = None,
    ) -> None:
        """@brief 以超级用户提交 SQL / Commit SQL as the cluster superuser.

        @param statement SQL 文本 / SQL text.
        @param parameters 可选绑定参数 / Optional bound parameters.
        """

        with psycopg.connect(self.super_dsn()) as connection:
            connection.execute(statement, parameters)

    def rows(
        self,
        statement: str,
        parameters: tuple[object, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """@brief 以超级用户读取验证行 / Read verification rows as the superuser.

        @param statement 只读 SQL / Read-only SQL.
        @param parameters 可选绑定参数 / Optional bound parameters.
        @return 字典行 / Dictionary rows.
        """

        with psycopg.connect(self.super_dsn(), row_factory=dict_row) as connection:
            return [
                dict(row)
                for row in connection.execute(statement, parameters).fetchall()
            ]


def _postgres_binary(name: str) -> Path | None:
    """@brief 定位 PATH 或 Debian versioned PostgreSQL binary / Locate a PostgreSQL binary.

    @param name binary 名 / Binary name.
    @return 路径或缺失 / Path or absence.
    """

    direct = shutil.which(name)
    if direct is not None:
        return Path(direct)
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return candidates[0] if candidates else None


def _migration_config(dsn: str) -> Config:
    """@brief 构造显式 Alembic 配置 / Build explicit Alembic configuration.

    @param dsn migrator DSN / Migrator DSN.
    @return 带所有 role 投影的配置 / Configuration with all role projections.
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
def deletion_postgres(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_PostgresHarness]:
    """@brief 启动临时 PostgreSQL 并执行完整链 / Start PostgreSQL and execute the full chain.

    @param tmp_path_factory pytest module 临时目录工厂 / Module temporary-path factory.
    @return 已升级到 head 的 harness / Harness upgraded to head.
    """

    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("account-deletion-postgres")
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
        # PostgreSQL extensions are cluster-administrator bootstrap concerns.  The
        # application migrator deliberately is not a superuser, so mirror the
        # production db-init ordering before Alembic reaches revision 0001.
        try:
            harness.psql(psql, "CREATE EXTENSION vector;")
        except subprocess.CalledProcessError:
            pytest.skip("the PostgreSQL vector extension is unavailable")
        configuration = _migration_config(harness.migration_dsn)
        command.upgrade(configuration, "20260723_0024")
        harness.execute(
            """
            INSERT INTO identity.users (
                id, external_subject, display_name, email, email_canonical,
                email_verified, account_status, locale
            ) VALUES (
                'user_slug_migration', 'subject-slug-migration', 'Slug Migration',
                'slug-migration@example.test', 'slug-migration@example.test',
                true, 'active', 'en'
            );
            INSERT INTO identity.workspaces (
                id, resource_owner_id, name, slug, plan, data_region, deleted_at
            ) VALUES
            (
                'workspace_slug_live', 'user_slug_migration', 'Live Reserved Slug',
                'deleted-workspace', 'personal', 'global', NULL
            ),
            (
                'workspace_slug_historical', 'user_slug_migration', 'Historical Deleted',
                'historical-private-slug', 'personal', 'global',
                '2026-01-01T00:00:00Z'
            );
            """
        )
        command.upgrade(configuration, "head")
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


@dataclass(frozen=True, slots=True)
class _SecretErasureResult:
    """@brief 一批 creator secret 擦除结果 / One creator-secret erasure result."""

    has_more: bool = False


@dataclass(slots=True)
class _RecordingSecretErasure:
    """@brief 记录精确 creator+Workspace scope / Record exact creator-and-Workspace scopes."""

    ownerships: list[ConnectionOwnership] = field(default_factory=list)

    async def erase_created_by(
        self,
        ownership: ConnectionOwnership,
        *,
        limit: int = 1_000,
    ) -> _SecretErasureResult:
        """@brief 记录并声明本批已清空 / Record and declare the bounded batch empty.

        @param ownership 精确所有权 / Exact ownership.
        @param limit 有界批量 / Bounded batch size.
        @return 无剩余数据 / No data remains.
        """

        assert limit == 1_000
        self.ownerships.append(ownership)
        return _SecretErasureResult()


@dataclass(slots=True)
class _RecordingUploadErasure:
    """@brief 记录被删除的对象存储 key / Record object-store keys selected for deletion."""

    batches: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)

    async def erase(self, workspace_id: str, upload_ids: tuple[UploadSessionId, ...]) -> int:
        """@brief 幂等接受显式对象集 / Idempotently accept an explicit object set.

        @param workspace_id 精确 Workspace / Exact Workspace.
        @param upload_ids 上传标识 / Upload identifiers.
        @return 删除数 / Deleted count.
        """

        self.batches.append((str(workspace_id), tuple(map(str, upload_ids))))
        return len(upload_ids)


@dataclass(slots=True)
class _FailOnceUploadErasure:
    """@brief 首次对象删除失败、随后幂等成功 / Fail the first object deletion and then succeed idempotently."""

    calls: int = 0

    async def erase(
        self,
        workspace_id: str,
        upload_ids: tuple[UploadSessionId, ...],
    ) -> int:
        """@brief 产生一次瞬时故障 / Produce one transient failure.

        @param workspace_id 精确 Workspace / Exact Workspace.
        @param upload_ids 待删除对象 / Objects to erase.
        @return 后续调用确认不存在的对象数 / Confirmed-absent count on later calls.
        """

        del workspace_id
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated object-store outage")
        return len(upload_ids)


@dataclass(slots=True)
class _AlwaysFailUploadErasure:
    """@brief 每次对象删除都产生依赖故障 / Fail every object-deletion attempt."""

    calls: int = 0

    async def erase(
        self,
        workspace_id: str,
        upload_ids: tuple[UploadSessionId, ...],
    ) -> int:
        """@brief 模拟持续对象存储故障 / Simulate a persistent object-store outage.

        @param workspace_id 精确 Workspace / Exact Workspace.
        @param upload_ids 待删除对象 / Objects to erase.
        @return 此 fake 永不返回 / This fake never returns.
        @raise RuntimeError 每次调用均抛出 / Raised on every invocation.
        """

        del workspace_id, upload_ids
        self.calls += 1
        raise RuntimeError("simulated persistent object-store outage")


@dataclass(slots=True)
class _RecordingEmailErasure:
    """@brief 记录删除前清理的收件人 / Record recipients cleared before deletion."""

    recipients: list[str] = field(default_factory=list)

    async def erase_recipient(self, recipient: str, *, limit: int = 1_000) -> bool:
        """@brief 声明无活跃邮件租约 / Declare no active email lease.

        @param recipient 待擦除收件人 / Recipient to erase.
        @param limit 有界批量 / Bounded batch size.
        @return 已清空 / Fully empty.
        """

        assert limit == 1_000
        self.recipients.append(recipient)
        return True


def _seed_personal(harness: _PostgresHarness) -> None:
    """@brief 插入含外部对象的个人 Workspace / Seed a personal Workspace with an external object."""

    harness.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES (
            'user_delete_personal', 'subject-delete-personal', 'Personal Owner',
            'personal-delete@example.test', 'personal-delete@example.test',
            true, 'deletion_scheduled', 'zh-CN'
        );
        INSERT INTO identity.oauth_authorization_requests (
            id, client_id, redirect_uri, scope, state, nonce, code_challenge,
            code_challenge_method, prompt, status, created_at, expires_at
        ) VALUES (
            'authreq_delete_personal', 'client_delete_personal',
            'https://app.example.test/oauth/callback', 'openid offline_access',
            'private-oauth-state', 'private-oauth-nonce', repeat('q', 43),
            'S256', 'login', 'code_issued', statement_timestamp(),
            statement_timestamp() + interval '1 hour'
        );
        INSERT INTO identity.identity_browser_sessions (
            id, authorization_request_id, browser_secret_hash, csrf_token_hash,
            user_id, created_at, last_seen_at, expires_at
        ) VALUES (
            'browser_delete_personal', 'authreq_delete_personal', repeat('1', 64),
            repeat('2', 64), 'user_delete_personal', statement_timestamp(),
            statement_timestamp(), statement_timestamp() + interval '1 hour'
        );
        INSERT INTO identity.identity_flows (
            id, purpose, status, allowed_steps, authorization_request_id,
            browser_session_id, client_id, redirect_uri, code_challenge,
            authorization_resume_uri, webauthn_options, user_id, internal_state,
            created_at, expires_at
        ) VALUES (
            'flow_delete_personal', 'recover', 'pending',
            '["verify_email_code"]'::jsonb, 'authreq_delete_personal',
            'browser_delete_personal', 'client_delete_personal',
            'https://app.example.test/oauth/callback', repeat('q', 43),
            'https://app.example.test/private-resume', '{"challenge":"private"}'::jsonb,
            'user_delete_personal', '{"identifier":"personal-delete@example.test"}'::jsonb,
            statement_timestamp(), statement_timestamp() + interval '1 hour'
        );
        INSERT INTO identity.identity_authenticators (
            id, user_id, kind, display_name, verifier, credential_metadata
        ) VALUES (
            'authenticator_delete_personal', 'user_delete_personal', 'password',
            'Password', 'private-password-verifier', '{"private":"metadata"}'::jsonb
        );
        INSERT INTO identity.identity_login_sessions (
            id, user_id, client_id, client_name, device_name, session_secret_hash,
            created_at, last_seen_at, idle_expires_at, absolute_expires_at
        ) VALUES (
            'login_delete_personal', 'user_delete_personal', 'client_delete_personal',
            'Web', 'Private device', repeat('3', 64), statement_timestamp(),
            statement_timestamp(), statement_timestamp() + interval '1 hour',
            statement_timestamp() + interval '1 day'
        );
        INSERT INTO identity.oauth_authorization_codes (
            id, code_hash, authorization_request_id, subject, user_id,
            login_session_id, client_id, redirect_uri, scope, nonce,
            code_challenge, auth_time, expires_at
        ) VALUES (
            'authcode_delete_personal', repeat('4', 64), 'authreq_delete_personal',
            'subject-delete-personal', 'user_delete_personal', 'login_delete_personal',
            'client_delete_personal', 'https://app.example.test/oauth/callback',
            'openid offline_access', 'private-oauth-nonce', repeat('q', 43),
            statement_timestamp(), statement_timestamp() + interval '10 minutes'
        );
        INSERT INTO identity.oauth_refresh_token_families (
            id, subject, user_id, client_id, login_session_id, scope
        ) VALUES (
            'family_delete_personal', 'subject-delete-personal',
            'user_delete_personal', 'client_delete_personal',
            'login_delete_personal', 'openid offline_access'
        );
        INSERT INTO identity.oauth_refresh_tokens (
            id, family_id, token_hash, sequence, expires_at
        ) VALUES (
            'refresh_delete_personal', 'family_delete_personal', repeat('5', 64),
            1, statement_timestamp() + interval '1 day'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_delete_personal', 'user_delete_personal',
            'Personal Workspace', 'personal-delete', 'personal', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status
        ) VALUES (
            'member_delete_personal', 'workspace_delete_personal',
            'user_delete_personal', 'user_delete_personal',
            'Personal Owner', 'owner', 'active'
        );
        INSERT INTO knowledge.upload_sessions (
            id, workspace_id, status, expires_at, legacy_payload
        ) VALUES (
            'upload_delete_personal', 'workspace_delete_personal', 'created',
            statement_timestamp() + interval '1 day', true
        );
        INSERT INTO identity.workspace_invitations (
            id, workspace_id, email_canonical, email_hint, role, status, expires_at,
            invited_by_actor_id, accepted_by_user_id, resolved_at
        ) VALUES (
            'invitation_delete_personal', 'workspace_delete_personal',
            'personal-delete@example.test', 'p***@example.test', 'viewer', 'accepted',
            '2027-01-01T00:00:00Z', 'user_delete_personal', 'user_delete_personal',
            '2026-01-01T00:00:00Z'
        );
        INSERT INTO identity.api_v2_idempotency_records (
            id, user_id, workspace_id, method, canonical_path, idempotency_key,
            request_fingerprint, status, response_status, response_headers,
            response_body, expires_at
        ) VALUES (
            'receipt_delete_personal', 'user_delete_personal',
            'workspace_delete_personal', 'POST', '/api/v2/private-command',
            'delete-personal-key-0001', repeat('e', 64), 'completed', 200,
            '[]'::jsonb,
            convert_to('{"email":"personal-delete@example.test"}', 'UTF8'),
            statement_timestamp() + interval '2 days'
        );
        INSERT INTO observability.telemetry_records (
            id, workspace_id, resource_owner_id, actor_id, kind, source,
            service, name, metric_type, value, unit, attributes
        ) VALUES (
            'telemetry_delete_personal', 'workspace_delete_personal',
            'user_delete_personal', 'user_delete_personal', 'metric', 'backend',
            'account-deletion-test', 'private.attribute', 'counter', 1, '1',
            '{"email":"personal-delete@example.test"}'::jsonb
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_request_personal', 'user_delete_personal',
            'scheduled', '2000-01-01T00:00:00Z'
        );
        """
    )


def _seed_shared(harness: _PostgresHarness) -> None:
    """@brief 插入必须保留且移交 owner 的共享 Workspace / Seed a retained shared Workspace requiring owner transfer."""

    harness.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES
        (
            'user_delete_shared', 'subject-delete-shared', 'Shared Owner',
            'shared-delete@example.test', 'shared-delete@example.test',
            true, 'deletion_scheduled', 'zh-CN'
        ),
        (
            'user_shared_successor', 'subject-shared-successor', 'Successor',
            'successor@example.test', 'successor@example.test',
            true, 'active', 'zh-CN'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_delete_shared', 'user_delete_shared',
            'Shared Workspace', 'shared-delete', 'team', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status, joined_at
        ) VALUES
        (
            'member_delete_shared', 'workspace_delete_shared', 'user_delete_shared',
            'user_delete_shared', 'Shared Owner', 'owner', 'active', '2025-01-01T00:00:00Z'
        ),
        (
            'member_shared_successor', 'workspace_delete_shared', 'user_delete_shared',
            'user_shared_successor', 'Successor', 'admin', 'active', '2025-02-01T00:00:00Z'
        );
        INSERT INTO identity.workspace_invitations (
            id, workspace_id, email_canonical, email_hint, role, status, expires_at,
            invited_by_actor_id, accepted_by_user_id, resolved_at
        ) VALUES (
            'invitation_delete_shared', 'workspace_delete_shared',
            'shared-delete@example.test', 's***@example.test', 'viewer', 'accepted',
            '2027-01-01T00:00:00Z', 'user_delete_shared', 'user_delete_shared',
            '2026-01-01T00:00:00Z'
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_request_shared', 'user_delete_shared',
            'scheduled', '2000-01-01T00:00:00Z'
        );
        """
    )


def _seed_cross_domain_personal(harness: _PostgresHarness) -> None:
    """@brief 插入跨 Agent/Resume FK 环的个人 Workspace / Seed a personal Workspace spanning Agent and Resume FK cycles."""

    harness.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES (
            'user_delete_graph01', 'subject-delete-graph', 'Graph Owner',
            'graph-delete@example.test', 'graph-delete@example.test',
            true, 'deletion_scheduled', 'en'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_delete_graph01', 'user_delete_graph01',
            'Graph Workspace', 'delete-graph', 'personal', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status
        ) VALUES (
            'member_delete_graph01', 'workspace_delete_graph01',
            'user_delete_graph01', 'user_delete_graph01',
            'Graph Owner', 'owner', 'active'
        );

        INSERT INTO agent.conversations (
            id, workspace_id, resource_owner_id, title, capability,
            status, message_sequence
        ) VALUES (
            'conversation_delete_graph01', 'workspace_delete_graph01',
            'user_delete_graph01', 'Deletion graph', 'general', 'active', 2
        );
        INSERT INTO agent.messages (
            id, workspace_id, resource_owner_id, conversation_id,
            sequence, role, content_parts
        ) VALUES (
            'message_delete_input01', 'workspace_delete_graph01',
            'user_delete_graph01', 'conversation_delete_graph01', 1, 'user',
            '[{"type":"text","text":"delete me"}]'::jsonb
        );
        INSERT INTO agent.jobs (
            id, workspace_id, resource_owner_id, job_type, status, phase,
            completed_units, total_units, progress_unit,
            target_resource_type, target_resource_id, request_payload
        ) VALUES
        (
            'job_delete_agent01', 'workspace_delete_graph01', 'user_delete_graph01',
            'agent.run', 'queued', 'queued', 0, 1, 'steps',
            'agent_run', 'run_delete_graph01', '{}'::jsonb
        ),
        (
            'job_delete_render01', 'workspace_delete_graph01', 'user_delete_graph01',
            'resume.render', 'queued', 'queued', 0, 1, 'steps',
            'resume', 'resume_delete_graph01', '{}'::jsonb
        );
        INSERT INTO agent.runs (
            id, workspace_id, resource_owner_id, conversation_id,
            input_message_id, job_id, capability, status, spec, execution_grant
        ) VALUES (
            'run_delete_graph01', 'workspace_delete_graph01', 'user_delete_graph01',
            'conversation_delete_graph01', 'message_delete_input01',
            'job_delete_agent01', 'general', 'queued', '{}'::jsonb, '{}'::jsonb
        );
        INSERT INTO agent.tool_approvals (
            id, workspace_id, resource_owner_id, run_id, tool_call_id,
            tool_name, summary, risk, invocation_type, invocation_id,
            invocation_revision, status, expires_at
        ) VALUES (
            'approval_delete_graph01', 'workspace_delete_graph01',
            'user_delete_graph01', 'run_delete_graph01', 'toolcall_delete_graph01',
            'calendar.create', 'Create a follow-up', 'high', 'tool_invocation',
            'invocation_delete_graph01', 1, 'pending',
            statement_timestamp() + interval '1 day'
        );
        UPDATE agent.runs
        SET status = 'waiting_for_approval',
            pending_approval_id = 'approval_delete_graph01',
            active_tool_call_id = 'toolcall_delete_graph01',
            updated_at = statement_timestamp(),
            revision = revision + 1
        WHERE id = 'run_delete_graph01';
        INSERT INTO agent.messages (
            id, workspace_id, resource_owner_id, conversation_id,
            sequence, role, content_parts, parent_message_id, source_run_id
        ) VALUES (
            'message_delete_output01', 'workspace_delete_graph01',
            'user_delete_graph01', 'conversation_delete_graph01', 2, 'assistant',
            '[{"type":"text","text":"waiting"}]'::jsonb,
            'message_delete_input01', 'run_delete_graph01'
        );

        INSERT INTO agent.artifacts (
            id, workspace_id, kind, subject_type, subject_id, subject_revision,
            media_type, size_bytes, sha256, storage_key
        ) VALUES (
            'artifact_delete_graph01', 'workspace_delete_graph01', 'generic',
            'agent_run', 'run_delete_graph01', 1, 'application/octet-stream',
            0, repeat('0', 64), 'delete-graph/artifact.bin'
        );
        INSERT INTO agent.artifact_contents (
            artifact_id, workspace_id, storage_key, media_type,
            size_bytes, sha256, content
        ) VALUES (
            'artifact_delete_graph01', 'workspace_delete_graph01',
            'delete-graph/artifact.bin', 'application/octet-stream',
            0, repeat('0', 64), decode('', 'hex')
        );

        INSERT INTO resume.template_versions (
            id, workspace_id, resource_owner_id, template_id, template_version,
            manifest, renderer_binding
        ) VALUES (
            'template_delete_graph01', 'workspace_delete_graph01',
            'user_delete_graph01', 'template_graph01', 'v1', '{}'::jsonb, '{}'::jsonb
        );
        INSERT INTO resume.documents (
            id, workspace_id, resource_owner_id, template_version_id,
            template_id, template_version, title, locale,
            current_revision_no, revision
        ) VALUES (
            'resume_delete_graph01', 'workspace_delete_graph01',
            'user_delete_graph01', 'template_delete_graph01',
            'template_graph01', 'v1', 'Graph Resume', 'en', 1, 1
        );
        INSERT INTO resume.revisions (
            id, workspace_id, resource_owner_id, resume_id, revision_no,
            semantic_document, content_hash, created_by_actor_id
        ) VALUES (
            'revision_delete_graph01', 'workspace_delete_graph01',
            'user_delete_graph01', 'resume_delete_graph01', 1,
            '{}'::jsonb, repeat('a', 64), 'user_delete_graph01'
        );
        INSERT INTO resume.render_jobs (
            id, workspace_id, resource_owner_id, job_id, resume_id,
            resume_revision_id, artifact_id, render_profile
        ) VALUES (
            'render_delete_graph01', 'workspace_delete_graph01',
            'user_delete_graph01', 'job_delete_render01', 'resume_delete_graph01',
            'revision_delete_graph01', 'artifact_delete_graph01', 'preview'
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_request_graph01', 'user_delete_graph01',
            'scheduled', '1998-01-01T00:00:00Z'
        );
        """
    )


def _seed_interview_knowledge_personal(harness: _PostgresHarness) -> None:
    """@brief 插入 Interview provenance 与 Knowledge current-version 环 / Seed Interview provenance and the Knowledge current-version cycle."""

    harness.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES (
            'user_delete_ikgraph', 'subject-delete-ikgraph', 'IK Graph Owner',
            'ikgraph-delete@example.test', 'ikgraph-delete@example.test',
            true, 'deletion_scheduled', 'en'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_delete_ikgraph', 'user_delete_ikgraph',
            'Interview Knowledge Graph', 'delete-ikgraph', 'personal', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status
        ) VALUES (
            'member_delete_ikgraph', 'workspace_delete_ikgraph',
            'user_delete_ikgraph', 'user_delete_ikgraph',
            'IK Graph Owner', 'owner', 'active'
        );

        INSERT INTO interview.scenarios (
            id, workspace_id, spec, status
        ) VALUES (
            'scenario_delete_ikgraph', 'workspace_delete_ikgraph',
            '{"rubric":{"dimensions":[{"id":"dimension_delete_01"}]}}'::jsonb,
            'active'
        );
        INSERT INTO interview.sessions (
            id, workspace_id, scenario_id, status, spec, execution_grant,
            created_at, updated_at
        ) VALUES (
            'session_delete_ikgraph', 'workspace_delete_ikgraph',
            'scenario_delete_ikgraph', 'created', '{}'::jsonb, '{}'::jsonb,
            '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
        );
        INSERT INTO interview.realtime_connections (
            id, workspace_id, session_id, audience_type, audience_id,
            audience_revision, transport, expires_at, created_at, updated_at
        ) VALUES (
            'connection_delete_ikgraph', 'workspace_delete_ikgraph',
            'session_delete_ikgraph', 'user', 'user_delete_ikgraph', 1,
            'websocket', '2026-01-01T00:20:00Z',
            '2026-01-01T00:10:00Z', '2026-01-01T00:10:00Z'
        );
        INSERT INTO interview.realtime_inputs (
            id, workspace_id, session_id, connection_id, sequence,
            fingerprint_sha256, occurred_at, created_at, updated_at
        ) VALUES (
            'input_delete_ikgraph01', 'workspace_delete_ikgraph',
            'session_delete_ikgraph', 'connection_delete_ikgraph', 1,
            repeat('b', 64), '2026-01-01T00:11:00Z',
            '2026-01-01T00:11:00Z', '2026-01-01T00:11:00Z'
        );
        INSERT INTO interview.transcript_segments (
            id, workspace_id, session_id, sequence, speaker, start_ms, end_ms,
            text_content, source_input_id, created_at, updated_at
        ) VALUES (
            'segment_delete_ikgraph01', 'workspace_delete_ikgraph',
            'session_delete_ikgraph', 1, 'candidate', 0, 1000,
            'Candidate answer', 'input_delete_ikgraph01',
            '2026-01-01T00:12:00Z', '2026-01-01T00:12:00Z'
        );
        INSERT INTO interview.reports (
            id, workspace_id, session_id, draft, generated_at,
            created_at, updated_at, revision
        ) VALUES (
            'report_delete_ikgraph01', 'workspace_delete_ikgraph',
            'session_delete_ikgraph', '{}'::jsonb, '2026-01-01T02:30:00Z',
            '2026-01-01T02:30:00Z', '2026-01-01T02:30:00Z', 1
        );
        INSERT INTO interview.report_evidence (
            id, workspace_id, report_id, session_id, segment_id,
            dimension_id, start_ms, end_ms
        ) VALUES (
            'evidence_delete_ikgraph01', 'workspace_delete_ikgraph',
            'report_delete_ikgraph01', 'session_delete_ikgraph',
            'segment_delete_ikgraph01', 'dimension_delete_01', 0, 1000
        );
        INSERT INTO agent.jobs (
            id, workspace_id, resource_owner_id, job_type, status, phase,
            completed_units, total_units, progress_unit,
            target_resource_type, target_resource_id, result_refs,
            request_payload, started_at, finished_at
        ) VALUES (
            'job_delete_ikreport01', 'workspace_delete_ikgraph',
            'user_delete_ikgraph', 'interview.report', 'succeeded', 'completed',
            1, 1, 'items', 'interview_session', 'session_delete_ikgraph',
            '[{"resource_type":"interview_report","id":"report_delete_ikgraph01","revision":1}]'::jsonb,
            '{"spec":{}}'::jsonb,
            '2026-01-01T02:00:00Z', '2026-01-01T02:30:00Z'
        );
        INSERT INTO interview.session_jobs (
            id, workspace_id, job_id, session_id, job_kind,
            created_at, updated_at, revision
        ) VALUES (
            'binding_delete_ikgraph01', 'workspace_delete_ikgraph',
            'job_delete_ikreport01', 'session_delete_ikgraph', 'interview.report',
            '2026-01-01T02:00:00Z', '2026-01-01T02:00:00Z', 1
        );
        UPDATE interview.sessions
        SET status = 'completed',
            report_id = 'report_delete_ikgraph01',
            started_at = '2026-01-01T01:00:00Z',
            ended_at = '2026-01-01T02:30:00Z',
            updated_at = '2026-01-01T03:00:00Z',
            revision = revision + 1
        WHERE id = 'session_delete_ikgraph';
        INSERT INTO knowledge.sources (
            id, workspace_id, resource_owner_id, source_type, title, config,
            source_input, public_config, current_policy_version,
            version_counter, ingestion_state
        ) VALUES (
            'source_delete_ikgraph01', 'workspace_delete_ikgraph',
            'user_delete_ikgraph', 'manual_note', 'Deletion note', '{}'::jsonb,
            '{}'::jsonb, '{}'::jsonb, 1, 0, 'not_started'
        );
        INSERT INTO knowledge.source_versions (
            id, workspace_id, resource_owner_id, source_id, version_no,
            content_hash, content_sha256, size_bytes, status,
            artifact_type, artifact_id, artifact_revision, origin
        ) VALUES (
            'version_delete_ikgraph01', 'workspace_delete_ikgraph',
            'user_delete_ikgraph', 'source_delete_ikgraph01', 1,
            repeat('c', 64), repeat('c', 64), 14, 'pending',
            'knowledge_document', 'document_delete_ikgraph01', 1, '{}'::jsonb
        );
        UPDATE knowledge.sources
        SET current_version_id = 'version_delete_ikgraph01',
            version_counter = 1,
            updated_at = statement_timestamp(),
            revision = revision + 1
        WHERE id = 'source_delete_ikgraph01';
        INSERT INTO knowledge.chunks (
            id, workspace_id, resource_owner_id, source_version_id,
            ordinal, text_content, content_hash, origin, token_count
        ) VALUES (
            'chunk_delete_ikgraph01', 'workspace_delete_ikgraph',
            'user_delete_ikgraph', 'version_delete_ikgraph01', 0,
            'Candidate note', repeat('d', 64), '{}'::jsonb, 2
        );
        INSERT INTO knowledge.visibility_policies (
            id, workspace_id, resource_owner_id, source_id, policy_version,
            default_effect, sensitivity, session_override_allowed,
            allow_external_model_processing, allowed_model_regions
        ) VALUES (
            'policy_delete_ikgraph01', 'workspace_delete_ikgraph',
            'user_delete_ikgraph', 'source_delete_ikgraph01', 1,
            'deny', 'normal', false, false, ARRAY['global']::varchar[]
        );
        INSERT INTO knowledge.embedding_spaces (
            id, workspace_id, resource_owner_id, provider, model,
            model_revision, dimension, distance_metric, normalization
        ) VALUES (
            'space_delete_ikgraph01', 'workspace_delete_ikgraph',
            'user_delete_ikgraph', 'test', 'embed-test', 'v1',
            1024, 'cosine', 'l2'
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_request_ikgraph', 'user_delete_ikgraph',
            'scheduled', '1997-01-01T00:00:00Z'
        );
        """
    )


def _seed_external_retry(harness: _PostgresHarness) -> None:
    """@brief 插入需要对象存储重试的删除请求 / Seed a deletion requiring object-store retry."""

    harness.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES (
            'user_delete_retry01', 'subject-delete-retry', 'Retry Owner',
            'retry-delete@example.test', 'retry-delete@example.test',
            true, 'deletion_scheduled', 'en'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_delete_retry01', 'user_delete_retry01',
            'Retry Workspace', 'delete-retry', 'personal', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status
        ) VALUES (
            'member_delete_retry01', 'workspace_delete_retry01',
            'user_delete_retry01', 'user_delete_retry01',
            'Retry Owner', 'owner', 'active'
        );
        INSERT INTO knowledge.upload_sessions (
            id, workspace_id, status, expires_at, legacy_payload
        ) VALUES (
            'upload_delete_retry01', 'workspace_delete_retry01', 'created',
            statement_timestamp() + interval '1 day', true
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_request_retry01', 'user_delete_retry01',
            'scheduled', '1996-01-01T00:00:00Z'
        );
        """
    )


def _seed_bounded_progress(harness: _PostgresHarness) -> None:
    """@brief 插入跨三批完成且失败预算仅为一的请求 / Seed work spanning three batches with a one-attempt failure budget."""

    harness.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES (
            'user_delete_progress', 'subject-delete-progress', 'Progress Owner',
            'progress-delete@example.test', 'progress-delete@example.test',
            true, 'deletion_scheduled', 'en'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_delete_progress', 'user_delete_progress',
            'Progress Workspace', 'delete-progress', 'personal', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status
        ) VALUES (
            'member_delete_progress', 'workspace_delete_progress',
            'user_delete_progress', 'user_delete_progress',
            'Progress Owner', 'owner', 'active'
        );
        INSERT INTO knowledge.upload_sessions (
            id, workspace_id, status, expires_at, legacy_payload
        ) VALUES
        (
            'upload_delete_progress01', 'workspace_delete_progress', 'created',
            statement_timestamp() + interval '1 day', true
        ),
        (
            'upload_delete_progress02', 'workspace_delete_progress', 'created',
            statement_timestamp() + interval '1 day', true
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_request_progress', 'user_delete_progress',
            'scheduled', '1995-01-01T00:00:00Z'
        );
        """
    )


def _seed_external_exhaustion(harness: _PostgresHarness) -> None:
    """@brief 插入会耗尽对象 item 尝试的删除请求 / Seed a deletion whose object item will exhaust attempts."""

    harness.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES (
            'user_delete_exhaust', 'subject-delete-exhaust', 'Exhaust Owner',
            'exhaust-delete@example.test', 'exhaust-delete@example.test',
            true, 'deletion_scheduled', 'en'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_delete_exhaust', 'user_delete_exhaust',
            'Exhaust Workspace', 'delete-exhaust', 'personal', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status
        ) VALUES (
            'member_delete_exhaust', 'workspace_delete_exhaust',
            'user_delete_exhaust', 'user_delete_exhaust',
            'Exhaust Owner', 'owner', 'active'
        );
        INSERT INTO knowledge.upload_sessions (
            id, workspace_id, status, expires_at, legacy_payload
        ) VALUES (
            'upload_delete_exhaust', 'workspace_delete_exhaust', 'created',
            statement_timestamp() + interval '1 day', true
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_request_exhaust', 'user_delete_exhaust',
            'scheduled', '1994-01-01T00:00:00Z'
        );
        """
    )


async def _run_deletion(
    harness: _PostgresHarness,
    *,
    upload_erasure: _RecordingUploadErasure,
    secret_erasure: _RecordingSecretErasure,
    email_erasure: _RecordingEmailErasure,
) -> None:
    """@brief 通过 app role 执行一轮删除 / Execute one deletion pass through the app role.

    @param harness PostgreSQL harness / PostgreSQL harness.
    @param upload_erasure 对象存储擦除器 / Object-store eraser.
    @param secret_erasure creator secret 擦除器 / Creator-secret eraser.
    @param email_erasure 待发邮件擦除器 / Queued-email eraser.
    """

    database = AsyncDatabase(
        AsyncDatabaseOptions(harness.app_dsn, pool_size=2, max_overflow=0)
    )
    now = datetime.now(UTC)
    try:
        port = PostgresAccountDeletionExecutionPort(
            database,
            upload_erasure=upload_erasure,
            creator_secret_erasure=secret_erasure,
            recipient_email_erasure=email_erasure,
        )
        result = await AccountDeletionExecutionService(
            port,
            batch_size=1,
            clock=lambda: now,
        ).run_once()
        assert result.claimed == 1
        assert result.completed == 1
        assert result.failed == 0
        assert result.retryable == 0
        assert result.stale_claims == 0
    finally:
        await database.aclose()


def test_0025_is_linear_narrow_and_modelled_as_durable_work(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 0025 仅授权窄函数且拒绝直读执行表 / 0025 grants narrow functions and denies direct table reads."""

    source = MIGRATION.read_text(encoding="utf-8")
    assert 'down_revision = "20260723_0024"' in source
    privileges = deletion_postgres.rows(
        """
        SELECT
          has_table_privilege(
            'aiws_app', 'identity.account_deletion_erasure_items', 'SELECT'
          ) AS app_table_read,
          has_function_privilege(
            'aiws_app',
            'identity.claim_due_account_deletions(text,timestamp with time zone,integer,integer,integer)',
            'EXECUTE'
          ) AS app_claim,
          has_function_privilege(
            'aiws_dashboard',
            'identity.claim_due_account_deletions(text,timestamp with time zone,integer,integer,integer)',
            'EXECUTE'
          ) AS dashboard_claim,
          has_function_privilege(
            'aiws_app',
            'identity.release_account_deletion_progress(text,text,integer)',
            'EXECUTE'
          ) AS app_release_progress,
          has_function_privilege(
            'aiws_dashboard',
            'identity.release_account_deletion_progress(text,text,integer)',
            'EXECUTE'
          ) AS dashboard_release_progress
        """
    )[0]
    assert privileges == {
        "app_table_read": False,
        "app_claim": True,
        "dashboard_claim": False,
        "app_release_progress": True,
        "dashboard_release_progress": False,
    }


def test_0025_migrates_deleted_slugs_and_keeps_live_uniqueness(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 旧 deleted slug 被清除、deleted 可重名而 live 仍唯一 / Legacy deleted slugs are scrubbed, deleted rows may overlap, and live rows remain unique."""

    migrated = deletion_postgres.rows(
        """
        SELECT id, slug, deleted_at IS NOT NULL AS deleted
        FROM identity.workspaces
        WHERE id IN ('workspace_slug_live', 'workspace_slug_historical')
        ORDER BY id
        """
    )
    assert migrated == [
        {
            "id": "workspace_slug_historical",
            "slug": "deleted-workspace",
            "deleted": True,
        },
        {
            "id": "workspace_slug_live",
            "slug": "deleted-workspace",
            "deleted": False,
        },
    ]
    index = deletion_postgres.rows(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'identity' AND indexname = 'uq_workspaces_slug'
        """
    )[0]["indexdef"]
    assert "WHERE (deleted_at IS NULL)" in index

    deletion_postgres.execute(
        """
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region, deleted_at
        ) VALUES (
            'workspace_slug_deleted02', 'user_slug_migration', 'Another Deleted',
            'deleted-workspace', 'personal', 'global', statement_timestamp()
        )
        """
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        deletion_postgres.execute(
            """
            INSERT INTO identity.workspaces (
                id, resource_owner_id, name, slug, plan, data_region
            ) VALUES (
                'workspace_slug_live02', 'user_slug_migration', 'Another Live',
                'deleted-workspace', 'personal', 'global'
            )
            """
        )
    counts = deletion_postgres.rows(
        """
        SELECT count(*) FILTER (WHERE deleted_at IS NULL) AS live,
               count(*) FILTER (WHERE deleted_at IS NOT NULL) AS deleted
        FROM identity.workspaces
        WHERE slug = 'deleted-workspace'
        """
    )[0]
    assert counts == {"live": 1, "deleted": 2}


@pytest.mark.asyncio
async def test_personal_deletion_erases_external_objects_and_proves_every_invariant(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 个人数据、对象、凭据、邮件与 token 全部完成后才 finalize / Finalize only after all personal state is erased."""

    _seed_personal(deletion_postgres)
    uploads = _RecordingUploadErasure()
    secrets = _RecordingSecretErasure()
    emails = _RecordingEmailErasure()
    issued_before = datetime.now(UTC)

    await _run_deletion(
        deletion_postgres,
        upload_erasure=uploads,
        secret_erasure=secrets,
        email_erasure=emails,
    )

    assert uploads.batches == [
        ("workspace_delete_personal", ("upload_delete_personal",))
    ]
    assert [(str(item.workspace_id), str(item.created_by)) for item in secrets.ownerships] == [
        ("workspace_delete_personal", "user_delete_personal")
    ]
    assert emails.recipients == ["personal-delete@example.test"]
    state = deletion_postgres.rows(
        """
        SELECT request.status,
               request.completed_at IS NOT NULL AS completed,
               request.erasure_evidence,
               users.account_status,
               users.external_subject,
               users.display_name,
               users.email,
               workspace.name AS workspace_name,
               workspace.slug AS workspace_slug,
               workspace.deleted_at IS NOT NULL AS workspace_deleted,
               EXISTS (
                 SELECT 1 FROM knowledge.upload_sessions AS upload
                 WHERE upload.id = 'upload_delete_personal'
               ) AS upload_exists
        FROM identity.account_deletion_requests AS request
        JOIN identity.users AS users ON users.id = request.user_id
        JOIN identity.workspaces AS workspace
          ON workspace.id = 'workspace_delete_personal'
        WHERE request.id = 'deletion_request_personal'
        """
    )[0]
    assert state["status"] == "completed"
    assert state["completed"] is True
    assert state["erasure_evidence"] is not None
    assert set(state["erasure_evidence"].values()) == {True, state["erasure_evidence"]["completed_at"]}
    assert state["account_status"] == "deleted"
    assert state["external_subject"] == "deleted:user_delete_personal"
    assert state["display_name"] is None
    assert state["email"] is None
    assert state["workspace_name"] == "Deleted workspace"
    assert state["workspace_slug"] == "deleted-workspace"
    assert state["workspace_deleted"] is True
    assert state["upload_exists"] is False
    invitation = deletion_postgres.rows(
        """
        SELECT status, email_canonical, email_hint,
               invited_by_actor_id, accepted_by_user_id
        FROM identity.workspace_invitations
        WHERE id = 'invitation_delete_personal'
        """
    )[0]
    assert invitation["status"] == "accepted"
    assert invitation["email_canonical"].startswith("deleted+")
    assert invitation["email_canonical"].endswith("@invalid.example")
    assert invitation["email_hint"] == "d***@invalid.example"
    assert invitation["invited_by_actor_id"] is None
    assert invitation["accepted_by_user_id"] == "user_delete_personal"
    private_records = deletion_postgres.rows(
        """
        SELECT
          EXISTS (
            SELECT 1 FROM identity.api_v2_idempotency_records
            WHERE id = 'receipt_delete_personal'
          ) AS receipt_exists,
          (
            SELECT attributes FROM observability.telemetry_records
            WHERE id = 'telemetry_delete_personal'
          ) AS telemetry_attributes
        """
    )[0]
    assert private_records == {
        "receipt_exists": False,
        "telemetry_attributes": {},
    }
    identity_state = deletion_postgres.rows(
        """
        SELECT
          (SELECT revoked_at IS NOT NULL FROM identity.identity_login_sessions
           WHERE id = 'login_delete_personal') AS session_revoked,
          (SELECT consumed_at IS NOT NULL
                    AND subject = 'deleted:user_delete_personal'
           FROM identity.oauth_authorization_codes
           WHERE id = 'authcode_delete_personal') AS code_revoked,
          (SELECT revoked_at IS NOT NULL
                    AND subject = 'deleted:user_delete_personal'
           FROM identity.oauth_refresh_token_families
           WHERE id = 'family_delete_personal') AS family_revoked,
          NOT EXISTS (
            SELECT 1 FROM identity.identity_authenticators
            WHERE user_id = 'user_delete_personal'
          ) AS authenticators_erased,
          (SELECT internal_state = '{}'::jsonb
                    AND webauthn_options IS NULL
                    AND authorization_resume_uri IS NULL
           FROM identity.identity_flows
           WHERE id = 'flow_delete_personal') AS flow_scrubbed,
          (SELECT browser_secret_hash = repeat('0', 64)
                    AND csrf_token_hash = repeat('0', 64)
                    AND expires_at <= statement_timestamp()
           FROM identity.identity_browser_sessions
           WHERE id = 'browser_delete_personal') AS browser_scrubbed
        """
    )[0]
    assert set(identity_state.values()) == {True}
    token_epoch = deletion_postgres.rows(
        "SELECT identity.user_access_tokens_revoked("  # nosec B608 - fixed test literal
        f"'user_delete_personal', '{issued_before.isoformat()}') AS revoked"
    )[0]
    assert token_epoch == {"revoked": True}


@pytest.mark.asyncio
async def test_shared_deletion_preserves_workspace_and_transfers_ownership(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 共享资源保留、被删用户脱钩且确定性选出继任 owner / Shared data survives with deterministic owner succession."""

    _seed_shared(deletion_postgres)
    uploads = _RecordingUploadErasure()
    secrets = _RecordingSecretErasure()
    emails = _RecordingEmailErasure()

    await _run_deletion(
        deletion_postgres,
        upload_erasure=uploads,
        secret_erasure=secrets,
        email_erasure=emails,
    )

    assert uploads.batches == []
    assert [(str(item.workspace_id), str(item.created_by)) for item in secrets.ownerships] == [
        ("workspace_delete_shared", "user_delete_shared")
    ]
    assert emails.recipients == ["shared-delete@example.test"]
    workspace = deletion_postgres.rows(
        """
        SELECT name, slug, deleted_at
        FROM identity.workspaces
        WHERE id = 'workspace_delete_shared'
        """
    )[0]
    assert workspace == {
        "name": "Shared Workspace",
        "slug": "shared-delete",
        "deleted_at": None,
    }
    members = deletion_postgres.rows(
        """
        SELECT user_id, display_name, role, status
        FROM identity.workspace_members
        WHERE workspace_id = 'workspace_delete_shared'
        ORDER BY user_id
        """
    )
    assert members == [
        {
            "user_id": "user_delete_shared",
            "display_name": "Deleted user",
            "role": "viewer",
            "status": "suspended",
        },
        {
            "user_id": "user_shared_successor",
            "display_name": "Successor",
            "role": "owner",
            "status": "active",
        },
    ]
    invitation = deletion_postgres.rows(
        """
        SELECT status, email_canonical, email_hint, invited_by_actor_id, accepted_by_user_id
        FROM identity.workspace_invitations
        WHERE id = 'invitation_delete_shared'
        """
    )[0]
    assert invitation["status"] == "accepted"
    assert invitation["email_canonical"].startswith("deleted+")
    assert invitation["email_canonical"].endswith("@invalid.example")
    assert invitation["email_hint"] == "d***@invalid.example"
    assert invitation["invited_by_actor_id"] is None
    assert invitation["accepted_by_user_id"] == "user_delete_shared"


@pytest.mark.asyncio
async def test_personal_deletion_resolves_agent_cycles_and_cross_domain_resume_fks(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 非空 Agent 环与 Resume→Job/Artifact 外键按拓扑顺序删除 / Non-empty Agent cycles and Resume-to-Job/Artifact FKs are deleted topologically."""

    _seed_cross_domain_personal(deletion_postgres)
    uploads = _RecordingUploadErasure()
    secrets = _RecordingSecretErasure()
    emails = _RecordingEmailErasure()

    await _run_deletion(
        deletion_postgres,
        upload_erasure=uploads,
        secret_erasure=secrets,
        email_erasure=emails,
    )

    assert uploads.batches == []
    assert [(str(item.workspace_id), str(item.created_by)) for item in secrets.ownerships] == [
        ("workspace_delete_graph01", "user_delete_graph01")
    ]
    assert emails.recipients == ["graph-delete@example.test"]
    remaining = deletion_postgres.rows(
        """
        SELECT
          (SELECT count(*) FROM agent.conversations
           WHERE workspace_id = 'workspace_delete_graph01') AS conversations,
          (SELECT count(*) FROM agent.messages
           WHERE workspace_id = 'workspace_delete_graph01') AS messages,
          (SELECT count(*) FROM agent.runs
           WHERE workspace_id = 'workspace_delete_graph01') AS runs,
          (SELECT count(*) FROM agent.tool_approvals
           WHERE workspace_id = 'workspace_delete_graph01') AS approvals,
          (SELECT count(*) FROM agent.jobs
           WHERE workspace_id = 'workspace_delete_graph01') AS jobs,
          (SELECT count(*) FROM agent.artifacts
           WHERE workspace_id = 'workspace_delete_graph01') AS artifacts,
          (SELECT count(*) FROM agent.artifact_contents
           WHERE workspace_id = 'workspace_delete_graph01') AS artifact_contents,
          (SELECT count(*) FROM resume.documents
           WHERE workspace_id = 'workspace_delete_graph01') AS resumes,
          (SELECT count(*) FROM resume.revisions
           WHERE workspace_id = 'workspace_delete_graph01') AS resume_revisions,
          (SELECT count(*) FROM resume.render_jobs
           WHERE workspace_id = 'workspace_delete_graph01') AS render_jobs
        """
    )[0]
    assert set(remaining.values()) == {0}


@pytest.mark.asyncio
async def test_personal_deletion_resolves_interview_provenance_and_knowledge_version_cycle(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief Interview 证据拓扑与 Knowledge current-version 环完整删除 / Interview evidence topology and Knowledge current-version cycles are fully erased."""

    _seed_interview_knowledge_personal(deletion_postgres)
    uploads = _RecordingUploadErasure()
    secrets = _RecordingSecretErasure()
    emails = _RecordingEmailErasure()

    await _run_deletion(
        deletion_postgres,
        upload_erasure=uploads,
        secret_erasure=secrets,
        email_erasure=emails,
    )

    assert uploads.batches == []
    assert [(str(item.workspace_id), str(item.created_by)) for item in secrets.ownerships] == [
        ("workspace_delete_ikgraph", "user_delete_ikgraph")
    ]
    assert emails.recipients == ["ikgraph-delete@example.test"]
    remaining = deletion_postgres.rows(
        """
        SELECT
          (SELECT count(*) FROM interview.scenarios
           WHERE workspace_id = 'workspace_delete_ikgraph') AS scenarios,
          (SELECT count(*) FROM interview.sessions
           WHERE workspace_id = 'workspace_delete_ikgraph') AS sessions,
          (SELECT count(*) FROM interview.realtime_connections
           WHERE workspace_id = 'workspace_delete_ikgraph') AS realtime_connections,
          (SELECT count(*) FROM interview.realtime_inputs
           WHERE workspace_id = 'workspace_delete_ikgraph') AS realtime_inputs,
          (SELECT count(*) FROM interview.transcript_segments
           WHERE workspace_id = 'workspace_delete_ikgraph') AS transcript_segments,
          (SELECT count(*) FROM interview.reports
           WHERE workspace_id = 'workspace_delete_ikgraph') AS reports,
          (SELECT count(*) FROM interview.report_evidence
           WHERE workspace_id = 'workspace_delete_ikgraph') AS report_evidence,
          (SELECT count(*) FROM knowledge.sources
           WHERE workspace_id = 'workspace_delete_ikgraph') AS sources,
          (SELECT count(*) FROM knowledge.source_versions
           WHERE workspace_id = 'workspace_delete_ikgraph') AS source_versions,
          (SELECT count(*) FROM knowledge.chunks
           WHERE workspace_id = 'workspace_delete_ikgraph') AS chunks,
          (SELECT count(*) FROM knowledge.visibility_policies
           WHERE workspace_id = 'workspace_delete_ikgraph') AS policies,
          (SELECT count(*) FROM knowledge.embedding_spaces
           WHERE workspace_id = 'workspace_delete_ikgraph') AS embedding_spaces
        """
    )[0]
    assert set(remaining.values()) == {0}


@pytest.mark.asyncio
async def test_email_erasure_is_bounded_and_waits_for_active_delivery_leases(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 邮件擦除有界推进且不伪装撤回在途 SMTP / Email erasure is bounded and never pretends to recall an in-flight SMTP delivery."""

    database = AsyncDatabase(
        AsyncDatabaseOptions(deletion_postgres.app_dsn, pool_size=2, max_overflow=0)
    )
    outbox = PostgresIdentityEmailOutbox(
        database,
        IdentityEmailKeyring("key-deletion", {"key-deletion": bytes(range(32))}),
        rate_limit_hmac_key=bytes(reversed(range(32))),
        lease_duration=timedelta(seconds=30),
        retention=timedelta(days=30),
    )
    recipient = "bounded-erasure@example.test"
    leased_recipient = "leased-erasure@example.test"
    try:
        for code in ("100001", "100002", "100003"):
            async with outbox.atomic():
                await outbox.send_verification_code(
                    recipient,
                    code,
                    browser_session_id=f"browser_{code}",
                    network_identifier=f"198.51.100.{int(code[-1])}",
                    limit_per_hour=10,
                )
        assert await outbox.erase_recipient(recipient, limit=2) is False
        recipient_digest = outbox._digest("recipient", recipient)
        account_digest = outbox._digest("account", recipient)
        after_first_batch = deletion_postgres.rows(
            """
            SELECT
              (SELECT count(*) FROM identity.identity_email_outbox
               WHERE recipient_digest = %s) AS messages,
              (SELECT count(*) FROM identity.identity_email_rate_limits
               WHERE dimension_kind = 'account'
                 AND dimension_digest = %s) AS account_budgets
            """,
            (recipient_digest, account_digest),
        )[0]
        assert after_first_batch == {"messages": 1, "account_budgets": 1}
        assert await outbox.erase_recipient(recipient, limit=2) is True
        after_completion = deletion_postgres.rows(
            """
            SELECT
              (SELECT count(*) FROM identity.identity_email_outbox
               WHERE recipient_digest = %s) AS messages,
              (SELECT count(*) FROM identity.identity_email_rate_limits
               WHERE dimension_kind = 'account'
                 AND dimension_digest = %s) AS account_budgets
            """,
            (recipient_digest, account_digest),
        )[0]
        assert after_completion == {"messages": 0, "account_budgets": 0}

        async with outbox.atomic():
            await outbox.send_recovery_notification(leased_recipient)
        claimed = await outbox.claim(worker_id="deletion-email-worker", batch_size=1)
        assert len(claimed) == 1
        assert await outbox.erase_recipient(leased_recipient) is False
        deletion_postgres.execute(
            """
            UPDATE identity.identity_email_outbox
            SET lease_expires_at = statement_timestamp() - interval '1 second'
            WHERE id = %s
            """,
            (claimed[0].id,),
        )
        assert await outbox.erase_recipient(leased_recipient) is True
        assert deletion_postgres.rows(
            "SELECT id FROM identity.identity_email_outbox WHERE id = %s",
            (claimed[0].id,),
        ) == []
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_external_object_failure_requeues_item_and_recovers_after_account_takeover(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 对象故障释放 item，过期 account claim 接管后完成 / Object failure releases its item and an expired account claim is safely taken over."""

    _seed_external_retry(deletion_postgres)
    database = AsyncDatabase(
        AsyncDatabaseOptions(deletion_postgres.app_dsn, pool_size=2, max_overflow=0)
    )
    uploads = _FailOnceUploadErasure()
    secrets = _RecordingSecretErasure()
    emails = _RecordingEmailErasure()
    port = PostgresAccountDeletionExecutionPort(
        database,
        upload_erasure=uploads,
        creator_secret_erasure=secrets,
        recipient_email_erasure=emails,
    )
    try:
        first = await AccountDeletionExecutionService(port, batch_size=1).run_once()
        assert first.claimed == first.retryable == 1
        assert first.completed == first.failed == first.stale_claims == 0
        retry_state = deletion_postgres.rows(
            """
            SELECT request.status AS request_status,
                   request.attempt_count AS request_attempts,
                   item.status AS item_status,
                   item.attempt_count AS item_attempts,
                   item.last_error_code
            FROM identity.account_deletion_requests AS request
            JOIN identity.account_deletion_erasure_items AS item
              ON item.request_id = request.id
             AND item.resource_kind = 'upload_object'
            WHERE request.id = 'deletion_request_retry01'
            """
        )[0]
        assert retry_state == {
            "request_status": "running",
            "request_attempts": 1,
            "item_status": "pending",
            "item_attempts": 1,
            "last_error_code": "account_deletion.object_store_unavailable",
        }
        assert emails.recipients == []

        deletion_postgres.execute(
            """
            UPDATE identity.account_deletion_requests
            SET started_at = statement_timestamp() - interval '10 seconds',
                lease_expires_at = statement_timestamp() - interval '1 second'
            WHERE id = 'deletion_request_retry01'
            """
        )
        second = await AccountDeletionExecutionService(port, batch_size=1).run_once()
        assert second.claimed == second.completed == 1
        assert second.failed == second.retryable == second.stale_claims == 0
        assert uploads.calls == 2
        assert [(str(item.workspace_id), str(item.created_by)) for item in secrets.ownerships] == [
            ("workspace_delete_retry01", "user_delete_retry01")
        ]
        assert emails.recipients == ["retry-delete@example.test"]
        terminal = deletion_postgres.rows(
            """
            SELECT request.status AS request_status,
                   request.attempt_count AS request_attempts,
                   item.status AS item_status,
                   item.attempt_count AS item_attempts,
                   item.last_error_code
            FROM identity.account_deletion_requests AS request
            JOIN identity.account_deletion_erasure_items AS item
              ON item.request_id = request.id
             AND item.resource_kind = 'upload_object'
            WHERE request.id = 'deletion_request_retry01'
            """
        )[0]
        assert terminal == {
            "request_status": "completed",
            "request_attempts": 2,
            "item_status": "completed",
            "item_attempts": 2,
            "last_error_code": None,
        }
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_exhausted_external_item_finalizes_operator_failure_without_an_extra_lease_wait(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 第百次 item 故障同轮落为永久失败 / The hundredth item failure becomes terminal in the same pass."""

    _seed_external_exhaustion(deletion_postgres)
    database = AsyncDatabase(
        AsyncDatabaseOptions(deletion_postgres.app_dsn, pool_size=2, max_overflow=0)
    )
    uploads = _AlwaysFailUploadErasure()
    port = PostgresAccountDeletionExecutionPort(
        database,
        upload_erasure=uploads,
        creator_secret_erasure=_RecordingSecretErasure(),
        recipient_email_erasure=_RecordingEmailErasure(),
    )
    service = AccountDeletionExecutionService(port, batch_size=1)
    try:
        first = await service.run_once()
        assert first.claimed == first.retryable == 1
        deletion_postgres.execute(
            """
            UPDATE identity.account_deletion_requests
            SET started_at = statement_timestamp() - interval '10 seconds',
                lease_expires_at = statement_timestamp() - interval '1 second'
            WHERE id = 'deletion_request_exhaust';
            UPDATE identity.account_deletion_erasure_items
            SET attempt_count = 99
            WHERE request_id = 'deletion_request_exhaust'
              AND resource_kind = 'upload_object';
            """
        )

        terminal = await service.run_once()
        assert terminal.claimed == terminal.failed == 1
        assert terminal.completed == terminal.retryable == terminal.stale_claims == 0
        assert uploads.calls == 2
        state = deletion_postgres.rows(
            """
            SELECT request.status AS request_status,
                   request.problem ->> 'code' AS failure_code,
                   item.status AS item_status,
                   item.attempt_count AS item_attempts,
                   item.last_error_code
            FROM identity.account_deletion_requests AS request
            JOIN identity.account_deletion_erasure_items AS item
              ON item.request_id = request.id
             AND item.resource_kind = 'upload_object'
            WHERE request.id = 'deletion_request_exhaust'
            """
        )[0]
        assert state == {
            "request_status": "failed",
            "failure_code": "account_deletion.external_erasure_failed",
            "item_status": "failed",
            "item_attempts": 100,
            "last_error_code": "account_deletion.object_store_unavailable",
        }
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_successful_bounded_progress_yields_immediately_without_spending_failure_budget(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 健康分批立即续跑且不耗尽故障预算 / Healthy batches continue immediately without exhausting failure budget."""

    _seed_bounded_progress(deletion_postgres)
    database = AsyncDatabase(
        AsyncDatabaseOptions(deletion_postgres.app_dsn, pool_size=2, max_overflow=0)
    )
    uploads = _RecordingUploadErasure()
    secrets = _RecordingSecretErasure()
    emails = _RecordingEmailErasure()
    port = PostgresAccountDeletionExecutionPort(
        database,
        maximum_attempts=1,
        external_batch_size=1,
        upload_erasure=uploads,
        creator_secret_erasure=secrets,
        recipient_email_erasure=emails,
    )
    service = AccountDeletionExecutionService(port, batch_size=1)
    try:
        first = await service.run_once()
        second = await service.run_once()
        third = await service.run_once()

        assert (first.claimed, first.retryable, first.completed) == (1, 1, 0)
        assert (second.claimed, second.retryable, second.completed) == (1, 1, 0)
        assert (third.claimed, third.retryable, third.completed) == (1, 0, 1)
        assert uploads.batches == [
            ("workspace_delete_progress", ("upload_delete_progress01",)),
            ("workspace_delete_progress", ("upload_delete_progress02",)),
        ]
        assert [(str(item.workspace_id), str(item.created_by)) for item in secrets.ownerships] == [
            ("workspace_delete_progress", "user_delete_progress")
        ]
        assert emails.recipients == ["progress-delete@example.test"]
        terminal = deletion_postgres.rows(
            """
            SELECT request.status,
                   request.attempt_count,
                   array_agg(item.attempt_count ORDER BY item.resource_kind, item.resource_id)
                     AS item_attempts,
                   bool_and(item.status = 'completed') AS items_completed
            FROM identity.account_deletion_requests AS request
            JOIN identity.account_deletion_erasure_items AS item
              ON item.request_id = request.id
            WHERE request.id = 'deletion_request_progress'
            GROUP BY request.id
            """
        )[0]
        assert terminal == {
            "status": "completed",
            "attempt_count": 1,
            "item_attempts": [1, 1, 1],
            "items_completed": True,
        }
    finally:
        await database.aclose()


def test_expired_account_and_item_leases_reject_stale_client_timestamps(
    deletion_postgres: _PostgresHarness,
) -> None:
    """@brief 历史 client 时间不能复活已过期 account/item lease / Historical client time cannot revive expired account or item leases."""

    deletion_postgres.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES (
            'user_stale_account', 'subject-stale-account', 'Stale Account',
            'stale-account@example.test', 'stale-account@example.test',
            true, 'deletion_scheduled', 'en'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_stale_account', 'user_stale_account',
            'Stale Account Workspace', 'stale-account', 'personal', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status
        ) VALUES (
            'member_stale_account', 'workspace_stale_account',
            'user_stale_account', 'user_stale_account',
            'Stale Account', 'owner', 'active'
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_stale_account', 'user_stale_account',
            'scheduled', '2000-01-01T00:00:00Z'
        );
        """
    )
    account_hash = "a" * 64
    with psycopg.connect(deletion_postgres.app_sync_dsn, row_factory=dict_row) as connection:
        claim = connection.execute(
            """
            SELECT request_id, expected_revision
            FROM identity.claim_due_account_deletions(%s, %s, 300, 1, 12)
            """,
            (account_hash, datetime.now(UTC)),
        ).fetchone()
    assert claim is not None
    assert claim["request_id"] == "deletion_stale_account"
    revision = int(claim["expected_revision"])
    deletion_postgres.execute(
        """
        UPDATE identity.account_deletion_requests
        SET started_at = statement_timestamp() - interval '10 seconds',
            lease_expires_at = statement_timestamp() - interval '1 second'
        WHERE id = 'deletion_stale_account'
        """
    )
    with psycopg.connect(deletion_postgres.app_sync_dsn, row_factory=dict_row) as connection:
        erased = connection.execute(
            """
            SELECT * FROM identity.erase_account_for_deletion(%s, %s, %s, %s)
            """,
            (
                "deletion_stale_account",
                account_hash,
                revision,
                datetime(2000, 1, 1, tzinfo=UTC),
            ),
        ).fetchall()
        finalized = connection.execute(
            """
            SELECT identity.finalize_account_deletion(
                %s, %s, %s, 'completed', NULL, NULL, %s
            ) AS finalized
            """,
            (
                "deletion_stale_account",
                account_hash,
                revision,
                datetime(2000, 1, 1, tzinfo=UTC),
            ),
        ).fetchone()
    assert erased == []
    assert finalized == {"finalized": False}
    stale_user = deletion_postgres.rows(
        """
        SELECT account_status, email
        FROM identity.users
        WHERE id = 'user_stale_account'
        """
    )[0]
    assert stale_user == {
        "account_status": "suspended",
        "email": "stale-account@example.test",
    }

    deletion_postgres.execute(
        """
        INSERT INTO identity.users (
            id, external_subject, display_name, email, email_canonical,
            email_verified, account_status, locale
        ) VALUES (
            'user_stale_item01', 'subject-stale-item', 'Stale Item',
            'stale-item@example.test', 'stale-item@example.test',
            true, 'deletion_scheduled', 'en'
        );
        INSERT INTO identity.workspaces (
            id, resource_owner_id, name, slug, plan, data_region
        ) VALUES (
            'workspace_stale_item01', 'user_stale_item01',
            'Stale Item Workspace', 'stale-item', 'personal', 'global'
        );
        INSERT INTO identity.workspace_members (
            id, workspace_id, resource_owner_id, user_id, display_name, role, status
        ) VALUES (
            'member_stale_item01', 'workspace_stale_item01',
            'user_stale_item01', 'user_stale_item01',
            'Stale Item', 'owner', 'active'
        );
        INSERT INTO identity.account_deletion_requests (
            id, user_id, status, scheduled_for
        ) VALUES (
            'deletion_stale_item01', 'user_stale_item01',
            'scheduled', '1999-01-01T00:00:00Z'
        );
        """
    )
    second_account_hash = "b" * 64
    item_hash = "c" * 64
    with psycopg.connect(deletion_postgres.app_sync_dsn, row_factory=dict_row) as connection:
        second_claim = connection.execute(
            """
            SELECT request_id, expected_revision
            FROM identity.claim_due_account_deletions(%s, %s, 300, 1, 12)
            """,
            (second_account_hash, datetime.now(UTC)),
        ).fetchone()
        assert second_claim is not None
        assert second_claim["request_id"] == "deletion_stale_item01"
        second_revision = int(second_claim["expected_revision"])
        item = connection.execute(
            """
            SELECT workspace_id, resource_kind, resource_id
            FROM identity.claim_account_deletion_erasure_items(
                %s, %s, %s, %s, %s, 120, 1
            )
            """,
            (
                "deletion_stale_item01",
                second_account_hash,
                second_revision,
                item_hash,
                datetime.now(UTC),
            ),
        ).fetchone()
    assert item is not None
    deletion_postgres.execute(
        """
        UPDATE identity.account_deletion_erasure_items
        SET lease_expires_at = statement_timestamp() - interval '1 second'
        WHERE request_id = 'deletion_stale_item01'
          AND workspace_id = 'workspace_stale_item01'
          AND resource_kind = 'credential_scope'
          AND resource_id = 'user_stale_item01'
        """
    )
    with psycopg.connect(deletion_postgres.app_sync_dsn, row_factory=dict_row) as connection:
        retried = connection.execute(
            """
            SELECT identity.retry_account_deletion_erasure_item(
                %s, %s, %s, %s, %s, %s, %s,
                'account_deletion.test_retry', false
            ) AS retried
            """,
            (
                "deletion_stale_item01",
                second_account_hash,
                second_revision,
                item["workspace_id"],
                item["resource_kind"],
                item["resource_id"],
                item_hash,
            ),
        ).fetchone()
    assert retried == {"retried": False}
    item_state = deletion_postgres.rows(
        """
        SELECT status, lease_token_hash
        FROM identity.account_deletion_erasure_items
        WHERE request_id = 'deletion_stale_item01'
          AND resource_kind = 'credential_scope'
        """
    )[0]
    assert item_state == {"status": "processing", "lease_token_hash": item_hash}
