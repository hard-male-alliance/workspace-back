"""@brief API V2 Agent PostgreSQL 原位迁移与并发边界 / API V2 Agent PostgreSQL migration and concurrency boundaries."""

from __future__ import annotations

import asyncio
import getpass
import json
import shutil
import socket
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg.rows import dict_row
from pydantic import TypeAdapter

from backend.application.agent_v2 import (
    AgentApplicationService,
    AgentMutationContext,
    AgentPreconditionFailed,
    AgentWorkerService,
    CreateConversationCommand,
    CreateMessageCommand,
)
from backend.application.agent_worker import AGENT_WORK_EVENT_TYPES, AgentRunOutboxHandler
from backend.application.outbox_dispatch import (
    OutboxDispatchService,
    OutboxDispatchSettings,
)
from backend.application.ports.agent_v2 import (
    AgentCasMismatch,
    AgentModelRoute,
    AgentProposalFailure,
    AgentResumeProposalCommand,
    AgentToolDecisionClaim,
    MessageSequenceReservation,
    ToolExecutionReceipt,
)
from backend.application.ports.outbox_dispatch import OutboxDispatchClaim, OutboxLease
from backend.domain.agent_v2 import (
    AgentOutboxId,
    AgentOutputMode,
    AgentProviderApprovalRequired,
    AgentProviderOutcome,
    AgentProviderRequest,
    AgentResumeContext,
    AgentResumeOperationDraft,
    AgentRunId,
    AgentRunQueuedDispatch,
    AgentRunSpec,
    ConversationCapability,
    ConversationId,
    MessageId,
    TextContentPart,
    ToolCallBinding,
    ToolCallId,
    ToolRisk,
)
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.principals import ClientId, Scope, Subject, TokenPrincipal, UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resumes import (
    PageSize,
    ResumeDocument,
    ResumeId,
    ResumeOperationId,
    ResumeSectionKind,
    SetResumeField,
    TemplatePolicy,
    TemplateRef,
    TemplateZonePolicy,
    create_resume_document,
)
from backend.infrastructure.agent_v2 import (
    PostgresAgentUnitOfWorkFactory,
    PostgresAgentWorkerUnitOfWorkFactory,
)
from backend.infrastructure.outbox_dispatch import PostgresOutboxClaimRepository
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions
from backend.infrastructure.persistence.models import (
    Base,
    ResumeDocumentRecord,
    ResumeRevisionRecord,
)
from backend.infrastructure.resumes import encode_resume_operation

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 固定测试时刻 / Fixed test instant."""


@dataclass(frozen=True, slots=True)
class _PostgresHarness:
    """@brief 隔离的真实 PostgreSQL harness / Isolated real-PostgreSQL harness."""

    port: int
    socket_dir: Path
    superuser: str

    def migration_dsn(self, database: str = "aiws") -> str:
        """@brief 返回 migrator async DSN / Return a migrator async DSN."""
        return f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/{database}"

    def app_dsn(self, database: str = "aiws") -> str:
        """@brief 返回 app async DSN / Return an application async DSN."""
        return f"postgresql+asyncpg://aiws_app@127.0.0.1:{self.port}/{database}"

    def app_psycopg_dsn(self, database: str = "aiws") -> str:
        """@brief 返回 app psycopg DSN / Return an application psycopg DSN."""
        return f"postgresql://aiws_app@127.0.0.1:{self.port}/{database}"

    def super_dsn(self, database: str = "aiws") -> str:
        """@brief 返回 cluster-superuser DSN / Return a cluster-superuser DSN."""
        return f"postgresql://{self.superuser}@127.0.0.1:{self.port}/{database}"

    def psql(self, binary: Path, sql: str, *, database: str = "aiws") -> None:
        """@brief 通过本地 socket 执行 SQL / Execute SQL over the local socket."""
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

    def rows(self, statement: str, *, database: str = "aiws") -> list[dict[str, Any]]:
        """@brief 用 cluster superuser 查询验证行 / Query verification rows as cluster superuser."""
        with psycopg.connect(self.super_dsn(database), row_factory=dict_row) as connection:
            return [dict(row) for row in connection.execute(statement).fetchall()]


def _postgres_binary(name: str) -> Path | None:
    """@brief 定位 PostgreSQL binary / Locate a PostgreSQL binary."""
    direct = shutil.which(name)
    if direct is not None:
        return Path(direct)
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return candidates[0] if candidates else None


def _migration_config(dsn: str) -> Config:
    """@brief 构造显式 Alembic 配置 / Build an explicit Alembic configuration."""
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


_LEGACY_AGENT_FIXTURE_SQL = r"""
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    'user_agentlegacy1', 'agent-legacy-subject', 'Agent Legacy Owner',
    'agent-legacy@example.com', true, 'agent-legacy@example.com', 'zh-CN', 'active',
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    'workspace_agentlegacy1', 'user_agentlegacy1', 'Agent Legacy Workspace', 'zh-CN',
    'agent-legacy-workspace', 'team', 'global', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO identity.workspace_members (
    id, workspace_id, resource_owner_id, user_id, display_name, role, status,
    joined_at, created_at, updated_at, revision, extensions
) VALUES (
    'membership_agentlegacy1', 'workspace_agentlegacy1', 'user_agentlegacy1',
    'user_agentlegacy1', 'Agent Legacy Owner', 'owner', 'active',
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO agent.conversations (
    id, workspace_id, resource_owner_id, title, capability, archived_at, deleted_at,
    created_at, updated_at, revision, extensions
) VALUES (
    'conversation_legacy01', 'workspace_agentlegacy1', 'user_agentlegacy1',
    'Legacy Agent Conversation', 'general', NULL, NULL,
    '2026-07-02T00:00:00Z', '2026-07-02T00:20:00Z', 1, '{"keep":"conversation"}'
);
INSERT INTO agent.messages (
    id, workspace_id, resource_owner_id, conversation_id, sequence, role,
    content_parts, final_at, model_metadata,
    created_at, updated_at, revision, extensions
) VALUES
(
    'message_legacyinput1', 'workspace_agentlegacy1', 'user_agentlegacy1',
    'conversation_legacy01', 1, 'user', '[{"type":"text","text":"hello"}]',
    '2026-07-02T00:05:00Z', '{}', '2026-07-02T00:05:00Z',
    '2026-07-02T00:05:00Z', 1, '{"keep":"input"}'
),
(
    'message_legacyoutput1', 'workspace_agentlegacy1', 'user_agentlegacy1',
    'conversation_legacy01', 2, 'assistant', '[{"type":"text","text":"world"}]',
    '2026-07-02T00:15:00Z', '{}', '2026-07-02T00:15:00Z',
    '2026-07-02T00:15:00Z', 1,
    '{"keep":"output","agent_v2":{"source_run_id":"agent_run_legacy01","parent_message_id":"message_legacyinput1"}}'
);
INSERT INTO agent.jobs (
    id, workspace_id, resource_owner_id, job_type, status, phase,
    completed_units, total_units, progress_unit, target_resource_type, target_resource_id,
    result_refs, problem, started_at, finished_at, request_payload,
    created_at, updated_at, revision, extensions
) VALUES (
    'agent_job_legacy01', 'workspace_agentlegacy1', 'user_agentlegacy1',
    'agent.run', 'succeeded', 'completed', 1, 1, 'steps', 'agent_run',
    'agent_run_legacy01', '[]', NULL, '2026-07-02T00:11:00Z',
    '2026-07-02T00:15:00Z', '{}', '2026-07-02T00:10:00Z',
    '2026-07-02T00:15:00Z', 2, '{"keep":"job"}'
);
INSERT INTO agent.runs (
    id, workspace_id, resource_owner_id, conversation_id, input_message_id, job_id,
    capability, status, response_locale, inference_intent, effective_knowledge_selection,
    provider, model, model_revision, token_usage, cost, error, started_at, finished_at,
    created_at, updated_at, revision, extensions
) VALUES (
    'agent_run_legacy01', 'workspace_agentlegacy1', 'user_agentlegacy1',
    'conversation_legacy01', 'message_legacyinput1', 'agent_job_legacy01',
    'general', 'succeeded', 'zh-CN',
    '{"quality_tier":"balanced","latency_budget_ms":10000,"cost_tier":"standard","data_region":"global","allow_provider_fallback":false,"allow_external_model_processing":false}',
    '{"mode":"none","include_source_ids":[],"exclude_source_ids":[],"pinned_versions":[],"agent_scope":"general_agent"}',
    NULL, NULL, NULL, '{}', '{}', NULL, '2026-07-02T00:11:00Z',
    '2026-07-02T00:15:00Z', '2026-07-02T00:10:00Z',
    '2026-07-02T00:15:00Z', 2,
    '{
      "keep":"run",
      "agent_v2":{
        "spec":{
          "conversation_id":"conversation_legacy01",
          "input_message_id":"message_legacyinput1",
          "capability":"general",
          "context_refs":[],
          "knowledge":{"mode":"none","include_source_ids":[],"exclude_source_ids":[],"pinned_versions":[],"agent_scope":"general_agent"},
          "inference":{"quality_tier":"balanced","latency_budget_ms":10000,"cost_tier":"standard","data_region":"global","allow_provider_fallback":false,"allow_external_model_processing":false},
          "output_modes":["text"],
          "response_locale":"zh-CN"
        },
        "execution_grant":{
          "session_ref":{"resource_type":"conversation","id":"conversation_legacy01","revision":1},
          "agent_scope":"general_agent",
          "model_ref":{"resource_type":"model","id":"model_policy_legacy1","revision":1},
          "model_region":"global",
          "external_model_processing":false,
          "context_refs":[],
          "knowledge_contexts":[],
          "policy_version":1
        },
        "output_message_id":"message_legacyoutput1",
        "proposal_refs":[],
        "usage":{"input_tokens":4,"output_tokens":2,"cost_micro_usd":"7"}
      }
    }'
);
INSERT INTO agent.tool_approvals (
    id, workspace_id, resource_owner_id, run_id, tool_name, request_payload,
    status, decision_payload, decided_by_actor_id, decided_at, expires_at,
    created_at, updated_at, revision, extensions
) VALUES (
    'approval_legacy001', 'workspace_agentlegacy1', 'user_agentlegacy1',
    'agent_run_legacy01', 'calendar.create_event', '{}', 'pending', NULL, NULL, NULL,
    '2026-07-03T00:00:00Z', '2026-07-02T00:12:00Z',
    '2026-07-02T00:12:00Z', 1,
    '{
      "keep":"approval",
      "agent_v2":{
        "tool_call_id":"tool_call_legacy001",
        "summary":"Create a follow-up event",
        "risk":"high",
        "invocation_ref":{"resource_type":"tool_invocation","id":"invocation_legacy001","revision":1}
      }
    }'
);
"""
"""@brief 可被 0021 无损表达的 0020 Agent fixture / 0020 fixture losslessly representable by 0021."""

_DISPATCH_OWNERSHIP_FIXTURE_SQL = r"""
BEGIN;
SELECT set_config('app.actor_id', 'user_dispatchowner1', true),
       set_config('app.workspace_id', 'workspace_dispatch01', true);
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    'user_dispatchowner1', 'dispatch-owner-subject', 'Dispatch Owner',
    'dispatch-owner@example.com', true, 'dispatch-owner@example.com', 'en', 'active',
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    'workspace_dispatch01', 'user_dispatchowner1', 'Dispatch Ownership', 'en',
    'dispatch-ownership', 'team', 'global', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO agent.outbox_events (
    id, workspace_id, resource_owner_id, aggregate_type, aggregate_id,
    subject_revision, event_type, sequence, occurred_at, payload, trace_id,
    replay_expires_at, status, created_at, updated_at, revision, extensions
) VALUES
(
    'event_agentdispatch1', 'workspace_dispatch01', 'user_dispatchowner1',
    'agent_run', 'agent_run_dispatch01', 1, 'agent.run.queued', 0,
    '2026-07-23T00:00:00Z',
    '{"actor_id":"user_dispatchowner1","run_id":"agent_run_dispatch01","job_id":"agent_job_dispatch01"}',
    NULL, '2027-07-23T00:00:00Z', 'pending', '2026-07-23T00:00:00Z',
    '2026-07-23T00:00:00Z', 1, '{}'
),
(
    'event_agentdecision1', 'workspace_dispatch01', 'user_dispatchowner1',
    'agent_run', 'agent_run_dispatch01', 4, 'agent.tool_decision.recorded', 1,
    '2026-07-23T00:00:00.500000Z',
    '{"actor_id":"user_dispatchowner1","run_id":"agent_run_dispatch01","run_revision":4,"job_id":"agent_job_dispatch01","job_revision":4,"approval_id":"approval_dispatch01","approval_revision":2,"tool_call_id":"tool_call_dispatch01","decision":"approve"}',
    NULL, '2027-07-23T00:00:00Z', 'pending', '2026-07-23T00:00:00Z',
    '2026-07-23T00:00:00Z', 1, '{}'
),
(
    'event_knowledgedispatch1', 'workspace_dispatch01', 'user_dispatchowner1',
    'knowledge_source', 'source_dispatch0001', 1, 'knowledge_source.job_created', 0,
    '2026-07-23T00:00:01Z', '{"job_id":"knowledge_job_dispatch01"}', NULL,
    '2027-07-23T00:00:01Z', 'pending', '2026-07-23T00:00:01Z',
    '2026-07-23T00:00:01Z', 1, '{}'
),
(
    'event_interviewdispatch1', 'workspace_dispatch01', 'user_dispatchowner1',
    'interview_session', 'interview_dispatch01', 1, 'interview.report.queued', 0,
    '2026-07-23T00:00:02Z', '{"job_id":"interview_job_dispatch01"}', NULL,
    '2027-07-23T00:00:02Z', 'pending', '2026-07-23T00:00:02Z',
    '2026-07-23T00:00:02Z', 1, '{}'
);
COMMIT;
"""
"""@brief 两类 Agent 工作与两个其他 consumer domain 事件 / Two Agent work types and two other consumer-domain events."""

_EXHAUSTION_IDENTITY_FIXTURE_SQL = r"""
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    'user_dispatchowner1', 'dispatch-owner-subject', 'Dispatch Owner',
    'dispatch-owner@example.com', true, 'dispatch-owner@example.com', 'en', 'active',
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    'workspace_dispatch01', 'user_dispatchowner1', 'Dispatch Exhaustion', 'en',
    'dispatch-exhaustion', 'team', 'global', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO identity.workspace_members (
    id, workspace_id, resource_owner_id, user_id, display_name, role, status,
    joined_at, created_at, updated_at, revision, extensions
) VALUES (
    'membership_dispatch001', 'workspace_dispatch01', 'user_dispatchowner1',
    'user_dispatchowner1', 'Dispatch Owner', 'owner', 'active',
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{}'
);
"""
"""@brief 不含预置 outbox 的耗尽集成身份 fixture / Exhaustion-integration identity fixture without seeded outbox rows."""


@pytest.fixture(scope="module")
def agent_postgres(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_PostgresHarness]:
    """@brief 启动 PostgreSQL 并执行非空 0020→0021 / Start PostgreSQL and run a non-empty 0020-to-0021 migration."""
    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("agent-postgres")
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
            CREATE DATABASE aiws_invalid OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_invalid TO aiws_owner;
            CREATE DATABASE aiws_dispatch OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_dispatch TO aiws_owner;
            CREATE DATABASE aiws_exhaustion OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_exhaustion TO aiws_owner;
            """,
            database="postgres",
        )
        for database in (
            "aiws",
            "aiws_empty",
            "aiws_invalid",
            "aiws_dispatch",
            "aiws_exhaustion",
        ):
            try:
                harness.psql(psql, "CREATE EXTENSION vector;", database=database)
            except subprocess.CalledProcessError:
                pytest.skip("the PostgreSQL vector extension is unavailable")
        configuration = _migration_config(harness.migration_dsn())
        command.upgrade(configuration, "20260723_0020")
        harness.psql(psql, _LEGACY_AGENT_FIXTURE_SQL)
        command.upgrade(configuration, "20260723_0021")
        command.upgrade(
            _migration_config(harness.migration_dsn("aiws_dispatch")),
            "20260723_0023",
        )
        harness.psql(
            psql,
            _DISPATCH_OWNERSHIP_FIXTURE_SQL,
            database="aiws_dispatch",
        )
        command.upgrade(
            _migration_config(harness.migration_dsn("aiws_exhaustion")),
            "20260723_0023",
        )
        harness.psql(
            psql,
            _EXHAUSTION_IDENTITY_FIXTURE_SQL,
            database="aiws_exhaustion",
        )
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


def test_0021_real_postgres_preserves_representable_agent_state(
    agent_postgres: _PostgresHarness,
) -> None:
    """@brief 非空迁移保留 typed truth 并退休 run_events / Preserve typed truth and retire run_events."""
    assert agent_postgres.rows("SELECT version_num FROM identity.alembic_version") == [
        {"version_num": "20260723_0021"}
    ]
    conversation = agent_postgres.rows(
        "SELECT title, status, message_sequence, extensions "
        "FROM agent.conversations WHERE id = 'conversation_legacy01'"
    )[0]
    assert conversation == {
        "title": "Legacy Agent Conversation",
        "status": "active",
        "message_sequence": 2,
        "extensions": {"keep": "conversation"},
    }
    messages = agent_postgres.rows(
        "SELECT id, role, parent_message_id, source_run_id, extensions "
        "FROM agent.messages ORDER BY sequence"
    )
    assert messages[0]["role"] == "user"
    assert messages[0]["source_run_id"] is None
    assert messages[1] == {
        "id": "message_legacyoutput1",
        "role": "assistant",
        "parent_message_id": "message_legacyinput1",
        "source_run_id": "agent_run_legacy01",
        "extensions": {"keep": "output"},
    }
    run = agent_postgres.rows(
        "SELECT status, spec, execution_grant, output_message_id, proposal_refs, usage, extensions "
        "FROM agent.runs WHERE id = 'agent_run_legacy01'"
    )[0]
    assert run["status"] == "succeeded"
    assert run["spec"]["conversation_id"] == "conversation_legacy01"
    assert run["execution_grant"]["model_ref"]["id"] == "model_policy_legacy1"
    assert run["output_message_id"] == "message_legacyoutput1"
    assert run["proposal_refs"] == []
    assert run["usage"]["cost_micro_usd"] == "7"
    assert run["extensions"] == {"keep": "run"}
    approval = agent_postgres.rows(
        "SELECT tool_call_id, tool_name, summary, risk, invocation_type, invocation_id, "
        "invocation_revision, status, extensions FROM agent.tool_approvals"
    )[0]
    assert approval == {
        "tool_call_id": "tool_call_legacy001",
        "tool_name": "calendar.create_event",
        "summary": "Create a follow-up event",
        "risk": "high",
        "invocation_type": "tool_invocation",
        "invocation_id": "invocation_legacy001",
        "invocation_revision": 1,
        "status": "pending",
        "extensions": {"keep": "approval"},
    }
    assert agent_postgres.rows(
        "SELECT to_regclass('agent.run_events') AS relation"
    ) == [{"relation": None}]
    assert agent_postgres.rows(
        "SELECT count(*) AS count FROM identity.api_migration_audits "
        "WHERE migration_id = 'api-v2-agent-persistence-0021'"
    ) == [{"count": 1}]
    assert agent_postgres.rows(
        "SELECT character_maximum_length AS maximum_length "
        "FROM information_schema.columns WHERE table_schema = 'agent' "
        "AND table_name = 'runs' AND column_name = 'status'"
    ) == [{"maximum_length": 32}]


def test_0021_installs_workspace_rls_and_column_level_mutation_boundary(
    agent_postgres: _PostgresHarness,
) -> None:
    """@brief RLS 隔离 Workspace 且 immutable 列不可更新 / RLS isolates workspaces and immutable columns cannot be updated."""
    policies = agent_postgres.rows(
        "SELECT tablename, policyname FROM pg_policies "
        "WHERE schemaname = 'agent' AND tablename IN "
        "('conversations','messages','runs','tool_approvals') "
        "AND policyname LIKE 'agent_v2_%' ORDER BY tablename, policyname"
    )
    assert {row["policyname"] for row in policies} == {
        "agent_v2_workspace_select",
        "agent_v2_workspace_insert",
        "agent_v2_workspace_update",
    }
    with psycopg.connect(agent_postgres.app_psycopg_dsn()) as connection:
        connection.execute(
            "SELECT set_config('app.actor_id', %s, true), "
            "set_config('app.workspace_id', %s, true)",
            ("user_agentlegacy1", "workspace_agentlegacy1"),
        )
        assert connection.execute("SELECT count(*) FROM agent.runs").fetchone() == (1,)
        connection.rollback()
        connection.execute(
            "SELECT set_config('app.actor_id', %s, true), "
            "set_config('app.workspace_id', %s, true)",
            ("user_agentlegacy1", "workspace_other0001"),
        )
        assert connection.execute("SELECT count(*) FROM agent.runs").fetchone() == (0,)
        connection.rollback()
        connection.execute(
            "SELECT set_config('app.actor_id', %s, true), "
            "set_config('app.workspace_id', %s, true)",
            ("user_agentlegacy1", "workspace_agentlegacy1"),
        )
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute(
                "UPDATE agent.runs SET capability = 'knowledge_query' "
                "WHERE id = 'agent_run_legacy01'"
            )


def test_0021_real_postgres_empty_round_trip(agent_postgres: _PostgresHarness) -> None:
    """@brief 严格空态可升级并恢复 0020 形状 / A strict empty state can upgrade and restore the 0020 shape."""
    configuration = _migration_config(agent_postgres.migration_dsn("aiws_empty"))
    command.upgrade(configuration, "20260723_0021")
    command.downgrade(configuration, "20260723_0020")
    assert agent_postgres.rows(
        "SELECT version_num FROM identity.alembic_version", database="aiws_empty"
    ) == [{"version_num": "20260723_0020"}]
    assert agent_postgres.rows(
        "SELECT to_regclass('agent.run_events') AS relation", database="aiws_empty"
    ) == [{"relation": "agent.run_events"}]
    legacy_columns = agent_postgres.rows(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'agent' AND table_name = 'runs' "
        "AND column_name IN ('spec','response_locale') ORDER BY column_name",
        database="aiws_empty",
    )
    assert legacy_columns == [{"column_name": "response_locale"}]
    assert agent_postgres.rows(
        "SELECT character_maximum_length AS maximum_length "
        "FROM information_schema.columns WHERE table_schema = 'agent' "
        "AND table_name = 'runs' AND column_name = 'status'",
        database="aiws_empty",
    ) == [{"maximum_length": 16}]
    command.upgrade(configuration, "20260723_0021")
    assert agent_postgres.rows(
        "SELECT version_num FROM identity.alembic_version", database="aiws_empty"
    ) == [{"version_num": "20260723_0021"}]
    assert agent_postgres.rows(
        "SELECT to_regclass('agent.run_events') AS relation", database="aiws_empty"
    ) == [{"relation": None}]


def test_agent_run_events_has_no_parallel_orm_truth() -> None:
    """@brief ORM 仅注册统一 outbox 真相 / ORM registers only the unified outbox truth."""
    assert "agent.outbox_events" in Base.metadata.tables
    assert "agent.run_events" not in Base.metadata.tables


def test_0021_preflight_rejects_unrepresentable_legacy_before_ddl(
    agent_postgres: _PostgresHarness,
) -> None:
    """@brief 缺失冻结 grant 的 legacy 行在 DDL 前失败 / A legacy row missing its frozen grant fails before DDL."""
    psql = _postgres_binary("psql")
    assert psql is not None
    configuration = _migration_config(agent_postgres.migration_dsn("aiws_invalid"))
    command.upgrade(configuration, "20260723_0020")
    agent_postgres.psql(psql, _LEGACY_AGENT_FIXTURE_SQL, database="aiws_invalid")
    agent_postgres.psql(
        psql,
        "UPDATE agent.runs SET extensions = "
        "jsonb_set(extensions, '{agent_v2}', (extensions #> '{agent_v2}') - 'execution_grant');",
        database="aiws_invalid",
    )
    with pytest.raises(RuntimeError, match="lossless frozen spec/grant"):
        command.upgrade(configuration, "20260723_0021")
    assert agent_postgres.rows(
        "SELECT version_num FROM identity.alembic_version", database="aiws_invalid"
    ) == [{"version_num": "20260723_0020"}]
    assert agent_postgres.rows(
        "SELECT count(*) AS count FROM information_schema.columns "
        "WHERE table_schema = 'agent' AND table_name = 'runs' AND column_name = 'spec'",
        database="aiws_invalid",
    ) == [{"count": 0}]


@pytest.mark.asyncio
async def test_postgres_agent_consumer_cannot_claim_other_domain_work(
    agent_postgres: _PostgresHarness,
) -> None:
    """@brief Agent allowlist 在 SECURITY DEFINER 内隔离 Knowledge/Interview / The Agent allowlist isolates Knowledge/Interview inside SECURITY DEFINER."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            agent_postgres.app_dsn("aiws_dispatch"),
            pool_size=2,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=2_000,
        )
    )
    repository = PostgresOutboxClaimRepository(
        database,
        event_types=AGENT_WORK_EVENT_TYPES,
    )
    try:
        claims = await repository.claim(
            lease=OutboxLease("agent-dispatch-ownership-lease-with-adequate-entropy"),
            now=NOW,
            lease_seconds=30,
            batch_size=10,
            maximum_attempts=3,
        )
        assert [claim.event_type for claim in claims] == [
            "agent.run.queued",
            "agent.tool_decision.recorded",
        ]
        assert claims[0].actor_id == UserId("user_dispatchowner1")
        assert claims[0].subject == ResourceRef("agent_run", "agent_run_dispatch01", 1)
        assert claims[1].subject == ResourceRef("agent_run", "agent_run_dispatch01", 4)
        states = agent_postgres.rows(
            "SELECT event_type, status, attempt_count FROM agent.outbox_events "
            "ORDER BY event_type",
            database="aiws_dispatch",
        )
        assert states == [
            {"event_type": "agent.run.queued", "status": "processing", "attempt_count": 1},
            {
                "event_type": "agent.tool_decision.recorded",
                "status": "processing",
                "attempt_count": 1,
            },
            {
                "event_type": "interview.report.queued",
                "status": "pending",
                "attempt_count": 0,
            },
            {
                "event_type": "knowledge_source.job_created",
                "status": "pending",
                "attempt_count": 0,
            },
        ]
    finally:
        await database.aclose()


class _FixedClock:
    """@brief 确定性测试时钟 / Deterministic test clock."""

    def now(self) -> datetime:
        """@brief 返回固定 UTC 时刻 / Return a fixed UTC instant."""
        return NOW


class _SequentialIds:
    """@brief 生成进程内确定性 opaque IDs / Generate deterministic in-process opaque IDs."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        """@brief 为每个 prefix 分配单调 ID / Allocate a monotonic ID per prefix."""
        count = self._counts.get(prefix, 0) + 1
        self._counts[prefix] = count
        return f"{prefix}_pg{count:08d}"


def _model_route() -> AgentModelRoute:
    """@brief 构造显式 global model route / Build an explicit global model route."""
    return AgentModelRoute(
        ResourceRef("model", "model_route_global1", 1),
        ModelRegion.GLOBAL,
        False,
    )


def _principal() -> TokenPrincipal:
    """@brief 构造与 legacy 本地账户绑定的 principal / Build a principal bound to the legacy local account."""
    return TokenPrincipal(
        UserId("user_agentlegacy1"),
        Subject("agent-legacy-subject"),
        ClientId("client_agenttests1"),
        frozenset({Scope("workspace.read"), Scope("workspace.write")}),
    )


def _run_spec(conversation_id: ConversationId, message_id: MessageId) -> AgentRunSpec:
    """@brief 构造无 Knowledge 的最小合法 Run spec / Build a minimal valid Run spec without Knowledge."""
    return AgentRunSpec(
        conversation_id,
        message_id,
        ConversationCapability.GENERAL,
        (),
        KnowledgeSelection(KnowledgeSelectionMode.NONE, (), (), (), "general_agent"),
        InferenceIntent(
            InferenceQualityTier.BALANCED,
            10_000,
            InferenceCostTier.STANDARD,
            ModelRegion.GLOBAL,
            False,
            False,
        ),
        (AgentOutputMode.TEXT,),
        "zh-CN",
    )


@pytest.mark.asyncio
async def test_postgres_agent_run_job_outbox_audit_and_cas_are_atomic(
    agent_postgres: _PostgresHarness,
) -> None:
    """@brief 实库创建与取消原子覆盖 Run、Job、outbox、audit 和 CAS / Real creation and cancellation atomically cover Run, Job, outbox, audit, and CAS."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            agent_postgres.app_dsn(),
            pool_size=3,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=2_000,
        )
    )
    factory = PostgresAgentUnitOfWorkFactory(database, model_routes=(_model_route(),))
    worker_factory = PostgresAgentWorkerUnitOfWorkFactory(
        database, model_routes=(_model_route(),)
    )
    ids = _SequentialIds()
    service = AgentApplicationService(factory, clock=_FixedClock(), id_factory=ids)
    principal = _principal()
    workspace_id = WorkspaceId("workspace_agentlegacy1")
    try:
        conversation = await service.create_conversation(
            principal,
            workspace_id,
            CreateConversationCommand(ConversationCapability.GENERAL, "Postgres atomic flow"),
            AgentMutationContext("request_agentcreate1"),
        )
        message = await service.create_message(
            principal,
            workspace_id,
            conversation.meta.id,
            CreateMessageCommand(None, (TextContentPart("create a run"),)),
            expected_conversation_revision=1,
            context=AgentMutationContext("request_agentmessage1"),
        )
        run = await service.create_agent_run(
            principal,
            workspace_id,
            _run_spec(conversation.meta.id, message.meta.id),
            AgentMutationContext("request_agentrun001"),
        )
        dispatch_rows = agent_postgres.rows(
            "SELECT resource_owner_id, payload FROM agent.outbox_events "
            f"WHERE aggregate_id = '{run.meta.id}' AND event_type = 'agent.run.queued'"
        )
        assert len(dispatch_rows) == 1
        assert dispatch_rows[0]["resource_owner_id"] == "user_agentlegacy1"
        assert dispatch_rows[0]["payload"]["actor_id"] == "user_agentlegacy1"
        queued_dispatch = AgentRunQueuedDispatch(
            id=AgentOutboxId(
                dispatch_rows[0]["payload"].get("outbox_id", "outbox_recovery001")
            ),
            workspace_id=workspace_id,
            actor_id=UserId(dispatch_rows[0]["payload"]["actor_id"]),
            run_ref=ResourceRef("agent_run", str(run.meta.id), run.meta.revision),
            job_ref=ResourceRef("job", dispatch_rows[0]["payload"]["job_id"], 1),
            occurred_at=NOW,
        )
        assert queued_dispatch.actor_id == UserId("user_agentlegacy1")

        async with worker_factory(workspace_id, queued_dispatch.actor_id) as unit:
            recovered = await unit.repository.get_run(
                workspace_id, AgentRunId(str(run.meta.id)), for_update=True
            )
            assert recovered is not None
        async with worker_factory(workspace_id, UserId("user_attacker0001")) as unit:
            assert (
                await unit.repository.get_run(
                    workspace_id, AgentRunId(str(run.meta.id)), for_update=True
                )
                is None
            )

        outcomes = await asyncio.gather(
            service.cancel_agent_run(
                principal,
                workspace_id,
                run.meta.id,
                expected_revision=1,
                context=AgentMutationContext("request_cancelrace1"),
            ),
            service.cancel_agent_run(
                principal,
                workspace_id,
                run.meta.id,
                expected_revision=1,
                context=AgentMutationContext("request_cancelrace2"),
            ),
            return_exceptions=True,
        )
        assert sum(not isinstance(item, BaseException) for item in outcomes) == 1
        assert sum(isinstance(item, AgentPreconditionFailed) for item in outcomes) == 1
        rows = agent_postgres.rows(
            "SELECT run.status AS run_status, job.status AS job_status "
            "FROM agent.runs AS run JOIN agent.jobs AS job ON job.id = run.job_id "
            f"WHERE run.id = '{run.meta.id}'"
        )
        assert rows == [{"run_status": "cancelled", "job_status": "cancelled"}]
        assert agent_postgres.rows(
            "SELECT count(*) AS count FROM agent.outbox_events "
            f"WHERE aggregate_id = '{run.meta.id}'"
        ) == [{"count": 2}]
        assert agent_postgres.rows(
            "SELECT count(*) AS count FROM identity.audit_events "
            f"WHERE resource_id = '{run.meta.id}'"
        ) == [{"count": 2}]
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_postgres_message_sequence_is_gap_free_and_revision_cas_has_one_winner(
    agent_postgres: _PostgresHarness,
) -> None:
    """@brief 并发 sequence 用单 SQL 串行化且同 revision CAS 仅一胜者 / Concurrent sequence allocation serializes in one SQL and same-revision CAS has one winner."""
    with psycopg.connect(agent_postgres.super_dsn()) as connection:
        connection.execute(
            """
            INSERT INTO agent.conversations (
                id, workspace_id, resource_owner_id, title, capability, status,
                message_sequence, deleted_at, created_at, updated_at, revision, extensions
            ) VALUES
            (
                'conversation_seqrace01', 'workspace_agentlegacy1', 'user_agentlegacy1',
                'sequence race', 'general', 'active', 0, NULL, %s, %s, 1, '{}'
            ),
            (
                'conversation_casrace01', 'workspace_agentlegacy1', 'user_agentlegacy1',
                'cas race', 'general', 'active', 0, NULL, %s, %s, 1, '{}'
            )
            """,
            (NOW, NOW, NOW, NOW),
        )
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            agent_postgres.app_dsn(),
            pool_size=4,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=2_000,
        )
    )
    factory = PostgresAgentWorkerUnitOfWorkFactory(
        database, model_routes=(_model_route(),)
    )
    workspace_id = WorkspaceId("workspace_agentlegacy1")
    actor_id = UserId("user_agentlegacy1")

    async def reserve(
        conversation_id: ConversationId,
        expected_revision: int | None,
    ) -> MessageSequenceReservation:
        """@brief 在独立 scoped transaction 分配序号 / Allocate in an independent scoped transaction."""
        async with factory(workspace_id, actor_id) as unit:
            reservation = await unit.repository.allocate_message_sequence(
                workspace_id,
                conversation_id,
                expected_conversation_revision=expected_revision,
                at=NOW,
            )
            await unit.commit()
            return reservation

    try:
        sequences = await asyncio.gather(
            reserve(ConversationId("conversation_seqrace01"), None),
            reserve(ConversationId("conversation_seqrace01"), None),
        )
        assert sorted(item.sequence for item in sequences) == [1, 2]
        cas_outcomes = await asyncio.gather(
            reserve(ConversationId("conversation_casrace01"), 1),
            reserve(ConversationId("conversation_casrace01"), 1),
            return_exceptions=True,
        )
        assert sum(isinstance(item, MessageSequenceReservation) for item in cas_outcomes) == 1
        assert sum(isinstance(item, AgentCasMismatch) for item in cas_outcomes) == 1
        assert agent_postgres.rows(
            "SELECT message_sequence, revision FROM agent.conversations "
            "WHERE id = 'conversation_seqrace01'"
        ) == [{"message_sequence": 2, "revision": 3}]
        assert agent_postgres.rows(
            "SELECT message_sequence, revision FROM agent.conversations "
            "WHERE id = 'conversation_casrace01'"
        ) == [{"message_sequence": 1, "revision": 2}]
    finally:
        await database.aclose()


_EXHAUSTION_NOW = datetime(2026, 7, 10, 12, tzinfo=UTC)
"""@brief 早于静态 dispatch fixture 的可 claim 时间 / Claimable time preceding the static dispatch fixture."""


class _ExhaustionClock:
    """@brief 固定耗尽补偿时钟 / Fixed exhaustion-compensation clock."""

    def now(self) -> datetime:
        """@brief 返回耗尽测试时刻 / Return the exhaustion-test instant."""
        return _EXHAUSTION_NOW


class _ApprovalModelProvider:
    """@brief 将真实 PG Run 推进到 waiting_for_approval / Advance a real PostgreSQL Run to waiting_for_approval."""

    async def execute(self, request: AgentProviderRequest) -> AgentProviderOutcome:
        """@brief 返回一个有界高风险工具建议 / Return one bounded high-risk tool proposal."""
        del request
        return AgentProviderApprovalRequired(
            ToolCallBinding(
                ToolCallId("tool_call_pgexhaust1"),
                "calendar.create_event",
                "Create an exhaustion test meeting",
                ToolRisk.HIGH,
                _EXHAUSTION_NOW + timedelta(days=1),
                ResourceRef("tool_invocation", "invocation_pgexhaust1", 1),
            )
        )


class _UnusedToolExecutor:
    """@brief 证明耗尽补偿不调用外部工具 / Prove exhaustion compensation never invokes a tool."""

    async def execute(
        self,
        dispatch: AgentToolDecisionClaim,
        invocation_ref: ResourceRef,
    ) -> ToolExecutionReceipt:
        """@brief 若被误调用则立即失败 / Fail immediately if called unexpectedly."""
        del dispatch, invocation_ref
        raise AssertionError("tool executor must not run during exhaustion compensation")


class _AllowExhaustionToolRegistry:
    """@brief 仅让耗尽补偿测试到达审批态 / Allow only the exhaustion test to reach approval."""

    def allows(
        self,
        request: AgentProviderRequest,
        binding: ToolCallBinding,
    ) -> bool:
        """@brief 验证测试调用后允许 / Validate and allow the test call."""
        return (
            request.run_id.startswith("run_pg")
            and binding.tool_name == "calendar.create_event"
        )


class _AlwaysFailingAgentHandler:
    """@brief 注入连续未知 handler 异常但保留真实耗尽补偿 / Inject unknown handler failures while retaining real exhaustion compensation."""

    def __init__(self, delegate: AgentRunOutboxHandler) -> None:
        """@brief 绑定真实 Agent 补偿 handler / Bind the real Agent compensation handler."""
        self._delegate = delegate

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 模拟无法分类的 handler 故障 / Simulate an unclassified handler failure."""
        del claim
        raise RuntimeError("secret injected dispatch failure")

    async def on_exhausted(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
    ) -> None:
        """@brief 委托真实 payload 独立补偿 / Delegate real payload-independent compensation."""
        await self._delegate.on_exhausted(claim, error_code=error_code)


def _dispatch_principal() -> TokenPrincipal:
    """@brief 构造 dispatch 数据库的 Workspace owner / Build the dispatch-database Workspace owner."""
    return TokenPrincipal(
        UserId("user_dispatchowner1"),
        Subject("dispatch-owner-subject"),
        ClientId("client_dispatchtests1"),
        frozenset({Scope("workspace.read"), Scope("workspace.write")}),
    )


@pytest.mark.asyncio
async def test_postgres_attempt_exhaustion_closes_run_job_before_outbox_failed(
    agent_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 PostgreSQL 中领域终态先提交，随后原事件才进入 failed / In real PostgreSQL the domain terminal state commits before the source event becomes failed."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            agent_postgres.app_dsn("aiws_exhaustion"),
            pool_size=3,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=2_000,
        )
    )
    routes = (_model_route(),)
    ids = _SequentialIds()
    service = AgentApplicationService(
        PostgresAgentUnitOfWorkFactory(database, model_routes=routes),
        clock=_ExhaustionClock(),
        id_factory=ids,
    )
    principal = _dispatch_principal()
    workspace_id = WorkspaceId("workspace_dispatch01")
    try:
        conversation = await service.create_conversation(
            principal,
            workspace_id,
            CreateConversationCommand(ConversationCapability.GENERAL, "exhaustion"),
            AgentMutationContext("request_pgexhaustconv1"),
        )
        message = await service.create_message(
            principal,
            workspace_id,
            conversation.meta.id,
            CreateMessageCommand(None, (TextContentPart("never reaches provider"),)),
            expected_conversation_revision=conversation.meta.revision,
            context=AgentMutationContext("request_pgexhaustmsg01"),
        )
        run = await service.create_agent_run(
            principal,
            workspace_id,
            _run_spec(conversation.meta.id, message.meta.id),
            AgentMutationContext("request_pgexhaustrun01"),
        )
        event = agent_postgres.rows(
            "SELECT id, payload FROM agent.outbox_events "
            f"WHERE aggregate_id = '{run.meta.id}' AND event_type = 'agent.run.queued'",
            database="aiws_exhaustion",
        )
        assert len(event) == 1

        worker = AgentWorkerService(
            PostgresAgentWorkerUnitOfWorkFactory(database, model_routes=routes),
            _ApprovalModelProvider(),
            _UnusedToolExecutor(),
            tool_registry=_AllowExhaustionToolRegistry(),
            clock=_ExhaustionClock(),
            id_factory=ids,
        )
        queued_dispatch = AgentRunQueuedDispatch(
            AgentOutboxId(event[0]["id"]),
            workspace_id,
            principal.user_id,
            ResourceRef("agent_run", str(run.meta.id), run.meta.revision),
            ResourceRef("job", event[0]["payload"]["job_id"], 1),
            _EXHAUSTION_NOW,
        )
        waiting = await worker.execute_run(queued_dispatch)
        assert waiting.pending_approval_id is not None
        handler = _AlwaysFailingAgentHandler(AgentRunOutboxHandler(worker))
        dispatcher = OutboxDispatchService(
            PostgresOutboxClaimRepository(database, event_types=AGENT_WORK_EVENT_TYPES),
            {event_type: handler for event_type in AGENT_WORK_EVENT_TYPES},
            required_event_types=AGENT_WORK_EVENT_TYPES,
            settings=OutboxDispatchSettings(
                batch_size=1,
                lease_seconds=30,
                maximum_attempts=1,
                retry_base_seconds=1,
                retry_cap_seconds=1,
            ),
            clock=lambda: datetime.now(UTC),
        )

        result = await dispatcher.run_once()

        assert result.failed == 1
        assert result.retried == result.completed == result.lost_leases == 0
        assert agent_postgres.rows(
            "SELECT run.status AS run_status, job.status AS job_status, "
            "run.problem->>'code' AS run_problem, job.problem->>'code' AS job_problem "
            "FROM agent.runs AS run JOIN agent.jobs AS job ON job.id = run.job_id "
            f"WHERE run.id = '{run.meta.id}'",
            database="aiws_exhaustion",
        ) == [
            {
                "run_status": "failed",
                "job_status": "failed",
                "run_problem": "agent.dispatch_exhausted",
                "job_problem": "agent.dispatch_exhausted",
            }
        ]
        assert agent_postgres.rows(
            "SELECT status, decision_by_type, decision_by_id "
            "FROM agent.tool_approvals "
            f"WHERE id = '{waiting.pending_approval_id}'",
            database="aiws_exhaustion",
        ) == [
            {
                "status": "rejected",
                "decision_by_type": "service",
                "decision_by_id": "agent_service",
            }
        ]
        assert agent_postgres.rows(
            "SELECT status, attempt_count, last_error_code FROM agent.outbox_events "
            f"WHERE id = '{event[0]['id']}'",
            database="aiws_exhaustion",
        ) == [
            {
                "status": "failed",
                "attempt_count": 1,
                "last_error_code": "outbox.handler_failed",
            }
        ]
    finally:
        await database.aclose()


def _proposal_resume_context() -> AgentResumeContext:
    """@brief 构造真实 PostgreSQL Proposal 测试的精确 SIR / Build the exact SIR for the real-PostgreSQL Proposal test."""

    template_ref = TemplateRef("template_pgproposal01", "1.0")
    kinds = frozenset(ResumeSectionKind)
    policy = TemplatePolicy(
        template_ref,
        frozenset({"zh-CN"}),
        frozenset({PageSize.A4}),
        frozenset({"pdf", "json"}),
        kinds,
        (TemplateZonePolicy("main", kinds, 100),),
        frozenset({"body.default"}),
        frozenset({"yyyy_mm"}),
        frozenset({"bullet.default"}),
        (),
    )
    document = create_resume_document(
        resume_id=ResumeId("resume_pgproposal01"),
        workspace_id=WorkspaceId("workspace_agentlegacy1"),
        title="Original Resume",
        locale="zh-CN",
        template_policy=policy,
        created_at=NOW,
        full_name="Klee",
    )
    return AgentResumeContext(
        ResourceRef("resume", str(document.meta.id), document.meta.revision),
        document,
    )


async def _insert_proposal_resume(
    database: AsyncDatabase,
    context: AgentResumeContext,
) -> None:
    """@brief 以 app role 插入精确 Resume root/revision fixture / Insert an exact Resume root/revision fixture as the app role."""

    adapter: TypeAdapter[ResumeDocument] = TypeAdapter(ResumeDocument)
    payload = adapter.dump_python(context.document, mode="json")
    if not isinstance(payload, dict):
        raise TypeError("Resume fixture codec must produce an object")
    digest = sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    session = database.new_session()
    transaction = await session.begin()
    try:
        await database.install_v2_request_scope(
            session,
            actor_id="user_agentlegacy1",
            workspace_id="workspace_agentlegacy1",
        )
        document = context.document
        session.add(
            ResumeDocumentRecord(
                id=str(document.meta.id),
                workspace_id=str(document.workspace_id),
                resource_owner_id="user_agentlegacy1",
                template_version_id=None,
                template_id=document.template.template_id,
                template_version=document.template.version,
                title=document.title,
                locale=document.locale,
                current_revision_no=document.meta.revision,
                deleted_at=None,
                created_at=document.meta.created_at,
                updated_at=document.meta.updated_at,
                revision=document.meta.revision,
                extensions={},
            )
        )
        session.add(
            ResumeRevisionRecord(
                id="resume_revision_pgproposal01",
                workspace_id=str(document.workspace_id),
                resource_owner_id="user_agentlegacy1",
                resume_id=str(document.meta.id),
                revision_no=document.meta.revision,
                semantic_document=payload,
                content_hash=digest,
                created_by_actor_id="user_agentlegacy1",
                source="v2",
                change_targets=[],
                created_at=document.meta.created_at,
                updated_at=document.meta.updated_at,
                revision=1,
                extensions={},
            )
        )
        await transaction.commit()
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_postgres_agent_resume_proposal_is_exact_idempotent_and_atomic(
    agent_postgres: _PostgresHarness,
) -> None:
    """@brief 实库验证精确快照、稳定重放与 UoW 回滚原子性 / Verify exact snapshots, stable replay, and UoW rollback atomicity in PostgreSQL."""

    database = AsyncDatabase(
        AsyncDatabaseOptions(
            agent_postgres.app_dsn(),
            pool_size=2,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=2_000,
        )
    )
    context = _proposal_resume_context()
    operation = encode_resume_operation(
        SetResumeField(
            ResumeOperationId("operation_draft_pg01"),
            context.resume_ref.id,
            ("title",),
            "Improved Resume",
        )
    )
    del operation["operation_id"]
    command_value = AgentResumeProposalCommand(
        WorkspaceId("workspace_agentlegacy1"),
        UserId("user_agentlegacy1"),
        AgentRunId("agent_run_legacy01"),
        context,
        "Improve the Resume title",
        (AgentResumeOperationDraft(operation),),
        (),
        NOW,
    )
    factory = PostgresAgentWorkerUnitOfWorkFactory(
        database,
        model_routes=(_model_route(),),
    )
    try:
        await _insert_proposal_resume(database, context)

        forged = replace(context.document, title="Forged snapshot")
        async with factory(
            command_value.workspace_id,
            command_value.actor_id,
        ) as unit:
            with pytest.raises(AgentProposalFailure) as captured:
                await unit.resume_proposals.create(
                    replace(
                        command_value,
                        base=AgentResumeContext(context.resume_ref, forged),
                    )
                )
        assert captured.value.problem.code == "agent.resume_context_stale"

        async with factory(
            command_value.workspace_id,
            command_value.actor_id,
        ) as unit:
            rolled_back_ref = await unit.resume_proposals.create(command_value)
        assert agent_postgres.rows(
            "SELECT count(*) AS count FROM resume.proposals "
            f"WHERE id = '{rolled_back_ref.id}'"
        ) == [{"count": 0}]

        async with factory(
            command_value.workspace_id,
            command_value.actor_id,
        ) as unit:
            committed_ref = await unit.resume_proposals.create(command_value)
            await unit.commit()
        assert committed_ref == rolled_back_ref

        async with factory(
            command_value.workspace_id,
            command_value.actor_id,
        ) as unit:
            replay_ref = await unit.resume_proposals.create(command_value)
            await unit.commit()
        assert replay_ref == committed_ref

        async with factory(
            command_value.workspace_id,
            command_value.actor_id,
        ) as unit:
            with pytest.raises(AgentProposalFailure) as conflict:
                await unit.resume_proposals.create(
                    replace(command_value, title="Conflicting replay")
                )
        assert conflict.value.problem.code == "agent.proposal_identity_conflict"

        proposal_rows = agent_postgres.rows(
            "SELECT proposal.title, proposal.base_revision_no, proposal.status, "
            "operation.operation_id, operation.operation_type, "
            "operation.payload->>'value' AS value "
            "FROM resume.proposals AS proposal "
            "JOIN resume.proposal_operations AS operation "
            "ON operation.proposal_id = proposal.id "
            f"WHERE proposal.id = '{committed_ref.id}'"
        )
        assert len(proposal_rows) == 1
        assert proposal_rows[0]["title"] == command_value.title
        assert proposal_rows[0]["base_revision_no"] == context.resume_ref.revision
        assert proposal_rows[0]["status"] == "pending"
        assert proposal_rows[0]["operation_type"] == "set_field"
        assert proposal_rows[0]["operation_id"].startswith("operation_")
        assert proposal_rows[0]["value"] == "Improved Resume"
        assert agent_postgres.rows(
            "SELECT title, current_revision_no FROM resume.documents "
            f"WHERE id = '{context.resume_ref.id}'"
        ) == [{"title": "Original Resume", "current_revision_no": 1}]
    finally:
        await database.aclose()
