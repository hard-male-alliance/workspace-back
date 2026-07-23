"""@brief API V2 Interview PostgreSQL migration 与持久化门禁 / API V2 Interview PostgreSQL migration and persistence gates."""

from __future__ import annotations

import asyncio
import getpass
import shutil
import socket
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg.rows import dict_row

from backend.application.interview_v2 import (
    CreateInterviewScenarioCommand,
    CreateInterviewSessionCommand,
    EndInterviewSessionCommand,
    InterviewApplicationService,
    InterviewMutationContext,
    InterviewWorkerService,
)
from backend.application.interview_worker import (
    INTERVIEW_WORK_EVENT_TYPES,
    InterviewJobOutboxHandler,
)
from backend.application.outbox_dispatch import (
    OutboxDispatchService,
    OutboxDispatchSettings,
)
from backend.application.platform import PlatformApplicationService
from backend.application.ports.interview_v2 import (
    EndSessionOutput,
    InterviewCasMismatch,
    InterviewWorkerOperationId,
    RealtimeInputKeyReused,
    ReportGenerationRequest,
)
from backend.application.ports.platform import MutationContext
from backend.domain.interview_v2 import (
    AvatarOutputMode,
    CreateRealtimeConnectionSpec,
    EndInterviewReason,
    FallbackTransport,
    InterviewAvatarPreferences,
    InterviewDifficulty,
    InterviewMediaPreferences,
    InterviewReportDraft,
    InterviewRubric,
    InterviewScenarioId,
    InterviewScenarioPatch,
    InterviewScenarioSpec,
    InterviewScenarioStatus,
    InterviewSession,
    InterviewSessionId,
    InterviewSessionStatus,
    JobTarget,
    RealtimeConnectionId,
    RealtimeConnectionLease,
    RealtimeControl,
    RealtimeControlInput,
    RealtimeInputEnvelope,
    RealtimeInputId,
    RealtimeInputLedgerRecord,
    RealtimeTransport,
    RecordingConsent,
    RubricDimension,
    ScoreScale,
    TranscriptSegment,
    TranscriptSegmentId,
    TranscriptSpeaker,
    realtime_input_fingerprint,
)
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.platform import (
    ApiArtifactContentUrl,
    Artifact,
    ArtifactId,
    ArtifactKind,
    JobStatus,
)
from backend.domain.principals import (
    ClientId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef
from backend.infrastructure.interview import (
    HmacInterviewRealtimeGateway,
    InterviewRealtimeSigningKey,
    InterviewRealtimeSigningKeyring,
    PostgresInterviewUnitOfWorkFactory,
)
from backend.infrastructure.outbox_dispatch import PostgresOutboxClaimRepository
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions
from backend.infrastructure.platform import PostgresPlatformUnitOfWorkFactory

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = (
    PROJECT_ROOT / "alembic" / "versions" / "20260723_0022_v2_interview_persistence.py"
)
"""@brief Interview V2 persistence migration / Interview V2 persistence migration."""

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 固定 persistence 测试时刻 / Fixed persistence-test instant."""


@dataclass(frozen=True, slots=True)
class _PostgresHarness:
    """@brief 隔离 PostgreSQL cluster / Isolated PostgreSQL cluster."""

    port: int
    socket_dir: Path
    superuser: str

    def migration_dsn(self, database: str = "aiws") -> str:
        """@brief 返回 migrator asyncpg DSN / Return a migrator asyncpg DSN."""
        return (
            f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/{database}"
        )

    def app_dsn(self, database: str = "aiws") -> str:
        """@brief 返回 app asyncpg DSN / Return an application asyncpg DSN."""
        return f"postgresql+asyncpg://aiws_app@127.0.0.1:{self.port}/{database}"

    def app_psycopg_dsn(self, database: str = "aiws") -> str:
        """@brief 返回 app psycopg DSN / Return an application psycopg DSN."""
        return f"postgresql://aiws_app@127.0.0.1:{self.port}/{database}"

    def superuser_dsn(self, database: str = "aiws") -> str:
        """@brief 返回 cluster superuser DSN / Return a cluster-superuser DSN."""
        return f"postgresql://{self.superuser}@127.0.0.1:{self.port}/{database}"

    def psql(
        self,
        binary: Path,
        sql: str,
        *,
        database: str = "aiws",
    ) -> None:
        """@brief 用 local socket 执行 fixture SQL / Execute fixture SQL through the local socket."""
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
        """@brief 以 superuser 读取验证行 / Read verification rows as the superuser."""
        with psycopg.connect(
            self.superuser_dsn(database),
            row_factory=dict_row,
        ) as connection:
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


_IDENTITY_FIXTURE_SQL = r"""
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    'user_interviewlegacy1', 'interview-legacy-subject', 'Interview Legacy Owner',
    'interview-legacy@example.com', true, 'interview-legacy@example.com', 'zh-CN',
    'active', '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    'workspace_interviewlegacy1', 'user_interviewlegacy1',
    'Interview Legacy Workspace', 'zh-CN', 'interview-legacy-workspace', 'team', 'cn',
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z', 1, '{}'
);
INSERT INTO identity.workspace_members (
    id, workspace_id, resource_owner_id, user_id, display_name, role, status,
    joined_at, created_at, updated_at, revision, extensions
) VALUES (
    'membership_interviewlegacy1', 'workspace_interviewlegacy1',
    'user_interviewlegacy1', 'user_interviewlegacy1', 'Interview Legacy Owner',
    'owner', 'active', '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{}'
);
"""
"""@brief Interview migration/runtime 共用真实租户 / Real tenant shared by Interview migration/runtime tests."""


_LEGACY_INTERVIEW_FIXTURE_SQL = r"""
INSERT INTO interview.scenarios (
    id, workspace_id, resource_owner_id, title, locale, role_target, rubric,
    is_template, deleted_at, created_at, updated_at, revision, extensions
) VALUES (
    'scenario_legacy01', 'workspace_interviewlegacy1', 'user_interviewlegacy1',
    'Legacy distributed systems', 'zh-CN', '{}', '{}', false, NULL,
    '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', 1,
    $scenario${
      "keep":"scenario",
      "v2":{
        "status":"active",
        "spec":{
          "name":"Legacy distributed systems",
          "description":"A losslessly migrated Interview scenario",
          "locale":"zh-CN",
          "interview_type":"system_design",
          "difficulty":"advanced",
          "duration_minutes":45,
          "target_question_count":8,
          "focus_areas":["consistency"],
          "allow_followups":true,
          "allow_barge_in":true,
          "rubric":{
            "rubric_id":"rubric_legacy01",
            "rubric_version":"1",
            "name":"System design",
            "dimensions":[{
              "dimension_id":"dimension_legacy01",
              "name":"Consistency",
              "description":"Explain consistency trade-offs",
              "weight":1.0,
              "observable_indicators":["Defines linearizability"],
              "scoring_scale":{"minimum":0,"maximum":100,"labels":{}}
            }],
            "overall_scale":{"minimum":0,"maximum":100,"labels":{}}
          }
        }
      }
    }$scenario$::jsonb
);
INSERT INTO interview.sessions (
    id, workspace_id, resource_owner_id, scenario_id, resume_revision_id, state,
    job_target, effective_knowledge_selection, inference_intent, media_capabilities,
    avatar_output_mode, consent, recording_retention_until, started_at, ended_at,
    failure, created_at, updated_at, revision, extensions
) VALUES (
    'session_legacy001', 'workspace_interviewlegacy1', 'user_interviewlegacy1',
    'scenario_legacy01', NULL, 'created', '{}', '{}', '{}', '{}', 'audio_only',
    '{}', '2026-08-01T00:00:00Z', NULL, NULL, NULL,
    '2026-07-02T01:00:00Z', '2026-07-02T01:00:00Z', 1,
    $session${
      "keep":"session",
      "v2":{
        "status":"created",
        "spec":{
          "scenario_id":"scenario_legacy01",
          "scenario_revision":1,
          "rubric_snapshot":{
            "rubric_id":"rubric_legacy01",
            "rubric_version":"1",
            "name":"System design",
            "dimensions":[{
              "dimension_id":"dimension_legacy01",
              "name":"Consistency",
              "description":"Explain consistency trade-offs",
              "weight":1.0,
              "observable_indicators":["Defines linearizability"],
              "scoring_scale":{"minimum":0,"maximum":100,"labels":{}}
            }],
            "overall_scale":{"minimum":0,"maximum":100,"labels":{}}
          },
          "resume_ref":null,
          "job_target":{
            "title":"Senior Engineer","company":null,"location":null,
            "description":null,"source_url":null,"seniority":"senior",
            "skills":["distributed-systems"]
          },
          "knowledge":{
            "mode":"none","include_source_ids":[],"exclude_source_ids":[],
            "pinned_versions":[],"agent_scope":"interview_agent"
          },
          "locale":"zh-CN",
          "media":{
            "user_audio":true,"user_video":false,"screen_share":false,
            "max_video_width":1920,"max_video_height":1080,"max_video_fps":30,
            "avatar":{
              "output_mode":"audio_only","avatar_id":null,"voice_id":"voice_legacy01",
              "preferred_audio_codecs":["opus"],"preferred_video_codecs":[],
              "include_visemes":false,"include_expression_cues":false
            },
            "fallback_transport":"websocket"
          },
          "recording":{
            "record_audio":false,"record_video":false,"store_transcript":true,
            "retention_days":30,"consented_at":"2026-07-01T00:00:00Z",
            "consent_version":"consent-1"
          },
          "inference":{
            "quality_tier":"balanced","latency_budget_ms":10000,
            "cost_tier":"standard","data_region":"cn",
            "allow_provider_fallback":false,"allow_external_model_processing":false
          }
        },
        "execution_grant":{
          "scenario_ref":{
            "resource_type":"interview_scenario","id":"scenario_legacy01","revision":1
          },
          "resume_ref":null,
          "agent_scope":"interview_agent",
          "model_ref":{"resource_type":"model","id":"model_legacy01","revision":1},
          "model_region":"cn",
          "external_model_processing":false,
          "knowledge_contexts":[],
          "policy_version":1
        }
      }
    }$session$::jsonb
);
"""
"""@brief 可被 0022 无损表达的 0021 Interview fixture / 0021 Interview fixture losslessly representable by 0022."""


def _interview_schema_fingerprint(
    harness: _PostgresHarness,
    database: str,
) -> list[dict[str, Any]]:
    """@brief 读取与 Interview 形状有关的稳定 catalog fingerprint / Read a stable catalog fingerprint for the Interview shape."""
    return harness.rows(
        """
        SELECT 'column' AS kind, table_name AS object_name, column_name AS member_name,
               data_type || ':' || udt_name || ':' || COALESCE(character_maximum_length::text, '')
                   AS definition,
               is_nullable || ':' || COALESCE(column_default, '') AS attributes
        FROM information_schema.columns
        WHERE table_schema = 'interview'
        UNION ALL
        SELECT 'constraint', n.nspname || '.' || c.relname, con.conname,
               pg_get_constraintdef(con.oid), con.contype::text
        FROM pg_constraint AS con
        JOIN pg_class AS c ON c.oid = con.conrelid
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'interview'
           OR (n.nspname = 'knowledge' AND c.relname = 'access_snapshots'
               AND pg_get_constraintdef(con.oid) LIKE '%interview.sessions%')
        UNION ALL
        SELECT 'index', schemaname || '.' || tablename, indexname, indexdef, ''
        FROM pg_indexes WHERE schemaname = 'interview'
        UNION ALL
        SELECT 'policy', schemaname || '.' || tablename, policyname,
               COALESCE(qual, '') || '|' || COALESCE(with_check, ''),
               cmd || ':' || permissive
        FROM pg_policies WHERE schemaname = 'interview'
        ORDER BY kind, object_name, member_name
        """,
        database=database,
    )


class _FixedClock:
    """@brief 确定性 UTC clock / Deterministic UTC clock."""

    def now(self) -> datetime:
        """@brief 返回固定时刻 / Return the fixed instant."""
        return NOW


class _NoopMediaFinalizer:
    """@brief 产生 managed Transcript Artifact 的 finalizer / Finalizer producing a managed Transcript Artifact."""

    async def finalize(
        self,
        session: InterviewSession,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> EndSessionOutput:
        """@brief 确认 ending Session 并返回同源 Artifact / Confirm an ending Session and return a same-origin Artifact."""
        assert session.view.status is InterviewSessionStatus.ENDING
        assert operation_id.startswith("interview.end:")
        artifact_id = ArtifactId("artifact_endoutput01")
        return EndSessionOutput(
            (
                Artifact(
                    ResourceMeta(artifact_id, 1, NOW, NOW),
                    session.workspace_id,
                    ArtifactKind.INTERVIEW_TRANSCRIPT,
                    ResourceRef(
                        "interview_session",
                        session.meta.id,
                        session.meta.revision,
                    ),
                    "application/json",
                    2,
                    "e" * 64,
                    ApiArtifactContentUrl.build(
                        "https://api.example.test",
                        session.workspace_id,
                        artifact_id,
                    ),
                    None,
                    session.spec.recording.retention_until,
                ),
            )
        )


class _UnexpectedReportProvider:
    """@brief End worker 测试中不得调用的 Report provider / Report provider that must not be called in the end-worker test."""

    async def generate(
        self,
        request: ReportGenerationRequest,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> InterviewReportDraft:
        """@brief 若错误调用则立即失败 / Fail immediately if called unexpectedly."""
        del request, operation_id
        raise AssertionError("end worker must not invoke the Report provider")


class _SequentialIds:
    """@brief 为 persistence 测试生成稳定 opaque IDs / Generate stable opaque IDs for persistence tests."""

    def __init__(self) -> None:
        """@brief 初始化分 prefix 计数器 / Initialize per-prefix counters."""
        self._counts: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        """@brief 分配下一个 ID / Allocate the next ID."""
        count = self._counts.get(prefix, 0) + 1
        self._counts[prefix] = count
        return f"{prefix}_pg{count:08d}"


def _principal() -> TokenPrincipal:
    """@brief 构造集中访问策略认可的 owner principal / Build an owner principal accepted by central access policy."""
    return TokenPrincipal(
        UserId("user_interviewlegacy1"),
        Subject("interview-legacy-subject"),
        ClientId("client_interviewtests1"),
        frozenset({Scope("interview.read"), Scope("interview.write")}),
    )


def _scenario_spec() -> InterviewScenarioSpec:
    """@brief 构造最小生产约束 Scenario spec / Build a minimal production-constrained Scenario spec."""
    rubric = InterviewRubric(
        "rubric_postgres01",
        "1",
        "System design",
        (
            RubricDimension(
                "dimension_postgres01",
                "Consistency",
                "Explain consistency trade-offs",
                1,
                ("Defines linearizability",),
                ScoreScale(0, 100),
            ),
        ),
        ScoreScale(0, 100),
    )
    return InterviewScenarioSpec(
        "PostgreSQL distributed systems",
        "A real persistence flow",
        "zh-CN",
        "system_design",
        InterviewDifficulty.ADVANCED,
        45,
        8,
        ("consistency",),
        True,
        True,
        rubric,
    )


def _session_command(scenario_id: InterviewScenarioId) -> CreateInterviewSessionCommand:
    """@brief 构造无 Resume/Knowledge 的 Session command / Build a Session command without Resume or Knowledge."""
    return CreateInterviewSessionCommand(
        scenario_id,
        None,
        JobTarget(
            "Senior Engineer",
            "HM Alliances",
            None,
            None,
            None,
            "senior",
            ("distributed-systems",),
        ),
        KnowledgeSelection(
            KnowledgeSelectionMode.NONE,
            (),
            (),
            (),
            "interview_agent",
        ),
        "zh-CN",
        InterviewMediaPreferences(
            True,
            False,
            False,
            1920,
            1080,
            30,
            InterviewAvatarPreferences(
                AvatarOutputMode.AUDIO_ONLY,
                None,
                "voice_postgres01",
                ("opus",),
                (),
                False,
                False,
            ),
            FallbackTransport.WEBSOCKET,
        ),
        RecordingConsent(False, False, True, 30, NOW, "consent-1"),
        InferenceIntent(
            InferenceQualityTier.BALANCED,
            10_000,
            InferenceCostTier.STANDARD,
            ModelRegion.CN,
            False,
            False,
        ),
    )


def _uow_factory(
    database: AsyncDatabase,
) -> PostgresInterviewUnitOfWorkFactory:
    """@brief 构造显式 model/service policy 的 UoW factory / Build a UoW factory with explicit model/service policy."""
    return PostgresInterviewUnitOfWorkFactory(
        database,
        model_ref=ResourceRef("model", "model_postgres01", 1),
        model_regions=frozenset({ModelRegion.CN}),
        allow_external_model_processing=False,
        service_actor=ResourceRef("service", "interview_worker_service01"),
    )


@pytest.fixture(scope="module")
def interview_postgres(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[_PostgresHarness]:
    """@brief 启动真实 PostgreSQL，保留 0022 migration 库并把 runtime 库升级到 head / Start real PostgreSQL, retain a 0022 migration database, and upgrade runtime to head."""
    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("interview-postgres")
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
            CREATE DATABASE aiws_migration OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_migration TO aiws_owner;
            CREATE DATABASE aiws_empty OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_empty TO aiws_owner;
            CREATE DATABASE aiws_bad OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_bad TO aiws_owner;
            CREATE DATABASE aiws_compare OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_compare TO aiws_owner;
            """,
            database="postgres",
        )
        for database in (
            "aiws",
            "aiws_migration",
            "aiws_empty",
            "aiws_bad",
            "aiws_compare",
        ):
            try:
                harness.psql(psql, "CREATE EXTENSION vector;", database=database)
            except subprocess.CalledProcessError:
                pytest.skip("the PostgreSQL vector extension is unavailable")
        migration = _migration_config(harness.migration_dsn("aiws_migration"))
        command.upgrade(migration, "20260723_0021")
        harness.psql(psql, _IDENTITY_FIXTURE_SQL, database="aiws_migration")
        harness.psql(psql, _LEGACY_INTERVIEW_FIXTURE_SQL, database="aiws_migration")
        command.upgrade(migration, "20260723_0022")

        runtime = _migration_config(harness.migration_dsn())
        command.upgrade(runtime, "20260723_0021")
        harness.psql(psql, _IDENTITY_FIXTURE_SQL)
        harness.psql(psql, _LEGACY_INTERVIEW_FIXTURE_SQL)
        command.upgrade(runtime, "head")
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


def test_0022_is_linear_and_reuses_platform_truths() -> None:
    """@brief 0022 保持线性且不创建平行 Job/Artifact/outbox/audit / 0022 is linear and creates no parallel platform truths."""
    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    script = scripts.get_revision("20260723_0022")
    assert script is not None
    assert script.down_revision == "20260723_0021"
    source = MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE interview.jobs" not in source
    assert "CREATE TABLE interview.artifacts" not in source
    assert "CREATE TABLE interview.outbox" not in source
    assert "CREATE TABLE interview.audit" not in source
    assert "agent.jobs" in source and "agent.artifacts" in source


def test_0022_real_nonempty_upgrade_reaches_exact_revision(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief 真实非空库完整升级到 0022 / A real non-empty database upgrades fully to 0022."""
    assert interview_postgres.rows(
        "SELECT version_num FROM identity.alembic_version",
        database="aiws_migration",
    ) == [{"version_num": "20260723_0022"}]
    assert interview_postgres.rows(
        "SELECT to_regclass('interview.realtime_inputs') AS inputs, "
        "to_regclass('interview.session_jobs') AS jobs",
        database="aiws_migration",
    ) == [{"inputs": "interview.realtime_inputs", "jobs": "interview.session_jobs"}]


def test_0022_preserves_representable_scenario_and_frozen_session(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief 原位迁移保留强类型 Scenario/Session 真相 / In-place migration preserves typed Scenario/Session truth."""
    scenario = interview_postgres.rows(
        "SELECT status, spec, extensions FROM interview.scenarios "
        "WHERE id = 'scenario_legacy01'",
        database="aiws_migration",
    )[0]
    assert scenario["status"] == "active"
    assert scenario["spec"]["rubric"]["rubric_id"] == "rubric_legacy01"
    assert scenario["extensions"] == {"keep": "scenario"}
    session = interview_postgres.rows(
        "SELECT status, spec, execution_grant, next_realtime_sequence, "
        "next_transcript_sequence, extensions FROM interview.sessions "
        "WHERE id = 'session_legacy001'",
        database="aiws_migration",
    )[0]
    assert session["status"] == "created"
    assert session["spec"]["scenario_revision"] == 1
    assert session["execution_grant"]["model_ref"] == {
        "resource_type": "model",
        "id": "model_legacy01",
        "revision": 1,
    }
    assert session["next_realtime_sequence"] == 1
    assert session["next_transcript_sequence"] == 1
    assert session["extensions"] == {"keep": "session"}


def test_0022_preflight_rejects_unprovable_legacy_before_ddl(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief 不可证明 Scenario 在任何 0022 DDL 前失败 / An unprovable Scenario fails before any 0022 DDL."""
    psql = _postgres_binary("psql")
    assert psql is not None
    configuration = _migration_config(interview_postgres.migration_dsn("aiws_bad"))
    command.upgrade(configuration, "20260723_0021")
    interview_postgres.psql(psql, _IDENTITY_FIXTURE_SQL, database="aiws_bad")
    interview_postgres.psql(
        psql,
        """
        INSERT INTO interview.scenarios (
            id, workspace_id, resource_owner_id, title, locale, role_target, rubric,
            is_template, deleted_at, created_at, updated_at, revision, extensions
        ) VALUES (
            'scenario_invalid01', 'workspace_interviewlegacy1',
            'user_interviewlegacy1', 'Unprovable legacy scenario', 'zh-CN', '{}', '{}',
            false, NULL, '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', 1, '{}'
        );
        """,
        database="aiws_bad",
    )
    with pytest.raises(RuntimeError, match="complete, provable V2 spec/status"):
        command.upgrade(configuration, "20260723_0022")
    assert interview_postgres.rows(
        "SELECT version_num FROM identity.alembic_version",
        database="aiws_bad",
    ) == [{"version_num": "20260723_0021"}]
    assert interview_postgres.rows(
        "SELECT count(*) AS count FROM information_schema.columns "
        "WHERE table_schema = 'interview' AND table_name = 'scenarios' "
        "AND column_name = 'spec'",
        database="aiws_bad",
    ) == [{"count": 0}]


def test_0022_installs_workspace_rls_and_append_only_boundaries(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief Workspace RLS 隔离读取并禁止 ledger 原地改写 / Workspace RLS isolates reads and forbids in-place ledger mutation."""
    with psycopg.connect(interview_postgres.app_psycopg_dsn()) as connection:
        connection.execute(
            "SELECT set_config('app.actor_id', %s, true), "
            "set_config('app.workspace_id', %s, true)",
            ("user_interviewlegacy1", "workspace_interviewlegacy1"),
        )
        assert connection.execute(
            "SELECT id FROM interview.scenarios WHERE id = 'scenario_legacy01'"
        ).fetchone() == ("scenario_legacy01",)
        connection.rollback()
        connection.execute(
            "SELECT set_config('app.actor_id', %s, true), "
            "set_config('app.workspace_id', %s, true)",
            ("user_interviewlegacy1", "workspace_outside0001"),
        )
        assert connection.execute(
            "SELECT id FROM interview.scenarios WHERE id = 'scenario_legacy01'"
        ).fetchone() is None
        connection.rollback()
        connection.execute(
            "SELECT set_config('app.actor_id', %s, true), "
            "set_config('app.workspace_id', %s, true)",
            ("user_interviewlegacy1", "workspace_interviewlegacy1"),
        )
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute(
                "UPDATE interview.realtime_inputs SET occurred_at = occurred_at"
            )
    columns = {
        row["column_name"]
        for row in interview_postgres.rows(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'interview' AND table_name = 'realtime_connections'"
        )
    }
    assert not columns.intersection({"token", "credential", "ice_credential", "secret"})


def test_0022_rejects_report_attachment_without_one_succeeded_report_job(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief deferred trigger 阻止绕过 Job 真相直接挂 Report / A deferred trigger rejects direct Report attachment that bypasses Job truth."""
    with pytest.raises(psycopg.errors.CheckViolation, match="succeeded Report Job"):
        with psycopg.connect(interview_postgres.superuser_dsn()) as connection:
            connection.execute(
                """
                INSERT INTO interview.reports (
                    id, workspace_id, session_id, draft, generated_at,
                    created_at, updated_at, revision, extensions
                ) VALUES (
                    'report_unbound0001', 'workspace_interviewlegacy1',
                    'session_legacy001', '{}', %s, %s, %s, 1, '{}'
                )
                """,
                (NOW, NOW, NOW),
            )
            connection.execute(
                """
                UPDATE interview.sessions
                SET status = 'completed', report_id = 'report_unbound0001',
                    started_at = created_at, ended_at = %s, updated_at = %s, revision = 2
                WHERE id = 'session_legacy001'
                """,
                (NOW, NOW),
            )
    assert interview_postgres.rows(
        "SELECT status, report_id, revision FROM interview.sessions "
        "WHERE id = 'session_legacy001'"
    ) == [{"status": "created", "report_id": None, "revision": 1}]


@pytest.mark.asyncio
async def test_postgres_interview_creation_connection_and_end_queue_are_atomic(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief 实库覆盖授权、两段 credential 事务及统一 Job/outbox/audit / Real PostgreSQL covers authorization, two-phase credentials, and unified Job/outbox/audit."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            interview_postgres.app_dsn(),
            pool_size=4,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=2_000,
        )
    )
    factory = _uow_factory(database)
    gateway = HmacInterviewRealtimeGateway(
        b"interview-postgres-signing-key-32-bytes-minimum",
        signaling_url="wss://realtime.example.test/interview",
        lifetime=timedelta(minutes=5),
        clock=_FixedClock(),
    )
    ids = _SequentialIds()
    service = InterviewApplicationService(
        factory,
        gateway,
        clock=_FixedClock(),
        id_factory=ids,
    )
    principal = _principal()
    workspace_id = WorkspaceId("workspace_interviewlegacy1")
    context = InterviewMutationContext("request_interviewpg01")
    try:
        scenario = await service.create_scenario(
            principal,
            workspace_id,
            CreateInterviewScenarioCommand(_scenario_spec()),
            context,
        )
        active = await service.update_scenario(
            principal,
            workspace_id,
            scenario.meta.id,
            InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
            expected_revision=1,
            context=context,
        )
        created = await service.create_session(
            principal,
            workspace_id,
            _session_command(active.meta.id),
            context,
        )
        connection = await service.create_realtime_connection(
            principal,
            workspace_id,
            created.meta.id,
            CreateRealtimeConnectionSpec((RealtimeTransport.WEBRTC,), ("opus",), ()),
            context,
        )
        token = connection.ephemeral_token.reveal_to_transport()
        claims = await gateway.verify(
            token,
            workspace_id=workspace_id,
            session_id=created.meta.id,
            audience=ResourceRef("user", principal.user_id),
        )
        assert claims["jti"] == connection.id
        rotated_gateway = HmacInterviewRealtimeGateway(
            InterviewRealtimeSigningKeyring(
                "v2",
                (
                    InterviewRealtimeSigningKey(
                        "v2",
                        b"interview-postgres-rotated-signing-key-minimum",
                    ),
                    InterviewRealtimeSigningKey(
                        "legacy",
                        b"interview-postgres-signing-key-32-bytes-minimum",
                    ),
                ),
            ),
            signaling_url="wss://realtime.example.test/interview",
            lifetime=timedelta(minutes=5),
            clock=_FixedClock(),
        )
        rotated_claims = await rotated_gateway.verify(
            token,
            workspace_id=workspace_id,
            session_id=created.meta.id,
            audience=ResourceRef("user", principal.user_id),
        )
        assert rotated_claims == claims
        control = RealtimeControlInput(RealtimeControl.MEDIA_STARTED)
        receipt = await service.ingest_realtime_input(
            ResourceRef("user", principal.user_id),
            RealtimeInputEnvelope(
                RealtimeInputId("input_postgresstart01"),
                workspace_id,
                created.meta.id,
                connection.id,
                NOW,
                control,
                realtime_input_fingerprint(control),
            ),
        )
        assert receipt.sequence == 1 and not receipt.replayed
        activated = await service.get_session(principal, workspace_id, created.meta.id)
        assert activated.status is InterviewSessionStatus.ACTIVE
        job = await service.create_end_request(
            principal,
            workspace_id,
            created.meta.id,
            EndInterviewSessionCommand(EndInterviewReason.COMPLETED),
            expected_revision=activated.meta.revision,
            context=context,
        )
        rows = interview_postgres.rows(
            "SELECT session.status AS session_status, session.pending_end_job_id, "
            "job.status AS job_status, binding.job_kind, job.resource_owner_id "
            "FROM interview.sessions AS session "
            "JOIN interview.session_jobs AS binding "
            "ON binding.session_id = session.id AND binding.workspace_id = session.workspace_id "
            "JOIN agent.jobs AS job ON job.id = binding.job_id "
            f"WHERE session.id = '{created.meta.id}'"
        )
        assert rows == [
            {
                "session_status": "ending",
                "pending_end_job_id": str(job.meta.id),
                "job_status": "queued",
                "job_kind": "interview.end",
                "resource_owner_id": "user_interviewlegacy1",
            }
        ]
        assert interview_postgres.rows(
            "SELECT count(*) AS count FROM agent.outbox_events "
            f"WHERE aggregate_id = '{job.meta.id}' "
            f"AND payload ->> 'session_id' = '{created.meta.id}' "
            "AND event_type = 'interview.job.queued'"
        ) == [{"count": 1}]
        audit_actors = interview_postgres.rows(
            "SELECT DISTINCT actor_type, actor_id FROM identity.audit_events "
            f"WHERE resource_id IN ('{scenario.meta.id}', '{created.meta.id}', "
            f"'{connection.id}')"
        )
        assert audit_actors == [
            {"actor_type": "user", "actor_id": "user_interviewlegacy1"}
        ]
        serialized = repr(
            interview_postgres.rows(
                "SELECT extensions FROM interview.realtime_connections "
                f"WHERE id = '{connection.id}'"
            )
        )
        assert token not in serialized

        gate = asyncio.Event()
        gate_lock = asyncio.Lock()
        arrived = 0

        async def start_same_job() -> int:
            """@brief 从同一 revision 竞争 Job CAS / Race the Job CAS from one revision."""
            nonlocal arrived
            async with factory() as unit:
                current = await unit.jobs.get(workspace_id, job.meta.id)
                assert current is not None
                async with gate_lock:
                    arrived += 1
                    if arrived == 2:
                        gate.set()
                await gate.wait()
                started = current.start(at=NOW)
                await unit.jobs.save(started, expected_revision=current.meta.revision)
                await unit.commit()
                return started.meta.revision

        cas_outcomes = await asyncio.gather(
            start_same_job(),
            start_same_job(),
            return_exceptions=True,
        )
        assert sum(outcome == 2 for outcome in cas_outcomes) == 1
        assert sum(isinstance(outcome, InterviewCasMismatch) for outcome in cas_outcomes) == 1
        assert interview_postgres.rows(
            "SELECT status, revision FROM agent.jobs "
            f"WHERE id = '{job.meta.id}'"
        ) == [{"status": "running", "revision": 2}]

        worker = InterviewWorkerService(
            factory,
            _NoopMediaFinalizer(),
            _UnexpectedReportProvider(),
            service_actor=ResourceRef("service", "interview_worker_service01"),
            clock=_FixedClock(),
            id_factory=ids,
        )
        dispatch_settings = OutboxDispatchSettings(
            batch_size=10,
            lease_seconds=30,
            maximum_attempts=3,
            retry_base_seconds=1,
            retry_cap_seconds=5,
        )
        handler = InterviewJobOutboxHandler(
            worker,
            maximum_attempts=dispatch_settings.maximum_attempts,
        )
        dispatcher = OutboxDispatchService(
            PostgresOutboxClaimRepository(
                database,
                event_types=INTERVIEW_WORK_EVENT_TYPES,
            ),
            {"interview.job.queued": handler},
            required_event_types=INTERVIEW_WORK_EVENT_TYPES,
            settings=dispatch_settings,
        )
        dispatch_result = await dispatcher.run_once()
        assert dispatch_result.claimed == 1
        assert dispatch_result.completed == 1
        assert dispatch_result.retried == 0
        completed = await service.get_session(principal, workspace_id, created.meta.id)
        assert completed.status is InterviewSessionStatus.COMPLETED
        assert interview_postgres.rows(
            "SELECT status, attempt_count FROM agent.outbox_events "
            f"WHERE aggregate_id = '{job.meta.id}'"
        ) == [{"status": "published", "attempt_count": 1}]
        assert interview_postgres.rows(
            "SELECT session.status AS session_status, session.pending_end_job_id, "
            "job.status AS job_status, job.revision AS job_revision "
            "FROM interview.sessions AS session "
            f"JOIN agent.jobs AS job ON job.id = '{job.meta.id}' "
            f"WHERE session.id = '{created.meta.id}'"
        ) == [
            {
                "session_status": "completed",
                "pending_end_job_id": None,
                "job_status": "succeeded",
                "job_revision": 3,
            }
        ]
        assert interview_postgres.rows(
            "SELECT actor_type, actor_id FROM identity.audit_events "
            "WHERE action = 'interview_session.end.complete' "
            f"AND resource_id = '{created.meta.id}'"
        ) == [
            {
                "actor_type": "service",
                "actor_id": "interview_worker_service01",
            }
        ]
        assert interview_postgres.rows(
            "SELECT kind, subject_type, subject_id, workspace_id, storage_key "
            "FROM agent.artifacts WHERE id = 'artifact_endoutput01'"
        ) == [
            {
                "kind": "interview_transcript",
                "subject_type": "interview_session",
                "subject_id": str(created.meta.id),
                "workspace_id": str(workspace_id),
                "storage_key": (
                    f"interview/{workspace_id}/artifact_endoutput01"
                ),
            }
        ]
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_generic_job_cancellation_atomically_cancels_queued_interview_end(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief 通用 Job 取消同事务终结 queued Interview Session / Generic Job cancellation atomically terminates a queued Interview Session."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            interview_postgres.app_dsn(),
            pool_size=2,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=2_000,
        )
    )
    counts: dict[str, int] = {}

    def ids(prefix: str) -> str:
        """@brief 为取消测试生成不冲突 ID / Generate collision-free IDs for cancellation testing."""
        count = counts.get(prefix, 0) + 1
        counts[prefix] = count
        return f"{prefix}_cancelpg{count:06d}"

    principal = TokenPrincipal(
        UserId("user_interviewlegacy1"),
        Subject("interview-legacy-subject"),
        ClientId("client_interviewcancel1"),
        frozenset(
            {
                Scope("workspace.read"),
                Scope("workspace.write"),
                Scope("interview.read"),
                Scope("interview.write"),
            }
        ),
    )
    workspace_id = WorkspaceId("workspace_interviewlegacy1")
    context = InterviewMutationContext("request_interviewcancel1")
    interview = InterviewApplicationService(
        _uow_factory(database),
        HmacInterviewRealtimeGateway(
            b"interview-cancel-signing-key-32-bytes-minimum",
            signaling_url="wss://realtime.example.test/interview",
            lifetime=timedelta(minutes=5),
            clock=_FixedClock(),
        ),
        clock=_FixedClock(),
        id_factory=ids,
    )
    platform_factory = PostgresPlatformUnitOfWorkFactory(
        database,
        api_origin="https://api.hmalliances.org:8022",
        event_poll_interval=0.01,
    )
    platform = PlatformApplicationService(
        platform_factory,
        platform_factory.content_store,
        platform_factory.event_feed,
        clock=_FixedClock(),
    )
    try:
        scenario = await interview.create_scenario(
            principal,
            workspace_id,
            CreateInterviewScenarioCommand(_scenario_spec()),
            context,
        )
        active = await interview.update_scenario(
            principal,
            workspace_id,
            scenario.meta.id,
            InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
            expected_revision=scenario.meta.revision,
            context=context,
        )
        session = await interview.create_session(
            principal,
            workspace_id,
            _session_command(active.meta.id),
            context,
        )
        end_job = await interview.create_end_request(
            principal,
            workspace_id,
            session.meta.id,
            EndInterviewSessionCommand(EndInterviewReason.USER_CANCELLED),
            expected_revision=session.meta.revision,
            context=context,
        )

        cancelled = await platform.cancel_job(
            principal,
            workspace_id,
            end_job.meta.id,
            MutationContext("request_jobcancel0001"),
            expected_revision=end_job.meta.revision,
        )

        assert cancelled.status is JobStatus.CANCELLED
        assert interview_postgres.rows(
            "SELECT session.status AS session_status, session.pending_end_job_id, "
            "session.end_reason, session.ended_at IS NOT NULL AS has_ended, "
            "job.status AS job_status, job.revision AS job_revision "
            "FROM interview.sessions AS session "
            f"JOIN agent.jobs AS job ON job.id = '{end_job.meta.id}' "
            f"WHERE session.id = '{session.meta.id}'"
        ) == [
            {
                "session_status": "cancelled",
                "pending_end_job_id": None,
                "end_reason": None,
                "has_ended": True,
                "job_status": "cancelled",
                "job_revision": 2,
            }
        ]
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_postgres_realtime_idempotency_and_transcript_sequences_are_gap_free(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief 并发 input 去重和 Transcript 序号在回滚下仍无洞 / Concurrent input deduplication and Transcript sequencing remain gap-free across rollback."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            interview_postgres.app_dsn(),
            pool_size=4,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=2_000,
        )
    )
    factory = _uow_factory(database)
    workspace_id = WorkspaceId("workspace_interviewlegacy1")
    session_id = InterviewSessionId("session_legacy001")
    connection_id = RealtimeConnectionId("connection_concurrent01")
    try:
        async with factory() as unit:
            await unit.repository.add_connection_lease(
                RealtimeConnectionLease(
                    connection_id,
                    workspace_id,
                    session_id,
                    ResourceRef("user", "user_interviewlegacy1"),
                    RealtimeTransport.WEBSOCKET,
                    NOW,
                    NOW + timedelta(minutes=5),
                )
            )
            await unit.commit()

        async def append(input_id: str, fingerprint: str) -> tuple[int, bool]:
            """@brief 在独立事务追加一个 plaintext-free input / Append one plaintext-free input in an independent transaction."""
            async with factory() as unit:
                receipt = await unit.repository.append_realtime_input(
                    RealtimeInputLedgerRecord(
                        RealtimeInputId(input_id),
                        workspace_id,
                        session_id,
                        connection_id,
                        NOW,
                        fingerprint,
                    )
                )
                await unit.commit()
                return receipt.sequence, receipt.replayed

        duplicate = await asyncio.gather(
            append("input_concurrent01", "a" * 64),
            append("input_concurrent01", "a" * 64),
        )
        assert sorted(duplicate) == [(1, False), (1, True)]
        with pytest.raises(RealtimeInputKeyReused):
            await append("input_concurrent01", "b" * 64)
        distinct = await asyncio.gather(
            append("input_concurrent02", "c" * 64),
            append("input_concurrent03", "d" * 64),
        )
        assert sorted(sequence for sequence, _replayed in distinct) == [2, 3]

        async with factory() as rolled_back:
            abandoned = await rolled_back.repository.allocate_transcript_sequence(
                workspace_id,
                session_id,
            )
            assert abandoned.sequence == 1

        async def append_segment(
            segment_id: str,
            source_input_id: str,
        ) -> int:
            """@brief 原子保留序号并追加 provenance segment / Atomically reserve a sequence and append a provenance segment."""
            async with factory() as unit:
                reservation = await unit.repository.allocate_transcript_sequence(
                    workspace_id,
                    session_id,
                )
                await unit.repository.add_transcript_segment(
                    TranscriptSegment(
                        TranscriptSegmentId(segment_id),
                        workspace_id,
                        session_id,
                        reservation.sequence,
                        ResourceRef("realtime_input", source_input_id),
                        TranscriptSpeaker.CANDIDATE,
                        0,
                        1_000,
                        "A provenance-bound segment.",
                    )
                )
                await unit.commit()
                return reservation.sequence

        segment_sequences = await asyncio.gather(
            append_segment("segment_concurrent01", "input_concurrent02"),
            append_segment("segment_concurrent02", "input_concurrent03"),
        )
        assert sorted(segment_sequences) == [1, 2]
        assert interview_postgres.rows(
            "SELECT next_realtime_sequence, next_transcript_sequence "
            "FROM interview.sessions WHERE id = 'session_legacy001'"
        ) == [{"next_realtime_sequence": 4, "next_transcript_sequence": 3}]
        segments = interview_postgres.rows(
            "SELECT sequence, source_input_id, source_artifact_id "
            "FROM interview.transcript_segments "
            "WHERE session_id = 'session_legacy001' ORDER BY sequence"
        )
        assert [row["sequence"] for row in segments] == [1, 2]
        assert {row["source_input_id"] for row in segments} == {
            "input_concurrent02",
            "input_concurrent03",
        }
        assert all(row["source_artifact_id"] is None for row in segments)
    finally:
        await database.aclose()


def test_0022_real_empty_downgrade_restores_0021_shape(
    interview_postgres: _PostgresHarness,
) -> None:
    """@brief 空库可形式化回退到 0021 / An empty database can formally downgrade to 0021."""
    configuration = _migration_config(interview_postgres.migration_dsn("aiws_empty"))
    command.upgrade(configuration, "20260723_0022")
    command.downgrade(configuration, "20260723_0021")
    assert interview_postgres.rows(
        "SELECT version_num FROM identity.alembic_version",
        database="aiws_empty",
    ) == [{"version_num": "20260723_0021"}]
    assert interview_postgres.rows(
        "SELECT to_regclass('interview.events') AS events, "
        "to_regclass('interview.report_jobs') AS jobs, "
        "to_regclass('interview.realtime_connections') AS connections",
        database="aiws_empty",
    ) == [
        {
            "events": "interview.events",
            "jobs": "interview.report_jobs",
            "connections": None,
        }
    ]
    comparison = _migration_config(interview_postgres.migration_dsn("aiws_compare"))
    command.upgrade(comparison, "20260723_0021")
    assert _interview_schema_fingerprint(
        interview_postgres,
        "aiws_empty",
    ) == _interview_schema_fingerprint(interview_postgres, "aiws_compare")
