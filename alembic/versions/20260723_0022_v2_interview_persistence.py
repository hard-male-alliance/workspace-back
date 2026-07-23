"""@brief 原位发布 API V2 Interview persistence / Publish API V2 Interview persistence in place.

Revision ID: 20260723_0022
Revises: 20260723_0021
Create Date: 2026-07-23

Scenario、Session、realtime input、Transcript、Report 与 Job binding 均原位演进；
统一 ``agent.jobs``、``agent.artifacts``、``agent.outbox_events`` 与
``identity.audit_events`` 保持唯一真相。迁移只接受带有可验证 V2 frozen snapshot、
credential binding 与 provenance 的历史行；无法证明等价的数据在任何 DDL 前失败。
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260723_0022"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0021"
"""@brief 线性前驱 revision / Linear predecessor revision."""

branch_labels = None
"""@brief 此迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 此迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal["owner_role", "app_role", "dashboard_role", "migrator_role"]
"""@brief dbctl 可配置角色 / Configurable dbctl roles."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role allowlist / PostgreSQL role allowlist."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL identifier byte limit / PostgreSQL identifier byte limit."""

_MIGRATION_POLICY = "interview_v2_owner_migration_0022"
"""@brief FORCE RLS 下的临时 owner policy / Temporary owner policy under FORCE RLS."""

_LEGACY_TABLES = (
    "interview.scenarios",
    "interview.sessions",
    "interview.events",
    "interview.transcript_segments",
    "interview.reports",
    "interview.report_jobs",
)
"""@brief 原位演进的 Interview 表 / Interview tables evolved in place."""

_SHARED_TABLES = ("agent.jobs", "agent.artifacts")
"""@brief preflight 与约束需要读取的统一表 / Unified tables needed by preflight and constraints."""

_FINAL_POLICY_TABLES = (
    "interview.scenarios",
    "interview.sessions",
    "interview.realtime_inputs",
    "interview.transcript_segments",
    "interview.reports",
    "interview.session_jobs",
    *_SHARED_TABLES,
)
"""@brief 临时 policy 在 rename 后的最终表名 / Final names carrying temporary policies after rename."""

_NEW_TABLES = (
    "interview.realtime_connections",
    "interview.report_evidence",
)
"""@brief 0022 新建的引用完整性表 / Referential-integrity tables created by 0022."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回安全引用的 runtime role / Return a safely quoted runtime role.

    @param option dbctl role option / dbctl role option.
    @return 双引号引用的标识符 / Double-quoted identifier.
    """
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option(f"aiws.{option}")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES
    ):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def _count(statement: str) -> int:
    """@brief 执行静态 count SQL / Execute static count SQL.

    @param statement 本 revision 内部常量 SQL / SQL constant owned by this revision.
    @return count 值 / Count value.
    """
    return int(op.get_bind().scalar(sa.text(statement)) or 0)


def _install_migration_visibility(owner_role: str) -> None:
    """@brief 为既有 FORCE RLS 表安装临时可见性 / Install temporary visibility on existing FORCE-RLS tables."""
    for table in (*_LEGACY_TABLES, *_SHARED_TABLES):
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )


def _remove_migration_visibility() -> None:
    """@brief 在最终表名上移除临时可见性 / Remove temporary visibility from final table names."""
    for table in reversed(_FINAL_POLICY_TABLES):
        op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {table}")


def _preflight_scenarios() -> None:
    """@brief 拒绝没有完整 V2 Scenario snapshot 的历史行 / Reject legacy rows without complete V2 Scenario snapshots."""
    invalid = _count(
        r"""
        SELECT count(*)
        FROM interview.scenarios AS scenario
        WHERE scenario.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR scenario.revision < 1
           OR scenario.updated_at < scenario.created_at
           OR jsonb_typeof(scenario.extensions) <> 'object'
           OR jsonb_typeof(scenario.extensions #> '{v2,spec}') <> 'object'
           OR scenario.extensions #>> '{v2,status}'
              NOT IN ('draft', 'active', 'archived')
           OR btrim(COALESCE(scenario.extensions #>> '{v2,spec,name}', '')) = ''
           OR length(scenario.extensions #>> '{v2,spec,name}') > 200
           OR COALESCE(scenario.extensions #>> '{v2,spec,locale}', '')
              !~ '^[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*$'
           OR COALESCE(scenario.extensions #>> '{v2,spec,interview_type}', '')
              !~ '^[a-z][a-z0-9_.-]{2,100}$'
           OR scenario.extensions #>> '{v2,spec,difficulty}'
              NOT IN ('introductory', 'intermediate', 'advanced', 'adaptive')
           OR jsonb_typeof(scenario.extensions #> '{v2,spec,duration_minutes}') <> 'number'
           OR NOT CASE
                WHEN jsonb_typeof(scenario.extensions #> '{v2,spec,duration_minutes}') = 'number'
                THEN (scenario.extensions #>> '{v2,spec,duration_minutes}')::numeric
                     BETWEEN 5 AND 240
                ELSE false
              END
           OR jsonb_typeof(scenario.extensions #> '{v2,spec,target_question_count}') <> 'number'
           OR NOT CASE
                WHEN jsonb_typeof(scenario.extensions #> '{v2,spec,target_question_count}') = 'number'
                THEN (scenario.extensions #>> '{v2,spec,target_question_count}')::numeric
                     BETWEEN 1 AND 100
                ELSE false
              END
           OR jsonb_typeof(scenario.extensions #> '{v2,spec,focus_areas}') <> 'array'
           OR jsonb_array_length(
                CASE
                    WHEN jsonb_typeof(scenario.extensions #> '{v2,spec,focus_areas}') = 'array'
                    THEN scenario.extensions #> '{v2,spec,focus_areas}'
                    ELSE '[]'::jsonb
                END
              ) > 50
           OR jsonb_typeof(scenario.extensions #> '{v2,spec,allow_followups}') <> 'boolean'
           OR jsonb_typeof(scenario.extensions #> '{v2,spec,allow_barge_in}') <> 'boolean'
           OR jsonb_typeof(scenario.extensions #> '{v2,spec,rubric}') <> 'object'
           OR COALESCE(scenario.extensions #>> '{v2,spec,rubric,rubric_id}', '')
              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR btrim(COALESCE(scenario.extensions #>> '{v2,spec,rubric,rubric_version}', '')) = ''
           OR jsonb_typeof(scenario.extensions #> '{v2,spec,rubric,dimensions}') <> 'array'
           OR jsonb_array_length(
                CASE
                    WHEN jsonb_typeof(
                        scenario.extensions #> '{v2,spec,rubric,dimensions}'
                    ) = 'array'
                    THEN scenario.extensions #> '{v2,spec,rubric,dimensions}'
                    ELSE '[]'::jsonb
                END
              )
              NOT BETWEEN 1 AND 50
        """
    )
    if invalid:
        raise RuntimeError(
            "legacy Interview Scenario rows lack a complete, provable V2 spec/status snapshot; "
            "placeholder rubrics and inferred defaults are not migrated"
        )


def _preflight_sessions() -> None:
    """@brief 拒绝不能证明 frozen spec/grant/state 的 Session / Reject Sessions without provable frozen spec/grant/state."""
    invalid = _count(
        r"""
        SELECT count(*)
        FROM interview.sessions AS session
        JOIN interview.scenarios AS scenario
          ON scenario.id = session.scenario_id
         AND scenario.workspace_id = session.workspace_id
        LEFT JOIN interview.reports AS report
          ON report.id = session.extensions #>> '{v2,report_id}'
         AND report.workspace_id = session.workspace_id
         AND report.session_id = session.id
        LEFT JOIN agent.jobs AS end_job
          ON end_job.id = session.extensions #>> '{v2,pending_end_job_id}'
         AND end_job.workspace_id = session.workspace_id
        WHERE session.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR session.revision < 1
           OR session.updated_at < session.created_at
           OR jsonb_typeof(session.extensions) <> 'object'
           OR jsonb_typeof(session.extensions #> '{v2,spec}') <> 'object'
           OR jsonb_typeof(session.extensions #> '{v2,execution_grant}') <> 'object'
           OR session.extensions #>> '{v2,status}'
              NOT IN ('created', 'connecting', 'active', 'ending', 'completed', 'failed', 'cancelled')
           OR session.extensions #>> '{v2,spec,scenario_id}' <> session.scenario_id
           OR jsonb_typeof(session.extensions #> '{v2,spec,scenario_revision}') <> 'number'
           OR NOT CASE
                WHEN jsonb_typeof(session.extensions #> '{v2,spec,scenario_revision}') = 'number'
                THEN (session.extensions #>> '{v2,spec,scenario_revision}')::numeric >= 1
                ELSE false
              END
           OR jsonb_typeof(session.extensions #> '{v2,spec,rubric_snapshot}') <> 'object'
           OR session.extensions #>> '{v2,execution_grant,scenario_ref,id}' <> session.scenario_id
           OR session.extensions #>> '{v2,execution_grant,scenario_ref,resource_type}'
              <> 'interview_scenario'
           OR session.extensions #>> '{v2,execution_grant,scenario_ref,revision}'
              <> session.extensions #>> '{v2,spec,scenario_revision}'
           OR session.extensions #>> '{v2,execution_grant,agent_scope}'
              <> session.extensions #>> '{v2,spec,knowledge,agent_scope}'
           OR COALESCE(session.extensions #>> '{v2,spec,locale}', '')
              !~ '^[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*$'
           OR jsonb_typeof(session.extensions #> '{v2,spec,job_target}') <> 'object'
           OR jsonb_typeof(session.extensions #> '{v2,spec,media}') <> 'object'
           OR jsonb_typeof(session.extensions #> '{v2,spec,recording}') <> 'object'
           OR jsonb_typeof(session.extensions #> '{v2,spec,inference}') <> 'object'
           OR (
                (session.extensions #>> '{v2,status}') IN ('completed', 'failed', 'cancelled')
                AND session.ended_at IS NULL
           )
           OR (
                (session.extensions #>> '{v2,status}')
                    NOT IN ('completed', 'failed', 'cancelled')
                AND session.ended_at IS NOT NULL
           )
           OR (
                (session.extensions #>> '{v2,status}') = 'completed'
                AND session.started_at IS NULL
           )
           OR (session.started_at IS NOT NULL AND session.started_at < session.created_at)
           OR (session.ended_at IS NOT NULL AND session.ended_at < session.created_at)
           OR (session.started_at IS NOT NULL AND session.ended_at IS NOT NULL
               AND session.ended_at < session.started_at)
           OR (
                session.extensions #>> '{v2,report_id}' IS NOT NULL
                AND (
                    session.extensions #>> '{v2,status}' <> 'completed'
                    OR report.id IS NULL
                )
           )
           OR (
                session.extensions #>> '{v2,status}' = 'ending'
                AND (
                    end_job.id IS NULL
                    OR end_job.job_type <> 'interview.end'
                    OR end_job.target_resource_type <> 'interview_session'
                    OR end_job.target_resource_id <> session.id
                    OR end_job.status NOT IN ('queued', 'running')
                    OR session.extensions #>> '{v2,end_reason}'
                       NOT IN ('completed', 'user_cancelled', 'technical_failure')
                )
           )
           OR (
                session.extensions #>> '{v2,status}' <> 'ending'
                AND (
                    session.extensions #>> '{v2,pending_end_job_id}' IS NOT NULL
                    OR session.extensions #>> '{v2,end_reason}' IS NOT NULL
                )
           )
        """
    )
    if invalid:
        raise RuntimeError(
            "legacy Interview Session rows lack a complete frozen V2 spec/execution grant, "
            "or their Session/end/report state cannot be represented exactly"
        )


def _preflight_realtime_inputs() -> None:
    """@brief 验证 legacy event 可收敛为 secret/plaintext-free ledger / Validate legacy events can become a secret/plaintext-free ledger."""
    invalid = _count(
        r"""
        SELECT count(*)
        FROM interview.events AS event
        JOIN interview.sessions AS session
          ON session.id = event.session_id AND session.workspace_id = event.workspace_id
        WHERE event.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR event.sequence < 1
           OR event.revision < 1
           OR event.updated_at < event.created_at
           OR jsonb_typeof(event.extensions #> '{v2,connection_lease}') <> 'object'
           OR COALESCE(event.extensions #>> '{v2,fingerprint_sha256}', '')
              !~ '^[a-f0-9]{64}$'
           OR COALESCE(event.extensions #>> '{v2,connection_lease,id}', '')
              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR event.extensions #>> '{v2,connection_lease,workspace_id}' <> event.workspace_id
           OR event.extensions #>> '{v2,connection_lease,session_id}' <> event.session_id
           OR event.extensions #>> '{v2,connection_lease,transport}'
              NOT IN ('webrtc', 'websocket')
           OR COALESCE(event.extensions #>> '{v2,connection_lease,audience,resource_type}', '')
              !~ '^[a-z][a-z0-9_.-]{2,100}$'
           OR COALESCE(event.extensions #>> '{v2,connection_lease,audience,id}', '')
              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR NOT pg_input_is_valid(
                COALESCE(event.extensions #>> '{v2,connection_lease,issued_at}', ''),
                'timestamp with time zone'
              )
           OR NOT pg_input_is_valid(
                COALESCE(event.extensions #>> '{v2,connection_lease,expires_at}', ''),
                'timestamp with time zone'
              )
           OR NOT CASE
                WHEN pg_input_is_valid(
                    COALESCE(event.extensions #>> '{v2,connection_lease,issued_at}', ''),
                    'timestamp with time zone'
                ) AND pg_input_is_valid(
                    COALESCE(event.extensions #>> '{v2,connection_lease,expires_at}', ''),
                    'timestamp with time zone'
                ) THEN
                    (event.extensions #>> '{v2,connection_lease,issued_at}')::timestamptz
                    < (event.extensions #>> '{v2,connection_lease,expires_at}')::timestamptz
                    AND (event.extensions #>> '{v2,connection_lease,expires_at}')::timestamptz
                        <= (event.extensions #>> '{v2,connection_lease,issued_at}')::timestamptz
                           + interval '15 minutes'
                ELSE false
              END
        """
    )
    conflicting_leases = _count(
        r"""
        SELECT count(*)
        FROM (
            SELECT event.extensions #>> '{v2,connection_lease,id}' AS connection_id
            FROM interview.events AS event
            GROUP BY event.extensions #>> '{v2,connection_lease,id}'
            HAVING count(DISTINCT (event.extensions #> '{v2,connection_lease}')::text) <> 1
        ) AS conflict
        """
    )
    if invalid or conflicting_leases:
        raise RuntimeError(
            "legacy Interview event rows lack a consistent single-Session credential lease or "
            "canonical plaintext-free input fingerprint"
        )


def _preflight_transcript() -> None:
    """@brief 验证 Transcript append-only 状态与来源 / Validate append-only Transcript state and provenance."""
    invalid = _count(
        r"""
        SELECT count(*)
        FROM interview.transcript_segments AS segment
        LEFT JOIN interview.events AS source_input
          ON segment.extensions #>> '{v2,source_ref,resource_type}' = 'realtime_input'
         AND source_input.id = segment.extensions #>> '{v2,source_ref,id}'
         AND source_input.workspace_id = segment.workspace_id
         AND source_input.session_id = segment.session_id
        LEFT JOIN agent.artifacts AS source_artifact
          ON segment.extensions #>> '{v2,source_ref,resource_type}' = 'artifact'
         AND source_artifact.id = segment.extensions #>> '{v2,source_ref,id}'
         AND source_artifact.workspace_id = segment.workspace_id
         AND source_artifact.revision = CASE
             WHEN jsonb_typeof(segment.extensions #> '{v2,source_ref,revision}') = 'number'
              AND COALESCE(segment.extensions #>> '{v2,source_ref,revision}', '')
                  ~ '^[1-9][0-9]*$'
              AND (segment.extensions #>> '{v2,source_ref,revision}')::numeric
                  <= 2147483647
             THEN (segment.extensions #>> '{v2,source_ref,revision}')::integer
             ELSE NULL
         END
        WHERE segment.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR segment.sequence < 1
           OR segment.end_ms IS NULL
           OR segment.start_ms < 0
           OR segment.end_ms < segment.start_ms
           OR length(segment.text_content) > 20000
           OR segment.is_final IS NOT TRUE
           OR segment.generated_text IS NOT NULL
           OR segment.media_scheduled_at IS NOT NULL
           OR segment.media_played_ack_at IS NOT NULL
           OR segment.revision < 1
           OR segment.updated_at < segment.created_at
           OR jsonb_typeof(segment.extensions #> '{v2,source_ref}') <> 'object'
           OR segment.extensions #>> '{v2,source_ref,resource_type}'
              NOT IN ('realtime_input', 'artifact')
           OR COALESCE(segment.extensions #>> '{v2,source_ref,id}', '')
              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR (
                segment.extensions #>> '{v2,source_ref,resource_type}' = 'realtime_input'
                AND (
                    source_input.id IS NULL
                    OR segment.extensions #> '{v2,source_ref,revision}' IS NOT NULL
                )
           )
           OR (
                segment.extensions #>> '{v2,source_ref,resource_type}' = 'artifact'
                AND (
                    source_artifact.id IS NULL
                    OR COALESCE(segment.extensions #>> '{v2,source_ref,revision}', '')
                       !~ '^[1-9][0-9]*$'
                    OR source_artifact.subject_type <> 'interview_session'
                    OR source_artifact.subject_id <> segment.session_id
                )
           )
        """
    )
    if invalid:
        raise RuntimeError(
            "legacy Transcript rows are mutable/partial or lack same-Session realtime-input/Artifact provenance"
        )


def _preflight_reports_and_jobs() -> None:
    """@brief 验证 immutable Report、evidence 与统一 Job binding / Validate immutable Reports, evidence, and unified Job bindings."""
    invalid_reports = _count(
        r"""
        SELECT count(*)
        FROM interview.reports AS report
        JOIN interview.sessions AS session
          ON session.id = report.session_id AND session.workspace_id = report.workspace_id
        WHERE report.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR jsonb_typeof(report.extensions #> '{v2,draft}') <> 'object'
           OR btrim(COALESCE(report.extensions #>> '{v2,draft,report_version}', '')) = ''
           OR COALESCE(report.extensions #>> '{v2,draft,rubric_id}', '')
              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR btrim(COALESCE(report.extensions #>> '{v2,draft,rubric_version}', '')) = ''
           OR jsonb_typeof(report.extensions #> '{v2,draft,rubric_scores}') <> 'array'
           OR EXISTS (
                SELECT 1
                FROM jsonb_array_elements(
                    CASE
                        WHEN jsonb_typeof(
                            report.extensions #> '{v2,draft,rubric_scores}'
                        ) = 'array'
                        THEN report.extensions #> '{v2,draft,rubric_scores}'
                        ELSE '[]'::jsonb
                    END
                ) AS score(value)
                WHERE jsonb_typeof(score.value) <> 'object'
                   OR jsonb_typeof(score.value -> 'evidence') <> 'array'
           )
           OR jsonb_typeof(report.extensions #> '{v2,draft,strengths}') <> 'array'
           OR jsonb_typeof(report.extensions #> '{v2,draft,improvements}') <> 'array'
           OR jsonb_typeof(report.extensions #> '{v2,draft,action_plan}') <> 'array'
           OR jsonb_typeof(report.extensions #> '{v2,draft,limitations}') <> 'array'
           OR session.extensions #>> '{v2,status}' <> 'completed'
           OR session.extensions #>> '{v2,report_id}' <> report.id
           OR report.extensions #>> '{v2,draft,rubric_id}'
              <> session.extensions #>> '{v2,spec,rubric_snapshot,rubric_id}'
           OR report.extensions #>> '{v2,draft,rubric_version}'
              <> session.extensions #>> '{v2,spec,rubric_snapshot,rubric_version}'
           OR report.generated_at < session.created_at
        """
    )
    duplicate_reports = _count(
        """
        SELECT count(*) FROM (
            SELECT workspace_id, session_id
            FROM interview.reports
            GROUP BY workspace_id, session_id
            HAVING count(*) > 1
        ) AS duplicate
        """
    )
    invalid_evidence = _count(
        r"""
        SELECT count(*)
        FROM interview.reports AS report
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(report.extensions #> '{v2,draft,rubric_scores}') = 'array'
                THEN report.extensions #> '{v2,draft,rubric_scores}'
                ELSE '[]'::jsonb
            END
        ) AS score(value)
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(score.value -> 'evidence') = 'array'
                THEN score.value -> 'evidence'
                ELSE '[]'::jsonb
            END
        ) AS evidence(value)
        LEFT JOIN interview.transcript_segments AS segment
          ON segment.id = evidence.value ->> 'segment_id'
         AND segment.workspace_id = report.workspace_id
         AND segment.session_id = report.session_id
        WHERE segment.id IS NULL
           OR jsonb_typeof(evidence.value -> 'start_ms') <> 'number'
           OR jsonb_typeof(evidence.value -> 'end_ms') <> 'number'
           OR CASE
                WHEN jsonb_typeof(evidence.value -> 'start_ms') = 'number'
                 AND jsonb_typeof(evidence.value -> 'end_ms') = 'number'
                 AND segment.id IS NOT NULL
                THEN (evidence.value ->> 'start_ms')::numeric >= segment.start_ms
                 AND (evidence.value ->> 'end_ms')::numeric <= segment.end_ms
                 AND (evidence.value ->> 'end_ms')::numeric
                     >= (evidence.value ->> 'start_ms')::numeric
                ELSE false
              END IS NOT TRUE
        """
    )
    invalid_jobs = _count(
        r"""
        SELECT count(*)
        FROM interview.report_jobs AS binding
        LEFT JOIN agent.jobs AS job
          ON job.id = binding.job_id AND job.workspace_id = binding.workspace_id
        WHERE binding.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR job.id IS NULL
           OR job.job_type <> 'interview.report'
           OR job.target_resource_type <> 'interview_session'
           OR job.target_resource_id <> binding.session_id
           OR jsonb_typeof(job.request_payload) <> 'object'
           OR jsonb_typeof(job.request_payload -> 'spec') <> 'object'
           OR jsonb_typeof(job.result_refs) <> 'array'
           OR (
                binding.report_id IS NOT NULL
                AND (
                    job.status <> 'succeeded'
                    OR NOT EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(job.result_refs) = 'array'
                                THEN job.result_refs
                                ELSE '[]'::jsonb
                            END
                        ) AS result(value)
                        WHERE result.value ->> 'resource_type' = 'interview_report'
                          AND result.value ->> 'id' = binding.report_id
                    )
                )
           )
        """
    )
    if invalid_reports or duplicate_reports or invalid_evidence or invalid_jobs:
        raise RuntimeError(
            "legacy Interview Report/evidence/report-job rows cannot be represented as one "
            "immutable Report truth plus unified Job state"
        )


def _preflight_upgrade() -> None:
    """@brief 在任何 schema mutation 前完成全部 exactness 检查 / Complete all exactness checks before any schema mutation."""
    _preflight_scenarios()
    _preflight_sessions()
    _preflight_realtime_inputs()
    _preflight_transcript()
    _preflight_reports_and_jobs()


def _evolve_scenarios() -> None:
    """@brief 原位收敛 Scenario 为 spec+status 聚合 / Collapse Scenario in place to a spec+status aggregate."""
    op.add_column(
        "scenarios",
        sa.Column("spec", postgresql.JSONB(astext_type=sa.Text())),
        schema="interview",
    )
    op.add_column("scenarios", sa.Column("status", sa.String(16)), schema="interview")
    op.execute(
        """
        UPDATE interview.scenarios
        SET spec = extensions #> '{v2,spec}',
            status = extensions #>> '{v2,status}',
            extensions = extensions - 'v2' - 'runtime'
        """
    )
    op.alter_column("scenarios", "id", type_=sa.String(160), schema="interview")
    op.alter_column("scenarios", "spec", nullable=False, schema="interview")
    op.alter_column("scenarios", "status", nullable=False, schema="interview")
    for column in ("title", "locale", "role_target", "rubric", "is_template", "deleted_at"):
        op.drop_column("scenarios", column, schema="interview")
    op.drop_index("ix_scenarios_workspace_id_updated_at", table_name="scenarios", schema="interview")
    op.create_check_constraint(
        "ck_scenarios_interview_scenarios_status",
        "scenarios",
        "status IN ('draft', 'active', 'archived')",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_scenarios_interview_scenarios_spec",
        "scenarios",
        "jsonb_typeof(spec) = 'object' "
        "AND jsonb_typeof(spec -> 'rubric') = 'object' "
        "AND jsonb_typeof(spec -> 'rubric' -> 'dimensions') = 'array' "
        "AND jsonb_array_length(spec -> 'rubric' -> 'dimensions') BETWEEN 1 AND 50",
        schema="interview",
    )
    op.create_unique_constraint(
        "interview_scenarios_id_workspace", "scenarios", ["id", "workspace_id"], schema="interview"
    )
    op.create_index(
        "ix_interview_scenarios_workspace_created_id",
        "scenarios",
        ["workspace_id", "created_at", "id"],
        schema="interview",
    )


def _add_session_v2_columns() -> None:
    """@brief 在保留 legacy 来源列时先回填 Session V2 字段 / Backfill Session V2 fields before dropping legacy source columns."""
    op.add_column("sessions", sa.Column("status", sa.String(16)), schema="interview")
    op.add_column(
        "sessions",
        sa.Column("spec", postgresql.JSONB(astext_type=sa.Text())),
        schema="interview",
    )
    op.add_column(
        "sessions",
        sa.Column("execution_grant", postgresql.JSONB(astext_type=sa.Text())),
        schema="interview",
    )
    op.add_column("sessions", sa.Column("report_id", sa.String(160)), schema="interview")
    op.add_column("sessions", sa.Column("pending_end_job_id", sa.String(160)), schema="interview")
    op.add_column("sessions", sa.Column("end_reason", sa.String(24)), schema="interview")
    op.add_column(
        "sessions",
        sa.Column("next_realtime_sequence", sa.BigInteger(), server_default=sa.text("1")),
        schema="interview",
    )
    op.add_column(
        "sessions",
        sa.Column("next_transcript_sequence", sa.BigInteger(), server_default=sa.text("1")),
        schema="interview",
    )
    op.execute(
        """
        UPDATE interview.sessions AS session
        SET status = session.extensions #>> '{v2,status}',
            spec = session.extensions #> '{v2,spec}',
            execution_grant = session.extensions #> '{v2,execution_grant}',
            report_id = session.extensions #>> '{v2,report_id}',
            pending_end_job_id = session.extensions #>> '{v2,pending_end_job_id}',
            end_reason = session.extensions #>> '{v2,end_reason}',
            next_realtime_sequence = COALESCE((
                SELECT max(event.sequence) + 1
                FROM interview.events AS event
                WHERE event.workspace_id = session.workspace_id
                  AND event.session_id = session.id
            ), 1),
            next_transcript_sequence = COALESCE((
                SELECT max(segment.sequence) + 1
                FROM interview.transcript_segments AS segment
                WHERE segment.workspace_id = session.workspace_id
                  AND segment.session_id = session.id
            ), 1),
            extensions = session.extensions - 'v2' - 'runtime'
        """
    )
    op.alter_column("sessions", "id", type_=sa.String(160), schema="interview")
    op.alter_column("sessions", "scenario_id", type_=sa.String(160), schema="interview")
    for column in (
        "status",
        "spec",
        "execution_grant",
        "next_realtime_sequence",
        "next_transcript_sequence",
    ):
        op.alter_column("sessions", column, nullable=False, schema="interview")
    op.create_unique_constraint(
        "interview_sessions_id_workspace",
        "sessions",
        ["id", "workspace_id"],
        schema="interview",
    )


def _create_realtime_connections() -> None:
    """@brief 创建无 secret connection lease 表并从 event marker 去重回填 / Create and backfill secret-free connection leases."""
    op.create_table(
        "realtime_connections",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("session_id", sa.String(160), nullable=False),
        sa.Column("audience_type", sa.String(101), nullable=False),
        sa.Column("audience_id", sa.String(160), nullable=False),
        sa.Column("audience_revision", sa.Integer()),
        sa.Column("transport", sa.String(16), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "audience_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND audience_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (audience_revision IS NULL OR audience_revision >= 1)",
            name="interview_realtime_connections_audience",
        ),
        sa.CheckConstraint(
            "transport IN ('webrtc', 'websocket')",
            name="interview_realtime_connections_transport",
        ),
        sa.CheckConstraint(
            "expires_at > created_at AND expires_at <= created_at + interval '15 minutes'",
            name="interview_realtime_connections_lifetime",
        ),
        sa.ForeignKeyConstraint(
            ["session_id", "workspace_id"],
            ["interview.sessions.id", "interview.sessions.workspace_id"],
            ondelete="CASCADE",
            name="interview_realtime_connections_session_workspace",
        ),
        sa.UniqueConstraint("id", "workspace_id", "session_id", name="interview_connections_scope"),
        schema="interview",
    )
    op.execute(
        """
        INSERT INTO interview.realtime_connections (
            id, workspace_id, session_id, audience_type, audience_id,
            audience_revision, transport, expires_at, created_at, updated_at,
            revision, extensions
        )
        SELECT DISTINCT ON (event.extensions #>> '{v2,connection_lease,id}')
            event.extensions #>> '{v2,connection_lease,id}',
            event.workspace_id,
            event.session_id,
            event.extensions #>> '{v2,connection_lease,audience,resource_type}',
            event.extensions #>> '{v2,connection_lease,audience,id}',
            (event.extensions #>> '{v2,connection_lease,audience,revision}')::integer,
            event.extensions #>> '{v2,connection_lease,transport}',
            (event.extensions #>> '{v2,connection_lease,expires_at}')::timestamptz,
            (event.extensions #>> '{v2,connection_lease,issued_at}')::timestamptz,
            (event.extensions #>> '{v2,connection_lease,issued_at}')::timestamptz,
            1,
            '{}'::jsonb
        FROM interview.events AS event
        ORDER BY event.extensions #>> '{v2,connection_lease,id}', event.sequence
        """
    )
    op.create_index(
        "ix_interview_realtime_connections_workspace_expiry",
        "realtime_connections",
        ["workspace_id", "expires_at", "id"],
        schema="interview",
    )


def _evolve_realtime_inputs() -> None:
    """@brief 原位把 legacy events 收敛为 input ledger / Evolve legacy events in place into the input ledger."""
    op.add_column("events", sa.Column("connection_id", sa.String(160)), schema="interview")
    op.add_column("events", sa.Column("fingerprint_sha256", sa.String(64)), schema="interview")
    op.execute(
        """
        UPDATE interview.events
        SET connection_id = extensions #>> '{v2,connection_lease,id}',
            fingerprint_sha256 = extensions #>> '{v2,fingerprint_sha256}',
            created_at = occurred_at,
            updated_at = occurred_at,
            revision = 1,
            extensions = extensions - 'v2' - 'runtime'
        """
    )
    op.alter_column("events", "id", type_=sa.String(160), schema="interview")
    op.alter_column("events", "session_id", type_=sa.String(160), schema="interview")
    op.alter_column("events", "connection_id", nullable=False, schema="interview")
    op.alter_column("events", "fingerprint_sha256", nullable=False, schema="interview")
    op.drop_constraint(
        "interview_events_session_sequence", "events", schema="interview", type_="unique"
    )
    op.drop_constraint(
        "events_session_id_fkey", "events", schema="interview", type_="foreignkey"
    )
    for index in (
        "ix_events_session_id_sequence",
        "ix_events_workspace_id",
    ):
        op.drop_index(index, table_name="events", schema="interview")
    for column in ("ack_sequence", "event_type", "payload", "trace_id"):
        op.drop_column("events", column, schema="interview")
    op.rename_table("events", "realtime_inputs", schema="interview")
    op.create_check_constraint(
        "ck_realtime_inputs_interview_realtime_inputs_envelope",
        "realtime_inputs",
        "sequence >= 1 AND fingerprint_sha256 ~ '^[a-f0-9]{64}$'",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_realtime_inputs_interview_realtime_inputs_immutable",
        "realtime_inputs",
        "revision = 1 AND created_at = updated_at",
        schema="interview",
    )
    op.create_foreign_key(
        "interview_realtime_inputs_connection_scope",
        "realtime_inputs",
        "realtime_connections",
        ["connection_id", "workspace_id", "session_id"],
        ["id", "workspace_id", "session_id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "interview_realtime_inputs_session_workspace",
        "realtime_inputs",
        "sessions",
        ["session_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="CASCADE",
    )
    op.create_unique_constraint(
        "interview_realtime_inputs_session_sequence",
        "realtime_inputs",
        ["workspace_id", "session_id", "sequence"],
        schema="interview",
    )
    op.create_unique_constraint(
        "interview_realtime_inputs_scope",
        "realtime_inputs",
        ["id", "workspace_id", "session_id"],
        schema="interview",
    )
    op.create_index(
        "ix_interview_realtime_inputs_session_sequence",
        "realtime_inputs",
        ["workspace_id", "session_id", "sequence", "id"],
        schema="interview",
    )


def _evolve_transcript() -> None:
    """@brief 原位冻结 Transcript 并建立 provenance / Freeze Transcript rows in place and establish provenance."""
    op.add_column(
        "transcript_segments", sa.Column("source_input_id", sa.String(160)), schema="interview"
    )
    op.add_column(
        "transcript_segments", sa.Column("source_artifact_id", sa.String(160)), schema="interview"
    )
    op.add_column(
        "transcript_segments", sa.Column("source_artifact_revision", sa.Integer()), schema="interview"
    )
    op.drop_constraint(
        "transcript_segments_session_id_fkey",
        "transcript_segments",
        schema="interview",
        type_="foreignkey",
    )
    op.execute(
        """
        UPDATE interview.transcript_segments
        SET source_input_id = CASE
                WHEN extensions #>> '{v2,source_ref,resource_type}' = 'realtime_input'
                THEN extensions #>> '{v2,source_ref,id}'
                ELSE NULL
            END,
            source_artifact_id = CASE
                WHEN extensions #>> '{v2,source_ref,resource_type}' = 'artifact'
                THEN extensions #>> '{v2,source_ref,id}'
                ELSE NULL
            END,
            source_artifact_revision = CASE
                WHEN extensions #>> '{v2,source_ref,resource_type}' = 'artifact'
                THEN (extensions #>> '{v2,source_ref,revision}')::integer
                ELSE NULL
            END,
            updated_at = created_at,
            revision = 1,
            extensions = extensions - 'v2' - 'runtime'
        """
    )
    op.alter_column("transcript_segments", "id", type_=sa.String(160), schema="interview")
    op.alter_column(
        "transcript_segments", "session_id", type_=sa.String(160), schema="interview"
    )
    op.alter_column("transcript_segments", "end_ms", nullable=False, schema="interview")
    op.drop_constraint(
        "transcript_segments_session_sequence",
        "transcript_segments",
        schema="interview",
        type_="unique",
    )
    op.drop_index(
        "ix_transcript_segments_session_id_start_ms",
        table_name="transcript_segments",
        schema="interview",
    )
    for column in (
        "is_final",
        "generated_text",
        "media_scheduled_at",
        "media_played_ack_at",
    ):
        op.drop_column("transcript_segments", column, schema="interview")
    op.create_unique_constraint(
        "transcript_segments_session_sequence",
        "transcript_segments",
        ["workspace_id", "session_id", "sequence"],
        schema="interview",
    )
    op.create_unique_constraint(
        "transcript_segments_scope",
        "transcript_segments",
        ["id", "workspace_id", "session_id"],
        schema="interview",
    )
    op.create_check_constraint(
        "ck_transcript_segments_transcript_segments_content",
        "transcript_segments",
        "start_ms >= 0 AND end_ms >= start_ms AND length(text_content) <= 20000",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_transcript_segments_transcript_segments_provenance",
        "transcript_segments",
        "(source_input_id IS NOT NULL AND source_artifact_id IS NULL "
        "AND source_artifact_revision IS NULL) OR "
        "(source_input_id IS NULL AND source_artifact_id IS NOT NULL "
        "AND source_artifact_revision >= 1)",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_transcript_segments_transcript_segments_immutable",
        "transcript_segments",
        "revision = 1 AND created_at = updated_at",
        schema="interview",
    )
    op.create_foreign_key(
        "transcript_segments_input_scope",
        "transcript_segments",
        "realtime_inputs",
        ["source_input_id", "workspace_id", "session_id"],
        ["id", "workspace_id", "session_id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "transcript_segments_artifact_workspace",
        "transcript_segments",
        "artifacts",
        ["source_artifact_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="interview",
        referent_schema="agent",
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_transcript_segments_session_sequence",
        "transcript_segments",
        ["workspace_id", "session_id", "sequence", "id"],
        schema="interview",
    )


def _evolve_reports_and_evidence() -> None:
    """@brief 原位冻结 Report，并将 evidence 投影到有 FK 的完整性表 / Freeze Reports and project evidence into an FK-backed integrity table."""
    op.add_column(
        "reports",
        sa.Column("draft", postgresql.JSONB(astext_type=sa.Text())),
        schema="interview",
    )
    op.drop_constraint(
        "reports_session_id_fkey",
        "reports",
        schema="interview",
        type_="foreignkey",
    )
    op.execute(
        """
        UPDATE interview.reports
        SET draft = extensions #> '{v2,draft}',
            extensions = jsonb_set(
                extensions - 'v2' - 'runtime',
                '{migration_0022}',
                jsonb_build_object(
                    'legacy_revision', revision,
                    'legacy_created_at', created_at,
                    'legacy_updated_at', updated_at
                ),
                true
            ),
            created_at = generated_at,
            updated_at = generated_at,
            revision = 1
        """
    )
    op.alter_column("reports", "id", type_=sa.String(160), schema="interview")
    op.alter_column("reports", "session_id", type_=sa.String(160), schema="interview")
    op.alter_column("reports", "draft", nullable=False, schema="interview")
    op.drop_constraint(
        "interview_reports_session_version",
        "reports",
        schema="interview",
        type_="unique",
    )
    for column in ("report_version", "rubric_version", "engine_version", "report"):
        op.drop_column("reports", column, schema="interview")
    op.create_check_constraint(
        "ck_reports_interview_reports_immutable",
        "reports",
        "jsonb_typeof(draft) = 'object' AND revision = 1 "
        "AND created_at = updated_at AND generated_at = created_at",
        schema="interview",
    )
    op.create_unique_constraint(
        "interview_reports_one_per_session",
        "reports",
        ["workspace_id", "session_id"],
        schema="interview",
    )
    op.create_unique_constraint(
        "interview_reports_scope",
        "reports",
        ["id", "workspace_id", "session_id"],
        schema="interview",
    )
    op.create_table(
        "report_evidence",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("report_id", sa.String(160), nullable=False),
        sa.Column("session_id", sa.String(160), nullable=False),
        sa.Column("segment_id", sa.String(160), nullable=False),
        sa.Column("dimension_id", sa.String(160), nullable=False),
        sa.Column("start_ms", sa.BigInteger(), nullable=False),
        sa.Column("end_ms", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "start_ms >= 0 AND end_ms >= start_ms",
            name="interview_report_evidence_range",
        ),
        sa.ForeignKeyConstraint(
            ["report_id", "workspace_id", "session_id"],
            ["interview.reports.id", "interview.reports.workspace_id", "interview.reports.session_id"],
            ondelete="CASCADE",
            name="interview_report_evidence_report_scope",
        ),
        sa.ForeignKeyConstraint(
            ["segment_id", "workspace_id", "session_id"],
            [
                "interview.transcript_segments.id",
                "interview.transcript_segments.workspace_id",
                "interview.transcript_segments.session_id",
            ],
            ondelete="RESTRICT",
            name="interview_report_evidence_segment_scope",
        ),
        schema="interview",
    )
    op.execute(
        """
        INSERT INTO interview.report_evidence (
            id, workspace_id, report_id, session_id, segment_id,
            dimension_id, start_ms, end_ms, created_at
        )
        SELECT
            'evidence_' || md5(
                report.id || ':' || score.ordinality::text || ':' || evidence.ordinality::text
            ),
            report.workspace_id,
            report.id,
            report.session_id,
            evidence.value ->> 'segment_id',
            score.value ->> 'dimension_id',
            (evidence.value ->> 'start_ms')::bigint,
            (evidence.value ->> 'end_ms')::bigint,
            report.generated_at
        FROM interview.reports AS report
        CROSS JOIN LATERAL jsonb_array_elements(report.draft -> 'rubric_scores')
            WITH ORDINALITY AS score(value, ordinality)
        CROSS JOIN LATERAL jsonb_array_elements(score.value -> 'evidence')
            WITH ORDINALITY AS evidence(value, ordinality)
        """
    )
    op.create_index(
        "ix_interview_report_evidence_report_segment",
        "report_evidence",
        ["workspace_id", "report_id", "segment_id"],
        schema="interview",
    )


def _evolve_session_jobs() -> None:
    """@brief 原位把 report_jobs 泛化为统一 Job typed binding / Generalize report_jobs in place into unified-Job typed bindings."""
    op.add_column("report_jobs", sa.Column("job_kind", sa.String(32)), schema="interview")
    op.execute(
        """
        UPDATE interview.report_jobs AS binding
        SET job_kind = job.job_type,
            extensions = binding.extensions - 'v2' - 'runtime'
        FROM agent.jobs AS job
        WHERE job.id = binding.job_id
        """
    )
    op.execute(
        """
        INSERT INTO interview.report_jobs (
            id, workspace_id, resource_owner_id, job_id, session_id, report_id,
            job_kind, created_at, updated_at, revision, extensions
        )
        SELECT
            'ijob_' || md5(session.pending_end_job_id),
            session.workspace_id,
            job.resource_owner_id,
            job.id,
            session.id,
            NULL,
            job.job_type,
            job.created_at,
            job.created_at,
            1,
            '{}'::jsonb
        FROM interview.sessions AS session
        JOIN agent.jobs AS job
          ON job.id = session.pending_end_job_id
         AND job.workspace_id = session.workspace_id
        WHERE session.pending_end_job_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM interview.report_jobs AS present WHERE present.job_id = job.id
          )
        """
    )
    op.alter_column("report_jobs", "id", type_=sa.String(160), schema="interview")
    op.alter_column("report_jobs", "session_id", type_=sa.String(160), schema="interview")
    op.alter_column("report_jobs", "job_kind", nullable=False, schema="interview")
    op.drop_constraint(
        "report_jobs_job_id_fkey",
        "report_jobs",
        schema="interview",
        type_="foreignkey",
    )
    op.drop_constraint(
        "report_jobs_session_id_fkey",
        "report_jobs",
        schema="interview",
        type_="foreignkey",
    )
    op.drop_column("report_jobs", "report_id", schema="interview")
    op.drop_constraint(
        "report_jobs_job_id_key",
        "report_jobs",
        schema="interview",
        type_="unique",
    )
    op.rename_table("report_jobs", "session_jobs", schema="interview")
    op.create_foreign_key(
        "fk_session_jobs_job_id_jobs",
        "session_jobs",
        "jobs",
        ["job_id"],
        ["id"],
        source_schema="interview",
        referent_schema="agent",
        ondelete="CASCADE",
    )
    op.create_unique_constraint(
        "uq_session_jobs_job_id", "session_jobs", ["job_id"], schema="interview"
    )
    op.create_check_constraint(
        "ck_session_jobs_interview_session_jobs_kind",
        "session_jobs",
        "job_kind IN ('interview.end', 'interview.report')",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_session_jobs_interview_session_jobs_immutable",
        "session_jobs",
        "revision = 1 AND created_at = updated_at",
        schema="interview",
    )
    op.create_index(
        "ix_interview_session_jobs_session_kind",
        "session_jobs",
        ["workspace_id", "session_id", "job_kind"],
        schema="interview",
    )


def _finish_sessions() -> None:
    """@brief 移除 legacy Session 平行列并安装最终约束 / Remove legacy Session duplicate columns and install final constraints."""
    op.drop_constraint(
        "interview_sessions_state", "sessions", schema="interview", type_="check"
    )
    op.drop_constraint(
        "sessions_scenario_id_fkey",
        "sessions",
        schema="interview",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_sessions_workspace_id_created_at", table_name="sessions", schema="interview"
    )
    for column in (
        "state",
        "resume_revision_id",
        "job_target",
        "effective_knowledge_selection",
        "inference_intent",
        "media_capabilities",
        "avatar_output_mode",
        "consent",
        "recording_retention_until",
        "failure",
    ):
        op.drop_column("sessions", column, schema="interview")
    op.create_check_constraint(
        "ck_sessions_interview_sessions_status",
        "sessions",
        "status IN ('created', 'connecting', 'active', 'ending', 'completed', 'failed', 'cancelled')",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_sessions_interview_sessions_snapshots",
        "sessions",
        "jsonb_typeof(spec) = 'object' AND jsonb_typeof(execution_grant) = 'object'",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_sessions_interview_sessions_sequences",
        "sessions",
        "next_realtime_sequence >= 1 AND next_transcript_sequence >= 1",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_sessions_interview_sessions_end_request",
        "sessions",
        "((status = 'ending' AND pending_end_job_id IS NOT NULL AND end_reason IS NOT NULL) "
        "OR (status <> 'ending' AND pending_end_job_id IS NULL AND end_reason IS NULL)) "
        "AND (end_reason IS NULL OR end_reason IN "
        "('completed', 'user_cancelled', 'technical_failure'))",
        schema="interview",
    )
    op.create_check_constraint(
        "ck_sessions_interview_sessions_timeline",
        "sessions",
        "((status IN ('completed', 'failed', 'cancelled') AND ended_at IS NOT NULL) "
        "OR (status NOT IN ('completed', 'failed', 'cancelled') AND ended_at IS NULL)) "
        "AND (status <> 'completed' OR started_at IS NOT NULL) "
        "AND (started_at IS NULL OR started_at BETWEEN created_at AND updated_at) "
        "AND (ended_at IS NULL OR ended_at BETWEEN created_at AND updated_at) "
        "AND (started_at IS NULL OR ended_at IS NULL OR ended_at >= started_at) "
        "AND (report_id IS NULL OR status = 'completed')",
        schema="interview",
    )
    op.create_foreign_key(
        "interview_sessions_scenario_workspace",
        "sessions",
        "scenarios",
        ["scenario_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_sessions_report_id_reports",
        "sessions",
        "reports",
        ["report_id"],
        ["id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="RESTRICT",
        deferrable=True,
        initially="DEFERRED",
    )
    op.create_foreign_key(
        "fk_sessions_pending_end_job_id_jobs",
        "sessions",
        "jobs",
        ["pending_end_job_id"],
        ["id"],
        source_schema="interview",
        referent_schema="agent",
        ondelete="RESTRICT",
        deferrable=True,
        initially="DEFERRED",
    )
    op.create_index(
        "ix_interview_sessions_workspace_created_id",
        "sessions",
        ["workspace_id", "created_at", "id"],
        schema="interview",
    )
    op.create_foreign_key(
        "transcript_segments_session_workspace",
        "transcript_segments",
        "sessions",
        ["session_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "interview_reports_session_workspace",
        "reports",
        "sessions",
        ["session_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "interview_session_jobs_session_workspace",
        "session_jobs",
        "sessions",
        ["session_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="CASCADE",
    )


def _remove_legacy_owner_axis() -> None:
    """@brief 从协作 Interview 表移除 legacy resource_owner 轴 / Remove the legacy resource-owner axis from collaborative Interview tables."""
    op.drop_constraint(
        "fk_tnt_access_snapshots_interview_session_id_scope",
        "access_snapshots",
        schema="knowledge",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_knowledge_access_snapshots_interview_session_workspace",
        "access_snapshots",
        "sessions",
        ["interview_session_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="knowledge",
        referent_schema="interview",
        ondelete="CASCADE",
    )
    for table in (
        "session_jobs",
        "reports",
        "transcript_segments",
        "realtime_inputs",
        "sessions",
        "scenarios",
    ):
        op.drop_column(table, "resource_owner_id", schema="interview")


def _install_integrity_triggers() -> None:
    """@brief 安装跨表 provenance/evidence/Job 状态约束 / Install cross-table provenance, evidence, and Job-state constraints."""
    op.create_check_constraint(
        "ck_jobs_jobs_interview_request_payload",
        "jobs",
        "job_type NOT LIKE 'interview.%' OR ("
        "job_type IN ('interview.end', 'interview.report') "
        "AND target_resource_type = 'interview_session' "
        "AND jsonb_typeof(request_payload) = 'object' "
        "AND jsonb_typeof(request_payload -> 'spec') = 'object')",
        schema="agent",
    )
    op.execute(
        """
        CREATE FUNCTION interview.validate_transcript_artifact_provenance()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog, interview, agent
        AS $$
        DECLARE
            artifact_row agent.artifacts%ROWTYPE;
        BEGIN
            IF NEW.source_artifact_id IS NULL THEN
                RETURN NEW;
            END IF;
            SELECT * INTO artifact_row
            FROM agent.artifacts AS artifact
            WHERE artifact.id = NEW.source_artifact_id
              AND artifact.workspace_id = NEW.workspace_id;
            IF NOT FOUND
               OR artifact_row.subject_type <> 'interview_session'
               OR artifact_row.subject_id <> NEW.session_id
               OR artifact_row.revision <> NEW.source_artifact_revision THEN
                RAISE EXCEPTION 'Transcript Artifact provenance crosses Session scope'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER validate_transcript_artifact_provenance
        BEFORE INSERT OR UPDATE ON interview.transcript_segments
        FOR EACH ROW EXECUTE FUNCTION interview.validate_transcript_artifact_provenance()
        """
    )
    op.execute(
        """
        CREATE FUNCTION interview.validate_report_evidence_range()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog, interview
        AS $$
        DECLARE
            segment_start bigint;
            segment_end bigint;
        BEGIN
            SELECT segment.start_ms, segment.end_ms
              INTO segment_start, segment_end
            FROM interview.transcript_segments AS segment
            WHERE segment.id = NEW.segment_id
              AND segment.workspace_id = NEW.workspace_id
              AND segment.session_id = NEW.session_id;
            IF NOT FOUND OR NEW.start_ms < segment_start OR NEW.end_ms > segment_end THEN
                RAISE EXCEPTION 'Report evidence exceeds its same-Session Transcript segment'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER validate_report_evidence_range
        BEFORE INSERT OR UPDATE ON interview.report_evidence
        FOR EACH ROW EXECUTE FUNCTION interview.validate_report_evidence_range()
        """
    )
    op.execute(
        """
        CREATE FUNCTION interview.validate_interview_job_alignment()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog, interview, agent
        AS $$
        DECLARE
            job_row agent.jobs%ROWTYPE;
            session_row interview.sessions%ROWTYPE;
            binding_count integer;
        BEGIN
            IF TG_TABLE_SCHEMA = 'agent' THEN
                job_row := NEW;
                IF job_row.job_type NOT IN ('interview.end', 'interview.report') THEN
                    RETURN NEW;
                END IF;
                SELECT * INTO session_row
                FROM interview.sessions AS session
                WHERE session.id = job_row.target_resource_id
                  AND session.workspace_id = job_row.workspace_id;
            ELSE
                session_row := NEW;
                IF session_row.report_id IS NOT NULL THEN
                    SELECT count(*) INTO binding_count
                    FROM interview.session_jobs AS report_binding
                    JOIN agent.jobs AS report_job
                      ON report_job.id = report_binding.job_id
                     AND report_job.workspace_id = report_binding.workspace_id
                    WHERE report_binding.workspace_id = session_row.workspace_id
                      AND report_binding.session_id = session_row.id
                      AND report_binding.job_kind = 'interview.report'
                      AND report_job.status = 'succeeded'
                      AND EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements(report_job.result_refs) AS result(value)
                          WHERE result.value ->> 'resource_type' = 'interview_report'
                            AND result.value ->> 'id' = session_row.report_id
                      );
                    IF binding_count <> 1 THEN
                        RAISE EXCEPTION 'attached Report requires exactly one succeeded Report Job'
                            USING ERRCODE = '23514';
                    END IF;
                END IF;
                IF session_row.pending_end_job_id IS NULL THEN
                    RETURN NEW;
                END IF;
                SELECT * INTO job_row
                FROM agent.jobs AS job
                WHERE job.id = session_row.pending_end_job_id
                  AND job.workspace_id = session_row.workspace_id;
            END IF;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'Interview Job and Session scope do not align'
                    USING ERRCODE = '23514';
            END IF;
            SELECT count(*) INTO binding_count
            FROM interview.session_jobs AS binding
            WHERE binding.job_id = job_row.id
              AND binding.workspace_id = job_row.workspace_id
              AND binding.session_id = session_row.id
              AND binding.job_kind = job_row.job_type;
            IF binding_count <> 1 THEN
                RAISE EXCEPTION 'Interview Job lacks exactly one typed Session binding'
                    USING ERRCODE = '23514';
            END IF;
            IF job_row.job_type = 'interview.end' THEN
                IF job_row.status IN ('queued', 'running') AND (
                    session_row.status <> 'ending'
                    OR session_row.pending_end_job_id IS DISTINCT FROM job_row.id
                ) THEN
                    RAISE EXCEPTION 'live end Job requires its ending Session'
                        USING ERRCODE = '23514';
                ELSIF job_row.status IN ('succeeded', 'failed', 'cancelled')
                      AND session_row.status NOT IN ('completed', 'failed', 'cancelled') THEN
                    RAISE EXCEPTION 'terminal end Job requires a terminal Session'
                        USING ERRCODE = '23514';
                ELSIF job_row.status = 'expired' THEN
                    RAISE EXCEPTION 'end Job cannot expire while bound to a Session'
                        USING ERRCODE = '23514';
                END IF;
            ELSE
                IF session_row.status <> 'completed' THEN
                    RAISE EXCEPTION 'Report Job requires a completed Session'
                        USING ERRCODE = '23514';
                END IF;
                IF job_row.status IN ('queued', 'running', 'failed', 'cancelled', 'expired')
                   AND session_row.report_id IS NOT NULL THEN
                    RAISE EXCEPTION 'non-succeeded Report Job cannot attach a Report'
                        USING ERRCODE = '23514';
                END IF;
                IF job_row.status = 'succeeded' AND (
                    session_row.report_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(job_row.result_refs) AS result(value)
                        WHERE result.value ->> 'resource_type' = 'interview_report'
                          AND result.value ->> 'id' = session_row.report_id
                    )
                ) THEN
                    RAISE EXCEPTION 'succeeded Report Job must reference the attached Report'
                        USING ERRCODE = '23514';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER validate_interview_session_job_alignment
        AFTER INSERT OR UPDATE ON interview.sessions
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION interview.validate_interview_job_alignment()
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER validate_interview_unified_job_alignment
        AFTER INSERT OR UPDATE ON agent.jobs
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION interview.validate_interview_job_alignment()
        """
    )


def _install_runtime_security(app_role: str, owner_role: str) -> None:
    """@brief 为新表安装 FORCE RLS 与最小权限 / Install FORCE RLS and least privileges for new tables."""
    for table in _NEW_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY workspace_app_tenant_scope ON {table} AS PERMISSIVE FOR ALL "
            f"TO {app_role} USING (workspace_id = current_setting('app.workspace_id', true)) "
            f"WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )
        op.execute(
            f"CREATE POLICY interview_owner_narrow_functions ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (workspace_id = current_setting('app.workspace_id', true)) "
            f"WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE {table} FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT ON TABLE interview.realtime_connections, "
        f"interview.report_evidence TO {app_role}"
    )
    for table in (
        "interview.realtime_inputs",
        "interview.transcript_segments",
        "interview.reports",
        "interview.session_jobs",
    ):
        op.execute(f"REVOKE UPDATE, DELETE ON TABLE {table} FROM {app_role}")
    op.execute(
        f"REVOKE UPDATE, DELETE ON TABLE interview.realtime_connections, "
        f"interview.report_evidence FROM {app_role}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION interview.validate_transcript_artifact_provenance(), "
        f"interview.validate_report_evidence_range(), "
        f"interview.validate_interview_job_alignment() TO {app_role}"
    )


def upgrade() -> None:
    """@brief 原位升级 Interview V2 persistence / Upgrade Interview V2 persistence in place."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    _install_migration_visibility(owner_role)
    _preflight_upgrade()
    _evolve_scenarios()
    _add_session_v2_columns()
    _create_realtime_connections()
    _evolve_realtime_inputs()
    _evolve_transcript()
    _evolve_reports_and_evidence()
    _evolve_session_jobs()
    _finish_sessions()
    _remove_legacy_owner_axis()
    _install_integrity_triggers()
    _install_runtime_security(app_role, owner_role)
    _remove_migration_visibility()


def _require_empty_downgrade() -> None:
    """@brief 仅允许无 Interview V2 业务状态时 downgrade / Allow downgrade only without Interview V2 business state."""
    populated = _count(
        """
        SELECT
            (SELECT count(*) FROM interview.scenarios)
          + (SELECT count(*) FROM interview.sessions)
          + (SELECT count(*) FROM interview.realtime_connections)
          + (SELECT count(*) FROM interview.realtime_inputs)
          + (SELECT count(*) FROM interview.transcript_segments)
          + (SELECT count(*) FROM interview.reports)
          + (SELECT count(*) FROM interview.report_evidence)
          + (SELECT count(*) FROM interview.session_jobs)
          + (SELECT count(*) FROM agent.jobs WHERE job_type LIKE 'interview.%')
          + (SELECT count(*) FROM agent.artifacts
             WHERE subject_type = 'interview_session')
        """
    )
    if populated:
        raise RuntimeError(
            "0022 downgrade is intentionally empty-state only; Interview V2 state cannot be "
            "losslessly represented by the legacy mutable/event schemas"
        )


def _drop_v2_relations_for_downgrade() -> None:
    """@brief 按依赖反序移除 0022 constraints/indexes / Remove 0022 constraints/indexes in reverse dependency order."""
    op.drop_constraint(
        "fk_knowledge_access_snapshots_interview_session_workspace",
        "access_snapshots",
        schema="knowledge",
        type_="foreignkey",
    )
    op.drop_constraint(
        "transcript_segments_input_scope",
        "transcript_segments",
        schema="interview",
        type_="foreignkey",
    )
    op.drop_constraint(
        "interview_realtime_inputs_connection_scope",
        "realtime_inputs",
        schema="interview",
        type_="foreignkey",
    )
    op.drop_table("report_evidence", schema="interview")
    op.drop_table("realtime_connections", schema="interview")
    for table, constraint, constraint_type in (
        ("realtime_inputs", "interview_realtime_inputs_session_workspace", "foreignkey"),
        ("realtime_inputs", "interview_realtime_inputs_session_sequence", "unique"),
        ("realtime_inputs", "interview_realtime_inputs_scope", "unique"),
        ("realtime_inputs", "ck_realtime_inputs_interview_realtime_inputs_envelope", "check"),
        ("realtime_inputs", "ck_realtime_inputs_interview_realtime_inputs_immutable", "check"),
        ("transcript_segments", "transcript_segments_artifact_workspace", "foreignkey"),
        ("transcript_segments", "transcript_segments_session_workspace", "foreignkey"),
        ("transcript_segments", "transcript_segments_session_sequence", "unique"),
        ("transcript_segments", "transcript_segments_scope", "unique"),
        ("transcript_segments", "ck_transcript_segments_transcript_segments_content", "check"),
        ("transcript_segments", "ck_transcript_segments_transcript_segments_provenance", "check"),
        ("transcript_segments", "ck_transcript_segments_transcript_segments_immutable", "check"),
        ("reports", "interview_reports_session_workspace", "foreignkey"),
        ("reports", "interview_reports_one_per_session", "unique"),
        ("reports", "interview_reports_scope", "unique"),
        ("reports", "ck_reports_interview_reports_immutable", "check"),
        ("session_jobs", "interview_session_jobs_session_workspace", "foreignkey"),
        ("session_jobs", "fk_session_jobs_job_id_jobs", "foreignkey"),
        ("session_jobs", "uq_session_jobs_job_id", "unique"),
        ("session_jobs", "ck_session_jobs_interview_session_jobs_kind", "check"),
        ("session_jobs", "ck_session_jobs_interview_session_jobs_immutable", "check"),
        ("sessions", "interview_sessions_scenario_workspace", "foreignkey"),
        ("sessions", "fk_sessions_report_id_reports", "foreignkey"),
        ("sessions", "fk_sessions_pending_end_job_id_jobs", "foreignkey"),
        ("sessions", "interview_sessions_id_workspace", "unique"),
        ("sessions", "ck_sessions_interview_sessions_status", "check"),
        ("sessions", "ck_sessions_interview_sessions_snapshots", "check"),
        ("sessions", "ck_sessions_interview_sessions_sequences", "check"),
        ("sessions", "ck_sessions_interview_sessions_end_request", "check"),
        ("sessions", "ck_sessions_interview_sessions_timeline", "check"),
        ("scenarios", "interview_scenarios_id_workspace", "unique"),
        ("scenarios", "ck_scenarios_interview_scenarios_status", "check"),
        ("scenarios", "ck_scenarios_interview_scenarios_spec", "check"),
    ):
        op.drop_constraint(
            constraint,
            table,
            schema="interview",
            type_=constraint_type,
        )
    for table, index in (
        ("realtime_inputs", "ix_interview_realtime_inputs_session_sequence"),
        ("transcript_segments", "ix_transcript_segments_session_sequence"),
        ("session_jobs", "ix_interview_session_jobs_session_kind"),
        ("sessions", "ix_interview_sessions_workspace_created_id"),
        ("scenarios", "ix_interview_scenarios_workspace_created_id"),
    ):
        op.drop_index(index, table_name=table, schema="interview")


def _restore_legacy_relations_and_indexes() -> None:
    """@brief 在空态精确恢复 0021 tenant relations/indexes / Exactly restore 0021 tenant relations/indexes in the empty state."""
    tables = (
        "scenarios",
        "sessions",
        "events",
        "transcript_segments",
        "reports",
        "report_jobs",
    )
    for table in tables:
        op.create_foreign_key(
            f"{table}_resource_owner_id_fkey",
            table,
            "users",
            ["resource_owner_id"],
            ["id"],
            source_schema="interview",
            referent_schema="identity",
            ondelete="RESTRICT",
        )
        op.create_unique_constraint(
            f"uq_tnt_{table}_id_ws_owner",
            table,
            ["id", "workspace_id", "resource_owner_id"],
            schema="interview",
        )
        op.create_foreign_key(
            f"fk_tnt_{table}_workspace_scope",
            table,
            "workspaces",
            ["workspace_id", "resource_owner_id"],
            ["id", "resource_owner_id"],
            source_schema="interview",
            referent_schema="identity",
            ondelete="RESTRICT",
        )
        op.create_index(
            f"ix_{table}_resource_owner_id",
            table,
            ["resource_owner_id"],
            schema="interview",
        )

    op.create_check_constraint(
        "interview_sessions_state",
        "sessions",
        "state IN ('created', 'preparing', 'ready', 'connecting', 'in_progress', "
        "'ending', 'processing_report', 'completed', 'aborted', 'expired', 'failed')",
        schema="interview",
    )
    op.create_unique_constraint(
        "interview_events_session_sequence",
        "events",
        ["session_id", "sequence"],
        schema="interview",
    )
    op.create_unique_constraint(
        "transcript_segments_session_sequence",
        "transcript_segments",
        ["session_id", "sequence"],
        schema="interview",
    )
    op.create_unique_constraint(
        "interview_reports_session_version",
        "reports",
        ["session_id", "report_version"],
        schema="interview",
    )
    op.create_unique_constraint(
        "report_jobs_job_id_key",
        "report_jobs",
        ["job_id"],
        schema="interview",
    )

    for name, table, target, local_columns, remote_columns, remote_schema, ondelete in (
        (
            "fk_tnt_sessions_scenario_id_scope",
            "sessions",
            "scenarios",
            ["scenario_id", "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            "interview",
            "RESTRICT",
        ),
        (
            "fk_tnt_sessions_resume_revision_id_scope",
            "sessions",
            "revisions",
            ["resume_revision_id", "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            "resume",
            "SET NULL",
        ),
        (
            "fk_tnt_events_session_id_scope",
            "events",
            "sessions",
            ["session_id", "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            "interview",
            "CASCADE",
        ),
        (
            "fk_tnt_transcript_segments_session_id_scope",
            "transcript_segments",
            "sessions",
            ["session_id", "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            "interview",
            "CASCADE",
        ),
        (
            "fk_tnt_reports_session_id_scope",
            "reports",
            "sessions",
            ["session_id", "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            "interview",
            "CASCADE",
        ),
        (
            "fk_tnt_report_jobs_job_id_scope",
            "report_jobs",
            "jobs",
            ["job_id", "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            "agent",
            "CASCADE",
        ),
        (
            "fk_tnt_report_jobs_session_id_scope",
            "report_jobs",
            "sessions",
            ["session_id", "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            "interview",
            "CASCADE",
        ),
        (
            "fk_tnt_report_jobs_report_id_scope",
            "report_jobs",
            "reports",
            ["report_id", "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            "interview",
            "SET NULL",
        ),
    ):
        scoped_delete = (
            f"SET NULL ({local_columns[0]})" if ondelete == "SET NULL" else ondelete
        )
        op.create_foreign_key(
            name,
            table,
            target,
            local_columns,
            remote_columns,
            source_schema="interview",
            referent_schema=remote_schema,
            ondelete=scoped_delete,
        )
    op.create_foreign_key(
        "sessions_resume_revision_id_fkey",
        "sessions",
        "revisions",
        ["resume_revision_id"],
        ["id"],
        source_schema="interview",
        referent_schema="resume",
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "report_jobs_report_id_fkey",
        "report_jobs",
        "reports",
        ["report_id"],
        ["id"],
        source_schema="interview",
        referent_schema="interview",
        ondelete="SET NULL",
    )
    for name, table, target, column, remote_schema, ondelete in (
        (
            "sessions_scenario_id_fkey",
            "sessions",
            "scenarios",
            "scenario_id",
            "interview",
            "RESTRICT",
        ),
        (
            "events_session_id_fkey",
            "events",
            "sessions",
            "session_id",
            "interview",
            "CASCADE",
        ),
        (
            "transcript_segments_session_id_fkey",
            "transcript_segments",
            "sessions",
            "session_id",
            "interview",
            "CASCADE",
        ),
        (
            "reports_session_id_fkey",
            "reports",
            "sessions",
            "session_id",
            "interview",
            "CASCADE",
        ),
        (
            "report_jobs_job_id_fkey",
            "report_jobs",
            "jobs",
            "job_id",
            "agent",
            "CASCADE",
        ),
        (
            "report_jobs_session_id_fkey",
            "report_jobs",
            "sessions",
            "session_id",
            "interview",
            "CASCADE",
        ),
    ):
        op.create_foreign_key(
            name,
            table,
            target,
            [column],
            ["id"],
            source_schema="interview",
            referent_schema=remote_schema,
            ondelete=ondelete,
        )
    op.create_foreign_key(
        "fk_tnt_access_snapshots_interview_session_id_scope",
        "access_snapshots",
        "sessions",
        ["interview_session_id", "workspace_id", "resource_owner_id"],
        ["id", "workspace_id", "resource_owner_id"],
        source_schema="knowledge",
        referent_schema="interview",
        ondelete="CASCADE",
    )

    for table, columns in (
        ("scenarios", ["workspace_id", "updated_at"]),
        ("sessions", ["workspace_id", "created_at"]),
        ("events", ["workspace_id"]),
        ("events", ["session_id", "sequence"]),
        ("transcript_segments", ["session_id", "start_ms"]),
    ):
        op.create_index(
            f"ix_{table}_{'_'.join(columns)}",
            table,
            columns,
            schema="interview",
        )


def downgrade() -> None:
    """@brief 仅在空态恢复 0021 legacy schema / Restore the 0021 legacy schema only when empty."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    for table in _FINAL_POLICY_TABLES:
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )
    for table in _NEW_TABLES:
        op.execute(f"DROP POLICY interview_owner_narrow_functions ON {table}")
        op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
    _require_empty_downgrade()
    op.execute(
        "DROP TRIGGER validate_interview_unified_job_alignment ON agent.jobs"
    )
    op.execute(
        "DROP TRIGGER validate_interview_session_job_alignment ON interview.sessions"
    )
    op.execute(
        "DROP TRIGGER validate_report_evidence_range ON interview.report_evidence"
    )
    op.execute(
        "DROP TRIGGER validate_transcript_artifact_provenance ON interview.transcript_segments"
    )
    op.execute("DROP FUNCTION interview.validate_interview_job_alignment()")
    op.execute("DROP FUNCTION interview.validate_report_evidence_range()")
    op.execute("DROP FUNCTION interview.validate_transcript_artifact_provenance()")
    op.drop_constraint(
        "ck_jobs_jobs_interview_request_payload", "jobs", schema="agent", type_="check"
    )

    # 空表允许直接恢复 legacy 形状；任何数据存在时上面的 gate 已失败。
    _drop_v2_relations_for_downgrade()
    op.rename_table("session_jobs", "report_jobs", schema="interview")
    op.rename_table("realtime_inputs", "events", schema="interview")

    for table in ("scenarios", "sessions", "events", "transcript_segments", "reports", "report_jobs"):
        op.add_column(
            table,
            sa.Column(
                "resource_owner_id",
                sa.String(128),
                nullable=False,
            ),
            schema="interview",
        )

    op.add_column("scenarios", sa.Column("title", sa.String(512), nullable=False), schema="interview")
    op.add_column(
        "scenarios",
        sa.Column("locale", sa.String(32), nullable=False, server_default=sa.text("'zh-CN'")),
        schema="interview",
    )
    op.add_column(
        "scenarios", sa.Column("role_target", postgresql.JSONB(), nullable=False), schema="interview"
    )
    op.add_column(
        "scenarios", sa.Column("rubric", postgresql.JSONB(), nullable=False), schema="interview"
    )
    op.add_column(
        "scenarios",
        sa.Column("is_template", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema="interview",
    )
    op.add_column("scenarios", sa.Column("deleted_at", sa.DateTime(timezone=True)), schema="interview")
    op.drop_column("scenarios", "status", schema="interview")
    op.drop_column("scenarios", "spec", schema="interview")
    op.alter_column("scenarios", "id", type_=sa.String(128), schema="interview")

    op.add_column(
        "sessions",
        sa.Column("state", sa.String(32), nullable=False, server_default=sa.text("'created'")),
        schema="interview",
    )
    op.add_column("sessions", sa.Column("resume_revision_id", sa.String(128)), schema="interview")
    for column in (
        "job_target",
        "effective_knowledge_selection",
        "inference_intent",
        "media_capabilities",
        "consent",
    ):
        op.add_column(
            "sessions", sa.Column(column, postgresql.JSONB(), nullable=False), schema="interview"
        )
    op.add_column(
        "sessions", sa.Column("avatar_output_mode", sa.String(32), nullable=False), schema="interview"
    )
    op.add_column(
        "sessions", sa.Column("recording_retention_until", sa.DateTime(timezone=True)), schema="interview"
    )
    op.add_column("sessions", sa.Column("failure", postgresql.JSONB()), schema="interview")
    for column in (
        "status",
        "spec",
        "execution_grant",
        "report_id",
        "pending_end_job_id",
        "end_reason",
        "next_realtime_sequence",
        "next_transcript_sequence",
    ):
        op.drop_column("sessions", column, schema="interview")
    op.alter_column("sessions", "id", type_=sa.String(128), schema="interview")
    op.alter_column("sessions", "scenario_id", type_=sa.String(128), schema="interview")

    op.add_column("events", sa.Column("ack_sequence", sa.BigInteger()), schema="interview")
    op.add_column(
        "events", sa.Column("event_type", sa.String(128), nullable=False), schema="interview"
    )
    op.add_column(
        "events", sa.Column("payload", postgresql.JSONB(), nullable=False), schema="interview"
    )
    op.add_column("events", sa.Column("trace_id", sa.String(128)), schema="interview")
    op.drop_column("events", "fingerprint_sha256", schema="interview")
    op.drop_column("events", "connection_id", schema="interview")
    op.alter_column("events", "id", type_=sa.String(128), schema="interview")
    op.alter_column("events", "session_id", type_=sa.String(128), schema="interview")

    op.add_column(
        "transcript_segments",
        sa.Column("is_final", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema="interview",
    )
    op.add_column("transcript_segments", sa.Column("generated_text", sa.Text()), schema="interview")
    op.add_column(
        "transcript_segments", sa.Column("media_scheduled_at", sa.DateTime(timezone=True)), schema="interview"
    )
    op.add_column(
        "transcript_segments", sa.Column("media_played_ack_at", sa.DateTime(timezone=True)), schema="interview"
    )
    op.drop_column("transcript_segments", "source_input_id", schema="interview")
    op.drop_column("transcript_segments", "source_artifact_id", schema="interview")
    op.drop_column("transcript_segments", "source_artifact_revision", schema="interview")
    op.alter_column("transcript_segments", "end_ms", nullable=True, schema="interview")
    op.alter_column("transcript_segments", "id", type_=sa.String(128), schema="interview")
    op.alter_column("transcript_segments", "session_id", type_=sa.String(128), schema="interview")

    op.add_column(
        "reports", sa.Column("report_version", sa.Integer(), nullable=False), schema="interview"
    )
    op.add_column(
        "reports", sa.Column("rubric_version", sa.String(128), nullable=False), schema="interview"
    )
    op.add_column(
        "reports", sa.Column("engine_version", sa.String(256), nullable=False), schema="interview"
    )
    op.add_column("reports", sa.Column("report", postgresql.JSONB(), nullable=False), schema="interview")
    op.drop_column("reports", "draft", schema="interview")
    op.alter_column("reports", "id", type_=sa.String(128), schema="interview")
    op.alter_column("reports", "session_id", type_=sa.String(128), schema="interview")

    op.add_column("report_jobs", sa.Column("report_id", sa.String(128)), schema="interview")
    op.drop_column("report_jobs", "job_kind", schema="interview")
    op.alter_column("report_jobs", "id", type_=sa.String(128), schema="interview")
    op.alter_column("report_jobs", "session_id", type_=sa.String(128), schema="interview")

    _restore_legacy_relations_and_indexes()

    for table in _FINAL_POLICY_TABLES:
        final_name = table
        if table == "interview.realtime_inputs":
            final_name = "interview.events"
        elif table == "interview.session_jobs":
            final_name = "interview.report_jobs"
        op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {final_name}")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE interview.scenarios, "
        f"interview.sessions, interview.events, interview.transcript_segments, "
        f"interview.reports, interview.report_jobs TO {app_role}"
    )
