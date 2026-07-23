"""@brief 发布 API V2 Platform persistence 与统一 Artifact/Event truth / Publish API V2 Platform persistence.

Revision ID: 20260723_0017
Revises: 20260723_0016
Create Date: 2026-07-23

Job、outbox 与 audit 原位演进，避免平行真相表。旧 Resume/Interview Artifact
由本 revision 在同一事务内确定性转换：保留 ID、Workspace、legacy owner、subject、
storage key、digest、尺寸、时间与 source map；只对冲突真相、孤儿引用、损坏内容
或无法表达的 envelope fail closed。转换后移除平行表，仅保留 ``agent.artifacts``
为 metadata truth。
"""

from __future__ import annotations

import re
from typing import Any, Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260723_0017"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0016"
"""@brief 线性前驱 revision / Linear predecessor revision."""

branch_labels = None
"""@brief 此迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 此迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal["owner_role", "app_role", "dashboard_role", "migrator_role"]
"""@brief 允许读取的 dbctl role 配置 / dbctl role options accepted by this revision."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role 标识符 allowlist / PostgreSQL role-identifier allowlist."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL 标识符字节上限 / PostgreSQL identifier byte limit."""

_MIGRATION_POLICY = "platform_v2_owner_migration_0017"
"""@brief FORCE RLS 下的临时 owner policy / Temporary owner policy under FORCE RLS."""

_CORE_TABLES = (
    "identity.audit_events",
    "agent.jobs",
    "agent.outbox_events",
)
"""@brief 原位演进的唯一 Job/Event/Audit 表 / Sole Job/Event/Audit tables evolved in place."""

_LEGACY_ARTIFACT_TABLES = (
    "resume.render_artifacts",
    "resume.artifact_blobs",
    "resume.pdf_source_map_entries",
    "interview.recording_artifacts",
)
"""@brief 必须转换或移除的旧 Artifact truth tables / Legacy Artifact truth tables requiring conversion or removal."""

_LEGACY_ARTIFACT_PARENT_TABLES = ("resume.revisions", "interview.sessions")
"""@brief Artifact subject 完整性校验所需父表 / Parent tables needed for Artifact subject integrity."""

_PREFLIGHT_TABLES = (
    *_CORE_TABLES,
    *_LEGACY_ARTIFACT_PARENT_TABLES,
    *_LEGACY_ARTIFACT_TABLES,
    "resume.render_jobs",
)
"""@brief migration preflight 的静态表 allowlist / Static table allowlist for migration preflight."""

_NEW_PLATFORM_TABLES = (
    "agent.workspace_event_sequences",
    "agent.artifacts",
    "agent.artifact_contents",
    "agent.artifact_pdf_source_maps",
)
"""@brief 0017 新建表 / Tables created by 0017."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回安全引用的 runtime role / Return a safely quoted runtime role."""
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


def _install_migration_visibility(owner_role: str, tables: tuple[str, ...]) -> None:
    """@brief 为静态表集安装临时 owner visibility / Install temporary owner visibility for a static table set."""
    for table in tables:
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )


def _remove_migration_visibility(tables: tuple[str, ...]) -> None:
    """@brief 从静态表集移除临时 owner visibility / Remove temporary owner visibility from a static table set."""
    for table in reversed(tables):
        op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {table}")


def _count(statement: str) -> int:
    """@brief 执行只来自本文件常量的 count SQL / Execute count SQL supplied only by this module."""
    value = op.get_bind().scalar(sa.text(statement))
    return int(value or 0)


def _preflight_upgrade() -> None:
    """@brief 分类拒绝不能无损转换的历史状态 / Classify legacy state that cannot be converted losslessly."""
    duplicate_identity = _count(
        """
        SELECT count(*)
        FROM resume.render_artifacts AS resume_artifact
        JOIN interview.recording_artifacts AS interview_artifact
          ON interview_artifact.id = resume_artifact.id
        """
    )
    duplicate_storage = _count(
        """
        SELECT count(*)
        FROM resume.render_artifacts AS resume_artifact
        JOIN interview.recording_artifacts AS interview_artifact
          ON interview_artifact.storage_key = resume_artifact.storage_key
        """
    )
    if duplicate_identity or duplicate_storage:
        raise RuntimeError(
            "legacy Artifact namespaces contain colliding IDs or storage keys; "
            "a single metadata truth cannot preserve both identities"
        )

    invalid_resume_artifacts = _count(
        r"""
        SELECT count(*)
        FROM resume.render_artifacts AS artifact
        LEFT JOIN resume.revisions AS resume_revision
          ON resume_revision.id = artifact.resume_revision_id
         AND resume_revision.resume_id = artifact.resume_id
         AND resume_revision.workspace_id = artifact.workspace_id
         AND resume_revision.resource_owner_id = artifact.resource_owner_id
        WHERE artifact.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR artifact.resume_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR resume_revision.id IS NULL OR resume_revision.revision_no < 1
           OR artifact.storage_key = ''
           OR artifact.content_sha256 !~* '^[a-f0-9]{64}$'
           OR artifact.content_bytes NOT BETWEEN 0 AND 1073741824
           OR jsonb_typeof(artifact.extensions) <> 'object'
           OR artifact.revision < 1
           OR artifact.updated_at < artifact.created_at
           OR (artifact.expires_at IS NOT NULL
               AND artifact.expires_at <= artifact.created_at)
        """
    )
    if invalid_resume_artifacts:
        raise RuntimeError(
            "legacy Resume Artifact rows contain invalid identity, scoped revision, digest, "
            "size, lifecycle, storage, or expiration data"
        )

    invalid_recordings = _count(
        r"""
        SELECT count(*)
        FROM interview.recording_artifacts AS artifact
        LEFT JOIN interview.sessions AS session
          ON session.id = artifact.session_id
         AND session.workspace_id = artifact.workspace_id
         AND session.resource_owner_id = artifact.resource_owner_id
        WHERE artifact.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR artifact.session_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR session.id IS NULL
           OR artifact.storage_key = ''
           OR artifact.content_sha256 !~* '^[a-f0-9]{64}$'
           OR artifact.content_bytes NOT BETWEEN 0 AND 1073741824
           OR btrim(artifact.media_kind) = ''
           OR jsonb_typeof(artifact.extensions) <> 'object'
           OR artifact.revision < 1
           OR artifact.updated_at < artifact.created_at
           OR (artifact.expires_at IS NOT NULL
               AND artifact.expires_at <= artifact.created_at)
           OR (artifact.deleted_at IS NOT NULL
               AND artifact.deleted_at < artifact.created_at)
        """
    )
    if invalid_recordings:
        raise RuntimeError(
            "legacy Interview recording rows contain invalid identity, scoped session, digest, "
            "size, media, lifecycle, storage, expiration, or deletion data"
        )

    invalid_blobs = _count(
        r"""
        SELECT count(*)
        FROM resume.artifact_blobs AS blob
        JOIN resume.render_artifacts AS artifact ON artifact.id = blob.artifact_id
        WHERE blob.workspace_id <> artifact.workspace_id
           OR blob.resource_owner_id <> artifact.resource_owner_id
           OR blob.revision < 1 OR blob.updated_at < blob.created_at
           OR jsonb_typeof(blob.extensions) <> 'object'
           OR octet_length(blob.content) <> artifact.content_bytes
           OR encode(sha256(blob.content), 'hex') <> lower(artifact.content_sha256)
        """
    )
    resume_artifacts_without_blob = _count(
        """
        SELECT count(*)
        FROM resume.render_artifacts AS artifact
        LEFT JOIN resume.artifact_blobs AS blob ON blob.artifact_id = artifact.id
        WHERE blob.id IS NULL
        """
    )
    if invalid_blobs:
        raise RuntimeError(
            "legacy Resume Artifact bytes disagree with metadata digest, size, or scope"
        )
    if resume_artifacts_without_blob:
        raise RuntimeError(
            "legacy Resume Artifacts lack a verified inline blob; import every Artifact into "
            "resume.artifact_blobs with matching scope, SHA-256, and size, or complete an "
            "explicit data-disposition migration before upgrading to API V2"
        )

    readable_recordings_without_content = _count(
        """
        SELECT count(*)
        FROM interview.recording_artifacts AS artifact
        WHERE artifact.deleted_at IS NULL
        """
    )
    if readable_recordings_without_content:
        raise RuntimeError(
            "legacy Interview recordings that are not deleted have no supported inline content "
            "source; import their verified bytes through an explicit supported migration or "
            "complete an explicit data disposition before upgrading to API V2"
        )

    invalid_entry_maps = _count(
        r"""
        SELECT count(*)
        FROM resume.pdf_source_map_entries AS entry
        JOIN resume.render_artifacts AS artifact ON artifact.id = entry.artifact_id
        WHERE entry.workspace_id <> artifact.workspace_id
           OR entry.resource_owner_id <> artifact.resource_owner_id
           OR entry.node_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR cardinality(entry.field_path) > 20
           OR EXISTS (
                SELECT 1 FROM unnest(entry.field_path) AS path(part)
                WHERE length(part) > 100
           )
           OR entry.page < 1
           OR jsonb_typeof(entry.rects) <> 'array'
           OR jsonb_array_length(entry.rects) = 0
           OR entry.revision < 1 OR entry.updated_at < entry.created_at
           OR jsonb_typeof(entry.extensions) <> 'object'
           OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(entry.rects) AS rectangle(value)
                WHERE jsonb_typeof(rectangle.value) <> 'object'
                   OR jsonb_typeof(rectangle.value -> 'x') <> 'number'
                   OR jsonb_typeof(rectangle.value -> 'y') <> 'number'
                   OR jsonb_typeof(rectangle.value -> 'width') <> 'number'
                   OR jsonb_typeof(rectangle.value -> 'height') <> 'number'
                   OR rectangle.value ->> 'unit' <> 'pt'
                   OR (rectangle.value ->> 'width')::numeric < 0
                   OR (rectangle.value ->> 'height')::numeric < 0
           )
        """
    )
    invalid_blob_maps = _count(
        r"""
        SELECT count(*)
        FROM resume.artifact_blobs AS blob
        JOIN resume.render_artifacts AS artifact ON artifact.id = blob.artifact_id
        JOIN resume.revisions AS resume_revision
          ON resume_revision.id = artifact.resume_revision_id
        WHERE blob.source_map IS NOT NULL AND (
               jsonb_typeof(blob.source_map) <> 'object'
            OR blob.source_map ->> 'artifact_id' <> artifact.id
            OR blob.source_map ->> 'resume_id' <> artifact.resume_id
            OR jsonb_typeof(blob.source_map -> 'resume_revision') <> 'number'
            OR (blob.source_map ->> 'resume_revision')::numeric
               <> resume_revision.revision_no
            OR jsonb_typeof(blob.source_map -> 'page_count') <> 'number'
            OR (blob.source_map ->> 'page_count')::numeric < 1
            OR (blob.source_map ->> 'page_count')::numeric > 2147483647
            OR trunc((blob.source_map ->> 'page_count')::numeric)
               <> (blob.source_map ->> 'page_count')::numeric
            OR jsonb_typeof(blob.source_map -> 'nodes') <> 'array'
            OR jsonb_array_length(blob.source_map -> 'nodes') > 10000
            OR EXISTS (
                SELECT 1
                FROM jsonb_array_elements(blob.source_map -> 'nodes') AS node(value)
                WHERE COALESCE(node.value ->> 'entity_id', node.value ->> 'node_id', '')
                      !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
                   OR jsonb_typeof(node.value -> 'field_path') <> 'array'
                   OR jsonb_array_length(node.value -> 'field_path') > 20
                   OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(node.value -> 'field_path') AS path(value)
                        WHERE jsonb_typeof(path.value) <> 'string'
                           OR length(path.value #>> '{}') > 100
                   )
                   OR jsonb_typeof(node.value -> 'page') <> 'number'
                   OR (node.value ->> 'page')::numeric < 1
                   OR trunc((node.value ->> 'page')::numeric)
                      <> (node.value ->> 'page')::numeric
                   OR (node.value ->> 'page')::numeric
                      > (blob.source_map ->> 'page_count')::numeric
                   OR jsonb_typeof(node.value -> 'rects') <> 'array'
                   OR jsonb_array_length(node.value -> 'rects') = 0
                   OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(node.value -> 'rects') AS rectangle(value)
                        WHERE jsonb_typeof(rectangle.value) <> 'object'
                           OR jsonb_typeof(rectangle.value -> 'x') <> 'number'
                           OR jsonb_typeof(rectangle.value -> 'y') <> 'number'
                           OR jsonb_typeof(rectangle.value -> 'width') <> 'number'
                           OR jsonb_typeof(rectangle.value -> 'height') <> 'number'
                           OR rectangle.value ->> 'unit' <> 'pt'
                           OR (rectangle.value ->> 'width')::numeric < 0
                           OR (rectangle.value ->> 'height')::numeric < 0
                   )
            )
        )
        """
    )
    if invalid_entry_maps or invalid_blob_maps:
        raise RuntimeError(
            "legacy PDF source maps contain invalid scope, identity, page, path, or rectangle data"
        )

    source_map_on_non_pdf = _count(
        """
        SELECT count(*)
        FROM resume.render_artifacts AS artifact
        LEFT JOIN resume.artifact_blobs AS blob ON blob.artifact_id = artifact.id
        WHERE lower(btrim(artifact.format)) NOT IN ('pdf', 'application/pdf')
          AND lower(btrim(artifact.artifact_kind)) <> 'resume_pdf'
          AND (
                blob.source_map IS NOT NULL
                OR EXISTS (
                    SELECT 1 FROM resume.pdf_source_map_entries AS entry
                    WHERE entry.artifact_id = artifact.id
                )
          )
        """
    )
    conflicting_source_maps = _count(
        r"""
        WITH blob_nodes AS (
            SELECT blob.artifact_id, normalized.nodes
            FROM resume.artifact_blobs AS blob
            CROSS JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(value.node ORDER BY value.node::text),
                    '[]'::jsonb
                ) AS nodes
                FROM (
                    SELECT jsonb_build_object(
                        'entity_id', COALESCE(
                            node.value ->> 'entity_id', node.value ->> 'node_id'
                        ),
                        'field_path', node.value -> 'field_path',
                        'page', node.value -> 'page',
                        'rects', node.value -> 'rects'
                    ) AS node
                    FROM jsonb_array_elements(blob.source_map -> 'nodes') AS node(value)
                ) AS value
            ) AS normalized
            WHERE blob.source_map IS NOT NULL
        ),
        entry_nodes AS (
            SELECT entry.artifact_id,
                   jsonb_agg(
                       jsonb_build_object(
                           'entity_id', entry.node_id,
                           'field_path', to_jsonb(entry.field_path),
                           'page', entry.page,
                           'rects', entry.rects
                       )
                       ORDER BY jsonb_build_object(
                           'entity_id', entry.node_id,
                           'field_path', to_jsonb(entry.field_path),
                           'page', entry.page,
                           'rects', entry.rects
                       )::text
                   ) AS nodes
            FROM resume.pdf_source_map_entries AS entry
            GROUP BY entry.artifact_id
        )
        SELECT count(*)
        FROM blob_nodes
        JOIN entry_nodes USING (artifact_id)
        WHERE blob_nodes.nodes <> entry_nodes.nodes
        """
    )
    if source_map_on_non_pdf or conflicting_source_maps:
        raise RuntimeError(
            "legacy source-map truths conflict with each other or belong to a non-PDF artifact"
        )

    unmappable_jobs = _count(
        r"""
        SELECT count(*) FROM agent.jobs
        WHERE id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR job_type !~ '^[a-z][a-z0-9_.-]{2,100}$'
           OR target_resource_type IS NULL
           OR target_resource_type !~ '^[a-z][a-z0-9_.-]{2,100}$'
           OR target_resource_id IS NULL
           OR target_resource_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR phase <> btrim(phase) OR length(phase) NOT BETWEEN 1 AND 80
           OR completed_units < 0
           OR (total_units IS NOT NULL AND (total_units < 0 OR completed_units > total_units))
           OR NOT (
                (status = 'queued' AND started_at IS NULL AND finished_at IS NULL)
             OR (status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL)
             OR (status = 'succeeded' AND started_at IS NOT NULL AND finished_at IS NOT NULL)
             OR (status = 'failed' AND started_at IS NOT NULL AND finished_at IS NOT NULL)
             OR (status = 'cancelled' AND finished_at IS NOT NULL)
             OR (status = 'expired' AND started_at IS NULL AND finished_at IS NOT NULL)
           )
        """
    )
    if unmappable_jobs:
        raise RuntimeError(
            "legacy Job rows contain an invalid subject, progress snapshot, or lifecycle; "
            "0017 preserves legacy result/error and only synthesizes a typed failure envelope"
        )

    invalid_events = _count(
        r"""
        SELECT count(*) FROM agent.outbox_events
        WHERE id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR aggregate_type !~ '^[a-z][a-z0-9_.-]{2,100}$'
           OR aggregate_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR event_type !~ '^[a-z][a-z0-9_.-]{2,127}$'
           OR jsonb_typeof(payload) <> 'object'
           OR jsonb_array_length(
                jsonb_path_query_array(payload, '$.keyvalue()')
              ) > 40
           OR (trace_id IS NOT NULL AND trace_id !~ '^[a-f0-9]{32}$')
        """
    )
    if invalid_events:
        raise RuntimeError(
            "legacy outbox rows violate the V2 ApiEvent envelope and require offline conversion"
        )

    invalid_audits = _count(
        r"""
        SELECT count(*) FROM identity.audit_events
        WHERE id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR actor_id IS NULL OR actor_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR action !~ '^[a-z][a-z0-9_.-]{2,127}$'
           OR resource_type !~ '^[a-z][a-z0-9_.-]{2,100}$'
           OR resource_id IS NULL OR resource_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR request_id IS NULL OR request_id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR outcome NOT IN ('allowed', 'denied', 'failed')
        """
    )
    if invalid_audits:
        raise RuntimeError(
            "legacy audit rows violate the V2 AuditEvent envelope and require offline conversion"
        )


def _evolve_jobs() -> None:
    """@brief 原位演进统一 Job projection / Evolve the unified Job projection in place."""
    op.alter_column("jobs", "job_type", schema="agent", type_=sa.String(101))
    op.alter_column("jobs", "phase", schema="agent", type_=sa.String(80))
    op.alter_column(
        "jobs", "target_resource_type", schema="agent", type_=sa.String(101), nullable=False
    )
    op.alter_column("jobs", "target_resource_id", schema="agent", nullable=False)
    op.add_column(
        "jobs",
        sa.Column(
            "progress_unit",
            sa.String(16),
            nullable=False,
            server_default=sa.text("'unknown'"),
        ),
        schema="agent",
    )
    op.add_column("jobs", sa.Column("target_resource_revision", sa.Integer()), schema="agent")
    op.add_column(
        "jobs",
        sa.Column(
            "result_refs",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        schema="agent",
    )
    op.add_column(
        "jobs",
        sa.Column("problem", postgresql.JSONB(astext_type=sa.Text())),
        schema="agent",
    )
    op.execute(
        """
        UPDATE agent.jobs
        SET problem = jsonb_build_object(
                'type_uri', 'https://api.hmalliances.org/problems/job/legacy-failure',
                'title', 'Legacy job failed',
                'status', 500,
                'code', 'job.legacy_failure',
                'request_id', CASE
                    WHEN request_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
                    THEN request_id
                    ELSE 'request_' || md5(id)
                END,
                'retryable', false,
                'errors', '[]'::jsonb,
                'extensions', jsonb_build_object(
                    'migration_0017', jsonb_build_object(
                        'legacy_error_preserved_in_column', error IS NOT NULL
                    )
                )
            )
        WHERE status = 'failed'
        """
    )
    op.drop_constraint("jobs_status", "jobs", schema="agent", type_="check")
    op.create_check_constraint(
        "ck_jobs_jobs_status",
        "jobs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
        schema="agent",
    )
    op.create_check_constraint(
        "ck_jobs_jobs_v2_subject",
        "jobs",
        "job_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
        "AND target_resource_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
        "AND target_resource_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
        "AND (target_resource_revision IS NULL OR target_resource_revision >= 1)",
        schema="agent",
    )
    op.create_check_constraint(
        "ck_jobs_jobs_v2_progress",
        "jobs",
        "phase = btrim(phase) AND length(phase) BETWEEN 1 AND 80 "
        "AND completed_units >= 0 "
        "AND (total_units IS NULL OR (total_units >= 0 AND completed_units <= total_units)) "
        "AND progress_unit IN ('items', 'bytes', 'pages', 'steps', 'unknown')",
        schema="agent",
    )
    op.create_check_constraint(
        "ck_jobs_jobs_v2_result_problem",
        "jobs",
        "jsonb_typeof(result_refs) = 'array' "
        "AND jsonb_array_length(result_refs) <= 50 "
        "AND (status = 'succeeded' OR jsonb_array_length(result_refs) = 0) "
        "AND ((status = 'failed' AND problem IS NOT NULL) "
        "OR (status <> 'failed' AND problem IS NULL))",
        schema="agent",
    )
    op.create_check_constraint(
        "ck_jobs_jobs_v2_timeline",
        "jobs",
        "(status = 'queued' AND started_at IS NULL AND finished_at IS NULL) OR "
        "(status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL) OR "
        "(status IN ('succeeded', 'failed') AND started_at IS NOT NULL "
        "AND finished_at IS NOT NULL) OR "
        "(status = 'cancelled' AND finished_at IS NOT NULL) OR "
        "(status = 'expired' AND started_at IS NULL AND finished_at IS NOT NULL)",
        schema="agent",
    )
    op.drop_index(
        "ix_jobs_workspace_id_status_created_at", table_name="jobs", schema="agent"
    )
    op.create_index(
        "ix_jobs_workspace_created_id",
        "jobs",
        ["workspace_id", "created_at", "id"],
        schema="agent",
    )
    op.create_index(
        "ix_jobs_workspace_kind_subject_created",
        "jobs",
        [
            "workspace_id",
            "job_type",
            "target_resource_type",
            "target_resource_id",
            "created_at",
            "id",
        ],
        schema="agent",
    )


def _evolve_audit_events() -> None:
    """@brief 原位演进 append-only AuditEvent envelope / Evolve the append-only AuditEvent envelope in place."""
    op.alter_column("audit_events", "id", schema="identity", type_=sa.String(160))
    op.alter_column("audit_events", "actor_id", schema="identity", type_=sa.String(160))
    op.alter_column("audit_events", "resource_type", schema="identity", type_=sa.String(101))
    op.alter_column("audit_events", "resource_id", schema="identity", type_=sa.String(160))
    op.alter_column("audit_events", "request_id", schema="identity", type_=sa.String(160))
    op.add_column(
        "audit_events",
        sa.Column(
            "actor_type", sa.String(101), nullable=False, server_default=sa.text("'user'")
        ),
        schema="identity",
    )
    op.add_column("audit_events", sa.Column("actor_revision", sa.Integer()), schema="identity")
    op.add_column(
        "audit_events", sa.Column("resource_revision", sa.Integer()), schema="identity"
    )
    for column in ("actor_id", "resource_id", "request_id"):
        op.alter_column("audit_events", column, schema="identity", nullable=False)
    constraints = {
        "ck_audit_events_audit_events_v2_id": "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
        "ck_audit_events_audit_events_v2_actor": (
            "actor_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND actor_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (actor_revision IS NULL OR actor_revision >= 1)"
        ),
        "ck_audit_events_audit_events_v2_action": (
            "action ~ '^[a-z][a-z0-9_.-]{2,127}$'"
        ),
        "ck_audit_events_audit_events_v2_target": (
            "resource_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND resource_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (resource_revision IS NULL OR resource_revision >= 1)"
        ),
        "ck_audit_events_audit_events_v2_outcome": (
            "outcome IN ('allowed', 'denied', 'failed')"
        ),
        "ck_audit_events_audit_events_v2_request": (
            "request_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'"
        ),
    }
    for name, condition in constraints.items():
        op.create_check_constraint(name, "audit_events", condition, schema="identity")
    op.drop_index(
        "ix_audit_events_workspace_id_occurred_at",
        table_name="audit_events",
        schema="identity",
    )
    op.create_index(
        "ix_audit_events_workspace_occurred_id",
        "audit_events",
        ["workspace_id", "occurred_at", "id"],
        schema="identity",
    )


def _evolve_outbox_events() -> None:
    """@brief 原位演进 committed outbox 为 ApiEvent feed source / Evolve committed outbox into the ApiEvent feed source."""
    op.alter_column("outbox_events", "id", schema="agent", type_=sa.String(160))
    op.alter_column(
        "outbox_events", "aggregate_type", schema="agent", type_=sa.String(101)
    )
    op.alter_column("outbox_events", "trace_id", schema="agent", type_=sa.String(32))
    op.add_column(
        "outbox_events", sa.Column("subject_revision", sa.Integer()), schema="agent"
    )
    op.add_column(
        "outbox_events",
        sa.Column("replay_expires_at", sa.DateTime(timezone=True)),
        schema="agent",
    )
    op.execute(
        """
        UPDATE agent.outbox_events
        SET subject_revision = CASE
                WHEN jsonb_typeof(payload -> 'subject' -> 'revision') = 'number'
                THEN (payload -> 'subject' ->> 'revision')::integer
                ELSE NULL
            END,
            replay_expires_at = occurred_at + interval '30 days'
        """
    )
    op.execute(
        """
        WITH ordered AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY workspace_id ORDER BY occurred_at, id
                   ) AS assigned_sequence
            FROM agent.outbox_events
        )
        UPDATE agent.outbox_events AS event
        SET sequence = ordered.assigned_sequence
        FROM ordered
        WHERE event.id = ordered.id
        """
    )
    op.alter_column(
        "outbox_events", "replay_expires_at", schema="agent", nullable=False
    )
    constraints = {
        "ck_outbox_events_outbox_events_v2_envelope": (
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND aggregate_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND aggregate_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (subject_revision IS NULL OR subject_revision >= 1) "
            "AND event_type ~ '^[a-z][a-z0-9_.-]{2,127}$' AND sequence >= 1"
        ),
        "ck_outbox_events_outbox_events_v2_payload": (
            "jsonb_typeof(payload) = 'object' AND jsonb_array_length("
            "jsonb_path_query_array(payload, '$.keyvalue()')) <= 40"
        ),
        "ck_outbox_events_outbox_events_v2_trace": (
            "trace_id IS NULL OR trace_id ~ '^[a-f0-9]{32}$'"
        ),
        "ck_outbox_events_outbox_events_v2_replay_window": (
            "replay_expires_at > occurred_at"
        ),
    }
    for name, condition in constraints.items():
        op.create_check_constraint(name, "outbox_events", condition, schema="agent")
    op.create_unique_constraint(
        "outbox_events_workspace_sequence",
        "outbox_events",
        ["workspace_id", "sequence"],
        schema="agent",
    )
    op.create_index(
        "ix_outbox_events_workspace_sequence",
        "outbox_events",
        ["workspace_id", "sequence"],
        schema="agent",
    )
    op.create_index(
        "ix_outbox_events_replay_expiry",
        "outbox_events",
        ["replay_expires_at", "workspace_id", "sequence"],
        schema="agent",
    )


def _create_workspace_sequence_allocator(
    *,
    owner_role: str,
    app_role: str,
    dashboard_role: str,
    migrator_role: str,
) -> None:
    """@brief 创建事务 counter 与强制分配 trigger / Create the transactional counter and mandatory allocation trigger."""
    op.create_table(
        "workspace_event_sequences",
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "last_sequence", sa.BigInteger(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "last_sequence >= 0",
            name="ck_workspace_event_sequences_nonnegative",
        ),
        schema="agent",
    )
    op.execute(
        """
        INSERT INTO agent.workspace_event_sequences (
            workspace_id, last_sequence, updated_at
        )
        SELECT workspace_id, max(sequence), statement_timestamp()
        FROM agent.outbox_events
        GROUP BY workspace_id
        """
    )
    op.execute("ALTER TABLE agent.workspace_event_sequences ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE agent.workspace_event_sequences FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY platform_v2_sequence_allocator "
        f"ON agent.workspace_event_sequences AS PERMISSIVE FOR ALL TO {owner_role} "
        "USING (workspace_id = current_setting('app.workspace_id', true)) "
        "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE agent.workspace_event_sequences "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(
        """
        CREATE FUNCTION agent.assign_workspace_event_sequence()
        RETURNS trigger
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, agent
        AS $function$
        DECLARE
            scoped_workspace text := current_setting('app.workspace_id', true);
        BEGIN
            IF scoped_workspace IS NULL
               OR scoped_workspace = ''
               OR NEW.workspace_id <> scoped_workspace THEN
                RAISE EXCEPTION 'outbox Workspace does not match the transaction scope'
                    USING ERRCODE = '42501';
            END IF;
            INSERT INTO agent.workspace_event_sequences (
                workspace_id, last_sequence, updated_at
            ) VALUES (
                NEW.workspace_id, 1, statement_timestamp()
            )
            ON CONFLICT (workspace_id) DO UPDATE
            SET last_sequence = agent.workspace_event_sequences.last_sequence + 1,
                updated_at = statement_timestamp()
            RETURNING last_sequence INTO NEW.sequence;
            IF NEW.replay_expires_at IS NULL THEN
                NEW.replay_expires_at := NEW.occurred_at + interval '30 days';
            END IF;
            IF NEW.subject_revision IS NULL
               AND jsonb_typeof(NEW.payload -> 'subject' -> 'revision') = 'number' THEN
                NEW.subject_revision := (NEW.payload -> 'subject' ->> 'revision')::integer;
            END IF;
            RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON FUNCTION agent.assign_workspace_event_sequence() "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(
        "ALTER FUNCTION agent.assign_workspace_event_sequence() "
        f"OWNER TO {owner_role}"
    )
    op.execute(
        """
        CREATE TRIGGER assign_workspace_event_sequence
        BEFORE INSERT ON agent.outbox_events
        FOR EACH ROW
        EXECUTE FUNCTION agent.assign_workspace_event_sequence()
        """
    )


def _lifecycle_columns() -> tuple[sa.Column[Any], ...]:
    """@brief 返回与 ORM mixin 精确一致的 lifecycle columns / Return lifecycle columns matching the ORM mixin."""
    return (
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )


def _workspace_column() -> sa.Column[Any]:
    """@brief 返回 WorkspaceScopedMixin 的 workspace column / Return the WorkspaceScopedMixin workspace column."""
    return sa.Column(
        "workspace_id",
        sa.String(128),
        sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
        nullable=False,
    )


def _create_artifact_tables() -> None:
    """@brief 创建唯一 Artifact metadata 与从属 content/source-map / Create sole Artifact metadata and dependent content/source-map."""
    op.create_table(
        "artifacts",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("subject_type", sa.String(101), nullable=False),
        sa.Column("subject_id", sa.String(160), nullable=False),
        sa.Column("subject_revision", sa.Integer()),
        sa.Column("media_type", sa.String(255), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("storage_key", sa.String(1024), nullable=False),
        sa.Column("page_count", sa.Integer()),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        _workspace_column(),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "kind IN ('resume_pdf', 'resume_json', 'resume_docx', 'interview_audio', "
            "'interview_video', 'interview_transcript', 'generic')",
            name="ck_artifacts_artifacts_kind",
        ),
        sa.CheckConstraint(
            "subject_type ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND subject_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND (subject_revision IS NULL OR subject_revision >= 1)",
            name="ck_artifacts_artifacts_subject",
        ),
        sa.CheckConstraint(
            "media_type ~ '^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$'",
            name="ck_artifacts_artifacts_media_type",
        ),
        sa.CheckConstraint(
            "size_bytes BETWEEN 0 AND 1073741824 AND sha256 ~ '^[a-f0-9]{64}$'",
            name="ck_artifacts_artifacts_content_identity",
        ),
        sa.CheckConstraint(
            "page_count IS NULL OR page_count >= 1",
            name="ck_artifacts_artifacts_page_count",
        ),
        sa.CheckConstraint(
            "expires_at IS NULL OR expires_at > created_at",
            name="ck_artifacts_artifacts_expiration",
        ),
        sa.CheckConstraint(
            "deleted_at IS NULL OR deleted_at >= created_at",
            name="ck_artifacts_artifacts_deletion",
        ),
        sa.UniqueConstraint("storage_key", name="artifacts_storage_key"),
        sa.UniqueConstraint("id", "workspace_id", name="uq_artifacts_id_workspace"),
        schema="agent",
    )
    op.create_index(
        "ix_artifacts_workspace_created_id",
        "artifacts",
        ["workspace_id", "created_at", "id"],
        schema="agent",
    )
    op.create_index(
        "ix_artifacts_workspace_kind_subject_created",
        "artifacts",
        ["workspace_id", "kind", "subject_type", "subject_id", "created_at", "id"],
        schema="agent",
    )
    op.create_table(
        "artifact_contents",
        sa.Column("artifact_id", sa.String(160), primary_key=True),
        sa.Column("storage_key", sa.String(1024), nullable=False),
        sa.Column("media_type", sa.String(255), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=False),
        _workspace_column(),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "size_bytes BETWEEN 0 AND 1073741824 "
            "AND octet_length(content) = size_bytes AND sha256 ~ '^[a-f0-9]{64}$'",
            name="ck_artifact_contents_artifact_contents_identity",
        ),
        sa.CheckConstraint(
            "media_type ~ '^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$'",
            name="ck_artifact_contents_artifact_contents_media_type",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id", "workspace_id"],
            ["agent.artifacts.id", "agent.artifacts.workspace_id"],
            name="fk_artifact_contents_artifact_scope",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("storage_key", name="artifact_contents_storage_key"),
        schema="agent",
    )
    op.create_table(
        "artifact_pdf_source_maps",
        sa.Column("artifact_id", sa.String(160), primary_key=True),
        sa.Column("resume_id", sa.String(160), nullable=False),
        sa.Column("resume_revision", sa.Integer(), nullable=False),
        sa.Column(
            "nodes", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        _workspace_column(),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "resume_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' AND resume_revision >= 1",
            name="ck_artifact_pdf_source_maps_artifact_pdf_source_maps_resume",
        ),
        sa.CheckConstraint(
            "CASE WHEN jsonb_typeof(nodes) = 'array' "
            "THEN jsonb_array_length(nodes) <= 10000 ELSE false END",
            name="ck_artifact_pdf_source_maps_artifact_pdf_source_maps_nodes",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id", "workspace_id"],
            ["agent.artifacts.id", "agent.artifacts.workspace_id"],
            name="fk_artifact_pdf_source_maps_artifact_scope",
            ondelete="CASCADE",
        ),
        schema="agent",
    )


def _migrate_legacy_artifacts() -> None:
    """@brief 将旧 Artifact truths 确定性合并到 V2 / Deterministically merge legacy Artifact truths into V2.

    @note 旧 resource owner、原始 format/media kind 与来源表保存在命名迁移
        extension 中；不把 owner 再度变成 V2 授权边界。/ Legacy resource owner,
        original format/media kind, and source table remain in a namespaced migration extension;
        the legacy owner is not reused as a V2 authorization boundary.
    """
    op.execute(
        r"""
        WITH raw_page_evidence AS (
            SELECT artifact.id AS artifact_id,
                   CASE
                       WHEN blob.source_map IS NOT NULL
                       THEN (blob.source_map ->> 'page_count')::numeric
                   END AS source_map_page_count,
                   CASE
                       WHEN jsonb_typeof(
                           artifact.extensions #> '{runtime,public_metadata,page_count}'
                       ) = 'number'
                       THEN (
                           artifact.extensions #>> '{runtime,public_metadata,page_count}'
                       )::numeric
                   END AS public_page_count,
                   (
                       SELECT max(entry.page)
                       FROM resume.pdf_source_map_entries AS entry
                       WHERE entry.artifact_id = artifact.id
                   ) AS entry_max_page
            FROM resume.render_artifacts AS artifact
            LEFT JOIN resume.artifact_blobs AS blob
              ON blob.artifact_id = artifact.id
        ),
        page_evidence AS (
            SELECT artifact_id,
                   source_map_page_count::integer AS source_map_page_count,
                   CASE
                       WHEN public_page_count BETWEEN 1 AND 2147483647
                        AND trunc(public_page_count) = public_page_count
                       THEN public_page_count::integer
                   END AS public_page_count,
                   entry_max_page
            FROM raw_page_evidence
        )
        INSERT INTO agent.artifacts (
            id, kind, subject_type, subject_id, subject_revision,
            media_type, size_bytes, sha256, storage_key, page_count,
            expires_at, deleted_at, workspace_id,
            created_at, updated_at, revision, extensions
        )
        SELECT artifact.id,
               CASE
                   WHEN lower(btrim(artifact.format)) IN ('pdf', 'application/pdf')
                     OR lower(btrim(artifact.artifact_kind)) = 'resume_pdf'
                   THEN 'resume_pdf'
                   WHEN lower(btrim(artifact.format)) IN ('json', 'application/json')
                     OR lower(btrim(artifact.artifact_kind)) = 'resume_json'
                   THEN 'resume_json'
                   WHEN lower(btrim(artifact.format)) IN (
                       'docx',
                       'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                   ) OR lower(btrim(artifact.artifact_kind)) = 'resume_docx'
                   THEN 'resume_docx'
                   ELSE 'generic'
               END,
               'resume', artifact.resume_id, resume_revision.revision_no,
               CASE
                   WHEN lower(btrim(artifact.format)) IN ('pdf', 'application/pdf')
                     OR lower(btrim(artifact.artifact_kind)) = 'resume_pdf'
                   THEN 'application/pdf'
                   WHEN lower(btrim(artifact.format)) IN ('json', 'application/json')
                     OR lower(btrim(artifact.artifact_kind)) = 'resume_json'
                   THEN 'application/json'
                   WHEN lower(btrim(artifact.format)) IN (
                       'docx',
                       'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                   ) OR lower(btrim(artifact.artifact_kind)) = 'resume_docx'
                   THEN 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                   ELSE 'application/octet-stream'
               END,
               artifact.content_bytes, lower(artifact.content_sha256), artifact.storage_key,
               CASE
                   WHEN lower(btrim(artifact.format)) IN ('pdf', 'application/pdf')
                     OR lower(btrim(artifact.artifact_kind)) = 'resume_pdf'
                   THEN COALESCE(
                       page_evidence.source_map_page_count,
                       page_evidence.public_page_count,
                       page_evidence.entry_max_page
                   )
               END,
               artifact.expires_at, NULL, artifact.workspace_id,
               artifact.created_at, artifact.updated_at, artifact.revision,
               artifact.extensions || jsonb_build_object(
                   '_migration_0017', jsonb_build_object(
                       'source_table', 'resume.render_artifacts',
                       'legacy_resource_owner_id', artifact.resource_owner_id,
                       'legacy_resume_revision_id', artifact.resume_revision_id,
                       'legacy_artifact_kind', artifact.artifact_kind,
                       'legacy_format', artifact.format,
                       'legacy_content_sha256', artifact.content_sha256
                   )
               )
        FROM resume.render_artifacts AS artifact
        JOIN resume.revisions AS resume_revision
          ON resume_revision.id = artifact.resume_revision_id
         AND resume_revision.resume_id = artifact.resume_id
         AND resume_revision.workspace_id = artifact.workspace_id
         AND resume_revision.resource_owner_id = artifact.resource_owner_id
        JOIN page_evidence ON page_evidence.artifact_id = artifact.id
        """
    )
    op.execute(
        r"""
        INSERT INTO agent.artifacts (
            id, kind, subject_type, subject_id, subject_revision,
            media_type, size_bytes, sha256, storage_key, page_count,
            expires_at, deleted_at, workspace_id,
            created_at, updated_at, revision, extensions
        )
        SELECT artifact.id,
               CASE
                   WHEN lower(btrim(artifact.media_kind)) LIKE 'audio/%'
                     OR lower(btrim(artifact.media_kind)) IN ('audio', 'wav', 'mp3', 'm4a', 'ogg')
                   THEN 'interview_audio'
                   WHEN lower(btrim(artifact.media_kind)) LIKE 'video/%'
                     OR lower(btrim(artifact.media_kind)) IN ('video', 'mp4', 'webm')
                   THEN 'interview_video'
                   WHEN lower(btrim(artifact.media_kind)) LIKE 'text/%'
                     OR lower(btrim(artifact.media_kind)) IN (
                         'transcript', 'txt', 'json', 'vtt', 'application/json'
                     )
                   THEN 'interview_transcript'
                   ELSE 'generic'
               END,
               'interview_session', artifact.session_id, NULL,
               CASE
                   WHEN lower(btrim(artifact.media_kind)) ~
                        '^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$'
                   THEN lower(btrim(artifact.media_kind))
                   WHEN lower(btrim(artifact.media_kind)) = 'wav' THEN 'audio/wav'
                   WHEN lower(btrim(artifact.media_kind)) = 'mp3' THEN 'audio/mpeg'
                   WHEN lower(btrim(artifact.media_kind)) = 'm4a' THEN 'audio/mp4'
                   WHEN lower(btrim(artifact.media_kind)) = 'ogg' THEN 'audio/ogg'
                   WHEN lower(btrim(artifact.media_kind)) = 'mp4' THEN 'video/mp4'
                   WHEN lower(btrim(artifact.media_kind)) = 'webm' THEN 'video/webm'
                   WHEN lower(btrim(artifact.media_kind)) IN ('transcript', 'txt')
                   THEN 'text/plain'
                   WHEN lower(btrim(artifact.media_kind)) = 'json' THEN 'application/json'
                   WHEN lower(btrim(artifact.media_kind)) = 'vtt' THEN 'text/vtt'
                   ELSE 'application/octet-stream'
               END,
               artifact.content_bytes, lower(artifact.content_sha256), artifact.storage_key,
               NULL, artifact.expires_at, artifact.deleted_at, artifact.workspace_id,
               artifact.created_at, artifact.updated_at, artifact.revision,
               artifact.extensions || jsonb_build_object(
                   '_migration_0017', jsonb_build_object(
                       'source_table', 'interview.recording_artifacts',
                       'content_disposition', 'deleted_tombstone_no_bytes',
                       'legacy_resource_owner_id', artifact.resource_owner_id,
                       'legacy_media_kind', artifact.media_kind,
                       'legacy_content_sha256', artifact.content_sha256
                   )
               )
        FROM interview.recording_artifacts AS artifact
        """
    )
    op.execute(
        r"""
        INSERT INTO agent.artifact_contents (
            artifact_id, storage_key, media_type, size_bytes, sha256, content,
            workspace_id, created_at, updated_at, revision, extensions
        )
        SELECT artifact.id, artifact.storage_key,
               CASE
                   WHEN lower(btrim(artifact.format)) IN ('pdf', 'application/pdf')
                     OR lower(btrim(artifact.artifact_kind)) = 'resume_pdf'
                   THEN 'application/pdf'
                   WHEN lower(btrim(artifact.format)) IN ('json', 'application/json')
                     OR lower(btrim(artifact.artifact_kind)) = 'resume_json'
                   THEN 'application/json'
                   WHEN lower(btrim(artifact.format)) IN (
                       'docx',
                       'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                   ) OR lower(btrim(artifact.artifact_kind)) = 'resume_docx'
                   THEN 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                   ELSE 'application/octet-stream'
               END,
               artifact.content_bytes, lower(artifact.content_sha256), blob.content,
               artifact.workspace_id, blob.created_at, blob.updated_at, blob.revision,
               blob.extensions || jsonb_build_object(
                   '_migration_0017', jsonb_build_object(
                       'source_table', 'resume.artifact_blobs',
                       'legacy_blob_id', blob.id,
                       'legacy_resource_owner_id', blob.resource_owner_id
                   )
               )
        FROM resume.artifact_blobs AS blob
        JOIN resume.render_artifacts AS artifact ON artifact.id = blob.artifact_id
        """
    )
    op.execute(
        r"""
        WITH blob_maps AS (
            SELECT blob.artifact_id,
                   blob.id AS blob_id,
                   blob.source_map,
                   (blob.source_map ->> 'page_count')::integer AS page_count,
                   normalized.nodes
            FROM resume.artifact_blobs AS blob
            CROSS JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'entity_id', COALESCE(
                                node.value ->> 'entity_id', node.value ->> 'node_id'
                            ),
                            'field_path', node.value -> 'field_path',
                            'page', node.value -> 'page',
                            'rects', node.value -> 'rects'
                        ) ORDER BY node.ordinality
                    ),
                    '[]'::jsonb
                ) AS nodes
                FROM jsonb_array_elements(blob.source_map -> 'nodes')
                     WITH ORDINALITY AS node(value, ordinality)
            ) AS normalized
            WHERE blob.source_map IS NOT NULL
        ),
        entry_maps AS (
            SELECT entry.artifact_id,
                   max(entry.page) AS page_count,
                   jsonb_agg(
                       jsonb_build_object(
                           'entity_id', entry.node_id,
                           'field_path', to_jsonb(entry.field_path),
                           'page', entry.page,
                           'rects', entry.rects
                       ) ORDER BY entry.created_at, entry.id
                   ) AS nodes,
                   jsonb_agg(
                       jsonb_build_object(
                           'id', entry.id,
                           'node_kind', entry.node_kind,
                           'node_id', entry.node_id,
                           'created_at', entry.created_at,
                           'updated_at', entry.updated_at,
                           'revision', entry.revision,
                           'extensions', entry.extensions
                       ) ORDER BY entry.created_at, entry.id
                   ) AS legacy_entries
            FROM resume.pdf_source_map_entries AS entry
            GROUP BY entry.artifact_id
        ),
        public_pages AS (
            SELECT artifact.id AS artifact_id,
                   CASE
                       WHEN jsonb_typeof(
                           artifact.extensions #> '{runtime,public_metadata,page_count}'
                       ) = 'number'
                       THEN (
                           artifact.extensions #>> '{runtime,public_metadata,page_count}'
                       )::numeric
                   END AS page_count
            FROM resume.render_artifacts AS artifact
        )
        INSERT INTO agent.artifact_pdf_source_maps (
            artifact_id, resume_id, resume_revision, nodes, workspace_id,
            created_at, updated_at, revision, extensions
        )
        SELECT artifact.id, artifact.resume_id, resume_revision.revision_no,
               COALESCE(blob_maps.nodes, entry_maps.nodes), artifact.workspace_id,
               artifact.created_at, artifact.updated_at, artifact.revision,
               jsonb_build_object(
                   '_migration_0017', jsonb_strip_nulls(jsonb_build_object(
                       'source_tables', CASE
                           WHEN blob_maps.artifact_id IS NOT NULL
                            AND entry_maps.artifact_id IS NOT NULL
                           THEN jsonb_build_array(
                               'resume.artifact_blobs', 'resume.pdf_source_map_entries'
                           )
                           WHEN blob_maps.artifact_id IS NOT NULL
                           THEN jsonb_build_array('resume.artifact_blobs')
                           ELSE jsonb_build_array('resume.pdf_source_map_entries')
                       END,
                       'legacy_resource_owner_id', artifact.resource_owner_id,
                       'legacy_blob_id', blob_maps.blob_id,
                       'legacy_blob_source_map', blob_maps.source_map,
                       'legacy_entries', entry_maps.legacy_entries,
                       'page_count_basis', CASE
                           WHEN blob_maps.page_count IS NOT NULL THEN 'blob_source_map'
                           WHEN public_pages.page_count BETWEEN 1 AND 2147483647
                            AND trunc(public_pages.page_count) = public_pages.page_count
                           THEN 'legacy_public_metadata'
                           ELSE 'source_map_max_page'
                       END
                   ))
               )
        FROM resume.render_artifacts AS artifact
        JOIN resume.revisions AS resume_revision
          ON resume_revision.id = artifact.resume_revision_id
         AND resume_revision.resume_id = artifact.resume_id
         AND resume_revision.workspace_id = artifact.workspace_id
         AND resume_revision.resource_owner_id = artifact.resource_owner_id
        LEFT JOIN blob_maps ON blob_maps.artifact_id = artifact.id
        LEFT JOIN entry_maps ON entry_maps.artifact_id = artifact.id
        LEFT JOIN public_pages ON public_pages.artifact_id = artifact.id
        WHERE blob_maps.artifact_id IS NOT NULL OR entry_maps.artifact_id IS NOT NULL
        """
    )


def _assert_active_artifact_contents() -> None:
    """@brief 在破坏旧真相前证明每个可读 Artifact 都有一致内容 / Prove every readable Artifact has coherent content before retiring legacy truth."""
    missing_or_inconsistent_contents = _count(
        """
        SELECT count(*)
        FROM agent.artifacts AS artifact
        LEFT JOIN agent.artifact_contents AS content
          ON content.artifact_id = artifact.id
         AND content.workspace_id = artifact.workspace_id
        WHERE artifact.deleted_at IS NULL
          AND (
               content.artifact_id IS NULL
            OR content.storage_key <> artifact.storage_key
            OR content.media_type <> artifact.media_type
            OR content.size_bytes <> artifact.size_bytes
            OR content.sha256 <> artifact.sha256
            OR octet_length(content.content) <> artifact.size_bytes
            OR encode(sha256(content.content), 'hex') <> artifact.sha256
          )
        """
    )
    if missing_or_inconsistent_contents:
        raise RuntimeError(
            "API V2 Artifact migration produced readable metadata without verified inline "
            "content; legacy Artifact tables were not retired"
        )


def _retire_legacy_artifact_tables() -> None:
    """@brief 转换后移除平行 Artifact truths 并重连 Job / Remove converted parallel truths and reconnect Jobs."""
    op.drop_constraint(
        "fk_tnt_render_jobs_artifact_id_scope",
        "render_jobs",
        schema="resume",
        type_="foreignkey",
    )
    op.drop_constraint(
        "render_jobs_artifact_id_fkey",
        "render_jobs",
        schema="resume",
        type_="foreignkey",
    )
    op.drop_table("pdf_source_map_entries", schema="resume")
    op.drop_table("artifact_blobs", schema="resume")
    op.drop_table("render_artifacts", schema="resume")
    op.drop_table("recording_artifacts", schema="interview")
    op.alter_column(
        "render_jobs", "artifact_id", schema="resume", type_=sa.String(160)
    )
    op.create_foreign_key(
        "fk_render_jobs_artifact_id_artifacts",
        "render_jobs",
        "artifacts",
        ["artifact_id"],
        ["id"],
        source_schema="resume",
        referent_schema="agent",
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_render_jobs_artifact_workspace",
        "render_jobs",
        "artifacts",
        ["artifact_id", "workspace_id"],
        ["id", "workspace_id"],
        source_schema="resume",
        referent_schema="agent",
    )


def _secure_platform_tables(
    *,
    app_role: str,
    dashboard_role: str,
    migrator_role: str,
) -> None:
    """@brief 配置 Workspace RLS、append-only audit 与最小权限 / Configure Workspace RLS and least privilege."""
    for table in _NEW_PLATFORM_TABLES[1:]:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY platform_v2_workspace_select ON {table} "
            f"AS PERMISSIVE FOR SELECT TO {app_role} "
            "USING (workspace_id = current_setting('app.workspace_id', true))"
        )
        op.execute(
            f"CREATE POLICY platform_v2_workspace_insert ON {table} "
            f"AS PERMISSIVE FOR INSERT TO {app_role} "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )

    table = "agent.jobs"
    op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {table} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"GRANT SELECT, INSERT ON TABLE {table} TO {app_role}")
    op.execute(
        f"GRANT UPDATE (status, phase, completed_units, total_units, progress_unit, "
        "result_refs, problem, started_at, finished_at, revision, updated_at) "
        f"ON TABLE {table} TO {app_role}"
    )
    op.execute(
        f"CREATE POLICY platform_v2_workspace_select ON {table} AS PERMISSIVE FOR SELECT "
        f"TO {app_role} USING (workspace_id = current_setting('app.workspace_id', true))"
    )
    op.execute(
        f"CREATE POLICY platform_v2_workspace_insert ON {table} AS PERMISSIVE FOR INSERT "
        f"TO {app_role} WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )
    op.execute(
        f"CREATE POLICY platform_v2_workspace_update ON {table} AS PERMISSIVE FOR UPDATE "
        f"TO {app_role} USING (workspace_id = current_setting('app.workspace_id', true)) "
        "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )

    table = "agent.outbox_events"
    op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {table} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"GRANT SELECT, INSERT ON TABLE {table} TO {app_role}")
    op.execute(
        f"GRANT UPDATE (status, published_at, attempt_count, updated_at) "
        f"ON TABLE {table} TO {app_role}"
    )
    for command in ("SELECT", "INSERT", "UPDATE"):
        predicate = (
            "USING (workspace_id = current_setting('app.workspace_id', true))"
            if command == "SELECT"
            else "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
            if command == "INSERT"
            else "USING (workspace_id = current_setting('app.workspace_id', true)) "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )
        op.execute(
            f"CREATE POLICY platform_v2_workspace_{command.lower()} ON {table} "
            f"AS PERMISSIVE FOR {command} TO {app_role} {predicate}"
        )

    table = "identity.audit_events"
    op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {table} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"GRANT SELECT, INSERT ON TABLE {table} TO {app_role}")
    op.execute(
        f"CREATE POLICY platform_v2_workspace_select ON {table} AS PERMISSIVE FOR SELECT "
        f"TO {app_role} USING (workspace_id = current_setting('app.workspace_id', true))"
    )
    op.execute(
        f"CREATE POLICY platform_v2_workspace_insert ON {table} AS PERMISSIVE FOR INSERT "
        f"TO {app_role} WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )


def upgrade() -> None:
    """@brief 原子发布 V2 Platform persistence / Atomically publish V2 Platform persistence."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _install_migration_visibility(owner_role, _PREFLIGHT_TABLES)
    _preflight_upgrade()
    _evolve_jobs()
    _evolve_audit_events()
    _evolve_outbox_events()
    _create_workspace_sequence_allocator(
        owner_role=owner_role,
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    _create_artifact_tables()
    _migrate_legacy_artifacts()
    _assert_active_artifact_contents()
    _remove_migration_visibility(_LEGACY_ARTIFACT_TABLES)
    _retire_legacy_artifact_tables()
    _secure_platform_tables(
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    _remove_migration_visibility(
        (*_CORE_TABLES, *_LEGACY_ARTIFACT_PARENT_TABLES, "resume.render_jobs")
    )


def _preflight_downgrade() -> None:
    """@brief 非空 Platform V2 state 一律拒绝有损 downgrade / Reject lossy downgrade for non-empty Platform V2 state."""
    rows = sum(_count(f"SELECT count(*) FROM {table}") for table in _CORE_TABLES)
    rows += sum(_count(f"SELECT count(*) FROM {table}") for table in _NEW_PLATFORM_TABLES)
    linked_artifacts = _count(
        "SELECT count(*) FROM resume.render_jobs WHERE artifact_id IS NOT NULL"
    )
    if rows or linked_artifacts:
        raise RuntimeError(
            "cannot downgrade non-empty API V2 Platform persistence state; Job revisions, "
            "Artifact identity, Workspace event sequences, and audit envelopes are irreversible"
        )


def _drop_platform_security(app_role: str) -> None:
    """@brief 移除 0017 policies 并恢复旧 common policies / Remove 0017 policies and restore legacy common policies."""
    op.execute(
        "DROP POLICY platform_v2_sequence_allocator "
        "ON agent.workspace_event_sequences"
    )
    for table in _NEW_PLATFORM_TABLES[1:]:
        op.execute(f"DROP POLICY platform_v2_workspace_insert ON {table}")
        op.execute(f"DROP POLICY platform_v2_workspace_select ON {table}")
    for table in ("agent.jobs", "agent.outbox_events"):
        op.execute(f"DROP POLICY platform_v2_workspace_update ON {table}")
        op.execute(f"DROP POLICY platform_v2_workspace_insert ON {table}")
        op.execute(f"DROP POLICY platform_v2_workspace_select ON {table}")
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY workspace_app_tenant_scope ON {table} AS PERMISSIVE FOR ALL "
            f"TO {app_role} USING (workspace_id = current_setting('app.workspace_id', true)) "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )
    table = "identity.audit_events"
    op.execute(f"DROP POLICY platform_v2_workspace_insert ON {table}")
    op.execute(f"DROP POLICY platform_v2_workspace_select ON {table}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")
    op.execute(
        f"CREATE POLICY workspace_app_tenant_scope ON {table} AS PERMISSIVE FOR ALL "
        f"TO {app_role} USING (workspace_id = current_setting('app.workspace_id', true)) "
        "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )


def _restore_legacy_artifact_tables(app_role: str) -> None:
    """@brief 仅为空库 downgrade 重建 0016 legacy Artifact schema / Recreate the 0016 legacy Artifact schema only for an empty downgrade."""
    tenant_columns = (
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "resource_owner_id",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
    )
    op.create_table(
        "render_artifacts",
        sa.Column("id", sa.String(128), primary_key=True),
        *tenant_columns,
        sa.Column(
            "resume_id",
            sa.String(160),
            sa.ForeignKey("resume.documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "resume_revision_id",
            sa.String(128),
            sa.ForeignKey("resume.revisions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("artifact_kind", sa.String(32), nullable=False),
        sa.Column("format", sa.String(32), nullable=False),
        sa.Column("storage_key", sa.String(1024), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("content_bytes", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("storage_key", name="render_artifacts_storage_key"),
        sa.UniqueConstraint(
            "id", "workspace_id", "resource_owner_id", name="uq_tnt_render_artifacts_id_ws_owner"
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "resource_owner_id"],
            ["identity.workspaces.id", "identity.workspaces.resource_owner_id"],
            name="fk_tnt_render_artifacts_workspace_scope",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["resume_id", "workspace_id", "resource_owner_id"],
            ["resume.documents.id", "resume.documents.workspace_id", "resume.documents.resource_owner_id"],
            name="fk_tnt_render_artifacts_resume_id_scope",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["resume_revision_id", "workspace_id", "resource_owner_id"],
            ["resume.revisions.id", "resume.revisions.workspace_id", "resume.revisions.resource_owner_id"],
            name="fk_tnt_render_artifacts_resume_revision_id_scope",
            ondelete="RESTRICT",
        ),
        schema="resume",
    )
    op.create_index(
        "ix_render_artifacts_workspace_id",
        "render_artifacts",
        ["workspace_id"],
        schema="resume",
    )
    op.create_index(
        "ix_render_artifacts_resource_owner_id",
        "render_artifacts",
        ["resource_owner_id"],
        schema="resume",
    )
    op.create_index(
        "ix_render_artifacts_resume_revision_id",
        "render_artifacts",
        ["resume_id", "resume_revision_id"],
        schema="resume",
    )
    op.create_table(
        "artifact_blobs",
        sa.Column("id", sa.String(128), primary_key=True),
        *(
            sa.Column(
                "workspace_id",
                sa.String(128),
                sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
                nullable=False,
            ),
            sa.Column(
                "resource_owner_id",
                sa.String(128),
                sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
                nullable=False,
            ),
        ),
        sa.Column(
            "artifact_id",
            sa.String(128),
            sa.ForeignKey("resume.render_artifacts.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("content", sa.LargeBinary(), nullable=False),
        sa.Column("source_map", postgresql.JSONB(astext_type=sa.Text())),
        *_lifecycle_columns(),
        sa.UniqueConstraint(
            "id", "workspace_id", "resource_owner_id", name="uq_tnt_artifact_blobs_id_ws_owner"
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "resource_owner_id"],
            ["identity.workspaces.id", "identity.workspaces.resource_owner_id"],
            name="fk_tnt_artifact_blobs_workspace_scope",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id", "workspace_id", "resource_owner_id"],
            [
                "resume.render_artifacts.id",
                "resume.render_artifacts.workspace_id",
                "resume.render_artifacts.resource_owner_id",
            ],
            name="fk_tnt_artifact_blobs_artifact_id_scope",
            ondelete="CASCADE",
        ),
        schema="resume",
    )
    op.create_index(
        "ix_artifact_blobs_workspace_id",
        "artifact_blobs",
        ["workspace_id"],
        schema="resume",
    )
    op.create_index(
        "ix_artifact_blobs_resource_owner_id",
        "artifact_blobs",
        ["resource_owner_id"],
        schema="resume",
    )
    op.create_table(
        "pdf_source_map_entries",
        sa.Column("id", sa.String(128), primary_key=True),
        *(
            sa.Column(
                "workspace_id",
                sa.String(128),
                sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
                nullable=False,
            ),
            sa.Column(
                "resource_owner_id",
                sa.String(128),
                sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
                nullable=False,
            ),
        ),
        sa.Column(
            "artifact_id",
            sa.String(128),
            sa.ForeignKey("resume.render_artifacts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("node_kind", sa.String(64), nullable=False),
        sa.Column("node_id", sa.String(128), nullable=False),
        sa.Column("field_path", postgresql.ARRAY(sa.String(128)), nullable=False),
        sa.Column("page", sa.Integer(), nullable=False),
        sa.Column("rects", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_lifecycle_columns(),
        sa.UniqueConstraint(
            "id",
            "workspace_id",
            "resource_owner_id",
            name="uq_tnt_pdf_source_map_entries_id_ws_owner",
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "resource_owner_id"],
            ["identity.workspaces.id", "identity.workspaces.resource_owner_id"],
            name="fk_tnt_pdf_source_map_entries_workspace_scope",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id", "workspace_id", "resource_owner_id"],
            [
                "resume.render_artifacts.id",
                "resume.render_artifacts.workspace_id",
                "resume.render_artifacts.resource_owner_id",
            ],
            name="fk_tnt_pdf_source_map_entries_artifact_id_scope",
            ondelete="CASCADE",
        ),
        schema="resume",
    )
    op.create_index(
        "ix_pdf_source_map_entries_workspace_id",
        "pdf_source_map_entries",
        ["workspace_id"],
        schema="resume",
    )
    op.create_index(
        "ix_pdf_source_map_entries_resource_owner_id",
        "pdf_source_map_entries",
        ["resource_owner_id"],
        schema="resume",
    )
    op.create_index(
        "ix_pdf_source_map_entries_artifact_node",
        "pdf_source_map_entries",
        ["artifact_id", "node_id"],
        schema="resume",
    )
    op.create_table(
        "recording_artifacts",
        sa.Column("id", sa.String(128), primary_key=True),
        *(
            sa.Column(
                "workspace_id",
                sa.String(128),
                sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
                nullable=False,
            ),
            sa.Column(
                "resource_owner_id",
                sa.String(128),
                sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
                nullable=False,
            ),
        ),
        sa.Column(
            "session_id",
            sa.String(128),
            sa.ForeignKey("interview.sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("storage_key", sa.String(1024), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("content_bytes", sa.BigInteger(), nullable=False),
        sa.Column("media_kind", sa.String(32), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("storage_key", name="interview_recordings_storage_key"),
        sa.UniqueConstraint(
            "id",
            "workspace_id",
            "resource_owner_id",
            name="uq_tnt_recording_artifacts_id_ws_owner",
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "resource_owner_id"],
            ["identity.workspaces.id", "identity.workspaces.resource_owner_id"],
            name="fk_tnt_recording_artifacts_workspace_scope",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["session_id", "workspace_id", "resource_owner_id"],
            [
                "interview.sessions.id",
                "interview.sessions.workspace_id",
                "interview.sessions.resource_owner_id",
            ],
            name="fk_tnt_recording_artifacts_session_id_scope",
            ondelete="CASCADE",
        ),
        schema="interview",
    )
    op.create_index(
        "ix_recording_artifacts_workspace_id",
        "recording_artifacts",
        ["workspace_id"],
        schema="interview",
    )
    op.create_index(
        "ix_recording_artifacts_resource_owner_id",
        "recording_artifacts",
        ["resource_owner_id"],
        schema="interview",
    )
    for table in _LEGACY_ARTIFACT_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY workspace_app_tenant_scope ON {table} AS PERMISSIVE FOR ALL "
            f"TO {app_role} USING (workspace_id = current_setting('app.workspace_id', true)) "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )


def _reverse_artifact_unification(app_role: str) -> None:
    """@brief 删除空统一 Artifact schema 并恢复旧表 / Remove the empty unified Artifact schema and restore legacy tables."""
    op.drop_constraint(
        "fk_render_jobs_artifact_workspace",
        "render_jobs",
        schema="resume",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_render_jobs_artifact_id_artifacts",
        "render_jobs",
        schema="resume",
        type_="foreignkey",
    )
    op.drop_table("artifact_pdf_source_maps", schema="agent")
    op.drop_table("artifact_contents", schema="agent")
    op.drop_table("artifacts", schema="agent")
    op.alter_column(
        "render_jobs", "artifact_id", schema="resume", type_=sa.String(128)
    )
    _restore_legacy_artifact_tables(app_role)
    op.create_foreign_key(
        "render_jobs_artifact_id_fkey",
        "render_jobs",
        "render_artifacts",
        ["artifact_id"],
        ["id"],
        source_schema="resume",
        referent_schema="resume",
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_tnt_render_jobs_artifact_id_scope",
        "render_jobs",
        "render_artifacts",
        ["artifact_id", "workspace_id", "resource_owner_id"],
        ["id", "workspace_id", "resource_owner_id"],
        source_schema="resume",
        referent_schema="resume",
    )


def _reverse_outbox_events() -> None:
    """@brief 回退空 outbox envelope 扩展 / Reverse empty outbox-envelope extensions."""
    op.execute("DROP TRIGGER assign_workspace_event_sequence ON agent.outbox_events")
    op.execute("DROP FUNCTION agent.assign_workspace_event_sequence()")
    for constraint in (
        "outbox_events_workspace_sequence",
        "ck_outbox_events_outbox_events_v2_replay_window",
        "ck_outbox_events_outbox_events_v2_trace",
        "ck_outbox_events_outbox_events_v2_payload",
        "ck_outbox_events_outbox_events_v2_envelope",
    ):
        constraint_type = "unique" if constraint == "outbox_events_workspace_sequence" else "check"
        op.drop_constraint(
            constraint,
            "outbox_events",
            schema="agent",
            type_=constraint_type,
        )
    op.drop_index(
        "ix_outbox_events_replay_expiry",
        table_name="outbox_events",
        schema="agent",
    )
    op.drop_index(
        "ix_outbox_events_workspace_sequence",
        table_name="outbox_events",
        schema="agent",
    )
    op.drop_column("outbox_events", "replay_expires_at", schema="agent")
    op.drop_column("outbox_events", "subject_revision", schema="agent")
    op.alter_column("outbox_events", "trace_id", schema="agent", type_=sa.String(128))
    op.alter_column(
        "outbox_events", "aggregate_type", schema="agent", type_=sa.String(64)
    )
    op.alter_column("outbox_events", "id", schema="agent", type_=sa.String(128))
    op.drop_table("workspace_event_sequences", schema="agent")


def _reverse_audit_events() -> None:
    """@brief 回退空 AuditEvent envelope 扩展 / Reverse empty AuditEvent-envelope extensions."""
    for constraint in (
        "ck_audit_events_audit_events_v2_request",
        "ck_audit_events_audit_events_v2_outcome",
        "ck_audit_events_audit_events_v2_target",
        "ck_audit_events_audit_events_v2_action",
        "ck_audit_events_audit_events_v2_actor",
        "ck_audit_events_audit_events_v2_id",
    ):
        op.drop_constraint(
            constraint,
            "audit_events",
            schema="identity",
            type_="check",
        )
    op.drop_index(
        "ix_audit_events_workspace_occurred_id",
        table_name="audit_events",
        schema="identity",
    )
    op.create_index(
        "ix_audit_events_workspace_id_occurred_at",
        "audit_events",
        ["workspace_id", "occurred_at"],
        schema="identity",
    )
    op.alter_column("audit_events", "actor_id", schema="identity", nullable=True)
    op.alter_column("audit_events", "resource_id", schema="identity", nullable=True)
    op.alter_column("audit_events", "request_id", schema="identity", nullable=True)
    op.drop_column("audit_events", "resource_revision", schema="identity")
    op.drop_column("audit_events", "actor_revision", schema="identity")
    op.drop_column("audit_events", "actor_type", schema="identity")
    op.alter_column("audit_events", "request_id", schema="identity", type_=sa.String(128))
    op.alter_column("audit_events", "resource_id", schema="identity", type_=sa.String(128))
    op.alter_column(
        "audit_events", "resource_type", schema="identity", type_=sa.String(64)
    )
    op.alter_column("audit_events", "actor_id", schema="identity", type_=sa.String(128))
    op.alter_column("audit_events", "id", schema="identity", type_=sa.String(128))


def _reverse_jobs() -> None:
    """@brief 回退空统一 Job projection 扩展 / Reverse empty unified Job-projection extensions."""
    op.drop_index(
        "ix_jobs_workspace_kind_subject_created",
        table_name="jobs",
        schema="agent",
    )
    op.drop_index(
        "ix_jobs_workspace_created_id",
        table_name="jobs",
        schema="agent",
    )
    op.create_index(
        "ix_jobs_workspace_id_status_created_at",
        "jobs",
        ["workspace_id", "status", "created_at"],
        schema="agent",
    )
    for constraint in (
        "ck_jobs_jobs_v2_timeline",
        "ck_jobs_jobs_v2_result_problem",
        "ck_jobs_jobs_v2_progress",
        "ck_jobs_jobs_v2_subject",
        "ck_jobs_jobs_status",
    ):
        op.drop_constraint(constraint, "jobs", schema="agent", type_="check")
    op.create_check_constraint(
        "jobs_status",
        "jobs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
        schema="agent",
    )
    op.drop_column("jobs", "problem", schema="agent")
    op.drop_column("jobs", "result_refs", schema="agent")
    op.drop_column("jobs", "target_resource_revision", schema="agent")
    op.drop_column("jobs", "progress_unit", schema="agent")
    op.alter_column("jobs", "target_resource_id", schema="agent", nullable=True)
    op.alter_column("jobs", "target_resource_type", schema="agent", nullable=True)
    op.alter_column(
        "jobs", "target_resource_type", schema="agent", type_=sa.String(64)
    )
    op.alter_column("jobs", "phase", schema="agent", type_=sa.String(64))
    op.alter_column("jobs", "job_type", schema="agent", type_=sa.String(96))


def downgrade() -> None:
    """@brief 仅允许空、完全可逆的 Platform state 回退 / Downgrade only empty, fully reversible Platform state."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    _install_migration_visibility(owner_role, _CORE_TABLES)
    _install_migration_visibility(owner_role, _NEW_PLATFORM_TABLES)
    _install_migration_visibility(owner_role, ("resume.render_jobs",))
    _preflight_downgrade()
    _drop_platform_security(app_role)
    _remove_migration_visibility(_NEW_PLATFORM_TABLES)
    _reverse_artifact_unification(app_role)
    _reverse_outbox_events()
    _reverse_audit_events()
    _reverse_jobs()
    _remove_migration_visibility((*_CORE_TABLES, "resume.render_jobs"))
