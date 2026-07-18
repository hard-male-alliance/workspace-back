"""Backfill Resume render-job associations from unified Job records.

Revision ID: 20260718_0005
Revises: 20260717_0004
Create Date: 2026-07-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260718_0005"
down_revision = "20260717_0004"
branch_labels = None
depends_on = None

_BACKFILL_TABLES = (
    "agent.jobs",
    "resume.documents",
    "resume.revisions",
    "resume.render_artifacts",
    "resume.render_jobs",
)


def upgrade() -> None:
    """Create missing revision-bound links for legacy Resume render Jobs."""
    for table in _BACKFILL_TABLES:
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
    op.execute(
        sa.text(
            """
            WITH candidates AS (
                SELECT
                    job.*,
                    job.extensions #>> '{runtime,extensions,resume_id}' AS resume_id,
                    job.extensions #>> '{runtime,extensions,resume_revision}'
                        AS resume_revision_text,
                    job.extensions #>> '{runtime,extensions,render_profile}' AS render_profile,
                    job.extensions #>> '{runtime,extensions,artifacts,0,id}' AS artifact_id,
                    job.extensions #> '{runtime,extensions,diagnostics}' AS diagnostics_json
                FROM agent.jobs AS job
                WHERE job.job_type = 'resume.render'
            ),
            valid_candidates AS (
                SELECT
                    candidate.*,
                    candidate.resume_revision_text::integer AS resume_revision
                FROM candidates AS candidate
                WHERE candidate.resume_id IS NOT NULL
                  AND candidate.resume_revision_text ~ '^[1-9][0-9]*$'
            )
            INSERT INTO resume.render_jobs (
                id,
                workspace_id,
                resource_owner_id,
                job_id,
                resume_id,
                resume_revision_id,
                artifact_id,
                render_profile,
                diagnostics,
                created_at,
                updated_at,
                revision,
                extensions
            )
            SELECT
                'renderjob_legacy_' || md5(candidate.id),
                candidate.workspace_id,
                candidate.resource_owner_id,
                candidate.id,
                candidate.resume_id,
                resume_revision.id,
                artifact.id,
                COALESCE(NULLIF(candidate.render_profile, ''), 'legacy_unknown'),
                CASE jsonb_typeof(candidate.diagnostics_json)
                    WHEN 'object' THEN candidate.diagnostics_json
                    WHEN 'array' THEN jsonb_build_object('items', candidate.diagnostics_json)
                    ELSE jsonb_build_object('items', '[]'::jsonb)
                END,
                candidate.created_at,
                candidate.updated_at,
                1,
                '{}'::jsonb
            FROM valid_candidates AS candidate
            JOIN resume.documents AS document
              ON document.id = candidate.resume_id
             AND document.workspace_id = candidate.workspace_id
             AND document.resource_owner_id = candidate.resource_owner_id
            JOIN resume.revisions AS resume_revision
              ON resume_revision.resume_id = candidate.resume_id
             AND resume_revision.revision_no = candidate.resume_revision
             AND resume_revision.workspace_id = candidate.workspace_id
             AND resume_revision.resource_owner_id = candidate.resource_owner_id
            LEFT JOIN resume.render_artifacts AS artifact
              ON artifact.id = candidate.artifact_id
             AND artifact.resume_id = candidate.resume_id
             AND artifact.resume_revision_id = resume_revision.id
             AND artifact.workspace_id = candidate.workspace_id
             AND artifact.resource_owner_id = candidate.resource_owner_id
            ON CONFLICT (job_id) DO NOTHING
            """
        )
    )
    for table in reversed(_BACKFILL_TABLES):
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")


def downgrade() -> None:
    """Remove only links created by this legacy backfill."""
    op.execute("ALTER TABLE resume.render_jobs NO FORCE ROW LEVEL SECURITY")
    op.execute(
        sa.text(
            "DELETE FROM resume.render_jobs "
            "WHERE id LIKE 'renderjob\\_legacy\\_%' ESCAPE '\\'"
        )
    )
    op.execute("ALTER TABLE resume.render_jobs FORCE ROW LEVEL SECURITY")
