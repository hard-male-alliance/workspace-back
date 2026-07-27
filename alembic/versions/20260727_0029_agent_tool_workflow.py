"""Persist narrow Agent tools and ProposalDecision waits.

Revision ID: 20260727_0029
Revises: 20260723_0028
Create Date: 2026-07-27
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260727_0029"
down_revision = "20260723_0028"
branch_labels = None
depends_on = None

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _role(option: str) -> str:
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option(f"aiws.{option}")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > 63
    ):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """Expand the Agent state machine and preserve prior Proposal causality."""

    app_role = _role("app_role")
    op.create_unique_constraint(
        "resume_proposals_v2_id_workspace",
        "proposals",
        ["id", "workspace_id"],
        schema="resume",
    )
    op.create_table(
        "tool_invocations",
        sa.Column("id", sa.String(160), primary_key=True),
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
        sa.Column("run_id", sa.String(160), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("tool_name", sa.String(101), nullable=False),
        sa.Column(
            "arguments",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "result",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("proposal_id", sa.String(160)),
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
        sa.CheckConstraint(
            "tool_name ~ '^[a-z][a-z0-9_]{2,100}$' "
            "AND status IN ('completed', 'decision_required', 'failed')",
            name="agent_tool_invocations_v1_state",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(arguments) = 'object' AND jsonb_typeof(result) = 'object'",
            name="agent_tool_invocations_v1_payload",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "workspace_id"],
            ["agent.runs.id", "agent.runs.workspace_id"],
            name="fk_agent_tool_invocations_run_workspace",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["proposal_id", "workspace_id"],
            ["resume.proposals.id", "resume.proposals.workspace_id"],
            name="fk_agent_tool_invocations_proposal_workspace",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "run_id",
            "ordinal",
            name="agent_tool_invocations_v1_run_ordinal",
        ),
        schema="agent",
    )
    op.create_index(
        "ix_agent_tool_invocations_workspace_run_ordinal",
        "tool_invocations",
        ["workspace_id", "run_id", "ordinal"],
        schema="agent",
    )

    op.execute(
        sa.text(
            """
            INSERT INTO agent.tool_invocations (
                id, workspace_id, resource_owner_id, run_id, ordinal, tool_name,
                arguments, result, status, proposal_id, created_at, updated_at,
                revision, extensions
            )
            SELECT
                'toolinv_' || md5(proposal.id),
                proposal.workspace_id,
                proposal.resource_owner_id,
                proposal.agent_run_id,
                1,
                'resume_request_proposal_decision',
                '{}'::jsonb,
                jsonb_build_object('proposal_id', proposal.id),
                CASE WHEN proposal.status = 'pending'
                     THEN 'decision_required' ELSE 'completed' END,
                proposal.id,
                proposal.created_at,
                proposal.updated_at,
                1,
                jsonb_build_object('migrated', true)
            FROM resume.proposals AS proposal
            WHERE proposal.agent_run_id IS NOT NULL
            """
        )
    )

    for name in (
        "agent_runs_v2_status",
        "agent_runs_v2_output",
        "agent_runs_v2_terminal_results",
    ):
        op.drop_constraint(name, "runs", schema="agent", type_="check")
    op.execute(
        sa.text(
            """
            UPDATE agent.jobs AS job
            SET status = 'running',
                phase = 'waiting_for_proposal_decision',
                completed_units = 0,
                total_units = NULL,
                progress_unit = 'steps',
                percent = NULL,
                result_refs = '[]'::jsonb,
                result = NULL,
                finished_at = NULL,
                revision = job.revision + 1,
                updated_at = now()
            FROM agent.runs AS run
            JOIN resume.proposals AS proposal
              ON proposal.workspace_id = run.workspace_id
             AND proposal.agent_run_id = run.id
             AND proposal.status = 'pending'
            WHERE job.workspace_id = run.workspace_id
              AND job.id = run.job_id
              AND job.status = 'succeeded'
              AND run.status = 'succeeded'
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE agent.runs AS run
            SET status = 'waiting_for_proposal_decision',
                revision = run.revision + 1,
                updated_at = now()
            FROM resume.proposals AS proposal
            WHERE proposal.workspace_id = run.workspace_id
              AND proposal.agent_run_id = run.id
              AND proposal.status = 'pending'
              AND run.status = 'succeeded'
            """
        )
    )
    op.create_check_constraint(
        "agent_runs_v2_status",
        "runs",
        "status IN ('queued','running','waiting_for_approval',"
        "'waiting_for_proposal_decision','succeeded','failed','cancelled')",
        schema="agent",
    )
    op.create_check_constraint(
        "agent_runs_v2_output",
        "runs",
        "((status IN ('succeeded','waiting_for_proposal_decision') "
        "AND output_message_id IS NOT NULL) OR "
        "(status NOT IN ('succeeded','waiting_for_proposal_decision') "
        "AND output_message_id IS NULL))",
        schema="agent",
    )
    op.create_check_constraint(
        "agent_runs_v2_terminal_results",
        "runs",
        "status IN ('succeeded','failed','cancelled','waiting_for_proposal_decision') "
        "OR (jsonb_array_length(proposal_refs) = 0 AND usage IS NULL)",
        schema="agent",
    )

    op.execute("ALTER TABLE agent.tool_invocations ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE agent.tool_invocations FORCE ROW LEVEL SECURITY")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE agent.tool_invocations FROM PUBLIC, {app_role}"
    )
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE (status, result, proposal_id, updated_at, revision) "
        f"ON TABLE agent.tool_invocations TO {app_role}"
    )
    op.execute(
        f"CREATE POLICY agent_v2_workspace_select ON agent.tool_invocations "
        f"AS PERMISSIVE FOR SELECT TO {app_role} "
        "USING (workspace_id = current_setting('app.workspace_id', true) AND "
        "current_setting('app.actor_id', true) IS NOT NULL)"
    )
    op.execute(
        f"CREATE POLICY agent_v2_workspace_insert ON agent.tool_invocations "
        f"AS PERMISSIVE FOR INSERT TO {app_role} WITH CHECK ("
        "workspace_id = current_setting('app.workspace_id', true) AND "
        "resource_owner_id = current_setting('app.actor_id', true))"
    )
    op.execute(
        f"CREATE POLICY agent_v2_workspace_update ON agent.tool_invocations "
        f"AS PERMISSIVE FOR UPDATE TO {app_role} "
        "USING (workspace_id = current_setting('app.workspace_id', true) AND "
        "current_setting('app.actor_id', true) IS NOT NULL) "
        "WITH CHECK (workspace_id = current_setting('app.workspace_id', true) AND "
        "resource_owner_id = current_setting('app.actor_id', true))"
    )


def downgrade() -> None:
    """Remove the new trace after making the old state machine representable."""

    op.execute(
        """
        UPDATE agent.runs
        SET status = 'failed',
            output_message_id = NULL,
            proposal_refs = '[]'::jsonb,
            usage = NULL,
            problem = jsonb_build_object(
                'type', 'about:blank',
                'title', 'Proposal decision wait is unsupported after downgrade',
                'status', 500,
                'code', 'agent.downgraded_proposal_wait',
                'retryable', false,
                'request_id', id
            ),
            revision = revision + 1,
            updated_at = now()
        WHERE status = 'waiting_for_proposal_decision'
        """
    )
    op.execute(
        sa.text(
            """
            UPDATE agent.jobs AS job
            SET status = 'failed',
                phase = 'failed',
                completed_units = 0,
                total_units = NULL,
                progress_unit = 'steps',
                percent = NULL,
                result_refs = '[]'::jsonb,
                result = NULL,
                problem = jsonb_build_object(
                    'type', 'about:blank',
                    'title', 'Proposal decision wait is unsupported after downgrade',
                    'status', 500,
                    'code', 'agent.downgraded_proposal_wait',
                    'retryable', false,
                    'request_id', run.id
                ),
                finished_at = now(),
                revision = job.revision + 1,
                updated_at = now()
            FROM agent.runs AS run
            WHERE job.workspace_id = run.workspace_id
              AND job.id = run.job_id
              AND job.status = 'running'
              AND run.status = 'failed'
              AND run.problem ->> 'code' = 'agent.downgraded_proposal_wait'
            """
        )
    )
    for name in (
        "agent_runs_v2_status",
        "agent_runs_v2_output",
        "agent_runs_v2_terminal_results",
    ):
        op.drop_constraint(name, "runs", schema="agent", type_="check")
    op.create_check_constraint(
        "agent_runs_v2_status",
        "runs",
        "status IN ('queued','running','waiting_for_approval','succeeded','failed','cancelled')",
        schema="agent",
    )
    op.create_check_constraint(
        "agent_runs_v2_output",
        "runs",
        "((status = 'succeeded' AND output_message_id IS NOT NULL) OR "
        "(status <> 'succeeded' AND output_message_id IS NULL))",
        schema="agent",
    )
    op.create_check_constraint(
        "agent_runs_v2_terminal_results",
        "runs",
        "status IN ('succeeded','failed','cancelled') OR "
        "(jsonb_array_length(proposal_refs) = 0 AND usage IS NULL)",
        schema="agent",
    )
    op.drop_table("tool_invocations", schema="agent")
    op.drop_constraint(
        "resume_proposals_v2_id_workspace",
        "proposals",
        schema="resume",
        type_="unique",
    )
