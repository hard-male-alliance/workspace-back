"""Allow committed Resume Proposal decisions to resume their Agent Run.

Revision ID: 20260727_0031
Revises: 20260727_0030
Create Date: 2026-07-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260727_0031"
down_revision = "20260727_0030"
branch_labels = None
depends_on = None

_PRIOR_WORK_EVENT_TYPES = (
    "agent.run.queued",
    "agent.tool_decision.recorded",
    "connection.revocation_requested",
    "interview.job.queued",
    "knowledge_source.deletion_requested",
    "knowledge_source.job_created",
    "resume.job_created",
)

_WORK_EVENT_TYPES = (
    "agent.proposal_decision.recorded",
    *_PRIOR_WORK_EVENT_TYPES,
)

_NOTIFICATION_EVENT_TYPES = (
    "agent.citation.added",
    "agent.message.completed",
    "agent.message.delta",
    "agent.run.cancelled",
    "agent.run.completed",
    "agent.run.failed",
    "agent.run.started",
    "agent.run.updated",
    "agent.status",
    "agent.tool_approval.expired",
    "agent.tool_approval.required",
    "connection.created",
    "job.updated",
    "knowledge_source.created",
    "knowledge_source.updated",
    "knowledge_source.version_created",
    "resume.created",
    "resume.deleted",
    "resume.metadata_updated",
    "resume.operations_applied",
    "resume.proposal_decided",
    "resume.updated",
)


def _sql_values(values: tuple[str, ...]) -> str:
    if not values or len(values) != len(set(values)):
        raise AssertionError("outbox migration event sets must be non-empty and unique")
    return ", ".join(f"'{value}'" for value in values)


def _replace_delivery_constraint(work_event_types: tuple[str, ...]) -> None:
    work_sql = _sql_values(work_event_types)
    notification_sql = _sql_values(_NOTIFICATION_EVENT_TYPES)
    op.drop_constraint(
        "outbox_events_delivery_class",
        "outbox_events",
        schema="agent",
        type_="check",
    )
    op.create_check_constraint(
        "outbox_events_delivery_class",
        "outbox_events",
        f"(event_type IN ({work_sql}) "
        "AND status IN ('pending', 'processing', 'published', 'failed')) OR "
        f"(event_type IN ({notification_sql}) "
        "AND status = 'published' AND published_at IS NOT NULL)",
        schema="agent",
    )


def upgrade() -> None:
    """Extend the durable-work closed set under one table lock."""

    op.execute("LOCK TABLE agent.outbox_events IN SHARE ROW EXCLUSIVE MODE")
    _replace_delivery_constraint(_WORK_EVENT_TYPES)


def downgrade() -> None:
    """Restore the prior closed set only when no continuation event exists."""

    op.execute("LOCK TABLE agent.outbox_events IN SHARE ROW EXCLUSIVE MODE")
    count = op.get_bind().scalar(
        sa.text(
            "SELECT count(*) FROM agent.outbox_events "
            "WHERE event_type = 'agent.proposal_decision.recorded'"
        )
    )
    if int(count or 0) != 0:
        raise RuntimeError(
            "0031 downgrade requires no Agent Proposal-decision continuation events"
        )
    _replace_delivery_constraint(_PRIOR_WORK_EVENT_TYPES)
