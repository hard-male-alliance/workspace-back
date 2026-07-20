"""@brief 强类型可观测性信封与固定 Dashboard 视图 / Typed observability envelope and fixed Dashboard view.

Revision ID: 20260721_0006
Revises: 20260718_0005
Create Date: 2026-07-21

@note 本 revision 以 v2 表回填后原子切换，删除 telemetry 上无意义的业务 lifecycle
与身份外键；历史 span 缺少可信 duration/causality，故转换为明确的 legacy log，绝不
伪造 trace。升级前由约束预检阻止非有限 metric 值被静默改写。
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260721_0006"
down_revision = "20260718_0005"
branch_labels = None
depends_on = None

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief dbctl 传入 role 标识符白名单 / Allowlist for role identifiers supplied by dbctl."""


def _configured_role(option: str) -> str:
    """@brief 读取并安全引用 migration role / Read and safely quote a migration role.

    @param option ``owner_role``、``app_role`` 或 ``dashboard_role`` / Role option name.
    @return 双引号 SQL 标识符 / Double-quoted SQL identifier.
    @raise RuntimeError 缺少或非法 role 时抛出 / Raised for a missing or invalid role.
    """
    migration_config = op.get_context().config
    if migration_config is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = migration_config.get_main_option(f"aiws.{option}")
    if not value or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def _create_v2_table() -> None:
    """@brief 创建具有判别联合约束的 v2 表 / Create the v2 table with discriminated-union constraints."""
    op.create_table(
        "telemetry_records_v2",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("workspace_id", sa.String(length=128)),
        sa.Column("resource_owner_id", sa.String(length=128)),
        sa.Column("actor_id", sa.String(length=128)),
        sa.Column(
            "occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column(
            "observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("source", sa.String(length=16), nullable=False),
        sa.Column("service", sa.String(length=128), nullable=False),
        sa.Column("service_version", sa.String(length=128)),
        sa.Column("deployment_environment", sa.String(length=128)),
        sa.Column("service_instance_id", sa.String(length=128)),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("metric_type", sa.String(length=16)),
        sa.Column("value", sa.Float()),
        sa.Column("unit", sa.String(length=32)),
        sa.Column("severity_number", sa.SmallInteger()),
        sa.Column("severity_text", sa.String(length=16)),
        sa.Column("duration_ms", sa.Float()),
        sa.Column("span_status", sa.String(length=16)),
        sa.Column("request_id", sa.String(length=128)),
        sa.Column("trace_id", sa.String(length=32)),
        sa.Column("span_id", sa.String(length=16)),
        sa.Column("parent_span_id", sa.String(length=16)),
        sa.Column("client_event_id", sa.String(length=128)),
        sa.Column(
            "attributes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        # PostgreSQL 会为 PRIMARY KEY 创建同名 backing index; shadow table 存续期间旧表的
        # ``pk_telemetry_records`` 仍占用 schema relation 名, 故必须先使用唯一临时名。
        sa.PrimaryKeyConstraint("id", name="pk_telemetry_records_v2"),
        sa.CheckConstraint(
            "kind IN ('metric', 'log', 'span')",
            name=op.f("ck_telemetry_records_telemetry_signal_kind"),
        ),
        sa.CheckConstraint(
            "source IN ('backend', 'frontend')",
            name=op.f("ck_telemetry_records_telemetry_signal_source"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(attributes) = 'object'",
            name=op.f("ck_telemetry_records_telemetry_attributes_object"),
        ),
        sa.CheckConstraint(
            "num_nonnulls(workspace_id, resource_owner_id, actor_id) IN (0, 3)",
            name=op.f("ck_telemetry_records_telemetry_scope_complete"),
        ),
        sa.CheckConstraint(
            "(source = 'frontend' AND workspace_id IS NOT NULL AND client_event_id IS NOT NULL) "
            "OR (source = 'backend' AND client_event_id IS NULL)",
            name=op.f("ck_telemetry_records_telemetry_source_contract"),
        ),
        sa.CheckConstraint(
            "(kind = 'metric' AND metric_type IS NOT NULL AND value IS NOT NULL AND unit IS NOT NULL "
            "AND severity_number IS NULL AND severity_text IS NULL AND duration_ms IS NULL "
            "AND span_status IS NULL) OR "
            "(kind = 'log' AND metric_type IS NULL AND value IS NULL AND unit IS NULL "
            "AND severity_number BETWEEN 1 AND 24 AND severity_text IS NOT NULL "
            "AND duration_ms IS NULL AND span_status IS NULL) OR "
            "(kind = 'span' AND metric_type IS NULL AND value IS NULL AND unit IS NULL "
            "AND severity_number IS NULL AND severity_text IS NULL AND duration_ms >= 0 "
            "AND span_status IS NOT NULL AND trace_id IS NOT NULL AND span_id IS NOT NULL)",
            name=op.f("ck_telemetry_records_telemetry_kind_fields"),
        ),
        sa.CheckConstraint(
            "metric_type IS NULL OR metric_type IN ('counter', 'gauge', 'histogram')",
            name=op.f("ck_telemetry_records_telemetry_metric_type"),
        ),
        sa.CheckConstraint(
            "value IS NULL OR value NOT IN "
            "('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)",
            name=op.f("ck_telemetry_records_telemetry_finite_value"),
        ),
        sa.CheckConstraint(
            "duration_ms IS NULL OR duration_ms NOT IN "
            "('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)",
            name=op.f("ck_telemetry_records_telemetry_finite_duration"),
        ),
        sa.CheckConstraint(
            "span_status IS NULL OR span_status IN ('unset', 'ok', 'error')",
            name=op.f("ck_telemetry_records_telemetry_span_status"),
        ),
        sa.CheckConstraint(
            "(trace_id IS NULL AND span_id IS NULL AND parent_span_id IS NULL) OR "
            "(trace_id ~ '^[0-9a-f]{32}$' AND trace_id <> repeat('0', 32) "
            "AND span_id ~ '^[0-9a-f]{16}$' AND span_id <> repeat('0', 16) "
            "AND (parent_span_id IS NULL OR "
            "(parent_span_id ~ '^[0-9a-f]{16}$' "
            "AND parent_span_id <> repeat('0', 16))))",
            name=op.f("ck_telemetry_records_telemetry_trace_context"),
        ),
        schema="observability",
    )


def _backfill_v2() -> None:
    """@brief 规范化旧指标并完整回填 v2 / Normalize legacy metrics and fully backfill v2.

    @raise RuntimeError 存在非有限旧 metric 或回填行数不一致时由 PostgreSQL 中止迁移。
    """
    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM observability.telemetry_records
                    WHERE kind = 'metric'
                      AND value IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)
                ) THEN
                    RAISE EXCEPTION 'legacy telemetry contains non-finite metric values';
                END IF;
            END
            $$
            """
        )
    )
    op.execute(
        sa.text(
            """
            INSERT INTO observability.telemetry_records_v2 (
                id,
                workspace_id,
                resource_owner_id,
                actor_id,
                occurred_at,
                observed_at,
                kind,
                source,
                service,
                service_version,
                deployment_environment,
                service_instance_id,
                name,
                metric_type,
                value,
                unit,
                severity_number,
                severity_text,
                duration_ms,
                span_status,
                request_id,
                trace_id,
                span_id,
                parent_span_id,
                client_event_id,
                attributes
            )
            SELECT
                legacy.id,
                legacy.workspace_id,
                legacy.resource_owner_id,
                legacy.actor_id,
                legacy.occurred_at,
                COALESCE(legacy.created_at, legacy.occurred_at),
                CASE WHEN legacy.kind = 'span' THEN 'log' ELSE legacy.kind END,
                'backend',
                legacy.service,
                NULL,
                'legacy_unknown',
                NULL,
                CASE
                    WHEN legacy.kind = 'span' THEN 'legacy.span.unlinked'
                    WHEN legacy.name = 'requests' THEN 'aiws.http.server.request.count'
                    WHEN legacy.name = 'errors' THEN 'aiws.http.server.error.count'
                    WHEN legacy.name = 'latency_ms' THEN 'http.server.request.duration'
                    WHEN legacy.name = 'saturation'
                        THEN 'aiws.runtime.supervisor.utilization'
                    WHEN legacy.kind = 'metric'
                     AND legacy.name NOT LIKE 'aiws.%'
                     AND legacy.name NOT LIKE 'http.%'
                        THEN 'aiws.' || legacy.name
                    ELSE legacy.name
                END,
                CASE
                    WHEN legacy.kind <> 'metric' THEN NULL
                    WHEN legacy.name = 'latency_ms' THEN 'histogram'
                    WHEN legacy.name = 'saturation' THEN 'gauge'
                    ELSE 'counter'
                END,
                CASE
                    WHEN legacy.kind <> 'metric' THEN NULL
                    WHEN legacy.name = 'latency_ms' THEN legacy.value / 1000.0
                    ELSE legacy.value
                END,
                CASE
                    WHEN legacy.kind <> 'metric' THEN NULL
                    WHEN legacy.name = 'latency_ms' THEN 's'
                    WHEN legacy.name = 'saturation' THEN '1'
                    WHEN legacy.name = 'requests' THEN '{request}'
                    WHEN legacy.name = 'errors' THEN '{error}'
                    ELSE '{event}'
                END,
                CASE
                    WHEN legacy.kind NOT IN ('log', 'span') THEN NULL
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        IN ('CRITICAL', 'FATAL') THEN 21
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        = 'ERROR' THEN 17
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        IN ('WARNING', 'WARN') THEN 13
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        = 'DEBUG' THEN 5
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        = 'TRACE' THEN 1
                    ELSE 9
                END,
                CASE
                    WHEN legacy.kind NOT IN ('log', 'span') THEN NULL
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        IN ('CRITICAL', 'FATAL') THEN 'CRITICAL'
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        = 'ERROR' THEN 'ERROR'
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        IN ('WARNING', 'WARN') THEN 'WARNING'
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        = 'DEBUG' THEN 'DEBUG'
                    WHEN upper(COALESCE(NULLIF(legacy.severity, ''), legacy.attributes->>'level', 'INFO'))
                        = 'TRACE' THEN 'TRACE'
                    ELSE 'INFO'
                END,
                NULL,
                NULL,
                legacy.request_id,
                CASE
                    WHEN legacy.trace_id ~ '^[0-9a-f]{32}$'
                     AND legacy.trace_id <> repeat('0', 32)
                     AND legacy.span_id ~ '^[0-9a-f]{16}$'
                     AND legacy.span_id <> repeat('0', 16)
                    THEN legacy.trace_id
                    ELSE NULL
                END,
                CASE
                    WHEN legacy.trace_id ~ '^[0-9a-f]{32}$'
                     AND legacy.trace_id <> repeat('0', 32)
                     AND legacy.span_id ~ '^[0-9a-f]{16}$'
                     AND legacy.span_id <> repeat('0', 16)
                    THEN legacy.span_id
                    ELSE NULL
                END,
                CASE
                    WHEN legacy.trace_id ~ '^[0-9a-f]{32}$'
                     AND legacy.trace_id <> repeat('0', 32)
                     AND legacy.span_id ~ '^[0-9a-f]{16}$'
                     AND legacy.span_id <> repeat('0', 16)
                     AND legacy.parent_span_id ~ '^[0-9a-f]{16}$'
                     AND legacy.parent_span_id <> repeat('0', 16)
                    THEN legacy.parent_span_id
                    ELSE NULL
                END,
                NULL,
                CASE
                    WHEN legacy.kind = 'span' THEN
                        (legacy.safe_attributes - 'status_code')
                            || '{"event_type":"legacy_span","outcome":"unknown"}'::jsonb
                    WHEN legacy.service = 'backend.api'
                     AND legacy.safe_attributes ? 'status_code' THEN
                        (legacy.safe_attributes - 'status_code') || jsonb_build_object(
                            'method',
                                upper(COALESCE(legacy.safe_attributes->>'operation', 'UNKNOWN')),
                            'status_class',
                                CASE
                                    WHEN (legacy.safe_attributes->>'status_code')
                                        ~ '^[1-5][0-9][0-9]$'
                                    THEN substring(
                                        legacy.safe_attributes->>'status_code', 1, 1
                                    ) || 'xx'
                                    ELSE 'unknown'
                                END,
                            'outcome',
                                CASE
                                    WHEN (legacy.safe_attributes->>'status_code') ~ '^5'
                                        THEN 'server_error'
                                    WHEN (legacy.safe_attributes->>'status_code') ~ '^4'
                                        THEN 'client_error'
                                    ELSE 'success'
                                END
                        )
                    ELSE legacy.safe_attributes - 'status_code'
                END
            FROM (
                SELECT
                    source.*,
                    COALESCE(
                        (
                            SELECT jsonb_object_agg(attribute.key, attribute.value)
                            FROM jsonb_each(
                                CASE WHEN jsonb_typeof(source.attributes) = 'object'
                                    THEN source.attributes ELSE '{}'::jsonb END
                            ) AS attribute(key, value)
                            WHERE attribute.key IN (
                                'capability',
                                'error_code',
                                'event_type',
                                'job_type',
                                'level',
                                'method',
                                'metric_name',
                                'operation',
                                'outcome',
                                'provider',
                                'release',
                                'route',
                                'stack_fingerprint',
                                'status_class',
                                'status_code',
                                'transport'
                            )
                              AND jsonb_typeof(attribute.value)
                                  IN ('string', 'number', 'boolean')
                              AND length(attribute.value #>> '{}') BETWEEN 1 AND 256
                              AND (
                                  jsonb_typeof(attribute.value) <> 'string'
                                  OR (attribute.value #>> '{}') !~ '[[:cntrl:]]'
                              )
                        ),
                        '{}'::jsonb
                    ) AS safe_attributes
                FROM observability.telemetry_records AS source
            ) AS legacy
            """
        )
    )
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                source_count bigint;
                target_count bigint;
                legacy_metric_count bigint;
                legacy_log_count bigint;
                legacy_span_count bigint;
                target_metric_count bigint;
                target_log_count bigint;
                target_span_count bigint;
                legacy_latency_count bigint;
                target_latency_count bigint;
                legacy_latency_sum double precision;
                target_latency_sum double precision;
            BEGIN
                SELECT count(*) INTO source_count FROM observability.telemetry_records;
                SELECT count(*) INTO target_count FROM observability.telemetry_records_v2;
                IF source_count <> target_count THEN
                    RAISE EXCEPTION 'telemetry backfill count mismatch: source %, target %',
                        source_count, target_count;
                END IF;
                SELECT
                    count(*) FILTER (WHERE kind = 'metric'),
                    count(*) FILTER (WHERE kind = 'log'),
                    count(*) FILTER (WHERE kind = 'span')
                INTO legacy_metric_count, legacy_log_count, legacy_span_count
                FROM observability.telemetry_records;
                SELECT
                    count(*) FILTER (WHERE kind = 'metric'),
                    count(*) FILTER (WHERE kind = 'log'),
                    count(*) FILTER (WHERE kind = 'span')
                INTO target_metric_count, target_log_count, target_span_count
                FROM observability.telemetry_records_v2;
                IF target_metric_count <> legacy_metric_count
                   OR target_log_count <> legacy_log_count + legacy_span_count
                   OR target_span_count <> 0 THEN
                    RAISE EXCEPTION
                        'telemetry kind backfill mismatch: metric %/%, log %/%, span %',
                        target_metric_count,
                        legacy_metric_count,
                        target_log_count,
                        legacy_log_count + legacy_span_count,
                        target_span_count;
                END IF;
                SELECT count(*), COALESCE(sum(value / 1000.0), 0.0)
                INTO legacy_latency_count, legacy_latency_sum
                FROM observability.telemetry_records
                WHERE kind = 'metric' AND name = 'latency_ms';
                SELECT count(*), COALESCE(sum(value), 0.0)
                INTO target_latency_count, target_latency_sum
                FROM observability.telemetry_records_v2
                WHERE kind = 'metric'
                  AND name = 'http.server.request.duration'
                  AND id IN (
                      SELECT id
                      FROM observability.telemetry_records
                      WHERE kind = 'metric' AND name = 'latency_ms'
                  );
                IF target_latency_count <> legacy_latency_count
                   OR abs(target_latency_sum - legacy_latency_sum)
                        > greatest(1e-9, abs(legacy_latency_sum) * 1e-12) THEN
                    RAISE EXCEPTION
                        'telemetry latency conversion mismatch: count %/%, sum %/%',
                        target_latency_count,
                        legacy_latency_count,
                        target_latency_sum,
                        legacy_latency_sum;
                END IF;
            END
            $$
            """
        )
    )


def _create_indexes() -> None:
    """@brief 创建读取、trace 与 retention 索引 / Create read, trace, and retention indexes."""
    op.create_index(
        "ix_telemetry_metric_workspace_occurred",
        "telemetry_records",
        ["workspace_id", "occurred_at", "service", "name"],
        schema="observability",
        postgresql_where=sa.text("kind = 'metric'"),
        postgresql_include=["value", "observed_at", "unit", "metric_type"],
    )
    op.create_index(
        "ix_telemetry_event_workspace_occurred_observed",
        "telemetry_records",
        ["workspace_id", "occurred_at", "observed_at"],
        schema="observability",
        postgresql_where=sa.text("kind IN ('log', 'span')"),
    )
    op.create_index(
        "ix_telemetry_trace_occurred",
        "telemetry_records",
        ["trace_id", "occurred_at", "span_id"],
        schema="observability",
        postgresql_where=sa.text("trace_id IS NOT NULL"),
    )
    op.create_index(
        "ix_telemetry_observed_at",
        "telemetry_records",
        ["observed_at"],
        schema="observability",
    )
    op.create_index(
        "uq_telemetry_frontend_client_event",
        "telemetry_records",
        ["workspace_id", "resource_owner_id", "actor_id", "client_event_id"],
        unique=True,
        schema="observability",
        postgresql_where=sa.text("source = 'frontend' AND client_event_id IS NOT NULL"),
    )


def _create_security_and_view() -> None:
    """@brief 重建最小 RLS、权限与固定 dashboard view / Rebuild minimal RLS, grants, and fixed Dashboard view."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    scoped_predicate = (
        "workspace_id = current_setting('app.workspace_id', true) "
        "AND resource_owner_id = current_setting('app.resource_owner_id', true) "
        "AND actor_id = current_setting('app.actor_id', true)"
    )
    op.execute("ALTER TABLE observability.telemetry_records ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE observability.telemetry_records FORCE ROW LEVEL SECURITY")
    op.execute(
        sa.text(
            "CREATE POLICY workspace_app_telemetry_insert "
            "ON observability.telemetry_records AS PERMISSIVE FOR INSERT "
            f"TO {app_role} WITH CHECK ("
            "(workspace_id IS NULL AND resource_owner_id IS NULL AND actor_id IS NULL) OR "
            f"({scoped_predicate}))"
        )
    )
    op.execute(
        sa.text(
            "CREATE POLICY workspace_owner_telemetry_maintenance "
            "ON observability.telemetry_records AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )
    )
    op.execute(
        """
        CREATE VIEW observability.dashboard_signals
        WITH (security_barrier = true)
        AS
        SELECT
            workspace_id,
            occurred_at,
            observed_at,
            source,
            service,
            kind,
            name,
            metric_type,
            value,
            unit,
            severity_number,
            severity_text,
            duration_ms,
            span_status,
            request_id,
            trace_id,
            span_id,
            attributes
        FROM observability.telemetry_records
        WHERE kind IN ('metric', 'log', 'span')
        """
    )
    op.execute(f"REVOKE ALL ON observability.telemetry_records FROM {dashboard_role}")
    op.execute(f"REVOKE ALL ON observability.telemetry_records FROM {app_role}")
    op.execute(f"GRANT INSERT ON observability.telemetry_records TO {app_role}")
    op.execute(f"GRANT SELECT ON observability.dashboard_signals TO {dashboard_role}")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner_role} IN SCHEMA observability "
        f"REVOKE INSERT ON TABLES FROM {app_role}"
    )


def upgrade() -> None:
    """@brief 原子升级为强类型统一信号存储 / Atomically upgrade to typed unified signal storage."""

    # 任一既有 writer 或 dashboard 长事务超过受控维护窗口时原子失败；不得先冻结
    # telemetry DML 后无限等待 DROP VIEW 所需的 AccessExclusiveLock。
    op.execute("SET LOCAL lock_timeout = '30s'")
    # SHARE ROW EXCLUSIVE 会先等待已开始的 writer 提交，随后阻止新的
    # INSERT/UPDATE/DELETE 直到本 migration 事务切表完成；ACCESS SHARE 读仍可在
    # backfill 阶段进行。若不先冻结旧表，backfill 后、DROP 前提交的行会丢失。
    op.execute(
        "LOCK TABLE observability.telemetry_records IN SHARE ROW EXCLUSIVE MODE"
    )
    _create_v2_table()
    _backfill_v2()
    op.execute("DROP VIEW observability.dashboard_metric_samples")
    op.drop_table("telemetry_records", schema="observability")
    op.rename_table(
        "telemetry_records_v2", "telemetry_records", schema="observability"
    )
    # RENAME CONSTRAINT 会同步重命名 PRIMARY KEY 的 backing index; 切换完成后恢复 ORM
    # naming convention 所期待的稳定名称。
    op.execute(
        "ALTER TABLE observability.telemetry_records "
        "RENAME CONSTRAINT pk_telemetry_records_v2 TO pk_telemetry_records"
    )
    _create_indexes()
    _create_security_and_view()


def downgrade() -> None:
    """@brief 明确拒绝会丢失新信号语义的 downgrade / Refuse a downgrade that would lose new signal semantics.

    @raise RuntimeError v2 含 frontend、span duration 与 resource metadata，无法无损映射旧表。
    """
    raise RuntimeError(
        "20260721_0006 is intentionally irreversible because the legacy schema cannot represent "
        "frontend idempotency, typed metrics, or completed spans"
    )
