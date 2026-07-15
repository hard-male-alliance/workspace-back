"""@brief 租户复合完整性与遥测保留基础 / Tenant composite integrity and telemetry-retention foundations.

Revision ID: 20260715_0003
Revises: 20260715_0002
Create Date: 2026-07-15

本 revision 为既有的单列外键保留原样，并新增两个更强的数据库不变量：

* 每个租户资源的 ``(id, workspace_id, resource_owner_id)`` 是可被引用的唯一键；
* 子资源引用租户父资源时，外键同时比较父、子的 workspace 与 owner。

因此，即使应用层遗漏租户谓词，数据库也不能把一个 workspace 的子记录挂到另一个
workspace 的父记录上。新外键先以 ``NOT VALID`` 加入，再逐一 ``VALIDATE``，以降低
在线 DDL 的初始锁定和扫描耦合；PostgreSQL 不支持 ``UNIQUE NOT VALID``，但这些唯一键
均包含既有主键 ``id``，所以在语义上已由既有数据不变量保证。

@note ``ON DELETE SET NULL`` 的复合外键只置空可空的引用 id 列，而不置空不可空的
``workspace_id`` / ``resource_owner_id``。这保持与原单列外键相同的删除语义。
遥测表还会补齐不可空 ``actor_id``：历史记录以 resource owner 回填；受 dbctl 配置化
owner role 保护的 maintenance policy 则可安全执行按保留期的删除。
"""

from __future__ import annotations

import re
from collections.abc import Iterable

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260715_0003"
down_revision = "20260715_0002"
branch_labels = None
depends_on = None


TableRef = tuple[str, str]
"""@brief schema/table 二元组 / Schema-and-table reference tuple."""


TenantRelation = tuple[TableRef, str, TableRef, str, bool]
"""@brief 子表、引用列、父表、删除动作、是否可空的租户关系 / Tenant parent relationship.

元组字段依次为 ``(child, foreign_key_column, parent, on_delete, nullable)``。
"""


_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
"""@brief 固定 PostgreSQL 标识符白名单 / Allowlist for fixed PostgreSQL identifiers."""


_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief dbctl 传入 PostgreSQL role 的标识符白名单 / Allowlist for dbctl PostgreSQL role identifiers."""


_POSTGRES_IDENTIFIER_MAX_LENGTH = 63
"""@brief PostgreSQL 标识符最大字节长度 / PostgreSQL identifier maximum length."""


TENANT_TABLES: tuple[TableRef, ...] = (
    ("identity", "workspace_members"),
    ("identity", "audit_events"),
    ("identity", "idempotency_records"),
    ("resume", "template_versions"),
    ("resume", "documents"),
    ("resume", "revisions"),
    ("resume", "operation_batches"),
    ("resume", "operations"),
    ("resume", "proposals"),
    ("resume", "proposal_operations"),
    ("resume", "render_artifacts"),
    ("resume", "artifact_blobs"),
    ("resume", "render_jobs"),
    ("resume", "pdf_source_map_entries"),
    ("agent", "jobs"),
    ("agent", "outbox_events"),
    ("agent", "conversations"),
    ("agent", "messages"),
    ("agent", "runs"),
    ("agent", "run_events"),
    ("agent", "tool_approvals"),
    ("interview", "scenarios"),
    ("interview", "sessions"),
    ("interview", "events"),
    ("interview", "transcript_segments"),
    ("interview", "reports"),
    ("interview", "report_jobs"),
    ("interview", "recording_artifacts"),
    ("knowledge", "sources"),
    ("knowledge", "source_versions"),
    ("knowledge", "visibility_policies"),
    ("knowledge", "visibility_grants"),
    ("knowledge", "chunks"),
    ("knowledge", "embedding_spaces"),
    ("knowledge", "embeddings"),
    ("knowledge", "citations"),
    ("knowledge", "ingestion_jobs"),
    ("knowledge", "access_snapshots"),
    ("observability", "telemetry_records"),
)
"""@brief 所有 39 个 workspace/owner 租户表 / All 39 workspace/owner tenant tables.

@note ``resume.artifact_blobs`` 由 0002 创建，因而必须纳入本 revision 的完整性边界，
而非只复制 0001 中较早的表清单。
"""


TENANT_PARENT_RELATIONS: tuple[TenantRelation, ...] = (
    (("agent", "messages"), "conversation_id", ("agent", "conversations"), "CASCADE", False),
    (("agent", "runs"), "conversation_id", ("agent", "conversations"), "CASCADE", False),
    (("agent", "runs"), "input_message_id", ("agent", "messages"), "SET NULL", True),
    (("agent", "runs"), "job_id", ("agent", "jobs"), "SET NULL", True),
    (("agent", "run_events"), "run_id", ("agent", "runs"), "CASCADE", False),
    (("agent", "tool_approvals"), "run_id", ("agent", "runs"), "CASCADE", False),
    (("resume", "documents"), "template_version_id", ("resume", "template_versions"), "RESTRICT", False),
    (("resume", "revisions"), "resume_id", ("resume", "documents"), "CASCADE", False),
    (("resume", "operation_batches"), "resume_id", ("resume", "documents"), "CASCADE", False),
    (("resume", "operation_batches"), "idempotency_record_id", ("identity", "idempotency_records"), "SET NULL", True),
    (("resume", "operations"), "batch_id", ("resume", "operation_batches"), "CASCADE", False),
    (("resume", "proposals"), "resume_id", ("resume", "documents"), "CASCADE", False),
    (("resume", "proposals"), "agent_run_id", ("agent", "runs"), "SET NULL", True),
    (("resume", "proposal_operations"), "proposal_id", ("resume", "proposals"), "CASCADE", False),
    (("resume", "render_artifacts"), "resume_id", ("resume", "documents"), "CASCADE", False),
    (("resume", "render_artifacts"), "resume_revision_id", ("resume", "revisions"), "RESTRICT", False),
    (("resume", "artifact_blobs"), "artifact_id", ("resume", "render_artifacts"), "CASCADE", False),
    (("resume", "render_jobs"), "job_id", ("agent", "jobs"), "CASCADE", False),
    (("resume", "render_jobs"), "resume_id", ("resume", "documents"), "CASCADE", False),
    (("resume", "render_jobs"), "resume_revision_id", ("resume", "revisions"), "RESTRICT", False),
    (("resume", "render_jobs"), "artifact_id", ("resume", "render_artifacts"), "SET NULL", True),
    (("resume", "pdf_source_map_entries"), "artifact_id", ("resume", "render_artifacts"), "CASCADE", False),
    (("interview", "sessions"), "scenario_id", ("interview", "scenarios"), "RESTRICT", False),
    (("interview", "sessions"), "resume_revision_id", ("resume", "revisions"), "SET NULL", True),
    (("interview", "events"), "session_id", ("interview", "sessions"), "CASCADE", False),
    (("interview", "transcript_segments"), "session_id", ("interview", "sessions"), "CASCADE", False),
    (("interview", "reports"), "session_id", ("interview", "sessions"), "CASCADE", False),
    (("interview", "report_jobs"), "job_id", ("agent", "jobs"), "CASCADE", False),
    (("interview", "report_jobs"), "session_id", ("interview", "sessions"), "CASCADE", False),
    (("interview", "report_jobs"), "report_id", ("interview", "reports"), "SET NULL", True),
    (("interview", "recording_artifacts"), "session_id", ("interview", "sessions"), "CASCADE", False),
    (("knowledge", "source_versions"), "source_id", ("knowledge", "sources"), "CASCADE", False),
    (("knowledge", "visibility_policies"), "source_id", ("knowledge", "sources"), "CASCADE", False),
    (("knowledge", "visibility_grants"), "policy_id", ("knowledge", "visibility_policies"), "CASCADE", False),
    (("knowledge", "chunks"), "source_version_id", ("knowledge", "source_versions"), "CASCADE", False),
    (("knowledge", "embeddings"), "chunk_id", ("knowledge", "chunks"), "CASCADE", False),
    (("knowledge", "embeddings"), "embedding_space_id", ("knowledge", "embedding_spaces"), "RESTRICT", False),
    (("knowledge", "citations"), "run_id", ("agent", "runs"), "CASCADE", False),
    (("knowledge", "citations"), "chunk_id", ("knowledge", "chunks"), "RESTRICT", False),
    (("knowledge", "ingestion_jobs"), "job_id", ("agent", "jobs"), "CASCADE", False),
    (("knowledge", "ingestion_jobs"), "source_id", ("knowledge", "sources"), "CASCADE", False),
    (("knowledge", "ingestion_jobs"), "source_version_id", ("knowledge", "source_versions"), "SET NULL", True),
    (("knowledge", "access_snapshots"), "agent_run_id", ("agent", "runs"), "CASCADE", True),
    (("knowledge", "access_snapshots"), "interview_session_id", ("interview", "sessions"), "CASCADE", True),
)
"""@brief 所有现有 tenant-to-tenant 单列关系 / All existing tenant-to-tenant single-column relations.

保留原先单列外键，并在此清单中为每个关系添加三列复合外键。多态的
``agent.outbox_events.aggregate_id`` 不是真实关系，故有意不在本清单中。
"""


def _identifier(value: str) -> str:
    """@brief 校验固定 DDL 标识符 / Validate a fixed DDL identifier.

    @param value migration 源码中的 schema、表、列或约束名称。
    @return 已校验的 identifier。
    @raise RuntimeError 标识符不符合 PostgreSQL 安全子集或超过长度限制时抛出。

    @note 所有 DDL 标识符都来自本 revision 的常量，而非配置或请求输入；显式校验仍可
    防止后续编辑把字符串格式化意外变成 SQL 注入入口。
    """
    if len(value) > _POSTGRES_IDENTIFIER_MAX_LENGTH or not _IDENTIFIER_PATTERN.fullmatch(value):
        raise RuntimeError(f"unsafe PostgreSQL identifier in migration: {value!r}")
    return value


def _configured_role(option: str) -> str:
    """@brief 读取并安全引用 dbctl 提供的 PostgreSQL role / Read and quote a dbctl-provided PostgreSQL role.

    @param option dbctl 注入的 Alembic 配置键，例如 ``owner_role``。
    @return 可嵌入固定 DDL 的双引号 role identifier。
    @raise RuntimeError 缺失或非法 role 配置时抛出。

    @note role 名称绝不硬编码：dbctl 已验证并仅通过内存 Alembic Config 传入该配置。
    """
    migration_config = op.get_context().config
    if migration_config is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = migration_config.get_main_option(f"aiws.{option}")
    if not value or not _ROLE_IDENTIFIER_PATTERN.fullmatch(value):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def _quoted_identifier(value: str) -> str:
    """@brief 返回已校验的双引号标识符 / Return a validated double-quoted identifier.

    @param value 固定 PostgreSQL identifier。
    @return 可安全放入静态 ``VALIDATE CONSTRAINT`` SQL 的标识符。
    """
    return '"' + _identifier(value) + '"'


def _qualified_name(reference: TableRef) -> str:
    """@brief 返回 schema-qualified 静态表名 / Return a schema-qualified static table name.

    @param reference ``(schema, table)`` 元组。
    @return 已验证、双引号引用的 ``schema.table`` 名称。
    """
    schema, table = reference
    return f"{_quoted_identifier(schema)}.{_quoted_identifier(table)}"


def _scope_unique_name(reference: TableRef) -> str:
    """@brief 构造租户复合唯一约束名 / Build a tenant composite-unique constraint name.

    @param reference 目标租户表。
    @return 稳定且不超过 PostgreSQL 长度限制的约束名。
    """
    _, table = reference
    return _identifier(f"uq_tnt_{table}_id_ws_owner")


def _workspace_scope_fk_name(reference: TableRef) -> str:
    """@brief 构造工作区根复合外键名 / Build a workspace-root composite foreign-key name.

    @param reference 子租户表。
    @return 稳定且不超过 PostgreSQL 长度限制的约束名。
    """
    _, table = reference
    return _identifier(f"fk_tnt_{table}_workspace_scope")


def _parent_scope_fk_name(child: TableRef, foreign_key_column: str) -> str:
    """@brief 构造 tenant parent 复合外键名 / Build a tenant-parent composite foreign-key name.

    @param child 子租户表。
    @param foreign_key_column 指向父资源的既有单列 id。
    @return 稳定且不超过 PostgreSQL 长度限制的约束名。
    """
    _, table = child
    return _identifier(f"fk_tnt_{table}_{foreign_key_column}_scope")


def _ondelete_for_composite(foreign_key_column: str, on_delete: str, nullable: bool) -> str:
    """@brief 保持原关系删除语义 / Preserve the original relationship delete semantics.

    @param foreign_key_column 子表中可空或不可空的父 id 列。
    @param on_delete 原单列外键的 ``ON DELETE`` 动作。
    @param nullable 该 id 列是否允许 ``NULL``。
    @return 可传给 Alembic 的复合外键 ``ondelete`` 动作。
    @raise RuntimeError 清单出现不一致的删除动作时抛出。

    @note 对 ``SET NULL``，PostgreSQL 默认会置空整个三列复合键；这里限定为只置空
    父 id，避免违反租户边界列的 ``NOT NULL`` 约束。
    """
    if on_delete not in {"CASCADE", "RESTRICT", "SET NULL"}:
        raise RuntimeError(f"unexpected ON DELETE action: {on_delete!r}")
    if on_delete == "SET NULL":
        if not nullable:
            raise RuntimeError("SET NULL tenant relation must have a nullable parent id")
        return f"SET NULL ({_identifier(foreign_key_column)})"
    return on_delete


def _validate_constraints(references: Iterable[tuple[TableRef, str]]) -> None:
    """@brief 分步验证此前 NOT VALID 的外键 / Validate previously NOT VALID foreign keys in stages.

    @param references ``(table, constraint_name)`` 的稳定序列。
    @return 无返回值。

    @note ``VALIDATE CONSTRAINT`` 只能接受静态标识符，故本函数在拼接前再次执行白名单
    校验；所有调用点均使用此 revision 的不可变常量。
    """
    for table, constraint_name in references:
        op.execute(
            sa.text(
                f"ALTER TABLE {_qualified_name(table)} "
                f"VALIDATE CONSTRAINT {_quoted_identifier(constraint_name)}"
            )
        )


def _create_scope_unique_constraints() -> None:
    """@brief 创建复合外键所需的唯一键 / Create unique keys required by composite foreign keys.

    @return 无返回值。

    @note PostgreSQL 不支持 ``UNIQUE NOT VALID``。所有待加唯一键均含已有主键 ``id``，
    因此不可能因现存合法数据发生重复；其索引用于让后续复合外键精确引用租户边界。
    """
    op.create_unique_constraint(
        "uq_workspaces_id_resource_owner",
        "workspaces",
        ["id", "resource_owner_id"],
        schema="identity",
    )
    for schema, table in TENANT_TABLES:
        op.create_unique_constraint(
            _scope_unique_name((schema, table)),
            table,
            ["id", "workspace_id", "resource_owner_id"],
            schema=schema,
        )


def _create_workspace_scope_foreign_keys() -> tuple[tuple[TableRef, str], ...]:
    """@brief 添加所有租户表到 workspace 根的复合外键 / Add tenant-to-workspace-root composite foreign keys.

    @return 待后续 ``VALIDATE`` 的 ``(table, constraint_name)`` 元组。
    """
    validations: list[tuple[TableRef, str]] = []
    for schema, table in TENANT_TABLES:
        reference = (schema, table)
        constraint_name = _workspace_scope_fk_name(reference)
        op.create_foreign_key(
            constraint_name,
            table,
            "workspaces",
            ["workspace_id", "resource_owner_id"],
            ["id", "resource_owner_id"],
            source_schema=schema,
            referent_schema="identity",
            ondelete="RESTRICT",
            match="SIMPLE",
            postgresql_not_valid=True,
        )
        validations.append((reference, constraint_name))
    return tuple(validations)


def _create_tenant_parent_foreign_keys() -> tuple[tuple[TableRef, str], ...]:
    """@brief 添加 tenant-to-tenant 的租户边界复合外键 / Add tenant-boundary composite parent foreign keys.

    @return 待后续 ``VALIDATE`` 的 ``(table, constraint_name)`` 元组。
    """
    validations: list[tuple[TableRef, str]] = []
    for child, foreign_key_column, parent, on_delete, nullable in TENANT_PARENT_RELATIONS:
        child_schema, child_table = child
        parent_schema, parent_table = parent
        constraint_name = _parent_scope_fk_name(child, foreign_key_column)
        op.create_foreign_key(
            constraint_name,
            child_table,
            parent_table,
            [foreign_key_column, "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            source_schema=child_schema,
            referent_schema=parent_schema,
            ondelete=_ondelete_for_composite(foreign_key_column, on_delete, nullable),
            match="SIMPLE",
            postgresql_not_valid=True,
        )
        validations.append((child, constraint_name))
    return tuple(validations)


def _create_telemetry_retention_index() -> None:
    """@brief 创建遥测查询与清理索引 / Create telemetry query and cleanup indexes.

    @return 无返回值。

    @note 此索引支持运维侧按 ``occurred_at`` 的分批删除（例如以配置的保留天数为界）。
    实际清理调度属于 dbctl/运维职责，避免后端请求路径自行触发大范围删除。
    """
    op.create_index(
        "ix_telemetry_records_occurred_at",
        "telemetry_records",
        ["occurred_at"],
        unique=False,
        schema="observability",
    )
    op.create_index(
        "ix_telemetry_records_ws_actor_occurred",
        "telemetry_records",
        ["workspace_id", "actor_id", "occurred_at"],
        unique=False,
        schema="observability",
    )


def _replace_telemetry_owner_policy() -> None:
    """@brief 允许配置化 owner role 执行遥测维护 / Allow the configured owner role to maintain telemetry.

    @return 无返回值。

    @note 0001 中的 policy 仅允许 owner ``SELECT``。表启用 ``FORCE ROW LEVEL
    SECURITY`` 后，该限制会阻止受控保留期清理的 ``DELETE``，即使 owner 是 DDL owner。
    app role 的最小 ``INSERT`` 授权及其租户 RLS policy 不受此处影响。
    """
    owner_role = _configured_role("owner_role")
    op.execute("DROP POLICY workspace_owner_telemetry_view ON observability.telemetry_records")
    op.execute(
        sa.text(
            "CREATE POLICY workspace_owner_telemetry_maintenance "
            "ON observability.telemetry_records AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )
    )


def _add_telemetry_actor_id() -> None:
    """@brief 回填并收紧 telemetry actor_id / Backfill and require telemetry actor_id.

    @return 无返回值。

    @note 写入器已经为新记录提供 actor；历史记录回退到 resource owner，以保留明确且
    不泄露跨租户主体的审计归属。owner maintenance policy 必须先建立，才能在 FORCE
    RLS 下执行此 UPDATE。
    """
    op.add_column(
        "telemetry_records",
        sa.Column("actor_id", sa.String(length=128), nullable=True),
        schema="observability",
    )
    op.execute(
        sa.text(
            "UPDATE observability.telemetry_records "
            "SET actor_id = resource_owner_id WHERE actor_id IS NULL"
        )
    )
    op.alter_column(
        "telemetry_records",
        "actor_id",
        existing_type=sa.String(length=128),
        nullable=False,
        schema="observability",
    )


def upgrade() -> None:
    """@brief 强化跨租户引用完整性并建立遥测保留索引 / Strengthen cross-tenant integrity and telemetry retention indexing.

    @return 无返回值。
    """
    _replace_telemetry_owner_policy()
    _add_telemetry_actor_id()
    _create_scope_unique_constraints()
    workspace_validations = _create_workspace_scope_foreign_keys()
    parent_validations = _create_tenant_parent_foreign_keys()
    _validate_constraints(workspace_validations)
    _validate_constraints(parent_validations)
    _create_telemetry_retention_index()


def downgrade() -> None:
    """@brief 移除本 revision 新增的完整性层 / Remove the integrity layer added by this revision.

    @return 无返回值。

    @note 先删除所有引用复合唯一键的外键，再删除唯一键，确保 downgrade 不依赖
    ``CASCADE``，不会意外移除其他版本或人工创建的对象。
    """
    op.drop_index(
        "ix_telemetry_records_ws_actor_occurred",
        table_name="telemetry_records",
        schema="observability",
    )
    op.drop_index(
        "ix_telemetry_records_occurred_at",
        table_name="telemetry_records",
        schema="observability",
    )
    for child, foreign_key_column, _parent, _on_delete, _nullable in reversed(TENANT_PARENT_RELATIONS):
        child_schema, child_table = child
        op.drop_constraint(
            _parent_scope_fk_name(child, foreign_key_column),
            child_table,
            type_="foreignkey",
            schema=child_schema,
        )
    for schema, table in reversed(TENANT_TABLES):
        op.drop_constraint(
            _workspace_scope_fk_name((schema, table)),
            table,
            type_="foreignkey",
            schema=schema,
        )
    for schema, table in reversed(TENANT_TABLES):
        op.drop_constraint(
            _scope_unique_name((schema, table)),
            table,
            type_="unique",
            schema=schema,
        )
    op.drop_constraint(
        "uq_workspaces_id_resource_owner",
        "workspaces",
        type_="unique",
        schema="identity",
    )
    op.drop_column("telemetry_records", "actor_id", schema="observability")
    owner_role = _configured_role("owner_role")
    op.execute("DROP POLICY workspace_owner_telemetry_maintenance ON observability.telemetry_records")
    op.execute(
        sa.text(
            "CREATE POLICY workspace_owner_telemetry_view "
            "ON observability.telemetry_records AS PERMISSIVE FOR SELECT "
            f"TO {owner_role} USING (true)"
        )
    )
