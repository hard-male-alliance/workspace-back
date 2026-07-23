"""@brief 活动 Job 的可证明取消补偿快照 / Provable cancellation snapshots for active Jobs.

Revision ID: 20260723_0026
Revises: 20260723_0025
Create Date: 2026-07-23

Generic Job cancellation must restore the domain aggregate from the immutable state captured when
the Job was queued.  Older Knowledge/Connection Jobs predate those ``previous_*`` fields.  This
revision validates every active binding against its aggregate row, then fills only absent fields
with conservative values.  A per-row marker records the exact JSON members inserted here so the
downgrade can remove precisely those members and nothing supplied by the application.
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op

revision = "20260723_0026"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0025"
"""@brief 前驱账户删除执行 revision / Preceding account-deletion revision."""

branch_labels = None
"""@brief 无分支标签 / No branch labels."""

depends_on = None
"""@brief 无额外依赖 / No additional dependencies."""

RuntimeRoleOption = Literal["owner_role"]
"""@brief 本迁移读取的 dbctl role 选项 / dbctl role option read by this migration."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role identifier 白名单 / PostgreSQL role-identifier allowlist."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL identifier 最大字节数 / Maximum PostgreSQL identifier size."""

_MIGRATION_POLICY = "job_cancellation_owner_0026"
"""@brief FORCE-RLS 表上的临时 owner policy / Temporary owner policy on forced-RLS tables."""

_MARKER = "_migration_0026"
"""@brief 逐行记录本迁移新增 JSON members 的 marker / Per-row marker for JSON members inserted here."""

_MARKER_REASON = "generic_job_cancellation_compensation"
"""@brief marker 的稳定语义标签 / Stable semantic label stored in each marker."""

_ACTIVE_KINDS = (
    "connection.revoke",
    "knowledge.delete",
    "knowledge.ingest",
    "knowledge.sync",
)
"""@brief 需要领域补偿的活动 Job kind 闭集 / Closed active Job-kind set requiring domain compensation."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回安全引用的 schema-owner role / Return a safely quoted schema-owner role.

    @param option dbctl role 配置键 / dbctl role configuration key.
    @return 可安全插入固定 DDL 的引用 role / Quoted role safe for static DDL.
    @raise RuntimeError 配置缺失或非法时抛出 / Raised for missing or invalid configuration.
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


def _active_job_predicate() -> str:
    """@brief 返回 migration-owned 活动 Job SQL predicate / Return the migration-owned active-Job SQL predicate.

    @return 仅由本模块常量构成的 SQL predicate / SQL predicate composed only from module constants.
    """

    kinds = ", ".join(f"'{kind}'" for kind in _ACTIVE_KINDS)
    return f"status IN ('queued', 'running') AND job_type IN ({kinds})"


def _install_upgrade_visibility(owner_role: str) -> None:
    """@brief 为冻结、验证和回填安装最小临时可见性 / Install minimal temporary visibility for locking, validation, and backfill.

    @param owner_role 已安全引用的 schema-owner role / Safely quoted schema-owner role.
    @return 无返回值 / No return value.
    @note Knowledge 表 policy 只暴露合法活动态；错误状态会在 LEFT JOIN 中表现为缺失并被
        preflight 拒绝。/ Knowledge-table policies expose only legal active states; an invalid
        state appears missing to the LEFT JOIN and is rejected by preflight.
    """

    active = _active_job_predicate()
    op.execute(
        f"CREATE POLICY {_MIGRATION_POLICY} ON agent.jobs AS PERMISSIVE FOR ALL "
        f"TO {owner_role} USING (({active}) OR (jsonb_typeof(request_payload) = 'object' "
        f"AND request_payload ? '{_MARKER}')) WITH CHECK (({active}) OR "
        f"(jsonb_typeof(request_payload) = 'object' AND request_payload ? '{_MARKER}'))"
    )
    op.execute(
        f"CREATE POLICY {_MIGRATION_POLICY} ON knowledge.connections AS PERMISSIVE FOR SELECT "
        f"TO {owner_role} USING (status = 'revoking')"
    )
    op.execute(
        f"CREATE POLICY {_MIGRATION_POLICY} ON knowledge.sources AS PERMISSIVE FOR SELECT "
        f"TO {owner_role} USING (ingestion_state IN ("
        "'queued', 'fetching', 'parsing', 'chunking', 'embedding', 'deleting'))"
    )


def _remove_upgrade_visibility() -> None:
    """@brief 移除 upgrade 临时可见性 / Remove temporary upgrade visibility.

    @return 无返回值 / No return value.
    """

    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON knowledge.sources")
    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON knowledge.connections")
    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON agent.jobs")


def _install_downgrade_visibility(owner_role: str) -> None:
    """@brief downgrade 仅暴露带有本 revision marker 的 Job / Expose only Jobs carrying this revision's marker during downgrade.

    @param owner_role 已安全引用的 schema-owner role / Safely quoted schema-owner role.
    @return 无返回值 / No return value.
    """

    marked = (
        f"jsonb_typeof(request_payload) = 'object' AND request_payload ? '{_MARKER}'"
    )
    restored = (
        f"jsonb_typeof(request_payload) = 'object' AND NOT (request_payload ? '{_MARKER}') "
        "AND job_type IN ('connection.revoke', 'knowledge.delete', "
        "'knowledge.ingest', 'knowledge.sync')"
    )
    visible = f"({marked}) OR ({restored})"
    op.execute(
        f"CREATE POLICY {_MIGRATION_POLICY} ON agent.jobs AS PERMISSIVE FOR ALL "
        f"TO {owner_role} USING ({visible}) WITH CHECK ({visible})"
    )


def _remove_downgrade_visibility() -> None:
    """@brief 移除 downgrade 临时可见性 / Remove temporary downgrade visibility.

    @return 无返回值 / No return value.
    """

    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON agent.jobs")


def _lock_evidence() -> None:
    """@brief 冻结 Job 与补偿证据，防止 preflight/backfill 间竞态 / Freeze Jobs and compensation evidence across preflight and backfill.

    @return 无返回值 / No return value.
    @note ``SHARE ROW EXCLUSIVE`` 允许在线读取但阻止三张表的并发写入；迁移因此不会把
        不同 transaction 时刻拼成一个伪造快照。/ ``SHARE ROW EXCLUSIVE`` allows reads but
        blocks concurrent writes to all three tables, preventing a synthetic mixed-time snapshot.
    """

    op.execute(
        "LOCK TABLE agent.jobs, knowledge.connections, knowledge.sources "
        "IN SHARE ROW EXCLUSIVE MODE"
    )


def _count(statement: str) -> int:
    """@brief 执行本模块静态 count SQL / Execute a static count query from this module.

    @param statement 不含外部输入的 SQL / SQL containing no external input.
    @return 非负 count / Non-negative count.
    """

    value = op.get_bind().scalar(sa.text(statement))
    return int(value or 0)


def _preflight_marker_namespace() -> None:
    """@brief 在写入前证明 marker namespace 未被占用 / Prove the marker namespace is unused before writing.

    @return 无返回值 / No return value.
    @raise RuntimeError 任意 Job 已占用 marker 时抛出 / Raised when any Job already owns the marker.
    """

    collisions = _count(
        f"SELECT count(*) FROM agent.jobs WHERE request_payload ? '{_MARKER}'"
    )
    if collisions:
        raise RuntimeError("0026 Job cancellation migration marker already exists")


def _preflight_bindings() -> None:
    """@brief 验证 kind、payload subject、spec、target 与聚合事实行 / Validate kind, payload subject, spec, target, and aggregate truth rows.

    @return 无返回值 / No return value.
    @raise RuntimeError 任一活动 Job 无法精确绑定时抛出 / Raised when any active Job cannot be bound exactly.
    """

    duplicates = _count(
        r"""
        SELECT count(*)
        FROM (
            SELECT job.workspace_id, job.target_resource_type, job.target_resource_id
            FROM agent.jobs AS job
            WHERE job.status IN ('queued', 'running')
              AND job.job_type IN (
                  'connection.revoke', 'knowledge.delete', 'knowledge.ingest', 'knowledge.sync'
              )
            GROUP BY job.workspace_id, job.target_resource_type, job.target_resource_id
            HAVING count(*) > 1
        ) AS duplicate
        """
    )
    invalid = _count(
        r"""
        SELECT count(*)
        FROM agent.jobs AS job
        LEFT JOIN knowledge.connections AS connection
          ON job.job_type = 'connection.revoke'
         AND connection.id = job.target_resource_id
         AND connection.workspace_id = job.workspace_id
        LEFT JOIN knowledge.sources AS source
          ON job.job_type IN ('knowledge.delete', 'knowledge.ingest', 'knowledge.sync')
         AND source.id = job.target_resource_id
         AND source.workspace_id = job.workspace_id
        WHERE job.status IN ('queued', 'running')
          AND job.job_type IN (
              'connection.revoke', 'knowledge.delete', 'knowledge.ingest', 'knowledge.sync'
          )
          AND (
              jsonb_typeof(job.request_payload) IS DISTINCT FROM 'object'
              OR jsonb_typeof(job.request_payload -> 'subject') IS DISTINCT FROM 'object'
              OR jsonb_typeof(job.request_payload -> 'spec') IS DISTINCT FROM 'object'
              OR job.target_resource_revision IS NULL
              OR job.request_payload -> 'subject' ->> 'resource_type'
                 IS DISTINCT FROM job.target_resource_type
              OR job.request_payload -> 'subject' ->> 'id'
                 IS DISTINCT FROM job.target_resource_id
              OR job.request_payload -> 'subject' -> 'revision'
                 IS DISTINCT FROM to_jsonb(job.target_resource_revision)
              OR CASE job.job_type
                  WHEN 'connection.revoke' THEN
                      job.target_resource_type <> 'connection'
                      OR connection.id IS NULL
                      OR connection.status <> 'revoking'
                      OR connection.problem IS NOT NULL
                      OR connection.revision < job.target_resource_revision
                      OR job.request_payload -> 'spec' ->> 'connection_id'
                         IS DISTINCT FROM job.target_resource_id
                      OR job.request_payload -> 'spec' ->> 'credential_reference'
                         IS DISTINCT FROM connection.credential_reference
                  WHEN 'knowledge.delete' THEN
                      job.target_resource_type <> 'knowledge_source'
                      OR source.id IS NULL
                      OR source.enabled IS DISTINCT FROM false
                      OR source.ingestion_state <> 'deleting'
                      OR source.revision < job.target_resource_revision
                      OR job.request_payload -> 'spec' ->> 'source_id'
                         IS DISTINCT FROM job.target_resource_id
                      OR job.request_payload -> 'spec' -> 'source_revision'
                         IS DISTINCT FROM to_jsonb(job.target_resource_revision)
                  ELSE
                      job.target_resource_type <> 'knowledge_source'
                      OR source.id IS NULL
                      OR source.enabled IS DISTINCT FROM true
                      OR source.ingestion_state NOT IN (
                          'queued', 'fetching', 'parsing', 'chunking', 'embedding'
                      )
                      OR source.revision < job.target_resource_revision
                      OR job.request_payload -> 'spec' ->> 'source_id'
                         IS DISTINCT FROM job.target_resource_id
                      OR job.request_payload -> 'spec' -> 'source_revision'
                         IS DISTINCT FROM to_jsonb(job.target_resource_revision)
                      OR job.request_payload -> 'spec' ->> 'requested_by'
                         IS DISTINCT FROM job.resource_owner_id
                      OR jsonb_typeof(job.request_payload -> 'spec' -> 'force')
                         IS DISTINCT FROM 'boolean'
                      OR NOT (job.request_payload -> 'spec' ? 'version_id')
                      OR jsonb_typeof(job.request_payload -> 'spec' -> 'version_id')
                         NOT IN ('string', 'null')
                      OR (
                          jsonb_typeof(job.request_payload -> 'spec' -> 'version_id') = 'string'
                          AND job.request_payload -> 'spec' ->> 'version_id'
                              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
                      )
                      OR (
                          job.job_type = 'knowledge.sync'
                          AND source.source_type NOT IN (
                              'url', 'website', 'blog_feed', 'git_repository', 'resume',
                              'cloud_drive'
                          )
                      )
                 END
          )
        """
    )
    if invalid or duplicates:
        raise RuntimeError(
            "0026 found active Knowledge/Connection Jobs with invalid kind, subject, spec, "
            "workspace, target, aggregate-state, or unique-active-work binding"
        )


def _preflight_snapshots() -> None:
    """@brief 验证已有 previous 字段，缺失字段使用 fail-closed 有效值 / Validate existing previous fields with fail-closed effective values for missing members.

    @return 无返回值 / No return value.
    @raise RuntimeError 显式快照损坏或互相矛盾时抛出 / Raised for corrupt or contradictory explicit snapshots.
    """

    invalid = _count(
        r"""
        SELECT count(*)
        FROM agent.jobs AS job
        WHERE job.status IN ('queued', 'running')
          AND job.job_type IN (
              'connection.revoke', 'knowledge.delete', 'knowledge.ingest', 'knowledge.sync'
          )
          AND CASE
              WHEN job.job_type = 'connection.revoke' THEN
                  (
                      job.request_payload -> 'spec' ? 'previous_status'
                      AND (
                          jsonb_typeof(
                              job.request_payload -> 'spec' -> 'previous_status'
                          ) IS DISTINCT FROM 'string'
                          OR job.request_payload -> 'spec' ->> 'previous_status'
                             NOT IN ('active', 'reauthorization_required', 'failed')
                      )
                  )
                  OR (
                      job.request_payload -> 'spec' ? 'previous_problem'
                      AND jsonb_typeof(
                          job.request_payload -> 'spec' -> 'previous_problem'
                      ) NOT IN ('object', 'null')
                  )
                  OR CASE COALESCE(
                      job.request_payload -> 'spec' ->> 'previous_status',
                      'reauthorization_required'
                  )
                      WHEN 'failed' THEN jsonb_typeof(
                          job.request_payload -> 'spec' -> 'previous_problem'
                      ) IS DISTINCT FROM 'object'
                      ELSE COALESCE(jsonb_typeof(
                          job.request_payload -> 'spec' -> 'previous_problem'
                      ), 'null') <> 'null'
                     END
              ELSE
                  (
                      job.request_payload -> 'spec' ? 'previous_ingestion_status'
                      AND (
                          jsonb_typeof(
                              job.request_payload -> 'spec' -> 'previous_ingestion_status'
                          ) IS DISTINCT FROM 'string'
                          OR job.request_payload -> 'spec' ->> 'previous_ingestion_status'
                             NOT IN ('not_started', 'ready', 'stale', 'failed')
                      )
                  )
                  OR (
                      job.request_payload -> 'spec' ? 'previous_problem'
                      AND jsonb_typeof(
                          job.request_payload -> 'spec' -> 'previous_problem'
                      ) NOT IN ('object', 'null')
                  )
                  OR CASE COALESCE(
                      job.request_payload -> 'spec' ->> 'previous_ingestion_status',
                      'not_started'
                  )
                      WHEN 'failed' THEN jsonb_typeof(
                          job.request_payload -> 'spec' -> 'previous_problem'
                      ) IS DISTINCT FROM 'object'
                      ELSE COALESCE(jsonb_typeof(
                          job.request_payload -> 'spec' -> 'previous_problem'
                      ), 'null') <> 'null'
                     END
                  OR (
                      job.job_type = 'knowledge.delete'
                      AND job.request_payload -> 'spec' ? 'previous_enabled'
                      AND jsonb_typeof(
                          job.request_payload -> 'spec' -> 'previous_enabled'
                      ) IS DISTINCT FROM 'boolean'
                  )
             END
        """
    )
    if invalid:
        raise RuntimeError(
            "0026 found corrupt or contradictory explicit cancellation snapshots"
        )


def _rows_requiring_backfill() -> int:
    """@brief 统计缺少补偿字段的活动 Job / Count active Jobs missing compensation fields.

    @return 将被本 revision 标记的行数 / Number of rows that this revision will mark.
    """

    return _count(
        r"""
        SELECT count(*)
        FROM agent.jobs AS job
        WHERE job.status IN ('queued', 'running')
          AND (
              (
                  job.job_type = 'connection.revoke'
                  AND (
                      NOT (job.request_payload -> 'spec' ? 'previous_status')
                      OR NOT (job.request_payload -> 'spec' ? 'previous_problem')
                  )
              )
              OR (
                  job.job_type IN ('knowledge.ingest', 'knowledge.sync')
                  AND (
                      NOT (job.request_payload -> 'spec' ? 'previous_ingestion_status')
                      OR NOT (job.request_payload -> 'spec' ? 'previous_problem')
                  )
              )
              OR (
                  job.job_type = 'knowledge.delete'
                  AND (
                      NOT (job.request_payload -> 'spec' ? 'previous_enabled')
                      OR NOT (job.request_payload -> 'spec' ? 'previous_ingestion_status')
                      OR NOT (job.request_payload -> 'spec' ? 'previous_problem')
                  )
              )
          )
        """
    )


def _backfill_connection_revoke() -> None:
    """@brief 缺失的 Connection 前态保守回填 reauthorization_required / Backfill a missing Connection prior state as reauthorization_required.

    @return 无返回值 / No return value.
    """

    op.execute(
        sa.text(
            f"""
            WITH candidates AS (
                SELECT job.id,
                       (CASE
                            WHEN NOT (job.request_payload -> 'spec' ? 'previous_status')
                            THEN jsonb_build_object(
                                'previous_status', 'reauthorization_required'
                            )
                            ELSE '{{}}'::jsonb
                        END)
                       ||
                       (CASE
                            WHEN NOT (job.request_payload -> 'spec' ? 'previous_problem')
                            THEN jsonb_build_object('previous_problem', NULL)
                            ELSE '{{}}'::jsonb
                        END) AS added_spec
                FROM agent.jobs AS job
                JOIN knowledge.connections AS connection
                  ON connection.id = job.target_resource_id
                 AND connection.workspace_id = job.workspace_id
                WHERE job.status IN ('queued', 'running')
                  AND job.job_type = 'connection.revoke'
                  AND (
                      NOT (job.request_payload -> 'spec' ? 'previous_status')
                      OR NOT (job.request_payload -> 'spec' ? 'previous_problem')
                  )
            )
            UPDATE agent.jobs AS job
            SET request_payload = jsonb_set(
                jsonb_set(
                    job.request_payload,
                    '{{spec}}',
                    (job.request_payload -> 'spec') || candidate.added_spec,
                    false
                ),
                '{{{_MARKER}}}',
                jsonb_build_object(
                    'reason', :reason,
                    'added_spec', candidate.added_spec
                ),
                true
            )
            FROM candidates AS candidate
            WHERE job.id = candidate.id
            """
        ).bindparams(reason=_MARKER_REASON)
    )


def _backfill_knowledge_process() -> None:
    """@brief 为 ingest/sync 生成保守稳定恢复点 / Generate conservative stable recovery points for ingest/sync.

    @return 无返回值 / No return value.
    """

    op.execute(
        sa.text(
            f"""
            WITH candidates AS (
                SELECT job.id,
                       (CASE
                            WHEN NOT (
                                job.request_payload -> 'spec'
                                ? 'previous_ingestion_status'
                            )
                            THEN jsonb_build_object(
                                'previous_ingestion_status',
                                CASE
                                    WHEN source.current_version_id IS NOT NULL
                                     AND source.last_success_at IS NOT NULL
                                    THEN 'stale'
                                    ELSE 'not_started'
                                END
                            )
                            ELSE '{{}}'::jsonb
                        END)
                       ||
                       (CASE
                            WHEN NOT (job.request_payload -> 'spec' ? 'previous_problem')
                            THEN jsonb_build_object('previous_problem', NULL)
                            ELSE '{{}}'::jsonb
                        END) AS added_spec
                FROM agent.jobs AS job
                JOIN knowledge.sources AS source
                  ON source.id = job.target_resource_id
                 AND source.workspace_id = job.workspace_id
                WHERE job.status IN ('queued', 'running')
                  AND job.job_type IN ('knowledge.ingest', 'knowledge.sync')
                  AND (
                      NOT (
                          job.request_payload -> 'spec' ? 'previous_ingestion_status'
                      )
                      OR NOT (job.request_payload -> 'spec' ? 'previous_problem')
                  )
            )
            UPDATE agent.jobs AS job
            SET request_payload = jsonb_set(
                jsonb_set(
                    job.request_payload,
                    '{{spec}}',
                    (job.request_payload -> 'spec') || candidate.added_spec,
                    false
                ),
                '{{{_MARKER}}}',
                jsonb_build_object(
                    'reason', :reason,
                    'added_spec', candidate.added_spec
                ),
                true
            )
            FROM candidates AS candidate
            WHERE job.id = candidate.id
            """
        ).bindparams(reason=_MARKER_REASON)
    )


def _backfill_knowledge_delete() -> None:
    """@brief 为 delete 生成不重新授权的保守恢复点 / Generate conservative delete recovery without re-enabling access.

    @return 无返回值 / No return value.
    @note ``previous_enabled=false`` 是唯一可证明不提升权限的 legacy fallback；当前
        ``deleting`` 行已被禁用，无法诚实推断删除前是否启用。/ ``previous_enabled=false``
        is the only legacy fallback proven not to elevate access; a current ``deleting`` row is
        already disabled and cannot reveal whether it was enabled before deletion.
    """

    op.execute(
        sa.text(
            f"""
            WITH candidates AS (
                SELECT job.id,
                       (CASE
                            WHEN NOT (job.request_payload -> 'spec' ? 'previous_enabled')
                            THEN jsonb_build_object('previous_enabled', false)
                            ELSE '{{}}'::jsonb
                        END)
                       ||
                       (CASE
                            WHEN NOT (
                                job.request_payload -> 'spec'
                                ? 'previous_ingestion_status'
                            )
                            THEN jsonb_build_object(
                                'previous_ingestion_status',
                                CASE
                                    WHEN source.current_version_id IS NOT NULL
                                     AND source.last_success_at IS NOT NULL
                                    THEN 'stale'
                                    ELSE 'not_started'
                                END
                            )
                            ELSE '{{}}'::jsonb
                        END)
                       ||
                       (CASE
                            WHEN NOT (job.request_payload -> 'spec' ? 'previous_problem')
                            THEN jsonb_build_object('previous_problem', NULL)
                            ELSE '{{}}'::jsonb
                        END) AS added_spec
                FROM agent.jobs AS job
                JOIN knowledge.sources AS source
                  ON source.id = job.target_resource_id
                 AND source.workspace_id = job.workspace_id
                WHERE job.status IN ('queued', 'running')
                  AND job.job_type = 'knowledge.delete'
                  AND (
                      NOT (job.request_payload -> 'spec' ? 'previous_enabled')
                      OR NOT (
                          job.request_payload -> 'spec' ? 'previous_ingestion_status'
                      )
                      OR NOT (job.request_payload -> 'spec' ? 'previous_problem')
                  )
            )
            UPDATE agent.jobs AS job
            SET request_payload = jsonb_set(
                jsonb_set(
                    job.request_payload,
                    '{{spec}}',
                    (job.request_payload -> 'spec') || candidate.added_spec,
                    false
                ),
                '{{{_MARKER}}}',
                jsonb_build_object(
                    'reason', :reason,
                    'added_spec', candidate.added_spec
                ),
                true
            )
            FROM candidates AS candidate
            WHERE job.id = candidate.id
            """
        ).bindparams(reason=_MARKER_REASON)
    )


def _preflight_downgrade_markers() -> None:
    """@brief 证明 marker 仍与本迁移写入的 exact values 一致 / Prove every marker still matches the exact values inserted here.

    @return 无返回值 / No return value.
    @raise RuntimeError marker 或被标记字段被篡改时抛出 / Raised when a marker or marked value was changed.
    """

    invalid = _count(
        f"""
        SELECT count(*)
        FROM agent.jobs AS job
        WHERE job.request_payload ? '{_MARKER}'
          AND (
              jsonb_typeof(job.request_payload) IS DISTINCT FROM 'object'
              OR jsonb_typeof(job.request_payload -> 'spec') IS DISTINCT FROM 'object'
              OR jsonb_typeof(job.request_payload -> '{_MARKER}') IS DISTINCT FROM 'object'
              OR job.request_payload -> '{_MARKER}' ->> 'reason'
                 IS DISTINCT FROM '{_MARKER_REASON}'
              OR jsonb_typeof(
                  job.request_payload -> '{_MARKER}' -> 'added_spec'
              ) IS DISTINCT FROM 'object'
              OR job.request_payload -> '{_MARKER}' -> 'added_spec' = '{{}}'::jsonb
              OR job.job_type NOT IN (
                  'connection.revoke', 'knowledge.delete', 'knowledge.ingest', 'knowledge.sync'
              )
              OR job.target_resource_type <> CASE
                  WHEN job.job_type = 'connection.revoke' THEN 'connection'
                  ELSE 'knowledge_source'
                 END
              OR jsonb_typeof(job.request_payload -> 'subject') IS DISTINCT FROM 'object'
              OR job.request_payload -> 'subject' ->> 'resource_type'
                 IS DISTINCT FROM job.target_resource_type
              OR job.request_payload -> 'subject' ->> 'id'
                 IS DISTINCT FROM job.target_resource_id
              OR job.request_payload -> 'subject' -> 'revision'
                 IS DISTINCT FROM to_jsonb(job.target_resource_revision)
              OR CASE
                  WHEN job.job_type = 'connection.revoke' THEN
                      job.request_payload -> 'spec' ->> 'connection_id'
                      IS DISTINCT FROM job.target_resource_id
                  ELSE
                      job.request_payload -> 'spec' ->> 'source_id'
                      IS DISTINCT FROM job.target_resource_id
                      OR job.request_payload -> 'spec' -> 'source_revision'
                         IS DISTINCT FROM to_jsonb(job.target_resource_revision)
                 END
              OR EXISTS (
                  SELECT 1
                  FROM jsonb_object_keys(
                      CASE
                          WHEN jsonb_typeof(
                              job.request_payload -> '{_MARKER}' -> 'added_spec'
                          ) = 'object'
                          THEN job.request_payload -> '{_MARKER}' -> 'added_spec'
                          ELSE '{{}}'::jsonb
                      END
                  ) AS added(key)
                  WHERE CASE job.job_type
                      WHEN 'connection.revoke' THEN added.key <> ALL(
                          ARRAY['previous_status', 'previous_problem']::text[]
                      )
                      WHEN 'knowledge.delete' THEN added.key <> ALL(
                          ARRAY[
                              'previous_enabled', 'previous_ingestion_status',
                              'previous_problem'
                          ]::text[]
                      )
                      ELSE added.key <> ALL(
                          ARRAY['previous_ingestion_status', 'previous_problem']::text[]
                      )
                     END
              )
              OR EXISTS (
                  SELECT 1
                  FROM jsonb_object_keys(
                      CASE
                          WHEN jsonb_typeof(
                              job.request_payload -> '{_MARKER}' -> 'added_spec'
                          ) = 'object'
                          THEN job.request_payload -> '{_MARKER}' -> 'added_spec'
                          ELSE '{{}}'::jsonb
                      END
                  ) AS added(key)
                  WHERE NOT (job.request_payload -> 'spec' ? added.key)
                     OR job.request_payload -> 'spec' -> added.key
                        IS DISTINCT FROM
                        job.request_payload -> '{_MARKER}' -> 'added_spec' -> added.key
              )
              OR (
                  job.request_payload -> '{_MARKER}' -> 'added_spec'
                      ? 'previous_status'
                  AND job.request_payload -> '{_MARKER}' -> 'added_spec'
                      ->> 'previous_status' <> 'reauthorization_required'
              )
              OR (
                  job.request_payload -> '{_MARKER}' -> 'added_spec'
                      ? 'previous_ingestion_status'
                  AND job.request_payload -> '{_MARKER}' -> 'added_spec'
                      ->> 'previous_ingestion_status' NOT IN ('not_started', 'stale')
              )
              OR (
                  job.request_payload -> '{_MARKER}' -> 'added_spec'
                      ? 'previous_problem'
                  AND jsonb_typeof(
                      job.request_payload -> '{_MARKER}' -> 'added_spec'
                      -> 'previous_problem'
                  ) <> 'null'
              )
              OR (
                  job.request_payload -> '{_MARKER}' -> 'added_spec'
                      ? 'previous_enabled'
                  AND job.request_payload -> '{_MARKER}' -> 'added_spec'
                      -> 'previous_enabled' <> 'false'::jsonb
              )
          )
        """
    )
    if invalid:
        raise RuntimeError(
            "cannot downgrade 0026 because a migration marker or inserted snapshot value changed"
        )


def _verify_upgrade(expected_markers: int) -> None:
    """@brief 证明全部活动行完整且 marker 数量守恒 / Prove every active row is complete and marker cardinality is conserved.

    @param expected_markers upgrade 前缺字段行数 / Number of rows missing fields before upgrade.
    @return 无返回值 / No return value.
    @raise RuntimeError 回填不完整或写入范围漂移时抛出 / Raised for incomplete or over-broad backfill.
    """

    remaining = _rows_requiring_backfill()
    markers = _count(
        f"SELECT count(*) FROM agent.jobs WHERE request_payload ? '{_MARKER}'"
    )
    if remaining or markers != expected_markers:
        raise RuntimeError("0026 cancellation-snapshot backfill did not preserve exact row scope")
    _preflight_snapshots()
    _preflight_downgrade_markers()


def _remove_marked_fields() -> None:
    """@brief 仅删除 marker 列出的 spec members 与 marker 本身 / Remove only marker-listed spec members and the marker itself.

    @return 无返回值 / No return value.
    """

    op.execute(
        sa.text(
            f"""
            WITH migrated AS (
                SELECT job.id,
                       ARRAY(
                           SELECT added.key
                           FROM jsonb_object_keys(
                               job.request_payload -> '{_MARKER}' -> 'added_spec'
                           ) AS added(key)
                           ORDER BY added.key
                       ) AS added_fields
                FROM agent.jobs AS job
                WHERE job.request_payload ? '{_MARKER}'
            )
            UPDATE agent.jobs AS job
            SET request_payload = jsonb_set(
                job.request_payload - '{_MARKER}',
                '{{spec}}',
                (job.request_payload -> 'spec') - migrated.added_fields,
                false
            )
            FROM migrated
            WHERE job.id = migrated.id
            """
        )
    )


def upgrade() -> None:
    """@brief 验证并回填活动 Job 的取消补偿快照 / Validate and backfill cancellation snapshots for active Jobs.

    @return 无返回值 / No return value.
    """

    owner_role = _configured_role("owner_role")
    _install_upgrade_visibility(owner_role)
    _lock_evidence()
    _preflight_marker_namespace()
    _preflight_bindings()
    _preflight_snapshots()
    expected_markers = _rows_requiring_backfill()
    _backfill_connection_revoke()
    _backfill_knowledge_process()
    _backfill_knowledge_delete()
    _verify_upgrade(expected_markers)
    _remove_upgrade_visibility()


def downgrade() -> None:
    """@brief 精确移除 0026 插入的 JSON members / Precisely remove JSON members inserted by 0026.

    @return 无返回值 / No return value.
    @raise RuntimeError marker 证据已漂移时拒绝有损回退 / Refuses a lossy downgrade when marker evidence drifted.
    """

    owner_role = _configured_role("owner_role")
    _install_downgrade_visibility(owner_role)
    op.execute("LOCK TABLE agent.jobs IN SHARE ROW EXCLUSIVE MODE")
    _preflight_downgrade_markers()
    _remove_marked_fields()
    if _count(f"SELECT count(*) FROM agent.jobs WHERE request_payload ? '{_MARKER}'"):
        raise RuntimeError("0026 downgrade left migration markers behind")
    _remove_downgrade_visibility()
