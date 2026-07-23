"""@brief 可审计演进既有 Resume 数据为 API V2 持久化模型 / Auditably evolve legacy Resume data into API V2 persistence.

Revision ID: 20260723_0016
Revises: 20260723_0015
Create Date: 2026-07-23

V1 异构 item/富文本/operation 与 V2 规范化 SIR 不是完全同构。本 revision 因此先将
每个源行完整封存到带版本与 SHA-256 的 append-only archive，再执行内嵌、确定性的
expand → backfill → validate → constrain 转换。无法安全继续的异常只指向具体表、行与字段；
不可表示的 V1 字段仍在只读 archive 中保留，不会静默丢弃。
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlsplit

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import RowMapping

revision = "20260723_0016"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0015"
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

_MIGRATION_POLICY = "resume_v2_owner_migration_0016"
"""@brief 事务内 owner 可见性 policy / Transaction-local owner-visibility policy."""

_CONVERTER_VERSION = "resume-v1-to-v2/1"
"""@brief 冻结在 revision 内的 converter 版本 / Converter version frozen into this revision."""

_ARCHIVE_TABLE = "resume.v1_migration_archive"
"""@brief 只读 V1 原始行封存表 / Read-only archive of original V1 rows."""

_ARCHIVE_TRIGGER = "resume_v1_migration_archive_append_only"
"""@brief 封存表 append-only trigger / Append-only trigger for the archive."""

_AUDIT_MIGRATION_ID = "20260723_0016_resume_v1_to_v2"
"""@brief 迁移审计 ledger 稳定 ID / Stable migration-audit ledger ID."""

_OPAQUE_ID_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API V2 opaque ID 语法 / API V2 opaque-ID grammar."""

_LOCALE_PATTERN = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")
"""@brief API V2 locale 语法 / API V2 locale grammar."""

_FIELD_PART_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
"""@brief API V2 operation field-path 语法 / API V2 operation field-path grammar."""

_RESUME_TABLES = (
    "resume.documents",
    "resume.revisions",
    "resume.operation_batches",
    "resume.operations",
    "resume.proposals",
    "resume.proposal_operations",
)
"""@brief 必须显式转换而不能猜测的既有 Resume 表 / Existing Resume tables requiring explicit conversion."""

_PREFLIGHT_TABLES = (
    "resume.template_versions",
    *_RESUME_TABLES,
    "agent.jobs",
    "agent.outbox_events",
)
"""@brief 0016 preflight 读取的 FORCE RLS 表 allowlist / FORCE-RLS table allowlist read by preflight."""

_ARCHIVED_SOURCES = (*_RESUME_TABLES, "agent.jobs", "agent.outbox_events")
"""@brief converter 可封存的固定 relation allowlist / Fixed relation allowlist archived by the converter."""

_JOB_REFERENCES = (
    ("agent", "runs", "job_id"),
    ("resume", "render_jobs", "job_id"),
    ("interview", "report_jobs", "job_id"),
    ("knowledge", "ingestion_jobs", "job_id"),
)
"""@brief 引用 agent.jobs 的既有 FK 列 / Existing FK columns referencing agent.jobs."""

_RESUME_REFERENCES = (
    ("resume", "revisions", "resume_id"),
    ("resume", "operation_batches", "resume_id"),
    ("resume", "proposals", "resume_id"),
    ("resume", "render_artifacts", "resume_id"),
    ("resume", "render_jobs", "resume_id"),
)
"""@brief 引用 resume.documents 的既有 FK 列 / Existing FK columns referencing resume.documents."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回安全引用的 runtime role / Return a safely quoted runtime role.

    @param option Alembic ``aiws.*`` role option / Alembic ``aiws.*`` role option.
    @return 双引号引用 role / Double-quoted role.
    @raise RuntimeError 配置缺失或不安全时抛出 / Raised for missing or unsafe input.
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


def _install_migration_visibility(owner_role: str) -> None:
    """@brief 在 FORCE RLS 下临时允许 owner 审计精确表集 / Temporarily let the owner audit an exact FORCE-RLS table set.

    @param owner_role schema owner role / Schema-owner role.
    """
    for table in _PREFLIGHT_TABLES:
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )


def _remove_migration_visibility() -> None:
    """@brief 移除事务 owner visibility / Remove transaction owner visibility."""
    for table in reversed(_PREFLIGHT_TABLES):
        op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {table}")


def _count(statement: str) -> int:
    """@brief 执行固定 SQL count / Execute a static SQL count.

    @param statement 仅来自本文件常量的 SQL / SQL supplied only by this module.
    @return count 值 / Count value.
    """
    value = op.get_bind().scalar(sa.text(statement))
    return int(value or 0)


class _LegacyRowError(RuntimeError):
    """@brief 指向精确源行与字段的迁移错误 / Migration error naming an exact source row and field."""


def _row_error(table: str, row_id: str, path: str, detail: str) -> _LegacyRowError:
    """@brief 构造 operator 可定位的 fail-closed 错误 / Build an operator-locatable fail-closed error.

    @param table 源 relation / Source relation.
    @param row_id 源行 ID / Source-row ID.
    @param path JSON 字段路径 / JSON field path.
    @param detail 不含 secret 的原因 / Secret-free reason.
    @return 精确迁移错误 / Precise migration error.
    """
    return _LegacyRowError(f"legacy Resume conversion failed at {table}[{row_id}].{path}: {detail}")


def _canonical_json(value: object) -> bytes:
    """@brief 以冻结规则编码 JSON / Encode JSON with frozen canonical rules.

    @param value JSON 值 / JSON value.
    @return UTF-8 规范字节 / Canonical UTF-8 bytes.
    """
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256_json(value: object) -> str:
    """@brief 计算规范 JSON SHA-256 / Compute a canonical JSON SHA-256.

    @param value JSON 值 / JSON value.
    @return 小写十六进制摘要 / Lowercase hexadecimal digest.
    """
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _stable_id(prefix: str, *parts: str) -> str:
    """@brief 从稳定源键生成 opaque ID / Generate an opaque ID from stable source keys.

    @param prefix 领域前缀 / Domain prefix.
    @param parts 不可变源键 / Immutable source-key parts.
    @return 确定性 ID / Deterministic ID.
    """
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:40]
    return f"{prefix}_{digest}"


def _as_object(value: object, *, table: str, row_id: str, path: str) -> dict[str, Any]:
    """@brief 要求 JSON object / Require a JSON object."""
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise _row_error(table, row_id, path, "expected a JSON object")
    return value


def _as_array(value: object, *, table: str, row_id: str, path: str) -> list[Any]:
    """@brief 要求 JSON array / Require a JSON array."""
    if not isinstance(value, list):
        raise _row_error(table, row_id, path, "expected a JSON array")
    return value


def _as_string(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
    minimum: int = 0,
    maximum: int | None = None,
) -> str:
    """@brief 要求有界字符串 / Require a bounded string."""
    if (
        not isinstance(value, str)
        or len(value) < minimum
        or (maximum is not None and len(value) > maximum)
    ):
        raise _row_error(table, row_id, path, "string length is outside V2 limits")
    return value


def _optional_string(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
    maximum: int,
) -> str | None:
    """@brief 要求 nullable 有界字符串 / Require a nullable bounded string."""
    if value is None:
        return None
    return _as_string(
        value,
        table=table,
        row_id=row_id,
        path=path,
        maximum=maximum,
    )


def _as_bool(value: object, *, table: str, row_id: str, path: str) -> bool:
    """@brief 要求 JSON boolean / Require a JSON boolean."""
    if not isinstance(value, bool):
        raise _row_error(table, row_id, path, "expected a boolean")
    return value


def _as_positive_int(value: object, *, table: str, row_id: str, path: str) -> int:
    """@brief 要求正整数 / Require a positive integer."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise _row_error(table, row_id, path, "expected a positive integer")
    return value


def _opaque_id(value: object, *, table: str, row_id: str, path: str) -> str:
    """@brief 要求 V2 opaque ID / Require a V2 opaque ID."""
    result = _as_string(
        value,
        table=table,
        row_id=row_id,
        path=path,
        minimum=8,
        maximum=160,
    )
    if _OPAQUE_ID_PATTERN.fullmatch(result) is None:
        raise _row_error(table, row_id, path, "value is not a V2 opaque ID")
    return result


def _timestamp(value: object, *, table: str, row_id: str, path: str) -> str:
    """@brief 要求带时区 ISO timestamp / Require a timezone-aware ISO timestamp."""
    result = _as_string(
        value,
        table=table,
        row_id=row_id,
        path=path,
        minimum=1,
        maximum=80,
    )
    try:
        parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    except ValueError as error:
        raise _row_error(table, row_id, path, "timestamp is not ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _row_error(table, row_id, path, "timestamp has no timezone")
    return result


def _safe_link(value: object) -> str | None:
    """@brief 只投影 V2 允许的无 userinfo URL / Project only V2-safe URLs."""
    if not isinstance(value, str):
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https", "mailto", "tel"}:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    return value


def _create_legacy_archive() -> None:
    """@brief 创建 owner-only append-only V1 archive / Create the owner-only append-only V1 archive."""
    op.create_table(
        "v1_migration_archive",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column("workspace_id", sa.String(128), nullable=False),
        sa.Column("source_table", sa.String(80), nullable=False),
        sa.Column("source_row_id", sa.String(160), nullable=False),
        sa.Column("converter_version", sa.String(80), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("source_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "archived_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "source_table",
            "source_row_id",
            name="resume_v1_migration_archive_source",
        ),
        sa.CheckConstraint(
            "converter_version = 'resume-v1-to-v2/1'",
            name="resume_v1_migration_archive_version",
        ),
        sa.CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="resume_v1_migration_archive_checksum",
        ),
        schema="resume",
    )
    op.create_index(
        "ix_resume_v1_migration_archive_workspace_source",
        "v1_migration_archive",
        ["workspace_id", "source_table", "source_row_id"],
        schema="resume",
    )
    op.execute(
        """
        CREATE FUNCTION resume.reject_v1_migration_archive_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION 'resume.v1_migration_archive is append-only'
                USING ERRCODE = '55000';
        END;
        $function$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_ARCHIVE_TRIGGER} BEFORE UPDATE OR DELETE ON {_ARCHIVE_TABLE} "
        "FOR EACH ROW EXECUTE FUNCTION resume.reject_v1_migration_archive_mutation()"
    )
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE {_ARCHIVE_TABLE} FROM PUBLIC")


def _seal_legacy_archive(
    *,
    owner_role: str,
    app_role: str,
    dashboard_role: str,
    migrator_role: str,
) -> None:
    """@brief 封存完成后收紧为 owner-only read / Seal the populated archive as owner-only read."""
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {_ARCHIVE_TABLE} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"ALTER TABLE {_ARCHIVE_TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {_ARCHIVE_TABLE} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY resume_v1_archive_owner_read ON {_ARCHIVE_TABLE} "
        f"AS PERMISSIVE FOR SELECT TO {owner_role} USING (true)"
    )


def _source_filter(table: str) -> str:
    """@brief 返回冻结 relation 的 Resume 行过滤器 / Return the frozen Resume-row filter for a relation."""
    if table == "agent.jobs":
        return "job_type LIKE 'resume.%'"
    if table == "agent.outbox_events":
        return "event_type LIKE 'resume.%' OR aggregate_type IN ('resume', 'resume_proposal')"
    return "true"


def _archive_legacy_rows() -> tuple[dict[str, int], str]:
    """@brief 封存所有候选源行并计算全局摘要 / Archive every candidate row and compute a global digest.

    @return relation 计数与全局 SHA-256 / Per-relation counts and global SHA-256.
    """
    connection = op.get_bind()
    archive = sa.table(
        "v1_migration_archive",
        sa.column("id", sa.String(160)),
        sa.column("workspace_id", sa.String(128)),
        sa.column("source_table", sa.String(80)),
        sa.column("source_row_id", sa.String(160)),
        sa.column("converter_version", sa.String(80)),
        sa.column("payload_sha256", sa.String(64)),
        sa.column("source_payload", postgresql.JSONB()),
        schema="resume",
    )
    counts: dict[str, int] = {}
    checksums: list[str] = []
    for table in _ARCHIVED_SOURCES:
        rows = list(
            connection.execute(
                sa.text(
                    f"SELECT id, workspace_id, to_jsonb(legacy_row) AS source_payload "
                    f"FROM {table} AS legacy_row WHERE {_source_filter(table)} ORDER BY id"
                )
            ).mappings()
        )
        counts[table] = len(rows)
        for row in rows:
            row_id = str(row["id"])
            workspace_id = str(row["workspace_id"])
            payload = row["source_payload"]
            digest = _sha256_json(payload)
            connection.execute(
                sa.insert(archive).values(
                    id=_stable_id("rv1arc", table, row_id),
                    workspace_id=workspace_id,
                    source_table=table,
                    source_row_id=row_id,
                    converter_version=_CONVERTER_VERSION,
                    payload_sha256=digest,
                    source_payload=payload,
                )
            )
            checksums.append(f"{table}\x1f{row_id}\x1f{digest}")
    snapshot = hashlib.sha256("\n".join(checksums).encode("utf-8")).hexdigest()
    return counts, snapshot


def _write_migration_audit(
    event_type: Literal["backup_created", "started", "verified", "completed"],
    phase: int,
    snapshot_sha256: str,
    details: Mapping[str, Any],
) -> None:
    """@brief 追加一条 API migration audit / Append one API migration audit record."""
    audit = sa.table(
        "api_migration_audits",
        sa.column("id", sa.String(128)),
        sa.column("migration_id", sa.String(128)),
        sa.column("phase", sa.SmallInteger()),
        sa.column("event_type", sa.String(32)),
        sa.column("source_api_version", sa.String(16)),
        sa.column("target_api_version", sa.String(16)),
        sa.column("source_snapshot_sha256", sa.String(64)),
        sa.column("details", postgresql.JSONB()),
        schema="identity",
    )
    op.get_bind().execute(
        sa.insert(audit).values(
            id=f"apimig_resume0016_{event_type}",
            migration_id=_AUDIT_MIGRATION_ID,
            phase=phase,
            event_type=event_type,
            source_api_version="v1",
            target_api_version="v2",
            source_snapshot_sha256=snapshot_sha256,
            details=dict(details),
        )
    )


def _v1_span_text(
    spans: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> tuple[str, list[dict[str, Any]], bool]:
    """@brief 将 V1 span 序列投影为 V2 text/marks / Project V1 spans into V2 text and marks."""
    values = _as_array(spans, table=table, row_id=row_id, path=path)
    chunks: list[str] = []
    marks: list[dict[str, Any]] = []
    offset = 0
    lossy = False
    mark_mapping = {"bold": "strong", "italic": "emphasis", "link": "link"}
    for index, raw_span in enumerate(values):
        span_path = f"{path}[{index}]"
        span = _as_object(raw_span, table=table, row_id=row_id, path=span_path)
        text = _as_string(
            span.get("text"),
            table=table,
            row_id=row_id,
            path=f"{span_path}.text",
            maximum=20_000,
        )
        chunks.append(text)
        link_added = False
        for mark_index, raw_mark in enumerate(span.get("marks", [])):
            mark_path = f"{span_path}.marks[{mark_index}]"
            mark = _as_object(raw_mark, table=table, row_id=row_id, path=mark_path)
            kind = mark.get("type")
            mapped = mark_mapping.get(kind) if isinstance(kind, str) else None
            if mapped is None:
                lossy = True
                continue
            href = _safe_link(mark.get("href")) if mapped == "link" else None
            if mapped == "link":
                if href is None or link_added:
                    lossy = True
                    continue
                link_added = True
            if text:
                marks.append(
                    {
                        "start": offset,
                        "end": offset + len(text),
                        "kind": mapped,
                        "href": href,
                    }
                )
        offset += len(text)
    return "".join(chunks), marks, lossy


def _v1_list_item_lines(
    raw_item: object,
    *,
    ordered: bool,
    depth: int,
    ordinal: int,
    table: str,
    row_id: str,
    path: str,
) -> tuple[list[tuple[str, list[dict[str, Any]]]], bool]:
    """@brief 递归展平 V1 list item，保留文本与可表示 mark / Recursively flatten a V1 list item."""
    item = _as_object(raw_item, table=table, row_id=row_id, path=path)
    text, marks, lossy = _v1_span_text(
        item.get("spans"),
        table=table,
        row_id=row_id,
        path=f"{path}.spans",
    )
    prefix = f"{'  ' * depth}{f'{ordinal}. ' if ordered else '• '}"
    shifted = [
        {**mark, "start": mark["start"] + len(prefix), "end": mark["end"] + len(prefix)}
        for mark in marks
    ]
    lines = [(prefix + text, shifted)]
    children = _as_array(
        item.get("children", []),
        table=table,
        row_id=row_id,
        path=f"{path}.children",
    )
    for child_index, child in enumerate(children, start=1):
        child_lines, child_lossy = _v1_list_item_lines(
            child,
            ordered=ordered,
            depth=depth + 1,
            ordinal=child_index,
            table=table,
            row_id=row_id,
            path=f"{path}.children[{child_index - 1}]",
        )
        lines.extend(child_lines)
        lossy = lossy or child_lossy
    return lines, lossy


def _convert_rich_text(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> tuple[dict[str, Any] | None, bool]:
    """@brief 将 block-based V1 RichText 确定性投影为 V2 range marks / Deterministically project block-based V1 RichText."""
    if value is None:
        return None, False
    rich = _as_object(value, table=table, row_id=row_id, path=path)
    if rich.get("schema_version") != "1.0":
        raise _row_error(table, row_id, f"{path}.schema_version", "unsupported RichText version")
    blocks = _as_array(rich.get("blocks"), table=table, row_id=row_id, path=f"{path}.blocks")
    lines: list[tuple[str, list[dict[str, Any]]]] = []
    lossy = rich.get("plain_text") not in (None, "") or bool(
        set(rich) - {"schema_version", "blocks", "plain_text"}
    )
    for block_index, raw_block in enumerate(blocks):
        block_path = f"{path}.blocks[{block_index}]"
        block = _as_object(raw_block, table=table, row_id=row_id, path=block_path)
        block_type = block.get("type")
        if block_type == "paragraph":
            text, span_marks, span_lossy = _v1_span_text(
                block.get("spans"),
                table=table,
                row_id=row_id,
                path=f"{block_path}.spans",
            )
            lines.append((text, span_marks))
            lossy = lossy or span_lossy or block.get("align", "start") != "start"
        elif block_type == "list":
            ordered = _as_bool(
                block.get("ordered"),
                table=table,
                row_id=row_id,
                path=f"{block_path}.ordered",
            )
            items = _as_array(
                block.get("items"),
                table=table,
                row_id=row_id,
                path=f"{block_path}.items",
            )
            for item_index, item in enumerate(items, start=1):
                item_lines, item_lossy = _v1_list_item_lines(
                    item,
                    ordered=ordered,
                    depth=0,
                    ordinal=item_index,
                    table=table,
                    row_id=row_id,
                    path=f"{block_path}.items[{item_index - 1}]",
                )
                lines.extend(item_lines)
                lossy = lossy or item_lossy
        else:
            raise _row_error(table, row_id, f"{block_path}.type", "unknown V1 RichText block")
    text_parts: list[str] = []
    combined_marks: list[dict[str, Any]] = []
    offset = 0
    for line_index, (line, line_marks) in enumerate(lines):
        if line_index:
            text_parts.append("\n")
            offset += 1
        text_parts.append(line)
        combined_marks.extend(
            {**mark, "start": mark["start"] + offset, "end": mark["end"] + offset}
            for mark in line_marks
        )
        offset += len(line)
    text = "".join(text_parts)
    if len(text) > 20_000:
        text = text[:20_000]
        combined_marks = [
            {**mark, "end": min(int(mark["end"]), 20_000)}
            for mark in combined_marks
            if int(mark["start"]) < 20_000
        ]
        lossy = True
    deduplicated: list[dict[str, Any]] = []
    identities: set[tuple[int, int, str, str | None]] = set()
    for mark in combined_marks:
        identity = (
            int(mark["start"]),
            int(mark["end"]),
            str(mark["kind"]),
            str(mark["href"]) if mark["href"] is not None else None,
        )
        if identity not in identities and identity[0] < identity[1]:
            identities.add(identity)
            deduplicated.append(mark)
    if len(deduplicated) > 1_000:
        deduplicated = deduplicated[:1_000]
        lossy = True
    return {"text": text, "marks": deduplicated}, lossy


def _convert_partial_date(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> dict[str, str] | None:
    """@brief 将 V1 structured PartialDate 投影为 V2 精度字符串 / Project structured V1 PartialDate to a V2 precision string."""
    if value is None:
        return None
    raw = _as_object(value, table=table, row_id=row_id, path=path)
    year = _as_positive_int(raw.get("year"), table=table, row_id=row_id, path=f"{path}.year")
    precision = raw.get("precision")
    if precision == "year":
        rendered = f"{year:04d}"
    elif precision == "month":
        month = _as_positive_int(raw.get("month"), table=table, row_id=row_id, path=f"{path}.month")
        if month > 12:
            raise _row_error(table, row_id, f"{path}.month", "month is outside the calendar")
        rendered = f"{year:04d}-{month:02d}"
    elif precision == "day":
        month = _as_positive_int(raw.get("month"), table=table, row_id=row_id, path=f"{path}.month")
        day = _as_positive_int(raw.get("day"), table=table, row_id=row_id, path=f"{path}.day")
        try:
            datetime(year, month, day)
        except ValueError as error:
            raise _row_error(table, row_id, path, "date is not a real calendar day") from error
        rendered = f"{year:04d}-{month:02d}-{day:02d}"
    else:
        raise _row_error(table, row_id, f"{path}.precision", "unknown date precision")
    return {"value": rendered}


def _convert_date_range(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> tuple[dict[str, Any] | None, bool]:
    """@brief 将 V1 DateRange 投影为 V2 DateRange / Project a V1 DateRange into V2."""
    if value is None:
        return None, False
    raw = _as_object(value, table=table, row_id=row_id, path=path)
    start = _convert_partial_date(
        raw.get("start"), table=table, row_id=row_id, path=f"{path}.start"
    )
    end = _convert_partial_date(raw.get("end"), table=table, row_id=row_id, path=f"{path}.end")
    present = _as_bool(raw.get("is_current"), table=table, row_id=row_id, path=f"{path}.is_current")
    if present and end is not None:
        raise _row_error(table, row_id, path, "current range also has an end date")
    return (
        {"start": start, "end": end, "present": present},
        raw.get("display_override") not in (None, ""),
    )


def _first_safe_item_link(
    raw_links: object, *, table: str, row_id: str, path: str
) -> tuple[str | None, bool]:
    """@brief 选取第一个 V2-safe item URL，其余保留在 archive / Select the first V2-safe item URL."""
    links = _as_array(raw_links, table=table, row_id=row_id, path=path)
    selected: str | None = None
    lossy = False
    for index, raw_link in enumerate(links):
        link = _as_object(raw_link, table=table, row_id=row_id, path=f"{path}[{index}]")
        candidate = _safe_link(link.get("url"))
        if candidate is not None and selected is None:
            selected = candidate
        else:
            lossy = True
        if link.get("label") not in (None, "") or link.get("kind") not in (None, "website"):
            lossy = True
    return selected, lossy


def _string_array(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
    maximum_items: int,
    maximum_length: int,
) -> tuple[list[str], bool]:
    """@brief 投影有界且去重的字符串数组 / Project a bounded, unique string array."""
    raw = _as_array(value, table=table, row_id=row_id, path=path)
    result: list[str] = []
    seen: set[str] = set()
    lossy = False
    for index, item in enumerate(raw):
        text = _as_string(
            item,
            table=table,
            row_id=row_id,
            path=f"{path}[{index}]",
            minimum=1,
            maximum=maximum_length,
        )
        if text in seen or len(result) >= maximum_items:
            lossy = True
            continue
        seen.add(text)
        result.append(text)
    return result, lossy


def _convert_item(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> tuple[dict[str, Any], bool]:
    """@brief 将 V1 异构 ResumeItem 规范化为 V2 item / Normalize a heterogeneous V1 ResumeItem into V2."""
    raw = _as_object(value, table=table, row_id=row_id, path=path)
    item_id = _opaque_id(raw.get("item_id"), table=table, row_id=row_id, path=f"{path}.item_id")
    kind = _as_string(
        raw.get("item_kind"),
        table=table,
        row_id=row_id,
        path=f"{path}.item_kind",
        minimum=1,
        maximum=80,
    )
    supported = {
        "experience",
        "education",
        "project",
        "skill_group",
        "publication",
        "award",
        "certification",
        "language",
        "volunteer",
        "custom",
    }
    if kind not in supported:
        raise _row_error(table, row_id, f"{path}.item_kind", "unknown V1 item kind")
    visible = _as_bool(raw.get("visible"), table=table, row_id=row_id, path=f"{path}.visible")
    tags, tags_lossy = _string_array(
        raw.get("tags", []),
        table=table,
        row_id=row_id,
        path=f"{path}.tags",
        maximum_items=100,
        maximum_length=100,
    )
    url, links_lossy = _first_safe_item_link(
        raw.get("links", []), table=table, row_id=row_id, path=f"{path}.links"
    )
    title: str | None = None
    subtitle: str | None = None
    organization: str | None = None
    location: str | None = None
    date_range: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None
    highlights: list[dict[str, Any]] = []
    skills: list[str] = []
    lossy = tags_lossy or links_lossy or bool(raw.get("extensions"))

    def optional(name: str, maximum: int = 300) -> str | None:
        """@brief 读取当前 item 的 nullable 文本 / Read nullable text from the current item."""
        return _optional_string(
            raw.get(name),
            table=table,
            row_id=row_id,
            path=f"{path}.{name}",
            maximum=maximum,
        )

    def rich(name: str) -> tuple[dict[str, Any] | None, bool]:
        """@brief 转换当前 item 的 RichText 字段 / Convert a RichText field on the current item."""
        return _convert_rich_text(raw.get(name), table=table, row_id=row_id, path=f"{path}.{name}")

    if kind == "experience":
        title = optional("position")
        organization = optional("organization")
        location = optional("location")
        date_range, range_lossy = _convert_date_range(
            raw.get("date_range"), table=table, row_id=row_id, path=f"{path}.date_range"
        )
        summary, summary_lossy = rich("description")
        lossy = lossy or range_lossy or summary_lossy
    elif kind == "education":
        title = optional("degree") or optional("field_of_study")
        subtitle = optional("field_of_study") if title != raw.get("field_of_study") else None
        organization = optional("institution")
        location = optional("location")
        date_range, range_lossy = _convert_date_range(
            raw.get("date_range"), table=table, row_id=row_id, path=f"{path}.date_range"
        )
        summary, summary_lossy = rich("description")
        lossy = lossy or range_lossy or summary_lossy or raw.get("score") not in (None, "")
    elif kind == "project":
        title = optional("name")
        subtitle = optional("role")
        date_range, range_lossy = _convert_date_range(
            raw.get("date_range"), table=table, row_id=row_id, path=f"{path}.date_range"
        )
        summary, summary_lossy = rich("description")
        skills, skills_lossy = _string_array(
            raw.get("technologies", []),
            table=table,
            row_id=row_id,
            path=f"{path}.technologies",
            maximum_items=200,
            maximum_length=100,
        )
        lossy = lossy or range_lossy or summary_lossy or skills_lossy
    elif kind == "skill_group":
        title = optional("name")
        subtitle = optional("proficiency")
        skills, skills_lossy = _string_array(
            raw.get("skills"),
            table=table,
            row_id=row_id,
            path=f"{path}.skills",
            maximum_items=200,
            maximum_length=100,
        )
        lossy = lossy or skills_lossy
    elif kind == "publication":
        title = optional("title")
        organization = optional("publisher")
        authors, authors_lossy = _string_array(
            raw.get("authors"),
            table=table,
            row_id=row_id,
            path=f"{path}.authors",
            maximum_items=100,
            maximum_length=300,
        )
        subtitle = ", ".join(authors) or None
        if subtitle is not None and len(subtitle) > 300:
            subtitle = subtitle[:300]
            authors_lossy = True
        published = _convert_partial_date(
            raw.get("published_at"), table=table, row_id=row_id, path=f"{path}.published_at"
        )
        date_range = {"start": published, "end": published, "present": False} if published else None
        summary, summary_lossy = rich("description")
        lossy = lossy or authors_lossy or summary_lossy
    elif kind == "award":
        title = optional("title")
        organization = optional("issuer")
        awarded = _convert_partial_date(
            raw.get("awarded_at"), table=table, row_id=row_id, path=f"{path}.awarded_at"
        )
        date_range = {"start": awarded, "end": awarded, "present": False} if awarded else None
        summary, summary_lossy = rich("description")
        lossy = lossy or summary_lossy
    elif kind == "certification":
        title = optional("name")
        organization = optional("issuer")
        subtitle = optional("credential_id")
        issued = _convert_partial_date(
            raw.get("issued_at"), table=table, row_id=row_id, path=f"{path}.issued_at"
        )
        expires = _convert_partial_date(
            raw.get("expires_at"), table=table, row_id=row_id, path=f"{path}.expires_at"
        )
        date_range = (
            {"start": issued, "end": expires, "present": False} if issued or expires else None
        )
    elif kind == "language":
        title = optional("language")
        subtitle = optional("proficiency")
        certificate = optional("certificate", 200)
        summary = {"text": certificate, "marks": []} if certificate else None
    elif kind == "volunteer":
        title = optional("role")
        organization = optional("organization")
        date_range, range_lossy = _convert_date_range(
            raw.get("date_range"), table=table, row_id=row_id, path=f"{path}.date_range"
        )
        summary, summary_lossy = rich("description")
        lossy = lossy or range_lossy or summary_lossy
    else:
        title = optional("title")
        subtitle = optional("subtitle")
        date_range, range_lossy = _convert_date_range(
            raw.get("date_range"), table=table, row_id=row_id, path=f"{path}.date_range"
        )
        summary, summary_lossy = rich("content")
        lossy = lossy or range_lossy or summary_lossy or bool(raw.get("data"))

    raw_highlights = _as_array(
        raw.get("highlights", []), table=table, row_id=row_id, path=f"{path}.highlights"
    )
    for index, highlight in enumerate(raw_highlights):
        converted, highlight_lossy = _convert_rich_text(
            highlight, table=table, row_id=row_id, path=f"{path}.highlights[{index}]"
        )
        if converted is not None:
            highlights.append(converted)
        lossy = lossy or highlight_lossy
    if len(highlights) > 100:
        highlights = highlights[:100]
        lossy = True
    return (
        {
            "id": item_id,
            "kind": kind,
            "title": title,
            "subtitle": subtitle,
            "organization": organization,
            "location": location,
            "date_range": date_range,
            "summary": summary,
            "highlights": highlights,
            "skills": skills,
            "tags": tags,
            "visible": visible,
            "url": url,
        },
        lossy,
    )


def _convert_section(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> tuple[dict[str, Any], bool]:
    """@brief 将 V1 ResumeSection 投影为 V2 section / Project a V1 ResumeSection into V2."""
    raw = _as_object(value, table=table, row_id=row_id, path=path)
    section_id = _opaque_id(
        raw.get("section_id"), table=table, row_id=row_id, path=f"{path}.section_id"
    )
    original_kind = _as_string(
        raw.get("kind"), table=table, row_id=row_id, path=f"{path}.kind", minimum=1, maximum=80
    )
    kinds = {
        "experience",
        "education",
        "projects",
        "skills",
        "publications",
        "awards",
        "certifications",
        "languages",
        "volunteer",
        "custom",
    }
    kind = original_kind if original_kind in kinds else "custom"
    raw_title = _as_string(
        raw.get("title"), table=table, row_id=row_id, path=f"{path}.title", minimum=1, maximum=200
    )
    title = raw_title[:120]
    visible = _as_bool(raw.get("visible"), table=table, row_id=row_id, path=f"{path}.visible")
    content, content_lossy = _convert_rich_text(
        raw.get("content"), table=table, row_id=row_id, path=f"{path}.content"
    )
    items: list[dict[str, Any]] = []
    lossy = (
        content_lossy or kind != original_kind or title != raw_title or bool(raw.get("extensions"))
    )
    for index, raw_item in enumerate(
        _as_array(raw.get("items"), table=table, row_id=row_id, path=f"{path}.items")
    ):
        item, item_lossy = _convert_item(
            raw_item, table=table, row_id=row_id, path=f"{path}.items[{index}]"
        )
        items.append(item)
        lossy = lossy or item_lossy
    return {
        "id": section_id,
        "kind": kind,
        "title": title,
        "visible": visible,
        "content": content,
        "items": items,
    }, lossy


def _convert_profile(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> tuple[dict[str, Any], bool]:
    """@brief 安全投影 V1 PersonProfile / Safely project a V1 PersonProfile."""
    raw = _as_object(value, table=table, row_id=row_id, path=path)
    full_name = _as_string(
        raw.get("full_name"),
        table=table,
        row_id=row_id,
        path=f"{path}.full_name",
        minimum=1,
        maximum=200,
    )
    headline = _optional_string(
        raw.get("headline"), table=table, row_id=row_id, path=f"{path}.headline", maximum=300
    )
    summary, summary_lossy = _convert_rich_text(
        raw.get("summary"), table=table, row_id=row_id, path=f"{path}.summary"
    )
    contacts: list[dict[str, Any]] = []
    lossy = (
        summary_lossy
        or raw.get("pronouns") not in (None, "")
        or raw.get("photo_asset_id") is not None
    )
    raw_contacts = _as_array(
        raw.get("contacts"), table=table, row_id=row_id, path=f"{path}.contacts"
    )
    for index, raw_contact in enumerate(raw_contacts):
        contact_path = f"{path}.contacts[{index}]"
        contact = _as_object(raw_contact, table=table, row_id=row_id, path=contact_path)
        public = _as_bool(
            contact.get("is_public"), table=table, row_id=row_id, path=f"{contact_path}.is_public"
        )
        if not public:
            lossy = True
            continue
        contact_id = _opaque_id(
            contact.get("contact_id"), table=table, row_id=row_id, path=f"{contact_path}.contact_id"
        )
        kind = _as_string(
            contact.get("kind"),
            table=table,
            row_id=row_id,
            path=f"{contact_path}.kind",
            minimum=1,
            maximum=30,
        )
        if kind not in {
            "email",
            "phone",
            "website",
            "linkedin",
            "github",
            "portfolio",
            "location",
            "other",
        }:
            kind = "custom"
            lossy = True
        label = _optional_string(
            contact.get("label"),
            table=table,
            row_id=row_id,
            path=f"{contact_path}.label",
            maximum=100,
        )
        if label is not None and len(label) > 80:
            label = label[:80]
            lossy = True
        contact_value = _as_string(
            contact.get("value"),
            table=table,
            row_id=row_id,
            path=f"{contact_path}.value",
            minimum=1,
            maximum=500,
        )
        raw_url = contact.get("url")
        url = _safe_link(raw_url)
        if raw_url is not None and url is None:
            lossy = True
        contacts.append(
            {"id": contact_id, "kind": kind, "label": label, "value": contact_value, "url": url}
        )
    if len(contacts) > 30:
        contacts = contacts[:30]
        lossy = True
    return {
        "full_name": full_name,
        "headline": headline,
        "summary": summary,
        "contacts": contacts,
    }, lossy


def _convert_template_ref(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> dict[str, str]:
    """@brief 将 V1 template_version 键改名为 V2 version / Rename the V1 template-version key for V2."""
    raw = _as_object(value, table=table, row_id=row_id, path=path)
    template_id = _opaque_id(
        raw.get("template_id"), table=table, row_id=row_id, path=f"{path}.template_id"
    )
    version = _as_string(
        raw.get("template_version", raw.get("version")),
        table=table,
        row_id=row_id,
        path=f"{path}.template_version",
        minimum=1,
        maximum=80,
    )
    return {"template_id": template_id, "version": version}


def _convert_style(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
) -> dict[str, Any]:
    """@brief 复用 V1/V2 共享的 style intent 结构 / Reuse the shared V1/V2 style-intent structure."""
    raw = deepcopy(_as_object(value, table=table, row_id=row_id, path=path))
    required = {
        "style_contract_version",
        "page",
        "typography",
        "palette",
        "density",
        "date_format_token",
        "bullet_style_token",
        "section_layout",
        "template_settings",
    }
    if not required <= set(raw) or raw.get("style_contract_version") != "1.0":
        raise _row_error(table, row_id, path, "style intent is incomplete or unsupported")
    raw.setdefault("extensions", {})
    return raw


def _convert_document(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str = "semantic_document",
    pinned_template: tuple[str, str] | None = None,
) -> tuple[dict[str, Any], bool]:
    """@brief 将一个 V1 ResumeDocument 快照投影为 V2 内部 codec / Project one V1 ResumeDocument snapshot into the V2 codec."""
    raw = _as_object(value, table=table, row_id=row_id, path=path)
    if raw.get("schema_version") != "1.0":
        raise _row_error(
            table,
            row_id,
            f"{path}.schema_version",
            "unsupported Resume document version",
        )
    resume_id = _opaque_id(raw.get("id"), table=table, row_id=row_id, path=f"{path}.id")
    workspace_id = _opaque_id(
        raw.get("workspace_id"), table=table, row_id=row_id, path=f"{path}.workspace_id"
    )
    revision = _as_positive_int(
        raw.get("revision"), table=table, row_id=row_id, path=f"{path}.revision"
    )
    created_at = _timestamp(
        raw.get("created_at"), table=table, row_id=row_id, path=f"{path}.created_at"
    )
    updated_at = _timestamp(
        raw.get("updated_at"), table=table, row_id=row_id, path=f"{path}.updated_at"
    )
    raw_title = _as_string(
        raw.get("title"), table=table, row_id=row_id, path=f"{path}.title", minimum=1, maximum=300
    )
    title = raw_title.strip()
    if not title:
        raise _row_error(table, row_id, f"{path}.title", "title is empty after V2 normalization")
    locale = _as_string(
        raw.get("locale"), table=table, row_id=row_id, path=f"{path}.locale", minimum=2, maximum=35
    )
    if _LOCALE_PATTERN.fullmatch(locale) is None:
        raise _row_error(table, row_id, f"{path}.locale", "locale is invalid")
    template = _convert_template_ref(
        raw.get("template"), table=table, row_id=row_id, path=f"{path}.template"
    )
    if pinned_template is not None and template != {
        "template_id": pinned_template[0],
        "version": pinned_template[1],
    }:
        raise _row_error(
            table,
            row_id,
            f"{path}.template",
            "current snapshot disagrees with fixed template_version FK",
        )
    profile, profile_lossy = _convert_profile(
        raw.get("profile"), table=table, row_id=row_id, path=f"{path}.profile"
    )
    sections: list[dict[str, Any]] = []
    lossy = profile_lossy or title != raw_title or bool(raw.get("extensions"))
    for index, raw_section in enumerate(
        _as_array(raw.get("sections"), table=table, row_id=row_id, path=f"{path}.sections")
    ):
        section, section_lossy = _convert_section(
            raw_section, table=table, row_id=row_id, path=f"{path}.sections[{index}]"
        )
        sections.append(section)
        lossy = lossy or section_lossy
    style = _convert_style(
        raw.get("style_intent", raw.get("style")),
        table=table,
        row_id=row_id,
        path=f"{path}.style_intent",
    )
    source_id = raw.get("knowledge_source_id")
    knowledge_source_id = (
        None
        if source_id is None
        else _opaque_id(source_id, table=table, row_id=row_id, path=f"{path}.knowledge_source_id")
    )
    entity_ids = [resume_id]
    entity_ids.extend(contact["id"] for contact in profile["contacts"])
    entity_ids.extend(section["id"] for section in sections)
    entity_ids.extend(item["id"] for section in sections for item in section["items"])
    if len(entity_ids) != len(set(entity_ids)):
        raise _row_error(
            table, row_id, path, "operation-addressable entity IDs are not globally unique"
        )
    return (
        {
            "meta": {
                "id": resume_id,
                "revision": revision,
                "created_at": created_at,
                "updated_at": updated_at,
            },
            "workspace_id": workspace_id,
            "title": title,
            "locale": locale,
            "profile": profile,
            "sections": sections,
            "template": template,
            "style": style,
            "knowledge_source_id": knowledge_source_id,
        },
        lossy,
    )


def _fallback_operation(operation_id: str, current_document: Mapping[str, Any]) -> dict[str, Any]:
    """@brief 为只能封存的 V1 operation 生成无损当前状态的 V2 占位 / Build a state-preserving V2 placeholder."""
    template = _as_object(
        current_document["template"],
        table="resume.revisions",
        row_id=str(current_document["meta"]["id"]),
        path="converted.template",
    )
    style = _as_object(
        current_document["style"],
        table="resume.revisions",
        row_id=str(current_document["meta"]["id"]),
        path="converted.style",
    )
    settings = _as_object(
        style.get("template_settings", {}),
        table="resume.revisions",
        row_id=str(current_document["meta"]["id"]),
        path="converted.style.template_settings",
    )
    return {
        "operation_id": operation_id,
        "op": "set_template",
        "template": deepcopy(template),
        "settings": deepcopy(settings),
    }


def _converted_operation_id(raw: Mapping[str, Any], *, table: str, row_id: str, path: str) -> str:
    """@brief 保留 V1 operation ID，缺失时确定性生成 / Preserve or deterministically generate an operation ID."""
    value = raw.get("operation_id")
    if value is None:
        return _stable_id("opmig", table, row_id, _sha256_json(raw))
    return _opaque_id(value, table=table, row_id=row_id, path=f"{path}.operation_id")


def _convert_operation(
    value: object,
    *,
    table: str,
    row_id: str,
    path: str,
    resume_id: str,
    current_document: Mapping[str, Any],
) -> tuple[dict[str, Any], bool]:
    """@brief 将 V1 ResumeOperation 投影为六种 V2 operation / Project a V1 ResumeOperation into the six V2 operations."""
    wrapper = _as_object(value, table=table, row_id=row_id, path=path)
    raw_value = wrapper.get("operation", wrapper)
    raw = _as_object(raw_value, table=table, row_id=row_id, path=f"{path}.operation")
    operation_id = _converted_operation_id(raw, table=table, row_id=row_id, path=path)
    kind = raw.get("op")
    # V1 proposal wrappers carry explanation/citation metadata.  Those fields are archived and do
    # not alter the embedded operation's execution semantics, so they must not expire an otherwise
    # exactly replayable pending proposal.
    wrapper_lossy = False
    if kind == "upsert_section":
        section, lossy = _convert_section(
            raw.get("section"), table=table, row_id=row_id, path=f"{path}.section"
        )
        after = raw.get("after_section_id")
        if after is not None:
            after = _opaque_id(after, table=table, row_id=row_id, path=f"{path}.after_section_id")
        return {
            "operation_id": operation_id,
            "op": "upsert_section",
            "section": section,
            "after_section_id": after,
        }, wrapper_lossy or lossy
    if kind == "upsert_item":
        section_id = _opaque_id(
            raw.get("section_id"), table=table, row_id=row_id, path=f"{path}.section_id"
        )
        item, lossy = _convert_item(
            raw.get("item"), table=table, row_id=row_id, path=f"{path}.item"
        )
        after = raw.get("after_item_id")
        if after is not None:
            after = _opaque_id(after, table=table, row_id=row_id, path=f"{path}.after_item_id")
        return {
            "operation_id": operation_id,
            "op": "upsert_item",
            "section_id": section_id,
            "item": item,
            "after_item_id": after,
        }, wrapper_lossy or lossy
    if kind in {"remove_section", "remove_item"}:
        entity_kind = "section" if kind == "remove_section" else "item"
        key = "section_id" if entity_kind == "section" else "item_id"
        removed_entity_id = _opaque_id(
            raw.get(key), table=table, row_id=row_id, path=f"{path}.{key}"
        )
        return {
            "operation_id": operation_id,
            "op": "remove_entity",
            "entity_kind": entity_kind,
            "entity_id": removed_entity_id,
        }, wrapper_lossy
    if kind in {"move_section", "move_item"}:
        entity_kind = "section" if kind == "move_section" else "item"
        key = "section_id" if entity_kind == "section" else "item_id"
        moved_entity_id = _opaque_id(raw.get(key), table=table, row_id=row_id, path=f"{path}.{key}")
        parent: str | None = None
        if entity_kind == "item":
            parent = _opaque_id(
                raw.get("to_section_id"), table=table, row_id=row_id, path=f"{path}.to_section_id"
            )
        raw_after = raw.get("after_section_id" if entity_kind == "section" else "after_item_id")
        after = (
            None
            if raw_after is None
            else _opaque_id(raw_after, table=table, row_id=row_id, path=f"{path}.after_id")
        )
        return {
            "operation_id": operation_id,
            "op": "move_entity",
            "entity_kind": entity_kind,
            "entity_id": moved_entity_id,
            "parent_id": parent,
            "after_id": after,
        }, wrapper_lossy
    if kind == "set_template":
        template = _convert_template_ref(
            raw.get("template"), table=table, row_id=row_id, path=f"{path}.template"
        )
        style = raw.get("style_intent")
        settings: dict[str, Any] = {}
        lossy = False
        if style is not None:
            converted_style = _convert_style(
                style, table=table, row_id=row_id, path=f"{path}.style_intent"
            )
            settings = deepcopy(
                _as_object(
                    converted_style.get("template_settings", {}),
                    table=table,
                    row_id=row_id,
                    path=f"{path}.style_intent.template_settings",
                )
            )
            lossy = True
        return {
            "operation_id": operation_id,
            "op": "set_template",
            "template": template,
            "settings": settings,
        }, wrapper_lossy or lossy
    if kind == "set_field":
        target = _as_object(raw.get("target"), table=table, row_id=row_id, path=f"{path}.target")
        entity_type = target.get("entity_type")
        field_path_raw = _as_array(
            raw.get("field_path"), table=table, row_id=row_id, path=f"{path}.field_path"
        )
        field_path = tuple(str(part) for part in field_path_raw)
        entity_id: str | None = None
        mapped_path = field_path
        if entity_type == "resume":
            entity_id = resume_id
        elif entity_type == "profile":
            entity_id = resume_id
            mapped_path = ("profile", *field_path)
        elif entity_type == "section":
            entity_id = _opaque_id(
                target.get("section_id"),
                table=table,
                row_id=row_id,
                path=f"{path}.target.section_id",
            )
        elif entity_type == "item":
            entity_id = _opaque_id(
                target.get("item_id"), table=table, row_id=row_id, path=f"{path}.target.item_id"
            )
        valid_path = bool(mapped_path) and all(
            _FIELD_PART_PATTERN.fullmatch(part) is not None for part in mapped_path
        )
        allowed_leaf = mapped_path[-1] if mapped_path else ""
        if (
            entity_id is None
            or not valid_path
            or allowed_leaf
            not in {
                "title",
                "locale",
                "full_name",
                "headline",
                "summary",
                "content",
                "visible",
                "tags",
                "skills",
                "location",
                "organization",
                "subtitle",
                "url",
            }
        ):
            return _fallback_operation(operation_id, current_document), True
        converted_value = deepcopy(raw.get("value"))
        lossy = False
        if allowed_leaf in {"summary", "content"}:
            converted_value, lossy = _convert_rich_text(
                converted_value, table=table, row_id=row_id, path=f"{path}.value"
            )
        return {
            "operation_id": operation_id,
            "op": "set_field",
            "entity_id": entity_id,
            "field_path": list(mapped_path),
            "value": converted_value,
        }, wrapper_lossy or lossy
    if kind in {"set_style_intent", "replace_document"}:
        return _fallback_operation(operation_id, current_document), True
    raise _row_error(table, row_id, f"{path}.op", "unknown V1 Resume operation")


def _operation_fingerprint(operation: Mapping[str, Any]) -> str:
    """@brief 计算与 V2 领域相同的 operation 指纹 / Compute the V2-domain operation fingerprint."""
    return _sha256_json(operation)


def _operation_targets(operation: Mapping[str, Any], resume_id: str) -> list[dict[str, Any]]:
    """@brief 将 converted operation 投影为保守 change targets / Project conservative change targets."""
    kind = operation.get("op")
    if kind == "set_field":
        return [{"entity_id": operation["entity_id"], "field_path": operation["field_path"]}]
    if kind == "upsert_section":
        return [{"entity_id": operation["section"]["id"], "field_path": []}]
    if kind == "upsert_item":
        return [{"entity_id": operation["item"]["id"], "field_path": []}]
    if kind in {"remove_entity", "move_entity"}:
        return [{"entity_id": operation["entity_id"], "field_path": []}]
    return [{"entity_id": resume_id, "field_path": []}]


def _migration_extension(
    existing: object,
    *,
    source_table: str,
    source_row_id: str,
    lossy_projection: bool,
    classification: str = "converted",
) -> dict[str, Any]:
    """@brief 在不覆盖既有 extension 的前提下记录 archive 证据 / Record archive evidence without overwriting extensions."""
    result = deepcopy(existing) if isinstance(existing, dict) else {}
    result["migration_0016"] = {
        "converter_version": _CONVERTER_VERSION,
        "archive_id": _stable_id("rv1arc", source_table, source_row_id),
        "lossy_projection": lossy_projection,
        "classification": classification,
    }
    return result


def _backfill_documents_and_revisions() -> tuple[
    dict[str, dict[str, Any]], dict[tuple[str, int], dict[str, Any]]
]:
    """@brief 从固定 template FK 与 V1 snapshot 回填 Resume 根/版本 / Backfill Resume roots and revisions."""
    connection = op.get_bind()
    document_rows = list(
        connection.execute(
            sa.text(
                """
                SELECT document.*, template.template_id AS pinned_template_id,
                       template.template_version AS pinned_template_version
                FROM resume.documents AS document
                LEFT JOIN resume.template_versions AS template
                  ON template.id = document.template_version_id
                ORDER BY document.id
                """
            )
        ).mappings()
    )
    current_documents: dict[str, dict[str, Any]] = {}
    revision_documents: dict[tuple[str, int], dict[str, Any]] = {}
    document_by_id = {str(row["id"]): row for row in document_rows}
    for row in document_rows:
        row_id = str(row["id"])
        _opaque_id(row_id, table="resume.documents", row_id=row_id, path="id")
        _opaque_id(
            row["workspace_id"], table="resume.documents", row_id=row_id, path="workspace_id"
        )
        _opaque_id(
            row["resource_owner_id"],
            table="resume.documents",
            row_id=row_id,
            path="resource_owner_id",
        )
        pinned_id = row["pinned_template_id"]
        pinned_version = row["pinned_template_version"]
        if pinned_id is None or pinned_version is None:
            raise _row_error(
                "resume.documents",
                row_id,
                "template_version_id",
                "fixed template version FK has no target row",
            )
        template_id = _opaque_id(
            pinned_id,
            table="resume.documents",
            row_id=row_id,
            path="template_version_id.template_id",
        )
        version = _as_string(
            pinned_version,
            table="resume.documents",
            row_id=row_id,
            path="template_version_id.template_version",
            minimum=1,
            maximum=80,
        )
        current_revision = _as_positive_int(
            row["current_revision_no"],
            table="resume.documents",
            row_id=row_id,
            path="current_revision_no",
        )
        raw_title = _as_string(
            row["title"],
            table="resume.documents",
            row_id=row_id,
            path="title",
            minimum=1,
            maximum=300,
        )
        title = raw_title.strip()
        if not title:
            raise _row_error(
                "resume.documents", row_id, "title", "title is empty after V2 normalization"
            )
        locale = _as_string(
            row["locale"],
            table="resume.documents",
            row_id=row_id,
            path="locale",
            minimum=2,
            maximum=35,
        )
        if _LOCALE_PATTERN.fullmatch(locale) is None:
            raise _row_error("resume.documents", row_id, "locale", "locale is invalid")
        if row["revision"] != current_revision:
            raise _row_error(
                "resume.documents",
                row_id,
                "revision",
                "root revision differs from current_revision_no",
            )
        connection.execute(
            sa.text(
                """
                UPDATE resume.documents
                SET template_id = :template_id,
                    template_version = :template_version,
                    title = :title,
                    extensions = :extensions
                WHERE id = :row_id
                """
            ).bindparams(
                sa.bindparam("extensions", type_=postgresql.JSONB()),
            ),
            {
                "template_id": template_id,
                "template_version": version,
                "title": title,
                "extensions": _migration_extension(
                    row["extensions"],
                    source_table="resume.documents",
                    source_row_id=row_id,
                    lossy_projection=title != raw_title,
                ),
                "row_id": row_id,
            },
        )

    revision_rows = list(
        connection.execute(
            sa.text("SELECT * FROM resume.revisions ORDER BY resume_id, revision_no, id")
        ).mappings()
    )
    for row in revision_rows:
        row_id = str(row["id"])
        resume_id = str(row["resume_id"])
        root = document_by_id.get(resume_id)
        if root is None:
            raise _row_error("resume.revisions", row_id, "resume_id", "parent Resume is missing")
        if (
            row["workspace_id"] != root["workspace_id"]
            or row["resource_owner_id"] != root["resource_owner_id"]
        ):
            raise _row_error(
                "resume.revisions",
                row_id,
                "workspace_id",
                "revision scope differs from parent Resume",
            )
        revision_no = _as_positive_int(
            row["revision_no"], table="resume.revisions", row_id=row_id, path="revision_no"
        )
        actor = row["created_by_actor_id"]
        if actor is None:
            raise _row_error(
                "resume.revisions",
                row_id,
                "created_by_actor_id",
                "revision actor cannot be inferred",
            )
        _opaque_id(actor, table="resume.revisions", row_id=row_id, path="created_by_actor_id")
        is_current = revision_no == int(root["current_revision_no"])
        pinned = (
            (str(root["pinned_template_id"]), str(root["pinned_template_version"]))
            if is_current
            else None
        )
        document, lossy = _convert_document(
            row["semantic_document"],
            table="resume.revisions",
            row_id=row_id,
            pinned_template=pinned,
        )
        if document["meta"]["id"] != resume_id:
            raise _row_error(
                "resume.revisions",
                row_id,
                "semantic_document.id",
                "snapshot ID differs from resume_id",
            )
        if document["workspace_id"] != row["workspace_id"]:
            raise _row_error(
                "resume.revisions",
                row_id,
                "semantic_document.workspace_id",
                "snapshot Workspace differs from row",
            )
        if document["meta"]["revision"] != revision_no:
            raise _row_error(
                "resume.revisions",
                row_id,
                "semantic_document.revision",
                "snapshot revision differs from revision_no",
            )
        if is_current and (
            document["title"] != str(root["title"]).strip() or document["locale"] != root["locale"]
        ):
            raise _row_error(
                "resume.revisions",
                row_id,
                "semantic_document",
                "current snapshot metadata differs from Resume root",
            )
        targets = [] if revision_no == 1 else [{"entity_id": resume_id, "field_path": []}]
        connection.execute(
            sa.text(
                """
                UPDATE resume.revisions
                SET semantic_document = :document,
                    content_hash = :content_hash,
                    change_targets = :change_targets,
                    extensions = :extensions
                WHERE id = :row_id
                """
            ).bindparams(
                sa.bindparam("document", type_=postgresql.JSONB()),
                sa.bindparam("change_targets", type_=postgresql.JSONB()),
                sa.bindparam("extensions", type_=postgresql.JSONB()),
            ),
            {
                "document": document,
                "content_hash": _sha256_json(document),
                "change_targets": targets,
                "extensions": _migration_extension(
                    row["extensions"],
                    source_table="resume.revisions",
                    source_row_id=row_id,
                    lossy_projection=lossy,
                ),
                "row_id": row_id,
            },
        )
        revision_documents[(resume_id, revision_no)] = document
        if is_current:
            current_documents[resume_id] = document

    for row in document_rows:
        resume_id = str(row["id"])
        if resume_id not in current_documents:
            raise _row_error(
                "resume.documents",
                resume_id,
                "current_revision_no",
                "no matching immutable revision exists",
            )
    return current_documents, revision_documents


def _backfill_operation_ledger(
    current_documents: Mapping[str, Mapping[str, Any]],
    revision_documents: Mapping[tuple[str, int], Mapping[str, Any]],
    migrated_at: datetime,
) -> None:
    """@brief 转换 applied ledger 并封存未应用 operation / Convert applied ledger entries and archive unapplied operations."""
    connection = op.get_bind()
    rows = list(
        connection.execute(
            sa.text(
                """
                SELECT operation.*, batch.resume_id, batch.client_batch_id,
                       batch.base_revision_no, batch.applied_revision_no AS batch_applied_revision_no,
                       batch.conflict_strategy, batch.status AS batch_status,
                       batch.created_at AS batch_created_at,
                       batch.extensions AS batch_extensions
                FROM resume.operations AS operation
                JOIN resume.operation_batches AS batch ON batch.id = operation.batch_id
                ORDER BY batch.id, operation.ordinal, operation.id
                """
            )
        ).mappings()
    )
    by_batch: dict[str, list[tuple[RowMapping, dict[str, Any], bool]]] = {}
    delete_ids: list[str] = []
    for row in rows:
        row_id = str(row["id"])
        batch_id = str(row["batch_id"])
        resume_id = str(row["resume_id"])
        current = current_documents.get(resume_id)
        if current is None:
            raise _row_error(
                "resume.operations", row_id, "batch_id", "batch parent Resume is missing"
            )
        converted, lossy = _convert_operation(
            row["payload"],
            table="resume.operations",
            row_id=row_id,
            path="payload",
            resume_id=resume_id,
            current_document=current,
        )
        stored_operation_id = _opaque_id(
            row["operation_id"], table="resume.operations", row_id=row_id, path="operation_id"
        )
        if converted["operation_id"] != stored_operation_id:
            raise _row_error(
                "resume.operations",
                row_id,
                "payload.operation_id",
                "payload and ledger operation IDs disagree",
            )
        if row["batch_status"] != "applied":
            delete_ids.append(row_id)
            continue
        applied_revision = row["batch_applied_revision_no"]
        if (
            isinstance(applied_revision, bool)
            or not isinstance(applied_revision, int)
            or (
                resume_id,
                applied_revision,
            )
            not in revision_documents
        ):
            raise _row_error(
                "resume.operations",
                row_id,
                "applied_revision_no",
                "applied batch has no exact immutable revision",
            )
        connection.execute(
            sa.text(
                """
                UPDATE resume.operations
                SET operation_type = :operation_type,
                    payload = :payload,
                    fingerprint = :fingerprint,
                    applied_revision_no = :applied_revision,
                    extensions = :extensions
                WHERE id = :row_id
                """
            ).bindparams(
                sa.bindparam("payload", type_=postgresql.JSONB()),
                sa.bindparam("extensions", type_=postgresql.JSONB()),
            ),
            {
                "operation_type": str(converted["op"]),
                "payload": converted,
                "fingerprint": _operation_fingerprint(converted),
                "applied_revision": applied_revision,
                "extensions": _migration_extension(
                    row["extensions"],
                    source_table="resume.operations",
                    source_row_id=row_id,
                    lossy_projection=lossy,
                ),
                "row_id": row_id,
            },
        )
        by_batch.setdefault(batch_id, []).append((row, converted, lossy))
    if delete_ids:
        connection.execute(
            sa.text("DELETE FROM resume.operations WHERE id = ANY(:row_ids)"),
            {"row_ids": delete_ids},
        )

    batches = list(
        connection.execute(sa.text("SELECT * FROM resume.operation_batches ORDER BY id")).mappings()
    )
    for batch in batches:
        batch_id = str(batch["id"])
        resume_id = str(batch["resume_id"])
        converted_rows = by_batch.get(batch_id, [])
        classification = "legacy_unapplied_archived"
        extensions = _migration_extension(
            batch["extensions"],
            source_table="resume.operation_batches",
            source_row_id=batch_id,
            lossy_projection=any(lossy for _, _, lossy in converted_rows),
            classification=classification,
        )
        if batch["status"] == "applied":
            if not converted_rows:
                raise _row_error(
                    "resume.operation_batches",
                    batch_id,
                    "status",
                    "applied batch contains no operations",
                )
            client_batch_id = _opaque_id(
                batch["client_batch_id"],
                table="resume.operation_batches",
                row_id=batch_id,
                path="client_batch_id",
            )
            base_revision = _as_positive_int(
                batch["base_revision_no"],
                table="resume.operation_batches",
                row_id=batch_id,
                path="base_revision_no",
            )
            strategy = batch["conflict_strategy"]
            if strategy not in {"reject", "rebase_if_safe"}:
                raise _row_error(
                    "resume.operation_batches",
                    batch_id,
                    "conflict_strategy",
                    "unknown conflict strategy",
                )
            applied_revision = _as_positive_int(
                batch["applied_revision_no"],
                table="resume.operation_batches",
                row_id=batch_id,
                path="applied_revision_no",
            )
            snapshot = revision_documents.get((resume_id, applied_revision))
            if snapshot is None:
                raise _row_error(
                    "resume.operation_batches",
                    batch_id,
                    "applied_revision_no",
                    "receipt snapshot is missing",
                )
            operations = [converted for _, converted, _ in converted_rows]
            operation_ids = [str(operation["operation_id"]) for operation in operations]
            request = {
                "client_batch_id": client_batch_id,
                "base_revision": base_revision,
                "conflict_strategy": strategy,
                "operations": operations,
                "render_hint": "none",
            }
            outcome = {
                "resume": deepcopy(snapshot),
                "applied_operation_ids": operation_ids,
                "conflicts": [],
                "render_job_ref": None,
            }
            extensions["migration_0016"]["classification"] = "converted_applied_receipt"
            connection.execute(
                sa.text(
                    """
                    UPDATE resume.operation_batches
                    SET request_fingerprint = :fingerprint,
                        outcome = :outcome,
                        expires_at = :expires_at,
                        extensions = :extensions
                    WHERE id = :row_id
                    """
                ).bindparams(
                    sa.bindparam("outcome", type_=postgresql.JSONB()),
                    sa.bindparam("extensions", type_=postgresql.JSONB()),
                ),
                {
                    "fingerprint": _sha256_json(request),
                    "outcome": outcome,
                    "expires_at": migrated_at + timedelta(days=30),
                    "extensions": extensions,
                    "row_id": batch_id,
                },
            )
        else:
            extensions["migration_0016"]["classification"] = "legacy_unapplied_quarantined"
            connection.execute(
                sa.text(
                    "UPDATE resume.operation_batches "
                    "SET client_batch_id = :client_batch_id, extensions = :extensions "
                    "WHERE id = :row_id"
                ).bindparams(sa.bindparam("extensions", type_=postgresql.JSONB())),
                {
                    "client_batch_id": _stable_id(
                        "rblegacy", batch_id, str(batch["client_batch_id"])
                    ),
                    "extensions": extensions,
                    "row_id": batch_id,
                },
            )


def _proposal_runtime(value: object) -> dict[str, Any]:
    """@brief 读取旧 runtime 命名空间 / Read the legacy runtime namespace."""
    if not isinstance(value, dict):
        return {}
    runtime = value.get("runtime")
    return runtime if isinstance(runtime, dict) else {}


def _proposal_applied_revision(
    proposal: RowMapping,
    runtime: Mapping[str, Any],
    *,
    row_id: str,
) -> int:
    """@brief 从 V1 application result 读取可证明 applied revision / Read a provable applied revision."""
    result = runtime.get("application_result")
    candidate: object = None
    if isinstance(result, dict):
        candidate = result.get("new_revision", result.get("revision"))
    if candidate is None and isinstance(proposal.get("decision_payload"), dict):
        candidate = proposal["decision_payload"].get("new_revision")
    return _as_positive_int(
        candidate,
        table="resume.proposals",
        row_id=row_id,
        path="decision_payload.runtime.application_result.new_revision",
    )


def _backfill_proposals(
    current_documents: Mapping[str, Mapping[str, Any]],
    revision_documents: Mapping[tuple[str, int], Mapping[str, Any]],
    migrated_at: datetime,
) -> None:
    """@brief 转换 proposal operations，将不可重放 pending 安全终结 / Convert proposal operations and safely terminate unreplayable pending work."""
    connection = op.get_bind()
    proposals = list(
        connection.execute(sa.text("SELECT * FROM resume.proposals ORDER BY id")).mappings()
    )
    for proposal in proposals:
        proposal_id = str(proposal["id"])
        resume_id = str(proposal["resume_id"])
        current = current_documents.get(resume_id)
        if current is None:
            raise _row_error(
                "resume.proposals", proposal_id, "resume_id", "parent Resume is missing"
            )
        operations = list(
            connection.execute(
                sa.text(
                    "SELECT * FROM resume.proposal_operations "
                    "WHERE proposal_id = :proposal_id ORDER BY ordinal, id"
                ),
                {"proposal_id": proposal_id},
            ).mappings()
        )
        if not 1 <= len(operations) <= 200:
            raise _row_error(
                "resume.proposals", proposal_id, "operations", "V2 requires one to 200 operations"
            )
        runtime = _proposal_runtime(proposal["decision_payload"])
        raw_selected = runtime.get("selected_operation_ids", [])
        selected = (
            {str(item) for item in raw_selected if isinstance(item, str)}
            if isinstance(raw_selected, list)
            else set()
        )
        converted_operations: list[tuple[RowMapping, dict[str, Any], bool]] = []
        converted_ids: set[str] = set()
        for operation in operations:
            operation_row_id = str(operation["id"])
            converted, lossy = _convert_operation(
                operation["payload"],
                table="resume.proposal_operations",
                row_id=operation_row_id,
                path="payload",
                resume_id=resume_id,
                current_document=current,
            )
            operation_id = str(converted["operation_id"])
            if operation_id in converted_ids:
                raise _row_error(
                    "resume.proposal_operations",
                    operation_row_id,
                    "payload.operation_id",
                    "proposal operation IDs are not unique",
                )
            converted_ids.add(operation_id)
            converted_operations.append((operation, converted, lossy))

        status = str(proposal["status"])
        if status not in {
            "pending",
            "accepted",
            "partially_accepted",
            "rejected",
            "expired",
            "conflicted",
        }:
            raise _row_error("resume.proposals", proposal_id, "status", "unknown proposal status")
        any_lossy = any(lossy for _, _, lossy in converted_operations)
        classification = "converted"
        decided_at = proposal["decided_at"]
        decided_by = proposal["decided_by_actor_id"]
        if status == "conflicted" or (status == "pending" and any_lossy):
            status = "expired"
            decided_at = proposal["updated_at"] or migrated_at
            decided_by = None
            classification = "terminal_unreplayable"
        if status == "pending":
            if decided_at is not None or decided_by is not None:
                raise _row_error(
                    "resume.proposals",
                    proposal_id,
                    "decided_at",
                    "pending proposal has decision metadata",
                )
        elif status == "expired":
            if decided_at is None:
                decided_at = proposal["updated_at"] or migrated_at
            decided_by = None
        else:
            if decided_at is None or decided_by is None:
                raise _row_error(
                    "resume.proposals",
                    proposal_id,
                    "decided_at",
                    "decided proposal lacks actor or timestamp",
                )
            _opaque_id(
                decided_by, table="resume.proposals", row_id=proposal_id, path="decided_by_actor_id"
            )

        accepted_ids: list[str] = []
        applied_revision: int | None = None
        if status in {"accepted", "partially_accepted"}:
            applied_revision = _proposal_applied_revision(proposal, runtime, row_id=proposal_id)
            if (resume_id, applied_revision) not in revision_documents:
                raise _row_error(
                    "resume.proposals",
                    proposal_id,
                    "decision_payload",
                    "accepted proposal has no exact revision snapshot",
                )
        for converted_row, converted, lossy in converted_operations:
            operation_row_id = str(converted_row["id"])
            operation_id = str(converted["operation_id"])
            accepted = False
            if status == "accepted":
                accepted = True
            elif status == "partially_accepted":
                accepted = (
                    converted_row["decision"] == "accepted"
                    or operation_row_id in selected
                    or operation_id in selected
                )
            decision = (
                "accepted"
                if accepted
                else (
                    "rejected"
                    if status in {"accepted", "partially_accepted", "rejected"}
                    else converted_row["decision"]
                )
            )
            if accepted:
                accepted_ids.append(operation_id)
            connection.execute(
                sa.text(
                    """
                    UPDATE resume.proposal_operations
                    SET operation_id = :operation_id,
                        operation_type = :operation_type,
                        payload = :payload,
                        fingerprint = :fingerprint,
                        applied_revision_no = :applied_revision,
                        decision = :decision,
                        extensions = :extensions
                    WHERE id = :row_id
                    """
                ).bindparams(
                    sa.bindparam("payload", type_=postgresql.JSONB()),
                    sa.bindparam("extensions", type_=postgresql.JSONB()),
                ),
                {
                    "operation_id": operation_id,
                    "operation_type": str(converted["op"]),
                    "payload": converted,
                    "fingerprint": _operation_fingerprint(converted),
                    "applied_revision": applied_revision if accepted else None,
                    "decision": decision,
                    "extensions": _migration_extension(
                        converted_row["extensions"],
                        source_table="resume.proposal_operations",
                        source_row_id=operation_row_id,
                        lossy_projection=lossy,
                        classification=classification,
                    ),
                    "row_id": operation_row_id,
                },
            )
        title_value = runtime.get("title")
        title = (
            title_value
            if isinstance(title_value, str) and 1 <= len(title_value) <= 300
            else f"Migrated proposal {proposal_id}"[:300]
        )
        decision_payload = {
            "accepted_operation_ids": accepted_ids,
            "migration": {
                "converter_version": _CONVERTER_VERSION,
                "classification": classification,
                "archive_id": _stable_id("rv1arc", "resume.proposals", proposal_id),
            },
        }
        connection.execute(
            sa.text(
                """
                UPDATE resume.proposals
                SET title = :title,
                    evidence_refs = '[]'::jsonb,
                    status = :status,
                    decision_payload = :decision_payload,
                    decided_by_actor_id = :decided_by,
                    decided_at = :decided_at,
                    extensions = :extensions
                WHERE id = :row_id
                """
            ).bindparams(
                sa.bindparam("decision_payload", type_=postgresql.JSONB()),
                sa.bindparam("extensions", type_=postgresql.JSONB()),
            ),
            {
                "title": title,
                "status": status,
                "decision_payload": decision_payload,
                "decided_by": decided_by,
                "decided_at": decided_at,
                "extensions": _migration_extension(
                    proposal["extensions"],
                    source_table="resume.proposals",
                    source_row_id=proposal_id,
                    lossy_projection=any_lossy,
                    classification=classification,
                ),
                "row_id": proposal_id,
            },
        )


def _job_extension(value: object) -> dict[str, Any]:
    """@brief 读取 legacy Job runtime extensions / Read legacy Job runtime extensions."""
    if not isinstance(value, dict):
        return {}
    runtime = value.get("runtime")
    if not isinstance(runtime, dict):
        return {}
    nested = runtime.get("extensions")
    return nested if isinstance(nested, dict) else {}


def _backfill_resume_jobs(migrated_at: datetime) -> int:
    """@brief 保留终态 Job，并将无可证明 worker spec 的活跃 Job 安全终结 / Safely classify legacy Resume jobs."""
    connection = op.get_bind()
    rows = list(
        connection.execute(
            sa.text(
                """
                SELECT job.*,
                       render.resume_id AS linked_resume_id,
                       revision.revision_no AS linked_revision_no
                FROM agent.jobs AS job
                LEFT JOIN resume.render_jobs AS render ON render.job_id = job.id
                LEFT JOIN resume.revisions AS revision ON revision.id = render.resume_revision_id
                WHERE job.job_type LIKE 'resume.%'
                ORDER BY job.id
                """
            )
        ).mappings()
    )
    classified = 0
    for row in rows:
        row_id = str(row["id"])
        _opaque_id(row_id, table="agent.jobs", row_id=row_id, path="id")
        runtime = _job_extension(row["extensions"])
        linked_resume = row["linked_resume_id"] or runtime.get("resume_id")
        existing_target = row["target_resource_id"]
        if isinstance(existing_target, str) and _OPAQUE_ID_PATTERN.fullmatch(existing_target):
            target_id = existing_target
            target_type = row["target_resource_type"] or "resume"
        elif isinstance(linked_resume, str) and _OPAQUE_ID_PATTERN.fullmatch(linked_resume):
            target_id = linked_resume
            target_type = "resume"
        else:
            target_id = str(row["workspace_id"])
            target_type = "workspace"
        if (
            not isinstance(target_type, str)
            or re.fullmatch(r"^[a-z][a-z0-9_.-]{2,100}$", target_type) is None
        ):
            target_type = "resume" if target_id != row["workspace_id"] else "workspace"
        status = str(row["status"])
        started_at = row["started_at"]
        finished_at = row["finished_at"]
        phase = str(row["phase"])
        classification = "legacy_terminal_preserved"
        error = row["error"]
        if status in {"queued", "running"}:
            status = "failed"
            started_at = started_at or row["created_at"]
            finished_at = migrated_at
            phase = "migration_terminal"
            error = {
                "code": "resume.legacy_job_unreplayable",
                "detail": "Legacy Resume job had no complete V2 worker specification",
                "retryable": False,
            }
            classification = "terminal_unreplayable"
            classified += 1
        elif status in {"succeeded", "failed"}:
            started_at = started_at or row["created_at"]
            finished_at = finished_at or row["updated_at"] or started_at
        elif status == "cancelled":
            finished_at = finished_at or row["updated_at"] or row["created_at"]
        elif status == "expired":
            started_at = None
            finished_at = finished_at or row["updated_at"] or row["created_at"]
        else:
            raise _row_error("agent.jobs", row_id, "status", "unknown legacy Job status")
        request_payload = {
            "migration": {
                "converter_version": _CONVERTER_VERSION,
                "classification": classification,
                "archive_id": _stable_id("rv1arc", "agent.jobs", row_id),
                "linked_revision": row["linked_revision_no"],
            }
        }
        connection.execute(
            sa.text(
                """
                UPDATE agent.jobs
                SET status = :status,
                    phase = :phase,
                    target_resource_type = :target_type,
                    target_resource_id = :target_id,
                    request_payload = :request_payload,
                    error = :error,
                    started_at = :started_at,
                    finished_at = :finished_at,
                    extensions = :extensions
                WHERE id = :row_id
                """
            ).bindparams(
                sa.bindparam("request_payload", type_=postgresql.JSONB()),
                sa.bindparam("error", type_=postgresql.JSONB()),
                sa.bindparam("extensions", type_=postgresql.JSONB()),
            ),
            {
                "status": status,
                "phase": phase[:80] or "migration_terminal",
                "target_type": target_type,
                "target_id": target_id,
                "request_payload": request_payload,
                "error": error,
                "started_at": started_at,
                "finished_at": finished_at,
                "extensions": _migration_extension(
                    row["extensions"],
                    source_table="agent.jobs",
                    source_row_id=row_id,
                    lossy_projection=True,
                    classification=classification,
                ),
                "row_id": row_id,
            },
        )
    return classified


def _backfill_resume_outbox() -> None:
    """@brief 将 V1 event data 放入 V2-compatible envelope 且保留原 payload / Wrap V1 event data in a V2-compatible envelope."""
    connection = op.get_bind()
    rows = list(
        connection.execute(
            sa.text(
                """
                SELECT * FROM agent.outbox_events
                WHERE event_type LIKE 'resume.%'
                   OR aggregate_type IN ('resume', 'resume_proposal')
                ORDER BY id
                """
            )
        ).mappings()
    )
    for row in rows:
        row_id = str(row["id"])
        _opaque_id(row_id, table="agent.outbox_events", row_id=row_id, path="id")
        aggregate_id = _opaque_id(
            row["aggregate_id"], table="agent.outbox_events", row_id=row_id, path="aggregate_id"
        )
        aggregate_type = _as_string(
            row["aggregate_type"],
            table="agent.outbox_events",
            row_id=row_id,
            path="aggregate_type",
            minimum=3,
            maximum=100,
        )
        if re.fullmatch(r"^[a-z][a-z0-9_.-]{2,100}$", aggregate_type) is None:
            raise _row_error(
                "agent.outbox_events",
                row_id,
                "aggregate_type",
                "aggregate type is not V2-compatible",
            )
        event_type = _as_string(
            row["event_type"],
            table="agent.outbox_events",
            row_id=row_id,
            path="event_type",
            minimum=3,
            maximum=128,
        )
        if re.fullmatch(r"^[a-z][a-z0-9_.-]{2,127}$", event_type) is None:
            raise _row_error(
                "agent.outbox_events", row_id, "event_type", "event type is not V2-compatible"
            )
        payload = _as_object(
            row["payload"], table="agent.outbox_events", row_id=row_id, path="payload"
        )
        legacy_data = payload.get("data", payload)
        event_data = (
            deepcopy(legacy_data)
            if isinstance(legacy_data, dict)
            else {"legacy_value": deepcopy(legacy_data)}
        )
        envelope = {
            "data": event_data,
            "migration": {
                "converter_version": _CONVERTER_VERSION,
                "archive_id": _stable_id("rv1arc", "agent.outbox_events", row_id),
                "legacy_aggregate_type": aggregate_type,
                "legacy_aggregate_id": aggregate_id,
            },
        }
        trace_id = row["trace_id"]
        if trace_id is not None and (
            not isinstance(trace_id, str) or re.fullmatch(r"^[a-f0-9]{32}$", trace_id) is None
        ):
            trace_id = None
        connection.execute(
            sa.text(
                """
                UPDATE agent.outbox_events
                SET payload = :payload,
                    trace_id = :trace_id,
                    extensions = :extensions
                WHERE id = :row_id
                """
            ).bindparams(
                sa.bindparam("payload", type_=postgresql.JSONB()),
                sa.bindparam("extensions", type_=postgresql.JSONB()),
            ),
            {
                "payload": envelope,
                "trace_id": trace_id,
                "extensions": _migration_extension(
                    row["extensions"],
                    source_table="agent.outbox_events",
                    source_row_id=row_id,
                    lossy_projection=False,
                ),
                "row_id": row_id,
            },
        )


def _widen_domain_ids() -> None:
    """@brief 将 V2 对外 opaque ID 列扩到 160 / Widen V2-facing opaque-ID columns to 160."""
    op.alter_column("jobs", "id", schema="agent", type_=sa.String(160))
    op.alter_column("jobs", "target_resource_id", schema="agent", type_=sa.String(160))
    op.alter_column("outbox_events", "aggregate_id", schema="agent", type_=sa.String(160))
    for schema, table, column in _JOB_REFERENCES:
        op.alter_column(table, column, schema=schema, type_=sa.String(160))
    op.alter_column("documents", "id", schema="resume", type_=sa.String(160))
    for schema, table, column in _RESUME_REFERENCES:
        op.alter_column(table, column, schema=schema, type_=sa.String(160))
    op.alter_column("operation_batches", "client_batch_id", schema="resume", type_=sa.String(160))
    op.alter_column("operations", "operation_id", schema="resume", type_=sa.String(160))
    op.alter_column("proposals", "id", schema="resume", type_=sa.String(160))
    op.alter_column("proposal_operations", "proposal_id", schema="resume", type_=sa.String(160))


def _expand_resume_tables() -> None:
    """@brief 仅扩展 nullable 列，不在 backfill 前锁死非空表 / Add nullable columns before backfill."""
    op.add_column(
        "jobs",
        sa.Column("request_payload", postgresql.JSONB(astext_type=sa.Text())),
        schema="agent",
    )

    op.alter_column("documents", "template_version_id", schema="resume", nullable=True)
    op.add_column("documents", sa.Column("template_id", sa.String(160)), schema="resume")
    op.add_column(
        "documents",
        sa.Column("template_version", sa.String(80)),
        schema="resume",
    )
    op.alter_column("documents", "title", schema="resume", type_=sa.String(300))
    op.alter_column("documents", "locale", schema="resume", type_=sa.String(35))

    op.add_column(
        "revisions",
        sa.Column("change_targets", postgresql.JSONB(astext_type=sa.Text())),
        schema="resume",
    )

    op.alter_column("operation_batches", "base_revision_no", schema="resume", nullable=True)
    op.alter_column("operation_batches", "conflict_strategy", schema="resume", nullable=True)
    op.add_column(
        "operation_batches",
        sa.Column("request_fingerprint", sa.String(64)),
        schema="resume",
    )
    op.add_column(
        "operation_batches",
        sa.Column("outcome", postgresql.JSONB(astext_type=sa.Text())),
        schema="resume",
    )
    op.add_column(
        "operation_batches",
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        schema="resume",
    )
    op.add_column("operations", sa.Column("fingerprint", sa.String(64)), schema="resume")
    op.add_column(
        "operations",
        sa.Column("applied_revision_no", sa.Integer()),
        schema="resume",
    )

    op.add_column("proposals", sa.Column("title", sa.String(300)), schema="resume")
    op.add_column(
        "proposals",
        sa.Column("evidence_refs", postgresql.JSONB(astext_type=sa.Text())),
        schema="resume",
    )

    op.add_column(
        "proposal_operations",
        sa.Column("operation_id", sa.String(160)),
        schema="resume",
    )
    op.add_column(
        "proposal_operations",
        sa.Column("fingerprint", sa.String(64)),
        schema="resume",
    )
    op.add_column(
        "proposal_operations",
        sa.Column("applied_revision_no", sa.Integer()),
        schema="resume",
    )


def _first_invalid(statement: str) -> RowMapping | None:
    """@brief 返回固定验证 SQL 的第一个异常行 / Return the first row from a static validation query."""
    return op.get_bind().execute(sa.text(statement)).mappings().first()


def _validate_backfill(archive_counts: Mapping[str, int]) -> None:
    """@brief 在增加 NOT NULL/unique/check 前验证完整转换 / Validate the complete backfill before constraints."""
    expected_archives = sum(archive_counts.values())
    actual_archives = _count(f"SELECT count(*) FROM {_ARCHIVE_TABLE}")
    if actual_archives != expected_archives:
        raise RuntimeError(
            f"legacy Resume archive count mismatch: expected {expected_archives}, got {actual_archives}"
        )
    invalid_archive = _first_invalid(
        f"SELECT source_table, source_row_id, converter_version, payload_sha256, "
        f"jsonb_typeof(source_payload) AS payload_type FROM {_ARCHIVE_TABLE} "
        "WHERE converter_version <> 'resume-v1-to-v2/1' "
        "OR payload_sha256 !~ '^[0-9a-f]{64}$' OR jsonb_typeof(source_payload) <> 'object' "
        "ORDER BY source_table, source_row_id LIMIT 1"
    )
    if invalid_archive is not None:
        raise _row_error(
            str(invalid_archive["source_table"]),
            str(invalid_archive["source_row_id"]),
            "archive",
            "archive checksum/version/payload validation failed "
            f"(version={invalid_archive['converter_version']!r}, "
            f"checksum={invalid_archive['payload_sha256']!r}, "
            f"payload_type={invalid_archive['payload_type']!r})",
        )
    validations = (
        (
            "resume.documents",
            "SELECT id FROM resume.documents WHERE template_id IS NULL OR template_version IS NULL "
            "OR title <> btrim(title) OR length(title) NOT BETWEEN 1 AND 300 "
            "OR locale !~ '^[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*$' "
            "OR current_revision_no < 1 OR revision <> current_revision_no ORDER BY id LIMIT 1",
            "root metadata is incomplete after backfill",
        ),
        (
            "resume.revisions",
            "SELECT id FROM resume.revisions WHERE change_targets IS NULL "
            "OR jsonb_typeof(change_targets) <> 'array' "
            "OR jsonb_typeof(semantic_document) <> 'object' "
            "OR content_hash !~ '^[0-9a-f]{64}$' ORDER BY id LIMIT 1",
            "snapshot or causal targets are invalid after backfill",
        ),
        (
            "resume.operations",
            "SELECT operation.id FROM resume.operations AS operation "
            "JOIN resume.operation_batches AS batch ON batch.id = operation.batch_id "
            "WHERE batch.status <> 'applied' OR operation.fingerprint IS NULL "
            "OR operation.fingerprint !~ '^[0-9a-f]{64}$' "
            "OR operation.applied_revision_no IS NULL OR operation.applied_revision_no < 1 "
            "OR jsonb_typeof(operation.payload) <> 'object' ORDER BY operation.id LIMIT 1",
            "ledger entry is not an applied V2 operation",
        ),
        (
            "resume.proposals",
            "SELECT id FROM resume.proposals WHERE title IS NULL OR length(title) NOT BETWEEN 1 AND 300 "
            "OR evidence_refs IS NULL OR jsonb_typeof(evidence_refs) <> 'array' "
            "OR status NOT IN ('pending','accepted','partially_accepted','rejected','expired') "
            "OR NOT ((status = 'pending' AND decided_by_actor_id IS NULL AND decided_at IS NULL) "
            "OR (status = 'expired' AND decided_by_actor_id IS NULL AND decided_at IS NOT NULL) "
            "OR (status IN ('accepted','partially_accepted','rejected') "
            "AND decided_by_actor_id IS NOT NULL AND decided_at IS NOT NULL)) ORDER BY id LIMIT 1",
            "proposal lifecycle is invalid after backfill",
        ),
        (
            "resume.proposal_operations",
            "SELECT id FROM resume.proposal_operations WHERE operation_id IS NULL "
            "OR fingerprint IS NULL OR fingerprint !~ '^[0-9a-f]{64}$' "
            "OR jsonb_typeof(payload) <> 'object' "
            "OR (applied_revision_no IS NOT NULL AND applied_revision_no < 1) ORDER BY id LIMIT 1",
            "proposal operation is incomplete after backfill",
        ),
        (
            "agent.jobs",
            "SELECT id FROM agent.jobs WHERE job_type LIKE 'resume.%' AND (request_payload IS NULL "
            "OR jsonb_typeof(request_payload) <> 'object' OR target_resource_type IS NULL "
            "OR target_resource_id IS NULL) ORDER BY id LIMIT 1",
            "Resume Job lacks a safe V2 classification",
        ),
    )
    for table, statement, detail in validations:
        row = _first_invalid(statement)
        if row is not None:
            raise _row_error(table, str(row["id"]), "backfill", detail)
    duplicate_batch = _first_invalid(
        "SELECT min(id) AS id FROM resume.operation_batches "
        "GROUP BY workspace_id, resume_id, client_batch_id HAVING count(*) > 1 LIMIT 1"
    )
    if duplicate_batch is not None:
        raise _row_error(
            "resume.operation_batches",
            str(duplicate_batch["id"]),
            "client_batch_id",
            "V2 Workspace/Resume batch identity is ambiguous",
        )
    duplicate_proposal_operation = _first_invalid(
        "SELECT min(id) AS id FROM resume.proposal_operations "
        "GROUP BY proposal_id, operation_id HAVING count(*) > 1 LIMIT 1"
    )
    if duplicate_proposal_operation is not None:
        raise _row_error(
            "resume.proposal_operations",
            str(duplicate_proposal_operation["id"]),
            "operation_id",
            "converted proposal operation identity is ambiguous",
        )


def _constrain_resume_tables() -> None:
    """@brief 在 backfill 验证后收紧 V2 约束 / Tighten V2 constraints after validated backfill."""
    op.create_check_constraint(
        "jobs_resume_request_payload",
        "jobs",
        "job_type NOT LIKE 'resume.%' OR request_payload IS NOT NULL",
        schema="agent",
    )
    op.alter_column("documents", "template_id", schema="resume", nullable=False)
    op.alter_column("documents", "template_version", schema="resume", nullable=False)
    op.create_check_constraint(
        "resume_documents_v2_title",
        "documents",
        "title = btrim(title) AND length(title) BETWEEN 1 AND 300",
        schema="resume",
    )
    op.create_check_constraint(
        "resume_documents_v2_locale",
        "documents",
        "locale ~ '^[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*$'",
        schema="resume",
    )
    op.create_check_constraint(
        "resume_documents_v2_revision",
        "documents",
        "current_revision_no >= 1 AND revision = current_revision_no",
        schema="resume",
    )
    op.alter_column(
        "revisions",
        "change_targets",
        schema="resume",
        nullable=False,
        server_default=sa.text("'[]'::jsonb"),
    )
    op.create_unique_constraint(
        "resume_batches_v2_client_id",
        "operation_batches",
        ["workspace_id", "resume_id", "client_batch_id"],
        schema="resume",
    )
    op.create_check_constraint(
        "resume_batches_v2_receipt",
        "operation_batches",
        "(request_fingerprint IS NULL AND outcome IS NULL AND expires_at IS NULL) OR "
        "(status = 'applied' AND request_fingerprint IS NOT NULL "
        "AND outcome IS NOT NULL AND expires_at IS NOT NULL)",
        schema="resume",
    )
    op.create_index(
        "ix_resume_operation_batches_receipt_expiry",
        "operation_batches",
        ["expires_at", "id"],
        schema="resume",
        postgresql_where=sa.text("request_fingerprint IS NOT NULL"),
    )
    op.alter_column("operations", "fingerprint", schema="resume", nullable=False)
    op.alter_column("operations", "applied_revision_no", schema="resume", nullable=False)
    op.alter_column("proposals", "title", schema="resume", nullable=False)
    op.alter_column(
        "proposals",
        "evidence_refs",
        schema="resume",
        nullable=False,
        server_default=sa.text("'[]'::jsonb"),
    )
    op.drop_constraint("resume_proposals_status", "proposals", schema="resume", type_="check")
    op.create_check_constraint(
        "resume_proposals_status",
        "proposals",
        "status IN ('pending', 'accepted', 'partially_accepted', 'rejected', 'expired')",
        schema="resume",
    )
    op.create_check_constraint(
        "resume_proposals_v2_decision",
        "proposals",
        "(status = 'pending' AND decided_by_actor_id IS NULL AND decided_at IS NULL) OR "
        "(status = 'expired' AND decided_by_actor_id IS NULL AND decided_at IS NOT NULL) OR "
        "(status IN ('accepted', 'partially_accepted', 'rejected') "
        "AND decided_by_actor_id IS NOT NULL AND decided_at IS NOT NULL)",
        schema="resume",
    )
    op.alter_column("proposal_operations", "operation_id", schema="resume", nullable=False)
    op.alter_column("proposal_operations", "fingerprint", schema="resume", nullable=False)
    op.create_unique_constraint(
        "resume_proposal_operations_operation",
        "proposal_operations",
        ["proposal_id", "operation_id"],
        schema="resume",
    )


def _create_upload_sessions() -> None:
    """@brief 创建不伪造完成态的最小 upload-session 表 / Create the minimal upload-session table without fabricating completion."""
    op.create_table(
        "import_upload_sessions",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "claimed_by_job_id",
            sa.String(160),
            sa.ForeignKey(
                "agent.jobs.id",
                ondelete="RESTRICT",
                deferrable=True,
                initially="DEFERRED",
            ),
            unique=True,
        ),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "status IN ('created', 'uploaded', 'verifying', 'completed', 'failed', 'expired')",
            name="resume_import_upload_sessions_status",
        ),
        sa.CheckConstraint(
            "(status = 'completed' AND completed_at IS NOT NULL "
            "AND ((claimed_by_job_id IS NULL AND consumed_at IS NULL) "
            "OR (claimed_by_job_id IS NOT NULL AND consumed_at IS NOT NULL))) "
            "OR (status <> 'completed' AND claimed_by_job_id IS NULL AND consumed_at IS NULL)",
            name="resume_import_upload_sessions_lifecycle",
        ),
        schema="resume",
    )
    op.create_index(
        "ix_resume_import_upload_sessions_claimable",
        "import_upload_sessions",
        ["workspace_id", "expires_at"],
        schema="resume",
        postgresql_where=sa.text("status = 'completed' AND claimed_by_job_id IS NULL"),
    )


def _secure_resume_storage(*, app_role: str, dashboard_role: str, migrator_role: str) -> None:
    """@brief 配置 append-only revision/ledger 与 upload FORCE RLS / Secure append-only revisions, ledger, and upload RLS."""
    for table in ("resume.revisions", "resume.operation_batches", "resume.operations"):
        op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY resume_v2_append_read ON {table} AS PERMISSIVE FOR SELECT "
            f"TO {app_role} USING (workspace_id = current_setting('app.workspace_id', true))"
        )
        op.execute(
            f"CREATE POLICY resume_v2_append_insert ON {table} AS PERMISSIVE FOR INSERT "
            f"TO {app_role} WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )
    table = "resume.import_upload_sessions"
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {table} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON TABLE {table} TO {app_role}")
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY resume_v2_upload_workspace_scope ON {table} "
        f"AS PERMISSIVE FOR ALL TO {app_role} "
        "USING (workspace_id = current_setting('app.workspace_id', true)) "
        "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )


def upgrade() -> None:
    """@brief 以 expand/backfill/validate/constrain 原子发布 V2 Resume / Atomically publish V2 Resume."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _install_migration_visibility(owner_role)
    _create_legacy_archive()
    archive_counts, snapshot_sha256 = _archive_legacy_rows()
    has_legacy_rows = any(archive_counts.values())
    if has_legacy_rows:
        _write_migration_audit(
            "backup_created",
            0,
            snapshot_sha256,
            {
                "converter_version": _CONVERTER_VERSION,
                "archive_table": _ARCHIVE_TABLE,
                "source_counts": archive_counts,
            },
        )
        _write_migration_audit(
            "started",
            1,
            snapshot_sha256,
            {"strategy": "expand-backfill-validate-constrain"},
        )
    _widen_domain_ids()
    _expand_resume_tables()
    migrated_at = datetime.now(UTC)
    current_documents, revision_documents = _backfill_documents_and_revisions()
    _backfill_operation_ledger(
        current_documents,
        revision_documents,
        migrated_at,
    )
    _backfill_proposals(
        current_documents,
        revision_documents,
        migrated_at,
    )
    terminal_jobs = _backfill_resume_jobs(migrated_at)
    _backfill_resume_outbox()
    _validate_backfill(archive_counts)
    if has_legacy_rows:
        _write_migration_audit(
            "verified",
            4,
            snapshot_sha256,
            {
                "archive_rows": sum(archive_counts.values()),
                "terminal_jobs": terminal_jobs,
            },
        )
    _constrain_resume_tables()
    _create_upload_sessions()
    _secure_resume_storage(
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    _seal_legacy_archive(
        owner_role=owner_role,
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    if has_legacy_rows:
        _write_migration_audit(
            "completed",
            5,
            snapshot_sha256,
            {
                "converter_version": _CONVERTER_VERSION,
                "archive_rows": sum(archive_counts.values()),
            },
        )
    _remove_migration_visibility()


def _preflight_downgrade() -> None:
    """@brief 非空或不可逆 V2 state 时拒绝 downgrade / Reject downgrade for non-empty or irreversible V2 state."""
    resume_rows = sum(_count(f"SELECT count(*) FROM {table}") for table in _RESUME_TABLES)
    upload_rows = _count("SELECT count(*) FROM resume.import_upload_sessions")
    resume_jobs = _count("SELECT count(*) FROM agent.jobs WHERE job_type LIKE 'resume.%'")
    resume_events = _count(
        "SELECT count(*) FROM agent.outbox_events "
        "WHERE event_type LIKE 'resume.%' OR aggregate_type IN ('resume', 'resume_proposal')"
    )
    extended_jobs = _count(
        "SELECT count(*) FROM agent.jobs WHERE length(id) > 128 "
        "OR length(COALESCE(target_resource_id, '')) > 128 OR request_payload IS NOT NULL"
    )
    extended_events = _count(
        "SELECT count(*) FROM agent.outbox_events WHERE length(aggregate_id) > 128"
    )
    archived_rows = _count(f"SELECT count(*) FROM {_ARCHIVE_TABLE}")
    if (
        resume_rows
        or upload_rows
        or resume_jobs
        or resume_events
        or extended_jobs
        or extended_events
        or archived_rows
    ):
        raise RuntimeError(
            "cannot downgrade non-empty or irreversible API V2 Resume persistence state"
        )


def _restore_legacy_policies(app_role: str) -> None:
    """@brief 仅在空状态 downgrade 时恢复旧 policy / Restore legacy policies only during an empty-state downgrade."""
    for table in ("resume.revisions", "resume.operation_batches", "resume.operations"):
        op.execute(f"DROP POLICY resume_v2_append_insert ON {table}")
        op.execute(f"DROP POLICY resume_v2_append_read ON {table}")
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY workspace_app_tenant_scope ON {table} AS PERMISSIVE FOR ALL "
            f"TO {app_role} USING ("
            "workspace_id = current_setting('app.workspace_id', true)) WITH CHECK ("
            "workspace_id = current_setting('app.workspace_id', true))"
        )


def downgrade() -> None:
    """@brief 仅允许空、可逆状态回退 / Permit rollback only for empty, reversible state."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    _install_migration_visibility(owner_role)
    op.execute(
        f"CREATE POLICY {_MIGRATION_POLICY} ON resume.import_upload_sessions "
        f"AS PERMISSIVE FOR ALL TO {owner_role} USING (true) WITH CHECK (true)"
    )
    _preflight_downgrade()
    _restore_legacy_policies(app_role)
    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON resume.import_upload_sessions")
    op.drop_table("import_upload_sessions", schema="resume")

    op.execute(f"DROP POLICY resume_v1_archive_owner_read ON {_ARCHIVE_TABLE}")
    op.execute(f"DROP TRIGGER {_ARCHIVE_TRIGGER} ON {_ARCHIVE_TABLE}")
    op.execute("DROP FUNCTION resume.reject_v1_migration_archive_mutation()")
    op.drop_index(
        "ix_resume_v1_migration_archive_workspace_source",
        table_name="v1_migration_archive",
        schema="resume",
    )
    op.drop_table("v1_migration_archive", schema="resume")

    op.drop_constraint(
        "resume_proposal_operations_operation",
        "proposal_operations",
        schema="resume",
        type_="unique",
    )
    for column in ("applied_revision_no", "fingerprint", "operation_id"):
        op.drop_column("proposal_operations", column, schema="resume")
    op.drop_constraint("resume_proposals_v2_decision", "proposals", schema="resume", type_="check")
    op.drop_constraint("resume_proposals_status", "proposals", schema="resume", type_="check")
    op.create_check_constraint(
        "resume_proposals_status",
        "proposals",
        "status IN ('pending', 'accepted', 'partially_accepted', "
        "'rejected', 'expired', 'conflicted')",
        schema="resume",
    )
    op.drop_column("proposals", "evidence_refs", schema="resume")
    op.drop_column("proposals", "title", schema="resume")
    op.drop_column("operations", "applied_revision_no", schema="resume")
    op.drop_column("operations", "fingerprint", schema="resume")
    op.drop_index(
        "ix_resume_operation_batches_receipt_expiry",
        table_name="operation_batches",
        schema="resume",
    )
    op.drop_constraint(
        "resume_batches_v2_receipt", "operation_batches", schema="resume", type_="check"
    )
    op.drop_constraint(
        "resume_batches_v2_client_id", "operation_batches", schema="resume", type_="unique"
    )
    for column in ("expires_at", "outcome", "request_fingerprint"):
        op.drop_column("operation_batches", column, schema="resume")
    op.alter_column("operation_batches", "conflict_strategy", schema="resume", nullable=False)
    op.alter_column("operation_batches", "base_revision_no", schema="resume", nullable=False)
    op.drop_column("revisions", "change_targets", schema="resume")
    for constraint in (
        "resume_documents_v2_revision",
        "resume_documents_v2_locale",
        "resume_documents_v2_title",
    ):
        op.drop_constraint(constraint, "documents", schema="resume", type_="check")
    op.alter_column("documents", "locale", schema="resume", type_=sa.String(32))
    op.alter_column("documents", "title", schema="resume", type_=sa.String(512))
    op.drop_column("documents", "template_version", schema="resume")
    op.drop_column("documents", "template_id", schema="resume")
    op.alter_column("documents", "template_version_id", schema="resume", nullable=False)
    op.drop_constraint("jobs_resume_request_payload", "jobs", schema="agent", type_="check")
    op.drop_column("jobs", "request_payload", schema="agent")

    op.alter_column("proposal_operations", "proposal_id", schema="resume", type_=sa.String(128))
    op.alter_column("proposals", "id", schema="resume", type_=sa.String(128))
    op.alter_column("operations", "operation_id", schema="resume", type_=sa.String(128))
    op.alter_column("operation_batches", "client_batch_id", schema="resume", type_=sa.String(128))
    for schema, table, column in reversed(_RESUME_REFERENCES):
        op.alter_column(table, column, schema=schema, type_=sa.String(128))
    op.alter_column("documents", "id", schema="resume", type_=sa.String(128))
    for schema, table, column in reversed(_JOB_REFERENCES):
        op.alter_column(table, column, schema=schema, type_=sa.String(128))
    op.alter_column("outbox_events", "aggregate_id", schema="agent", type_=sa.String(128))
    op.alter_column("jobs", "target_resource_id", schema="agent", type_=sa.String(128))
    op.alter_column("jobs", "id", schema="agent", type_=sa.String(128))

    _remove_migration_visibility()
