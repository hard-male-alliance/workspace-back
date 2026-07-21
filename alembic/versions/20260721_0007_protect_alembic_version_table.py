"""@brief 保护 Alembic 版本表权限边界 / Protect the Alembic version-table privilege boundary.

Revision ID: 20260721_0007
Revises: 20260721_0006
Create Date: 2026-07-21

@note ``identity.alembic_version`` 是迁移控制面元数据，不是业务表。历史 bootstrap 与
``0001`` 对 ``identity`` schema 的宽泛表授权会让 app 获得该表的 DML 权限；本 revision
只收紧该控制面 relation，不读取或改写任何业务数据。
/ ``identity.alembic_version`` is migration control-plane metadata, not a business table.
Historical broad table grants on the ``identity`` schema gave the app role DML privileges on it;
this revision tightens only that control-plane relation and reads or mutates no business data.
"""

from __future__ import annotations

import re
from typing import Literal

from alembic import op

revision = "20260721_0007"
"""@brief 当前 Alembic revision 标识 / Current Alembic revision identifier."""

down_revision = "20260721_0006"
"""@brief 直接父 revision 标识 / Immediate parent revision identifier."""

branch_labels = None
"""@brief 本 revision 不创建分支标签 / This revision creates no branch label."""

depends_on = None
"""@brief 本 revision 没有额外依赖 / This revision has no additional dependency."""

RuntimeRoleOption = Literal["app_role", "dashboard_role", "migrator_role"]
"""@brief 可由本 revision 读取的运行时 role 配置键 / Runtime role options readable by this revision."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief dbctl 传入 role 标识符白名单 / Allowlist for role identifiers supplied by dbctl."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL 标识符最大字节数 / PostgreSQL identifier byte limit."""

_VERSION_TABLE = "identity.alembic_version"
"""@brief 受保护的 Alembic 控制面 relation / Protected Alembic control-plane relation."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 读取并安全引用 dbctl migration role / Read and safely quote a dbctl migration role.

    @param option app、dashboard 或 migrator role 的固定配置键。
    / Fixed configuration key for the app, dashboard, or migrator role.
    @return 双引号引用的 PostgreSQL role 标识符 / Double-quoted PostgreSQL role identifier.
    @raise RuntimeError Alembic 配置缺失或 role 不满足可移植标识符约束时抛出。
    / Raised when Alembic configuration is absent or the role violates portable identifier rules.

    @note role 值只从 dbctl 写入的 Alembic ``Config`` 读取；不读取环境变量，也不把未经
    校验的配置拼接进 SQL。/ Role values come only from the Alembic ``Config`` populated by
    dbctl; environment variables are not consulted and unvalidated values never reach SQL.
    """
    migration_config = op.get_context().config
    if migration_config is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = migration_config.get_main_option(f"aiws.{option}")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES
    ):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """@brief 撤销非 owner 对版本表的直接权限 / Revoke non-owner direct privileges on the version table.

    @return 无返回值 / No return value.

    @note migrator 仍可通过 bootstrap 明确授予的 membership 执行 ``SET ROLE owner``；
    它不需要、也不应拥有版本表的直接权限。/ The migrator can still use its explicit
    bootstrap membership to ``SET ROLE owner``; it neither needs nor should have direct privileges.
    """
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {_VERSION_TABLE} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )


def downgrade() -> None:
    """@brief 精确恢复 0006 的 app 版本表 DML 权限 / Restore exactly the 0006 app DML privileges.

    @return 无返回值 / No return value.

    @note PUBLIC、dashboard 与 migrator 在 0006 状态均无须直接访问版本表，因此 downgrade
    不向它们授予权限。/ PUBLIC, dashboard, and migrator require no direct version-table access
    in the 0006 state, so the downgrade grants them nothing.
    """
    app_role = _configured_role("app_role")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {_VERSION_TABLE} TO {app_role}")
