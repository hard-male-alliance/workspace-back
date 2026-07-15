"""@brief 由 dbctl 显式调用的 Alembic 迁移 / Alembic migrations explicitly invoked by dbctl."""

from __future__ import annotations

import re
from pathlib import Path

from .errors import DbctlConfigurationError, DbctlDependencyError, MigrationExecutionError
from .identifiers import validate_postgres_identifier

_REVISION_PATTERN = re.compile(r"^[A-Za-z0-9_+\-]+$")


class AlembicMigrationRunner:
    """@brief 使用迁移专用 DSN 执行 Alembic / Execute Alembic with a migrator-only DSN.

    此 runner 只在 ``dbctl migrate`` 明确调用时运行。DSN 通过 Alembic ``Config`` 的
    内存 option 传入，不进入 shell argv，也不写入项目配置文件。
    / This runner runs only when ``dbctl migrate`` is explicitly called. The DSN is supplied via an
    in-memory Alembic ``Config`` option, never shell argv or a project configuration file.
    """

    def __init__(
        self,
        migrator_dsn: str,
        script_location: Path,
        owner_role: str,
        app_role: str,
        dashboard_role: str,
    ) -> None:
        """@brief 初始化 Alembic runner / Initialize the Alembic runner.

        @param migrator_dsn 迁移专用 PostgreSQL DSN，仅保存在私有内存字段。
        / Migrator-only PostgreSQL DSN, retained only in a private in-memory field.
        @param script_location Alembic ``alembic`` 脚本目录 / Alembic ``alembic`` script directory.
        @param owner_role 受 bootstrap 管理、供 migrator 显式 ``SET ROLE`` 的对象 owner。
        / Bootstrap-managed object owner explicitly assumed by the migrator.
        @param app_role 后端运行时最小 DML role / Backend runtime least-privilege DML role.
        @param dashboard_role 运维稳定视图的只读 role / Read-only role for operational views.
        @raise DbctlConfigurationError DSN 为空、脚本目录不存在或 role 非法时抛出。
        / Raised when DSN is empty or script directory does not exist.
        """
        if not isinstance(migrator_dsn, str) or not migrator_dsn:
            raise DbctlConfigurationError("迁移 PostgreSQL DSN 必须是非空字符串。")
        if not script_location.is_dir():
            raise DbctlConfigurationError("未找到 Alembic 脚本目录。")
        self._migrator_dsn = migrator_dsn
        self._script_location = script_location
        self._owner_role = validate_postgres_identifier(owner_role, kind="owner role")
        self._app_role = validate_postgres_identifier(app_role, kind="app role")
        self._dashboard_role = validate_postgres_identifier(
            dashboard_role,
            kind="dashboard role",
        )

    def upgrade(self, revision: str = "head") -> None:
        """@brief 将数据库显式迁移到指定 revision / Explicitly migrate database to a revision.

        @param revision Alembic revision，默认 ``head`` / Alembic revision, defaults to ``head``.
        @return 无返回值 / No return value.
        @raise DbctlDependencyError Alembic 依赖不可用时抛出。
        / Raised when the Alembic dependency is unavailable.
        @raise MigrationExecutionError 迁移失败时抛出，且不回显底层凭证或 SQL 错误。
        / Raised on migration failure without echoing underlying credential or SQL errors.
        """
        if not isinstance(revision, str) or not _REVISION_PATTERN.fullmatch(revision):
            raise DbctlConfigurationError("Alembic revision 只能包含字母、数字、下划线、加号或减号。")
        try:
            from alembic import command
            from alembic.config import Config
        except ImportError as error:
            raise DbctlDependencyError("执行 migration 需要 Alembic 依赖。") from error

        config = Config()
        config.set_main_option("script_location", str(self._script_location))
        config.set_main_option("sqlalchemy.url", self._migrator_dsn)
        # Owner has a dbctl-validated identifier and is passed only through in-memory Config.
        config.set_main_option("aiws.owner_role", self._owner_role)
        config.set_main_option("aiws.app_role", self._app_role)
        config.set_main_option("aiws.dashboard_role", self._dashboard_role)
        try:
            command.upgrade(config, revision)
        except Exception as error:
            raise MigrationExecutionError("Alembic migration 失败；底层数据库详情已隐藏。") from error
