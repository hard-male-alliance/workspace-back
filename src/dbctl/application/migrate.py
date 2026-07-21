"""@brief 强类型数据库迁移用例 / Strongly typed database-migration use case."""

import re
from dataclasses import dataclass
from typing import Final

from dbctl.domain.database import DatabaseLogin, DbctlSettings, MigratorLogin
from dbctl.domain.roles import LoginRole

from .errors import DbctlConfigurationError, MigrationExecutionError
from .ports import MigrationPort

_REVISION_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9_+\-]+$")
"""@brief Alembic revision 的安全白名单 / Safe allow-list for Alembic revisions."""


@dataclass(frozen=True, slots=True)
class MigrationRevision:
    """@brief 已验证的 Alembic revision 值对象 / Validated Alembic revision value object.

    @param value ``head``、revision id 或 Alembic 相对表达式 / ``head``, revision id, or relative expression.
    """

    value: str = "head"

    def __post_init__(self) -> None:
        """@brief 校验 revision 文法 / Validate revision grammar.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError revision 超出命令白名单时抛出。
        / Raised when the revision falls outside the command allow-list.
        """
        if not isinstance(self.value, str) or not _REVISION_PATTERN.fullmatch(self.value):
            raise DbctlConfigurationError(
                "Alembic revision 只能包含字母、数字、下划线、加号或减号。"
            )

    def __str__(self) -> str:
        """@brief 返回 revision 文本 / Return revision text.

        @return Alembic revision 表达式 / Alembic revision expression.
        """
        return self.value


HEAD_REVISION: Final[MigrationRevision] = MigrationRevision()
"""@brief 默认迁移目标 head / Default migration target ``head``."""


class MigrationService:
    """@brief 将迁移身份、revision 与设置交给单一端口 / Delegate typed migration input to one port."""

    def __init__(self, port: MigrationPort) -> None:
        """@brief 初始化迁移用例 / Initialize the migration use case.

        @param port Alembic 基础设施端口 / Alembic infrastructure port.
        """
        self._port = port

    def execute(
        self,
        login: MigratorLogin,
        revision: MigrationRevision,
        settings: DbctlSettings,
    ) -> None:
        """@brief 显式执行目标 revision 升级 / Explicitly execute an upgrade to a revision.

        @param login 强类型 migrator 登录 / Purpose-typed migrator login.
        @param revision 强类型迁移目标 / Strongly typed migration target.
        @param settings 已验证 dbctl 领域设置 / Validated dbctl domain settings.
        @return 无返回值 / No return value.
        """
        if not isinstance(login, DatabaseLogin) or login.role is not LoginRole.MIGRATOR:
            raise MigrationExecutionError("migration 用例只接受 migrator 登录。")
        if not isinstance(revision, MigrationRevision):
            raise MigrationExecutionError("migration revision 必须是 MigrationRevision。")
        if not isinstance(settings, DbctlSettings):
            raise MigrationExecutionError("migration settings 必须是 DbctlSettings。")
        if login != settings.connections.migrator:
            raise MigrationExecutionError("migration 登录必须来自同一份 DbctlSettings。")
        self._port.upgrade(login, revision, settings.blueprint)
