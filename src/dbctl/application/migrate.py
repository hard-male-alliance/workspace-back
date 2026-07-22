"""@brief 强类型数据库迁移用例 / Strongly typed database-migration use case."""

import re
from dataclasses import dataclass
from typing import Final, Protocol

from dbctl.domain.database import DatabaseBlueprint, DatabaseLogin, DbctlSettings, MigratorLogin
from dbctl.domain.roles import LoginRole

from .errors import DbctlConfigurationError, MigrationExecutionError, add_safe_diagnostic_note
from .progress import (
    OperationName,
    ProgressSink,
    ProgressState,
    ProgressUpdate,
    publish_progress,
)

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


class MigrationPort(Protocol):
    """@brief Alembic 升级的基础设施端口 / Infrastructure port for Alembic upgrades."""

    def upgrade(
        self,
        login: MigratorLogin,
        revision: MigrationRevision,
        blueprint: DatabaseBlueprint,
    ) -> None:
        """@brief 使用 migrator 升级到强类型 revision / Upgrade with migrator to a typed revision.

        @param login 强类型 migrator 登录 / Purpose-typed migrator login.
        @param revision 已验证 Alembic revision / Validated Alembic revision.
        @param blueprint 不含其他登录 secret 的 role 与 schema 目标状态。
        / Role and schema target state without unrelated login secrets.
        @return 无返回值 / No return value.
        """
        ...


class MigrationService:
    """@brief 将迁移身份、revision 与设置交给单一端口 / Delegate typed migration input to one port."""

    def __init__(self, port: MigrationPort, *, progress: ProgressSink | None = None) -> None:
        """@brief 初始化迁移用例 / Initialize the migration use case.

        @param port Alembic 基础设施端口 / Alembic infrastructure port.
        @param progress 可选同步进度输出端口 / Optional synchronous progress output port.
        """
        self._port = port
        self._progress = progress

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
        detail = (
            f"目标={login.target.host}:{login.target.port}/{login.target.database.value}；"
            f"身份={login.role_name.value}；revision={revision.value}"
        )
        self._publish(
            ProgressUpdate(
                operation=OperationName.MIGRATION,
                state=ProgressState.STARTED,
                message="执行 Alembic schema upgrade",
                detail=detail,
            )
        )
        try:
            self._port.upgrade(login, revision, settings.blueprint)
        except Exception as error:
            add_safe_diagnostic_note(error, f"dbctl migrate：{detail}。")
            add_safe_diagnostic_note(
                error,
                "运维影响：migration 未报告完成；重试前请核验数据库当前 revision。",
            )
            self._publish(
                ProgressUpdate(
                    operation=OperationName.MIGRATION,
                    state=ProgressState.FAILED,
                    message="Alembic schema upgrade 未完成",
                    detail="请根据 traceback 核验当前 revision 后再重试",
                )
            )
            raise
        self._publish(
            ProgressUpdate(
                operation=OperationName.MIGRATION,
                state=ProgressState.SUCCEEDED,
                message="Alembic schema upgrade 已完成",
                detail=detail,
            )
        )

    def _publish(self, update: ProgressUpdate) -> None:
        """@brief 向可选输出端口同步发布进度 / Publish progress synchronously to the optional output port.

        @param update 已验证且不含 secret 的进度 / Validated secret-free progress update.
        @return 无返回值 / No return value.
        """

        publish_progress(self._progress, update)
