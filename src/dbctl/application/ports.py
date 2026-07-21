"""@brief dbctl 应用层端口 / Application-layer ports for dbctl."""

from types import TracebackType
from typing import TYPE_CHECKING, Protocol, Self

from dbctl.domain.database import DatabaseBlueprint, LoginDatabase, MigratorLogin
from dbctl.domain.names import DatabaseName

if TYPE_CHECKING:
    from .migrate import MigrationRevision
    from .provision import BootstrapAccessMode, BootstrapPlan, BootstrapStage
    from .prune_telemetry import DeleteTelemetryBatch, StaleTelemetryProbe


class BootstrapRunner(Protocol):
    """@brief 一个 bootstrap 会话的批量执行端口 / Batch-execution port for one bootstrap session."""

    def __enter__(self) -> Self:
        """@brief 进入受控 runner 生命周期 / Enter the controlled runner lifecycle.

        @return 当前 runner / Current runner.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """@brief 关闭 runner 并清理临时凭证 / Close the runner and clean temporary credentials.

        @param exc_type 退出时的异常类型 / Exception type at exit.
        @param exc_value 退出时的异常对象 / Exception instance at exit.
        @param traceback 退出时的 traceback / Traceback at exit.
        @return 无返回值 / No return value.
        """
        ...

    def database_exists(self, database: DatabaseName) -> bool:
        """@brief 判断项目数据库是否存在 / Determine whether the project database exists.

        @param database 强类型数据库名 / Strongly typed database name.
        @return 数据库存在时为真 / True when the database exists.
        """
        ...

    def execute_stage(self, stage: BootstrapStage) -> None:
        """@brief 按 stage 契约批量执行 SQL / Execute SQL as one batch under the stage contract.

        @param stage 已验证的连接目标、事务模式与 SQL 批次 / Validated target, transaction mode, and batch.
        @return 无返回值 / No return value.
        """
        ...


class BootstrapRunnerFactory(Protocol):
    """@brief 按计划与访问模式打开 bootstrap runner / Open a runner for a plan and access mode."""

    def open(
        self,
        plan: BootstrapPlan,
        access_mode: BootstrapAccessMode,
    ) -> BootstrapRunner:
        """@brief 创建尚未进入上下文的 runner / Create a runner not yet entered.

        @param plan 含 maintenance 与项目目标的自足计划 / Self-contained plan with both targets.
        @param access_mode 显式管理权限模式 / Explicit administrative-access mode.
        @return 由应用服务管理生命周期的 BootstrapRunner / Runner lifecycle-owned by application.
        """
        ...


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


class TelemetryRetentionPort(Protocol):
    """@brief 短事务遥测删除端口 / Short-transaction telemetry-deletion port."""

    def delete_batch(self, command: DeleteTelemetryBatch) -> int:
        """@brief 删除一个有界批次 / Delete one bounded batch.

        @param command UTC cutoff 与资源护栏 / UTC cutoff and resource guardrails.
        @return 当前短事务提交的删除数 / Rows deleted and committed in this short transaction.
        """
        ...

    def has_stale(self, probe: StaleTelemetryProbe) -> bool:
        """@brief 探测是否仍有过期记录 / Probe whether stale records remain.

        @param probe UTC cutoff 与查询护栏 / UTC cutoff and query guardrails.
        @return 至少一条过期记录存在时为真 / True when at least one stale record remains.
        """
        ...


class ShellPort(Protocol):
    """@brief 交互式 PostgreSQL shell 启动端口 / Interactive PostgreSQL shell launch port."""

    def launch(self, login: LoginDatabase) -> int:
        """@brief 直接使用强类型登录启动 shell / Launch a shell directly with a typed login.

        @param login config 中验证过的登录身份 / Login identity validated from configuration.
        @return subprocess 风格退出状态 / Subprocess-style exit status.
        """
        ...
