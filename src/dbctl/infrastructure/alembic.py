"""@brief Alembic migration port 适配器 / Alembic migration-port adapter."""

from alembic import command
from alembic.config import Config

from dbctl.application.errors import MigrationExecutionError
from dbctl.application.migrate import MigrationRevision
from dbctl.domain.database import DatabaseBlueprint, MigratorLogin
from dbctl.infrastructure.resources import alembic_script_location


class AlembicMigrationAdapter:
    """@brief 通过内存配置执行显式 Alembic upgrade / Run explicit Alembic upgrades via in-memory configuration."""

    def upgrade(
        self,
        login: MigratorLogin,
        revision: MigrationRevision,
        blueprint: DatabaseBlueprint,
    ) -> None:
        """@brief 使用强类型 migrator 升级 schema / Upgrade the schema with the typed migrator.

        @param login 与目标数据库绑定的 migrator 登录 / Migrator login bound to the target database.
        @param revision 已验证的 Alembic revision / Validated Alembic revision.
        @param blueprint owner 与运行时角色的非秘密目标状态 / Non-secret desired owner and runtime-role state.
        @return 无返回值 / No return value.
        @raise MigrationExecutionError Alembic 或数据库执行失败时抛出且隐藏底层详情。
        / Raised when Alembic or the database fails, with underlying details hidden.

        @note DSN 只经 ``Config.attributes`` 传递，避免 ConfigParser 对 URI 百分号插值；
        四个 role option 都由领域值对象验证后传给 revision。/ The DSN travels only through
        ``Config.attributes`` to avoid ConfigParser interpolation, while every role option comes
        from a validated domain value object.
        """

        roles = blueprint.roles
        try:
            with alembic_script_location() as script_location:
                configuration = Config()
                configuration.set_main_option("script_location", str(script_location))
                configuration.attributes["aiws.migration_dsn"] = login.dsn.reveal()
                configuration.set_main_option("aiws.owner_role", roles.owner.value)
                configuration.set_main_option("aiws.migrator_role", roles.migrator.value)
                configuration.set_main_option("aiws.app_role", roles.application.value)
                configuration.set_main_option("aiws.dashboard_role", roles.dashboard.value)
                command.upgrade(configuration, revision.value)
        except Exception:
            raise MigrationExecutionError(
                "Alembic migration 失败；底层数据库详情已隐藏。"
            ) from None


__all__ = ["AlembicMigrationAdapter"]
