"""@brief dbctl 独立 composition root / Independent composition root for dbctl."""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from pathlib import Path

from .bootstrap import (
    BootstrapExecutionResult,
    BootstrapExecutor,
    BootstrapPlan,
    BootstrapPlanBuilder,
)
from .config import (
    DatabaseRole,
    DbctlConfigurationService,
    DbctlSettings,
)
from .migration import AlembicMigrationRunner
from .package_resources import alembic_script_location
from .retention import (
    DEFAULT_TELEMETRY_PRUNE_BATCH_SIZE,
    DEFAULT_TELEMETRY_PRUNE_LOCK_TIMEOUT_MS,
    DEFAULT_TELEMETRY_PRUNE_MAX_BATCHES,
    DEFAULT_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS,
    PsycopgTelemetryRetentionRunner,
    TelemetryPruneExecutor,
    TelemetryPruneRequest,
    TelemetryPruneResult,
)
from .runners import BootstrapAccessMode, LocalPsqlBootstrapRunner
from .shell import PasswordPolicy, PreparedPsqlCommand, PsqlShellLauncher


@dataclass(slots=True)
class DbctlComposition:
    """@brief 将 dbctl 配置、计划和 I/O 适配器装配在一起 / Compose dbctl configuration, plans, and I/O adapters.

    与 backend、dashboard 的 composition root 相互独立。这里不启动 FastAPI、不运行
    migration，也不修改 PostgreSQL 认证文件；只在 CLI 明确调用时装配 bootstrap 或
    交互式 ``psql``。
    / This composition root is independent from backend and dashboard roots. It starts no FastAPI,
    runs no migration, and changes no PostgreSQL authentication file; it composes bootstrap or
    interactive ``psql`` only when explicitly called by CLI.
    """

    settings: DbctlSettings
    _environ: MutableMapping[str, str] = field(default_factory=lambda: os.environ, repr=False)

    @classmethod
    def from_config_path(
        cls,
        config_path: Path | str | None = None,
        *,
        dbinit_path: Path | str | None = None,
        environ: MutableMapping[str, str] | None = None,
    ) -> DbctlComposition:
        """@brief 从 JSONC 配置构造 composition root / Construct composition root from JSONC configuration.

        @param config_path 根 JSONC 配置路径；``None`` 使用可回退到安装包资源的默认路径。
        / Root JSONC configuration path; ``None`` uses the default with installed-resource fallback.
        @param dbinit_path 独立数据库初始化声明路径 / Separate database-initialization declaration path.
        @param environ 可选可变环境映射；默认使用当前 ``os.environ``。
        / Optional mutable environment mapping; defaults to current ``os.environ``.
        @return 已加载但尚未执行外部 I/O 的 DbctlComposition。
        / DbctlComposition loaded without executing external I/O.
        """
        configuration_service = DbctlConfigurationService(config_path, dbinit_path)
        loaded_settings = configuration_service.load()
        return cls(
            settings=loaded_settings,
            _environ=os.environ if environ is None else environ,
        )

    def build_bootstrap_plan(self) -> BootstrapPlan:
        """@brief 生成 bootstrap 计划但不执行 / Build a bootstrap plan without executing it.

        @return 可用于 dry-run 或执行的 BootstrapPlan / BootstrapPlan usable for dry-run or execution.
        @raise DbctlConfigurationError config.jsonc 的 DSN 身份与 dbinit role 不匹配时抛出。
        / Raised when a config.jsonc DSN identity mismatches its dbinit role.
        """
        return BootstrapPlanBuilder().build(
            self.settings.administration,
            role_passwords=self._resolve_role_passwords(),
        )

    def execute_bootstrap(
        self,
        plan: BootstrapPlan,
        *,
        access_mode: BootstrapAccessMode | str = BootstrapAccessMode.AUTO,
    ) -> BootstrapExecutionResult:
        """@brief 执行已生成的 bootstrap 计划 / Execute a generated bootstrap plan.

        @param plan 待执行的 BootstrapPlan / BootstrapPlan to execute.
        @param access_mode 自动、sudo 或终端密码模式 / Auto, sudo, or terminal-password mode.
        @return 不含 DSN 或密码的执行摘要 / Execution summary containing no DSN or password.
        @note Windows 或找不到 sudo 时，auto 会改为提示 PostgreSQL 管理角色密码。
        / On Windows or when sudo is unavailable, auto prompts for the PostgreSQL administrator password.
        """
        runner = self._bootstrap_runner(access_mode=access_mode)
        try:
            return BootstrapExecutor(runner).apply(plan)
        finally:
            runner.close()

    def execute_migration(self, revision: str = "head") -> None:
        """@brief 显式执行 Alembic migration / Explicitly execute Alembic migration.

        @param revision 迁移目标 revision，默认 ``head`` / Target migration revision, defaults to ``head``.
        @return 无返回值 / No return value.
        @note 此方法绝不会由 backend 启动路径调用；只有 ``dbctl migrate`` 明确调用。
        / This method is never called by backend startup; only explicit ``dbctl migrate`` invokes it.
        """
        with alembic_script_location() as script_location:
            AlembicMigrationRunner(
                self.settings.require_migrator_dsn(),
                script_location,
                self.settings.administration.owner_role,
                self.settings.administration.app_role,
                self.settings.administration.dashboard_role,
            ).upgrade(revision)

    def prune_telemetry(
        self,
        *,
        apply: bool = False,
        batch_size: int = DEFAULT_TELEMETRY_PRUNE_BATCH_SIZE,
        max_batches: int = DEFAULT_TELEMETRY_PRUNE_MAX_BATCHES,
        statement_timeout_ms: int = DEFAULT_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS,
        lock_timeout_ms: int = DEFAULT_TELEMETRY_PRUNE_LOCK_TIMEOUT_MS,
    ) -> TelemetryPruneResult:
        """@brief 通过 dbctl 显式执行或预览遥测保留期清理 / Explicitly execute or preview telemetry retention pruning via dbctl.

        @param apply 只有 ``True`` 才允许连接数据库与删除；默认 ``False`` 为 dry-run。
        / Only ``True`` permits database connection and deletion; default ``False`` is dry-run.
        @param batch_size 每一个短事务可删除的最大记录数 / Maximum records each short transaction may delete.
        @param max_batches 本次运维调用可执行的最大短事务数 / Maximum short transactions for this operator invocation.
        @param statement_timeout_ms 每个清理/计数 SQL 的事务本地超时。
        / Transaction-local timeout for each prune/count SQL.
        @param lock_timeout_ms 每个清理事务的锁等待上限 / Lock-wait timeout per prune transaction.
        @return 不含数据库凭证、原始 SQL 或驱动异常的清理摘要。
        / Pruning summary containing no credentials, raw SQL, or driver exception.
        @raise DbctlConfigurationError 运维参数无效或 ``--apply`` 缺少 migrator DSN 时抛出。
        / Raised for invalid operator parameters or missing migrator DSN with ``--apply``.

        @note 这是 dbctl 的运维入口，而不是 backend/Dashboard 的请求路径。实际删除使用
        migrator DSN，并在每批独立事务内由 runner ``SET LOCAL ROLE`` 到配置 owner。
        / This is a dbctl operator entry point, not a backend/Dashboard request path. Actual deletion
        uses the migrator DSN and the runner ``SET LOCAL ROLE`` to the configured owner in every
        independent batch transaction.
        """
        request = TelemetryPruneRequest(
            retention_days=self.settings.observability.retention_days,
            batch_size=batch_size,
            max_batches=max_batches,
            statement_timeout_ms=statement_timeout_ms,
            lock_timeout_ms=lock_timeout_ms,
            apply=apply,
        )
        if request.disabled or not request.apply:
            return TelemetryPruneExecutor(None).execute(request)
        runner = PsycopgTelemetryRetentionRunner(
            self.settings.require_migrator_dsn(),
            owner_role=self.settings.administration.owner_role,
            observability_schema=self.settings.administration.observability_schema,
        )
        return TelemetryPruneExecutor(runner).execute(request)

    def prepare_shell(
        self,
        role: DatabaseRole,
        *,
        password_policy: PasswordPolicy | str = PasswordPolicy.AUTO,
    ) -> PreparedPsqlCommand:
        """@brief 为某个登录 role 准备 psql shell / Prepare psql shell for a login role.

        @param role app、migrator 或 dashboard 登录角色 / app, migrator, or dashboard login role.
        @param password_policy 密码来源策略 / Password-source policy.
        @return 不含密码 argv 的已准备 psql 命令 / Prepared psql command with password-free argv.
        @raise DbctlConfigurationError role 不是登录身份或相应 DSN 缺失时抛出。
        / Raised when role is not a login identity or its DSN is missing.
        """
        dsn = self.settings.require_shell_dsn(role)
        return PsqlShellLauncher(self._environ).prepare(
            dsn,
            password_policy=password_policy,
        )

    def exec_prepared_shell(self, prepared: PreparedPsqlCommand) -> None:
        """@brief 直接 exec 已准备的 psql shell / Directly exec a prepared psql shell.

        @param prepared 不含密码 argv 的 psql 命令 / psql command with password-free argv.
        @return 正常情况下不返回 / Does not normally return.
        """
        PsqlShellLauncher(self._environ).exec_prepared(prepared)

    def _bootstrap_runner(
        self,
        *,
        access_mode: BootstrapAccessMode | str = BootstrapAccessMode.AUTO,
    ) -> LocalPsqlBootstrapRunner:
        """@brief 创建跨平台本地 bootstrap runner / Create a cross-platform local bootstrap runner.

        @param access_mode 自动、sudo 或终端密码模式 / Auto, sudo, or terminal-password mode.
        @return 已绑定目标数据库的本地 runner / Local runner bound to the target database.
        @note 不接受管理员 DSN；两种模式都要求本机终端验证。
        / Accepts no administrator DSN; both modes require local terminal authentication.
        """
        administration = self.settings.administration
        return LocalPsqlBootstrapRunner(
            local_postgres_user=administration.local_postgres_user,
            maintenance_database=administration.maintenance_database,
            bootstrap_database_user=administration.bootstrap_database_user,
            access_mode=access_mode,
        ).with_target_database(administration.database_name)

    def _resolve_role_passwords(self) -> dict[DatabaseRole, str]:
        """@brief 返回 dbctl 自动生成的登录角色密码 / Return dbctl-generated login-role passwords.

        @return 仅内存保存的登录角色密码映射 / In-memory login-role password mapping.
        @note 密码从被 Git 忽略的 config.jsonc 中三个实际 DSN 解析，环境变量不能覆盖。
        / Passwords are parsed from the three actual DSNs in Git-ignored config.jsonc and cannot
        be overridden by environment variables.
        """
        return dict(self.settings.role_passwords)
