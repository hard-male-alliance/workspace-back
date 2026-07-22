"""@brief 本地 psql bootstrap 批处理适配器 / Local psql bootstrap batch adapter."""

from __future__ import annotations

import getpass
import ipaddress
import os
import shutil
import subprocess
from collections.abc import Callable, Mapping
from types import TracebackType

from dbctl.application.errors import (
    BootstrapExecutionError,
    DatabaseAlreadyExistsError,
    DbctlConfigurationError,
    add_safe_diagnostic_note,
    safe_external_cause,
    safe_process_exit_cause,
)
from dbctl.application.provision import (
    BootstrapAccessMode,
    BootstrapPlan,
    BootstrapStage,
    ExecutionTarget,
    SqlStatement,
    StageCondition,
    TransactionMode,
)
from dbctl.domain.names import DatabaseName
from dbctl.domain.roles import Secret
from dbctl.infrastructure.postgres.pgpass import PgpassLease, create_pgpass_lease
from dbctl.infrastructure.postgres.process import sanitized_libpq_environment


class LocalPsqlBootstrapRunnerFactory:
    """@brief 为一个自足计划创建 psql 会话 / Create a psql session for a self-contained plan."""

    def __init__(
        self,
        *,
        platform_name: str | None = None,
        executable_finder: Callable[[str], str | None] = shutil.which,
        password_prompt: Callable[[str], str] = getpass.getpass,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        """@brief 保存可注入的本机进程依赖 / Retain injectable local-process dependencies.

        @param platform_name 可注入平台名；默认 ``os.name`` / Injectable platform; defaults to ``os.name``.
        @param executable_finder sudo 可执行文件查询器 / Finder for the sudo executable.
        @param password_prompt 不回显密码读取器 / Non-echoing password reader.
        @param environ psql 基础环境 / Base environment for psql.
        """

        self._platform_name = os.name if platform_name is None else platform_name
        self._executable_finder = executable_finder
        self._password_prompt = password_prompt
        self._environ = dict(os.environ if environ is None else environ)

    def open(
        self,
        plan: BootstrapPlan,
        access_mode: BootstrapAccessMode,
    ) -> LocalPsqlBootstrapRunner:
        """@brief 解析访问模式并返回未进入的 runner / Resolve access mode and return an unentered runner.

        @param plan 含 maintenance/项目目标的计划 / Plan containing maintenance and project targets.
        @param access_mode auto、sudo 或 prompt / Auto, sudo, or prompt.
        @return 由应用服务管理上下文的 runner / Runner whose context is owned by the application service.
        """

        if not isinstance(access_mode, BootstrapAccessMode):
            raise DbctlConfigurationError("不支持的 bootstrap 权限模式。")
        sudo_executable = self._executable_finder("sudo") if self._platform_name != "nt" else None
        sudo_available = sudo_executable is not None
        sudo_compatible = sudo_available and _is_explicit_loopback(
            plan.access.maintenance_target.host
        )
        effective_mode = access_mode
        if effective_mode is BootstrapAccessMode.AUTO:
            effective_mode = (
                BootstrapAccessMode.SUDO if sudo_compatible else BootstrapAccessMode.PROMPT
            )
        if effective_mode is BootstrapAccessMode.SUDO and not sudo_available:
            raise DbctlConfigurationError("当前平台未找到兼容的 sudo；请使用 prompt 模式。")
        if effective_mode is BootstrapAccessMode.SUDO and not sudo_compatible:
            raise DbctlConfigurationError("sudo bootstrap 只允许显式 loopback 数据库目标。")
        return LocalPsqlBootstrapRunner(
            plan=plan,
            access_mode=effective_mode,
            password_prompt=self._password_prompt,
            environ=self._environ,
            sudo_executable=sudo_executable,
        )


class LocalPsqlBootstrapRunner:
    """@brief 按 stage 批量执行、持有临时凭证的 psql 会话 / Stage-batched psql session owning temporary credentials."""

    def __init__(
        self,
        *,
        plan: BootstrapPlan,
        access_mode: BootstrapAccessMode,
        password_prompt: Callable[[str], str],
        environ: Mapping[str, str],
        sudo_executable: str | None,
    ) -> None:
        """@brief 绑定不可变计划与实际访问模式 / Bind an immutable plan and effective access mode.

        @param plan 自足 bootstrap 计划 / Self-contained bootstrap plan.
        @param access_mode 已解析为 sudo 或 prompt 的模式 / Mode resolved to sudo or prompt.
        @param password_prompt prompt 模式密码读取器 / Password reader for prompt mode.
        @param environ 将被清除 PG* 覆盖的基础环境 / Base environment stripped of PG* overrides.
        @param sudo_executable 已解析的 sudo 绝对或稳定路径 / Resolved absolute or stable sudo path.
        """

        if access_mode not in (BootstrapAccessMode.SUDO, BootstrapAccessMode.PROMPT):
            raise DbctlConfigurationError("bootstrap runner 需要已解析的 sudo 或 prompt 模式。")
        self._plan = plan
        self._access_mode = access_mode
        self._password_prompt = password_prompt
        self._environ = sanitized_libpq_environment(environ)
        self._sudo_executable = sudo_executable
        self._admin_password: Secret[str] | None = None
        self._leases: dict[DatabaseName, PgpassLease] = {}

    @property
    def access_mode(self) -> BootstrapAccessMode:
        """@brief 返回实际访问模式 / Return the effective access mode.

        @return sudo 或 prompt / Sudo or prompt.
        """

        return self._access_mode

    def __enter__(self) -> LocalPsqlBootstrapRunner:
        """@brief 进入会话生命周期 / Enter the session lifecycle.

        @return 当前 runner / This runner.
        """

        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """@brief 清理所有精确目标的密码租约 / Clean every exact-target password lease.

        @param _exception_type 未使用的异常类型 / Unused exception type.
        @param exception 正在传播的异常；存在时不以清理错误遮蔽它。
        / Propagating exception, which cleanup failures must not mask.
        @param _traceback 未使用的 traceback / Unused traceback.
        @return 无返回值 / No return value.
        """

        try:
            self.close()
        except BootstrapExecutionError:
            if exception is None:
                raise
            add_safe_diagnostic_note(
                exception,
                "安全影响：bootstrap 主故障之后，至少一个临时 PGPASSFILE 清理失败；"
                "请检查系统临时目录并按 owner 权限手工删除残留凭据文件。",
            )

    def close(self) -> None:
        """@brief 幂等删除全部临时密码文件 / Idempotently remove all temporary password files.

        @return 无返回值 / No return value.
        @raise BootstrapExecutionError 任一租约无法删除时抛出 / Raised when any lease cannot be removed.
        """

        cleanup_error: OSError | None = None
        leases, self._leases = tuple(self._leases.values()), {}
        for lease in leases:
            try:
                lease.close()
            except OSError as error:
                if cleanup_error is None:
                    cleanup_error = error
        self._admin_password = None
        if cleanup_error is not None:
            raise BootstrapExecutionError(
                "无法删除一个或多个临时 PostgreSQL 密码文件。"
            ) from safe_external_cause(
                cleanup_error,
                operation="清理 bootstrap 临时 PGPASSFILE",
            )

    def database_exists(self, database: DatabaseName) -> bool:
        """@brief 在 maintenance database 查询目标是否存在 / Query target existence via the maintenance database.

        @param database 强类型项目数据库名 / Strongly typed project database name.
        @return 存在时为真 / True when present.
        """

        statement = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = "
        statement += _quote_literal(database.value) + ");\n"
        completed = self._run(
            database=self._plan.access.maintenance_target.database,
            script=statement,
            transactional=False,
            tuples_only=True,
        )
        value = completed.stdout.strip().casefold()
        if value not in {"t", "f", "true", "false", "1", "0"}:
            raise BootstrapExecutionError("本地 psql 返回了无法识别的数据库存在性结果。")
        return value in {"t", "true", "1"}

    def execute_stage(self, stage: BootstrapStage) -> None:
        """@brief 在一次 psql 调用中执行整个 stage / Execute a whole stage in one psql invocation.

        @param stage 已验证的有序 SQL 批次 / Validated ordered SQL batch.
        @return 无返回值 / No return value.
        @raise DatabaseAlreadyExistsError 并发创建已由其他 bootstrap 完成时抛出。
        / Raised when a concurrent bootstrap completed database creation.
        """

        database = (
            self._plan.access.maintenance_target.database
            if stage.target is ExecutionTarget.MAINTENANCE
            else self._plan.database_target.database
        )
        script = "\n".join(_render_statement(statement) for statement in stage.statements) + "\n"
        try:
            self._run(
                database=database,
                script=script,
                transactional=stage.transaction_mode is TransactionMode.TRANSACTIONAL,
            )
        except BootstrapExecutionError as error:
            if stage.condition is StageCondition.DATABASE_ABSENT and self.database_exists(
                self._plan.database
            ):
                raise DatabaseAlreadyExistsError("目标数据库已由并发 bootstrap 创建。") from error
            raise

    def _run(
        self,
        *,
        database: DatabaseName,
        script: str,
        transactional: bool,
        tuples_only: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        """@brief 通过 stdin ``--file=-`` 安全运行 psql / Safely run psql via stdin ``--file=-``.

        @param database 当前连接数据库 / Current connection database.
        @param script 只存在内存和 stdin 的 SQL / SQL retained only in memory and stdin.
        @param transactional 是否用一个事务包住完整 stage / Whether one transaction wraps the stage.
        @param tuples_only 是否请求无格式元组输出 / Whether to request unformatted tuple output.
        @return 已完成且成功的 subprocess 结果 / Completed successful subprocess result.
        @raise BootstrapExecutionError 进程启动或 SQL 执行失败时抛出且隐藏输出。
        / Raised on startup or SQL failure with child output hidden.
        """

        command = [
            "psql",
            "-X",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            "--file=-",
        ]
        if transactional:
            command.append("--single-transaction")
        child_environment = dict(self._environ)
        if self._access_mode is BootstrapAccessMode.SUDO:
            if self._sudo_executable is None:
                raise BootstrapExecutionError("sudo bootstrap runner 缺少已解析 executable。")
            command.extend(
                (
                    f"--port={self._plan.access.maintenance_target.port}",
                    f"--dbname={database.value}",
                )
            )
            command = [
                self._sudo_executable,
                "-u",
                self._plan.access.local_postgres_user,
                "--",
                *command,
            ]
        else:
            target = (
                self._plan.access.maintenance_target
                if database == self._plan.access.maintenance_target.database
                else self._plan.database_target
            )
            command.extend(
                (
                    f"--host={target.host}",
                    f"--port={target.port}",
                    f"--dbname={database.value}",
                    f"--username={self._plan.access.bootstrap_database_user.value}",
                    "--no-password",
                )
            )
            child_environment["PGPASSFILE"] = str(self._password_lease(target.database).path)
        if tuples_only:
            command.extend(("-A", "-t", "-q"))
        try:
            completed = subprocess.run(
                command,
                input=script,
                text=True,
                capture_output=True,
                check=False,
                shell=False,
                env=child_environment,
            )
        except OSError as error:
            program = "sudo" if self._access_mode is BootstrapAccessMode.SUDO else "psql"
            raise BootstrapExecutionError(
                f"无法启动本地程序 {program}；bootstrap 阶段未执行。"
            ) from safe_external_cause(
                error,
                operation=f"启动 {program} 子进程",
            )
        if completed.returncode != 0:
            raise BootstrapExecutionError(
                f"本地 psql 执行 bootstrap SQL 失败（退出码 {completed.returncode}）。"
            ) from safe_process_exit_cause(
                program="psql",
                exit_code=completed.returncode,
            )
        return completed

    def _password_lease(self, database: DatabaseName) -> PgpassLease:
        """@brief 为一个精确数据库复用管理员凭证租约 / Reuse an admin credential lease for one exact database.

        @param database maintenance 或项目数据库 / Maintenance or project database.
        @return 与 endpoint/database/user 精确匹配的租约 / Lease exactly matching endpoint/database/user.
        """

        existing = self._leases.get(database)
        if existing is not None:
            return existing
        if self._admin_password is None:
            try:
                raw_password = self._password_prompt(
                    "PostgreSQL bootstrap 管理角色 "
                    f"{self._plan.access.bootstrap_database_user.value} 密码："
                )
            except (EOFError, OSError) as error:
                raise DbctlConfigurationError(
                    "无法从终端读取 PostgreSQL 管理密码；请使用交互式 TTY 或 sudo 模式。"
                ) from safe_external_cause(
                    error,
                    operation="读取 PostgreSQL bootstrap 管理密码",
                )
            if (
                not isinstance(raw_password, str)
                or not raw_password
                or any(character in raw_password for character in ("\x00", "\r", "\n"))
            ):
                raise DbctlConfigurationError("bootstrap 管理角色密码不能为空或包含控制字符。")
            self._admin_password = Secret(raw_password)
        target = (
            self._plan.access.maintenance_target
            if database == self._plan.access.maintenance_target.database
            else self._plan.database_target
        )
        try:
            lease = create_pgpass_lease(
                target=target,
                username=self._plan.access.bootstrap_database_user,
                password=self._admin_password,
                prefix="dbctl-bootstrap-pgpass-",
            )
        except (OSError, ValueError) as error:
            raise BootstrapExecutionError(
                "无法创建临时 PostgreSQL 密码文件。"
            ) from safe_external_cause(
                error,
                operation="创建 bootstrap 临时 PGPASSFILE",
            )
        self._leases[database] = lease
        return lease


def _render_statement(statement: SqlStatement) -> str:
    """@brief 将参数仅渲染到 psql stdin / Render parameters only into psql stdin.

    @param statement 应用层参数化语句 / Application-layer parameterized statement.
    @return 可执行 SQL；可能含 secret，禁止记录 / Executable SQL that may contain secrets and must not be logged.
    """

    pieces = statement.sql.split("%s")
    rendered = [pieces[0]]
    for index, parameter in enumerate(statement.parameters):
        value = parameter.reveal() if isinstance(parameter, Secret) else parameter
        rendered.extend((_quote_literal(value), pieces[index + 1]))
    return "".join(rendered)


def _quote_literal(value: str) -> str:
    """@brief 生成不依赖 standard_conforming_strings 的 E-string / Build an E-string independent of server settings.

    @param value 无 NUL 文本 / NUL-free text.
    @return 正确转义反斜杠与引号的 PostgreSQL 字面量 / PostgreSQL literal escaping backslashes and quotes.
    """

    if "\x00" in value:
        raise BootstrapExecutionError("PostgreSQL 文本字面量不能包含 NUL。")
    return "E'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _is_explicit_loopback(host: str) -> bool:
    """@brief 判断 host 是否明确表示本机 loopback / Determine whether host explicitly denotes loopback.

    @param host dbinit 声明且已验证的 host / Validated host declared by dbinit.
    @return localhost、127/8 或 ::1 时为真 / True for localhost, 127/8, or ::1.
    """

    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


__all__ = ["LocalPsqlBootstrapRunner", "LocalPsqlBootstrapRunnerFactory"]
