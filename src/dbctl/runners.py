"""@brief Bootstrap I/O runner 实现 / Bootstrap I/O runner implementations."""

from __future__ import annotations

import re
import subprocess
from typing import Any

from .bootstrap import ExecutionTarget, SqlStatement
from .errors import (
    BootstrapExecutionError,
    DatabaseAlreadyExistsError,
    DbctlConfigurationError,
    DbctlDependencyError,
)
from .identifiers import quote_postgres_literal, validate_postgres_identifier


class PsycopgBootstrapRunner:
    """@brief 使用管理员 DSN 的 PostgreSQL bootstrap runner / PostgreSQL bootstrap runner using an administrator DSN.

    管理员 DSN 只交给 psycopg，绝不放入 subprocess argv、日志或异常展示文本。该
    runner 是默认生产路径；本地 ``sudo`` 路径由 :class:`LocalPsqlBootstrapRunner` 显式选择。
    / The administrator DSN is handed only to psycopg and never put in subprocess argv, logs, or
    displayed exception text. This is the default production path; the local ``sudo`` path is
    explicitly selected through :class:`LocalPsqlBootstrapRunner`.
    """

    def __init__(
        self,
        admin_dsn: str,
        maintenance_database: str = "postgres",
        target_database: str | None = None,
    ) -> None:
        """@brief 初始化管理员 DSN runner / Initialize the administrator-DSN runner.

        @param admin_dsn 管理员 PostgreSQL DSN；仅保存在内存私有字段。
        / Administrator PostgreSQL DSN; stored only in a private in-memory field.
        @param maintenance_database 创建目标数据库时连接的 maintenance database。
        / Maintenance database used while creating the target database.
        @param target_database 可选项目数据库名；完整计划执行时必须提供。
        / Optional project database name; required for complete plan execution.
        @raise DbctlConfigurationError DSN 或 maintenance database 无效时抛出。
        / Raised when DSN or maintenance database is invalid.
        """
        if not isinstance(admin_dsn, str) or not admin_dsn:
            raise DbctlConfigurationError("管理员 PostgreSQL DSN 必须是非空字符串。")
        self._admin_dsn = admin_dsn
        self._maintenance_database = validate_postgres_identifier(
            maintenance_database, kind="maintenance 数据库名"
        )
        self._target_database = (
            validate_postgres_identifier(target_database, kind="目标数据库名")
            if target_database is not None
            else None
        )

    def database_exists(self, database_name: str) -> bool:
        """@brief 查询目标数据库是否存在 / Query whether the target database exists.

        @param database_name 已验证的目标数据库名 / Validated target database name.
        @return 存在时为 ``True`` / ``True`` when it exists.
        @raise BootstrapExecutionError 管理员连接或查询失败时抛出；不回显 DSN。
        / Raised when administrator connection or query fails; DSN is not echoed.
        """
        name = validate_postgres_identifier(database_name, kind="数据库名")
        connection: Any | None = None
        try:
            connection = self._connect(self._maintenance_database)
            cursor = connection.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = "
                + quote_postgres_literal(name)
                + ");"
            )
            row = cursor.fetchone()
            return bool(row and row[0])
        except DbctlDependencyError:
            raise
        except Exception as error:
            raise BootstrapExecutionError("管理员 DSN 查询目标数据库失败；数据库详情已隐藏。") from error
        finally:
            if connection is not None:
                connection.close()

    def execute(self, target: ExecutionTarget, statement: SqlStatement) -> None:
        """@brief 在管理员或项目数据库执行计划语句 / Execute a planned statement on administrator or project database.

        @param target 连接 maintenance 或项目数据库的目标 / Target selecting maintenance or project database.
        @param statement 参数化且可安全渲染的计划语句 / Parameterized, safely renderable plan statement.
        @return 无返回值 / No return value.
        @raise DatabaseAlreadyExistsError 并发创建数据库时抛出，供执行器按幂等成功处理。
        / Raised for concurrent database creation so executor can treat it as idempotent success.
        @raise BootstrapExecutionError 其他数据库错误时抛出，且不回显底层详情。
        / Raised for other database errors without echoing lower-level details.
        """
        database_name = self._database_for_target(target)
        connection: Any | None = None
        try:
            connection = self._connect(database_name)
            rendered_sql = statement.render_with_literals(
                lambda value: self._psycopg_literal(connection, value)
            )
            connection.execute(rendered_sql)
        except DbctlDependencyError:
            raise
        except Exception as error:
            if getattr(error, "sqlstate", None) == "42P04":
                raise DatabaseAlreadyExistsError("目标数据库已由并发 bootstrap 创建。") from error
            raise BootstrapExecutionError("管理员 DSN 执行 bootstrap SQL 失败；数据库详情已隐藏。") from error
        finally:
            if connection is not None:
                connection.close()

    def _database_for_target(self, target: ExecutionTarget) -> str:
        """@brief 映射执行目标到数据库名 / Map an execution target to a database name.

        @param target 计划执行目标 / Planned execution target.
        @return 对应数据库名 / Corresponding database name.
        @raise DbctlConfigurationError 未知目标时抛出。
        / Raised for an unknown target.
        """
        if target is ExecutionTarget.MAINTENANCE:
            return self._maintenance_database
        if target is ExecutionTarget.DATABASE:
            if self._target_database is not None:
                return self._target_database
            raise DbctlConfigurationError("PsycopgBootstrapRunner 尚未绑定目标数据库。")
        raise DbctlConfigurationError("未知 bootstrap 执行目标。")

    def with_target_database(self, database_name: str) -> PsycopgBootstrapRunner:
        """@brief 绑定项目数据库名 / Bind a target project database name.

        @param database_name 已验证的项目数据库名 / Validated project database name.
        @return 可执行完整 BootstrapPlan 的 runner 视图 / Runner view able to execute a complete BootstrapPlan.
        """
        return PsycopgBootstrapRunner(
            self._admin_dsn,
            maintenance_database=self._maintenance_database,
            target_database=database_name,
        )

    def _connect(self, database_name: str) -> Any:
        """@brief 建立 autocommit 管理连接 / Establish an autocommit administrator connection.

        @param database_name 本次连接的数据库 / Database for this connection.
        @return psycopg 连接对象 / psycopg connection object.
        @raise DbctlDependencyError psycopg 不可用时抛出。
        / Raised when psycopg is unavailable.
        """
        try:
            import psycopg
        except ImportError as error:
            raise DbctlDependencyError("执行 PostgreSQL bootstrap 需要 psycopg 依赖。") from error
        return psycopg.connect(self._admin_dsn, dbname=database_name, autocommit=True)

    @staticmethod
    def _psycopg_literal(connection: Any, value: str) -> str:
        """@brief 使用 psycopg 引用文本参数 / Quote a text parameter using psycopg.

        @param connection 当前 psycopg 连接 / Current psycopg connection.
        @param value 仅内存的文本参数 / In-memory text parameter.
        @return 已按当前连接编码安全引用的 SQL 字面量 / Safely quoted SQL literal in connection encoding.
        """
        try:
            from psycopg import sql
        except ImportError as error:
            raise DbctlDependencyError("执行 PostgreSQL bootstrap 需要 psycopg 依赖。") from error
        return sql.Literal(value).as_string(connection)


class LocalPsqlBootstrapRunner:
    """@brief 显式本地 PostgreSQL psql runner / Explicit local PostgreSQL psql runner.

    此 runner 仅由 ``bootstrap --local-postgres`` 构造。它使用固定 argv
    ``sudo -u <local-user> -- psql ...``，SQL 通过 stdin 输入，从不启用 shell
    解释，也不会把角色密码放到参数列表。
    / This runner is constructed only by ``bootstrap --local-postgres``. It uses fixed argv
    ``sudo -u <local-user> -- psql ...``, sends SQL through stdin, never enables shell
    interpretation, and never puts role passwords in argument lists.
    """

    def __init__(self, local_postgres_user: str = "postgres", maintenance_database: str = "postgres") -> None:
        """@brief 初始化本地 psql runner / Initialize local psql runner.

        @param local_postgres_user 传入 ``sudo -u`` 的受限 Unix 账户名。
        / Restricted Unix account name passed to ``sudo -u``.
        @param maintenance_database 本地 psql 默认连接的 maintenance database。
        / Maintenance database to which local psql connects by default.
        @raise DbctlConfigurationError 本机账户或数据库标识符无效时抛出。
        / Raised when local account or database identifier is invalid.
        """
        if not isinstance(local_postgres_user, str) or not re.fullmatch(
            r"[a-z_][a-z0-9_-]{0,31}", local_postgres_user
        ):
            raise DbctlConfigurationError("local PostgreSQL Unix 账户不能为空。")
        self._local_postgres_user = local_postgres_user
        self._maintenance_database = validate_postgres_identifier(
            maintenance_database, kind="maintenance 数据库名"
        )
        self._target_database: str | None = None

    def with_target_database(self, database_name: str) -> LocalPsqlBootstrapRunner:
        """@brief 绑定项目数据库名 / Bind the project database name.

        @param database_name 已验证的目标数据库名 / Validated target database name.
        @return 当前 runner，便于 composition 以链式方式绑定。
        / This runner, enabling fluent composition-time binding.
        """
        self._target_database = validate_postgres_identifier(database_name, kind="目标数据库名")
        return self

    def database_exists(self, database_name: str) -> bool:
        """@brief 用本地 psql 查询目标数据库存在性 / Query target database existence using local psql.

        @param database_name 已验证的目标数据库名 / Validated target database name.
        @return 数据库存在时为 ``True`` / ``True`` when database exists.
        """
        name = validate_postgres_identifier(database_name, kind="数据库名")
        completed = self._run(
            self._maintenance_database,
            "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = "
            + quote_postgres_literal(name)
            + ");\n",
            tuples_only=True,
        )
        result = completed.stdout.strip().casefold()
        if result not in {"t", "f", "true", "false", "1", "0"}:
            raise BootstrapExecutionError("本地 psql 返回了无法识别的数据库存在性结果。")
        return result in {"t", "true", "1"}

    def execute(self, target: ExecutionTarget, statement: SqlStatement) -> None:
        """@brief 通过本地 psql stdin 执行 SQL / Execute SQL through local psql stdin.

        @param target maintenance 或目标数据库 / Maintenance or target database.
        @param statement 待执行的计划语句 / Plan statement to execute.
        @return 无返回值 / No return value.
        @raise BootstrapExecutionError psql 失败时抛出，且不回显 stderr 以保护潜在密码。
        / Raised when psql fails without echoing stderr to protect potential passwords.
        """
        database_name = self._database_for_target(target)
        try:
            self._run(database_name, statement.render_for_psql() + "\n")
        except BootstrapExecutionError as error:
            if (
                target is ExecutionTarget.MAINTENANCE
                and statement.sql.startswith("CREATE DATABASE ")
                and self._target_database is not None
                and self.database_exists(self._target_database)
            ):
                raise DatabaseAlreadyExistsError("目标数据库已由并发 bootstrap 创建。") from error
            raise

    def _database_for_target(self, target: ExecutionTarget) -> str:
        """@brief 将执行目标解析成 psql 数据库名 / Resolve execution target to a psql database name.

        @param target 计划执行目标 / Plan execution target.
        @return psql 数据库名 / psql database name.
        @raise DbctlConfigurationError 目标数据库尚未绑定或目标未知时抛出。
        / Raised when target database is unbound or target is unknown.
        """
        if target is ExecutionTarget.MAINTENANCE:
            return self._maintenance_database
        if target is ExecutionTarget.DATABASE and self._target_database is not None:
            return self._target_database
        raise DbctlConfigurationError("本地 psql runner 尚未绑定目标数据库。")

    def _run(
        self,
        database_name: str,
        script: str,
        *,
        tuples_only: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        """@brief 以无 shell 的 psql 子进程执行 stdin 脚本 / Run an stdin script in a shell-free psql subprocess.

        @param database_name psql 连接数据库名 / psql connection database name.
        @param script 仅内存 SQL stdin 脚本 / In-memory SQL stdin script.
        @param tuples_only 是否使用无格式元组输出 / Whether to use unformatted tuples-only output.
        @return 已完成的 subprocess 结果 / Completed subprocess result.
        @raise BootstrapExecutionError 启动或执行失败时抛出，且不回显子进程输出。
        / Raised when startup or execution fails, without echoing child-process output.
        """
        database = validate_postgres_identifier(database_name, kind="数据库名")
        command = [
            "sudo",
            "-u",
            self._local_postgres_user,
            "--",
            "psql",
            "-X",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            f"--dbname={database}",
        ]
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
            )
        except OSError as error:
            raise BootstrapExecutionError("无法启动本地 sudo -u postgres psql。") from error
        if completed.returncode != 0:
            raise BootstrapExecutionError("本地 psql 执行 bootstrap SQL 失败；数据库详情已隐藏。")
        return completed
