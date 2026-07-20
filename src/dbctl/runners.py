"""@brief Bootstrap I/O runner 实现 / Bootstrap I/O runner implementations."""

from __future__ import annotations

import re
import subprocess

from .bootstrap import ExecutionTarget, SqlStatement
from .errors import BootstrapExecutionError, DatabaseAlreadyExistsError, DbctlConfigurationError
from .identifiers import quote_postgres_literal, validate_postgres_identifier


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
