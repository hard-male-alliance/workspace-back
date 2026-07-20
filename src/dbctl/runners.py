"""@brief Bootstrap I/O runner 实现 / Bootstrap I/O runner implementations."""

from __future__ import annotations

import getpass
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Mapping
from enum import StrEnum
from pathlib import Path

from .bootstrap import ExecutionTarget, SqlStatement
from .errors import BootstrapExecutionError, DatabaseAlreadyExistsError, DbctlConfigurationError
from .identifiers import quote_postgres_literal, validate_postgres_identifier


class BootstrapAccessMode(StrEnum):
    """@brief bootstrap 管理权限获取模式 / Bootstrap administrative-access mode."""

    AUTO = "auto"
    SUDO = "sudo"
    PROMPT = "prompt"


class LocalPsqlBootstrapRunner:
    """@brief 跨平台本地 PostgreSQL psql runner / Cross-platform local PostgreSQL psql runner.

    ``auto`` 在 POSIX 且存在 sudo 时切换本机 PostgreSQL 账户；Windows 或无 sudo
    平台改为终端提示数据库管理角色密码。SQL 始终通过 stdin 输入且从不启用 shell。
    / ``auto`` switches to the local PostgreSQL account when sudo exists on POSIX; Windows and
    platforms without sudo prompt for the database administrator password in the terminal. SQL is
    always sent through stdin and a shell is never enabled.
    """

    def __init__(
        self,
        local_postgres_user: str = "postgres",
        maintenance_database: str = "postgres",
        *,
        bootstrap_database_user: str = "postgres",
        access_mode: BootstrapAccessMode | str = BootstrapAccessMode.AUTO,
        platform_name: str | None = None,
        executable_finder: Callable[[str], str | None] = shutil.which,
        password_prompt: Callable[[str], str] = getpass.getpass,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        """@brief 初始化跨平台 psql runner / Initialize the cross-platform psql runner.

        @param local_postgres_user 传入 ``sudo -u`` 的受限 Unix 账户名。
        / Restricted Unix account name passed to ``sudo -u``.
        @param maintenance_database 本地 psql 默认连接的 maintenance database。
        / Maintenance database to which local psql connects by default.
        @param bootstrap_database_user prompt 模式连接使用的 PostgreSQL 管理角色。
        / PostgreSQL administrative role used by prompt mode.
        @param access_mode auto、sudo 或 prompt / One of auto, sudo, or prompt.
        @param platform_name 可注入平台名；默认 ``os.name`` / Injectable platform name; defaults to ``os.name``.
        @param executable_finder 可注入的可执行文件查询器 / Injectable executable finder.
        @param password_prompt 不回显的终端密码读取器 / Non-echoing terminal password reader.
        @param environ psql 子进程继承的环境 / Environment inherited by psql subprocesses.
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
        self._bootstrap_database_user = validate_postgres_identifier(
            bootstrap_database_user,
            kind="bootstrap 数据库用户",
        )
        try:
            normalized_mode = BootstrapAccessMode(access_mode)
        except ValueError as error:
            raise DbctlConfigurationError("不支持的 bootstrap 权限模式。") from error
        current_platform = os.name if platform_name is None else platform_name
        sudo_path = executable_finder("sudo") if current_platform != "nt" else None
        if normalized_mode is BootstrapAccessMode.AUTO:
            normalized_mode = (
                BootstrapAccessMode.SUDO if sudo_path is not None else BootstrapAccessMode.PROMPT
            )
        if normalized_mode is BootstrapAccessMode.SUDO and sudo_path is None:
            raise DbctlConfigurationError("当前平台未找到兼容的 sudo；请使用 prompt 模式。")
        self._access_mode = normalized_mode
        self._password_prompt = password_prompt
        self._environ = dict(os.environ if environ is None else environ)
        self._password_file: Path | None = None
        self._target_database: str | None = None

    @property
    def access_mode(self) -> BootstrapAccessMode:
        """@brief 返回实际采用的权限模式 / Return the effective administrative-access mode.

        @return 已解析的 sudo 或 prompt 模式 / Resolved sudo or prompt mode.
        """
        return self._access_mode

    def close(self) -> None:
        """@brief 删除 prompt 模式的临时密码文件 / Remove the prompt-mode temporary password file.

        @return 无返回值 / No return value.
        """
        if self._password_file is None:
            return
        try:
            self._password_file.unlink(missing_ok=True)
        except OSError as error:
            raise BootstrapExecutionError("无法删除临时 PostgreSQL 密码文件。") from error
        finally:
            self._password_file = None

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
            "psql",
            "-X",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            f"--dbname={database}",
        ]
        child_environment: dict[str, str] | None = None
        if self._access_mode is BootstrapAccessMode.SUDO:
            command = ["sudo", "-u", self._local_postgres_user, "--", *command]
        else:
            command.append(f"--username={self._bootstrap_database_user}")
            child_environment = dict(self._environ)
            child_environment.pop("PGPASSWORD", None)
            child_environment["PGPASSFILE"] = str(self._ensure_password_file())
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
            raise BootstrapExecutionError("无法启动本地 PostgreSQL psql。") from error
        if completed.returncode != 0:
            raise BootstrapExecutionError("本地 psql 执行 bootstrap SQL 失败；数据库详情已隐藏。")
        return completed

    def _ensure_password_file(self) -> Path:
        """@brief 提示一次管理员密码并创建临时 pgpass / Prompt once and create a temporary pgpass file.

        @return 当前 runner 独占的临时密码文件 / Temporary password file owned by this runner.
        @raise DbctlConfigurationError 终端未提供非空密码时抛出。
        / Raised when the terminal does not provide a non-empty password.
        """
        if self._password_file is not None:
            return self._password_file
        password = self._password_prompt(
            f"PostgreSQL bootstrap 管理角色 {self._bootstrap_database_user} 密码："
        )
        if not isinstance(password, str) or not password or "\x00" in password:
            raise DbctlConfigurationError("bootstrap 管理角色密码不能为空或包含 NUL。")
        escaped_user = _escape_pgpass_field(self._bootstrap_database_user)
        escaped_password = _escape_pgpass_field(password)
        path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                prefix="dbctl-pgpass-",
                delete=False,
            ) as password_file:
                password_file.write(f"*:*:*:{escaped_user}:{escaped_password}\n")
                path = Path(password_file.name)
            path.chmod(0o600)
        except OSError as error:
            if path is not None:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise BootstrapExecutionError("无法创建临时 PostgreSQL 密码文件。") from error
        self._password_file = path
        return path


def _escape_pgpass_field(value: str) -> str:
    """@brief 转义 pgpass 字段 / Escape one pgpass field.

    @param value 用户名或密码 / User name or password.
    @return 已转义的 pgpass 字段 / Escaped pgpass field.
    """
    return value.replace("\\", "\\\\").replace(":", "\\:")
