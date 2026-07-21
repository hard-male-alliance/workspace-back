"""@brief 使用 config 凭证启动交互式 psql / Launch interactive psql with config credentials."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from .credentials import create_pgpass_file
from .domain import DatabaseLogin
from .errors import DbctlDependencyError, DbctlError


@dataclass(frozen=True, slots=True)
class PreparedPsqlCommand:
    """@brief 已脱敏、绑定配置身份的 psql 命令 / Redacted psql command bound to config identity.

    @param argv 不含密码的 psql argv / Password-free psql argv.
    @param login 从 config.jsonc 解析出的完整身份 / Complete identity parsed from config.jsonc.
    """

    argv: tuple[str, ...]
    login: DatabaseLogin = field(repr=False)

    def policy_message(self) -> str:
        """@brief 返回安全的自动认证说明 / Return a safe automatic-authentication message.

        @return 不含 secret 的操作者说明 / Secret-free operator message.
        """
        return f"自动使用 config.jsonc 中的 {self.login.role.value} role 与密码。"


class PsqlShellLauncher:
    """@brief 通过临时 pgpass 启动并等待交互式 psql / Run interactive psql with temporary pgpass.

    子进程继承真实 TTY；wrapper 仅负责 secret 文件生命周期和退出码。与 ``exec`` 相比，
    等待子进程使成功、失败和中断路径都能删除临时 pgpass。
    / The subprocess inherits the real TTY. The wrapper only owns the secret-file lifecycle and
    exit status. Waiting, instead of exec, guarantees pgpass cleanup on success, failure, and interrupts.
    """

    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        """@brief 初始化 shell launcher / Initialize the shell launcher.

        @param environ psql 子进程的基础环境 / Base environment for the psql subprocess.
        """
        self._environ = dict(os.environ if environ is None else environ)

    def prepare(self, login: DatabaseLogin) -> PreparedPsqlCommand:
        """@brief 从配置身份生成不含密码的命令 / Build a password-free command from config identity.

        @param login 已在配置边界验证的登录身份 / Login validated at the configuration boundary.
        @return 可安全展示且待执行的命令 / Safe-to-display prepared command.
        """
        return PreparedPsqlCommand(
            argv=(
                "psql",
                "-X",
                "--no-psqlrc",
                "-v",
                "ON_ERROR_STOP=1",
                "--no-password",
                f"--dbname={login.safe_conninfo}",
            ),
            login=login,
        )

    def run(self, prepared: PreparedPsqlCommand) -> int:
        """@brief 使用一次性 pgpass 运行交互式 psql / Run interactive psql with one-shot pgpass.

        @param prepared 已绑定 config 登录身份的命令 / Command bound to a config login.
        @return psql 退出码；被信号终止时使用 shell 惯例 ``128 + signal``。
        / psql exit status, or ``128 + signal`` when terminated by a signal.
        @raise DbctlDependencyError 无法启动 psql 时抛出 / Raised when psql cannot be started.
        @raise DbctlError 无法创建或清理临时 pgpass 时抛出 / Raised on pgpass lifecycle failure.
        """
        password_file = self._create_password_file(prepared.login)
        child_environment = dict(self._environ)
        child_environment.pop("PGPASSWORD", None)
        child_environment["PGPASSFILE"] = str(password_file)
        try:
            try:
                completed = subprocess.run(
                    prepared.argv,
                    check=False,
                    shell=False,
                    env=child_environment,
                )
            except OSError as error:
                raise DbctlDependencyError("无法启动本地 PostgreSQL psql。") from error
            return (
                completed.returncode
                if completed.returncode >= 0
                else 128 + abs(completed.returncode)
            )
        finally:
            try:
                password_file.unlink(missing_ok=True)
            except OSError as error:
                raise DbctlError("无法删除临时 PostgreSQL 密码文件。") from error

    @staticmethod
    def _create_password_file(login: DatabaseLogin) -> Path:
        """@brief 创建只匹配当前角色的一次性 pgpass / Create a role-scoped one-shot pgpass.

        @param login config 中的数据库登录身份 / Database login identity from config.
        @return 权限为 0600 的临时文件路径 / Temporary path with mode 0600.
        """
        try:
            return create_pgpass_file(
                login.role_name,
                login.password,
                prefix="dbctl-shell-pgpass-",
            )
        except OSError as error:
            raise DbctlError("无法创建临时 PostgreSQL 密码文件。") from error
