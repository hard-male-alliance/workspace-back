"""@brief 交互式 psql shell 适配器 / Interactive psql-shell adapter."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping

from dbctl.application.errors import (
    ShellExecutionError,
    add_safe_diagnostic_note,
    safe_external_cause,
)
from dbctl.domain.database import LoginDatabase
from dbctl.infrastructure.postgres.pgpass import create_pgpass_lease
from dbctl.infrastructure.postgres.process import sanitized_libpq_environment


class PsqlShellAdapter:
    """@brief 以精确 target 临时凭证启动 psql / Launch psql with an exact-target temporary credential."""

    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        """@brief 保存 shell 子进程基础环境 / Retain the shell subprocess base environment.

        @param environ 可注入环境；默认复制当前环境 / Injectable environment; defaults to a copy of the process environment.
        """

        self._environ = dict(os.environ if environ is None else environ)

    def launch(self, login: LoginDatabase) -> int:
        """@brief 继承真实 TTY 并等待 psql / Inherit the real TTY and wait for psql.

        @param login 配置边界验证的 app/migrator/dashboard 身份。
        / App, migrator, or dashboard identity validated at the configuration boundary.
        @return psql 原始 subprocess 状态 / Raw subprocess status from psql.
        @raise ShellExecutionError 凭证文件或进程生命周期失败时抛出且不泄露秘密。
        / Raised for credential-file or process-lifecycle failures without leaking secrets.
        """

        try:
            lease = create_pgpass_lease(
                target=login.target,
                username=login.role_name,
                password=login.password,
                prefix="dbctl-shell-pgpass-",
            )
        except (OSError, ValueError) as error:
            raise ShellExecutionError(
                "无法创建临时 PostgreSQL 密码文件。"
            ) from safe_external_cause(
                error,
                operation="创建 psql 临时 PGPASSFILE",
            )

        command = (
            "psql",
            "-X",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            "--no-password",
            f"--dbname={login.safe_conninfo}",
        )
        child_environment = sanitized_libpq_environment(self._environ)
        child_environment["PGPASSFILE"] = str(lease.path)
        process_error: ShellExecutionError | None = None
        result: int | None = None
        try:
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    shell=False,
                    env=child_environment,
                )
                result = completed.returncode
            except OSError as error:
                process_error = ShellExecutionError("无法启动本地 PostgreSQL psql。")
                process_error.__cause__ = safe_external_cause(
                    error,
                    operation="启动本地 psql 进程",
                )
        finally:
            try:
                lease.close()
            except OSError as error:
                if process_error is None:
                    process_error = ShellExecutionError("无法删除临时 PostgreSQL 密码文件。")
                    process_error.__cause__ = safe_external_cause(
                        error,
                        operation="删除 psql 临时 PGPASSFILE",
                    )
                else:
                    add_safe_diagnostic_note(
                        process_error,
                        "安全影响：psql 启动失败后，临时 PGPASSFILE 清理也失败；"
                        "请检查系统临时目录并按 owner 权限手工删除残留凭据文件。",
                    )
        if process_error is not None:
            raise process_error
        if result is None:
            raise ShellExecutionError("psql 未返回退出状态。")
        return result


__all__ = ["PsqlShellAdapter"]
