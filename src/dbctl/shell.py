"""@brief 安全启动交互式 psql shell / Securely launch an interactive psql shell."""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from .connection import parse_postgres_dsn
from .errors import DbctlConfigurationError


class PasswordPolicy(StrEnum):
    """@brief ``dbctl shell`` 的密码来源策略 / Password-source policy for ``dbctl shell``."""

    AUTO = "auto"
    PGPASS = "pgpass"
    PROMPT = "prompt"
    ENVIRONMENT = "environment"


class ShellCredentialStrategy(StrEnum):
    """@brief 实际采用的 psql 凭证策略 / Effective psql credential strategy."""

    NONE = "none"
    PGPASS = "pgpass"
    PROMPT = "prompt"
    ENVIRONMENT = "environment"


@dataclass(frozen=True, slots=True)
class PreparedPsqlCommand:
    """@brief 已脱敏、待 exec 的 psql 命令 / Redacted psql command ready for exec.

    @param argv 不含密码的 psql argv / Password-free psql argv.
    @param credential_strategy 实际采用的认证策略 / Effective authentication strategy.
    @param _password 仅 environment 策略使用的内存密码；永不出现在 repr 或 argv。
    / In-memory password used only by environment strategy; never in repr or argv.
    """

    argv: tuple[str, ...]
    credential_strategy: ShellCredentialStrategy
    _password: str | None = field(default=None, repr=False)

    def apply_credential_environment(self, environ: MutableMapping[str, str]) -> None:
        """@brief 在 exec 前应用认证环境策略 / Apply authentication environment policy before exec.

        @param environ 将传给 ``os.execvp`` 的可变环境映射 / Mutable environment mapping passed to ``os.execvp``.
        @return 无返回值 / No return value.
        @raise DbctlConfigurationError environment 策略缺少仅内存密码时抛出。
        / Raised when environment strategy lacks its in-memory password.

        @note ``pgpass`` 与 ``prompt`` 会清除继承的 ``PGPASSWORD``，避免策略悄然降级；
        密码始终不会被写入 argv。
        / ``pgpass`` and ``prompt`` clear inherited ``PGPASSWORD`` to avoid a silent policy downgrade;
        a password is never written into argv.
        """
        if self.credential_strategy is ShellCredentialStrategy.ENVIRONMENT:
            if not self._password:
                raise DbctlConfigurationError("environment 密码策略缺少 DSN 密码。")
            environ["PGPASSWORD"] = self._password
            return
        if self.credential_strategy in {
            ShellCredentialStrategy.PGPASS,
            ShellCredentialStrategy.PROMPT,
        }:
            environ.pop("PGPASSWORD", None)

    def policy_message(self) -> str:
        """@brief 返回不含 secret 的策略说明 / Return a secret-free policy explanation.

        @return 可展示给操作者的简体中文说明 / Simplified-Chinese explanation safe to display.
        """
        messages = {
            ShellCredentialStrategy.NONE: "未注入密码；psql 将使用既有 libpq 认证配置。",
            ShellCredentialStrategy.PGPASS: "使用 .pgpass 或 PGPASSFILE；不会把密码放入命令行。",
            ShellCredentialStrategy.PROMPT: "psql 将交互式提示密码；不会把密码放入命令行。",
            ShellCredentialStrategy.ENVIRONMENT: (
                "按显式请求通过 PGPASSWORD 传递密码；不会把密码放入命令行。"
            ),
        }
        return messages[self.credential_strategy]


class PsqlShellLauncher:
    """@brief 生成并直接 exec 到 psql / Build and directly exec into psql.

    ``dbctl shell`` 不经由 shell 或 wrapper 子进程。最终使用 ``os.execvp`` 取代当前
    进程，因此终端控制、退出码与 Unix 信号都由真实 ``psql`` 保留。
    / ``dbctl shell`` uses neither a shell nor a wrapper child process. It ultimately replaces the
    current process with ``os.execvp``, preserving terminal control, exit status, and Unix signals
    for the real ``psql``.
    """

    def __init__(self, environ: MutableMapping[str, str] | None = None) -> None:
        """@brief 初始化 shell launcher / Initialize the shell launcher.

        @param environ 可选进程环境；默认使用 ``os.environ``，便于受控测试。
        / Optional process environment; defaults to ``os.environ`` for controlled tests.
        """
        self._environ = os.environ if environ is None else environ

    def prepare(
        self,
        dsn: str,
        *,
        password_policy: PasswordPolicy | str = PasswordPolicy.AUTO,
    ) -> PreparedPsqlCommand:
        """@brief 生成不含密码 argv 的 psql 命令 / Prepare a password-free psql argv.

        @param dsn 原始角色 DSN；仅在此函数内解析，绝不回显。
        / Raw role DSN; parsed only in this function and never echoed.
        @param password_policy ``auto``、``pgpass``、``prompt`` 或 ``environment``。
        / One of ``auto``, ``pgpass``, ``prompt``, or ``environment``.
        @return 带有效认证策略的 PreparedPsqlCommand / PreparedPsqlCommand with effective credential strategy.
        @raise DbctlConfigurationError 策略无效或其前提不满足时抛出。
        / Raised when policy is invalid or its preconditions are unmet.
        """
        try:
            normalized_policy = PasswordPolicy(password_policy)
        except ValueError as error:
            raise DbctlConfigurationError("不支持的 psql 密码策略。") from error

        parsed = parse_postgres_dsn(dsn)
        strategy = self._choose_strategy(normalized_policy, parsed.password)
        argv: tuple[str, ...] = (
            "psql",
            "-X",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            f"--dbname={parsed.safe_conninfo}",
        )
        if strategy is ShellCredentialStrategy.PROMPT:
            argv += ("--password",)
        return PreparedPsqlCommand(
            argv=argv,
            credential_strategy=strategy,
            _password=(
                parsed.password if strategy is ShellCredentialStrategy.ENVIRONMENT else None
            ),
        )

    def launch(
        self,
        dsn: str,
        *,
        password_policy: PasswordPolicy | str = PasswordPolicy.AUTO,
    ) -> None:
        """@brief 应用认证策略并直接 exec 到 psql / Apply credential policy and directly exec into psql.

        @param dsn 原始角色 DSN / Raw role DSN.
        @param password_policy 密码来源策略 / Password-source policy.
        @return 正常情况下不返回；``os.execvp`` 成功时当前进程被替换。
        / Does not normally return; the current process is replaced on successful ``os.execvp``.
        """
        self.exec_prepared(self.prepare(dsn, password_policy=password_policy))

    def exec_prepared(self, prepared: PreparedPsqlCommand) -> None:
        """@brief 对已准备命令执行 os.execvp / Execute os.execvp for a prepared command.

        @param prepared 不含密码 argv 的已准备 psql 命令 / Prepared psql command with password-free argv.
        @return 正常情况下不返回 / Does not normally return.
        @note 必须使用 ``os.execvp``，而非 ``subprocess``，从而保留 psql 的信号和 TTY 行为。
        / Must use ``os.execvp`` rather than ``subprocess`` to preserve psql signal and TTY behavior.
        """
        prepared.apply_credential_environment(self._environ)
        os.execvp(prepared.argv[0], list(prepared.argv))

    def _choose_strategy(
        self,
        policy: PasswordPolicy,
        dsn_password: str | None,
    ) -> ShellCredentialStrategy:
        """@brief 从策略与 DSN 内容选择认证方式 / Choose authentication strategy from policy and DSN content.

        @param policy 用户请求的策略 / User-requested policy.
        @param dsn_password 仅内存 DSN password / In-memory DSN password.
        @return 实际 credential strategy / Effective credential strategy.
        @raise DbctlConfigurationError 指定 pgpass 或 environment 但其条件不成立时抛出。
        / Raised when pgpass or environment is requested but unavailable.
        """
        has_pgpass = self._pgpass_available()
        if policy is PasswordPolicy.AUTO:
            if dsn_password is None:
                return ShellCredentialStrategy.NONE
            return ShellCredentialStrategy.PGPASS if has_pgpass else ShellCredentialStrategy.PROMPT
        if policy is PasswordPolicy.PGPASS:
            if not has_pgpass:
                raise DbctlConfigurationError("请求 pgpass 策略，但未找到可读的 .pgpass 或 PGPASSFILE。")
            return ShellCredentialStrategy.PGPASS
        if policy is PasswordPolicy.PROMPT:
            return ShellCredentialStrategy.PROMPT
        if policy is PasswordPolicy.ENVIRONMENT:
            if dsn_password is None:
                raise DbctlConfigurationError("environment 密码策略要求 DSN 中提供 password。")
            return ShellCredentialStrategy.ENVIRONMENT
        raise DbctlConfigurationError("不支持的 psql 密码策略。")

    def _pgpass_available(self) -> bool:
        """@brief 判断 .pgpass/PGPASSFILE 是否可用 / Determine whether .pgpass/PGPASSFILE is available.

        @return 发现常规文件时为 ``True`` / ``True`` when a regular file is found.
        @note libpq 仍会自行校验 Unix 权限位；这里仅用于选择不会泄露密码的默认策略。
        / libpq still validates Unix mode bits; this check only selects a non-leaking default policy.
        """
        configured_path = self._environ.get("PGPASSFILE")
        candidate = (
            Path(configured_path).expanduser()
            if isinstance(configured_path, str) and configured_path
            else Path.home() / ".pgpass"
        )
        return candidate.is_file()
