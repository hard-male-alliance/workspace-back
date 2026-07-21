"""@brief 交互式 PostgreSQL shell 用例 / Interactive PostgreSQL shell use case."""

from dbctl.domain.database import DatabaseLogin, LoginDatabase

from .errors import ShellExecutionError
from .ports import ShellPort


class OpenShellService:
    """@brief 直接启动强类型登录的 shell / Directly launch a shell for a purpose-typed login."""

    def __init__(self, port: ShellPort) -> None:
        """@brief 初始化 shell 用例 / Initialize the shell use case.

        @param port 管理临时凭证与进程的 shell 端口 / Shell port owning temporary credentials and process.
        """
        self._port = port

    def execute(self, login: LoginDatabase) -> int:
        """@brief 启动 shell 并规范化退出状态 / Launch the shell and normalize its exit status.

        @param login app、migrator 或 dashboard 登录 / App, migrator, or dashboard login.
        @return 非负 shell 风格退出码 / Non-negative shell-style exit status.
        @raise ShellExecutionError 端口返回非整数状态时抛出 / Raised for a non-integer port status.
        """
        if not isinstance(login, DatabaseLogin):
            raise ShellExecutionError("shell 用例需要 DatabaseLogin。")
        exit_code = self._port.launch(login)
        if not isinstance(exit_code, int) or isinstance(exit_code, bool):
            raise ShellExecutionError("shell port 返回了无效退出码。")
        return exit_code if exit_code >= 0 else 128 + abs(exit_code)
