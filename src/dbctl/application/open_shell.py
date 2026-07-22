"""@brief 交互式 PostgreSQL shell 用例 / Interactive PostgreSQL shell use case."""

from typing import Protocol

from dbctl.domain.database import DatabaseLogin, LoginDatabase

from .errors import ShellExecutionError, add_safe_diagnostic_note
from .progress import (
    OperationName,
    ProgressSink,
    ProgressState,
    ProgressUpdate,
    publish_progress,
)


class ShellPort(Protocol):
    """@brief 交互式 PostgreSQL shell 启动端口 / Interactive PostgreSQL shell launch port."""

    def launch(self, login: LoginDatabase) -> int:
        """@brief 直接使用强类型登录启动 shell / Launch a shell directly with a typed login.

        @param login config 中验证过的登录身份 / Login identity validated from configuration.
        @return subprocess 风格退出状态 / Subprocess-style exit status.
        """
        ...


class OpenShellService:
    """@brief 直接启动强类型登录的 shell / Directly launch a shell for a purpose-typed login."""

    def __init__(self, port: ShellPort, *, progress: ProgressSink | None = None) -> None:
        """@brief 初始化 shell 用例 / Initialize the shell use case.

        @param port 管理临时凭证与进程的 shell 端口 / Shell port owning temporary credentials and process.
        @param progress 可选同步进度输出端口 / Optional synchronous progress output port.
        """
        self._port = port
        self._progress = progress

    def execute(self, login: LoginDatabase) -> int:
        """@brief 启动 shell 并规范化退出状态 / Launch the shell and normalize its exit status.

        @param login app、migrator 或 dashboard 登录 / App, migrator, or dashboard login.
        @return 非负 shell 风格退出码 / Non-negative shell-style exit status.
        @raise ShellExecutionError 端口返回非整数状态时抛出 / Raised for a non-integer port status.
        """
        if not isinstance(login, DatabaseLogin):
            raise ShellExecutionError("shell 用例需要 DatabaseLogin。")
        detail = (
            f"目标={login.target.host}:{login.target.port}/{login.target.database.value}；"
            f"身份={login.role_name.value}；认证=临时 PGPASSFILE"
        )
        self._publish(
            ProgressUpdate(
                operation=OperationName.SHELL,
                state=ProgressState.STARTED,
                message="启动交互式 psql",
                detail=detail,
            )
        )
        try:
            exit_code = self._port.launch(login)
            if not isinstance(exit_code, int) or isinstance(exit_code, bool):
                raise ShellExecutionError("shell port 返回了无效退出码。")
        except Exception as error:
            add_safe_diagnostic_note(error, f"dbctl shell：{detail}。")
            self._publish(
                ProgressUpdate(
                    operation=OperationName.SHELL,
                    state=ProgressState.FAILED,
                    message="交互式 psql 启动或清理失败",
                    detail="数据库凭据未写入命令行；请检查 traceback",
                )
            )
            raise
        normalized_exit_code = exit_code if exit_code >= 0 else 128 + abs(exit_code)
        self._publish(
            ProgressUpdate(
                operation=OperationName.SHELL,
                state=(
                    ProgressState.SUCCEEDED if normalized_exit_code == 0 else ProgressState.FAILED
                ),
                message="交互式 psql 已退出",
                detail=f"退出码={normalized_exit_code}",
            )
        )
        return normalized_exit_code

    def _publish(self, update: ProgressUpdate) -> None:
        """@brief 向可选输出端口同步发布进度 / Publish progress synchronously to the optional output port.

        @param update 已验证且不含 secret 的进度 / Validated secret-free progress update.
        @return 无返回值 / No return value.
        """

        publish_progress(self._progress, update)
