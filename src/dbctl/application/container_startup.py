"""@brief 容器配置投影与进程替换用例 / Container projection and process-replacement use case."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

from .errors import ContainerEntrypointError, add_safe_diagnostic_note, safe_external_cause
from .progress import (
    OperationName,
    ProgressSink,
    ProgressState,
    ProgressUpdate,
    publish_progress,
)


class ContainerProjectionError(ContainerEntrypointError):
    """@brief 容器运行配置投影发生预期故障 / Expected container runtime-projection failure."""


class ContainerLaunchError(ContainerEntrypointError):
    """@brief 容器目标进程替换发生预期故障 / Expected container process-replacement failure."""


class ContainerRuntimePort(Protocol):
    """@brief 容器运行时唯一外部能力端口 / Sole external-capability port for container startup."""

    def project(
        self,
        source_config_path: Path,
        runtime_config_path: Path,
        environ: Mapping[str, str],
    ) -> None:
        """@brief 原子投影容器运行配置 / Atomically project the container runtime configuration.

        @param source_config_path dbctl 持久配置路径 / Persistent dbctl configuration path.
        @param runtime_config_path 临时运行配置路径 / Ephemeral runtime configuration path.
        @param environ 容器环境覆盖 / Container environment overrides.
        @return 无返回值 / No return value.
        """

        ...

    def replace(self, command: tuple[str, ...], environ: Mapping[str, str]) -> None:
        """@brief 以目标命令替换当前进程 / Replace the current process with the target command.

        @param command 保持原样的非空 argv / Unmodified non-empty argv.
        @param environ 保持原样的目标环境 / Unmodified target environment.
        @return 成功时不返回；测试替身可返回 / Does not return on success; test doubles may return.
        """

        ...


class ContainerStartupService:
    """@brief 依次投影配置并替换容器入口进程 / Project configuration then replace the entrypoint process."""

    def __init__(
        self,
        port: ContainerRuntimePort,
        *,
        progress: ProgressSink | None = None,
    ) -> None:
        """@brief 初始化容器启动用例 / Initialize the container-startup use case.

        @param port 投影与进程替换基础设施端口 / Projection and process-replacement port.
        @param progress 可选同步操作者进度端口 / Optional synchronous operator-progress port.
        """

        self._port = port
        self._progress = progress

    def execute(
        self,
        source_config_path: Path,
        runtime_config_path: Path,
        command: tuple[str, ...],
        environ: Mapping[str, str],
        *,
        source_overridden: bool,
        runtime_overridden: bool,
    ) -> None:
        """@brief 执行不可交换的投影再替换流程 / Execute the ordered projection-then-replacement flow.

        @param source_config_path dbctl 持久配置路径 / Persistent dbctl configuration path.
        @param runtime_config_path 临时运行配置路径 / Ephemeral runtime configuration path.
        @param command 保持原样的非空目标 argv / Unmodified non-empty target argv.
        @param environ 保持原样的目标环境 / Unmodified target environment.
        @param source_overridden 是否由 ``AIWS_SOURCE_CONFIG`` 覆盖源路径。
        / Whether ``AIWS_SOURCE_CONFIG`` overrides the source path.
        @param runtime_overridden 是否由 ``AIWS_CONFIG`` 覆盖目标路径。
        / Whether ``AIWS_CONFIG`` overrides the destination path.
        @return 成功替换进程时不返回；测试替身返回时返回 ``None``。
        / Does not return after a successful replacement; returns ``None`` when a test double does.
        @raise ContainerProjectionError 配置或文件系统导致的预期投影失败。
        / Raised for expected configuration or filesystem projection failures.
        @raise ContainerLaunchError 命令或操作系统导致的预期替换失败。
        / Raised for expected command or operating-system replacement failures.
        """

        if not command:
            raise ContainerLaunchError("容器目标命令不能为空；运行配置未投影。")

        self._publish(
            ProgressUpdate(
                operation=OperationName.CONTAINER,
                state=ProgressState.STARTED,
                message="投影容器运行配置",
                detail=(
                    f"源={_path_provenance('AIWS_SOURCE_CONFIG', source_overridden)}；"
                    f"目标={_path_provenance('AIWS_CONFIG', runtime_overridden)}；路径值不回显"
                ),
            )
        )
        try:
            self._port.project(source_config_path, runtime_config_path, environ)
        except (OSError, ValueError) as error:
            self._projection_failed(error)
            projection_error = ContainerProjectionError("容器运行配置投影失败。")
            add_safe_diagnostic_note(
                projection_error,
                "运维影响：运行配置投影未报告完成；目标进程尚未启动。",
            )
            add_safe_diagnostic_note(
                projection_error,
                "目标进程未启动；请先运行 `dbctl bootstrap`，再检查容器配置环境变量。",
            )
            raise projection_error from safe_external_cause(
                error,
                operation="投影容器运行配置失败",
            )
        except Exception as error:
            self._projection_failed(error)
            raise

        self._publish(
            ProgressUpdate(
                operation=OperationName.CONTAINER,
                state=ProgressState.SUCCEEDED,
                message="容器运行配置已原子投影",
                detail="运行副本使用 owner-only 权限；目标进程尚未启动",
            )
        )
        self._publish(
            ProgressUpdate(
                operation=OperationName.CONTAINER,
                state=ProgressState.STARTED,
                message="以 exec 替换容器入口进程",
                detail=f"argv={len(command)} 项；命令与参数内容不回显",
            )
        )
        try:
            self._port.replace(command, environ)
        except (OSError, ValueError) as error:
            self._launch_failed(error)
            launch_error = ContainerLaunchError("容器目标进程启动失败。")
            add_safe_diagnostic_note(
                launch_error,
                "运维影响：运行配置已原子投影；目标进程尚未启动。",
            )
            add_safe_diagnostic_note(
                launch_error,
                "运行配置已投影，但目标进程未启动；请检查命令是否存在及容器用户执行权限。",
            )
            raise launch_error from safe_external_cause(
                error,
                operation="以 exec 启动容器目标进程失败",
            )
        except Exception as error:
            self._launch_failed(error)
            raise

    def _projection_failed(self, error: BaseException) -> None:
        """@brief 报告投影未确认完成且进程未启动 / Report unconfirmed projection and no process launch.

        @param error 将传播或包装的当前异常 / Current exception to propagate or wrap.
        @return 无返回值 / No return value.
        """

        add_safe_diagnostic_note(
            error,
            "运维影响：运行配置投影未报告完成；目标进程尚未启动。",
        )
        self._publish(
            ProgressUpdate(
                operation=OperationName.CONTAINER,
                state=ProgressState.FAILED,
                message="容器运行配置投影失败",
                detail="目标进程尚未启动；本次投影未确认完成",
            )
        )

    def _launch_failed(self, error: BaseException) -> None:
        """@brief 报告投影已完成但进程未启动 / Report completed projection but no process launch.

        @param error 将传播或包装的当前异常 / Current exception to propagate or wrap.
        @return 无返回值 / No return value.
        """

        add_safe_diagnostic_note(
            error,
            "运维影响：运行配置已原子投影；目标进程尚未启动。",
        )
        self._publish(
            ProgressUpdate(
                operation=OperationName.CONTAINER,
                state=ProgressState.FAILED,
                message="容器目标进程启动失败",
                detail="运行配置已投影；目标进程未启动",
            )
        )

    def _publish(self, update: ProgressUpdate) -> None:
        """@brief 尽力发布容器启动进度 / Publish container-startup progress best-effort.

        @param update 已验证且不含 secret 的进度 / Validated secret-free progress update.
        @return 无返回值 / No return value.
        """

        publish_progress(self._progress, update)


def _path_provenance(environment_name: str, overridden: bool) -> str:
    """@brief 说明路径来源而不回显路径值 / Describe path provenance without echoing its value.

    @param environment_name 可覆盖路径的环境变量名 / Environment variable that may override the path.
    @param overridden 本轮是否存在该覆盖 / Whether the override is present for this run.
    @return 不含路径或 secret 的来源标签 / Provenance label containing no path or secret.
    """

    return f"{environment_name} 覆盖" if overridden else "内置默认"


__all__ = [
    "ContainerLaunchError",
    "ContainerProjectionError",
    "ContainerRuntimePort",
    "ContainerStartupService",
]
