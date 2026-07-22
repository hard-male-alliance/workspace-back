"""@brief 本地容器运行时基础设施适配器 / Local container-runtime infrastructure adapter."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from dbctl.application.container_startup import ContainerRuntimePort

from .runtime_projection import write_runtime_config


class ContainerRuntimeAdapter(ContainerRuntimePort):
    """@brief 以文件系统与 exec 实现容器启动端口 / Implement container startup with files and exec."""

    def project(
        self,
        source_config_path: Path,
        runtime_config_path: Path,
        environ: Mapping[str, str],
    ) -> None:
        """@brief 原子写入容器运行配置 / Atomically write the container runtime configuration.

        @param source_config_path dbctl 持久配置路径 / Persistent dbctl configuration path.
        @param runtime_config_path 临时运行配置路径 / Ephemeral runtime configuration path.
        @param environ 容器环境覆盖 / Container environment overrides.
        @return 无返回值 / No return value.
        """

        write_runtime_config(source_config_path, runtime_config_path, environ)

    def replace(self, command: tuple[str, ...], environ: Mapping[str, str]) -> None:
        """@brief 通过 ``execvpe`` 原样替换当前进程 / Replace this process unchanged through ``execvpe``.

        @param command 保持原样的非空 argv / Unmodified non-empty argv.
        @param environ 保持原样的目标环境 / Unmodified target environment.
        @return 成功时不返回 / Does not return on success.
        """

        os.execvpe(command[0], command, environ)


__all__ = ["ContainerRuntimeAdapter"]
