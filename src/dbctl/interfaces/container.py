"""@brief Docker 容器进程入口 / Docker container process entry point."""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

from dbctl.application.container_startup import ContainerLaunchError, ContainerProjectionError
from dbctl.composition import compose_container
from dbctl.interfaces.console import OperatorConsole

_DEFAULT_SOURCE_CONFIG_PATH: Final[Path] = Path("/var/lib/aiws-config/config.jsonc")
"""@brief dbctl 持久配置默认路径 / Default path of the persistent dbctl-owned configuration."""

_DEFAULT_RUNTIME_CONFIG_PATH: Final[Path] = Path("/tmp/aiws/config.jsonc")
"""@brief 容器运行副本默认路径 / Default path of the container runtime projection."""


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 呈现容器启动用例并保持入口退出码 / Present startup and preserve entrypoint statuses.

    @param argv 待执行命令；``None`` 时读取 ``sys.argv``。
    / Command to execute; reads ``sys.argv`` when ``None``.
    @return 参数或预期投影错误返回 ``2``；启动或未知错误返回 ``1``；测试替身成功返回 ``0``。
    / Returns ``2`` for arguments or expected projection failures, ``1`` for launch or unknown
    failures, and ``0`` when a successful test double returns.
    """

    command = tuple(sys.argv[1:] if argv is None else argv)
    if not command:
        print("container entrypoint requires a command", file=sys.stderr)
        return 2

    console = OperatorConsole(sys.stderr)
    source_overridden = "AIWS_SOURCE_CONFIG" in os.environ
    runtime_overridden = "AIWS_CONFIG" in os.environ
    source_config_path = Path(
        os.environ.get("AIWS_SOURCE_CONFIG", str(_DEFAULT_SOURCE_CONFIG_PATH))
    )
    runtime_config_path = Path(os.environ.get("AIWS_CONFIG", str(_DEFAULT_RUNTIME_CONFIG_PATH)))
    try:
        service = compose_container(progress=console)
        service.execute(
            source_config_path,
            runtime_config_path,
            command,
            os.environ,
            source_overridden=source_overridden,
            runtime_overridden=runtime_overridden,
        )
    except ContainerProjectionError as error:
        console.failure("container-entrypoint", error, exit_code=2)
        return 2
    except ContainerLaunchError as error:
        console.failure("container-entrypoint", error, exit_code=1)
        return 1
    except Exception as error:
        console.failure("container-entrypoint", error, exit_code=1)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
