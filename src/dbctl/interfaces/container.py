"""@brief Docker 容器进程入口 / Docker container process entry point."""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

from dbctl.infrastructure.runtime_projection import write_runtime_config
from workspace_shared.jsonc import ConfigurationError

_DEFAULT_SOURCE_CONFIG_PATH: Final[Path] = Path("/var/lib/aiws-config/config.jsonc")
"""@brief dbctl 持久配置默认路径 / Default path of the persistent dbctl-owned configuration."""

_DEFAULT_RUNTIME_CONFIG_PATH: Final[Path] = Path("/tmp/aiws/config.jsonc")
"""@brief 容器运行副本默认路径 / Default path of the container runtime projection."""


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 投影配置后以目标进程替换入口 / Project configuration and replace the entrypoint process.

    @param argv 待执行命令；``None`` 时读取 ``sys.argv``。
    / Command to execute; reads ``sys.argv`` when ``None``.
    @return 仅参数或配置错误时返回 ``2``；成功路径由目标进程替换且不返回。
    / Returns ``2`` only for argument or configuration errors; successful execution replaces the process.
    """

    command = tuple(sys.argv[1:] if argv is None else argv)
    if not command:
        print("container entrypoint requires a command", file=sys.stderr)
        return 2
    source_config_path = Path(
        os.environ.get("AIWS_SOURCE_CONFIG", str(_DEFAULT_SOURCE_CONFIG_PATH))
    )
    runtime_config_path = Path(os.environ.get("AIWS_CONFIG", str(_DEFAULT_RUNTIME_CONFIG_PATH)))
    try:
        write_runtime_config(source_config_path, runtime_config_path, os.environ)
    except ConfigurationError, OSError, ValueError:
        print(
            "container entrypoint could not read the dbctl-generated configuration; "
            "run dbctl bootstrap first",
            file=sys.stderr,
        )
        return 2
    os.execvpe(command[0], command, os.environ)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
