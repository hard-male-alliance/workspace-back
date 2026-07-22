"""@brief Docker 容器进程入口 / Docker container process entry point."""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

from dbctl.infrastructure.runtime_projection import validate_runtime_config
from workspace_shared.jsonc import ConfigurationError

_CONFIG_PATH: Final[Path] = Path("/var/lib/aiws-config/config.jsonc")
"""Private dbctl-owned configuration mounted into every runtime service."""


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the mounted configuration and replace the entrypoint process.

    @param argv 待执行命令；``None`` 时读取 ``sys.argv``。
    / Command to execute; reads ``sys.argv`` when ``None``.
    @return 仅参数或配置错误时返回 ``2``；成功路径由目标进程替换且不返回。
    / Returns ``2`` only for argument or configuration errors; successful execution replaces the process.
    """

    command = tuple(sys.argv[1:] if argv is None else argv)
    if not command:
        print("container entrypoint requires a command", file=sys.stderr)
        return 2
    try:
        validate_runtime_config(_CONFIG_PATH)
    except ConfigurationError, OSError, ValueError:
        print(
            "container entrypoint could not read /var/lib/aiws-config/config.jsonc; "
            "run dbctl bootstrap and edit that file before starting services",
            file=sys.stderr,
        )
        return 2
    os.execvp(command[0], command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
