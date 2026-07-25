"""@brief XeLaTeX 的最小强隔离 exec launcher / Minimal strong-confinement exec launcher for XeLaTeX.

该 fresh Python child 在 exec XeLaTeX 前应用 rlimit、Landlock 与 libseccomp；生产路径
不依赖 mount namespace，也不需要任何 Linux capability。
"""

from __future__ import annotations

import json
import math
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from backend.infrastructure.process_confinement import (
    ProcessConfinementMode,
    ProcessConfinementUnavailable,
    apply_strong_confinement,
    python_runtime_read_paths,
)

_INVOCATION_ERROR_EXIT = 64
"""@brief 内部 argv 无效退出码 / Invalid internal-argv exit code."""

_SANDBOX_ERROR_EXIT = 70
"""@brief kernel confinement 无法安装退出码 / Kernel-confinement installation failure exit code."""

_MAXIMUM_FONT_DIRECTORIES = 32
"""@brief allowed font roots 数量上限 / Maximum number of allowed font roots."""


def _positive_integer(value: str) -> int:
    """@brief 解析内部正整数 / Parse an internal positive integer.

    @param value argv 字段 / argv field.
    @return 正整数 / Positive integer.
    @raise ValueError 非正整数 / Value is not a positive integer.
    """

    parsed = int(value)
    if parsed < 1:
        raise ValueError("renderer sandbox limits must be positive")
    return parsed


def _font_directories(value: str) -> tuple[Path, ...]:
    """@brief 解析父进程生成的 font-root JSON / Parse parent-generated font-root JSON.

    @param value JSON array / JSON 数组.
    @return 已验证绝对目录 / Validated absolute directories.
    @raise ValueError envelope 非闭合字符串数组 / Envelope is not a closed string array.
    """

    parsed = json.loads(value)
    if (
        not isinstance(parsed, list)
        or len(parsed) > _MAXIMUM_FONT_DIRECTORIES
        or any(not isinstance(item, str) or not item for item in parsed)
    ):
        raise ValueError("renderer font roots are invalid")
    paths = tuple(Path(item) for item in parsed)
    if any(not path.is_absolute() or not path.is_dir() for path in paths):
        raise ValueError("renderer font roots must be existing absolute directories")
    return paths


def _apply_resource_limits(
    *,
    memory_bytes: int,
    output_bytes: int,
    cpu_time_seconds: int,
) -> None:
    """@brief 在 exec 前应用 renderer hard rlimit / Apply renderer hard rlimits before exec.

    @param memory_bytes virtual address-space 上限 / Virtual-address-space limit.
    @param output_bytes 单文件输出上限 / Per-file output limit.
    @param cpu_time_seconds CPU 时间上限 / CPU-time limit.
    @return 无返回值 / No return value.
    @note 不使用按 real UID 全局计数的 ``RLIMIT_NPROC``；XeLaTeX 必须派生 PDF converter，
        进程总量由部署 cgroup 边界约束，父进程仍按 session 超时整体回收。
        / Do not use globally real-UID-scoped ``RLIMIT_NPROC``; XeLaTeX must spawn its
        PDF converter, while deployment cgroups bound process count and the parent reaps
        the whole session at its deadline.
    """

    import resource

    limits = (
        (resource.RLIMIT_CORE, 0),
        (resource.RLIMIT_CPU, cpu_time_seconds),
        (resource.RLIMIT_AS, memory_bytes),
        (resource.RLIMIT_FSIZE, output_bytes),
        (resource.RLIMIT_NOFILE, 64),
    )
    for resource_kind, hard_limit in limits:
        resource.setrlimit(resource_kind, (hard_limit, hard_limit))


def _read_only_paths(
    executable: Path,
    font_directories: Sequence[Path],
) -> tuple[Path, ...]:
    """@brief 构造 XeLaTeX 所需只读 allowlist / Build the read-only allowlist required by XeLaTeX.

    @param executable 已验证 XeLaTeX executable / Validated XeLaTeX executable.
    @param font_directories 配置允许的额外字体目录 / Configured additional font directories.
    @return runtime、TeX 与 font state roots / Runtime, TeX, and font-state roots.
    """

    return (
        *python_runtime_read_paths(),
        Path("/etc/fonts"),
        Path("/etc/papersize"),
        Path("/etc/paperspecs"),
        Path("/etc/texmf"),
        Path("/var/cache/fontconfig"),
        Path("/var/lib/texmf"),
        executable.parent,
        *font_directories,
    )


def _xelatex_environment(workdir: Path) -> dict[str, str]:
    """@brief 生成不继承 backend secrets 的 XeLaTeX 环境 / Build an XeLaTeX environment that inherits no backend secrets.

    @param workdir 唯一可写私有目录 / Sole writable private directory.
    @return 最小 environment / Minimal environment.
    """

    return {
        "HOME": str(workdir),
        "XDG_CACHE_HOME": str(workdir / "cache"),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "TEXMFCONFIG": str(workdir / "texmf-config"),
        "TEXMFVAR": str(workdir / "texmf-var"),
        "TMPDIR": str(workdir / "tmp"),
    }


def main(arguments: Sequence[str] | None = None) -> int:
    """@brief 应用边界并 exec 固定 XeLaTeX command / Apply boundaries and exec the fixed XeLaTeX command.

    @param arguments 测试可注入 argv / Test-injectable argv.
    @return 仅 setup 失败时返回；成功由 XeLaTeX 决定 / Returns only on setup failure; XeLaTeX owns successful-path status.
    """

    argv = list(sys.argv[1:] if arguments is None else arguments)
    if len(argv) != 7:
        return _INVOCATION_ERROR_EXIT
    try:
        mode = ProcessConfinementMode(argv[0])
        workdir = Path(argv[1]).resolve(strict=True)
        invocation_name = Path(argv[2]).name
        executable = Path(argv[2]).resolve(strict=True)
        memory_bytes = _positive_integer(argv[3])
        output_bytes = _positive_integer(argv[4])
        timeout_ms = _positive_integer(argv[5])
        font_directories = _font_directories(argv[6])
        if not workdir.is_dir() or not executable.is_file():
            raise ValueError("renderer sandbox paths are invalid")
        for name in ("cache", "cache/fontconfig", "texmf-config", "texmf-var", "tmp"):
            target = workdir / name
            target.mkdir(mode=0o700, exist_ok=True)
        os.chdir(workdir)
        _apply_resource_limits(
            memory_bytes=memory_bytes,
            output_bytes=output_bytes,
            cpu_time_seconds=max(1, math.ceil(timeout_ms / 1000) + 1),
        )
        if mode is ProcessConfinementMode.STRONG:
            apply_strong_confinement(
                read_only_paths=_read_only_paths(executable, font_directories),
                read_write_paths=(workdir,),
            )
    except (
        ImportError,
        OSError,
        ProcessConfinementUnavailable,
        TypeError,
        ValueError,
    ):
        return _SANDBOX_ERROR_EXIT
    command = [
        invocation_name,
        "-no-shell-escape",
        "-halt-on-error",
        "-file-line-error",
        "-interaction=nonstopmode",
        f"-output-directory={workdir}",
        "resume.tex",
    ]
    try:
        os.execve(str(executable), command, _xelatex_environment(workdir))
    except OSError:
        return _SANDBOX_ERROR_EXIT


if __name__ == "__main__":
    raise SystemExit(main())


__all__: list[str] = []
"""@brief 本模块不暴露进程内 API / This module exposes no in-process API."""
