"""Minimal strong-confinement launcher for Interview video frame extraction."""

from __future__ import annotations

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
_SANDBOX_ERROR_EXIT = 70


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError("video sandbox values must be positive")
    return parsed


def _limits(*, timeout_ms: int, frame_bytes: int) -> None:
    import resource

    for resource_kind, hard_limit in (
        (resource.RLIMIT_CORE, 0),
        (resource.RLIMIT_CPU, max(1, math.ceil(timeout_ms / 1_000) + 1)),
        (resource.RLIMIT_AS, 1_073_741_824),
        (resource.RLIMIT_FSIZE, frame_bytes),
        (resource.RLIMIT_NOFILE, 64),
    ):
        resource.setrlimit(resource_kind, (hard_limit, hard_limit))


def main(arguments: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if arguments is None else arguments)
    if len(argv) != 8:
        return _INVOCATION_ERROR_EXIT
    try:
        mode = ProcessConfinementMode(argv[0])
        workdir = Path(argv[1]).resolve(strict=True)
        executable = Path(argv[2]).resolve(strict=True)
        extension = argv[3]
        interval = _positive(argv[4])
        maximum_frames = _positive(argv[5])
        timeout_ms = _positive(argv[6])
        frame_bytes = _positive(argv[7])
        if (
            extension not in {"webm", "mp4"}
            or not workdir.is_dir()
            or not executable.is_file()
            or not (workdir / f"input.{extension}").is_file()
        ):
            raise ValueError("video sandbox paths are invalid")
        os.chdir(workdir)
        _limits(timeout_ms=timeout_ms, frame_bytes=frame_bytes)
        if mode is ProcessConfinementMode.STRONG:
            apply_strong_confinement(
                read_only_paths=(*python_runtime_read_paths(), executable.parent),
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
        executable.name,
        "-nostdin",
        "-v",
        "error",
        "-threads",
        "1",
        "-filter_threads",
        "1",
        "-i",
        f"input.{extension}",
        "-vf",
        (
            f"fps=1/{interval},"
            "scale=1280:-2:force_original_aspect_ratio=decrease"
        ),
        "-frames:v",
        str(maximum_frames),
        "-threads",
        "1",
        "-q:v",
        "4",
        "frame-%03d.jpg",
    ]
    try:
        os.execve(
            str(executable),
            command,
            {
                "HOME": str(workdir),
                "PATH": "/usr/bin:/bin",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "TMPDIR": str(workdir),
            },
        )
    except OSError:
        return _SANDBOX_ERROR_EXIT


if __name__ == "__main__":
    raise SystemExit(main())


__all__: list[str] = []
