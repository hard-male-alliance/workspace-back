"""@brief Resume import parser 的最小隔离进程入口 / Minimal isolated-process entry point for Resume import parsing.

本模块在读取不可信 stdin、加载 pypdf/python-docx 及业务模块前，先预开唯一 result
descriptor，并应用 POSIX rlimit 与生产 Landlock/libseccomp。它不是可复用 API，只能
由 :mod:`backend.infrastructure.resume_worker` 以 ``python -I -m`` 启动。
"""

from __future__ import annotations

import json
import os
import stat
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from backend.infrastructure.process_confinement import (
    ProcessConfinementMode,
    ProcessConfinementUnavailable,
    apply_strong_confinement,
    python_runtime_read_paths,
)

_INVOCATION_ERROR_EXIT = 64
"""@brief argv 不满足内部协议的退出码 / Exit code for an invalid internal invocation."""

_SANDBOX_ERROR_EXIT = 70
"""@brief POSIX 资源限制无法应用的退出码 / Exit code when POSIX limits cannot be applied."""

_RESULT_ERROR_EXIT = 74
"""@brief result envelope 无法安全写入的退出码 / Exit code when the result envelope cannot be safely written."""

_DETERMINISTIC_FAILURE_CODES = frozenset(
    {
        "resume.import_empty_document",
        "resume.import_format_unsupported",
        "resume.import_invalid_document",
        "resume.import_invalid_encoding",
        "resume.import_text_too_large",
        "resume.import_too_many_pages",
    }
)
"""@brief 允许跨进程边界的错误码 / Error codes allowed across the process boundary."""


def _positive_integer(value: str) -> int:
    """@brief 解析内部协议的正整数 / Parse a positive integer from the internal protocol.

    @param value argv 字段 / argv field.
    @return 正整数 / Positive integer.
    @raise ValueError 字段不是正整数 / The field is not a positive integer.
    """

    parsed = int(value)
    if parsed < 1:
        raise ValueError("sandbox limits must be positive")
    return parsed


def _apply_resource_limits(
    resource_module: Any,
    *,
    cpu_time_seconds: int,
    memory_bytes: int,
    result_bytes: int,
    open_files: int,
    processes: int,
) -> None:
    """@brief 在加载文档 parser 前降低 POSIX hard limits / Lower POSIX hard limits before loading document parsers.

    @param resource_module Unix ``resource`` module / Unix ``resource`` 模块.
    @param cpu_time_seconds CPU 时间秒数 / CPU seconds.
    @param memory_bytes virtual address-space 字节上限 / Virtual-address-space byte limit.
    @param result_bytes 可创建文件的字节上限 / Created-file byte limit.
    @param open_files file-descriptor 上限 / File-descriptor limit.
    @param processes 可创建的进程上限 / Process-creation limit.
    @return 无返回值 / No return value.
    @note RLIMIT_CORE=0 防止敏感文档进入 core dump；其余限制分别约束
    CPU、address space、文件输出、descriptor 与派生进程。
    """

    limits = (
        (resource_module.RLIMIT_CORE, 0),
        (resource_module.RLIMIT_CPU, cpu_time_seconds),
        (resource_module.RLIMIT_AS, memory_bytes),
        (resource_module.RLIMIT_FSIZE, result_bytes),
        (resource_module.RLIMIT_NOFILE, open_files),
        (resource_module.RLIMIT_NPROC, processes),
    )
    for resource_kind, hard_limit in limits:
        resource_module.setrlimit(resource_kind, (hard_limit, hard_limit))


def _read_stdin_limited(maximum_input_bytes: int) -> bytes:
    """@brief 从 stdin 有界读取已验证文档 / Bounded-read the verified document from stdin.

    @param maximum_input_bytes 输入字节硬上限 / Hard input-byte limit.
    @return 完整文档字节 / Complete document bytes.
    @raise ValueError 父进程违反内部输入上限 / Parent violates the internal input limit.
    """

    payload = sys.stdin.buffer.read(maximum_input_bytes + 1)
    if len(payload) > maximum_input_bytes:
        raise ValueError("Resume import child input exceeds its byte budget")
    return payload


def _parse_document(payload: bytes, media_type: str, maximum_text_characters: int) -> dict[str, object]:
    """@brief 调用受限 parser 并生成白名单 result / Invoke bounded parsers and build an allowlisted result.

    @param payload 文档字节 / Document bytes.
    @param media_type 服务端 sniff 的 MIME / Server-sniffed MIME type.
    @param maximum_text_characters 规范文本字符上限 / Normalized-text character limit.
    @return 成功文本或稳定错误 envelope / Success-text or stable-error envelope.
    @note 延迟 import 保证不可信 parser 代码在 rlimit 之后才加载。
    """

    from backend.application.ports.resume_worker import ResumeCapabilityFailure
    from backend.infrastructure.resume_worker import _extract_import_text, _normalize_import_text

    try:
        text = _normalize_import_text(_extract_import_text(payload, media_type))
        if not text:
            raise ResumeCapabilityFailure("resume.import_empty_document", retryable=False)
        if len(text) > maximum_text_characters:
            raise ResumeCapabilityFailure("resume.import_text_too_large", retryable=False)
        return {"ok": True, "text": text}
    except ResumeCapabilityFailure as error:
        code = error.code if error.code in _DETERMINISTIC_FAILURE_CODES else "resume.import_invalid_document"
        return {"ok": False, "code": code}
    except Exception:
        return {"ok": False, "code": "resume.import_invalid_document"}


def _open_result(path: Path) -> int:
    """@brief 在启用隔离前打开父进程预建 result inode / Open the parent-created result inode before confinement.

    @param path 私有 result path / Private result path.
    @return 保持到解析完成的可写 descriptor / Writable descriptor retained through parsing.
    @raise OSError 路径不是可安全打开的现有 regular file / Path is not a safely openable existing regular file.
    """

    flags = os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC
    descriptor = os.open(path, flags)
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise OSError("Resume import result must be a regular file")
    return descriptor


def _write_result(descriptor: int, value: dict[str, object], maximum_result_bytes: int) -> None:
    """@brief 通过预开 descriptor 填充有界 JSON result / Fill bounded JSON through a pre-opened descriptor.

    @param descriptor 隔离前预开的 result descriptor / Result descriptor opened before confinement.
    @param value 白名单 result envelope / Allowlisted result envelope.
    @param maximum_result_bytes JSON 字节硬上限 / Hard JSON-byte limit.
    @return 无返回值 / No return value.
    @raise ValueError envelope 编码超限 / Encoded envelope exceeds its limit.
    """

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if not encoded or len(encoded) > maximum_result_bytes:
        raise ValueError("Resume import result exceeds its byte budget")
    os.ftruncate(descriptor, 0)
    os.lseek(descriptor, 0, os.SEEK_SET)
    written = os.write(descriptor, encoded)
    if written != len(encoded):
        raise OSError("Resume import result write was incomplete")


def main(arguments: Sequence[str] | None = None) -> int:
    """@brief 验证 argv、先设 rlimit，再解析 stdin / Validate argv, set rlimits first, then parse stdin.

    @param arguments 仅测试可注入的 argv；缺省使用 ``sys.argv`` / Test-injectable argv, defaulting to ``sys.argv``.
    @return 内部协议退出码 / Internal-protocol exit code.
    """

    argv = list(sys.argv[1:] if arguments is None else arguments)
    if len(argv) != 10:
        return _INVOCATION_ERROR_EXIT
    result_path = Path(argv[0])
    media_type = argv[1]
    try:
        confinement_mode = ProcessConfinementMode(argv[2])
        (
            maximum_input_bytes,
            maximum_text_characters,
            maximum_result_bytes,
            cpu_time_seconds,
            memory_bytes,
            open_files,
            processes,
        ) = (_positive_integer(value) for value in argv[3:])
    except (TypeError, ValueError):
        return _INVOCATION_ERROR_EXIT
    result_descriptor = -1
    try:
        import resource

        result_descriptor = _open_result(result_path)
        _apply_resource_limits(
            resource,
            cpu_time_seconds=cpu_time_seconds,
            memory_bytes=memory_bytes,
            result_bytes=maximum_result_bytes,
            open_files=open_files,
            processes=processes,
        )
        if confinement_mode is ProcessConfinementMode.STRONG:
            apply_strong_confinement(read_only_paths=python_runtime_read_paths())
    except (
        AttributeError,
        ImportError,
        OSError,
        ProcessConfinementUnavailable,
        ValueError,
    ):
        if result_descriptor >= 0:
            os.close(result_descriptor)
        return _SANDBOX_ERROR_EXIT
    try:
        payload = _read_stdin_limited(maximum_input_bytes)
        result = _parse_document(payload, media_type, maximum_text_characters)
        _write_result(result_descriptor, result, maximum_result_bytes)
    except Exception:
        return _RESULT_ERROR_EXIT
    finally:
        os.close(result_descriptor)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__: list[str] = []
"""@brief 本模块不暴露应用内 API / This module exposes no in-process application API."""
