"""@brief Knowledge parser 的最小隔离进程入口 / Minimal isolated-process entry point for Knowledge parsing.

本模块在读取不可信 stdin、加载 pypdf/python-docx 或 Knowledge parser 前，先预开唯一
result descriptor，再应用 POSIX rlimit 与生产 Landlock/libseccomp。它只能由
``LocalKnowledgeFileParser`` 通过 ``python -I -B -m`` 启动。
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
"""@brief 内部 argv 协议非法退出码 / Exit code for an invalid internal argv protocol."""

_SANDBOX_ERROR_EXIT = 70
"""@brief rlimit 或强隔离安装失败退出码 / Exit code for rlimit or strong-confinement failure."""

_RESULT_ERROR_EXIT = 74
"""@brief result envelope 无法安全写入退出码 / Exit code when the result envelope cannot be safely written."""

_FORMATS = frozenset({"pdf", "docx", "text", "markdown"})
"""@brief child 接受的闭合格式 / Closed formats accepted by the child."""

_DETERMINISTIC_FAILURE_CODES = frozenset(
    {
        "knowledge.file_encoding_invalid",
        "knowledge.pdf_encrypted",
        "knowledge.pdf_invalid",
        "knowledge.docx_invalid",
        "knowledge.file_no_extractable_text",
        "knowledge.extracted_text_too_large",
        "knowledge.file_too_complex",
    }
)
"""@brief 允许跨进程边界的文档错误码 / Document-error codes allowed across the process boundary."""


def _positive_integer(value: str) -> int:
    """@brief 解析内部协议正整数 / Parse a positive integer from the internal protocol."""

    parsed = int(value)
    if parsed < 1:
        raise ValueError("Knowledge sandbox limits must be positive")
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
    """@brief 在加载不可信 parser 前降低 POSIX hard limits / Lower POSIX hard limits before loading untrusted parsers."""

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
    """@brief 有界读取不可信 stdin / Bounded-read untrusted stdin."""

    payload = sys.stdin.buffer.read(maximum_input_bytes + 1)
    if len(payload) > maximum_input_bytes:
        raise ValueError("Knowledge parser input exceeds its byte budget")
    return payload


def _parse_document(
    payload: bytes,
    parser_format: str,
    maximum_extracted_characters: int,
    maximum_parts: int,
) -> dict[str, object]:
    """@brief 延迟加载 parser 并生成闭合 result envelope / Lazily load parsers and build a closed result envelope."""

    from backend.domain.common import DomainError
    from backend.infrastructure.knowledge_parsing import (
        _KnowledgeFileFormat,
        _parse_document_sync,
    )

    try:
        document = _parse_document_sync(
            payload,
            _KnowledgeFileFormat(parser_format),
            maximum_extracted_characters,
            maximum_parts,
        )
        return {
            "ok": True,
            "parts": [
                {
                    "text": part.text,
                    "content_type": part.content_type.value,
                    "metadata": part.metadata,
                }
                for part in document.parts
            ],
            "metadata": document.metadata,
        }
    except DomainError as error:
        code = error.problem.code
        if code not in _DETERMINISTIC_FAILURE_CODES:
            code = _invalid_document_code(parser_format)
        return {"ok": False, "code": code}
    except Exception:
        return {"ok": False, "code": _invalid_document_code(parser_format)}


def _invalid_document_code(parser_format: str) -> str:
    """@brief 将未知 parser 失败收敛到格式稳定错误 / Collapse unknown parser failures to a format-stable error."""

    if parser_format == "pdf":
        return "knowledge.pdf_invalid"
    if parser_format == "docx":
        return "knowledge.docx_invalid"
    return "knowledge.file_encoding_invalid"


def _open_result(path: Path) -> int:
    """@brief 隔离前打开父进程预建的 regular result inode / Open the parent-created regular result inode before confinement."""

    flags = os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC
    descriptor = os.open(path, flags)
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise OSError("Knowledge parser result must be a regular file")
    return descriptor


def _write_result(
    descriptor: int,
    value: dict[str, object],
    maximum_result_bytes: int,
) -> None:
    """@brief 通过预开 descriptor 写入有界 canonical JSON / Write bounded canonical JSON through a pre-opened descriptor."""

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if not encoded or len(encoded) > maximum_result_bytes:
        raise ValueError("Knowledge parser result exceeds its byte budget")
    os.ftruncate(descriptor, 0)
    os.lseek(descriptor, 0, os.SEEK_SET)
    written = os.write(descriptor, encoded)
    if written != len(encoded):
        raise OSError("Knowledge parser result write was incomplete")


def main(arguments: Sequence[str] | None = None) -> int:
    """@brief 验证 argv、先施加隔离、再读取 stdin / Validate argv, confine first, then read stdin."""

    argv = list(sys.argv[1:] if arguments is None else arguments)
    if len(argv) != 11:
        return _INVOCATION_ERROR_EXIT
    result_path = Path(argv[0])
    parser_format = argv[1]
    if parser_format not in _FORMATS:
        return _INVOCATION_ERROR_EXIT
    try:
        confinement_mode = ProcessConfinementMode(argv[2])
        (
            maximum_input_bytes,
            maximum_extracted_characters,
            maximum_parts,
            maximum_result_bytes,
            cpu_time_seconds,
            memory_bytes,
            open_files,
            processes,
        ) = (_positive_integer(value) for value in argv[3:])
    except TypeError, ValueError:
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
        result = _parse_document(
            payload,
            parser_format,
            maximum_extracted_characters,
            maximum_parts,
        )
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
