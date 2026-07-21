"""@brief 私密文件持久化基础设施 / Private-file persistence infrastructure."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Final

_PRIVATE_FILE_MODE: Final[int] = 0o600
"""@brief 私密文件的固定权限 / Fixed mode for private files."""


def atomic_write_private_text(path: Path, content: str) -> None:
    """@brief 原子写入已落盘同步的私密文本 / Atomically write fsynced private text.

    @param path 目标文件路径 / Destination file path.
    @param content 待写入的 UTF-8 文本 / UTF-8 text to write.
    @return 无返回值 / No return value.
    @raise OSError 创建临时文件、写入、同步、替换或清理失败时抛出。
    / Raised when temporary-file creation, writing, syncing, replacement, or cleanup fails.

    @note 临时文件固定创建在目标目录中，先以 ``0600`` 权限完成写入与 ``fsync``，
    再通过同文件系统的原子替换发布；任何失败路径都会尽力删除未发布的临时文件。
    / The temporary file is created in the destination directory, written with mode ``0600`` and
    fsynced before an atomic same-filesystem replacement. Every failure path attempts to remove an
    unpublished temporary file.
    """

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            os.chmod(temporary.name, _PRIVATE_FILE_MODE)
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
