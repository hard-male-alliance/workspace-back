"""@brief PostgreSQL 临时凭证文件基础设施 / Temporary PostgreSQL credential-file infrastructure."""

from __future__ import annotations

import tempfile
from pathlib import Path


def create_pgpass_file(username: str, password: str, *, prefix: str) -> Path:
    """@brief 创建权限为 0600 的单身份 pgpass / Create a mode-0600, single-identity pgpass.

    @param username PostgreSQL 登录用户名 / PostgreSQL login username.
    @param password PostgreSQL 登录密码 / PostgreSQL login password.
    @param prefix 临时文件安全前缀 / Safe temporary-file prefix.
    @return 已关闭、可供子进程读取的文件路径 / Closed path ready for a subprocess.
    @raise OSError 创建、写入或收紧权限失败时抛出 / Raised on create, write, or chmod failure.
    """
    path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=prefix,
            delete=False,
        ) as password_file:
            password_file.write(
                f"*:*:*:{_escape_pgpass_field(username)}:{_escape_pgpass_field(password)}\n"
            )
            path = Path(password_file.name)
        path.chmod(0o600)
        return path
    except OSError:
        if path is not None:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        raise


def _escape_pgpass_field(value: str) -> str:
    """@brief 转义 pgpass 字段 / Escape one pgpass field.

    @param value 用户名或密码 / Username or password.
    @return 已转义的字段 / Escaped field.
    """
    return value.replace("\\", "\\\\").replace(":", "\\:")
