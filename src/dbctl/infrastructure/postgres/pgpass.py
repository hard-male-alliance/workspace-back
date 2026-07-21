"""@brief 精确目标的临时 libpq 密码租约 / Exact-target temporary libpq password leases."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from dbctl.domain.database import DatabaseTarget
from dbctl.domain.names import RoleName
from dbctl.domain.roles import Secret

_PRIVATE_FILE_MODE: Final[int] = 0o600
"""@brief PostgreSQL 密码文件固定权限 / Fixed PostgreSQL password-file mode."""


@dataclass(slots=True)
class PgpassLease:
    """@brief 拥有临时 pgpass 生命周期的租约 / Lease owning a temporary pgpass lifecycle.

    @param path 只应放入 ``PGPASSFILE`` 的临时文件路径 / Temporary path used only as ``PGPASSFILE``.
    """

    path: Path
    _closed: bool = False

    def close(self) -> None:
        """@brief 幂等删除临时密码文件 / Idempotently remove the temporary password file.

        @return 无返回值 / No return value.
        @raise OSError 删除失败时抛出 / Raised when deletion fails.
        """

        if self._closed:
            return
        self.path.unlink(missing_ok=True)
        self._closed = True

    def __enter__(self) -> PgpassLease:
        """@brief 进入密码租约上下文 / Enter the password-lease context.

        @return 当前租约 / This lease.
        """

        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: object | None,
    ) -> None:
        """@brief 离开上下文并删除密码文件 / Leave the context and remove the password file.

        @param _exception_type 未使用的异常类型 / Unused exception type.
        @param _exception 未使用的异常对象 / Unused exception value.
        @param _traceback 未使用的 traceback / Unused traceback.
        @return 无返回值 / No return value.
        """

        self.close()


def create_pgpass_lease(
    *,
    target: DatabaseTarget,
    username: RoleName,
    password: Secret[str],
    prefix: str,
) -> PgpassLease:
    """@brief 创建只匹配一个 target/role 的 pgpass / Create a pgpass matching one target and role.

    @param target 精确 host、port 与 database / Exact host, port, and database.
    @param username 精确 PostgreSQL 登录角色 / Exact PostgreSQL login role.
    @param password 仅在写入临时文件时揭示的密码 / Password revealed only while writing the file.
    @param prefix 安全的临时文件前缀 / Safe temporary-file prefix.
    @return 拥有 ``0600`` 临时文件的 PgpassLease / PgpassLease owning a mode-``0600`` file.
    @raise ValueError 任一字段会破坏 pgpass 单行语法时抛出。
    / Raised when any field could break pgpass's single-line syntax.
    @raise OSError 创建、写入或同步失败时抛出 / Raised on creation, writing, or syncing failure.
    """

    fields = (
        target.host,
        str(target.port),
        target.database.value,
        username.value,
        password.reveal(),
    )
    if any(not value or "\x00" in value or "\r" in value or "\n" in value for value in fields):
        raise ValueError("pgpass 字段不能为空或包含控制字符。")
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=prefix,
            delete=False,
        ) as password_file:
            temporary_path = Path(password_file.name)
            os.chmod(password_file.name, _PRIVATE_FILE_MODE)
            password_file.write(":".join(_escape_field(value) for value in fields) + "\n")
            password_file.flush()
            os.fsync(password_file.fileno())
        return PgpassLease(temporary_path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def _escape_field(value: str) -> str:
    """@brief 转义一个 pgpass 字段 / Escape one pgpass field.

    @param value 已拒绝控制字符的字段 / Field already free of control characters.
    @return 反斜杠和冒号已转义的字段 / Field with backslashes and colons escaped.
    """

    return value.replace("\\", "\\\\").replace(":", "\\:")


__all__ = ["PgpassLease", "create_pgpass_lease"]
