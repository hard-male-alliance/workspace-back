"""@brief 不透明资源 ID 工具 / Opaque resource identifier utilities."""

from __future__ import annotations

from uuid import uuid7


def new_opaque_id(prefix: str) -> str:
    """@brief 生成时间有序的不透明 ID / Create a time-ordered opaque ID.

    @param prefix 资源类型前缀 / Resource type prefix.
    @return 不含业务数据的稳定格式 ID / Stable ID without business data.
    @raise ValueError 前缀非法时抛出 / Raised for an invalid prefix.
    """
    if not prefix.replace("_", "").isalnum() or not prefix[0].isalpha():
        raise ValueError("prefix must start with a letter and contain only letters, digits, or underscores")
    return f"{prefix}_{uuid7().hex}"
