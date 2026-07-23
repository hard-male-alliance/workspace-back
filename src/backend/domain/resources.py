"""@brief API V2 跨领域资源引用原语 / API V2 cross-domain resource-reference primitive.

``ResourceRef`` 出现在 Resume、Knowledge、Agent、Interview、Job、Artifact、Event 与 Audit
边界。把它放在任一具体子域都会倒置依赖方向；本模块因此只依赖最底层领域错误，并保持
值对象不可变、无 I/O、无 transport 语义。
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from backend.domain.principals import DomainInvariantError

_OPAQUE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API V2 OpaqueId 语法 / API V2 OpaqueId grammar."""

_RESOURCE_TYPE = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief ResourceRef 类型名语法 / ResourceRef type-name grammar."""


@dataclass(frozen=True, slots=True)
class ResourceRef:
    """@brief 跨领域资源的稳定、可选版本引用 / Stable optionally versioned cross-domain resource reference.

    @param resource_type 小写稳定资源类型 / Stable lower-case resource type.
    @param id API V2 不透明资源标识 / API V2 opaque resource identifier.
    @param revision 可选的一起读取版本 / Optional revision read with the reference.
    """

    resource_type: str
    id: str
    revision: int | None = None

    def __post_init__(self) -> None:
        """@brief 校验契约级引用形状 / Validate the contract-level reference shape.

        @raise DomainInvariantError 类型、ID 或 revision 不符合 API V2 时抛出 / Raised when
            the type, ID, or revision violates API V2.
        """
        if _RESOURCE_TYPE.fullmatch(self.resource_type) is None:
            raise DomainInvariantError("resource reference type is invalid")
        if _OPAQUE_ID.fullmatch(self.id) is None:
            raise DomainInvariantError("resource reference id is invalid")
        if self.revision is not None and (
            isinstance(self.revision, bool) or self.revision < 1
        ):
            raise DomainInvariantError("resource reference revision must be at least one")


__all__ = ["ResourceRef"]
