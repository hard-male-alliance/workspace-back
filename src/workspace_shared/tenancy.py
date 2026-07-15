"""@brief 多租户边界数据类型 / Multi-tenant boundary data types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ActorScope:
    """@brief 访问主体与资源范围 / Actor and resource scope.

    @note 每个 Repository 调用都必须收到该范围，禁止无 workspace 的查询。
    """

    actor_id: str
    workspace_id: str
    resource_owner_id: str

    def __post_init__(self) -> None:
        """@brief 验证所有租户边界字段 / Validate all tenant-boundary fields.

        @raise ValueError 字段为空时抛出 / Raised when a field is empty.
        """
        if not all((self.actor_id, self.workspace_id, self.resource_owner_id)):
            raise ValueError("actor_id, workspace_id, and resource_owner_id are required")
