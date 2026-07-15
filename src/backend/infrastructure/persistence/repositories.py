"""@brief 强制工作区范围的 Repository 基类 / Workspace-scoped repository foundation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, cast

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from workspace_shared.tenancy import ActorScope


class TenantScopedRecord(Protocol):
    """@brief 可按租户隔离的 ORM 记录协议 / Tenant-isolated ORM record protocol.

    所有业务资源必须拥有 ``workspace_id`` 与 ``resource_owner_id``。协议仅描述
    Repository 所需的最小形状，不把领域对象绑死在某一个 ORM 基类上。
    """

    id: str
    workspace_id: str
    resource_owner_id: str


def scoped_select[ModelT: TenantScopedRecord](
    model_type: type[ModelT], scope: ActorScope
) -> Select[tuple[ModelT]]:
    """@brief 构造必含租户过滤的 SELECT / Build a SELECT with mandatory tenant predicates.

    @param model_type 包含 tenant 字段的 SQLAlchemy 映射类型。
    @param scope actor/workspace/resource-owner 范围。
    @return 同时按 workspace 与 resource owner 限定的 SELECT。
    """
    model = cast(Any, model_type)
    return select(model_type).where(
        model.workspace_id == scope.workspace_id,
        model.resource_owner_id == scope.resource_owner_id,
    )


class TenantScopedRepository[ModelT: TenantScopedRecord]:
    """@brief 所有租户 Repository 的最小实现 / Minimal implementation for tenant repositories.

    @param session 当前事务唯一使用的异步 Session。
    @param scope 不可省略的访问范围。
    @param model_type 此 Repository 管理的 ORM 类型。

    @note 此基类故意不暴露无范围的 ``get``；RLS（Row-Level Security）是数据库
    兜底，显式谓词是应用层的第二道防线。
    """

    def __init__(self, session: AsyncSession, scope: ActorScope, model_type: type[ModelT]) -> None:
        """@brief 绑定一个已有短事务 / Bind an existing short transaction.

        @param session 当前 Task 专属的 AsyncSession。
        @param scope 必填租户范围。
        @param model_type 当前表的 ORM 类型。
        @return 新建的 Repository 实例。
        """
        self._session = session
        self._scope = scope
        self._model_type = model_type

    @property
    def scope(self) -> ActorScope:
        """@brief 返回不可变访问范围 / Return immutable access scope.

        @return Repository 创建时绑定的 ActorScope。
        """
        return self._scope

    def statement(self) -> Select[tuple[ModelT]]:
        """@brief 返回该资源的有范围查询 / Return a scope-constrained statement.

        @return 已附加 workspace 与 resource owner 谓词的 SELECT。
        """
        return scoped_select(self._model_type, self._scope)

    async def get(self, record_id: str) -> ModelT | None:
        """@brief 按 ID 获取同租户记录 / Get a same-tenant record by ID.

        @param record_id 资源不透明 ID。
        @return 匹配记录，若不存在或不属于范围则返回 None。
        """
        model = cast(Any, self._model_type)
        result = await self._session.scalars(self.statement().where(model.id == record_id))
        return result.first()

    async def list(self, *, limit: int = 100) -> Sequence[ModelT]:
        """@brief 有界列出同租户记录 / List same-tenant records with a bound.

        @param limit 最多返回条数，必须为正数。
        @return 受 tenant 谓词限制的不可变记录序列。
        @raise ValueError 上限不为正数时抛出。
        """
        if limit < 1:
            raise ValueError("limit must be positive")
        result = await self._session.scalars(self.statement().limit(limit))
        return result.all()

    def add(self, record: ModelT) -> None:
        """@brief 加入与当前范围一致的新记录 / Add a record matching the current scope.

        @param record 待持久化 ORM 记录。
        @return 无返回值。
        @raise ValueError 记录的 workspace 或 owner 与 Repository 不一致时抛出。
        """
        if (
            record.workspace_id != self._scope.workspace_id
            or record.resource_owner_id != self._scope.resource_owner_id
        ):
            raise ValueError("record tenant scope does not match repository scope")
        self._session.add(record)

    async def delete(self, record: ModelT) -> None:
        """@brief 删除同租户记录 / Delete a record in the current tenant scope.

        @param record 待删除 ORM 记录。
        @return 无返回值。
        @raise ValueError 记录不属于当前范围时抛出。
        """
        if (
            record.workspace_id != self._scope.workspace_id
            or record.resource_owner_id != self._scope.resource_owner_id
        ):
            raise ValueError("record tenant scope does not match repository scope")
        await self._session.delete(record)


def scope_parameters(scope: ActorScope) -> dict[str, str]:
    """@brief 生成 SQL 参数中的租户范围 / Build tenant scope SQL parameters.

    @param scope actor/workspace/resource-owner 范围。
    @return 可安全传给参数化 SQL 的范围字典。
    """
    return {
        "actor_id": scope.actor_id,
        "workspace_id": scope.workspace_id,
        "resource_owner_id": scope.resource_owner_id,
    }


__all__ = [
    "TenantScopedRecord",
    "TenantScopedRepository",
    "scope_parameters",
    "scoped_select",
]
