"""@brief 后端 PostgreSQL 持久化基础设施 / Backend PostgreSQL persistence infrastructure.

该包只提供 ORM（Object-Relational Mapping）、短事务和迁移元数据；业务用例应从
domain/application 层经 Repository 访问它，且后端启动绝不自动运行 Alembic。
"""

from .database import (
    AsyncDatabase,
    AsyncDatabaseOptions,
    create_session_factory,
    install_tenant_scope,
    normalize_asyncpg_dsn,
)
from .models import Base
from .observability import PostgresTelemetryWriter
from .repositories import TenantScopedRepository, scope_parameters, scoped_select
from .runtime_repository import (
    PostgresIdempotencyRegistry,
    PostgresWorkspaceRepository,
)

__all__ = [
    "AsyncDatabase",
    "AsyncDatabaseOptions",
    "Base",
    "PostgresIdempotencyRegistry",
    "PostgresTelemetryWriter",
    "PostgresWorkspaceRepository",
    "TenantScopedRepository",
    "create_session_factory",
    "install_tenant_scope",
    "normalize_asyncpg_dsn",
    "scope_parameters",
    "scoped_select",
]
