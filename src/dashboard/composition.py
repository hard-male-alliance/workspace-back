"""Dashboard 的独立组合根（composition root）。"""

from __future__ import annotations

import inspect
import os
from collections.abc import Callable, Mapping
from pathlib import Path

from .access import DashboardAccessPolicy
from .config import DashboardConfigService, DashboardSettings
from .errors import (
    DashboardConfigurationError,
    DashboardUnavailableError,
    DashboardValidationError,
)
from .models import DashboardScope
from .ports import ObservabilityRepository
from .repositories import (
    MemoryObservabilityRepository,
    PostgresObservabilityRepository,
    SqlAlchemyAsyncRowFetcher,
)
from .service import DashboardService

RepositoryFactory = Callable[[DashboardSettings], ObservabilityRepository]


class DashboardApplication:
    """@brief Dashboard 入口适配器共享的已组合应用（composed application）。

    @param settings: 不可变配置快照。
    @param repository: 注入的可观测性只读仓库。
    @param service: CLI、API 与 GUI 共同使用的应用服务。
    @param owns_repository: 是否由本组合根负责关闭仓库资源。
    @param access_policy: 强制入口使用受控 operator 身份的访问策略。
    """

    def __init__(
        self,
        *,
        settings: DashboardSettings,
        repository: ObservabilityRepository,
        service: DashboardService,
        owns_repository: bool,
        access_policy: DashboardAccessPolicy | None = None,
    ) -> None:
        """@brief 创建已装配的 DashboardApplication。

        @param settings: DashboardSettings 配置快照。
        @param repository: ObservabilityRepository 实现。
        @param service: 绑定同一 repository 与 settings 的 DashboardService。
        @param owns_repository: 为 True 时 aclose 会尝试关闭 repository。
        @param access_policy: 可选访问策略；省略时仅能为 memory/mock 配置派生默认策略。
        @return: 新建的 DashboardApplication 实例。
        """

        self.settings = settings
        self.repository = repository
        self.service = service
        self._owns_repository = owns_repository
        self._access_policy = access_policy or DashboardAccessPolicy.from_settings(settings, {})
        self._closed = False

    def scope_for_local_operator(
        self,
        workspace_id: str,
        *,
        requested_actor_id: str | None = None,
    ) -> DashboardScope:
        """@brief 为受控 CLI/GUI 入口解析一个工作区范围（local operator scope）。

        @param workspace_id: 必填单一工作区标识。
        @param requested_actor_id: 可选审计声明；只能等于配置 operator_id。
        @return: 已验证的 DashboardScope。
        @raise DashboardUnavailableError: 应用已经关闭时抛出。
        """

        self._ensure_open()
        return self._access_policy.scope_for_local_operator(
            workspace_id,
            requested_actor_id=requested_actor_id,
        )

    def scope_for_http_operator(
        self,
        workspace_id: str,
        *,
        presented_token: str | None,
        requested_actor_id: str | None = None,
    ) -> DashboardScope:
        """@brief 为 HTTP API 验证凭证并解析单一工作区范围。

        @param workspace_id: 必填单一工作区标识。
        @param presented_token: 受控 header 携带的 operator token。
        @param requested_actor_id: 可选审计声明；不能用于伪造身份。
        @return: 已验证且固定 operator_id 的 DashboardScope。
        @raise DashboardUnavailableError: 应用已经关闭时抛出。
        """

        self._ensure_open()
        return self._access_policy.scope_for_http_operator(
            workspace_id,
            presented_token=presented_token,
            requested_actor_id=requested_actor_id,
        )

    async def aclose(self) -> None:
        """@brief 在应用生命周期末尾关闭由组合根拥有的资源。

        @return: 无返回值；重复调用幂等（idempotent）。

        @note 注入仓库默认由调用方拥有，除非 composition root 自己创建了它。
        """

        if self._closed:
            return
        self._closed = True
        if not self._owns_repository:
            return

        close = getattr(self.repository, "aclose", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    def _ensure_open(self) -> None:
        """@brief 拒绝生命周期结束后的数据读取。

        @return: 无返回值。
        @raise DashboardUnavailableError: 应用已关闭时抛出。
        """

        if self._closed:
            raise DashboardUnavailableError("Dashboard 应用已经关闭。")


class DashboardCompositionRoot:
    """@brief 组装 Dashboard 配置、仓库与应用服务的独立根（composition root）。

    @param config_service: Dashboard 专属配置服务；缺省时读取当前目录 config.jsonc。
    @param repository_factory: 可选仓库工厂；缺省时按 database.mode 选择 memory 或 PostgreSQL。
    @param environ: 可注入环境变量映射；只读取 DSN 和 operator token，不记录 secret。

    @note 该类从不 import backend 或 dbctl，数据库连接可由外层以 repository_factory 注入。
    """

    def __init__(
        self,
        *,
        config_service: DashboardConfigService | None = None,
        repository_factory: RepositoryFactory | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        """@brief 创建组合根但不立即读取配置或创建连接。

        @param config_service: 可替换的 DashboardConfigService。
        @param repository_factory: 可替换的 ObservabilityRepository 工厂。
        @param environ: 可替换环境变量映射，适合测试和受控部署。
        @return: 新建的 DashboardCompositionRoot 实例。
        """

        self._config_service = config_service or DashboardConfigService()
        self._repository_factory = repository_factory
        self._environ = dict(os.environ if environ is None else environ)

    def build(
        self,
        *,
        settings: DashboardSettings | None = None,
        repository: ObservabilityRepository | None = None,
    ) -> DashboardApplication:
        """@brief 组装一个可被任一入口适配器共享的 DashboardApplication。

        @param settings: 可选配置快照；省略时由 config_service 加载。
        @param repository: 可选仓库实例；省略时调用 repository_factory。
        @return: 已装配的 DashboardApplication。
        """

        resolved_settings = settings or self._config_service.load()
        if not isinstance(resolved_settings, DashboardSettings):
            raise DashboardValidationError("settings 必须是 DashboardSettings。")

        owns_repository = repository is None
        access_policy = DashboardAccessPolicy.from_settings(resolved_settings, self._environ)
        if repository is not None:
            resolved_repository = repository
        elif self._repository_factory is not None:
            resolved_repository = self._repository_factory(resolved_settings)
        else:
            resolved_repository = _default_repository_factory(resolved_settings, self._environ)
        if not isinstance(resolved_repository, ObservabilityRepository):
            raise DashboardValidationError(
                "repository 或 repository_factory 返回值必须实现 ObservabilityRepository。"
            )

        service = DashboardService(resolved_repository, resolved_settings)
        return DashboardApplication(
            settings=resolved_settings,
            repository=resolved_repository,
            service=service,
            owns_repository=owns_repository,
            access_policy=access_policy,
        )


def create_dashboard_application(
    *,
    config_path: str | Path = "config.jsonc",
    repository: ObservabilityRepository | None = None,
    repository_factory: RepositoryFactory | None = None,
    allow_missing_config: bool = True,
    environ: Mapping[str, str] | None = None,
) -> DashboardApplication:
    """@brief 快速创建一个独立 DashboardApplication。

    @param config_path: 共享根 config.jsonc 的路径。
    @param repository: 可选已创建仓库；适合测试或由外层管理连接池的部署。
    @param repository_factory: 可选仓库工厂；适合按配置创建 PostgreSQL 读取适配器。
    @param allow_missing_config: 开发测试中缺少配置文件时是否采用默认安全配置。
    @param environ: 可选进程环境；DSN/operator token 仅从这里读取。
    @return: memory 模式使用内存仓库，postgresql 模式使用受限 PostgreSQL 读模型的应用。
    """

    root = DashboardCompositionRoot(
        config_service=DashboardConfigService(
            config_path,
            allow_missing=allow_missing_config,
        ),
        repository_factory=repository_factory,
        environ=environ,
    )
    return root.build(repository=repository)


def _default_repository_factory(
    settings: DashboardSettings,
    environ: Mapping[str, str],
) -> ObservabilityRepository:
    """@brief 按 database.mode 创建默认只读仓库（default repository factory）。

    @param settings: 已校验的 Dashboard 设置。
    @param environ: secret 环境变量映射。
    @return: memory 模式的内存仓库或 PostgreSQL 模式的稳定视图仓库。
    @raise DashboardConfigurationError: PostgreSQL 只读 DSN 缺失时抛出。

    @note 不会将 application DSN 借给 Dashboard。dashboard_dsn_env 缺失时配置层只使用
    公开约定名，运行时仍必须显式提供 dashboard role 的 DSN。
    """

    if settings.database_mode == "memory":
        return MemoryObservabilityRepository()
    if settings.database_mode != "postgresql":
        raise DashboardConfigurationError("不支持的 Dashboard database.mode。")

    dsn = environ.get(settings.dashboard_dsn_env)
    if not isinstance(dsn, str) or not dsn:
        raise DashboardConfigurationError(
            "Dashboard PostgreSQL DSN 环境变量未设置："
            f"{settings.dashboard_dsn_env}"
        )
    fetcher = SqlAlchemyAsyncRowFetcher(
        dsn,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        connect_timeout_ms=settings.database_connect_timeout_ms,
        query_timeout_ms=settings.query_timeout_ms,
    )
    return PostgresObservabilityRepository(
        fetcher,
        relation=settings.observability_view,
        owns_row_fetcher=True,
    )


__all__ = [
    "DashboardApplication",
    "DashboardCompositionRoot",
    "RepositoryFactory",
    "create_dashboard_application",
]
