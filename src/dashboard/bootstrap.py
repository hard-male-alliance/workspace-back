"""@brief Dashboard 唯一组合入口 / Single Dashboard composition entry point."""

from __future__ import annotations

import inspect
import os
from collections.abc import Mapping
from pathlib import Path

from dashboard.application.errors import DashboardConfigurationError, DashboardQueryError
from dashboard.application.ports import ObservabilityReadStore
from dashboard.application.service import Clock, DashboardQueryPolicy, DashboardQueryService
from dashboard.domain.model import OperatorPrincipal, WorkspaceScope
from dashboard.infrastructure.auth import OperatorAuthenticator
from dashboard.infrastructure.config import DashboardSettings
from dashboard.infrastructure.demo import DemoObservabilityReadStore
from dashboard.infrastructure.postgres import PostgresObservabilityReadStore


class DashboardRuntime:
    """@brief 入口共享的最小运行时资源 / Minimal runtime resources shared by entry adapters.

    @param settings 不可变配置 / Immutable settings.
    @param queries 查询服务 / Query service.
    @param authenticator 运维认证器 / Operator authenticator.
    @param store 只读存储 / Read-only store.
    @param owns_store 是否负责关闭存储 / Whether the runtime owns the store.
    """

    def __init__(
        self,
        *,
        settings: DashboardSettings,
        queries: DashboardQueryService,
        authenticator: OperatorAuthenticator,
        store: ObservabilityReadStore,
        owns_store: bool,
    ) -> None:
        """@brief 创建已组合运行时 / Create a composed runtime.

        @param settings Dashboard 配置 / Dashboard settings.
        @param queries 查询服务 / Query service.
        @param authenticator 认证器 / Authenticator.
        @param store 读存储 / Read store.
        @param owns_store 存储所有权 / Store ownership.
        @return 新运行时 / New runtime.
        """

        self.settings = settings
        self.queries = queries
        self.authenticator = authenticator
        self._store = store
        self._owns_store = owns_store
        self._closed = False

    def local_principal(self) -> OperatorPrincipal:
        """@brief 返回受控本地运维主体 / Return the controlled local operator principal.

        @return 已认证运维主体 / Authenticated operator principal.
        """

        self._ensure_available()
        return self.authenticator.authenticate_local()

    def authenticate_http(self, presented_token: str | None) -> OperatorPrincipal:
        """@brief 在运行时可用性门禁后认证 HTTP operator / Authenticate an HTTP operator after the runtime-availability gate.

        @param presented_token HTTP 请求中的可选 token / Optional token presented by the HTTP request.
        @return 已认证运维主体 / Authenticated operator principal.
        """

        self._ensure_available()
        return self.authenticator.authenticate_http(presented_token)

    def workspace_scope(self, workspace_id: str | None = None) -> WorkspaceScope:
        """@brief 解析显式或默认工作区 / Resolve an explicit or default workspace.

        @param workspace_id 可选显式工作区 / Optional explicit workspace.
        @return 单工作区范围 / Single-workspace scope.
        """

        self._ensure_available()
        return WorkspaceScope(workspace_id or self.settings.default_workspace_id)

    async def aclose(self) -> None:
        """@brief 幂等关闭自有资源 / Idempotently close owned resources.

        @return 无返回值 / No return value.
        """

        if self._closed:
            return
        self._closed = True
        if not self._owns_store:
            return
        close = getattr(self._store, "aclose", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    def _ensure_available(self) -> None:
        """@brief 拒绝关闭或禁用状态 / Reject closed or disabled state.

        @return 无返回值 / No return value.
        """

        if self._closed:
            raise DashboardQueryError("Dashboard runtime 已关闭。")
        if not self.settings.enabled:
            raise DashboardQueryError("Dashboard 已禁用。")


def build_runtime(
    *,
    config_path: str | Path = "config.jsonc",
    settings: DashboardSettings | None = None,
    store: ObservabilityReadStore | None = None,
    environ: Mapping[str, str] | None = None,
    clock: Clock | None = None,
) -> DashboardRuntime:
    """@brief 组合 Dashboard 配置、认证、读存储和查询服务 / Compose settings, authentication, read store, and query service.

    @param config_path 根配置路径 / Root configuration path.
    @param settings 可选注入配置 / Optional injected settings.
    @param store 可选注入读存储 / Optional injected read store.
    @param environ 可选环境变量映射 / Optional environment mapping.
    @param clock 可选测试时钟 / Optional test clock.
    @return 可由 CLI、API 或 GUI 使用的运行时 / Runtime usable by CLI, API, or GUI.
    """

    resolved_settings = settings or DashboardSettings.from_file(config_path)
    environment = dict(os.environ if environ is None else environ)
    authenticator = OperatorAuthenticator(resolved_settings.access, environment)
    owns_store = store is None
    resolved_store = store or _build_store(resolved_settings)
    query_settings = resolved_settings.query
    policy = DashboardQueryPolicy(
        default_window=query_settings.default_window,
        max_window=query_settings.max_window,
        freshness_target=query_settings.freshness_target,
        target_points=query_settings.target_points,
        max_event_limit=query_settings.max_event_limit,
        objective=resolved_settings.objective,
    )
    queries = DashboardQueryService(resolved_store, policy, clock=clock)
    return DashboardRuntime(
        settings=resolved_settings,
        queries=queries,
        authenticator=authenticator,
        store=resolved_store,
        owns_store=owns_store,
    )


def _build_store(settings: DashboardSettings) -> ObservabilityReadStore:
    """@brief 按配置创建明确读存储 / Build the explicitly configured read store.

    @param settings Dashboard 配置 / Dashboard settings.
    @return PostgreSQL 产品存储或空 demo 存储 / PostgreSQL product store or empty demo store.
    """

    database = settings.database
    if database.mode == "memory":
        return DemoObservabilityReadStore()
    if database.dsn is None:
        raise DashboardConfigurationError("Dashboard PostgreSQL DSN 未配置。")
    return PostgresObservabilityReadStore.from_dsn(
        database.dsn,
        pool_size=database.pool_size,
        max_overflow=database.max_overflow,
        connect_timeout_ms=database.connect_timeout_ms,
        statement_timeout_ms=settings.query.statement_timeout_ms,
    )


__all__ = ["DashboardRuntime", "build_runtime"]
