# ruff: noqa: B008
"""FastAPI 边界适配器（adapter），只调用 Dashboard 应用层。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from importlib import import_module
from typing import Any

from .composition import DashboardApplication, create_dashboard_application
from .errors import (
    DashboardAuthorizationError,
    DashboardConfigurationError,
    DashboardDataError,
    DashboardDependencyError,
    DashboardError,
    DashboardUnavailableError,
    DashboardValidationError,
)


def create_api(application: DashboardApplication | None = None) -> Any:
    """@brief 创建使用同一 DashboardService 的 FastAPI 应用。

    @param application: 可选已组合应用；省略时按根配置创建应用并在 lifespan 结束时关闭。
    @return: FastAPI 应用对象；FastAPI 未安装时抛出 DashboardDependencyError。

    @note 返回的是私有运维 API，不扩张前后端公开业务契约。路径来自
    DashboardSettings.api_prefix，部署时应由 Nginx 反向代理暴露。
    """

    try:
        fastapi = import_module("fastapi")
    except ModuleNotFoundError as error:
        raise DashboardDependencyError(
            "FastAPI API 需要可选依赖 fastapi；headless CLI 不需要该依赖。"
        ) from error
    FastAPI = fastapi.FastAPI
    HTTPException = fastapi.HTTPException
    Header = fastapi.Header
    Query = fastapi.Query

    owns_application = application is None
    resolved_application = create_dashboard_application() if application is None else application
    prefix = resolved_application.settings.api_prefix

    @asynccontextmanager
    async def _lifespan(_: Any) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if owns_application:
                await resolved_application.aclose()

    app = FastAPI(
        title="AI Job Workspace Dashboard",
        version="0.1.0",
        lifespan=_lifespan,
    )
    app.state.dashboard_application = resolved_application

    async def _healthz() -> dict[str, object]:
        """返回不读取用户数据的进程健康信息。"""

        return {
            "status": "ok" if resolved_application.settings.enabled else "disabled",
            "component": "dashboard",
            "access_mode": resolved_application.settings.operator_access_mode,
        }

    def _http_error(error: DashboardError) -> Any:
        """@brief 将受控 Dashboard 异常映射为不泄密的 HTTP 响应。

        @param error: Dashboard 应用边界抛出的已知异常。
        @return: FastAPI HTTPException。

        @note 认证失败不回显 token；数据库错误不回显 DSN、SQL 或底层异常文本。
        """

        if isinstance(error, DashboardAuthorizationError):
            return HTTPException(status_code=401, detail="Dashboard operator 身份未获授权。")
        if isinstance(error, (DashboardUnavailableError, DashboardConfigurationError)):
            return HTTPException(status_code=503, detail="Dashboard 当前不可用。")
        if isinstance(error, DashboardDataError):
            return HTTPException(status_code=503, detail="Dashboard 读模型暂不可用。")
        if isinstance(error, DashboardValidationError):
            return HTTPException(status_code=422, detail=str(error))
        return HTTPException(status_code=500, detail="Dashboard 内部错误。")

    async def _overview(
        workspace_id: str = Query(min_length=1),
        actor_id: str | None = Query(default=None, min_length=1),
        operator_token: str | None = Header(
            default=None,
            alias=resolved_application.settings.operator_token_header,
        ),
        start_at: datetime | None = Query(default=None),
        end_at: datetime | None = Query(default=None),
        service: str | None = Query(default=None, min_length=1),
        max_samples: int | None = Query(default=None, ge=1),
    ) -> dict[str, object]:
        """通过应用服务返回一个工作区的完整 SRE 聚合概览。"""

        try:
            scope = resolved_application.scope_for_http_operator(
                workspace_id,
                presented_token=operator_token,
                requested_actor_id=actor_id,
            )
            overview = await resolved_application.service.overview(
                scope,
                start_at=start_at,
                end_at=end_at,
                service=service,
                max_samples=max_samples,
            )
        except DashboardError as error:
            raise _http_error(error) from error
        return overview.to_dict()

    async def _services(
        workspace_id: str = Query(min_length=1),
        actor_id: str | None = Query(default=None, min_length=1),
        operator_token: str | None = Header(
            default=None,
            alias=resolved_application.settings.operator_token_header,
        ),
        start_at: datetime | None = Query(default=None),
        end_at: datetime | None = Query(default=None),
        max_samples: int | None = Query(default=None, ge=1),
    ) -> dict[str, object]:
        """通过应用服务返回按服务排序的健康摘要列表。"""

        try:
            scope = resolved_application.scope_for_http_operator(
                workspace_id,
                presented_token=operator_token,
                requested_actor_id=actor_id,
            )
            services = await resolved_application.service.list_services(
                scope,
                start_at=start_at,
                end_at=end_at,
                max_samples=max_samples,
            )
        except DashboardError as error:
            raise _http_error(error) from error
        return {
            "scope": scope.to_dict(),
            "items": [summary.to_dict() for summary in services],
        }

    app.add_api_route(f"{prefix}/healthz", _healthz, methods=["GET"], tags=["dashboard"])
    app.add_api_route(f"{prefix}/overview", _overview, methods=["GET"], tags=["dashboard"])
    app.add_api_route(f"{prefix}/services", _services, methods=["GET"], tags=["dashboard"])
    return app


def create_fastapi_app(application: DashboardApplication | None = None) -> Any:
    """@brief create_api 的语义化别名，便于 ASGI 服务器发现工厂。

    @param application: 可选已组合 DashboardApplication。
    @return: 使用同一 application layer 的 FastAPI 应用对象。
    """

    return create_api(application)


__all__ = ["create_api", "create_fastapi_app"]
