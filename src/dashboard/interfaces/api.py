"""@brief Dashboard 私有只读 HTTP API / Private read-only Dashboard HTTP API."""

from __future__ import annotations

import argparse
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request

from dashboard.application.errors import (
    DashboardAuthorizationError,
    DashboardConfigurationError,
    DashboardError,
    DashboardQueryError,
    DashboardReadStoreUnavailable,
)
from dashboard.bootstrap import DashboardRuntime, build_runtime
from dashboard.domain.model import (
    DashboardDomainError,
    OperatorPrincipal,
    SignalKind,
    WorkspaceScope,
)
from dashboard.infrastructure.config import DashboardSettings

from .presenters import (
    JsonValue,
    event_report_payload,
    overview_payload,
    system_health_payload,
    trend_report_payload,
)
from .time import resolve_window


def create_fastapi_app(
    runtime: DashboardRuntime | None = None,
    *,
    config_path: str | Path = "config.jsonc",
) -> FastAPI:
    """@brief 创建共享应用层的 FastAPI 适配器 / Create a FastAPI adapter over the shared application layer.

    @param runtime 可选外部运行时 / Optional externally owned runtime.
    @param config_path 未注入运行时时的根配置 / Root config used when no runtime is injected.
    @return 私有只读 FastAPI 应用 / Private read-only FastAPI application.
    """

    owns_runtime = runtime is None
    resolved_runtime = runtime or build_runtime(config_path=config_path)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        """@brief 管理组合根拥有的资源 / Manage resources owned by the composition root.

        @param _ FastAPI 应用 / FastAPI application.
        @return 异步 lifespan 迭代器 / Async lifespan iterator.
        """

        try:
            yield
        finally:
            if owns_runtime:
                await resolved_runtime.aclose()

    app = FastAPI(
        title="AI Job Workspace Observability Dashboard",
        version="2.0.0",
        lifespan=lifespan,
    )
    app.state.dashboard_runtime = resolved_runtime
    prefix = resolved_runtime.settings.api.prefix

    @app.get(f"{prefix}/healthz", tags=["dashboard"])
    async def healthz() -> dict[str, object]:
        """@brief 返回不读取租户数据的进程健康 / Return process health without reading tenant data.

        @return 健康对象 / Health object.
        """

        return {
            "status": "ok" if resolved_runtime.settings.enabled else "disabled",
            "component": "dashboard",
            "access_mode": resolved_runtime.settings.access.mode,
            "data_source": _data_source(resolved_runtime),
        }

    @app.get(f"{prefix}/overview", tags=["dashboard"])
    async def overview(
        request: Request,
        workspace_id: str | None = None,
        service: str | None = None,
        since: str | None = None,
        start_at: str | None = None,
        end_at: str | None = None,
    ) -> dict[str, JsonValue]:
        """@brief 返回 SLO、错误预算、新鲜度与服务摘要 / Return SLO, error budget, freshness, and services.

        @param request FastAPI 请求 / FastAPI request.
        @param workspace_id 可选工作区；默认来自根配置 / Optional workspace; defaults from root config.
        @param service 可选服务过滤 / Optional service filter.
        @param since 相对窗口 / Relative window.
        @param start_at 精确起点 / Exact start.
        @param end_at 精确终点 / Exact end.
        @return Overview JSON / Overview JSON.
        """

        try:
            principal, scope = _authorize(resolved_runtime, request, workspace_id)
            window = resolve_window(since=since, start_at=start_at, end_at=end_at)
            report = await resolved_runtime.queries.overview(
                principal,
                scope,
                window=window,
                service=service,
            )
            return _with_data_source(overview_payload(report), resolved_runtime)
        except (DashboardError, DashboardDomainError, ValueError) as error:
            raise _http_error(error) from error

    @app.get(f"{prefix}/trends", tags=["dashboard"])
    async def trends(
        request: Request,
        signal: str = "traffic",
        workspace_id: str | None = None,
        service: str | None = None,
        since: str | None = None,
        start_at: str | None = None,
        end_at: str | None = None,
        bucket_seconds: int | None = None,
    ) -> dict[str, JsonValue]:
        """@brief 返回数据库侧分桶的黄金信号趋势 / Return database-bucketed golden-signal trends.

        @param request FastAPI 请求 / FastAPI request.
        @param signal traffic、latency、errors 或 saturation / Traffic, latency, errors, or saturation.
        @param workspace_id 可选工作区 / Optional workspace.
        @param service 可选服务过滤 / Optional service filter.
        @param since 相对窗口 / Relative window.
        @param start_at 精确起点 / Exact start.
        @param end_at 精确终点 / Exact end.
        @param bucket_seconds 可选显式桶宽 / Optional explicit bucket width.
        @return 趋势 JSON / Trend JSON.
        """

        try:
            principal, scope = _authorize(resolved_runtime, request, workspace_id)
            window = resolve_window(since=since, start_at=start_at, end_at=end_at)
            report = await resolved_runtime.queries.trends(
                principal,
                scope,
                SignalKind(signal),
                window=window,
                service=service,
                bucket_seconds=bucket_seconds,
            )
            return _with_data_source(trend_report_payload(report), resolved_runtime)
        except (DashboardError, DashboardDomainError, ValueError) as error:
            raise _http_error(error) from error

    @app.get(f"{prefix}/events", tags=["dashboard"])
    async def events(
        request: Request,
        workspace_id: str | None = None,
        service: str | None = None,
        since: str | None = None,
        start_at: str | None = None,
        end_at: str | None = None,
        limit: int = 100,
    ) -> dict[str, JsonValue]:
        """@brief 返回有界 log/span 与前端 metric 诊断上下文 / Return bounded log/span and frontend-metric context.

        @param request FastAPI 请求 / FastAPI request.
        @param workspace_id 可选工作区 / Optional workspace.
        @param service 可选服务过滤 / Optional service filter.
        @param since 相对窗口 / Relative window.
        @param start_at 精确起点 / Exact start.
        @param end_at 精确终点 / Exact end.
        @param limit 硬事件上限 / Hard event limit.
        @return 事件 JSON / Event JSON.
        """

        try:
            principal, scope = _authorize(resolved_runtime, request, workspace_id)
            window = resolve_window(since=since, start_at=start_at, end_at=end_at)
            report = await resolved_runtime.queries.recent_events(
                principal,
                scope,
                window=window,
                service=service,
                limit=limit,
            )
            return _with_data_source(event_report_payload(report), resolved_runtime)
        except (DashboardError, DashboardDomainError, ValueError) as error:
            raise _http_error(error) from error

    @app.get(f"{prefix}/system-health", tags=["dashboard"])
    async def system_health(
        request: Request,
        since: str | None = None,
        start_at: str | None = None,
        end_at: str | None = None,
    ) -> dict[str, JsonValue]:
        """@brief 返回无 workspace 归因的 worker self-health / Return worker self-health without workspace attribution.

        @param request FastAPI 请求 / FastAPI request.
        @param since 相对窗口 / Relative window.
        @param start_at 精确起点 / Exact start.
        @param end_at 精确终点 / Exact end.
        @return operator-only 系统健康 JSON / Operator-only system-health JSON.
        """

        try:
            principal = _authenticate(resolved_runtime, request)
            window = resolve_window(since=since, start_at=start_at, end_at=end_at)
            report = await resolved_runtime.queries.system_health(
                principal,
                window=window,
            )
            return _with_data_source(system_health_payload(report), resolved_runtime)
        except (DashboardError, DashboardDomainError, ValueError) as error:
            raise _http_error(error) from error

    return app


def create_app(
    runtime: DashboardRuntime | None = None,
    *,
    config_path: str | Path = "config.jsonc",
) -> FastAPI:
    """@brief 提供简洁 ASGI 工厂名 / Provide a concise ASGI factory name.

    @param runtime 可选运行时 / Optional runtime.
    @param config_path 根配置路径 / Root config path.
    @return FastAPI 应用 / FastAPI application.
    """

    return create_fastapi_app(runtime, config_path=config_path)


def build_parser() -> argparse.ArgumentParser:
    """@brief 构造 Dashboard API console 参数 / Build Dashboard API console arguments.

    @return 只接受根配置路径的解析器 / Parser accepting only the root config path.
    """

    parser = argparse.ArgumentParser(
        prog="dashboard-api",
        description="启动私有、只读的 observability Dashboard API。",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.jsonc"),
        help="根 JSONC 配置（默认：config.jsonc）。",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 按 Dashboard 配置启动私有 Uvicorn / Start private Uvicorn from Dashboard settings.

    @param argv 可选命令行参数 / Optional command-line arguments.
    @return 正常停止时为 0 / Zero after a normal stop.
    """

    arguments = build_parser().parse_args(argv)
    settings = DashboardSettings.from_file(arguments.config)
    uvicorn.run(
        create_fastapi_app(config_path=arguments.config),
        host=settings.api.host,
        port=settings.api.port,
        proxy_headers=False,
        log_config=None,
    )
    return 0


def _authorize(
    runtime: DashboardRuntime,
    request: Request,
    workspace_id: str | None,
) -> tuple[OperatorPrincipal, WorkspaceScope]:
    """@brief 将 HTTP token 与 workspace 分别解析 / Resolve the HTTP token and workspace separately.

    @param runtime Dashboard 运行时 / Dashboard runtime.
    @param request HTTP 请求 / HTTP request.
    @param workspace_id 可选工作区 / Optional workspace.
    @return OperatorPrincipal 与 WorkspaceScope / ``OperatorPrincipal`` and ``WorkspaceScope``.
    """

    principal = _authenticate(runtime, request)
    return principal, runtime.workspace_scope(workspace_id)


def _authenticate(runtime: DashboardRuntime, request: Request) -> OperatorPrincipal:
    """@brief 认证 operator 而不制造 workspace scope / Authenticate an operator without manufacturing a workspace scope.

    @param runtime Dashboard 运行时 / Dashboard runtime.
    @param request HTTP 请求 / HTTP request.
    @return 已认证 operator / Authenticated operator.
    """

    return runtime.authenticate_http(
        request.headers.get(runtime.authenticator.token_header)
    )


def _with_data_source(
    payload: dict[str, JsonValue],
    runtime: DashboardRuntime,
) -> dict[str, JsonValue]:
    """@brief 为 API 响应附加明确数据源标签 / Attach an explicit data-source label to an API response.

    @param payload 呈现对象 / Presentation object.
    @param runtime Dashboard 运行时 / Dashboard runtime.
    @return 带标签的新对象 / Newly labelled object.
    """

    return {"data_source": _data_source(runtime), **payload}


def _data_source(runtime: DashboardRuntime) -> str:
    """@brief 返回产品可见的数据源模式 / Return the product-visible data-source mode.

    @param runtime Dashboard 运行时 / Dashboard runtime.
    @return demo 或 PostgreSQL 标签 / Demo or PostgreSQL label.
    """

    return (
        "demo_empty_adapter"
        if runtime.settings.database.mode == "memory"
        else "postgresql_observability"
    )


def _http_error(error: Exception) -> HTTPException:
    """@brief 将已知边界错误映射为不泄密的 HTTP 错误 / Map known boundary errors to non-secret HTTP errors.

    @param error 已知异常 / Known exception.
    @return FastAPI HTTPException / FastAPI ``HTTPException``.
    """

    if isinstance(error, DashboardAuthorizationError):
        return HTTPException(status_code=401, detail="Dashboard operator 身份未获授权。")
    if isinstance(error, DashboardReadStoreUnavailable):
        return HTTPException(status_code=503, detail="Dashboard 读模型暂不可用。")
    if isinstance(error, DashboardConfigurationError):
        return HTTPException(status_code=503, detail="Dashboard 当前不可用。")
    if isinstance(error, (DashboardQueryError, DashboardDomainError, ValueError)):
        return HTTPException(status_code=422, detail=str(error))
    return HTTPException(status_code=500, detail="Dashboard 内部错误。")


if __name__ == "__main__":  # pragma: no cover - console module path.
    raise SystemExit(main())


__all__ = ["build_parser", "create_app", "create_fastapi_app", "main"]
