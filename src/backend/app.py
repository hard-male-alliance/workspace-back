"""@brief FastAPI 应用工厂 / FastAPI application factory."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from backend.api.diagnostics import diagnostics_router
from backend.api.errors import (
    domain_error_handler,
    http_exception_handler,
    request_validation_error_handler,
)
from backend.api.middleware.context import correlate_http_response
from backend.api.middleware.transport import (
    TransportTelemetryMiddleware,
    log_http_start,
)
from backend.api.routes import router
from backend.composition import build_container
from backend.config import BackendSettings
from backend.domain.common import DomainError, Problem
from backend.infrastructure.identity import IdentityVerificationError, peer_is_trusted_proxy
from backend.infrastructure.observability.context import (
    ObservabilityContext,
    ServerTraceContext,
    bind_observability_context,
)
from workspace_shared.tenancy import ActorScope

logger = logging.getLogger(__name__)
"""@brief HTTP 边界稳定事件 logger / Stable-event logger for the HTTP boundary."""


def config_path() -> Path:
    """@brief 解析配置路径 / Resolve the configuration path.

    @return 当前目录 config.jsonc / Current-directory config.jsonc.
    """
    return Path("config.jsonc")


def _identity_problem_response(request: Request, error: IdentityVerificationError) -> JSONResponse:
    """@brief 将身份验证失败转换为不泄密的 ProblemDetails / Convert an identity failure into non-leaking ProblemDetails.

    @param request 当前 HTTP 请求 / Current HTTP request.
    @param error 仅包含稳定错误码的身份失败 / Identity failure containing only a stable code.
    @return HTTP 401 的 ``application/problem+json`` 响应。

    @note 未验证的 header 永远不回显；对可信代理 HMAC（Hash-based Message
    Authentication Code）模式，此响应也不提示哪一个签名字段错误。
    """
    problem = Problem(error.code, 401, "Request identity could not be verified")
    response = JSONResponse(
        problem.as_dict(getattr(request.state, "request_id", None), request.url.path),
        status_code=401,
        media_type="application/problem+json",
    )
    response.headers["WWW-Authenticate"] = "AIWS-HMAC"
    return response


def _raw_transport_target(request: Request) -> tuple[str | bytes, str | bytes]:
    """@brief 读取 ASGI 未解码请求目标 / Read the undecoded ASGI request target.

    @param request 当前 HTTP 请求 / Current HTTP request.
    @return ``(raw_path, raw_query)``，均保留原始百分号编码。
    @raise IdentityVerificationError ASGI server 未提供安全签名所需原文时抛出。

    @note HMAC（Hash-based Message Authentication Code）不得签已解码的
    ``request.url.path``，否则 ``%2F`` 等编码可造成代理与应用间歧义。
    """
    raw_path = request.scope.get("raw_path")
    raw_query = request.scope.get("query_string", b"")
    if not isinstance(raw_path, (str, bytes)) or not isinstance(raw_query, (str, bytes)):
        raise IdentityVerificationError("identity.request_target_invalid")
    return raw_path, raw_query


def create_app(settings: BackendSettings | None = None) -> FastAPI:
    """@brief 创建无需自动 migration 的 FastAPI 应用 / Create a FastAPI app without automatic migrations.

    @param settings 可注入测试设置；None 时从根配置读取 / Injectable test settings; None loads root configuration.
    @return 已配置的 FastAPI 应用 / Configured FastAPI application.
    """
    resolved_settings = settings or BackendSettings.from_file(config_path())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """@brief 生命周期拥有所有 I/O 资源 / Lifespan owns all I/O resources.

        @param app FastAPI 应用 / FastAPI application.
        @return 生命周期上下文 / Lifespan context.
        """
        runtime_root = resolved_settings.config_path.resolve().parent
        async with build_container(resolved_settings, runtime_root) as container:
            app.state.container = container
            yield

    app = FastAPI(
        title="AI Job Workspace Backend",
        version="0.1.0",
        lifespan=lifespan,
        openapi_url="/openapi.json",
        docs_url="/docs" if resolved_settings.environment != "production" else None,
    )
    app.include_router(router)
    app.include_router(diagnostics_router)
    app.add_exception_handler(DomainError, domain_error_handler)
    app.add_exception_handler(RequestValidationError, request_validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)

    @app.middleware("http")
    async def request_context(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """@brief 注入 request ID 并持久化业务边界指标 / Inject request ID and persist business-boundary metrics.

        @param request HTTP 请求 / HTTP request.
        @param call_next FastAPI 下游调用 / FastAPI downstream callable.
        @return HTTP 响应 / HTTP response.
        """
        response: Response
        trace = cast(ServerTraceContext, request.state.trace_context)
        container = getattr(request.app.state, "container", None)
        if bool(request.state.request_id_invalid):
            request.state.route_fallback = "pre_auth"
            if container is not None:
                log_http_start(request, trace)
            problem = Problem("http.invalid_request_id", 400, "X-Request-Id is invalid")
            response = JSONResponse(
                problem.as_dict(request.state.request_id, request.url.path),
                status_code=400,
                media_type="application/problem+json",
            )
            return correlate_http_response(response, request, trace)
        if container is None:
            response = await call_next(request)
            return correlate_http_response(response, request, trace)
        if request.url.path == "/_internal/healthz":
            response = await call_next(request)
            return correlate_http_response(response, request, trace)
        log_http_start(request, trace)
        if (
            container.settings.security.identity_mode == "trusted_proxy_hmac"
            and not peer_is_trusted_proxy(
                request.client.host if request.client is not None else None,
                container.settings.network.trusted_proxy_cidrs,
            )
        ):
            request.state.route_fallback = "pre_auth"
            response = _identity_problem_response(
                request,
                IdentityVerificationError("identity.proxy_source_not_trusted"),
            )
            return correlate_http_response(response, request, trace)
        try:
            raw_path, raw_query = _raw_transport_target(request)
            scope = container.identity.resolve(
                method=request.method,
                path=raw_path,
                query_string=raw_query,
                headers=request.headers,
            )
        except IdentityVerificationError as error:
            request.state.route_fallback = "pre_auth"
            response = _identity_problem_response(request, error)
            return correlate_http_response(response, request, trace)
        request.state.actor_scope = scope
        with bind_observability_context(
            ObservabilityContext(scope, request.state.request_id, trace)
        ):
            response = await call_next(request)
        return correlate_http_response(response, request, trace)

    @app.get("/_internal/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        """@brief 内部健康探针 / Internal health probe.

        @return 进程健康状态 / Process health status.
        @note MOCK/内部运维路由，未加入产品前端契约。
        """
        return {"status": "ok"}

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, error: Exception) -> JSONResponse:
        """@brief 防止未处理异常泄漏 / Prevent unhandled exception leakage.

        @param request HTTP 请求 / HTTP request.
        @param error 未处理异常 / Unhandled exception.
        @return 通用 ProblemDetails / Generic ProblemDetails.
        """
        actor_scope = getattr(request.state, "actor_scope", None)
        request_id = getattr(request.state, "request_id", None)
        trace = getattr(request.state, "trace_context", None)
        context = ObservabilityContext(
            actor_scope if isinstance(actor_scope, ActorScope) else None,
            request_id if isinstance(request_id, str) else None,
            trace if isinstance(trace, ServerTraceContext) else None,
        )
        with bind_observability_context(context):
            logger.error(
                "backend.http.unexpected_error",
                extra={
                    "event_name": "backend.http.unexpected_error",
                    "telemetry_attributes": {
                        "operation": "request",
                        "outcome": "server_error",
                    },
                },
                exc_info=error,
            )
        problem = Problem("internal.unexpected", 500, "Unexpected server error")
        response = JSONResponse(
            problem.as_dict(getattr(request.state, "request_id", None), request.url.path),
            status_code=500,
            media_type="application/problem+json",
        )
        request_id = getattr(request.state, "request_id", None)
        if isinstance(request_id, str):
            response.headers["X-Request-Id"] = request_id
        trace = getattr(request.state, "trace_context", None)
        if isinstance(trace, ServerTraceContext):
            response.headers["traceparent"] = trace.traceparent
        return response

    if resolved_settings.network.cors_allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(resolved_settings.network.cors_allowed_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
            allow_headers=[
                "Accept",
                "Content-Type",
                "Idempotency-Key",
                "If-Match",
                "If-None-Match",
                "Last-Event-ID",
                "Range",
                "X-Request-Id",
                "traceparent",
            ],
            expose_headers=[
                "Accept-Ranges",
                "Content-Length",
                "Content-Range",
                "ETag",
                "Location",
                "X-Request-Id",
                "traceparent",
            ],
            max_age=600,
        )

    app.add_middleware(TransportTelemetryMiddleware)

    return app
