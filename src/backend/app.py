"""@brief FastAPI 应用工厂 / FastAPI application factory."""

from __future__ import annotations

import os
import re
from asyncio import CancelledError
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from time import perf_counter

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from backend.api.errors import (
    domain_error_handler,
    http_exception_handler,
    request_validation_error_handler,
)
from backend.api.routes import router
from backend.composition import BackendContainer, build_container
from backend.config import BackendSettings
from backend.domain.common import DomainError, Problem
from backend.infrastructure.identity import IdentityVerificationError, peer_is_trusted_proxy
from backend.infrastructure.telemetry import bind_telemetry_scope
from workspace_shared.ids import new_opaque_id
from workspace_shared.tenancy import ActorScope

_REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
"""@brief 可安全持久化的 request ID 格式 / Request-ID format safe for persistence."""


def project_root() -> Path:
    """@brief 定位项目根目录 / Locate the project root.

    @return 包含 pyproject.toml 的项目目录 / Project directory containing pyproject.toml.
    """
    return Path(__file__).resolve().parents[2]


def config_path() -> Path:
    """@brief 解析配置路径 / Resolve the configuration path.

    @return AIWS_CONFIG 或根 config.jsonc / AIWS_CONFIG or root config.jsonc.
    """
    return Path(os.environ.get("AIWS_CONFIG", project_root() / "config.jsonc"))


def _record_http_telemetry(
    container: BackendContainer,
    scope: ActorScope,
    request: Request,
    status_code: int,
    latency_ms: float,
) -> None:
    """@brief 写入 Google SRE 四个黄金信号 / Write Google SRE's four golden signals.

    @param container 当前 worker 容器 / Current worker container.
    @param scope 已认证或 mock 的租户范围 / Authenticated or mock tenant scope.
    @param request HTTP 请求 / HTTP request.
    @param status_code 最终 HTTP 状态码 / Final HTTP status code.
    @param latency_ms 请求端到端耗时毫秒 / End-to-end request duration in milliseconds.
    @return 无返回值 / No return value.
    @note 只记录低基数 method/status/outcome，不记录 URL、prompt 或异常文本。
    """
    outcome = "failure" if status_code >= 500 else "success"
    attributes: dict[str, str | int | float | bool] = {
        "operation": request.method.lower(),
        "outcome": outcome,
        "status_code": status_code,
    }
    telemetry = container.telemetry
    telemetry.record(
        "metric",
        "requests",
        1,
        scope,
        request.state.request_id,
        attributes,
        service="backend.api",
    )
    telemetry.record(
        "metric",
        "latency_ms",
        max(0.0, latency_ms),
        scope,
        request.state.request_id,
        attributes,
        service="backend.api",
    )
    telemetry.record(
        "metric",
        "saturation",
        container.supervisor.saturation,
        scope,
        request.state.request_id,
        {"operation": request.method.lower(), "outcome": outcome},
        service="backend.api",
    )
    if status_code >= 500:
        telemetry.record(
            "metric",
            "errors",
            1,
            scope,
            request.state.request_id,
            attributes,
            service="backend.api",
        )


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


def _request_id_from_headers(request: Request) -> str:
    """@brief 验证入口关联 ID 或生成新 ID / Validate an ingress correlation ID or generate a new one.

    @param request 当前 HTTP 请求 / Current HTTP request.
    @return 可写入 telemetry 与 PostgreSQL ``VARCHAR(128)`` 的 request ID。
    @raise ValueError 客户端提供了重复、过长或含控制字符的值时抛出。

    @note request ID 不是身份凭据，但其会进入持久化 telemetry。边界校验避免一个
    任意长 header 将普通业务请求放大成数据库列溢出或日志污染。
    """
    values = request.headers.getlist("X-Request-Id")
    if not values:
        return new_opaque_id("req")
    if len(values) != 1 or not _REQUEST_ID_PATTERN.fullmatch(values[0]):
        raise ValueError("invalid request ID")
    return values[0]


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
        async with build_container(resolved_settings, project_root()) as container:
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
        try:
            request.state.request_id = _request_id_from_headers(request)
        except ValueError:
            request.state.request_id = new_opaque_id("req")
            problem = Problem("http.invalid_request_id", 400, "X-Request-Id is invalid")
            response = JSONResponse(
                problem.as_dict(request.state.request_id, request.url.path),
                status_code=400,
                media_type="application/problem+json",
            )
            response.headers["X-Request-Id"] = request.state.request_id
            return response
        container = getattr(request.app.state, "container", None)
        if container is None:
            response = await call_next(request)
            response.headers["X-Request-Id"] = request.state.request_id
            return response
        if request.url.path == "/_internal/healthz":
            response = await call_next(request)
            response.headers["X-Request-Id"] = request.state.request_id
            return response
        if (
            container.settings.security.identity_mode == "trusted_proxy_hmac"
            and not peer_is_trusted_proxy(
                request.client.host if request.client is not None else None,
                container.settings.network.trusted_proxy_cidrs,
            )
        ):
            response = _identity_problem_response(
                request,
                IdentityVerificationError("identity.proxy_source_not_trusted"),
            )
            response.headers["X-Request-Id"] = request.state.request_id
            return response
        try:
            raw_path, raw_query = _raw_transport_target(request)
            scope = container.identity.resolve(
                method=request.method,
                path=raw_path,
                query_string=raw_query,
                headers=request.headers,
            )
        except IdentityVerificationError as error:
            response = _identity_problem_response(request, error)
            response.headers["X-Request-Id"] = request.state.request_id
            return response
        request.state.actor_scope = scope
        started_at = perf_counter()
        with bind_telemetry_scope(scope):
            try:
                response = await call_next(request)
            except CancelledError:
                raise
            except BaseException:
                _record_http_telemetry(
                    container,
                    scope,
                    request,
                    500,
                    (perf_counter() - started_at) * 1000,
                )
                raise
        response.headers["X-Request-Id"] = request.state.request_id
        _record_http_telemetry(
            container,
            scope,
            request,
            response.status_code,
            (perf_counter() - started_at) * 1000,
        )
        return response

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
        problem = Problem("internal.unexpected", 500, "Unexpected server error")
        return JSONResponse(
            problem.as_dict(getattr(request.state, "request_id", None), request.url.path),
            status_code=500,
            media_type="application/problem+json",
        )

    return app
