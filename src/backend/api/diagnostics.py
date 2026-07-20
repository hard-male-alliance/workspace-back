"""@brief 浏览器诊断 API 的严格输入边界 / Strict input boundary for the browser diagnostics API."""

from __future__ import annotations

from time import perf_counter
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from backend.api.routes import router as product_router
from backend.application.diagnostics import (
    ClientDiagnostic,
    ClientErrorDiagnostic,
    ClientNetworkDiagnostic,
    ClientPerformanceDiagnostic,
)
from backend.composition import BackendContainer
from backend.domain.common import DomainError, Problem
from workspace_shared.tenancy import ActorScope

diagnostics_router = APIRouter(prefix="/api/v1")
"""@brief 浏览器诊断路由 / Browser diagnostics router."""

_CLIENT_EVENT_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
"""@brief 客户端幂等事件 ID 格式 / Client idempotency event-ID format."""

_PRODUCT_API_PREFIX = "/api/v1"
"""@brief 公开产品路由的统一前缀 / Common prefix of published product routes."""

_ROUTE_TEMPLATES = frozenset(
    {"/"}
    | {
        path.removeprefix(_PRODUCT_API_PREFIX)
        for route in product_router.routes
        if isinstance(path := getattr(route, "path", None), str)
        and path.startswith(f"{_PRODUCT_API_PREFIX}/")
    }
)
"""@brief 可由浏览器诊断引用的服务端发布路由模板 / Server-published route templates accepted from browser diagnostics.

@note 从产品 router 的权威模板派生，而非用路径正则猜测；这样实际资源 ID、任意 URL
或尚未发布的高基数路径都不能进入 telemetry。
"""

_RELEASE_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$"
"""@brief 前端 release 标签格式 / Frontend release-label format."""

_ERROR_CODE_PATTERN = r"^[a-z][a-z0-9_.-]{0,127}$"
"""@brief 稳定错误码格式 / Stable error-code format."""


class _DiagnosticBase(BaseModel):
    """@brief 三类诊断共享的严格字段 / Strict fields shared by all diagnostic kinds."""

    model_config = ConfigDict(extra="forbid", strict=True)

    client_event_id: str = Field(pattern=_CLIENT_EVENT_PATTERN)
    occurred_at: AwareDatetime
    route: str = Field(max_length=256)
    release: str = Field(pattern=_RELEASE_PATTERN, max_length=64)

    @field_validator("route")
    @classmethod
    def published_route_template(cls, value: str) -> str:
        """@brief 只接受服务端已发布的规范路由模板 / Accept only canonical server-published route templates.

        @param value 客户端声明的模板 / Client-declared template.
        @return 经权威路由集合确认的模板 / Template confirmed by the authoritative route set.
        @raise ValueError 实际资源路径或未知模板被提交时抛出 / Raised for a concrete resource path or unknown template.
        """
        if value not in _ROUTE_TEMPLATES:
            raise ValueError("route must be a published product route template")
        return value


class ErrorDiagnosticRequest(_DiagnosticBase):
    """@brief 无自由文本的浏览器错误事件 / Browser error event without free-form text."""

    event_type: Literal["error"]
    error_code: str = Field(pattern=_ERROR_CODE_PATTERN, max_length=128)
    stack_fingerprint: str | None = Field(
        default=None, min_length=16, max_length=64, pattern=r"^[0-9a-f]+$"
    )


class PerformanceDiagnosticRequest(_DiagnosticBase):
    """@brief 浏览器 Web Vital/性能观测 / Browser Web Vital or performance observation."""

    event_type: Literal["performance"]
    metric_name: Literal[
        "cumulative_layout_shift",
        "first_contentful_paint",
        "interaction_to_next_paint",
        "largest_contentful_paint",
        "time_to_first_byte",
    ]
    value: float = Field(ge=0, allow_inf_nan=False)
    unit: Literal["ms", "1"]

    @model_validator(mode="after")
    def metric_unit_contract(self) -> PerformanceDiagnosticRequest:
        """@brief 强制 Web Vital 的规范单位 / Enforce canonical units for Web Vitals.

        @return 当前已验证模型 / This validated model.
        @raise ValueError CLS 与 duration 指标单位不匹配时抛出 / Raised for an invalid metric/unit pair.
        """
        expected = "1" if self.metric_name == "cumulative_layout_shift" else "ms"
        if self.unit != expected:
            raise ValueError(f"{self.metric_name} requires unit {expected}")
        return self


class NetworkDiagnosticRequest(_DiagnosticBase):
    """@brief 不含 URL 的浏览器网络性能事件 / Browser network performance event without a URL."""

    event_type: Literal["network"]
    operation: Literal["asset", "fetch", "navigation"]
    duration_ms: float = Field(ge=0, allow_inf_nan=False)
    status_code: int = Field(ge=0, le=599)

    @model_validator(mode="after")
    def network_status_contract(self) -> NetworkDiagnosticRequest:
        """@brief 仅允许网络失败 0 或真实 HTTP 状态 / Allow network failure 0 or a real HTTP status.

        @return 当前已验证模型 / This validated model.
        @raise ValueError 状态码为 1..99 时抛出 / Raised for status codes 1 through 99.
        """
        if 0 < self.status_code < 100:
            raise ValueError("network status_code must be 0 or between 100 and 599")
        return self


DiagnosticRequest = Annotated[
    ErrorDiagnosticRequest | PerformanceDiagnosticRequest | NetworkDiagnosticRequest,
    Field(discriminator="event_type"),
]
"""@brief Pydantic 诊断判别联合 / Pydantic diagnostic discriminated union."""


class DiagnosticBatchRequest(BaseModel):
    """@brief 最多 50 条的严格诊断批次 / Strict diagnostic batch of at most 50 events."""

    model_config = ConfigDict(extra="forbid", strict=True)

    events: list[DiagnosticRequest] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def unique_client_event_ids(self) -> DiagnosticBatchRequest:
        """@brief 拒绝批内重复幂等 ID / Reject duplicate idempotency IDs within one batch.

        @return 当前已验证模型 / This validated model.
        @raise ValueError 存在重复 ID 时抛出 / Raised for duplicate IDs.
        """
        identifiers = [event.client_event_id for event in self.events]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("client_event_id values must be unique within a batch")
        return self


@diagnostics_router.post("/diagnostics", status_code=202)
async def ingest_diagnostics(request: Request) -> JSONResponse:
    """@brief 有界接收前端诊断并返回准入反馈 / Boundedly ingest frontend diagnostics and return admission feedback.

    @param request 已经身份中间件认证的请求 / Request authenticated by identity middleware.
    @return ``202`` accepted/dropped，或 ``429`` Retry-After / ``202`` feedback or ``429`` Retry-After.
    @raise DomainError 内容类型、大小、schema 或时间窗口非法时抛出。
    """
    container = cast(BackendContainer, request.app.state.container)
    settings = container.settings.observability.diagnostics
    ingest_started_at = perf_counter()
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().lower()
    if content_type != "application/json":
        raise DomainError(
            Problem("diagnostics.unsupported_media_type", 415, "Content-Type must be application/json")
        )
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError as error:
            raise DomainError(
                Problem("diagnostics.invalid_content_length", 400, "Content-Length is invalid")
            ) from error
        if declared_length < 0 or declared_length > settings.max_body_bytes:
            raise DomainError(
                Problem("diagnostics.payload_too_large", 413, "Diagnostic payload is too large")
            )
    payload = await _read_bounded_body(request, settings.max_body_bytes)
    try:
        batch = DiagnosticBatchRequest.model_validate_json(payload)
    except ValidationError as error:
        raise DomainError(
            Problem("diagnostics.invalid_payload", 422, "Diagnostic payload is invalid")
        ) from error
    if len(batch.events) > settings.max_batch_size:
        raise DomainError(
            Problem("diagnostics.batch_too_large", 413, "Diagnostic batch is too large")
        )
    scope = cast(ActorScope, request.state.actor_scope)
    retry_after = await container.diagnostics.retry_after(scope, len(batch.events))
    if retry_after is not None:
        container.diagnostics.observe_ingestion(
            scope,
            request.state.request_id,
            payload_bytes=len(payload),
            accepted=0,
            dropped=0,
            rate_limited=len(batch.events),
            duration_seconds=perf_counter() - ingest_started_at,
        )
        problem = Problem("diagnostics.rate_limited", 429, "Diagnostic ingestion rate exceeded")
        response = JSONResponse(
            problem.as_dict(request.state.request_id, request.url.path),
            status_code=429,
            media_type="application/problem+json",
        )
        response.headers["Retry-After"] = str(retry_after)
        return response
    events = tuple(_to_command(event) for event in batch.events)
    try:
        accepted, dropped = container.diagnostics.ingest(
            scope, request.state.request_id, events
        )
    except ValueError as error:
        raise DomainError(
            Problem("diagnostics.timestamp_out_of_range", 422, "Diagnostic timestamp is invalid")
        ) from error
    container.diagnostics.observe_ingestion(
        scope,
        request.state.request_id,
        payload_bytes=len(payload),
        accepted=accepted,
        dropped=dropped,
        rate_limited=0,
        duration_seconds=perf_counter() - ingest_started_at,
    )
    return JSONResponse({"accepted": accepted, "dropped": dropped}, status_code=202)


async def _read_bounded_body(request: Request, maximum_bytes: int) -> bytes:
    """@brief 流式读取并在 JSON 解析前执行字节上限 / Stream the body and enforce bytes before JSON parsing.

    @param request FastAPI 请求 / FastAPI request.
    @param maximum_bytes 最大原始 body 字节数 / Maximum raw body bytes.
    @return 完整但有界的 JSON 字节 / Complete bounded JSON bytes.
    @raise DomainError 超出上限或 body 为空时抛出 / Raised for an oversized or empty body.
    """
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > maximum_bytes:
            raise DomainError(
                Problem("diagnostics.payload_too_large", 413, "Diagnostic payload is too large")
            )
        chunks.append(chunk)
    payload = b"".join(chunks)
    if not payload:
        raise DomainError(
            Problem("diagnostics.invalid_payload", 422, "Diagnostic payload is invalid")
        )
    return payload


def _to_command(event: DiagnosticRequest) -> ClientDiagnostic:
    """@brief 将严格 API DTO 转为应用命令 / Convert a strict API DTO to an application command.

    @param event 已验证判别联合成员 / Validated union member.
    @return 不含客户端权限字段的应用命令 / Application command without client-controlled authority.
    """
    if isinstance(event, ErrorDiagnosticRequest):
        return ClientErrorDiagnostic(
            event.client_event_id,
            event.occurred_at,
            event.route,
            event.release,
            event.error_code,
            event.stack_fingerprint,
        )
    if isinstance(event, PerformanceDiagnosticRequest):
        return ClientPerformanceDiagnostic(
            event.client_event_id,
            event.occurred_at,
            event.route,
            event.release,
            event.metric_name,
            event.value,
            event.unit,
        )
    return ClientNetworkDiagnostic(
        event.client_event_id,
        event.occurred_at,
        event.route,
        event.release,
        event.operation,
        event.duration_ms,
        event.status_code,
    )


__all__ = [
    "DiagnosticBatchRequest",
    "DiagnosticRequest",
    "ErrorDiagnosticRequest",
    "NetworkDiagnosticRequest",
    "PerformanceDiagnosticRequest",
    "diagnostics_router",
]
