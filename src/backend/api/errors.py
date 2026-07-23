"""@brief FastAPI 错误转换 / FastAPI error translation."""

from __future__ import annotations

from collections.abc import Sequence
from http import HTTPStatus
from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from backend.domain.common import DomainError, Problem


async def domain_error_handler(request: Request, error: Exception) -> JSONResponse:
    """@brief 将领域错误转换为 ProblemDetails / Translate a domain error to ProblemDetails.

    @param request 当前 HTTP 请求 / Current HTTP request.
    @param error 领域错误 / Domain error.
    @return application/problem+json 响应 / application/problem+json response.
    """
    if not isinstance(error, DomainError):
        raise error
    return api_problem_response(request, error.problem)


async def request_validation_error_handler(
    request: Request,
    error: Exception,
) -> JSONResponse:
    """@brief 将 FastAPI/Pydantic 输入错误转换为 ProblemDetails / Translate FastAPI/Pydantic input errors to ProblemDetails.

    @param request 当前 HTTP 请求 / Current HTTP request.
    @param error FastAPI 请求校验错误 / FastAPI request validation error.
    @return application/problem+json 响应 / application/problem+json response.
    @note 不回显未信任的 rejected value，避免 prompt、token 或大请求体进入错误响应。
    """
    if not isinstance(error, RequestValidationError):
        raise error
    errors = error.errors()
    is_invalid_json = any(item.get("type") == "json_invalid" for item in errors)
    problem = Problem(
        "http.invalid_json" if is_invalid_json else "http.validation_failed",
        400 if is_invalid_json else 422,
        "Request body is not valid JSON" if is_invalid_json else "Request validation failed",
        violations=[_validation_violation(item) for item in errors],
    )
    return _problem_response(request, problem)


async def http_exception_handler(
    request: Request,
    error: Exception,
) -> JSONResponse:
    """@brief 将框架 HTTP 错误转换为 ProblemDetails / Translate framework HTTP errors to ProblemDetails.

    @param request 当前 HTTP 请求 / Current HTTP request.
    @param error Starlette HTTP 异常 / Starlette HTTP exception.
    @return application/problem+json 响应 / application/problem+json response.
    """
    if not isinstance(error, StarletteHTTPException):
        raise error
    try:
        title = HTTPStatus(error.status_code).phrase
    except ValueError:
        title = "HTTP request failed"
    return _problem_response(request, Problem("http.request_rejected", error.status_code, title))


def _validation_violation(error: dict[str, Any]) -> dict[str, Any]:
    """@brief 将一条 Pydantic 错误映射为 FieldViolation / Map one Pydantic error to FieldViolation.

    @param error Pydantic 错误对象 / Pydantic error object.
    @return 合同兼容的字段违反项 / Contract-compatible field violation.
    """
    location = error.get("loc", ())
    pointer = _json_pointer(location if isinstance(location, Sequence) else ())
    error_type = error.get("type")
    return {
        "pointer": pointer,
        "code": "schema.invalid"
        if not isinstance(error_type, str)
        else f"schema.{error_type.replace('_', '.')}",
        "message": {
            "message_key": "errors.request.validation",
            "fallback_message": "Request input is invalid.",
            "params": {},
        },
    }


def _json_pointer(location: Sequence[object]) -> str:
    """@brief 把 FastAPI location 转成 JSON Pointer / Convert a FastAPI location to a JSON Pointer.

    @param location FastAPI 的错误位置序列 / FastAPI error location sequence.
    @return 经过 RFC 6901 转义的 JSON Pointer / RFC 6901-escaped JSON Pointer.
    """
    parts = (
        location[1:]
        if location and location[0] in {"body", "query", "path", "header"}
        else location
    )
    return "".join(f"/{str(part).replace('~', '~0').replace('/', '~1')}" for part in parts)


def _problem_response(request: Request, problem: Problem) -> JSONResponse:
    """@brief 生成统一错误响应 / Build a unified error response.

    @param request 当前 HTTP 请求 / Current HTTP request.
    @param problem 结构化业务问题 / Structured application problem.
    @return application/problem+json 响应 / application/problem+json response.
    """
    return api_problem_response(request, problem)


def api_problem_response(request: Request, problem: Problem) -> JSONResponse:
    """Build the version-specific public Problem Details representation."""

    if request.url.path.startswith("/api/v2/"):
        errors = []
        for violation in problem.violations:
            message = violation.get("message")
            message_key = message.get("message_key") if isinstance(message, dict) else None
            params = message.get("params", {}) if isinstance(message, dict) else {}
            errors.append(
                {
                    "pointer": violation.get("pointer", ""),
                    "code": violation.get("code", "schema.invalid"),
                    "message_key": message_key,
                    "params": params if isinstance(params, dict) else {},
                }
            )
        return JSONResponse(
            status_code=problem.status,
            content={
                "type": (
                    "https://api.hmalliances.org:8022/problems/" + problem.code.replace(".", "/")
                ),
                "title": problem.title,
                "status": problem.status,
                "detail": problem.detail,
                "instance": request.url.path,
                "code": problem.code,
                "request_id": getattr(request.state, "request_id", None),
                "retryable": problem.retryable,
                "errors": errors,
                "extensions": problem.extensions,
            },
            media_type="application/problem+json",
        )
    return JSONResponse(
        status_code=problem.status,
        content=problem.as_dict(
            request_id=getattr(request.state, "request_id", None),
            instance=request.url.path,
        ),
        media_type="application/problem+json",
    )
