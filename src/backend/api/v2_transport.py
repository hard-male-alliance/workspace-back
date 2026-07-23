"""@brief API V2 的共享 HTTP transport 边界 / Shared HTTP transport boundary for API V2.

该模块把纯协议内核组合为 FastAPI request/response 原语，供 Access、Resume、Knowledge
与 Platform adapter 共用。资源专属应用错误必须由调用方通过小型映射函数注入；本模块
不依赖任何资源专属 application 或 domain 类型。
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, cast

from fastapi import Path, Query, Request
from fastapi.responses import Response

from backend.api.constants import PROTECTED_RESOURCE_METADATA_URL
from backend.api.v2_http import (
    JSON_MEDIA_TYPE,
    MERGE_PATCH_MEDIA_TYPE,
    ContractDefinitionValidator,
    CursorCodec,
    JsonValue,
    canonical_json_bytes,
    decode_contract_json,
    list_response,
    require_strong_if_match,
    resource_response_headers,
    strong_etag,
    token_principal_from_claims,
    validate_idempotency_key,
)
from backend.application.ports.v2_idempotency import (
    IdempotencyConflict,
    IdempotencyPreparationId,
    IdempotencyRequest,
    IdempotencyScope,
    ReplayableResponse,
    V2IdempotencyExecutor,
    V2PreparedIdempotencyExecutor,
)
from backend.domain.common import DomainError, Problem
from backend.domain.principals import ResourceMeta, TokenPrincipal, WorkspaceId

#: @brief 集合默认页长 / Default collection page size.
DEFAULT_PAGE_LIMIT = 50
#: @brief 契约允许的最大页长 / Maximum page size allowed by the contract.
MAX_PAGE_LIMIT = 200
#: @brief 小型 V2 命令的默认原始 body 上限 / Default raw-body limit for small V2 commands.
DEFAULT_MAX_BODY_BYTES = 64 * 1024
#: @brief 小型 V2 JSON 的默认最大容器深度 / Default maximum container depth for small V2 JSON.
DEFAULT_MAX_JSON_DEPTH = 8
#: @brief JSON response 的默认规范字节上限 / Default canonical-byte limit for JSON responses.
DEFAULT_MAX_JSON_RESPONSE_BYTES = 8 * 1024 * 1024
#: @brief 不透明标识的正式语法 / Published opaque-identifier grammar.
_OPAQUE_ID_PATTERN = r"^[A-Za-z][A-Za-z0-9_-]*$"
#: @brief 通用 ResourceMeta 页的稳定 keyset 排序 / Stable keyset ordering for ResourceMeta pages.
_RESOURCE_KEYSET_SORT = ("created_at", "id")
#: @brief 契约 extensions 的反向域名 key 语法 / Reverse-domain key grammar for contract extensions.
_EXTENSION_KEY = re.compile(r"^[a-z][a-z0-9]*(?:\.[a-z0-9][a-z0-9_-]*)+$")

PageLimit = Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)]
"""@brief 契约分页 limit 类型 / Contract page-limit type."""

PageCursor = Annotated[str | None, Query(min_length=1, max_length=2048)]
"""@brief 有界 opaque cursor query 类型 / Bounded opaque-cursor query type."""

OpaquePath = Annotated[
    str,
    Path(min_length=8, max_length=160, pattern=_OPAQUE_ID_PATTERN),
]
"""@brief 不透明资源 path 参数类型 / Opaque resource-path parameter type."""

type ProblemMapper = Callable[[Exception], Problem]
"""@brief 将资源专属预期异常映射为公开 Problem / Map expected resource errors to public Problems."""


@dataclass(frozen=True, slots=True)
class _PreparedOutcome[PreparedT]:
    """@brief 分相幂等 prepare 的成功值或已映射失败 / Successful preparation or mapped failure.

    @param value 成功值或可重放 Problem response / Prepared value or replayable Problem response.
    @param failed ``value`` 是否为失败 response / Whether ``value`` is a failure response.
    """

    value: PreparedT | ReplayableResponse
    failed: bool


def verified_principal(request: Request) -> TokenPrincipal:
    """@brief 从 middleware 已验签 claims 构建 principal / Build a principal from middleware-verified claims.

    @param request middleware 已完成 token 验证的 request / Request whose token was verified by middleware.
    @return 不可变 token principal / Immutable token principal.
    @raise DomainError claims 缺失或无效时抛出 / Raised when claims are absent or invalid.
    @note 本函数只投影 claims，不执行 JWT 密码学验证 / This function only projects claims; it does not verify JWT cryptography.
    """

    claims = getattr(request.state, "oauth_claims", None)
    if not isinstance(claims, Mapping):
        raise DomainError(Problem("oauth.invalid_token", 401, "Bearer token is invalid"))
    return token_principal_from_claims(cast(Mapping[str, object], claims))


def request_id(request: Request) -> str:
    """@brief 返回 transport middleware 分配的 request ID / Return the request ID assigned by transport middleware.

    @param request 当前 request / Current request.
    @return 非空 request ID；middleware 缺失时返回冻结占位值 / Non-empty request ID, or the frozen fallback when middleware is absent.
    """

    value = getattr(request.state, "request_id", None)
    return value if isinstance(value, str) and value else "req_unavailable"


async def strict_json_object(
    request: Request,
    *,
    validator: ContractDefinitionValidator,
    definition: str,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    max_depth: int = DEFAULT_MAX_JSON_DEPTH,
) -> dict[str, JsonValue]:
    """@brief 严格解码并按正式 definition 校验 JSON object / Strictly decode and validate a JSON object by definition.

    @param request 当前 request / Current request.
    @param validator 权威 V2 definition 校验器 / Authoritative V2 definition validator.
    @param definition request definition 名称 / Request definition name.
    @param max_body_bytes 路由允许的原始字节上限 / Route raw-byte limit.
    @param max_depth 路由允许的容器嵌套深度 / Route container-depth limit.
    @return 已严格解码且通过 schema 的 JSON object / Strictly decoded, schema-valid JSON object.
    @raise DomainError 媒体类型、大小、深度、JSON 或 schema 无效时抛出 / Raised for invalid media type, size, depth, JSON, or schema.
    @raise RuntimeError 正式 object definition 异常接受非 object 时抛出 / Raised if an object definition unexpectedly accepts a non-object.
    """

    if max_body_bytes < 0:
        raise ValueError("request body limit cannot be negative")
    content_types = request.headers.getlist("Content-Type")
    content_type = content_types[0] if len(content_types) == 1 else None
    payload = decode_contract_json(
        raw_body=await _bounded_body(request, maximum_bytes=max_body_bytes),
        content_type=content_type,
        method=request.method,
        max_body_bytes=max_body_bytes,
        max_depth=max_depth,
        validator=validator,
        definition=definition,
    )
    if not isinstance(payload, dict):
        raise RuntimeError(f"contract definition {definition} must accept only objects")
    return payload


def require_query(request: Request, *allowed: str) -> None:
    """@brief 拒绝未知或重复 query 参数 / Reject unknown or repeated query parameters.

    @param request 当前 request / Current request.
    @param allowed 路由声明的唯一参数名 / Sole parameter names declared by the route.
    @return 无返回值 / No return value.
    @raise DomainError query 含未知名或重复名时抛出 / Raised for an unknown or repeated name.
    """

    allowed_names = frozenset(allowed)
    seen: set[str] = set()
    for name, _value in request.query_params.multi_items():
        if name not in allowed_names or name in seen:
            raise DomainError(Problem("http.invalid_query", 400, "Query parameters are invalid"))
        seen.add(name)


async def require_no_body(request: Request) -> None:
    """@brief 拒绝标记为无 body 的路由携带 payload / Reject payloads on routes declared without a body.

    @param request 当前 request / Current request.
    @return 无返回值 / No return value.
    @raise DomainError body 非空时抛出 / Raised when a body is present.
    """

    lengths = request.headers.getlist("Content-Length")
    if len(lengths) > 1:
        raise DomainError(Problem("http.invalid_content_length", 400, "Content-Length is invalid"))
    if lengths:
        declared = _content_length(lengths[0])
        if declared > 0:
            raise DomainError(
                Problem("http.unexpected_body", 400, "This route does not accept a body")
            )
    async for chunk in request.stream():
        if chunk:
            raise DomainError(
                Problem("http.unexpected_body", 400, "This route does not accept a body")
            )


def if_match_header(request: Request) -> str | None:
    """@brief 无损读取唯一 If-Match header / Read the sole If-Match header without rewriting.

    @param request 当前 request / Current request.
    @return header、缺失或可被强校验拒绝的重复形状 / Header, absence, or a duplicate shape rejected by strong validation.
    """

    values = request.headers.getlist("If-Match")
    if len(values) <= 1:
        return values[0] if values else None
    return ",".join(values)


def match_etag_revision(
    if_match: str | None,
    representation: JsonValue,
    revision: int,
) -> int:
    """@brief 强比较公开表示并返回对应领域 revision / Strongly compare a representation and return its domain revision.

    @param if_match 原始 If-Match header / Raw If-Match header.
    @param representation 当前公开表示 / Current public representation.
    @param revision 与该表示同一快照的领域 revision / Domain revision from the same snapshot.
    @return CAS 使用的匹配 revision / Matching revision used for compare-and-swap.
    @raise DomainError If-Match 缺失、弱、无效或 stale 时抛出 / Raised when If-Match is absent, weak, invalid, or stale.
    """

    require_strong_if_match(if_match, current_etag=strong_etag(representation))
    return revision


def json_response(
    request: Request,
    payload: JsonValue,
    *,
    status_code: int = 200,
    cache_control: str | None = None,
    max_response_bytes: int = DEFAULT_MAX_JSON_RESPONSE_BYTES,
) -> Response:
    """@brief 返回 canonical JSON bytes / Return canonical JSON bytes.

    @param request 当前 request / Current request.
    @param payload JSON payload / JSON payload.
    @param status_code HTTP status / HTTP status.
    @param cache_control 可选 Cache-Control / Optional Cache-Control.
    @param max_response_bytes canonical JSON response 字节上限 / Canonical JSON response-byte limit.
    @return 含当前 X-Request-Id 的 JSON response / JSON response with the current X-Request-Id.
    """

    headers = {"X-Request-Id": request_id(request)}
    if cache_control is not None:
        headers["Cache-Control"] = cache_control
    content = _bounded_json_response(payload, maximum_bytes=max_response_bytes)
    return Response(
        content=content,
        status_code=status_code,
        headers=headers,
        media_type=JSON_MEDIA_TYPE,
    )


def resource_response(
    request: Request,
    payload: JsonValue,
    *,
    status_code: int = 200,
    location: str | None = None,
    max_response_bytes: int = DEFAULT_MAX_JSON_RESPONSE_BYTES,
) -> Response:
    """@brief 返回与实际 canonical bytes 一致的强 ETag / Return a strong ETag matching the actual canonical bytes.

    @param request 当前 request / Current request.
    @param payload 单资源表示 / Single-resource representation.
    @param status_code HTTP status / HTTP status.
    @param location 可选 canonical Location / Optional canonical Location.
    @param max_response_bytes canonical JSON response 字节上限 / Canonical JSON response-byte limit.
    @return 带 ETag、X-Request-Id 与可选 Location 的 response / Response with ETag, X-Request-Id, and optional Location.
    """

    headers = resource_response_headers(
        payload,
        request_id=request_id(request),
        location=location,
    )
    content = _bounded_json_response(payload, maximum_bytes=max_response_bytes)
    return Response(
        content=content,
        status_code=status_code,
        headers=headers,
        media_type=JSON_MEDIA_TYPE,
    )


def empty_response(request: Request) -> Response:
    """@brief 返回契约 204 且不生成非法 JSON body / Return contract 204 without an illegal JSON body.

    @param request 当前 request / Current request.
    @return 含当前 X-Request-Id 的空 204 response / Empty 204 response with the current X-Request-Id.
    """

    return Response(status_code=204, headers={"X-Request-Id": request_id(request)})


def replayable_json(
    payload: JsonValue,
    *,
    status_code: int,
    location: str | None = None,
    etag: bool = False,
    max_response_bytes: int = DEFAULT_MAX_JSON_RESPONSE_BYTES,
) -> ReplayableResponse:
    """@brief 构造可逐字持久化的成功 response / Build a byte-exact persistable success response.

    @param payload JSON payload / JSON payload.
    @param status_code HTTP status / HTTP status.
    @param location 可选 Location / Optional Location.
    @param etag 是否包含强 ETag / Whether to include a strong ETag.
    @param max_response_bytes 可持久化 canonical body 的字节上限 / Persistable canonical-body byte limit.
    @return 不含 request-specific header 的可重放 response / Replayable response without request-specific headers.
    """

    headers: list[tuple[str, str]] = [("Content-Type", JSON_MEDIA_TYPE)]
    if etag:
        headers.append(("ETag", strong_etag(payload)))
    if location is not None:
        headers.append(("Location", location))
    return ReplayableResponse(
        status_code,
        tuple(headers),
        _bounded_json_response(payload, maximum_bytes=max_response_bytes),
    )


async def idempotent_response(
    request: Request,
    *,
    executor: V2IdempotencyExecutor,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId | None,
    canonical_path: str,
    canonical_body: bytes,
    content_type: str | None,
    if_match: str | None,
    operation: Callable[[], Awaitable[ReplayableResponse]],
    mapped_error_types: tuple[type[Exception], ...],
    map_error: ProblemMapper,
) -> Response:
    """@brief 执行或逐字重放一个 V2 command / Execute or byte-exactly replay a V2 command.

    @param request 当前 request / Current request.
    @param executor 幂等 callback executor / Idempotent callback executor.
    @param principal 已验证 principal / Verified principal.
    @param workspace_id 路径 Workspace 或空 / Path workspace, if any.
    @param canonical_path 不含 query 的具体 canonical path / Concrete canonical path without query.
    @param canonical_body 规范请求 body / Canonical request body.
    @param content_type 规范 Content-Type / Canonical Content-Type.
    @param if_match 原始 If-Match / Raw If-Match.
    @param operation 仅首次 claim 执行的 callback / Callback executed only for the first claim.
    @param mapped_error_types 可安全固化的资源专属预期异常 / Resource-specific expected errors safe to materialize.
    @param map_error 资源专属异常到 Problem 的映射 / Mapping from resource-specific errors to Problems.
    @return 首次或 replay HTTP response / First or replayed HTTP response.
    @note DomainError 与显式列出的资源异常会成为 receipt；未知异常保持抛出 / DomainError and explicitly listed resource errors become receipts; unknown errors propagate.
    """

    idempotency_request = _idempotency_request(
        request,
        principal=principal,
        workspace_id=workspace_id,
        canonical_path=canonical_path,
        canonical_body=canonical_body,
        content_type=content_type,
        if_match=if_match,
    )

    async def captured_operation() -> ReplayableResponse:
        """@brief 把确定性 boundary 失败固化为 receipt / Materialize deterministic boundary failures as receipts.

        @return 成功或确定性 ProblemDetails receipt / Success or deterministic Problem Details receipt.
        """

        try:
            return await operation()
        except DomainError as error:
            return _replayable_problem(request, error.problem)
        except mapped_error_types as error:
            return _replayable_problem(request, map_error(error))

    replay = await executor.execute(idempotency_request, captured_operation)
    return _response_from_replay(request, replay)


async def prepared_idempotent_response[PreparedT](
    request: Request,
    *,
    executor: V2PreparedIdempotencyExecutor,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId | None,
    canonical_path: str,
    canonical_body: bytes,
    content_type: str | None,
    if_match: str | None,
    prepare: Callable[[IdempotencyPreparationId], Awaitable[PreparedT]],
    commit: Callable[[PreparedT], Awaitable[ReplayableResponse]],
    mapped_error_types: tuple[type[Exception], ...],
    map_error: ProblemMapper,
) -> Response:
    """@brief 事务外准备外部 I/O，再原子提交领域状态与 receipt / Prepare external I/O outside a transaction, then atomically commit state and receipt.

    @param request 当前 request / Current request.
    @param executor 支持 prepare/commit 分相的 executor / Executor supporting split phases.
    @param principal 已验证 principal / Verified principal.
    @param workspace_id 路径 Workspace 或空 / Path workspace, if any.
    @param canonical_path 规范具体路径 / Canonical concrete path.
    @param canonical_body 规范请求 body / Canonical request body.
    @param content_type 规范媒体类型 / Canonical media type.
    @param if_match 可选强 If-Match / Optional strong If-Match.
    @param prepare 仅执行外部准备且使用稳定 preparation ID / External-only preparation using a
        stable preparation ID.
    @param commit 仅执行数据库工作的最终提交 / Final database-only commit.
    @param mapped_error_types 可固化的资源异常 / Resource errors safe to materialize.
    @param map_error 资源异常映射 / Resource-error mapper.
    @return 首次或逐字重放 HTTP response / First or byte-replayed HTTP response.

    @note prepare 的确定性 boundary 失败也会在最终短事务中成为 receipt；未知异常不固化，
        外部 adapter 在重试时必须按 preparation ID 去重 / Deterministic preparation failures
        become receipts in the final short transaction. Unknown failures propagate, and external
        adapters must deduplicate retries by preparation ID.
    """

    idempotency_request = _idempotency_request(
        request,
        principal=principal,
        workspace_id=workspace_id,
        canonical_path=canonical_path,
        canonical_body=canonical_body,
        content_type=content_type,
        if_match=if_match,
    )

    async def captured_prepare(
        preparation_id: IdempotencyPreparationId,
    ) -> _PreparedOutcome[PreparedT]:
        """@brief 捕获可确定重放的准备阶段结果 / Capture a deterministically replayable preparation result.

        @param preparation_id 跨崩溃稳定的外部操作标识 / Crash-stable external-operation identifier.
        @return 已准备值或可重放问题 / Prepared value or replayable problem.
        """
        try:
            return _PreparedOutcome(await prepare(preparation_id), False)
        except DomainError as error:
            return _PreparedOutcome(_replayable_problem(request, error.problem), True)
        except mapped_error_types as error:
            return _PreparedOutcome(_replayable_problem(request, map_error(error)), True)

    async def captured_commit(
        outcome: _PreparedOutcome[PreparedT],
    ) -> ReplayableResponse:
        """@brief 在最终短事务中固化结果 / Materialize the result in the final short transaction.

        @param outcome 准备值或已捕获问题 / Prepared value or captured problem.
        @return 与领域提交原子保存的逐字 response / Byte-exact response saved atomically with the domain commit.
        """
        if outcome.failed:
            if not isinstance(outcome.value, ReplayableResponse):
                raise RuntimeError("failed preparation outcome has no replayable response")
            return outcome.value
        try:
            return await commit(cast(PreparedT, outcome.value))
        except DomainError as error:
            return _replayable_problem(request, error.problem)
        except mapped_error_types as error:
            return _replayable_problem(request, map_error(error))

    replay = await executor.execute_prepared(
        idempotency_request,
        captured_prepare,
        captured_commit,
    )
    return _response_from_replay(request, replay)


def require_idempotency_key(request: Request) -> str:
    """@brief 读取且严格校验唯一 Idempotency-Key / Read and strictly validate one Idempotency-Key.

    @param request 当前 HTTP request / Current HTTP request.
    @return 未经改写的合法 key / Valid key without rewriting.
    @raise DomainError header 缺失、重复或语法非法时抛出 / Raised when the header is missing,
        duplicated, or syntactically invalid.

    @note 专用 secret-safe replay 协议与通用 receipt 共用此边界，避免两种幂等路径对
        duplicate header 产生不同解释 / Dedicated secret-safe replay and generic receipts share
        this boundary so duplicate headers cannot be interpreted differently.
    """

    keys = request.headers.getlist("Idempotency-Key")
    if len(keys) > 1:
        raise DomainError(
            Problem("http.invalid_idempotency_key", 400, "Idempotency-Key is invalid")
        )
    return validate_idempotency_key(keys[0] if keys else None)


def _idempotency_request(
    request: Request,
    *,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId | None,
    canonical_path: str,
    canonical_body: bytes,
    content_type: str | None,
    if_match: str | None,
) -> IdempotencyRequest:
    """@brief 构造共享的完整幂等请求 / Build the shared complete idempotency request.

    @return 已验证 scope 与指纹输入 / Validated scope and fingerprint inputs.
    """

    return IdempotencyRequest(
        IdempotencyScope(
            principal.user_id,
            workspace_id,
            request.method.upper(),
            canonical_path,
            require_idempotency_key(request),
        ),
        canonical_body,
        content_type,
        if_match,
    )


def _response_from_replay(request: Request, replay: ReplayableResponse) -> Response:
    """@brief 把可重放值恢复为当前 request 的 HTTP response / Restore a replayable value for the current request.

    @param request 当前 request / Current request.
    @param replay 已持久化或首次生成的 response / Persisted or newly generated response.
    @return 注入当前 request ID 与认证 challenge 的 HTTP response / HTTP response carrying the
        current request ID and authentication challenge.
    """

    headers = dict(replay.headers)
    headers["X-Request-Id"] = request_id(request)
    if replay.status_code == 401:
        headers["WWW-Authenticate"] = _bearer_challenge()
    return Response(content=replay.json_body, status_code=replay.status_code, headers=headers)


def problem_response(
    request: Request,
    problem: Problem,
    *,
    error: BaseException | None = None,
) -> Response:
    """@brief 构造 V2 ProblemDetails 与必要协议 header / Build V2 Problem Details and required protocol headers.

    @param request 当前 request / Current request.
    @param problem 公开 problem / Public problem.
    @param error 可选来源异常，用于 Retry-After / Optional source exception used for Retry-After.
    @return application/problem+json response / application/problem+json response.
    """

    headers = {"X-Request-Id": request_id(request)}
    if problem.status == 401:
        headers["WWW-Authenticate"] = _bearer_challenge()
    if isinstance(error, IdempotencyConflict) and error.retry_after_seconds is not None:
        headers["Retry-After"] = str(error.retry_after_seconds)
    return Response(
        content=canonical_json_bytes(problem_payload(request, problem)),
        status_code=problem.status,
        headers=headers,
        media_type="application/problem+json",
    )


def problem_payload(request: Request, problem: Problem) -> dict[str, JsonValue]:
    """@brief 投影冻结的 RFC 9457 扩展形状 / Project the frozen RFC 9457 extension shape.

    @param request 当前或首次 request / Current or first request.
    @param problem 内部 problem / Internal problem.
    @return API V2 ProblemDetails / API V2 ProblemDetails.
    """

    errors: list[JsonValue] = []
    for violation in problem.violations:
        message = violation.get("message")
        message_key = message.get("message_key") if isinstance(message, dict) else None
        params = message.get("params", {}) if isinstance(message, dict) else {}
        errors.append(
            {
                "pointer": str(violation.get("pointer", "")),
                "code": str(violation.get("code", "schema.invalid")),
                "message_key": message_key if isinstance(message_key, str) else None,
                "params": cast(JsonValue, params if isinstance(params, dict) else {}),
            }
        )
    validation_pointer = problem.extensions.get("pointer")
    if isinstance(validation_pointer, str) and not errors:
        errors.append(
            {
                "pointer": validation_pointer,
                "code": "schema.invalid",
                "message_key": None,
                "params": {},
            }
        )
    extensions = {
        key: cast(JsonValue, value)
        for key, value in problem.extensions.items()
        if _EXTENSION_KEY.fullmatch(key) is not None
    }
    return {
        "type": "https://api.hmalliances.org:8022/problems/" + problem.code.replace(".", "/"),
        "title": problem.title,
        "status": problem.status,
        "detail": problem.detail,
        "instance": request.url.path,
        "code": problem.code,
        "request_id": request_id(request),
        "retryable": problem.retryable,
        "errors": errors,
        "extensions": extensions,
    }


def timestamp(value: datetime) -> str:
    """@brief 序列化 UTC RFC 3339 timestamp / Serialize a UTC RFC 3339 timestamp.

    @param value 带时区 datetime / Timezone-aware datetime.
    @return 毫秒精度、Z 结尾 timestamp / Millisecond-precision, Z-suffixed timestamp.
    """

    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def resource_meta[IdT: str](meta: ResourceMeta[IdT]) -> dict[str, JsonValue]:
    """@brief 投影通用 Resource 字段 / Project common Resource fields.

    @param meta 领域资源元数据 / Domain resource metadata.
    @return API Resource 字段 / API Resource fields.
    """

    return {
        "id": str(meta.id),
        "revision": meta.revision,
        "created_at": timestamp(meta.created_at),
        "updated_at": timestamp(meta.updated_at),
    }


def keyset_page[ItemT, IdT: str](
    items: Sequence[ItemT],
    *,
    cursor: str | None,
    limit: int,
    codec: CursorCodec,
    principal: TokenPrincipal,
    workspace_id: WorkspaceId | None,
    collection: str,
    key: Callable[[ItemT], ResourceMeta[IdT]],
    project: Callable[[ItemT], JsonValue],
) -> dict[str, JsonValue]:
    """@brief 以授权上下文绑定的签名 keyset cursor 构建一页 / Build a page with an authorization-bound signed keyset cursor.

    @param items 应用层返回的可见序列 / Visible sequence returned by the application layer.
    @param cursor 可选 cursor / Optional cursor.
    @param limit 页长 / Page size.
    @param codec 签名 cursor codec / Signed cursor codec.
    @param principal 当前 principal / Current principal.
    @param workspace_id 路径 Workspace 或空 / Path workspace, if any.
    @param collection 集合身份，用于阻止跨路由 replay / Collection identity preventing cross-route replay.
    @param key 提取稳定 ResourceMeta 的函数 / Stable ResourceMeta extractor.
    @param project 公开投影函数 / Public projection function.
    @return `{items,page}` / `{items,page}`.
    """

    ordered = sorted(items, key=lambda item: (key(item).created_at, str(key(item).id)))
    position: tuple[str, str] | None = None
    filters: dict[str, JsonValue] = {"collection": collection}
    if cursor is not None:
        decoded = codec.decode(
            cursor,
            principal=principal,
            workspace_id=workspace_id,
            filters=filters,
            sort=_RESOURCE_KEYSET_SORT,
        )
        if (
            not isinstance(decoded, dict)
            or set(decoded) != {"created_at", "id"}
            or not isinstance(decoded["created_at"], str)
            or not isinstance(decoded["id"], str)
        ):
            raise DomainError(Problem("http.cursor_invalid", 400, "Pagination cursor is invalid"))
        position = (decoded["created_at"], decoded["id"])
    remaining = [
        item
        for item in ordered
        if position is None or (timestamp(key(item).created_at), str(key(item).id)) > position
    ]
    selected = remaining[:limit]
    has_more = len(remaining) > len(selected)
    next_cursor: str | None = None
    if has_more:
        last = key(selected[-1])
        next_cursor = codec.encode(
            {"created_at": timestamp(last.created_at), "id": str(last.id)},
            principal=principal,
            workspace_id=workspace_id,
            filters=filters,
            sort=_RESOURCE_KEYSET_SORT,
        )
    return list_response([project(item) for item in selected], next_cursor=next_cursor)


def _replayable_problem(request: Request, problem: Problem) -> ReplayableResponse:
    """@brief 把确定性 ProblemDetails 变成 receipt / Convert deterministic Problem Details into a receipt.

    @param request 首次 request / First request.
    @param problem 结构化 problem / Structured problem.
    @return 可逐字重放的问题 response / Byte-exact replayable problem response.
    """

    return ReplayableResponse(
        problem.status,
        (("Content-Type", "application/problem+json"),),
        canonical_json_bytes(problem_payload(request, problem)),
    )


def _bearer_challenge() -> str:
    """@brief 构造冻结的 Resource Server challenge / Build the frozen Resource Server challenge.

    @return WWW-Authenticate 值 / WWW-Authenticate value.
    """

    return f'Bearer resource_metadata="{PROTECTED_RESOURCE_METADATA_URL}"'


async def _bounded_body(request: Request, *, maximum_bytes: int) -> bytes:
    """@brief 在分配完整 body 前执行字节上限 / Enforce the byte limit before full allocation.

    @param request 当前 ASGI request / Current ASGI request.
    @param maximum_bytes 路由原始 body 上限 / Route raw-body limit.
    @return 不超过上限的原始 bytes / Raw bytes no larger than the limit.
    @raise DomainError Content-Length 无效或流超过上限时抛出 / Raised for an invalid length or
        an oversized stream.

    @note ``Content-Length`` 只用于尽早拒绝；chunked 请求仍按实际累计字节计数，不能靠省略
        header 绕过上限。/ Content-Length enables early rejection only; chunked requests are
        still bounded by the actual accumulated bytes.
    """
    lengths = request.headers.getlist("Content-Length")
    if len(lengths) > 1:
        raise DomainError(Problem("http.invalid_content_length", 400, "Content-Length is invalid"))
    if lengths and _content_length(lengths[0]) > maximum_bytes:
        raise DomainError(Problem("http.payload_too_large", 413, "Request payload is too large"))
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > maximum_bytes:
            raise DomainError(
                Problem("http.payload_too_large", 413, "Request payload is too large")
            )
        body.extend(chunk)
    return bytes(body)


def _bounded_json_response(payload: JsonValue, *, maximum_bytes: int) -> bytes:
    """@brief 在发送或写入 receipt 前限制 canonical JSON response / Bound canonical JSON before sending or receipt persistence.

    @param payload 待编码 JSON / JSON value to encode.
    @param maximum_bytes 允许的最大规范字节数 / Maximum canonical byte count.
    @return 不超过上限的 canonical JSON / Canonical JSON within the limit.
    @raise DomainError 应用结果超过公开响应预算时抛出 / Raised when an application result
        exceeds the public response budget.
    @note 该门禁防止 collection/SIR 扩张绕过请求侧上限，并保证幂等 receipt 不会存入无界
        body / This prevents collection/SIR expansion from bypassing request-side limits and keeps
        idempotency receipts bounded.
    """
    if maximum_bytes < 0:
        raise ValueError("response body limit cannot be negative")
    encoded = canonical_json_bytes(payload)
    if len(encoded) > maximum_bytes:
        raise DomainError(
            Problem("http.response_too_large", 500, "Response exceeds the configured limit")
        )
    return encoded


def _content_length(value: str) -> int:
    """@brief 严格解析非负十进制 Content-Length / Strictly parse decimal Content-Length.

    @param value 原始 header 值 / Raw header value.
    @return 非负字节数 / Non-negative byte count.
    @raise DomainError 空、带符号、空白或非十进制值时抛出 / Raised for empty, signed, padded,
        or non-decimal values.
    """
    if not value or not value.isascii() or not value.isdecimal():
        raise DomainError(Problem("http.invalid_content_length", 400, "Content-Length is invalid"))
    return int(value)


__all__ = [
    "DEFAULT_MAX_BODY_BYTES",
    "DEFAULT_MAX_JSON_DEPTH",
    "DEFAULT_MAX_JSON_RESPONSE_BYTES",
    "DEFAULT_PAGE_LIMIT",
    "JSON_MEDIA_TYPE",
    "MAX_PAGE_LIMIT",
    "MERGE_PATCH_MEDIA_TYPE",
    "ContractDefinitionValidator",
    "CursorCodec",
    "JsonValue",
    "OpaquePath",
    "PageCursor",
    "PageLimit",
    "ProblemMapper",
    "canonical_json_bytes",
    "empty_response",
    "idempotent_response",
    "if_match_header",
    "json_response",
    "keyset_page",
    "match_etag_revision",
    "problem_payload",
    "problem_response",
    "replayable_json",
    "request_id",
    "require_no_body",
    "require_query",
    "require_strong_if_match",
    "resource_meta",
    "resource_response",
    "strict_json_object",
    "strong_etag",
    "timestamp",
    "verified_principal",
]
