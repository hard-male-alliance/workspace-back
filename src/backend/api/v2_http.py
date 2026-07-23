"""@brief API V2 的纯 HTTP 语义内核 / Pure HTTP semantics kernel for API V2."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Never, Protocol, cast

from backend.domain.common import DomainError, Problem
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM
from backend.domain.principals import (
    ClientId,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceId,
)

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]

#: @brief V2 幂等键的唯一合法语法 / The sole valid V2 idempotency-key syntax.
_IDEMPOTENCY_KEY = re.compile(r"[A-Za-z0-9._~-]{16,128}\Z", flags=re.ASCII)
#: @brief OAuth scope-token 的 RFC 6749 字符范围 / RFC 6749 scope-token character range.
_SCOPE_TOKEN = re.compile(r"[\x21\x23-\x5b\x5d-\x7e]+\Z", flags=re.ASCII)
#: @brief 单个强实体标签的 RFC 9110 ASCII 子集 / RFC 9110 ASCII subset for one strong entity tag.
_STRONG_ETAG = re.compile(r'"[\x21\x23-\x7e]*"\Z', flags=re.ASCII)
#: @brief 完整 JSON 请求的媒体类型 / Media type for complete JSON requests.
JSON_MEDIA_TYPE = "application/json"
#: @brief JSON Merge Patch 请求的媒体类型 / Media type for JSON Merge Patch requests.
MERGE_PATCH_MEDIA_TYPE = "application/merge-patch+json"
#: @brief Cursor wire format 的版本 / Cursor wire-format version.
_CURSOR_VERSION = 1
#: @brief Cursor 的最大传输长度 / Maximum cursor transport length.
_MAX_CURSOR_LENGTH = 2048


class ContractDefinitionValidator(Protocol):
    """@brief 契约 definition 校验端口 / Port for contract-definition validation."""

    def validate_definition(self, definition: str, payload: object) -> None:
        """@brief 校验一个正式 definition / Validate one published definition.

        @param definition JSON Schema `$defs` 名称 / JSON Schema `$defs` name.
        @param payload 已严格解码的 JSON 值 / Strictly decoded JSON value.
        @return 无返回值 / No return value.
        """
        ...


def token_principal_from_claims(claims: Mapping[str, object]) -> TokenPrincipal:
    """@brief 将已验签 OAuth claims 投影为不可变主体 / Project verified OAuth claims into an immutable principal.

    @param claims 已完成签名、issuer、audience 与时间验证的 claims / Claims whose signature, issuer, audience and time were verified.
    @return 只含授权所需字段的不可变主体 / Immutable principal containing only authorization fields.
    @raise DomainError 必需 claim 缺失或语法不合法时抛出 / Raised when a required claim is missing or malformed.
    @note 本函数不替代 JWT 密码学验证 / This function does not replace cryptographic JWT validation.
    """

    subject = claims.get("sub")
    user_id = claims.get(ACCESS_TOKEN_USER_ID_CLAIM)
    client_id = claims.get("client_id")
    raw_scope = claims.get("scope")
    if not isinstance(subject, str) or not subject or subject.strip() != subject:
        raise _invalid_token_claims()
    if not isinstance(user_id, str) or not user_id or user_id.strip() != user_id:
        raise _invalid_token_claims()
    if not isinstance(client_id, str) or not client_id or client_id.strip() != client_id:
        raise _invalid_token_claims()
    if not isinstance(raw_scope, str) or not raw_scope:
        raise _invalid_token_claims()
    scope_names = raw_scope.split(" ")
    if any(not name or _SCOPE_TOKEN.fullmatch(name) is None for name in scope_names) or len(
        scope_names
    ) != len(set(scope_names)):
        raise _invalid_token_claims()
    return TokenPrincipal(
        UserId(user_id),
        Subject(subject),
        ClientId(client_id),
        frozenset(Scope(name) for name in scope_names),
    )


def validate_idempotency_key(value: str | None) -> str:
    """@brief 校验并返回 V2 幂等键 / Validate and return a V2 idempotency key.

    @param value 原始 `Idempotency-Key` header 值 / Raw `Idempotency-Key` header value.
    @return 未经改写的合法键 / Valid key without rewriting.
    @raise DomainError header 缺失或不满足 16--128 URL-safe ASCII 时抛出 / Raised when the header is absent or violates the 16--128 URL-safe ASCII rule.
    """

    if value is None:
        raise DomainError(
            Problem("http.idempotency_key_required", 400, "Idempotency-Key is required")
        )
    if _IDEMPOTENCY_KEY.fullmatch(value) is None:
        raise DomainError(
            Problem("http.invalid_idempotency_key", 400, "Idempotency-Key is invalid")
        )
    return value


def decode_contract_json(
    *,
    raw_body: bytes,
    content_type: str | None,
    method: str,
    max_body_bytes: int,
    max_depth: int,
    validator: ContractDefinitionValidator,
    definition: str,
) -> JsonValue:
    """@brief 在契约校验前安全解码 JSON body / Safely decode a JSON body before contract validation.

    @param raw_body 尚未反序列化的请求字节 / Request bytes not yet deserialized.
    @param content_type 原始 Content-Type header / Raw Content-Type header.
    @param method HTTP method；PATCH 强制 Merge Patch / HTTP method; PATCH requires Merge Patch.
    @param max_body_bytes 路由级原始字节上限 / Route-level raw-byte limit.
    @param max_depth JSON 容器最大嵌套深度 / Maximum JSON container nesting depth.
    @param validator 注入的权威契约校验器 / Injected authoritative contract validator.
    @param definition 要校验的正式 `$defs` 名称 / Published `$defs` name to validate.
    @return 严格 JSON 值 / Strict JSON value.
    @raise DomainError 媒体类型、大小、深度、JSON 或 Schema 不合法时抛出 / Raised for invalid media type, size, depth, JSON, or schema.
    @note 大小与深度均在完整 JSON 反序列化前检查 / Size and depth are checked before full JSON deserialization.
    """

    if max_body_bytes < 0 or max_depth < 0 or not definition:
        raise ValueError("JSON boundary configuration is invalid")
    if len(raw_body) > max_body_bytes:
        raise _payload_too_large("Request body exceeds the route byte limit")
    expected_media_type = MERGE_PATCH_MEDIA_TYPE if method.upper() == "PATCH" else JSON_MEDIA_TYPE
    if content_type is None or content_type.strip().lower() != expected_media_type:
        raise DomainError(
            Problem(
                "http.unsupported_media_type",
                415,
                f"Content-Type must be {expected_media_type}",
            )
        )
    try:
        serialized = raw_body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise _invalid_json() from error
    _enforce_raw_json_depth(serialized, max_depth=max_depth)
    try:
        payload = _strict_json_loads(serialized)
    except (json.JSONDecodeError, RecursionError, ValueError) as error:
        raise _invalid_json() from error
    validator.validate_definition(definition, payload)
    return payload


def canonical_json_bytes(value: JsonValue) -> bytes:
    """@brief 生成确定性 UTF-8 JSON 表示 / Produce a deterministic UTF-8 JSON representation.

    @param value 合法 JSON 值 / Valid JSON value.
    @return 排序键、无多余空白的 UTF-8 字节 / UTF-8 bytes with sorted keys and no insignificant whitespace.
    @raise ValueError 值含非有限浮点数时抛出 / Raised when the value contains a non-finite float.
    """

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def strong_etag(value: JsonValue) -> str:
    """@brief 从 canonical JSON 表示生成强 ETag / Generate a strong ETag from canonical JSON.

    @param value 公开资源表示，而非领域 revision / Public resource representation, not a domain revision.
    @return 带双引号的 SHA-256 强实体标签 / Quoted SHA-256 strong entity tag.
    """

    digest = hashlib.sha256(canonical_json_bytes(value)).digest()
    return f'"sha256-{_base64url_encode(digest)}"'


def require_strong_if_match(if_match: str | None, *, current_etag: str) -> str:
    """@brief 强制单个强 If-Match 并执行强比较 / Require one strong If-Match and perform strong comparison.

    @param if_match 客户端 header 值 / Client header value.
    @param current_etag 当前选中表示的强 ETag / Strong ETag of the selected representation.
    @return 规范化后的匹配实体标签 / Normalized matching entity tag.
    @raise DomainError 缺失、弱、语法错误或不匹配时抛出 / Raised when absent, weak, malformed, or mismatched.
    """

    if _STRONG_ETAG.fullmatch(current_etag) is None:
        raise ValueError("current_etag must be one strong entity tag")
    if if_match is None:
        raise DomainError(
            Problem("http.precondition_failed", 412, "A strong If-Match header is required")
        )
    candidate = if_match.strip()
    if candidate.startswith("W/") or _STRONG_ETAG.fullmatch(candidate) is None:
        raise DomainError(
            Problem("http.precondition_failed", 412, "If-Match must contain one strong ETag")
        )
    if not hmac.compare_digest(candidate, current_etag):
        raise DomainError(
            Problem("http.precondition_failed", 412, "The selected representation has changed")
        )
    return candidate


class CursorCodec:
    """@brief HMAC 签名且绑定授权上下文的 opaque cursor / HMAC-signed opaque cursor bound to authorization context."""

    def __init__(
        self,
        secret: bytes,
        *,
        lifetime: timedelta = timedelta(minutes=15),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """@brief 初始化 cursor codec / Initialize the cursor codec.

        @param secret 至少 32 字节的服务端 HMAC secret / Server-side HMAC secret of at least 32 bytes.
        @param lifetime cursor 有效期 / Cursor lifetime.
        @param clock 可注入的 UTC 时钟 / Injectable UTC clock.
        @raise ValueError secret、有效期或时钟不安全时抛出 / Raised for an unsafe secret, lifetime, or clock.
        """

        if len(secret) < 32:
            raise ValueError("cursor HMAC secret must contain at least 32 bytes")
        if lifetime <= timedelta(0):
            raise ValueError("cursor lifetime must be positive")
        self._secret = bytes(secret)
        self._lifetime = lifetime
        self._clock = clock or (lambda: datetime.now(UTC))

    def encode(
        self,
        position: JsonValue,
        *,
        principal: TokenPrincipal | None,
        workspace_id: WorkspaceId | None,
        filters: Mapping[str, JsonValue],
        sort: Sequence[str],
    ) -> str:
        """@brief 签发绑定完整查询上下文的 cursor / Issue a cursor bound to the complete query context.

        @param position 稳定排序中的续页位置 / Continuation position in the stable ordering.
        @param principal OAuth 主体；公开集合为空 / OAuth principal, absent for public collections.
        @param workspace_id 路径 Workspace，可为空 / Path Workspace, if any.
        @param filters 影响结果集的全部 filter / Every filter affecting the result set.
        @param sort 含确定性 tie-breaker 的稳定排序 / Stable ordering including a deterministic tie-breaker.
        @return URL-safe 签名 cursor / URL-safe signed cursor.
        """

        now = self._now()
        context = _cursor_context(principal, workspace_id, filters, sort)
        payload: dict[str, JsonValue] = {
            "v": _CURSOR_VERSION,
            "exp": int((now + self._lifetime).timestamp()),
            "ctx": hashlib.sha256(canonical_json_bytes(context)).hexdigest(),
            "pos": position,
        }
        serialized = canonical_json_bytes(payload)
        signature = hmac.digest(self._secret, serialized, "sha256")
        cursor = f"{_base64url_encode(serialized)}.{_base64url_encode(signature)}"
        if len(cursor) > _MAX_CURSOR_LENGTH:
            raise ValueError("cursor position exceeds the transport limit")
        return cursor

    def decode(
        self,
        cursor: str,
        *,
        principal: TokenPrincipal | None,
        workspace_id: WorkspaceId | None,
        filters: Mapping[str, JsonValue],
        sort: Sequence[str],
    ) -> JsonValue:
        """@brief 验证 cursor 完整性、上下文与过期时间 / Verify cursor integrity, context, and expiry.

        @param cursor 不可信的客户端 cursor / Untrusted client cursor.
        @param principal 当前 OAuth 主体；公开集合为空 / Current OAuth principal, absent for public collections.
        @param workspace_id 当前路径 Workspace，可为空 / Current path Workspace, if any.
        @param filters 当前全部 filter / Current complete filter set.
        @param sort 当前稳定排序 / Current stable ordering.
        @return 已认证的续页位置 / Authenticated continuation position.
        @raise DomainError cursor 损坏、跨上下文重放或过期时抛出 / Raised when tampered, replayed across context, or expired.
        """

        try:
            if not cursor or len(cursor) > _MAX_CURSOR_LENGTH:
                raise ValueError("cursor length is invalid")
            encoded_payload, separator, encoded_signature = cursor.partition(".")
            if separator != "." or "." in encoded_signature:
                raise ValueError("cursor shape is invalid")
            serialized = _base64url_decode(encoded_payload)
            signature = _base64url_decode(encoded_signature)
            expected_signature = hmac.digest(self._secret, serialized, "sha256")
            if len(signature) != len(expected_signature) or not hmac.compare_digest(
                signature, expected_signature
            ):
                raise ValueError("cursor signature is invalid")
            decoded = _strict_json_loads(serialized.decode("utf-8", errors="strict"))
            if not isinstance(decoded, dict) or set(decoded) != {"v", "exp", "ctx", "pos"}:
                raise ValueError("cursor payload is invalid")
            version = decoded["v"]
            expires_at = decoded["exp"]
            context_digest = decoded["ctx"]
            if (
                isinstance(version, bool)
                or not isinstance(version, int)
                or version != _CURSOR_VERSION
            ):
                raise ValueError("cursor version is invalid")
            if isinstance(expires_at, bool) or not isinstance(expires_at, int):
                raise ValueError("cursor expiry is invalid")
            if not isinstance(context_digest, str) or len(context_digest) != 64:
                raise ValueError("cursor context is invalid")
            expected_context = _cursor_context(principal, workspace_id, filters, sort)
            expected_context_digest = hashlib.sha256(
                canonical_json_bytes(expected_context)
            ).hexdigest()
            if not hmac.compare_digest(context_digest, expected_context_digest):
                raise ValueError("cursor context does not match")
            if expires_at <= int(self._now().timestamp()):
                raise ValueError("cursor has expired")
            return decoded["pos"]
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
            raise DomainError(
                Problem("http.cursor_invalid", 400, "Pagination cursor is invalid")
            ) from error

    def _now(self) -> datetime:
        """@brief 读取并验证 UTC-aware 时钟 / Read and validate the timezone-aware clock.

        @return 带时区的当前时间 / Timezone-aware current time.
        @raise ValueError 时钟返回 naive datetime 时抛出 / Raised when the clock returns a naive datetime.
        """

        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("cursor clock must return a timezone-aware datetime")
        return now


def resource_response_headers(
    representation: JsonValue,
    *,
    request_id: str,
    location: str | None = None,
) -> dict[str, str]:
    """@brief 构建 V2 单资源响应头 / Build V2 single-resource response headers.

    @param representation 实际返回的 JSON 表示 / JSON representation actually returned.
    @param request_id 服务端关联 ID / Server correlation ID.
    @param location 创建或异步接受资源的 canonical URI / Canonical URI for a created or accepted resource.
    @return 含强 ETag、请求 ID 与可选 Location 的新字典 / New mapping with strong ETag, request ID, and optional Location.
    """

    if not request_id:
        raise ValueError("request_id must not be empty")
    headers = {"ETag": strong_etag(representation), "X-Request-Id": request_id}
    if location is not None:
        if not location:
            raise ValueError("location must not be empty")
        headers["Location"] = location
    return headers


def list_response(items: Sequence[JsonValue], *, next_cursor: str | None) -> dict[str, JsonValue]:
    """@brief 构建满足 V2 page 不变量的集合表示 / Build a collection representation satisfying V2 page invariants.

    @param items 当前页资源 / Resources in the current page.
    @param next_cursor 下一页 cursor；没有更多结果时为空 / Next cursor, absent when no more results exist.
    @return `{items,page}` 集合表示 / `{items,page}` collection representation.
    @raise ValueError 空字符串 cursor 会破坏 has_more 不变量时抛出 / Raised when an empty cursor would violate the has_more invariant.
    """

    if next_cursor == "":
        raise ValueError("next_cursor must be non-empty or None")
    return {
        "items": list(items),
        "page": {"next_cursor": next_cursor, "has_more": next_cursor is not None},
    }


def _cursor_context(
    principal: TokenPrincipal | None,
    workspace_id: WorkspaceId | None,
    filters: Mapping[str, JsonValue],
    sort: Sequence[str],
) -> dict[str, JsonValue]:
    """@brief 规范化 cursor 查询上下文 / Normalize a cursor query context.

    @param principal OAuth 主体；公开集合为空 / OAuth principal, absent for public collections.
    @param workspace_id 路径 Workspace / Path Workspace.
    @param filters 完整 filter / Complete filters.
    @param sort 稳定排序 / Stable ordering.
    @return 可 canonicalize 的上下文 / Canonicalizable context.
    """

    sort_strings = list(sort)
    if not sort_strings or any(not value for value in sort_strings):
        raise ValueError("cursor sort must be non-empty and deterministic")
    sort_values = [cast(JsonValue, value) for value in sort_strings]
    principal_value: JsonValue
    if principal is None:
        principal_value = None
    else:
        scope_values = [
            cast(JsonValue, value)
            for value in sorted(str(scope) for scope in principal.scopes)
        ]
        principal_value = {
            "user_id": str(principal.user_id),
            "subject": str(principal.subject),
            "client_id": str(principal.client_id),
            "scopes": scope_values,
        }
    return {
        "principal": principal_value,
        "workspace_id": str(workspace_id) if workspace_id is not None else None,
        "filters": dict(filters),
        "sort": sort_values,
    }


def _strict_json_loads(serialized: str) -> JsonValue:
    """@brief 拒绝重复键和非有限数的严格 JSON 解析 / Strictly parse JSON while rejecting duplicate keys and non-finite numbers.

    @param serialized UTF-8 解码后的 JSON / UTF-8-decoded JSON.
    @return 严格 JSON 值 / Strict JSON value.
    """

    decoded: object = json.loads(
        serialized,
        object_pairs_hook=_unique_object,
        parse_constant=_reject_json_constant,
    )
    return _require_json_value(decoded)


def _unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    """@brief 将无重复 member 的 pairs 构造成对象 / Build an object from member pairs without duplicates.

    @param pairs JSON object member pairs / JSON object member pairs.
    @return 唯一键对象 / Object with unique keys.
    @raise ValueError 发现重复键时抛出 / Raised when a duplicate key is found.
    """

    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object member")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> Never:
    """@brief 拒绝 NaN 与 Infinity 扩展 / Reject NaN and Infinity extensions.

    @param value 非标准数字 token / Non-standard numeric token.
    @return 永不返回 / Never returns.
    @raise ValueError 总是抛出 / Always raised.
    """

    raise ValueError(f"non-standard JSON constant: {value}")


def _require_json_value(value: object) -> JsonValue:
    """@brief 将无类型解析结果收窄为递归 JSON 类型 / Narrow an untyped parse result to the recursive JSON type.

    @param value JSON parser 的值 / Value produced by the JSON parser.
    @return 类型安全 JSON 值 / Type-safe JSON value.
    @raise ValueError 值不属于 RFC 8259 数据模型时抛出 / Raised when outside the RFC 8259 data model.
    """

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON number must be finite")
        return value
    if isinstance(value, list):
        return [_require_json_value(item) for item in cast(list[object], value)]
    if isinstance(value, dict):
        untyped = cast(dict[object, object], value)
        if any(not isinstance(key, str) for key in untyped):
            raise ValueError("JSON object key must be a string")
        return {cast(str, key): _require_json_value(item) for key, item in untyped.items()}
    raise ValueError("decoded value is not JSON")


def _enforce_raw_json_depth(serialized: str, *, max_depth: int) -> None:
    """@brief 在解析前扫描 JSON 容器深度 / Scan JSON container depth before parsing.

    @param serialized UTF-8 解码后的未解析 JSON / UTF-8-decoded, unparsed JSON.
    @param max_depth 最大 `{`/`[` 嵌套层数 / Maximum `{`/`[` nesting level.
    @return 无返回值 / No return value.
    @raise DomainError 深度超限时以 413 抛出 / Raised with 413 when the depth limit is exceeded.
    """

    depth = 0
    in_string = False
    escaped = False
    for character in serialized:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > max_depth:
                raise _payload_too_large("JSON nesting exceeds the route depth limit")
        elif character in "]}" and depth > 0:
            depth -= 1


def _base64url_encode(value: bytes) -> str:
    """@brief 无 padding 编码 Base64url / Encode unpadded Base64url.

    @param value 原始字节 / Raw bytes.
    @return URL-safe ASCII / URL-safe ASCII.
    """

    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    """@brief 严格解码无 padding Base64url / Strictly decode unpadded Base64url.

    @param value 不可信 ASCII token / Untrusted ASCII token.
    @return 解码字节 / Decoded bytes.
    @raise ValueError 字母表、padding 或 canonical form 不合法时抛出 / Raised for invalid alphabet, padding, or non-canonical form.
    """

    if not value or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for character in value
    ):
        raise ValueError("invalid base64url")
    decoded = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    if _base64url_encode(decoded) != value:
        raise ValueError("non-canonical base64url")
    return decoded


def _invalid_token_claims() -> DomainError:
    """@brief 构造不泄漏 claim 细节的 token 错误 / Build a token error without leaking claim details.

    @return OAuth 401 领域错误 / OAuth 401 domain error.
    """

    return DomainError(Problem("oauth.invalid_token", 401, "Access token claims are invalid"))


def _invalid_json() -> DomainError:
    """@brief 构造统一严格 JSON 错误 / Build the uniform strict-JSON error.

    @return HTTP 400 领域错误 / HTTP 400 domain error.
    """

    return DomainError(Problem("http.invalid_json", 400, "Request body is not valid JSON"))


def _payload_too_large(detail: str) -> DomainError:
    """@brief 构造反序列化前的 payload 限制错误 / Build a pre-deserialization payload-limit error.

    @param detail 安全的限制说明 / Safe limit description.
    @return HTTP 413 领域错误 / HTTP 413 domain error.
    """

    return DomainError(
        Problem("http.payload_too_large", 413, "Request payload is too large", detail)
    )


__all__ = [
    "JSON_MEDIA_TYPE",
    "MERGE_PATCH_MEDIA_TYPE",
    "ContractDefinitionValidator",
    "CursorCodec",
    "JsonValue",
    "canonical_json_bytes",
    "decode_contract_json",
    "list_response",
    "require_strong_if_match",
    "resource_response_headers",
    "strong_etag",
    "token_principal_from_claims",
    "validate_idempotency_key",
]
