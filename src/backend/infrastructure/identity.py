"""@brief 受可信代理保护的身份断言解析 / Trusted-proxy-protected identity assertion resolution.

@note 该模块不依赖 FastAPI，因此 HTTP 与 WebSocket 调用方必须传入 ASGI 保留的原始
request target（``raw_path`` 与 ``query_string``），而不是经过路由器解码或重写的 URL。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import math
import re
import time
from collections.abc import Callable, Iterable, Mapping
from ipaddress import IPv4Network, IPv6Network, ip_address
from typing import Final, Protocol

from backend.config import SecuritySettings
from workspace_shared.jsonc import ConfigurationError
from workspace_shared.tenancy import ActorScope

IDENTITY_SIGNATURE_VERSION: Final[str] = "v1"
"""@brief 当前可信代理身份签名版本 / Current trusted-proxy identity signature version."""


def peer_is_trusted_proxy(
    peer_host: str | None,
    trusted_proxy_cidrs: Iterable[IPv4Network | IPv6Network],
) -> bool:
    """@brief 判断 ASGI 对端是否属于可信入口代理 / Check whether the ASGI peer belongs to a trusted ingress proxy.

    @param peer_host ASGI ``client.host`` 提供的实际 TCP 对端 IP / Actual TCP peer IP from ASGI ``client.host``.
    @param trusted_proxy_cidrs 后端配置并已解析的私有 proxy CIDR / Configured parsed private proxy CIDRs.
    @return IP 被任一 CIDR 覆盖时为 ``True``；缺失、主机名或非法 IP 一律为 ``False``。

    @note 该函数刻意不接受 ``X-Forwarded-For`` 或 DNS 名称。HMAC（Hash-based Message
    Authentication Code）防伪造声明，CIDR allowlist 则防止网络层可达 backend 的任意对端
    借泄漏密钥或错误路由直接使用该声明；二者缺一不可。
    """
    if not peer_host:
        return False
    try:
        peer = ip_address(peer_host)
    except ValueError:
        return False
    return any(peer in network for network in trusted_proxy_cidrs)

HEADER_IDENTITY_VERSION: Final[str] = "X-AIWS-Identity-Version"
"""@brief 身份签名版本请求头 / Identity signature version header."""

HEADER_ACTOR_ID: Final[str] = "X-AIWS-Actor-Id"
"""@brief 经代理断言的 actor 标识请求头 / Proxy-asserted actor identifier header."""

HEADER_WORKSPACE_ID: Final[str] = "X-AIWS-Workspace-Id"
"""@brief 经代理断言的 workspace 标识请求头 / Proxy-asserted workspace identifier header."""

HEADER_RESOURCE_OWNER_ID: Final[str] = "X-AIWS-Resource-Owner-Id"
"""@brief 经代理断言的资源所有者标识请求头 / Proxy-asserted resource-owner identifier header."""

HEADER_AUTH_TIMESTAMP: Final[str] = "X-AIWS-Auth-Timestamp"
"""@brief Unix 秒级签发时间请求头 / Unix-seconds assertion timestamp header."""

HEADER_IDENTITY_SIGNATURE: Final[str] = "X-AIWS-Identity-Signature"
"""@brief URL-safe Base64 HMAC-SHA-256 签名请求头 / URL-safe Base64 HMAC-SHA-256 signature header."""

MOCK_HEADER_ACTOR_ID: Final[str] = "X-Mock-Actor-Id"
"""@brief 仅 development/test 可用的 mock actor 请求头 / Development/test-only mock actor header."""

MOCK_HEADER_WORKSPACE_ID: Final[str] = "X-Mock-Workspace-Id"
"""@brief 仅 development/test 可用的 mock workspace 请求头 / Development/test-only mock workspace header."""

MOCK_HEADER_RESOURCE_OWNER_ID: Final[str] = "X-Mock-Resource-Owner-Id"
"""@brief 仅 development/test 可用的 mock owner 请求头 / Development/test-only mock owner header."""

_CANONICALIZATION_LABEL: Final[str] = "AIWS-TRUSTED-PROXY-HMAC-V1"
"""@brief 签名原文的域隔离标签 / Domain-separation label for signed payloads."""

_DEVELOPMENT_IDENTITY_ENVIRONMENTS: Final[frozenset[str]] = frozenset({"development", "test"})
"""@brief 允许 mock resolver 的部署环境 / Deployment environments allowed to use the mock resolver."""

_IDENTIFIER_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
"""@brief 稳定身份 ID 格式 / Stable identity-ID format."""

_TIMESTAMP_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?:0|[1-9][0-9]{0,15})")
"""@brief 无前导零的 Unix 秒格式 / Unix-seconds format without leading zeroes."""

_METHOD_PATTERN: Final[re.Pattern[str]] = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Z]+")
"""@brief RFC 9110 HTTP token 方法格式 / RFC 9110 HTTP-token method format."""

_SIGNATURE_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9_-]{43}")
"""@brief 无填充 SHA-256 URL-safe Base64 签名格式 / Unpadded SHA-256 URL-safe Base64 signature format."""

_MIN_HMAC_SECRET_BYTES: Final[int] = 32
"""@brief HMAC 密钥最小字节长度 / Minimum HMAC secret length in bytes."""

_MAX_CLOCK_SKEW_SECONDS: Final[int] = 600
"""@brief 支持的最大时钟偏差 / Maximum supported clock skew in seconds."""

_MAX_REQUEST_TARGET_BYTES: Final[int] = 4096
"""@brief 可参与签名的最大原始请求目标长度 / Maximum signed raw request-target length."""

IdentityHeaders = Mapping[str, str]
"""@brief HTTP/WS 身份请求头的通用类型 / Generic HTTP/WS identity-header type."""

Clock = Callable[[], float]
"""@brief 返回 Unix 秒的可注入时钟 / Injectable clock returning Unix seconds."""


class IdentityVerificationError(PermissionError):
    """@brief 安全但可分类的身份验证失败 / Safe, classifiable identity-verification failure.

    @param code 稳定、无敏感信息的公开错误码 / Stable public error code without sensitive data.
    @note 此异常文本永远不包含密钥、原始签名或未验证 header 值。
    """

    def __init__(self, code: str) -> None:
        """@brief 创建身份失败 / Create an identity failure.

        @param code 稳定错误码 / Stable error code.
        """
        super().__init__(code)
        self.code = code


class IdentityResolver(Protocol):
    """@brief HTTP 和 WebSocket 共用的身份解析协议 / Shared HTTP and WebSocket identity-resolution protocol."""

    def resolve(
        self,
        *,
        method: str,
        path: str | bytes,
        headers: IdentityHeaders,
        query_string: str | bytes = b"",
    ) -> ActorScope:
        """@brief 从传输元数据解析多租户范围 / Resolve a tenant scope from transport metadata.

        @param method HTTP 或 WebSocket upgrade 方法 / HTTP or WebSocket-upgrade method.
        @param path 未解码的 raw path / Undecoded raw path.
        @param headers 传输层身份头 / Transport identity headers.
        @param query_string 未解码 query string（不带 ``?``）/ Undecoded query string without ``?``.
        @return 已验证的 ActorScope。
        @raise IdentityVerificationError 身份断言不完整、过期或无效时抛出 /
            Raised when the assertion is incomplete, expired, or invalid.
        """


class DevelopmentMockIdentityResolver:
    """@brief 仅 development/test 使用的确定性 mock 身份解析器 / Development/test-only deterministic mock resolver.

    @note 这是迁移适配器，不是认证方案。构造时即拒绝 staging/production，以防路由层
    误接线后把用户可控 header 带入生产租户范围。
    """

    def __init__(self, default_scope: ActorScope, *, environment: str) -> None:
        """@brief 初始化 mock resolver / Initialize the mock resolver.

        @param default_scope 未提供 mock header 时的本地默认范围 / Local default scope without mock headers.
        @param environment 当前部署环境 / Current deployment environment.
        @raise ConfigurationError 环境不允许 mock 身份时抛出 / Raised when the environment disallows mock identity.
        """
        if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS:
            raise ConfigurationError("development_mock identity is only allowed in development/test")
        self._default_scope = default_scope

    def resolve(
        self,
        *,
        method: str,
        path: str | bytes,
        headers: IdentityHeaders,
        query_string: str | bytes = b"",
    ) -> ActorScope:
        """@brief 解析 mock 范围 / Resolve a mock scope.

        @param method 未参与 mock 身份判断的请求方法 / Request method unused by mock identity.
        @param path 未参与 mock 身份判断的请求路径 / Request path unused by mock identity.
        @param headers 可选的本地 mock 头 / Optional local mock headers.
        @param query_string 未参与 mock 身份判断的 query string / Query string unused by mock identity.
        @return 经基本 ID 格式校验的 ActorScope。
        @raise IdentityVerificationError mock 头重复或 ID 非法时抛出 /
            Raised for duplicate mock headers or invalid IDs.
        """
        del method, path, query_string
        normalized_headers = _normalize_headers(headers)
        return _scope_from_claims(
            normalized_headers.get(MOCK_HEADER_ACTOR_ID.lower(), self._default_scope.actor_id),
            normalized_headers.get(MOCK_HEADER_WORKSPACE_ID.lower(), self._default_scope.workspace_id),
            normalized_headers.get(
                MOCK_HEADER_RESOURCE_OWNER_ID.lower(), self._default_scope.resource_owner_id
            ),
        )


class TrustedProxyHMACIdentityResolver:
    """@brief 验证可信代理 HMAC 断言 / Verify trusted-proxy HMAC assertions.

    ``X-AIWS-Identity-Signature`` 是下列 UTF-8、LF 分隔字段的 HMAC-SHA-256，再编码为
    无 ``=`` 填充的 URL-safe Base64：

    ``AIWS-TRUSTED-PROXY-HMAC-V1\nMETHOD\nREQUEST_TARGET\nACTOR\nWORKSPACE\nOWNER\nTIMESTAMP``。

    @note REQUEST_TARGET 是保留百分号编码的原始 path，若有 query 则紧跟 ``?query``；
    调用方不得传入路由器解码后的 URL。入口代理必须先删除来自公网的所有
    ``X-AIWS-*`` 身份头，再以仅代理持有的密钥重新签名。
    """

    def __init__(
        self,
        secret: str | bytes,
        max_clock_skew_seconds: int,
        *,
        clock: Clock | None = None,
    ) -> None:
        """@brief 初始化 HMAC resolver / Initialize the HMAC resolver.

        @param secret 仅进程内使用的 HMAC 密钥 / HMAC secret used only in process memory.
        @param max_clock_skew_seconds 可接受的双向时钟偏差 / Accepted bidirectional clock skew.
        @param clock 可注入的 Unix 秒时钟 / Injectable Unix-seconds clock.
        @raise ConfigurationError 密钥或偏差配置不安全时抛出 / Raised for an unsafe secret or skew setting.
        """
        try:
            self._secret = _secret_bytes(secret)
        except ValueError as error:
            raise ConfigurationError("trusted-proxy HMAC secret must contain at least 32 bytes") from error
        if (
            isinstance(max_clock_skew_seconds, bool)
            or not isinstance(max_clock_skew_seconds, int)
            or max_clock_skew_seconds < 1
            or max_clock_skew_seconds > _MAX_CLOCK_SKEW_SECONDS
        ):
            raise ConfigurationError(
                f"trusted-proxy HMAC clock skew must be an integer in [1, {_MAX_CLOCK_SKEW_SECONDS}]"
            )
        self._max_clock_skew_seconds = max_clock_skew_seconds
        self._clock = clock if clock is not None else time.time

    def resolve(
        self,
        *,
        method: str,
        path: str | bytes,
        headers: IdentityHeaders,
        query_string: str | bytes = b"",
    ) -> ActorScope:
        """@brief 验证并解析可信代理范围 / Verify and resolve a trusted-proxy scope.

        @param method HTTP 或 WebSocket upgrade 方法 / HTTP or WebSocket-upgrade method.
        @param path ASGI ``raw_path`` 或同等未解码 path / ASGI ``raw_path`` or equivalent undecoded path.
        @param headers 含代理断言的请求头 / Headers containing the proxy assertion.
        @param query_string ASGI ``query_string`` 或同等未解码 query / ASGI ``query_string`` or equivalent undecoded query.
        @return 经 HMAC、时效和字段格式校验的 ActorScope。
        @raise IdentityVerificationError 头缺失、签名伪造或断言过期时抛出 /
            Raised for missing headers, a forged signature, or an expired assertion.
        """
        normalized_headers = _normalize_headers(headers)
        version = _require_header(normalized_headers, HEADER_IDENTITY_VERSION)
        if version != IDENTITY_SIGNATURE_VERSION:
            raise IdentityVerificationError("identity.version_unsupported")
        actor_id = _require_header(normalized_headers, HEADER_ACTOR_ID)
        workspace_id = _require_header(normalized_headers, HEADER_WORKSPACE_ID)
        resource_owner_id = _require_header(normalized_headers, HEADER_RESOURCE_OWNER_ID)
        timestamp_text = _require_header(normalized_headers, HEADER_AUTH_TIMESTAMP)
        signature = _require_header(normalized_headers, HEADER_IDENTITY_SIGNATURE)
        timestamp = _parse_timestamp(timestamp_text)
        canonical = canonicalize_trusted_proxy_assertion(
            method=method,
            path=path,
            query_string=query_string,
            actor_id=actor_id,
            workspace_id=workspace_id,
            resource_owner_id=resource_owner_id,
            timestamp=timestamp_text,
        )
        expected_signature = hmac.new(self._secret, canonical, hashlib.sha256).digest()
        supplied_signature = _decode_signature(signature)
        if not hmac.compare_digest(expected_signature, supplied_signature):
            raise IdentityVerificationError("identity.signature_invalid")
        current_time = self._clock()
        if not math.isfinite(current_time) or abs(current_time - timestamp) > self._max_clock_skew_seconds:
            raise IdentityVerificationError("identity.timestamp_out_of_window")
        return _scope_from_claims(actor_id, workspace_id, resource_owner_id)


def build_identity_resolver(
    *,
    environment: str,
    default_scope: ActorScope,
    security: SecuritySettings,
) -> IdentityResolver:
    """@brief 从后端配置构造唯一身份入口 / Build the single identity boundary from backend settings.

    @param environment 部署环境标签 / Deployment environment label.
    @param default_scope development/test mock 的默认范围 / Default scope for development/test mock mode.
    @param security 已解析的身份安全设置 / Parsed identity security settings.
    @return 可供 HTTP 和 WebSocket 共用的 IdentityResolver。
    @raise ConfigurationError HMAC 密钥缺失或 mock 被用于非开发环境时抛出 /
        Raised when the HMAC secret is absent or mock identity is used outside development.
    """
    if security.identity_mode == "development_mock":
        return DevelopmentMockIdentityResolver(default_scope, environment=environment)
    if security.identity_mode == "trusted_proxy_hmac":
        secret = security.trusted_proxy_hmac_secret
        if secret is None:
            raise ConfigurationError("trusted-proxy HMAC secret is not configured in config.jsonc")
        return TrustedProxyHMACIdentityResolver(
            secret,
            security.trusted_proxy_max_clock_skew_seconds,
        )
    raise ConfigurationError("unsupported identity mode")


def canonicalize_trusted_proxy_assertion(
    *,
    method: str,
    path: str | bytes,
    query_string: str | bytes = b"",
    actor_id: str,
    workspace_id: str,
    resource_owner_id: str,
    timestamp: str | int,
) -> bytes:
    """@brief 生成稳定的 HMAC 签名原文 / Build the stable HMAC canonical payload.

    @param method 请求方法；会规范为大写 / Request method; normalized to uppercase.
    @param path 保留百分号编码的原始 path / Raw percent-encoded path.
    @param query_string 不带 ``?`` 的原始 query / Raw query without ``?``.
    @param actor_id 经代理断言的 actor ID / Proxy-asserted actor ID.
    @param workspace_id 经代理断言的 workspace ID / Proxy-asserted workspace ID.
    @param resource_owner_id 经代理断言的 owner ID / Proxy-asserted owner ID.
    @param timestamp 无前导零的 Unix 秒 / Unix seconds without leading zeroes.
    @return UTF-8 编码的 canonical bytes。
    @raise IdentityVerificationError 任何字段含歧义、控制符或超限时抛出 /
        Raised when a field is ambiguous, contains controls, or exceeds limits.

    @example
    ``POST /api/v1/resumes?dry_run=false`` 的第一行后续字段依次为原始 target、
    actor、workspace、owner 与签发秒数；每一个字段由单个 LF 分隔。
    """
    normalized_method = _normalize_method(method)
    request_target = canonical_request_target(path, query_string)
    normalized_actor_id = _validate_identifier(actor_id)
    normalized_workspace_id = _validate_identifier(workspace_id)
    normalized_resource_owner_id = _validate_identifier(resource_owner_id)
    timestamp_text, _ = _normalize_timestamp(timestamp)
    return "\n".join(
        (
            _CANONICALIZATION_LABEL,
            normalized_method,
            request_target,
            normalized_actor_id,
            normalized_workspace_id,
            normalized_resource_owner_id,
            timestamp_text,
        )
    ).encode("utf-8")


def canonical_request_target(path: str | bytes, query_string: str | bytes = b"") -> str:
    """@brief 规范化待签名的原始 request target / Canonicalize the raw request target to be signed.

    @param path 不含 query 的 ASCII raw path / ASCII raw path without query.
    @param query_string 不带 ``?`` 的 ASCII raw query / ASCII raw query without ``?``.
    @return ``/path`` 或 ``/path?query`` 形式的稳定 request target。
    @raise IdentityVerificationError path/query 已解码、含控制符或不符合长度限制时抛出 /
        Raised when path/query is decoded, contains controls, or exceeds length limits.
    """
    path_text = _raw_ascii(path, "path")
    query_text = _raw_ascii(query_string, "query_string")
    if not path_text.startswith("/") or "?" in path_text or "#" in path_text:
        raise IdentityVerificationError("identity.request_target_invalid")
    if query_text.startswith("?") or "#" in query_text:
        raise IdentityVerificationError("identity.request_target_invalid")
    request_target = path_text if not query_text else f"{path_text}?{query_text}"
    if len(request_target.encode("ascii")) > _MAX_REQUEST_TARGET_BYTES:
        raise IdentityVerificationError("identity.request_target_invalid")
    return request_target


def sign_trusted_proxy_assertion(
    secret: str | bytes,
    *,
    method: str,
    path: str | bytes,
    query_string: str | bytes = b"",
    actor_id: str,
    workspace_id: str,
    resource_owner_id: str,
    timestamp: str | int,
) -> str:
    """@brief 为可信入口代理生成测试/集成签名 / Generate a test/integration signature for a trusted ingress proxy.

    @param secret HMAC 密钥 / HMAC secret.
    @param method 请求方法 / Request method.
    @param path 原始请求路径 / Raw request path.
    @param query_string 原始 query string / Raw query string.
    @param actor_id actor ID / Actor ID.
    @param workspace_id workspace ID / Workspace ID.
    @param resource_owner_id 资源 owner ID / Resource-owner ID.
    @param timestamp Unix 秒 / Unix seconds.
    @return 无填充 URL-safe Base64 HMAC-SHA-256 签名。
    @raise ValueError 密钥过短时抛出 / Raised when the secret is too short.

    @note 该帮助器服务于入口代理集成测试；后端请求路径绝不应调用它或暴露密钥。
    """
    canonical = canonicalize_trusted_proxy_assertion(
        method=method,
        path=path,
        query_string=query_string,
        actor_id=actor_id,
        workspace_id=workspace_id,
        resource_owner_id=resource_owner_id,
        timestamp=timestamp,
    )
    digest = hmac.new(_secret_bytes(secret), canonical, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _normalize_headers(headers: IdentityHeaders) -> dict[str, str]:
    """@brief 将 header 名规范为小写并拒绝重复值 / Normalize header names to lowercase and reject duplicates.

    @param headers 通用 Mapping 形式的 HTTP/WS 头 / Generic Mapping-form HTTP/WS headers.
    @return 小写 header 名到原始值的映射 / Lowercase header names mapped to raw values.
    @raise IdentityVerificationError header 名、值或重复性非法时抛出 /
        Raised for invalid names, values, or duplicates.
    """
    normalized: dict[str, str] = {}
    for name, value in headers.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise IdentityVerificationError("identity.header_invalid")
        normalized_name = name.lower()
        if normalized_name in normalized:
            raise IdentityVerificationError("identity.header_ambiguous")
        normalized[normalized_name] = value
    return normalized


def _require_header(headers: Mapping[str, str], name: str) -> str:
    """@brief 读取唯一且非空的签名 header / Read a unique, non-empty signed header.

    @param headers 已规范化的 header 映射 / Normalized header mapping.
    @param name 固定 header 名 / Fixed header name.
    @return 不做 trim 的原始 header 值 / Untrimmed raw header value.
    @raise IdentityVerificationError header 缺失或为空时抛出 / Raised when the header is missing or empty.
    """
    value = headers.get(name.lower())
    if value is None or not value:
        raise IdentityVerificationError("identity.header_missing")
    return value


def _scope_from_claims(actor_id: str, workspace_id: str, resource_owner_id: str) -> ActorScope:
    """@brief 将已读取的声明变成范围 / Turn read claims into a scope.

    @param actor_id actor 声明 / Actor claim.
    @param workspace_id workspace 声明 / Workspace claim.
    @param resource_owner_id owner 声明 / Owner claim.
    @return 格式受限的 ActorScope。
    @raise IdentityVerificationError 声明 ID 非法时抛出 / Raised when a claim ID is invalid.
    """
    return ActorScope(
        _validate_identifier(actor_id),
        _validate_identifier(workspace_id),
        _validate_identifier(resource_owner_id),
    )


def _validate_identifier(value: str) -> str:
    """@brief 验证身份声明 ID / Validate an identity-claim ID.

    @param value 未验证 ID / Unverified ID.
    @return 原样返回的稳定 ID / Unchanged stable ID.
    @raise IdentityVerificationError ID 含空白、控制符或非稳定字符时抛出 /
        Raised when an ID contains whitespace, controls, or unstable characters.
    """
    if not isinstance(value, str) or _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise IdentityVerificationError("identity.claim_invalid")
    return value


def _normalize_method(value: str) -> str:
    """@brief 验证并大写化 HTTP 方法 / Validate and uppercase an HTTP method.

    @param value 原始方法 / Raw method.
    @return 规范大写方法 / Canonical uppercase method.
    @raise IdentityVerificationError 方法不是 RFC token 时抛出 / Raised when the method is not an RFC token.
    """
    if not isinstance(value, str):
        raise IdentityVerificationError("identity.method_invalid")
    normalized = value.upper()
    if _METHOD_PATTERN.fullmatch(normalized) is None:
        raise IdentityVerificationError("identity.method_invalid")
    return normalized


def _raw_ascii(value: str | bytes, name: str) -> str:
    """@brief 读取无控制符的 raw ASCII 字段 / Read a control-free raw ASCII field.

    @param value 原始 bytes 或 ASCII 字符串 / Raw bytes or ASCII string.
    @param name 仅用于选择安全错误码的字段名 / Field name used only to select a safe error code.
    @return 原始 ASCII 字符串 / Raw ASCII text.
    @raise IdentityVerificationError 不是 raw ASCII 或含 CR/LF 时抛出 /
        Raised when data is not raw ASCII or contains CR/LF.
    """
    del name
    try:
        text = value.decode("ascii") if isinstance(value, bytes) else value.encode("ascii").decode("ascii")
    except (AttributeError, UnicodeError) as error:
        raise IdentityVerificationError("identity.request_target_invalid") from error
    if "\r" in text or "\n" in text:
        raise IdentityVerificationError("identity.request_target_invalid")
    return text


def _normalize_timestamp(value: str | int) -> tuple[str, int]:
    """@brief 验证 Unix 秒字符串 / Validate a Unix-seconds value.

    @param value 字符串或整数形式的 Unix 秒 / Unix seconds as a string or integer.
    @return ``(canonical_text, integer_seconds)``。
    @raise IdentityVerificationError 时间格式不稳定时抛出 / Raised when the timestamp format is unstable.
    """
    if isinstance(value, bool):
        raise IdentityVerificationError("identity.timestamp_invalid")
    if isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        text = value
    else:
        raise IdentityVerificationError("identity.timestamp_invalid")
    if _TIMESTAMP_PATTERN.fullmatch(text) is None:
        raise IdentityVerificationError("identity.timestamp_invalid")
    return text, int(text)


def _parse_timestamp(value: str) -> int:
    """@brief 从签名 header 读取 Unix 秒 / Parse Unix seconds from a signed header.

    @param value 签名 header 中的时间字符串 / Timestamp string from the signed header.
    @return 整数 Unix 秒 / Integer Unix seconds.
    @raise IdentityVerificationError 时间格式不合法时抛出 / Raised for an invalid timestamp format.
    """
    _, timestamp = _normalize_timestamp(value)
    return timestamp


def _decode_signature(value: str) -> bytes:
    """@brief 严格解码 HMAC 签名 / Strictly decode an HMAC signature.

    @param value 无填充 URL-safe Base64 签名 / Unpadded URL-safe Base64 signature.
    @return 32 字节 SHA-256 digest。
    @raise IdentityVerificationError 签名编码或长度非法时抛出 / Raised for invalid signature encoding or length.
    """
    if _SIGNATURE_PATTERN.fullmatch(value) is None:
        raise IdentityVerificationError("identity.signature_invalid")
    try:
        decoded = base64.urlsafe_b64decode(f"{value}=")
    except (ValueError, UnicodeError) as error:
        raise IdentityVerificationError("identity.signature_invalid") from error
    if len(decoded) != hashlib.sha256().digest_size:
        raise IdentityVerificationError("identity.signature_invalid")
    return decoded


def _secret_bytes(value: str | bytes) -> bytes:
    """@brief 读取最小长度的 HMAC 密钥 / Read an HMAC secret with a minimum length.

    @param value 进程内密钥文本或 bytes / In-process secret text or bytes.
    @return UTF-8 编码后的密钥 bytes / UTF-8 encoded secret bytes.
    @raise ValueError 密钥类型错误或不足 32 bytes 时抛出 / Raised when secret type is invalid or under 32 bytes.
    """
    if isinstance(value, bytes):
        secret = value
    elif isinstance(value, str):
        secret = value.encode("utf-8")
    else:
        raise ValueError("invalid secret type")
    if len(secret) < _MIN_HMAC_SECRET_BYTES:
        raise ValueError("secret is too short")
    return secret
