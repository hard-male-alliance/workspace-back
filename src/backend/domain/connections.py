"""@brief API v2 外部 Connection 领域模型 / API v2 external-connection domain models.

公开 Connection 与授权 session 只携带契约允许的安全投影。API token、OAuth state、
provider device code 和服务端 credential reference 留在专用私有值或持久记录中，不能
通过普通 ``repr``、事件或公开投影泄漏。
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import StrEnum
from typing import NewType
from urllib.parse import urlsplit

from backend.domain.platform import ProblemDetails
from backend.domain.principals import (
    DomainInvariantError,
    ResourceMeta,
    UserId,
    WorkspaceId,
)

ConnectionId = NewType("ConnectionId", str)
"""@brief Connection 不透明标识 / Opaque Connection identifier."""

ConnectionAuthorizationSessionId = NewType("ConnectionAuthorizationSessionId", str)
"""@brief Connection 授权 session 标识 / Connection-authorization session identifier."""

CredentialReference = NewType("CredentialReference", str)
"""@brief 服务端 credential vault 引用 / Server-side credential-vault reference."""

ProviderSessionReference = NewType("ProviderSessionReference", str)
"""@brief provider 私有授权事务引用 / Private provider-authorization transaction reference."""

_PROVIDER_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief Connection provider 稳定名称语法 / Stable Connection-provider name grammar."""

_OPAQUE_ID_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API v2 不透明标识语法 / API v2 opaque-identifier grammar."""

_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
"""@brief 小写 SHA-256 语法 / Lower-case SHA-256 grammar."""


class ConnectionDomainError(DomainInvariantError):
    """@brief Connection 领域不变量错误 / Connection-domain invariant error."""


class ConnectionTransitionError(ConnectionDomainError):
    """@brief Connection 或授权 session 拒绝非法状态迁移 / Reject an invalid state transition."""


class ConnectionAuthMethod(StrEnum):
    """@brief 契约冻结的 Connection 认证方式 / Contract-frozen Connection auth methods."""

    OAUTH = "oauth"
    DEVICE_CODE = "device_code"
    API_TOKEN = "api_token"


class ConnectionStatus(StrEnum):
    """@brief 契约冻结的 Connection 状态 / Contract-frozen Connection states."""

    ACTIVE = "active"
    REAUTHORIZATION_REQUIRED = "reauthorization_required"
    REVOKING = "revoking"
    REVOKED = "revoked"
    FAILED = "failed"


class ConnectionAuthorizationFlow(StrEnum):
    """@brief provider 授权交互方式 / Provider-authorization interaction modes."""

    BROWSER_REDIRECT = "browser_redirect"
    DEVICE_CODE = "device_code"


class ConnectionAuthorizationState(StrEnum):
    """@brief 服务端授权 session 生命周期 / Server-side authorization-session lifecycle."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"

    @property
    def is_terminal(self) -> bool:
        """@brief 判断授权 session 是否终态 / Test whether an authorization session is terminal.

        @return completed、failed 或 expired 时为真 / True for completed, failed, or expired.
        """
        return self is not ConnectionAuthorizationState.PENDING


@dataclass(frozen=True, slots=True)
class ConnectionProvider:
    """@brief 经语法校验的开放 provider 标识 / Validated open provider identifier.

    @param value 契约允许的稳定 provider 名 / Contract-compatible stable provider name.
    """

    value: str

    def __post_init__(self) -> None:
        """@brief 校验 provider 名 / Validate the provider name.

        @raise ConnectionDomainError provider 不满足契约语法时抛出 / Raised for invalid syntax.
        """
        if _PROVIDER_PATTERN.fullmatch(self.value) is None:
            raise ConnectionDomainError("connection provider does not satisfy the API v2 grammar")

    def __str__(self) -> str:
        """@brief 返回 provider 的 wire value / Return the provider wire value.

        @return provider 字符串 / Provider string.
        """
        return self.value


class SecretValue:
    """@brief 默认脱敏的短生命周期 secret 输入 / Redacted-by-default short-lived secret input.

    @param value 只允许传给专用 secret adapter 的原始值 / Raw value passed only to a secret adapter.
    @note 该值不能进入通用日志、遥测、outbox 或领域持久对象 / This value must not enter generic
        logs, telemetry, outbox records, or persisted domain objects.
    """

    __slots__ = ("__value",)

    def __init__(self, value: str) -> None:
        """@brief 包装并隐藏 secret / Wrap and hide a secret.

        @param value 非空且最长 8192 字符的 secret / Non-empty secret of at most 8192 characters.
        @raise ConnectionDomainError secret 长度非法时抛出 / Raised for an invalid secret length.
        """
        if not 1 <= len(value) <= 8192:
            raise ConnectionDomainError("secret must contain between one and 8192 characters")
        self.__value = value

    def __repr__(self) -> str:
        """@brief 返回固定脱敏表示 / Return a fixed redacted representation.

        @return 不含 secret 的字符串 / String containing no secret material.
        """
        return "SecretValue(<redacted>)"

    def __str__(self) -> str:
        """@brief 阻止隐式字符串化泄漏 / Prevent disclosure through implicit stringification.

        @return 固定脱敏文本 / Fixed redacted text.
        """
        return "<redacted>"

    @property
    def length(self) -> int:
        """@brief 返回不暴露内容的字符数 / Return the character count without exposing content.

        @return secret 字符数 / Secret character count.
        """
        return len(self.__value)

    def reveal_to_secret_adapter(self) -> str:
        """@brief 仅向专用 secret adapter 交付原始值 / Reveal only to a dedicated secret adapter.

        @return 原始 secret / Raw secret.
        @note 调用方必须立即清除引用且不得记录返回值 / The caller must drop the reference
            immediately and must never log the returned value.
        """
        return self.__value

    def keyed_fingerprint(self, key: bytes, *, context: bytes) -> str:
        """@brief 计算不暴露 secret 的有上下文指纹 / Compute a context-bound secret fingerprint.

        @param key 服务端独立 HMAC key / Independent server-side HMAC key.
        @param context 防跨用途复用的非空上下文 / Non-empty context preventing cross-use reuse.
        @return 小写 SHA-256 HMAC 十六进制 / Lower-case SHA-256 HMAC hexadecimal.
        @note 该指纹可用于 secret-aware 幂等比较；不能使用裸 SHA-256 猜测低熵 token。
            / This fingerprint supports secret-aware comparison; raw SHA-256 is forbidden for
            potentially low-entropy tokens.
        """
        if len(key) < 32 or not context:
            raise ConnectionDomainError("secret fingerprinting requires a strong key and context")
        import hmac

        return hmac.new(key, context + b"\x00" + self.__value.encode(), hashlib.sha256).hexdigest()


@dataclass(frozen=True, slots=True)
class ConnectionOwnership:
    """@brief Connection 的 Workspace 与创建者所有权 / Workspace and creator ownership.

    @param workspace_id 唯一租户边界 / Sole tenant boundary.
    @param created_by 创建 Connection 的本地用户 / Local user who created the Connection.
    """

    workspace_id: WorkspaceId
    created_by: UserId

    def __post_init__(self) -> None:
        """@brief 校验所有权标识 / Validate ownership identifiers.

        @raise ConnectionDomainError 标识非法时抛出 / Raised for invalid identifiers.
        """
        _require_opaque_id(self.workspace_id, "connection workspace id")
        _require_opaque_id(self.created_by, "connection creator id")


@dataclass(frozen=True, slots=True)
class Connection:
    """@brief 不含 credential 的公开 Connection 投影 / Public credential-free Connection projection.

    @param meta 通用资源元数据 / Common resource metadata.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param provider 外部 provider / External provider.
    @param auth_method 认证方式 / Authentication method.
    @param display_name 用户可见名称 / User-visible name.
    @param status 生命周期状态 / Lifecycle state.
    @param scopes provider 授予的去重 scopes / Unique provider-granted scopes.
    @param last_validated_at 最近成功验证时刻 / Most recent successful validation instant.
    @param problem 可公开结构化问题 / Public-safe structured problem.
    """

    meta: ResourceMeta[ConnectionId]
    workspace_id: WorkspaceId
    provider: ConnectionProvider
    auth_method: ConnectionAuthMethod
    display_name: str
    status: ConnectionStatus
    scopes: tuple[str, ...] = ()
    last_validated_at: datetime | None = None
    problem: ProblemDetails | None = None

    def __post_init__(self) -> None:
        """@brief 校验公开 Connection 投影 / Validate the public Connection projection.

        @raise ConnectionDomainError 字段或状态关联非法时抛出 / Raised for invalid fields or state.
        """
        _require_opaque_id(self.meta.id, "connection id")
        _require_opaque_id(self.workspace_id, "connection workspace id")
        _require_canonical_text(self.display_name, "connection display name", 1, 200)
        _require_scopes(self.scopes)
        if self.last_validated_at is not None:
            _require_aware(self.last_validated_at, "connection last_validated_at")
        if self.status is ConnectionStatus.ACTIVE and self.problem is not None:
            raise ConnectionDomainError("active connection cannot carry a problem")
        if self.status is ConnectionStatus.FAILED and self.problem is None:
            raise ConnectionDomainError("failed connection requires a public-safe problem")


@dataclass(frozen=True, slots=True)
class ConnectionAggregate:
    """@brief 带私有 server reference 的 Connection 聚合 / Connection aggregate with private server reference.

    @param connection 公开安全投影 / Public-safe projection.
    @param ownership Workspace 与创建者所有权 / Workspace and creator ownership.
    @param credential_reference 服务端 credential vault 引用 / Server-side credential-vault reference.
    """

    connection: Connection
    ownership: ConnectionOwnership
    credential_reference: CredentialReference = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验公开投影与私有记录绑定 / Validate public/private record binding.

        @raise ConnectionDomainError Workspace 或 reference 不一致时抛出 / Raised for invalid binding.
        """
        if self.connection.workspace_id != self.ownership.workspace_id:
            raise ConnectionDomainError("connection ownership must match its public workspace")
        _require_opaque_id(self.credential_reference, "connection credential reference")

    def request_revocation(self, *, at: datetime) -> ConnectionAggregate:
        """@brief 将可用 Connection 迁移为 revoking / Move an available Connection to revoking.

        @param at 状态修改时刻 / State-change instant.
        @return 下一 revision 聚合 / Next-revision aggregate.
        @raise ConnectionTransitionError 当前状态不可撤销时抛出 / Raised from an invalid state.
        """
        if self.connection.status not in {
            ConnectionStatus.ACTIVE,
            ConnectionStatus.REAUTHORIZATION_REQUIRED,
            ConnectionStatus.FAILED,
        }:
            raise ConnectionTransitionError(
                "connection cannot enter revoking from its current state"
            )
        return replace(
            self,
            connection=replace(
                self.connection,
                meta=self.connection.meta.advance(at),
                status=ConnectionStatus.REVOKING,
                problem=None,
            ),
        )

    def mark_revoked(self, *, at: datetime) -> ConnectionAggregate:
        """@brief 完成 credential 撤销 / Complete credential revocation.

        @param at 完成时刻 / Completion instant.
        @return revoked 的下一 revision / Next revision in revoked state.
        @raise ConnectionTransitionError 当前不是 revoking 时抛出 / Raised unless revoking.
        """
        if self.connection.status is not ConnectionStatus.REVOKING:
            raise ConnectionTransitionError("only a revoking connection can become revoked")
        return replace(
            self,
            connection=replace(
                self.connection,
                meta=self.connection.meta.advance(at),
                status=ConnectionStatus.REVOKED,
                problem=None,
            ),
        )

    def require_reauthorization(
        self,
        *,
        at: datetime,
        problem: ProblemDetails | None = None,
    ) -> ConnectionAggregate:
        """@brief 标记 credential 需要重新授权 / Mark credentials as requiring reauthorization.

        @param at 检测时刻 / Detection instant.
        @param problem 可公开的 provider 问题 / Optional public-safe provider problem.
        @return 下一 revision 聚合 / Next-revision aggregate.
        """
        if self.connection.status in {ConnectionStatus.REVOKING, ConnectionStatus.REVOKED}:
            raise ConnectionTransitionError(
                "revoking or revoked connection cannot require reauthorization"
            )
        return replace(
            self,
            connection=replace(
                self.connection,
                meta=self.connection.meta.advance(at),
                status=ConnectionStatus.REAUTHORIZATION_REQUIRED,
                problem=problem,
            ),
        )

    def validated(self, scopes: tuple[str, ...], *, at: datetime) -> ConnectionAggregate:
        """@brief 记录 credential 成功验证并恢复 active / Record successful validation and activate.

        @param scopes provider 当前授予 scopes / Current provider-granted scopes.
        @param at 验证时刻 / Validation instant.
        @return active 的下一 revision 聚合 / Next-revision active aggregate.
        """
        if self.connection.status in {ConnectionStatus.REVOKING, ConnectionStatus.REVOKED}:
            raise ConnectionTransitionError("revoking or revoked connection cannot be validated")
        _require_scopes(scopes)
        return replace(
            self,
            connection=replace(
                self.connection,
                meta=self.connection.meta.advance(at),
                status=ConnectionStatus.ACTIVE,
                scopes=scopes,
                last_validated_at=at,
                problem=None,
            ),
        )


@dataclass(frozen=True, slots=True)
class ConnectionAuthorizationSession:
    """@brief 可返回客户端但默认隐藏交互凭据的授权 session / Client projection with redacted interaction credentials.

    @param id session 标识 / Session identifier.
    @param provider provider 标识 / Provider identifier.
    @param flow 授权 flow / Authorization flow.
    @param authorization_url browser redirect 地址 / Browser redirect URL.
    @param verification_uri device flow 验证地址 / Device-flow verification URI.
    @param user_code 用户输入 code；普通 repr 隐藏 / User-entered code, hidden from normal repr.
    @param expires_at provider 授权过期时刻 / Provider-authorization expiration instant.
    @param poll_interval_ms device polling 下限 / Device polling lower bound.
    """

    id: ConnectionAuthorizationSessionId
    provider: ConnectionProvider
    flow: ConnectionAuthorizationFlow
    expires_at: datetime
    authorization_url: str | None = field(default=None, repr=False)
    verification_uri: str | None = None
    user_code: str | None = field(default=None, repr=False)
    poll_interval_ms: int | None = None

    def __post_init__(self) -> None:
        """@brief 校验 flow 判别联合 / Validate the flow-discriminated union.

        @raise ConnectionDomainError URL、code、poll 或时间非法时抛出 / Raised for invalid fields.
        """
        _require_opaque_id(self.id, "connection authorization session id")
        _require_aware(self.expires_at, "connection authorization expires_at")
        if self.flow is ConnectionAuthorizationFlow.BROWSER_REDIRECT:
            _require_https_url(self.authorization_url, "connection authorization URL")
            if any(
                value is not None
                for value in (self.verification_uri, self.user_code, self.poll_interval_ms)
            ):
                raise ConnectionDomainError("browser authorization cannot carry device-flow fields")
            return
        if self.authorization_url is not None:
            raise ConnectionDomainError("device authorization cannot carry an authorization URL")
        _require_https_url(self.verification_uri, "connection verification URI")
        _require_canonical_text(self.user_code, "connection user code", 1, 100)
        if self.poll_interval_ms is None or not 1_000 <= self.poll_interval_ms <= 120_000:
            raise ConnectionDomainError(
                "device polling interval must be between 1000 and 120000 ms"
            )


@dataclass(frozen=True, slots=True)
class ConnectionAuthorizationIdempotency:
    """@brief 授权 session 的专用加密重放索引 / Dedicated encrypted-replay index for authorization sessions.

    @param key_hash 服务端 keyed hash，不保存原始 Idempotency-Key / Server-keyed hash; the raw
        Idempotency-Key is never persisted.
    @param request_fingerprint 不含 secret 的规范请求指纹 / Canonical secret-free request fingerprint.
    @param expires_at 专用重放记录保留边界 / Dedicated replay-record retention boundary.

    @note 这里只保存索引元数据；authorization URL、device user code 由 infrastructure 的
       专用 AEAD（Authenticated Encryption with Associated Data）保护，不进入通用 receipt。
    """

    key_hash: str = field(repr=False)
    request_fingerprint: str = field(repr=False)
    expires_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验摘要与保留时间 / Validate digests and retention timestamp.

        @raise ConnectionDomainError 摘要或时间非法时抛出 / Raised for an invalid digest or
            timestamp.
        """

        if _SHA256_PATTERN.fullmatch(self.key_hash) is None:
            raise ConnectionDomainError("authorization idempotency key hash must be SHA-256")
        if _SHA256_PATTERN.fullmatch(self.request_fingerprint) is None:
            raise ConnectionDomainError("authorization request fingerprint must be SHA-256")
        _require_aware(self.expires_at, "authorization idempotency expiry")


@dataclass(frozen=True, slots=True)
class ConnectionAuthorizationRecord:
    """@brief 含 state 摘要与 provider 引用的私有授权记录 / Private authorization record with state digest.

    @param session 可安全返回的 session 投影 / Client-safe session projection.
    @param ownership Workspace 与创建者所有权 / Workspace and creator ownership.
    @param requested_scopes 去重的请求 scopes / Unique requested scopes.
    @param state 生命周期状态 / Lifecycle state.
    @param state_sha256 随机 OAuth state 的 SHA-256，不保存原值 / SHA-256 of random OAuth state.
    @param provider_session_reference provider 端私有事务引用 / Private provider transaction reference.
    @param idempotency 专用、加密重放的索引元数据 / Dedicated encrypted-replay metadata.
    @param created_at 创建时刻 / Creation instant.
    @param connection_id 完成后产生的 Connection / Connection produced on completion.
    @param problem 失败时的公开问题 / Public-safe problem on failure.
    """

    session: ConnectionAuthorizationSession
    ownership: ConnectionOwnership
    requested_scopes: tuple[str, ...]
    state: ConnectionAuthorizationState
    state_sha256: str = field(repr=False)
    provider_session_reference: ProviderSessionReference = field(repr=False)
    idempotency: ConnectionAuthorizationIdempotency
    created_at: datetime
    connection_id: ConnectionId | None = None
    problem: ProblemDetails | None = None

    def __post_init__(self) -> None:
        """@brief 校验私有授权记录及终态关联 / Validate private record and terminal associations.

        @raise ConnectionDomainError 记录不一致时抛出 / Raised for an inconsistent record.
        """
        _require_scopes(self.requested_scopes)
        _require_aware(self.created_at, "connection authorization created_at")
        if self.idempotency.expires_at < self.created_at + timedelta(hours=24):
            raise ConnectionDomainError(
                "authorization idempotency replay must be retained for at least 24 hours"
            )
        if self.session.expires_at <= self.created_at:
            raise ConnectionDomainError("connection authorization expiry must follow creation")
        if _SHA256_PATTERN.fullmatch(self.state_sha256) is None:
            raise ConnectionDomainError("connection authorization state digest must be SHA-256")
        _require_opaque_id(
            self.provider_session_reference,
            "connection provider session reference",
        )
        if self.state is ConnectionAuthorizationState.COMPLETED:
            if self.connection_id is None or self.problem is not None:
                raise ConnectionDomainError("completed authorization requires only a connection id")
        elif self.state is ConnectionAuthorizationState.FAILED:
            if self.problem is None or self.connection_id is not None:
                raise ConnectionDomainError("failed authorization requires only a problem")
        elif self.connection_id is not None or self.problem is not None:
            raise ConnectionDomainError(
                "pending or expired authorization cannot carry outcome fields"
            )

    def matches_state(self, candidate: SecretValue) -> bool:
        """@brief 常量时间比较 OAuth state 摘要 / Constant-time compare an OAuth state digest.

        @param candidate callback 提交的原始 state / Raw state submitted by the callback.
        @return 摘要匹配时为真 / True when the digest matches.
        """
        import hmac

        digest = hashlib.sha256(candidate.reveal_to_secret_adapter().encode()).hexdigest()
        return hmac.compare_digest(digest, self.state_sha256)

    def complete(
        self,
        connection_id: ConnectionId,
        *,
        at: datetime,
    ) -> ConnectionAuthorizationRecord:
        """@brief 一次性完成授权 session / Complete an authorization session once.

        @param connection_id 原子创建的 Connection / Atomically created Connection.
        @param at 完成时刻 / Completion instant.
        @return completed 记录 / Completed record.
        @raise ConnectionTransitionError session 非 pending 或已过期时抛出 / Raised unless pending and live.
        """
        self._require_pending(at)
        _require_opaque_id(connection_id, "authorized connection id")
        return replace(
            self,
            state=ConnectionAuthorizationState.COMPLETED,
            connection_id=connection_id,
        )

    def fail(
        self,
        problem: ProblemDetails,
        *,
        at: datetime,
    ) -> ConnectionAuthorizationRecord:
        """@brief 以公开安全 problem 终止授权 / Fail authorization with a public-safe problem.

        @param problem 结构化失败 / Structured failure.
        @param at 失败时刻 / Failure instant.
        @return failed 记录 / Failed record.
        """
        self._require_pending(at)
        return replace(self, state=ConnectionAuthorizationState.FAILED, problem=problem)

    def expire(self, *, at: datetime) -> ConnectionAuthorizationRecord:
        """@brief 在到期后标记 session expired / Mark a session expired after its deadline.

        @param at 判定时刻 / Evaluation instant.
        @return expired 记录 / Expired record.
        @raise ConnectionTransitionError session 非 pending 或尚未到期时抛出 / Raised unless due.
        """
        if self.state is not ConnectionAuthorizationState.PENDING:
            raise ConnectionTransitionError("only a pending authorization can expire")
        _require_aware(at, "connection authorization expiration instant")
        if at < self.session.expires_at:
            raise ConnectionTransitionError("authorization cannot expire before its deadline")
        return replace(self, state=ConnectionAuthorizationState.EXPIRED)

    def _require_pending(self, at: datetime) -> None:
        """@brief 要求 session 仍 pending 且未到期 / Require a still-live pending session.

        @param at 请求处理时刻 / Request-processing instant.
        @raise ConnectionTransitionError session 不可继续时抛出 / Raised when processing cannot continue.
        """
        _require_aware(at, "connection authorization transition instant")
        if self.state is not ConnectionAuthorizationState.PENDING:
            raise ConnectionTransitionError(
                "authorization session has already reached a terminal state"
            )
        if at >= self.session.expires_at:
            raise ConnectionTransitionError("authorization session has expired")


def authorization_state_sha256(state: SecretValue) -> str:
    """@brief 计算随机 OAuth state 的持久摘要 / Compute the persisted digest of random OAuth state.

    @param state 高熵、单 session state / High-entropy per-session state.
    @return 小写 SHA-256 / Lower-case SHA-256.
    @note state 必须由密码学随机生成器产生；摘要不能替代高熵要求 / State must be generated
        cryptographically; hashing does not compensate for low entropy.
    """
    return hashlib.sha256(state.reveal_to_secret_adapter().encode()).hexdigest()


def _require_scopes(scopes: tuple[str, ...]) -> None:
    """@brief 校验有界唯一 provider scopes / Validate bounded unique provider scopes.

    @param scopes 待校验 scopes / Scopes to validate.
    @raise ConnectionDomainError scopes 非规范、重复或超限时抛出 / Raised for invalid scopes.
    """
    if len(scopes) > 100 or len(set(scopes)) != len(scopes):
        raise ConnectionDomainError("connection scopes must be unique and at most 100 entries")
    if any(not scope or len(scope) > 200 or scope.strip() != scope for scope in scopes):
        raise ConnectionDomainError(
            "connection scopes must be canonical and at most 200 characters"
        )


def _require_canonical_text(
    value: str | None,
    label: str,
    minimum: int,
    maximum: int,
) -> None:
    """@brief 校验有界无外围空白文本 / Validate bounded text without surrounding whitespace.

    @param value 待校验值 / Value to validate.
    @param label 错误标签 / Error label.
    @param minimum 最小长度 / Minimum length.
    @param maximum 最大长度 / Maximum length.
    @raise ConnectionDomainError 文本非法时抛出 / Raised for invalid text.
    """
    if value is None or not minimum <= len(value) <= maximum or value.strip() != value:
        raise ConnectionDomainError(
            f"{label} must be canonical and {minimum} to {maximum} characters"
        )


def _require_opaque_id(value: str, label: str) -> None:
    """@brief 校验 API v2 不透明标识 / Validate an API v2 opaque identifier.

    @param value 标识 / Identifier.
    @param label 错误标签 / Error label.
    @raise ConnectionDomainError 标识非法时抛出 / Raised for an invalid identifier.
    """
    if _OPAQUE_ID_PATTERN.fullmatch(value) is None:
        raise ConnectionDomainError(f"{label} does not satisfy the API v2 grammar")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 校验带时区时间 / Validate a timezone-aware datetime.

    @param value 时间 / Datetime.
    @param label 错误标签 / Error label.
    @raise ConnectionDomainError 时间 naive 时抛出 / Raised for a naive datetime.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ConnectionDomainError(f"{label} must be timezone-aware")


def _require_https_url(value: str | None, label: str) -> None:
    """@brief 校验无 userinfo 的 HTTPS URL / Validate an HTTPS URL without userinfo.

    @param value URL / URL.
    @param label 错误标签 / Error label.
    @raise ConnectionDomainError URL 非法时抛出 / Raised for an invalid URL.
    """
    if value is None:
        raise ConnectionDomainError(f"{label} is required")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ConnectionDomainError(f"{label} must be an HTTPS URL without userinfo or fragment")


__all__ = [
    "Connection",
    "ConnectionAggregate",
    "ConnectionAuthMethod",
    "ConnectionAuthorizationFlow",
    "ConnectionAuthorizationIdempotency",
    "ConnectionAuthorizationRecord",
    "ConnectionAuthorizationSession",
    "ConnectionAuthorizationSessionId",
    "ConnectionAuthorizationState",
    "ConnectionDomainError",
    "ConnectionId",
    "ConnectionOwnership",
    "ConnectionProvider",
    "ConnectionStatus",
    "ConnectionTransitionError",
    "CredentialReference",
    "ProviderSessionReference",
    "SecretValue",
    "authorization_state_sha256",
]
