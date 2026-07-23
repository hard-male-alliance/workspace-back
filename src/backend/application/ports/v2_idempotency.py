"""@brief API V2 幂等执行端口 / API V2 idempotent-execution ports.

该模块只描述应用层可观察的幂等语义。HTTP 层负责先校验 ``Idempotency-Key``
语法并提供规范请求字节；持久化 adapter 负责跨请求保存 claim 与完整响应快照。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import NewType, Protocol

from backend.domain.common import DomainError, Problem
from backend.domain.principals import UserId, WorkspaceId


class IdempotencyStatus(StrEnum):
    """@brief 幂等记录状态 / Idempotency-record states."""

    PENDING = "pending"
    COMPLETED = "completed"


class IdempotencyDecisionKind(StrEnum):
    """@brief claim 操作的互斥结果 / Mutually exclusive claim outcomes."""

    CLAIMED = "claimed"
    REPLAY = "replay"
    IN_PROGRESS = "in_progress"


_REPLAYABLE_HEADER_NAMES = frozenset(
    {"cache-control", "content-location", "content-type", "etag", "location", "retry-after", "vary"}
)
"""@brief 可安全持久化并重放的响应头 / Response headers safe to persist and replay."""


@dataclass(frozen=True, slots=True)
class IdempotencyScope:
    """@brief 一个 API V2 幂等 key 的完整作用域 / Full scope of one API V2 idempotency key.

    @param user_id 服务端签名 token 中的本地用户标识 / Local user ID from a server-signed token.
    @param workspace_id 路径 Workspace；非 Workspace 命令为 ``None`` / Path Workspace, or
        ``None`` for a non-Workspace command.
    @param method 大写 HTTP method / Upper-case HTTP method.
    @param canonical_path 不含 query/fragment 的规范路由路径 / Canonical route path without
        query or fragment.
    @param key 已由 HTTP boundary 校验的原始 Idempotency-Key / Original Idempotency-Key already
        validated by the HTTP boundary.
    """

    user_id: UserId
    workspace_id: WorkspaceId | None
    method: str
    canonical_path: str
    key: str

    def __post_init__(self) -> None:
        """@brief 防止不完整 scope 进入持久层 / Keep incomplete scopes out of persistence.

        @raise ValueError 标识、method、路径或 key 不是规范值时抛出 / Raised for a
            non-canonical identifier, method, path, or key.

        @note key 的长度和字符集由 HTTP boundary 校验；这里仅拒绝空值，避免 adapter
            意外绕过 boundary 后把所有请求折叠到同一个 key。
        """
        if not self.user_id or self.user_id.strip() != self.user_id:
            raise ValueError("idempotency user_id must be a non-empty canonical value")
        if self.workspace_id is not None and (
            not self.workspace_id or self.workspace_id.strip() != self.workspace_id
        ):
            raise ValueError("idempotency workspace_id must be a non-empty canonical value")
        if (
            not self.method
            or not self.method.isascii()
            or self.method != self.method.upper()
            or not self.method.isalpha()
        ):
            raise ValueError("idempotency method must be upper-case ASCII letters")
        if (
            not self.canonical_path.startswith("/")
            or "?" in self.canonical_path
            or "#" in self.canonical_path
        ):
            raise ValueError("idempotency canonical_path must be an absolute path without query")
        if not self.key:
            raise ValueError("idempotency key must not be empty")


@dataclass(frozen=True, slots=True)
class IdempotencyRequest:
    """@brief 用于指纹计算的规范请求 / Canonical request used for fingerprinting.

    @param scope principal、Workspace、method、path 与 key 的完整 scope / Complete scope.
    @param canonical_body HTTP 层产生的规范 JSON 请求字节 / Canonical JSON request bytes from
        the HTTP layer.
    @param content_type 影响解析语义的规范 Content-Type / Canonical Content-Type affecting
        parsing semantics.
    @param if_match 影响并发语义的原始强 If-Match / Original strong If-Match affecting
        concurrency semantics.
    """

    scope: IdempotencyScope
    canonical_body: bytes
    content_type: str | None
    if_match: str | None

    @property
    def fingerprint(self) -> str:
        """@brief 返回无歧义 SHA-256 请求指纹 / Return an unambiguous SHA-256 fingerprint.

        @return 64 个小写十六进制字符 / Sixty-four lower-case hexadecimal characters.
        """
        return request_fingerprint(
            self.canonical_body,
            content_type=self.content_type,
            if_match=self.if_match,
        )


@dataclass(frozen=True, slots=True)
class ReplayableResponse:
    """@brief 可逐字重放的 JSON HTTP 响应 / Byte-exact replayable JSON HTTP response.

    @param status_code 原始 HTTP status / Original HTTP status.
    @param headers 有序的关键响应头；名称和值保持原样 / Ordered critical response headers,
        preserving original names and values.
    @param json_body 原始 JSON body 字节，不重新序列化 / Original JSON body bytes, never
        re-serialized during replay.

    @note ``X-Request-Id`` 不在允许集合中；重放请求必须使用当前请求的追踪 ID，不能
        泄漏首次请求 ID。认证头与 ``Set-Cookie`` 同样不能进入 receipt。
    """

    status_code: int
    headers: tuple[tuple[str, str], ...]
    json_body: bytes

    def __post_init__(self) -> None:
        """@brief 校验可安全重放的响应快照 / Validate a safely replayable response snapshot.

        @raise ValueError status、header 或 JSON body 无效时抛出 / Raised for an invalid
            status, header, or JSON body.
        """
        if isinstance(self.status_code, bool) or not 100 <= self.status_code <= 599:
            raise ValueError("replay status_code must be an HTTP status")
        seen: set[str] = set()
        for name, value in self.headers:
            normalized_name = name.lower()
            if normalized_name not in _REPLAYABLE_HEADER_NAMES:
                raise ValueError(f"response header is not replayable: {name}")
            if normalized_name in seen:
                raise ValueError(f"duplicate replay response header: {name}")
            if not name or any(character in name for character in "\r\n:"):
                raise ValueError("response header name is invalid")
            if "\r" in value or "\n" in value:
                raise ValueError("response header value is invalid")
            seen.add(normalized_name)
        try:
            json.loads(self.json_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("replay body must contain valid UTF-8 JSON") from error


@dataclass(frozen=True, slots=True)
class IdempotencyClaim:
    """@brief 仅首次请求持有的不透明 claim / Opaque claim held only by the first request.

    @param scope 被 claim 的完整 scope / Claimed scope.
    @param fingerprint 被 claim 的请求指纹 / Claimed request fingerprint.
    @param token 高熵 adapter 私有所有权令牌 / High-entropy adapter-private ownership token.
    """

    scope: IdempotencyScope
    fingerprint: str
    token: str


@dataclass(frozen=True, slots=True)
class IdempotencyDecision:
    """@brief claim 的 tagged-union 风格结果 / Tagged-union-style result of a claim.

    @param kind claimed、replay 或 in-progress / Claimed, replay, or in-progress.
    @param claim 仅 ``claimed`` 时存在 / Present only for ``claimed``.
    @param replay 仅 ``replay`` 时存在 / Present only for ``replay``.
    """

    kind: IdempotencyDecisionKind
    claim: IdempotencyClaim | None = None
    replay: ReplayableResponse | None = None

    def __post_init__(self) -> None:
        """@brief 校验 tagged-union 形状 / Validate the tagged-union shape.

        @raise ValueError payload 与 discriminator 不一致时抛出 / Raised when payloads do not
            match the discriminator.
        """
        expected_claim = self.kind is IdempotencyDecisionKind.CLAIMED
        expected_replay = self.kind is IdempotencyDecisionKind.REPLAY
        if (self.claim is not None) is not expected_claim:
            raise ValueError("only a claimed decision carries a claim")
        if (self.replay is not None) is not expected_replay:
            raise ValueError("only a replay decision carries a response")


class IdempotencyConflict(DomainError):
    """@brief API V2 幂等冲突 / API V2 idempotency conflict.

    @param code 契约冻结的冲突 code / Contract-frozen conflict code.
    @param retry_after_seconds ``in_progress`` 建议的 Retry-After 秒数 / Suggested Retry-After
        seconds for ``in_progress``.
    """

    retry_after_seconds: int | None

    def __init__(self, code: str, *, retry_after_seconds: int | None = None) -> None:
        """@brief 构造 409 ProblemDetails 来源 / Construct a 409 ProblemDetails source.

        @param code 必须为 ``idempotency.key_reused`` 或 ``idempotency.in_progress``。
        @param retry_after_seconds 可选正整数重试秒数 / Optional positive retry delay.
        @raise ValueError code 或重试秒数不合法时抛出 / Raised for an invalid code or delay.
        """
        if code not in {"idempotency.key_reused", "idempotency.in_progress"}:
            raise ValueError("unsupported idempotency conflict code")
        if retry_after_seconds is not None and retry_after_seconds < 1:
            raise ValueError("Retry-After must be a positive number of seconds")
        title = (
            "Idempotency key was reused with different input"
            if code == "idempotency.key_reused"
            else "An identical request is still being processed"
        )
        super().__init__(Problem(code, 409, title, retryable=code == "idempotency.in_progress"))
        self.retry_after_seconds = retry_after_seconds


class V2IdempotencyStore(Protocol):
    """@brief 持久化 claim 与响应 receipt 的应用端口 / Application port for claims and receipts."""

    async def claim(
        self,
        request: IdempotencyRequest,
        *,
        now: datetime,
        expires_at: datetime,
    ) -> IdempotencyDecision:
        """@brief 原子 claim、检查冲突或读取 replay / Atomically claim, conflict, or replay.

        @param request 包含完整 scope 与指纹输入的请求 / Fully scoped request.
        @param now 带时区当前时刻 / Timezone-aware current instant.
        @param expires_at 完成 receipt 的最早保留边界 / Earliest retention boundary.
        @return 三种互斥 decision 之一 / One of three mutually exclusive decisions.
        @raise IdempotencyConflict 同 scope/key 的指纹不同时抛出 / Raised for a fingerprint
            mismatch under the same scope/key.
        """

    async def complete(
        self,
        claim: IdempotencyClaim,
        response: ReplayableResponse,
        *,
        completed_at: datetime,
        expires_at: datetime,
    ) -> ReplayableResponse | None:
        """@brief 把自己的 pending claim 完成为 receipt / Complete an owned pending claim.

        @param claim 首次请求获得的不透明 claim / Opaque claim issued to the first request.
        @param response 要逐字保存的响应 / Response to persist byte-for-byte.
        @param completed_at 带时区完成时刻 / Timezone-aware completion instant.
        @param expires_at 从完成时刻计算的保留边界 / Retention boundary from completion.
        @return 已保存或已完成的 replay；所有权丢失时返回 ``None`` / Stored or already
            completed replay, or ``None`` after ownership loss.
        """


type IdempotentOperation = Callable[[], Awaitable[ReplayableResponse]]
"""@brief 首次 claim 后执行的异步业务 callback / Async business callback after first claim."""

IdempotencyPreparationId = NewType("IdempotencyPreparationId", str)
"""@brief 跨重试稳定且不泄漏原始 key 的外部准备标识 / Stable, key-redacted external preparation identifier."""

type IdempotentPrepare[PreparedT] = Callable[
    [IdempotencyPreparationId],
    Awaitable[PreparedT],
]
"""@brief 在数据库事务外执行的可幂等准备 callback / Idempotent preparation callback run outside database transactions."""

type IdempotentCommit[PreparedT] = Callable[[PreparedT], Awaitable[ReplayableResponse]]
"""@brief 只含数据库工作的最终提交 callback / Final commit callback containing database work only."""


class V2IdempotencyExecutor(Protocol):
    """@brief API 层使用的幂等 callback executor / Idempotent callback executor used by the API."""

    async def execute(
        self,
        request: IdempotencyRequest,
        operation: IdempotentOperation,
    ) -> ReplayableResponse:
        """@brief 执行首次请求或返回 receipt / Execute the first request or return its receipt.

        @param request 规范请求 / Canonical request.
        @param operation 仅首次 claim 执行的业务 callback / Business callback run only for the
            first claim.
        @return 首次响应或逐字 replay / First response or byte-exact replay.
        """


class V2PreparedIdempotencyExecutor(V2IdempotencyExecutor, Protocol):
    """@brief 支持事务外 I/O 与事务内提交分相的幂等 executor / Idempotency executor separating external I/O from commit.

    @note ``prepare`` 必须以 ``preparation_id`` 向外部系统提供稳定幂等键；``commit`` 不得
        发起网络 I/O。生产 PostgreSQL adapter 在两阶段之间只持有 session advisory lock，
        不持有数据库事务、行锁或 MVCC snapshot / ``prepare`` must use ``preparation_id`` as
        the stable external idempotency key, while ``commit`` must not perform network I/O. The
        production PostgreSQL adapter holds only a session advisory lock between phases.
    """

    async def execute_prepared[PreparedT](
        self,
        request: IdempotencyRequest,
        prepare: IdempotentPrepare[PreparedT],
        commit: IdempotentCommit[PreparedT],
    ) -> ReplayableResponse:
        """@brief 事务外准备后原子提交业务与 receipt / Prepare outside a transaction, then atomically commit business state and receipt.

        @param request 规范请求 / Canonical request.
        @param prepare 跨重试幂等的外部准备 / Retry-idempotent external preparation.
        @param commit 只含数据库工作的提交 / Database-only commit.
        @return 首次响应或逐字 replay / First response or byte-exact replay.
        """


def request_fingerprint(
    canonical_body: bytes,
    *,
    content_type: str | None,
    if_match: str | None,
) -> str:
    """@brief 计算长度分帧的请求指纹 / Compute a length-framed request fingerprint.

    @param canonical_body HTTP boundary 提供的规范 body 字节 / Canonical body bytes supplied by
        the HTTP boundary.
    @param content_type 规范 Content-Type 或 ``None`` / Canonical Content-Type or ``None``.
    @param if_match 原始强 If-Match 或 ``None`` / Original strong If-Match or ``None``.
    @return SHA-256 小写十六进制摘要 / Lower-case SHA-256 hexadecimal digest.

    @note 每段都含标签、存在位和长度；因此 header 缺失、空值、字段边界移动不会产生
        拼接歧义。method/path/key 已在唯一 scope 中，不重复混入请求指纹。
    """
    digest = hashlib.sha256()
    fields = (
        (b"body", canonical_body),
        (b"content-type", None if content_type is None else content_type.encode("utf-8")),
        (b"if-match", None if if_match is None else if_match.encode("utf-8")),
    )
    for label, value in fields:
        digest.update(len(label).to_bytes(2, "big"))
        digest.update(label)
        if value is None:
            digest.update(b"\x00")
            continue
        digest.update(b"\x01")
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


__all__ = [
    "IdempotencyClaim",
    "IdempotencyConflict",
    "IdempotencyDecision",
    "IdempotencyDecisionKind",
    "IdempotencyPreparationId",
    "IdempotencyRequest",
    "IdempotencyScope",
    "IdempotencyStatus",
    "IdempotentCommit",
    "IdempotentOperation",
    "IdempotentPrepare",
    "ReplayableResponse",
    "V2IdempotencyExecutor",
    "V2IdempotencyStore",
    "V2PreparedIdempotencyExecutor",
    "request_fingerprint",
]
