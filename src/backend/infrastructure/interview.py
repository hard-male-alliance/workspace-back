"""@brief API V2 Interview 事务持久化与短期凭据 adapter / API V2 Interview transactional persistence and short-lived credential adapters.

Scenario、Session、Transcript、Report 与统一 Job/Artifact/outbox/audit 共用一个 UoW。
PostgreSQL 查询始终以 Workspace 为首键；Realtime input 在 Session 行锁下完成幂等去重
与 sequence 分配。外部 realtime grant 只持久化无 secret lease，token/ICE credential
只存在于一次性返回对象中。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any, Protocol, Self, cast

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import and_, func, literal, or_, select, update
from sqlalchemy.engine import CursorResult, Result
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from backend.application.ports.access import AccessAuthorizer
from backend.application.ports.interview_v2 import (
    EndSessionOutput,
    InterviewCasMismatch,
    InterviewMediaFinalizer,
    InterviewPage,
    InterviewPageRequest,
    InterviewPermission,
    InterviewPermissionGrant,
    InterviewPermissionRequest,
    InterviewPolicyDenied,
    InterviewSessionPolicy,
    InterviewSessionPolicyRequest,
    InterviewWorkerOperationId,
    InterviewWorkerPortFailure,
    RealtimeInputKeyReused,
    ReportGenerationRequest,
    TranscriptSequenceReservation,
)
from backend.domain.interview_v2 import (
    INTERVIEW_END_JOB_KIND,
    INTERVIEW_REPORT_JOB_KIND,
    CreateRealtimeConnectionSpec,
    EndInterviewReason,
    EndSessionJobSpec,
    EphemeralToken,
    IceServer,
    InterviewExecutionGrant,
    InterviewJobQueuedRecord,
    InterviewJobSpec,
    InterviewKnowledgeContext,
    InterviewReport,
    InterviewReportDraft,
    InterviewReportId,
    InterviewScenario,
    InterviewScenarioId,
    InterviewScenarioSpec,
    InterviewScenarioStatus,
    InterviewSession,
    InterviewSessionId,
    InterviewSessionSpec,
    InterviewSessionStatus,
    RealtimeConnection,
    RealtimeConnectionId,
    RealtimeConnectionLease,
    RealtimeInputLedgerRecord,
    RealtimeInputReceipt,
    RealtimeTransport,
    ReportJobSpec,
    TranscriptSegment,
    TranscriptSegmentId,
    TranscriptSpeaker,
)
from backend.domain.knowledge_retrieval import KnowledgeSelectionMode
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    ModelRegion,
)
from backend.domain.platform import (
    ApiArtifactContentUrl,
    ApiEventId,
    Artifact,
    AuditEvent,
    Job,
    JobId,
    JobProgress,
    JobProgressUnit,
    JobStatus,
    ProblemDetails,
)
from backend.domain.principals import (
    AuthenticatedActor,
    ResourceMeta,
    TokenPrincipal,
    UserId,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef
from backend.infrastructure.access import (
    InMemoryAccessRepository,
    InMemoryAccessStore,
    PostgresAccessRepository,
)
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.models import (
    ArtifactRecord,
    AuditEventRecord,
    InterviewEventRecord,
    InterviewRealtimeConnectionRecord,
    InterviewReportEvidenceRecord,
    InterviewReportJobRecord,
    InterviewReportRecord,
    InterviewScenarioRecord,
    InterviewSessionRecord,
    JobRecord,
    JsonObject,
    KnowledgeSourceRecord,
    KnowledgeSourceVersionRecord,
    KnowledgeVisibilityGrantRecord,
    KnowledgeVisibilityPolicyRecord,
    ResumeDocumentRecord,
    ResumeRevisionRecord,
    TranscriptSegmentRecord,
)
from backend.infrastructure.platform import append_workspace_outbox_event
from workspace_shared.ids import new_opaque_id

_SCENARIO_SPEC_ADAPTER: TypeAdapter[InterviewScenarioSpec] = TypeAdapter(InterviewScenarioSpec)
"""@brief Scenario spec JSON codec / Scenario-spec JSON codec."""

_SESSION_SPEC_ADAPTER: TypeAdapter[InterviewSessionSpec] = TypeAdapter(InterviewSessionSpec)
"""@brief frozen Session spec JSON codec / Frozen Session-spec JSON codec."""

_EXECUTION_GRANT_ADAPTER: TypeAdapter[InterviewExecutionGrant] = TypeAdapter(
    InterviewExecutionGrant
)
"""@brief execution grant JSON codec / Execution-grant JSON codec."""

_REPORT_DRAFT_ADAPTER: TypeAdapter[InterviewReportDraft] = TypeAdapter(InterviewReportDraft)
"""@brief immutable Report draft JSON codec / Immutable Report-draft JSON codec."""

_JOB_SPEC_ADAPTER: TypeAdapter[InterviewJobSpec] = TypeAdapter(InterviewJobSpec)
"""@brief Interview worker spec JSON codec / Interview worker-spec JSON codec."""

_RESOURCE_REF_ADAPTER: TypeAdapter[ResourceRef] = TypeAdapter(ResourceRef)
"""@brief ResourceRef JSON codec / ResourceRef JSON codec."""

_RESOURCE_REFS_ADAPTER: TypeAdapter[tuple[ResourceRef, ...]] = TypeAdapter(
    tuple[ResourceRef, ...]
)
"""@brief Job result references JSON codec / Job result-reference JSON codec."""

_PROBLEM_ADAPTER: TypeAdapter[ProblemDetails] = TypeAdapter(ProblemDetails)
"""@brief Job ProblemDetails JSON codec / Job ProblemDetails JSON codec."""

_EVENT_RETENTION = timedelta(days=30)
"""@brief unified outbox replay retention / Unified-outbox replay retention."""

_MAX_CONNECTION_LIFETIME = timedelta(minutes=15)
"""@brief realtime credential hard lifetime / Realtime credential hard lifetime."""

_REALTIME_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
"""@brief realtime signing key ID 的稳定语法 / Stable syntax for realtime signing-key IDs."""

_PERMISSION_ACTION: Mapping[InterviewPermission, WorkspaceAction] = {
    InterviewPermission.LIST_SCENARIOS: WorkspaceAction.LIST_INTERVIEW_SCENARIOS,
    InterviewPermission.CREATE_SCENARIO: WorkspaceAction.CREATE_INTERVIEW_SCENARIO,
    InterviewPermission.READ_SCENARIO: WorkspaceAction.READ_INTERVIEW_SCENARIO,
    InterviewPermission.UPDATE_SCENARIO: WorkspaceAction.UPDATE_INTERVIEW_SCENARIO,
    InterviewPermission.LIST_SESSIONS: WorkspaceAction.LIST_INTERVIEW_SESSIONS,
    InterviewPermission.CREATE_SESSION: WorkspaceAction.CREATE_INTERVIEW_SESSION,
    InterviewPermission.READ_SESSION: WorkspaceAction.READ_INTERVIEW_SESSION,
    InterviewPermission.CREATE_CONNECTION: WorkspaceAction.CREATE_INTERVIEW_CONNECTION,
    InterviewPermission.END_SESSION: WorkspaceAction.END_INTERVIEW_SESSION,
    InterviewPermission.READ_TRANSCRIPT: WorkspaceAction.READ_INTERVIEW_TRANSCRIPT,
    InterviewPermission.CREATE_REPORT_JOB: WorkspaceAction.CREATE_INTERVIEW_REPORT_JOB,
    InterviewPermission.READ_REPORT: WorkspaceAction.READ_INTERVIEW_REPORT,
}
"""@brief Interview endpoint permission 到集中 action 的穷尽映射 / Exhaustive Interview-permission to central-action mapping."""


def _json_fallback(value: Any) -> Any:
    """@brief 将领域只读 Mapping 投影为 JSON object / Project domain read-only mappings to JSON objects.

    @param value Pydantic 不原生识别的领域值 / Domain value not natively recognized by Pydantic.
    @return 普通 ``dict`` 供递归 JSON 序列化 / A plain ``dict`` for recursive JSON serialization.
    @raise TypeError 未列入持久化白名单的类型 / A type outside the persistence allowlist.
    """
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError(f"Interview persistence cannot serialize {type(value).__name__}")


def _dump_object[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> JsonObject:
    """@brief 编码强类型值为 JSON object / Encode a typed value as a JSON object.

    @param adapter Pydantic codec / Pydantic codec.
    @param value 领域值 / Domain value.
    @return JSONB object / JSONB object.
    """
    payload = adapter.dump_python(
        value,
        mode="json",
        warnings="none",
        fallback=_json_fallback,
    )
    if not isinstance(payload, dict):
        raise TypeError("Interview persistence codec must produce an object")
    return cast(JsonObject, payload)


def _dump_array[ValueT](adapter: TypeAdapter[ValueT], value: ValueT) -> list[JsonObject]:
    """@brief 编码强类型值为 JSON object array / Encode a typed value as a JSON-object array."""
    payload = adapter.dump_python(
        value,
        mode="json",
        warnings="none",
        fallback=_json_fallback,
    )
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise TypeError("Interview persistence codec must produce an object array")
    return cast(list[JsonObject], payload)


def _load[ValueT](adapter: TypeAdapter[ValueT], payload: object, label: str) -> ValueT:
    """@brief 从不可信 JSONB 重建领域值 / Rebuild a domain value from untrusted JSONB.

    @param adapter Pydantic codec / Pydantic codec.
    @param payload decoded database value / Decoded database value.
    @param label diagnostic label / Diagnostic label.
    @return validated domain value / Validated domain value.
    """
    try:
        return adapter.validate_python(payload)
    except ValidationError as error:
        raise ValueError(f"persisted {label} violates the API V2 domain model") from error


def _affected_rows(result: Result[Any]) -> int:
    """@brief 返回 DML affected-row count / Return the DML affected-row count."""
    return cast(CursorResult[Any], result).rowcount


def _time_position(at: datetime, identifier: str) -> str:
    """@brief 编码稳定 created_at+ID keyset / Encode a stable created-at-plus-ID keyset."""
    return f"{int(at.timestamp() * 1_000_000)}:{identifier}"


def _parse_time_position(value: str | None) -> tuple[datetime, str] | None:
    """@brief 解码稳定 created_at+ID keyset / Decode a stable created-at-plus-ID keyset."""
    if value is None:
        return None
    raw, separator, identifier = value.partition(":")
    if not separator or not identifier:
        raise ValueError("Interview page position is invalid")
    try:
        instant = datetime.fromtimestamp(int(raw) / 1_000_000, tz=UTC)
    except (OverflowError, ValueError) as error:
        raise ValueError("Interview page position is invalid") from error
    return instant, identifier


def _sequence_position(sequence: int, identifier: str) -> str:
    """@brief 编码 Transcript sequence+ID keyset / Encode a Transcript sequence-plus-ID keyset."""
    return f"{sequence}:{identifier}"


def _parse_sequence_position(value: str | None) -> tuple[int, str] | None:
    """@brief 解码 Transcript sequence+ID keyset / Decode a Transcript sequence-plus-ID keyset."""
    if value is None:
        return None
    raw, separator, identifier = value.partition(":")
    if not separator or not identifier:
        raise ValueError("Transcript page position is invalid")
    try:
        sequence = int(raw)
    except ValueError as error:
        raise ValueError("Transcript page position is invalid") from error
    if sequence < 1:
        raise ValueError("Transcript page position is invalid")
    return sequence, identifier


def _b64url(payload: bytes) -> str:
    """@brief 无 padding URL-safe base64 / Unpadded URL-safe base64."""
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _decode_b64url(value: str, *, label: str, maximum_bytes: int) -> bytes:
    """@brief 严格解码有界 JWT segment / Strictly decode a bounded JWT segment.

    @param value 无 padding base64url segment / Unpadded base64url segment.
    @param label 公开安全错误标签 / Public-safe error label.
    @param maximum_bytes 解码后硬上限 / Hard decoded-size limit.
    @return 解码 bytes / Decoded bytes.
    @raise PermissionError 非规范编码或越界时抛出 / Raised for non-canonical or oversized input.
    """
    if not value or len(value) > ((maximum_bytes + 2) // 3) * 4:
        raise PermissionError(f"realtime token {label} is invalid")
    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(
            (value + padding).encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, ValueError) as error:
        raise PermissionError(f"realtime token {label} is invalid") from error
    if len(decoded) > maximum_bytes or _b64url(decoded) != value:
        raise PermissionError(f"realtime token {label} is invalid")
    return decoded


class _Clock(Protocol):
    """@brief 最小 timezone-aware clock / Minimal timezone-aware clock."""

    def now(self) -> datetime:
        """@brief 返回当前时刻 / Return the current instant."""


class _UtcClock:
    """@brief production UTC clock / Production UTC clock."""

    def now(self) -> datetime:
        """@brief 返回当前 UTC / Return current UTC."""
        return datetime.now(UTC)


class FailClosedInterviewMediaFinalizer:
    """@brief 未配置媒体能力时显式终态失败 / Explicitly fail terminally when media capability is unconfigured."""

    async def finalize(
        self,
        session: InterviewSession,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> EndSessionOutput:
        """@brief 拒绝伪造媒体 finalize 成功 / Refuse to fabricate successful media finalization.

        @param session 等待 finalize 的冻结 Session / Frozen Session awaiting finalization.
        @param operation_id 稳定 provider 幂等键 / Stable provider idempotency key.
        @return 永不返回 / Never returns.
        @raise InterviewWorkerPortFailure 始终返回稳定非重试错误 / Always raises a stable,
            non-retryable failure.
        """
        del session, operation_id
        raise InterviewWorkerPortFailure(
            "interview.media_finalizer_unconfigured",
            retryable=False,
        )


class ConsentAwareInterviewMediaFinalizer:
    """@brief 无录制时直接完成，有录制时委托显式 provider / Complete without recording or delegate to an explicit provider.

    @note Transcript 已在 realtime ingest 事务中按 consent 独立持久化；没有音视频录制请求时，
        end Job 不应被一个不存在的 media provider 阻塞。
        / Transcript persistence is independently consent-gated during realtime ingest; an end Job
        without audio/video recording must not depend on a nonexistent media provider.
    """

    def __init__(self, recording_finalizer: InterviewMediaFinalizer | None = None) -> None:
        """@brief 绑定可选录制 provider / Bind an optional recording provider.

        @param recording_finalizer 只有请求音视频录制时使用 / Used only when audio or video
            recording was requested.
        """
        self._recording_finalizer = recording_finalizer

    async def finalize(
        self,
        session: InterviewSession,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> EndSessionOutput:
        """@brief 依据冻结 consent 执行恰好一个 finalize 路径 / Execute exactly one finalization path from frozen consent.

        @param session 等待结束的冻结 Session / Frozen Session awaiting completion.
        @param operation_id 稳定 provider 幂等键 / Stable provider idempotency key.
        @return 无录制时为空 Artifact 集，或 provider 结果 / Empty Artifacts without recording,
            or the provider result.
        @raise InterviewWorkerPortFailure 请求录制但 provider 未配置 / Recording was requested
            but no provider is configured.
        """
        recording = session.spec.recording
        if not recording.record_audio and not recording.record_video:
            return EndSessionOutput(())
        if self._recording_finalizer is None:
            raise InterviewWorkerPortFailure(
                "interview.media_finalizer_unconfigured",
                retryable=False,
            )
        return await self._recording_finalizer.finalize(
            session,
            operation_id=operation_id,
        )


class FailClosedInterviewReportProvider:
    """@brief 未配置评估模型时显式终态失败 / Explicitly fail terminally when no evaluation model is configured."""

    async def generate(
        self,
        request: ReportGenerationRequest,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> InterviewReportDraft:
        """@brief 拒绝生成虚构 Interview Report / Refuse to generate a fabricated Interview Report.

        @param request 已脱敏的冻结评估输入 / Redacted frozen evaluation input.
        @param operation_id 稳定 provider 幂等键 / Stable provider idempotency key.
        @return 永不返回 / Never returns.
        @raise InterviewWorkerPortFailure 始终返回稳定非重试错误 / Always raises a stable,
            non-retryable failure.
        """
        del request, operation_id
        raise InterviewWorkerPortFailure(
            "interview.report_provider_unconfigured",
            retryable=False,
        )


@dataclass(frozen=True, slots=True)
class InterviewRealtimeSigningKey:
    """@brief 一个 realtime HMAC signing key 版本 / One versioned realtime HMAC signing key.

    @param key_id 不含秘密的稳定 ``kid`` / Stable non-secret ``kid``.
    @param key 至少 256-bit 的秘密材料 / Secret material of at least 256 bits.
    """

    key_id: str
    key: bytes = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 key ID 与熵下限 / Validate key ID and minimum entropy material."""
        if _REALTIME_KEY_ID.fullmatch(self.key_id) is None:
            raise ValueError("realtime signing key ID is invalid")
        if len(self.key) < 32:
            raise ValueError("realtime signing key must contain at least 32 bytes")
        object.__setattr__(self, "key", bytes(self.key))


class InterviewRealtimeSigningKeyring:
    """@brief active+historical realtime signing keyring / Active-plus-historical realtime signing keyring."""

    def __init__(
        self,
        active_key_id: str,
        keys: tuple[InterviewRealtimeSigningKey, ...],
    ) -> None:
        """@brief 固定签发 key 并保留验证窗口 / Pin the issuing key and retain a verification window.

        @param active_key_id 新凭据使用的 key ID / Key ID used for new credentials.
        @param keys active 与尚未过期历史 keys / Active and not-yet-retired historical keys.
        @raise ValueError key ID/material 重复或 active 缺失时抛出 / Raised for duplicate IDs,
            reused material, or a missing active key.
        """
        if not keys:
            raise ValueError("realtime signing keyring cannot be empty")
        by_id: dict[str, InterviewRealtimeSigningKey] = {}
        for candidate in keys:
            if candidate.key_id in by_id:
                raise ValueError("realtime signing key IDs must be unique")
            if any(hmac.compare_digest(candidate.key, prior.key) for prior in by_id.values()):
                raise ValueError("realtime signing key material cannot be reused")
            by_id[candidate.key_id] = candidate
        try:
            self._active = by_id[active_key_id]
        except KeyError as error:
            raise ValueError("active realtime signing key is absent") from error
        self._by_id = by_id

    @property
    def active(self) -> InterviewRealtimeSigningKey:
        """@brief 返回仅用于签发的 active key / Return the active issue-only key."""
        return self._active

    def verification_key(self, key_id: str) -> InterviewRealtimeSigningKey:
        """@brief 按 JWT ``kid`` 解析验证 key / Resolve a verification key by JWT ``kid``.

        @param key_id token header 中的 key ID / Key ID in the token header.
        @return active 或历史 key / Active or historical key.
        @raise PermissionError 未知 key ID 时抛出 / Raised for an unknown key ID.
        """
        key = self._by_id.get(key_id)
        if key is None:
            raise PermissionError("realtime token signing key is unknown")
        return key


class HmacInterviewRealtimeGateway:
    """@brief 签发可验证、短期、单 Session/单 audience 凭据 / Issue verifiable short-lived single-Session/single-audience credentials.

    @note signer 是无状态的，支持多 worker 与进程重启；持久 ``RealtimeConnectionLease``
        才是授权真相，signaling 边界验签后仍必须通过应用层 lease 校验。
        / The signer is stateless for multi-worker and restart safety; the durable
        ``RealtimeConnectionLease`` remains authoritative and must be checked after signature
        verification at the signaling boundary.
    """

    def __init__(
        self,
        signing_key: bytes | InterviewRealtimeSigningKeyring,
        *,
        signaling_url: str,
        allowed_transports: tuple[RealtimeTransport, ...] = (
            RealtimeTransport.WEBRTC,
            RealtimeTransport.WEBSOCKET,
        ),
        lifetime: timedelta = timedelta(minutes=5),
        heartbeat_interval_ms: int = 5_000,
        ice_urls: tuple[str, ...] = (),
        clock: _Clock | None = None,
    ) -> None:
        """@brief 配置 credential policy / Configure credential policy.

        @param signing_key 至少 32-byte legacy secret 或可轮换 keyring / Secret of at least 32
            bytes or a rotatable keyring.
        @param signaling_url HTTPS/WSS signaling endpoint / HTTPS/WSS signaling endpoint.
        @param allowed_transports provider-supported transports / Provider-supported transports.
        @param lifetime grant lifetime, at most fifteen minutes / Grant lifetime, at most fifteen minutes.
        @param heartbeat_interval_ms heartbeat interval / Heartbeat interval.
        @param ice_urls optional TURN/STUN URLs / Optional TURN/STUN URLs.
        @param clock injectable clock / Injectable clock.
        """
        keyring = (
            signing_key
            if isinstance(signing_key, InterviewRealtimeSigningKeyring)
            else InterviewRealtimeSigningKeyring(
                "legacy",
                (InterviewRealtimeSigningKey("legacy", signing_key),),
            )
        )
        if not allowed_transports or len(set(allowed_transports)) != len(allowed_transports):
            raise ValueError("realtime allowed transports must be non-empty and unique")
        if lifetime <= timedelta(0) or lifetime > _MAX_CONNECTION_LIFETIME:
            raise ValueError("realtime credential lifetime must be in (0, 15 minutes]")
        if not 1_000 <= heartbeat_interval_ms <= 120_000:
            raise ValueError("realtime heartbeat interval is invalid")
        self._keyring = keyring
        self._signaling_url = signaling_url
        self._allowed = allowed_transports
        self._lifetime = lifetime
        self._heartbeat = heartbeat_interval_ms
        self._ice_urls = ice_urls
        self._clock = clock or _UtcClock()

    async def issue(
        self,
        workspace_id: WorkspaceId,
        session: InterviewSession,
        audience: ResourceRef,
        spec: CreateRealtimeConnectionSpec,
        *,
        issued_at: datetime,
    ) -> RealtimeConnection:
        """@brief 签发不超过 policy 上限的一次 grant / Issue one grant bounded by policy.

        @param workspace_id path Workspace / Path Workspace.
        @param session target Session / Target Session.
        @param audience exact audience / Exact audience.
        @param spec client capabilities / Client capabilities.
        @param issued_at application-issued instant / Application-issued instant.
        @return transport descriptor with ephemeral credentials / Transport descriptor with ephemeral credentials.
        """
        if session.workspace_id != workspace_id or audience.resource_type != "user":
            raise PermissionError("realtime grant scope is invalid")
        transport = next(
            (item for item in spec.supported_transports if item in self._allowed),
            None,
        )
        if transport is None:
            raise ValueError("no mutually supported realtime transport")
        connection_id = RealtimeConnectionId(new_opaque_id("connection"))
        expires_at = issued_at + self._lifetime
        claims: JsonObject = {
            "jti": str(connection_id),
            "workspace_id": str(workspace_id),
            "session_id": str(session.meta.id),
            "aud": {"resource_type": audience.resource_type, "id": audience.id},
            "transport": transport.value,
            "iat": int(issued_at.timestamp()),
            "exp": int(expires_at.timestamp()),
            "nonce": _b64url(secrets.token_bytes(16)),
        }
        active_key = self._keyring.active
        header = _b64url(
            json.dumps(
                {"alg": "HS256", "kid": active_key.key_id, "typ": "JWT"},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        body = _b64url(
            json.dumps(claims, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        signature = _b64url(
            hmac.new(
                active_key.key,
                f"{header}.{body}".encode("ascii"),
                hashlib.sha256,
            ).digest()
        )
        token = EphemeralToken(f"{header}.{body}.{signature}")
        ice_servers: tuple[IceServer, ...] = ()
        if self._ice_urls:
            username = f"{int(expires_at.timestamp())}:{connection_id}:{audience.id}"
            credential = _b64url(
                hmac.new(active_key.key, username.encode(), hashlib.sha256).digest()
            )
            ice_servers = (IceServer(self._ice_urls, username, credential),)
        return RealtimeConnection(
            connection_id,
            workspace_id,
            session.meta.id,
            audience,
            transport,
            self._signaling_url,
            token,
            ice_servers,
            issued_at,
            expires_at,
            self._heartbeat,
        )

    async def revoke(self, connection_id: RealtimeConnectionId) -> None:
        """@brief 丢弃尚未返回的孤儿 grant / Discard an orphan grant not returned to a client.

        @param connection_id 未持久化的 grant ID / Unpersisted grant ID.
        @note 无状态 signer 没有可撤销 secret；第二阶段失败时 token 尚未越过 transport 边界。
            / A stateless signer has no secret state to revoke; a token has not crossed the
            transport boundary when phase two fails.
        """
        del connection_id

    async def verify(
        self,
        token: str,
        *,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        audience: ResourceRef,
    ) -> JsonObject:
        """@brief signaling boundary 验证 signature、expiry 与三重 binding / Verify signature, expiry, and triple binding at the signaling boundary."""
        parts = token.split(".")
        if len(parts) != 3 or len(token) > 8_192:
            raise PermissionError("realtime token is malformed")
        try:
            header_payload = json.loads(
                _decode_b64url(parts[0], label="header", maximum_bytes=512)
            )
        except json.JSONDecodeError as error:
            raise PermissionError("realtime token header is invalid") from error
        if (
            not isinstance(header_payload, dict)
            or set(header_payload) != {"alg", "kid", "typ"}
            or header_payload.get("alg") != "HS256"
            or header_payload.get("typ") != "JWT"
            or not isinstance(header_payload.get("kid"), str)
        ):
            raise PermissionError("realtime token header is invalid")
        verification_key = self._keyring.verification_key(header_payload["kid"])
        expected = _b64url(
            hmac.new(
                verification_key.key,
                f"{parts[0]}.{parts[1]}".encode("ascii"),
                hashlib.sha256,
            ).digest()
        )
        if not hmac.compare_digest(parts[2], expected):
            raise PermissionError("realtime token signature is invalid")
        try:
            payload = json.loads(
                _decode_b64url(parts[1], label="payload", maximum_bytes=4_096)
            )
        except json.JSONDecodeError as error:
            raise PermissionError("realtime token payload is invalid") from error
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {"aud", "exp", "iat", "jti", "nonce", "session_id", "transport", "workspace_id"}
            or isinstance(payload.get("iat"), bool)
            or not isinstance(payload.get("iat"), int)
            or isinstance(payload.get("exp"), bool)
            or not isinstance(payload.get("exp"), int)
            or not isinstance(payload.get("nonce"), str)
            or not isinstance(payload.get("transport"), str)
        ):
            raise PermissionError("realtime token payload is invalid")
        RealtimeConnectionId(str(payload.get("jti", "")))
        now = self._clock.now()
        if (
            payload["exp"] <= int(now.timestamp())
            or payload["iat"] > int(now.timestamp())
            or payload["exp"] <= payload["iat"]
            or payload["exp"] - payload["iat"] > int(_MAX_CONNECTION_LIFETIME.total_seconds())
        ):
            raise PermissionError("realtime token has expired")
        if (
            payload.get("workspace_id") != str(workspace_id)
            or payload.get("session_id") != str(session_id)
            or payload.get("aud")
            != {"resource_type": audience.resource_type, "id": audience.id}
            or payload.get("transport") not in {item.value for item in RealtimeTransport}
        ):
            raise PermissionError("realtime token claims are scope-mismatched")
        try:
            if len(_decode_b64url(payload["nonce"], label="nonce", maximum_bytes=16)) != 16:
                raise PermissionError("realtime token nonce is invalid")
        except PermissionError as error:
            raise PermissionError("realtime token nonce is invalid") from error
        return cast(JsonObject, payload)


class StaticInterviewSessionPolicy:
    """@brief 显式配置资源 allowlist 的本地 Session policy / Local Session policy backed by explicit resource allowlists."""

    def __init__(
        self,
        *,
        model_ref: ResourceRef,
        allowed_regions: frozenset[ModelRegion],
        allow_external_model_processing: bool,
        resume_refs: frozenset[tuple[WorkspaceId, str, int]] = frozenset(),
        knowledge_contexts: Mapping[
            tuple[WorkspaceId, KnowledgeSourceId], InterviewKnowledgeContext
        ] = {},
    ) -> None:
        """@brief 构造 deny-by-default policy / Construct a deny-by-default policy."""
        if model_ref.resource_type != "model" or model_ref.revision is None:
            raise ValueError("Interview model policy requires an exact model ref")
        if not allowed_regions:
            raise ValueError("Interview model policy requires at least one region")
        self._model_ref = model_ref
        self._regions = allowed_regions
        self._external = allow_external_model_processing
        self._resumes = resume_refs
        self._knowledge = dict(knowledge_contexts)

    async def authorize_session(
        self,
        request: InterviewSessionPolicyRequest,
    ) -> InterviewExecutionGrant:
        """@brief 解析显式 allowlist 并产生 frozen grant / Resolve explicit allowlists and produce a frozen grant."""
        spec = request.spec
        if spec.inference.data_region not in self._regions:
            raise InterviewPolicyDenied("Interview model region is not allowed")
        external = self._external and spec.inference.allow_external_model_processing
        if spec.resume_ref is not None:
            key = (
                request.workspace_id,
                spec.resume_ref.id,
                cast(int, spec.resume_ref.revision),
            )
            if key not in self._resumes:
                raise InterviewPolicyDenied("Interview Resume revision is not allowed")
        contexts = tuple(
            item
            for (workspace_id, source_id), item in self._knowledge.items()
            if workspace_id == request.workspace_id
            and source_id not in spec.knowledge.exclude_source_ids
            and (
                spec.knowledge.mode is KnowledgeSelectionMode.POLICY_DEFAULT
                or source_id in spec.knowledge.include_source_ids
            )
        )
        if spec.knowledge.mode is KnowledgeSelectionMode.NONE:
            contexts = ()
        if spec.knowledge.mode is KnowledgeSelectionMode.EXPLICIT and {
            item.source_id for item in contexts
        } != set(spec.knowledge.include_source_ids):
            raise InterviewPolicyDenied("Interview explicit Knowledge selection is not allowed")
        grant = InterviewExecutionGrant(
            ResourceRef(
                "interview_scenario",
                request.scenario.meta.id,
                request.scenario.meta.revision,
            ),
            spec.resume_ref,
            spec.knowledge.agent_scope,
            self._model_ref,
            spec.inference.data_region,
            external,
            contexts,
            max((item.policy_version for item in contexts), default=1),
        )
        grant.validate_for(request.scenario, spec)
        return grant


class _TrackingInterviewAuthorizer:
    """@brief 复用集中 AccessAuthorizer 并固定 actor/Workspace / Reuse the central AccessAuthorizer and pin actor/Workspace."""

    def __init__(
        self,
        delegate: AccessAuthorizer,
        bind_scope: Callable[[AuthenticatedActor, WorkspaceId], Awaitable[None]] | None = None,
    ) -> None:
        """@brief 绑定集中 authorizer 与可选 RLS scope callback / Bind central authorizer and optional RLS-scope callback."""
        self._delegate = delegate
        self._bind_scope = bind_scope
        self.actor_id: UserId | None = None
        self.workspace_id: WorkspaceId | None = None

    async def authorize(
        self,
        principal: TokenPrincipal,
        request: InterviewPermissionRequest,
    ) -> InterviewPermissionGrant:
        """@brief 授权一个精确 Interview permission / Authorize one exact Interview permission."""
        action = _PERMISSION_ACTION.get(request.permission)
        if action is None:
            raise PermissionError("Interview permission lacks a centralized policy mapping")
        actor = await self._delegate.authenticate(principal)
        if self.actor_id is not None and self.actor_id != actor.user_id:
            raise PermissionError("an Interview unit of work cannot switch actors")
        await self._delegate.authorize(actor, request.workspace_id, action)
        if self.workspace_id is not None and self.workspace_id != request.workspace_id:
            raise PermissionError("an Interview unit of work cannot switch Workspaces")
        self.actor_id = actor.user_id
        self.workspace_id = request.workspace_id
        if self._bind_scope is not None:
            await self._bind_scope(actor, request.workspace_id)
        return InterviewPermissionGrant(actor.user_id, request)

    def require_actor(self) -> UserId:
        """@brief 要求已有集中认证 actor / Require a centrally authenticated actor."""
        if self.actor_id is None:
            raise PermissionError("Interview mutation requires centralized authentication")
        return self.actor_id

    def require_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 要求参数 Workspace 与授权路径一致 / Require the argument Workspace to match the authorized path."""
        if self.workspace_id != workspace_id:
            raise PermissionError("Interview persistence requires prior Workspace authorization")


@dataclass(slots=True)
class InMemoryInterviewStore:
    """@brief Interview V2 共享进程内状态 / Shared in-process Interview V2 state."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    scenarios: dict[tuple[WorkspaceId, InterviewScenarioId], InterviewScenario] = field(
        default_factory=dict
    )
    sessions: dict[tuple[WorkspaceId, InterviewSessionId], InterviewSession] = field(
        default_factory=dict
    )
    leases: dict[tuple[WorkspaceId, InterviewSessionId, RealtimeConnectionId], RealtimeConnectionLease] = field(
        default_factory=dict
    )
    realtime_inputs: dict[
        tuple[WorkspaceId, InterviewSessionId, str], tuple[str, int]
    ] = field(default_factory=dict)
    transcript: dict[tuple[WorkspaceId, InterviewSessionId], list[TranscriptSegment]] = field(
        default_factory=dict
    )
    reports: dict[tuple[WorkspaceId, InterviewReportId], InterviewReport] = field(
        default_factory=dict
    )
    jobs: dict[tuple[WorkspaceId, JobId], Job] = field(default_factory=dict)
    job_owners: dict[tuple[WorkspaceId, JobId], UserId] = field(default_factory=dict)
    """@brief Job creator 的可信内存索引 / Trusted in-memory index of Job creators."""

    job_specs: dict[tuple[WorkspaceId, JobId], InterviewJobSpec] = field(default_factory=dict)
    artifacts: dict[tuple[WorkspaceId, str], Artifact] = field(default_factory=dict)
    outbox_events: list[InterviewJobQueuedRecord] = field(default_factory=list)
    audit_events: list[AuditEvent] = field(default_factory=list)
    next_realtime_sequence: dict[tuple[WorkspaceId, InterviewSessionId], int] = field(
        default_factory=dict
    )
    next_transcript_sequence: dict[tuple[WorkspaceId, InterviewSessionId], int] = field(
        default_factory=dict
    )


class InMemoryInterviewRepository:
    """@brief copy-on-write Workspace-first Interview repository / Copy-on-write Workspace-first Interview repository."""

    def __init__(self, store: InMemoryInterviewStore) -> None:
        """@brief 绑定事务 snapshot / Bind a transaction snapshot."""
        self._store = store

    async def list_scenarios(
        self, workspace_id: WorkspaceId, page: InterviewPageRequest
    ) -> InterviewPage[InterviewScenario]:
        """@brief 按 created_at+ID keyset 列 Scenario / List Scenarios by a created-at-plus-ID keyset."""
        values = sorted(
            (
                value
                for (candidate_workspace, _), value in self._store.scenarios.items()
                if candidate_workspace == workspace_id
            ),
            key=lambda value: (value.meta.created_at, str(value.meta.id)),
        )
        after = _parse_time_position(page.after)
        if after is not None:
            values = [
                value
                for value in values
                if (value.meta.created_at, str(value.meta.id)) > after
            ]
        items = tuple(values[: page.limit])
        next_position = (
            _time_position(items[-1].meta.created_at, str(items[-1].meta.id))
            if len(values) > page.limit and items
            else None
        )
        return InterviewPage(items, next_position)

    async def get_scenario(
        self,
        workspace_id: WorkspaceId,
        scenario_id: InterviewScenarioId,
        *,
        for_update: bool = False,
    ) -> InterviewScenario | None:
        """@brief 读取一个 Workspace Scenario / Read one Workspace Scenario."""
        del for_update
        return self._store.scenarios.get((workspace_id, scenario_id))

    async def add_scenario(self, scenario: InterviewScenario) -> None:
        """@brief 添加 Scenario / Add a Scenario."""
        key = (scenario.workspace_id, scenario.meta.id)
        if key in self._store.scenarios:
            raise ValueError("Interview Scenario already exists")
        self._store.scenarios[key] = scenario

    async def save_scenario(
        self, scenario: InterviewScenario, *, expected_revision: int
    ) -> None:
        """@brief revision CAS 保存 Scenario / Save a Scenario with revision CAS."""
        key = (scenario.workspace_id, scenario.meta.id)
        current = self._store.scenarios.get(key)
        if (
            current is None
            or current.meta.revision != expected_revision
            or scenario.meta.revision != expected_revision + 1
        ):
            raise InterviewCasMismatch
        self._store.scenarios[key] = scenario

    async def list_sessions(
        self, workspace_id: WorkspaceId, page: InterviewPageRequest
    ) -> InterviewPage[InterviewSession]:
        """@brief 按 created_at+ID keyset 列 Session / List Sessions by a created-at-plus-ID keyset."""
        values = sorted(
            (
                value
                for (candidate_workspace, _), value in self._store.sessions.items()
                if candidate_workspace == workspace_id
            ),
            key=lambda value: (value.meta.created_at, str(value.meta.id)),
        )
        after = _parse_time_position(page.after)
        if after is not None:
            values = [
                value
                for value in values
                if (value.meta.created_at, str(value.meta.id)) > after
            ]
        items = tuple(values[: page.limit])
        next_position = (
            _time_position(items[-1].meta.created_at, str(items[-1].meta.id))
            if len(values) > page.limit and items
            else None
        )
        return InterviewPage(items, next_position)

    async def get_session(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        for_update: bool = False,
    ) -> InterviewSession | None:
        """@brief 读取一个 Workspace Session / Read one Workspace Session."""
        del for_update
        return self._store.sessions.get((workspace_id, session_id))

    async def add_session(self, session: InterviewSession) -> None:
        """@brief 添加 frozen Session / Add a frozen Session."""
        key = (session.workspace_id, session.meta.id)
        if key in self._store.sessions:
            raise ValueError("Interview Session already exists")
        self._store.sessions[key] = session
        self._store.next_realtime_sequence[key] = 1
        self._store.next_transcript_sequence[key] = 1
        self._store.transcript[key] = []

    async def save_session(
        self, session: InterviewSession, *, expected_revision: int
    ) -> None:
        """@brief revision CAS 保存 Session / Save a Session with revision CAS."""
        key = (session.workspace_id, session.meta.id)
        current = self._store.sessions.get(key)
        if (
            current is None
            or current.meta.revision != expected_revision
            or session.meta.revision != expected_revision + 1
        ):
            raise InterviewCasMismatch
        self._store.sessions[key] = session

    async def add_connection_lease(self, lease: RealtimeConnectionLease) -> None:
        """@brief 保存 secret-free connection lease / Save a secret-free connection lease."""
        key = (lease.workspace_id, lease.session_id, lease.id)
        if key in self._store.leases:
            raise ValueError("Realtime connection already exists")
        self._store.leases[key] = lease

    async def get_connection_lease(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        connection_id: RealtimeConnectionId,
    ) -> RealtimeConnectionLease | None:
        """@brief 以 Workspace+Session+Connection 读取 lease / Read a lease by Workspace+Session+Connection."""
        return self._store.leases.get((workspace_id, session_id, connection_id))

    async def append_realtime_input(
        self, record: RealtimeInputLedgerRecord
    ) -> RealtimeInputReceipt:
        """@brief 原子去重并分配 realtime sequence / Atomically deduplicate and allocate a realtime sequence."""
        session_key = (record.workspace_id, record.session_id)
        if session_key not in self._store.sessions:
            raise InterviewCasMismatch
        key = (record.workspace_id, record.session_id, str(record.input_id))
        prior = self._store.realtime_inputs.get(key)
        if prior is not None:
            if prior[0] != record.fingerprint_sha256:
                raise RealtimeInputKeyReused
            return RealtimeInputReceipt(prior[1], True)
        sequence = self._store.next_realtime_sequence[session_key]
        self._store.next_realtime_sequence[session_key] = sequence + 1
        self._store.realtime_inputs[key] = (record.fingerprint_sha256, sequence)
        return RealtimeInputReceipt(sequence, False)

    async def allocate_transcript_sequence(
        self, workspace_id: WorkspaceId, session_id: InterviewSessionId
    ) -> TranscriptSequenceReservation:
        """@brief 原子保留 Transcript sequence / Atomically reserve a Transcript sequence."""
        key = (workspace_id, session_id)
        if key not in self._store.sessions:
            raise InterviewCasMismatch
        sequence = self._store.next_transcript_sequence[key]
        self._store.next_transcript_sequence[key] = sequence + 1
        return TranscriptSequenceReservation(sequence)

    async def add_transcript_segment(self, segment: TranscriptSegment) -> None:
        """@brief 添加有 provenance 的 immutable segment / Add an immutable segment with provenance."""
        key = (segment.workspace_id, segment.session_id)
        values = self._store.transcript.get(key)
        if values is None:
            raise InterviewCasMismatch
        if segment.source_ref.resource_type == "realtime_input":
            source_key = (segment.workspace_id, segment.session_id, segment.source_ref.id)
            if source_key not in self._store.realtime_inputs:
                raise ValueError("Transcript realtime-input provenance does not exist")
        elif (segment.workspace_id, segment.source_ref.id) not in self._store.artifacts:
            raise ValueError("Transcript Artifact provenance does not exist")
        if any(item.id == segment.id or item.sequence == segment.sequence for item in values):
            raise ValueError("Transcript identity or sequence already exists")
        values.append(segment)

    async def list_transcript(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        page: InterviewPageRequest,
    ) -> InterviewPage[TranscriptSegment]:
        """@brief 按 sequence+ID keyset 列 Transcript / List Transcript by a sequence-plus-ID keyset."""
        values = sorted(
            self._store.transcript.get((workspace_id, session_id), ()),
            key=lambda value: (value.sequence, str(value.id)),
        )
        after = _parse_sequence_position(page.after)
        if after is not None:
            values = [
                value for value in values if (value.sequence, str(value.id)) > after
            ]
        items = tuple(values[: page.limit])
        next_position = (
            _sequence_position(items[-1].sequence, str(items[-1].id))
            if len(values) > page.limit and items
            else None
        )
        return InterviewPage(items, next_position)

    async def load_transcript_snapshot(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        maximum_segments: int,
    ) -> tuple[TranscriptSegment, ...]:
        """@brief 加载有界完整 Transcript snapshot / Load a bounded complete Transcript snapshot."""
        values = tuple(
            sorted(
                self._store.transcript.get((workspace_id, session_id), ()),
                key=lambda value: (value.sequence, str(value.id)),
            )
        )
        if len(values) > maximum_segments:
            raise ValueError("Transcript exceeds the worker snapshot bound")
        return values

    async def get_report(
        self, workspace_id: WorkspaceId, report_id: InterviewReportId
    ) -> InterviewReport | None:
        """@brief 读取 immutable Report / Read an immutable Report."""
        return self._store.reports.get((workspace_id, report_id))

    async def add_report(self, report: InterviewReport) -> None:
        """@brief 添加 immutable Report / Add an immutable Report."""
        key = (report.workspace_id, report.meta.id)
        if key in self._store.reports or any(
            value.session_id == report.session_id
            for (workspace_id, _), value in self._store.reports.items()
            if workspace_id == report.workspace_id
        ):
            raise ValueError("Interview Report already exists for this Session")
        self._store.reports[key] = report

    async def has_live_report_job(
        self, workspace_id: WorkspaceId, session_id: InterviewSessionId
    ) -> bool:
        """@brief 判断是否存在 live Report Job / Test whether a live Report Job exists."""
        return any(
            job.kind == INTERVIEW_REPORT_JOB_KIND
            and job.subject.id == session_id
            and not job.is_terminal
            for (candidate_workspace, _), job in self._store.jobs.items()
            if candidate_workspace == workspace_id
        )


class _InMemoryInterviewJobs:
    """@brief 内存统一 Job adapter / In-memory unified Job adapter."""

    def __init__(
        self,
        store: InMemoryInterviewStore,
        authorizer: _TrackingInterviewAuthorizer,
    ) -> None:
        """@brief 绑定事务 snapshot 与 actor proof / Bind the transaction snapshot and actor proof.

        @param store copy-on-write snapshot / Copy-on-write snapshot.
        @param authorizer 公开写入时提供 creator 的 authorizer / Authorizer providing the creator
            for public writes.
        """
        self._store = store
        self._authorizer = authorizer

    async def add(self, job: Job, spec: InterviewJobSpec) -> None:
        """@brief 原子添加 Job 与 typed spec / Atomically add a Job and typed spec."""
        key = (job.workspace_id, job.meta.id)
        if key in self._store.jobs:
            raise ValueError("Interview Job already exists")
        self._store.jobs[key] = job
        self._store.job_owners[key] = self._authorizer.require_actor()
        self._store.job_specs[key] = spec

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 读取统一 Job / Read a unified Job."""
        del for_update
        return self._store.jobs.get((workspace_id, job_id))

    async def get_owned(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 按 Workspace+creator+Job 读取内存状态 / Read in-memory state by Workspace, creator, and Job."""
        del for_update
        key = (workspace_id, job_id)
        if self._store.job_owners.get(key) != actor_id:
            return None
        return self._store.jobs.get(key)

    async def save(self, job: Job, *, expected_revision: int) -> None:
        """@brief CAS 保存统一 Job / Save a unified Job with CAS."""
        key = (job.workspace_id, job.meta.id)
        current = self._store.jobs.get(key)
        if (
            current is None
            or current.meta.revision != expected_revision
            or job.meta.revision != expected_revision + 1
        ):
            raise InterviewCasMismatch
        self._store.jobs[key] = job


class _InMemoryInterviewArtifacts:
    """@brief 内存统一 Artifact adapter / In-memory unified Artifact adapter."""

    def __init__(self, store: InMemoryInterviewStore) -> None:
        self._store = store

    async def add(self, artifact: Artifact) -> None:
        """@brief 添加统一 Artifact / Add a unified Artifact."""
        key = (artifact.workspace_id, str(artifact.meta.id))
        if key in self._store.artifacts:
            raise ValueError("Artifact already exists")
        self._store.artifacts[key] = artifact


class _InMemoryInterviewOutbox:
    """@brief 内存统一 outbox adapter / In-memory unified-outbox adapter."""

    def __init__(self, store: InMemoryInterviewStore) -> None:
        self._store = store

    async def add(self, record: InterviewJobQueuedRecord) -> None:
        """@brief 添加 secret-free queued event / Add a secret-free queued event."""
        self._store.outbox_events.append(record)


class _InMemoryInterviewAudit:
    """@brief 内存统一 audit adapter / In-memory unified-audit adapter."""

    def __init__(self, store: InMemoryInterviewStore) -> None:
        self._store = store

    async def add(self, event: AuditEvent) -> None:
        """@brief 添加 append-only AuditEvent / Add an append-only AuditEvent."""
        self._store.audit_events.append(event)


class InMemoryInterviewUnitOfWork:
    """@brief copy-on-write Interview UoW / Copy-on-write Interview UoW."""

    def __init__(
        self,
        store: InMemoryInterviewStore,
        access_store: InMemoryAccessStore,
        policy: InterviewSessionPolicy,
    ) -> None:
        """@brief 绑定共享 store、Access state 与 policy / Bind shared store, Access state, and policy."""
        self._shared = store
        self._access_store = access_store
        self._policy = policy
        self._snapshot: InMemoryInterviewStore | None = None
        self._authorizer: _TrackingInterviewAuthorizer | None = None
        self._repository: InMemoryInterviewRepository | None = None
        self._jobs: _InMemoryInterviewJobs | None = None
        self._artifacts: _InMemoryInterviewArtifacts | None = None
        self._outbox: _InMemoryInterviewOutbox | None = None
        self._audit: _InMemoryInterviewAudit | None = None
        self._entered = False
        self._committed = False
        self._rolled_back = False

    @property
    def authorizer(self) -> _TrackingInterviewAuthorizer:
        """@brief 返回 centralized authorizer / Return the centralized authorizer."""
        if self._authorizer is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._authorizer

    @property
    def policy(self) -> InterviewSessionPolicy:
        """@brief 返回 local Session policy / Return the local Session policy."""
        return self._policy

    @property
    def repository(self) -> InMemoryInterviewRepository:
        """@brief 返回 transaction repository / Return the transaction repository."""
        if self._repository is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._repository

    @property
    def jobs(self) -> _InMemoryInterviewJobs:
        """@brief 返回统一 Job adapter / Return the unified Job adapter."""
        if self._jobs is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._jobs

    @property
    def artifacts(self) -> _InMemoryInterviewArtifacts:
        """@brief 返回统一 Artifact adapter / Return the unified Artifact adapter."""
        if self._artifacts is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._artifacts

    @property
    def outbox(self) -> _InMemoryInterviewOutbox:
        """@brief 返回统一 outbox adapter / Return the unified-outbox adapter."""
        if self._outbox is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._outbox

    @property
    def audit(self) -> _InMemoryInterviewAudit:
        """@brief 返回统一 audit adapter / Return the unified-audit adapter."""
        if self._audit is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._audit

    async def __aenter__(self) -> Self:
        """@brief 固定锁顺序并创建 copy-on-write snapshot / Acquire locks in a fixed order and create a copy-on-write snapshot."""
        if self._entered:
            raise RuntimeError("Interview unit of work cannot be re-entered")
        await self._access_store.lock.acquire()
        try:
            await self._shared.lock.acquire()
        except BaseException:
            self._access_store.lock.release()
            raise
        self._entered = True
        snapshot = InMemoryInterviewStore(
            scenarios=dict(self._shared.scenarios),
            sessions=dict(self._shared.sessions),
            leases=dict(self._shared.leases),
            realtime_inputs=dict(self._shared.realtime_inputs),
            transcript={key: list(values) for key, values in self._shared.transcript.items()},
            reports=dict(self._shared.reports),
            jobs=dict(self._shared.jobs),
            job_owners=dict(self._shared.job_owners),
            job_specs=dict(self._shared.job_specs),
            artifacts=dict(self._shared.artifacts),
            outbox_events=list(self._shared.outbox_events),
            audit_events=list(self._shared.audit_events),
            next_realtime_sequence=dict(self._shared.next_realtime_sequence),
            next_transcript_sequence=dict(self._shared.next_transcript_sequence),
        )
        self._snapshot = snapshot
        access_repository = InMemoryAccessRepository(
            users=dict(self._access_store.users),
            workspaces=dict(self._access_store.workspaces),
            memberships=dict(self._access_store.memberships),
            invitations=dict(self._access_store.invitations),
            account_deletions=dict(self._access_store.account_deletions),
        )
        self._authorizer = _TrackingInterviewAuthorizer(AccessAuthorizer(access_repository))
        self._repository = InMemoryInterviewRepository(snapshot)
        self._jobs = _InMemoryInterviewJobs(snapshot, self._authorizer)
        self._artifacts = _InMemoryInterviewArtifacts(snapshot)
        self._outbox = _InMemoryInterviewOutbox(snapshot)
        self._audit = _InMemoryInterviewAudit(snapshot)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 回滚未提交 snapshot 并释放锁 / Roll back an uncommitted snapshot and release locks."""
        del exc, traceback
        if self._entered:
            if exc_type is not None or not self._committed:
                await self.rollback()
            self._clear()
            self._entered = False
            self._shared.lock.release()
            self._access_store.lock.release()
        return None

    async def commit(self) -> None:
        """@brief 原子发布完整 snapshot / Atomically publish the complete snapshot."""
        if self._snapshot is None or not self._entered:
            raise RuntimeError("Interview unit of work has not been entered")
        if self._committed:
            raise RuntimeError("Interview unit of work is already committed")
        if self._rolled_back:
            raise RuntimeError("rolled-back Interview unit of work cannot commit")
        snapshot = self._snapshot
        self._shared.scenarios = snapshot.scenarios
        self._shared.sessions = snapshot.sessions
        self._shared.leases = snapshot.leases
        self._shared.realtime_inputs = snapshot.realtime_inputs
        self._shared.transcript = snapshot.transcript
        self._shared.reports = snapshot.reports
        self._shared.jobs = snapshot.jobs
        self._shared.job_owners = snapshot.job_owners
        self._shared.job_specs = snapshot.job_specs
        self._shared.artifacts = snapshot.artifacts
        self._shared.outbox_events = snapshot.outbox_events
        self._shared.audit_events = snapshot.audit_events
        self._shared.next_realtime_sequence = snapshot.next_realtime_sequence
        self._shared.next_transcript_sequence = snapshot.next_transcript_sequence
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等丢弃 snapshot / Idempotently discard the snapshot."""
        if not self._entered:
            raise RuntimeError("Interview unit of work has not been entered")
        self._rolled_back = True

    def _clear(self) -> None:
        """@brief 清理 transaction-bound adapter / Clear transaction-bound adapters."""
        self._snapshot = None
        self._authorizer = None
        self._repository = None
        self._jobs = None
        self._artifacts = None
        self._outbox = None
        self._audit = None


class InMemoryInterviewUnitOfWorkFactory:
    """@brief 创建共享状态的内存 Interview UoW / Create in-memory Interview UoWs over shared state."""

    def __init__(
        self,
        access_store: InMemoryAccessStore,
        policy: InterviewSessionPolicy,
        *,
        store: InMemoryInterviewStore | None = None,
    ) -> None:
        """@brief 绑定 Access、policy 与可选 Interview state / Bind Access, policy, and optional Interview state."""
        self.access_store = access_store
        self.policy = policy
        self.store = store or InMemoryInterviewStore()

    def __call__(self) -> InMemoryInterviewUnitOfWork:
        """@brief 创建未进入的 UoW / Create a not-yet-entered UoW."""
        return InMemoryInterviewUnitOfWork(self.store, self.access_store, self.policy)


class _PostgresInterviewScope:
    """@brief 一个事务内显式 user/service actor 与 Workspace scope / Explicit user/service actor and Workspace scope for one transaction."""

    def __init__(
        self,
        database: AsyncDatabase,
        session: AsyncSession,
        service_actor: ResourceRef,
    ) -> None:
        """@brief 绑定 database Session 与真实 service identity / Bind a database Session and a real service identity."""
        if service_actor.resource_type != "service" or service_actor.revision is not None:
            raise ValueError("Interview service actor must be an unversioned service ResourceRef")
        self._database = database
        self._session = session
        self._service_actor = service_actor
        self.actor: ResourceRef | None = None
        self.workspace_id: WorkspaceId | None = None

    async def bind_user(self, actor: AuthenticatedActor, workspace_id: WorkspaceId) -> None:
        """@brief 固定已认证 user actor 与授权 Workspace / Pin an authenticated user actor and authorized Workspace."""
        actor_ref = ResourceRef("user", actor.user_id)
        if self.actor is not None and self.actor != actor_ref:
            raise PermissionError("an Interview transaction cannot switch actors")
        if self.workspace_id is not None and self.workspace_id != workspace_id:
            raise PermissionError("an Interview transaction cannot switch Workspaces")
        self.actor = actor_ref
        self.workspace_id = workspace_id
        await self._database.install_v2_request_scope(
            self._session,
            actor_id=actor_ref.id,
            workspace_id=str(workspace_id),
        )

    async def ensure_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 无 user authorization 的 worker/realtime 路径显式绑定 service actor / Explicitly bind the service actor for worker/realtime paths without user authorization."""
        if self.workspace_id is not None:
            if self.workspace_id != workspace_id:
                raise PermissionError("an Interview transaction cannot switch Workspaces")
            return
        if self.actor is not None and self.actor != self._service_actor:
            raise PermissionError("an authenticated Interview transaction lacks Workspace authorization")
        self.actor = self._service_actor
        self.workspace_id = workspace_id
        await self._database.install_v2_request_scope(
            self._session,
            actor_id=self._service_actor.id,
            workspace_id=str(workspace_id),
        )

    def require_authorized_user(self, workspace_id: WorkspaceId) -> UserId:
        """@brief 要求当前 scope 来自集中 user authorization / Require scope from centralized user authorization."""
        if (
            self.actor is None
            or self.actor.resource_type != "user"
            or self.workspace_id != workspace_id
        ):
            raise PermissionError("Interview mutation requires an authorized user scope")
        return UserId(self.actor.id)

    async def legacy_owner_id(
        self,
        workspace_id: WorkspaceId,
        *,
        job_id: str,
    ) -> str:
        """@brief 为仍含 legacy owner FK 的 Audit 表解析真实用户 owner / Resolve a real user owner for the Audit table retaining a legacy owner FK.

        @param workspace_id 精确 Workspace / Exact Workspace.
        @param job_id worker audit 关联的统一 Job / Unified Job correlated to a worker audit.

        @note 返回值只填充 0017 统一表尚未移除的兼容列；Audit actor 仍使用独立
            ``actor_type/actor_id``，绝不把 service 冒充为用户。
            / This only fills a compatibility column retained by 0017; audit actor identity remains
            independent and a service is never impersonated as a user.
        """
        await self.ensure_workspace(workspace_id)
        if self.actor is not None and self.actor.resource_type == "user":
            return self.actor.id
        statement = select(JobRecord.resource_owner_id).where(
            JobRecord.workspace_id == str(workspace_id),
            JobRecord.id == job_id,
            JobRecord.job_type.in_(
                (INTERVIEW_END_JOB_KIND, INTERVIEW_REPORT_JOB_KIND)
            ),
        )
        owner_id = await self._session.scalar(statement)
        if not isinstance(owner_id, str):
            raise PermissionError("Interview Job has no real legacy storage owner")
        return owner_id


class PostgresInterviewSessionPolicy:
    """@brief 在当前 PostgreSQL snapshot 中解析 Resume/Knowledge/model policy / Resolve Resume, Knowledge, and model policy in the current PostgreSQL snapshot."""

    def __init__(
        self,
        session: AsyncSession,
        scope: _PostgresInterviewScope,
        *,
        model_ref: ResourceRef,
        model_regions: frozenset[ModelRegion],
        allow_external_model_processing: bool,
    ) -> None:
        """@brief 绑定事务与部署 model policy / Bind the transaction and deployment model policy."""
        if model_ref.resource_type != "model" or model_ref.revision is None:
            raise ValueError("Interview model policy requires an exact model ref")
        if not model_regions:
            raise ValueError("Interview model policy requires at least one region")
        self._session = session
        self._scope = scope
        self._model_ref = model_ref
        self._model_regions = model_regions
        self._external = allow_external_model_processing

    async def authorize_session(
        self,
        request: InterviewSessionPolicyRequest,
    ) -> InterviewExecutionGrant:
        """@brief 解析 exact revisions 与 policy intersection / Resolve exact revisions and the policy intersection."""
        if self._scope.workspace_id != request.workspace_id:
            raise InterviewPolicyDenied("Interview policy requires an authorized Workspace")
        spec = request.spec
        if spec.inference.data_region not in self._model_regions:
            raise InterviewPolicyDenied("Interview model region is unavailable")
        external = self._external and spec.inference.allow_external_model_processing
        await self._validate_resume(request.workspace_id, spec)
        contexts = await self._resolve_knowledge(request.workspace_id, spec)
        grant = InterviewExecutionGrant(
            ResourceRef(
                "interview_scenario",
                request.scenario.meta.id,
                request.scenario.meta.revision,
            ),
            spec.resume_ref,
            spec.knowledge.agent_scope,
            self._model_ref,
            spec.inference.data_region,
            external,
            contexts,
            max((item.policy_version for item in contexts), default=1),
        )
        grant.validate_for(request.scenario, spec)
        return grant

    async def _validate_resume(
        self,
        workspace_id: WorkspaceId,
        spec: InterviewSessionSpec,
    ) -> None:
        """@brief 验证 exact Resume document revision / Validate an exact Resume document revision."""
        if spec.resume_ref is None:
            return
        revision = cast(int, spec.resume_ref.revision)
        exists = await self._session.scalar(
            select(literal(True))
            .select_from(ResumeRevisionRecord)
            .join(
                ResumeDocumentRecord,
                and_(
                    ResumeDocumentRecord.id == ResumeRevisionRecord.resume_id,
                    ResumeDocumentRecord.workspace_id == ResumeRevisionRecord.workspace_id,
                ),
            )
            .where(
                ResumeRevisionRecord.workspace_id == str(workspace_id),
                ResumeRevisionRecord.resume_id == spec.resume_ref.id,
                ResumeRevisionRecord.revision_no == revision,
                ResumeDocumentRecord.deleted_at.is_(None),
            )
        )
        if exists is not True:
            raise InterviewPolicyDenied("Interview Resume revision does not exist")

    async def _resolve_knowledge(
        self,
        workspace_id: WorkspaceId,
        spec: InterviewSessionSpec,
    ) -> tuple[InterviewKnowledgeContext, ...]:
        """@brief 解析同 Workspace ready versions 与 retrieve policy / Resolve same-Workspace ready versions and retrieve policy."""
        selection = spec.knowledge
        if selection.mode is KnowledgeSelectionMode.NONE:
            return ()
        statement = select(KnowledgeSourceRecord).where(
            KnowledgeSourceRecord.workspace_id == str(workspace_id),
            KnowledgeSourceRecord.enabled.is_(True),
            KnowledgeSourceRecord.deleted_at.is_(None),
            KnowledgeSourceRecord.current_version_id.is_not(None),
        )
        if selection.mode is KnowledgeSelectionMode.EXPLICIT:
            statement = statement.where(
                KnowledgeSourceRecord.id.in_(tuple(map(str, selection.include_source_ids)))
            )
        if selection.exclude_source_ids:
            statement = statement.where(
                KnowledgeSourceRecord.id.not_in(tuple(map(str, selection.exclude_source_ids)))
            )
        sources = list(
            (
                await self._session.scalars(
                    statement.order_by(KnowledgeSourceRecord.id).limit(201)
                )
            ).all()
        )
        if len(sources) > 200:
            raise InterviewPolicyDenied("Interview Knowledge selection exceeds its hard bound")
        pins = {str(pin.source_id): str(pin.version_id) for pin in selection.pinned_versions}
        contexts: list[InterviewKnowledgeContext] = []
        for source in sources:
            context = await self._knowledge_context(
                workspace_id,
                source,
                pins.get(source.id, source.current_version_id),
                spec,
            )
            if context is not None:
                contexts.append(context)
        if selection.mode is KnowledgeSelectionMode.EXPLICIT and {
            str(item.source_id) for item in contexts
        } != set(map(str, selection.include_source_ids)):
            raise InterviewPolicyDenied(
                "Interview explicit Knowledge selection is missing a ready authorized version"
            )
        return tuple(contexts)

    async def _knowledge_context(
        self,
        workspace_id: WorkspaceId,
        source: KnowledgeSourceRecord,
        version_id: str | None,
        spec: InterviewSessionSpec,
    ) -> InterviewKnowledgeContext | None:
        """@brief 判断单来源是否产生 authorized context / Decide whether one source yields an authorized context."""
        if version_id is None:
            return None
        version = await self._session.scalar(
            select(KnowledgeSourceVersionRecord).where(
                KnowledgeSourceVersionRecord.workspace_id == str(workspace_id),
                KnowledgeSourceVersionRecord.source_id == source.id,
                KnowledgeSourceVersionRecord.id == version_id,
                KnowledgeSourceVersionRecord.status == "ready",
            )
        )
        policy = await self._session.scalar(
            select(KnowledgeVisibilityPolicyRecord).where(
                KnowledgeVisibilityPolicyRecord.workspace_id == str(workspace_id),
                KnowledgeVisibilityPolicyRecord.source_id == source.id,
                KnowledgeVisibilityPolicyRecord.policy_version
                == source.current_policy_version,
            )
        )
        if version is None or policy is None:
            return None
        grants = list(
            (
                await self._session.scalars(
                    select(KnowledgeVisibilityGrantRecord).where(
                        KnowledgeVisibilityGrantRecord.workspace_id == str(workspace_id),
                        KnowledgeVisibilityGrantRecord.policy_id == policy.id,
                        KnowledgeVisibilityGrantRecord.agent_scope
                        == spec.knowledge.agent_scope,
                    )
                )
            ).all()
        )
        denied = any(
            grant.effect == "deny" and "retrieve" in grant.allowed_operations
            for grant in grants
        )
        explicitly_allowed = any(
            grant.effect == "allow" and "retrieve" in grant.allowed_operations
            for grant in grants
        )
        allowed = explicitly_allowed or (not grants and policy.default_effect == "allow")
        if (
            denied
            or not allowed
            or spec.inference.data_region.value not in policy.allowed_model_regions
            or (
                spec.inference.allow_external_model_processing
                and not policy.allow_external_model_processing
            )
        ):
            return None
        return InterviewKnowledgeContext(
            KnowledgeSourceId(source.id),
            KnowledgeSourceVersionId(version.id),
            policy.policy_version,
        )


def _scenario_from_record(record: InterviewScenarioRecord) -> InterviewScenario:
    """@brief 从 ORM row 重建 Scenario / Rebuild a Scenario from an ORM row."""
    return InterviewScenario(
        ResourceMeta(
            InterviewScenarioId(record.id),
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        WorkspaceId(record.workspace_id),
        _load(_SCENARIO_SPEC_ADAPTER, record.spec, "Interview Scenario spec"),
        InterviewScenarioStatus(record.status),
    )


def _session_from_record(record: InterviewSessionRecord) -> InterviewSession:
    """@brief 从 ORM row 重建 frozen Session / Rebuild a frozen Session from an ORM row."""
    from backend.domain.interview_v2 import InterviewSessionView

    spec = _load(_SESSION_SPEC_ADAPTER, record.spec, "Interview Session spec")
    grant = _load(
        _EXECUTION_GRANT_ADAPTER,
        record.execution_grant,
        "Interview execution grant",
    )
    view = InterviewSessionView(
        ResourceMeta(
            InterviewSessionId(record.id),
            record.revision,
            record.created_at,
            record.updated_at,
        ),
        WorkspaceId(record.workspace_id),
        InterviewScenarioId(record.scenario_id),
        spec.resume_ref,
        spec.job_target,
        InterviewSessionStatus(record.status),
        spec.locale,
        spec.media,
        spec.recording,
        record.started_at,
        record.ended_at,
        InterviewReportId(record.report_id) if record.report_id is not None else None,
    )
    return InterviewSession(
        view,
        spec,
        grant,
        JobId(record.pending_end_job_id) if record.pending_end_job_id is not None else None,
        EndInterviewReason(record.end_reason) if record.end_reason is not None else None,
    )


def _job_from_record(record: JobRecord) -> Job:
    """@brief 从统一 ORM row 重建 Job / Rebuild a Job from the unified ORM row."""
    progress = JobProgress(
        record.phase,
        record.completed_units,
        record.total_units,
        JobProgressUnit(record.progress_unit),
    )
    result_refs = _load(_RESOURCE_REFS_ADAPTER, record.result_refs, "Job result refs")
    problem = (
        _load(_PROBLEM_ADAPTER, record.problem, "Job problem")
        if record.problem is not None
        else None
    )
    return Job(
        ResourceMeta(JobId(record.id), record.revision, record.created_at, record.updated_at),
        WorkspaceId(record.workspace_id),
        record.job_type,
        ResourceRef(
            record.target_resource_type,
            record.target_resource_id,
            record.target_resource_revision,
        ),
        JobStatus(record.status),
        progress,
        result_refs,
        problem,
        record.started_at,
        record.finished_at,
    )


class PostgresInterviewRepository:
    """@brief transaction-bound PostgreSQL Interview repository / Transaction-bound PostgreSQL Interview repository."""

    def __init__(self, session: AsyncSession, scope: _PostgresInterviewScope) -> None:
        """@brief 绑定 Session 与显式 scope / Bind a Session and explicit scope."""
        self._session = session
        self._scope = scope

    async def list_scenarios(
        self, workspace_id: WorkspaceId, page: InterviewPageRequest
    ) -> InterviewPage[InterviewScenario]:
        """@brief 按稳定 keyset 列 Scenario / List Scenarios by a stable keyset."""
        await self._scope.ensure_workspace(workspace_id)
        statement = select(InterviewScenarioRecord).where(
            InterviewScenarioRecord.workspace_id == str(workspace_id)
        )
        after = _parse_time_position(page.after)
        if after is not None:
            after_at, after_id = after
            statement = statement.where(
                or_(
                    InterviewScenarioRecord.created_at > after_at,
                    and_(
                        InterviewScenarioRecord.created_at == after_at,
                        InterviewScenarioRecord.id > after_id,
                    ),
                )
            )
        rows = list(
            (
                await self._session.scalars(
                    statement.order_by(
                        InterviewScenarioRecord.created_at,
                        InterviewScenarioRecord.id,
                    ).limit(page.limit + 1)
                )
            ).all()
        )
        items = tuple(_scenario_from_record(item) for item in rows[: page.limit])
        next_position = (
            _time_position(items[-1].meta.created_at, str(items[-1].meta.id))
            if len(rows) > page.limit and items
            else None
        )
        return InterviewPage(items, next_position)

    async def get_scenario(
        self,
        workspace_id: WorkspaceId,
        scenario_id: InterviewScenarioId,
        *,
        for_update: bool = False,
    ) -> InterviewScenario | None:
        """@brief Workspace-first 读取或锁定 Scenario / Read or lock a Scenario workspace-first."""
        await self._scope.ensure_workspace(workspace_id)
        statement = select(InterviewScenarioRecord).where(
            InterviewScenarioRecord.workspace_id == str(workspace_id),
            InterviewScenarioRecord.id == str(scenario_id),
        )
        if for_update:
            statement = statement.with_for_update()
        row = await self._session.scalar(statement)
        return _scenario_from_record(row) if row is not None else None

    async def add_scenario(self, scenario: InterviewScenario) -> None:
        """@brief 添加 Scenario / Add a Scenario."""
        await self._scope.ensure_workspace(scenario.workspace_id)
        self._session.add(
            InterviewScenarioRecord(
                id=str(scenario.meta.id),
                workspace_id=str(scenario.workspace_id),
                spec=_dump_object(_SCENARIO_SPEC_ADAPTER, scenario.spec),
                status=scenario.status.value,
                created_at=scenario.meta.created_at,
                updated_at=scenario.meta.updated_at,
                revision=scenario.meta.revision,
                extensions={},
            )
        )

    async def save_scenario(
        self, scenario: InterviewScenario, *, expected_revision: int
    ) -> None:
        """@brief 单 SQL revision CAS 保存 Scenario / Save a Scenario with one-SQL revision CAS."""
        await self._scope.ensure_workspace(scenario.workspace_id)
        if scenario.meta.revision != expected_revision + 1:
            raise InterviewCasMismatch
        result = await self._session.execute(
            update(InterviewScenarioRecord)
            .where(
                InterviewScenarioRecord.workspace_id == str(scenario.workspace_id),
                InterviewScenarioRecord.id == str(scenario.meta.id),
                InterviewScenarioRecord.revision == expected_revision,
            )
            .values(
                spec=_dump_object(_SCENARIO_SPEC_ADAPTER, scenario.spec),
                status=scenario.status.value,
                updated_at=scenario.meta.updated_at,
                revision=scenario.meta.revision,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise InterviewCasMismatch

    async def list_sessions(
        self, workspace_id: WorkspaceId, page: InterviewPageRequest
    ) -> InterviewPage[InterviewSession]:
        """@brief 按稳定 keyset 列 Session / List Sessions by a stable keyset."""
        await self._scope.ensure_workspace(workspace_id)
        statement = select(InterviewSessionRecord).where(
            InterviewSessionRecord.workspace_id == str(workspace_id)
        )
        after = _parse_time_position(page.after)
        if after is not None:
            after_at, after_id = after
            statement = statement.where(
                or_(
                    InterviewSessionRecord.created_at > after_at,
                    and_(
                        InterviewSessionRecord.created_at == after_at,
                        InterviewSessionRecord.id > after_id,
                    ),
                )
            )
        rows = list(
            (
                await self._session.scalars(
                    statement.order_by(
                        InterviewSessionRecord.created_at,
                        InterviewSessionRecord.id,
                    ).limit(page.limit + 1)
                )
            ).all()
        )
        items = tuple(_session_from_record(item) for item in rows[: page.limit])
        next_position = (
            _time_position(items[-1].meta.created_at, str(items[-1].meta.id))
            if len(rows) > page.limit and items
            else None
        )
        return InterviewPage(items, next_position)

    async def get_session(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        for_update: bool = False,
    ) -> InterviewSession | None:
        """@brief Workspace-first 读取或锁定 Session / Read or lock a Session workspace-first."""
        await self._scope.ensure_workspace(workspace_id)
        statement = select(InterviewSessionRecord).where(
            InterviewSessionRecord.workspace_id == str(workspace_id),
            InterviewSessionRecord.id == str(session_id),
        )
        if for_update:
            statement = statement.with_for_update()
        row = await self._session.scalar(statement)
        return _session_from_record(row) if row is not None else None

    async def add_session(self, session: InterviewSession) -> None:
        """@brief 添加 frozen Session / Add a frozen Session."""
        await self._scope.ensure_workspace(session.workspace_id)
        self._session.add(
            InterviewSessionRecord(
                id=str(session.meta.id),
                workspace_id=str(session.workspace_id),
                scenario_id=str(session.spec.scenario_id),
                status=session.view.status.value,
                spec=_dump_object(_SESSION_SPEC_ADAPTER, session.spec),
                execution_grant=_dump_object(_EXECUTION_GRANT_ADAPTER, session.grant),
                report_id=(str(session.view.report_id) if session.view.report_id else None),
                pending_end_job_id=(
                    str(session.pending_end_job_id) if session.pending_end_job_id else None
                ),
                end_reason=session.end_reason.value if session.end_reason else None,
                next_realtime_sequence=1,
                next_transcript_sequence=1,
                started_at=session.view.started_at,
                ended_at=session.view.ended_at,
                created_at=session.meta.created_at,
                updated_at=session.meta.updated_at,
                revision=session.meta.revision,
                extensions={},
            )
        )

    async def save_session(
        self, session: InterviewSession, *, expected_revision: int
    ) -> None:
        """@brief 单 SQL revision CAS 保存 Session / Save a Session with one-SQL revision CAS."""
        await self._scope.ensure_workspace(session.workspace_id)
        if session.meta.revision != expected_revision + 1:
            raise InterviewCasMismatch
        result = await self._session.execute(
            update(InterviewSessionRecord)
            .where(
                InterviewSessionRecord.workspace_id == str(session.workspace_id),
                InterviewSessionRecord.id == str(session.meta.id),
                InterviewSessionRecord.revision == expected_revision,
            )
            .values(
                status=session.view.status.value,
                report_id=(str(session.view.report_id) if session.view.report_id else None),
                pending_end_job_id=(
                    str(session.pending_end_job_id) if session.pending_end_job_id else None
                ),
                end_reason=session.end_reason.value if session.end_reason else None,
                started_at=session.view.started_at,
                ended_at=session.view.ended_at,
                updated_at=session.meta.updated_at,
                revision=session.meta.revision,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise InterviewCasMismatch

    async def add_connection_lease(self, lease: RealtimeConnectionLease) -> None:
        """@brief 保存无 token/ICE secret 的 lease / Save a lease without token or ICE secrets."""
        await self._scope.ensure_workspace(lease.workspace_id)
        self._session.add(
            InterviewRealtimeConnectionRecord(
                id=str(lease.id),
                workspace_id=str(lease.workspace_id),
                session_id=str(lease.session_id),
                audience_type=lease.audience.resource_type,
                audience_id=lease.audience.id,
                audience_revision=lease.audience.revision,
                transport=lease.transport.value,
                expires_at=lease.expires_at,
                created_at=lease.issued_at,
                updated_at=lease.issued_at,
                revision=1,
                extensions={},
            )
        )

    async def get_connection_lease(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        connection_id: RealtimeConnectionId,
    ) -> RealtimeConnectionLease | None:
        """@brief 以 Workspace+Session+Connection 读取 lease / Read a lease by Workspace+Session+Connection."""
        await self._scope.ensure_workspace(workspace_id)
        row = await self._session.scalar(
            select(InterviewRealtimeConnectionRecord).where(
                InterviewRealtimeConnectionRecord.workspace_id == str(workspace_id),
                InterviewRealtimeConnectionRecord.session_id == str(session_id),
                InterviewRealtimeConnectionRecord.id == str(connection_id),
            )
        )
        if row is None:
            return None
        return RealtimeConnectionLease(
            RealtimeConnectionId(row.id),
            WorkspaceId(row.workspace_id),
            InterviewSessionId(row.session_id),
            ResourceRef(row.audience_type, row.audience_id, row.audience_revision),
            RealtimeTransport(row.transport),
            row.created_at,
            row.expires_at,
        )

    async def append_realtime_input(
        self, record: RealtimeInputLedgerRecord
    ) -> RealtimeInputReceipt:
        """@brief 在 Session row lock 下幂等追加并分配 sequence / Idempotently append and allocate a sequence under the Session row lock."""
        await self._scope.ensure_workspace(record.workspace_id)
        session_row = await self._session.scalar(
            select(InterviewSessionRecord)
            .where(
                InterviewSessionRecord.workspace_id == str(record.workspace_id),
                InterviewSessionRecord.id == str(record.session_id),
            )
            .with_for_update()
        )
        if session_row is None:
            raise InterviewCasMismatch
        prior = await self._session.scalar(
            select(InterviewEventRecord).where(
                InterviewEventRecord.workspace_id == str(record.workspace_id),
                InterviewEventRecord.session_id == str(record.session_id),
                InterviewEventRecord.id == str(record.input_id),
            )
        )
        if prior is not None:
            if prior.fingerprint_sha256 != record.fingerprint_sha256:
                raise RealtimeInputKeyReused
            return RealtimeInputReceipt(prior.sequence, True)
        sequence = session_row.next_realtime_sequence
        updated = await self._session.execute(
            update(InterviewSessionRecord)
            .where(
                InterviewSessionRecord.workspace_id == str(record.workspace_id),
                InterviewSessionRecord.id == str(record.session_id),
            )
            .values(
                next_realtime_sequence=sequence + 1,
                updated_at=InterviewSessionRecord.updated_at,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(updated) != 1:
            raise InterviewCasMismatch
        self._session.add(
            InterviewEventRecord(
                id=str(record.input_id),
                workspace_id=str(record.workspace_id),
                session_id=str(record.session_id),
                connection_id=str(record.connection_id),
                sequence=sequence,
                fingerprint_sha256=record.fingerprint_sha256,
                occurred_at=record.occurred_at,
                created_at=record.occurred_at,
                updated_at=record.occurred_at,
                revision=1,
                extensions={},
            )
        )
        return RealtimeInputReceipt(sequence, False)

    async def allocate_transcript_sequence(
        self, workspace_id: WorkspaceId, session_id: InterviewSessionId
    ) -> TranscriptSequenceReservation:
        """@brief 在 Session row lock 下保留 Transcript sequence / Reserve a Transcript sequence under the Session row lock."""
        await self._scope.ensure_workspace(workspace_id)
        row = await self._session.scalar(
            select(InterviewSessionRecord)
            .where(
                InterviewSessionRecord.workspace_id == str(workspace_id),
                InterviewSessionRecord.id == str(session_id),
            )
            .with_for_update()
        )
        if row is None:
            raise InterviewCasMismatch
        sequence = row.next_transcript_sequence
        updated = await self._session.execute(
            update(InterviewSessionRecord)
            .where(
                InterviewSessionRecord.workspace_id == str(workspace_id),
                InterviewSessionRecord.id == str(session_id),
            )
            .values(
                next_transcript_sequence=sequence + 1,
                updated_at=InterviewSessionRecord.updated_at,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(updated) != 1:
            raise InterviewCasMismatch
        return TranscriptSequenceReservation(sequence)

    async def add_transcript_segment(self, segment: TranscriptSegment) -> None:
        """@brief 添加 immutable provenance-bound segment / Add an immutable provenance-bound segment."""
        await self._scope.ensure_workspace(segment.workspace_id)
        persisted_at = datetime.now(UTC)
        source_input_id: str | None = None
        source_artifact_id: str | None = None
        source_artifact_revision: int | None = None
        if segment.source_ref.resource_type == "realtime_input":
            source_input_id = segment.source_ref.id
        else:
            source_artifact_id = segment.source_ref.id
            source_artifact_revision = segment.source_ref.revision
        self._session.add(
            TranscriptSegmentRecord(
                id=str(segment.id),
                workspace_id=str(segment.workspace_id),
                session_id=str(segment.session_id),
                sequence=segment.sequence,
                speaker=segment.speaker.value,
                start_ms=segment.start_ms,
                end_ms=segment.end_ms,
                text_content=segment.text,
                source_input_id=source_input_id,
                source_artifact_id=source_artifact_id,
                source_artifact_revision=source_artifact_revision,
                created_at=persisted_at,
                updated_at=persisted_at,
                revision=1,
                extensions={},
            )
        )

    async def list_transcript(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        page: InterviewPageRequest,
    ) -> InterviewPage[TranscriptSegment]:
        """@brief 按稳定 sequence+ID keyset 列 Transcript / List Transcript by a stable sequence-plus-ID keyset."""
        await self._scope.ensure_workspace(workspace_id)
        statement = select(TranscriptSegmentRecord).where(
            TranscriptSegmentRecord.workspace_id == str(workspace_id),
            TranscriptSegmentRecord.session_id == str(session_id),
        )
        after = _parse_sequence_position(page.after)
        if after is not None:
            after_sequence, after_id = after
            statement = statement.where(
                or_(
                    TranscriptSegmentRecord.sequence > after_sequence,
                    and_(
                        TranscriptSegmentRecord.sequence == after_sequence,
                        TranscriptSegmentRecord.id > after_id,
                    ),
                )
            )
        rows = list(
            (
                await self._session.scalars(
                    statement.order_by(
                        TranscriptSegmentRecord.sequence,
                        TranscriptSegmentRecord.id,
                    ).limit(page.limit + 1)
                )
            ).all()
        )
        items = tuple(self._transcript(row) for row in rows[: page.limit])
        next_position = (
            _sequence_position(items[-1].sequence, str(items[-1].id))
            if len(rows) > page.limit and items
            else None
        )
        return InterviewPage(items, next_position)

    async def load_transcript_snapshot(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        maximum_segments: int,
    ) -> tuple[TranscriptSegment, ...]:
        """@brief 一致 snapshot 内加载有界完整 Transcript / Load a bounded complete Transcript in the current snapshot."""
        await self._scope.ensure_workspace(workspace_id)
        rows = list(
            (
                await self._session.scalars(
                    select(TranscriptSegmentRecord)
                    .where(
                        TranscriptSegmentRecord.workspace_id == str(workspace_id),
                        TranscriptSegmentRecord.session_id == str(session_id),
                    )
                    .order_by(
                        TranscriptSegmentRecord.sequence,
                        TranscriptSegmentRecord.id,
                    )
                    .limit(maximum_segments + 1)
                )
            ).all()
        )
        if len(rows) > maximum_segments:
            raise ValueError("Transcript exceeds the worker snapshot bound")
        return tuple(self._transcript(row) for row in rows)

    @staticmethod
    def _transcript(row: TranscriptSegmentRecord) -> TranscriptSegment:
        """@brief 从 row 重建 Transcript segment / Rebuild a Transcript segment from a row."""
        source = (
            ResourceRef("realtime_input", row.source_input_id)
            if row.source_input_id is not None
            else ResourceRef(
                "artifact",
                cast(str, row.source_artifact_id),
                cast(int, row.source_artifact_revision),
            )
        )
        return TranscriptSegment(
            TranscriptSegmentId(row.id),
            WorkspaceId(row.workspace_id),
            InterviewSessionId(row.session_id),
            row.sequence,
            source,
            TranscriptSpeaker(row.speaker),
            row.start_ms,
            row.end_ms,
            row.text_content,
        )

    async def get_report(
        self, workspace_id: WorkspaceId, report_id: InterviewReportId
    ) -> InterviewReport | None:
        """@brief 读取 immutable Report / Read an immutable Report."""
        await self._scope.ensure_workspace(workspace_id)
        row = await self._session.scalar(
            select(InterviewReportRecord).where(
                InterviewReportRecord.workspace_id == str(workspace_id),
                InterviewReportRecord.id == str(report_id),
            )
        )
        if row is None:
            return None
        return InterviewReport(
            ResourceMeta(
                InterviewReportId(row.id),
                row.revision,
                row.created_at,
                row.updated_at,
            ),
            WorkspaceId(row.workspace_id),
            InterviewSessionId(row.session_id),
            _load(_REPORT_DRAFT_ADAPTER, row.draft, "Interview Report draft"),
            row.generated_at,
        )

    async def add_report(self, report: InterviewReport) -> None:
        """@brief 添加 Report 与同 Session evidence integrity rows / Add a Report and same-Session evidence-integrity rows."""
        await self._scope.ensure_workspace(report.workspace_id)
        self._session.add(
            InterviewReportRecord(
                id=str(report.meta.id),
                workspace_id=str(report.workspace_id),
                session_id=str(report.session_id),
                draft=_dump_object(_REPORT_DRAFT_ADAPTER, report.draft),
                generated_at=report.generated_at,
                created_at=report.meta.created_at,
                updated_at=report.meta.updated_at,
                revision=report.meta.revision,
                extensions={},
            )
        )
        for score in report.draft.rubric_scores:
            for evidence in score.evidence:
                self._session.add(
                    InterviewReportEvidenceRecord(
                        id=new_opaque_id("evidence"),
                        workspace_id=str(report.workspace_id),
                        report_id=str(report.meta.id),
                        session_id=str(report.session_id),
                        segment_id=str(evidence.segment_id),
                        dimension_id=score.dimension_id,
                        start_ms=evidence.start_ms,
                        end_ms=evidence.end_ms,
                        created_at=report.generated_at,
                    )
                )

    async def has_live_report_job(
        self, workspace_id: WorkspaceId, session_id: InterviewSessionId
    ) -> bool:
        """@brief 查询统一 Job truth 的 live report job / Query the unified Job truth for a live Report Job."""
        await self._scope.ensure_workspace(workspace_id)
        count = await self._session.scalar(
            select(func.count())
            .select_from(JobRecord)
            .where(
                JobRecord.workspace_id == str(workspace_id),
                JobRecord.job_type == INTERVIEW_REPORT_JOB_KIND,
                JobRecord.target_resource_type == "interview_session",
                JobRecord.target_resource_id == str(session_id),
                JobRecord.status.in_(("queued", "running")),
            )
        )
        return bool(count)


class _PostgresInterviewJobs:
    """@brief 复用统一 ``agent.jobs`` 的 Interview adapter / Interview adapter reusing unified ``agent.jobs``."""

    def __init__(self, session: AsyncSession, scope: _PostgresInterviewScope) -> None:
        """@brief 绑定事务与显式 actor scope / Bind the transaction and explicit actor scope."""
        self._session = session
        self._scope = scope

    async def add(self, job: Job, spec: InterviewJobSpec) -> None:
        """@brief 同事务添加统一 Job 与 immutable typed binding / Add a unified Job and immutable typed binding in one transaction.

        @param job 必须是新建 queued Interview Job / A newly queued Interview Job.
        @param spec worker 私有类型化参数 / Private typed worker parameters.
        @return 无返回值 / No return value.
        """
        actor_id = self._scope.require_authorized_user(job.workspace_id)
        if (
            job.meta.revision != 1
            or job.meta.created_at != job.meta.updated_at
            or job.status is not JobStatus.QUEUED
            or job.subject.resource_type != "interview_session"
            or job.subject.id != spec.session_id
            or (
                job.kind == INTERVIEW_END_JOB_KIND
                and not isinstance(spec, EndSessionJobSpec)
            )
            or (
                job.kind == INTERVIEW_REPORT_JOB_KIND
                and not isinstance(spec, ReportJobSpec)
            )
            or job.kind not in {INTERVIEW_END_JOB_KIND, INTERVIEW_REPORT_JOB_KIND}
        ):
            raise ValueError("Interview Job and worker spec are not aligned")
        progress = job.progress
        record = JobRecord(
            id=str(job.meta.id),
            workspace_id=str(job.workspace_id),
            resource_owner_id=str(actor_id),
            job_type=job.kind,
            status=job.status.value,
            phase="queued" if progress is None else progress.phase,
            completed_units=0 if progress is None else progress.completed,
            total_units=None if progress is None else progress.total,
            progress_unit=(
                JobProgressUnit.UNKNOWN.value if progress is None else progress.unit.value
            ),
            target_resource_type=job.subject.resource_type,
            target_resource_id=job.subject.id,
            target_resource_revision=job.subject.revision,
            result_refs=_dump_array(_RESOURCE_REFS_ADAPTER, job.result_refs),
            problem=(
                None
                if job.problem is None
                else _dump_object(_PROBLEM_ADAPTER, job.problem)
            ),
            started_at=job.started_at,
            finished_at=job.finished_at,
            request_payload={
                "subject": _dump_object(_RESOURCE_REF_ADAPTER, job.subject),
                "spec": _dump_object(_JOB_SPEC_ADAPTER, spec),
            },
            created_at=job.meta.created_at,
            updated_at=job.meta.updated_at,
            revision=job.meta.revision,
            extensions={},
        )
        self._session.add(record)
        # binding 继承 non-deferrable legacy FK；先 flush Job，但仍保留外层事务原子性。
        # The binding inherits a non-deferrable legacy FK; flush Job within the same transaction.
        await self._session.flush((record,))
        self._session.add(
            InterviewReportJobRecord(
                id=new_opaque_id("sessionjob"),
                workspace_id=str(job.workspace_id),
                job_id=str(job.meta.id),
                session_id=str(spec.session_id),
                job_kind=job.kind,
                created_at=job.meta.created_at,
                updated_at=job.meta.created_at,
                revision=1,
                extensions={},
            )
        )

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief Workspace-first 读取或锁定统一 Job / Read or lock a unified Job Workspace-first."""
        await self._scope.ensure_workspace(workspace_id)
        statement = select(JobRecord).where(
            JobRecord.workspace_id == str(workspace_id),
            JobRecord.id == str(job_id),
            JobRecord.job_type.in_((INTERVIEW_END_JOB_KIND, INTERVIEW_REPORT_JOB_KIND)),
        )
        if for_update:
            statement = statement.with_for_update()
        row = await self._session.scalar(statement)
        return _job_from_record(row) if row is not None else None

    async def get_owned(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 按 durable creator 精确读取或锁定 Job / Read or lock a Job by its durable creator."""
        await self._scope.ensure_workspace(workspace_id)
        statement = select(JobRecord).where(
            JobRecord.workspace_id == str(workspace_id),
            JobRecord.resource_owner_id == str(actor_id),
            JobRecord.id == str(job_id),
            JobRecord.job_type.in_((INTERVIEW_END_JOB_KIND, INTERVIEW_REPORT_JOB_KIND)),
        )
        if for_update:
            statement = statement.with_for_update()
        row = await self._session.scalar(statement)
        return _job_from_record(row) if row is not None else None

    async def save(self, job: Job, *, expected_revision: int) -> None:
        """@brief affected-row CAS 保存统一 Job / Save the unified Job with affected-row CAS."""
        await self._scope.ensure_workspace(job.workspace_id)
        if (
            job.kind not in {INTERVIEW_END_JOB_KIND, INTERVIEW_REPORT_JOB_KIND}
            or job.subject.resource_type != "interview_session"
            or job.meta.revision != expected_revision + 1
        ):
            raise InterviewCasMismatch
        progress = job.progress
        result = await self._session.execute(
            update(JobRecord)
            .where(
                JobRecord.workspace_id == str(job.workspace_id),
                JobRecord.id == str(job.meta.id),
                JobRecord.job_type == job.kind,
                JobRecord.target_resource_type == job.subject.resource_type,
                JobRecord.target_resource_id == job.subject.id,
                JobRecord.revision == expected_revision,
            )
            .values(
                status=job.status.value,
                phase="queued" if progress is None else progress.phase,
                completed_units=0 if progress is None else progress.completed,
                total_units=None if progress is None else progress.total,
                progress_unit=(
                    JobProgressUnit.UNKNOWN.value
                    if progress is None
                    else progress.unit.value
                ),
                result_refs=_dump_array(_RESOURCE_REFS_ADAPTER, job.result_refs),
                problem=(
                    None
                    if job.problem is None
                    else _dump_object(_PROBLEM_ADAPTER, job.problem)
                ),
                started_at=job.started_at,
                finished_at=job.finished_at,
                revision=job.meta.revision,
                updated_at=job.meta.updated_at,
            )
            .execution_options(synchronize_session=False)
        )
        if _affected_rows(result) != 1:
            raise InterviewCasMismatch


class _PostgresInterviewArtifacts:
    """@brief 复用统一 Artifact metadata 真相的 adapter / Adapter reusing the unified Artifact metadata truth."""

    def __init__(self, session: AsyncSession, scope: _PostgresInterviewScope) -> None:
        """@brief 绑定事务与 service/user scope / Bind the transaction and service/user scope."""
        self._session = session
        self._scope = scope

    async def add(self, artifact: Artifact) -> None:
        """@brief 添加不含签名 URL 的统一 Artifact metadata / Add unified Artifact metadata without a signed URL.

        @note 外部 provider 的临时签名地址绝不持久化；worker 必须先把内容导入受管
            Artifact storage 并返回同源 ``ApiArtifactContentUrl``。
            / Temporary provider-signed URLs are never persisted; the worker must first import
            content into managed Artifact storage and return an ``ApiArtifactContentUrl``.
        """
        await self._scope.ensure_workspace(artifact.workspace_id)
        if (
            artifact.meta.revision != 1
            or artifact.meta.created_at != artifact.meta.updated_at
            or not isinstance(artifact.content_location, ApiArtifactContentUrl)
            or artifact.subject.resource_type != "interview_session"
        ):
            raise ValueError("Interview Artifact must be immutable, managed, and Session-bound")
        self._session.add(
            ArtifactRecord(
                id=str(artifact.meta.id),
                workspace_id=str(artifact.workspace_id),
                kind=artifact.kind.value,
                subject_type=artifact.subject.resource_type,
                subject_id=artifact.subject.id,
                subject_revision=artifact.subject.revision,
                media_type=artifact.media_type,
                size_bytes=artifact.size_bytes,
                sha256=artifact.sha256,
                storage_key=(
                    f"interview/{artifact.workspace_id}/{artifact.meta.id}"
                ),
                page_count=artifact.page_count,
                expires_at=artifact.expires_at,
                deleted_at=None,
                created_at=artifact.meta.created_at,
                updated_at=artifact.meta.updated_at,
                revision=artifact.meta.revision,
                extensions={},
            )
        )


class _PostgresInterviewOutbox:
    """@brief 统一 transactional outbox 的 Interview producer / Interview producer for the unified transactional outbox."""

    def __init__(
        self,
        session: AsyncSession,
        scope: _PostgresInterviewScope,
        retention: timedelta,
    ) -> None:
        """@brief 绑定事务、actor scope 与 replay retention / Bind transaction, actor scope, and replay retention."""
        self._session = session
        self._scope = scope
        self._retention = retention

    async def add(self, record: InterviewJobQueuedRecord) -> None:
        """@brief 原子追加 secret-free ``interview.job.queued`` / Atomically append a secret-free ``interview.job.queued`` event."""
        actor_id = self._scope.require_authorized_user(record.workspace_id)
        if (
            record.actor_id != actor_id
            or record.session_ref.resource_type != "interview_session"
            or record.job_ref.resource_type != "job"
            or record.job_ref.revision is None
        ):
            raise ValueError("Interview outbox record has invalid subjects")
        await append_workspace_outbox_event(
            self._session,
            event_id=ApiEventId(record.id),
            workspace_id=record.workspace_id,
            resource_owner_id=str(actor_id),
            subject=record.job_ref,
            event_type=record.kind,
            occurred_at=record.occurred_at,
            data=record.as_payload(),
            trace_id=None,
            replay_expires_at=record.occurred_at + self._retention,
        )


class _PostgresInterviewAudit:
    """@brief append-only 统一 AuditEvent sink / Append-only unified AuditEvent sink."""

    def __init__(self, session: AsyncSession, scope: _PostgresInterviewScope) -> None:
        """@brief 绑定事务与真实 actor scope / Bind transaction and real actor scope."""
        self._session = session
        self._scope = scope

    async def add(self, event: AuditEvent) -> None:
        """@brief 同业务 transaction 写入真实 user/service actor / Write the real user/service actor in the business transaction."""
        await self._scope.ensure_workspace(event.workspace_id)
        if self._scope.actor != event.actor:
            raise PermissionError("Interview AuditEvent actor does not match transaction scope")
        owner_id = await self._scope.legacy_owner_id(
            event.workspace_id,
            job_id=event.request_id,
        )
        self._session.add(
            AuditEventRecord(
                id=str(event.id),
                workspace_id=str(event.workspace_id),
                resource_owner_id=owner_id,
                occurred_at=event.occurred_at,
                actor_type=event.actor.resource_type,
                actor_id=event.actor.id,
                actor_revision=event.actor.revision,
                action=event.action,
                resource_type=event.target.resource_type,
                resource_id=event.target.id,
                resource_revision=event.target.revision,
                request_id=event.request_id,
                outcome=event.outcome.value,
                details={},
                created_at=event.occurred_at,
                updated_at=event.occurred_at,
                revision=1,
                extensions={},
            )
        )


class PostgresInterviewUnitOfWork:
    """@brief 一个 PostgreSQL Interview 短事务 UoW / One PostgreSQL Interview short-transaction UoW."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        model_ref: ResourceRef,
        model_regions: frozenset[ModelRegion],
        allow_external_model_processing: bool,
        service_actor: ResourceRef,
        retention: timedelta,
    ) -> None:
        """@brief 绑定数据库、部署策略与显式 service actor / Bind database, deployment policy, and explicit service actor."""
        self._database = database
        self._model_ref = model_ref
        self._model_regions = model_regions
        self._allow_external = allow_external_model_processing
        self._service_actor = service_actor
        self._retention = retention
        self._session: AsyncSession | None = None
        self._transaction: AsyncSessionTransaction | None = None
        self._authorizer: _TrackingInterviewAuthorizer | None = None
        self._policy: PostgresInterviewSessionPolicy | None = None
        self._repository: PostgresInterviewRepository | None = None
        self._jobs: _PostgresInterviewJobs | None = None
        self._artifacts: _PostgresInterviewArtifacts | None = None
        self._outbox: _PostgresInterviewOutbox | None = None
        self._audit: _PostgresInterviewAudit | None = None
        self._committed = False
        self._rolled_back = False

    @property
    def authorizer(self) -> _TrackingInterviewAuthorizer:
        """@brief 返回集中权限 adapter / Return the centralized-permission adapter."""
        if self._authorizer is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._authorizer

    @property
    def policy(self) -> PostgresInterviewSessionPolicy:
        """@brief 返回 snapshot-bound Session policy / Return the snapshot-bound Session policy."""
        if self._policy is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._policy

    @property
    def repository(self) -> PostgresInterviewRepository:
        """@brief 返回 transaction-bound repository / Return the transaction-bound repository."""
        if self._repository is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._repository

    @property
    def jobs(self) -> _PostgresInterviewJobs:
        """@brief 返回统一 Job adapter / Return the unified Job adapter."""
        if self._jobs is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._jobs

    @property
    def artifacts(self) -> _PostgresInterviewArtifacts:
        """@brief 返回统一 Artifact adapter / Return the unified Artifact adapter."""
        if self._artifacts is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._artifacts

    @property
    def outbox(self) -> _PostgresInterviewOutbox:
        """@brief 返回统一 outbox adapter / Return the unified-outbox adapter."""
        if self._outbox is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._outbox

    @property
    def audit(self) -> _PostgresInterviewAudit:
        """@brief 返回统一 audit sink / Return the unified-audit sink."""
        if self._audit is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._audit

    async def __aenter__(self) -> Self:
        """@brief 创建独占 Session/transaction 并组装 adapters / Create an exclusive Session/transaction and assemble adapters."""
        if self._session is not None or self._committed or self._rolled_back:
            raise RuntimeError("Interview unit of work cannot be re-entered")
        session = self._database.new_session()
        self._session = session
        self._transaction = await session.begin()
        scope = _PostgresInterviewScope(self._database, session, self._service_actor)
        self._authorizer = _TrackingInterviewAuthorizer(
            AccessAuthorizer(PostgresAccessRepository(session)),
            scope.bind_user,
        )
        self._policy = PostgresInterviewSessionPolicy(
            session,
            scope,
            model_ref=self._model_ref,
            model_regions=self._model_regions,
            allow_external_model_processing=self._allow_external,
        )
        self._repository = PostgresInterviewRepository(session, scope)
        self._jobs = _PostgresInterviewJobs(session, scope)
        self._artifacts = _PostgresInterviewArtifacts(session, scope)
        self._outbox = _PostgresInterviewOutbox(session, scope, self._retention)
        self._audit = _PostgresInterviewAudit(session, scope)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时回滚并关闭 Session / Roll back on error or absent commit, then close the Session."""
        del exc, traceback
        if self._session is not None:
            if exc_type is not None or not self._committed:
                await self.rollback()
            await self._session.close()
        self._session = None
        self._transaction = None
        self._authorizer = None
        self._policy = None
        self._repository = None
        self._jobs = None
        self._artifacts = None
        self._outbox = None
        self._audit = None
        return None

    async def commit(self) -> None:
        """@brief 原子提交聚合、统一平台记录、outbox 与 audit / Atomically commit aggregates, unified platform records, outbox, and audit."""
        session, transaction = self._require_active()
        if self._committed:
            raise RuntimeError("Interview unit of work is already committed")
        if self._rolled_back:
            raise RuntimeError("rolled-back Interview unit of work cannot commit")
        await session.flush()
        await transaction.commit()
        self._committed = True

    async def rollback(self) -> None:
        """@brief 幂等回滚活动 transaction / Idempotently roll back the active transaction."""
        if self._transaction is not None and self._transaction.is_active:
            await self._transaction.rollback()
        self._rolled_back = True

    def _require_active(self) -> tuple[AsyncSession, AsyncSessionTransaction]:
        """@brief 要求活动 Session/transaction / Require an active Session/transaction."""
        if self._session is None or self._transaction is None:
            raise RuntimeError("Interview unit of work has not been entered")
        return self._session, self._transaction


class PostgresInterviewUnitOfWorkFactory:
    """@brief 组装 PostgreSQL Interview adapters / Assemble PostgreSQL Interview adapters."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        model_ref: ResourceRef,
        model_regions: frozenset[ModelRegion],
        allow_external_model_processing: bool,
        service_actor: ResourceRef,
        retention: timedelta = _EVENT_RETENTION,
    ) -> None:
        """@brief 固定部署模型、region、数据外发与 worker identity policy / Pin deployment model, region, external-processing, and worker-identity policy.

        @param database 共享 PostgreSQL 资源 / Shared PostgreSQL resource.
        @param model_ref 精确模型 revision / Exact model revision.
        @param model_regions 部署可用 region / Deployment-available regions.
        @param allow_external_model_processing 部署是否允许外发 / Whether deployment permits external processing.
        @param service_actor 显式真实 worker identity / Explicit real worker identity.
        @param retention outbox replay window / Outbox replay window.
        """
        if model_ref.resource_type != "model" or model_ref.revision is None:
            raise ValueError("Interview model policy requires an exact model ref")
        if not model_regions:
            raise ValueError("Interview model policy requires at least one region")
        if service_actor.resource_type != "service" or service_actor.revision is not None:
            raise ValueError("Interview service actor must be an unversioned service ref")
        if retention <= timedelta(0):
            raise ValueError("Interview outbox retention must be positive")
        self._database = database
        self._model_ref = model_ref
        self._model_regions = model_regions
        self._allow_external = allow_external_model_processing
        self._service_actor = service_actor
        self._retention = retention

    def __call__(self) -> PostgresInterviewUnitOfWork:
        """@brief 创建未进入的 PostgreSQL UoW / Create a not-yet-entered PostgreSQL UoW."""
        return PostgresInterviewUnitOfWork(
            self._database,
            model_ref=self._model_ref,
            model_regions=self._model_regions,
            allow_external_model_processing=self._allow_external,
            service_actor=self._service_actor,
            retention=self._retention,
        )


__all__ = [
    "ConsentAwareInterviewMediaFinalizer",
    "FailClosedInterviewMediaFinalizer",
    "FailClosedInterviewReportProvider",
    "HmacInterviewRealtimeGateway",
    "InMemoryInterviewStore",
    "InMemoryInterviewUnitOfWork",
    "InMemoryInterviewUnitOfWorkFactory",
    "InterviewRealtimeSigningKey",
    "InterviewRealtimeSigningKeyring",
    "PostgresInterviewRepository",
    "PostgresInterviewSessionPolicy",
    "PostgresInterviewUnitOfWork",
    "PostgresInterviewUnitOfWorkFactory",
    "StaticInterviewSessionPolicy",
]
