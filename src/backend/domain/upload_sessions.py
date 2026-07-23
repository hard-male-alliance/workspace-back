"""@brief API v2 直传 UploadSession 领域模型 / API v2 direct-upload session domain models.

UploadSession 把客户端声明、对象存储观测与安全扫描结果分开。只有 size、SHA-256、
MIME sniff、恶意内容、解压膨胀和配额检查全部成功后才能进入 ``completed``；完成后的
内容只能由一个下游资源（例如 Knowledge version 或 Resume import Job）原子领取。
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Literal, NewType
from urllib.parse import urlsplit

from backend.domain.principals import DomainInvariantError, WorkspaceId
from backend.domain.resources import ResourceRef

UploadSessionId = NewType("UploadSessionId", str)
"""@brief UploadSession 不透明标识 / Opaque UploadSession identifier."""

UploadVerificationId = NewType("UploadVerificationId", str)
"""@brief 崩溃可恢复的验证操作标识 / Crash-resumable upload-verification operation identifier."""

_OPAQUE_ID_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API v2 不透明标识语法 / API v2 opaque-identifier grammar."""

_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
"""@brief 小写 SHA-256 语法 / Lower-case SHA-256 grammar."""

_STABLE_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief 稳定失败 code 语法 / Stable failure-code grammar."""

MAX_UPLOAD_BYTES = 1_073_741_824
"""@brief 契约冻结的单次上传上限 / Contract-frozen per-upload byte limit."""


class UploadDomainError(DomainInvariantError):
    """@brief UploadSession 领域不变量错误 / UploadSession-domain invariant error."""


class UploadTransitionError(UploadDomainError):
    """@brief UploadSession 状态机拒绝迁移 / UploadSession state machine rejected a transition."""


class UploadClaimError(UploadDomainError):
    """@brief 上传内容已被领取或不可领取 / Uploaded content is already claimed or unclaimable."""


class UploadStatus(StrEnum):
    """@brief 契约冻结的 UploadSession 状态 / Contract-frozen UploadSession states."""

    CREATED = "created"
    UPLOADED = "uploaded"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"

    @property
    def is_terminal(self) -> bool:
        """@brief 判断状态是否终态 / Test whether the status is terminal.

        @return completed、failed 或 expired 时为真 / True for completed, failed, or expired.
        """
        return self in {UploadStatus.COMPLETED, UploadStatus.FAILED, UploadStatus.EXPIRED}


@dataclass(frozen=True, slots=True)
class UploadDeclaration:
    """@brief 创建 session 时冻结的客户端内容声明 / Client content declaration frozen at creation.

    @param filename 仅作显示与 MIME 策略输入的原始文件名 / Original filename used only for display and MIME policy.
    @param media_type 声明 MIME；不能被安全逻辑信任 / Declared MIME, never trusted for security.
    @param size_bytes 声明字节数 / Declared byte count.
    @param sha256 声明内容摘要 / Declared content digest.
    """

    filename: str
    media_type: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        """@brief 校验上传声明边界 / Validate upload declaration bounds.

        @raise UploadDomainError 字段不满足 v2 Schema 时抛出 / Raised when fields violate v2.
        """
        _require_text(self.filename, "upload filename", 1, 300)
        _require_text(self.media_type, "upload media type", 3, 200)
        _require_size(self.size_bytes)
        _require_sha256(self.sha256, "upload declared SHA-256")


@dataclass(frozen=True, slots=True)
class UploadGrant:
    """@brief 对象存储签发的短期直传授权 / Short-lived direct-upload grant issued by storage.

    @param upload_url HTTPS 或固定隔离测试 Origin 的签名 URL / Signed HTTPS or fixed isolated-test URL.
    @param required_headers 客户端 PUT 必须原样发送的 headers / Headers the client must send verbatim.
    @param method 契约固定为 PUT / Method fixed by contract to PUT.
    """

    upload_url: str = field(repr=False)
    required_headers: Mapping[str, str] = field(repr=False)
    method: Literal["PUT"] = "PUT"

    def __post_init__(self) -> None:
        """@brief 校验并冻结直传授权 / Validate and freeze the direct-upload grant.

        @raise UploadDomainError URL、method 或 header 非法时抛出 / Raised for invalid grant fields.
        """
        _require_upload_url(self.upload_url)
        if self.method != "PUT":
            raise UploadDomainError("direct upload method must be PUT")
        if len(self.required_headers) > 20:
            raise UploadDomainError("upload required headers cannot exceed 20 entries")
        copied: dict[str, str] = {}
        for name, value in self.required_headers.items():
            if (
                not name
                or name.lower() in copied
                or any(character in name for character in "\r\n:")
            ):
                raise UploadDomainError("upload required header name is invalid or duplicated")
            if len(value) > 2_000 or "\r" in value or "\n" in value:
                raise UploadDomainError("upload required header value is invalid")
            copied[name.lower()] = value
        object.__setattr__(self, "required_headers", MappingProxyType(copied))


@dataclass(frozen=True, slots=True)
class UploadCompletionClaim:
    """@brief completion 请求冻结的 size/hash 声明 / Size/hash claim frozen by completion.

    @param size_bytes completion 声明字节数 / Byte count declared at completion.
    @param sha256 completion 声明摘要 / Digest declared at completion.
    """

    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        """@brief 校验 completion 声明 / Validate the completion claim.

        @raise UploadDomainError size 或 hash 非法时抛出 / Raised for an invalid size or hash.
        """
        _require_size(self.size_bytes)
        _require_sha256(self.sha256, "upload completion SHA-256")


@dataclass(frozen=True, slots=True)
class VerifiedUpload:
    """@brief 存储读取与安全扫描共同产生的可信结果 / Trusted storage-read and security-scan result.

    @param size_bytes 服务端读取的实际字节数 / Actual byte count read by the server.
    @param sha256 服务端流式计算的摘要 / Digest streamed and computed by the server.
    @param detected_media_type MIME sniff 得到的类型 / MIME type determined by content sniffing.
    @param artifact_ref 隔离对象的服务端引用 / Server-side reference to the quarantined object.
    @param malware_scan_passed 恶意内容扫描是否通过 / Whether malware scanning passed.
    @param archive_limits_passed 解压层数、条目与膨胀率是否通过 / Whether archive limits passed.
    @param quota_reserved 是否在同一流程中预留配额 / Whether quota was reserved in the same flow.
    """

    size_bytes: int
    sha256: str
    detected_media_type: str
    artifact_ref: ResourceRef
    malware_scan_passed: Literal[True]
    archive_limits_passed: Literal[True]
    quota_reserved: Literal[True]

    def __post_init__(self) -> None:
        """@brief 校验可信验证证明 / Validate the trusted verification evidence.

        @raise UploadDomainError 证明不完整时抛出 / Raised when evidence is incomplete.
        """
        _require_size(self.size_bytes)
        _require_sha256(self.sha256, "verified upload SHA-256")
        _require_text(self.detected_media_type, "detected upload media type", 3, 200)
        if (
            self.malware_scan_passed is not True
            or self.archive_limits_passed is not True
            or self.quota_reserved is not True
        ):
            raise UploadDomainError("verified upload requires every security gate to pass")


@dataclass(frozen=True, slots=True)
class UploadSessionView:
    """@brief 契约允许的 UploadSession 公开投影 / Contract-compatible UploadSession projection.

    @param id session 标识 / Session identifier.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param status 状态 / Status.
    @param grant 直传授权 / Direct-upload grant.
    @param expires_at session 与签名 URL 的截止时间 / Session and signed-URL deadline.
    @param artifact_ref 完成后的服务端 artifact 引用 / Server artifact reference after completion.
    """

    id: UploadSessionId
    workspace_id: WorkspaceId
    status: UploadStatus
    grant: UploadGrant = field(repr=False)
    expires_at: datetime
    artifact_ref: ResourceRef | None

    def __post_init__(self) -> None:
        """@brief 校验公开投影状态关联 / Validate public projection associations.

        @raise UploadDomainError 标识、时间或 artifact 关联非法时抛出 / Raised for invalid associations.
        """
        _require_opaque_id(self.id, "upload session id")
        _require_opaque_id(self.workspace_id, "upload workspace id")
        _require_aware(self.expires_at, "upload expires_at")
        if (self.artifact_ref is not None) is (self.status is not UploadStatus.COMPLETED):
            raise UploadDomainError("only a completed upload exposes an artifact reference")

    @property
    def method(self) -> Literal["PUT"]:
        """@brief 返回契约扁平字段 method / Return the contract's flattened method field.

        @return 固定 PUT / Fixed PUT.
        """
        return self.grant.method

    @property
    def upload_url(self) -> str:
        """@brief 返回契约扁平字段 upload_url / Return the contract's flattened upload_url field.

        @return 短期签名 URL / Short-lived signed URL.
        """
        return self.grant.upload_url

    @property
    def required_headers(self) -> Mapping[str, str]:
        """@brief 返回契约扁平字段 required_headers / Return the contract's flattened required_headers field.

        @return 冻结 headers / Frozen headers.
        """
        return self.grant.required_headers


@dataclass(frozen=True, slots=True)
class UploadSession:
    """@brief 可 CAS 持久化的一次性 UploadSession 聚合 / CAS-persistable one-shot UploadSession aggregate.

    @param view 公开安全投影 / Public-safe projection.
    @param declaration session 创建时冻结的声明 / Declaration frozen at session creation.
    @param created_at 创建时刻 / Creation instant.
    @param generation 内部 CAS generation / Internal CAS generation.
    @param completion_claim completion 请求快照 / Completion-request snapshot.
    @param verification_operation_id 拥有 verifying saga 的稳定操作 ID / Stable operation ID
        owning the verification saga.
    @param failure_code 失败时的稳定内部 code / Stable internal failure code.
    @param claimed_by 完成内容的唯一消费资源 / Sole consumer of completed content.
    """

    view: UploadSessionView
    declaration: UploadDeclaration = field(repr=False)
    created_at: datetime
    generation: int = 1
    completion_claim: UploadCompletionClaim | None = field(default=None, repr=False)
    verification_operation_id: UploadVerificationId | None = field(default=None, repr=False)
    failure_code: str | None = None
    claimed_by: ResourceRef | None = None

    def __post_init__(self) -> None:
        """@brief 穷举校验 UploadSession 判别状态 / Exhaustively validate UploadSession state.

        @raise UploadDomainError generation、时间或状态关联非法时抛出 / Raised for invalid state.
        """
        _require_aware(self.created_at, "upload created_at")
        if self.view.expires_at <= self.created_at:
            raise UploadDomainError("upload expiration must follow creation")
        if self.generation < 1:
            raise UploadDomainError("upload generation must be positive")
        if self.claimed_by is not None and self.view.status is not UploadStatus.COMPLETED:
            raise UploadDomainError("only completed upload content can be claimed")
        status = self.view.status
        if status in {UploadStatus.CREATED, UploadStatus.UPLOADED, UploadStatus.EXPIRED}:
            if (
                self.completion_claim is not None
                or self.verification_operation_id is not None
                or self.failure_code is not None
            ):
                raise UploadDomainError(
                    "created, uploaded, or expired state cannot carry completion outcome"
                )
        elif status is UploadStatus.VERIFYING:
            if (
                self.completion_claim is None
                or self.verification_operation_id is None
                or self.failure_code is not None
            ):
                raise UploadDomainError("verifying state requires only a completion claim")
        elif status is UploadStatus.COMPLETED:
            if (
                self.completion_claim is None
                or self.verification_operation_id is None
                or self.failure_code is not None
            ):
                raise UploadDomainError("completed state requires only a completion claim")
        elif status is UploadStatus.FAILED:
            if (
                self.failure_code is None
                or self.completion_claim is None
                or self.verification_operation_id is None
            ):
                raise UploadDomainError("failed state requires claim and failure code")
            if _STABLE_CODE_PATTERN.fullmatch(self.failure_code) is None:
                raise UploadDomainError("upload failure code is invalid")
        if self.verification_operation_id is not None:
            _require_opaque_id(
                self.verification_operation_id,
                "upload verification operation id",
            )

    @classmethod
    def create(
        cls,
        *,
        upload_id: UploadSessionId,
        workspace_id: WorkspaceId,
        declaration: UploadDeclaration,
        grant: UploadGrant,
        created_at: datetime,
        expires_at: datetime,
    ) -> UploadSession:
        """@brief 创建 created 状态的直传 session / Create a direct-upload session in created state.

        @param upload_id session 标识 / Session identifier.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param declaration 客户端冻结声明 / Frozen client declaration.
        @param grant 对象存储授权 / Object-storage grant.
        @param created_at 创建时刻 / Creation instant.
        @param expires_at 过期时刻 / Expiration instant.
        @return 新 UploadSession / New UploadSession.
        """
        return cls(
            UploadSessionView(
                upload_id,
                workspace_id,
                UploadStatus.CREATED,
                grant,
                expires_at,
                None,
            ),
            declaration,
            created_at,
        )

    def mark_uploaded(self, *, at: datetime) -> UploadSession:
        """@brief 由存储事件或可信 HEAD 标记对象已到达 / Mark object arrival from storage event or trusted HEAD.

        @param at 观测时刻 / Observation instant.
        @return uploaded 的下一 generation / Next generation in uploaded state.
        @raise UploadTransitionError session 非 created 或已过期时抛出 / Raised unless created and live.
        """
        self._require_live(at)
        if self.view.status is not UploadStatus.CREATED:
            raise UploadTransitionError("only a created upload can be marked uploaded")
        return self._with_status(UploadStatus.UPLOADED)

    def begin_completion(
        self,
        claim: UploadCompletionClaim,
        operation_id: UploadVerificationId,
        *,
        at: datetime,
    ) -> UploadSession:
        """@brief 锁定一次 completion 并进入 verifying / Lock one completion and enter verifying.

        @param claim completion 请求的 size/hash / Completion size/hash claim.
        @param operation_id 拥有扫描 saga 的稳定操作 ID / Stable operation ID owning the scan saga.
        @param at 请求时刻 / Request instant.
        @return verifying 的下一 generation / Next generation in verifying state.
        @raise UploadTransitionError session 已处理、过期或声明不匹配时抛出 / Raised if unavailable or mismatched.
        """
        self._require_live(at)
        if self.view.status not in {UploadStatus.CREATED, UploadStatus.UPLOADED}:
            raise UploadTransitionError("upload completion can begin exactly once")
        if (
            claim.size_bytes != self.declaration.size_bytes
            or claim.sha256 != self.declaration.sha256
        ):
            raise UploadTransitionError(
                "completion size or SHA-256 does not match session declaration"
            )
        return replace(
            self,
            view=replace(self.view, status=UploadStatus.VERIFYING, artifact_ref=None),
            generation=self.generation + 1,
            completion_claim=claim,
            verification_operation_id=operation_id,
        )

    def resume_completion(
        self,
        claim: UploadCompletionClaim,
        operation_id: UploadVerificationId,
    ) -> UploadSession:
        """@brief 仅允许原操作恢复中断的 verifying saga / Allow only the owning operation to resume verification.

        @param claim 当前 completion 请求 / Current completion request.
        @param operation_id 稳定准备操作 ID / Stable preparation operation ID.
        @return 未修改的 verifying 聚合 / Unchanged verifying aggregate.
        @raise UploadTransitionError 状态、claim 或 owner 不匹配时抛出 / Raised when state,
            claim, or owner does not match.
        """

        if (
            self.view.status is not UploadStatus.VERIFYING
            or self.completion_claim != claim
            or self.verification_operation_id != operation_id
        ):
            raise UploadTransitionError("upload verification is owned by another operation")
        return self

    def complete(self, evidence: VerifiedUpload, *, at: datetime) -> UploadSession:
        """@brief 仅在全部内容验证通过后完成 session / Complete only after all content checks pass.

        @param evidence 存储与安全扫描可信证明 / Trusted storage and security evidence.
        @param at 完成时刻 / Completion instant.
        @return completed 的下一 generation / Next generation in completed state.
        @raise UploadTransitionError 当前非 verifying 或证据不一致时抛出 / Raised unless evidence matches.
        """
        _require_aware(at, "upload completion instant")
        if self.view.status is not UploadStatus.VERIFYING or self.completion_claim is None:
            raise UploadTransitionError("only a verifying upload can complete")
        if at >= self.view.expires_at:
            raise UploadTransitionError("upload expired before verification completed")
        if (
            evidence.size_bytes != self.declaration.size_bytes
            or evidence.size_bytes != self.completion_claim.size_bytes
            or evidence.sha256 != self.declaration.sha256
            or evidence.sha256 != self.completion_claim.sha256
        ):
            raise UploadTransitionError("verified size or SHA-256 does not match frozen claims")
        if evidence.detected_media_type.casefold() != self.declaration.media_type.casefold():
            raise UploadTransitionError("sniffed media type does not match the declared media type")
        return replace(
            self,
            view=replace(
                self.view,
                status=UploadStatus.COMPLETED,
                artifact_ref=evidence.artifact_ref,
            ),
            generation=self.generation + 1,
        )

    def fail(self, code: str, *, at: datetime) -> UploadSession:
        """@brief 终止 verifying session 并保留稳定失败 code / Terminate verification with a stable code.

        @param code 不含敏感内容的稳定失败 code / Stable failure code without sensitive detail.
        @param at 失败时刻 / Failure instant.
        @return failed 的下一 generation / Next generation in failed state.
        @raise UploadTransitionError 当前不是 verifying 时抛出 / Raised unless verifying.
        """
        _require_aware(at, "upload failure instant")
        if self.view.status is not UploadStatus.VERIFYING:
            raise UploadTransitionError("only a verifying upload can fail")
        if _STABLE_CODE_PATTERN.fullmatch(code) is None:
            raise UploadDomainError("upload failure code is invalid")
        return replace(
            self,
            view=replace(self.view, status=UploadStatus.FAILED, artifact_ref=None),
            generation=self.generation + 1,
            failure_code=code,
        )

    def expire(self, *, at: datetime) -> UploadSession:
        """@brief 使尚未完成的 session 到期 / Expire a session that has not completed.

        @param at 判定时刻 / Evaluation instant.
        @return expired 的下一 generation / Next generation in expired state.
        @raise UploadTransitionError 尚未到期或已终态时抛出 / Raised before deadline or after terminal state.
        """
        _require_aware(at, "upload expiration instant")
        if self.view.status.is_terminal:
            raise UploadTransitionError("terminal upload session cannot expire again")
        if at < self.view.expires_at:
            raise UploadTransitionError("upload session cannot expire before its deadline")
        return replace(
            self,
            view=replace(self.view, status=UploadStatus.EXPIRED, artifact_ref=None),
            generation=self.generation + 1,
            completion_claim=None,
            verification_operation_id=None,
        )

    def claim_content(self, consumer: ResourceRef) -> UploadSession:
        """@brief 为一个下游资源原子领取已验证内容 / Atomically claim content for one downstream resource.

        @param consumer 唯一消费资源，例如 Knowledge version 或 Resume import Job / Sole
            consuming resource, such as a Knowledge version or Resume import Job.
        @return 带 claim 的下一 generation / Next generation with a claim.
        @raise UploadClaimError session 未完成或已被领取时抛出 / Raised unless completed and unclaimed.
        """
        if self.view.status is not UploadStatus.COMPLETED:
            raise UploadClaimError("only completed upload content can be claimed")
        if self.claimed_by is not None:
            raise UploadClaimError("upload content has already been claimed")
        return replace(self, generation=self.generation + 1, claimed_by=consumer)

    def _require_live(self, at: datetime) -> None:
        """@brief 要求请求时 session 尚未过期 / Require the session to be live at request time.

        @param at 请求时刻 / Request instant.
        @raise UploadTransitionError session 已到期时抛出 / Raised after expiration.
        """
        _require_aware(at, "upload transition instant")
        if at >= self.view.expires_at:
            raise UploadTransitionError("upload session has expired")

    def _with_status(self, status: UploadStatus) -> UploadSession:
        """@brief 生成仅修改 status 的下一 generation / Produce the next generation changing only status.

        @param status 目标状态 / Target status.
        @return 替换后的聚合 / Replaced aggregate.
        """
        return replace(
            self,
            view=replace(self.view, status=status, artifact_ref=None),
            generation=self.generation + 1,
        )


def _require_size(value: int) -> None:
    """@brief 校验契约上传字节范围 / Validate the contract upload byte range.

    @param value 字节数 / Byte count.
    @raise UploadDomainError 超出范围时抛出 / Raised outside the supported range.
    """
    if isinstance(value, bool) or not 1 <= value <= MAX_UPLOAD_BYTES:
        raise UploadDomainError("upload size must be between one byte and one GiB")


def _require_sha256(value: str, label: str) -> None:
    """@brief 校验小写 SHA-256 / Validate lower-case SHA-256.

    @param value 摘要 / Digest.
    @param label 错误标签 / Error label.
    @raise UploadDomainError 摘要非法时抛出 / Raised for an invalid digest.
    """
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise UploadDomainError(f"{label} must contain 64 lower-case hexadecimal characters")


def _require_text(value: str, label: str, minimum: int, maximum: int) -> None:
    """@brief 校验有界规范文本 / Validate bounded canonical text.

    @param value 文本 / Text.
    @param label 错误标签 / Error label.
    @param minimum 最小长度 / Minimum length.
    @param maximum 最大长度 / Maximum length.
    @raise UploadDomainError 文本非法时抛出 / Raised for invalid text.
    """
    if (
        not minimum <= len(value) <= maximum
        or value.strip() != value
        or any(ord(character) < 32 for character in value)
    ):
        raise UploadDomainError(f"{label} must be safe and {minimum} to {maximum} characters")


def _require_opaque_id(value: str, label: str) -> None:
    """@brief 校验 API v2 不透明标识 / Validate an API v2 opaque identifier.

    @param value 标识 / Identifier.
    @param label 错误标签 / Error label.
    @raise UploadDomainError 标识非法时抛出 / Raised for an invalid identifier.
    """
    if _OPAQUE_ID_PATTERN.fullmatch(value) is None:
        raise UploadDomainError(f"{label} does not satisfy the API v2 grammar")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 校验带时区时间 / Validate a timezone-aware datetime.

    @param value 时间 / Datetime.
    @param label 错误标签 / Error label.
    @raise UploadDomainError 时间 naive 时抛出 / Raised for a naive datetime.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise UploadDomainError(f"{label} must be timezone-aware")


def _require_upload_url(value: str) -> None:
    """@brief 校验生产 HTTPS 或固定隔离测试上传 URL / Validate production HTTPS or isolated test URL.

    @param value 上传 URL / Upload URL.
    @raise UploadDomainError URL 不满足 NetworkUrl 边界时抛出 / Raised outside the NetworkUrl boundary.
    """
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None or parsed.fragment:
        raise UploadDomainError("upload URL cannot contain userinfo or a fragment")
    production = parsed.scheme == "https" and bool(parsed.hostname)
    isolated_test = (
        parsed.scheme == "http" and parsed.hostname == "dev.hmalliances.org" and parsed.port == 9000
    )
    if not (production or isolated_test):
        raise UploadDomainError("upload URL must use HTTPS or the fixed isolated test origin")


__all__ = [
    "MAX_UPLOAD_BYTES",
    "UploadClaimError",
    "UploadCompletionClaim",
    "UploadDeclaration",
    "UploadDomainError",
    "UploadGrant",
    "UploadSession",
    "UploadSessionId",
    "UploadSessionView",
    "UploadStatus",
    "UploadTransitionError",
    "VerifiedUpload",
]
