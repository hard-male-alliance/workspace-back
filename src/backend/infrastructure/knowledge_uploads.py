"""@brief Knowledge 安全直传、本地签名 PUT 与 S3-compatible adapter / Secure Knowledge direct uploads, local signed PUT, and S3-compatible adapter.

对象验证始终重新流式计算 size/SHA-256，不使用 Content-Type 或 ETag 作为可信证据。
内容先写入私有临时文件，再依次执行 MIME sniff、archive/Docx 边界、malware scan 与
跨 worker 原子 quota reservation；只有全部成功才产生 ``VerifiedUpload``。
"""

from __future__ import annotations

import asyncio
import base64
import codecs
import hashlib
import hmac
import os
import secrets
import struct
import tempfile
import zipfile
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Final, Protocol
from urllib.parse import parse_qs, quote, urlencode, urlsplit, urlunsplit

import httpx
import sqlalchemy as sa
from fastapi import APIRouter, HTTPException, Request, Response

from backend.application.ports.knowledge import IssuedUploadGrant, UploadVerificationRejected
from backend.application.ports.v2_idempotency import IdempotencyPreparationId
from backend.domain.principals import WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.upload_sessions import (
    UploadCompletionClaim,
    UploadDeclaration,
    UploadGrant,
    UploadSessionId,
    VerifiedUpload,
)
from backend.infrastructure.persistence.database import AsyncDatabase

_LOCAL_UPLOAD_ORIGIN: Final[str] = "http://dev.hmalliances.org:9000"
"""@brief 契约允许的隔离本地上传 origin / Contract-allowed isolated local-upload origin."""

_quota_metadata = sa.MetaData(schema="knowledge")
"""@brief 独立 quota Core metadata / Standalone quota Core metadata."""

upload_quota_reservations = sa.Table(
    "upload_quota_reservations",
    _quota_metadata,
    sa.Column("workspace_id", sa.String(128), primary_key=True),
    sa.Column("operation_id", sa.String(160), primary_key=True),
    sa.Column("upload_id", sa.String(160), nullable=False, unique=True),
    sa.Column("size_bytes", sa.BigInteger(), nullable=False),
    sa.Column("reserved_at", sa.DateTime(timezone=True), nullable=False),
)
"""@brief 幂等、可审计 quota reservation 表 / Idempotent, auditable quota-reservation table."""


class MalwareScanUnavailable(RuntimeError):
    """@brief malware scanner 不可用，调用方必须 fail closed / Malware scanner unavailable; callers must fail closed."""


class MalwareScanner(Protocol):
    """@brief 私有临时文件 malware scanner Port / Malware-scanner port over a private temporary file."""

    async def scan(self, path: Path, *, size_bytes: int) -> None:
        """@brief 扫描并在感染或不可用时抛错 / Scan and raise for infection or unavailability."""


class UploadQuotaLedger(Protocol):
    """@brief 跨进程原子上传配额 Port / Cross-process atomic upload-quota port."""

    async def reserve(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        operation_id: IdempotencyPreparationId,
        size_bytes: int,
    ) -> bool:
        """@brief 幂等预留配额 / Idempotently reserve quota.

        @return 已预留或本次成功预留为真，超额为假 / True when already or newly reserved;
            false when quota would be exceeded.
        """


@dataclass(frozen=True, slots=True)
class UploadSafetyLimits:
    """@brief 上传读取、archive 与 MIME 安全边界 / Upload read, archive, and MIME safety bounds."""

    maximum_object_bytes: int
    maximum_archive_entries: int = 2_000
    maximum_archive_depth: int = 3
    maximum_expanded_bytes: int = 256 * 1024 * 1024
    maximum_inflation_ratio: float = 100.0
    maximum_scanner_chunk_bytes: int = 1024 * 1024

    def __post_init__(self) -> None:
        """@brief 校验边界有界且相互一致 / Validate bounded and internally consistent limits."""

        if not 1 <= self.maximum_object_bytes <= 1_073_741_824:
            raise ValueError("maximum upload object size must be one byte to one GiB")
        if not 1 <= self.maximum_archive_entries <= 100_000:
            raise ValueError("maximum archive entries must be 1 to 100000")
        if not 0 <= self.maximum_archive_depth <= 10:
            raise ValueError("maximum archive depth must be zero to ten")
        if not self.maximum_object_bytes <= self.maximum_expanded_bytes <= 10 * 1_073_741_824:
            raise ValueError("maximum expanded bytes is outside safe bounds")
        if not 1.0 <= self.maximum_inflation_ratio <= 1_000.0:
            raise ValueError("maximum archive inflation ratio is outside safe bounds")
        if not 4_096 <= self.maximum_scanner_chunk_bytes <= 16 * 1024 * 1024:
            raise ValueError("malware scanner chunk size is outside safe bounds")


class DevelopmentAllowAllMalwareScanner:
    """@brief 仅供 development/test 显式选择的 scanner / Scanner explicitly selectable only in development/test."""

    async def scan(self, path: Path, *, size_bytes: int) -> None:
        """@brief 保持异步边界但不声称生产扫描 / Preserve the async boundary without claiming production scanning."""

        if size_bytes < 1 or not await asyncio.to_thread(path.is_file):
            raise MalwareScanUnavailable("development scanner received no file")


class RejectingMalwareScanner:
    """@brief 未配置生产 scanner 时的 fail-closed adapter / Fail-closed adapter when no production scanner is configured."""

    async def scan(self, path: Path, *, size_bytes: int) -> None:
        """@brief 总是拒绝而不是伪造成功 / Always reject instead of manufacturing success."""

        del path, size_bytes
        raise MalwareScanUnavailable("malware scanner is not configured")


class ClamAvInstreamScanner:
    """@brief clamd ``INSTREAM`` TCP adapter / clamd ``INSTREAM`` TCP adapter."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        connect_timeout_ms: int,
        read_timeout_ms: int,
        chunk_bytes: int = 1024 * 1024,
    ) -> None:
        """@brief 绑定固定 clamd endpoint 与超时 / Bind a fixed clamd endpoint and timeouts."""

        if not host or host.strip() != host or not 1 <= port <= 65_535:
            raise ValueError("clamd host or port is invalid")
        if not 100 <= connect_timeout_ms <= 60_000 or not 100 <= read_timeout_ms <= 300_000:
            raise ValueError("clamd timeout is outside safe bounds")
        if not 4_096 <= chunk_bytes <= 16 * 1024 * 1024:
            raise ValueError("clamd chunk size is outside safe bounds")
        self._host = host
        self._port = port
        self._connect_timeout = connect_timeout_ms / 1_000
        self._read_timeout = read_timeout_ms / 1_000
        self._chunk_bytes = chunk_bytes

    async def scan(self, path: Path, *, size_bytes: int) -> None:
        """@brief 按官方 zINSTREAM framing 扫描 / Scan with the documented zINSTREAM framing."""

        if size_bytes < 1:
            raise UploadVerificationRejected("empty upload cannot be scanned")
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=self._connect_timeout,
            )
            try:
                writer.write(b"zINSTREAM\0")
                with path.open("rb") as handle:
                    while chunk := await asyncio.to_thread(handle.read, self._chunk_bytes):
                        writer.write(struct.pack("!I", len(chunk)))
                        writer.write(chunk)
                        await writer.drain()
                writer.write(struct.pack("!I", 0))
                await writer.drain()
                reply = await asyncio.wait_for(reader.readuntil(b"\0"), timeout=self._read_timeout)
            finally:
                writer.close()
                await writer.wait_closed()
        except UploadVerificationRejected:
            raise
        except (
            TimeoutError,
            OSError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
        ) as error:
            raise MalwareScanUnavailable("clamd is unavailable") from error
        text = reply.rstrip(b"\0\n").decode("utf-8", errors="replace")
        if text.endswith(" OK"):
            return
        if text.endswith(" FOUND"):
            raise UploadVerificationRejected("malware scanner rejected uploaded content")
        raise MalwareScanUnavailable("clamd returned an indeterminate result")


class MemoryUploadQuotaLedger:
    """@brief 进程内 development/test 原子 quota ledger / In-process atomic quota ledger for development/test."""

    def __init__(self, maximum_workspace_bytes: int) -> None:
        """@brief 设置每 Workspace 固定配额 / Set a fixed per-Workspace quota."""

        if maximum_workspace_bytes < 1:
            raise ValueError("workspace upload quota must be positive")
        self._maximum = maximum_workspace_bytes
        self._reservations: dict[tuple[str, str], tuple[str, int]] = {}
        self._usage: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def reserve(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        operation_id: IdempotencyPreparationId,
        size_bytes: int,
    ) -> bool:
        """@brief 在 lock 内做幂等 compare-and-reserve / Idempotently compare and reserve under a lock."""

        operation = str(operation_id)
        async with self._lock:
            reservation_key = (str(workspace_id), operation)
            existing = self._reservations.get(reservation_key)
            requested = (str(upload_id), size_bytes)
            if existing is not None:
                if existing != requested:
                    raise ValueError("upload quota operation was reused with different input")
                return True
            current = self._usage.get(str(workspace_id), 0)
            if current + size_bytes > self._maximum:
                return False
            self._reservations[reservation_key] = requested
            self._usage[str(workspace_id)] = current + size_bytes
            return True


class PostgresUploadQuotaLedger:
    """@brief 调用 migration 安装的原子 quota function / Invoke the migration-installed atomic quota function."""

    def __init__(self, database: AsyncDatabase, maximum_workspace_bytes: int) -> None:
        """@brief 绑定数据库与全局 Workspace 配额 / Bind the database and global Workspace quota."""

        if maximum_workspace_bytes < 1:
            raise ValueError("workspace upload quota must be positive")
        self._database = database
        self._maximum = maximum_workspace_bytes

    async def reserve(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        operation_id: IdempotencyPreparationId,
        size_bytes: int,
    ) -> bool:
        """@brief 单条 SECURITY DEFINER function 原子预留 / Atomically reserve through one SECURITY DEFINER function call."""

        async with self._database.new_session() as session:
            async with session.begin():
                value = await session.scalar(
                    sa.text(
                        "SELECT knowledge.reserve_upload_quota("
                        ":workspace_id, :upload_id, :operation_id, :size_bytes, :maximum_bytes)"
                    ),
                    {
                        "workspace_id": str(workspace_id),
                        "upload_id": str(upload_id),
                        "operation_id": str(operation_id),
                        "size_bytes": size_bytes,
                        "maximum_bytes": self._maximum,
                    },
                )
        if not isinstance(value, bool):
            raise RuntimeError("upload quota function returned an invalid result")
        return value


class UploadByteSource(Protocol):
    """@brief verifier 使用的可信 server-side object reader / Trusted server-side object reader used by verification."""

    def read(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        """@brief 打开有界 chunk stream / Open a bounded chunk stream."""


class UploadObjectEraser(Protocol):
    """@brief 仅以 Workspace+opaque upload ID 删除对象的 Port / Port erasing objects only by Workspace and opaque upload ID."""

    async def delete_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> None:
        """@brief 幂等删除对象 / Idempotently delete an object."""


class BoundedUploadErasure:
    """@brief durable saga 可重放的有界批量对象擦除器 / Bounded replay-safe object eraser for durable sagas."""

    def __init__(self, store: UploadObjectEraser, *, maximum_batch_size: int = 250) -> None:
        """@brief 绑定对象存储与硬批量上限 / Bind object storage and a hard batch limit."""

        if not 1 <= maximum_batch_size <= 1_000:
            raise ValueError("upload erasure batch size must be 1 to 1000")
        self._store = store
        self._maximum_batch_size = maximum_batch_size

    async def erase(
        self,
        workspace_id: WorkspaceId,
        upload_ids: tuple[UploadSessionId, ...],
    ) -> int:
        """@brief 依稳定 ID 次序幂等擦除显式对象集合 / Idempotently erase an explicit object set in stable-ID order.

        @return 已确认不存在的对象数 / Number of objects confirmed absent.
        @note 调用者的 durable job 保存 cursor；崩溃后重放当前 batch 安全 / The caller's
            durable job stores the cursor; replaying the current batch after a crash is safe.
        """

        if len(upload_ids) > self._maximum_batch_size or len(set(upload_ids)) != len(upload_ids):
            raise ValueError("upload erasure batch is oversized or contains duplicate IDs")
        for upload_id in sorted(upload_ids, key=str):
            await self._store.delete_object(workspace_id, upload_id)
        return len(upload_ids)


class UploadContentVerifier:
    """@brief 共享 size/hash/MIME/archive/malware/quota pipeline / Shared size/hash/MIME/archive/malware/quota pipeline."""

    def __init__(
        self,
        source: UploadByteSource,
        scanner: MalwareScanner,
        quota: UploadQuotaLedger,
        limits: UploadSafetyLimits,
        *,
        temporary_directory: Path | None = None,
    ) -> None:
        """@brief 注入 server reader 与全部安全门禁 / Inject the server reader and every security gate."""

        self._source = source
        self._scanner = scanner
        self._quota = quota
        self._limits = limits
        self._temporary_directory = temporary_directory

    async def verify(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        claim: UploadCompletionClaim,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> VerifiedUpload:
        """@brief 流式重算后依序执行门禁 / Recompute by streaming, then execute gates in order."""

        temporary_path = await self._materialize(workspace_id, upload_id, declaration, claim)
        try:
            detected = await asyncio.to_thread(
                _inspect_content,
                temporary_path,
                declaration.filename,
                self._limits,
            )
            try:
                await self._scanner.scan(temporary_path, size_bytes=claim.size_bytes)
            except MalwareScanUnavailable as error:
                raise UploadVerificationRejected(
                    "malware scan could not establish a safe result"
                ) from error
            if not await self._quota.reserve(
                workspace_id,
                upload_id,
                operation_id,
                claim.size_bytes,
            ):
                raise UploadVerificationRejected("workspace upload quota would be exceeded")
            artifact_digest = hashlib.sha256(
                _framed(str(workspace_id), str(upload_id), claim.sha256)
            ).hexdigest()[:32]
            return VerifiedUpload(
                claim.size_bytes,
                claim.sha256,
                detected,
                ResourceRef("upload_artifact", f"upload_artifact_{artifact_digest}", 1),
                True,
                True,
                True,
            )
        finally:
            await asyncio.to_thread(temporary_path.unlink, missing_ok=True)

    async def _materialize(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        claim: UploadCompletionClaim,
    ) -> Path:
        """@brief 从 server-side GET 写入私有临时文件并重算证据 / Write server-side GET to a private temp file and recompute evidence."""

        if declaration.size_bytes != claim.size_bytes or declaration.sha256 != claim.sha256:
            raise UploadVerificationRejected("completion does not match the frozen declaration")
        handle = tempfile.NamedTemporaryFile(
            prefix="aiws-upload-",
            suffix=".quarantine",
            dir=self._temporary_directory,
            delete=False,
        )
        path = Path(handle.name)
        os.chmod(path, 0o600)
        digest = hashlib.sha256()
        size = 0
        try:
            async with self._source.read(workspace_id, upload_id) as chunks:
                async for chunk in chunks:
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > self._limits.maximum_object_bytes or size > declaration.size_bytes:
                        raise UploadVerificationRejected(
                            "uploaded object exceeds its declared size"
                        )
                    digest.update(chunk)
                    await asyncio.to_thread(handle.write, chunk)
            await asyncio.to_thread(handle.flush)
            await asyncio.to_thread(os.fsync, handle.fileno())
        except BaseException:
            handle.close()
            await asyncio.to_thread(path.unlink, missing_ok=True)
            raise
        finally:
            handle.close()
        if size != declaration.size_bytes or digest.hexdigest() != declaration.sha256:
            await asyncio.to_thread(path.unlink, missing_ok=True)
            raise UploadVerificationRejected("uploaded object size or SHA-256 does not match")
        return path


class LocalSignedUploadStore:
    """@brief development/test 私有目录签名 PUT + verify adapter / Development/test private-directory signed-PUT and verification adapter."""

    def __init__(
        self,
        root: Path,
        signing_key: bytes,
        scanner: MalwareScanner,
        quota: UploadQuotaLedger,
        limits: UploadSafetyLimits,
        *,
        public_origin: str = _LOCAL_UPLOAD_ORIGIN,
    ) -> None:
        """@brief 绑定隔离 origin、私有目录与安全 pipeline / Bind the isolated origin, private directory, and security pipeline."""

        if len(signing_key) < 32:
            raise ValueError("local upload signing key must contain at least 32 bytes")
        _require_upload_origin(public_origin)
        self._root = root.resolve()
        self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._signing_key = signing_key
        self._public_origin = public_origin.rstrip("/")
        self._verifier = UploadContentVerifier(self, scanner, quota, limits)
        self._maximum_bytes = limits.maximum_object_bytes

    async def issue_upload_grant(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        *,
        expires_at: datetime,
        operation_id: IdempotencyPreparationId,
    ) -> IssuedUploadGrant:
        """@brief 签发 method/path/expiry/size/hash 全绑定 URL / Issue a URL bound to method, path, expiry, size, and hash."""

        del operation_id
        now = datetime.now(UTC)
        if expires_at.tzinfo is None or expires_at <= now:
            raise ValueError("local upload expiry must be a future aware datetime")
        expiry = int(expires_at.timestamp())
        path = _local_url_path(workspace_id, upload_id)
        signature = _local_signature(
            self._signing_key,
            path,
            expiry,
            declaration.size_bytes,
            declaration.sha256,
        )
        query = urlencode(
            {
                "expires": str(expiry),
                "size": str(declaration.size_bytes),
                "sha256": declaration.sha256,
                "signature": signature,
            }
        )
        return IssuedUploadGrant(
            UploadGrant(
                f"{self._public_origin}{path}?{query}",
                {
                    "content-length": str(declaration.size_bytes),
                    "x-aiws-content-sha256": declaration.sha256,
                },
            ),
            now,
            expires_at,
        )

    async def verify_uploaded_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        claim: UploadCompletionClaim,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> VerifiedUpload:
        """@brief 从私有目录 server-side read 后完整验证 / Fully verify after a server-side read from the private directory."""

        return await self._verifier.verify(
            workspace_id,
            upload_id,
            declaration,
            claim,
            operation_id=operation_id,
        )

    async def delete_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> None:
        """@brief 幂等擦除本地上传对象 / Idempotently erase a local upload object."""

        await asyncio.to_thread(self._object_path(workspace_id, upload_id).unlink, missing_ok=True)

    @asynccontextmanager
    async def read(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> AsyncIterator[AsyncIterator[bytes]]:
        """@brief 流式读取固定 object path / Stream a fixed object path."""

        path = self._object_path(workspace_id, upload_id)
        if not path.is_file():
            raise UploadVerificationRejected("uploaded object does not exist")

        async def chunks() -> AsyncIterator[bytes]:
            with path.open("rb") as handle:
                while chunk := await asyncio.to_thread(handle.read, 1024 * 1024):
                    yield chunk

        yield chunks()

    async def accept_signed_put(
        self,
        workspace_id: str,
        upload_id: str,
        query: str,
        headers: Mapping[str, str],
        chunks: AsyncIterator[bytes],
        *,
        now: datetime | None = None,
    ) -> None:
        """@brief 验签、流式重算并原子发布本地对象 / Verify, stream-recompute, and atomically publish a local object."""

        values = parse_qs(query, keep_blank_values=True, strict_parsing=True)
        if set(values) != {"expires", "size", "sha256", "signature"} or any(
            len(items) != 1 for items in values.values()
        ):
            raise UploadVerificationRejected("local upload signature query is invalid")
        try:
            expiry = int(values["expires"][0])
            size = int(values["size"][0])
        except (KeyError, ValueError) as error:
            raise UploadVerificationRejected("local upload signature values are invalid") from error
        digest_claim = values["sha256"][0]
        signature = values["signature"][0]
        current = now or datetime.now(UTC)
        if current.tzinfo is None or int(current.timestamp()) > expiry:
            raise UploadVerificationRejected("local upload grant has expired")
        if not 1 <= size <= self._maximum_bytes or len(digest_claim) != 64:
            raise UploadVerificationRejected("local upload bounds are invalid")
        path_part = _local_url_path(WorkspaceId(workspace_id), UploadSessionId(upload_id))
        expected = _local_signature(self._signing_key, path_part, expiry, size, digest_claim)
        if not hmac.compare_digest(signature, expected):
            raise UploadVerificationRejected("local upload signature is invalid")
        normalized_headers = {name.lower(): value for name, value in headers.items()}
        if normalized_headers.get("content-length") != str(size) or not hmac.compare_digest(
            normalized_headers.get("x-aiws-content-sha256", ""), digest_claim
        ):
            raise UploadVerificationRejected("local upload required headers do not match")
        destination = self._object_path(WorkspaceId(workspace_id), UploadSessionId(upload_id))
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = destination.with_name(f".{destination.name}.{secrets.token_hex(12)}.tmp")
        actual = hashlib.sha256()
        actual_size = 0
        try:
            with temporary.open("xb") as handle:
                os.chmod(temporary, 0o600)
                async for chunk in chunks:
                    actual_size += len(chunk)
                    if actual_size > size:
                        raise UploadVerificationRejected("local upload body exceeds signed size")
                    actual.update(chunk)
                    await asyncio.to_thread(handle.write, chunk)
                await asyncio.to_thread(handle.flush)
                await asyncio.to_thread(os.fsync, handle.fileno())
            if actual_size != size or not hmac.compare_digest(actual.hexdigest(), digest_claim):
                raise UploadVerificationRejected("local upload body does not match signed evidence")
            await asyncio.to_thread(os.replace, temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    def _object_path(self, workspace_id: WorkspaceId, upload_id: UploadSessionId) -> Path:
        """@brief 从 opaque IDs 构造无用户路径的 object path / Build an object path without user-controlled path segments."""

        workspace_digest = hashlib.sha256(str(workspace_id).encode()).hexdigest()[:32]
        upload_digest = hashlib.sha256(str(upload_id).encode()).hexdigest()[:40]
        candidate = (self._root / workspace_digest / upload_digest).resolve()
        if self._root not in candidate.parents:
            raise RuntimeError("local upload object path escaped its root")
        return candidate


type LocalUploadStoreResolver = Callable[[Request], LocalSignedUploadStore | None]
"""@brief 从当前 app lifespan 解析 local store / Resolve a local store from the current app lifespan."""


def build_local_upload_router(
    store: LocalSignedUploadStore | LocalUploadStoreResolver,
) -> APIRouter:
    """@brief 构造可选但不自动 mount 的本地 PUT router / Build an optional local PUT router without mounting it.

    @param store 固定 store 或 lifespan-aware resolver / Fixed store or lifespan-aware resolver.
    @return 仅含签名 PUT 的 router / Router containing only the signed PUT endpoint.
    """

    router = APIRouter(include_in_schema=False)

    @router.put("/__local-uploads/{workspace_id}/{upload_id}")
    async def put_local_upload(workspace_id: str, upload_id: str, request: Request) -> Response:
        """@brief 接收开发环境签名 PUT / Accept a development signed PUT."""

        selected = store(request) if not isinstance(store, LocalSignedUploadStore) else store
        if selected is None:
            raise HTTPException(status_code=404, detail="local upload storage is unavailable")
        try:
            await selected.accept_signed_put(
                workspace_id,
                upload_id,
                request.url.query,
                request.headers,
                request.stream(),
            )
        except UploadVerificationRejected as error:
            raise HTTPException(
                status_code=403, detail="upload grant or body was rejected"
            ) from error
        return Response(status_code=204)

    return router


@dataclass(frozen=True, slots=True)
class S3UploadSettings:
    """@brief S3-compatible SigV4 设置 / S3-compatible SigV4 settings."""

    endpoint: str
    region: str
    bucket: str
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    session_token: str | None = field(default=None, repr=False)
    object_prefix: str = "aiws-uploads"

    def __post_init__(self) -> None:
        """@brief 校验固定 HTTPS endpoint 与凭据 / Validate a fixed HTTPS endpoint and credentials."""

        parsed = urlsplit(self.endpoint)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("S3 endpoint must be a fixed credential-free HTTPS URL")
        if any(
            not value or value.strip() != value
            for value in (self.region, self.bucket, self.access_key_id, self.secret_access_key)
        ):
            raise ValueError("S3 region, bucket, and credentials are required")
        prefix = PurePosixPath(self.object_prefix)
        if prefix.is_absolute() or ".." in prefix.parts or not prefix.parts:
            raise ValueError("S3 object prefix is invalid")


class S3UploadObjectStore:
    """@brief 无 SDK 隐式配置的 S3-compatible SigV4 adapter / S3-compatible SigV4 adapter without implicit SDK configuration."""

    def __init__(
        self,
        settings: S3UploadSettings,
        scanner: MalwareScanner,
        quota: UploadQuotaLedger,
        limits: UploadSafetyLimits,
        *,
        connect_timeout_ms: int,
        read_timeout_ms: int,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """@brief 注入明确凭据、timeout 与安全 pipeline / Inject explicit credentials, timeouts, and the security pipeline."""

        if not 100 <= connect_timeout_ms <= 60_000 or not 100 <= read_timeout_ms <= 300_000:
            raise ValueError("S3 upload timeouts are outside safe bounds")
        self._settings = settings
        self._timeout = httpx.Timeout(
            read_timeout_ms / 1_000,
            connect=connect_timeout_ms / 1_000,
            write=connect_timeout_ms / 1_000,
            pool=connect_timeout_ms / 1_000,
        )
        self._transport = transport
        self._verifier = UploadContentVerifier(self, scanner, quota, limits)

    async def issue_upload_grant(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        *,
        expires_at: datetime,
        operation_id: IdempotencyPreparationId,
    ) -> IssuedUploadGrant:
        """@brief 签发 SigV4 PUT 并签入 SHA-256 checksum / Issue a SigV4 PUT with SHA-256 checksum signed in."""

        del operation_id
        now = datetime.now(UTC)
        seconds = int((expires_at - now).total_seconds())
        if expires_at.tzinfo is None or not 1 <= seconds <= 3_600:
            raise ValueError("S3 upload grant lifetime must be one second to one hour")
        url = self._object_url(workspace_id, upload_id)
        checksum = base64.b64encode(bytes.fromhex(declaration.sha256)).decode("ascii")
        required_headers = {
            "content-length": str(declaration.size_bytes),
            "x-amz-checksum-sha256": checksum,
        }
        presigned = _sigv4_presign(
            self._settings,
            "PUT",
            url,
            required_headers,
            now,
            seconds,
        )
        return IssuedUploadGrant(UploadGrant(presigned, required_headers), now, expires_at)

    async def verify_uploaded_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        claim: UploadCompletionClaim,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> VerifiedUpload:
        """@brief 通过签名 GET stream 重算全部证据 / Recompute all evidence through a signed GET stream."""

        return await self._verifier.verify(
            workspace_id,
            upload_id,
            declaration,
            claim,
            operation_id=operation_id,
        )

    async def delete_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> None:
        """@brief 以签名 DELETE 幂等擦除 S3 对象 / Idempotently erase an S3 object with a signed DELETE."""

        url = self._object_url(workspace_id, upload_id)
        headers = _sigv4_authorization_headers(self._settings, "DELETE", url, datetime.now(UTC))
        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                transport=self._transport,
                trust_env=False,
                follow_redirects=False,
            ) as client:
                response = await client.delete(url, headers=headers)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.ProtocolError) as error:
            raise RuntimeError("S3-compatible object store is unavailable") from error
        if response.status_code != 404 and not 200 <= response.status_code < 300:
            raise RuntimeError("S3-compatible object deletion failed")

    @asynccontextmanager
    async def read(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> AsyncIterator[AsyncIterator[bytes]]:
        """@brief 发出禁代理、禁 redirect 的 server-side signed GET / Issue a no-proxy, no-redirect server-side signed GET."""

        url = self._object_url(workspace_id, upload_id)
        headers = _sigv4_authorization_headers(self._settings, "GET", url, datetime.now(UTC))
        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                transport=self._transport,
                trust_env=False,
                follow_redirects=False,
            ) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    if response.status_code == 404:
                        raise UploadVerificationRejected("uploaded object does not exist")
                    if response.status_code < 200 or response.status_code >= 300:
                        raise RuntimeError("S3-compatible object read failed")
                    yield response.aiter_bytes(1024 * 1024)
        except UploadVerificationRejected:
            raise
        except (httpx.HTTPError, httpx.StreamError) as error:
            raise RuntimeError("S3-compatible object store is unavailable") from error

    def _object_url(self, workspace_id: WorkspaceId, upload_id: UploadSessionId) -> str:
        """@brief 构造 path-style、Workspace 隔离 object URL / Build a path-style, Workspace-isolated object URL."""

        endpoint = self._settings.endpoint.rstrip("/")
        parts = (
            self._settings.bucket,
            *PurePosixPath(self._settings.object_prefix).parts,
            hashlib.sha256(str(workspace_id).encode()).hexdigest()[:32],
            hashlib.sha256(str(upload_id).encode()).hexdigest()[:40],
        )
        return endpoint + "/" + "/".join(quote(part, safe="-_.~") for part in parts)


def _inspect_content(path: Path, filename: str, limits: UploadSafetyLimits) -> str:
    """@brief sniff MIME 并验证所有 archive entry / Sniff MIME and validate every archive entry."""

    with path.open("rb") as handle:
        prefix = handle.read(8_192)
    suffix = Path(filename).suffix.lower()
    if prefix.startswith(b"%PDF-"):
        if suffix != ".pdf":
            raise UploadVerificationRejected("PDF content does not match its filename")
        return "application/pdf"
    if zipfile.is_zipfile(path):
        state = _ArchiveState()
        names = _inspect_zip(path, limits, state, depth=1)
        is_docx = "[Content_Types].xml" in names and "word/document.xml" in names
        if is_docx:
            if suffix != ".docx":
                raise UploadVerificationRejected("DOCX content does not match its filename")
            return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        if suffix != ".zip":
            raise UploadVerificationRejected("ZIP content does not match its filename")
        return "application/zip"
    if suffix not in {".txt", ".md", ".markdown"}:
        raise UploadVerificationRejected("text content does not match a supported filename")
    _validate_utf8_text(path)
    return "text/markdown" if suffix in {".md", ".markdown"} else "text/plain"


def _validate_utf8_text(path: Path) -> None:
    """@brief 增量验证整个文本对象而非只看 prefix / Incrementally validate the entire text object, not only its prefix."""

    decoder = codecs.getincrementaldecoder("utf-8-sig")("strict")
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                if "\x00" in decoder.decode(chunk):
                    raise UploadVerificationRejected("uploaded text contains NUL bytes")
            if "\x00" in decoder.decode(b"", final=True):
                raise UploadVerificationRejected("uploaded text contains NUL bytes")
    except UnicodeDecodeError as error:
        raise UploadVerificationRejected("uploaded content is not valid UTF-8 text") from error


@dataclass(slots=True)
class _ArchiveState:
    """@brief 递归 archive 的累计预算 / Accumulated budget for recursive archives."""

    entries: int = 0
    expanded_bytes: int = 0
    compressed_bytes: int = 0


def _inspect_zip(
    source: Path | bytes,
    limits: UploadSafetyLimits,
    state: _ArchiveState,
    *,
    depth: int,
) -> set[str]:
    """@brief 有界递归检查 ZIP/Docx entries / Recursively inspect ZIP/Docx entries within bounds."""

    if depth > limits.maximum_archive_depth:
        raise UploadVerificationRejected("archive nesting depth exceeds the configured limit")
    names: set[str] = set()
    try:
        archive = zipfile.ZipFile(source if isinstance(source, Path) else BytesIO(source))
    except (zipfile.BadZipFile, OSError) as error:
        raise UploadVerificationRejected("uploaded archive is invalid") from error
    with archive:
        for info in archive.infolist():
            state.entries += 1
            if state.entries > limits.maximum_archive_entries:
                raise UploadVerificationRejected("archive entry count exceeds the configured limit")
            normalized = PurePosixPath(info.filename.replace("\\", "/"))
            if (
                normalized.is_absolute()
                or ".." in normalized.parts
                or not normalized.parts
                or normalized.parts[0].endswith(":")
                or info.flag_bits & 0x1
                or _zip_entry_is_symlink(info)
            ):
                raise UploadVerificationRejected("archive contains an unsafe entry")
            state.expanded_bytes += info.file_size
            state.compressed_bytes += max(info.compress_size, 1)
            if state.expanded_bytes > limits.maximum_expanded_bytes:
                raise UploadVerificationRejected(
                    "archive expanded bytes exceed the configured limit"
                )
            if state.expanded_bytes / state.compressed_bytes > limits.maximum_inflation_ratio:
                raise UploadVerificationRejected(
                    "archive inflation ratio exceeds the configured limit"
                )
            canonical_name = normalized.as_posix()
            if canonical_name in names:
                raise UploadVerificationRejected("archive contains duplicate entry names")
            names.add(canonical_name)
            if info.is_dir():
                continue
            try:
                with archive.open(info, "r") as entry:
                    signature = entry.read(4)
                    if signature.startswith(b"PK\x03\x04"):
                        remaining_limit = limits.maximum_expanded_bytes - state.expanded_bytes
                        if info.file_size > remaining_limit:
                            raise UploadVerificationRejected(
                                "nested archive exceeds remaining expansion budget"
                            )
                        nested = signature + entry.read(info.file_size + 1)
                        if len(nested) > info.file_size:
                            raise UploadVerificationRejected("nested archive size is inconsistent")
                        _inspect_zip(nested, limits, state, depth=depth + 1)
            except UploadVerificationRejected:
                raise
            except (
                zipfile.BadZipFile,
                RuntimeError,
                NotImplementedError,
                EOFError,
                OSError,
            ) as error:
                raise UploadVerificationRejected("archive entry cannot be safely read") from error
    return names


def _zip_entry_is_symlink(info: zipfile.ZipInfo) -> bool:
    """@brief 检查 Unix mode 中的 symlink bit / Check the symlink bit in the Unix mode."""

    return ((info.external_attr >> 16) & 0o170000) == 0o120000


def _local_url_path(workspace_id: WorkspaceId, upload_id: UploadSessionId) -> str:
    """@brief 构造只含 opaque ID 的本地 URL path / Build a local URL path containing only opaque IDs."""

    return f"/__local-uploads/{quote(str(workspace_id), safe='')}/{quote(str(upload_id), safe='')}"


def _local_signature(key: bytes, path: str, expiry: int, size: int, sha256: str) -> str:
    """@brief 签名本地 PUT 的安全相关字段 / Sign security-relevant fields of a local PUT."""

    digest = hmac.new(
        key,
        _framed("PUT", path, str(expiry), str(size), sha256),
        hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _require_upload_origin(value: str) -> None:
    """@brief 仅允许契约隔离 origin 或 HTTPS origin / Allow only the contract-isolated origin or an HTTPS origin."""

    parsed = urlsplit(value)
    exact_origin = urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    if (
        parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or not parsed.hostname
        or (parsed.scheme != "https" and exact_origin != _LOCAL_UPLOAD_ORIGIN)
    ):
        raise ValueError("local upload public origin must be HTTPS or the isolated test origin")


def _sigv4_presign(
    settings: S3UploadSettings,
    method: str,
    url: str,
    required_headers: Mapping[str, str],
    now: datetime,
    expires_seconds: int,
) -> str:
    """@brief 生成 S3 SigV4 query authentication / Generate S3 SigV4 query authentication."""

    parsed = urlsplit(url)
    timestamp = now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    date = timestamp[:8]
    scope = f"{date}/{settings.region}/s3/aws4_request"
    headers = {
        "host": parsed.netloc.lower(),
        **{key.lower(): value for key, value in required_headers.items()},
    }
    signed_headers = ";".join(sorted(headers))
    parameters: dict[str, str] = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{settings.access_key_id}/{scope}",
        "X-Amz-Date": timestamp,
        "X-Amz-Expires": str(expires_seconds),
        "X-Amz-SignedHeaders": signed_headers,
    }
    if settings.session_token is not None:
        parameters["X-Amz-Security-Token"] = settings.session_token
    canonical_query = _canonical_query(parameters)
    canonical_headers = "".join(f"{key}:{_header_value(headers[key])}\n" for key in sorted(headers))
    canonical_request = "\n".join(
        (
            method,
            _canonical_uri(parsed.path),
            canonical_query,
            canonical_headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        )
    )
    signature = _sigv4_signature(settings, timestamp, scope, canonical_request)
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            f"{canonical_query}&X-Amz-Signature={signature}",
            "",
        )
    )


def _sigv4_authorization_headers(
    settings: S3UploadSettings,
    method: str,
    url: str,
    now: datetime,
) -> dict[str, str]:
    """@brief 生成 server-side S3 Authorization headers / Generate server-side S3 Authorization headers."""

    parsed = urlsplit(url)
    timestamp = now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    date = timestamp[:8]
    scope = f"{date}/{settings.region}/s3/aws4_request"
    headers = {
        "host": parsed.netloc.lower(),
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
        "x-amz-date": timestamp,
    }
    if settings.session_token is not None:
        headers["x-amz-security-token"] = settings.session_token
    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join(f"{key}:{_header_value(headers[key])}\n" for key in sorted(headers))
    canonical_request = "\n".join(
        (
            method,
            _canonical_uri(parsed.path),
            "",
            canonical_headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        )
    )
    signature = _sigv4_signature(settings, timestamp, scope, canonical_request)
    headers["authorization"] = (
        "AWS4-HMAC-SHA256 "
        f"Credential={settings.access_key_id}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return headers


def _sigv4_signature(
    settings: S3UploadSettings,
    timestamp: str,
    scope: str,
    canonical_request: str,
) -> str:
    """@brief 计算 AWS4 signing-key chain / Compute the AWS4 signing-key chain."""

    date = timestamp[:8]
    if scope.split("/", maxsplit=1)[0] != date:
        raise ValueError("SigV4 date and scope disagree")
    string_to_sign = "\n".join(
        (
            "AWS4-HMAC-SHA256",
            timestamp,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        )
    )
    signing = _hmac_sha256(("AWS4" + settings.secret_access_key).encode(), date.encode())
    signing = _hmac_sha256(signing, settings.region.encode())
    signing = _hmac_sha256(signing, b"s3")
    signing = _hmac_sha256(signing, b"aws4_request")
    return hmac.new(signing, string_to_sign.encode(), hashlib.sha256).hexdigest()


def _canonical_query(parameters: Mapping[str, str]) -> str:
    """@brief 按 SigV4 RFC3986 规则排序 query / Sort a query using SigV4 RFC3986 rules."""

    return "&".join(
        f"{quote(key, safe='-_.~')}={quote(value, safe='-_.~')}"
        for key, value in sorted(parameters.items())
    )


def _canonical_uri(path: str) -> str:
    """@brief 保留 path separators 并 RFC3986 encode / Preserve path separators and RFC3986-encode segments."""

    return "/".join(quote(segment, safe="-_.~") for segment in path.split("/")) or "/"


def _header_value(value: str) -> str:
    """@brief 规范化 SigV4 header whitespace / Normalize SigV4 header whitespace."""

    return " ".join(value.strip().split())


def _hmac_sha256(key: bytes, value: bytes) -> bytes:
    """@brief 返回二进制 HMAC-SHA256 / Return binary HMAC-SHA256."""

    return hmac.new(key, value, hashlib.sha256).digest()


def _framed(*parts: str) -> bytes:
    """@brief 长度前缀编码稳定字段 / Length-prefix stable fields."""

    output = bytearray()
    for part in parts:
        raw = part.encode()
        output.extend(len(raw).to_bytes(4, "big"))
        output.extend(raw)
    return bytes(output)


__all__ = [
    "BoundedUploadErasure",
    "ClamAvInstreamScanner",
    "DevelopmentAllowAllMalwareScanner",
    "LocalSignedUploadStore",
    "LocalUploadStoreResolver",
    "MalwareScanUnavailable",
    "MalwareScanner",
    "MemoryUploadQuotaLedger",
    "PostgresUploadQuotaLedger",
    "RejectingMalwareScanner",
    "S3UploadObjectStore",
    "S3UploadSettings",
    "UploadByteSource",
    "UploadContentVerifier",
    "UploadObjectEraser",
    "UploadQuotaLedger",
    "UploadSafetyLimits",
    "build_local_upload_router",
    "upload_quota_reservations",
]
