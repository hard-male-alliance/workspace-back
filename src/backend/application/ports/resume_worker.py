"""@brief API V2 Resume Job worker 端口 / API V2 Resume-job worker ports.

这些类型把持久 Job/spec、外部转换能力和第二阶段原子结果写入分开。应用层只依赖
窄端口，因此 renderer/importer I/O 永远位于两个短事务之间。
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from types import TracebackType
from typing import Protocol, Self

from backend.application.ports.resumes import ResumeRepository, ResumeTemplateCatalog
from backend.domain.platform import ArtifactId, Job, JobId
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resume_jobs import RenderFormat, ResumeJobSpec
from backend.domain.resumes import JsonValue, ResumeDocument, ResumeRevision
from backend.domain.upload_sessions import UploadSessionId


@dataclass(frozen=True, slots=True)
class PersistedResumeJob:
    """@brief 持久 Job 与已验证私有 spec / Persisted Job and validated private spec.

    @param job 统一 Job 聚合 / Unified Job aggregate.
    @param spec 类型化 worker 输入；损坏时为空 / Typed worker input, absent when corrupt.
    @param spec_error 持久 payload 损坏的稳定 code / Stable code for corrupt persisted payload.
    """

    job: Job
    spec: ResumeJobSpec | None
    spec_error: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验 spec/error 判别联合 / Validate the spec/error discriminated union."""
        if (self.spec is None) is (self.spec_error is None):
            raise ValueError("persisted Resume Job requires exactly one of spec or spec_error")


@dataclass(frozen=True, slots=True)
class ResumeImportSource:
    """@brief 已领取且已安全扫描的 import 输入证明 / Claimed and safely scanned import-input evidence.

    @param upload_session_id UploadSession 标识 / UploadSession identifier.
    @param media_type 服务端 sniff 后的媒体类型 / Server-sniffed media type.
    @param size_bytes 服务端验证字节数 / Server-verified byte count.
    @param sha256 服务端验证 SHA-256 / Server-verified SHA-256.
    """

    upload_session_id: UploadSessionId
    media_type: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        """@brief 校验 import 证明完整性 / Validate import-evidence integrity."""
        if not self.upload_session_id or not self.media_type or self.size_bytes < 1:
            raise ValueError("Resume import source evidence is incomplete")
        if len(self.sha256) != 64 or any(character not in "0123456789abcdef" for character in self.sha256):
            raise ValueError("Resume import source SHA-256 is invalid")


@dataclass(frozen=True, slots=True)
class ResumeImportedContent:
    """@brief 文档转换器产生的安全语义输入 / Safe semantic input produced by a document converter.

    @param full_name 从原文提取的候选人姓名 / Candidate name extracted from the source.
    @param plain_text 有界纯文本，不含 HTML/LaTeX / Bounded plain text without HTML or LaTeX.
    """

    full_name: str
    plain_text: str

    def __post_init__(self) -> None:
        """@brief 校验可进入 SIR 的文本边界 / Validate text bounds before entering the SIR."""
        if not 1 <= len(self.full_name) <= 200 or self.full_name.strip() != self.full_name:
            raise ValueError("imported Resume full name is invalid")
        if not 1 <= len(self.plain_text) <= 20_000 or "\x00" in self.plain_text:
            raise ValueError("imported Resume text is invalid")


@dataclass(frozen=True, slots=True)
class RenderedResumeArtifact:
    """@brief 一个 format 的可信渲染结果 / Trusted render result for one format.

    @param artifact_id 由稳定 Job operation ID 派生的 Artifact ID / Artifact ID derived from the stable Job operation ID.
    @param format 输出格式 / Output format.
    @param media_type 标准媒体类型 / Standard media type.
    @param content 受限大小的不可变内容 / Size-bounded immutable content.
    @param page_count 可选页数 / Optional page count.
    @param source_map PDF 的规范 source map / Canonical source map for PDF.
    """

    artifact_id: ArtifactId
    format: RenderFormat
    media_type: str
    content: bytes = field(repr=False)
    page_count: int | None = None
    source_map: dict[str, JsonValue] | None = None

    def __post_init__(self) -> None:
        """@brief 校验 format/content 关联 / Validate format/content associations."""
        if not self.artifact_id or not self.content or len(self.content) > 1_073_741_824:
            raise ValueError("rendered Resume artifact size is invalid")
        if "/" not in self.media_type or (self.page_count is not None and self.page_count < 1):
            raise ValueError("rendered Resume artifact metadata is invalid")
        if (self.source_map is not None) is (self.format is not RenderFormat.PDF):
            raise ValueError("only a PDF Resume artifact may carry a source map")


def resume_worker_artifact_id(
    operation_id: str,
    output_format: RenderFormat,
) -> ArtifactId:
    """@brief 从稳定 Job operation ID 派生 Artifact ID / Derive an Artifact ID from a stable Job operation ID.

    @param operation_id ``persisted kind:Job ID`` 幂等键 / ``persisted kind:Job ID`` idempotency key.
    @param output_format 输出格式判别值 / Output-format discriminator.
    @return crash 重放稳定的 Artifact ID / Crash-replay-stable Artifact ID.
    """
    if not operation_id:
        raise ValueError("Resume worker operation ID is required")
    digest = sha256(
        f"aiws:v2:resume-worker:artifact:{operation_id}:{output_format.value}".encode()
    ).hexdigest()[:32]
    return ArtifactId(f"artifact_{digest}")


class ResumeWorkerJobStore(Protocol):
    """@brief worker 使用的统一 Job/spec store / Unified Job/spec store used by the worker."""

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> PersistedResumeJob | None:
        """@brief 在 Workspace 内读取或锁定 Job/spec / Read or lock a Job/spec in one Workspace."""

    async def save(self, job: Job, *, expected_revision: int) -> None:
        """@brief 用旧 revision CAS 保存 Job / Save a Job by CAS on its old revision."""

    async def get_import_source(
        self,
        workspace_id: WorkspaceId,
        upload_session_id: str,
        job_id: JobId,
    ) -> ResumeImportSource | None:
        """@brief 读取精确绑定当前 Job 的已验证 upload / Read the verified upload bound to this exact Job."""


class ResumeWorkerResultStore(Protocol):
    """@brief 第二阶段原子结果 sink / Atomic second-phase result sink."""

    async def add_render_results(
        self,
        job: Job,
        revision: ResumeRevision,
        artifacts: Sequence[RenderedResumeArtifact],
        *,
        operation_id: str,
        created_at: datetime,
    ) -> tuple[ResourceRef, ...]:
        """@brief 写统一 Artifact/content/source-map 与 render binding / Write unified artifacts, content, source maps, and render binding.

        @return 统一 Artifact 结果引用 / Unified Artifact result references.
        """


class ResumeWorkerUnitOfWork(Protocol):
    """@brief Resume Job 的短事务工作单元 / Short-transaction unit of work for Resume Jobs."""

    @property
    def repository(self) -> ResumeRepository:
        """@brief 返回 Resume 聚合 repository / Return the Resume aggregate repository."""

    @property
    def templates(self) -> ResumeTemplateCatalog:
        """@brief 返回不可变模板 catalog / Return the immutable template catalog."""

    @property
    def worker_jobs(self) -> ResumeWorkerJobStore:
        """@brief 返回 worker Job store / Return the worker Job store."""

    @property
    def worker_results(self) -> ResumeWorkerResultStore:
        """@brief 返回第二阶段结果 sink / Return the second-phase result sink."""

    async def __aenter__(self) -> Self:
        """@brief 开始短事务 / Enter a short transaction."""

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时回滚 / Roll back on error or absent commit."""

    async def commit(self) -> None:
        """@brief 原子提交当前阶段 / Atomically commit the current phase."""


class ResumeWorkerUnitOfWorkFactory(Protocol):
    """@brief 从 durable event 身份创建 worker UoW / Create worker UoWs from durable-event identity."""

    def __call__(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
    ) -> ResumeWorkerUnitOfWork:
        """@brief 创建已密封 actor/Workspace scope 的 UoW / Create a UoW sealed to actor and Workspace."""


class ResumeImportCapability(Protocol):
    """@brief 安全文档解析能力 / Safe document-parsing capability."""

    async def import_resume(
        self,
        workspace_id: WorkspaceId,
        source: ResumeImportSource,
        *,
        operation_id: str,
    ) -> ResumeImportedContent:
        """@brief 在事务外把已验证对象转为纯语义文本 / Convert a verified object to semantic text outside transactions."""


class ResumeRenderCapability(Protocol):
    """@brief 多格式 Resume renderer / Multi-format Resume renderer."""

    async def render_resume(
        self,
        document: ResumeDocument,
        formats: Sequence[RenderFormat],
        *,
        operation_id: str,
    ) -> tuple[RenderedResumeArtifact, ...]:
        """@brief 在事务外渲染不可变 revision / Render an immutable revision outside transactions."""


class ResumeUploadObjectReader(Protocol):
    """@brief import adapter 所需的 server-side object reader / Server-side object reader required by import."""

    def read(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        """@brief 流式读取隔离 upload 对象 / Stream the isolated upload object."""


class ResumeCapabilityFailure(RuntimeError):
    """@brief capability 的稳定、脱敏失败 / Stable redacted capability failure."""

    code: str
    """@brief 可公开稳定 code / Public-safe stable code."""

    retryable: bool
    """@brief 相同 operation ID 是否值得重试 / Whether the same operation ID may be retried."""

    def __init__(self, code: str, *, retryable: bool) -> None:
        """@brief 初始化 capability 失败 / Initialize a capability failure.

        @param code 稳定错误码 / Stable error code.
        @param retryable 是否为瞬态失败 / Whether the failure is transient.
        """
        super().__init__(code)
        self.code = code
        self.retryable = retryable


__all__ = [
    "PersistedResumeJob",
    "RenderedResumeArtifact",
    "ResumeCapabilityFailure",
    "ResumeImportCapability",
    "ResumeImportSource",
    "ResumeImportedContent",
    "ResumeRenderCapability",
    "ResumeUploadObjectReader",
    "ResumeWorkerJobStore",
    "ResumeWorkerResultStore",
    "ResumeWorkerUnitOfWork",
    "ResumeWorkerUnitOfWorkFactory",
    "resume_worker_artifact_id",
]
