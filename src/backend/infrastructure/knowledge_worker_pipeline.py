"""@brief Knowledge worker 的来源读取、解析、分块与 embedding pipeline / Source loading, parsing, chunking, and embedding pipeline for Knowledge workers.

Pipeline 的输入是第一段数据库事务冻结的 typed claim。所有网络、对象读取、解析和
embedding 均发生在事务外；未知来源没有隐式 fallback，必须由 composition 显式注册。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from html.parser import HTMLParser
from pathlib import PurePosixPath
from typing import Protocol, cast
from urllib.parse import unquote, urlsplit
from xml.parsers import expat

import sqlalchemy as sa

from backend.application.ports.knowledge_worker import (
    KnowledgeMaterial,
    KnowledgeWorkerClaim,
    KnowledgeWorkerTerminalFailure,
    PreparedEmbeddingSpace,
    PreparedKnowledgeChunk,
    PreparedKnowledgeIndex,
)
from backend.domain.common import DomainError
from backend.domain.knowledge import (
    KnowledgeContentType,
    KnowledgeDocumentPart,
    ParsedKnowledgeDocument,
)
from backend.domain.knowledge_sources import (
    FileSourceInput,
    GitSourceInput,
    KnowledgeSourceType,
    ManualSourceInput,
    ResumeSourceInput,
    UrlSourceInput,
)
from backend.domain.ports import EmbeddingProvider, KnowledgeFileParser
from backend.domain.principals import WorkspaceId
from backend.domain.upload_sessions import UploadSessionId
from backend.infrastructure.knowledge_network import PinnedHttpSourceFetcher
from backend.infrastructure.knowledge_search import (
    EmbeddingSpaceSelection,
    l2_normalized_vector,
)
from backend.infrastructure.knowledge_uploads import UploadByteSource
from backend.infrastructure.persistence.database import AsyncDatabase


class ExternalKnowledgeSourceAdapter(Protocol):
    """@brief Git/cloud 等显式 provider 来源 adapter / Explicit provider adapter for Git, cloud, and similar sources."""

    async def load(self, claim: KnowledgeWorkerClaim) -> KnowledgeMaterial:
        """@brief 读取一个不可变 snapshot / Load one immutable snapshot."""

    async def erase(self, claim: KnowledgeWorkerClaim, *, operation_id: str) -> None:
        """@brief 幂等擦除 provider material / Idempotently erase provider material."""


class DeletableUploadByteSource(UploadByteSource, Protocol):
    """@brief 可读取并删除的上传对象 Port / Upload-object port supporting reads and deletion."""

    async def delete_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> None:
        """@brief 幂等删除一个上传对象 / Idempotently delete one upload object."""


class PostgresKnowledgeMaterialLoader:
    """@brief 对内读取 file/Resume，对外经 pinned fetcher/显式 adapter 读取来源 / Load file and Resume internally and external sources through pinned or registered adapters."""

    def __init__(
        self,
        database: AsyncDatabase,
        upload_source: UploadByteSource,
        http_fetcher: PinnedHttpSourceFetcher,
        *,
        maximum_material_bytes: int,
        external_adapters: Mapping[KnowledgeSourceType, ExternalKnowledgeSourceAdapter]
        | None = None,
    ) -> None:
        """@brief 绑定闭合来源分发表 / Bind a closed source-dispatch table."""

        if not 1 <= maximum_material_bytes <= 1_073_741_824:
            raise ValueError("Knowledge material bound must be one byte to one GiB")
        allowed_external = {
            KnowledgeSourceType.GIT_REPOSITORY,
            KnowledgeSourceType.CLOUD_DRIVE,
        }
        adapters = {} if external_adapters is None else external_adapters
        if not set(adapters) <= allowed_external:
            raise ValueError("Knowledge external adapter table contains an unsupported source type")
        self._database = database
        self._upload_source = upload_source
        self._http_fetcher = http_fetcher
        self._maximum_material_bytes = maximum_material_bytes
        self._external_adapters = dict(adapters)

    async def load(self, claim: KnowledgeWorkerClaim) -> KnowledgeMaterial:
        """@brief 按判别联合穷尽分发，未知能力 fail closed / Dispatch exhaustively by the discriminated union and fail closed for absent capabilities."""

        source_input = claim.source_input
        if source_input is None:
            raise KnowledgeWorkerTerminalFailure("knowledge.source_input_missing")
        if isinstance(source_input, ManualSourceInput):
            content = source_input.content.encode("utf-8")
            self._require_material_bound(content)
            return KnowledgeMaterial(
                content,
                "manual-note.md",
                "text/markdown",
                {"source_type": source_input.source_type.value},
            )
        if isinstance(source_input, FileSourceInput):
            return await self._load_file(claim, source_input)
        if isinstance(source_input, ResumeSourceInput):
            return await self._load_resume(claim, source_input)
        if isinstance(source_input, UrlSourceInput):
            fetched = await self._http_fetcher.fetch(source_input.url)
            self._require_material_bound(fetched.content)
            filename = _filename_for_url(fetched.final_url, fetched.media_type)
            return KnowledgeMaterial(
                fetched.content,
                filename,
                fetched.media_type,
                {
                    "source_type": source_input.source_type.value,
                    "url": fetched.final_url,
                },
            )
        if isinstance(source_input, GitSourceInput):
            return await self._load_external(KnowledgeSourceType.GIT_REPOSITORY, claim)
        return await self._load_external(KnowledgeSourceType.CLOUD_DRIVE, claim)

    def _require_material_bound(self, content: bytes) -> None:
        """@brief 对所有来源统一执行非空与 byte 上限 / Apply one non-empty byte bound to every source."""

        if not content:
            raise KnowledgeWorkerTerminalFailure("knowledge.source_material_empty")
        if len(content) > self._maximum_material_bytes:
            raise KnowledgeWorkerTerminalFailure("knowledge.source_material_too_large")

    async def _load_file(
        self,
        claim: KnowledgeWorkerClaim,
        source_input: FileSourceInput,
    ) -> KnowledgeMaterial:
        """@brief 从已完成 upload 的 server-side reader 重取 bytes / Re-read bytes from a completed upload's server-side reader."""

        filename = claim.source_metadata.get("filename")
        media_type = claim.source_metadata.get("media_type")
        if not filename or not media_type:
            raise KnowledgeWorkerTerminalFailure("knowledge.file_metadata_invalid")
        content = await _read_upload_bounded(
            self._upload_source,
            claim.workspace_id,
            source_input.upload_session_id,
            self._maximum_material_bytes,
        )
        return KnowledgeMaterial(
            content,
            filename,
            media_type,
            {
                "source_type": source_input.source_type.value,
                "upload_session_id": str(source_input.upload_session_id),
            },
        )

    async def _load_resume(
        self,
        claim: KnowledgeWorkerClaim,
        source_input: ResumeSourceInput,
    ) -> KnowledgeMaterial:
        """@brief 在真实 actor+Workspace RLS 下冻结当前 Resume SIR / Freeze the current Resume SIR under real actor-and-Workspace RLS."""

        async with self._database.new_session() as session:
            async with session.begin():
                await self._database.install_v2_request_scope(
                    session,
                    actor_id=str(claim.actor_id),
                    workspace_id=str(claim.workspace_id),
                )
                document = (
                    (
                        await session.execute(
                            sa.text(
                                "SELECT revision.semantic_document, revision.revision_no "
                                "FROM resume.documents AS document "
                                "JOIN resume.revisions AS revision "
                                "ON revision.resume_id = document.id "
                                "AND revision.workspace_id = document.workspace_id "
                                "AND revision.revision_no = document.current_revision_no "
                                "WHERE document.workspace_id = :workspace_id "
                                "AND document.id = :resume_id AND document.deleted_at IS NULL"
                            ),
                            {
                                "workspace_id": str(claim.workspace_id),
                                "resume_id": str(source_input.resume_id),
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        if document is None:
            raise KnowledgeWorkerTerminalFailure("knowledge.resume_unavailable")
        payload = json.dumps(
            document["semantic_document"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if not payload or len(payload) > self._maximum_material_bytes:
            raise KnowledgeWorkerTerminalFailure("knowledge.resume_material_too_large")
        return KnowledgeMaterial(
            payload,
            "resume.json",
            "application/json",
            {
                "source_type": source_input.source_type.value,
                "resume_id": str(source_input.resume_id),
                "resume_revision": str(document["revision_no"]),
            },
        )

    async def _load_external(
        self,
        source_type: KnowledgeSourceType,
        claim: KnowledgeWorkerClaim,
    ) -> KnowledgeMaterial:
        """@brief 要求 composition 显式提供 provider adapter / Require an explicit provider adapter from composition."""

        adapter = self._external_adapters.get(source_type)
        if adapter is None:
            raise KnowledgeWorkerTerminalFailure("knowledge.source_adapter_unconfigured")
        material = await adapter.load(claim)
        if len(material.content) > self._maximum_material_bytes:
            raise KnowledgeWorkerTerminalFailure("knowledge.source_material_too_large")
        return material


class CompositeKnowledgeSourceEraser:
    """@brief 幂等删除本地对象或委派外部 provider / Idempotently delete local objects or delegate to external providers."""

    def __init__(
        self,
        upload_source: DeletableUploadByteSource,
        *,
        external_adapters: Mapping[KnowledgeSourceType, ExternalKnowledgeSourceAdapter]
        | None = None,
    ) -> None:
        """@brief 绑定上传存储与显式 provider 删除器 / Bind upload storage and explicit provider erasers."""

        self._upload_source = upload_source
        self._external_adapters = dict(external_adapters or {})

    async def erase(self, claim: KnowledgeWorkerClaim, *, operation_id: str) -> None:
        """@brief file 擦除对象，其余只在确有外部持久 material 时委派 / Erase file objects and delegate only for externally persisted material."""

        source_input = claim.source_input
        if isinstance(source_input, FileSourceInput):
            await self._upload_source.delete_object(
                claim.workspace_id,
                source_input.upload_session_id,
            )
            return
        if source_input is None:
            return
        adapter = self._external_adapters.get(source_input.source_type)
        if adapter is not None:
            await adapter.erase(claim, operation_id=operation_id)


class KnowledgeIndexPipeline:
    """@brief 有界且确定性的 parse/chunk/embed pipeline / Bounded deterministic parse/chunk/embed pipeline."""

    def __init__(
        self,
        parser: KnowledgeFileParser,
        embedder: EmbeddingProvider,
        embedding_space: EmbeddingSpaceSelection,
        *,
        model_region: str,
        external_model_processing: bool,
        maximum_extracted_characters: int,
        maximum_chunks: int,
        chunk_max_characters: int,
        chunk_overlap_characters: int,
        embedding_batch_size: int = 64,
    ) -> None:
        """@brief 注入 parser、不可变模型身份与资源上限 / Inject parser, immutable model identity, and resource bounds."""

        if model_region not in {"cn", "global", "private_deployment"}:
            raise ValueError("Knowledge model region is invalid")
        if not 1 <= maximum_extracted_characters <= 10_000_000:
            raise ValueError("Knowledge extracted-character bound is invalid")
        if not 1 <= maximum_chunks <= 100_000:
            raise ValueError("Knowledge chunk-count bound is invalid")
        if not 100 <= chunk_max_characters <= 50_000:
            raise ValueError("Knowledge chunk size is invalid")
        if not 0 <= chunk_overlap_characters < chunk_max_characters:
            raise ValueError("Knowledge chunk overlap is invalid")
        if not 1 <= embedding_batch_size <= 512:
            raise ValueError("Knowledge embedding batch size is invalid")
        self._parser = parser
        self._embedder = embedder
        self._selection = embedding_space
        self._model_region = model_region
        self._external_model_processing = external_model_processing
        self._maximum_extracted_characters = maximum_extracted_characters
        self._maximum_chunks = maximum_chunks
        self._chunk_max_characters = chunk_max_characters
        self._chunk_overlap_characters = chunk_overlap_characters
        self._embedding_batch_size = embedding_batch_size

    async def build(
        self,
        claim: KnowledgeWorkerClaim,
        material: KnowledgeMaterial,
    ) -> PreparedKnowledgeIndex:
        """@brief 在 transaction 外生成完整不可变 index / Build the complete immutable index outside a transaction."""

        if self._external_model_processing:
            if not claim.allow_external_model_processing:
                raise KnowledgeWorkerTerminalFailure("knowledge.external_embedding_not_allowed")
            if self._model_region not in claim.allowed_model_regions:
                raise KnowledgeWorkerTerminalFailure("knowledge.embedding_region_not_allowed")
        try:
            parsed = await self._parse(material)
        except DomainError as error:
            if error.problem.retryable:
                raise
            raise KnowledgeWorkerTerminalFailure(error.problem.code) from error
        extracted_characters = sum(len(part.text) for part in parsed.parts)
        if extracted_characters > self._maximum_extracted_characters:
            raise KnowledgeWorkerTerminalFailure("knowledge.extracted_text_too_large")
        chunk_inputs = _chunk_document(
            parsed,
            maximum_chunks=self._maximum_chunks,
            maximum_characters=self._chunk_max_characters,
            overlap_characters=self._chunk_overlap_characters,
        )
        if not chunk_inputs:
            raise KnowledgeWorkerTerminalFailure("knowledge.no_extractable_text")
        vectors: list[tuple[float, ...]] = []
        for offset in range(0, len(chunk_inputs), self._embedding_batch_size):
            texts = [item[0] for item in chunk_inputs[offset : offset + self._embedding_batch_size]]
            batch = await self._embedder.embed(texts)
            if len(batch) != len(texts):
                raise RuntimeError("embedding provider returned an incomplete batch")
            vectors.extend(
                l2_normalized_vector(
                    vector,
                    expected_dimension=self._selection.dimension,
                )
                for vector in batch
            )
        space = _prepared_space(self._selection, claim)
        chunks = tuple(
            PreparedKnowledgeChunk(
                ordinal,
                text,
                locator,
                content_type,
                vectors[ordinal],
            )
            for ordinal, (text, locator, content_type) in enumerate(chunk_inputs)
        )
        return PreparedKnowledgeIndex(
            hashlib.sha256(material.content).hexdigest(),
            len(material.content),
            parsed,
            chunks,
            space,
        )

    async def _parse(self, material: KnowledgeMaterial) -> ParsedKnowledgeDocument:
        """@brief 对网页/feed/JSON 做安全文本化，其余交给文件 parser / Safely textify web/feed/JSON and delegate other formats to the file parser."""

        media_type = material.media_type.partition(";")[0].strip().lower()
        if media_type == "text/html":
            text = _html_text(material.content, self._maximum_extracted_characters)
            return _single_part_document(text, "html", material.source_metadata)
        if media_type in {
            "application/xml",
            "text/xml",
            "application/atom+xml",
            "application/rss+xml",
        }:
            text = _xml_text(material.content, self._maximum_extracted_characters)
            return _single_part_document(text, "xml-feed", material.source_metadata)
        if media_type == "application/json":
            try:
                value = json.loads(material.content)
                text = json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
                raise KnowledgeWorkerTerminalFailure("knowledge.json_invalid") from error
            if len(text) > self._maximum_extracted_characters:
                raise KnowledgeWorkerTerminalFailure("knowledge.extracted_text_too_large")
            return _single_part_document(text, "json", material.source_metadata)
        return await self._parser.parse(material.filename, media_type, material.content)


async def _read_upload_bounded(
    source: UploadByteSource,
    workspace_id: WorkspaceId,
    upload_id: UploadSessionId,
    maximum_bytes: int,
) -> bytes:
    """@brief 有界读取服务端上传 stream / Read a server-side upload stream within a hard bound."""

    body = bytearray()
    async with source.read(workspace_id, upload_id) as chunks:
        async for chunk in chunks:
            body.extend(chunk)
            if len(body) > maximum_bytes:
                raise KnowledgeWorkerTerminalFailure("knowledge.source_material_too_large")
    if not body:
        raise KnowledgeWorkerTerminalFailure("knowledge.source_material_empty")
    return bytes(body)


def _filename_for_url(url: str, media_type: str) -> str:
    """@brief 从 canonical URL 和媒体类型构造安全 parser filename / Build a safe parser filename from a canonical URL and media type."""

    raw_name = PurePosixPath(unquote(urlsplit(url).path)).name
    name = raw_name if 1 <= len(raw_name) <= 300 else "source"
    suffixes = {
        "text/plain": ".txt",
        "text/markdown": ".md",
        "text/html": ".html",
        "application/pdf": ".pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/xml": ".xml",
        "application/atom+xml": ".xml",
        "application/rss+xml": ".xml",
    }
    suffix = suffixes.get(media_type.lower())
    return name if suffix is None or name.lower().endswith(suffix) else name + suffix


class _VisibleHtmlText(HTMLParser):
    """@brief 忽略 script/style 的有界 HTML 文本抽取器 / Bounded HTML text extractor ignoring script and style."""

    def __init__(self, maximum_characters: int) -> None:
        super().__init__(convert_charrefs=True)
        self.maximum_characters = maximum_characters
        self.hidden_depth = 0
        self.parts: list[str] = []
        self.characters = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """@brief 进入不可见元素 / Enter a hidden element."""

        del attrs
        if tag.lower() in {"script", "style", "noscript", "template", "svg"}:
            self.hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        """@brief 离开不可见元素 / Leave a hidden element."""

        if tag.lower() in {"script", "style", "noscript", "template", "svg"}:
            self.hidden_depth = max(0, self.hidden_depth - 1)

    def handle_data(self, data: str) -> None:
        """@brief 累加规范化可见文本 / Accumulate normalized visible text."""

        if self.hidden_depth:
            return
        value = " ".join(data.split())
        if not value:
            return
        self.characters += len(value) + 1
        if self.characters > self.maximum_characters:
            raise KnowledgeWorkerTerminalFailure("knowledge.extracted_text_too_large")
        self.parts.append(value)


def _html_text(content: bytes, maximum_characters: int) -> str:
    """@brief 以 UTF-8 有界解析 HTML 可见文本 / Parse bounded visible HTML text as UTF-8."""

    try:
        value = content.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise KnowledgeWorkerTerminalFailure("knowledge.html_encoding_invalid") from error
    parser = _VisibleHtmlText(maximum_characters)
    parser.feed(value)
    parser.close()
    text = "\n".join(parser.parts).strip()
    if not text:
        raise KnowledgeWorkerTerminalFailure("knowledge.no_extractable_text")
    return text


def _xml_text(content: bytes, maximum_characters: int) -> str:
    """@brief 用 SAX-style Expat 拒绝 DTD/entity 并有界抽取 / Reject DTD/entities and extract within bounds using SAX-style Expat."""

    parts: list[str] = []
    characters = 0

    def append_text(candidate: str) -> None:
        """@brief 按文档顺序累加规范化 character data / Accumulate normalized character data in document order."""

        nonlocal characters
        value = " ".join(candidate.split())
        if not value:
            return
        characters += len(value) + 1
        if characters > maximum_characters:
            raise KnowledgeWorkerTerminalFailure("knowledge.extracted_text_too_large")
        parts.append(value)

    def reject_doctype(
        doctype_name: str,
        system_id: str | None,
        public_id: str | None,
        has_internal_subset: int,
    ) -> None:
        """@brief 拒绝所有 DTD，不解析内部或外部实体 / Reject every DTD without parsing internal or external entities."""

        del doctype_name, system_id, public_id, has_internal_subset
        raise KnowledgeWorkerTerminalFailure("knowledge.xml_unsafe")

    parser = expat.ParserCreate()
    parser.buffer_text = True
    parser.CharacterDataHandler = append_text
    parser.StartDoctypeDeclHandler = reject_doctype
    try:
        for offset in range(0, len(content), 1024 * 1024):
            parser.Parse(content[offset : offset + 1024 * 1024], False)
        parser.Parse(b"", True)
    except KnowledgeWorkerTerminalFailure:
        raise
    except expat.ExpatError as error:
        raise KnowledgeWorkerTerminalFailure("knowledge.xml_invalid") from error
    text = "\n".join(parts).strip()
    if not text:
        raise KnowledgeWorkerTerminalFailure("knowledge.no_extractable_text")
    return text


def _single_part_document(
    text: str,
    parser: str,
    source_metadata: Mapping[str, str],
) -> ParsedKnowledgeDocument:
    """@brief 构造保留 source provenance 的单 part 文档 / Build a single-part document preserving source provenance."""

    metadata = {**source_metadata, "path": source_metadata.get("url", "document/1")}
    return ParsedKnowledgeDocument(
        (
            KnowledgeDocumentPart(
                text,
                KnowledgeContentType.GENERAL,
                cast(dict[str, object], metadata),
            ),
        ),
        {"parser": parser, "extracted_characters": len(text)},
    )


def _chunk_document(
    parsed: ParsedKnowledgeDocument,
    *,
    maximum_chunks: int,
    maximum_characters: int,
    overlap_characters: int,
) -> list[tuple[str, str, str]]:
    """@brief 按语义 part 稳定切分并保留 locator / Deterministically chunk semantic parts while preserving locators."""

    chunks: list[tuple[str, str, str]] = []
    for part_index, part in enumerate(parsed.parts):
        path = part.metadata.get("path")
        base_locator = path if isinstance(path, str) and path else f"part/{part_index}"
        for local_index, text in enumerate(
            _chunk_text(part.text, maximum_characters, overlap_characters)
        ):
            chunks.append(
                (
                    text,
                    f"{base_locator}#chunk={local_index}",
                    part.content_type.value,
                )
            )
            if len(chunks) > maximum_chunks:
                raise KnowledgeWorkerTerminalFailure("knowledge.chunk_count_exceeded")
    return chunks


def _chunk_text(content: str, maximum: int, overlap: int) -> list[str]:
    """@brief 在自然边界优先的滑窗中确定性切分 / Deterministically split in a natural-boundary-first sliding window."""

    normalized = content.strip()
    if not normalized:
        return []
    chunks: list[str] = []
    start = 0
    boundary_window = max(80, maximum // 3)
    while start < len(normalized):
        hard_end = min(start + maximum, len(normalized))
        end = hard_end
        if hard_end < len(normalized):
            window_start = max(start + 1, hard_end - boundary_window)
            for marker in ("\n\n", "\n", "。", ". ", "；", "; ", "，", ", ", " "):
                candidate = normalized.rfind(marker, window_start, hard_end)
                if candidate >= window_start:
                    end = candidate + len(marker)
                    break
        value = normalized[start:end].strip()
        if value:
            chunks.append(value)
        if end >= len(normalized):
            break
        start = max(end - overlap, start + 1)
    return chunks


def _prepared_space(
    selection: EmbeddingSpaceSelection,
    claim: KnowledgeWorkerClaim,
) -> PreparedEmbeddingSpace:
    """@brief 为 Workspace+owner+模型身份派生稳定 space ID / Derive a stable space ID from Workspace, owner, and model identity."""

    owner_id = claim.source_owner_id
    if owner_id is None:
        raise KnowledgeWorkerTerminalFailure("knowledge.source_owner_missing")
    framed = "\x00".join(
        (
            str(claim.workspace_id),
            str(owner_id),
            selection.provider,
            selection.model,
            selection.model_revision,
            str(selection.dimension),
            selection.distance_metric,
            selection.normalization,
        )
    ).encode("utf-8")
    identifier = "embsp_" + hashlib.sha256(framed).hexdigest()[:32]
    return PreparedEmbeddingSpace(
        identifier,
        selection.provider,
        selection.model,
        selection.model_revision,
        selection.dimension,
        selection.distance_metric,
        selection.normalization,
    )


__all__ = [
    "CompositeKnowledgeSourceEraser",
    "DeletableUploadByteSource",
    "ExternalKnowledgeSourceAdapter",
    "KnowledgeIndexPipeline",
    "PostgresKnowledgeMaterialLoader",
]
