"""Bounded TXT, Markdown, PDF, and DOCX parsing for personal knowledge."""

from __future__ import annotations

import asyncio
import re
from io import BytesIO
from pathlib import Path
from typing import Any

from docx import Document
from pypdf import PdfReader
from pypdf.errors import PdfReadError

from backend.domain.common import DomainError, Problem
from backend.domain.knowledge import (
    KnowledgeContentType,
    KnowledgeDocumentPart,
    ParsedKnowledgeDocument,
)

_MARKDOWN_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


class LocalKnowledgeFileParser:
    """Parse supported files while preserving page/heading/paragraph locators."""

    def __init__(self, max_extracted_characters: int) -> None:
        self._max_extracted_characters = max_extracted_characters

    async def parse(
        self,
        filename: str,
        content_type: str,
        content: bytes,
    ) -> ParsedKnowledgeDocument:
        """Dispatch CPU/blocking parser work away from the event loop."""
        return await asyncio.to_thread(self._parse_sync, filename, content_type, content)

    def _parse_sync(
        self,
        filename: str,
        content_type: str,
        content: bytes,
    ) -> ParsedKnowledgeDocument:
        suffix = Path(filename).suffix.lower()
        if content_type == "application/pdf" and suffix == ".pdf":
            return self._parse_pdf(content)
        if (
            content_type
            == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            and suffix == ".docx"
        ):
            return self._parse_docx(content)
        if content_type in {"text/plain", "text/markdown"} and suffix in {
            ".txt",
            ".md",
            ".markdown",
        }:
            return self._parse_text(content, markdown=content_type == "text/markdown")
        raise DomainError(
            Problem("knowledge.file_type_unsupported", 422, "Knowledge file type is unsupported")
        )

    def _parse_text(self, content: bytes, *, markdown: bool) -> ParsedKnowledgeDocument:
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            raise DomainError(
                Problem("knowledge.file_encoding_invalid", 422, "Text knowledge files must use UTF-8")
            ) from error
        if "\x00" in text:
            raise DomainError(
                Problem("knowledge.file_encoding_invalid", 422, "Text knowledge file contains binary data")
            )
        parts = self._markdown_parts(text) if markdown else self._plain_text_parts(text)
        return self._finish(parts, {"parser": "markdown" if markdown else "plain_text"})

    def _parse_pdf(self, content: bytes) -> ParsedKnowledgeDocument:
        try:
            reader = PdfReader(BytesIO(content))
            if reader.is_encrypted and reader.decrypt("") == 0:
                raise DomainError(
                    Problem("knowledge.pdf_encrypted", 422, "Encrypted PDF files are unsupported")
                )
            parts = [
                KnowledgeDocumentPart(
                    text=text,
                    content_type=KnowledgeContentType.GENERAL,
                    metadata={"page": page_number, "path": f"page/{page_number}"},
                )
                for page_number, page in enumerate(reader.pages, start=1)
                if (text := (page.extract_text() or "").strip())
            ]
        except DomainError:
            raise
        except (PdfReadError, OSError, ValueError) as error:
            raise DomainError(
                Problem("knowledge.pdf_invalid", 422, "PDF file could not be parsed")
            ) from error
        return self._finish(parts, {"parser": "pypdf", "page_count": len(reader.pages)})

    def _parse_docx(self, content: bytes) -> ParsedKnowledgeDocument:
        try:
            document = Document(BytesIO(content))
        except (OSError, ValueError, KeyError) as error:
            raise DomainError(
                Problem("knowledge.docx_invalid", 422, "DOCX file could not be parsed")
            ) from error
        parts: list[KnowledgeDocumentPart] = []
        heading: str | None = None
        for index, paragraph in enumerate(document.paragraphs, start=1):
            text = paragraph.text.strip()
            if not text:
                continue
            style_name = str(paragraph.style.name or "") if paragraph.style is not None else ""
            if style_name.lower().startswith("heading"):
                heading = text
                continue
            parts.append(
                KnowledgeDocumentPart(
                    text=text,
                    content_type=KnowledgeContentType.GENERAL,
                    metadata={
                        "heading": heading,
                        "paragraph": index,
                        "path": f"paragraph/{index}",
                    },
                )
            )
        for table_index, table in enumerate(document.tables, start=1):
            for row_index, row in enumerate(table.rows, start=1):
                text = " | ".join(cell.text.strip() for cell in row.cells if cell.text.strip())
                if text:
                    parts.append(
                        KnowledgeDocumentPart(
                            text=text,
                            content_type=KnowledgeContentType.GENERAL,
                            metadata={
                                "heading": heading,
                                "path": f"table/{table_index}/row/{row_index}",
                            },
                        )
                    )
        return self._finish(
            parts,
            {
                "parser": "python-docx",
                "paragraph_count": len(document.paragraphs),
                "table_count": len(document.tables),
            },
        )

    def _markdown_parts(self, text: str) -> list[KnowledgeDocumentPart]:
        parts: list[KnowledgeDocumentPart] = []
        heading: str | None = None
        buffer: list[str] = []
        start_line = 1

        def flush(end_line: int) -> None:
            nonlocal buffer, start_line
            value = "\n".join(buffer).strip()
            if value:
                parts.append(
                    KnowledgeDocumentPart(
                        text=value,
                        content_type=KnowledgeContentType.GENERAL,
                        metadata={
                            "heading": heading,
                            "line_start": start_line,
                            "line_end": end_line,
                            "path": f"line/{start_line}",
                        },
                    )
                )
            buffer = []

        for line_number, line in enumerate(text.splitlines(), start=1):
            match = _MARKDOWN_HEADING.match(line)
            if match:
                flush(line_number - 1)
                heading = match.group(2).strip()
                start_line = line_number + 1
            elif line.strip():
                if not buffer:
                    start_line = line_number
                buffer.append(line.rstrip())
            else:
                flush(line_number - 1)
                start_line = line_number + 1
        flush(len(text.splitlines()))
        return parts

    @staticmethod
    def _plain_text_parts(text: str) -> list[KnowledgeDocumentPart]:
        parts: list[KnowledgeDocumentPart] = []
        for index, paragraph in enumerate(re.split(r"\n\s*\n", text), start=1):
            value = paragraph.strip()
            if value:
                parts.append(
                    KnowledgeDocumentPart(
                        text=value,
                        content_type=KnowledgeContentType.GENERAL,
                        metadata={"paragraph": index, "path": f"paragraph/{index}"},
                    )
                )
        return parts

    def _finish(
        self,
        parts: list[KnowledgeDocumentPart],
        metadata: dict[str, Any],
    ) -> ParsedKnowledgeDocument:
        extracted_characters = sum(len(part.text) for part in parts)
        if extracted_characters == 0:
            raise DomainError(
                Problem(
                    "knowledge.file_no_extractable_text",
                    422,
                    "Knowledge file contains no extractable text; OCR is not enabled",
                )
            )
        if extracted_characters > self._max_extracted_characters:
            raise DomainError(
                Problem(
                    "knowledge.extracted_text_too_large",
                    413,
                    "Extracted knowledge text exceeds the configured limit",
                )
            )
        return ParsedKnowledgeDocument(
            parts=tuple(parts),
            metadata={**metadata, "extracted_characters": extracted_characters},
        )


__all__ = ["LocalKnowledgeFileParser"]
