"""@brief API v2 Resume 长任务与 outbox 事件 / API v2 Resume jobs and outbox events."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resumes import (
    JsonValue,
    ResumeDomainError,
    ResumeId,
    TemplateRef,
)


class ResumeJobKind(StrEnum):
    """@brief Resume 领域长任务种类 / Resume-domain long-running job kinds."""

    IMPORT = "resume.import"
    RESTORE = "resume.restore"
    RENDER = "resume.render"


class RenderMode(StrEnum):
    """@brief Resume 渲染模式 / Resume render modes."""

    PREVIEW = "preview"
    FINAL = "final"
    EXPORT = "export"


class RenderFormat(StrEnum):
    """@brief Resume 渲染输出格式 / Resume render output formats."""

    PDF = "pdf"
    JSON = "json"
    DOCX = "docx"


@dataclass(frozen=True, slots=True)
class ResumeImportSpec:
    """@brief Resume import Job 输入 / Resume import-job input."""

    upload_session_id: str
    title: str
    locale: str
    template: TemplateRef

    def __post_init__(self) -> None:
        """@brief 校验 import 请求的引用与文本字段 / Validate import references and text fields.

        @raise ResumeDomainError upload session、标题或 locale 无效时抛出 / Raised for invalid import specs.
        """
        if re.fullmatch(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$", self.upload_session_id) is None:
            raise ResumeDomainError(
                "resume.invalid_import_request",
                "upload session ID is invalid",
            )
        if not 1 <= len(self.title) <= 300 or re.fullmatch(
            r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$",
            self.locale,
        ) is None:
            raise ResumeDomainError(
                "resume.invalid_import_request",
                "import title or locale is invalid",
            )


@dataclass(frozen=True, slots=True)
class ResumeRestoreSpec:
    """@brief Resume restore Job 输入 / Resume restore-job input."""

    resume_id: ResumeId
    source_revision: int

    def __post_init__(self) -> None:
        """@brief 校验恢复来源 revision / Validate the restore source revision.

        @raise ResumeDomainError revision 非正整数时抛出 / Raised unless revision is positive.
        """
        if self.source_revision < 1:
            raise ResumeDomainError(
                "resume.invalid_restore_request",
                "source revision must be positive",
            )


@dataclass(frozen=True, slots=True)
class ResumeRenderSpec:
    """@brief Resume render Job 输入 / Resume render-job input."""

    resume_id: ResumeId
    resume_revision: int
    mode: RenderMode
    formats: tuple[RenderFormat, ...]

    def __post_init__(self) -> None:
        """@brief 校验渲染 revision 与格式集 / Validate render revision and formats.

        @raise ResumeDomainError 格式为空、重复或 revision 无效时抛出 / Raised for invalid render specs.
        """
        if self.resume_revision < 1 or not self.formats:
            raise ResumeDomainError(
                "resume.invalid_render_request",
                "render revision and formats are required",
            )
        if len(set(self.formats)) != len(self.formats):
            raise ResumeDomainError(
                "resume.invalid_render_request",
                "render formats must be unique",
            )


type ResumeJobSpec = ResumeImportSpec | ResumeRestoreSpec | ResumeRenderSpec
"""@brief Resume Job 输入穷尽 union / Exhaustive Resume-job input union."""


@dataclass(frozen=True, slots=True)
class ResumeOutboxEvent:
    """@brief 与 Resume 变更同事务持久化的 outbox 事件 / Outbox event persisted with a Resume change."""

    event_id: str
    workspace_id: WorkspaceId
    event_type: str
    occurred_at: datetime
    actor_id: UserId
    subject: ResourceRef
    data: dict[str, JsonValue]

    def __post_init__(self) -> None:
        """@brief 校验 outbox 事件的可公开语义 / Validate public-safe outbox semantics.

        @raise ResumeDomainError 事件类型或时间无效时抛出 / Raised for invalid event metadata.
        """
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ResumeDomainError(
                "resume.invalid_outbox_event",
                "outbox timestamp must be timezone-aware",
            )
        if not self.event_type.startswith("resume.") or len(self.data) > 40:
            raise ResumeDomainError(
                "resume.invalid_outbox_event",
                "outbox event type or data is invalid",
            )


__all__ = [
    "RenderFormat",
    "RenderMode",
    "ResumeImportSpec",
    "ResumeJobKind",
    "ResumeJobSpec",
    "ResumeOutboxEvent",
    "ResumeRenderSpec",
    "ResumeRestoreSpec",
]
