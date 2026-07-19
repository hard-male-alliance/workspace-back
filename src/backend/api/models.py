"""@brief 缺失契约的明确 mock 请求适配器 / Explicit mock request adapters for missing contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class MockContractAdapter(BaseModel):
    """@brief 无正式路径级契约时的临时 DTO / Temporary DTO where no formal path-level contract exists.

    @note MOCK — 不得视作 `contract/` 的正式补充；待确认项记录在 docs/CONTRACT_GAPS.md。
    """

    model_config = ConfigDict(extra="forbid", json_schema_extra={"x-contract-status": "mock", "x-pending-contract": True})


class MockResumeCreateRequest(MockContractAdapter):
    """@brief 简历创建的 mock 请求 / Mock request for resume creation."""

    title: str = Field(min_length=1, max_length=300)
    locale: str = Field(default="zh-CN", min_length=2, max_length=32)
    template_id: str = Field(default="tpl_default_v1", min_length=8, max_length=128)
    template_version: str = Field(default="1.0", min_length=1, max_length=128)


class MockResumeProposalCreateRequest(MockContractAdapter):
    """Temporary phase-one request for evidence-grounded Proposal generation."""

    instruction: str = Field(min_length=1, max_length=4000)
    title: str | None = Field(default=None, min_length=1, max_length=300)
    source_ids: list[str] = Field(default_factory=list, max_length=100)
    draft_text: str | None = Field(default=None, min_length=1, max_length=200000)
    target: dict[str, Any] = Field(default_factory=lambda: {"entity_type": "profile"})
    field_path: list[str] = Field(default_factory=lambda: ["summary"], min_length=1, max_length=20)
    render_hint: Literal["none", "preview", "final"] = "preview"


class MockConversationCreateRequest(MockContractAdapter):
    """@brief Conversation 创建的 mock 请求 / Mock request for conversation creation."""

    capability: str = Field(min_length=1, max_length=100)
    title: str | None = Field(default=None, max_length=300)
    context_refs: list[dict[str, Any]] = Field(default_factory=list, max_length=100)


class MockMessageCreateRequest(MockContractAdapter):
    """@brief 用户消息创建的 mock 请求 / Mock request for user-message creation."""

    text: str = Field(min_length=1, max_length=200000)
    parent_message_id: str | None = Field(default=None, min_length=8, max_length=128)


class MockKnowledgeSourceCreateRequest(MockContractAdapter):
    """@brief 确定性知识来源创建 mock 请求 / Mock request for deterministic knowledge-source creation."""

    name: str = Field(min_length=1, max_length=300)
    source_type: Literal["manual_note", "url", "website", "blog_feed", "git_repository"]
    content: str = Field(default="", max_length=200000)
    location: str | None = Field(default=None, max_length=2000)
    visibility: dict[str, Any] | None = None


class MockEndRequest(MockContractAdapter):
    """@brief 面试结束命令 mock 请求 / Mock request for an interview end command."""

    reason: Literal["normal", "technical_abort"] = "normal"


class MockToolApprovalDecision(MockContractAdapter):
    """@brief Tool approval 决策 mock 请求 / Mock request for a tool-approval decision."""

    decision: Literal["approved", "rejected"]


class CursorPage(BaseModel):
    """Opaque cursor metadata shared by browser-facing collection endpoints."""

    model_config = ConfigDict(extra="forbid")

    next_cursor: str | None = None
    has_more: bool
    total_estimate: int | None = Field(default=None, ge=0)


class ResumeListResponse(BaseModel):
    """Paginated formal ResumeDocument collection."""

    model_config = ConfigDict(extra="forbid")

    items: list[dict[str, Any]]
    page: CursorPage


class KnowledgeSourceListResponse(BaseModel):
    """Paginated formal KnowledgeSource collection."""

    model_config = ConfigDict(extra="forbid")

    items: list[dict[str, Any]]
    page: CursorPage


class TemplateManifestListResponse(BaseModel):
    """Paginated formal TemplateManifest collection."""

    model_config = ConfigDict(extra="forbid")

    items: list[dict[str, Any]]
    page: CursorPage


class ResumeProposalListResponse(BaseModel):
    """Paginated ResumeProposal collection for page reload recovery."""

    model_config = ConfigDict(extra="forbid")

    items: list[dict[str, Any]]
    page: CursorPage


class RenderArtifactListResponse(BaseModel):
    """Paginated RenderArtifact metadata collection for preview discovery."""

    model_config = ConfigDict(extra="forbid")

    items: list[dict[str, Any]]
    page: CursorPage
