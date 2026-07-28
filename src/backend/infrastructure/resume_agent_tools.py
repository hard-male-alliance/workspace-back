"""LangChain tools bound to one authorized Resume-Agent execution.

The model sees a small instruction set. Workspace, actor, Resume identity, revision, and
authorization are sealed in ``ResumeToolSession`` and never appear in tool arguments.
Read tools expose only requested semantic content. Draft tools collect identity-free operations;
they never mutate the Resume.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from backend.domain.agent_v2 import AgentResumeContext, AgentResumeOperationDraft
from backend.domain.platform import JsonValue
from backend.domain.resumes import (
    ResumeDocument,
    ResumeDomainError,
    ResumeItem,
    ResumeProfile,
    ResumeSection,
)
from backend.infrastructure.agent_resume_proposals import validate_resume_operation_drafts
from backend.infrastructure.resumes import normalize_resume_operation_wire

_PROFILE = TypeAdapter(ResumeProfile)
_SECTION = TypeAdapter(ResumeSection)
_ITEM = TypeAdapter(ResumeItem)
_DOCUMENT = TypeAdapter(ResumeDocument)
_JSON_VALUE: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class _ClosedInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _NoInput(_ClosedInput):
    pass


class _SectionInput(_ClosedInput):
    section_id: str = Field(min_length=1, max_length=160)


class _ItemInput(_ClosedInput):
    item_id: str = Field(min_length=1, max_length=160)


class _SetFieldInput(_ClosedInput):
    entity_id: str = Field(min_length=1, max_length=160)
    field_path: list[str] = Field(min_length=1, max_length=16)
    value: JsonValue


class _SetProfileFieldInput(_ClosedInput):
    field: str = Field(pattern=r"^(full_name|headline|summary)$")
    value: JsonValue


class _SetDocumentTitleInput(_ClosedInput):
    """@brief 简历文档标题输入 / Resume document-title input."""

    value: str = Field(min_length=1, max_length=300)


class _ProfileFieldUpdateInput(_ClosedInput):
    """@brief 同构批量中的候选人字段更新 / Profile-field update in a homogeneous batch."""

    field: Literal["full_name", "headline", "summary"]
    value: JsonValue


class _SetProfileFieldsInput(_ClosedInput):
    """@brief 一批候选人字段更新 / A batch of candidate profile-field updates."""

    updates: list[_ProfileFieldUpdateInput] = Field(min_length=1, max_length=3)


class _ContactInput(_ClosedInput):
    """@brief 强类型简历联系方式 / Strongly typed Resume contact."""

    id: str = Field(min_length=1, max_length=160)
    kind: str = Field(
        pattern=r"^(email|phone|website|linkedin|github|portfolio|location|other|custom)$"
    )
    label: str | None = Field(default=None, max_length=80)
    value: str = Field(min_length=1, max_length=500)
    url: str | None = Field(default=None, max_length=2_000)


class _SetContactsInput(_ClosedInput):
    """@brief 完整替换联系方式列表 / Replace the complete contact list."""

    contacts: list[_ContactInput] = Field(max_length=30)


class _TextMarkInput(_ClosedInput):
    """@brief 强类型富文本标记 / Strongly typed rich-text mark."""

    start: int = Field(ge=0)
    end: int = Field(gt=0)
    kind: Literal["strong", "emphasis", "link"]
    href: str | None = Field(default=None, max_length=2_000)


class _RichTextInput(_ClosedInput):
    """@brief 强类型富文本 / Strongly typed rich text."""

    text: str = Field(max_length=20_000)
    marks: list[_TextMarkInput] = Field(max_length=1_000)


class _DateRangeInput(_ClosedInput):
    """@brief 强类型简历日期范围 / Strongly typed Resume date range."""

    start: str | None = Field(default=None, max_length=10)
    end: str | None = Field(default=None, max_length=10)


class _ResumeItemInput(_ClosedInput):
    """@brief 完整简历条目输入 / Complete Resume-item input."""

    id: str = Field(min_length=1, max_length=160)
    kind: Literal[
        "experience",
        "education",
        "project",
        "skill_group",
        "publication",
        "award",
        "certification",
        "language",
        "volunteer",
        "custom",
    ]
    title: str | None = Field(default=None, max_length=300)
    subtitle: str | None = Field(default=None, max_length=300)
    organization: str | None = Field(default=None, max_length=300)
    location: str | None = Field(default=None, max_length=300)
    date_range: _DateRangeInput | None = None
    summary: _RichTextInput | None = None
    highlights: list[_RichTextInput] = Field(max_length=100)
    skills: list[str] = Field(max_length=200)
    tags: list[str] = Field(max_length=100)
    visible: bool
    url: str | None = Field(default=None, max_length=2_000)


class _ResumeSectionInput(_ClosedInput):
    """@brief 完整简历模块输入 / Complete Resume-section input."""

    id: str = Field(min_length=1, max_length=160)
    kind: Literal[
        "experience",
        "education",
        "projects",
        "skills",
        "publications",
        "awards",
        "certifications",
        "languages",
        "volunteer",
        "custom",
    ]
    title: str = Field(min_length=1, max_length=120)
    visible: bool
    content: _RichTextInput | None
    items: list[_ResumeItemInput] = Field(max_length=1_000)


class _TemplateRefInput(_ClosedInput):
    """@brief 不可变模板引用输入 / Immutable Template-reference input."""

    template_id: str = Field(min_length=1, max_length=160)
    version: str = Field(min_length=1, max_length=80)


class _UpsertSectionInput(_ClosedInput):
    section: _ResumeSectionInput
    after_section_id: str | None = Field(default=None, max_length=160)


class _UpsertItemInput(_ClosedInput):
    section_id: str = Field(min_length=1, max_length=160)
    item: _ResumeItemInput
    after_item_id: str | None = Field(default=None, max_length=160)


class _RemoveEntityInput(_ClosedInput):
    entity_kind: str = Field(pattern=r"^(section|item)$")
    entity_id: str = Field(min_length=1, max_length=160)


class _MoveEntityInput(_ClosedInput):
    entity_kind: str = Field(pattern=r"^(section|item)$")
    entity_id: str = Field(min_length=1, max_length=160)
    parent_id: str | None = Field(default=None, max_length=160)
    after_id: str | None = Field(default=None, max_length=160)


class _SetTemplateInput(_ClosedInput):
    template: _TemplateRefInput
    settings: dict[str, JsonValue]


class _RequestDecisionInput(_ClosedInput):
    title: str = Field(min_length=1, max_length=300)


class _SetFieldUpdateInput(_ClosedInput):
    """@brief 同构批量中的字段更新 / Field update in a homogeneous batch."""

    entity_id: str = Field(min_length=1, max_length=160)
    field_path: list[str] = Field(min_length=1, max_length=16)
    value: JsonValue


class _SetFieldsInput(_ClosedInput):
    """@brief 一批同构字段更新 / A homogeneous batch of field updates."""

    updates: list[_SetFieldUpdateInput] = Field(min_length=1, max_length=100)


class _UpsertSectionUpdateInput(_ClosedInput):
    """@brief 同构批量中的模块写入 / Section upsert in a homogeneous batch."""

    section: _ResumeSectionInput
    after_section_id: str | None = Field(default=None, max_length=160)


class _UpsertSectionsInput(_ClosedInput):
    """@brief 一批同构模块写入 / A homogeneous batch of section upserts."""

    updates: list[_UpsertSectionUpdateInput] = Field(min_length=1, max_length=50)


class _UpsertItemUpdateInput(_ClosedInput):
    """@brief 同构批量中的条目写入 / Item upsert in a homogeneous batch."""

    section_id: str = Field(min_length=1, max_length=160)
    item: _ResumeItemInput
    after_item_id: str | None = Field(default=None, max_length=160)


class _UpsertItemsInput(_ClosedInput):
    """@brief 一批同构条目写入 / A homogeneous batch of item upserts."""

    updates: list[_UpsertItemUpdateInput] = Field(min_length=1, max_length=100)


class _SkillGroupDraftInput(_ClosedInput):
    """@brief 由后端补全的技能分组 / Backend-completed skill group."""

    name: str = Field(min_length=1, max_length=120)
    skills: list[str] = Field(min_length=1, max_length=100)


class _AddSkillSectionInput(_ClosedInput):
    """@brief 新增技能模块的窄输入 / Narrow input for adding a skills section."""

    title: str = Field(min_length=1, max_length=120)
    groups: list[_SkillGroupDraftInput] = Field(min_length=1, max_length=50)
    after_section_id: str | None = Field(default=None, max_length=160)


class _ExperienceDraftInput(_ClosedInput):
    """@brief 由后端补全的工作经历 / Backend-completed work experience."""

    title: str = Field(min_length=1, max_length=300)
    organization: str = Field(min_length=1, max_length=300)
    location: str | None = Field(default=None, max_length=300)
    date_range: _DateRangeInput | None = None
    summary: str | None = Field(default=None, max_length=20_000)
    highlights: list[str] = Field(default_factory=list, max_length=100)
    skills: list[str] = Field(default_factory=list, max_length=200)


class _AddExperienceSectionInput(_ClosedInput):
    """@brief 新增工作经历模块的窄输入 / Narrow input for adding experience."""

    title: str = Field(min_length=1, max_length=120)
    items: list[_ExperienceDraftInput] = Field(min_length=1, max_length=50)
    after_section_id: str | None = Field(default=None, max_length=160)


class _ProjectDraftInput(_ClosedInput):
    """@brief 由后端补全的项目经历 / Backend-completed project."""

    title: str = Field(min_length=1, max_length=300)
    organization: str | None = Field(default=None, max_length=300)
    subtitle: str | None = Field(default=None, max_length=300)
    date_range: _DateRangeInput | None = None
    summary: str | None = Field(default=None, max_length=20_000)
    highlights: list[str] = Field(default_factory=list, max_length=100)
    skills: list[str] = Field(default_factory=list, max_length=200)
    url: str | None = Field(default=None, max_length=2_000)


class _AddProjectSectionInput(_ClosedInput):
    """@brief 新增项目经历模块的窄输入 / Narrow input for adding projects."""

    title: str = Field(min_length=1, max_length=120)
    items: list[_ProjectDraftInput] = Field(min_length=1, max_length=100)
    after_section_id: str | None = Field(default=None, max_length=160)


class _EducationDraftInput(_ClosedInput):
    """@brief 由后端补全的教育经历 / Backend-completed education item."""

    organization: str = Field(min_length=1, max_length=300)
    title: str | None = Field(default=None, max_length=300)
    subtitle: str | None = Field(default=None, max_length=300)
    location: str | None = Field(default=None, max_length=300)
    date_range: _DateRangeInput | None = None
    highlights: list[str] = Field(default_factory=list, max_length=100)


class _AddEducationSectionInput(_ClosedInput):
    """@brief 新增教育经历模块的窄输入 / Narrow input for adding education."""

    title: str = Field(min_length=1, max_length=120)
    items: list[_EducationDraftInput] = Field(min_length=1, max_length=50)
    after_section_id: str | None = Field(default=None, max_length=160)


class _AddBulletSectionInput(_ClosedInput):
    """@brief 新增简单要点模块的窄输入 / Narrow input for adding a bullet section."""

    title: str = Field(min_length=1, max_length=120)
    section_kind: Literal["awards", "certifications", "custom"]
    bullets: list[str] = Field(min_length=1, max_length=100)
    after_section_id: str | None = Field(default=None, max_length=160)


@dataclass(slots=True)
class ResumeToolSession:
    """One Run-bound Resume tool session with hidden authority and staged writes."""

    context: AgentResumeContext = field(repr=False)
    _drafts: list[AgentResumeOperationDraft] = field(default_factory=list, repr=False)
    _proposal_title: str | None = field(default=None, repr=False)

    @property
    def drafts(self) -> tuple[AgentResumeOperationDraft, ...]:
        """Return the immutable proposal draft accumulated by write tools."""

        return tuple(self._drafts)

    @property
    def proposal_title(self) -> str | None:
        """Return the title after the Agent explicitly requests a human decision."""

        return self._proposal_title

    def read_profile(self) -> str:
        return _json_result(
            {
                "kind": "resume_profile",
                "profile": _PROFILE.dump_python(self.context.document.profile, mode="json"),
            }
        )

    def read_snapshot(self) -> str:
        """@brief 读取一次编辑所需的完整语义快照 / Read one complete semantic editing snapshot.

        @return 不含 Workspace、actor 或 revision 的 JSON / JSON without Workspace, actor, or revision.
        """

        document = _DOCUMENT.dump_python(self.context.document, mode="json")
        return _json_result(
            {
                "kind": "resume_snapshot",
                "resume": {
                    "id": document["meta"]["id"],
                    "title": document["title"],
                    "locale": document["locale"],
                    "profile": document["profile"],
                    "sections": document["sections"],
                    "template": document["template"],
                    "style": document["style"],
                },
            }
        )

    def list_sections(self) -> str:
        return _json_result(
            {
                "kind": "resume_section_list",
                "items": tuple(
                    {
                        "id": section.id,
                        "kind": section.kind.value,
                        "title": section.title,
                        "item_count": len(section.items),
                        "has_content": section.content is not None,
                    }
                    for section in self.context.document.sections
                ),
            }
        )

    def read_section(self, section_id: str) -> str:
        section = next(
            (item for item in self.context.document.sections if item.id == section_id),
            None,
        )
        if section is None:
            return _json_result({"kind": "not_found", "resource": "resume_section"})
        return _json_result(
            {
                "kind": "resume_section",
                "section": _SECTION.dump_python(section, mode="json"),
            }
        )

    def read_item(self, item_id: str) -> str:
        item = next(
            (
                item
                for section in self.context.document.sections
                for item in section.items
                if item.id == item_id
            ),
            None,
        )
        if item is None:
            return _json_result({"kind": "not_found", "resource": "resume_item"})
        return _json_result(
            {
                "kind": "resume_item",
                "item": _ITEM.dump_python(item, mode="json"),
            }
        )

    def stage(self, payload: Mapping[str, JsonValue]) -> str:
        """@brief 原子暂存一个简历操作 / Atomically stage one Resume operation.

        @param payload 不含 operation_id 的领域操作 / Domain operation without operation_id.
        @return 结构化验证结果 / Structured validation result.
        """

        result = self.stage_many((payload,))
        decoded = json.loads(result)
        if decoded.get("kind") == "resume_change_batch_staged":
            staged: dict[str, JsonValue] = {
                "kind": "resume_change_staged",
                "operation_number": decoded["last_operation_number"],
            }
            diagnostics = decoded.get("diagnostics")
            if isinstance(diagnostics, dict):
                staged["diagnostics"] = _json_value(diagnostics)
            return _json_result(staged)
        return result

    def stage_many(self, payloads: Sequence[Mapping[str, JsonValue]]) -> str:
        """@brief 原子暂存一批简历操作 / Atomically stage a batch of Resume operations.

        @param payloads 按顺序执行的领域操作 / Ordered domain operations.
        @return 全批成功或全批失败的结构化结果 / Structured all-or-nothing result.
        """

        if self._proposal_title is not None:
            raise ValueError("Resume proposal was already submitted for decision")
        if not payloads:
            raise ValueError("Resume operation batch cannot be empty")
        working = list(self._drafts)
        changed_operation_numbers: list[int] = []
        inserted_operation_count = 0
        date_normalization_count = 0
        for operation_index, payload in enumerate(payloads):
            try:
                normalized_payload = normalize_resume_operation_wire(payload)
                date_normalization_count += _date_normalization_count(
                    payload,
                    normalized_payload,
                )
                candidate = AgentResumeOperationDraft(normalized_payload)
                slot = _operation_slot(payload)
                existing_index = next(
                    (
                        index
                        for index in range(len(working) - 1, -1, -1)
                        if _operation_slot(working[index].payload) == slot
                    ),
                    None,
                )
                trial = list(working)
                if existing_index is None:
                    if len(trial) >= 200:
                        raise ValueError("Resume proposal cannot exceed 200 operations")
                    trial.append(candidate)
                    changed_number = len(trial)
                else:
                    trial[existing_index] = candidate
                    changed_number = existing_index + 1
                validate_resume_operation_drafts(
                    f"validate:{self.context.resume_ref.id}",
                    self.context,
                    tuple(trial),
                )
            except (TypeError, ValueError) as error:
                code = (
                    error.code
                    if isinstance(error, ResumeDomainError)
                    else "resume.operation_invalid"
                )
                date_rejection_reason = getattr(error, "reason", None)
                return _json_result(
                    {
                        "kind": "invalid_draft",
                        "code": code,
                        "recoverable": True,
                        "issues": (
                            {
                                "operation_index": operation_index,
                                "path": _operation_path(payload),
                                "issue": code,
                            },
                        ),
                        "retry": {
                            "strategy": "correct_operation",
                            "operation_index": operation_index,
                            "suggested_tool": _suggested_tool(payload),
                        },
                        **(
                            {"diagnostics": {"date_rejection_reason": date_rejection_reason}}
                            if isinstance(date_rejection_reason, str)
                            else {}
                        ),
                        "draft_state": self._draft_state(),
                    }
                )
            working = trial
            changed_operation_numbers.append(changed_number)
            inserted_operation_count += existing_index is None
        self._drafts = working
        result: dict[str, JsonValue] = {
            "kind": "resume_change_batch_staged",
            "operation_count": len(payloads),
            "inserted_operation_count": inserted_operation_count,
            "changed_operation_numbers": tuple(changed_operation_numbers),
            "first_operation_number": min(changed_operation_numbers),
            "last_operation_number": max(changed_operation_numbers),
            "draft_state": self._draft_state(),
        }
        if date_normalization_count:
            result["diagnostics"] = {
                "date_normalization_count": date_normalization_count,
            }
        return _json_result(result)

    def _draft_state(self) -> Mapping[str, JsonValue]:
        """@brief 返回不含内容的暂存摘要 / Return a content-free staged-operation summary.

        @return 操作序号和类型组成的有界摘要 / Bounded summary of operation ordinals and types.
        """

        return {
            "operation_count": len(self._drafts),
            "operations": tuple(
                {
                    "number": number,
                    "op": str(draft.payload.get("op", "unknown")),
                }
                for number, draft in enumerate(self._drafts, start=1)
            ),
        }

    def request_decision(self, title: str) -> str:
        if not self._drafts:
            raise ValueError("A Resume proposal requires at least one staged operation")
        if self._proposal_title is not None:
            raise ValueError("Resume proposal was already submitted for decision")
        self._proposal_title = title.strip()
        return _json_result(
            {
                "kind": "proposal_decision_required",
                "operation_count": len(self._drafts),
            }
        )


def resume_agent_tools(session: ResumeToolSession) -> tuple[StructuredTool, ...]:
    """Build the small LangChain instruction set for an authorized Run."""

    def set_document_title(value: str) -> str:
        """@brief 暂存简历文档标题修改 / Stage a Resume document-title change.

        @param value 编辑器与简历列表使用的文档标题 / Document title used by the editor and Resume list.
        @return 结构化草稿验证结果 / Structured draft-validation result.
        @note 服务端绑定简历根身份 / The server binds the Resume root identity.
        """

        return session.stage(
            {
                "op": "set_field",
                "entity_id": str(session.context.document.meta.id),
                "field_path": ("title",),
                "value": value,
            }
        )

    def set_profile_field(field: str, value: JsonValue) -> str:
        return session.stage(
            {
                "op": "set_field",
                "entity_id": str(session.context.document.meta.id),
                "field_path": ("profile", field),
                "value": value,
            }
        )

    def set_contacts(contacts: list[_ContactInput]) -> str:
        """@brief 暂存完整联系方式列表 / Stage the complete contact list.

        @param contacts 符合强类型 schema 的联系方式 / Contacts matching the strong schema.
        @return 结构化草稿验证结果 / Structured draft-validation result.
        """

        return session.stage(
            {
                "op": "set_field",
                "entity_id": str(session.context.document.meta.id),
                "field_path": ("profile", "contacts"),
                "value": _json_value([contact.model_dump(mode="json") for contact in contacts]),
            }
        )

    def set_profile_fields(updates: list[_ProfileFieldUpdateInput]) -> str:
        """@brief 原子暂存多个候选人字段 / Atomically stage candidate profile fields.

        @param updates 最多三个互异的候选人字段 / Up to three distinct profile fields.
        @return 全批成功或失败的结构化结果 / Structured all-or-nothing result.
        """

        fields = [update.field for update in updates]
        if len(fields) != len(set(fields)):
            raise ValueError("Profile field updates must be unique")
        return session.stage_many(
            tuple(
                {
                    "op": "set_field",
                    "entity_id": str(session.context.document.meta.id),
                    "field_path": ("profile", update.field),
                    "value": update.value,
                }
                for update in updates
            )
        )

    def set_field(entity_id: str, field_path: list[str], value: JsonValue) -> str:
        return session.stage(
            {
                "op": "set_field",
                "entity_id": entity_id,
                "field_path": tuple(field_path),
                "value": value,
            }
        )

    def upsert_section(
        section: _ResumeSectionInput,
        after_section_id: str | None = None,
    ) -> str:
        return session.stage(
            {
                "op": "upsert_section",
                "section": _json_value(section.model_dump(mode="json")),
                "after_section_id": after_section_id,
            }
        )

    def upsert_item(
        section_id: str,
        item: _ResumeItemInput,
        after_item_id: str | None = None,
    ) -> str:
        return session.stage(
            {
                "op": "upsert_item",
                "section_id": section_id,
                "item": _json_value(item.model_dump(mode="json")),
                "after_item_id": after_item_id,
            }
        )

    def remove_entity(entity_kind: str, entity_id: str) -> str:
        return session.stage(
            {
                "op": "remove_entity",
                "entity_kind": entity_kind,
                "entity_id": entity_id,
            }
        )

    def move_entity(
        entity_kind: str,
        entity_id: str,
        parent_id: str | None = None,
        after_id: str | None = None,
    ) -> str:
        return session.stage(
            {
                "op": "move_entity",
                "entity_kind": entity_kind,
                "entity_id": entity_id,
                "parent_id": parent_id,
                "after_id": after_id,
            }
        )

    def set_template(
        template: _TemplateRefInput,
        settings: dict[str, JsonValue],
    ) -> str:
        return session.stage(
            {
                "op": "set_template",
                "template": _json_value(template.model_dump(mode="json")),
                "settings": _json_value(settings),
            }
        )

    def set_fields(updates: list[_SetFieldUpdateInput]) -> str:
        """@brief 原子暂存一批字段更新 / Atomically stage field updates.

        @param updates 同构字段更新 / Homogeneous field updates.
        @return 全批成功或失败的结构化结果 / Structured all-or-nothing result.
        """

        return session.stage_many(
            tuple(
                {
                    "op": "set_field",
                    "entity_id": update.entity_id,
                    "field_path": tuple(update.field_path),
                    "value": update.value,
                }
                for update in updates
            )
        )

    def upsert_sections(updates: list[_UpsertSectionUpdateInput]) -> str:
        """@brief 原子暂存一批完整模块 / Atomically stage complete sections.

        @param updates 同构模块写入 / Homogeneous section upserts.
        @return 全批成功或失败的结构化结果 / Structured all-or-nothing result.
        """

        return session.stage_many(
            tuple(
                {
                    "op": "upsert_section",
                    "section": _json_value(update.section.model_dump(mode="json")),
                    "after_section_id": update.after_section_id,
                }
                for update in updates
            )
        )

    def upsert_items(updates: list[_UpsertItemUpdateInput]) -> str:
        """@brief 原子暂存一批完整条目 / Atomically stage complete items.

        @param updates 同构条目写入 / Homogeneous item upserts.
        @return 全批成功或失败的结构化结果 / Structured all-or-nothing result.
        """

        return session.stage_many(
            tuple(
                {
                    "op": "upsert_item",
                    "section_id": update.section_id,
                    "item": _json_value(update.item.model_dump(mode="json")),
                    "after_item_id": update.after_item_id,
                }
                for update in updates
            )
        )

    def add_skill_section(
        title: str,
        groups: list[_SkillGroupDraftInput],
        after_section_id: str | None = None,
    ) -> str:
        """@brief 新增由服务端补全的技能模块 / Add a server-completed skills section.

        @param title 模块标题 / Section title.
        @param groups 技能分组 / Skill groups.
        @param after_section_id 前置模块锚点 / Previous-section anchor.
        @return 结构化草稿结果 / Structured draft result.
        """

        return _stage_generated_section(
            session,
            section_kind="skills",
            title=title,
            after_section_id=after_section_id,
            items=tuple(
                _complete_item(
                    item_id=_generated_entity_id(session, "skill", index),
                    kind="skill_group",
                    title=group.name,
                    skills=group.skills,
                )
                for index, group in enumerate(groups, start=1)
            ),
        )

    def add_experience_section(
        title: str,
        items: list[_ExperienceDraftInput],
        after_section_id: str | None = None,
    ) -> str:
        """@brief 新增由服务端补全的工作经历 / Add server-completed experience.

        @param title 模块标题 / Section title.
        @param items 工作经历 / Experience items.
        @param after_section_id 前置模块锚点 / Previous-section anchor.
        @return 结构化草稿结果 / Structured draft result.
        """

        return _stage_generated_section(
            session,
            section_kind="experience",
            title=title,
            after_section_id=after_section_id,
            items=tuple(
                _complete_item(
                    item_id=_generated_entity_id(session, "experience", index),
                    kind="experience",
                    title=item.title,
                    organization=item.organization,
                    location=item.location,
                    start=None if item.date_range is None else item.date_range.start,
                    end=None if item.date_range is None else item.date_range.end,
                    summary=item.summary,
                    highlights=item.highlights,
                    skills=item.skills,
                )
                for index, item in enumerate(items, start=1)
            ),
        )

    def add_project_section(
        title: str,
        items: list[_ProjectDraftInput],
        after_section_id: str | None = None,
    ) -> str:
        """@brief 新增由服务端补全的项目经历 / Add server-completed projects.

        @param title 模块标题 / Section title.
        @param items 项目经历 / Project items.
        @param after_section_id 前置模块锚点 / Previous-section anchor.
        @return 结构化草稿结果 / Structured draft result.
        """

        return _stage_generated_section(
            session,
            section_kind="projects",
            title=title,
            after_section_id=after_section_id,
            items=tuple(
                _complete_item(
                    item_id=_generated_entity_id(session, "project", index),
                    kind="project",
                    title=item.title,
                    subtitle=item.subtitle,
                    organization=item.organization,
                    start=None if item.date_range is None else item.date_range.start,
                    end=None if item.date_range is None else item.date_range.end,
                    summary=item.summary,
                    highlights=item.highlights,
                    skills=item.skills,
                    url=item.url,
                )
                for index, item in enumerate(items, start=1)
            ),
        )

    def add_education_section(
        title: str,
        items: list[_EducationDraftInput],
        after_section_id: str | None = None,
    ) -> str:
        """@brief 新增由服务端补全的教育经历 / Add server-completed education.

        @param title 模块标题 / Section title.
        @param items 教育经历 / Education items.
        @param after_section_id 前置模块锚点 / Previous-section anchor.
        @return 结构化草稿结果 / Structured draft result.
        """

        return _stage_generated_section(
            session,
            section_kind="education",
            title=title,
            after_section_id=after_section_id,
            items=tuple(
                _complete_item(
                    item_id=_generated_entity_id(session, "education", index),
                    kind="education",
                    title=item.title,
                    subtitle=item.subtitle,
                    organization=item.organization,
                    location=item.location,
                    start=None if item.date_range is None else item.date_range.start,
                    end=None if item.date_range is None else item.date_range.end,
                    highlights=item.highlights,
                )
                for index, item in enumerate(items, start=1)
            ),
        )

    def add_bullet_section(
        title: str,
        section_kind: Literal["awards", "certifications", "custom"],
        bullets: list[str],
        after_section_id: str | None = None,
    ) -> str:
        """@brief 新增由服务端补全的要点模块 / Add a server-completed bullet section.

        @param title 模块标题 / Section title.
        @param section_kind 模块类型 / Section kind.
        @param bullets 简短要点 / Concise bullets.
        @param after_section_id 前置模块锚点 / Previous-section anchor.
        @return 结构化草稿结果 / Structured draft result.
        """

        item_kind = {
            "awards": "award",
            "certifications": "certification",
            "custom": "custom",
        }[section_kind]
        return _stage_generated_section(
            session,
            section_kind=section_kind,
            title=title,
            after_section_id=after_section_id,
            items=tuple(
                _complete_item(
                    item_id=_generated_entity_id(session, item_kind, index),
                    kind=item_kind,
                    title=bullet,
                )
                for index, bullet in enumerate(bullets, start=1)
            ),
        )

    return (
        StructuredTool.from_function(
            func=session.read_snapshot,
            name="resume_read_snapshot",
            description=(
                "Read the complete current Resume semantic snapshot for a broad or multi-part "
                "edit. Server authority remains hidden."
            ),
            args_schema=_NoInput,
        ),
        StructuredTool.from_function(
            func=session.read_profile,
            name="resume_read_profile",
            description="Read only the candidate profile and contact information.",
            args_schema=_NoInput,
        ),
        StructuredTool.from_function(
            func=session.list_sections,
            name="resume_list_sections",
            description="List Resume sections with small summaries; use before choosing a section.",
            args_schema=_NoInput,
        ),
        StructuredTool.from_function(
            func=session.read_section,
            name="resume_read_section",
            description="Read one Resume section and its items by semantic section ID.",
            args_schema=_SectionInput,
        ),
        StructuredTool.from_function(
            func=session.read_item,
            name="resume_read_item",
            description="Read one Resume item by semantic item ID.",
            args_schema=_ItemInput,
        ),
        StructuredTool.from_function(
            func=set_document_title,
            name="resume_draft_set_document_title",
            description=(
                "Stage the Resume document title shown in the editor and Resume list. "
                "This is not the candidate full name, professional headline, section title, "
                "or item title. The server binds the Resume root identity."
            ),
            args_schema=_SetDocumentTitleInput,
        ),
        StructuredTool.from_function(
            func=set_profile_field,
            name="resume_draft_set_profile_field",
            description=(
                "Stage full_name, headline, or summary replacement for user review; "
                "the Resume root identity is bound by the server and does not write."
            ),
            args_schema=_SetProfileFieldInput,
        ),
        StructuredTool.from_function(
            func=set_profile_fields,
            name="resume_draft_set_profile_fields",
            description=(
                "Atomically stage two or three distinct full_name, headline, or summary "
                "replacements. The Resume root identity is bound by the server."
            ),
            args_schema=_SetProfileFieldsInput,
        ),
        StructuredTool.from_function(
            func=set_contacts,
            name="resume_draft_set_contacts",
            description=(
                "Stage one complete, strongly typed contact-list replacement for user review. "
                "Read the current profile first and preserve contacts the user did not replace."
            ),
            args_schema=_SetContactsInput,
        ),
        StructuredTool.from_function(
            func=set_field,
            name="resume_draft_set_field",
            description="Stage one semantic field replacement for user review; does not write.",
            args_schema=_SetFieldInput,
        ),
        StructuredTool.from_function(
            func=upsert_section,
            name="resume_draft_upsert_section",
            description="Stage insertion or replacement of one complete section; does not write.",
            args_schema=_UpsertSectionInput,
        ),
        StructuredTool.from_function(
            func=upsert_item,
            name="resume_draft_upsert_item",
            description="Stage insertion or replacement of one complete item; does not write.",
            args_schema=_UpsertItemInput,
        ),
        StructuredTool.from_function(
            func=remove_entity,
            name="resume_draft_remove_entity",
            description="Stage removal of one section or item; does not write.",
            args_schema=_RemoveEntityInput,
        ),
        StructuredTool.from_function(
            func=move_entity,
            name="resume_draft_move_entity",
            description="Stage one section or item move using semantic anchors; does not write.",
            args_schema=_MoveEntityInput,
        ),
        StructuredTool.from_function(
            func=set_template,
            name="resume_draft_set_template",
            description="Stage one atomic Template and settings replacement; does not write.",
            args_schema=_SetTemplateInput,
        ),
        StructuredTool.from_function(
            func=set_fields,
            name="resume_draft_set_fields",
            description=(
                "Atomically stage multiple field replacements with one simple homogeneous schema. "
                "Use only when every requested change is a set-field operation."
            ),
            args_schema=_SetFieldsInput,
        ),
        StructuredTool.from_function(
            func=upsert_sections,
            name="resume_draft_upsert_sections",
            description=(
                "Atomically stage insertion or replacement of multiple complete Resume sections. "
                "Use only for complete sections; every section must include all required fields."
            ),
            args_schema=_UpsertSectionsInput,
        ),
        StructuredTool.from_function(
            func=upsert_items,
            name="resume_draft_upsert_items",
            description=(
                "Atomically stage insertion or replacement of multiple complete Resume items. "
                "Use only for complete items within explicitly identified sections."
            ),
            args_schema=_UpsertItemsInput,
        ),
        StructuredTool.from_function(
            func=add_skill_section,
            name="resume_draft_add_skill_section",
            description=(
                "Add a new skills section from simple named groups. The server generates IDs "
                "and all complete Resume fields. Fails instead of replacing an existing skills "
                "section."
            ),
            args_schema=_AddSkillSectionInput,
        ),
        StructuredTool.from_function(
            func=add_experience_section,
            name="resume_draft_add_experience_section",
            description=(
                "Add a new experience section from simple work records. The server generates "
                "IDs, rich-text wrappers, defaults, and complete Resume fields. Fails instead "
                "of replacing an existing experience section."
            ),
            args_schema=_AddExperienceSectionInput,
        ),
        StructuredTool.from_function(
            func=add_project_section,
            name="resume_draft_add_project_section",
            description=(
                "Add a new projects section from simple project records. The server generates "
                "IDs, rich-text wrappers, defaults, and complete Resume fields. Fails instead "
                "of replacing an existing projects section."
            ),
            args_schema=_AddProjectSectionInput,
        ),
        StructuredTool.from_function(
            func=add_education_section,
            name="resume_draft_add_education_section",
            description=(
                "Add a new education section from simple education records. The server "
                "generates IDs, rich-text wrappers, defaults, and complete Resume fields. "
                "Fails instead of replacing an existing education section."
            ),
            args_schema=_AddEducationSectionInput,
        ),
        StructuredTool.from_function(
            func=add_bullet_section,
            name="resume_draft_add_bullet_section",
            description=(
                "Add a new awards, certifications, or custom bullet section. The server "
                "generates complete items. Use custom for self-evaluation or other concise "
                "bullet modules. Fails instead of replacing a section with the same identity."
            ),
            args_schema=_AddBulletSectionInput,
        ),
        StructuredTool.from_function(
            func=session.request_decision,
            name="resume_request_proposal_decision",
            description=(
                "Finish the staged Resume change set and request the user's ProposalDecision. "
                "Call only after all desired draft operations have been staged."
            ),
            args_schema=_RequestDecisionInput,
        ),
    )


def _stage_generated_section(
    session: ResumeToolSession,
    *,
    section_kind: str,
    title: str,
    after_section_id: str | None,
    items: Sequence[Mapping[str, JsonValue]],
) -> str:
    """@brief 暂存一个由服务端补全的新模块 / Stage one server-completed new section.

    @param session 当前工具会话 / Current tool session.
    @param section_kind 规范模块类型 / Canonical section kind.
    @param title 模块标题 / Section title.
    @param after_section_id 前置模块锚点 / Previous-section anchor.
    @param items 已补全条目 / Completed items.
    @return 结构化草稿结果 / Structured draft result.
    @note 已存在相同语义模块时拒绝覆盖 / Refuses to replace an existing semantic section.
    """

    existing_id = _existing_section_id(session, section_kind, title)
    if existing_id is not None:
        return _json_result(
            {
                "kind": "invalid_draft",
                "code": "resume.section_already_exists",
                "recoverable": True,
                "issues": (
                    {
                        "path": "section",
                        "issue": "existing_section_requires_explicit_merge",
                    },
                ),
                "existing_section_id": existing_id,
                "retry": {
                    "strategy": "read_then_merge",
                    "suggested_tool": "resume_read_section",
                },
                "draft_state": session._draft_state(),
            }
        )
    section_id = _generated_entity_id(session, f"section_{section_kind}", 1)
    result = session.stage(
        {
            "op": "upsert_section",
            "section": {
                "id": section_id,
                "kind": section_kind,
                "title": title,
                "visible": True,
                "content": None,
                "items": tuple(items),
            },
            "after_section_id": after_section_id,
        }
    )
    decoded = json.loads(result)
    if isinstance(decoded, dict) and decoded.get("kind") == "resume_change_staged":
        decoded["entity"] = {
            "resource_type": "resume_section",
            "id": section_id,
            "kind": section_kind,
        }
        return _json_result(decoded)
    return result


def _complete_item(
    *,
    item_id: str,
    kind: str,
    title: str | None = None,
    subtitle: str | None = None,
    organization: str | None = None,
    location: str | None = None,
    start: str | None = None,
    end: str | None = None,
    summary: str | None = None,
    highlights: Sequence[str] = (),
    skills: Sequence[str] = (),
    url: str | None = None,
) -> Mapping[str, JsonValue]:
    """@brief 补全一个规范简历条目 / Complete one canonical Resume item.

    @param item_id 服务端生成的条目标识 / Server-generated item ID.
    @param kind 规范条目类型 / Canonical item kind.
    @param title 主标题 / Primary title.
    @param subtitle 副标题 / Subtitle.
    @param organization 组织名称 / Organization name.
    @param location 地点 / Location.
    @param start 开始日期 / Start date.
    @param end 结束日期 / End date.
    @param summary 摘要 / Summary.
    @param highlights 亮点列表 / Highlight list.
    @param skills 技能列表 / Skill list.
    @param url 可选链接 / Optional URL.
    @return 不含缺失字段的完整条目 / Complete item with no missing fields.
    """

    return {
        "id": item_id,
        "kind": kind,
        "title": title,
        "subtitle": subtitle,
        "organization": organization,
        "location": location,
        "date_range": (
            None
            if start is None and end is None
            else {
                "start": start,
                "end": end,
            }
        ),
        "summary": None if summary is None else _rich_text(summary),
        "highlights": tuple(_rich_text(text) for text in highlights),
        "skills": tuple(skills),
        "tags": (),
        "visible": True,
        "url": url,
    }


def _rich_text(text: str) -> Mapping[str, JsonValue]:
    """@brief 将纯文本包装成规范富文本 / Wrap plain text as canonical rich text.

    @param text 用户可见文本 / User-visible text.
    @return 无标记富文本 / Rich text without marks.
    """

    return {"text": text, "marks": ()}


def _existing_section_id(
    session: ResumeToolSession,
    section_kind: str,
    title: str,
) -> str | None:
    """@brief 查找已有或已暂存的同语义模块 / Find an existing or staged semantic section.

    @param session 当前工具会话 / Current tool session.
    @param section_kind 规范模块类型 / Canonical section kind.
    @param title 模块标题 / Section title.
    @return 已有模块 ID；不存在则为空 / Existing section ID, or null.
    """

    for section in session.context.document.sections:
        if section.kind.value == section_kind and (
            section_kind != "custom" or section.title.casefold() == title.casefold()
        ):
            return section.id
    for draft in session.drafts:
        draft_section = draft.payload.get("section")
        if not isinstance(draft_section, Mapping):
            continue
        draft_kind = draft_section.get("kind")
        draft_title = draft_section.get("title")
        if draft_kind == section_kind and (
            section_kind != "custom"
            or (isinstance(draft_title, str) and draft_title.casefold() == title.casefold())
        ):
            section_id = draft_section.get("id")
            return section_id if isinstance(section_id, str) else None
    return None


def _generated_entity_id(
    session: ResumeToolSession,
    stem: str,
    ordinal: int,
) -> str:
    """@brief 生成不与当前文档冲突的稳定临时 ID / Generate a stable non-conflicting temporary ID.

    @param session 当前工具会话 / Current tool session.
    @param stem 语义 ID 主干 / Semantic ID stem.
    @param ordinal 同类实体序号 / Same-kind entity ordinal.
    @return 可用于 Proposal 的临时实体 ID / Temporary entity ID suitable for a Proposal.
    """

    base = f"tmp_{stem}_{ordinal:02d}"
    occupied = {
        entity_id
        for section in session.context.document.sections
        for entity_id in (section.id, *(item.id for item in section.items))
    }
    if base not in occupied:
        return base
    suffix = 2
    while f"{base}_{suffix}" in occupied:
        suffix += 1
    return f"{base}_{suffix}"


def tool_catalog(tools: Sequence[StructuredTool]) -> tuple[dict[str, JsonValue], ...]:
    """Project LangChain tools into the provider-neutral catalog shown to the model."""

    return tuple(
        {
            "name": tool.name,
            "description": tool.description,
            "input_schema": _tool_input_schema(tool),
        }
        for tool in tools
    )


def _tool_input_schema(tool: StructuredTool) -> JsonValue:
    schema = tool.tool_call_schema
    if isinstance(schema, dict):
        return _json_value(schema)
    if hasattr(schema, "model_json_schema"):
        return _json_value(schema.model_json_schema())
    return _json_value(schema.schema())


def _json_result(value: Mapping[str, JsonValue]) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _json_value(value: object) -> JsonValue:
    return _JSON_VALUE.validate_json(
        json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    )


def _operation_path(payload: Mapping[str, JsonValue]) -> str:
    """@brief 提取不含用户内容的失败字段路径 / Extract a content-free failure path.

    @param payload 失败领域操作 / Failed domain operation.
    @return 仅含结构键的字段路径 / Field path containing structural keys only.
    """

    raw_path = payload.get("field_path")
    if isinstance(raw_path, (list, tuple)) and all(
        isinstance(item, str) and item for item in raw_path
    ):
        return ".".join(str(item) for item in raw_path[:16])
    operation = payload.get("op")
    return str(operation) if isinstance(operation, str) else "operation"


def _suggested_tool(payload: Mapping[str, JsonValue]) -> str:
    """@brief 为失败操作选择窄工具 / Select a narrow tool for a failed operation.

    @param payload 失败领域操作 / Failed domain operation.
    @return 可供模型恢复的工具名 / Tool name suitable for model recovery.
    """

    operation = payload.get("op")
    return {
        "set_field": "resume_draft_set_field",
        "upsert_section": "resume_draft_upsert_section",
        "upsert_item": "resume_draft_upsert_item",
        "remove_entity": "resume_draft_remove_entity",
        "move_entity": "resume_draft_move_entity",
        "set_template": "resume_draft_set_template",
    }.get(str(operation), "resume_read_snapshot")


def _operation_slot(payload: Mapping[str, JsonValue]) -> tuple[str, ...]:
    """@brief 为可纠正草稿计算语义槽位 / Compute a semantic slot for a correctable draft.

    @param payload 不含用户内容的领域操作结构 / Domain operation structure excluding values.
    @return 用于原位替换的稳定槽位 / Stable slot used for in-place replacement.
    """

    operation = str(payload.get("op", "unknown"))
    if operation == "set_field":
        raw_path = payload.get("field_path")
        path = tuple(str(item) for item in raw_path) if isinstance(raw_path, (list, tuple)) else ()
        return (operation, str(payload.get("entity_id", "")), *path)
    if operation == "upsert_section":
        section = payload.get("section")
        section_id = section.get("id") if isinstance(section, Mapping) else ""
        return (operation, str(section_id))
    if operation == "upsert_item":
        item = payload.get("item")
        item_id = item.get("id") if isinstance(item, Mapping) else ""
        return (
            operation,
            str(payload.get("section_id", "")),
            str(item_id),
        )
    if operation in {"remove_entity", "move_entity"}:
        return (
            operation,
            str(payload.get("entity_kind", "")),
            str(payload.get("entity_id", "")),
        )
    if operation == "set_template":
        return (operation,)
    return (operation, str(len(payload)))


def _date_normalization_count(
    before: Mapping[str, JsonValue],
    after: Mapping[str, JsonValue],
) -> int:
    """@brief 统计发生规范化的结构化日期 / Count normalized structured date ranges.

    @param before 模型提供的原始 operation / Original model-provided operation.
    @param after 后端规范化后的 operation / Backend-normalized operation.
    @return 日期范围发生变化的 item 数 / Number of items whose date range changed.
    """

    before_ranges = _operation_date_ranges(before)
    after_ranges = _operation_date_ranges(after)
    return sum(left != right for left, right in zip(before_ranges, after_ranges, strict=True))


def _operation_date_ranges(
    payload: Mapping[str, JsonValue],
) -> tuple[JsonValue, ...]:
    """@brief 提取 operation 中的日期范围 / Extract structured date ranges from an operation.

    @param payload Resume operation JSON / Resume operation JSON.
    @return 按 item 顺序排列的日期值 / Date values ordered by item.
    """

    operation = payload.get("op")
    if operation == "upsert_item":
        item = payload.get("item")
        return (item.get("date_range"),) if isinstance(item, Mapping) else ()
    if operation != "upsert_section":
        return ()
    section = payload.get("section")
    items = section.get("items") if isinstance(section, Mapping) else None
    if not isinstance(items, (list, tuple)):
        return ()
    return tuple(item.get("date_range") for item in items if isinstance(item, Mapping))


__all__ = ["ResumeToolSession", "resume_agent_tools", "tool_catalog"]
