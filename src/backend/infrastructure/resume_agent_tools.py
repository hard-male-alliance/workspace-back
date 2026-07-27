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
from typing import Any

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from backend.domain.agent_v2 import AgentResumeContext, AgentResumeOperationDraft
from backend.domain.platform import JsonValue
from backend.domain.resumes import ResumeDomainError, ResumeItem, ResumeProfile, ResumeSection
from backend.infrastructure.agent_resume_proposals import validate_resume_operation_drafts

_PROFILE = TypeAdapter(ResumeProfile)
_SECTION = TypeAdapter(ResumeSection)
_ITEM = TypeAdapter(ResumeItem)
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
    field: str = Field(pattern=r"^(full_name|headline|summary|contacts)$")
    value: JsonValue


class _UpsertSectionInput(_ClosedInput):
    section: dict[str, Any]
    after_section_id: str | None = Field(default=None, max_length=160)


class _UpsertItemInput(_ClosedInput):
    section_id: str = Field(min_length=1, max_length=160)
    item: dict[str, Any]
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
    template: dict[str, Any]
    settings: dict[str, JsonValue]


class _RequestDecisionInput(_ClosedInput):
    title: str = Field(min_length=1, max_length=300)


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
        if self._proposal_title is not None:
            raise ValueError("Resume proposal was already submitted for decision")
        if len(self._drafts) >= 200:
            raise ValueError("Resume proposal cannot exceed 200 operations")
        candidate = AgentResumeOperationDraft(payload)
        try:
            validate_resume_operation_drafts(
                f"validate:{self.context.resume_ref.id}",
                self.context,
                (*self._drafts, candidate),
            )
        except (TypeError, ValueError) as error:
            code = (
                error.code
                if isinstance(error, ResumeDomainError)
                else "resume.operation_invalid"
            )
            return _json_result(
                {
                    "kind": "invalid_draft",
                    "code": code,
                    "recoverable": True,
                }
            )
        self._drafts.append(candidate)
        return _json_result(
            {
                "kind": "resume_change_staged",
                "operation_number": len(self._drafts),
            }
        )

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

    def set_profile_field(field: str, value: JsonValue) -> str:
        return session.stage(
            {
                "op": "set_field",
                "entity_id": str(session.context.document.meta.id),
                "field_path": ("profile", field),
                "value": value,
            }
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

    def upsert_section(section: dict[str, Any], after_section_id: str | None = None) -> str:
        return session.stage(
            {
                "op": "upsert_section",
                "section": _json_value(section),
                "after_section_id": after_section_id,
            }
        )

    def upsert_item(
        section_id: str,
        item: dict[str, Any],
        after_item_id: str | None = None,
    ) -> str:
        return session.stage(
            {
                "op": "upsert_item",
                "section_id": section_id,
                "item": _json_value(item),
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

    def set_template(template: dict[str, Any], settings: dict[str, JsonValue]) -> str:
        return session.stage(
            {
                "op": "set_template",
                "template": _json_value(template),
                "settings": _json_value(settings),
            }
        )

    return (
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
            func=set_profile_field,
            name="resume_draft_set_profile_field",
            description=(
                "Stage one candidate profile field replacement for user review; "
                "the Resume root identity is bound by the server and does not write."
            ),
            args_schema=_SetProfileFieldInput,
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
            func=session.request_decision,
            name="resume_request_proposal_decision",
            description=(
                "Finish the staged Resume change set and request the user's ProposalDecision. "
                "Call only after all desired draft operations have been staged."
            ),
            args_schema=_RequestDecisionInput,
        ),
    )


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


__all__ = ["ResumeToolSession", "resume_agent_tools", "tool_catalog"]
