"""Tests for immutable built-in Resume template manifests."""

from __future__ import annotations

import pytest

from backend.domain.resumes import TemplateRef
from backend.domain.templates import get_template_manifest, list_template_manifests
from backend.infrastructure.resumes import BuiltinResumeTemplateCatalog


@pytest.mark.asyncio
async def test_legacy_ats_template_remains_addressable_and_projects_to_a_domain_policy() -> None:
    ats = get_template_manifest("tpl_ats_v1", "1.0")
    assert ats is not None

    assert ats["capabilities"]["max_columns"] == 1
    assert ats["capabilities"]["supports_photo"] is False
    assert ats["extensions"]["ats_friendly"] is True
    assert get_template_manifest("tpl_ats_v1", "1.0") == ats

    policy = await BuiltinResumeTemplateCatalog().get_policy(
        TemplateRef("tpl_ats_v1", "1.0")
    )
    assert policy is not None
    assert policy.ref == TemplateRef("tpl_ats_v1", "1.0")
    assert policy.default_style().page.size.value == "A4"
    assert "tpl_ats_v1" not in {
        item["id"] for item in list_template_manifests("en-US")
    }


def test_two_new_professional_templates_are_immutable_and_visually_distinct() -> None:
    manifests = list_template_manifests("zh-CN")
    assert {item["id"] for item in manifests} == {
        "tpl_ats_professional_v1",
        "tpl_modern_professional_v1",
    }
    ats = next(item for item in manifests if item["id"] == "tpl_ats_professional_v1")
    modern = next(item for item in manifests if item["id"] == "tpl_modern_professional_v1")

    assert ats["template_version"] == modern["template_version"] == "1.0"
    assert ats["extensions"]["renderer_layout"] == "ats_professional"
    assert modern["extensions"]["renderer_layout"] == "modern_professional"
    assert ats["preview_asset_url"] is None
    assert modern["preview_asset_url"] is None
    assert get_template_manifest(ats["id"], "1.0") == ats
    assert get_template_manifest(modern["id"], "1.0") == modern


def test_legacy_default_template_remains_addressable_but_is_not_selectable() -> None:
    assert get_template_manifest("tpl_default_v1", "1.0") is not None
    assert "tpl_default_v1" not in {
        item["id"] for item in list_template_manifests("zh-CN")
    }
