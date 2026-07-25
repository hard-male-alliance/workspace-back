"""Tests for immutable built-in Resume template manifests."""

from __future__ import annotations

import pytest

from backend.domain.resumes import TemplateRef
from backend.domain.templates import get_template_manifest, list_template_manifests
from backend.infrastructure.resumes import BuiltinResumeTemplateCatalog


@pytest.mark.asyncio
async def test_ats_template_is_public_and_projects_to_a_domain_policy() -> None:
    manifests = list_template_manifests("en-US")
    ats = next(item for item in manifests if item["id"] == "tpl_ats_v1")

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
