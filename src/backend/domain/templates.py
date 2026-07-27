"""Public, renderer-independent Resume template catalog."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

_SUPPORTED_SECTION_KINDS = [
    "summary",
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

_BUILTIN_TEMPLATE_MANIFESTS: tuple[dict[str, Any], ...] = (
    {
        "id": "tpl_default_v1",
        "created_at": "2026-07-19T00:00:00Z",
        "updated_at": "2026-07-19T00:00:00Z",
        "revision": 1,
        "template_version": "1.0",
        "name": "AIWS Classic",
        "description": "A stable single-column Resume template for the v0.1 integration flow.",
        "preview_asset_url": None,
        "supported_locales": ["zh-CN", "zh-SG", "en-US"],
        "supported_page_sizes": ["A4", "LETTER"],
        "supported_output_formats": ["pdf"],
        "supported_section_kinds": _SUPPORTED_SECTION_KINDS,
        "zones": [
            {
                "zone_id": "main",
                "label_key": "template.zone.main",
                "accepted_section_kinds": _SUPPORTED_SECTION_KINDS,
                "max_sections": 100,
            }
        ],
        "font_family_tokens": ["body.default"],
        "date_format_tokens": ["yyyy_mm"],
        "bullet_style_tokens": ["bullet.default"],
        "settings": [],
        "capabilities": {
            "supports_photo": False,
            "supports_sidebar": False,
            "supports_custom_sections": True,
            "supports_source_map": True,
            "max_columns": 1,
        },
        "extensions": {},
    },
    {
        "id": "tpl_ats_v1",
        "created_at": "2026-07-24T00:00:00Z",
        "updated_at": "2026-07-24T00:00:00Z",
        "revision": 1,
        "template_version": "1.0",
        "name": "ATS Single Column",
        "description": "A restrained single-column template optimized for text extraction and applicant tracking systems.",
        "preview_asset_url": None,
        "supported_locales": ["zh-CN", "zh-SG", "en-US"],
        "supported_page_sizes": ["A4", "LETTER"],
        "supported_output_formats": ["pdf"],
        "supported_section_kinds": _SUPPORTED_SECTION_KINDS,
        "zones": [
            {
                "zone_id": "main",
                "label_key": "template.zone.main",
                "accepted_section_kinds": _SUPPORTED_SECTION_KINDS,
                "max_sections": 100,
            }
        ],
        "font_family_tokens": ["body.default"],
        "date_format_tokens": ["yyyy_mm"],
        "bullet_style_tokens": ["bullet.default"],
        "settings": [],
        "capabilities": {
            "supports_photo": False,
            "supports_sidebar": False,
            "supports_custom_sections": True,
            "supports_source_map": True,
            "max_columns": 1,
        },
        "extensions": {"ats_friendly": True},
    },
    {
        "id": "tpl_ats_professional_v1",
        "created_at": "2026-07-27T00:00:00Z",
        "updated_at": "2026-07-27T00:00:00Z",
        "revision": 1,
        "template_version": "1.0",
        "name": "ATS 极简单栏",
        "description": "黑白、紧凑、文本提取顺序稳定的正式 ATS 单栏模板。",
        "preview_asset_url": None,
        "supported_locales": ["zh-CN", "zh-SG", "en-US"],
        "supported_page_sizes": ["A4", "LETTER"],
        "supported_output_formats": ["pdf"],
        "supported_section_kinds": _SUPPORTED_SECTION_KINDS,
        "zones": [
            {
                "zone_id": "main",
                "label_key": "template.zone.main",
                "accepted_section_kinds": _SUPPORTED_SECTION_KINDS,
                "max_sections": 100,
            }
        ],
        "font_family_tokens": ["body.default"],
        "date_format_tokens": ["yyyy_mm"],
        "bullet_style_tokens": ["bullet.default"],
        "settings": [],
        "capabilities": {
            "supports_photo": False,
            "supports_sidebar": False,
            "supports_custom_sections": True,
            "supports_source_map": True,
            "max_columns": 1,
        },
        "extensions": {"ats_friendly": True, "renderer_layout": "ats_professional"},
    },
    {
        "id": "tpl_modern_professional_v1",
        "created_at": "2026-07-27T00:00:00Z",
        "updated_at": "2026-07-27T00:00:00Z",
        "revision": 1,
        "template_version": "1.0",
        "name": "现代专业单栏",
        "description": "使用轻量强调色、清晰标题层级和现代间距的正式单栏模板。",
        "preview_asset_url": None,
        "supported_locales": ["zh-CN", "zh-SG", "en-US"],
        "supported_page_sizes": ["A4", "LETTER"],
        "supported_output_formats": ["pdf"],
        "supported_section_kinds": _SUPPORTED_SECTION_KINDS,
        "zones": [
            {
                "zone_id": "main",
                "label_key": "template.zone.main",
                "accepted_section_kinds": _SUPPORTED_SECTION_KINDS,
                "max_sections": 100,
            }
        ],
        "font_family_tokens": ["body.default"],
        "date_format_tokens": ["yyyy_mm"],
        "bullet_style_tokens": ["bullet.default"],
        "settings": [],
        "capabilities": {
            "supports_photo": False,
            "supports_sidebar": False,
            "supports_custom_sections": True,
            "supports_source_map": True,
            "max_columns": 1,
        },
        "extensions": {"renderer_layout": "modern_professional"},
    },
)

_PUBLIC_TEMPLATE_IDS = frozenset(
    {
        "tpl_ats_professional_v1",
        "tpl_modern_professional_v1",
    }
)
"""Templates offered for new Resume creation; legacy IDs remain directly readable."""


def list_template_manifests(locale: str | None = None) -> list[dict[str, Any]]:
    """Return selectable immutable manifests, optionally filtered by content locale."""
    return [
        deepcopy(manifest)
        for manifest in _BUILTIN_TEMPLATE_MANIFESTS
        if manifest["id"] in _PUBLIC_TEMPLATE_IDS
        and (locale is None or locale in manifest["supported_locales"])
    ]


def get_template_manifest(template_id: str, template_version: str) -> dict[str, Any] | None:
    """Read one immutable public manifest without exposing renderer bindings."""
    for manifest in _BUILTIN_TEMPLATE_MANIFESTS:
        if manifest["id"] == template_id and manifest["template_version"] == template_version:
            return deepcopy(manifest)
    return None
