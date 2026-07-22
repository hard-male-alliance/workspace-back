"""Published API v2 routes that are safe to expose during the parallel migration."""

from __future__ import annotations

import base64
import json
from typing import Annotated, Any

from fastapi import APIRouter, Query, Request

from backend.api.constants import (
    PROTECTED_RESOURCE_METADATA_URL,
    PUBLIC_ORIGIN,
    TEST_RESOURCE_SERVER_ORIGIN,
)
from backend.composition import BackendContainer
from backend.domain.common import DomainError, Problem
from backend.domain.templates import get_template_manifest, list_template_manifests

router_v2 = APIRouter(prefix="/api/v2")

_DEFAULT_PAGE_LIMIT = 50
_MAX_PAGE_LIMIT = 200
PageLimit = Annotated[int, Query(ge=1, le=_MAX_PAGE_LIMIT)]


def is_public_v2_path(path: str) -> bool:
    """Return whether a v2 path is one of the explicitly public immutable resources."""

    prefix = "/api/v2/resume-templates"
    return path == prefix or path.startswith(f"{prefix}/")


def _container(request: Request) -> BackendContainer:
    container = getattr(request.app.state, "container", None)
    if not isinstance(container, BackendContainer):
        raise RuntimeError("backend container is unavailable")
    return container


def _v2_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Project the legacy internal catalog into the immutable v2 public representation."""

    return {
        "id": manifest["id"],
        "version": manifest["template_version"],
        "name": manifest["name"],
        "description": manifest["description"],
        "preview_url": manifest["preview_asset_url"],
        "supported_locales": manifest["supported_locales"],
        "supported_page_sizes": manifest["supported_page_sizes"],
        "supported_output_formats": manifest["supported_output_formats"],
        "supported_section_kinds": manifest["supported_section_kinds"],
        "zones": [
            {
                "id": zone["zone_id"],
                "label_key": zone["label_key"],
                "accepted_section_kinds": zone["accepted_section_kinds"],
                "max_sections": zone["max_sections"],
            }
            for zone in manifest["zones"]
        ],
        "font_family_tokens": manifest["font_family_tokens"],
        "date_format_tokens": manifest["date_format_tokens"],
        "bullet_style_tokens": manifest["bullet_style_tokens"],
        "capabilities": manifest["capabilities"],
        "settings": manifest["settings"],
        "published_at": manifest["created_at"],
    }


def _encode_cursor(index: int) -> str:
    serialized = json.dumps({"v": 2, "offset": index}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(serialized).decode().rstrip("=")


def _decode_cursor(cursor: str | None) -> int:
    if cursor is None:
        return 0
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.b64decode(padded, altchars=b"-_", validate=True))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DomainError(
            Problem("http.cursor_invalid", 400, "Pagination cursor is invalid")
        ) from error
    if not isinstance(payload, dict) or payload.get("v") != 2:
        raise DomainError(Problem("http.cursor_invalid", 400, "Pagination cursor is invalid"))
    offset = payload.get("offset")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise DomainError(Problem("http.cursor_invalid", 400, "Pagination cursor is invalid"))
    return offset


@router_v2.get(
    "/resume-templates",
    openapi_extra={"x-contract-response": "TemplateList", "x-api-v2-phase": 0},
)
async def list_resume_templates_v2(
    request: Request,
    locale: Annotated[str | None, Query(min_length=2, max_length=32)] = None,
    cursor: str | None = None,
    limit: PageLimit = _DEFAULT_PAGE_LIMIT,
) -> dict[str, Any]:
    """List public immutable templates using the v2 collection and pagination shape."""

    items = [_v2_manifest(item) for item in list_template_manifests(locale)]
    offset = _decode_cursor(cursor)
    selected = items[offset : offset + limit]
    next_offset = offset + len(selected)
    has_more = next_offset < len(items)
    payload = {
        "items": selected,
        "page": {
            "next_cursor": _encode_cursor(next_offset) if has_more else None,
            "has_more": has_more,
        },
    }
    _container(request).contracts_v2.validate_definition("TemplateList", payload)
    return payload


@router_v2.get(
    "/resume-templates/{template_id}",
    openapi_extra={"x-contract-response": "TemplateManifest", "x-api-v2-phase": 0},
)
async def get_resume_template_v2(
    request: Request,
    template_id: str,
    version: Annotated[str, Query(min_length=1, max_length=80)],
) -> dict[str, Any]:
    """Get the exact immutable template version required by the v2 canonical path."""

    manifest = get_template_manifest(template_id, version)
    if manifest is None:
        raise DomainError(
            Problem("resume.template_not_found", 404, "Resume template was not found")
        )
    payload = _v2_manifest(manifest)
    _container(request).contracts_v2.validate_definition("TemplateManifest", payload)
    return payload


__all__ = [
    "PROTECTED_RESOURCE_METADATA_URL",
    "PUBLIC_ORIGIN",
    "TEST_RESOURCE_SERVER_ORIGIN",
    "is_public_v2_path",
    "router_v2",
]
