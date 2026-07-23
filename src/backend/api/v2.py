"""Published API v2 routes that are safe to expose during the parallel migration."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import Response

from backend.api.constants import (
    PROTECTED_RESOURCE_METADATA_URL,
    PUBLIC_ORIGIN,
    TEST_RESOURCE_SERVER_ORIGIN,
)
from backend.api.v2_http import CursorCodec, JsonValue, list_response
from backend.api.v2_transport import (
    DEFAULT_PAGE_LIMIT,
    OpaquePath,
    PageCursor,
    PageLimit,
    json_response,
    require_no_body,
    require_query,
)
from backend.composition import BackendContainer
from backend.domain.common import DomainError, Problem
from backend.domain.templates import get_template_manifest, list_template_manifests

router_v2 = APIRouter(prefix="/api/v2")

_TEMPLATE_FILTERS: dict[str, JsonValue] = {"collection": "resume_templates"}
"""@brief 公开模板 cursor 的冻结 filter 绑定 / Frozen public-template cursor filter binding."""

_TEMPLATE_SORT = ("id", "version")
"""@brief 公开模板的稳定 keyset 顺序 / Stable keyset ordering for public templates."""


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


def _decode_template_position(
    codec: CursorCodec,
    cursor: str | None,
) -> tuple[str, str] | None:
    """@brief 验证公开模板 cursor 并取得 keyset 位置 / Verify a public-template cursor and return its keyset position.

    @param codec 共享 HMAC cursor codec / Shared HMAC cursor codec.
    @param cursor 可选不透明 cursor / Optional opaque cursor.
    @return ``(template_id, version)`` 或首页空位置 / ``(template_id, version)`` or no first-page position.
    @raise DomainError cursor payload 不是模板位置时抛出 / Raised when the cursor payload is not
        a template position.
    """
    if cursor is None:
        return None
    decoded = codec.decode(
        cursor,
        principal=None,
        workspace_id=None,
        filters=_TEMPLATE_FILTERS,
        sort=_TEMPLATE_SORT,
    )
    if (
        not isinstance(decoded, dict)
        or set(decoded) != {"id", "version"}
        or not isinstance(decoded["id"], str)
        or not isinstance(decoded["version"], str)
    ):
        raise DomainError(Problem("http.cursor_invalid", 400, "Pagination cursor is invalid"))
    return decoded["id"], decoded["version"]


@router_v2.get(
    "/resume-templates",
    openapi_extra={"x-contract-response": "TemplateList", "x-api-v2-phase": 0},
)
async def list_resume_templates_v2(
    request: Request,
    cursor: PageCursor = None,
    limit: PageLimit = DEFAULT_PAGE_LIMIT,
) -> Response:
    """List public immutable templates using the v2 collection and pagination shape."""

    require_query(request, "cursor", "limit")
    await require_no_body(request)
    container = _container(request)
    items = sorted(
        (_v2_manifest(item) for item in list_template_manifests(None)),
        key=lambda item: (str(item["id"]), str(item["version"])),
    )
    position = _decode_template_position(container.v2_cursor, cursor)
    remaining = [
        item
        for item in items
        if position is None or (str(item["id"]), str(item["version"])) > position
    ]
    selected = remaining[:limit]
    next_cursor: str | None = None
    if len(remaining) > len(selected):
        last = selected[-1]
        next_cursor = container.v2_cursor.encode(
            {"id": str(last["id"]), "version": str(last["version"])},
            principal=None,
            workspace_id=None,
            filters=_TEMPLATE_FILTERS,
            sort=_TEMPLATE_SORT,
        )
    payload = list_response(selected, next_cursor=next_cursor)
    container.contracts_v2.validate_definition("TemplateList", payload)
    return json_response(request, payload, cache_control="public, max-age=300")


@router_v2.get(
    "/resume-templates/{template_id}",
    openapi_extra={"x-contract-response": "TemplateManifest", "x-api-v2-phase": 0},
)
async def get_resume_template_v2(
    request: Request,
    template_id: OpaquePath,
    version: Annotated[str, Query(min_length=1, max_length=80)],
) -> Response:
    """Get the exact immutable template version required by the v2 canonical path."""

    require_query(request, "version")
    await require_no_body(request)
    manifest = get_template_manifest(template_id, version)
    if manifest is None:
        raise DomainError(
            Problem("resume.template_not_found", 404, "Resume template was not found")
        )
    payload = _v2_manifest(manifest)
    _container(request).contracts_v2.validate_definition("TemplateManifest", payload)
    return json_response(
        request,
        payload,
        cache_control="public, max-age=31536000, immutable",
    )


__all__ = [
    "PROTECTED_RESOURCE_METADATA_URL",
    "PUBLIC_ORIGIN",
    "TEST_RESOURCE_SERVER_ORIGIN",
    "is_public_v2_path",
    "router_v2",
]
