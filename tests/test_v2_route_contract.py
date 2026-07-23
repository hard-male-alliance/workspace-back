"""@brief API V2 全量路由与正式契约的一致性门禁 / Full API V2 route-to-published-contract conformance gate.

期望集合只从只读 ``contracts/v2/contract.md`` 动态抽取；schema 名只从正式
``schema.jsonc`` 解析。测试不维护第八份手写路由清单，因此新增、删除或重命名正式
route 时，router 与契约必须在同一次审阅中一致变化。
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from fastapi import APIRouter
from fastapi.routing import APIRoute

from backend.api.v2 import router_v2
from backend.api.v2_access import create_v2_access_router
from backend.api.v2_agent import create_v2_agent_router
from backend.api.v2_interview import create_v2_interview_router
from backend.api.v2_knowledge import create_v2_knowledge_router
from backend.api.v2_platform import create_v2_platform_router
from backend.api.v2_resumes import create_v2_resume_router
from backend.app import create_app
from backend.config import BackendSettings
from backend.infrastructure.contracts import load_jsonc_document
from conftest import PROJECT_ROOT

_V2_DIRECTORY = PROJECT_ROOT / "workspace-shared-docs" / "contracts" / "v2"
"""@brief 只读正式 V2 发布目录 / Read-only published V2 directory."""

_ROUTE_ROW = re.compile(
    r"^\| (?P<method>GET|POST|PATCH|DELETE) \| "
    r"`(?P<target>/api/v2[^`]*)` \| (?P<binding>[^|]+?) \|$"
)
"""@brief 正式 Product API Markdown 路由行 / Published Product API Markdown route row."""

_SECTION_HEADING = re.compile(r"^### (?P<section>5\.[1-6])(?:\s|$)")
"""@brief Product API 子域 section heading / Product API bounded-context section heading."""

_SCHEMA_REFERENCE = re.compile(r"`(?P<name>[A-Z][A-Za-z0-9]+)`")
"""@brief Markdown 中 schema definition 引用 / Schema-definition reference in Markdown."""

_EXPLICIT_STATUS = re.compile(r"\((?P<status>20[0-9])\)")
"""@brief route binding 中显式成功状态 / Explicit success status in a route binding."""

_CONTRACT_METADATA_KEYS = frozenset(
    {
        "x-contract-request",
        "x-contract-response",
        "x-contract-stream-item",
    }
)
"""@brief router 必须与契约逐项相等的 OpenAPI extensions / OpenAPI extensions compared exactly."""

_SECTION_OWNER: Mapping[str, str] = {
    "5.1": "access",
    "5.2": "resumes",
    "5.3": "knowledge",
    "5.4": "agent",
    "5.5": "interview",
    "5.6": "platform",
}
"""@brief 正式 bounded context 到 layered router 的映射 / Published bounded-context to layered-router mapping."""


@dataclass(frozen=True, slots=True)
class _PublishedRoute:
    """@brief 从正式 Markdown 解析的单条 Product API 契约 / One Product API contract parsed from published Markdown.

    @param section 正式 5.x section / Published 5.x section.
    @param owner 应实现该路由的七个 router 之一 / One of the seven routers owning the route.
    @param method HTTP method / HTTP method.
    @param path 不含 query template 的 FastAPI path / FastAPI path without a query template.
    @param request_definition 可选 request schema / Optional request schema.
    @param response_definition 可选 JSON response schema / Optional JSON response schema.
    @param stream_definition 可选 SSE item schema / Optional SSE item schema.
    @param mentioned_definitions binding 中全部 schema 引用 / Every schema reference in the binding.
    @param success_status 规范化后的成功状态 / Normalized success status.
    """

    section: str
    owner: str
    method: str
    path: str
    request_definition: str | None
    response_definition: str | None
    stream_definition: str | None
    mentioned_definitions: tuple[str, ...]
    success_status: int

    @property
    def key(self) -> tuple[str, str]:
        """@brief 返回 method+path 唯一键 / Return the method-plus-path unique key."""

        return self.method, self.path

    @property
    def contract_metadata(self) -> dict[str, str]:
        """@brief 返回该 route 应公开的 schema extensions / Return expected schema extensions for the route."""

        metadata: dict[str, str] = {}
        if self.request_definition is not None:
            metadata["x-contract-request"] = self.request_definition
        if self.response_definition is not None:
            metadata["x-contract-response"] = self.response_definition
        if self.stream_definition is not None:
            metadata["x-contract-stream-item"] = self.stream_definition
        return metadata


@dataclass(frozen=True, slots=True)
class _ImplementedRoute:
    """@brief 七个 router 之一公开的单条 /api/v2 route / One /api/v2 route exposed by one of seven routers."""

    owner: str
    route: APIRoute
    method: str

    @property
    def key(self) -> tuple[str, str]:
        """@brief 返回 method+path 唯一键 / Return the method-plus-path unique key."""

        return self.method, self.route.path

    @property
    def success_status(self) -> int:
        """@brief 返回 FastAPI 实际成功状态，未声明时按默认 200 / Return effective FastAPI success status, defaulting to 200."""

        return self.route.status_code or 200

    @property
    def contract_metadata(self) -> dict[str, str]:
        """@brief 只投影 schema contract extensions / Project only schema-contract extensions."""

        extras = self.route.openapi_extra or {}
        return {
            key: _required_metadata_string(extras, key)
            for key in _CONTRACT_METADATA_KEYS
            if key in extras
        }


def _published_routes(contract_path: Path) -> tuple[_PublishedRoute, ...]:
    """@brief 从正式 contract.md 动态解析全部 Product API routes / Parse every Product API route dynamically from contract.md.

    @param contract_path 正式 Markdown 文件 / Published Markdown file.
    @return 保持发布顺序的 85 条路由 / Eighty-five routes in publication order.
    @raise AssertionError 路由落在未知 section 或 binding 语法未知时抛出 / Raised for an unknown section or binding grammar.
    """

    section: str | None = None
    parsed: list[_PublishedRoute] = []
    for line in contract_path.read_text(encoding="utf-8").splitlines():
        heading = _SECTION_HEADING.match(line)
        if heading is not None:
            section = heading.group("section")
            continue
        row = _ROUTE_ROW.match(line)
        if row is None:
            continue
        assert section is not None, f"route outside a published 5.x section: {line}"
        assert section in _SECTION_OWNER, f"unknown published Product API section: {section}"
        method = row.group("method")
        target = row.group("target")
        binding = row.group("binding")
        path, separator, query_template = target.partition("?")
        if separator:
            assert query_template, f"empty query template in {target}"
        request_definition, response_definition, stream_definition = _binding_schemas(
            binding
        )
        owner = _SECTION_OWNER[section]
        if section == "5.2" and path.startswith("/api/v2/resume-templates"):
            owner = "templates"
        parsed.append(
            _PublishedRoute(
                section=section,
                owner=owner,
                method=method,
                path=path,
                request_definition=request_definition,
                response_definition=response_definition,
                stream_definition=stream_definition,
                mentioned_definitions=tuple(_SCHEMA_REFERENCE.findall(binding)),
                success_status=_success_status(
                    method,
                    path,
                    binding,
                    response_definition,
                ),
            )
        )
    return tuple(parsed)


def _binding_schemas(binding: str) -> tuple[str | None, str | None, str | None]:
    """@brief 解析 Request → Response、binary 或 SSE binding / Parse a request-response, binary, or SSE binding.

    @param binding Markdown 表格第三列 / Third Markdown-table column.
    @return request、response、stream definition / Request, response, and stream definitions.
    """

    if "→" in binding:
        request_side, response_side = binding.split("→", maxsplit=1)
        request_names = _SCHEMA_REFERENCE.findall(request_side)
        response_names = _SCHEMA_REFERENCE.findall(response_side)
        assert len(request_names) <= 1, f"ambiguous request schema binding: {binding}"
        if not response_names:
            assert response_side.strip() == "204", f"unknown response binding: {binding}"
        return (
            request_names[0] if request_names else None,
            response_names[0] if response_names else None,
            None,
        )
    if binding == "binary":
        return None, None, None
    if binding.startswith("SSE "):
        stream_names = _SCHEMA_REFERENCE.findall(binding)
        assert len(stream_names) == 1, f"ambiguous SSE binding: {binding}"
        return None, None, stream_names[0]
    raise AssertionError(f"unknown Product API route binding: {binding}")


def _success_status(
    method: str,
    path: str,
    binding: str,
    response_definition: str | None,
) -> int:
    """@brief 按 contract.md 4.2 与 route binding 推导成功状态 / Derive success status from section 4.2 and the route binding.

    @param method HTTP method / HTTP method.
    @param path 不含 query 的 canonical path / Canonical path without query.
    @param binding 正式 request-response binding / Published request-response binding.
    @param response_definition 可选主 response schema / Optional primary response schema.
    @return 200、201、202 或 204 / 200, 201, 202, or 204.
    @note 4.2 冻结：创建为 201、异步资源为 202；对已有资源的 cancellation、decision、
        completion 与同步 Result 为 200。显式 ``204``/``(202)`` 优先。
    """

    explicit = _EXPLICIT_STATUS.search(binding)
    if explicit is not None:
        return int(explicit.group("status"))
    if binding.rsplit("→", maxsplit=1)[-1].strip() == "204":
        return 204
    if method in {"GET", "PATCH"}:
        return 200
    if method == "DELETE":
        raise AssertionError(f"DELETE route must publish 204 or (202): {path}")
    assert method == "POST", f"unsupported Product API method: {method}"
    if path.endswith(("/cancellations", "/decisions", "/completions")):
        return 200
    if response_definition == "Job":
        return 202
    if response_definition is not None and response_definition.endswith("Result"):
        return 200
    return 201


def _schema_definitions(schema_path: Path) -> frozenset[str]:
    """@brief 从正式 schema.jsonc 读取 definition 名集合 / Read definition names from published schema.jsonc.

    @param schema_path 正式 JSONC schema / Published JSONC schema.
    @return `$defs` key 集合 / Set of `$defs` keys.
    """

    document = load_jsonc_document(schema_path.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    definitions = document.get("$defs")
    assert isinstance(definitions, dict)
    assert all(isinstance(name, str) for name in definitions)
    return frozenset(cast(dict[str, Any], definitions))


def _routers() -> Mapping[str, APIRouter]:
    """@brief 构造不依赖 composition 的七个 layered routers / Build seven layered routers without composition.

    @return owner 到 router 的封闭映射 / Closed owner-to-router mapping.
    """

    return {
        "templates": router_v2,
        "access": create_v2_access_router(),
        "resumes": create_v2_resume_router(),
        "knowledge": create_v2_knowledge_router(),
        "agent": create_v2_agent_router(),
        "interview": create_v2_interview_router(),
        "platform": create_v2_platform_router(),
    }


def _implemented_routes(routers: Mapping[str, APIRouter]) -> tuple[_ImplementedRoute, ...]:
    """@brief 收集七个 router 的 /api/v2 method+path / Collect /api/v2 method-plus-path entries from seven routers.

    @param routers 七个 owner router / Seven owner routers.
    @return 保留 owner 的扁平 route 集合 / Flat route collection retaining owners.
    """

    implemented: list[_ImplementedRoute] = []
    for owner, router in routers.items():
        for route in router.routes:
            if not isinstance(route, APIRoute) or not route.path.startswith("/api/v2/"):
                continue
            methods = route.methods
            assert methods is not None and len(methods) == 1, (
                f"{owner} route must declare exactly one method: {route.path}"
            )
            implemented.append(_ImplementedRoute(owner, route, next(iter(methods))))
    return tuple(implemented)


def _required_metadata_string(extras: Mapping[str, Any], key: str) -> str:
    """@brief 读取 OpenAPI schema extension 字符串 / Read an OpenAPI schema-extension string.

    @param extras route OpenAPI extras / Route OpenAPI extras.
    @param key extension key / Extension key.
    @return 非空 definition 名 / Non-empty definition name.
    """

    value = extras[key]
    assert isinstance(value, str) and value
    return value


def _duplicates[ItemT, KeyT: Hashable](
    items: Sequence[ItemT],
    key: Callable[[ItemT], KeyT],
) -> set[KeyT]:
    """@brief 返回重复 key 集合 / Return the set of duplicate keys.

    @param items 输入序列 / Input sequence.
    @param key key extractor / Key extractor.
    @return 出现多次的 key / Keys occurring more than once.
    """

    counts = Counter(key(item) for item in items)
    return {item_key for item_key, count in counts.items() if count > 1}


def test_published_contract_has_exactly_85_unique_routes_and_valid_schema_names() -> None:
    """@brief 正式 Markdown 必须给出 85 条唯一且可解析的 schema route / Published Markdown exposes 85 unique schema-valid routes."""

    routes = _published_routes(_V2_DIRECTORY / "contract.md")
    definitions = _schema_definitions(_V2_DIRECTORY / "schema.jsonc")
    assert len(routes) == 85
    assert not _duplicates(routes, lambda route: route.key)
    assert Counter(route.method for route in routes) == {
        "GET": 39,
        "POST": 32,
        "PATCH": 7,
        "DELETE": 7,
    }
    mentioned = {
        definition
        for route in routes
        for definition in route.mentioned_definitions
    }
    assert mentioned
    assert mentioned <= definitions
    assert {route.success_status for route in routes} == {200, 201, 202, 204}


def test_seven_routers_exactly_match_all_published_route_contracts() -> None:
    """@brief 七个 router 合计必须无重复、无缺失且 metadata/status 完全一致 / Seven routers have no duplicates or gaps and exact metadata/status."""

    published = _published_routes(_V2_DIRECTORY / "contract.md")
    implemented = _implemented_routes(_routers())
    expected = {route.key: route for route in published}
    actual = {route.key: route for route in implemented}

    assert len(implemented) == len(published) == 85
    assert not _duplicates(implemented, lambda route: route.key)
    assert actual.keys() == expected.keys()

    expected_owner_counts = Counter(route.owner for route in published)
    actual_owner_counts = Counter(route.owner for route in implemented)
    assert actual_owner_counts == expected_owner_counts == {
        "templates": 2,
        "access": 19,
        "resumes": 14,
        "knowledge": 17,
        "agent": 12,
        "interview": 12,
        "platform": 9,
    }

    for key, published_route in expected.items():
        implemented_route = actual[key]
        assert implemented_route.owner == published_route.owner, key
        assert implemented_route.success_status == published_route.success_status, key
        assert implemented_route.contract_metadata == published_route.contract_metadata, key


def test_every_router_contract_extension_names_a_published_schema_definition() -> None:
    """@brief 实现不得通过 OpenAPI extension 引用不存在的 schema / Router extensions cannot name absent schemas."""

    definitions = _schema_definitions(_V2_DIRECTORY / "schema.jsonc")
    implemented = _implemented_routes(_routers())
    referenced = {
        definition
        for route in implemented
        for definition in route.contract_metadata.values()
    }
    assert referenced
    assert referenced <= definitions


def test_application_mounts_each_published_v2_route_exactly_once() -> None:
    """@brief production app 不得遗漏或重复挂载已实现 route / Production app mounts every implemented route once.

    Router 级契约测试无法防止 composition root 忘记 ``include_router``；本门禁不启动
    lifespan，只核对 FastAPI 最终 route inventory。/ Router-level conformance cannot detect a
    missing ``include_router`` in the composition root, so this gate inspects the final FastAPI
    inventory without entering its lifespan.
    """

    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")
    application = create_app(settings)
    published = _published_routes(_V2_DIRECTORY / "contract.md")
    openapi = application.openapi()
    paths = cast(dict[str, Any], openapi["paths"])
    mounted = [
        (method.upper(), path)
        for path, path_item in paths.items()
        if path.startswith("/api/v2/") and isinstance(path_item, dict)
        for method in path_item
        if method.upper() in {"GET", "POST", "PATCH", "DELETE"}
    ]
    expected = [route.key for route in published]

    assert len(mounted) == len(expected) == 85
    assert not _duplicates(mounted, lambda key: key)
    assert set(mounted) == set(expected)
