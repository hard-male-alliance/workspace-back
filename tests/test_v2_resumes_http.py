"""@brief API V2 Resume HTTP 适配器测试 / API V2 Resume HTTP adapter tests."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from backend.api.v2_http import CursorCodec
from backend.api.v2_resumes import create_v2_resume_router, router_v2_resumes
from backend.application.resumes import ResumeApplicationService
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM
from backend.domain.principals import (
    MembershipId,
    ResourceMeta,
    Subject,
    UserId,
    WorkspaceId,
)
from backend.domain.resume_proposals import ResumeProposal, ResumeProposalStatus
from backend.domain.resumes import (
    PageSize,
    ResumeId,
    ResumeOperationId,
    ResumeProposalId,
    ResumeSectionKind,
    SetResumeField,
    TemplatePolicy,
    TemplateRef,
    TemplateZonePolicy,
)
from backend.domain.users import User
from backend.domain.workspaces import (
    DataRegion,
    Membership,
    MemberStatus,
    Workspace,
    WorkspacePlan,
    WorkspaceRole,
)
from backend.infrastructure.access import InMemoryAccessStore
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.resumes import (
    InMemoryResumeStore,
    InMemoryResumeUnitOfWorkFactory,
    MappingResumeTemplateCatalog,
)
from backend.infrastructure.v2_idempotency import (
    InMemoryIdempotencyExecutor,
    InMemoryV2IdempotencyStore,
)
from backend.package_resources import read_contract_schema_text

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 固定测试时刻 / Fixed test instant."""

USER_ID = UserId("user_http_000001")
"""@brief 主测试用户 / Primary test user."""

WORKSPACE_ID = WorkspaceId("workspace_http_000001")
"""@brief 主测试 Workspace / Primary test workspace."""

OTHER_WORKSPACE_ID = WorkspaceId("workspace_http_000002")
"""@brief 无成员关系 Workspace / Workspace without membership."""

TEMPLATE_REF = TemplateRef("template_http_000001", "1.0")
"""@brief 测试模板版本 / Test template version."""


class _Clock:
    """@brief 返回单调测试时刻 / Return monotonic test instants."""

    def __init__(self) -> None:
        """@brief 初始化测试时钟 / Initialize the test clock."""

        self.current = NOW

    def now(self) -> datetime:
        """@brief 返回当前值并前进一步 / Return the current value and advance it.

        @return 带时区测试时刻 / Timezone-aware test instant.
        """

        value = self.current
        self.current += timedelta(seconds=1)
        return value


class _Ids:
    """@brief 生成满足 OpaqueId 的确定性标识 / Generate deterministic OpaqueIds."""

    def __init__(self) -> None:
        """@brief 初始化每前缀计数器 / Initialize counters per prefix."""

        self.values: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        """@brief 返回下一个确定性标识 / Return the next deterministic identifier.

        @param prefix 领域前缀 / Domain prefix.
        @return 契约合法标识 / Contract-valid identifier.
        """

        value = self.values.get(prefix, 0) + 1
        self.values[prefix] = value
        return f"{prefix}_http_{value:06d}"


@dataclass(slots=True)
class _Runtime:
    """@brief Resume adapter 的隔离 runtime / Isolated Resume-adapter runtime."""

    resumes_v2: ResumeApplicationService
    """@brief Resume 应用服务 / Resume application service."""

    contracts_v2: ContractValidator
    """@brief 权威 V2 validator / Authoritative V2 validator."""

    v2_cursor: CursorCodec
    """@brief 测试 cursor codec / Test cursor codec."""

    v2_idempotency: InMemoryIdempotencyExecutor
    """@brief 测试幂等 executor / Test idempotency executor."""


@dataclass(slots=True)
class _Harness:
    """@brief 组合 client、状态与 validator / Bundle client, state, and validator."""

    client: TestClient
    """@brief 同步 HTTP client / Synchronous HTTP client."""

    factory: InMemoryResumeUnitOfWorkFactory
    """@brief 可播种的 Resume UoW factory / Seedable Resume UoW factory."""

    validator: ContractValidator
    """@brief response schema validator / Response schema validator."""


def _policy() -> TemplatePolicy:
    """@brief 构造支持测试命令的最小模板策略 / Build a minimal test template policy.

    @return immutable TemplatePolicy / Immutable TemplatePolicy.
    """

    kinds = frozenset(ResumeSectionKind)
    return TemplatePolicy(
        TEMPLATE_REF,
        frozenset({"zh-CN", "en-US"}),
        frozenset({PageSize.A4}),
        frozenset({"pdf", "json", "docx"}),
        kinds,
        (TemplateZonePolicy("main", kinds, 100),),
        frozenset({"body.default"}),
        frozenset({"yyyy_mm"}),
        frozenset({"bullet.default"}),
    )


def _claims(actor: str) -> dict[str, object]:
    """@brief 构造 middleware 已验签 claims / Build middleware-verified claims.

    @param actor owner 或 narrow / owner or narrow.
    @return token claims / Token claims.
    """

    scope = (
        "resume.read"
        if actor == "narrow"
        else "resume.read resume.write resume.render"
    )
    return {
        ACCESS_TOKEN_USER_ID_CLAIM: str(USER_ID),
        "sub": "subject_http_000001",
        "client_id": "client_http_000001",
        "scope": scope,
    }


@contextmanager
def _harness() -> Iterator[_Harness]:
    """@brief 启动隔离 Resume FastAPI app / Start an isolated Resume FastAPI app.

    @return 活跃测试 harness / Active test harness.
    """

    access = InMemoryAccessStore()
    user = User(
        ResourceMeta(USER_ID, 1, NOW, NOW),
        Subject("subject_http_000001"),
        "klee@example.com",
        True,
        "Klee",
        "zh-CN",
        WORKSPACE_ID,
    )
    access.users[str(USER_ID)] = user
    access.workspaces[str(WORKSPACE_ID)] = Workspace(
        ResourceMeta(WORKSPACE_ID, 1, NOW, NOW),
        "Klee Lab",
        "klee-http-lab",
        WorkspacePlan.TEAM,
        DataRegion.CN,
    )
    access.workspaces[str(OTHER_WORKSPACE_ID)] = Workspace(
        ResourceMeta(OTHER_WORKSPACE_ID, 1, NOW, NOW),
        "Other Lab",
        "other-http-lab",
        WorkspacePlan.TEAM,
        DataRegion.CN,
    )
    membership = Membership(
        ResourceMeta(MembershipId("membership_http_000001"), 1, NOW, NOW),
        WORKSPACE_ID,
        USER_ID,
        "Klee",
        WorkspaceRole.EDITOR,
        MemberStatus.ACTIVE,
    )
    access.memberships[str(membership.meta.id)] = membership
    factory = InMemoryResumeUnitOfWorkFactory(
        access,
        store=InMemoryResumeStore(),
        templates=MappingResumeTemplateCatalog({TEMPLATE_REF: _policy()}),
    )
    validator = ContractValidator.from_jsonc(read_contract_schema_text("v2"))
    runtime = _Runtime(
        ResumeApplicationService(factory, clock=_Clock(), id_factory=_Ids()),
        validator,
        CursorCodec(b"resume-http-cursor-secret-000000001"),
        InMemoryIdempotencyExecutor(
            InMemoryV2IdempotencyStore(),
            retention=timedelta(days=30),
        ),
    )
    app = FastAPI()
    app.include_router(create_v2_resume_router(lambda _request: runtime))

    @app.middleware("http")
    async def verified_context(request: Request, call_next: object) -> object:
        """@brief 模拟生产 middleware 注入已验签 claims / Simulate verified claims middleware.

        @param request 当前 request / Current request.
        @param call_next ASGI downstream callable / ASGI downstream callable.
        @return downstream response / Downstream response.
        """

        request.state.request_id = request.headers.get("X-Request-Id", "request_http_000001")
        request.state.oauth_claims = _claims(request.headers.get("X-Test-Actor", "owner"))
        return await call_next(request)  # type: ignore[operator]

    with TestClient(app, raise_server_exceptions=False) as client:
        yield _Harness(client, factory, validator)


def _headers(
    *,
    actor: str = "owner",
    request_id: str = "request_http_000001",
    idempotency_key: str | None = None,
    etag: str | None = None,
    merge_patch: bool = False,
) -> dict[str, str]:
    """@brief 构造 Resume transport headers / Build Resume transport headers.

    @param actor 测试 actor / Test actor.
    @param request_id request ID / Request ID.
    @param idempotency_key 可选幂等键 / Optional idempotency key.
    @param etag 可选 If-Match / Optional If-Match.
    @param merge_patch 是否声明 Merge Patch / Whether to declare Merge Patch.
    @return header 字典 / Header dictionary.
    """

    headers = {"X-Test-Actor": actor, "X-Request-Id": request_id}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    if etag is not None:
        headers["If-Match"] = etag
    if merge_patch:
        headers["Content-Type"] = "application/merge-patch+json"
    return headers


def _create_resume(
    harness: _Harness,
    *,
    title: str = "Distributed Systems Engineer",
    key: str = "resume-create-key-000001",
) -> object:
    """@brief 通过正式 HTTP route 创建 Resume / Create a Resume through the HTTP route.

    @param harness 当前测试 harness / Current test harness.
    @param title Resume title / Resume title.
    @param key 幂等键 / Idempotency key.
    @return TestClient response / TestClient response.
    """

    return harness.client.post(
        f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
        headers=_headers(idempotency_key=key),
        json={
            "title": title,
            "locale": "zh-CN",
            "template": {
                "template_id": TEMPLATE_REF.template_id,
                "version": TEMPLATE_REF.version,
            },
        },
    )


def test_router_registers_exactly_fourteen_resume_routes_with_contract_markers() -> None:
    """@brief router 精确注册十四条冻结路由 / Router registers exactly fourteen frozen routes."""

    actual = {
        (method, route.path)
        for route in router_v2_resumes.routes
        if isinstance(route, APIRoute)
        for method in route.methods or set()
    }
    assert actual == {
        ("GET", "/api/v2/workspaces/{workspace_id}/resumes"),
        ("POST", "/api/v2/workspaces/{workspace_id}/resumes"),
        ("POST", "/api/v2/workspaces/{workspace_id}/resume-import-jobs"),
        ("GET", "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}"),
        ("PATCH", "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}"),
        ("DELETE", "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}"),
        ("GET", "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/revisions"),
        (
            "GET",
            "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/revisions/{revision}",
        ),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/restore-jobs",
        ),
        ("POST", "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/operations"),
        ("POST", "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/render-jobs"),
        ("GET", "/api/v2/workspaces/{workspace_id}/resumes/{resume_id}/proposals"),
        ("GET", "/api/v2/workspaces/{workspace_id}/resume-proposals/{proposal_id}"),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/resume-proposals/{proposal_id}/decisions",
        ),
    }
    assert all(
        route.openapi_extra is not None
        and route.openapi_extra["x-api-v2-phase"] == 2
        and (
            "x-contract-response" in route.openapi_extra
            or "DELETE" in (route.methods or set())
        )
        for route in router_v2_resumes.routes
        if isinstance(route, APIRoute)
    )


def test_resume_crud_revisions_operations_and_jobs_follow_the_published_contract() -> None:
    """@brief CRUD、revision、operation 与 Job 共享正式 schema / CRUD, revisions, operations, and jobs share the schema."""

    with _harness() as harness:
        created = _create_resume(harness)
        assert created.status_code == 201
        assert created.headers["location"].endswith(created.json()["id"])
        assert created.headers["etag"].startswith('"')
        harness.validator.validate_definition("ResumeDocument", created.json())
        resume_id = created.json()["id"]

        replay = _create_resume(
            harness,
            key="resume-create-key-000001",
        )
        assert replay.status_code == created.status_code
        assert replay.content == created.content
        assert replay.headers["etag"] == created.headers["etag"]
        assert replay.headers["location"] == created.headers["location"]

        collection = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
            headers=_headers(),
        )
        assert collection.status_code == 200
        harness.validator.validate_definition("ResumeList", collection.json())
        assert [item["id"] for item in collection.json()["items"]] == [resume_id]

        detail = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}",
            headers=_headers(),
        )
        assert detail.status_code == 200
        assert detail.json() == created.json()

        patched = harness.client.patch(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}",
            headers=_headers(etag=detail.headers["etag"], merge_patch=True),
            content=json.dumps({"title": "Staff Distributed Systems Engineer"}),
        )
        assert patched.status_code == 200
        assert patched.json()["revision"] == 2
        assert patched.json()["title"].startswith("Staff")
        harness.validator.validate_definition("ResumeDocument", patched.json())

        stale = harness.client.patch(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}",
            headers=_headers(etag=detail.headers["etag"], merge_patch=True),
            content=json.dumps({"locale": "en-US"}),
        )
        assert stale.status_code == 412
        assert stale.json()["code"] == "http.precondition_failed"

        revisions = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}/revisions",
            headers=_headers(),
        )
        assert revisions.status_code == 200
        harness.validator.validate_definition("ResumeRevisionList", revisions.json())
        assert [item["revision"] for item in revisions.json()["items"]] == [1, 2]

        revision = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}/revisions/1",
            headers=_headers(),
        )
        assert revision.status_code == 200
        assert revision.headers["etag"].startswith('"')
        harness.validator.validate_definition("ResumeRevision", revision.json())

        conflict = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}/operations",
            headers=_headers(
                idempotency_key="resume-conflict-key-0001",
                etag=patched.headers["etag"],
            ),
            json={
                "client_batch_id": "batch_http_000098",
                "base_revision": 1,
                "conflict_strategy": "reject",
                "operations": [
                    {
                        "operation_id": "operation_http_000098",
                        "op": "set_field",
                        "entity_id": resume_id,
                        "field_path": ["title"],
                        "value": "Stale write",
                    }
                ],
                "render_hint": "none",
            },
        )
        assert conflict.status_code == 412
        assert conflict.json()["code"] == "resume.revision_conflict"
        assert conflict.json()["retryable"] is True
        assert conflict.json()["errors"][0] == {
            "pointer": "/base_revision",
            "code": "stale_revision",
            "message_key": "errors.resume.stale_revision",
            "params": {"current_revision": 2},
        }
        assert conflict.json()["extensions"] == {
            "org.hmalliances.current_revision": 2
        }
        harness.validator.validate_definition("ProblemDetails", conflict.json())

        operation = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}/operations",
            headers=_headers(
                idempotency_key="resume-operation-key-0001",
                etag=patched.headers["etag"],
            ),
            json={
                "client_batch_id": "batch_http_000001",
                "base_revision": 2,
                "conflict_strategy": "reject",
                "operations": [
                    {
                        "operation_id": "operation_http_000001",
                        "op": "upsert_section",
                        "section": {
                            "id": "section_http_000001",
                            "kind": "experience",
                            "title": "Experience",
                            "visible": True,
                            "content": {
                                "text": "Systems",
                                "marks": [{"start": 0, "end": 7, "kind": "strong"}],
                            },
                            "items": [
                                {
                                    "id": "item_http_000001",
                                    "kind": "experience",
                                    "title": "Backend Engineer",
                                    "subtitle": None,
                                    "organization": "Favonius",
                                    "location": None,
                                    "date_range": {"start": "2024-01", "end": "present"},
                                    "summary": None,
                                    "highlights": [],
                                    "skills": ["Python"],
                                    "tags": [],
                                    "visible": True,
                                    "url": None,
                                }
                            ],
                        },
                        "after_section_id": None,
                    },
                    {
                        "operation_id": "operation_http_000002",
                        "op": "upsert_item",
                        "section_id": "section_http_000001",
                        "item": {
                            "id": "item_http_000002",
                            "kind": "project",
                            "title": "Consensus Simulator",
                            "subtitle": None,
                            "organization": None,
                            "location": None,
                            "date_range": None,
                            "summary": None,
                            "highlights": [],
                            "skills": [],
                            "tags": [],
                            "visible": True,
                            "url": None,
                        },
                        "after_item_id": "item_http_000001",
                    },
                    {
                        "operation_id": "operation_http_000003",
                        "op": "set_field",
                        "entity_id": resume_id,
                        "field_path": ["title"],
                        "value": "Principal Distributed Systems Engineer",
                    },
                    {
                        "operation_id": "operation_http_000004",
                        "op": "move_entity",
                        "entity_kind": "item",
                        "entity_id": "item_http_000002",
                        "parent_id": "section_http_000001",
                        "after_id": None,
                    },
                    {
                        "operation_id": "operation_http_000005",
                        "op": "remove_entity",
                        "entity_kind": "item",
                        "entity_id": "item_http_000002",
                    },
                    {
                        "operation_id": "operation_http_000006",
                        "op": "set_template",
                        "template": {
                            "template_id": TEMPLATE_REF.template_id,
                            "version": TEMPLATE_REF.version,
                        },
                        "settings": {},
                    },
                ],
                "render_hint": "none",
            },
        )
        assert operation.status_code == 200
        assert operation.json()["resume"]["revision"] == 3
        assert operation.json()["applied_operation_ids"] == [
            f"operation_http_00000{index}" for index in range(1, 7)
        ]
        assert "href" not in operation.json()["resume"]["sections"][0]["content"][
            "marks"
        ][0]
        harness.validator.validate_definition("ResumeOperationResult", operation.json())

        current = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}",
            headers=_headers(),
        )
        assert current.json()["title"].startswith("Principal")

        render = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}/render-jobs",
            headers=_headers(idempotency_key="resume-render-key-000001"),
            json={"resume_revision": 3, "mode": "final", "formats": ["pdf", "json"]},
        )
        assert render.status_code == 202
        assert render.json()["status"] == "queued"
        assert render.json()["started_at"] is None
        harness.validator.validate_definition("Job", render.json())

        restore = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}/restore-jobs",
            headers=_headers(
                idempotency_key="resume-restore-key-00001",
                etag=current.headers["etag"],
            ),
            json={"source_revision": 1},
        )
        assert restore.status_code == 202
        assert restore.json()["kind"] == "resume.restore"
        harness.validator.validate_definition("Job", restore.json())

        harness.factory.store.add_completed_upload(
            WORKSPACE_ID,
            "upload_http_000001",
            completed_at=NOW,
            expires_at=NOW + timedelta(days=1),
        )
        imported = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resume-import-jobs",
            headers=_headers(idempotency_key="resume-import-key-000001"),
            json={
                "upload_session_id": "upload_http_000001",
                "title": "Imported Resume",
                "locale": "zh-CN",
                "template": {
                    "template_id": TEMPLATE_REF.template_id,
                    "version": TEMPLATE_REF.version,
                },
            },
        )
        assert imported.status_code == 202
        assert imported.json()["kind"] == "resume.import"
        harness.validator.validate_definition("Job", imported.json())

        deleted = harness.client.delete(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}",
            headers=_headers(etag=current.headers["etag"]),
        )
        assert deleted.status_code == 204
        assert deleted.content == b""


def test_proposal_list_detail_and_decision_are_workspace_bound_and_replayable() -> None:
    """@brief Proposal 读取与决策保持 Workspace/ETag/幂等边界 / Proposal paths preserve all boundaries."""

    with _harness() as harness:
        created = _create_resume(harness)
        resume_id = ResumeId(created.json()["id"])
        proposal_id = ResumeProposalId("proposal_http_000001")
        proposal = ResumeProposal(
            ResourceMeta(proposal_id, 1, NOW, NOW),
            WORKSPACE_ID,
            resume_id,
            1,
            "Promote title",
            ResumeProposalStatus.PENDING,
            (
                SetResumeField(
                    ResumeOperationId("operation_http_000002"),
                    str(resume_id),
                    ("title",),
                    "Staff Systems Researcher",
                ),
            ),
        )
        harness.factory.store.proposals[(WORKSPACE_ID, proposal_id)] = proposal

        collection = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}/proposals",
            headers=_headers(),
        )
        assert collection.status_code == 200
        harness.validator.validate_definition("ResumeProposalList", collection.json())
        assert collection.json()["items"][0]["operations"][0]["op"] == "set_field"

        detail = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resume-proposals/{proposal_id}",
            headers=_headers(),
        )
        assert detail.status_code == 200
        harness.validator.validate_definition("ResumeProposal", detail.json())

        decision = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resume-proposals/{proposal_id}/decisions",
            headers=_headers(
                idempotency_key="proposal-decision-key-0001",
                etag=detail.headers["etag"],
            ),
            json={"decision": "accept", "accepted_operation_ids": []},
        )
        assert decision.status_code == 200
        assert decision.json()["resume"]["title"] == "Staff Systems Researcher"
        harness.validator.validate_definition("ResumeOperationResult", decision.json())

        replay = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resume-proposals/{proposal_id}/decisions",
            headers=_headers(
                request_id="request_http_000002",
                idempotency_key="proposal-decision-key-0001",
                etag=detail.headers["etag"],
            ),
            json={"decision": "accept", "accepted_operation_ids": []},
        )
        assert replay.content == decision.content
        assert replay.headers["X-Request-Id"] == "request_http_000002"

        decided = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resume-proposals/{proposal_id}",
            headers=_headers(),
        )
        assert decided.json()["status"] == "accepted"
        assert decided.json()["revision"] == 2

        hidden = harness.client.get(
            f"/api/v2/workspaces/{OTHER_WORKSPACE_ID}/resume-proposals/{proposal_id}",
            headers=_headers(),
        )
        assert hidden.status_code == 404
        assert hidden.json()["code"] == "resource.not_found"


def test_strict_schema_scope_query_body_depth_and_cursor_boundaries_fail_closed() -> None:
    """@brief Schema、scope、query、body depth 与 cursor 均 fail closed / Boundaries fail closed."""

    with _harness() as harness:
        first = _create_resume(harness, title="First", key="resume-create-key-000011")
        second = _create_resume(harness, title="Second", key="resume-create-key-000012")
        assert first.status_code == second.status_code == 201

        page = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
            headers=_headers(),
            params={"limit": 1},
        )
        assert page.status_code == 200
        assert page.json()["page"]["has_more"] is True
        cursor = page.json()["page"]["next_cursor"]
        continued = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
            headers=_headers(),
            params={"limit": 1, "cursor": cursor},
        )
        assert continued.status_code == 200
        assert continued.json()["items"][0]["id"] != page.json()["items"][0]["id"]

        tampered = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
            headers=_headers(),
            params={"limit": 1, "cursor": f"{cursor}x"},
        )
        assert tampered.status_code == 400
        assert tampered.json()["code"] == "http.cursor_invalid"

        wrong_collection = harness.client.get(
            (
                f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/"
                f"{first.json()['id']}/revisions"
            ),
            headers=_headers(),
            params={"limit": 1, "cursor": cursor},
        )
        assert wrong_collection.status_code == 400
        assert wrong_collection.json()["code"] == "http.cursor_invalid"

        unknown_query = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
            headers=_headers(),
            params={"offset": 0},
        )
        assert unknown_query.status_code == 400
        assert unknown_query.json()["code"] == "http.invalid_query"

        unexpected_body = harness.client.request(
            "GET",
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
            headers={**_headers(), "Content-Type": "application/json"},
            content="{}",
        )
        assert unexpected_body.status_code == 400
        assert unexpected_body.json()["code"] == "http.unexpected_body"

        invalid_schema = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
            headers=_headers(idempotency_key="resume-invalid-key-00001"),
            json={
                "title": "Invalid",
                "locale": "zh-CN",
                "template": {
                    "template_id": TEMPLATE_REF.template_id,
                    "version": TEMPLATE_REF.version,
                },
                "workspace_id": str(OTHER_WORKSPACE_ID),
            },
        )
        assert invalid_schema.status_code == 422
        assert invalid_schema.json()["code"] == "contract.validation_failed"

        denied = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/resumes",
            headers=_headers(
                actor="narrow",
                idempotency_key="resume-denied-key-000001",
            ),
            json={
                "title": "Denied",
                "locale": "zh-CN",
                "template": {
                    "template_id": TEMPLATE_REF.template_id,
                    "version": TEMPLATE_REF.version,
                },
            },
        )
        assert denied.status_code == 403
        assert denied.json()["code"] == "oauth.insufficient_scope"

        nested: object = "leaf"
        for _ in range(30):
            nested = {"child": nested}
        too_deep = harness.client.post(
            (
                f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/"
                f"{first.json()['id']}/operations"
            ),
            headers=_headers(
                idempotency_key="resume-depth-key-000001",
                etag=first.headers["etag"],
            ),
            json={
                "client_batch_id": "batch_http_000099",
                "base_revision": 1,
                "conflict_strategy": "reject",
                "operations": [
                    {
                        "operation_id": "operation_http_000099",
                        "op": "set_field",
                        "entity_id": first.json()["id"],
                        "field_path": ["title"],
                        "value": nested,
                    }
                ],
                "render_hint": "none",
            },
        )
        assert too_deep.status_code == 413
        assert too_deep.json()["code"] == "http.payload_too_large"
