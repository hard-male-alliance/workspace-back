"""@brief API V2 Platform HTTP 适配器测试 / API V2 Platform HTTP adapter tests."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from backend.api.v2_http import CursorCodec
from backend.api.v2_platform import create_v2_platform_router, router_v2_platform
from backend.application.platform import PlatformApplicationService
from backend.application.ports.platform import (
    EventReplayRequest,
    EventReplayWindowExpired,
)
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM
from backend.domain.platform import (
    ApiArtifactContentUrl,
    ApiEvent,
    ApiEventId,
    Artifact,
    ArtifactId,
    ArtifactKind,
    AuditEvent,
    AuditEventId,
    AuditOutcome,
    Job,
    JobId,
    PdfRect,
    PdfSourceMap,
    PdfSourceNode,
    ResourceRef,
)
from backend.domain.principals import (
    MembershipId,
    ResourceMeta,
    Subject,
    UserId,
    WorkspaceAccessContext,
    WorkspaceId,
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
from backend.infrastructure.platform import (
    InMemoryPlatformStore,
    InMemoryPlatformUnitOfWorkFactory,
)
from backend.infrastructure.v2_idempotency import (
    InMemoryIdempotencyExecutor,
    InMemoryV2IdempotencyStore,
)
from backend.package_resources import read_contract_schema_text

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 固定测试时刻 / Fixed test instant."""

USER_ID = UserId("user_platform_http_000001")
"""@brief 平台测试用户 / Platform test user."""

WORKSPACE_ID = WorkspaceId("workspace_platform_http_000001")
"""@brief 主测试 Workspace / Primary test Workspace."""

OTHER_WORKSPACE_ID = WorkspaceId("workspace_platform_http_000002")
"""@brief 无成员关系 Workspace / Workspace without membership."""

JOB_ID = JobId("job_platform_http_000001")
"""@brief 可取消测试 Job / Cancellable test Job."""

OTHER_JOB_ID = JobId("job_platform_http_000002")
"""@brief 列表过滤测试 Job / List-filter test Job."""

ARTIFACT_ID = ArtifactId("artifact_platform_http_000001")
"""@brief 测试 Artifact / Test Artifact."""

RESUME_ID = "resume_platform_http_000001"
"""@brief 测试 Resume 标识 / Test Resume identifier."""

CONTENT = b"%PDF-1.7\nKlee platform artifact\n"
"""@brief 测试 PDF bytes / Test PDF bytes."""

CONTENT_SHA256 = sha256(CONTENT).hexdigest()
"""@brief 测试 PDF SHA-256 / Test PDF SHA-256."""


class _Clock:
    """@brief 返回单调测试时刻 / Return monotonic test instants."""

    def __init__(self) -> None:
        """@brief 初始化测试时钟 / Initialize the test clock."""

        self.current = NOW + timedelta(minutes=1)

    def now(self) -> datetime:
        """@brief 返回当前时刻并前进 / Return and advance the current instant.

        @return 带时区时刻 / Timezone-aware instant.
        """

        value = self.current
        self.current += timedelta(seconds=1)
        return value


class _FiniteEventFeed:
    """@brief 为 HTTP 测试提供有限可重放 event feed / Finite replayable event feed for HTTP tests."""

    def __init__(self, events: tuple[ApiEvent, ...]) -> None:
        """@brief 绑定有序测试事件 / Bind ordered test events.

        @param events 按 sequence 排序的事件 / Events ordered by sequence.
        """

        self._events = events

    async def open(
        self,
        access: WorkspaceAccessContext,
        replay: EventReplayRequest,
    ) -> AsyncIterator[ApiEvent]:
        """@brief 从 Last-Event-ID 之后有限重放 / Replay finitely after Last-Event-ID.

        @param access 已授权 Workspace 证明 / Authorized Workspace proof.
        @param replay 重放起点 / Replay starting point.
        @return 有限异步事件流 / Finite asynchronous event stream.
        @raise EventReplayWindowExpired 起点不存在时抛出 / Raised when the starting event is absent.
        """

        del access
        start = len(self._events)
        if replay.after_event_id is not None:
            for index, event in enumerate(self._events):
                if event.event_id == replay.after_event_id:
                    start = index + 1
                    break
            else:
                raise EventReplayWindowExpired(replay.after_event_id)

        async def stream() -> AsyncIterator[ApiEvent]:
            """@brief 生成剩余事件 / Yield remaining events.

            @return 异步事件迭代器 / Async event iterator.
            """

            for event in self._events[start:]:
                yield event

        return stream()


@dataclass(slots=True)
class _Runtime:
    """@brief Platform adapter 的隔离 runtime / Isolated Platform-adapter runtime."""

    platform: PlatformApplicationService
    """@brief Platform 应用服务 / Platform application service."""

    contracts_v2: ContractValidator
    """@brief 权威 V2 schema validator / Authoritative V2 schema validator."""

    v2_cursor: CursorCodec
    """@brief 测试 cursor codec / Test cursor codec."""

    v2_idempotency: InMemoryIdempotencyExecutor
    """@brief 测试幂等 executor / Test idempotency executor."""


@dataclass(slots=True)
class _Harness:
    """@brief 组合 client、Platform store 与 validator / Bundle client, store, and validator."""

    client: TestClient
    """@brief 同步 HTTP client / Synchronous HTTP client."""

    store: InMemoryPlatformStore
    """@brief 可观察 Platform store / Observable Platform store."""

    validator: ContractValidator
    """@brief response schema validator / Response schema validator."""


def _claims(actor: str) -> dict[str, object]:
    """@brief 构造 middleware 已验签 claims / Build middleware-verified claims.

    @param actor owner、writer 或 narrow / owner, writer, or narrow.
    @return token claims / Token claims.
    """

    if actor == "narrow":
        scope = "workspace.read"
    elif actor == "writer":
        scope = "workspace.write resume.write resume.render"
    else:
        scope = "workspace.read workspace.write resume.write resume.render"
    return {
        ACCESS_TOKEN_USER_ID_CLAIM: str(USER_ID),
        "sub": "subject_platform_http_000001",
        "client_id": "client_platform_http_000001",
        "scope": scope,
    }


def _job(
    job_id: JobId,
    *,
    created_at: datetime,
    kind: str,
    subject: ResourceRef,
) -> Job:
    """@brief 构造 queued Job / Build a queued Job.

    @param job_id Job 标识 / Job identifier.
    @param created_at 创建时刻 / Creation instant.
    @param kind Job kind / Job kind.
    @param subject 领域目标 / Domain subject.
    @return queued Job / Queued Job.
    """

    return Job(
        ResourceMeta(job_id, 1, created_at, created_at),
        WORKSPACE_ID,
        kind,
        subject,
    )


def _artifact() -> Artifact:
    """@brief 构造同源 PDF Artifact / Build a same-origin PDF Artifact.

    @return 测试 Artifact metadata / Test Artifact metadata.
    """

    return Artifact(
        ResourceMeta(ARTIFACT_ID, 1, NOW + timedelta(seconds=2), NOW + timedelta(seconds=2)),
        WORKSPACE_ID,
        ArtifactKind.RESUME_PDF,
        ResourceRef("resume", RESUME_ID, 7),
        "application/pdf",
        len(CONTENT),
        CONTENT_SHA256,
        ApiArtifactContentUrl.build(
            "https://api.hmalliances.org:8022",
            WORKSPACE_ID,
            ARTIFACT_ID,
        ),
        page_count=1,
        expires_at=NOW + timedelta(hours=1),
    )


def _source_map() -> PdfSourceMap:
    """@brief 构造 PDF source map / Build a PDF source map.

    @return 与测试 Artifact 匹配的 map / Map matching the test Artifact.
    """

    return PdfSourceMap(
        ARTIFACT_ID,
        RESUME_ID,
        7,
        (
            PdfSourceNode(
                "item_platform_http_000001",
                ("title",),
                1,
                (PdfRect(10.0, 20.0, 100.0, 12.0),),
            ),
        ),
    )


def _events() -> tuple[ApiEvent, ApiEvent]:
    """@brief 构造可重放的两个 ApiEvent / Build two replayable ApiEvents.

    @return 按 sequence 排序的事件 / Events ordered by sequence.
    """

    return (
        ApiEvent(
            ApiEventId("event_platform_http_000001"),
            1,
            "job.updated",
            NOW,
            ResourceRef("job", JOB_ID, 1),
            {"status": "queued"},
        ),
        ApiEvent(
            ApiEventId("event_platform_http_000002"),
            2,
            "job.updated",
            NOW + timedelta(seconds=1),
            ResourceRef("job", JOB_ID, 2),
            {"status": "running", "hints": ("poll",)},
            "0123456789abcdef0123456789abcdef",
        ),
    )


@contextmanager
def _harness() -> Iterator[_Harness]:
    """@brief 启动隔离 Platform FastAPI app / Start an isolated Platform FastAPI app.

    @return 活跃测试 harness / Active test harness.
    """

    access = InMemoryAccessStore()
    access.users[str(USER_ID)] = User(
        ResourceMeta(USER_ID, 1, NOW, NOW),
        Subject("subject_platform_http_000001"),
        "klee@example.com",
        True,
        "Klee",
        "zh-CN",
        WORKSPACE_ID,
    )
    access.workspaces[str(WORKSPACE_ID)] = Workspace(
        ResourceMeta(WORKSPACE_ID, 1, NOW, NOW),
        "Klee Platform Lab",
        "klee-platform-http-lab",
        WorkspacePlan.TEAM,
        DataRegion.CN,
    )
    access.workspaces[str(OTHER_WORKSPACE_ID)] = Workspace(
        ResourceMeta(OTHER_WORKSPACE_ID, 1, NOW, NOW),
        "Other Platform Lab",
        "other-platform-http-lab",
        WorkspacePlan.TEAM,
        DataRegion.CN,
    )
    membership = Membership(
        ResourceMeta(MembershipId("membership_platform_http_000001"), 1, NOW, NOW),
        WORKSPACE_ID,
        USER_ID,
        "Klee",
        WorkspaceRole.OWNER,
        MemberStatus.ACTIVE,
    )
    access.memberships[str(membership.meta.id)] = membership

    store = InMemoryPlatformStore()
    store.seed_job(
        _job(
            JOB_ID,
            created_at=NOW,
            kind="resume.render",
            subject=ResourceRef("resume", RESUME_ID, 7),
        )
    )
    store.seed_job(
        _job(
            OTHER_JOB_ID,
            created_at=NOW + timedelta(seconds=1),
            kind="knowledge.sync",
            subject=ResourceRef("knowledge_source", "knowledge_platform_http_000001", 3),
        )
    )
    artifact = _artifact()
    store.seed_artifact(artifact, CONTENT, source_map=_source_map())
    store.seed_audit_event(
        AuditEvent(
            AuditEventId("audit_platform_http_000001"),
            WORKSPACE_ID,
            NOW,
            ResourceRef("user", USER_ID),
            "artifact.read",
            ResourceRef("artifact", ARTIFACT_ID, 1),
            AuditOutcome.ALLOWED,
            "request_platform_http_000001",
        )
    )

    factory = InMemoryPlatformUnitOfWorkFactory(access, store=store, clock=_Clock())
    validator = ContractValidator.from_jsonc(read_contract_schema_text("v2"))
    runtime = _Runtime(
        PlatformApplicationService(
            factory,
            factory.content_store,
            _FiniteEventFeed(_events()),
            clock=_Clock(),
        ),
        validator,
        CursorCodec(b"platform-http-cursor-secret-00000001"),
        InMemoryIdempotencyExecutor(
            InMemoryV2IdempotencyStore(),
            retention=timedelta(days=30),
        ),
    )
    app = FastAPI()
    app.include_router(create_v2_platform_router(lambda _request: runtime))

    @app.middleware("http")
    async def verified_context(request: Request, call_next: object) -> object:
        """@brief 模拟生产 middleware 注入已验签 claims / Simulate verified claims middleware.

        @param request 当前 request / Current request.
        @param call_next ASGI downstream callable / ASGI downstream callable.
        @return downstream response / Downstream response.
        """

        request.state.request_id = request.headers.get(
            "X-Request-Id", "request_platform_http_000001"
        )
        request.state.oauth_claims = _claims(request.headers.get("X-Test-Actor", "owner"))
        return await call_next(request)  # type: ignore[operator]

    with TestClient(app, raise_server_exceptions=False) as client:
        yield _Harness(client, store, validator)


def _headers(
    *,
    actor: str = "owner",
    request_id: str = "request_platform_http_000001",
    idempotency_key: str | None = None,
    etag: str | None = None,
) -> dict[str, str]:
    """@brief 构造 Platform transport headers / Build Platform transport headers.

    @param actor 测试 actor / Test actor.
    @param request_id request ID / Request ID.
    @param idempotency_key 可选幂等键 / Optional idempotency key.
    @param etag 可选 If-Match / Optional If-Match.
    @return header 字典 / Header dictionary.
    """

    headers = {"X-Test-Actor": actor, "X-Request-Id": request_id}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    if etag is not None:
        headers["If-Match"] = etag
    return headers


def test_router_registers_exactly_nine_platform_routes_with_contract_markers() -> None:
    """@brief router 精确注册九条冻结路由 / Router registers exactly nine frozen routes."""

    actual = {
        (method, route.path)
        for route in router_v2_platform.routes
        if isinstance(route, APIRoute)
        for method in route.methods or set()
    }
    assert actual == {
        ("GET", "/api/v2/workspaces/{workspace_id}/jobs"),
        ("GET", "/api/v2/workspaces/{workspace_id}/jobs/{job_id}"),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/jobs/{job_id}/cancellations",
        ),
        ("GET", "/api/v2/workspaces/{workspace_id}/artifacts"),
        ("GET", "/api/v2/workspaces/{workspace_id}/artifacts/{artifact_id}"),
        (
            "GET",
            "/api/v2/workspaces/{workspace_id}/artifacts/{artifact_id}/content",
        ),
        (
            "GET",
            "/api/v2/workspaces/{workspace_id}/artifacts/{artifact_id}/source-map",
        ),
        ("GET", "/api/v2/workspaces/{workspace_id}/events"),
        ("GET", "/api/v2/workspaces/{workspace_id}/audit-events"),
    }
    markers = {
        route.path: route.openapi_extra
        for route in router_v2_platform.routes
        if isinstance(route, APIRoute)
    }
    assert all(marker is not None and marker["x-api-v2-phase"] == 6 for marker in markers.values())
    assert markers["/api/v2/workspaces/{workspace_id}/events"] == {
        "x-api-v2-phase": 6,
        "x-contract-stream-item": "ApiEvent",
    }


def test_job_collection_detail_cancellation_etag_idempotency_and_cursor_are_exact() -> None:
    """@brief Job HTTP 语义绑定 schema、cursor、ETag 与幂等 / Job HTTP semantics bind schema, cursor, ETag, and idempotency."""

    with _harness() as harness:
        page = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs",
            headers=_headers(),
            params={"limit": 1},
        )
        assert page.status_code == 200
        harness.validator.validate_definition("JobList", page.json())
        assert page.json()["items"][0]["id"] == str(OTHER_JOB_ID)
        assert page.json()["page"]["has_more"] is True

        cursor = page.json()["page"]["next_cursor"]
        continued = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs",
            headers=_headers(),
            params={"limit": 1, "cursor": cursor},
        )
        assert continued.status_code == 200
        assert continued.json()["items"][0]["id"] == str(JOB_ID)

        rebound = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs",
            headers=_headers(),
            params={"limit": 1, "cursor": cursor, "kind": "resume.render"},
        )
        assert rebound.status_code == 400
        assert rebound.json()["code"] == "http.cursor_invalid"

        filtered = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs",
            headers=_headers(),
            params={
                "kind": "resume.render",
                "subject_type": "resume",
                "subject_id": RESUME_ID,
            },
        )
        assert filtered.status_code == 200
        assert [item["id"] for item in filtered.json()["items"]] == [str(JOB_ID)]

        detail = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs/{JOB_ID}",
            headers=_headers(),
        )
        assert detail.status_code == 200
        assert detail.headers["etag"].startswith('"sha256-')
        harness.validator.validate_definition("Job", detail.json())

        missing_precondition = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs/{JOB_ID}/cancellations",
            headers=_headers(idempotency_key="platform-cancel-key-0001"),
        )
        assert missing_precondition.status_code == 412
        assert missing_precondition.json()["code"] == "http.precondition_failed"

        denied = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs/{JOB_ID}/cancellations",
            headers=_headers(
                actor="narrow",
                idempotency_key="platform-cancel-key-0002",
                etag=detail.headers["etag"],
            ),
        )
        assert denied.status_code == 403
        assert denied.json()["code"] == "oauth.insufficient_scope"

        cancelled = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs/{JOB_ID}/cancellations",
            headers=_headers(
                actor="writer",
                idempotency_key="platform-cancel-key-0003",
                etag=detail.headers["etag"],
            ),
        )
        assert cancelled.status_code == 200
        assert cancelled.json()["status"] == "cancelled"
        assert cancelled.json()["revision"] == 2
        assert cancelled.headers["etag"].startswith('"sha256-')
        harness.validator.validate_definition("Job", cancelled.json())

        replay = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs/{JOB_ID}/cancellations",
            headers=_headers(
                actor="writer",
                request_id="request_platform_http_000002",
                idempotency_key="platform-cancel-key-0003",
                etag=detail.headers["etag"],
            ),
        )
        assert replay.status_code == cancelled.status_code
        assert replay.content == cancelled.content
        assert replay.headers["etag"] == cancelled.headers["etag"]
        assert replay.headers["X-Request-Id"] == "request_platform_http_000002"

        assert harness.store.jobs[JOB_ID].status.value == "cancelled"
        assert any(event.action == "job.cancel" for event in harness.store.audit_events.values())


def test_artifact_metadata_source_map_and_binary_range_follow_the_contract() -> None:
    """@brief Artifact metadata、source map 与 binary Range 符合契约 / Artifact metadata, source map, and binary Range follow the contract."""

    with _harness() as harness:
        collection = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/artifacts",
            headers=_headers(),
            params={
                "kind": "resume_pdf",
                "subject_type": "resume",
                "subject_id": RESUME_ID,
            },
        )
        assert collection.status_code == 200
        harness.validator.validate_definition("ArtifactList", collection.json())
        assert [item["id"] for item in collection.json()["items"]] == [str(ARTIFACT_ID)]

        detail = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/artifacts/{ARTIFACT_ID}",
            headers=_headers(),
        )
        assert detail.status_code == 200
        assert detail.headers["etag"].startswith('"sha256-')
        assert detail.json()["content_url"].endswith(f"/{ARTIFACT_ID}/content")
        harness.validator.validate_definition("Artifact", detail.json())

        source_map = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/artifacts/{ARTIFACT_ID}/source-map",
            headers=_headers(),
        )
        assert source_map.status_code == 200
        assert source_map.json()["nodes"][0]["rects"][0]["unit"] == "pt"
        harness.validator.validate_definition("PdfSourceMap", source_map.json())

        full = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/artifacts/{ARTIFACT_ID}/content",
            headers=_headers(),
        )
        assert full.status_code == 200
        assert full.content == CONTENT
        assert full.headers["content-type"] == "application/pdf"
        assert full.headers["content-length"] == str(len(CONTENT))
        assert full.headers["accept-ranges"] == "bytes"
        assert full.headers["etag"] == f'"sha256-{CONTENT_SHA256}"'
        assert full.headers["content-disposition"] == (f'attachment; filename="{ARTIFACT_ID}.pdf"')

        selected = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/artifacts/{ARTIFACT_ID}/content",
            headers={**_headers(), "Range": "bytes=5-12"},
        )
        assert selected.status_code == 206
        assert selected.content == CONTENT[5:13]
        assert selected.headers["content-range"] == f"bytes 5-12/{len(CONTENT)}"
        assert selected.headers["content-length"] == "8"
        assert selected.headers["etag"] == full.headers["etag"]

        suffix = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/artifacts/{ARTIFACT_ID}/content",
            headers={**_headers(), "Range": "bytes=-4"},
        )
        assert suffix.status_code == 206
        assert suffix.content == CONTENT[-4:]

        unsatisfied = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/artifacts/{ARTIFACT_ID}/content",
            headers={**_headers(), "Range": f"bytes={len(CONTENT)}-"},
        )
        assert unsatisfied.status_code == 416
        assert unsatisfied.headers["content-range"] == f"bytes */{len(CONTENT)}"
        assert unsatisfied.headers["accept-ranges"] == "bytes"
        assert unsatisfied.json()["code"] == "http.range_not_satisfiable"
        harness.validator.validate_definition("ProblemDetails", unsatisfied.json())

        multiple = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/artifacts/{ARTIFACT_ID}/content",
            headers={**_headers(), "Range": "bytes=0-1,4-5"},
        )
        assert multiple.status_code == 400
        assert multiple.json()["code"] == "http.invalid_range"


def test_sse_audit_replay_errors_and_strict_read_boundaries_are_stable() -> None:
    """@brief SSE、audit、replay 失败与严格读边界保持稳定 / SSE, audit, replay failures, and strict read boundaries remain stable."""

    with _harness() as harness:
        stream = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/events",
            headers={**_headers(), "Last-Event-ID": "event_platform_http_000001"},
        )
        assert stream.status_code == 200
        assert stream.headers["content-type"].startswith("text/event-stream")
        assert stream.headers["cache-control"] == "no-cache"
        assert stream.headers["x-accel-buffering"] == "no"
        assert stream.text.startswith("id: event_platform_http_000002\nevent: job.updated\ndata: ")
        data_line = next(
            line.removeprefix("data: ")
            for line in stream.text.splitlines()
            if line.startswith("data: ")
        )
        payload = json.loads(data_line)
        assert payload["event_id"] == "event_platform_http_000002"
        assert payload["data"]["hints"] == ["poll"]
        harness.validator.validate_definition("ApiEvent", payload)

        expired = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/events",
            headers={**_headers(), "Last-Event-ID": "event_platform_http_missing"},
        )
        assert expired.status_code == 409
        assert expired.json()["code"] == "event.replay_window_expired"
        harness.validator.validate_definition("ProblemDetails", expired.json())

        audits = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/audit-events",
            headers=_headers(),
        )
        assert audits.status_code == 200
        harness.validator.validate_definition("AuditEventList", audits.json())
        assert audits.json()["items"][0]["action"] == "artifact.read"

        hidden = harness.client.get(
            f"/api/v2/workspaces/{OTHER_WORKSPACE_ID}/audit-events",
            headers=_headers(),
        )
        assert hidden.status_code == 404
        assert hidden.json()["code"] == "resource.not_found"

        unknown_query = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/audit-events",
            headers=_headers(),
            params={"offset": 0},
        )
        assert unknown_query.status_code == 400
        assert unknown_query.json()["code"] == "http.invalid_query"

        unexpected_body = harness.client.request(
            "GET",
            f"/api/v2/workspaces/{WORKSPACE_ID}/jobs",
            headers={**_headers(), "Content-Type": "application/json"},
            content="{}",
        )
        assert unexpected_body.status_code == 400
        assert unexpected_body.json()["code"] == "http.unexpected_body"
