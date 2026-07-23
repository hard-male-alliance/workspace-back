"""@brief API V2 Interview HTTP 适配器测试 / API V2 Interview HTTP-adapter tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from backend.api.v2_http import CursorCodec
from backend.api.v2_interview import create_v2_interview_router
from backend.application.ports.access import UnknownPrincipal
from backend.application.ports.interview_v2 import InterviewPage, InterviewPageRequest
from backend.domain.interview_v2 import (
    ActionPriority,
    AvatarOutputMode,
    EndInterviewReason,
    EphemeralToken,
    FallbackTransport,
    IceServer,
    InterviewActionPlanItem,
    InterviewAvatarPreferences,
    InterviewCommunicationMetrics,
    InterviewDifficulty,
    InterviewEvidence,
    InterviewMediaPreferences,
    InterviewReport,
    InterviewReportDraft,
    InterviewReportId,
    InterviewRichText,
    InterviewRubric,
    InterviewScenario,
    InterviewScenarioId,
    InterviewScenarioSpec,
    InterviewScenarioStatus,
    InterviewSessionId,
    InterviewSessionStatus,
    InterviewSessionView,
    JobTarget,
    RealtimeConnection,
    RealtimeConnectionId,
    RealtimeTransport,
    RecordingConsent,
    RubricDimension,
    RubricScore,
    ScoreScale,
    TranscriptSegment,
    TranscriptSegmentId,
    TranscriptSpeaker,
)
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM
from backend.domain.platform import Job, JobId
from backend.domain.principals import ResourceMeta, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.v2_idempotency import (
    InMemoryIdempotencyExecutor,
    InMemoryV2IdempotencyStore,
)
from backend.package_resources import read_contract_schema_text

NOW = datetime(2026, 7, 23, 5, 0, tzinfo=UTC)
WORKSPACE = WorkspaceId("workspace_http_0001")
SCENARIO_ID = InterviewScenarioId("scenario_http_0001")
SESSION_ID = InterviewSessionId("session_http_0001")
REPORT_ID = InterviewReportId("report_http_0001")


def _rubric() -> InterviewRubric:
    """@brief 构造最小合法 Rubric / Build a minimal valid Rubric."""
    return InterviewRubric(
        "rubric_http_0001",
        "1",
        "System design",
        (
            RubricDimension(
                "dimension_http_0001",
                "Consistency",
                "Explain consistency trade-offs",
                1,
                ("Defines linearizability",),
                ScoreScale(0, 100, {"80": "strong"}),
            ),
        ),
        ScoreScale(0, 100),
    )


def _scenario() -> InterviewScenario:
    """@brief 构造 active Scenario / Build an active Scenario."""
    return InterviewScenario(
        ResourceMeta(SCENARIO_ID, 2, NOW, NOW),
        WORKSPACE,
        InterviewScenarioSpec(
            "Distributed systems",
            "A systems interview",
            "zh-CN",
            "system_design",
            InterviewDifficulty.ADVANCED,
            45,
            8,
            ("consistency",),
            True,
            True,
            _rubric(),
        ),
        InterviewScenarioStatus.ACTIVE,
    )


def _media() -> InterviewMediaPreferences:
    """@brief 构造 media preferences / Build media preferences."""
    return InterviewMediaPreferences(
        True,
        False,
        False,
        1920,
        1080,
        30,
        InterviewAvatarPreferences(
            AvatarOutputMode.AUDIO_ONLY,
            None,
            "voice_http_0001",
            ("opus",),
            (),
            False,
            False,
        ),
        FallbackTransport.WEBSOCKET,
    )


def _recording() -> RecordingConsent:
    """@brief 构造显式 Transcript consent / Build explicit Transcript consent."""
    return RecordingConsent(False, False, True, 30, NOW, "consent-http-1")


def _target() -> JobTarget:
    """@brief 构造 JobTarget / Build a JobTarget."""
    return JobTarget(
        "Senior Engineer",
        "HM Alliances",
        "Singapore",
        "Build reliable systems",
        "https://example.com/jobs/1",
        "senior",
        ("distributed-systems",),
    )


def _session() -> InterviewSessionView:
    """@brief 构造 active Session 投影 / Build an active Session projection."""
    return InterviewSessionView(
        ResourceMeta(SESSION_ID, 3, NOW, NOW),
        WORKSPACE,
        SCENARIO_ID,
        ResourceRef("resume", "resume_http_0001", 4),
        _target(),
        InterviewSessionStatus.ACTIVE,
        "zh-CN",
        _media(),
        _recording(),
        NOW,
        None,
        None,
    )


def _segment() -> TranscriptSegment:
    """@brief 构造 Transcript segment / Build a Transcript segment."""
    return TranscriptSegment(
        TranscriptSegmentId("segment_http_0001"),
        WORKSPACE,
        SESSION_ID,
        7,
        ResourceRef("realtime_input", "input_http_0001"),
        TranscriptSpeaker.CANDIDATE,
        100,
        900,
        "Linearizability defines a single global order.",
    )


def _report() -> InterviewReport:
    """@brief 构造带精确 evidence 的 Report / Build a Report with exact evidence."""
    segment = _segment()
    draft = InterviewReportDraft(
        "1",
        _rubric().rubric_id,
        _rubric().rubric_version,
        "engine-http-1",
        84,
        0.9,
        InterviewRichText("Strong systems reasoning."),
        (
            RubricScore(
                "dimension_http_0001",
                84,
                0.9,
                InterviewRichText("Clear consistency model."),
                (
                    InterviewEvidence(
                        segment.id,
                        segment.start_ms,
                        segment.end_ms,
                        "single global order",
                    ),
                ),
                ("Quantify availability trade-offs",),
            ),
        ),
        (InterviewRichText("Clear structure."),),
        (InterviewRichText("Add failure analysis."),),
        InterviewCommunicationMetrics(800, 800, 120, 0, 0, 0, ()),
        (
            InterviewActionPlanItem(
                ActionPriority.HIGH,
                "Practice failure modes",
                "Improve completeness",
                "Run a fault-injection drill",
                "Identify three failure domains",
            ),
        ),
        ("Single short interview",),
    )
    return InterviewReport(
        ResourceMeta(REPORT_ID, 1, NOW, NOW),
        WORKSPACE,
        SESSION_ID,
        draft,
        NOW,
    )


def _job() -> Job:
    """@brief 构造 queued unified Job / Build a queued unified Job."""
    return Job(
        ResourceMeta(JobId("job_http_0001"), 1, NOW, NOW),
        WORKSPACE,
        "interview.report",
        ResourceRef("interview_session", SESSION_ID, 3),
    )


class _Service:
    """@brief 记录 12 个 HTTP 用例调用的 fake service / Fake service recording the twelve HTTP use cases."""

    def __init__(self) -> None:
        self.scenario = _scenario()
        self.session = _session()
        self.report = _report()
        self.job = _job()
        self.calls: list[str] = []
        self.connection_calls = 0
        self.session_calls = 0
        self.last_after: str | None = None
        self.reject_session = False

    async def list_scenarios(
        self, _principal: object, _workspace: object, page: InterviewPageRequest
    ) -> InterviewPage[InterviewScenario]:
        self.calls.append("list_scenarios")
        self.last_after = page.after
        return InterviewPage((self.scenario,), "scenario-next")

    async def create_scenario(self, *_args: object) -> InterviewScenario:
        self.calls.append("create_scenario")
        return self.scenario

    async def get_scenario(self, *_args: object) -> InterviewScenario:
        self.calls.append("get_scenario")
        return self.scenario

    async def get_scenario_for_update(self, *_args: object) -> InterviewScenario:
        return self.scenario

    async def update_scenario(self, *_args: object, **_kwargs: object) -> InterviewScenario:
        self.calls.append("update_scenario")
        return self.scenario

    async def list_sessions(self, *_args: object) -> InterviewPage[InterviewSessionView]:
        self.calls.append("list_sessions")
        return InterviewPage((self.session,), None)

    async def create_session(self, *_args: object) -> InterviewSessionView:
        self.calls.append("create_session")
        self.session_calls += 1
        if self.reject_session:
            raise UnknownPrincipal
        return self.session

    async def get_session(self, *_args: object) -> InterviewSessionView:
        self.calls.append("get_session")
        return self.session

    async def create_realtime_connection(self, *_args: object) -> RealtimeConnection:
        self.calls.append("create_realtime_connection")
        self.connection_calls += 1
        return RealtimeConnection(
            RealtimeConnectionId("connection_http_0001"),
            WORKSPACE,
            SESSION_ID,
            ResourceRef("user", "user_http_0001"),
            RealtimeTransport.WEBRTC,
            "wss://realtime.example.com/interview",
            EphemeralToken("ephemeral-secret-http-0001"),
            (IceServer(("turn:turn.example.com",), "turn-user", "turn-secret-http"),),
            NOW,
            NOW + timedelta(minutes=5),
            5_000,
        )

    async def get_session_for_end(self, *_args: object) -> InterviewSessionView:
        return self.session

    async def create_end_request(self, *_args: object, **_kwargs: object) -> Job:
        self.calls.append("create_end_request")
        return self.job

    async def get_transcript(self, *_args: object) -> InterviewPage[TranscriptSegment]:
        self.calls.append("get_transcript")
        return InterviewPage((_segment(),), None)

    async def create_report_job(self, *_args: object) -> Job:
        self.calls.append("create_report_job")
        return self.job

    async def get_report(self, *_args: object) -> InterviewReport:
        self.calls.append("get_report")
        return self.report


@dataclass(slots=True)
class _Runtime:
    """@brief Interview HTTP 测试 runtime / Interview HTTP test runtime."""

    interview_v2: _Service
    contracts_v2: ContractValidator
    v2_cursor: CursorCodec
    v2_idempotency: InMemoryIdempotencyExecutor
    sensitive_idempotency_key: bytes


@dataclass(slots=True)
class _Harness:
    """@brief 组合 client、service 与幂等 store / Bundle client, service, and idempotency store."""

    client: TestClient
    service: _Service
    store: InMemoryV2IdempotencyStore


def _harness() -> _Harness:
    """@brief 创建隔离 FastAPI harness / Create an isolated FastAPI harness."""
    service = _Service()
    store = InMemoryV2IdempotencyStore()
    runtime = _Runtime(
        service,
        ContractValidator.from_jsonc(read_contract_schema_text("v2")),
        CursorCodec(b"interview-http-cursor-secret-0000001"),
        InMemoryIdempotencyExecutor(store, retention=timedelta(days=2)),
        b"interview-sensitive-idempotency-key-0000001",
    )
    app = FastAPI()
    app.include_router(create_v2_interview_router(lambda _request: runtime))

    @app.middleware("http")
    async def verified_context(request: Request, call_next: object) -> object:
        request.state.request_id = request.headers.get("X-Request-Id", "request_http_0001")
        request.state.oauth_claims = {
            ACCESS_TOKEN_USER_ID_CLAIM: "user_http_0001",
            "sub": "subject_http_0001",
            "client_id": "client_http_0001",
            "scope": "interview.read interview.write",
        }
        return await call_next(request)  # type: ignore[operator]

    return _Harness(TestClient(app, raise_server_exceptions=False), service, store)


def _headers(
    *, key: str | None = None, etag: str | None = None, patch: bool = False
) -> dict[str, str]:
    """@brief 构造协议 headers / Build protocol headers."""
    headers = {"X-Request-Id": "request_http_0001"}
    if key is not None:
        headers["Idempotency-Key"] = key
    if etag is not None:
        headers["If-Match"] = etag
    if patch:
        headers["Content-Type"] = "application/merge-patch+json"
    return headers


def _scenario_body() -> dict[str, object]:
    """@brief 构造契约 Scenario body / Build a contract Scenario body."""
    return {
        "name": "Distributed systems",
        "description": "A systems interview",
        "locale": "zh-CN",
        "interview_type": "system_design",
        "difficulty": "advanced",
        "duration_minutes": 45,
        "target_question_count": 8,
        "focus_areas": ["consistency"],
        "allow_followups": True,
        "allow_barge_in": True,
        "rubric": {
            "rubric_id": "rubric_http_0001",
            "rubric_version": "1",
            "name": "System design",
            "dimensions": [
                {
                    "dimension_id": "dimension_http_0001",
                    "name": "Consistency",
                    "description": "Explain consistency trade-offs",
                    "weight": 1,
                    "observable_indicators": ["Defines linearizability"],
                    "scoring_scale": {"minimum": 0, "maximum": 100},
                }
            ],
            "overall_scale": {"minimum": 0, "maximum": 100},
        },
    }


def _session_body() -> dict[str, object]:
    """@brief 构造含 consent 的 Session body / Build a Session body containing consent."""
    return {
        "scenario_id": str(SCENARIO_ID),
        "resume_ref": {"resource_type": "resume", "id": "resume_http_0001", "revision": 4},
        "job_target": {
            "title": "Senior Engineer",
            "company": "HM Alliances",
            "location": "Singapore",
            "description": "Build reliable systems",
            "source_url": "https://example.com/jobs/1",
            "seniority": "senior",
            "skills": ["distributed-systems"],
        },
        "knowledge": {
            "mode": "none",
            "include_source_ids": [],
            "exclude_source_ids": [],
            "pinned_versions": [],
            "agent_scope": "interview_agent",
        },
        "locale": "zh-CN",
        "media": {
            "user_audio": True,
            "user_video": False,
            "screen_share": False,
            "max_video_width": 1920,
            "max_video_height": 1080,
            "max_video_fps": 30,
            "avatar": {
                "output_mode": "audio_only",
                "avatar_id": None,
                "voice_id": "voice_http_0001",
                "preferred_audio_codecs": ["opus"],
                "preferred_video_codecs": [],
                "include_visemes": False,
                "include_expression_cues": False,
            },
            "fallback_transport": "websocket",
        },
        "recording": {
            "record_audio": False,
            "record_video": False,
            "store_transcript": True,
            "retention_days": 30,
            "consented_at": "2026-07-23T05:00:00.000Z",
            "consent_version": "consent-http-1",
        },
        "inference": {
            "quality_tier": "balanced",
            "latency_budget_ms": 10000,
            "cost_tier": "standard",
            "data_region": "cn",
            "allow_provider_fallback": False,
            "allow_external_model_processing": False,
        },
    }


def test_router_registers_exactly_twelve_contract_routes() -> None:
    """@brief 路由表必须 12/12 且状态正确 / Route table is exactly 12/12 with correct statuses."""
    router = create_v2_interview_router(lambda _request: object())  # type: ignore[arg-type]
    routes = [route for route in router.routes if isinstance(route, APIRoute)]
    assert len(routes) == 12
    shapes = {(next(iter(route.methods)), route.path, route.status_code) for route in routes}
    assert ("POST", "/api/v2/workspaces/{workspace_id}/interview-scenarios", 201) in shapes
    assert (
        "POST",
        "/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/end-requests",
        202,
    ) in shapes
    assert (
        "POST",
        "/api/v2/workspaces/{workspace_id}/interview-sessions/{session_id}/report-jobs",
        202,
    ) in shapes


def test_all_twelve_routes_validate_and_project_exact_contract_shapes() -> None:
    """@brief 通过全部 12 条路由并校验敏感投影 / Exercise all twelve routes and validate sensitive projections."""
    harness = _harness()
    client = harness.client
    workspace = str(WORKSPACE)
    scenario = str(SCENARIO_ID)
    session = str(SESSION_ID)

    listed = client.get(f"/api/v2/workspaces/{workspace}/interview-scenarios")
    assert listed.status_code == 200
    cursor = listed.json()["page"]["next_cursor"]
    assert cursor
    assert (
        client.get(
            f"/api/v2/workspaces/{workspace}/interview-scenarios", params={"cursor": cursor}
        ).status_code
        == 200
    )
    assert harness.service.last_after == "scenario-next"

    created_scenario = client.post(
        f"/api/v2/workspaces/{workspace}/interview-scenarios",
        json=_scenario_body(),
        headers=_headers(key="scenario-create-0001"),
    )
    assert created_scenario.status_code == 201
    assert created_scenario.headers["location"].endswith(f"/interview-scenarios/{scenario}")
    fetched_scenario = client.get(f"/api/v2/workspaces/{workspace}/interview-scenarios/{scenario}")
    assert fetched_scenario.status_code == 200
    patched = client.patch(
        f"/api/v2/workspaces/{workspace}/interview-scenarios/{scenario}",
        json={"name": "Updated systems interview"},
        headers=_headers(etag=fetched_scenario.headers["etag"], patch=True),
    )
    assert patched.status_code == 200

    sessions = client.get(f"/api/v2/workspaces/{workspace}/interview-sessions")
    assert sessions.status_code == 200
    assert sessions.headers["cache-control"] == "private, no-store"
    created_session = client.post(
        f"/api/v2/workspaces/{workspace}/interview-sessions",
        json=_session_body(),
        headers=_headers(key="session-create-0001"),
    )
    assert created_session.status_code == 201
    assert created_session.headers["cache-control"] == "private, no-store"
    fetched_session = client.get(f"/api/v2/workspaces/{workspace}/interview-sessions/{session}")
    assert fetched_session.status_code == 200
    assert fetched_session.json()["recording"]["consent_version"] == "consent-http-1"

    connection = client.post(
        f"/api/v2/workspaces/{workspace}/interview-sessions/{session}/connections",
        json={"supported_transports": ["webrtc"], "audio_codecs": ["opus"], "video_codecs": []},
        headers=_headers(key="connection-create-0001"),
    )
    assert connection.status_code == 201
    assert connection.headers["cache-control"] == "private, no-store"
    assert connection.headers["pragma"] == "no-cache"
    assert connection.json()["ephemeral_token"] == "ephemeral-secret-http-0001"
    assert "audience" not in connection.json() and "issued_at" not in connection.json()

    end_job = client.post(
        f"/api/v2/workspaces/{workspace}/interview-sessions/{session}/end-requests",
        json={"reason": EndInterviewReason.COMPLETED.value},
        headers=_headers(key="session-end-000001", etag=fetched_session.headers["etag"]),
    )
    assert end_job.status_code == 202
    assert end_job.headers["location"].endswith("/jobs/job_http_0001")
    transcript = client.get(
        f"/api/v2/workspaces/{workspace}/interview-sessions/{session}/transcript"
    )
    assert transcript.status_code == 200
    assert transcript.headers["cache-control"] == "private, no-store"
    assert transcript.json()["items"][0] == {
        "id": "segment_http_0001",
        "speaker": "candidate",
        "start_ms": 100,
        "end_ms": 900,
        "text": "Linearizability defines a single global order.",
    }
    report_job = client.post(
        f"/api/v2/workspaces/{workspace}/interview-sessions/{session}/report-jobs",
        json={"rubric_version": "1"},
        headers=_headers(key="report-create-0001"),
    )
    assert report_job.status_code == 202
    report = client.get(f"/api/v2/workspaces/{workspace}/interview-reports/{REPORT_ID}")
    assert report.status_code == 200
    evidence = report.json()["rubric_scores"][0]["evidence"][0]
    assert evidence == {
        "segment_id": "segment_http_0001",
        "start_ms": 100,
        "end_ms": 900,
        "quote": "single global order",
    }
    assert report.headers["cache-control"] == "private, no-store"

    expected = {
        "list_scenarios",
        "create_scenario",
        "get_scenario",
        "update_scenario",
        "list_sessions",
        "create_session",
        "get_session",
        "create_realtime_connection",
        "create_end_request",
        "get_transcript",
        "create_report_job",
        "get_report",
    }
    assert expected <= set(harness.service.calls)


def test_sensitive_receipts_replay_byte_exactly_without_plaintext_at_rest() -> None:
    """@brief Consent 与 Realtime secret 可重放且 receipt 不含明文 / Consent and Realtime secrets replay without plaintext at rest."""
    harness = _harness()
    session_path = f"/api/v2/workspaces/{WORKSPACE}/interview-sessions"
    session_headers = _headers(key="session-replay-00001")
    first_session = harness.client.post(session_path, json=_session_body(), headers=session_headers)
    second_session = harness.client.post(
        session_path, json=_session_body(), headers=session_headers
    )
    path = f"/api/v2/workspaces/{WORKSPACE}/interview-sessions/{SESSION_ID}/connections"
    headers = _headers(key="connection-replay-0001")
    body = {"supported_transports": ["webrtc"], "audio_codecs": [], "video_codecs": []}
    first = harness.client.post(path, json=body, headers=headers)
    second = harness.client.post(path, json=body, headers=headers)

    assert first_session.status_code == second_session.status_code == 201
    assert first_session.content == second_session.content
    assert harness.service.session_calls == 1
    assert first.status_code == second.status_code == 201
    assert first.content == second.content
    assert first.headers["etag"] == second.headers["etag"]
    assert harness.service.connection_calls == 1
    records = repr(harness.store._records)
    assert "ephemeral-secret-http-0001" not in records
    assert "turn-secret-http" not in records
    assert "consent-http-1" not in records


def test_sensitive_failure_keeps_bearer_challenge_and_no_store_policy() -> None:
    """@brief 加密失败 receipt 仍保留 Bearer challenge 与 no-store / Encrypted failures retain the Bearer challenge and no-store."""
    harness = _harness()
    harness.service.reject_session = True
    response = harness.client.post(
        f"/api/v2/workspaces/{WORKSPACE}/interview-sessions",
        json=_session_body(),
        headers=_headers(key="session-rejected-0001"),
    )

    assert response.status_code == 401
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["www-authenticate"].startswith("Bearer resource_metadata=")


def test_strict_query_body_schema_idempotency_and_if_match_boundaries() -> None:
    """@brief 严格拒绝未知 query、GET body、非法 schema、缺 key 与 stale ETag / Strictly reject invalid transport shapes."""
    harness = _harness()
    client = harness.client
    base = f"/api/v2/workspaces/{WORKSPACE}"

    assert client.get(f"{base}/interview-scenarios?unknown=1").status_code == 400
    assert (
        client.request(
            "GET", f"{base}/interview-scenarios/{SCENARIO_ID}", content=b"{}"
        ).status_code
        == 400
    )
    assert client.post(f"{base}/interview-scenarios", json=_scenario_body()).status_code == 400
    invalid = _scenario_body()
    invalid["unexpected"] = True
    assert (
        client.post(
            f"{base}/interview-scenarios",
            json=invalid,
            headers=_headers(key="invalid-schema-0001"),
        ).status_code
        == 422
    )

    assert (
        client.patch(
            f"{base}/interview-scenarios/{SCENARIO_ID}",
            json={"name": "No precondition"},
            headers=_headers(patch=True),
        ).status_code
        == 412
    )
    assert (
        client.post(
            f"{base}/interview-sessions/{SESSION_ID}/end-requests",
            json={"reason": "completed"},
            headers=_headers(key="missing-etag-0001"),
        ).status_code
        == 412
    )
