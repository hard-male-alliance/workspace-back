"""@brief 面试 REST、mock WebSocket 与报告生成的纵向测试 / Vertical tests for interview REST, mock WebSocket, and report generation."""

from __future__ import annotations

import time
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from backend.composition import BackendContainer
from backend.domain.observability import MetricPoint, SpanEvent
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.observability.pipeline import InMemoryTelemetryWriter
from conftest import idempotency_headers, wait_for_json


def _timestamp() -> str:
    """@brief 生成正式契约接受的 UTC 时间戳 / Produce a UTC timestamp accepted by the formal contract.

    @return RFC 3339 UTC timestamp / RFC 3339 UTC timestamp.
    """

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _realtime_event(
    *,
    event_id: str,
    event_type: str,
    session_id: str,
    sequence: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """@brief 构建正式 WebSocket 控制事件 / Build a formal WebSocket control event.

    @param event_id 客户端事件不透明 ID / Opaque client event ID.
    @param event_type 已声明的事件类型 / Declared event type.
    @param session_id 面试 session ID / Interview session ID.
    @param sequence 客户端顺序号 / Client sequence number.
    @param payload 对应 event 的正式载荷 / Formal payload for the event.
    @return InterviewRealtimeEvent 对象 / InterviewRealtimeEvent object.
    """

    return {
        "protocol_version": "1.0",
        "event_id": event_id,
        "event_type": event_type,
        "session_id": session_id,
        "sequence": sequence,
        "ack_sequence": None,
        "occurred_at": _timestamp(),
        "trace_id": None,
        "payload": payload,
        "extensions": {},
    }


def test_interview_scenario_and_session_collections(
    backend_client: TestClient,
    contract_examples: dict[str, Any],
    contract_validator: ContractValidator,
) -> None:
    """Scenario discovery and scoped session recovery are available over HTTP."""
    scenarios_response = backend_client.get("/api/v1/interview-scenarios?limit=20")
    assert scenarios_response.status_code == 200, scenarios_response.text
    scenarios = scenarios_response.json()
    assert scenarios["items"]
    scenario = scenarios["items"][0]
    contract_validator.validate_definition("InterviewScenario", scenario)

    scenario_response = backend_client.get(
        f"/api/v1/interview-scenarios/{scenario['id']}"
    )
    assert scenario_response.status_code == 200, scenario_response.text
    assert scenario_response.json() == scenario

    application = cast(Any, backend_client.app)
    container = cast(BackendContainer, application.state.container)
    request = deepcopy(contract_examples["interview_create_request"])
    request["workspace_id"] = container.settings.default_scope.workspace_id
    request["scenario_id"] = scenario["id"]
    request["resume_ref"] = None
    created = backend_client.post(
        "/api/v1/interview-sessions",
        json=request,
        headers=idempotency_headers("interview-list-create-0001"),
    )
    assert created.status_code == 201, created.text

    collection = backend_client.get("/api/v1/interview-sessions?limit=20")
    assert collection.status_code == 200, collection.text
    items = collection.json()["items"]
    assert [item["id"] for item in items] == [created.json()["id"]]
    contract_validator.validate("InterviewSession", items[0])


def test_interview_rest_websocket_and_report_flow(
    backend_client: TestClient,
    contract_examples: dict[str, Any],
    contract_validator: ContractValidator,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief REST 创建/连接、WebSocket 双向事件和 REST 结束应生成正式报告 / REST create/connect, bidirectional WebSocket events, and REST end must produce a formal report.

    @param backend_client 已启动的后端 TestClient / Started backend TestClient.
    @param contract_examples 已发布的正式请求样例 / Published formal request examples.
    @param contract_validator 权威契约验证器 / Authoritative contract validator.
    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    """

    application = cast(Any, backend_client.app)
    container = cast(BackendContainer, application.state.container)
    request = deepcopy(contract_examples["interview_create_request"])
    request["workspace_id"] = container.settings.default_scope.workspace_id
    request["resume_ref"] = None
    contract_validator.validate("InterviewSessionCreateRequest", request)

    create_response = backend_client.post(
        "/api/v1/interview-sessions",
        json=request,
        headers=idempotency_headers("interview-create-flow-0001"),
    )
    assert create_response.status_code == 201, create_response.text
    session = create_response.json()
    contract_validator.validate("InterviewSession", session)
    session_id = session["id"]
    assert session["status"] == "created"

    connection_response = backend_client.post(
        f"/api/v1/interview-sessions/{session_id}/connections",
        headers=idempotency_headers("interview-connection-flow-1"),
    )
    assert connection_response.status_code == 200, connection_response.text
    connection = connection_response.json()
    contract_validator.validate("RealtimeConnectionDescriptor", connection)
    assert connection["session_id"] == session_id
    assert connection["fallback"]["websocket_url"].endswith(
        f"/interview-sessions/{session_id}/realtime"
    )

    websocket_request_id = "req-websocket-flow-0001"
    websocket_trace_id = "1" * 32
    websocket_parent_span_id = "2" * 16
    captured_contexts: list[tuple[str | None, str | None]] = []
    captured_jobs: list[Any] = []
    original_realtime = container.interview.handle_realtime_event
    original_end = container.interview._end_locked

    async def tracked_realtime(
        scope: object,
        target_session_id: str,
        event: dict[str, Any],
        *,
        request_id: str | None,
        trace_id: str | None,
    ) -> list[dict[str, Any]]:
        """@brief 捕获 route 向应用层传递的两类关联 ID / Capture both correlation IDs passed from route to application.

        @return 原服务响应 / Original service responses.
        """

        captured_contexts.append((request_id, trace_id))
        return await original_realtime(
            cast(Any, scope),
            target_session_id,
            event,
            request_id=request_id,
            trace_id=trace_id,
        )

    async def tracked_end(
        scope: object,
        session_record: object,
        request_id: str | None,
    ) -> Any:
        """@brief 捕获 WebSocket 结束事件创建的 Job / Capture the Job created by a WebSocket end event.

        @return 新建 Job / Newly created Job.
        """

        job = await original_end(
            cast(Any, scope),
            cast(Any, session_record),
            request_id,
        )
        captured_jobs.append(job)
        return job

    monkeypatch.setattr(container.interview, "handle_realtime_event", tracked_realtime)
    monkeypatch.setattr(container.interview, "_end_locked", tracked_end)

    with backend_client.websocket_connect(
        f"/api/v1/interview-sessions/{session_id}/realtime",
        headers={
            "X-Request-Id": websocket_request_id,
            "traceparent": (
                f"00-{websocket_trace_id}-{websocket_parent_span_id}-01"
            ),
        },
    ) as websocket:
        ready_event = _realtime_event(
            event_id="evt-ready-flow-000000001",
            event_type="interview.client.ready",
            session_id=session_id,
            sequence=0,
            payload={
                "client_time": _timestamp(),
                "media_ready": True,
                "last_received_sequence": None,
            },
        )
        contract_validator.validate("InterviewRealtimeEvent", ready_event)
        websocket.send_json(ready_event)
        ready_response = websocket.receive_json()
        contract_validator.validate("InterviewRealtimeEvent", ready_response)
        assert ready_response["event_type"] == "interview.session.state"
        assert ready_response["payload"]["status"] == "in_progress"
        assert ready_response["trace_id"] == websocket_trace_id

        sent_at = _timestamp()
        ping_event = _realtime_event(
            event_id="evt-ping-flow-0000000001",
            event_type="interview.ping",
            session_id=session_id,
            sequence=1,
            payload={"nonce": "klee-ping", "sent_at": sent_at},
        )
        contract_validator.validate("InterviewRealtimeEvent", ping_event)
        websocket.send_json(ping_event)
        pong_response = websocket.receive_json()
        contract_validator.validate("InterviewRealtimeEvent", pong_response)
        assert pong_response["event_type"] == "interview.pong"
        assert pong_response["payload"] == {"nonce": "klee-ping", "sent_at": sent_at}
        assert pong_response["trace_id"] == websocket_trace_id

        end_event = _realtime_event(
            event_id="evt-end-flow-00000000001",
            event_type="interview.session.end_requested",
            session_id=session_id,
            sequence=2,
            payload={"reason": "user_finished", "generate_report": True},
        )
        websocket.send_json(end_event)
        end_response = websocket.receive_json()
        contract_validator.validate("InterviewRealtimeEvent", end_response)
        assert end_response["event_type"] == "interview.session.state"
        assert end_response["trace_id"] == websocket_trace_id

    assert captured_contexts == [
        (websocket_request_id, websocket_trace_id),
        (websocket_request_id, websocket_trace_id),
        (websocket_request_id, websocket_trace_id),
    ]
    assert len(captured_jobs) == 1
    assert captured_jobs[0].request_id == websocket_request_id

    writer = container.telemetry_writer
    assert isinstance(writer, InMemoryTelemetryWriter)
    websocket_signals: list[MetricPoint | SpanEvent] = []
    for _attempt in range(50):
        websocket_signals = [
            signal
            for signal in writer.snapshot()
            if isinstance(signal, (MetricPoint, SpanEvent))
            and signal.envelope.name.startswith(
                ("aiws.websocket.server.", "websocket.server.")
            )
        ]
        if len(websocket_signals) >= 5:
            break
        time.sleep(0.01)
    websocket_names = [signal.envelope.name for signal in websocket_signals]
    assert websocket_names.count("aiws.websocket.server.connection.count") == 1
    assert websocket_names.count("aiws.websocket.server.connection.duration") == 1
    assert websocket_names.count("websocket.server.connection") == 1
    assert "aiws.websocket.server.error.count" not in websocket_names
    active_values = [
        signal.value
        for signal in websocket_signals
        if isinstance(signal, MetricPoint)
        and signal.envelope.name == "aiws.websocket.server.active_connections"
    ]
    assert active_values == [1.0, 0.0]
    terminal_signals = [
        signal
        for signal in websocket_signals
        if signal.envelope.name
        in {
            "aiws.websocket.server.connection.count",
            "aiws.websocket.server.connection.duration",
            "websocket.server.connection",
        }
    ]
    assert all(signal.envelope.attributes["close_code"] == 1000 for signal in terminal_signals)
    assert all(
        signal.envelope.scope == container.settings.default_scope
        for signal in terminal_signals
    )
    assert all(
        signal.envelope.scope is None
        for signal in websocket_signals
        if signal.envelope.name == "aiws.websocket.server.active_connections"
    )

    report_job = captured_jobs[0].as_dict()
    contract_validator.validate_definition("Job", report_job)
    assert report_job["job_type"] == "interview.report"

    completed_session = wait_for_json(
        backend_client,
        f"/api/v1/interview-sessions/{session_id}",
        lambda payload: payload["status"] in {"completed", "failed", "aborted"},
    )
    assert completed_session["status"] == "completed"
    contract_validator.validate("InterviewSession", completed_session)
    assert completed_session["report_id"] is not None

    report_response = backend_client.get(
        f"/api/v1/interview-reports/{completed_session['report_id']}"
    )
    assert report_response.status_code == 200, report_response.text
    report = report_response.json()
    contract_validator.validate("InterviewReport", report)
    assert report["session_id"] == session_id
    assert report["report_version"] == "mock-v1"
