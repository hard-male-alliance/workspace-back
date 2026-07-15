"""@brief 面试 REST、mock WebSocket 与报告生成的纵向测试 / Vertical tests for interview REST, mock WebSocket, and report generation."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, cast

from fastapi.testclient import TestClient

from backend.composition import BackendContainer
from backend.infrastructure.contracts import ContractValidator
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


def test_interview_rest_websocket_and_report_flow(
    backend_client: TestClient,
    contract_examples: dict[str, Any],
    contract_validator: ContractValidator,
) -> None:
    """@brief REST 创建/连接、WebSocket 双向事件和 REST 结束应生成正式报告 / REST create/connect, bidirectional WebSocket events, and REST end must produce a formal report.

    @param backend_client 已启动的后端 TestClient / Started backend TestClient.
    @param contract_examples 已发布的正式请求样例 / Published formal request examples.
    @param contract_validator 权威契约验证器 / Authoritative contract validator.
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

    with backend_client.websocket_connect(
        f"/api/v1/interview-sessions/{session_id}/realtime"
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

    end_response = backend_client.post(
        f"/api/v1/interview-sessions/{session_id}/end-requests",
        json={"reason": "normal"},
        headers=idempotency_headers("interview-end-flow-00000001"),
    )
    assert end_response.status_code == 202, end_response.text
    report_job = end_response.json()
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
