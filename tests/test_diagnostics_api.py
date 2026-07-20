"""@brief 浏览器诊断 HTTP 边界测试 / Browser diagnostics HTTP-boundary tests."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from backend.domain.observability import LogEvent, MetricPoint, SpanEvent
from backend.infrastructure.observability.pipeline import InMemoryTelemetryWriter


def _base_event(event_type: str, identifier: str) -> dict[str, object]:
    """@brief 构造诊断公共字段 / Build common diagnostic fields.

    @param event_type 判别字段 / Discriminator.
    @param identifier 客户端幂等 ID / Client idempotency ID.
    @return 公共请求对象 / Common request object.
    """
    return {
        "event_type": event_type,
        "client_event_id": identifier,
        "occurred_at": datetime.now(UTC).isoformat(),
        "route": "/resumes/{resume_id}",
        "release": "1.2.3",
    }


def test_diagnostics_accepts_strict_union_and_network_status_zero(
    backend_client: TestClient,
) -> None:
    """@brief 合法三类事件与网络 status=0 返回 202 / Valid union members including network status 0 return 202.

    @param backend_client 完整后端客户端 / Full backend client.
    """
    error_event = {
        **_base_event("error", "client-error-1"),
        "error_code": "ui.render.failed",
        "stack_fingerprint": "a" * 32,
    }
    performance_event = {
        **_base_event("performance", "client-performance-1"),
        "metric_name": "largest_contentful_paint",
        "value": 1200.5,
        "unit": "ms",
    }
    network_event = {
        **_base_event("network", "client-network-1"),
        "operation": "fetch",
        "duration_ms": 500.0,
        "status_code": 0,
    }
    payload = json.dumps(
        {"events": [error_event, performance_event, network_event]},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    response = backend_client.post(
        "/api/v1/diagnostics",
        content=payload,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 202, response.text
    assert response.json() == {"accepted": 3, "dropped": 0}
    assert response.headers["traceparent"].startswith("00-")

    request_id = response.headers["X-Request-Id"]
    writer = backend_client.app.state.container.telemetry_writer
    assert isinstance(writer, InMemoryTelemetryWriter)
    self_metrics: list[MetricPoint] = []
    for _attempt in range(50):
        self_metrics = [
            signal
            for signal in writer.snapshot()
            if isinstance(signal, MetricPoint)
            and signal.envelope.request_id == request_id
            and signal.envelope.name.startswith("aiws.diagnostics.ingest.")
        ]
        if len(self_metrics) == 4:
            break
        time.sleep(0.01)
    assert len(self_metrics) == 4
    assert all(
        metric.envelope.scope
        == backend_client.app.state.container.settings.default_scope
        for metric in self_metrics
    )
    assert all(
        set(metric.envelope.attributes) == {"operation", "outcome"}
        for metric in self_metrics
    )
    metrics_by_name = {metric.envelope.name: metric for metric in self_metrics}
    assert metrics_by_name["aiws.diagnostics.ingest.payload.size"].value == len(payload)
    assert metrics_by_name["aiws.diagnostics.ingest.payload.size"].unit == "By"
    assert metrics_by_name["aiws.diagnostics.ingest.batch.count"].value == 1
    assert metrics_by_name["aiws.diagnostics.ingest.event.count"].value == 3
    assert metrics_by_name["aiws.diagnostics.ingest.event.count"].envelope.attributes[
        "outcome"
    ] == "accepted"
    assert metrics_by_name["aiws.diagnostics.ingest.duration"].value >= 0


def test_diagnostics_rejects_sensitive_or_authority_fields(backend_client: TestClient) -> None:
    """@brief message/stack/service/scope 等未声明字段必须拒绝 / Sensitive and authority fields must be rejected.

    @param backend_client 完整后端客户端 / Full backend client.
    """
    event = {
        **_base_event("error", "client-error-forbidden"),
        "error_code": "ui.render.failed",
        "message": "token and user text must never cross this boundary",
        "service": "attacker.service",
    }
    response = backend_client.post("/api/v1/diagnostics", json={"events": [event]})
    assert response.status_code == 422
    assert response.json()["code"] == "diagnostics.invalid_payload"


def test_diagnostics_rejects_concrete_resource_path(backend_client: TestClient) -> None:
    """@brief 实际资源路径不得冒充低基数模板 / A concrete resource path cannot masquerade as a low-cardinality template.

    @param backend_client 完整后端客户端 / Full backend client.
    """
    event = {
        **_base_event("error", "client-error-concrete-route"),
        "route": "/resumes/res_customer_123456789",
        "error_code": "ui.render.failed",
    }
    response = backend_client.post("/api/v1/diagnostics", json={"events": [event]})
    assert response.status_code == 422
    assert response.json()["code"] == "diagnostics.invalid_payload"


def test_diagnostics_enforces_metric_unit_pairs_and_batch_uniqueness(
    backend_client: TestClient,
) -> None:
    """@brief CLS 单位与批内幂等 ID 必须严格 / CLS units and in-batch IDs are strict.

    @param backend_client 完整后端客户端 / Full backend client.
    """
    invalid = {
        **_base_event("performance", "duplicate-id"),
        "metric_name": "cumulative_layout_shift",
        "value": 0.1,
        "unit": "ms",
    }
    response = backend_client.post(
        "/api/v1/diagnostics", json={"events": [invalid, invalid]}
    )
    assert response.status_code == 422
    assert response.json()["code"] == "diagnostics.invalid_payload"


def test_diagnostics_rejects_raw_body_before_json_parsing(backend_client: TestClient) -> None:
    """@brief 超过 64 KiB 的原始 body 在 JSON 解析前返回 413 / A raw body over 64 KiB returns 413 before JSON parsing.

    @param backend_client 完整后端客户端 / Full backend client.
    """
    response = backend_client.post(
        "/api/v1/diagnostics",
        content=b"{" + b"x" * 70_000,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 413
    assert response.json()["code"] == "diagnostics.payload_too_large"


def test_diagnostics_records_rate_limited_event_count(backend_client: TestClient) -> None:
    """@brief 429 必须记录整批被限流事件数 / A 429 records the full rate-limited event count.

    @param backend_client 完整后端客户端 / Full backend client.
    """
    for batch_number in range(2):
        events = [
            {
                **_base_event("error", f"warmup-{batch_number}-{event_number}"),
                "error_code": "ui.render.failed",
            }
            for event_number in range(50)
        ]
        response = backend_client.post("/api/v1/diagnostics", json={"events": events})
        assert response.status_code == 202, response.text

    limited_events = [
        {
            **_base_event("error", f"rate-limited-{event_number}"),
            "error_code": "ui.render.failed",
        }
        for event_number in range(50)
    ]
    response = backend_client.post(
        "/api/v1/diagnostics", json={"events": limited_events}
    )
    assert response.status_code == 429, response.text
    assert int(response.headers["Retry-After"]) >= 1

    request_id = response.headers["X-Request-Id"]
    writer = backend_client.app.state.container.telemetry_writer
    assert isinstance(writer, InMemoryTelemetryWriter)
    rate_limited_metric: MetricPoint | None = None
    for _attempt in range(50):
        rate_limited_metric = next(
            (
                signal
                for signal in writer.snapshot()
                if isinstance(signal, MetricPoint)
                and signal.envelope.request_id == request_id
                and signal.envelope.name == "aiws.diagnostics.ingest.event.count"
                and signal.envelope.attributes["outcome"] == "rate_limited"
            ),
            None,
        )
        if rate_limited_metric is not None:
            break
        time.sleep(0.01)
    assert rate_limited_metric is not None
    assert rate_limited_metric.value == 50
    assert (
        rate_limited_metric.envelope.scope
        == backend_client.app.state.container.settings.default_scope
    )


def test_invalid_request_id_early_return_is_finalized_exactly_once(
    backend_client: TestClient,
) -> None:
    """@brief 身份前早退仍恰好产生一组 traffic/duration/span / A pre-identity return emits exactly one traffic/duration/span set.

    @param backend_client 完整后端客户端 / Full backend client.
    """
    response = backend_client.get(
        "/api/v1/resumes", headers={"X-Request-Id": "x" * 129}
    )
    assert response.status_code == 400
    request_id = response.headers["X-Request-Id"]
    writer = backend_client.app.state.container.telemetry_writer
    assert isinstance(writer, InMemoryTelemetryWriter)
    matching: list[MetricPoint | SpanEvent] = []
    for _attempt in range(50):
        matching = [
            signal
            for signal in writer.snapshot()
            if isinstance(signal, (MetricPoint, SpanEvent))
            and signal.envelope.request_id == request_id
        ]
        if len(matching) >= 5:
            break
        time.sleep(0.01)
    names = [signal.envelope.name for signal in matching]
    assert names.count("aiws.http.server.request.count") == 1
    assert names.count("http.server.request.duration") == 1
    assert names.count("http.server.request") == 1
    assert names.count("aiws.http.server.error.count") == 0
    duration = next(
        signal
        for signal in matching
        if isinstance(signal, MetricPoint)
        and signal.envelope.name == "http.server.request.duration"
    )
    assert duration.envelope.attributes == {
        "http.request.method": "GET",
        "http.response.status_code": 400,
        "http.route": "pre_auth",
        "outcome": "client_error",
        "url.scheme": "http",
    }


def test_unexpected_error_response_preserves_correlation_headers(
    backend_client: TestClient,
) -> None:
    """@brief 未处理 500 仍返回请求与 trace 关联头 / An unhandled 500 still returns request and trace correlation headers.

    @param backend_client 完整后端客户端 / Full backend client.
    """

    async def fail_unexpectedly() -> None:
        """@brief 触发外层 ServerErrorMiddleware / Trigger the outer ServerErrorMiddleware.

        @raise RuntimeError 始终抛出测试异常 / Always raises the test exception.
        """
        raise RuntimeError("synthetic unexpected failure")

    backend_client.app.add_api_route(
        "/api/v1/__test_unexpected_error",
        fail_unexpectedly,
        methods=["GET"],
        include_in_schema=False,
    )
    response = backend_client.get(
        "/api/v1/__test_unexpected_error",
        headers={"X-Request-Id": "req-unexpected-correlation"},
    )
    assert response.status_code == 500
    assert response.headers["X-Request-Id"] == "req-unexpected-correlation"
    assert response.headers["traceparent"].startswith("00-")
    expected_trace_id = response.headers["traceparent"].split("-")[1]
    writer = backend_client.app.state.container.telemetry_writer
    assert isinstance(writer, InMemoryTelemetryWriter)
    matching: list[LogEvent] = []
    for _attempt in range(50):
        matching = [
            signal
            for signal in writer.snapshot()
            if isinstance(signal, LogEvent)
            and signal.envelope.name == "backend.http.unexpected_error"
            and signal.envelope.request_id == "req-unexpected-correlation"
        ]
        if matching:
            break
        time.sleep(0.01)
    assert len(matching) == 1
    assert matching[0].envelope.trace_id == expected_trace_id
    assert (
        matching[0].envelope.scope
        == backend_client.app.state.container.settings.default_scope
    )
