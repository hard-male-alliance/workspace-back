"""@brief HTTP 边界输入长度与字符集测试 / HTTP-boundary input length and character-set tests."""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_request_and_idempotency_headers_fail_before_persistence(backend_client: TestClient) -> None:
    """@brief 不安全关联/幂等 header 必须返回 400 而非数据库错误 / Unsafe correlation/idempotency headers return 400 instead of database errors.

    @param backend_client 已启动的 backend 测试客户端 / Started backend test client.
    """
    invalid_request_id = backend_client.get(
        "/api/v1/resumes", headers={"X-Request-Id": "x" * 129}
    )
    assert invalid_request_id.status_code == 400
    assert invalid_request_id.json()["code"] == "http.invalid_request_id"

    invalid_key = backend_client.post(
        "/api/v1/resumes",
        json={"title": "边界测试", "locale": "zh-CN"},
        headers={"Idempotency-Key": "x" * 257},
    )
    assert invalid_key.status_code == 400
    assert invalid_key.json()["code"] == "http.invalid_idempotency_key"
