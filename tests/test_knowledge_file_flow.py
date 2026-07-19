"""Vertical tests for private file ingestion, versioning, and cited retrieval."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from backend.infrastructure.contracts import ContractValidator
from conftest import idempotency_headers, wait_for_json


def _search_request(query: str, source_id: str) -> dict[str, Any]:
    return {
        "query": query,
        "top_k": 5,
        "selection": {
            "mode": "explicit",
            "include_source_ids": [source_id],
            "exclude_source_ids": [],
            "pinned_versions": [],
            "agent_scope": "resume_assistant",
        },
        "include_quotes": True,
    }


def _upload_markdown(
    client: TestClient,
    *,
    key: str,
    content: str,
    path: str = "/api/v1/knowledge-sources/uploads",
) -> dict[str, Any]:
    response = client.post(
        path,
        files={"file": ("career.md", content.encode(), "text/markdown")},
        data={"name": "职业证据"},
        headers=idempotency_headers(key),
    )
    assert response.status_code == 202, response.text
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def test_markdown_upload_is_private_indexed_and_cited(
    backend_client: TestClient,
    contract_validator: ContractValidator,
) -> None:
    payload = _upload_markdown(
        backend_client,
        key="knowledge-file-upload-0001",
        content="# 平台项目\n\n我使用 kubernetes 构建了可观测的发布流水线。",
    )
    source = payload["source"]
    job_id = payload["ingestion_job"]["id"]
    assert source["source_type"] == "file"
    assert source["config"]["filename"] == "career.md"
    assert "storage_key" not in str(source)
    contract_validator.validate("KnowledgeSource", source)

    job = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-ingestion-jobs/{job_id}",
        lambda value: value["status"] in {"succeeded", "failed"},
    )
    assert job["status"] == "succeeded", job
    ready = backend_client.get(f"/api/v1/knowledge-sources/{source['id']}")
    assert ready.status_code == 200, ready.text
    assert ready.json()["ingestion"]["status"] == "ready"
    contract_validator.validate("KnowledgeSource", ready.json())

    search = backend_client.post(
        "/api/v1/knowledge-searches",
        json=_search_request("kubernetes", source["id"]),
    )
    assert search.status_code == 200, search.text
    result = search.json()["items"][0]
    assert "kubernetes" in result["text"]
    assert result["citation"]["source_id"] == source["id"]
    assert result["citation"]["locator"]["symbol"] == "平台项目"
    assert result["citation"]["locator"]["line_start"] == 3


def test_file_reupload_keeps_source_id_and_creates_new_current_version(
    backend_client: TestClient,
) -> None:
    created = _upload_markdown(
        backend_client,
        key="knowledge-file-version-create-0001",
        content="# 技能\n\nPython API 开发",
    )
    source_id = created["source"]["id"]
    first_job = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-ingestion-jobs/{created['ingestion_job']['id']}",
        lambda value: value["status"] in {"succeeded", "failed"},
    )
    assert first_job["status"] == "succeeded", first_job
    first_version = first_job["source_version_id"]

    replaced = _upload_markdown(
        backend_client,
        key="knowledge-file-version-replace-0001",
        content="# 技能\n\nPostgreSQL 与 pgvector 检索",
        path=f"/api/v1/knowledge-sources/{source_id}/versions",
    )
    assert replaced["source"]["id"] == source_id
    second_job = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-ingestion-jobs/{replaced['ingestion_job']['id']}",
        lambda value: value["status"] in {"succeeded", "failed"},
    )
    assert second_job["status"] == "succeeded", second_job
    assert second_job["source_version_id"] != first_version

    search = backend_client.post(
        "/api/v1/knowledge-searches",
        json=_search_request("pgvector", source_id),
    )
    assert search.status_code == 200, search.text
    assert "pgvector" in search.json()["items"][0]["text"]


def test_upload_rejects_extension_and_mime_mismatch(backend_client: TestClient) -> None:
    response = backend_client.post(
        "/api/v1/knowledge-sources/uploads",
        files={"file": ("notes.pdf", b"plain text", "text/plain")},
        headers=idempotency_headers("knowledge-file-mismatch-0001"),
    )
    assert response.status_code == 422, response.text
    assert response.json()["code"] == "knowledge.file_type_mismatch"
