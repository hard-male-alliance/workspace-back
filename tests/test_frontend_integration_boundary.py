"""Browser transport, collection pagination, and Resume discovery tests."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from fastapi.testclient import TestClient

from backend.infrastructure.contracts import ContractValidator
from conftest import idempotency_headers, wait_for_json


def test_development_cors_allows_only_configured_frontend_origin(
    backend_client: TestClient,
) -> None:
    """Configured Vite origins may preflight public headers without identity forgery."""
    response = backend_client.options(
        "/api/v1/resumes",
        headers={
            "Origin": "http://127.0.0.1:5173",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "If-Match,Idempotency-Key",
        },
    )
    assert response.status_code == 200, response.text
    assert response.headers["access-control-allow-origin"] == "http://127.0.0.1:5173"
    allowed_headers = response.headers["access-control-allow-headers"].lower()
    assert "if-match" in allowed_headers
    assert "idempotency-key" in allowed_headers

    rejected = backend_client.options(
        "/api/v1/resumes",
        headers={
            "Origin": "https://attacker.example",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert rejected.status_code == 400
    assert "access-control-allow-origin" not in rejected.headers


def test_knowledge_patch_preflight_and_new_collection_routes_are_discoverable(
    backend_client: TestClient,
) -> None:
    """The browser-facing additions must be present in both CORS and OpenAPI."""
    preflight = backend_client.options(
        "/api/v1/knowledge-sources/src_example",
        headers={
            "Origin": "http://127.0.0.1:5173",
            "Access-Control-Request-Method": "PATCH",
            "Access-Control-Request-Headers": "Content-Type,If-Match",
        },
    )
    assert preflight.status_code == 200, preflight.text
    assert preflight.headers["access-control-allow-origin"] == "http://127.0.0.1:5173"
    assert "patch" in preflight.headers["access-control-allow-methods"].lower()
    assert "if-match" in preflight.headers["access-control-allow-headers"].lower()

    paths = backend_client.get("/openapi.json").json()["paths"]
    expected_methods = {
        "/api/v1/me": "get",
        "/api/v1/workspaces": "get",
        "/api/v1/workspaces/{workspace_id}": "get",
        "/api/v1/workspaces/{workspace_id}/members": "get",
        "/api/v1/knowledge-sources/{source_id}": "patch",
        "/api/v1/interview-scenarios": "get",
        "/api/v1/interview-scenarios/{scenario_id}": "get",
        "/api/v1/interview-sessions": "get",
    }
    for path, method in expected_methods.items():
        assert method in paths[path]


def test_template_catalog_and_resume_cursor_pagination(
    backend_client: TestClient,
    contract_validator: ContractValidator,
) -> None:
    """Frontend collection endpoints expose formal manifests and opaque keyset cursors."""
    templates = backend_client.get("/api/v1/resume-templates", params={"locale": "zh-CN"})
    assert templates.status_code == 200, templates.text
    template_items = templates.json()["items"]
    assert len(template_items) == 1
    contract_validator.validate("TemplateManifest", template_items[0])

    detail = backend_client.get(
        "/api/v1/resume-templates/tpl_default_v1/versions/1.0"
    )
    assert detail.status_code == 200, detail.text
    assert detail.json() == template_items[0]

    for index in range(3):
        response = backend_client.post(
            "/api/v1/resumes",
            json={"title": f"Frontend pagination {index}", "locale": "zh-CN"},
            headers=idempotency_headers(f"frontend-page-create-{index:04d}"),
        )
        assert response.status_code == 201, response.text

    first = backend_client.get("/api/v1/resumes", params={"limit": 2})
    assert first.status_code == 200, first.text
    first_payload = first.json()
    assert len(first_payload["items"]) == 2
    assert first_payload["page"]["has_more"] is True
    assert isinstance(first_payload["page"]["next_cursor"], str)

    second = backend_client.get(
        "/api/v1/resumes",
        params={"limit": 2, "cursor": first_payload["page"]["next_cursor"]},
    )
    assert second.status_code == 200, second.text
    second_payload = second.json()
    assert len(second_payload["items"]) == 1
    assert second_payload["page"]["has_more"] is False
    assert {
        item["id"] for item in first_payload["items"]
    }.isdisjoint({item["id"] for item in second_payload["items"]})

    invalid = backend_client.get("/api/v1/resumes", params={"cursor": "not-a-cursor"})
    assert invalid.status_code == 400
    assert invalid.json()["code"] == "http.cursor_invalid"


def test_proposal_and_artifact_discovery_survive_page_navigation(
    backend_client: TestClient,
    contract_examples: dict[str, Any],
) -> None:
    """A frontend can rediscover pending proposals and completed PDF metadata by Resume ID."""
    visibility = {
        "policy_version": 1,
        "default_effect": "deny",
        "sensitivity": "confidential",
        "agent_grants": [
            {
                "agent_scope": "resume_assistant",
                "effect": "allow",
                "allowed_operations": ["retrieve", "derive"],
            }
        ],
        "session_override_allowed": False,
        "allow_external_model_processing": False,
        "allowed_model_regions": ["cn"],
        "retention_days": None,
    }
    source_response = backend_client.post(
        "/api/v1/knowledge-sources",
        json={
            "name": "Frontend proposal evidence",
            "source_type": "manual_note",
            "content": "负责可靠的后端服务建设",
            "visibility": visibility,
        },
        headers=idempotency_headers("frontend-evidence-create-0001"),
    )
    assert source_response.status_code == 201, source_response.text
    source = source_response.json()
    ingestion_response = backend_client.post(
        f"/api/v1/knowledge-sources/{source['id']}/ingestion-jobs",
        headers=idempotency_headers("frontend-evidence-ingest-0001"),
    )
    assert ingestion_response.status_code == 202, ingestion_response.text
    ingestion = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-ingestion-jobs/{ingestion_response.json()['id']}",
        lambda payload: payload["status"] in {"succeeded", "failed", "cancelled"},
    )
    assert ingestion["status"] == "succeeded"

    resume_response = backend_client.post(
        "/api/v1/resumes",
        json={"title": "Frontend discovery", "locale": "zh-CN"},
        headers=idempotency_headers("frontend-discovery-resume-0001"),
    )
    assert resume_response.status_code == 201, resume_response.text
    resume = resume_response.json()

    proposal_response = backend_client.post(
        f"/api/v1/resumes/{resume['id']}/proposals",
        json={
            "instruction": "更新职业摘要",
            "source_ids": [source["id"]],
            "draft_text": "负责可靠的后端服务建设",
            "render_hint": "preview",
        },
        headers=idempotency_headers("frontend-discovery-proposal-0001"),
    )
    assert proposal_response.status_code == 201, proposal_response.text
    proposal = proposal_response.json()

    proposal_list = backend_client.get(
        f"/api/v1/resumes/{resume['id']}/proposals",
        params={"status": "pending"},
    )
    assert proposal_list.status_code == 200, proposal_list.text
    assert [item["id"] for item in proposal_list.json()["items"]] == [proposal["id"]]

    fetched = backend_client.get(f"/api/v1/resumes/{resume['id']}")
    batch = deepcopy(contract_examples["resume_operation_batch"])
    batch["client_batch_id"] = "frontend-discovery-batch-0001"
    batch["base_revision"] = 1
    batch["operations"][0]["operation_id"] = "frontend-discovery-operation-0001"
    batch["operations"][0]["target"] = {"entity_type": "profile"}
    batch["operations"][0]["field_path"] = ["full_name"]
    batch["operations"][0]["value"] = "Frontend User"
    batch["render_hint"] = "preview"
    applied = backend_client.post(
        f"/api/v1/resumes/{resume['id']}/operations",
        json=batch,
        headers={
            **idempotency_headers(batch["client_batch_id"]),
            "If-Match": fetched.headers["etag"],
        },
    )
    assert applied.status_code == 200, applied.text
    render_job_id = applied.json()["render_job"]["id"]
    render_job = wait_for_json(
        backend_client,
        f"/api/v1/resume-render-jobs/{render_job_id}",
        lambda payload: payload["status"] in {"succeeded", "failed", "cancelled"},
    )
    assert render_job["status"] == "succeeded"

    artifacts = backend_client.get(
        f"/api/v1/resumes/{resume['id']}/render-artifacts",
        params={"limit": 1},
    )
    assert artifacts.status_code == 200, artifacts.text
    assert artifacts.json()["items"][0]["id"] == render_job["artifacts"][0]["id"]


def test_current_web_gateway_payloads_complete_resume_and_knowledge_flow(
    backend_client: TestClient,
) -> None:
    """The current Web gateway payloads remain executable without DTO adaptation."""
    created_response = backend_client.post(
        "/api/v1/resumes",
        json={
            "title": "Frontend gateway integration",
            "locale": "zh-CN",
            "template_id": "tpl_default_v1",
            "template_version": "1.0",
        },
        headers=idempotency_headers("frontend-gateway-resume-create-0001"),
    )
    assert created_response.status_code == 201, created_response.text
    resume = created_response.json()

    resume_source = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-sources/{resume['knowledge_source_id']}",
        lambda payload: payload["ingestion"]["status"] in {"ready", "failed"},
    )
    assert resume_source["ingestion"]["status"] == "ready"

    proposal_response = backend_client.post(
        f"/api/v1/resumes/{resume['id']}/proposals",
        json={
            "draft_text": None,
            "field_path": ["summary"],
            "instruction": "Improve the professional summary",
            "render_hint": "preview",
            "source_ids": [],
            "target": {"entity_type": "profile"},
            "title": None,
        },
        headers=idempotency_headers("frontend-gateway-proposal-create-0001"),
    )
    assert proposal_response.status_code == 201, proposal_response.text
    proposal = proposal_response.json()

    decision_response = backend_client.post(
        f"/api/v1/resume-proposals/{proposal['id']}/decisions",
        json={
            "comment": None,
            "conflict_strategy": "reject",
            "decision": "accept_all",
            "operation_ids": [],
        },
        headers=idempotency_headers("frontend-gateway-proposal-decision-0001"),
    )
    assert decision_response.status_code == 200, decision_response.text
    assert decision_response.json()["status"] == "accepted"

    latest_response = backend_client.get(f"/api/v1/resumes/{resume['id']}")
    assert latest_response.status_code == 200, latest_response.text
    latest = latest_response.json()
    assert latest["revision"] == 2

    render_response = backend_client.post(
        f"/api/v1/resumes/{resume['id']}/render-jobs",
        json={
            "formats": ["pdf"],
            "include_accessibility_tree": False,
            "include_source_map": True,
            "locale": None,
            "mode": "preview",
            "page_range": None,
            "resume_revision": latest["revision"],
        },
        headers=idempotency_headers("frontend-gateway-render-create-0001"),
    )
    assert render_response.status_code == 202, render_response.text
    render_job = wait_for_json(
        backend_client,
        f"/api/v1/resume-render-jobs/{render_response.json()['id']}",
        lambda payload: payload["status"] in {"succeeded", "failed", "cancelled"},
    )
    assert render_job["status"] == "succeeded"
    artifact = render_job["artifacts"][0]
    content_response = backend_client.get(
        f"/api/v1/render-artifacts/{artifact['id']}/content"
    )
    assert content_response.status_code == 200, content_response.text
    assert content_response.content.startswith(b"%PDF-")

    upload_response = backend_client.post(
        "/api/v1/knowledge-sources/uploads",
        files={
            "file": (
                "integration.md",
                b"# Backend Skill\n\nPostgreSQL pgvector integration evidence.",
                "text/markdown",
            )
        },
        data={"name": "Frontend Integration Knowledge"},
        headers=idempotency_headers("frontend-gateway-upload-create-0001"),
    )
    assert upload_response.status_code == 202, upload_response.text
    upload = upload_response.json()
    source_id = upload["source"]["id"]
    ingestion_job = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-ingestion-jobs/{upload['ingestion_job']['id']}",
        lambda payload: payload["status"] in {"succeeded", "failed", "cancelled"},
    )
    assert ingestion_job["status"] == "succeeded"

    search_response = backend_client.post(
        "/api/v1/knowledge-searches",
        json={
            "filters": {},
            "include_quotes": True,
            "query": "pgvector",
            "selection": {
                "agent_scope": "general_chat",
                "exclude_source_ids": [],
                "include_source_ids": [source_id],
                "mode": "explicit",
                "pinned_versions": [],
            },
            "top_k": 20,
        },
    )
    assert search_response.status_code == 200, search_response.text
    search_items = search_response.json()["items"]
    assert search_items
    assert search_items[0]["citation"]["source_id"] == source_id
