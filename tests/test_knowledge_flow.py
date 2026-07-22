"""@brief 知识库 deny-priority、索引与检索的纵向测试 / Vertical tests for knowledge deny-priority, ingestion, and retrieval."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from backend.domain.common import DomainError
from backend.infrastructure.contracts import ContractValidator
from conftest import idempotency_headers, wait_for_json


def _visibility_with_effect(effect: str) -> dict[str, Any]:
    """@brief 构建 general_chat 的显式可见性策略 / Build an explicit visibility policy for general_chat.

    @param effect allow 或 deny 的策略效果 / Policy effect, allow or deny.
    @return 符合正式形状的最小策略 / Minimal policy with the formal shape.
    """

    return {
        "policy_version": 1,
        "default_effect": "deny",
        "sensitivity": "confidential",
        "agent_grants": [
            {
                "agent_scope": "general_chat",
                "effect": effect,
                "allowed_operations": ["retrieve"],
            }
        ],
        "session_override_allowed": True,
        "allow_external_model_processing": False,
        "allowed_model_regions": ["cn"],
        "retention_days": None,
    }


def _knowledge_search_request() -> dict[str, Any]:
    """@brief 构建正式知识搜索请求 / Build a formal knowledge search request.

    @return KnowledgeSearchRequest 对象 / KnowledgeSearchRequest object.
    """

    return {
        "query": "kubernetes",
        "selection": {
            "mode": "policy_default",
            "include_source_ids": [],
            "exclude_source_ids": [],
            "pinned_versions": [],
            "agent_scope": "general_chat",
        },
        "top_k": 5,
        "include_quotes": True,
    }


def _create_and_ingest_source(
    client: TestClient,
    *,
    name: str,
    content: str,
    visibility: dict[str, Any],
    key_suffix: str,
    contract_validator: ContractValidator,
) -> dict[str, Any]:
    """@brief 通过 mock adapter 创建来源并等待确定性索引完成 / Create a source through the mock adapter and wait for deterministic ingestion.

    @param client 已启动的后端 TestClient / Started backend TestClient.
    @param name 来源名称 / Source name.
    @param content mock 解析内容 / Mock parsed content.
    @param visibility 来源可见性策略 / Source visibility policy.
    @param key_suffix 幂等键中的稳定后缀 / Stable suffix for idempotency keys.
    @param contract_validator 权威契约验证器 / Authoritative contract validator.
    @return 已准备好的 KnowledgeSource 公开对象 / Ready KnowledgeSource public object.
    """

    source_response = client.post(
        "/api/v1/knowledge-sources",
        json={
            "name": name,
            "source_type": "manual_note",
            "content": content,
            "visibility": visibility,
        },
        headers=idempotency_headers(f"knowledge-source-{key_suffix}"),
    )
    assert source_response.status_code == 201, source_response.text
    source = source_response.json()
    contract_validator.validate("KnowledgeSource", source)

    job_response = client.post(
        f"/api/v1/knowledge-sources/{source['id']}/ingestion-jobs",
        headers=idempotency_headers(f"knowledge-ingest-{key_suffix}"),
    )
    assert job_response.status_code == 202, job_response.text
    job_id = job_response.json()["id"]
    job = wait_for_json(
        client,
        f"/api/v1/knowledge-ingestion-jobs/{job_id}",
        lambda payload: payload["status"] in {"succeeded", "failed", "cancelled"},
    )
    assert job["status"] == "succeeded"
    contract_validator.validate("KnowledgeIngestionJob", job)
    assert job["stats"]["chunks"] >= 1

    ready_source_response = client.get(f"/api/v1/knowledge-sources/{source['id']}")
    assert ready_source_response.status_code == 200, ready_source_response.text
    ready_source = cast(dict[str, Any], ready_source_response.json())
    contract_validator.validate("KnowledgeSource", ready_source)
    assert ready_source["ingestion"]["status"] == "ready"
    return ready_source


def test_knowledge_deny_priority_ingestion_and_search(
    backend_client: TestClient,
    contract_validator: ContractValidator,
) -> None:
    """@brief 明确 deny 必须压过 allow；允许来源索引后才可被正式搜索请求返回 / Explicit deny must override allow; an allowed indexed source must then be returned by formal search.

    @param backend_client 已启动的后端 TestClient / Started backend TestClient.
    @param contract_validator 权威契约验证器 / Authoritative contract validator.
    """

    deny_priority_visibility = _visibility_with_effect("allow")
    deny_priority_visibility["agent_grants"].append(
        {
            "agent_scope": "general_chat",
            "effect": "deny",
            "allowed_operations": ["retrieve"],
        }
    )
    denied_source = _create_and_ingest_source(
        backend_client,
        name="应被拒绝的 Kubernetes 笔记",
        content="kubernetes scheduler should never become visible to this agent",
        visibility=deny_priority_visibility,
        key_suffix="deny-priority-0001",
        contract_validator=contract_validator,
    )

    request = _knowledge_search_request()
    contract_validator.validate_declared("KnowledgeSearchRequest", request)
    denied_search_response = backend_client.post("/api/v1/knowledge-searches", json=request)
    assert denied_search_response.status_code == 200, denied_search_response.text
    assert denied_search_response.json() == {"items": []}

    allowed_source = _create_and_ingest_source(
        backend_client,
        name="允许的 Kubernetes 笔记",
        content="kubernetes deterministic retrieval verifies the knowledge pipeline",
        visibility=_visibility_with_effect("allow"),
        key_suffix="allow-source-000001",
        contract_validator=contract_validator,
    )
    allowed_search_response = backend_client.post("/api/v1/knowledge-searches", json=request)
    assert allowed_search_response.status_code == 200, allowed_search_response.text
    results = allowed_search_response.json()["items"]
    assert results
    for result in results:
        contract_validator.validate_definition("KnowledgeSearchResult", result)
        assert result["citation"]["source_id"] != denied_source["id"]
    assert results[0]["citation"]["source_id"] == allowed_source["id"]
    assert results[0]["citation"]["quote"] is not None
    assert "kubernetes" in results[0]["text"].lower()

    portal = backend_client.portal
    assert portal is not None
    container = backend_client.app.state.container

    async def externally_processed_search() -> list[dict[str, Any]]:
        return await container.knowledge.search_for_agent(
            container.settings.default_scope,
            request,
            external_processing=True,
            data_region="cn",
        )

    with pytest.raises(DomainError) as raised:
        portal.call(externally_processed_search)
    assert raised.value.problem.code == "knowledge.external_model_processing_not_allowed"


def test_knowledge_detail_etag_and_visibility_patch_are_concurrency_safe(
    backend_client: TestClient,
) -> None:
    """Knowledge visibility writes require the latest strong ETag."""
    created = backend_client.post(
        "/api/v1/knowledge-sources",
        json={
            "name": "ETag visibility source",
            "source_type": "manual_note",
            "content": "concurrency evidence",
            "visibility": _visibility_with_effect("allow"),
        },
        headers=idempotency_headers("knowledge-etag-create-0001"),
    )
    assert created.status_code == 201, created.text
    source_id = created.json()["id"]

    detail = backend_client.get(f"/api/v1/knowledge-sources/{source_id}")
    assert detail.status_code == 200, detail.text
    first_etag = detail.headers.get("etag")
    assert first_etag and first_etag.startswith('"knowledge-source-1-')

    visibility = _visibility_with_effect("allow")
    visibility.update(
        {
            "allow_external_model_processing": True,
            "allowed_model_regions": ["global"],
        }
    )
    updated = backend_client.patch(
        f"/api/v1/knowledge-sources/{source_id}",
        json={"visibility": visibility},
        headers={"If-Match": first_etag},
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["revision"] == 2
    second_etag = updated.headers.get("etag")
    assert second_etag and second_etag != first_etag
    assert updated.json()["visibility"]["allowed_model_regions"] == ["global"]

    stale = backend_client.patch(
        f"/api/v1/knowledge-sources/{source_id}",
        json={"visibility": _visibility_with_effect("deny")},
        headers={"If-Match": first_etag},
    )
    assert stale.status_code == 412, stale.text
    assert stale.json()["code"] == "knowledge.revision_conflict"

    missing = backend_client.patch(
        f"/api/v1/knowledge-sources/{source_id}",
        json={"visibility": visibility},
    )
    assert missing.status_code == 412, missing.text


def test_resume_is_automatically_derived_reindexed_and_tenant_scoped(
    backend_client: TestClient,
    contract_examples: dict[str, Any],
    contract_validator: ContractValidator,
) -> None:
    """@brief Resume 提交会生成同租户 resume source 并在新 revision 后重建索引 / A submitted Resume creates a same-tenant resume source and reindexes it after a new revision.

    @param backend_client 已启动的后端客户端 / Started backend client.
    @param contract_examples 已发布的正式请求示例 / Published formal request examples.
    @param contract_validator 权威契约验证器 / Authoritative contract validator.
    @return 无返回值 / No return value.

    @note 测试只使用已存在的 ResumeDocument、KnowledgeSource、ResumeOperationBatch 和
    KnowledgeSearchRequest 结构；没有为内部自动桥接发明新的公开命令或 DTO（Data
    Transfer Object，数据传输对象）。
    """
    create_response = backend_client.post(
        "/api/v1/resumes",
        json={"title": "Kubernetes platform resume", "locale": "zh-CN"},
        headers=idempotency_headers("resume-knowledge-create-0001"),
    )
    assert create_response.status_code == 201, create_response.text
    created = cast(dict[str, Any], create_response.json())
    contract_validator.validate("ResumeDocument", created)
    resume_id = str(created["id"])
    source_id = created["knowledge_source_id"]
    assert isinstance(source_id, str)

    ready_source = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-sources/{source_id}",
        lambda payload: payload["ingestion"]["status"] in {"ready", "failed"},
    )
    assert ready_source["ingestion"]["status"] == "ready"
    contract_validator.validate("KnowledgeSource", ready_source)
    assert ready_source["source_type"] == "resume"
    assert ready_source["config"] == {
        "source_type": "resume",
        "resume_id": resume_id,
        "revision_mode": "latest",
    }

    initial_search = _knowledge_search_request()
    initial_search["query"] = "kubernetes"
    search_response = backend_client.post("/api/v1/knowledge-searches", json=initial_search)
    assert search_response.status_code == 200, search_response.text
    assert any(
        item["citation"]["source_id"] == source_id for item in search_response.json()["items"]
    )

    resume_response = backend_client.get(f"/api/v1/resumes/{resume_id}")
    assert resume_response.status_code == 200, resume_response.text
    batch = deepcopy(contract_examples["resume_operation_batch"])
    batch["client_batch_id"] = "resume-knowledge-update-0001"
    batch["base_revision"] = 1
    batch["render_hint"] = "none"
    batch["operations"][0]["operation_id"] = "op-resume-knowledge-update-0001"
    batch["operations"][0]["target"] = {"entity_type": "profile"}
    batch["operations"][0]["field_path"] = ["full_name"]
    batch["operations"][0]["value"] = "Klee Rust"
    contract_validator.validate("ResumeOperationBatch", batch)
    update_response = backend_client.post(
        f"/api/v1/resumes/{resume_id}/operations",
        json=batch,
        headers={
            **idempotency_headers("resume-knowledge-update-0001"),
            "If-Match": resume_response.headers["etag"],
        },
    )
    assert update_response.status_code == 200, update_response.text
    assert update_response.json()["new_revision"] == 2

    reindexed_source = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-sources/{source_id}",
        lambda payload: payload["ingestion"]["status"] in {"ready", "failed"},
    )
    assert reindexed_source["ingestion"]["status"] == "ready"
    rust_search = _knowledge_search_request()
    rust_search["query"] = "rust"
    rust_response = backend_client.post("/api/v1/knowledge-searches", json=rust_search)
    assert rust_response.status_code == 200, rust_response.text
    assert any(item["citation"]["source_id"] == source_id for item in rust_response.json()["items"])

    other_scope_headers = {
        "X-Mock-Actor-Id": "usr_other",
        "X-Mock-Workspace-Id": "ws_other",
        "X-Mock-Resource-Owner-Id": "usr_other",
    }
    other_source_response = backend_client.get(
        f"/api/v1/knowledge-sources/{source_id}", headers=other_scope_headers
    )
    assert other_source_response.status_code == 404, other_source_response.text
    other_search_response = backend_client.post(
        "/api/v1/knowledge-searches",
        json=rust_search,
        headers=other_scope_headers,
    )
    assert other_search_response.status_code == 200, other_search_response.text
    assert other_search_response.json() == {"items": []}
