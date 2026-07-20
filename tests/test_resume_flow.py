"""@brief 简历创建、版本、幂等与 mock PDF 渲染的纵向测试 / Vertical tests for resume creation, versioning, idempotency, and mock PDF rendering."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.application.services import (
    KnowledgeApplicationService,
    ResumeApplicationService,
    ScopedKeyLocks,
    ServiceDependencies,
)
from backend.config import BackendSettings
from backend.infrastructure.concurrency import BoundedTaskSupervisor, WorkLimits
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.embeddings import DeterministicEmbeddingProvider
from backend.infrastructure.knowledge_parsing import LocalKnowledgeFileParser
from backend.infrastructure.knowledge_storage import LocalKnowledgeBlobStorage
from backend.infrastructure.memory import InMemoryWorkspaceRepository
from backend.infrastructure.rendering import MockRenderer
from backend.infrastructure.telemetry import BufferedTelemetrySink, InMemoryTelemetryWriter
from conftest import PROJECT_ROOT, idempotency_headers, wait_for_json
from workspace_shared.tenancy import ActorScope


def test_resume_version_idempotency_and_render_flow(
    backend_client: TestClient,
    contract_examples: dict[str, Any],
    contract_validator: ContractValidator,
) -> None:
    """@brief 以正式 operation 契约完成编辑、原请求重试、版本读取和 PDF 下载 / Complete edit, exact retry, revision read, and PDF download with the formal operation contract.

    @param backend_client 已在生命周期中的后端客户端 / Backend client inside its lifecycle.
    @param contract_examples 已发布的正式请求示例 / Published formal request examples.
    @param contract_validator 权威契约验证器 / Authoritative contract validator.
    """

    create_key = "resume-create-flow-000001"
    created_response = backend_client.post(
        "/api/v1/resumes",
        json={"title": "Klee 的后端简历", "locale": "zh-CN"},
        headers=idempotency_headers(create_key),
    )
    assert created_response.status_code == 201, created_response.text
    created = created_response.json()
    contract_validator.validate("ResumeDocument", created)
    resume_id = created["id"]

    fetched_response = backend_client.get(f"/api/v1/resumes/{resume_id}")
    assert fetched_response.status_code == 200, fetched_response.text
    initial_etag = fetched_response.headers["etag"]
    assert fetched_response.json()["revision"] == 1

    batch = deepcopy(contract_examples["resume_operation_batch"])
    batch["client_batch_id"] = "batch-resume-flow-000001"
    batch["base_revision"] = 1
    batch["operations"][0]["operation_id"] = "op-resume-flow-00000001"
    batch["operations"][0]["target"] = {"entity_type": "profile"}
    batch["operations"][0]["field_path"] = ["full_name"]
    batch["operations"][0]["value"] = "Klee"
    contract_validator.validate("ResumeOperationBatch", batch)
    command_headers = {
        **idempotency_headers(batch["client_batch_id"]),
        "If-Match": initial_etag,
    }

    applied_response = backend_client.post(
        f"/api/v1/resumes/{resume_id}/operations",
        json=batch,
        headers=command_headers,
    )
    assert applied_response.status_code == 200, applied_response.text
    applied = applied_response.json()
    contract_validator.validate("ResumeOperationBatchResult", applied)
    assert applied["previous_revision"] == 1
    assert applied["new_revision"] == 2
    assert applied["results"] == [
        {
            "operation_id": batch["operations"][0]["operation_id"],
            "status": "applied",
            "problem": None,
        }
    ]
    assert applied["normalized_document"]["profile"]["full_name"] == "Klee"
    assert applied["render_job"] is not None

    replay_response = backend_client.post(
        f"/api/v1/resumes/{resume_id}/operations",
        json=batch,
        headers=command_headers,
    )
    assert replay_response.status_code == 200, replay_response.text
    assert replay_response.json() == applied

    latest_response = backend_client.get(f"/api/v1/resumes/{resume_id}")
    assert latest_response.status_code == 200, latest_response.text
    latest = latest_response.json()
    assert latest["revision"] == 2
    assert latest["profile"]["full_name"] == "Klee"
    assert latest_response.headers["etag"] != initial_etag

    historical_response = backend_client.get(f"/api/v1/resumes/{resume_id}/revisions/1")
    assert historical_response.status_code == 200, historical_response.text
    historical = historical_response.json()
    assert historical["revision"] == 1
    assert historical["profile"]["full_name"] == "未命名求职者"

    render_job_id = applied["render_job"]["id"]
    render_job = wait_for_json(
        backend_client,
        f"/api/v1/resume-render-jobs/{render_job_id}",
        lambda payload: payload["status"] in {"succeeded", "failed", "cancelled"},
    )
    assert render_job["status"] == "succeeded"
    contract_validator.validate("ResumeRenderJob", render_job)
    assert len(render_job["artifacts"]) == 1
    artifact = render_job["artifacts"][0]
    contract_validator.validate_definition("RenderArtifact", artifact)

    artifact_response = backend_client.get(f"/api/v1/render-artifacts/{artifact['id']}")
    assert artifact_response.status_code == 200, artifact_response.text
    assert artifact_response.json() == artifact

    content_response = backend_client.get(f"/api/v1/render-artifacts/{artifact['id']}/content")
    assert content_response.status_code == 200, content_response.text
    assert content_response.headers["content-type"] == "application/pdf"
    assert content_response.headers["etag"] == f'"sha256-{artifact["sha256"]}"'
    assert content_response.content.startswith(b"%PDF-")

    source_map_response = backend_client.get(
        f"/api/v1/render-artifacts/{artifact['id']}/source-map"
    )
    assert source_map_response.status_code == 200, source_map_response.text
    source_map = source_map_response.json()
    contract_validator.validate("PdfSourceMap", source_map)
    assert source_map["artifact_id"] == artifact["id"]


@pytest.mark.asyncio
async def test_render_hint_backpressure_returns_a_persisted_failed_job_and_idempotent_batch(
    contract_examples: dict[str, Any],
) -> None:
    """@brief render_hint 队列满后仍返回可重放的一致操作结果 / A full render queue still returns a replayable coherent operation result.

    @param contract_examples 已发布的正式请求示例 / Published formal request examples.
    @return 无返回值。

    @note 该测试填满全局 supervisor（监督器）容量，而不是 mock ``create_render_job``。
    因而它覆盖真实的 ``BackpressureError`` 分支、失败 Job 持久化和同 client batch 的
    重放语义，防止“revision 已提交但客户端只拿到 503”的部分成功状态。
    """
    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")
    scope = ActorScope("usr_klee", "ws_backpressure", "usr_klee")
    repository = InMemoryWorkspaceRepository()
    supervisor = BoundedTaskSupervisor(
        (
            WorkLimits("llm", 1),
            WorkLimits("render", 1),
            WorkLimits("knowledge", 1),
            WorkLimits("interview", 1),
            WorkLimits("telemetry", 1),
        ),
        queue_capacity=1,
        shutdown_grace_ms=1_000,
    )
    dependencies = ServiceDependencies(
        settings.network,
        settings.ai,
        settings.knowledge,
        supervisor,
        BufferedTelemetrySink(InMemoryTelemetryWriter(), 8, 1, 10, "drop_newest"),
    )
    locks = ScopedKeyLocks()
    knowledge = KnowledgeApplicationService(
        repository,
        repository,
        LocalKnowledgeBlobStorage(PROJECT_ROOT / ".pytest_cache" / "knowledge-blobs"),
        LocalKnowledgeFileParser(settings.knowledge.max_extracted_characters),
        DeterministicEmbeddingProvider(settings.ai.embedding_dimension),
        dependencies,
        locks,
    )
    service = ResumeApplicationService(
        repository,
        repository,
        repository,
        MockRenderer(),
        knowledge,
        dependencies,
        locks,
    )
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def occupy_supervisor() -> None:
        """@brief 占满全局任务容量 / Occupy the global task capacity.

        @return 在测试释放前不返回 / Does not return before the test releases it.
        """
        blocker_started.set()
        await release_blocker.wait()

    async with supervisor:
        supervisor.submit("render", occupy_supervisor, name="test:render-capacity-holder")
        await blocker_started.wait()
        record = await service.create_resume(scope, "反压简历", "zh-CN")
        batch = deepcopy(contract_examples["resume_operation_batch"])
        batch["client_batch_id"] = "batch-render-backpressure-0001"
        batch["base_revision"] = 1
        batch["operations"][0]["operation_id"] = "op-render-backpressure-0001"
        batch["operations"][0]["target"] = {"entity_type": "profile"}
        batch["operations"][0]["field_path"] = ["full_name"]
        batch["operations"][0]["value"] = "Klee"
        batch["render_hint"] = "preview"

        result = await service.apply_operations(
            scope,
            record.id,
            batch,
            record.etag(),
            "req-render-backpressure",
        )
        assert result["new_revision"] == 2
        assert result["render_job"] is not None
        assert result["render_job"]["status"] == "failed"
        assert result["render_job"]["error"]["code"] == "runtime.overloaded"

        replay = await service.apply_operations(
            scope,
            record.id,
            batch,
            record.etag(),
            "req-render-backpressure-replay",
        )
        assert replay == result
        assert (await service.get_resume(scope, record.id)).revision == 2
        release_blocker.set()
