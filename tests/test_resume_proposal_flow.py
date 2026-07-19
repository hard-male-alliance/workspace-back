"""Phase-one Resume Proposal, knowledge grounding, conflict, and PDF tests."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

from fastapi.testclient import TestClient

from backend.domain.common import DomainError, utc_now
from backend.domain.knowledge import KnowledgeTrustLevel
from backend.domain.proposal import (
    ResumeProposalOperation,
    ResumeProposalRecord,
)
from backend.infrastructure.contracts import ContractValidator
from conftest import idempotency_headers, wait_for_json
from workspace_shared.tenancy import ActorScope


def _proposal_visibility() -> dict[str, Any]:
    return {
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


def _create_ready_evidence(client: TestClient) -> dict[str, Any]:
    response = client.post(
        "/api/v1/knowledge-sources",
        json={
            "name": "后端项目证据",
            "source_type": "manual_note",
            "content": "Python async API reduced latency by 30 percent with measured load tests",
            "visibility": _proposal_visibility(),
        },
        headers=idempotency_headers("proposal-evidence-create-0001"),
    )
    assert response.status_code == 201, response.text
    source = cast(dict[str, Any], response.json())
    job_response = client.post(
        f"/api/v1/knowledge-sources/{source['id']}/ingestion-jobs",
        headers=idempotency_headers("proposal-evidence-ingest-0001"),
    )
    assert job_response.status_code == 202, job_response.text
    job = wait_for_json(
        client,
        f"/api/v1/knowledge-ingestion-jobs/{job_response.json()['id']}",
        lambda payload: payload["status"] in {"succeeded", "failed", "cancelled"},
    )
    assert job["status"] == "succeeded"
    return source


def _create_resume(client: TestClient, key: str) -> dict[str, Any]:
    response = client.post(
        "/api/v1/resumes",
        json={"title": "阶段一后端简历", "locale": "zh-CN"},
        headers=idempotency_headers(key),
    )
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


def _create_proposal(
    client: TestClient, resume_id: str, source_id: str, key: str
) -> dict[str, Any]:
    response = client.post(
        f"/api/v1/resumes/{resume_id}/proposals",
        json={
            "instruction": "Use measured Python latency evidence in the profile summary",
            "source_ids": [source_id],
            "draft_text": "Built Python async APIs and reduced latency by 30 percent.",
            "render_hint": "preview",
        },
        headers=idempotency_headers(key),
    )
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


def test_evidence_grounded_proposal_accepts_reindexes_and_renders(
    backend_client: TestClient,
    contract_validator: ContractValidator,
) -> None:
    evidence = _create_ready_evidence(backend_client)
    resume = _create_resume(backend_client, "proposal-resume-create-0001")
    proposal = _create_proposal(
        backend_client, resume["id"], evidence["id"], "proposal-create-0000001"
    )
    contract_validator.validate_declared("ResumeProposal", proposal)
    assert proposal["status"] == "pending"
    citations = proposal["operations"][0]["extensions"]["aiws"]["citations"]
    assert citations[0]["source_id"] == evidence["id"]
    assert citations[0]["trust_level"] == "user_provided"

    operation_id = proposal["operations"][0]["operation_id"]
    decision = backend_client.post(
        f"/api/v1/resume-proposals/{proposal['id']}/decisions",
        json={
            "decision": "accept_selected",
            "operation_ids": [operation_id],
            "comment": "证据已核对",
            "conflict_strategy": "reject",
        },
        headers=idempotency_headers("proposal-decision-accept-0001"),
    )
    assert decision.status_code == 200, decision.text
    decided = decision.json()
    contract_validator.validate_declared("ResumeProposal", decided)
    assert decided["status"] == "accepted"
    application_result = decided["extensions"]["aiws"]["application_result"]
    assert application_result["new_revision"] == 2
    assert application_result["render_job"] is not None

    render_job = wait_for_json(
        backend_client,
        f"/api/v1/resume-render-jobs/{application_result['render_job']['id']}",
        lambda payload: payload["status"] in {"succeeded", "failed", "cancelled"},
    )
    assert render_job["status"] == "succeeded"
    updated = backend_client.get(f"/api/v1/resumes/{resume['id']}")
    assert updated.status_code == 200
    assert "30 percent" in updated.json()["profile"]["summary"]["plain_text"]

    ready_source = wait_for_json(
        backend_client,
        f"/api/v1/knowledge-sources/{resume['knowledge_source_id']}",
        lambda payload: payload["ingestion"]["status"] == "ready"
        and payload["extensions"]["aiws"]["source_metadata"]["resume_revision"] == 2,
    )
    classification = ready_source["extensions"]["aiws"]["classification"]
    assert classification["source_role"] == "resume_current"
    assert classification["lifecycle"] == "current"


def test_stale_proposal_becomes_conflicted(
    backend_client: TestClient,
    contract_examples: dict[str, Any],
) -> None:
    evidence = _create_ready_evidence(backend_client)
    resume = _create_resume(backend_client, "proposal-resume-create-0002")
    proposal = _create_proposal(
        backend_client, resume["id"], evidence["id"], "proposal-create-0000002"
    )
    current = backend_client.get(f"/api/v1/resumes/{resume['id']}")
    batch = deepcopy(contract_examples["resume_operation_batch"])
    batch["client_batch_id"] = "batch-proposal-stale-0001"
    batch["base_revision"] = 1
    batch["render_hint"] = "none"
    batch["operations"][0]["operation_id"] = "op-proposal-stale-000001"
    batch["operations"][0]["target"] = {"entity_type": "profile"}
    batch["operations"][0]["field_path"] = ["headline"]
    batch["operations"][0]["value"] = "Backend Engineer"
    edited = backend_client.post(
        f"/api/v1/resumes/{resume['id']}/operations",
        json=batch,
        headers={
            **idempotency_headers("proposal-stale-edit-0001"),
            "If-Match": current.headers["etag"],
        },
    )
    assert edited.status_code == 200, edited.text
    decision = backend_client.post(
        f"/api/v1/resume-proposals/{proposal['id']}/decisions",
        json={
            "decision": "accept_all",
            "operation_ids": [],
            "comment": None,
            "conflict_strategy": "rebase_if_safe",
        },
        headers=idempotency_headers("proposal-stale-decision-0001"),
    )
    assert decision.status_code == 412, decision.text
    fetched = backend_client.get(f"/api/v1/resume-proposals/{proposal['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["status"] == "conflicted"


def test_proposal_rejects_unsupported_numeric_claim(backend_client: TestClient) -> None:
    evidence = _create_ready_evidence(backend_client)
    resume = _create_resume(backend_client, "proposal-resume-create-0003")
    response = backend_client.post(
        f"/api/v1/resumes/{resume['id']}/proposals",
        json={
            "instruction": "Write a measured latency result",
            "source_ids": [evidence["id"]],
            "draft_text": "Reduced latency by 99 percent.",
            "render_hint": "none",
        },
        headers=idempotency_headers("proposal-unsupported-number-0001"),
    )
    assert response.status_code == 422, response.text
    assert response.json()["code"] == "resume.proposal_unsupported_numeric_claim"


def test_atomic_group_cannot_be_partially_selected() -> None:
    now = utc_now()
    operation_one = ResumeProposalOperation(
        id="op_atomic_group_0001",
        operation={"operation_id": "op_atomic_group_0001", "op": "set_field"},
        reason="one",
        atomic_group_id="grp_atomic_0001",
        trust_level=KnowledgeTrustLevel.GENERATED,
    )
    operation_two = ResumeProposalOperation(
        id="op_atomic_group_0002",
        operation={"operation_id": "op_atomic_group_0002", "op": "set_field"},
        reason="two",
        atomic_group_id="grp_atomic_0001",
        trust_level=KnowledgeTrustLevel.GENERATED,
    )
    proposal = ResumeProposalRecord(
        scope=ActorScope("actor_atomic", "ws_atomic", "owner_atomic"),
        id="prop_atomic_0001",
        created_at=now,
        updated_at=now,
        resume_id="res_atomic_0001",
        base_revision=1,
        source_run_id="run_atomic_0001",
        title="Atomic group",
        summary="Atomic group",
        operations=[operation_one, operation_two],
    )
    try:
        proposal.select_operations("accept_selected", [operation_one.id])
    except DomainError as error:
        assert error.problem.code == "resume.partial_atomic_group"
    else:
        raise AssertionError("partial atomic group selection must fail")
