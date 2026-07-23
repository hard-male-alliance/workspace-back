"""@brief API V2 durable runtime 能力边界 / API V2 durable-runtime capability boundary."""

from __future__ import annotations

from backend.app import _requires_durable_v2_runtime


def test_memory_boundary_rejects_every_command_that_would_enqueue_unowned_work() -> None:
    """@brief memory 入口不返回永远 queued 的假成功 / Memory entrypoint never returns permanently queued fake success."""

    paths = (
        ("POST", "/api/v2/me/account-deletion-requests"),
        ("POST", "/api/v2/workspaces/ws_example/resume-import-jobs"),
        ("POST", "/api/v2/workspaces/ws_example/resumes/resume_example/operations"),
        ("POST", "/api/v2/workspaces/ws_example/resumes/resume_example/render-jobs"),
        ("POST", "/api/v2/workspaces/ws_example/resumes/resume_example/restore-jobs"),
        ("DELETE", "/api/v2/workspaces/ws_example/connections/connection_example"),
        ("DELETE", "/api/v2/workspaces/ws_example/knowledge-sources/source_example"),
        ("POST", "/api/v2/workspaces/ws_example/upload-sessions/upload_example/completions"),
        (
            "POST",
            "/api/v2/workspaces/ws_example/knowledge-sources/source_example/ingestion-jobs",
        ),
        ("POST", "/api/v2/workspaces/ws_example/knowledge-sources/source_example/sync-jobs"),
        ("POST", "/api/v2/workspaces/ws_example/agent-runs"),
        (
            "POST",
            "/api/v2/workspaces/ws_example/tool-approvals/approval_example/decisions",
        ),
        (
            "POST",
            "/api/v2/workspaces/ws_example/interview-sessions/session_example/end-requests",
        ),
        (
            "POST",
            "/api/v2/workspaces/ws_example/interview-sessions/session_example/report-jobs",
        ),
    )

    assert all(_requires_durable_v2_runtime(method, path) for method, path in paths)


def test_memory_boundary_rejects_unified_projections_but_keeps_sync_resources() -> None:
    """@brief 跨领域 projection fail closed，普通同步资源仍可测试 / Unified projections fail closed while synchronous resources remain testable."""

    durable_paths = (
        "/api/v2/workspaces/ws_example/jobs",
        "/api/v2/workspaces/ws_example/jobs/job_example",
        "/api/v2/workspaces/ws_example/artifacts/artifact_example/content",
        "/api/v2/workspaces/ws_example/events",
        "/api/v2/workspaces/ws_example/audit-events",
    )
    synchronous_paths = (
        ("GET", "/api/v2/workspaces/ws_example/resumes"),
        ("POST", "/api/v2/workspaces/ws_example/resumes"),
        ("GET", "/api/v2/workspaces/ws_example/conversations"),
        ("POST", "/api/v2/workspaces/ws_example/knowledge-searches"),
        ("GET", "/oauth/jwks"),
    )

    assert all(_requires_durable_v2_runtime("GET", path) for path in durable_paths)
    assert not any(
        _requires_durable_v2_runtime(method, path) for method, path in synchronous_paths
    )
