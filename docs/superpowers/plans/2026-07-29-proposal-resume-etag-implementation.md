# Proposal Resume ETag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure the ETag returned after accepting a Resume Proposal is the strong ETag of the returned Resume document and can be used immediately by a restore command.

**Architecture:** Keep the existing proposal-decision and restore workflows unchanged. Align the proposal-decision response with the existing Resume operations response by generating its ETag from `payload["resume"]`, while preserving the complete `ResumeOperationResult` response body and idempotent replay behavior.

**Tech Stack:** Python 3.14, FastAPI, pytest, Pydantic 2.

## Global Constraints

- Do not change the frontend, PDF rendering, restore concurrency protection, or Mock behavior.
- Do not remove or weaken `If-Match` validation.
- Use the existing `replayable_json(..., etag_representation=...)` abstraction.
- Run only the directly relevant HTTP test.

---

### Task 1: Return the Resume document ETag after a Proposal decision

**Files:**
- Modify: `src/backend/api/v2_resumes.py`
- Test: `tests/test_v2_resumes_http.py`

**Interfaces:**
- Consumes: `replayable_json(payload, status_code=200, etag=True, etag_representation=...)`
- Produces: A proposal-decision `ETag` equal to the subsequent `GET /resumes/{resume_id}` ETag.

- [ ] **Step 1: Write the failing test**

Add an assertion after the accepted Proposal decision:

```python
current = harness.client.get(
    f"/api/v2/workspaces/{WORKSPACE_ID}/resumes/{resume_id}",
    headers=_headers(),
)
assert current.status_code == 200
assert decision.headers["etag"] == current.headers["etag"]
```

- [ ] **Step 2: Run the test to verify RED**

Run:

```text
uv run pytest tests/test_v2_resumes_http.py::test_proposal_list_detail_and_decision_are_workspace_bound_and_replayable -q
```

Expected: FAIL because the proposal-decision ETag currently hashes the complete `ResumeOperationResult`, while the GET ETag hashes only the Resume document.

- [ ] **Step 3: Implement the minimal fix**

Change the proposal-decision response to:

```python
return replayable_json(
    payload,
    status_code=200,
    etag=True,
    etag_representation=payload["resume"],
)
```

- [ ] **Step 4: Run the test to verify GREEN**

Run:

```text
uv run pytest tests/test_v2_resumes_http.py::test_proposal_list_detail_and_decision_are_workspace_bound_and_replayable -q
```

Expected: PASS.

- [ ] **Step 5: Run the adjacent restore HTTP test**

Run:

```text
uv run pytest tests/test_v2_resumes_http.py -q -k "proposal_list_detail_and_decision_are_workspace_bound_and_replayable or resume_collection_detail_mutation_job_and_delete_flow"
```

Expected: Both selected tests pass.

- [ ] **Step 6: Commit the focused change**

```text
git add docs/superpowers/plans/2026-07-29-proposal-resume-etag-implementation.md tests/test_v2_resumes_http.py src/backend/api/v2_resumes.py
git commit -m "fix(resume): return document etag after proposal decision"
```
