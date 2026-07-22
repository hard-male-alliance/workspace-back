"""Read-only current-user and workspace membership HTTP boundary tests."""

from __future__ import annotations

from fastapi.testclient import TestClient

from backend.infrastructure.contracts import ContractValidator


def test_current_user_workspace_and_members_are_scoped(
    backend_client: TestClient,
    contract_validator: ContractValidator,
) -> None:
    """The development identity adapter exposes only its asserted workspace."""
    current = backend_client.get("/api/v1/me")
    assert current.status_code == 200, current.text
    user = current.json()
    contract_validator.validate("CurrentUser", user)

    workspaces = backend_client.get("/api/v1/workspaces?limit=20")
    assert workspaces.status_code == 200, workspaces.text
    items = workspaces.json()["items"]
    assert len(items) == 1
    contract_validator.validate_definition("Workspace", items[0])
    assert items[0]["id"] == user["default_workspace_id"]

    workspace_id = items[0]["id"]
    detail = backend_client.get(f"/api/v1/workspaces/{workspace_id}")
    assert detail.status_code == 200, detail.text
    assert detail.json() == items[0]

    members = backend_client.get(f"/api/v1/workspaces/{workspace_id}/members")
    assert members.status_code == 200, members.text
    member_items = members.json()["items"]
    assert len(member_items) == 1
    contract_validator.validate_definition("WorkspaceMember", member_items[0])
    assert member_items[0]["user_id"] == user["id"]

    denied = backend_client.get("/api/v1/workspaces/ws_outside_scope")
    assert denied.status_code == 404, denied.text

