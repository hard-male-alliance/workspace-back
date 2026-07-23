"""@brief API V2 Phase-1 HTTP 适配器测试 / API V2 Phase-1 HTTP adapter tests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from backend.api.v2_access import create_v2_access_router, router_v2_access
from backend.api.v2_http import CursorCodec
from backend.application.access import AccessApplicationService
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM
from backend.domain.principals import (
    MembershipId,
    ResourceMeta,
    Subject,
    UserId,
    WorkspaceId,
)
from backend.domain.users import User
from backend.domain.workspaces import (
    DataRegion,
    Membership,
    MemberStatus,
    Workspace,
    WorkspacePlan,
    WorkspaceRole,
)
from backend.infrastructure.access import (
    InMemoryAccessStore,
    InMemoryAccessUnitOfWorkFactory,
)
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.v2_idempotency import (
    InMemoryIdempotencyExecutor,
    InMemoryV2IdempotencyStore,
)
from backend.package_resources import read_contract_schema_text

NOW = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
"""@brief 测试固定时刻 / Fixed test instant."""

OWNER_ID = UserId("usr_klee_0001")
"""@brief owner 用户标识 / Owner user identifier."""

RECIPIENT_ID = UserId("usr_amber_0001")
"""@brief 邀请收件人用户标识 / Invitation-recipient user identifier."""

WORKSPACE_ID = WorkspaceId("wsp_team_0001")
"""@brief 主测试 Workspace 标识 / Primary test workspace identifier."""

OWNER_MEMBER_ID = MembershipId("mem_owner_0001")
"""@brief owner 成员标识 / Owner membership identifier."""


class _FixedClock:
    """@brief 返回单调测试时刻的 clock / Clock returning monotonic test instants."""

    current: datetime
    """@brief 下一次读取的测试时刻 / Test instant returned by the next read."""

    def __init__(self) -> None:
        """@brief 初始化固定时钟 / Initialize the fixed clock."""

        self.current = NOW + timedelta(hours=1)

    def now(self) -> datetime:
        """@brief 返回当前时刻并前进一步 / Return the current instant and advance it.

        @return 带时区测试时刻 / Timezone-aware test instant.
        """

        value = self.current
        self.current += timedelta(seconds=1)
        return value


class _Ids:
    """@brief 生成满足 OpaqueId 的确定性测试标识 / Generate deterministic contract-valid opaque IDs."""

    counters: dict[str, int]
    """@brief 每个领域前缀的计数 / Counter per domain prefix."""

    def __init__(self) -> None:
        """@brief 初始化空计数器 / Initialize empty counters."""

        self.counters = {}

    def __call__(self, prefix: str) -> str:
        """@brief 生成下一个标识 / Generate the next identifier.

        @param prefix 领域前缀 / Domain prefix.
        @return 契约合法标识 / Contract-valid identifier.
        """

        sequence = self.counters.get(prefix, 0) + 1
        self.counters[prefix] = sequence
        return f"{prefix}_http_{sequence:04d}"


class _Reauthentication:
    """@brief 只接受测试 flow 的 reauthentication verifier / Reauthentication verifier accepting only the test flow."""

    async def verify_recent(
        self,
        user_id: UserId,
        flow_id: str,
        verified_at: datetime,
    ) -> bool:
        """@brief 验证用户绑定的测试 flow / Verify the user-bound test flow.

        @param user_id 发起用户 / Requesting user.
        @param flow_id reauthentication flow 标识 / Reauthentication flow identifier.
        @param verified_at 验证时刻 / Verification instant.
        @return 仅 owner 的固定 flow 返回真 / True only for the owner's fixed flow.
        """

        del verified_at
        return user_id == OWNER_ID and flow_id == "flow_reauth_0001"


@dataclass(slots=True)
class _Runtime:
    """@brief HTTP adapter 的隔离 runtime / Isolated runtime for the HTTP adapter."""

    access: AccessApplicationService
    """@brief Phase-1 应用服务 / Phase-1 application service."""

    contracts_v2: ContractValidator
    """@brief 权威 V2 校验器 / Authoritative V2 validator."""

    v2_cursor: CursorCodec
    """@brief 测试签名 cursor codec / Test signed cursor codec."""

    v2_idempotency: InMemoryIdempotencyExecutor
    """@brief 测试幂等 executor / Test idempotency executor."""


@dataclass(slots=True)
class _Harness:
    """@brief 组合 HTTP client、状态与校验器 / Bundle the HTTP client, state, and validator."""

    client: TestClient
    """@brief 同步测试 client / Synchronous test client."""

    store: InMemoryAccessStore
    """@brief 可断言的 Access 状态 / Inspectable access state."""

    validator: ContractValidator
    """@brief response contract 校验器 / Response-contract validator."""


def _user(
    user_id: UserId,
    subject: str,
    email: str,
    display_name: str,
) -> User:
    """@brief 构造测试用户 / Build a test user.

    @param user_id 用户标识 / User identifier.
    @param subject OIDC subject / OIDC subject.
    @param email 规范邮箱 / Canonical email.
    @param display_name 显示名 / Display name.
    @return 用户聚合 / User aggregate.
    """

    return User(
        ResourceMeta(user_id, 1, NOW, NOW),
        Subject(subject),
        email,
        True,
        display_name,
        "zh-CN",
        WORKSPACE_ID if user_id == OWNER_ID else None,
    )


def _claims(user: str) -> dict[str, object]:
    """@brief 为指定测试 actor 构造已验签 claims / Build verified claims for a test actor.

    @param user 测试 actor 名 / Test actor name.
    @return middleware claims / Middleware claims.
    """

    if user == "recipient":
        return {
            ACCESS_TOKEN_USER_ID_CLAIM: str(RECIPIENT_ID),
            "sub": "sub_amber_0001",
            "client_id": "web_client_0001",
            "scope": "openid profile workspace.read",
        }
    if user == "narrow":
        return {
            ACCESS_TOKEN_USER_ID_CLAIM: str(OWNER_ID),
            "sub": "sub_klee_0001",
            "client_id": "web_client_0001",
            "scope": "openid",
        }
    if user == "phantom":
        return {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_phantom_0001",
            "sub": "sub_phantom_0001",
            "client_id": "web_client_0001",
            "scope": "openid profile workspace.read",
        }
    return {
        ACCESS_TOKEN_USER_ID_CLAIM: str(OWNER_ID),
        "sub": "sub_klee_0001",
        "client_id": "web_client_0001",
        "scope": "openid profile workspace.read workspace.write",
    }


@contextmanager
def _harness() -> Iterator[_Harness]:
    """@brief 启动隔离的 Phase-1 FastAPI app / Start an isolated Phase-1 FastAPI app.

    @return 活跃测试 harness iterator / Active test-harness iterator.
    """

    store = InMemoryAccessStore()
    owner = _user(OWNER_ID, "sub_klee_0001", "klee@example.com", "Klee")
    recipient = _user(
        RECIPIENT_ID,
        "sub_amber_0001",
        "amber@example.com",
        "Amber",
    )
    store.users[str(OWNER_ID)] = owner
    store.users[str(RECIPIENT_ID)] = recipient
    workspace = Workspace(
        ResourceMeta(WORKSPACE_ID, 1, NOW, NOW),
        "Knights of Favonius",
        "favonius",
        WorkspacePlan.TEAM,
        DataRegion.GLOBAL,
    )
    store.workspaces[str(WORKSPACE_ID)] = workspace
    store.memberships[str(OWNER_MEMBER_ID)] = Membership(
        ResourceMeta(OWNER_MEMBER_ID, 1, NOW, NOW),
        WORKSPACE_ID,
        OWNER_ID,
        owner.display_name,
        WorkspaceRole.OWNER,
        MemberStatus.ACTIVE,
    )
    validator = ContractValidator.from_jsonc(read_contract_schema_text("v2"))
    runtime = _Runtime(
        AccessApplicationService(
            InMemoryAccessUnitOfWorkFactory(store),
            _Reauthentication(),
            clock=_FixedClock(),
            id_factory=_Ids(),
        ),
        validator,
        CursorCodec(b"phase-1-http-cursor-secret-000001"),
        InMemoryIdempotencyExecutor(InMemoryV2IdempotencyStore()),
    )
    app = FastAPI()
    app.include_router(create_v2_access_router(lambda _request: runtime))

    @app.middleware("http")
    async def verified_context(
        request: Request,
        call_next: object,
    ) -> object:
        """@brief 模拟只注入已验签 claims 的生产 middleware / Simulate production middleware injecting only verified claims.

        @param request 当前 request / Current request.
        @param call_next ASGI downstream callable / ASGI downstream callable.
        @return downstream response / Downstream response.
        """

        request.state.request_id = request.headers.get("X-Request-Id", "req_http_0001")
        request.state.oauth_claims = _claims(request.headers.get("X-Test-Actor", "owner"))
        return await call_next(request)  # type: ignore[operator]

    with TestClient(app, raise_server_exceptions=False) as client:
        yield _Harness(client, store, validator)


def _headers(
    *,
    request_id: str = "req_http_0001",
    actor: str = "owner",
    **headers: str,
) -> dict[str, str]:
    """@brief 构造测试 transport headers / Build test transport headers.

    @param request_id request ID / Request ID.
    @param actor 测试 actor / Test actor.
    @param headers 额外 headers / Additional headers.
    @return header 字典 / Header dictionary.
    """

    return {"X-Request-Id": request_id, "X-Test-Actor": actor, **headers}


def test_router_registers_userinfo_and_all_nineteen_phase_one_routes() -> None:
    """@brief 默认 router 精确暴露 UserInfo 与十九条产品路由 / The default router exposes exactly UserInfo and nineteen product routes."""

    actual = {
        (method, route.path)
        for route in router_v2_access.routes
        if isinstance(route, APIRoute)
        for method in route.methods or set()
    }
    expected = {
        ("GET", "/userinfo"),
        ("GET", "/api/v2/me"),
        ("PATCH", "/api/v2/me"),
        ("POST", "/api/v2/me/account-deletion-requests"),
        ("GET", "/api/v2/me/account-deletion-requests/{request_id}"),
        ("POST", "/api/v2/me/account-deletion-requests/{request_id}/cancellations"),
        ("GET", "/api/v2/workspaces"),
        ("POST", "/api/v2/workspaces"),
        ("GET", "/api/v2/workspaces/{workspace_id}"),
        ("PATCH", "/api/v2/workspaces/{workspace_id}"),
        ("DELETE", "/api/v2/workspaces/{workspace_id}"),
        ("GET", "/api/v2/workspaces/{workspace_id}/members"),
        ("GET", "/api/v2/workspaces/{workspace_id}/members/{member_id}"),
        ("PATCH", "/api/v2/workspaces/{workspace_id}/members/{member_id}"),
        ("DELETE", "/api/v2/workspaces/{workspace_id}/members/{member_id}"),
        ("GET", "/api/v2/workspaces/{workspace_id}/invitations"),
        ("POST", "/api/v2/workspaces/{workspace_id}/invitations"),
        ("GET", "/api/v2/workspaces/{workspace_id}/invitations/{invitation_id}"),
        ("DELETE", "/api/v2/workspaces/{workspace_id}/invitations/{invitation_id}"),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/invitations/{invitation_id}/acceptances",
        ),
    }
    assert actual == expected
    assert all(
        route.openapi_extra is not None
        and route.openapi_extra["x-api-v2-phase"] == 1
        for route in router_v2_access.routes
        if isinstance(route, APIRoute)
    )


def test_userinfo_is_scope_narrowed_and_me_has_contract_etag() -> None:
    """@brief UserInfo 不越权投影且 `/me` 返回正式资源 / UserInfo is scope-narrowed and `/me` returns a published resource."""

    with _harness() as harness:
        narrow = harness.client.get(
            "/userinfo",
            headers=_headers(actor="narrow"),
        )
        assert narrow.status_code == 200
        assert narrow.json() == {"sub": "sub_klee_0001"}
        assert narrow.headers["cache-control"] == "no-store"

        profile = harness.client.get("/userinfo", headers=_headers())
        assert profile.status_code == 200
        assert profile.json() == {
            "sub": "sub_klee_0001",
            "name": "Klee",
            "locale": "zh-CN",
        }
        assert "email" not in profile.json()

        me = harness.client.get("/api/v2/me", headers=_headers())
        assert me.status_code == 200
        harness.validator.validate_definition("CurrentUser", me.json())
        assert me.headers["etag"].startswith('"sha256-')
        assert me.headers["x-request-id"] == "req_http_0001"


def test_me_patch_requires_merge_patch_and_a_current_strong_etag() -> None:
    """@brief `/me` PATCH 同时执行媒体类型、ETag 与 revision CAS / `/me` PATCH enforces media type, ETag, and revision CAS."""

    with _harness() as harness:
        current = harness.client.get("/api/v2/me", headers=_headers())
        etag = current.headers["etag"]
        wrong_media = harness.client.patch(
            "/api/v2/me",
            content='{"display_name":"Spark Knight"}',
            headers=_headers(**{"Content-Type": "application/json", "If-Match": etag}),
        )
        assert (wrong_media.status_code, wrong_media.json()["code"]) == (
            415,
            "http.unsupported_media_type",
        )

        missing = harness.client.patch(
            "/api/v2/me",
            content='{"display_name":"Spark Knight"}',
            headers=_headers(**{"Content-Type": "application/merge-patch+json"}),
        )
        assert (missing.status_code, missing.json()["code"]) == (
            412,
            "http.precondition_failed",
        )

        updated = harness.client.patch(
            "/api/v2/me",
            content='{"display_name":"Spark Knight"}',
            headers=_headers(
                **{
                    "Content-Type": "application/merge-patch+json",
                    "If-Match": etag,
                }
            ),
        )
        assert updated.status_code == 200
        assert updated.json()["display_name"] == "Spark Knight"
        assert updated.json()["revision"] == 2
        assert updated.headers["etag"] != etag
        harness.validator.validate_definition("CurrentUser", updated.json())

        stale = harness.client.patch(
            "/api/v2/me",
            content='{"locale":"en-US"}',
            headers=_headers(
                **{
                    "Content-Type": "application/merge-patch+json",
                    "If-Match": etag,
                }
            ),
        )
        assert (stale.status_code, stale.json()["code"]) == (
            412,
            "http.precondition_failed",
        )


def test_workspace_creation_replays_exact_receipt_and_rejects_key_reuse() -> None:
    """@brief Workspace 创建按完整 scope 重放且拒绝不同指纹 / Workspace creation replays by full scope and rejects a different fingerprint."""

    with _harness() as harness:
        body = '{"name":"Adventure Team","slug":"adventure-team","data_region":"global"}'
        first = harness.client.post(
            "/api/v2/workspaces",
            content=body,
            headers=_headers(
                request_id="req_create_0001",
                **{
                    "Content-Type": "application/json",
                    "Idempotency-Key": "workspace-create-0001",
                },
            ),
        )
        replay = harness.client.post(
            "/api/v2/workspaces",
            content=body,
            headers=_headers(
                request_id="req_create_0002",
                **{
                    "Content-Type": "application/json",
                    "Idempotency-Key": "workspace-create-0001",
                },
            ),
        )
        assert first.status_code == replay.status_code == 201
        assert first.content == replay.content
        assert first.headers["etag"] == replay.headers["etag"]
        assert first.headers["location"] == replay.headers["location"]
        assert first.headers["x-request-id"] == "req_create_0001"
        assert replay.headers["x-request-id"] == "req_create_0002"
        harness.validator.validate_definition("Workspace", first.json())
        assert len(harness.store.workspaces) == 2

        reused = harness.client.post(
            "/api/v2/workspaces",
            json={"name": "Different", "slug": "different", "data_region": "global"},
            headers=_headers(**{"Idempotency-Key": "workspace-create-0001"}),
        )
        assert (reused.status_code, reused.json()["code"]) == (
            409,
            "idempotency.key_reused",
        )


def test_transport_rejects_oversized_or_unexpected_body_before_domain_work() -> None:
    """@brief transport 在领域调用前拒绝超大或无 body 路由 payload / Reject bodies before domain work.

    @return 无返回值 / No return value.
    """
    with _harness() as harness:
        workspace_count = len(harness.store.workspaces)
        oversized = harness.client.post(
            "/api/v2/workspaces",
            content=b'{' + b'"padding":"' + (b"x" * 70_000) + b'"}',
            headers=_headers(
                **{
                    "Content-Type": "application/json",
                    "Idempotency-Key": "workspace-oversize-0001",
                }
            ),
        )
        assert (oversized.status_code, oversized.json()["code"]) == (
            413,
            "http.payload_too_large",
        )
        assert len(harness.store.workspaces) == workspace_count

        unexpected = harness.client.request(
            "GET",
            f"/api/v2/workspaces/{WORKSPACE_ID}",
            content=b"not-allowed",
            headers=_headers(),
        )
        assert (unexpected.status_code, unexpected.json()["code"]) == (
            400,
            "http.unexpected_body",
        )


def test_signed_keyset_cursor_pages_and_rejects_cross_collection_replay() -> None:
    """@brief cursor 绑定 principal、集合与 Workspace / A cursor is bound to principal, collection, and workspace."""

    with _harness() as harness:
        created = harness.client.post(
            "/api/v2/workspaces",
            json={"name": "Second", "slug": "second", "data_region": "cn"},
            headers=_headers(**{"Idempotency-Key": "workspace-page-create-0001"}),
        )
        assert created.status_code == 201

        first_page = harness.client.get(
            "/api/v2/workspaces",
            params={"limit": 1},
            headers=_headers(),
        )
        assert first_page.status_code == 200
        harness.validator.validate_definition("WorkspaceList", first_page.json())
        cursor = first_page.json()["page"]["next_cursor"]
        assert isinstance(cursor, str)
        second_page = harness.client.get(
            "/api/v2/workspaces",
            params={"limit": 1, "cursor": cursor},
            headers=_headers(),
        )
        assert second_page.status_code == 200
        assert second_page.json()["page"] == {"next_cursor": None, "has_more": False}
        assert first_page.json()["items"] != second_page.json()["items"]

        cross_collection = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/members",
            params={"cursor": cursor},
            headers=_headers(),
        )
        assert (cross_collection.status_code, cross_collection.json()["code"]) == (
            400,
            "http.cursor_invalid",
        )


def test_invitation_acceptance_member_update_and_revoke_flow() -> None:
    """@brief 邀请、收件人接受、成员修改与撤销形成完整 HTTP 流 / Invitation, recipient acceptance, member update, and revocation form a complete HTTP flow."""

    with _harness() as harness:
        created = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/invitations",
            json={"email": "amber@example.com", "role": "editor"},
            headers=_headers(**{"Idempotency-Key": "invitation-create-0001"}),
        )
        assert created.status_code == 201
        invitation = created.json()
        harness.validator.validate_definition("WorkspaceInvitation", invitation)
        assert invitation["email_hint"] == "a***@example.com"
        assert "amber@example.com" not in created.text
        invitation_path = created.headers["location"]

        recipient_view = harness.client.get(
            invitation_path,
            headers=_headers(actor="recipient"),
        )
        assert recipient_view.status_code == 200
        assert recipient_view.json() == invitation
        invitation_etag = recipient_view.headers["etag"]

        accepted = harness.client.post(
            f"{invitation_path}/acceptances",
            headers=_headers(
                actor="recipient",
                request_id="req_accept_0001",
                **{
                    "Idempotency-Key": "invitation-accept-0001",
                    "If-Match": invitation_etag,
                },
            ),
        )
        replay = harness.client.post(
            f"{invitation_path}/acceptances",
            headers=_headers(
                actor="recipient",
                request_id="req_accept_0002",
                **{
                    "Idempotency-Key": "invitation-accept-0001",
                    "If-Match": invitation_etag,
                },
            ),
        )
        assert accepted.status_code == replay.status_code == 201
        assert accepted.content == replay.content
        harness.validator.validate_definition("WorkspaceMember", accepted.json())
        member_path = accepted.headers["location"]

        changed = harness.client.patch(
            member_path,
            json={"role": "viewer"},
            headers=_headers(
                **{
                    "Content-Type": "application/merge-patch+json",
                    "If-Match": accepted.headers["etag"],
                }
            ),
        )
        assert changed.status_code == 200
        assert changed.json()["role"] == "viewer"
        removed = harness.client.delete(
            member_path,
            headers=_headers(**{"If-Match": changed.headers["etag"]}),
        )
        assert removed.status_code == 204
        assert removed.content == b""

        revocable = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/invitations",
            json={"email": "other@example.com", "role": "viewer"},
            headers=_headers(**{"Idempotency-Key": "invitation-create-0002"}),
        )
        assert revocable.status_code == 201
        revoked = harness.client.delete(
            revocable.headers["location"],
            headers=_headers(**{"If-Match": revocable.headers["etag"]}),
        )
        assert revoked.status_code == 204


def test_account_deletion_request_get_and_idempotent_cancellation() -> None:
    """@brief 账户删除创建、读取和取消都保持契约状态关联 / Account-deletion creation, read, and cancellation preserve contract state correlations."""

    with _harness() as harness:
        created = harness.client.post(
            "/api/v2/me/account-deletion-requests",
            json={
                "confirmation": "delete_my_account",
                "reauthentication_flow_id": "flow_reauth_0001",
            },
            headers=_headers(**{"Idempotency-Key": "account-delete-create-0001"}),
        )
        assert created.status_code == 201
        harness.validator.validate_definition("AccountDeletionRequest", created.json())
        assert created.json()["status"] == "scheduled"

        fetched = harness.client.get(created.headers["location"], headers=_headers())
        assert fetched.status_code == 200
        assert fetched.json() == created.json()
        cancelled = harness.client.post(
            f"{created.headers['location']}/cancellations",
            headers=_headers(
                request_id="req_cancel_0001",
                **{
                    "Idempotency-Key": "account-delete-cancel-0001",
                    "If-Match": fetched.headers["etag"],
                },
            ),
        )
        replay = harness.client.post(
            f"{created.headers['location']}/cancellations",
            headers=_headers(
                request_id="req_cancel_0002",
                **{
                    "Idempotency-Key": "account-delete-cancel-0001",
                    "If-Match": fetched.headers["etag"],
                },
            ),
        )
        assert cancelled.status_code == replay.status_code == 200
        assert cancelled.content == replay.content
        assert cancelled.json()["status"] == "cancelled"
        assert cancelled.json()["completed_at"] is None
        assert cancelled.json()["problem"] is None
        harness.validator.validate_definition("AccountDeletionRequest", cancelled.json())


def test_schema_errors_authorization_and_unknown_principal_use_v2_problems() -> None:
    """@brief 输入、scope 与本地 principal 失败均使用 V2 ProblemDetails / Input, scope, and local-principal failures all use V2 Problem Details."""

    with _harness() as harness:
        unknown_field = harness.client.post(
            "/api/v2/workspaces",
            content=('{"name":"Team","slug":"team","data_region":"global","legacy":true}'),
            headers=_headers(
                **{
                    "Content-Type": "application/json",
                    "Idempotency-Key": "unknown-field-test-0001",
                }
            ),
        )
        assert (unknown_field.status_code, unknown_field.json()["code"]) == (
            422,
            "contract.validation_failed",
        )
        harness.validator.validate_definition("ProblemDetails", unknown_field.json())

        insufficient = harness.client.get(
            "/api/v2/me",
            headers=_headers(actor="narrow"),
        )
        assert (insufficient.status_code, insufficient.json()["code"]) == (
            403,
            "oauth.insufficient_scope",
        )
        harness.validator.validate_definition("ProblemDetails", insufficient.json())

        phantom = harness.client.get(
            "/api/v2/me",
            headers=_headers(actor="phantom"),
        )
        assert (phantom.status_code, phantom.json()["code"]) == (
            401,
            "oauth.invalid_token",
        )
        assert "resource_metadata=" in phantom.headers["www-authenticate"]
        harness.validator.validate_definition("ProblemDetails", phantom.json())
