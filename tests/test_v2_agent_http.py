"""@brief API V2 Conversation 与 Agent HTTP 适配器测试 / API V2 Conversation and Agent HTTP adapter tests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from backend.api.v2_agent import create_v2_agent_router
from backend.api.v2_http import CursorCodec
from backend.application.agent_v2 import (
    AgentApplicationService,
    AgentMutationContext,
    CreateConversationCommand,
    CreateMessageCommand,
    ToolApprovalDecisionCommand,
)
from backend.application.ports.agent_v2 import AgentPage, AgentPageRequest
from backend.domain.agent_v2 import (
    AgentRunId,
    AgentRunSpec,
    AgentRunStatus,
    AgentRunView,
    CitationContentPart,
    Conversation,
    ConversationCapability,
    ConversationId,
    ConversationPatch,
    Message,
    MessageId,
    MessageRole,
    ResumeProposalContentPart,
    TextContentPart,
    ToolApprovalId,
    ToolApprovalStatus,
    ToolApprovalView,
    ToolDecision,
    ToolRisk,
)
from backend.domain.knowledge_retrieval import KnowledgeCitation
from backend.domain.knowledge_sources import KnowledgeSourceId, KnowledgeSourceVersionId
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM
from backend.domain.principals import ResourceMeta, TokenPrincipal, UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.v2_idempotency import (
    InMemoryIdempotencyExecutor,
    InMemoryV2IdempotencyStore,
)
from backend.package_resources import read_contract_schema_text

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 固定测试时刻 / Fixed test instant."""

USER_ID = UserId("user_agent_http_000001")
"""@brief 测试用户 / Test user."""

WORKSPACE_ID = WorkspaceId("workspace_agent_http_000001")
"""@brief 主测试 Workspace / Primary test Workspace."""

OTHER_WORKSPACE_ID = WorkspaceId("workspace_agent_http_000002")
"""@brief cursor 跨租户重放目标 / Cross-tenant cursor replay target."""

SEED_CONVERSATION_ID = ConversationId("conversation_agent_http_000001")
"""@brief 预置 Conversation 标识 / Seed Conversation identifier."""

SECOND_CONVERSATION_ID = ConversationId("conversation_agent_http_000002")
"""@brief 分页用第二 Conversation / Second Conversation used for paging."""

SEED_MESSAGE_ID = MessageId("message_agent_http_000001")
"""@brief 预置 assistant Message 标识 / Seed assistant Message identifier."""

SEED_RUN_ID = AgentRunId("agent_run_http_000001")
"""@brief 预置 Run 标识 / Seed Run identifier."""

SEED_APPROVAL_ID = ToolApprovalId("tool_approval_http_000001")
"""@brief 预置 ToolApproval 标识 / Seed ToolApproval identifier."""


@dataclass(slots=True)
class _FakeAgentService:
    """@brief 只观察 transport 输入输出的 Agent service fake / Agent service fake observing transport I/O."""

    conversations: dict[ConversationId, Conversation] = field(default_factory=dict)
    messages: dict[MessageId, Message] = field(default_factory=dict)
    runs: dict[AgentRunId, AgentRunView] = field(default_factory=dict)
    approvals: dict[ToolApprovalId, ToolApprovalView] = field(default_factory=dict)
    calls: dict[str, int] = field(default_factory=dict)
    deleted: set[ConversationId] = field(default_factory=set)

    def __post_init__(self) -> None:
        """@brief 安装覆盖 tagged union 与分页的公开安全 fixture / Install public-safe union and paging fixtures."""

        first = Conversation(
            ResourceMeta(SEED_CONVERSATION_ID, 3, NOW, NOW),
            WORKSPACE_ID,
            "Agent fixture",
            ConversationCapability.GENERAL,
        )
        second = Conversation(
            ResourceMeta(
                SECOND_CONVERSATION_ID,
                1,
                NOW + timedelta(seconds=1),
                NOW + timedelta(seconds=1),
            ),
            WORKSPACE_ID,
            None,
            ConversationCapability.RESUME_EDIT,
        )
        self.conversations = {first.meta.id: first, second.meta.id: second}
        self.messages[SEED_MESSAGE_ID] = Message(
            ResourceMeta(SEED_MESSAGE_ID, 1, NOW, NOW),
            WORKSPACE_ID,
            SEED_CONVERSATION_ID,
            1,
            MessageRole.ASSISTANT,
            None,
            (
                TextContentPart("公开回答"),
                CitationContentPart(
                    KnowledgeCitation(
                        KnowledgeSourceId("knowledge_source_http_000001"),
                        KnowledgeSourceVersionId("knowledge_version_http_000001"),
                        "page:1",
                        "公开引用",
                        0.75,
                    )
                ),
                ResumeProposalContentPart(
                    ResourceRef(
                        "resume_proposal",
                        "resume_proposal_http_000001",
                        2,
                    )
                ),
            ),
            source_run_id=SEED_RUN_ID,
        )
        self.runs[SEED_RUN_ID] = AgentRunView(
            ResourceMeta(SEED_RUN_ID, 1, NOW, NOW),
            WORKSPACE_ID,
            SEED_CONVERSATION_ID,
            SEED_MESSAGE_ID,
            ConversationCapability.GENERAL,
            AgentRunStatus.QUEUED,
        )
        self.approvals[SEED_APPROVAL_ID] = ToolApprovalView(
            ResourceMeta(SEED_APPROVAL_ID, 1, NOW, NOW),
            WORKSPACE_ID,
            SEED_RUN_ID,
            "calendar.create_event",
            "创建一个公开摘要中的会议",
            ToolRisk.HIGH,
            ToolApprovalStatus.PENDING,
            NOW + timedelta(minutes=10),
            None,
        )

    def _called(self, name: str) -> None:
        """@brief 记录一个用例调用 / Record one use-case call."""

        self.calls[name] = self.calls.get(name, 0) + 1

    async def list_conversations(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        page: AgentPageRequest,
    ) -> AgentPage[Conversation]:
        """@brief 返回稳定分页 Conversation / Return stably paged Conversations."""

        del principal
        self._called("list_conversations")
        items = sorted(
            (
                item
                for item in self.conversations.values()
                if item.workspace_id == workspace_id and item.meta.id not in self.deleted
            ),
            key=lambda item: (item.meta.created_at, item.meta.id),
        )
        start = 1 if page.after is not None else 0
        selected = tuple(items[start : start + page.limit])
        next_position = "conversation_after_first" if start == 0 and len(items) > page.limit else None
        return AgentPage(selected, next_position)

    async def create_conversation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateConversationCommand,
        context: AgentMutationContext,
    ) -> Conversation:
        """@brief 创建确定性 Conversation / Create a deterministic Conversation."""

        del principal, context
        self._called("create_conversation")
        identifier = ConversationId("conversation_created_http_000001")
        created = Conversation(
            ResourceMeta(identifier, 1, NOW + timedelta(minutes=1), NOW + timedelta(minutes=1)),
            workspace_id,
            command.title,
            command.capability,
        )
        self.conversations[identifier] = created
        return created

    async def get_conversation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Conversation:
        """@brief 返回路径 Workspace 内 Conversation / Return a Conversation within the path Workspace."""

        del principal
        self._called("get_conversation")
        item = self.conversations[conversation_id]
        assert item.workspace_id == workspace_id
        return item

    async def update_conversation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        patch: ConversationPatch,
        *,
        expected_revision: int,
        context: AgentMutationContext,
    ) -> Conversation:
        """@brief 精确检查 transport 传入 revision 并更新 / Check the transport revision and update."""

        del principal, context
        self._called("update_conversation")
        current = self.conversations[conversation_id]
        assert current.workspace_id == workspace_id
        assert current.meta.revision == expected_revision
        updated = current.update(patch, at=current.meta.updated_at + timedelta(seconds=1))
        self.conversations[conversation_id] = updated
        return updated

    async def get_conversation_for_update(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Conversation:
        """@brief 返回 update 权限 snapshot / Return the update-permission snapshot."""

        self._called("get_conversation_for_update")
        return await self.get_conversation(principal, workspace_id, conversation_id)

    async def get_conversation_for_deletion(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Conversation:
        """@brief 返回 delete 权限 snapshot / Return the delete-permission snapshot."""

        self._called("get_conversation_for_deletion")
        return await self.get_conversation(principal, workspace_id, conversation_id)

    async def get_conversation_for_message_creation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Conversation:
        """@brief 返回 message-create 权限 snapshot / Return the message-create-permission snapshot."""

        self._called("get_conversation_for_message_creation")
        return await self.get_conversation(principal, workspace_id, conversation_id)

    async def delete_conversation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        expected_revision: int,
        context: AgentMutationContext,
    ) -> None:
        """@brief 记录精确 revision 删除 / Record an exact-revision deletion."""

        del principal, context
        self._called("delete_conversation")
        current = self.conversations[conversation_id]
        assert current.workspace_id == workspace_id
        assert current.meta.revision == expected_revision
        self.deleted.add(conversation_id)

    async def list_messages(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[Message]:
        """@brief 返回稳定分页 Message / Return stably paged Messages."""

        del principal
        self._called("list_messages")
        items = sorted(
            (
                item
                for item in self.messages.values()
                if item.workspace_id == workspace_id
                and item.conversation_id == conversation_id
            ),
            key=lambda item: (item.sequence, item.meta.id),
        )
        start = 1 if page.after is not None else 0
        selected = tuple(items[start : start + page.limit])
        next_position = "message_after_first" if start == 0 and len(items) > page.limit else None
        return AgentPage(selected, next_position)

    async def create_message(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        command: CreateMessageCommand,
        *,
        expected_conversation_revision: int,
        context: AgentMutationContext,
    ) -> Message:
        """@brief 追加 Message 并推进 Conversation revision / Append a Message and advance the Conversation revision."""

        del principal, context
        self._called("create_message")
        conversation = self.conversations[conversation_id]
        assert conversation.workspace_id == workspace_id
        assert conversation.meta.revision == expected_conversation_revision
        identifier = MessageId("message_created_http_000001")
        message = Message(
            ResourceMeta(identifier, 1, NOW + timedelta(minutes=2), NOW + timedelta(minutes=2)),
            workspace_id,
            conversation_id,
            2,
            MessageRole.USER,
            command.parent_message_id,
            command.content,
        )
        self.messages[identifier] = message
        self.conversations[conversation_id] = replace(
            conversation,
            meta=conversation.meta.advance(conversation.meta.updated_at + timedelta(seconds=1)),
        )
        return message

    async def create_agent_run(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        spec: AgentRunSpec,
        context: AgentMutationContext,
    ) -> AgentRunView:
        """@brief 创建确定性 queued Run / Create a deterministic queued Run."""

        del principal, context
        self._called("create_agent_run")
        identifier = AgentRunId("agent_run_created_http_000001")
        run = AgentRunView(
            ResourceMeta(identifier, 1, NOW + timedelta(minutes=3), NOW + timedelta(minutes=3)),
            workspace_id,
            spec.conversation_id,
            spec.input_message_id,
            spec.capability,
            AgentRunStatus.QUEUED,
        )
        self.runs[identifier] = run
        return run

    async def get_agent_run(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
    ) -> AgentRunView:
        """@brief 返回路径 Workspace 内 Run / Return a Run in the path Workspace."""

        del principal
        self._called("get_agent_run")
        run = self.runs[run_id]
        assert run.workspace_id == workspace_id
        return run

    async def cancel_agent_run(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        *,
        expected_revision: int,
        context: AgentMutationContext,
    ) -> AgentRunView:
        """@brief 取消精确 revision Run / Cancel an exact-revision Run."""

        del principal, context
        self._called("cancel_agent_run")
        run = self.runs[run_id]
        assert run.workspace_id == workspace_id
        assert run.meta.revision == expected_revision
        cancelled = replace(
            run,
            meta=run.meta.advance(run.meta.updated_at + timedelta(seconds=1)),
            status=AgentRunStatus.CANCELLED,
        )
        self.runs[run_id] = cancelled
        return cancelled

    async def get_agent_run_for_cancellation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
    ) -> AgentRunView:
        """@brief 返回 cancel 权限 snapshot / Return the cancel-permission snapshot."""

        self._called("get_agent_run_for_cancellation")
        return await self.get_agent_run(principal, workspace_id, run_id)

    async def get_tool_approval(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
    ) -> ToolApprovalView:
        """@brief 返回公开 ToolApproval view / Return a public ToolApproval view."""

        del principal
        self._called("get_tool_approval")
        approval = self.approvals[approval_id]
        assert approval.workspace_id == workspace_id
        return approval

    async def decide_tool_approval(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
        command: ToolApprovalDecisionCommand,
        *,
        expected_revision: int,
        context: AgentMutationContext,
    ) -> ToolApprovalView:
        """@brief 决定精确 revision approval / Decide an exact-revision approval."""

        del context
        self._called("decide_tool_approval")
        approval = self.approvals[approval_id]
        assert approval.workspace_id == workspace_id
        assert approval.meta.revision == expected_revision
        status = (
            ToolApprovalStatus.APPROVED
            if command.decision is ToolDecision.APPROVE
            else ToolApprovalStatus.REJECTED
        )
        decided = replace(
            approval,
            meta=approval.meta.advance(approval.meta.updated_at + timedelta(seconds=1)),
            status=status,
            decision_by=ResourceRef("user", principal.user_id),
        )
        self.approvals[approval_id] = decided
        return decided

    async def get_tool_approval_for_decision(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
    ) -> ToolApprovalView:
        """@brief 返回 decision 权限 snapshot / Return the decision-permission snapshot."""

        self._called("get_tool_approval_for_decision")
        return await self.get_tool_approval(principal, workspace_id, approval_id)


@dataclass(slots=True)
class _Runtime:
    """@brief Agent adapter 的隔离 runtime / Isolated Agent-adapter runtime."""

    agent_v2: AgentApplicationService
    contracts_v2: ContractValidator
    v2_cursor: CursorCodec
    v2_idempotency: InMemoryIdempotencyExecutor


@dataclass(slots=True)
class _Harness:
    """@brief 组合 client、service 与 schema validator / Bundle client, service, and schema validator."""

    client: TestClient
    service: _FakeAgentService
    validator: ContractValidator


@contextmanager
def _harness() -> Iterator[_Harness]:
    """@brief 启动只挂载 Agent router 的隔离 FastAPI app / Start an isolated app mounting only the Agent router."""

    validator = ContractValidator.from_jsonc(read_contract_schema_text("v2"))
    service = _FakeAgentService()
    runtime = _Runtime(
        cast(AgentApplicationService, service),
        validator,
        CursorCodec(b"agent-http-cursor-secret-00000000001"),
        InMemoryIdempotencyExecutor(
            InMemoryV2IdempotencyStore(),
            retention=timedelta(days=30),
        ),
    )
    app = FastAPI()
    app.include_router(create_v2_agent_router(lambda _request: runtime))

    @app.middleware("http")
    async def verified_context(request: Request, call_next: Any) -> Any:
        """@brief 模拟生产 middleware 注入已验签 claims / Simulate verified claims middleware."""

        request.state.request_id = request.headers.get(
            "X-Request-Id",
            "request_agent_http_000001",
        )
        request.state.oauth_claims = {
            ACCESS_TOKEN_USER_ID_CLAIM: str(USER_ID),
            "sub": "subject_agent_http_000001",
            "client_id": "client_agent_http_000001",
            "scope": "workspace.read workspace.write agent.read agent.write",
        }
        return await call_next(request)

    with TestClient(app, raise_server_exceptions=False) as client:
        yield _Harness(client, service, validator)


def _headers(
    *,
    key: str | None = None,
    etag: str | None = None,
    merge_patch: bool = False,
    request_id: str = "request_agent_http_000001",
) -> dict[str, str]:
    """@brief 构造 Agent transport headers / Build Agent transport headers."""

    headers = {"X-Request-Id": request_id}
    if key is not None:
        headers["Idempotency-Key"] = key
    if etag is not None:
        headers["If-Match"] = etag
    if merge_patch:
        headers["Content-Type"] = "application/merge-patch+json"
    return headers


def _run_body(conversation_id: str, message_id: str) -> dict[str, object]:
    """@brief 构造正式 CreateAgentRunRequest / Build a contract-valid CreateAgentRunRequest."""

    return {
        "conversation_id": conversation_id,
        "input_message_id": message_id,
        "capability": "general",
        "context_refs": [],
        "knowledge": {
            "mode": "none",
            "include_source_ids": [],
            "exclude_source_ids": [],
            "pinned_versions": [],
            "agent_scope": "general_agent",
        },
        "inference": {
            "quality_tier": "balanced",
            "latency_budget_ms": 10000,
            "cost_tier": "standard",
            "data_region": "cn",
            "allow_provider_fallback": False,
            "allow_external_model_processing": False,
        },
        "output_modes": ["text"],
        "response_locale": "zh-CN",
    }


def test_route_inventory_and_openapi_contract_markers_are_exact() -> None:
    """@brief 验证 5.4 的 method/path/schema/status 精确集合 / Verify exact method/path/schema/status inventory."""

    router = create_v2_agent_router()
    routes = [route for route in router.routes if isinstance(route, APIRoute)]
    inventory: dict[tuple[str, str], APIRoute] = {}
    for route in routes:
        assert route.methods is not None and len(route.methods) == 1
        assert route.openapi_extra is not None
        assert route.openapi_extra["x-api-v2-phase"] == 4
        inventory[(next(iter(route.methods)), route.path)] = route
    assert len(inventory) == 12
    assert set(inventory) == {
        ("GET", "/api/v2/workspaces/{workspace_id}/conversations"),
        ("POST", "/api/v2/workspaces/{workspace_id}/conversations"),
        ("GET", "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}"),
        ("PATCH", "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}"),
        ("DELETE", "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}"),
        ("GET", "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}/messages"),
        ("POST", "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}/messages"),
        ("POST", "/api/v2/workspaces/{workspace_id}/agent-runs"),
        ("GET", "/api/v2/workspaces/{workspace_id}/agent-runs/{run_id}"),
        ("POST", "/api/v2/workspaces/{workspace_id}/agent-runs/{run_id}/cancellations"),
        ("GET", "/api/v2/workspaces/{workspace_id}/tool-approvals/{approval_id}"),
        ("POST", "/api/v2/workspaces/{workspace_id}/tool-approvals/{approval_id}/decisions"),
    }
    assert inventory[("DELETE", "/api/v2/workspaces/{workspace_id}/conversations/{conversation_id}")].status_code == 204
    assert inventory[("POST", "/api/v2/workspaces/{workspace_id}/conversations")].status_code == 201
    assert inventory[("POST", "/api/v2/workspaces/{workspace_id}/agent-runs/{run_id}/cancellations")].status_code == 200


def test_all_twelve_routes_status_etag_location_and_schema() -> None:
    """@brief 端到端覆盖十二条路由的成功协议 / Cover successful protocol semantics for all twelve routes."""

    with _harness() as harness:
        client = harness.client
        workspace = str(WORKSPACE_ID)

        listed = client.get(f"/api/v2/workspaces/{workspace}/conversations")
        assert listed.status_code == 200
        harness.validator.validate_definition("ConversationList", listed.json())

        created = client.post(
            f"/api/v2/workspaces/{workspace}/conversations",
            json={"capability": "general", "title": "Klee Agent"},
            headers=_headers(key="conversation-key-000001"),
        )
        assert created.status_code == 201
        assert created.headers["location"].endswith("/conversation_created_http_000001")
        assert created.headers["etag"].startswith('"sha256-')
        conversation_id = created.json()["id"]
        harness.validator.validate_definition("Conversation", created.json())

        fetched = client.get(
            f"/api/v2/workspaces/{workspace}/conversations/{conversation_id}"
        )
        assert fetched.status_code == 200
        assert fetched.headers["etag"] == created.headers["etag"]

        updated = client.patch(
            f"/api/v2/workspaces/{workspace}/conversations/{conversation_id}",
            json={"title": None, "status": "active"},
            headers=_headers(etag=fetched.headers["etag"], merge_patch=True),
        )
        assert updated.status_code == 200
        assert updated.headers["etag"] != fetched.headers["etag"]
        assert updated.json()["title"] is None

        messages = client.get(
            f"/api/v2/workspaces/{workspace}/conversations/{conversation_id}/messages"
        )
        assert messages.status_code == 200
        harness.validator.validate_definition("MessageList", messages.json())

        message = client.post(
            f"/api/v2/workspaces/{workspace}/conversations/{conversation_id}/messages",
            json={
                "parent_message_id": None,
                "content": [{"type": "text", "text": "请给我一个公开答案"}],
            },
            headers=_headers(key="message-key-000000001", etag=updated.headers["etag"]),
        )
        assert message.status_code == 201
        assert message.headers["location"].endswith("/message_created_http_000001")
        harness.validator.validate_definition("Message", message.json())
        message_id = message.json()["id"]

        run = client.post(
            f"/api/v2/workspaces/{workspace}/agent-runs",
            json=_run_body(conversation_id, message_id),
            headers=_headers(key="agent-run-key-000001"),
        )
        assert run.status_code == 201
        assert run.headers["location"].endswith("/agent_run_created_http_000001")
        harness.validator.validate_definition("AgentRun", run.json())
        run_id = run.json()["id"]

        fetched_run = client.get(f"/api/v2/workspaces/{workspace}/agent-runs/{run_id}")
        assert fetched_run.status_code == 200
        assert fetched_run.headers["etag"] == run.headers["etag"]

        cancelled = client.post(
            f"/api/v2/workspaces/{workspace}/agent-runs/{run_id}/cancellations",
            headers=_headers(
                key="agent-cancel-key-0001",
                etag=fetched_run.headers["etag"],
            ),
        )
        assert cancelled.status_code == 200
        assert cancelled.json()["status"] == "cancelled"
        assert cancelled.headers["etag"] != fetched_run.headers["etag"]

        approval = client.get(
            f"/api/v2/workspaces/{workspace}/tool-approvals/{SEED_APPROVAL_ID}"
        )
        assert approval.status_code == 200
        harness.validator.validate_definition("ToolApproval", approval.json())

        decided = client.post(
            f"/api/v2/workspaces/{workspace}/tool-approvals/{SEED_APPROVAL_ID}/decisions",
            json={"decision": "approve"},
            headers=_headers(
                key="approval-decision-key-1",
                etag=approval.headers["etag"],
            ),
        )
        assert decided.status_code == 200
        assert decided.json()["status"] == "approved"
        harness.validator.validate_definition("ToolApproval", decided.json())

        current = client.get(
            f"/api/v2/workspaces/{workspace}/conversations/{conversation_id}"
        )
        deleted = client.delete(
            f"/api/v2/workspaces/{workspace}/conversations/{conversation_id}",
            headers=_headers(etag=current.headers["etag"]),
        )
        assert deleted.status_code == 204
        assert deleted.content == b""
        assert harness.service.calls["delete_conversation"] == 1


def test_idempotency_replays_creation_and_rejects_fingerprint_reuse() -> None:
    """@brief 验证 byte-exact replay 与同 key 异指纹冲突 / Verify byte-exact replay and fingerprint conflict."""

    with _harness() as harness:
        path = f"/api/v2/workspaces/{WORKSPACE_ID}/conversations"
        body = {"capability": "general", "title": "Replay"}
        first = harness.client.post(
            path,
            json=body,
            headers=_headers(key="replay-key-00000001", request_id="request_agent_first_000001"),
        )
        replay = harness.client.post(
            path,
            json=body,
            headers=_headers(key="replay-key-00000001", request_id="request_agent_second_00001"),
        )
        assert first.status_code == replay.status_code == 201
        assert first.content == replay.content
        assert first.headers["etag"] == replay.headers["etag"]
        assert first.headers["location"] == replay.headers["location"]
        assert replay.headers["x-request-id"] == "request_agent_second_00001"
        assert harness.service.calls["create_conversation"] == 1

        conflict = harness.client.post(
            path,
            json={"capability": "general", "title": "Different"},
            headers=_headers(key="replay-key-00000001"),
        )
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "idempotency.key_reused"
        assert harness.service.calls["create_conversation"] == 1


def test_if_match_media_type_unknown_fields_and_delete_key_rules() -> None:
    """@brief 验证强条件请求、严格 JSON 与纯 DELETE 不强制幂等键 / Verify preconditions, strict JSON, and DELETE key rules."""

    with _harness() as harness:
        path = f"/api/v2/workspaces/{WORKSPACE_ID}/conversations/{SEED_CONVERSATION_ID}"
        current = harness.client.get(path)
        assert current.status_code == 200

        missing = harness.client.patch(
            path,
            json={"title": "missing"},
            headers=_headers(merge_patch=True),
        )
        assert missing.status_code == 412
        assert missing.json()["code"] == "http.precondition_failed"

        weak = harness.client.patch(
            path,
            json={"title": "weak"},
            headers=_headers(etag="W/" + current.headers["etag"], merge_patch=True),
        )
        assert weak.status_code == 412

        wrong_media = harness.client.patch(
            path,
            json={"title": "json"},
            headers=_headers(etag=current.headers["etag"]),
        )
        assert wrong_media.status_code == 415

        unknown = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/conversations",
            json={"capability": "general", "title": None, "secret": "must reject"},
            headers=_headers(key="unknown-field-key-0001"),
        )
        assert unknown.status_code == 422
        assert unknown.json()["code"] == "contract.validation_failed"

        no_key = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/conversations",
            json={"capability": "general", "title": None},
            headers=_headers(),
        )
        assert no_key.status_code == 400
        assert no_key.json()["code"] == "http.idempotency_key_required"

        deleted = harness.client.delete(
            path,
            headers=_headers(etag=current.headers["etag"]),
        )
        assert deleted.status_code == 204
        assert harness.service.calls["delete_conversation"] == 1


def test_cursor_is_bound_to_workspace_collection_and_conversation() -> None:
    """@brief 验证 cursor 不能跨 Workspace 或 collection replay / Verify cursor cannot replay across Workspace or collection."""

    with _harness() as harness:
        page = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/conversations?limit=1"
        )
        assert page.status_code == 200
        cursor = page.json()["page"]["next_cursor"]
        assert isinstance(cursor, str)

        other_workspace = harness.client.get(
            f"/api/v2/workspaces/{OTHER_WORKSPACE_ID}/conversations",
            params={"cursor": cursor, "limit": 1},
        )
        assert other_workspace.status_code == 400
        assert other_workspace.json()["code"] == "http.cursor_invalid"

        other_collection = harness.client.get(
            (
                f"/api/v2/workspaces/{WORKSPACE_ID}/conversations/"
                f"{SEED_CONVERSATION_ID}/messages"
            ),
            params={"cursor": cursor, "limit": 1},
        )
        assert other_collection.status_code == 400
        assert other_collection.json()["code"] == "http.cursor_invalid"

        unknown_query = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/conversations?offset=1"
        )
        assert unknown_query.status_code == 400
        assert unknown_query.json()["code"] == "http.invalid_query"


def test_public_projections_never_leak_internal_agent_state() -> None:
    """@brief 验证 Message、Run、Approval 不泄露私有字段 / Verify public projections exclude private fields."""

    with _harness() as harness:
        messages = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/conversations/"
            f"{SEED_CONVERSATION_ID}/messages"
        )
        assert messages.status_code == 200
        message = messages.json()["items"][0]
        assert set(message) == {
            "id",
            "revision",
            "created_at",
            "updated_at",
            "workspace_id",
            "conversation_id",
            "role",
            "parent_message_id",
            "content",
        }
        assert [part["type"] for part in message["content"]] == [
            "text",
            "citation",
            "resume_proposal",
        ]
        serialized = messages.text
        assert "source_run_id" not in serialized
        assert "sequence" not in serialized
        assert "chain_of_thought" not in serialized

        run = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/agent-runs/{SEED_RUN_ID}"
        )
        assert run.status_code == 200
        assert "job_id" not in run.text
        assert "grant" not in run.text
        assert "active_tool_call_id" not in run.text

        approval = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/tool-approvals/{SEED_APPROVAL_ID}"
        )
        assert approval.status_code == 200
        assert "invocation" not in approval.text
        assert "tool_call_id" not in approval.text
