"""@brief API V2 Knowledge HTTP 适配器测试 / API V2 Knowledge HTTP adapter tests."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from backend.api.v2_http import CursorCodec, JsonValue, canonical_json_bytes
from backend.api.v2_knowledge import create_v2_knowledge_router, router_v2_knowledge
from backend.application.knowledge import (
    CreateConnectionAuthorizationSessionCommand,
    CreateConnectionCommand,
    CreateKnowledgeJobCommand,
    CreateKnowledgeSourceCommand,
    KnowledgeApplicationService,
    UpdateKnowledgeSourceCommand,
)
from backend.application.ports.knowledge import KnowledgePage, KnowledgePageRequest
from backend.application.ports.v2_idempotency import (
    IdempotencyConflict,
    IdempotencyPreparationId,
    IdempotencyRequest,
    ReplayableResponse,
)
from backend.domain.connections import (
    Connection,
    ConnectionAuthMethod,
    ConnectionAuthorizationFlow,
    ConnectionAuthorizationSession,
    ConnectionAuthorizationSessionId,
    ConnectionId,
    ConnectionProvider,
    ConnectionStatus,
)
from backend.domain.knowledge_retrieval import (
    KnowledgeAccessDecision,
    KnowledgeAccessEvaluationRequest,
    KnowledgeAccessEvaluationResult,
    KnowledgeCitation,
    KnowledgeSearchRequest,
    KnowledgeSearchResult,
)
from backend.domain.knowledge_sources import (
    KnowledgeIngestionState,
    KnowledgeOperation,
    KnowledgeSensitivity,
    KnowledgeSource,
    KnowledgeSourceId,
    KnowledgeSourceType,
    KnowledgeSourceVersion,
    KnowledgeSourceVersionId,
    KnowledgeVersionSnapshot,
    KnowledgeVersionStatus,
    KnowledgeVisibilityPolicy,
    ManualSourceInput,
    ModelRegion,
    PolicyEffect,
    PublicKnowledgeSourceConfig,
)
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM
from backend.domain.platform import Job, JobId, ResourceRef
from backend.domain.principals import ResourceMeta, UserId, WorkspaceId
from backend.domain.upload_sessions import (
    UploadCompletionClaim,
    UploadDeclaration,
    UploadGrant,
    UploadSessionId,
    UploadSessionView,
    UploadStatus,
)
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.v2_idempotency import (
    InMemoryIdempotencyExecutor,
    InMemoryV2IdempotencyStore,
)
from backend.package_resources import read_contract_schema_text

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 固定测试时刻 / Fixed test instant."""

USER_ID = UserId("user_knowledge_http_000001")
"""@brief 测试用户 / Test user."""

WORKSPACE_ID = WorkspaceId("workspace_knowledge_http_000001")
"""@brief 测试 Workspace / Test Workspace."""

CONNECTION_ID = ConnectionId("connection_knowledge_http_000001")
"""@brief 测试 Connection / Test Connection."""

SOURCE_ID = KnowledgeSourceId("knowledge_source_http_000001")
"""@brief 测试 KnowledgeSource / Test KnowledgeSource."""

VERSION_ID = KnowledgeSourceVersionId("knowledge_version_http_000001")
"""@brief 测试 KnowledgeSourceVersion / Test KnowledgeSourceVersion."""

UPLOAD_ID = UploadSessionId("upload_knowledge_http_000001")
"""@brief 测试 UploadSession / Test UploadSession."""

SHA256 = "0123456789abcdef" * 4
"""@brief 测试 SHA-256 / Test SHA-256."""

API_TOKEN = "secret-api-token-that-must-never-enter-a-receipt"
"""@brief 只能抵达 secret adapter fake 的 token / Token that may reach only the secret-adapter fake."""


def _policy(*, version: int = 1) -> KnowledgeVisibilityPolicy:
    """@brief 构造完整 visibility policy / Build a complete visibility policy.

    @param version policy 版本 / Policy version.
    @return 测试 policy / Test policy.
    """

    return KnowledgeVisibilityPolicy(
        KnowledgeSensitivity.CONFIDENTIAL,
        PolicyEffect.DENY,
        (),
        False,
        (ModelRegion.CN,),
        False,
        365,
        version,
    )


def _connection(*, revision: int = 1) -> Connection:
    """@brief 构造安全 Connection 投影 / Build a safe Connection projection.

    @param revision 资源 revision / Resource revision.
    @return Connection / Connection.
    """

    updated = NOW + timedelta(seconds=revision - 1)
    return Connection(
        ResourceMeta(CONNECTION_ID, revision, NOW, updated),
        WORKSPACE_ID,
        ConnectionProvider("github"),
        ConnectionAuthMethod.API_TOKEN,
        "GitHub",
        ConnectionStatus.ACTIVE,
        ("repo.read",),
        NOW,
    )


def _source(*, revision: int = 1, name: str = "Klee Notes") -> KnowledgeSource:
    """@brief 构造无私有字段泄漏的 manual source / Build a manual source without private-field leakage.

    @param revision 资源 revision / Resource revision.
    @param name 来源名称 / Source name.
    @return KnowledgeSource / KnowledgeSource.
    """

    return KnowledgeSource(
        ResourceMeta(SOURCE_ID, revision, NOW, NOW + timedelta(seconds=revision - 1)),
        WORKSPACE_ID,
        USER_ID,
        name,
        KnowledgeSourceType.MANUAL_NOTE,
        True,
        PublicKnowledgeSourceConfig(),
        _policy(version=revision),
        KnowledgeIngestionState(),
        VERSION_ID,
        1,
        ManualSourceInput("private manual content"),
    )


def _version() -> KnowledgeSourceVersion:
    """@brief 构造 pending KnowledgeSourceVersion / Build a pending KnowledgeSourceVersion.

    @return KnowledgeSourceVersion / KnowledgeSourceVersion.
    """

    return KnowledgeSourceVersion(
        ResourceMeta(VERSION_ID, 1, NOW, NOW),
        WORKSPACE_ID,
        KnowledgeVersionSnapshot(
            SOURCE_ID,
            1,
            SHA256,
            12,
            ResourceRef("upload_artifact", "artifact_knowledge_http_000001", 1),
        ),
        KnowledgeVersionStatus.PENDING,
    )


def _upload(*, completed: bool = False) -> UploadSessionView:
    """@brief 构造 created/completed upload view / Build a created or completed upload view.

    @param completed 是否完成 / Whether the upload is completed.
    @return UploadSessionView / UploadSessionView.
    """

    return UploadSessionView(
        UPLOAD_ID,
        WORKSPACE_ID,
        UploadStatus.COMPLETED if completed else UploadStatus.CREATED,
        UploadGrant(
            "https://objects.example.test/upload/object?signature=short-lived",
            {"content-type": "text/plain"},
        ),
        NOW + timedelta(minutes=15),
        (
            ResourceRef("upload_artifact", "artifact_knowledge_http_000001", 1)
            if completed
            else None
        ),
    )


def _job(kind: str) -> Job:
    """@brief 构造 queued Job / Build a queued Job.

    @param kind Job kind / Job kind.
    @return Job / Job.
    """

    return Job(
        ResourceMeta(JobId(f"job_{kind.replace('.', '_')}_http_000001"), 1, NOW, NOW),
        WORKSPACE_ID,
        kind,
        ResourceRef("knowledge_source", SOURCE_ID, 1),
    )


@dataclass(slots=True)
class _RecordingIdempotencyExecutor:
    """@brief 记录 generic executor 可见请求 / Record requests visible to the generic executor."""

    delegate: InMemoryIdempotencyExecutor
    """@brief 实际 in-memory executor / Actual in-memory executor."""

    requests: list[IdempotencyRequest]
    """@brief generic executor 接收到的请求 / Requests received by the generic executor."""

    async def execute(
        self,
        request: IdempotencyRequest,
        operation: Callable[[], Awaitable[ReplayableResponse]],
    ) -> ReplayableResponse:
        """@brief 记录后委托执行 / Record and delegate execution.

        @param request 幂等请求 / Idempotency request.
        @param operation 首次 claim callback / First-claim callback.
        @return 首次或重放 response / First or replayed response.
        """

        self.requests.append(request)
        return await self.delegate.execute(request, operation)

    async def execute_prepared[PreparedT](
        self,
        request: IdempotencyRequest,
        prepare: Callable[[IdempotencyPreparationId], Awaitable[PreparedT]],
        commit: Callable[[PreparedT], Awaitable[ReplayableResponse]],
    ) -> ReplayableResponse:
        """@brief 记录后委托分相执行 / Record and delegate split-phase execution.

        @param request 幂等请求 / Idempotency request.
        @param prepare 事务外准备 / External preparation.
        @param commit 最终提交 / Final commit.
        @return 首次或重放 response / First or replayed response.
        """

        self.requests.append(request)
        return await self.delegate.execute_prepared(request, prepare, commit)


class _FakeKnowledgeService:
    """@brief 独立 HTTP 测试用可观察 5.3 service fake / Observable section-5.3 service fake for isolated HTTP tests."""

    def __init__(self) -> None:
        """@brief 初始化领域结果与调用计数 / Initialize domain results and call counts."""

        self.connection = _connection()
        self.source = _source()
        self.seen_api_tokens: list[str] = []
        self.calls: dict[str, int] = {}
        self.last_page: KnowledgePageRequest | None = None
        self.last_source_command: CreateKnowledgeSourceCommand | None = None
        self.last_search: KnowledgeSearchRequest | None = None
        self.last_evaluation: KnowledgeAccessEvaluationRequest | None = None
        self.authorization_replays: dict[
            str,
            tuple[str, ConnectionAuthorizationSession],
        ] = {}

    def _called(self, name: str) -> None:
        """@brief 增加一个方法调用计数 / Increment one method call count.

        @param name 方法名 / Method name.
        @return 无返回值 / No return value.
        """

        self.calls[name] = self.calls.get(name, 0) + 1

    async def list_connections(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        page: KnowledgePageRequest | None = None,
    ) -> KnowledgePage[Connection]:
        """@brief 返回可观察分页 Connection / Return observable paginated Connections."""

        del principal, workspace_id
        requested = page or KnowledgePageRequest()
        self.last_page = requested
        second = replace(
            self.connection,
            meta=ResourceMeta(
                ConnectionId("connection_knowledge_http_000002"),
                1,
                NOW + timedelta(seconds=1),
                NOW + timedelta(seconds=1),
            ),
            display_name="GitLab",
        )
        if requested.after is not None:
            return KnowledgePage((second,), None)
        if requested.limit == 1:
            return KnowledgePage((self.connection,), "connection-position-1")
        return KnowledgePage((self.connection, second), None)

    async def create_connection_authorization_session(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        command: CreateConnectionAuthorizationSessionCommand,
    ) -> ConnectionAuthorizationSession:
        """@brief 返回 device authorization session / Return a device authorization session."""

        del principal, workspace_id
        existing = self.authorization_replays.get(command.idempotency_key_hash)
        if existing is not None:
            fingerprint, session = existing
            if fingerprint != command.request_fingerprint:
                raise IdempotencyConflict("idempotency.key_reused")
            return session
        self._called("create_connection_authorization_session")
        session = ConnectionAuthorizationSession(
            ConnectionAuthorizationSessionId("connection_auth_http_000001"),
            ConnectionProvider("github"),
            ConnectionAuthorizationFlow.DEVICE_CODE,
            NOW + timedelta(minutes=10),
            verification_uri="https://github.example.test/device",
            user_code="KLEE-CODE",
            poll_interval_ms=5_000,
        )
        self.authorization_replays[command.idempotency_key_hash] = (
            command.request_fingerprint,
            session,
        )
        return session

    async def create_connection(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        command: CreateConnectionCommand,
    ) -> Connection:
        """@brief 捕获 token 并返回脱敏 Connection / Capture the token and return a redacted Connection."""

        del principal, workspace_id
        self._called("create_connection")
        self.seen_api_tokens.append(command.api_token.reveal_to_secret_adapter())
        return self.connection

    async def prepare_connection_creation(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        command: CreateConnectionCommand,
        operation_id: IdempotencyPreparationId,
    ) -> CreateConnectionCommand:
        """@brief 返回 HTTP fake 的 prepared connection 命令 / Return a prepared connection command for the HTTP fake."""

        del principal, workspace_id, operation_id
        return command

    async def commit_connection_creation(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        prepared: CreateConnectionCommand,
    ) -> Connection:
        """@brief 复用 fake Connection 创建 / Reuse fake Connection creation."""

        return await self.create_connection(principal, workspace_id, prepared)

    async def get_connection_for_deletion(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
    ) -> Connection:
        """@brief 返回删除权限下的精确 Connection 快照 / Return an exact Connection snapshot under delete permission."""

        del principal, workspace_id, connection_id
        self._called("get_connection_for_deletion")
        return self.connection

    async def delete_connection(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
        *,
        expected_revision: int,
    ) -> Job:
        """@brief 记录 CAS revision 并返回撤销 Job / Record the CAS revision and return a revocation Job."""

        del principal, workspace_id, connection_id
        assert expected_revision == self.connection.meta.revision
        self._called("delete_connection")
        return _job("connection.revoke")

    async def list_knowledge_sources(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        page: KnowledgePageRequest | None = None,
    ) -> KnowledgePage[KnowledgeSource]:
        """@brief 返回分页来源 / Return paginated sources."""

        del principal, workspace_id
        requested = page or KnowledgePageRequest()
        self.last_page = requested
        if requested.after is not None:
            return KnowledgePage((_source(name="Second Source"),), None)
        if requested.limit == 1:
            return KnowledgePage((self.source,), "source-position-1")
        return KnowledgePage((self.source,), None)

    async def create_knowledge_source(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        command: CreateKnowledgeSourceCommand,
    ) -> KnowledgeSource:
        """@brief 记录 typed command 并返回来源 / Record the typed command and return a source."""

        del principal, workspace_id
        self._called("create_knowledge_source")
        self.last_source_command = command
        self.source = replace(
            self.source,
            name=command.name,
            visibility=command.visibility,
            source_input=command.source_input,
            source_type=command.source_input.source_type,
        )
        return self.source

    async def prepare_knowledge_source_creation(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        command: CreateKnowledgeSourceCommand,
        operation_id: IdempotencyPreparationId,
    ) -> CreateKnowledgeSourceCommand:
        """@brief 返回 fake prepared source 命令 / Return a fake prepared source command."""

        del principal, workspace_id, operation_id
        return command

    async def commit_knowledge_source_creation(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        prepared: CreateKnowledgeSourceCommand,
    ) -> KnowledgeSource:
        """@brief 复用 fake source 创建 / Reuse fake source creation."""

        return await self.create_knowledge_source(principal, workspace_id, prepared)

    async def get_knowledge_source(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
    ) -> KnowledgeSource:
        """@brief 返回当前来源 / Return the current source."""

        del principal, workspace_id, source_id
        self._called("get_knowledge_source")
        return self.source

    async def get_knowledge_source_for_deletion(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
    ) -> KnowledgeSource:
        """@brief 返回删除权限下的精确来源快照 / Return an exact source snapshot under delete permission."""

        del principal, workspace_id, source_id
        self._called("get_knowledge_source_for_deletion")
        return self.source

    async def update_knowledge_source(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        command: UpdateKnowledgeSourceCommand,
        *,
        expected_revision: int,
    ) -> KnowledgeSource:
        """@brief 应用测试 merge patch / Apply the test merge patch."""

        del principal, workspace_id, source_id
        assert expected_revision == self.source.meta.revision
        self._called("update_knowledge_source")
        self.source = self.source.revise(
            name=command.name,
            visibility=command.visibility,
            at=NOW + timedelta(minutes=1),
        )
        return self.source

    async def delete_knowledge_source(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        *,
        expected_revision: int,
    ) -> Job:
        """@brief 记录来源 CAS 并返回删除 Job / Record source CAS and return a deletion Job."""

        del principal, workspace_id, source_id
        assert expected_revision == self.source.meta.revision
        self._called("delete_knowledge_source")
        return _job("knowledge.delete")

    async def list_knowledge_source_versions(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        page: KnowledgePageRequest | None = None,
    ) -> KnowledgePage[KnowledgeSourceVersion]:
        """@brief 返回分页版本 / Return paginated versions."""

        del principal, workspace_id, source_id
        self.last_page = page or KnowledgePageRequest()
        return KnowledgePage((_version(),), None)

    async def create_knowledge_source_version(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        upload_session_id: UploadSessionId,
    ) -> KnowledgeSourceVersion:
        """@brief 返回新版本 / Return a new version."""

        del principal, workspace_id, source_id, upload_session_id
        self._called("create_knowledge_source_version")
        return _version()

    async def create_upload_session(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        declaration: UploadDeclaration,
    ) -> UploadSessionView:
        """@brief 验证 declaration 并返回 signed upload view / Validate the declaration and return a signed upload view."""

        del principal, workspace_id
        assert declaration.sha256 == SHA256
        self._called("create_upload_session")
        return _upload()

    async def prepare_upload_session_creation(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        declaration: UploadDeclaration,
        operation_id: IdempotencyPreparationId,
    ) -> UploadDeclaration:
        """@brief 返回 fake prepared upload declaration / Return a fake prepared upload declaration."""

        del principal, workspace_id, operation_id
        return declaration

    async def commit_upload_session_creation(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        prepared: UploadDeclaration,
    ) -> UploadSessionView:
        """@brief 复用 fake upload 创建 / Reuse fake upload creation."""

        return await self.create_upload_session(principal, workspace_id, prepared)

    async def complete_upload_session(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        claim: UploadCompletionClaim,
    ) -> UploadSessionView:
        """@brief 验证 completion claim 并返回 completed view / Validate the claim and return a completed view."""

        del principal, workspace_id, upload_id
        assert claim.sha256 == SHA256
        self._called("complete_upload_session")
        return _upload(completed=True)

    async def prepare_upload_completion(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        claim: UploadCompletionClaim,
        operation_id: IdempotencyPreparationId,
    ) -> tuple[UploadSessionId, UploadCompletionClaim]:
        """@brief 返回 fake prepared completion / Return a fake prepared completion."""

        del principal, workspace_id, operation_id
        return upload_id, claim

    async def commit_upload_completion(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        prepared: tuple[UploadSessionId, UploadCompletionClaim],
    ) -> UploadSessionView:
        """@brief 复用 fake upload completion / Reuse fake upload completion."""

        upload_id, claim = prepared
        return await self.complete_upload_session(
            principal,
            workspace_id,
            upload_id,
            claim,
        )

    async def create_ingestion_job(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        command: CreateKnowledgeJobCommand,
    ) -> Job:
        """@brief 返回 ingestion Job / Return an ingestion Job."""

        del principal, workspace_id, source_id
        assert command.force
        self._called("create_ingestion_job")
        return _job("knowledge.ingest")

    async def create_sync_job(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        command: CreateKnowledgeJobCommand,
    ) -> Job:
        """@brief 返回 sync Job / Return a sync Job."""

        del principal, workspace_id, source_id
        assert not command.force
        self._called("create_sync_job")
        return _job("knowledge.sync")

    async def search_knowledge(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        request: KnowledgeSearchRequest,
    ) -> KnowledgeSearchResult:
        """@brief 记录 typed search 并返回 citation / Record a typed search and return a citation."""

        del principal, workspace_id
        self.last_search = request
        self._called("search_knowledge")
        return KnowledgeSearchResult(
            request.query,
            (
                KnowledgeCitation(
                    SOURCE_ID,
                    VERSION_ID,
                    "line:1",
                    "Klee knows the answer.",
                    0.95,
                ),
            ),
            1,
        )

    async def evaluate_knowledge_access(
        self,
        principal: Any,
        workspace_id: WorkspaceId,
        request: KnowledgeAccessEvaluationRequest,
    ) -> KnowledgeAccessEvaluationResult:
        """@brief 记录 typed evaluation 并返回 allow decision / Record a typed evaluation and return allow."""

        del principal, workspace_id
        self.last_evaluation = request
        self._called("evaluate_knowledge_access")
        return KnowledgeAccessEvaluationResult(
            NOW,
            tuple(
                KnowledgeAccessDecision(source_id, PolicyEffect.ALLOW, 1, ("policy.agent_allow",))
                for source_id in request.source_ids
            ),
        )


@dataclass(slots=True)
class _Runtime:
    """@brief 可注入独立 Knowledge router 的 runtime / Runtime injected into the isolated Knowledge router."""

    knowledge_v2: KnowledgeApplicationService
    """@brief service fake 的静态接口视图 / Static service view of the fake."""

    contracts_v2: ContractValidator
    """@brief 权威 schema validator / Authoritative schema validator."""

    v2_cursor: CursorCodec
    """@brief 签名 cursor codec / Signed cursor codec."""

    v2_idempotency: _RecordingIdempotencyExecutor
    """@brief 可观察幂等 executor / Observable idempotency executor."""

    sensitive_idempotency_key: bytes
    """@brief 独立 secret-aware HMAC key / Independent secret-aware HMAC key."""


@dataclass(slots=True)
class _Harness:
    """@brief 组合 client、fake、validator 与 receipt 观测 / Bundle client, fake, validator, and receipt observations."""

    client: TestClient
    """@brief 测试 HTTP client / Test HTTP client."""

    service: _FakeKnowledgeService
    """@brief 可观察 service fake / Observable service fake."""

    validator: ContractValidator
    """@brief response validator / Response validator."""

    idempotency: _RecordingIdempotencyExecutor
    """@brief 可观察幂等 executor / Observable idempotency executor."""


@contextmanager
def _harness() -> Iterator[_Harness]:
    """@brief 启动只挂载 Knowledge router 的隔离 FastAPI app / Start an isolated app mounting only the Knowledge router.

    @return 活跃 harness / Active harness.
    """

    validator = ContractValidator.from_jsonc(read_contract_schema_text("v2"))
    service = _FakeKnowledgeService()
    recording = _RecordingIdempotencyExecutor(
        InMemoryIdempotencyExecutor(
            InMemoryV2IdempotencyStore(),
            retention=timedelta(days=30),
        ),
        [],
    )
    runtime = _Runtime(
        cast(KnowledgeApplicationService, service),
        validator,
        CursorCodec(b"knowledge-http-cursor-secret-00000001"),
        recording,
        b"knowledge-http-sensitive-idempotency-key-000001",
    )
    app = FastAPI()
    app.include_router(create_v2_knowledge_router(lambda _request: runtime))

    @app.middleware("http")
    async def verified_context(request: Request, call_next: Any) -> Any:
        """@brief 模拟生产 middleware 注入已验签 claims / Simulate production middleware claims.

        @param request 当前 request / Current request.
        @param call_next 下游 ASGI callable / Downstream ASGI callable.
        @return downstream response / Downstream response.
        """

        request.state.request_id = request.headers.get(
            "X-Request-Id",
            "request_knowledge_http_000001",
        )
        request.state.oauth_claims = {
            ACCESS_TOKEN_USER_ID_CLAIM: str(USER_ID),
            "sub": "subject_knowledge_http_000001",
            "client_id": "client_knowledge_http_000001",
            "scope": "workspace.read workspace.write knowledge.read knowledge.write",
        }
        return await call_next(request)

    with TestClient(app, raise_server_exceptions=False) as client:
        yield _Harness(client, service, validator, recording)


def _headers(
    *,
    key: str | None = None,
    etag: str | None = None,
    content_type: str | None = None,
) -> dict[str, str]:
    """@brief 构造 Knowledge transport headers / Build Knowledge transport headers.

    @param key 可选 Idempotency-Key / Optional Idempotency-Key.
    @param etag 可选 If-Match / Optional If-Match.
    @param content_type 可选 Content-Type / Optional Content-Type.
    @return header mapping / Header mapping.
    """

    headers = {"X-Request-Id": "request_knowledge_http_000001"}
    if key is not None:
        headers["Idempotency-Key"] = key
    if etag is not None:
        headers["If-Match"] = etag
    if content_type is not None:
        headers["Content-Type"] = content_type
    return headers


def _visibility_json(*, version: int = 1) -> dict[str, object]:
    """@brief 构造 schema 完整 visibility JSON / Build schema-complete visibility JSON.

    @param version policy 版本 / Policy version.
    @return JSON object / JSON object.
    """

    return {
        "sensitivity": "confidential",
        "default_effect": "deny",
        "agent_grants": [],
        "session_override_allowed": False,
        "allowed_model_regions": ["cn"],
        "allow_external_model_processing": False,
        "retention_days": 365,
        "policy_version": version,
    }


def _search_json() -> dict[str, object]:
    """@brief 构造完整 KnowledgeSearchRequest JSON / Build a complete KnowledgeSearchRequest JSON.

    @return JSON object / JSON object.
    """

    return {
        "query": "What does Klee know?",
        "selection": {
            "mode": "explicit",
            "include_source_ids": [str(SOURCE_ID)],
            "exclude_source_ids": [],
            "pinned_versions": [{"source_id": str(SOURCE_ID), "version_id": str(VERSION_ID)}],
            "agent_scope": "agent.research",
        },
        "top_k": 5,
        "filters": {"language": "en", "tags": ["cs", "math"]},
    }


def _evaluation_json() -> dict[str, object]:
    """@brief 构造完整 access evaluation JSON / Build a complete access-evaluation JSON.

    @return JSON object / JSON object.
    """

    return {
        "source_ids": [str(SOURCE_ID)],
        "agent_scope": "agent.research",
        "operation": "retrieve",
        "inference": {
            "quality_tier": "balanced",
            "latency_budget_ms": 2_000,
            "cost_tier": "standard",
            "data_region": "cn",
            "allow_provider_fallback": False,
            "allow_external_model_processing": False,
        },
    }


def test_router_exposes_exact_section_53_surface() -> None:
    """@brief router 只暴露 5.3 冻结的 17 条路由 / Expose exactly the 17 frozen section-5.3 routes.

    @return 无返回值 / No return value.
    """

    actual = {
        (method, route.path)
        for route in router_v2_knowledge.routes
        if isinstance(route, APIRoute)
        for method in route.methods or set()
    }
    expected = {
        ("GET", "/api/v2/workspaces/{workspace_id}/connections"),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/connection-authorization-sessions",
        ),
        ("POST", "/api/v2/workspaces/{workspace_id}/connections"),
        (
            "DELETE",
            "/api/v2/workspaces/{workspace_id}/connections/{connection_id}",
        ),
        ("GET", "/api/v2/workspaces/{workspace_id}/knowledge-sources"),
        ("POST", "/api/v2/workspaces/{workspace_id}/knowledge-sources"),
        (
            "GET",
            "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}",
        ),
        (
            "PATCH",
            "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}",
        ),
        (
            "DELETE",
            "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}",
        ),
        (
            "GET",
            "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/versions",
        ),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/versions",
        ),
        ("POST", "/api/v2/workspaces/{workspace_id}/upload-sessions"),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/upload-sessions/{upload_id}/completions",
        ),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/ingestion-jobs",
        ),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/knowledge-sources/{source_id}/sync-jobs",
        ),
        ("POST", "/api/v2/workspaces/{workspace_id}/knowledge-searches"),
        (
            "POST",
            "/api/v2/workspaces/{workspace_id}/knowledge-access-evaluations",
        ),
    }

    assert actual == expected
    for route in router_v2_knowledge.routes:
        if isinstance(route, APIRoute):
            assert route.openapi_extra is not None
            assert route.openapi_extra["x-api-v2-phase"] == 3


def test_connection_routes_bind_cursor_sensitive_idempotency_etag_and_secret_redaction() -> None:
    """@brief Connection 路由绑定 cursor、secret HMAC、ETag 与 replay / Bind cursor, secret HMAC, ETag, and replay.

    @return 无返回值 / No return value.
    """

    with _harness() as harness:
        first = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            params={"limit": 1},
            headers=_headers(),
        )
        assert first.status_code == 200
        harness.validator.validate_definition("ConnectionList", first.json())
        assert first.json()["page"]["next_cursor"]
        second = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            params={"cursor": first.json()["page"]["next_cursor"], "limit": 1},
            headers=_headers(),
        )
        assert second.status_code == 200
        assert second.json()["items"][0]["display_name"] == "GitLab"
        assert harness.service.last_page == KnowledgePageRequest(
            limit=1,
            after="connection-position-1",
        )

        authorization_body: dict[str, JsonValue] = {
            "provider": "github",
            "flow": "device_code",
            "requested_scopes": ["repo.read"],
        }
        missing_authorization_key = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connection-authorization-sessions",
            headers=_headers(),
            json=authorization_body,
        )
        assert missing_authorization_key.status_code == 400
        assert missing_authorization_key.json()["code"] == "http.idempotency_key_required"

        auth = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connection-authorization-sessions",
            headers=_headers(key="authorization-session-key-0001"),
            json=authorization_body,
        )
        auth_replay = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connection-authorization-sessions",
            headers=_headers(key="authorization-session-key-0001"),
            json=authorization_body,
        )
        assert auth.status_code == 201
        assert auth_replay.status_code == 201
        assert auth.content == auth_replay.content
        harness.validator.validate_definition("ConnectionAuthorizationSession", auth.json())
        assert auth.headers["cache-control"] == "no-store"
        assert auth_replay.headers["cache-control"] == "no-store"
        assert auth.json()["user_code"] == "KLEE-CODE"
        assert harness.service.calls["create_connection_authorization_session"] == 1
        assert len(harness.service.authorization_replays) == 1
        ((stored_key_hash, (stored_fingerprint, _stored_session)),) = (
            harness.service.authorization_replays.items()
        )
        assert len(stored_key_hash) == len(stored_fingerprint) == 64
        assert "authorization-session-key-0001" not in stored_key_hash
        assert harness.idempotency.requests == []

        changed_authorization = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connection-authorization-sessions",
            headers=_headers(key="authorization-session-key-0001"),
            json={**authorization_body, "requested_scopes": ["repo.read", "repo.write"]},
        )
        assert changed_authorization.status_code == 409
        assert changed_authorization.json()["code"] == "idempotency.key_reused"
        assert harness.service.calls["create_connection_authorization_session"] == 1
        assert harness.idempotency.requests == []

        missing_key = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            json={
                "provider": "github",
                "display_name": "GitHub",
                "api_token": API_TOKEN,
            },
            headers=_headers(),
        )
        assert missing_key.status_code == 400
        assert missing_key.json()["code"] == "http.idempotency_key_required"

        create_body: dict[str, JsonValue] = {
            "provider": "github",
            "display_name": "GitHub",
            "api_token": API_TOKEN,
        }
        created = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            json=create_body,
            headers=_headers(key="connection-create-key-0001"),
        )
        replay = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            json=create_body,
            headers=_headers(key="connection-create-key-0001"),
        )
        assert created.status_code == replay.status_code == 201
        assert created.content == replay.content
        assert created.headers["etag"] == replay.headers["etag"]
        assert created.headers["location"].endswith(f"/connections/{CONNECTION_ID}")
        harness.validator.validate_definition("Connection", created.json())
        assert API_TOKEN not in created.text
        assert harness.service.seen_api_tokens == [API_TOKEN]
        assert harness.service.calls["create_connection"] == 1
        generic_bodies = [request.canonical_body for request in harness.idempotency.requests]
        assert generic_bodies
        assert all(API_TOKEN.encode() not in body for body in generic_bodies)
        assert generic_bodies[0].startswith(b"hmac-sha256:")
        assert len(generic_bodies[0]) == len(b"hmac-sha256:") + 64
        assert generic_bodies[0] == b"hmac-sha256:" + hmac.new(
            b"knowledge-http-sensitive-idempotency-key-000001",
            canonical_json_bytes(create_body),
            hashlib.sha256,
        ).hexdigest().encode("ascii")

        changed_secret = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            json={**create_body, "api_token": "different-api-token-value"},
            headers=_headers(key="connection-create-key-0001"),
        )
        assert changed_secret.status_code == 409
        assert changed_secret.json()["code"] == "idempotency.key_reused"

        receipt_count = len(harness.idempotency.requests)
        deleted = harness.client.delete(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections/{CONNECTION_ID}",
            headers=_headers(
                etag=created.headers["etag"],
            ),
        )
        assert deleted.status_code == 202
        harness.validator.validate_definition("Job", deleted.json())
        assert harness.service.calls["get_connection_for_deletion"] == 1
        assert harness.service.calls["delete_connection"] == 1
        assert len(harness.idempotency.requests) == receipt_count


def test_connection_delete_stale_etag_returns_412_without_generic_receipt() -> None:
    """@brief stale Connection If-Match 返回 412 且不进入通用 receipt / Return 412 without a generic receipt.

    @return 无返回值 / No return value.
    """

    with _harness() as harness:
        first = harness.client.delete(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections/{CONNECTION_ID}",
            headers=_headers(
                etag='"stale-etag"',
            ),
        )
        assert first.status_code == 412
        assert first.json()["code"] == "http.precondition_failed"
        assert harness.service.calls["get_connection_for_deletion"] == 1
        assert "delete_connection" not in harness.service.calls
        assert harness.idempotency.requests == []


def test_knowledge_source_crud_versions_cursor_binding_and_private_projection() -> None:
    """@brief Source CRUD/versions 绑定 schema、cursor、ETag 且排除私有 input / Bind schemas, cursors, ETags, and private projection.

    @return 无返回值 / No return value.
    """

    with _harness() as harness:
        connection_page = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            params={"limit": 1},
            headers=_headers(),
        )
        foreign_cursor = connection_page.json()["page"]["next_cursor"]
        rejected = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources",
            params={"cursor": foreign_cursor, "limit": 1},
            headers=_headers(),
        )
        assert rejected.status_code == 400
        assert rejected.json()["code"] == "http.cursor_invalid"

        source_page = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources",
            params={"limit": 1},
            headers=_headers(),
        )
        assert source_page.status_code == 200
        harness.validator.validate_definition("KnowledgeSourceList", source_page.json())
        assert source_page.json()["page"]["next_cursor"]

        private_content = "manual secret body that is not part of public_config"
        source_body = {
            "name": "Klee Notes",
            "input": {"source_type": "manual_note", "content": private_content},
            "visibility": _visibility_json(),
        }
        created = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources",
            json=source_body,
            headers=_headers(key="knowledge-source-create-0001"),
        )
        replay = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources",
            json=source_body,
            headers=_headers(key="knowledge-source-create-0001"),
        )
        assert created.status_code == replay.status_code == 201
        assert created.content == replay.content
        harness.validator.validate_definition("KnowledgeSource", created.json())
        assert created.json()["public_config"] == {}
        assert private_content not in created.text
        assert harness.service.calls["create_knowledge_source"] == 1
        assert isinstance(harness.service.last_source_command, CreateKnowledgeSourceCommand)
        assert isinstance(harness.service.last_source_command.source_input, ManualSourceInput)

        fetched = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}",
            headers=_headers(),
        )
        assert fetched.status_code == 200
        assert fetched.headers["etag"] == created.headers["etag"]
        harness.validator.validate_definition("KnowledgeSource", fetched.json())

        patched = harness.client.patch(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}",
            json={
                "name": "Klee Research Notes",
                "visibility": _visibility_json(version=2),
            },
            headers=_headers(
                etag=fetched.headers["etag"],
                content_type="application/merge-patch+json",
            ),
        )
        assert patched.status_code == 200
        assert patched.json()["revision"] == 2
        assert patched.json()["name"] == "Klee Research Notes"
        assert patched.headers["etag"] != fetched.headers["etag"]
        harness.validator.validate_definition("KnowledgeSource", patched.json())

        stale_patch = harness.client.patch(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}",
            json={"name": "Stale Name"},
            headers=_headers(
                etag=fetched.headers["etag"],
                content_type="application/merge-patch+json",
            ),
        )
        assert stale_patch.status_code == 412

        receipt_count = len(harness.idempotency.requests)
        deleted = harness.client.delete(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}",
            headers=_headers(
                etag=patched.headers["etag"],
            ),
        )
        assert deleted.status_code == 202
        harness.validator.validate_definition("Job", deleted.json())
        assert harness.service.calls["delete_knowledge_source"] == 1
        assert harness.service.calls["get_knowledge_source_for_deletion"] == 1
        assert len(harness.idempotency.requests) == receipt_count

        versions = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}/versions",
            params={"limit": 7},
            headers=_headers(),
        )
        assert versions.status_code == 200
        harness.validator.validate_definition("KnowledgeSourceVersionList", versions.json())
        assert harness.service.last_page == KnowledgePageRequest(limit=7)

        version_created = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}/versions",
            json={"upload_session_id": str(UPLOAD_ID)},
            headers=_headers(key="knowledge-version-create-0001"),
        )
        assert version_created.status_code == 201
        assert version_created.headers["location"].endswith(f"/versions/{VERSION_ID}")
        harness.validator.validate_definition("KnowledgeSourceVersion", version_created.json())


def test_upload_job_search_and_access_routes_validate_all_inputs_and_outputs() -> None:
    """@brief Upload、Job、search 与 evaluation 端到端通过正式 schema / Validate Upload, Job, search, and evaluation through official schemas.

    @return 无返回值 / No return value.
    """

    with _harness() as harness:
        upload_body = {
            "filename": "notes.txt",
            "media_type": "text/plain",
            "size_bytes": 12,
            "sha256": SHA256,
        }
        upload = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/upload-sessions",
            json=upload_body,
            headers=_headers(key="upload-session-create-0001"),
        )
        upload_replay = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/upload-sessions",
            json=upload_body,
            headers=_headers(key="upload-session-create-0001"),
        )
        assert upload.status_code == upload_replay.status_code == 201
        assert upload.content == upload_replay.content
        assert upload.headers["cache-control"] == "no-store"
        assert upload.headers["location"].endswith(f"/upload-sessions/{UPLOAD_ID}")
        harness.validator.validate_definition("UploadSession", upload.json())
        assert harness.service.calls["create_upload_session"] == 1

        completed = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/upload-sessions/{UPLOAD_ID}/completions",
            json={"size_bytes": 12, "sha256": SHA256},
            headers=_headers(key="upload-session-complete-0001"),
        )
        completed_replay = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/upload-sessions/{UPLOAD_ID}/completions",
            json={"size_bytes": 12, "sha256": SHA256},
            headers=_headers(key="upload-session-complete-0001"),
        )
        assert completed.status_code == completed_replay.status_code == 200
        assert completed.content == completed_replay.content
        assert completed.headers["cache-control"] == "no-store"
        assert completed.json()["status"] == "completed"
        harness.validator.validate_definition("UploadSession", completed.json())
        assert harness.service.calls["complete_upload_session"] == 1

        ingestion = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}/ingestion-jobs",
            json={"force": True},
            headers=_headers(key="knowledge-ingestion-job-0001"),
        )
        ingestion_replay = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}/ingestion-jobs",
            json={"force": True},
            headers=_headers(key="knowledge-ingestion-job-0001"),
        )
        assert ingestion.status_code == ingestion_replay.status_code == 202
        assert ingestion.content == ingestion_replay.content
        assert ingestion.json()["kind"] == "knowledge.ingest"
        harness.validator.validate_definition("Job", ingestion.json())
        assert harness.service.calls["create_ingestion_job"] == 1

        sync = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-sources/{SOURCE_ID}/sync-jobs",
            json={},
            headers=_headers(key="knowledge-sync-job-0001"),
        )
        assert sync.status_code == 202
        assert sync.json()["kind"] == "knowledge.sync"
        harness.validator.validate_definition("Job", sync.json())

        search = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-searches",
            json=_search_json(),
            headers=_headers(),
        )
        assert search.status_code == 200
        assert search.headers["cache-control"] == "no-store"
        assert search.json()["citations"][0]["score"] == 0.95
        harness.validator.validate_definition("KnowledgeSearchResult", search.json())
        assert harness.service.last_search is not None
        assert harness.service.last_search.filters.values["language"] == "en"
        assert harness.service.last_search.selection.pinned_versions[0].version_id == VERSION_ID

        evaluation = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-access-evaluations",
            json=_evaluation_json(),
            headers=_headers(),
        )
        assert evaluation.status_code == 200
        assert evaluation.headers["cache-control"] == "no-store"
        assert evaluation.json()["decisions"][0]["effect"] == "allow"
        harness.validator.validate_definition(
            "KnowledgeAccessEvaluationResult",
            evaluation.json(),
        )
        assert harness.service.last_evaluation is not None
        assert harness.service.last_evaluation.operation is KnowledgeOperation.RETRIEVE


def test_transport_rejects_unknown_query_bodies_media_types_and_over_eight_mib() -> None:
    """@brief 5.3 transport 拒绝未知 query/body、错误媒体类型与超过 8 MiB / Reject invalid query/body/media type and over 8 MiB.

    @return 无返回值 / No return value.
    """

    with _harness() as harness:
        unknown_query = harness.client.get(
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            params={"unexpected": "1"},
            headers=_headers(),
        )
        assert unknown_query.status_code == 400
        assert unknown_query.json()["code"] == "http.invalid_query"

        unexpected_body = harness.client.request(
            "GET",
            f"/api/v2/workspaces/{WORKSPACE_ID}/connections",
            content=b"{}",
            headers=_headers(content_type="application/json"),
        )
        assert unexpected_body.status_code == 400
        assert unexpected_body.json()["code"] == "http.unexpected_body"

        wrong_media = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-searches",
            content=b"{}",
            headers=_headers(content_type="text/plain"),
        )
        assert wrong_media.status_code == 415

        unknown_field = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-searches",
            json={**_search_json(), "unexpected": True},
            headers=_headers(),
        )
        assert unknown_field.status_code == 422
        assert unknown_field.json()["code"] == "contract.validation_failed"

        oversized = harness.client.post(
            f"/api/v2/workspaces/{WORKSPACE_ID}/knowledge-searches",
            content=b"{}",
            headers={
                **_headers(content_type="application/json"),
                "Content-Length": str(8 * 1024 * 1024 + 1),
            },
        )
        assert oversized.status_code == 413
        assert oversized.json()["code"] == "http.payload_too_large"
