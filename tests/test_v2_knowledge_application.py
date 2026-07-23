"""@brief API v2 5.3 应用服务、授权与租户隔离测试 / API v2 section-5.3 application tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from backend.application.knowledge import (
    V2_KNOWLEDGE_ENDPOINT_METHODS,
    CreateConnectionAuthorizationSessionCommand,
    CreateConnectionCommand,
    CreateKnowledgeJobCommand,
    CreateKnowledgeSourceCommand,
    KnowledgeApplicationService,
    KnowledgePreconditionFailed,
    KnowledgeResourceNotFound,
    UpdateKnowledgeSourceCommand,
)
from backend.application.ports.access import (
    WORKSPACE_AUTHORIZATION_MATRIX,
    AccessAuthorizer,
    AuthorizationDenied,
)
from backend.application.ports.knowledge import (
    ConnectionAuthorizationLaunch,
    HybridSearchResponse,
    IssuedUploadGrant,
    KnowledgeCasMismatch,
    KnowledgePage,
    KnowledgePageRequest,
    ProvisionedConnectionCredential,
    UploadVerificationRejected,
)
from backend.application.ports.v2_idempotency import (
    IdempotencyConflict,
    IdempotencyPreparationId,
)
from backend.domain.connections import (
    ConnectionAggregate,
    ConnectionAuthorizationFlow,
    ConnectionAuthorizationRecord,
    ConnectionAuthorizationSessionId,
    ConnectionId,
    ConnectionProvider,
    CredentialReference,
    ProviderSessionReference,
    SecretValue,
)
from backend.domain.knowledge_jobs import KnowledgeJobSpec, KnowledgeOutboxEvent
from backend.domain.knowledge_retrieval import (
    HybridScore,
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeAccessEvaluationRequest,
    KnowledgeSearchHit,
    KnowledgeSearchRequest,
    KnowledgeSelection,
    KnowledgeSelectionMode,
    KnowledgeVersionPin,
    SearchFilters,
)
from backend.domain.knowledge_sources import (
    FileSourceInput,
    KnowledgeOperation,
    KnowledgeSensitivity,
    KnowledgeSource,
    KnowledgeSourceId,
    KnowledgeSourceType,
    KnowledgeSourceVersion,
    KnowledgeSourceVersionId,
    KnowledgeVisibilityPolicy,
    ManualSourceInput,
    ModelRegion,
    PolicyEffect,
    UrlSourceInput,
)
from backend.domain.platform import Job, ResourceRef
from backend.domain.principals import (
    ClientId,
    MembershipId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.upload_sessions import (
    UploadCompletionClaim,
    UploadDeclaration,
    UploadGrant,
    UploadSession,
    UploadSessionId,
    VerifiedUpload,
)
from backend.domain.users import User
from backend.domain.workspaces import Membership, MemberStatus, WorkspaceRole

_NOW = datetime(2026, 7, 23, 9, 0, tzinfo=UTC)
"""@brief 应用测试固定时刻 / Fixed instant for application tests."""

_WORKSPACE_A = WorkspaceId("ws_knowledge_alpha")
"""@brief 主测试 Workspace / Primary test Workspace."""

_WORKSPACE_B = WorkspaceId("ws_knowledge_beta")
"""@brief 隔离测试 Workspace / Isolation-test Workspace."""

_USER_ID = UserId("usr_knowledge_klee")
"""@brief 测试用户 / Test user."""

_SHA256 = "0123456789abcdef" * 4
"""@brief 固定测试 SHA-256 / Fixed test SHA-256."""


class _Clock:
    """@brief 返回固定 UTC 时间的测试时钟 / Test clock returning a fixed UTC instant."""

    def now(self) -> datetime:
        """@brief 返回固定时刻 / Return the fixed instant.

        @return 固定 UTC 时间 / Fixed UTC time.
        """
        return _NOW


@dataclass(slots=True)
class _Ids:
    """@brief 生成确定性且合法的不透明 ID / Generate deterministic valid opaque IDs.

    @param value 当前序号 / Current ordinal.
    """

    value: int = 0

    def __call__(self, prefix: str) -> str:
        """@brief 生成下一 ID / Generate the next ID.

        @param prefix 领域前缀 / Domain prefix.
        @return 确定性 ID / Deterministic ID.
        """
        self.value += 1
        return f"{prefix}_{self.value:08d}"


@dataclass(slots=True)
class _AccessRepository:
    """@brief AccessAuthorizer 所需最小用户与 membership store / Minimal store used by AccessAuthorizer.

    @param user 测试用户 / Test user.
    @param memberships Workspace memberships / Workspace memberships.
    """

    user: User
    memberships: dict[WorkspaceId, Membership]

    async def get_user(self, user_id: UserId) -> User | None:
        """@brief 按 ID 读取测试用户 / Read the test user by ID."""
        return self.user if user_id == self.user.meta.id else None

    async def get_membership(
        self,
        workspace_id: WorkspaceId,
        user_id: UserId,
    ) -> Membership | None:
        """@brief 读取路径 Workspace membership / Read membership in the path Workspace."""
        membership = self.memberships.get(workspace_id)
        return membership if membership is not None and membership.user_id == user_id else None


@dataclass(slots=True)
class _Repository:
    """@brief Workspace-first 的内存 5.3 repository fake / Workspace-first in-memory section-5.3 repository fake."""

    connections: dict[tuple[WorkspaceId, ConnectionId], ConnectionAggregate] = field(
        default_factory=dict
    )
    authorization_records: dict[
        tuple[WorkspaceId, ConnectionAuthorizationSessionId], ConnectionAuthorizationRecord
    ] = field(default_factory=dict)
    sources: dict[tuple[WorkspaceId, KnowledgeSourceId], KnowledgeSource] = field(
        default_factory=dict
    )
    versions: dict[
        tuple[WorkspaceId, KnowledgeSourceId, KnowledgeSourceVersionId], KnowledgeSourceVersion
    ] = field(default_factory=dict)
    uploads: dict[tuple[WorkspaceId, UploadSessionId], UploadSession] = field(default_factory=dict)

    async def list_connections(
        self,
        workspace_id: WorkspaceId,
        page: KnowledgePageRequest,
    ) -> KnowledgePage[Any]:
        """@brief 列出一个 Workspace 的 Connection / List Connections in one Workspace."""
        items = tuple(
            aggregate.connection
            for (owner, _), aggregate in sorted(
                self.connections.items(), key=lambda item: str(item[0][1])
            )
            if owner == workspace_id
        )[: page.limit]
        return KnowledgePage(items, None)

    async def get_connection(
        self,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAggregate | None:
        """@brief 读取 Workspace-scoped Connection / Read a Workspace-scoped Connection."""
        return self.connections.get((workspace_id, connection_id))

    async def add_connection(self, connection: ConnectionAggregate) -> None:
        """@brief 添加 Connection / Add a Connection."""
        key = (connection.connection.workspace_id, connection.connection.meta.id)
        self.connections[key] = connection

    async def save_connection(
        self,
        connection: ConnectionAggregate,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 模拟 revision CAS / Simulate revision CAS."""
        key = (connection.connection.workspace_id, connection.connection.meta.id)
        current = self.connections.get(key)
        if current is None or current.connection.meta.revision != expected_revision:
            raise KnowledgeCasMismatch
        self.connections[key] = connection

    async def add_authorization_record(self, record: ConnectionAuthorizationRecord) -> None:
        """@brief 添加授权记录 / Add an authorization record."""
        self.authorization_records[(record.ownership.workspace_id, record.session.id)] = record

    async def get_authorization_record_by_idempotency(
        self,
        workspace_id: WorkspaceId,
        created_by: UserId,
        idempotency_key_hash: str,
        *,
        for_update: bool = False,
    ) -> ConnectionAuthorizationRecord | None:
        """@brief 按专用 replay scope 读取授权记录 / Read authorization by dedicated replay scope."""

        del for_update
        return next(
            (
                record
                for record in self.authorization_records.values()
                if record.ownership.workspace_id == workspace_id
                and record.ownership.created_by == created_by
                and record.idempotency.key_hash == idempotency_key_hash
            ),
            None,
        )

    async def get_authorization_record(
        self,
        workspace_id: WorkspaceId,
        session_id: ConnectionAuthorizationSessionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAuthorizationRecord | None:
        """@brief 读取授权记录 / Read an authorization record."""
        return self.authorization_records.get((workspace_id, session_id))

    async def save_authorization_record(
        self,
        record: ConnectionAuthorizationRecord,
        *,
        expected_state: str,
    ) -> None:
        """@brief 模拟授权 state CAS / Simulate authorization-state CAS."""
        key = (record.ownership.workspace_id, record.session.id)
        current = self.authorization_records.get(key)
        if current is None or current.state.value != expected_state:
            raise KnowledgeCasMismatch
        self.authorization_records[key] = record

    async def list_sources(
        self,
        workspace_id: WorkspaceId,
        page: KnowledgePageRequest,
    ) -> KnowledgePage[Any]:
        """@brief 列出一个 Workspace 的来源 / List sources in one Workspace."""
        items = tuple(
            source
            for (owner, _), source in sorted(self.sources.items(), key=lambda item: str(item[0][1]))
            if owner == workspace_id
        )[: page.limit]
        return KnowledgePage(items, None)

    async def get_source(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        *,
        for_update: bool = False,
    ) -> KnowledgeSource | None:
        """@brief 读取 Workspace-scoped 来源 / Read a Workspace-scoped source."""
        return self.sources.get((workspace_id, source_id))

    async def list_policy_default_sources(
        self,
        workspace_id: WorkspaceId,
        *,
        include_source_ids: tuple[KnowledgeSourceId, ...],
        exclude_source_ids: tuple[KnowledgeSourceId, ...],
        limit: int,
    ) -> tuple[KnowledgeSource, ...]:
        """@brief 解析有界 policy-default 来源 / Resolve bounded policy-default sources."""
        excluded = set(exclude_source_ids)
        sources = [
            source
            for (owner, source_id), source in self.sources.items()
            if owner == workspace_id and source_id not in excluded
        ]
        return tuple(sorted(sources, key=lambda source: str(source.meta.id))[:limit])

    async def add_source(
        self,
        source: KnowledgeSource,
        initial_version: KnowledgeSourceVersion | None,
    ) -> None:
        """@brief 添加来源与可选首版本 / Add a source and optional first version."""
        self.sources[(source.workspace_id, source.meta.id)] = source
        if initial_version is not None:
            self.versions[
                (
                    initial_version.workspace_id,
                    initial_version.snapshot.source_id,
                    initial_version.meta.id,
                )
            ] = initial_version

    async def save_source(
        self,
        source: KnowledgeSource,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 模拟来源 revision CAS / Simulate source revision CAS."""
        key = (source.workspace_id, source.meta.id)
        current = self.sources.get(key)
        if current is None or current.meta.revision != expected_revision:
            raise KnowledgeCasMismatch
        self.sources[key] = source

    async def list_versions(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        page: KnowledgePageRequest,
    ) -> KnowledgePage[Any]:
        """@brief 按版本号列出来源版本 / List source versions by version number."""
        items = tuple(
            sorted(
                (
                    version
                    for (owner, source, _), version in self.versions.items()
                    if owner == workspace_id and source == source_id
                ),
                key=lambda version: version.snapshot.version_number,
            )[: page.limit]
        )
        return KnowledgePage(items, None)

    async def get_version(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        version_id: KnowledgeSourceVersionId,
    ) -> KnowledgeSourceVersion | None:
        """@brief 读取三元组 scoped 版本 / Read a tuple-scoped version."""
        return self.versions.get((workspace_id, source_id, version_id))

    async def add_version(self, version: KnowledgeSourceVersion) -> None:
        """@brief 添加来源版本 / Add a source version."""
        self.versions[(version.workspace_id, version.snapshot.source_id, version.meta.id)] = version

    async def add_upload(self, upload: UploadSession) -> None:
        """@brief 添加 upload / Add an upload."""
        self.uploads[(upload.view.workspace_id, upload.view.id)] = upload

    async def get_upload(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        *,
        for_update: bool = False,
    ) -> UploadSession | None:
        """@brief 读取 Workspace-scoped upload / Read a Workspace-scoped upload."""
        return self.uploads.get((workspace_id, upload_id))

    async def save_upload(
        self,
        upload: UploadSession,
        *,
        expected_generation: int,
    ) -> None:
        """@brief 模拟 upload generation CAS / Simulate upload generation CAS."""
        key = (upload.view.workspace_id, upload.view.id)
        current = self.uploads.get(key)
        if current is None or current.generation != expected_generation:
            raise KnowledgeCasMismatch
        self.uploads[key] = upload


@dataclass(slots=True)
class _UowProbe:
    """@brief 断言外部 I/O 不在活动 UoW 内 / Assert external I/O is outside active UoWs."""

    active: int = 0
    external_calls: list[str] = field(default_factory=list)

    def external(self, name: str) -> None:
        """@brief 记录事务外调用 / Record an out-of-transaction call.

        @param name 外部端口名称 / External-port name.
        """
        assert self.active == 0, f"{name} was invoked inside an active unit of work"
        self.external_calls.append(name)


@dataclass(slots=True)
class _CredentialBroker:
    """@brief 捕获 token 并返回 server reference 的 credential fake / Credential fake returning a server reference."""

    seen_token: str | None = None
    probe: _UowProbe | None = None

    async def provision_api_token(
        self,
        ownership: Any,
        connection_id: ConnectionId,
        provider: ConnectionProvider,
        token: SecretValue,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> ProvisionedConnectionCredential:
        """@brief 模拟 provider 验证 / Simulate provider validation."""
        del operation_id
        if self.probe is not None:
            self.probe.external("credential_broker")
        self.seen_token = token.reveal_to_secret_adapter()
        return ProvisionedConnectionCredential(
            CredentialReference(f"credential_{connection_id}"),
            ("read",),
            _NOW,
        )


@dataclass(slots=True)
class _AuthorizationGateway:
    """@brief 产生 flow-specific launch 的授权 fake / Authorization fake producing flow-specific launch data."""

    seen_state: SecretValue | None = None
    probe: _UowProbe | None = None

    async def begin(
        self,
        ownership: Any,
        provider: ConnectionProvider,
        flow: ConnectionAuthorizationFlow,
        requested_scopes: tuple[str, ...],
        state: SecretValue,
    ) -> ConnectionAuthorizationLaunch:
        """@brief 返回 browser/device 判别 launch / Return a browser/device discriminated launch."""
        if self.probe is not None:
            self.probe.external("authorization_gateway")
        self.seen_state = state
        if flow is ConnectionAuthorizationFlow.BROWSER_REDIRECT:
            return ConnectionAuthorizationLaunch(
                ProviderSessionReference("provider_session_browser_01"),
                _NOW + timedelta(minutes=10),
                authorization_url="https://provider.example/authorize?state=opaque",
            )
        return ConnectionAuthorizationLaunch(
            ProviderSessionReference("provider_session_device_01"),
            _NOW + timedelta(minutes=10),
            verification_uri="https://provider.example/device",
            user_code="KLEE-CODE",
            poll_interval_ms=5_000,
        )


@dataclass(slots=True)
class _UploadStore:
    """@brief 签发 grant 并返回可信扫描证明的对象存储 fake / Object-store fake issuing grants and evidence."""

    fail_verification: bool = False
    probe: _UowProbe | None = None

    async def issue_upload_grant(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        *,
        expires_at: datetime,
        operation_id: IdempotencyPreparationId,
    ) -> IssuedUploadGrant:
        """@brief 签发测试 PUT / Issue a test PUT grant."""
        del operation_id
        if self.probe is not None:
            self.probe.external("upload_grant")
        return IssuedUploadGrant(
            UploadGrant(
                f"https://objects.example.test/{workspace_id}/{upload_id}?signature=private",
                {"content-type": declaration.media_type},
            ),
            _NOW,
            expires_at,
        )

    async def verify_uploaded_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        claim: UploadCompletionClaim,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> VerifiedUpload:
        """@brief 返回与冻结声明一致的扫描结果 / Return evidence matching the frozen declaration."""
        del operation_id
        if self.probe is not None:
            self.probe.external("upload_verification")
        if self.fail_verification:
            raise UploadVerificationRejected("scanner rejected content")
        return VerifiedUpload(
            declaration.size_bytes,
            declaration.sha256,
            declaration.media_type,
            ResourceRef("upload_artifact", f"artifact_{upload_id}", 1),
            True,
            True,
            True,
        )

    async def delete_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> None:
        """@brief 幂等删除测试上传对象 / Idempotently delete a test upload object."""
        del workspace_id, upload_id


@dataclass(slots=True)
class _NetworkGuard:
    """@brief 记录 SSRF 策略调用的 fake / Fake recording SSRF-policy calls."""

    calls: list[object] = field(default_factory=list)
    probe: _UowProbe | None = None

    async def validate(self, source_input: object) -> None:
        """@brief 记录网络来源 / Record a network source."""
        if self.probe is not None:
            self.probe.external("network_guard")
        self.calls.append(source_input)


@dataclass(slots=True)
class _Dependencies:
    """@brief 跨 context 引用 verifier fake / Cross-context reference-verifier fake."""

    resumes: set[tuple[WorkspaceId, str]] = field(default_factory=set)
    probe: _UowProbe | None = None

    async def resume_exists(
        self,
        workspace_id: WorkspaceId,
        resume_id: str,
        *,
        actor_id: UserId,
    ) -> bool:
        """@brief 验证 Workspace-scoped Resume / Verify a Workspace-scoped Resume."""
        del actor_id
        if self.probe is not None:
            self.probe.external("dependency_verifier")
        return (workspace_id, resume_id) in self.resumes


@dataclass(slots=True)
class _Search:
    """@brief 返回可配置候选的 hybrid-search fake / Hybrid-search fake returning configured candidates."""

    response: HybridSearchResponse = field(default_factory=lambda: HybridSearchResponse((), 1))
    plans: list[object] = field(default_factory=list)
    probe: _UowProbe | None = None

    async def search(self, plan: object) -> HybridSearchResponse:
        """@brief 记录计划并返回候选 / Record a plan and return candidates."""
        if self.probe is not None:
            self.probe.external("hybrid_search")
        self.plans.append(plan)
        return self.response


@dataclass(slots=True)
class _Jobs:
    """@brief 收集统一 Job 与 typed specs / Collect unified Jobs and typed specs."""

    items: list[tuple[Job, KnowledgeJobSpec]] = field(default_factory=list)

    async def add(self, job: Job, spec: KnowledgeJobSpec) -> None:
        """@brief 收集 Job / Collect a Job."""
        self.items.append((job, spec))


@dataclass(slots=True)
class _Outbox:
    """@brief 收集 transactional outbox 事件 / Collect transactional outbox events."""

    items: list[KnowledgeOutboxEvent] = field(default_factory=list)

    async def add(self, event: KnowledgeOutboxEvent) -> None:
        """@brief 收集事件 / Collect an event."""
        self.items.append(event)


@dataclass(slots=True)
class _Environment:
    """@brief 共享所有 UoW fake adapter 的测试环境 / Test environment shared by all fake UoWs."""

    repository: _Repository
    authorizer: AccessAuthorizer
    probe: _UowProbe = field(default_factory=_UowProbe)
    credentials: _CredentialBroker = field(default_factory=_CredentialBroker)
    authorization_gateway: _AuthorizationGateway = field(default_factory=_AuthorizationGateway)
    uploads: _UploadStore = field(default_factory=_UploadStore)
    network_guard: _NetworkGuard = field(default_factory=_NetworkGuard)
    dependencies: _Dependencies = field(default_factory=_Dependencies)
    search: _Search = field(default_factory=_Search)
    jobs: _Jobs = field(default_factory=_Jobs)
    outbox: _Outbox = field(default_factory=_Outbox)
    commits: int = 0


class _UnitOfWork:
    """@brief 共享环境且记录 commit 的 UoW fake / UoW fake sharing an environment and recording commits."""

    def __init__(self, environment: _Environment) -> None:
        """@brief 绑定共享环境 / Bind the shared environment.

        @param environment 共享 adapter / Shared adapters.
        """
        self._environment = environment
        self.repository = environment.repository
        self.authorizer = environment.authorizer
        self.jobs = environment.jobs
        self.outbox = environment.outbox

    async def __aenter__(self) -> _UnitOfWork:
        """@brief 进入 UoW / Enter the UoW."""
        self._environment.probe.active += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """@brief 离开 UoW 且不吞异常 / Exit without suppressing exceptions."""
        self._environment.probe.active -= 1

    async def commit(self) -> None:
        """@brief 记录 commit / Record a commit."""
        self._environment.commits += 1

    async def rollback(self) -> None:
        """@brief 测试 fake 的幂等回滚 / Idempotent rollback for the fake."""


@dataclass(slots=True)
class _Factory:
    """@brief 每次调用返回新 UoW 的工厂 / Factory returning a fresh UoW on every call."""

    environment: _Environment

    def __call__(self) -> Any:
        """@brief 创建 UoW / Create a UoW."""
        return _UnitOfWork(self.environment)


def _user() -> User:
    """@brief 构造测试用户 / Build the test user.

    @return 活动用户 / Active user.
    """
    return User(
        ResourceMeta(_USER_ID, 1, _NOW, _NOW),
        Subject("subject_knowledge_klee"),
        "klee@example.cn",
        True,
        "Klee",
        "zh-CN",
        _WORKSPACE_A,
    )


def _membership(workspace_id: WorkspaceId, suffix: str) -> Membership:
    """@brief 构造 owner membership / Build an owner membership.

    @param workspace_id Workspace / Workspace.
    @param suffix 唯一后缀 / Unique suffix.
    @return owner membership / Owner membership.
    """
    return Membership(
        ResourceMeta(MembershipId(f"membership_{suffix}_0001"), 1, _NOW, _NOW),
        workspace_id,
        _USER_ID,
        "Klee",
        WorkspaceRole.OWNER,
        MemberStatus.ACTIVE,
    )


def _principal(*, write: bool = True) -> TokenPrincipal:
    """@brief 构造带 read/可选 write scope 的 principal / Build a principal with read and optional write scope.

    @param write 是否含 workspace.write / Whether workspace.write is present.
    @return 已验证 principal 投影 / Verified principal projection.
    """
    scopes = {Scope("workspace.read")}
    if write:
        scopes.add(Scope("workspace.write"))
    return TokenPrincipal(
        _USER_ID,
        Subject("subject_knowledge_klee"),
        ClientId("client_knowledge_web"),
        frozenset(scopes),
    )


def _policy(*, version: int = 1) -> KnowledgeVisibilityPolicy:
    """@brief 构造默认允许的本地 policy / Build a default-allow local policy.

    @param version policy 版本 / Policy version.
    @return visibility policy / Visibility policy.
    """
    return KnowledgeVisibilityPolicy(
        KnowledgeSensitivity.NORMAL,
        PolicyEffect.ALLOW,
        (),
        False,
        (ModelRegion.CN,),
        False,
        None,
        version,
    )


def _inference() -> InferenceIntent:
    """@brief 构造 CN 本地推理意图 / Build a local-CN inference intent.

    @return 推理意图 / Inference intent.
    """
    return InferenceIntent(
        InferenceQualityTier.BALANCED,
        5_000,
        InferenceCostTier.STANDARD,
        ModelRegion.CN,
        True,
        False,
    )


def _environment() -> _Environment:
    """@brief 构造含两个 owner memberships 的完整 fake 环境 / Build a complete fake environment with two memberships.

    @return 测试环境 / Test environment.
    """
    access_repository = _AccessRepository(
        _user(),
        {
            _WORKSPACE_A: _membership(_WORKSPACE_A, "alpha"),
            _WORKSPACE_B: _membership(_WORKSPACE_B, "beta"),
        },
    )
    environment = _Environment(
        _Repository(),
        AccessAuthorizer(access_repository),  # type: ignore[arg-type]
    )
    environment.credentials.probe = environment.probe
    environment.authorization_gateway.probe = environment.probe
    environment.uploads.probe = environment.probe
    environment.network_guard.probe = environment.probe
    environment.dependencies.probe = environment.probe
    environment.search.probe = environment.probe
    return environment


def _service(environment: _Environment) -> KnowledgeApplicationService:
    """@brief 构造确定性应用服务 / Build a deterministic application service.

    @param environment fake 环境 / Fake environment.
    @return 应用服务 / Application service.
    """
    return KnowledgeApplicationService(
        _Factory(environment),
        environment.authorization_gateway,
        environment.credentials,
        environment.uploads,
        environment.network_guard,
        environment.dependencies,
        environment.search,
        clock=_Clock(),
        id_factory=_Ids(),
        state_factory=lambda: SecretValue("deterministic-state-secret-32-bytes"),
    )


@pytest.mark.asyncio
async def test_all_seventeen_section_53_routes_have_working_application_use_cases() -> None:
    """@brief 逐一执行 5.3 实际 17 个应用入口 / Execute every one of the 17 actual section-5.3 use cases.

    @return 无返回值 / No return value.
    """
    environment = _environment()
    service = _service(environment)
    principal = _principal()

    assert len(V2_KNOWLEDGE_ENDPOINT_METHODS) == 17
    assert all(callable(getattr(service, method)) for method in V2_KNOWLEDGE_ENDPOINT_METHODS)
    assert (await service.list_connections(principal, _WORKSPACE_A)).items == ()

    authorization_command = CreateConnectionAuthorizationSessionCommand(
        ConnectionProvider("github"),
        ConnectionAuthorizationFlow.BROWSER_REDIRECT,
        ("repo.read",),
        _SHA256,
        "abcdef0123456789" * 4,
    )
    authorization = await service.create_connection_authorization_session(
        principal,
        _WORKSPACE_A,
        authorization_command,
    )
    authorization_replay = await service.create_connection_authorization_session(
        principal,
        _WORKSPACE_A,
        authorization_command,
    )
    assert authorization_replay == authorization
    assert environment.probe.external_calls.count("authorization_gateway") == 1
    with pytest.raises(IdempotencyConflict) as reused:
        await service.create_connection_authorization_session(
            principal,
            _WORKSPACE_A,
            CreateConnectionAuthorizationSessionCommand(
                ConnectionProvider("github"),
                ConnectionAuthorizationFlow.BROWSER_REDIRECT,
                ("repo.read",),
                _SHA256,
                "fedcba9876543210" * 4,
            ),
        )
    assert reused.value.problem.code == "idempotency.key_reused"
    assert environment.probe.external_calls.count("authorization_gateway") == 1
    connection = await service.create_connection(
        principal,
        _WORKSPACE_A,
        CreateConnectionCommand(
            ConnectionProvider("github"),
            "GitHub",
            SecretValue("api-token-never-in-projection"),
        ),
    )
    listed_connections = await service.list_connections(principal, _WORKSPACE_A)

    first_upload = await service.create_upload_session(
        principal,
        _WORKSPACE_A,
        UploadDeclaration("notes.txt", "text/plain", 12, _SHA256),
    )
    completed_first = await service.complete_upload_session(
        principal,
        _WORKSPACE_A,
        first_upload.id,
        UploadCompletionClaim(12, _SHA256),
    )
    file_source = await service.create_knowledge_source(
        principal,
        _WORKSPACE_A,
        CreateKnowledgeSourceCommand(
            "Interview notes",
            FileSourceInput(first_upload.id),
            _policy(),
        ),
    )
    listed_sources = await service.list_knowledge_sources(principal, _WORKSPACE_A)
    fetched = await service.get_knowledge_source(
        principal,
        _WORKSPACE_A,
        file_source.meta.id,
    )
    updated = await service.update_knowledge_source(
        principal,
        _WORKSPACE_A,
        file_source.meta.id,
        UpdateKnowledgeSourceCommand(name="Interview notes v2"),
        expected_revision=file_source.meta.revision,
    )
    versions = await service.list_knowledge_source_versions(
        principal,
        _WORKSPACE_A,
        file_source.meta.id,
    )

    second_upload = await service.create_upload_session(
        principal,
        _WORKSPACE_A,
        UploadDeclaration("notes-v2.txt", "text/plain", 12, _SHA256),
    )
    await service.complete_upload_session(
        principal,
        _WORKSPACE_A,
        second_upload.id,
        UploadCompletionClaim(12, _SHA256),
    )
    version_two = await service.create_knowledge_source_version(
        principal,
        _WORKSPACE_A,
        file_source.meta.id,
        second_upload.id,
    )
    ready_version = version_two.begin_indexing(at=_NOW).mark_ready(at=_NOW)
    environment.repository.versions[(_WORKSPACE_A, file_source.meta.id, version_two.meta.id)] = (
        ready_version
    )

    current_source = environment.repository.sources[(_WORKSPACE_A, file_source.meta.id)]
    environment.search.response = HybridSearchResponse(
        (
            KnowledgeSearchHit(
                chunk_id="knowledge_chunk_route_test_01",
                workspace_id=_WORKSPACE_A,
                source_id=file_source.meta.id,
                version_id=version_two.meta.id,
                locator="page:1#paragraph:2",
                quote="Consensus separates safety from liveness.",
                score=HybridScore(0.8, 0.9, 0.87),
            ),
        ),
        2,
    )
    selection = KnowledgeSelection(
        KnowledgeSelectionMode.EXPLICIT,
        (file_source.meta.id,),
        (),
        (KnowledgeVersionPin(file_source.meta.id, version_two.meta.id),),
        "interview_agent",
    )
    search_result = await service.search_knowledge(
        principal,
        _WORKSPACE_A,
        KnowledgeSearchRequest("What is consensus?", selection, 10, SearchFilters({})),
    )
    access_result = await service.evaluate_knowledge_access(
        principal,
        _WORKSPACE_A,
        KnowledgeAccessEvaluationRequest(
            (file_source.meta.id,),
            "interview_agent",
            KnowledgeOperation.RETRIEVE,
            _inference(),
        ),
    )
    ingestion_job = await service.create_ingestion_job(
        principal,
        _WORKSPACE_A,
        file_source.meta.id,
        CreateKnowledgeJobCommand(),
    )

    url_source = await service.create_knowledge_source(
        principal,
        _WORKSPACE_A,
        CreateKnowledgeSourceCommand(
            "Engineering blog",
            UrlSourceInput(KnowledgeSourceType.BLOG_FEED, "https://example.com/feed.xml"),
            _policy(),
        ),
    )
    sync_job = await service.create_sync_job(
        principal,
        _WORKSPACE_A,
        url_source.meta.id,
        CreateKnowledgeJobCommand(),
    )
    latest_file_source = environment.repository.sources[(_WORKSPACE_A, file_source.meta.id)]
    delete_source_job = await service.delete_knowledge_source(
        principal,
        _WORKSPACE_A,
        file_source.meta.id,
        expected_revision=latest_file_source.meta.revision,
    )
    delete_connection_job = await service.delete_connection(
        principal,
        _WORKSPACE_A,
        connection.meta.id,
        expected_revision=connection.meta.revision,
    )

    assert authorization.authorization_url is not None
    assert len(listed_connections.items) == 1
    assert completed_first.artifact_ref is not None
    assert listed_sources.items[0].workspace_id == _WORKSPACE_A
    assert fetched.meta.id == file_source.meta.id
    assert updated.name == "Interview notes v2"
    assert versions.items[0].snapshot.version_number == 1
    assert version_two.snapshot.version_number == 2
    assert search_result.citations[0].score == pytest.approx(0.87)
    assert access_result.decisions[0].reason_codes == ("policy.default_allow",)
    assert all(
        isinstance(job, Job)
        for job in (ingestion_job, sync_job, delete_source_job, delete_connection_job)
    )
    assert current_source.current_version_id == version_two.meta.id
    assert environment.network_guard.calls == [url_source.source_input]
    assert environment.credentials.seen_token == "api-token-never-in-projection"
    assert "api-token-never-in-projection" not in repr(connection)
    assert len(environment.jobs.items) == 4
    assert [
        (event.subject.resource_type, event.subject.id, event.subject.revision)
        for event in environment.outbox.items
        if event.event_type
        in {
            "connection.revocation_requested",
            "knowledge_source.deletion_requested",
            "knowledge_source.job_created",
        }
    ] == [
        ("job", job.meta.id, 1)
        for job in (ingestion_job, sync_job, delete_source_job, delete_connection_job)
    ]


@pytest.mark.asyncio
async def test_workspace_isolation_hides_cross_tenant_source_upload_and_connection() -> None:
    """@brief 同一 ID 从另一 Workspace 路径读取或引用均返回隐藏缺失 / Cross-tenant IDs are hidden.

    @return 无返回值 / No return value.
    """
    environment = _environment()
    service = _service(environment)
    principal = _principal()
    source = KnowledgeSource.create(
        meta=ResourceMeta(KnowledgeSourceId("knowledge_source_isolated"), 1, _NOW, _NOW),
        workspace_id=_WORKSPACE_A,
        created_by=_USER_ID,
        name="Tenant A source",
        source_input=ManualSourceInput("private"),
        visibility=_policy(),
    )
    environment.repository.sources[(_WORKSPACE_A, source.meta.id)] = source

    with pytest.raises(KnowledgeResourceNotFound):
        await service.get_knowledge_source(principal, _WORKSPACE_B, source.meta.id)


@pytest.mark.asyncio
async def test_existing_access_authorizer_denies_write_without_workspace_write_scope() -> None:
    """@brief 所有写入都经过现有 AccessAuthorizer 的 scope∩role / Writes use the existing scope-role authorizer.

    @return 无返回值 / No return value.
    """
    service = _service(_environment())

    with pytest.raises(AuthorizationDenied, match="scope_missing"):
        await service.create_upload_session(
            _principal(write=False),
            _WORKSPACE_A,
            UploadDeclaration("notes.txt", "text/plain", 12, _SHA256),
        )


def test_connection_and_knowledge_actions_are_exact_and_least_privileged() -> None:
    """@brief 验证 Connection 与 Knowledge 不再借用 Workspace 管理动作 / Verify exact least-privilege Connection and Knowledge actions.

    @return 无返回值 / No return value.
    """
    connection_read = WORKSPACE_AUTHORIZATION_MATRIX[WorkspaceAction.LIST_CONNECTIONS]
    knowledge_read = WORKSPACE_AUTHORIZATION_MATRIX[WorkspaceAction.LIST_KNOWLEDGE_SOURCES]
    knowledge_write = WORKSPACE_AUTHORIZATION_MATRIX[WorkspaceAction.CREATE_KNOWLEDGE_SOURCE]

    assert connection_read.scope == Scope("workspace.read")
    assert connection_read.roles == frozenset({WorkspaceRole.OWNER, WorkspaceRole.ADMIN})
    assert knowledge_read.scope == Scope("workspace.read")
    assert set(knowledge_read.roles) == set(WorkspaceRole)
    assert knowledge_write.scope == Scope("workspace.write")
    assert knowledge_write.roles == frozenset(
        {WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.EDITOR}
    )


@pytest.mark.asyncio
async def test_delete_snapshots_use_exact_actions_without_list_scope() -> None:
    """@brief If-Match 快照只要求 exact delete action，不误引入 list scope / If-Match snapshots use exact delete actions.

    @return 无返回值 / No return value.
    """

    environment = _environment()
    service = _service(environment)
    connection = await service.create_connection(
        _principal(),
        _WORKSPACE_A,
        CreateConnectionCommand(
            ConnectionProvider("github"),
            "GitHub",
            SecretValue("api-token-for-delete-snapshot"),
        ),
    )
    source = await service.create_knowledge_source(
        _principal(),
        _WORKSPACE_A,
        CreateKnowledgeSourceCommand("Notes", ManualSourceInput("body"), _policy()),
    )
    write_only = TokenPrincipal(
        _USER_ID,
        Subject("subject_knowledge_klee"),
        ClientId("client_knowledge_web"),
        frozenset({Scope("workspace.write")}),
    )

    snapshot = await service.get_connection_for_deletion(
        write_only,
        _WORKSPACE_A,
        connection.meta.id,
    )
    source_snapshot = await service.get_knowledge_source_for_deletion(
        write_only,
        _WORKSPACE_A,
        source.meta.id,
    )

    assert snapshot == connection
    assert source_snapshot == source
    with pytest.raises(AuthorizationDenied, match="scope_missing"):
        await service.list_connections(write_only, _WORKSPACE_A)
    with pytest.raises(AuthorizationDenied, match="scope_missing"):
        await service.list_knowledge_sources(write_only, _WORKSPACE_A)


@pytest.mark.asyncio
async def test_source_patch_rejects_stale_if_match_before_mutation() -> None:
    """@brief stale If-Match 返回预条件失败且不修改来源 / Stale If-Match fails before mutation.

    @return 无返回值 / No return value.
    """
    environment = _environment()
    service = _service(environment)
    source = await service.create_knowledge_source(
        _principal(),
        _WORKSPACE_A,
        CreateKnowledgeSourceCommand("Notes", ManualSourceInput("body"), _policy()),
    )

    with pytest.raises(KnowledgePreconditionFailed):
        await service.update_knowledge_source(
            _principal(),
            _WORKSPACE_A,
            source.meta.id,
            UpdateKnowledgeSourceCommand(name="Changed"),
            expected_revision=source.meta.revision + 10,
        )

    assert environment.repository.sources[(_WORKSPACE_A, source.meta.id)].name == "Notes"


@pytest.mark.asyncio
async def test_search_rejects_adapter_hit_outside_authorized_workspace() -> None:
    """@brief 应用层重验 hybrid hit provenance / Application revalidates hybrid-hit provenance.

    @return 无返回值 / No return value.
    """
    environment = _environment()
    service = _service(environment)
    source = await service.create_knowledge_source(
        _principal(),
        _WORKSPACE_A,
        CreateKnowledgeSourceCommand("Notes", ManualSourceInput("body"), _policy()),
    )
    version_id = KnowledgeSourceVersionId("knowledge_version_search")
    changed, version = source.allocate_version(
        version_id=version_id,
        content_sha256=_SHA256,
        size_bytes=12,
        artifact_ref=ResourceRef("upload_artifact", "artifact_search_01", 1),
        at=_NOW,
    )
    ready = version.begin_indexing(at=_NOW).mark_ready(at=_NOW)
    environment.repository.sources[(_WORKSPACE_A, source.meta.id)] = changed
    environment.repository.versions[(_WORKSPACE_A, source.meta.id, version_id)] = ready
    environment.search.response = HybridSearchResponse(
        (
            KnowledgeSearchHit(
                chunk_id="knowledge_chunk_foreign_workspace_01",
                workspace_id=_WORKSPACE_B,
                source_id=source.meta.id,
                version_id=version_id,
                locator="page:1",
                quote="quote",
                score=HybridScore(0.5, 0.6, 0.55),
            ),
        ),
        1,
    )
    selection = KnowledgeSelection(
        KnowledgeSelectionMode.EXPLICIT,
        (source.meta.id,),
        (),
        (KnowledgeVersionPin(source.meta.id, version_id),),
        "interview_agent",
    )

    with pytest.raises(PermissionError, match="outside"):
        await service.search_knowledge(
            _principal(),
            _WORKSPACE_A,
            KnowledgeSearchRequest("query", selection, 5),
        )


@pytest.mark.asyncio
async def test_upload_scanner_failure_is_persisted_without_a_long_transaction() -> None:
    """@brief 扫描失败在独立事务把 session 标记 failed / Scanner failure is persisted in a separate transaction.

    @return 无返回值 / No return value.
    """
    environment = _environment()
    environment.uploads.fail_verification = True
    service = _service(environment)
    upload = await service.create_upload_session(
        _principal(),
        _WORKSPACE_A,
        UploadDeclaration("notes.txt", "text/plain", 12, _SHA256),
    )

    with pytest.raises(Exception, match="server verification"):
        await service.complete_upload_session(
            _principal(),
            _WORKSPACE_A,
            upload.id,
            UploadCompletionClaim(12, _SHA256),
        )

    stored = environment.repository.uploads[(_WORKSPACE_A, upload.id)]
    assert stored.view.status.value == "failed"
    assert environment.commits == 3
