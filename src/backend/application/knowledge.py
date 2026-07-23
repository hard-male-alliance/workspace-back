"""@brief API v2 Connection、Upload 与 Knowledge 应用用例 / API v2 Connection, Upload, and Knowledge use cases.

本服务覆盖 ``contract.md`` 5.3 实际列出的 17 个路由。所有资源查询都以路径
``workspace_id`` 作为首个 repository key，并在返回前再次验证归属。写入通过同一 UoW
原子提交领域状态、统一 Job 与 transactional outbox；外部上传扫描使用两段短事务，
避免在昂贵 I/O 期间持有 PostgreSQL 行锁。
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

from backend.application.ports.knowledge import (
    ConnectionAuthorizationGateway,
    ConnectionCredentialBroker,
    HybridKnowledgeSearch,
    HybridSearchResponse,
    KnowledgeCasMismatch,
    KnowledgeDependencyVerifier,
    KnowledgePage,
    KnowledgePageRequest,
    KnowledgeRepository,
    KnowledgeUnitOfWork,
    KnowledgeUnitOfWorkFactory,
    ProvisionedConnectionCredential,
    SourceNetworkGuard,
    UploadObjectStore,
    UploadVerificationRejected,
)
from backend.application.ports.v2_idempotency import (
    IdempotencyConflict,
    IdempotencyPreparationId,
)
from backend.domain.connections import (
    Connection,
    ConnectionAggregate,
    ConnectionAuthMethod,
    ConnectionAuthorizationFlow,
    ConnectionAuthorizationIdempotency,
    ConnectionAuthorizationRecord,
    ConnectionAuthorizationSession,
    ConnectionAuthorizationSessionId,
    ConnectionAuthorizationState,
    ConnectionId,
    ConnectionOwnership,
    ConnectionProvider,
    ConnectionStatus,
    SecretValue,
    authorization_state_sha256,
)
from backend.domain.knowledge_jobs import (
    ConnectionRevokeSpec,
    KnowledgeDeleteSpec,
    KnowledgeJobKind,
    KnowledgeOutboxEvent,
    KnowledgeOutboxEventId,
    KnowledgeProcessSpec,
)
from backend.domain.knowledge_retrieval import (
    KnowledgeAccessEvaluationRequest,
    KnowledgeAccessEvaluationResult,
    KnowledgeCitation,
    KnowledgeSearchPlan,
    KnowledgeSearchRequest,
    KnowledgeSearchResult,
    KnowledgeSearchScope,
    KnowledgeSelectionMode,
    evaluate_visibility,
)
from backend.domain.knowledge_sources import (
    CloudDriveSourceInput,
    FilePublicMetadata,
    FileSourceInput,
    GitSourceInput,
    KnowledgeIngestionStatus,
    KnowledgeOperation,
    KnowledgeSource,
    KnowledgeSourceId,
    KnowledgeSourceInput,
    KnowledgeSourceVersion,
    KnowledgeSourceVersionId,
    KnowledgeVisibilityPolicy,
    ResumeSourceInput,
    UrlSourceInput,
)
from backend.domain.platform import Job, JobId
from backend.domain.principals import (
    ResourceMeta,
    TokenPrincipal,
    UserId,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef
from backend.domain.upload_sessions import (
    UploadCompletionClaim,
    UploadDeclaration,
    UploadSession,
    UploadSessionId,
    UploadSessionView,
    UploadStatus,
    UploadVerificationId,
    VerifiedUpload,
)
from workspace_shared.ids import new_opaque_id

V2_KNOWLEDGE_ENDPOINT_METHODS = (
    "list_connections",
    "create_connection_authorization_session",
    "create_connection",
    "delete_connection",
    "list_knowledge_sources",
    "create_knowledge_source",
    "get_knowledge_source",
    "update_knowledge_source",
    "delete_knowledge_source",
    "list_knowledge_source_versions",
    "create_knowledge_source_version",
    "create_upload_session",
    "complete_upload_session",
    "create_ingestion_job",
    "create_sync_job",
    "search_knowledge",
    "evaluate_knowledge_access",
)
"""@brief 5.3 实际 17 个路由对应的应用方法 / Application methods for the 17 actual section-5.3 routes."""

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
"""@brief 专用幂等摘要语法 / Dedicated-idempotency digest grammar."""


class Clock(Protocol):
    """@brief 应用层可替换时钟 / Replaceable application clock."""

    def now(self) -> datetime:
        """@brief 返回带时区当前时刻 / Return the current timezone-aware instant.

        @return 当前时刻 / Current instant.
        """


class StateSecretFactory(Protocol):
    """@brief OAuth state secret 工厂 / OAuth state-secret factory."""

    def __call__(self) -> SecretValue:
        """@brief 生成单 session 高熵 state / Generate high-entropy state for one session.

        @return 默认脱敏 state / Redacted-by-default state.
        """


class UtcClock:
    """@brief 使用 UTC 的生产时钟 / Production clock using UTC."""

    def now(self) -> datetime:
        """@brief 返回 UTC 当前时刻 / Return the current UTC instant.

        @return UTC 当前时刻 / Current UTC instant.
        """
        return datetime.now(UTC)


class SecureStateFactory:
    """@brief 使用系统 CSPRNG 的 OAuth state 工厂 / OAuth state factory using the system CSPRNG."""

    def __call__(self) -> SecretValue:
        """@brief 生成至少 256-bit 随机 state / Generate at least 256 bits of random state.

        @return URL-safe 脱敏 state / URL-safe redacted state.
        """
        return SecretValue(secrets.token_urlsafe(32))


class KnowledgeApplicationError(Exception):
    """@brief 可稳定映射为 API problem 的应用错误 / Application error mappable to a stable API problem.

    @param code 稳定错误 code / Stable error code.
    @param detail 不泄漏资源或 secret 的公开说明 / Public detail without resource or secret disclosure.
    """

    code: str
    """@brief 稳定应用错误 code / Stable application error code."""

    detail: str
    """@brief 可公开说明 / Public-safe detail."""

    def __init__(self, code: str, detail: str) -> None:
        """@brief 初始化结构化应用错误 / Initialize a structured application error.

        @param code 稳定 code / Stable code.
        @param detail 可公开说明 / Public-safe detail.
        """
        super().__init__(detail)
        self.code = code
        self.detail = detail


class KnowledgeResourceNotFound(KnowledgeApplicationError):
    """@brief 资源不存在或为防枚举不可暴露 / Resource is absent or hidden to prevent enumeration."""

    def __init__(self, resource: str) -> None:
        """@brief 创建统一不泄漏 404 来源 / Create a uniform non-disclosing not-found result.

        @param resource 稳定资源类型 / Stable resource kind.
        """
        super().__init__(f"{resource}.not_found", f"{resource} was not found")


class KnowledgePreconditionFailed(KnowledgeApplicationError):
    """@brief 强 ETag 对应 revision 已过期 / Revision represented by a strong ETag is stale."""

    def __init__(self) -> None:
        """@brief 创建统一 412 来源 / Create a uniform precondition-failed result."""
        super().__init__("http.precondition_failed", "resource revision precondition failed")


class KnowledgeConflict(KnowledgeApplicationError):
    """@brief 当前领域状态拒绝命令 / Current domain state rejects the command."""


class InvalidKnowledgeCommand(KnowledgeApplicationError):
    """@brief 应用边界收到空或矛盾命令 / Application boundary received an empty or contradictory command."""


@dataclass(frozen=True, slots=True)
class CreateConnectionAuthorizationSessionCommand:
    """@brief 创建 provider 授权 session 的命令 / Command creating a provider authorization session.

    @param provider provider 标识 / Provider identifier.
    @param flow browser 或 device flow / Browser or device flow.
    @param requested_scopes 去重 provider scopes / Unique provider scopes.
    @param idempotency_key_hash 原始 key 的服务端 keyed hash / Server-keyed hash of the raw key.
    @param request_fingerprint 不含 secret 的请求指纹 / Secret-free request fingerprint.
    """

    provider: ConnectionProvider
    flow: ConnectionAuthorizationFlow
    requested_scopes: tuple[str, ...]
    idempotency_key_hash: str = field(repr=False)
    request_fingerprint: str = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 scope 边界 / Validate scope bounds.

        @raise InvalidKnowledgeCommand scopes 重复、超限或非法时抛出 / Raised for invalid scopes.
        """
        if len(self.requested_scopes) > 100 or len(set(self.requested_scopes)) != len(
            self.requested_scopes
        ):
            raise InvalidKnowledgeCommand(
                "connection.invalid_scopes",
                "requested connection scopes must be unique and at most 100 entries",
            )
        if any(
            not scope or len(scope) > 200 or scope.strip() != scope
            for scope in self.requested_scopes
        ):
            raise InvalidKnowledgeCommand(
                "connection.invalid_scopes",
                "requested connection scopes are invalid",
            )
        if (
            _SHA256_HEX.fullmatch(self.idempotency_key_hash) is None
            or _SHA256_HEX.fullmatch(self.request_fingerprint) is None
        ):
            raise InvalidKnowledgeCommand(
                "connection.invalid_idempotency_metadata",
                "connection authorization idempotency metadata is invalid",
            )


@dataclass(frozen=True, slots=True)
class CreateConnectionCommand:
    """@brief CreateConnectionRequest 的 secret-safe 形式 / Secret-safe form of CreateConnectionRequest.

    @param provider provider / Provider.
    @param display_name 显示名 / Display name.
    @param api_token 默认脱敏 token / Redacted-by-default token.
    """

    provider: ConnectionProvider
    display_name: str
    api_token: SecretValue = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 token 最小长度且不记录原文 / Validate minimum token length without logging it.

        @raise InvalidKnowledgeCommand token 不满足 Schema 下限时抛出 / Raised below the schema minimum.
        """
        if self.api_token.length < 8:
            raise InvalidKnowledgeCommand(
                "connection.invalid_api_token",
                "connection API token must contain at least eight characters",
            )


@dataclass(frozen=True, slots=True)
class CreateKnowledgeSourceCommand:
    """@brief CreateKnowledgeSourceRequest 的类型化形式 / Typed form of CreateKnowledgeSourceRequest.

    @param name 来源显示名 / Source display name.
    @param source_input 判别来源输入 / Discriminated source input.
    @param visibility 初始 visibility policy / Initial visibility policy.
    """

    name: str
    source_input: KnowledgeSourceInput = field(repr=False)
    visibility: KnowledgeVisibilityPolicy


@dataclass(frozen=True, slots=True)
class UpdateKnowledgeSourceCommand:
    """@brief UpdateKnowledgeSourceRequest 的 merge-patch 形式 / Merge-patch form of UpdateKnowledgeSourceRequest.

    @param name 可选完整目标名称 / Optional complete target name.
    @param visibility 可选完整目标 policy / Optional complete target policy.
    """

    name: str | None = None
    visibility: KnowledgeVisibilityPolicy | None = None

    def __post_init__(self) -> None:
        """@brief 拒绝空 PATCH / Reject an empty PATCH.

        @raise InvalidKnowledgeCommand 两字段均缺失时抛出 / Raised when both fields are absent.
        """
        if self.name is None and self.visibility is None:
            raise InvalidKnowledgeCommand(
                "knowledge_source.patch_empty",
                "knowledge source patch must contain a field",
            )


@dataclass(frozen=True, slots=True)
class CreateKnowledgeJobCommand:
    """@brief CreateKnowledgeJobRequest 的类型化形式 / Typed form of CreateKnowledgeJobRequest.

    @param force 是否显式强制重做 / Whether reprocessing is explicitly forced.
    """

    force: bool = False


@dataclass(frozen=True, slots=True)
class PreparedConnectionCreation:
    """@brief 已完成 provider I/O、等待原子提交的 Connection / Connection prepared by provider I/O and awaiting atomic commit.

    @param ownership Workspace 与 actor 快照 / Workspace and actor snapshot.
    @param connection_id 稳定资源 ID / Stable resource ID.
    @param provider provider / Provider.
    @param display_name 公开显示名 / Public display name.
    @param credential 仅含 vault reference 的 provider 结果 / Provider result containing only a
        vault reference.
    """

    ownership: ConnectionOwnership
    connection_id: ConnectionId
    provider: ConnectionProvider
    display_name: str
    credential: ProvisionedConnectionCredential = field(repr=False)


@dataclass(frozen=True, slots=True)
class PreparedKnowledgeSourceCreation:
    """@brief 已通过事务外检查的 KnowledgeSource 创建 / KnowledgeSource creation after external checks.

    @param workspace_id 准备阶段路径 Workspace / Path workspace during preparation.
    @param actor_id 准备阶段授权 actor / Actor authorized during preparation.
    @param command 类型化创建命令 / Typed creation command.
    """

    workspace_id: WorkspaceId
    actor_id: UserId
    command: CreateKnowledgeSourceCommand


@dataclass(frozen=True, slots=True)
class PreparedUploadSessionCreation:
    """@brief 已签发对象存储 grant 的 UploadSession / UploadSession with an issued object-store grant.

    @param actor_id 准备阶段授权 actor / Actor authorized during preparation.
    @param upload 待持久化聚合 / Aggregate awaiting persistence.
    """

    actor_id: UserId
    upload: UploadSession = field(repr=False)


@dataclass(frozen=True, slots=True)
class PreparedUploadCompletion:
    """@brief 已完成可信扫描、等待原子状态提交的 upload / Scanned upload awaiting atomic state commit.

    @param upload_id UploadSession ID / UploadSession identifier.
    @param operation_id 拥有 verifying saga 的稳定 ID / Stable ID owning the verification saga.
    @param claim 冻结 completion 声明 / Frozen completion claim.
    @param evidence 可信扫描证明 / Trusted verification evidence.
    """

    upload_id: UploadSessionId
    operation_id: UploadVerificationId
    claim: UploadCompletionClaim = field(repr=False)
    evidence: VerifiedUpload = field(repr=False)


class KnowledgeApplicationService:
    """@brief 5.3 的 Workspace 隔离应用协调器 / Workspace-isolated application coordinator for section 5.3.

    @param uow_factory 每个事务步骤创建 UoW / UoW factory for each transaction step.
    @param authorization_gateway provider 授权适配器 / Provider-authorization adapter.
    @param credential_broker credential vault 适配器 / Credential-vault adapter.
    @param upload_store 对象存储与核验适配器 / Object-storage and verification adapter.
    @param network_guard SSRF/allowlist 策略适配器 / SSRF and allowlist-policy adapter.
    @param dependency_verifier 跨 context 引用校验器 / Cross-context reference verifier.
    @param search_engine 混合检索适配器 / Hybrid-search adapter.
    @param clock 可测试时钟 / Testable clock.
    @param id_factory 可测试 ID 工厂 / Testable ID factory.
    @param state_factory CSPRNG state 工厂 / CSPRNG state factory.
    @param upload_lifetime 直传 session 寿命 / Direct-upload session lifetime.
    """

    def __init__(
        self,
        uow_factory: KnowledgeUnitOfWorkFactory,
        authorization_gateway: ConnectionAuthorizationGateway,
        credential_broker: ConnectionCredentialBroker,
        upload_store: UploadObjectStore,
        network_guard: SourceNetworkGuard,
        dependency_verifier: KnowledgeDependencyVerifier,
        search_engine: HybridKnowledgeSearch,
        *,
        clock: Clock | None = None,
        id_factory: Callable[[str], str] = new_opaque_id,
        state_factory: StateSecretFactory | None = None,
        upload_lifetime: timedelta = timedelta(minutes=15),
        authorization_replay_retention: timedelta = timedelta(hours=24),
    ) -> None:
        """@brief 组装 fail-closed 5.3 服务 / Assemble the fail-closed section-5.3 service.

        @param uow_factory UoW 工厂 / UoW factory.
        @param authorization_gateway 事务外 provider 授权端口 / Provider authorization port
            invoked outside database transactions.
        @param credential_broker 事务外 credential vault 端口 / Credential-vault port invoked
            outside database transactions.
        @param upload_store 事务外对象存储端口 / Object-store port invoked outside database
            transactions.
        @param network_guard 事务外网络策略端口 / Network-policy port invoked outside database
            transactions.
        @param dependency_verifier 跨 context 只读校验端口 / Cross-context read verifier.
        @param search_engine 事务外混合检索端口 / Hybrid-search port invoked outside database
            transactions.
        @param clock 可选时钟 / Optional clock.
        @param id_factory ID 工厂 / ID factory.
        @param state_factory OAuth state 工厂 / OAuth state factory.
        @param upload_lifetime 上传寿命 / Upload lifetime.
        @param authorization_replay_retention 专用授权 session 重放保留期 / Dedicated
            authorization-session replay retention.
        @raise ValueError 上传寿命非正时抛出 / Raised for a non-positive lifetime.
        """
        if upload_lifetime <= timedelta(0):
            raise ValueError("upload lifetime must be positive")
        if authorization_replay_retention < timedelta(hours=24):
            raise ValueError("authorization-session replay retention must be at least 24 hours")
        self._uow_factory = uow_factory
        self._authorization_gateway = authorization_gateway
        self._credential_broker = credential_broker
        self._upload_store = upload_store
        self._network_guard = network_guard
        self._dependency_verifier = dependency_verifier
        self._search_engine = search_engine
        self._clock = clock or UtcClock()
        self._id_factory = id_factory
        self._state_factory = state_factory or SecureStateFactory()
        self._upload_lifetime = upload_lifetime
        self._authorization_replay_retention = authorization_replay_retention

    async def list_connections(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        page: KnowledgePageRequest | None = None,
    ) -> KnowledgePage[Connection]:
        """@brief 列出路径 Workspace 的安全 Connection 投影 / List safe Connection projections in the path Workspace."""
        async with self._uow_factory() as uow:
            await self._authorize(uow, principal, workspace_id, WorkspaceAction.LIST_CONNECTIONS)
            result = await uow.repository.list_connections(
                workspace_id, page or KnowledgePageRequest()
            )
            if any(item.workspace_id != workspace_id for item in result.items):
                raise PermissionError("connection repository returned a cross-workspace row")
            return result

    async def create_connection_authorization_session(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateConnectionAuthorizationSessionCommand,
    ) -> ConnectionAuthorizationSession:
        """@brief 创建 state 绑定且 secret-redacted 的 provider session / Create a state-bound, secret-redacted provider session."""
        async with self._uow_factory() as authorization_uow:
            authorized = await self._authorize(
                authorization_uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_CONNECTION_AUTHORIZATION_SESSION,
            )
            existing = await authorization_uow.repository.get_authorization_record_by_idempotency(
                workspace_id,
                authorized.actor.user_id,
                command.idempotency_key_hash,
            )
            if existing is not None:
                self._require_authorization_fingerprint(existing, command)
                return existing.session
        ownership = ConnectionOwnership(workspace_id, authorized.actor.user_id)
        state = self._state_factory()
        launch = await self._authorization_gateway.begin(
            ownership,
            command.provider,
            command.flow,
            command.requested_scopes,
            state,
        )
        session = ConnectionAuthorizationSession(
            id=ConnectionAuthorizationSessionId(self._id_factory("connection_auth")),
            provider=command.provider,
            flow=command.flow,
            authorization_url=launch.authorization_url,
            verification_uri=launch.verification_uri,
            user_code=launch.user_code,
            expires_at=launch.expires_at,
            poll_interval_ms=launch.poll_interval_ms,
        )
        created_at = self._clock.now()
        record = ConnectionAuthorizationRecord(
            session,
            ownership,
            command.requested_scopes,
            ConnectionAuthorizationState.PENDING,
            authorization_state_sha256(state),
            launch.provider_session_reference,
            ConnectionAuthorizationIdempotency(
                command.idempotency_key_hash,
                command.request_fingerprint,
                created_at + self._authorization_replay_retention,
            ),
            created_at,
        )
        async with self._uow_factory() as persistence_uow:
            current = await self._authorize(
                persistence_uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_CONNECTION_AUTHORIZATION_SESSION,
            )
            if current.actor.user_id != ownership.created_by:
                raise PermissionError("connection authorization actor changed before persistence")
            concurrent = await persistence_uow.repository.get_authorization_record_by_idempotency(
                workspace_id,
                current.actor.user_id,
                command.idempotency_key_hash,
                for_update=True,
            )
            if concurrent is not None:
                self._require_authorization_fingerprint(concurrent, command)
                return concurrent.session
            await persistence_uow.repository.add_authorization_record(record)
            await persistence_uow.commit()
        return session

    async def create_connection(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateConnectionCommand,
    ) -> Connection:
        """@brief 兼容应用调用的 prepare+commit 组合 / Compose prepare and commit for direct application callers."""

        prepared = await self.prepare_connection_creation(
            principal,
            workspace_id,
            command,
            self._direct_preparation_id(),
        )
        return await self.commit_connection_creation(principal, workspace_id, prepared)

    async def prepare_connection_creation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateConnectionCommand,
        operation_id: IdempotencyPreparationId,
    ) -> PreparedConnectionCreation:
        """@brief 在事务外验证并暂存 API token / Validate and stage an API token outside a transaction.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param command 含脱敏 token 的命令 / Command carrying a redacted token.
        @param operation_id 跨崩溃稳定的 provider 幂等 ID / Crash-stable provider idempotency ID.
        @return 不含原始 token 的准备结果 / Prepared result excluding the raw token.
        """

        async with self._uow_factory() as authorization_uow:
            authorized = await self._authorize(
                authorization_uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_CONNECTION,
            )
        connection_id = ConnectionId(_prepared_resource_id("connection", operation_id))
        ownership = ConnectionOwnership(workspace_id, authorized.actor.user_id)
        credential = await self._credential_broker.provision_api_token(
            ownership,
            connection_id,
            command.provider,
            command.api_token,
            operation_id=operation_id,
        )
        return PreparedConnectionCreation(
            ownership,
            connection_id,
            command.provider,
            command.display_name,
            credential,
        )

    async def commit_connection_creation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        prepared: PreparedConnectionCreation,
    ) -> Connection:
        """@brief 原子持久化 prepared credential reference 与 outbox / Atomically persist a prepared credential reference and outbox.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param prepared 不含 secret 的准备结果 / Secret-free prepared result.
        @return 新 Connection / New Connection.
        """

        if prepared.ownership.workspace_id != workspace_id:
            raise PermissionError("prepared connection belongs to another workspace")
        now = self._clock.now()
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.CREATE_CONNECTION
            )
            if context.actor.user_id != prepared.ownership.created_by:
                raise PermissionError("connection actor changed before persistence")
            connection = Connection(
                ResourceMeta(prepared.connection_id, 1, now, now),
                workspace_id,
                prepared.provider,
                ConnectionAuthMethod.API_TOKEN,
                prepared.display_name,
                ConnectionStatus.ACTIVE,
                prepared.credential.scopes,
                prepared.credential.validated_at,
            )
            aggregate = ConnectionAggregate(
                connection,
                prepared.ownership,
                prepared.credential.reference,
            )
            await uow.repository.add_connection(aggregate)
            await self._emit(
                uow,
                context,
                "connection.created",
                ResourceRef("connection", prepared.connection_id, 1),
                {
                    "provider": prepared.provider.value,
                    "auth_method": ConnectionAuthMethod.API_TOKEN.value,
                },
                now,
            )
            await uow.commit()
            return connection

    async def delete_connection(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
        *,
        expected_revision: int,
    ) -> Job:
        """@brief CAS 标记 revoking 并原子创建统一 Job / CAS to revoking and atomically create a unified Job."""
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.DELETE_CONNECTION
            )
            aggregate = await self._require_connection(
                uow.repository,
                workspace_id,
                connection_id,
                for_update=True,
            )
            self._require_revision(aggregate.connection.meta.revision, expected_revision)
            now = self._clock.now()
            changed = aggregate.request_revocation(at=now)
            await self._save_connection(uow, changed, expected_revision=expected_revision)
            job = self._job(
                workspace_id,
                KnowledgeJobKind.CONNECTION_REVOKE,
                ResourceRef("connection", connection_id, changed.connection.meta.revision),
                now,
            )
            await uow.jobs.add(
                job,
                ConnectionRevokeSpec(
                    connection_id,
                    aggregate.credential_reference,
                    aggregate.connection.status,
                    aggregate.connection.problem,
                ),
            )
            await self._emit(
                uow,
                context,
                "connection.revocation_requested",
                ResourceRef("job", job.meta.id, job.meta.revision),
                {"job_id": str(job.meta.id)},
                now,
            )
            await uow.commit()
            return job

    async def get_connection_for_deletion(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
    ) -> Connection:
        """@brief 以删除权限读取 If-Match 所需 Connection / Read a Connection snapshot for If-Match under delete permission.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param connection_id 待删除 Connection / Connection targeted for deletion.
        @return 已通过 DELETE_CONNECTION 授权的安全投影 / Safe projection authorized by DELETE_CONNECTION.
        @raise KnowledgeResourceNotFound Connection 不存在或属于其他 Workspace 时抛出 /
            Raised when absent or owned by another Workspace.
        @note 调用方必须把该快照 revision 传回 ``delete_connection``；后者在变更
            事务内再次比较，以封闭 TOCTOU / The caller must pass the snapshot revision
            to ``delete_connection``, which compares it again in the mutation transaction to close TOCTOU.
        """

        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.DELETE_CONNECTION,
            )
            aggregate = await self._require_connection(
                uow.repository,
                workspace_id,
                connection_id,
            )
            return aggregate.connection

    async def list_knowledge_sources(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        page: KnowledgePageRequest | None = None,
    ) -> KnowledgePage[KnowledgeSource]:
        """@brief 列出路径 Workspace KnowledgeSources / List KnowledgeSources in the path Workspace."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.LIST_KNOWLEDGE_SOURCES
            )
            result = await uow.repository.list_sources(workspace_id, page or KnowledgePageRequest())
            if any(item.workspace_id != workspace_id for item in result.items):
                raise PermissionError("knowledge repository returned a cross-workspace row")
            return result

    async def create_knowledge_source(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateKnowledgeSourceCommand,
    ) -> KnowledgeSource:
        """@brief 兼容应用调用的 source prepare+commit 组合 / Compose source preparation and commit for direct callers."""

        prepared = await self.prepare_knowledge_source_creation(
            principal,
            workspace_id,
            command,
            self._direct_preparation_id(),
        )
        return await self.commit_knowledge_source_creation(
            principal,
            workspace_id,
            prepared,
        )

    async def prepare_knowledge_source_creation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateKnowledgeSourceCommand,
        operation_id: IdempotencyPreparationId,
    ) -> PreparedKnowledgeSourceCreation:
        """@brief 在事务外执行 SSRF/跨 context 只读检查 / Run SSRF and cross-context read checks outside a transaction.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param command 来源创建命令 / Source-creation command.
        @param operation_id 统一分相协议 ID；只读检查无需消费 / Split-phase operation ID, not
            consumed by read-only checks.
        @return 带授权 actor 快照的准备结果 / Prepared result carrying the authorized actor.
        """

        del operation_id
        async with self._uow_factory() as authorization_uow:
            authorized = await self._authorize(
                authorization_uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_KNOWLEDGE_SOURCE,
            )
        await self._validate_external_source_input(
            workspace_id,
            command.source_input,
            actor_id=principal.user_id,
        )
        return PreparedKnowledgeSourceCreation(
            workspace_id,
            authorized.actor.user_id,
            command,
        )

    async def commit_knowledge_source_creation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        prepared: PreparedKnowledgeSourceCreation,
    ) -> KnowledgeSource:
        """@brief 原子创建来源并可消费 file upload / Atomically create a source and optionally consume a file upload.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param prepared 已通过事务外检查的创建 / Externally validated creation.
        @return 新 KnowledgeSource / New KnowledgeSource.
        """

        if prepared.workspace_id != workspace_id:
            raise PermissionError("prepared knowledge source belongs to another workspace")
        command = prepared.command
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.CREATE_KNOWLEDGE_SOURCE
            )
            if context.actor.user_id != prepared.actor_id:
                raise PermissionError("knowledge-source actor changed before persistence")
            await self._validate_source_references(uow, workspace_id, command.source_input)
            now = self._clock.now()
            source_id = KnowledgeSourceId(self._id_factory("knowledge_source"))
            upload: UploadSession | None = None
            old_upload_generation: int | None = None
            file_metadata: FilePublicMetadata | None = None
            if isinstance(command.source_input, FileSourceInput):
                upload = await self._require_upload(
                    uow.repository,
                    workspace_id,
                    command.source_input.upload_session_id,
                    for_update=True,
                )
                self._require_completed_unclaimed_upload(upload)
                old_upload_generation = upload.generation
                file_metadata = FilePublicMetadata(
                    upload.declaration.filename,
                    upload.declaration.media_type,
                )
            source = KnowledgeSource.create(
                meta=ResourceMeta(source_id, 1, now, now),
                workspace_id=workspace_id,
                created_by=context.actor.user_id,
                name=command.name,
                source_input=command.source_input,
                visibility=command.visibility,
                file_metadata=file_metadata,
            )
            initial_version: KnowledgeSourceVersion | None = None
            if upload is not None:
                version_id = KnowledgeSourceVersionId(self._id_factory("knowledge_version"))
                if upload.view.artifact_ref is None:
                    raise AssertionError("completed upload must expose its artifact")
                source, initial_version = source.allocate_version(
                    version_id=version_id,
                    content_sha256=upload.declaration.sha256,
                    size_bytes=upload.declaration.size_bytes,
                    artifact_ref=upload.view.artifact_ref,
                    at=now,
                )
                upload = upload.claim_content(
                    ResourceRef("knowledge_source_version", version_id, 1)
                )
            await uow.repository.add_source(source, initial_version)
            if upload is not None and old_upload_generation is not None:
                await self._save_upload(
                    uow,
                    upload,
                    expected_generation=old_upload_generation,
                )
            await self._emit(
                uow,
                context,
                "knowledge_source.created",
                ResourceRef("knowledge_source", source_id, source.meta.revision),
                {
                    "source_type": source.source_type.value,
                    "current_version_id": None
                    if source.current_version_id is None
                    else str(source.current_version_id),
                },
                now,
            )
            await uow.commit()
            return source

    async def get_knowledge_source(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
    ) -> KnowledgeSource:
        """@brief 读取路径 Workspace 内来源 / Read a source within the path Workspace."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.READ_KNOWLEDGE_SOURCE
            )
            return await self._require_source(uow.repository, workspace_id, source_id)

    async def update_knowledge_source(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        command: UpdateKnowledgeSourceCommand,
        *,
        expected_revision: int,
    ) -> KnowledgeSource:
        """@brief 以强 If-Match/CAS 修改名称或 policy / Update name or policy through strong If-Match/CAS."""
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.UPDATE_KNOWLEDGE_SOURCE
            )
            source = await self._require_source(
                uow.repository,
                workspace_id,
                source_id,
                for_update=True,
            )
            self._require_revision(source.meta.revision, expected_revision)
            now = self._clock.now()
            changed = source.revise(name=command.name, visibility=command.visibility, at=now)
            await self._save_source(uow, changed, expected_revision=expected_revision)
            await self._emit(
                uow,
                context,
                "knowledge_source.updated",
                ResourceRef("knowledge_source", source_id, changed.meta.revision),
                {"policy_version": changed.visibility.policy_version},
                now,
            )
            await uow.commit()
            return changed

    async def delete_knowledge_source(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        *,
        expected_revision: int,
    ) -> Job:
        """@brief 标记 deleting 并原子创建统一删除 Job / Mark deleting and atomically create a unified deletion Job."""
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.DELETE_KNOWLEDGE_SOURCE
            )
            source = await self._require_source(
                uow.repository,
                workspace_id,
                source_id,
                for_update=True,
            )
            self._require_revision(source.meta.revision, expected_revision)
            now = self._clock.now()
            recovery_status = source.ingestion.status
            recovery_problem = source.ingestion.last_problem
            if recovery_status.is_active:
                recovery_status = (
                    KnowledgeIngestionStatus.STALE
                    if source.current_version_id is not None
                    and source.ingestion.last_success_at is not None
                    else KnowledgeIngestionStatus.NOT_STARTED
                )
                recovery_problem = None
            changed = source.begin_deletion(at=now)
            await self._save_source(uow, changed, expected_revision=expected_revision)
            job = self._job(
                workspace_id,
                KnowledgeJobKind.KNOWLEDGE_DELETE,
                ResourceRef("knowledge_source", source_id, changed.meta.revision),
                now,
            )
            await uow.jobs.add(
                job,
                KnowledgeDeleteSpec(
                    source_id,
                    changed.meta.revision,
                    source.enabled,
                    recovery_status,
                    recovery_problem,
                ),
            )
            await self._emit(
                uow,
                context,
                "knowledge_source.deletion_requested",
                ResourceRef("job", job.meta.id, job.meta.revision),
                {"job_id": str(job.meta.id)},
                now,
            )
            await uow.commit()
            return job

    async def get_knowledge_source_for_deletion(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
    ) -> KnowledgeSource:
        """@brief 以删除权限读取 If-Match 所需来源 / Read a source snapshot for If-Match under delete permission.

        @param principal 已验证 token principal / Verified token principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_id 待删除来源 / Source targeted for deletion.
        @return 已通过 DELETE_KNOWLEDGE_SOURCE 授权的来源 / Source authorized by DELETE_KNOWLEDGE_SOURCE.
        @raise KnowledgeResourceNotFound 来源不存在或属于其他 Workspace 时抛出 / Raised when
            absent or owned by another Workspace.
        @note 调用方必须把快照 revision 传回 ``delete_knowledge_source``；后者在变更事务内
            再次比较以封闭 TOCTOU / The caller must pass the snapshot revision back to
            ``delete_knowledge_source``, which compares it again in the mutation transaction.
        """

        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                WorkspaceAction.DELETE_KNOWLEDGE_SOURCE,
            )
            return await self._require_source(
                uow.repository,
                workspace_id,
                source_id,
            )

    async def list_knowledge_source_versions(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        page: KnowledgePageRequest | None = None,
    ) -> KnowledgePage[KnowledgeSourceVersion]:
        """@brief 列出一个来源的版本 / List versions of one source."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.READ_KNOWLEDGE_VERSIONS
            )
            await self._require_source(uow.repository, workspace_id, source_id)
            result = await uow.repository.list_versions(
                workspace_id,
                source_id,
                page or KnowledgePageRequest(),
            )
            if any(
                version.workspace_id != workspace_id or version.snapshot.source_id != source_id
                for version in result.items
            ):
                raise PermissionError("knowledge repository returned a cross-boundary version")
            return result

    async def create_knowledge_source_version(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        upload_session_id: UploadSessionId,
    ) -> KnowledgeSourceVersion:
        """@brief 在 source 行锁内分配版本号并唯一消费 upload / Allocate a version under lock and consume an upload once."""
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.CREATE_KNOWLEDGE_VERSION
            )
            source = await self._require_source(
                uow.repository,
                workspace_id,
                source_id,
                for_update=True,
            )
            upload = await self._require_upload(
                uow.repository,
                workspace_id,
                upload_session_id,
                for_update=True,
            )
            self._require_completed_unclaimed_upload(upload)
            if upload.view.artifact_ref is None:
                raise AssertionError("completed upload must expose its artifact")
            old_source_revision = source.meta.revision
            old_upload_generation = upload.generation
            now = self._clock.now()
            version_id = KnowledgeSourceVersionId(self._id_factory("knowledge_version"))
            changed, version = source.allocate_version(
                version_id=version_id,
                content_sha256=upload.declaration.sha256,
                size_bytes=upload.declaration.size_bytes,
                artifact_ref=upload.view.artifact_ref,
                at=now,
            )
            claimed = upload.claim_content(ResourceRef("knowledge_source_version", version_id, 1))
            await self._save_source(uow, changed, expected_revision=old_source_revision)
            await uow.repository.add_version(version)
            await self._save_upload(
                uow,
                claimed,
                expected_generation=old_upload_generation,
            )
            await self._emit(
                uow,
                context,
                "knowledge_source.version_created",
                ResourceRef("knowledge_source_version", version_id, 1),
                {"source_id": str(source_id), "version_number": version.snapshot.version_number},
                now,
            )
            await uow.commit()
            return version

    async def create_upload_session(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        declaration: UploadDeclaration,
    ) -> UploadSessionView:
        """@brief 兼容应用调用的 upload prepare+commit 组合 / Compose upload preparation and commit for direct callers."""

        prepared = await self.prepare_upload_session_creation(
            principal,
            workspace_id,
            declaration,
            self._direct_preparation_id(),
        )
        return await self.commit_upload_session_creation(
            principal,
            workspace_id,
            prepared,
        )

    async def prepare_upload_session_creation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        declaration: UploadDeclaration,
        operation_id: IdempotencyPreparationId,
    ) -> PreparedUploadSessionCreation:
        """@brief 在事务外幂等签发短期 PUT grant / Idempotently issue a short-lived PUT grant outside a transaction.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param declaration 冻结上传声明 / Frozen upload declaration.
        @param operation_id 对象存储去重 ID / Object-store deduplication ID.
        @return 待原子持久化的 UploadSession / UploadSession awaiting atomic persistence.
        """

        async with self._uow_factory() as authorization_uow:
            authorized = await self._authorize(
                authorization_uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_UPLOAD_SESSION,
            )
        now = self._clock.now()
        requested_expires_at = now + self._upload_lifetime
        upload_id = UploadSessionId(_prepared_resource_id("upload", operation_id))
        issued = await self._upload_store.issue_upload_grant(
            workspace_id,
            upload_id,
            declaration,
            expires_at=requested_expires_at,
            operation_id=operation_id,
        )
        if issued.expires_at > issued.issued_at + self._upload_lifetime or issued.expires_at <= now:
            raise RuntimeError("object store returned an unsafe upload grant lifetime")
        upload = UploadSession.create(
            upload_id=upload_id,
            workspace_id=workspace_id,
            declaration=declaration,
            grant=issued.grant,
            created_at=issued.issued_at,
            expires_at=issued.expires_at,
        )
        return PreparedUploadSessionCreation(authorized.actor.user_id, upload)

    async def commit_upload_session_creation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        prepared: PreparedUploadSessionCreation,
    ) -> UploadSessionView:
        """@brief 原子持久化已签发 grant 的 UploadSession / Atomically persist an UploadSession with an issued grant.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param prepared 对象存储准备结果 / Object-store preparation result.
        @return 公开 UploadSession / Public UploadSession.
        """

        if prepared.upload.view.workspace_id != workspace_id:
            raise PermissionError("prepared upload belongs to another workspace")
        async with self._uow_factory() as persistence_uow:
            current = await self._authorize(
                persistence_uow,
                principal,
                workspace_id,
                WorkspaceAction.CREATE_UPLOAD_SESSION,
            )
            if current.actor.user_id != prepared.actor_id:
                raise PermissionError("upload actor changed before persistence")
            await persistence_uow.repository.add_upload(prepared.upload)
            await persistence_uow.commit()
        return prepared.upload.view

    async def complete_upload_session(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        claim: UploadCompletionClaim,
    ) -> UploadSessionView:
        """@brief 兼容应用调用的 upload-completion prepare+commit 组合 / Compose upload completion phases for direct callers."""

        prepared = await self.prepare_upload_completion(
            principal,
            workspace_id,
            upload_id,
            claim,
            self._direct_preparation_id(),
        )
        return await self.commit_upload_completion(
            principal,
            workspace_id,
            prepared,
        )

    async def prepare_upload_completion(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        claim: UploadCompletionClaim,
        operation_id: IdempotencyPreparationId,
    ) -> PreparedUploadCompletion:
        """@brief 短事务 claim/resume verifying 后在事务外扫描 / Claim or resume verification in a short transaction, then scan outside it.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param upload_id UploadSession ID / UploadSession identifier.
        @param claim 冻结 completion 声明 / Frozen completion claim.
        @param operation_id 崩溃后仍稳定的 scan saga ID / Crash-stable scan-saga ID.
        @return 扫描完成、等待原子提交的结果 / Scanned result awaiting atomic commit.

        @note transient adapter 异常原样传播并保留 ``verifying``，同 operation ID 可恢复；
            只有 ``UploadVerificationRejected`` 才进入 failed / Transient adapter failures
            propagate while leaving ``verifying`` resumable by the same operation ID. Only a
            deterministic ``UploadVerificationRejected`` transitions to failed.
        """

        verification_id = UploadVerificationId(str(operation_id))
        verifying: UploadSession
        async with self._uow_factory() as first:
            await self._authorize(
                first, principal, workspace_id, WorkspaceAction.COMPLETE_UPLOAD_SESSION
            )
            upload = await self._require_upload(
                first.repository,
                workspace_id,
                upload_id,
                for_update=True,
            )
            if upload.view.status is UploadStatus.VERIFYING:
                try:
                    verifying = upload.resume_completion(claim, verification_id)
                except ValueError as error:
                    raise KnowledgeConflict(
                        "upload.completion_rejected",
                        "upload completion is owned by another operation",
                    ) from error
            elif (
                upload.view.status is UploadStatus.FAILED
                and upload.verification_operation_id == verification_id
                and upload.completion_claim == claim
            ):
                raise KnowledgeConflict(
                    upload.failure_code or "upload.verification_failed",
                    "uploaded content did not pass server verification",
                )
            else:
                expected_generation = upload.generation
                try:
                    verifying = upload.begin_completion(
                        claim,
                        verification_id,
                        at=self._clock.now(),
                    )
                except ValueError as error:
                    raise KnowledgeConflict(
                        "upload.completion_rejected",
                        "upload completion is unavailable or does not match the session",
                    ) from error
                await self._save_upload(
                    first,
                    verifying,
                    expected_generation=expected_generation,
                )
                await first.commit()
        try:
            evidence = await self._upload_store.verify_uploaded_object(
                workspace_id,
                upload_id,
                verifying.declaration,
                claim,
                operation_id=operation_id,
            )
        except UploadVerificationRejected as error:
            await self._record_upload_failure(
                principal,
                workspace_id,
                upload_id,
                verification_id,
                "upload.verification_failed",
            )
            raise KnowledgeConflict(
                "upload.verification_failed",
                "uploaded content did not pass server verification",
            ) from error
        return PreparedUploadCompletion(
            upload_id,
            verification_id,
            claim,
            evidence,
        )

    async def commit_upload_completion(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        prepared: PreparedUploadCompletion,
    ) -> UploadSessionView:
        """@brief 原子完成 owned verifying saga / Atomically complete the owned verification saga.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param prepared 可信扫描结果 / Trusted scan result.
        @return completed UploadSession / Completed UploadSession.
        """

        async with self._uow_factory() as second:
            await self._authorize(
                second, principal, workspace_id, WorkspaceAction.COMPLETE_UPLOAD_SESSION
            )
            current = await self._require_upload(
                second.repository,
                workspace_id,
                prepared.upload_id,
                for_update=True,
            )
            expected_generation = current.generation
            try:
                current.resume_completion(prepared.claim, prepared.operation_id)
                completed = current.complete(prepared.evidence, at=self._clock.now())
            except ValueError as error:
                if (
                    current.view.status is UploadStatus.VERIFYING
                    and current.verification_operation_id == prepared.operation_id
                ):
                    failed = current.fail(
                        "upload.verification_mismatch",
                        at=self._clock.now(),
                    )
                    await self._save_upload(
                        second,
                        failed,
                        expected_generation=expected_generation,
                    )
                    await second.commit()
                raise KnowledgeConflict(
                    "upload.verification_mismatch",
                    "uploaded content did not match the frozen declaration",
                ) from error
            await self._save_upload(
                second,
                completed,
                expected_generation=expected_generation,
            )
            await second.commit()
            return completed.view

    async def create_ingestion_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        command: CreateKnowledgeJobCommand,
    ) -> Job:
        """@brief 创建 knowledge.ingest Job / Create a knowledge.ingest Job."""
        return await self._create_processing_job(
            principal,
            workspace_id,
            source_id,
            command,
            KnowledgeJobKind.KNOWLEDGE_INGEST,
        )

    async def create_sync_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        command: CreateKnowledgeJobCommand,
    ) -> Job:
        """@brief 仅为 refreshable 来源创建 knowledge.sync Job / Create knowledge.sync only for refreshable sources."""
        return await self._create_processing_job(
            principal,
            workspace_id,
            source_id,
            command,
            KnowledgeJobKind.KNOWLEDGE_SYNC,
        )

    async def search_knowledge(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        request: KnowledgeSearchRequest,
    ) -> KnowledgeSearchResult:
        """@brief 重新授权 selection 后执行 hybrid search 并验证 citation provenance / Reauthorize selection, run hybrid search, and verify provenance."""
        async with self._uow_factory() as planning_uow:
            await self._authorize(
                planning_uow, principal, workspace_id, WorkspaceAction.SEARCH_KNOWLEDGE
            )
            scopes = await self._resolve_search_scopes(planning_uow, workspace_id, request)
        if not scopes:
            return KnowledgeSearchResult(request.query, (), 1)
        plan = KnowledgeSearchPlan(
            workspace_id,
            principal.user_id,
            request.query,
            scopes,
            request.selection.agent_scope,
            request.top_k,
            request.filters,
        )
        response = await self._search_engine.search(plan)
        self._validate_search_response(workspace_id, scopes, response)

        # Search indexes are external materialized views. Re-authorize after I/O and intersect
        # with the newest policy snapshot so a concurrent membership/policy revocation can only
        # remove results, never leak a now-forbidden citation.
        async with self._uow_factory() as verification_uow:
            await self._authorize(
                verification_uow, principal, workspace_id, WorkspaceAction.SEARCH_KNOWLEDGE
            )
            current_scopes = await self._resolve_search_scopes(
                verification_uow,
                workspace_id,
                request,
            )
        allowed_now = {(scope.source_id, scope.version_id) for scope in current_scopes}
        safe_hits = tuple(
            hit for hit in response.hits if (hit.source_id, hit.version_id) in allowed_now
        )
        ordered = sorted(
            safe_hits,
            key=lambda hit: (
                -hit.score.fused,
                str(hit.source_id),
                str(hit.version_id),
                hit.locator,
            ),
        )[: request.top_k]
        citations = tuple(KnowledgeCitation.from_hit(hit) for hit in ordered)
        return KnowledgeSearchResult(request.query, citations, response.policy_version)

    async def evaluate_knowledge_access(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        request: KnowledgeAccessEvaluationRequest,
    ) -> KnowledgeAccessEvaluationResult:
        """@brief 生成不可当作执行授权的最小充分 policy 解释 / Produce minimal explanations that are not execution grants."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.EVALUATE_KNOWLEDGE_ACCESS
            )
            decisions = []
            for source_id in request.source_ids:
                source = await self._require_source(uow.repository, workspace_id, source_id)
                decisions.append(
                    evaluate_visibility(
                        source_id=source_id,
                        enabled=source.enabled,
                        policy=source.visibility,
                        agent_scope=request.agent_scope,
                        operation=request.operation,
                        inference=request.inference,
                    )
                )
            return KnowledgeAccessEvaluationResult(self._clock.now(), tuple(decisions))

    async def _create_processing_job(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        command: CreateKnowledgeJobCommand,
        kind: KnowledgeJobKind,
    ) -> Job:
        """@brief 原子 queue 来源、Job 与 outbox / Atomically queue a source, Job, and outbox."""
        async with self._uow_factory() as uow:
            context = await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.CREATE_KNOWLEDGE_JOB
            )
            source = await self._require_source(
                uow.repository,
                workspace_id,
                source_id,
                for_update=True,
            )
            if kind is KnowledgeJobKind.KNOWLEDGE_SYNC and not source.source_type.supports_sync:
                raise KnowledgeConflict(
                    "knowledge_source.sync_unsupported",
                    "this knowledge source type cannot be synchronized",
                )
            old_revision = source.meta.revision
            now = self._clock.now()
            try:
                changed = source.queue_ingestion(force=command.force, at=now)
            except ValueError as error:
                raise KnowledgeConflict(
                    "knowledge_source.job_conflict",
                    "knowledge source cannot start this job in its current state",
                ) from error
            await self._save_source(uow, changed, expected_revision=old_revision)
            job = self._job(
                workspace_id,
                kind,
                ResourceRef("knowledge_source", source_id, changed.meta.revision),
                now,
            )
            await uow.jobs.add(
                job,
                KnowledgeProcessSpec(
                    source_id,
                    changed.meta.revision,
                    changed.current_version_id,
                    command.force,
                    context.actor.user_id,
                    source.ingestion.status,
                    source.ingestion.last_problem,
                ),
            )
            await self._emit(
                uow,
                context,
                "knowledge_source.job_created",
                ResourceRef("job", job.meta.id, job.meta.revision),
                {"job_id": str(job.meta.id), "kind": kind.value, "force": command.force},
                now,
            )
            await uow.commit()
            return job

    async def _resolve_search_scopes(
        self,
        uow: KnowledgeUnitOfWork,
        workspace_id: WorkspaceId,
        request: KnowledgeSearchRequest,
    ) -> tuple[KnowledgeSearchScope, ...]:
        """@brief 解析 selection、pin、policy 与 ready version / Resolve selection, pins, policy, and ready versions."""
        selection = request.selection
        if selection.mode is KnowledgeSelectionMode.NONE:
            return ()
        if selection.mode is KnowledgeSelectionMode.EXPLICIT:
            sources = tuple(
                [
                    await self._require_source(uow.repository, workspace_id, source_id)
                    for source_id in selection.include_source_ids
                ]
            )
        else:
            sources = await uow.repository.list_policy_default_sources(
                workspace_id,
                include_source_ids=selection.include_source_ids,
                exclude_source_ids=selection.exclude_source_ids,
                limit=200,
            )
        if len(sources) > 200:
            raise PermissionError("policy-default source resolver exceeded its hard bound")
        if any(source.workspace_id != workspace_id for source in sources):
            raise PermissionError("policy-default resolver returned a cross-workspace source")
        pins = {pin.source_id: pin for pin in selection.pinned_versions}
        scopes: list[KnowledgeSearchScope] = []
        seen: set[KnowledgeSourceId] = set()
        for source in sources:
            if source.meta.id in seen or source.meta.id in selection.exclude_source_ids:
                continue
            seen.add(source.meta.id)
            decision = evaluate_visibility(
                source_id=source.meta.id,
                enabled=source.enabled,
                policy=source.visibility,
                agent_scope=selection.agent_scope,
                operation=KnowledgeOperation.RETRIEVE,
                inference=None,
            )
            if decision.effect.value == "deny":
                continue
            pin = pins.get(source.meta.id)
            version_id = source.current_version_id if pin is None else pin.version_id
            if version_id is None:
                continue
            version = await self._require_version(
                uow.repository,
                workspace_id,
                source.meta.id,
                version_id,
            )
            if version.status.value != "ready":
                continue
            scopes.append(
                KnowledgeSearchScope(
                    source.meta.id,
                    version_id,
                    source.visibility.policy_version,
                )
            )
        return tuple(scopes)

    async def _record_upload_failure(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        operation_id: UploadVerificationId,
        code: str,
    ) -> None:
        """@brief 为 owning saga 在独立短事务持久化确定性失败 / Persist deterministic failure for the owning saga in a short transaction.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path workspace.
        @param upload_id UploadSession ID / UploadSession identifier.
        @param operation_id owning verification saga / Owning verification saga.
        @param code 稳定失败 code / Stable failure code.
        """

        async with self._uow_factory() as uow:
            await self._authorize(
                uow, principal, workspace_id, WorkspaceAction.COMPLETE_UPLOAD_SESSION
            )
            upload = await self._require_upload(
                uow.repository,
                workspace_id,
                upload_id,
                for_update=True,
            )
            if (
                upload.view.status is not UploadStatus.VERIFYING
                or upload.verification_operation_id != operation_id
            ):
                return
            expected_generation = upload.generation
            failed = upload.fail(code, at=self._clock.now())
            await self._save_upload(
                uow,
                failed,
                expected_generation=expected_generation,
            )
            await uow.commit()

    async def _validate_external_source_input(
        self,
        workspace_id: WorkspaceId,
        source_input: KnowledgeSourceInput,
        *,
        actor_id: UserId,
    ) -> None:
        """@brief 在数据库事务外验证网络和跨 context 引用 / Validate network and cross-context references outside transactions.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_input 待验证来源输入 / Source input to validate.
        @param actor_id 已认证且用于跨 context RLS 的 actor / Authenticated actor used for cross-context RLS.
        @raise KnowledgeResourceNotFound Resume 不属于路径 Workspace 时抛出 / Raised when a
            Resume does not belong to the path Workspace.
        """

        if isinstance(source_input, (UrlSourceInput, GitSourceInput)):
            await self._network_guard.validate(source_input)
        if isinstance(
            source_input, ResumeSourceInput
        ) and not await self._dependency_verifier.resume_exists(
            workspace_id,
            source_input.resume_id,
            actor_id=actor_id,
        ):
            raise KnowledgeResourceNotFound("resume")

    async def _validate_source_references(
        self,
        uow: KnowledgeUnitOfWork,
        workspace_id: WorkspaceId,
        source_input: KnowledgeSourceInput,
    ) -> None:
        """@brief 在写事务内重验同 context Connection 引用 / Revalidate same-context Connection references in the write transaction.

        @param uow 当前写工作单元 / Current write unit of work.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param source_input 待验证来源输入 / Source input to validate.
        """

        connection_id = None
        if isinstance(source_input, GitSourceInput):
            connection_id = source_input.connection_id
        elif isinstance(source_input, CloudDriveSourceInput):
            connection_id = source_input.connection_id
        if connection_id is not None:
            connection = await self._require_connection(
                uow.repository,
                workspace_id,
                connection_id,
            )
            if connection.connection.status is not ConnectionStatus.ACTIVE:
                raise KnowledgeConflict(
                    "connection.not_active",
                    "knowledge source requires an active connection",
                )

    def _direct_preparation_id(self) -> IdempotencyPreparationId:
        """@brief 为非 HTTP 直接调用生成一次性准备 ID / Generate a one-shot preparation ID for direct non-HTTP calls.

        @return 合法不透明准备 ID / Valid opaque preparation ID.
        @note HTTP 必须使用 executor 派生的崩溃稳定 ID；该路径只服务应用层测试、worker 或
            显式非重试调用 / HTTP must use the executor's crash-stable ID. This path exists only
            for direct application tests, workers, or explicitly non-retried calls.
        """

        return IdempotencyPreparationId(self._id_factory("preparation"))

    async def _authorize(
        self,
        uow: KnowledgeUnitOfWork,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        action: WorkspaceAction,
    ) -> WorkspaceAccessContext:
        """@brief 使用现有 AccessAuthorizer 并验证精确上下文 / Use the existing authorizer and verify exact context."""
        actor = await uow.authorizer.authenticate(principal)
        context = await uow.authorizer.authorize(actor, workspace_id, action)
        if (
            context.workspace_id != workspace_id
            or context.action is not action
            or context.actor.principal != principal
        ):
            raise PermissionError(
                "authorization context does not match the requested knowledge action"
            )
        return context

    @staticmethod
    async def _require_connection(
        repository: KnowledgeRepository,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAggregate:
        """@brief 读取 Connection 并隐藏跨租户结果 / Read a Connection and hide cross-tenant results."""
        connection = await repository.get_connection(
            workspace_id,
            connection_id,
            for_update=for_update,
        )
        if connection is None or connection.connection.workspace_id != workspace_id:
            raise KnowledgeResourceNotFound("connection")
        return connection

    @staticmethod
    async def _require_source(
        repository: KnowledgeRepository,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        *,
        for_update: bool = False,
    ) -> KnowledgeSource:
        """@brief 读取来源并隐藏跨租户结果 / Read a source and hide cross-tenant results."""
        source = await repository.get_source(workspace_id, source_id, for_update=for_update)
        if source is None or source.workspace_id != workspace_id:
            raise KnowledgeResourceNotFound("knowledge_source")
        return source

    @staticmethod
    async def _require_version(
        repository: KnowledgeRepository,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        version_id: KnowledgeSourceVersionId,
    ) -> KnowledgeSourceVersion:
        """@brief 读取版本并验证 Workspace+source 归属 / Read a version and verify Workspace+source ownership."""
        version = await repository.get_version(workspace_id, source_id, version_id)
        if (
            version is None
            or version.workspace_id != workspace_id
            or version.snapshot.source_id != source_id
        ):
            raise KnowledgeResourceNotFound("knowledge_source_version")
        return version

    @staticmethod
    async def _require_upload(
        repository: KnowledgeRepository,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        *,
        for_update: bool = False,
    ) -> UploadSession:
        """@brief 读取 upload 并隐藏跨租户结果 / Read an upload and hide cross-tenant results."""
        upload = await repository.get_upload(workspace_id, upload_id, for_update=for_update)
        if upload is None or upload.view.workspace_id != workspace_id:
            raise KnowledgeResourceNotFound("upload_session")
        return upload

    @staticmethod
    def _require_completed_unclaimed_upload(upload: UploadSession) -> None:
        """@brief 要求 upload 已完成且尚未消费 / Require a completed, unconsumed upload."""
        if upload.view.status is not UploadStatus.COMPLETED or upload.claimed_by is not None:
            raise KnowledgeConflict(
                "upload.not_claimable",
                "upload session is not completed or has already been consumed",
            )

    @staticmethod
    def _require_authorization_fingerprint(
        record: ConnectionAuthorizationRecord,
        command: CreateConnectionAuthorizationSessionCommand,
    ) -> None:
        """@brief 比较专用授权 session 请求指纹 / Compare a dedicated authorization-session request fingerprint.

        @param record 已持久化授权记录 / Persisted authorization record.
        @param command 当前创建命令 / Current creation command.
        @raise IdempotencyConflict 同 key 输入不同时抛出 / Raised when the same key is reused
            with different input.
        """

        if not hmac.compare_digest(
            record.idempotency.request_fingerprint,
            command.request_fingerprint,
        ):
            raise IdempotencyConflict("idempotency.key_reused")

    @staticmethod
    def _require_revision(current: int, expected: int) -> None:
        """@brief 在修改前检查强 If-Match revision / Check strong If-Match revision before mutation."""
        if current != expected:
            raise KnowledgePreconditionFailed

    @staticmethod
    async def _save_connection(
        uow: KnowledgeUnitOfWork,
        connection: ConnectionAggregate,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 将 Connection CAS 失败归一为 412 / Normalize Connection CAS failure to 412."""
        try:
            await uow.repository.save_connection(
                connection,
                expected_revision=expected_revision,
            )
        except KnowledgeCasMismatch as error:
            raise KnowledgePreconditionFailed from error

    @staticmethod
    async def _save_source(
        uow: KnowledgeUnitOfWork,
        source: KnowledgeSource,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 将来源 CAS 失败归一为 412 / Normalize source CAS failure to 412."""
        try:
            await uow.repository.save_source(source, expected_revision=expected_revision)
        except KnowledgeCasMismatch as error:
            raise KnowledgePreconditionFailed from error

    @staticmethod
    async def _save_upload(
        uow: KnowledgeUnitOfWork,
        upload: UploadSession,
        *,
        expected_generation: int,
    ) -> None:
        """@brief 将 upload generation CAS 失败归一为冲突 / Normalize upload CAS failure to conflict."""
        try:
            await uow.repository.save_upload(
                upload,
                expected_generation=expected_generation,
            )
        except KnowledgeCasMismatch as error:
            raise KnowledgeConflict(
                "upload.concurrent_transition",
                "upload session changed concurrently",
            ) from error

    def _job(
        self,
        workspace_id: WorkspaceId,
        kind: KnowledgeJobKind,
        subject: ResourceRef,
        at: datetime,
    ) -> Job:
        """@brief 构造统一 queued platform Job / Construct a unified queued platform Job."""
        return Job(
            ResourceMeta(JobId(self._id_factory("job")), 1, at, at),
            workspace_id,
            kind.value,
            subject,
        )

    async def _emit(
        self,
        uow: KnowledgeUnitOfWork,
        context: WorkspaceAccessContext,
        event_type: str,
        subject: ResourceRef,
        data: dict[str, str | int | bool | None],
        at: datetime,
    ) -> None:
        """@brief 添加不含 secret 的 transactional outbox 事件 / Add a secret-free transactional outbox event."""
        await uow.outbox.add(
            KnowledgeOutboxEvent(
                KnowledgeOutboxEventId(self._id_factory("event")),
                context.workspace_id,
                event_type,
                subject,
                context.actor.user_id,
                at,
                data,
            )
        )

    @staticmethod
    def _validate_search_response(
        workspace_id: WorkspaceId,
        scopes: Sequence[KnowledgeSearchScope],
        response: HybridSearchResponse,
    ) -> None:
        """@brief 把检索 adapter 当作不可信并重验 provenance / Treat search output as untrusted and revalidate provenance."""
        allowed = {(scope.source_id, scope.version_id) for scope in scopes}
        if any(
            hit.workspace_id != workspace_id or (hit.source_id, hit.version_id) not in allowed
            for hit in response.hits
        ):
            raise PermissionError("hybrid search returned a hit outside its authorized plan")


def _prepared_resource_id(prefix: str, operation_id: IdempotencyPreparationId) -> str:
    """@brief 从稳定准备 ID 派生确定性资源 ID / Derive a deterministic resource ID from a stable preparation ID.

    @param prefix 领域资源前缀 / Domain resource prefix.
    @param operation_id executor 派生的稳定 ID / Stable executor-derived ID.
    @return 不暴露幂等 key 的不透明资源 ID / Opaque resource ID that does not expose the
        idempotency key.
    @raise ValueError prefix 或 operation ID 非规范时抛出 / Raised for a non-canonical prefix or ID.
    """

    if not prefix or not prefix.isascii() or not prefix.replace("_", "").isalnum():
        raise ValueError("prepared resource prefix is invalid")
    if not operation_id or str(operation_id).strip() != operation_id:
        raise ValueError("preparation operation ID is invalid")
    digest = hashlib.sha256(str(operation_id).encode("utf-8")).hexdigest()[:32]
    return f"{prefix}_{digest}"


__all__ = [
    "V2_KNOWLEDGE_ENDPOINT_METHODS",
    "CreateConnectionAuthorizationSessionCommand",
    "CreateConnectionCommand",
    "CreateKnowledgeJobCommand",
    "CreateKnowledgeSourceCommand",
    "InvalidKnowledgeCommand",
    "KnowledgeApplicationError",
    "KnowledgeApplicationService",
    "KnowledgeConflict",
    "KnowledgePreconditionFailed",
    "KnowledgeResourceNotFound",
    "PreparedConnectionCreation",
    "PreparedKnowledgeSourceCreation",
    "PreparedUploadCompletion",
    "PreparedUploadSessionCreation",
    "SecureStateFactory",
    "UpdateKnowledgeSourceCommand",
    "UtcClock",
]
