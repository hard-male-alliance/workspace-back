"""@brief 后端 composition root / Backend composition root."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import secrets
import socket
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

import httpx

from backend.api.constants import PUBLIC_ORIGIN
from backend.api.v2_http import CursorCodec
from backend.application.access import AccessApplicationService
from backend.application.account_deletion import (
    AccountDeletionExecutionService,
    AccountDeletionRunResult,
)
from backend.application.agent_v2 import (
    AgentApplicationService as V2AgentApplicationService,
)
from backend.application.agent_v2 import AgentWorkerService
from backend.application.agent_worker import (
    AGENT_WORK_EVENT_TYPES,
    AgentRunOutboxHandler,
)
from backend.application.concurrency import BoundedTaskSupervisor, WorkLimits
from backend.application.diagnostics import DiagnosticIngestionService, DiagnosticRateLimiter
from backend.application.identity import HostedIdentityService
from backend.application.interview_v2 import (
    InterviewApplicationService as V2InterviewApplicationService,
)
from backend.application.interview_v2 import (
    InterviewPortProtocolError,
    InterviewWorkerService,
)
from backend.application.interview_worker import (
    INTERVIEW_WORK_EVENT_TYPES,
    InterviewJobOutboxHandler,
)
from backend.application.knowledge import (
    KnowledgeApplicationService as V2KnowledgeApplicationService,
)
from backend.application.knowledge_worker import (
    KNOWLEDGE_WORK_EVENT_TYPES,
    KnowledgeWorkerService,
)
from backend.application.maintenance import (
    MaintenanceBatchSizes,
    MaintenanceRunResult,
    V2MaintenanceService,
)
from backend.application.oauth import OAuthAuthorizationService
from backend.application.outbox_dispatch import (
    OutboxDispatchResult,
    OutboxDispatchService,
    OutboxDispatchSettings,
)
from backend.application.platform import PlatformApplicationService
from backend.application.ports.access import AccessUnitOfWorkFactory
from backend.application.ports.agent_v2 import (
    AgentModelRoute,
    AgentUnitOfWorkFactory,
    AgentWorkerUnitOfWorkFactory,
)
from backend.application.ports.interview_v2 import (
    InterviewRealtimeGateway,
    InterviewReportProvider,
    InterviewUnitOfWorkFactory,
)
from backend.application.ports.knowledge import (
    ConnectionAuthorizationGateway,
    ConnectionCredentialBroker,
    KnowledgeUnitOfWorkFactory,
)
from backend.application.ports.knowledge_worker import (
    KnowledgeCredentialRevoker,
    KnowledgeWorkerClaim,
    KnowledgeWorkerTerminalFailure,
)
from backend.application.ports.maintenance import MaintenanceRepository
from backend.application.ports.platform import (
    ArtifactContentStore,
    PlatformEventFeed,
    PlatformUnitOfWorkFactory,
)
from backend.application.ports.resume_worker import ResumeUploadObjectReader
from backend.application.ports.resumes import ResumeUnitOfWorkFactory
from backend.application.ports.v2_idempotency import V2IdempotencyExecutor
from backend.application.resume_worker import (
    RESUME_WORK_EVENT_TYPES,
    ResumeJobOutboxHandler,
    ResumeJobWorkerService,
)
from backend.application.resumes import ResumeApplicationService as V2ResumeApplicationService
from backend.application.services import (
    AgentApplicationService as LegacyAgentApplicationService,
)
from backend.application.services import (
    InterviewApplicationService,
    KnowledgeApplicationService,
    ResumeApplicationService,
    ResumeProposalApplicationService,
    ScopedKeyLocks,
    ServiceDependencies,
    WorkspaceApplicationService,
)
from backend.config import (
    AIProviderEndpoint,
    BackendSettings,
    KnowledgeLocalUploadStorageSettings,
    KnowledgeS3UploadStorageSettings,
)
from backend.domain.connections import ConnectionProvider
from backend.domain.interview_v2 import (
    CreateRealtimeConnectionSpec,
    InterviewSession,
    RealtimeConnection,
    RealtimeConnectionId,
    RealtimeTransport,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.observability import ResourceMetadata
from backend.domain.ports import (
    AgentRepository,
    ArtifactRepository,
    BreachedPasswordChecker,
    EmbeddingProvider,
    HostedIdentityRepository,
    IdentityEmailSender,
    InterviewRepository,
    JobRepository,
    KnowledgeRepository,
    OAuthAuthorizationRequestRepository,
    ResumeProposalRepository,
    ResumeRepository,
    TelemetryWriter,
    WorkspaceRepository,
)
from backend.domain.principals import WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.workspaces import DataRegion
from backend.infrastructure.access import (
    InMemoryAccessStore,
    InMemoryAccessUnitOfWorkFactory,
    PostgresAccessUnitOfWorkFactory,
)
from backend.infrastructure.account_deletion import PostgresAccountDeletionExecutionPort
from backend.infrastructure.agent_provider import (
    EmptyAgentToolRegistry,
    StreamingTextAgentProvider,
    UnavailableAgentToolExecutor,
)
from backend.infrastructure.agent_retrieval import GrantedAgentKnowledgeRetriever
from backend.infrastructure.agent_v2 import (
    InMemoryAgentDispatchResult,
    InMemoryAgentDispatchService,
    InMemoryAgentPolicyStore,
    InMemoryAgentStore,
    InMemoryAgentUnitOfWorkFactory,
    InMemoryAgentWorkerUnitOfWorkFactory,
    PostgresAgentUnitOfWorkFactory,
    PostgresAgentWorkerUnitOfWorkFactory,
)
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.embeddings import (
    DeterministicEmbeddingProvider,
    OpenAICompatibleEmbeddingProvider,
)
from backend.infrastructure.hosted_identity import (
    InMemoryHostedIdentityRepository,
    PostgresHostedIdentityRepository,
)
from backend.infrastructure.idempotency import IdempotencyRegistry
from backend.infrastructure.identity import IdentityResolver, build_identity_resolver
from backend.infrastructure.identity_email import (
    MemoryIdentityEmailSender,
    identity_email_transport_for,
)
from backend.infrastructure.identity_email_outbox import (
    IdentityEmailKeyring,
    IdentityEmailOutboxWorker,
    IdentityEmailWorkerResult,
    PostgresIdentityEmailOutbox,
)
from backend.infrastructure.interview import (
    ConsentAwareInterviewMediaFinalizer,
    FailClosedInterviewReportProvider,
    HmacInterviewRealtimeGateway,
    InMemoryInterviewUnitOfWorkFactory,
    InterviewRealtimeSigningKey,
    InterviewRealtimeSigningKeyring,
    PostgresInterviewUnitOfWorkFactory,
    StaticInterviewSessionPolicy,
)
from backend.infrastructure.interview_report import (
    DeterministicInterviewReportProvider,
    StreamingJsonInterviewReportProvider,
)
from backend.infrastructure.knowledge import (
    AesGcmAuthorizationLaunchCipher,
    InMemoryKnowledgeUnitOfWorkFactory,
    PostgresKnowledgeUnitOfWorkFactory,
)
from backend.infrastructure.knowledge_connections import (
    ApiTokenValidation,
    ConnectionProviderDefinition,
    ConnectionProviderRegistry,
    ConnectionSecretKeyring,
    ConnectionVaultKey,
    PostgresConnectionCredentialVault,
    ProviderConnectionAdapter,
    ProviderCredentialRevoker,
    UnavailableConnectionAdapter,
)
from backend.infrastructure.knowledge_network import (
    PinnedHttpSourceFetcher,
    SourceNetworkPolicy,
    StrictSourceNetworkGuard,
)
from backend.infrastructure.knowledge_parsing import LocalKnowledgeFileParser
from backend.infrastructure.knowledge_search import (
    EmbeddingSpaceSelection,
    MemoryHybridKnowledgeSearch,
    MemoryKnowledgeDependencyVerifier,
    PostgresHybridKnowledgeSearch,
    PostgresKnowledgeDependencyVerifier,
)
from backend.infrastructure.knowledge_storage import LocalKnowledgeBlobStorage
from backend.infrastructure.knowledge_uploads import (
    BoundedUploadErasure,
    ClamAvInstreamScanner,
    DevelopmentAllowAllMalwareScanner,
    LocalSignedUploadStore,
    MalwareScanner,
    MemoryUploadQuotaLedger,
    PostgresUploadQuotaLedger,
    RejectingMalwareScanner,
    S3UploadObjectStore,
    S3UploadSettings,
    UploadSafetyLimits,
)
from backend.infrastructure.knowledge_worker import PostgresKnowledgeWorkerStore
from backend.infrastructure.knowledge_worker_pipeline import (
    CompositeKnowledgeSourceEraser,
    KnowledgeIndexPipeline,
    PostgresKnowledgeMaterialLoader,
)
from backend.infrastructure.maintenance import (
    InMemoryMaintenanceRepository,
    PostgresMaintenanceRepository,
)
from backend.infrastructure.memory import InMemoryWorkspaceRepository
from backend.infrastructure.oauth import (
    InMemoryOAuthAuthorizationRequestRepository,
    PostgresOAuthAuthorizationRequestRepository,
)
from backend.infrastructure.oauth_tokens import OAuthTokenSigner
from backend.infrastructure.observability.logging import LoggingRuntime, configure_logging
from backend.infrastructure.observability.pipeline import (
    InMemoryTelemetryWriter,
    ObservabilityPipeline,
)
from backend.infrastructure.outbox_dispatch import PostgresOutboxClaimRepository
from backend.infrastructure.password_breach import PwnedPasswordsChecker
from backend.infrastructure.persistence import (
    AsyncDatabase,
    AsyncDatabaseOptions,
    PostgresIdempotencyRegistry,
    PostgresTelemetryWriter,
    PostgresWorkspaceRepository,
)
from backend.infrastructure.platform import (
    InMemoryPlatformUnitOfWorkFactory,
    PostgresPlatformUnitOfWorkFactory,
)
from backend.infrastructure.providers import (
    AgentModelProvider,
    FallbackModelProvider,
    MockModelProvider,
    OpenAICompatibleModelProvider,
    ProviderRateLimiter,
)
from backend.infrastructure.rendering import renderer_for
from backend.infrastructure.resume_worker import MultiFormatResumeRenderer, SafeResumeImporter
from backend.infrastructure.resumes import (
    InMemoryResumeUnitOfWorkFactory,
    PostgresResumeUnitOfWorkFactory,
    PostgresResumeWorkerUnitOfWorkFactory,
)
from backend.infrastructure.v2_idempotency import (
    AtomicPostgresIdempotencyExecutor,
    InMemoryIdempotencyExecutor,
    InMemoryV2IdempotencyStore,
)
from backend.package_resources import read_contract_schema_text

logger = logging.getLogger(__name__)
"""@brief composition root 稳定事件 logger / Stable-event logger for the composition root."""


class WorkspaceRuntimeRepository(
    WorkspaceRepository,
    ResumeRepository,
    AgentRepository,
    InterviewRepository,
    KnowledgeRepository,
    JobRepository,
    ArtifactRepository,
    ResumeProposalRepository,
    Protocol,
):
    """@brief 后端运行时需要的聚合 Repository 交集 / Aggregate repository intersection needed at runtime.

    内存与 PostgreSQL 适配器都在同一进程内实现这六个领域端口；该组合根专用协议让
    具体实现保持可替换，而不把基础设施类型泄漏给 application service。
    """


class _OutboxDispatchResult(Protocol):
    """@brief 领域 outbox loop 记录所需的最小结果 / Minimal result shape logged by a domain outbox loop."""

    @property
    def claimed(self) -> int:
        """@brief 返回本轮 claim 数 / Return claims in this pass."""

    @property
    def completed(self) -> int:
        """@brief 返回本轮完成数 / Return completions in this pass."""


class _OutboxDispatchService(Protocol):
    """@brief memory/PostgreSQL 领域 dispatcher 的共同窄形状 / Shared narrow shape of memory/PostgreSQL domain dispatchers."""

    async def run_once(self) -> _OutboxDispatchResult:
        """@brief 执行一个有界批次 / Run one bounded pass."""


@dataclass(frozen=True, slots=True)
class _AgentRuntimeComponents:
    """@brief composition 构造的 Agent V2 对象图 / Agent V2 object graph built by composition."""

    application: V2AgentApplicationService
    """@brief 12 条公开路由的应用服务 / Application service for all 12 public routes."""

    worker: AgentWorkerService
    """@brief UoW 外执行 provider/tool 的 worker / Worker executing provider/tool work outside UoWs."""

    dispatcher: _OutboxDispatchService
    """@brief 仅消费 Agent-owned queued 事件的 dispatcher / Dispatcher consuming only Agent-owned queued events."""


@dataclass(frozen=True, slots=True)
class _IdentityEmailRuntimeComponents:
    """@brief 身份邮件入队、投递与擦除对象图 / Identity-email enqueue, delivery, and erasure graph."""

    sender: IdentityEmailSender
    """@brief Hosted Identity 仅见的入队端口 / Enqueue-only port exposed to Hosted Identity."""

    worker: IdentityEmailOutboxWorker | None
    """@brief 仅 PostgreSQL 的 durable delivery worker / PostgreSQL-only durable delivery worker."""

    erasure: PostgresIdentityEmailOutbox | None
    """@brief 账户删除使用的收件人擦除端口 / Recipient-erasure port used by account deletion."""


@dataclass(frozen=True, slots=True)
class _KnowledgeRuntimeComponents:
    """@brief Knowledge V2 公开服务、worker 与擦除对象图 / Knowledge V2 public, worker, and erasure graph."""

    application: V2KnowledgeApplicationService
    """@brief 5.3 公开路由的应用服务 / Application service for public section 5.3 routes."""

    dispatcher: _OutboxDispatchService | None
    """@brief PostgreSQL durable worker dispatcher；memory 模式不伪造持久 worker / PostgreSQL durable worker dispatcher; absent in memory mode."""

    local_upload_store: LocalSignedUploadStore | None
    """@brief 仅 development/test 可 mount 的签名 PUT store / Signed PUT store mountable only in development/test."""

    upload_erasure: BoundedUploadErasure
    """@brief 账户删除可复用的有界对象擦除器 / Bounded object eraser reused by account deletion."""

    creator_secret_erasure: PostgresConnectionCredentialVault | None
    """@brief PostgreSQL Connection credential 加密擦除端口 / PostgreSQL Connection-credential crypto-erasure port."""

    upload_reader: ResumeUploadObjectReader
    """@brief Resume import 复用的 Workspace-scoped 对象 reader / Workspace-scoped object reader reused by Resume import."""


@dataclass(frozen=True, slots=True)
class _ResumeRuntimeComponents:
    """@brief Resume V2 公开服务与 durable worker 图 / Resume V2 public-service and durable-worker graph."""

    application: V2ResumeApplicationService
    """@brief Resume V2 公开应用服务 / Public Resume V2 application service."""

    dispatcher: _OutboxDispatchService | None
    """@brief PostgreSQL durable dispatcher；memory 模式不伪造 worker / PostgreSQL durable dispatcher; absent in memory mode."""


@dataclass(frozen=True, slots=True)
class _InterviewRuntimeComponents:
    """@brief Interview V2 公开服务与 durable worker 对象图 / Interview V2 public-service and durable-worker graph."""

    application: V2InterviewApplicationService
    """@brief 5.5 公开路由的应用服务 / Application service for public section 5.5 routes."""

    dispatcher: _OutboxDispatchService | None
    """@brief PostgreSQL Job dispatcher；memory 模式同步持久但不伪造 durable worker / PostgreSQL Job dispatcher; absent in memory mode."""

    realtime_gateway: InterviewRealtimeGateway
    """@brief 短期、绑定 audience 的 realtime credential gateway / Short-lived audience-bound realtime credential gateway."""


class _UnavailableInterviewRealtimeGateway:
    """@brief development/test 未配置 signaling 时显式失败 / Explicitly fail when signaling is unconfigured in development/test."""

    async def issue(
        self,
        workspace_id: WorkspaceId,
        session: InterviewSession,
        audience: ResourceRef,
        spec: CreateRealtimeConnectionSpec,
        *,
        issued_at: datetime,
    ) -> RealtimeConnection:
        """@brief 拒绝签发指向虚构 endpoint 的凭据 / Refuse credentials for a fabricated endpoint.

        @return 永不返回 / Never returns.
        @raise InterviewPortProtocolError signaling capability 未配置 / Signaling capability is
            unconfigured.
        """
        del workspace_id, session, audience, spec, issued_at
        raise InterviewPortProtocolError(
            "interview.realtime_unconfigured",
            "Interview realtime signaling is not configured",
        )

    async def revoke(self, connection_id: RealtimeConnectionId) -> None:
        """@brief 未签发凭据时 no-op / No-op when no credential was issued."""
        del connection_id


class _UnavailableKnowledgeCredentialRevoker:
    """@brief 无 durable credential vault 时终止 revoke Job / Terminally reject revoke Jobs without a durable vault."""

    async def revoke(
        self,
        claim: KnowledgeWorkerClaim,
        *,
        operation_id: str,
    ) -> None:
        """@brief 以公开安全错误终止，不伪造远端撤销 / Fail terminally without manufacturing remote revocation.

        @param claim 已冻结的 Connection revoke claim / Frozen Connection-revocation claim.
        @param operation_id 稳定重放 ID / Stable replay ID.
        @raise KnowledgeWorkerTerminalFailure 始终抛出 / Always raised.
        """

        del claim, operation_id
        raise KnowledgeWorkerTerminalFailure("connection.credential_vault_unconfigured")


@dataclass(slots=True)
class BackendContainer:
    """@brief 一个 worker 的完整运行时对象图 / Complete runtime object graph for one worker."""

    settings: BackendSettings
    identity: IdentityResolver
    contracts: ContractValidator
    contracts_v2: ContractValidator
    idempotency: IdempotencyRegistry | PostgresIdempotencyRegistry
    workspace: WorkspaceApplicationService
    resume: ResumeApplicationService
    resumes_v2: V2ResumeApplicationService
    platform: PlatformApplicationService
    proposals: ResumeProposalApplicationService
    agent: LegacyAgentApplicationService
    agent_v2: V2AgentApplicationService
    interview: InterviewApplicationService
    interview_v2: V2InterviewApplicationService
    knowledge: KnowledgeApplicationService
    knowledge_v2: V2KnowledgeApplicationService
    knowledge_local_upload_store: LocalSignedUploadStore | None
    model_provider: AgentModelProvider
    supervisor: BoundedTaskSupervisor
    telemetry: ObservabilityPipeline
    telemetry_writer: TelemetryWriter
    logging_runtime: LoggingRuntime
    diagnostics: DiagnosticIngestionService
    oauth: OAuthAuthorizationService
    hosted_identity: HostedIdentityService
    access: AccessApplicationService
    maintenance: V2MaintenanceService
    v2_cursor: CursorCodec
    v2_idempotency: V2IdempotencyExecutor
    sensitive_idempotency_key: bytes
    database: AsyncDatabase | None


@asynccontextmanager
async def build_container(
    settings: BackendSettings, runtime_root: Path
) -> AsyncIterator[BackendContainer]:
    """@brief 创建并销毁一个后端运行时 / Create and tear down one backend runtime.

    @param settings 后端强类型设置 / Backend typed settings.
    @param runtime_root 相对日志与数据路径的配置目录 / Configuration directory anchoring relative log and data paths.
    @return 生命周期拥有的容器 / Lifespan-owned container.
    @raise RuntimeError PostgreSQL DSN 缺失时抛出 / Raised when the PostgreSQL DSN is missing.
    """
    identity = build_identity_resolver(
        environment=settings.environment,
        default_scope=settings.default_scope,
        security=settings.security,
    )
    oauth_token_signer = OAuthTokenSigner.from_paths(
        settings.oauth.signing_private_key_paths,
        runtime_root=runtime_root,
        allow_generate=settings.environment in {"development", "test"},
    )
    supervisor = BoundedTaskSupervisor(
        (
            WorkLimits("llm", settings.runtime.llm_concurrency),
            WorkLimits("render", settings.runtime.render_concurrency),
            WorkLimits("knowledge", settings.runtime.knowledge_concurrency),
            WorkLimits("interview", settings.runtime.interview_concurrency),
            WorkLimits("agent", 1),
            WorkLimits("identity_email", 1),
            WorkLimits("maintenance", 1),
            WorkLimits("account_deletion", 1),
        ),
        settings.runtime.job_queue_capacity,
        settings.runtime.shutdown_grace_ms,
    )
    database: AsyncDatabase | None = None
    telemetry_database: AsyncDatabase | None = None
    oauth_repository: OAuthAuthorizationRequestRepository
    hosted_identity_repository: HostedIdentityRepository
    access_uow_factory: AccessUnitOfWorkFactory
    maintenance_repository: MaintenanceRepository
    resume_v2_uow_factory: ResumeUnitOfWorkFactory
    platform_uow_factory: PlatformUnitOfWorkFactory
    platform_content_store: ArtifactContentStore
    platform_event_feed: PlatformEventFeed
    memory_access_store: InMemoryAccessStore | None = None
    data_region = DataRegion(settings.workspace_default_data_region)
    try:
        if settings.database.mode == "postgresql":
            dsn = settings.database.application_dsn
            if not dsn:
                raise RuntimeError("PostgreSQL application DSN is not configured")
            database = AsyncDatabase(
                AsyncDatabaseOptions(
                    dsn=dsn,
                    pool_size=settings.database.pool_size,
                    max_overflow=settings.database.max_overflow,
                    connect_timeout_s=settings.database.connect_timeout_ms / 1_000,
                    statement_timeout_ms=settings.database.statement_timeout_ms,
                    lock_timeout_ms=settings.database.lock_timeout_ms,
                )
            )
            storage: WorkspaceRuntimeRepository = PostgresWorkspaceRepository(database)
            telemetry_database = AsyncDatabase(
                AsyncDatabaseOptions(
                    dsn=dsn,
                    pool_size=settings.observability.writer.pool_size,
                    max_overflow=0,
                    pool_timeout_s=settings.observability.writer.connect_timeout_ms / 1_000,
                    connect_timeout_s=settings.observability.writer.connect_timeout_ms / 1_000,
                    statement_timeout_ms=settings.observability.writer.statement_timeout_ms,
                    lock_timeout_ms=settings.observability.writer.lock_timeout_ms,
                )
            )
            telemetry_writer: TelemetryWriter = PostgresTelemetryWriter(telemetry_database)
            idempotency: IdempotencyRegistry | PostgresIdempotencyRegistry = (
                PostgresIdempotencyRegistry(database)
            )
            v2_idempotency: V2IdempotencyExecutor = AtomicPostgresIdempotencyExecutor(
                database,
                retention=timedelta(days=30),
            )
            maintenance_repository = PostgresMaintenanceRepository(database)
            oauth_repository = PostgresOAuthAuthorizationRequestRepository(database)
            hosted_identity_repository = PostgresHostedIdentityRepository(
                database,
                data_region=data_region,
            )
            access_uow_factory = PostgresAccessUnitOfWorkFactory(database)
            resume_v2_uow_factory = PostgresResumeUnitOfWorkFactory(database)
            postgres_platform = PostgresPlatformUnitOfWorkFactory(
                database,
                api_origin=PUBLIC_ORIGIN,
            )
            platform_uow_factory = postgres_platform
            platform_content_store = postgres_platform.content_store
            platform_event_feed = postgres_platform.event_feed
        else:
            storage = InMemoryWorkspaceRepository()
            telemetry_writer = InMemoryTelemetryWriter()
            idempotency = IdempotencyRegistry()
            memory_v2_idempotency_store = InMemoryV2IdempotencyStore()
            v2_idempotency = InMemoryIdempotencyExecutor(
                memory_v2_idempotency_store,
                retention=timedelta(days=30),
            )
            memory_oauth_repository = InMemoryOAuthAuthorizationRequestRepository()
            oauth_repository = memory_oauth_repository
            memory_identity_repository = InMemoryHostedIdentityRepository(
                data_region=data_region,
                revoke_token_families=(memory_oauth_repository.revoke_families_for_login_session),
            )
            hosted_identity_repository = memory_identity_repository
            memory_access_store = memory_identity_repository.access_store
            access_uow_factory = InMemoryAccessUnitOfWorkFactory(memory_access_store)
            resume_v2_uow_factory = InMemoryResumeUnitOfWorkFactory(memory_access_store)
            memory_platform = InMemoryPlatformUnitOfWorkFactory(memory_access_store)
            platform_uow_factory = memory_platform
            platform_content_store = memory_platform.content_store
            platform_event_feed = memory_platform.event_feed
            maintenance_repository = InMemoryMaintenanceRepository(
                memory_access_store,
                memory_v2_idempotency_store,
            )
    except BaseException as database_initialization_error:
        try:
            await _close_runtime_resources(
                None,
                None,
                None,
                database,
                telemetry_database,
            )
        except BaseException:
            database_initialization_error.add_note(
                "one or more database resources also failed during initialization cleanup"
            )
        raise
    telemetry: ObservabilityPipeline | None = None
    logging_runtime = None
    try:
        telemetry = ObservabilityPipeline(
            telemetry_writer,
            ResourceMetadata(
                service="backend",
                service_version="0.1.0",
                deployment_environment=settings.environment,
                service_instance_id=socket.gethostname()[:128],
            ),
            queue_capacity=settings.observability.queue_capacity,
            batch_size=settings.observability.batch_size,
            flush_interval_ms=settings.observability.flush_interval_ms,
            drop_policy=settings.observability.drop_policy,
            shutdown_flush_timeout_ms=settings.observability.shutdown_flush_timeout_ms,
            enabled=settings.observability.enabled,
        )
        telemetry.start()
        logging_runtime = configure_logging(settings.logging, telemetry, runtime_root)
    except BaseException as runtime_initialization_error:
        try:
            await _close_runtime_resources(
                None,
                logging_runtime,
                telemetry,
                database,
                telemetry_database,
            )
        except BaseException:
            runtime_initialization_error.add_note(
                "one or more runtime resources also failed during initialization cleanup"
            )
        raise
    assert telemetry is not None
    assert logging_runtime is not None
    logger.info(
        "backend.runtime.starting",
        extra={
            "event_name": "backend.runtime.starting",
            "telemetry_attributes": {"operation": "startup", "outcome": "accepted"},
        },
    )
    provider: AgentModelProvider | None = None
    try:
        provider = await _provider_for(settings)
        dependencies = ServiceDependencies(
            settings.network,
            settings.ai,
            settings.knowledge,
            supervisor,
            telemetry,
        )
        locks = ScopedKeyLocks()
        blob_root = settings.knowledge.blob_directory
        if not blob_root.is_absolute():
            blob_root = runtime_root / blob_root
        blob_storage = LocalKnowledgeBlobStorage(blob_root)
        file_parser = LocalKnowledgeFileParser(
            settings.knowledge.max_extracted_characters,
            maximum_input_bytes=settings.knowledge.worker.maximum_material_bytes,
            maximum_parts=settings.knowledge.index.maximum_chunks,
            deployment_environment=settings.environment,
        )
        embedding_provider = _embedding_provider_for(settings)
        async with _password_breach_checker_for(settings) as breached_password_checker, supervisor:
            knowledge = KnowledgeApplicationService(
                storage,
                storage,
                blob_storage,
                file_parser,
                embedding_provider,
                dependencies,
                locks,
            )
            resume = ResumeApplicationService(
                storage,
                storage,
                storage,
                renderer_for(settings.renderer, environment=settings.environment),
                knowledge,
                dependencies,
                locks,
            )
            proposals = ResumeProposalApplicationService(
                storage,
                resume,
                knowledge,
                locks,
                provider,
                settings.ai,
            )
            oauth_service = OAuthAuthorizationService(
                oauth_repository,
                settings.oauth,
                oauth_token_signer,
            )
            identity_email_runtime = _identity_email_runtime_for(
                settings,
                database,
            )
            hosted_identity_service = HostedIdentityService(
                hosted_identity_repository,
                oauth_service,
                identity_email_runtime.sender,
                breached_password_checker=breached_password_checker,
                lifetime_seconds=settings.hosted_identity.flow_ttl_seconds,
                email_code_ttl_seconds=settings.hosted_identity.email_code_ttl_seconds,
                email_code_max_attempts=settings.hosted_identity.email_code_max_attempts,
                email_send_limit_per_hour=(settings.hosted_identity.email_send_limit_per_hour),
                session_idle_ttl_seconds=settings.hosted_identity.session_idle_ttl_seconds,
                session_absolute_ttl_seconds=(
                    settings.hosted_identity.session_absolute_ttl_seconds
                ),
                recent_reauthentication_seconds=(
                    settings.hosted_identity.recent_reauthentication_seconds
                ),
                allow_test_email_codes=settings.environment in {"development", "test"},
            )
            cursor_secret = settings.security.cursor_hmac_secret
            v2_cursor = CursorCodec(
                cursor_secret.encode("utf-8")
                if cursor_secret is not None
                else secrets.token_bytes(32)
            )
            sensitive_idempotency_secret = settings.security.sensitive_idempotency_hmac_secret
            sensitive_idempotency_key = (
                sensitive_idempotency_secret.encode("utf-8")
                if sensitive_idempotency_secret is not None
                else secrets.token_bytes(32)
            )
            maintenance = V2MaintenanceService(
                maintenance_repository,
                batch_sizes=MaintenanceBatchSizes(
                    invitations=settings.runtime.maintenance_invitation_batch_size,
                    idempotency_receipts=(settings.runtime.maintenance_idempotency_batch_size),
                ),
            )
            _log_maintenance_result(await maintenance.run_once())
            agent_runtime = _agent_runtime_for(
                settings,
                database=database,
                memory_access_store=memory_access_store,
                provider=provider,
                embedder=embedding_provider,
            )
            knowledge_runtime = _knowledge_runtime_for(
                settings,
                runtime_root=runtime_root,
                database=database,
                memory_access_store=memory_access_store,
                parser=file_parser,
                embedder=embedding_provider,
            )
            resume_runtime = _resume_runtime_for(
                settings,
                database=database,
                memory_access_store=memory_access_store,
                public_uow_factory=resume_v2_uow_factory,
                upload_reader=knowledge_runtime.upload_reader,
            )
            interview_runtime = _interview_runtime_for(
                settings,
                database=database,
                memory_access_store=memory_access_store,
                provider=provider,
            )
            account_deletion = (
                AccountDeletionExecutionService(
                    PostgresAccountDeletionExecutionPort(
                        database,
                        upload_erasure=knowledge_runtime.upload_erasure,
                        creator_secret_erasure=knowledge_runtime.creator_secret_erasure,
                        recipient_email_erasure=identity_email_runtime.erasure,
                    )
                )
                if database is not None
                else None
            )
            maintenance_stop = asyncio.Event()
            identity_email_stop = asyncio.Event()
            agent_stop = asyncio.Event()
            knowledge_stop = asyncio.Event()
            resume_stop = asyncio.Event()
            interview_stop = asyncio.Event()
            account_deletion_stop = asyncio.Event()
            container = BackendContainer(
                settings=settings,
                identity=identity,
                contracts=ContractValidator.from_json(read_contract_schema_text()),
                contracts_v2=ContractValidator.from_jsonc(read_contract_schema_text("v2")),
                idempotency=idempotency,
                workspace=WorkspaceApplicationService(storage),
                resume=resume,
                resumes_v2=resume_runtime.application,
                platform=PlatformApplicationService(
                    platform_uow_factory,
                    platform_content_store,
                    platform_event_feed,
                ),
                proposals=proposals,
                agent=LegacyAgentApplicationService(
                    storage,
                    provider,
                    knowledge,
                    dependencies,
                    locks,
                ),
                agent_v2=agent_runtime.application,
                interview=InterviewApplicationService(storage, storage, dependencies, locks),
                interview_v2=interview_runtime.application,
                knowledge=knowledge,
                knowledge_v2=knowledge_runtime.application,
                knowledge_local_upload_store=knowledge_runtime.local_upload_store,
                model_provider=provider,
                supervisor=supervisor,
                telemetry=telemetry,
                telemetry_writer=telemetry_writer,
                logging_runtime=logging_runtime,
                diagnostics=DiagnosticIngestionService(
                    settings.observability.diagnostics,
                    telemetry,
                    DiagnosticRateLimiter(settings.observability.diagnostics),
                ),
                oauth=oauth_service,
                hosted_identity=hosted_identity_service,
                access=AccessApplicationService(
                    access_uow_factory,
                    hosted_identity_service,
                ),
                maintenance=maintenance,
                v2_cursor=v2_cursor,
                v2_idempotency=v2_idempotency,
                sensitive_idempotency_key=sensitive_idempotency_key,
                database=database,
            )
            supervisor.submit(
                "maintenance",
                lambda: _run_maintenance_loop(
                    maintenance,
                    interval_seconds=settings.runtime.maintenance_interval_seconds,
                    stop=maintenance_stop,
                ),
                name="aiws:maintenance:v2",
            )
            if account_deletion is not None:
                supervisor.submit(
                    "account_deletion",
                    lambda: _run_account_deletion_loop(
                        account_deletion,
                        interval_seconds=settings.runtime.maintenance_interval_seconds,
                        stop=account_deletion_stop,
                    ),
                    name="aiws:account-deletion:v2",
                )
            supervisor.submit(
                "agent",
                lambda: _run_outbox_dispatch_loop(
                    agent_runtime.dispatcher,
                    domain="agent",
                    poll_interval_seconds=1.0,
                    stop=agent_stop,
                ),
                name="aiws:agent:v2-outbox",
            )
            knowledge_dispatcher = knowledge_runtime.dispatcher
            if knowledge_dispatcher is not None:
                supervisor.submit(
                    "knowledge",
                    lambda: _run_outbox_dispatch_loop(
                        knowledge_dispatcher,
                        domain="knowledge",
                        poll_interval_seconds=1.0,
                        stop=knowledge_stop,
                    ),
                    name="aiws:knowledge:v2-outbox",
                )
            resume_dispatcher = resume_runtime.dispatcher
            if resume_dispatcher is not None:
                supervisor.submit(
                    "render",
                    lambda: _run_outbox_dispatch_loop(
                        resume_dispatcher,
                        domain="resume",
                        poll_interval_seconds=1.0,
                        stop=resume_stop,
                    ),
                    name="aiws:resume:v2-outbox",
                )
            interview_dispatcher = interview_runtime.dispatcher
            if interview_dispatcher is not None:
                supervisor.submit(
                    "interview",
                    lambda: _run_outbox_dispatch_loop(
                        interview_dispatcher,
                        domain="interview",
                        poll_interval_seconds=1.0,
                        stop=interview_stop,
                    ),
                    name="aiws:interview:v2-outbox",
                )
            identity_email_worker = identity_email_runtime.worker
            if identity_email_worker is not None:
                supervisor.submit(
                    "identity_email",
                    lambda: _run_identity_email_loop(
                        identity_email_worker,
                        poll_interval_seconds=(
                            settings.hosted_identity.email.outbox.poll_interval_ms / 1_000
                        ),
                        stop=identity_email_stop,
                    ),
                    name="aiws:identity-email:outbox",
                )
            logger.info(
                "backend.runtime.started",
                extra={
                    "event_name": "backend.runtime.started",
                    "telemetry_attributes": {"operation": "startup", "outcome": "success"},
                },
            )
            try:
                yield container
            finally:
                maintenance_stop.set()
                identity_email_stop.set()
                agent_stop.set()
                knowledge_stop.set()
                resume_stop.set()
                interview_stop.set()
                account_deletion_stop.set()
                logger.info(
                    "backend.runtime.stopping",
                    extra={
                        "event_name": "backend.runtime.stopping",
                        "telemetry_attributes": {"operation": "shutdown", "outcome": "accepted"},
                    },
                )
    except BaseException:
        logger.critical(
            "backend.runtime.failed",
            extra={
                "event_name": "backend.runtime.failed",
                "telemetry_attributes": {"operation": "runtime", "outcome": "failure"},
            },
        )
        raise
    finally:
        active_error = sys.exception()
        shutdown_failures: list[BaseException] = []
        try:
            logger.info(
                "backend.runtime.stopped",
                extra={
                    "event_name": "backend.runtime.stopped",
                    "telemetry_attributes": {
                        "operation": "shutdown",
                        "outcome": "success",
                    },
                },
            )
        except BaseException as error:
            shutdown_failures.append(error)
        try:
            await _close_runtime_resources(
                provider,
                logging_runtime,
                telemetry,
                database,
                telemetry_database,
            )
        except BaseException as error:
            shutdown_failures.append(error)
        if shutdown_failures:
            if active_error is None:
                first = shutdown_failures[0]
                if len(shutdown_failures) > 1:
                    first.add_note(
                        f"{len(shutdown_failures) - 1} additional shutdown failure(s) suppressed"
                    )
                raise first
            active_error.add_note(
                f"{len(shutdown_failures)} backend shutdown operation(s) also failed"
            )


def _agent_runtime_for(
    settings: BackendSettings,
    *,
    database: AsyncDatabase | None,
    memory_access_store: InMemoryAccessStore | None,
    provider: AgentModelProvider,
    embedder: EmbeddingProvider,
) -> _AgentRuntimeComponents:
    """@brief 构造 memory/PostgreSQL Agent V2 应用与 worker 图 / Build the memory/PostgreSQL Agent V2 application and worker graph.

    @param settings 已验证服务端设置 / Validated server settings.
    @param database PostgreSQL runtime；memory 模式为空 / PostgreSQL runtime, absent in memory mode.
    @param memory_access_store memory 模式中央 Access 真相 / Central Access truth in memory mode.
    @param provider lifespan-owned 流式模型 provider / Lifespan-owned streaming model provider.
    @param embedder 与 Knowledge index 同空间的查询 embedding provider / Query embedding
        provider sharing the Knowledge index space.
    @return 完整公开服务、worker 与 dispatcher / Complete public service, worker, and dispatcher.
    @raise RuntimeError runtime mode 与依赖不一致时 fail closed / Fail closed when runtime mode
        and dependencies disagree.
    """
    model_routes = _agent_model_routes(settings)
    model_adapter = StreamingTextAgentProvider(
        provider,
        input_cost_microusd_per_million_tokens=(
            settings.ai.metering.input_cost_microusd_per_million_tokens
        ),
        output_cost_microusd_per_million_tokens=(
            settings.ai.metering.output_cost_microusd_per_million_tokens
        ),
    )
    tool_executor = UnavailableAgentToolExecutor()
    tool_registry = EmptyAgentToolRegistry()
    if database is not None:
        if memory_access_store is not None:
            raise RuntimeError("PostgreSQL Agent runtime cannot use an in-memory Access store")
        postgres_public_uow = PostgresAgentUnitOfWorkFactory(
            database,
            model_routes=model_routes,
        )
        postgres_worker_uow = PostgresAgentWorkerUnitOfWorkFactory(
            database,
            model_routes=model_routes,
        )
        embedding_space = EmbeddingSpaceSelection(
            settings.ai.embedding_provider,
            settings.ai.embedding_model,
            settings.ai.embedding_model_revision,
            settings.ai.embedding_dimension,
            settings.ai.embedding_distance_metric,
            settings.ai.embedding_normalization,
        )
        retriever = GrantedAgentKnowledgeRetriever(
            PostgresHybridKnowledgeSearch(
                database,
                embedder,
                embedding_space,
                lexical_weight=settings.knowledge.search.lexical_weight,
                semantic_weight=settings.knowledge.search.semantic_weight,
                candidate_multiplier=settings.knowledge.search.candidate_multiplier,
            )
        )
        application = V2AgentApplicationService(
            cast(AgentUnitOfWorkFactory, postgres_public_uow)
        )
        worker = AgentWorkerService(
            cast(AgentWorkerUnitOfWorkFactory, postgres_worker_uow),
            model_adapter,
            tool_executor,
            knowledge_retriever=retriever,
            tool_registry=tool_registry,
        )
        handler = AgentRunOutboxHandler(worker)
        dispatcher: _OutboxDispatchService = OutboxDispatchService(
            PostgresOutboxClaimRepository(
                database,
                event_types=AGENT_WORK_EVENT_TYPES,
            ),
            {event_type: handler for event_type in AGENT_WORK_EVENT_TYPES},
            required_event_types=AGENT_WORK_EVENT_TYPES,
            settings=OutboxDispatchSettings(),
        )
        return _AgentRuntimeComponents(application, worker, dispatcher)

    if memory_access_store is None:
        raise RuntimeError("memory Agent runtime requires the shared Access store")
    store = InMemoryAgentStore()
    policy_store = InMemoryAgentPolicyStore()
    memory_public_uow = InMemoryAgentUnitOfWorkFactory(
        store,
        memory_access_store,
        policy_store=policy_store,
        model_routes=model_routes,
    )
    memory_worker_uow = InMemoryAgentWorkerUnitOfWorkFactory(
        store,
        memory_access_store,
        policy_store=policy_store,
        model_routes=model_routes,
    )
    application = V2AgentApplicationService(
        cast(AgentUnitOfWorkFactory, memory_public_uow)
    )
    worker = AgentWorkerService(
        cast(AgentWorkerUnitOfWorkFactory, memory_worker_uow),
        model_adapter,
        tool_executor,
        knowledge_retriever=GrantedAgentKnowledgeRetriever(
            MemoryHybridKnowledgeSearch(())
        ),
        tool_registry=tool_registry,
    )
    dispatcher = InMemoryAgentDispatchService(store, worker)
    return _AgentRuntimeComponents(application, worker, dispatcher)


def _resume_runtime_for(
    settings: BackendSettings,
    *,
    database: AsyncDatabase | None,
    memory_access_store: InMemoryAccessStore | None,
    public_uow_factory: ResumeUnitOfWorkFactory,
    upload_reader: ResumeUploadObjectReader,
) -> _ResumeRuntimeComponents:
    """@brief 组装 Resume V2 公开服务与持久 Job worker / Assemble the Resume V2 public service and durable Job worker.

    @param settings 已验证 renderer 与运行模式配置 / Validated renderer and runtime settings.
    @param database PostgreSQL runtime；memory 模式为空 / PostgreSQL runtime, absent in memory mode.
    @param memory_access_store memory 模式共享 Access 真相 / Shared Access truth in memory mode.
    @param public_uow_factory 公开命令使用的 UoW / Unit-of-work factory used by public commands.
    @param upload_reader 与 Knowledge upload 共享的隔离对象 reader / Isolated object reader shared with Knowledge uploads.
    @return 公开应用与可选 durable dispatcher / Public application and optional durable dispatcher.
    @raise RuntimeError runtime 依赖组合不一致时 fail closed / Fail closed when runtime dependencies are inconsistent.
    """

    application = V2ResumeApplicationService(public_uow_factory)
    if database is None:
        if memory_access_store is None:
            raise RuntimeError("memory Resume runtime requires the shared Access store")
        return _ResumeRuntimeComponents(application, None)
    if memory_access_store is not None:
        raise RuntimeError("PostgreSQL Resume runtime cannot use an in-memory Access store")

    dispatch_settings = OutboxDispatchSettings()
    worker = ResumeJobWorkerService(
        PostgresResumeWorkerUnitOfWorkFactory(database, api_origin=PUBLIC_ORIGIN),
        SafeResumeImporter(
            upload_reader,
            deployment_environment=settings.environment,
        ),
        MultiFormatResumeRenderer(
            renderer_for(settings.renderer, environment=settings.environment)
        ),
    )
    handler = ResumeJobOutboxHandler(
        worker,
        maximum_attempts=dispatch_settings.maximum_attempts,
    )
    dispatcher = OutboxDispatchService(
        PostgresOutboxClaimRepository(
            database,
            event_types=RESUME_WORK_EVENT_TYPES,
        ),
        {event_type: handler for event_type in RESUME_WORK_EVENT_TYPES},
        required_event_types=RESUME_WORK_EVENT_TYPES,
        settings=dispatch_settings,
    )
    return _ResumeRuntimeComponents(application, dispatcher)


def _interview_runtime_for(
    settings: BackendSettings,
    *,
    database: AsyncDatabase | None,
    memory_access_store: InMemoryAccessStore | None,
    provider: AgentModelProvider,
) -> _InterviewRuntimeComponents:
    """@brief 组装 Interview V2 应用、短期凭据与 Job worker / Assemble Interview V2 application, short-lived credentials, and Job worker.

    @param settings 已验证服务端设置 / Validated server settings.
    @param database PostgreSQL runtime；memory 模式为空 / PostgreSQL runtime, absent in memory mode.
    @param memory_access_store memory 模式中央 Access 真相 / Central Access truth in memory mode.
    @param provider lifespan-owned 通用模型 provider / Lifespan-owned generic model provider.
    @return 公开服务、realtime gateway 与可选 durable dispatcher / Public service, realtime
        gateway, and optional durable dispatcher.
    @raise RuntimeError runtime mode 与依赖不一致时 fail closed / Fail closed when runtime mode
        and dependencies disagree.
    """
    primary_model_route = _agent_model_routes(settings)[0]
    realtime_gateway = _interview_realtime_gateway(settings)
    if database is None:
        if memory_access_store is None:
            raise RuntimeError("memory Interview runtime requires the shared Access store")
        policy = StaticInterviewSessionPolicy(
            model_ref=primary_model_route.model_ref,
            allowed_regions=frozenset({primary_model_route.data_region}),
            allow_external_model_processing=primary_model_route.external_processing,
        )
        memory_uow = cast(
            InterviewUnitOfWorkFactory,
            InMemoryInterviewUnitOfWorkFactory(memory_access_store, policy),
        )
        return _InterviewRuntimeComponents(
            V2InterviewApplicationService(memory_uow, realtime_gateway),
            None,
            realtime_gateway,
        )

    if memory_access_store is not None:
        raise RuntimeError("PostgreSQL Interview runtime cannot use an in-memory Access store")
    service_actor = ResourceRef("service", "service_interview_worker_v2")
    postgres_uow = cast(
        InterviewUnitOfWorkFactory,
        PostgresInterviewUnitOfWorkFactory(
            database,
            model_ref=primary_model_route.model_ref,
            model_regions=frozenset({primary_model_route.data_region}),
            allow_external_model_processing=primary_model_route.external_processing,
            service_actor=service_actor,
        ),
    )
    application = V2InterviewApplicationService(postgres_uow, realtime_gateway)
    dispatch_settings = OutboxDispatchSettings()
    worker = InterviewWorkerService(
        postgres_uow,
        ConsentAwareInterviewMediaFinalizer(),
        _interview_report_provider(settings, provider, primary_model_route),
        service_actor=service_actor,
    )
    handler = InterviewJobOutboxHandler(
        worker,
        maximum_attempts=dispatch_settings.maximum_attempts,
    )
    dispatcher = OutboxDispatchService(
        PostgresOutboxClaimRepository(
            database,
            event_types=INTERVIEW_WORK_EVENT_TYPES,
        ),
        {"interview.job.queued": handler},
        required_event_types=INTERVIEW_WORK_EVENT_TYPES,
        settings=dispatch_settings,
    )
    return _InterviewRuntimeComponents(application, dispatcher, realtime_gateway)


def _interview_report_provider(
    settings: BackendSettings,
    provider: AgentModelProvider,
    route: AgentModelRoute,
) -> InterviewReportProvider:
    """@brief 将冻结 Session model route 收窄为 Report provider / Narrow the frozen Session model route into a Report provider.

    @param settings 已验证私有 provider 配置 / Validated private provider configuration.
    @param provider lifespan-owned 模型端口 / Lifespan-owned model port.
    @param route 与 Session policy 相同的 primary route / Primary route shared with Session policy.
    @return production 严格 JSON adapter、开发 mock 或 fail-closed adapter / Production strict
        JSON adapter, development mock, or fail-closed adapter.
    @note Report request 尚未携带用户冻结的 fallback 授权，因此此边界固定禁止 provider
        fallback；不能把全局配置当成用户同意。/ Report requests do not yet carry frozen user
        fallback consent, so this boundary disables provider fallback rather than treating global
        configuration as user authorization.
    """
    if settings.ai.provider == "mock":
        if settings.environment in {"development", "test"}:
            return DeterministicInterviewReportProvider(
                environment=settings.environment,
            )
        return FailClosedInterviewReportProvider()
    return StreamingJsonInterviewReportProvider(
        provider,
        engine_version=f"model-route:{route.model_ref.id}",
        model_data_region=route.data_region.value,
        allow_external_model_processing=route.external_processing,
        allow_provider_fallback=False,
    )


def _interview_realtime_gateway(settings: BackendSettings) -> InterviewRealtimeGateway:
    """@brief 从独立 keyring 构造 realtime gateway / Build a realtime gateway from its independent keyring.

    @param settings 已完成环境约束与密钥域隔离的配置 / Configuration already validated for
        environment constraints and key-domain separation.
    @return HMAC gateway，或 development/test 的显式 unavailable adapter / HMAC gateway or an
        explicit unavailable adapter in development/test.
    """
    realtime = settings.interview.realtime
    active_key_id = realtime.signing_keyring.active_key_id
    if active_key_id is None or realtime.signaling_url is None:
        return _UnavailableInterviewRealtimeGateway()
    keyring = InterviewRealtimeSigningKeyring(
        active_key_id,
        tuple(
            InterviewRealtimeSigningKey(item.key_id, item.key)
            for item in realtime.signing_keyring.keys
        ),
    )
    return HmacInterviewRealtimeGateway(
        keyring,
        signaling_url=realtime.signaling_url,
        allowed_transports=tuple(RealtimeTransport(item) for item in realtime.allowed_transports),
        lifetime=timedelta(seconds=realtime.credential_ttl_seconds),
        heartbeat_interval_ms=realtime.heartbeat_interval_ms,
        ice_urls=realtime.ice_urls,
    )


def _knowledge_runtime_for(
    settings: BackendSettings,
    *,
    runtime_root: Path,
    database: AsyncDatabase | None,
    memory_access_store: InMemoryAccessStore | None,
    parser: LocalKnowledgeFileParser,
    embedder: EmbeddingProvider,
) -> _KnowledgeRuntimeComponents:
    """@brief 组装 Knowledge V2 公开端口、安全外部 adapter 与 worker / Assemble Knowledge V2 public, secure external, and worker ports.

    @param settings 已验证服务端配置 / Validated server settings.
    @param runtime_root 本地 development 目录的基准 / Base for local development paths.
    @param database PostgreSQL runtime；memory 模式为空 / PostgreSQL runtime, absent in memory mode.
    @param memory_access_store memory 模式的中央 Access 真相 / Central Access truth in memory mode.
    @param parser 受限文件 parser / Bounded file parser.
    @param embedder 查询与 chunk 共用的 embedding provider / Embedding provider shared by query and chunks.
    @return 公开服务、工作调度与擦除端口 / Public service, work dispatcher, and erasure ports.
    @raise RuntimeError 运行模式与持久密钥不一致时抛出 / Raised for runtime and durable-key mismatches.
    """

    upload_store, local_upload_store = _knowledge_upload_store_for(
        settings,
        runtime_root=runtime_root,
        database=database,
    )
    upload_erasure = BoundedUploadErasure(
        upload_store,
        maximum_batch_size=settings.knowledge.uploads.erasure_batch_size,
    )
    network = settings.knowledge.source_network
    network_guard = StrictSourceNetworkGuard(
        SourceNetworkPolicy(
            frozenset(network.allowed_schemes),
            frozenset(network.allowed_ports),
            network.allowed_host_patterns,
            network.maximum_redirects,
            network.allow_https_downgrade,
        )
    )
    embedding_space = EmbeddingSpaceSelection(
        settings.ai.embedding_provider,
        settings.ai.embedding_model,
        settings.ai.embedding_model_revision,
        settings.ai.embedding_dimension,
        settings.ai.embedding_distance_metric,
        settings.ai.embedding_normalization,
    )
    if database is None:
        if memory_access_store is None:
            raise RuntimeError("memory Knowledge runtime requires the shared Access store")
        if settings.knowledge.connections.providers:
            raise RuntimeError("memory Knowledge runtime cannot configure external providers")
        unavailable = UnavailableConnectionAdapter()
        application = V2KnowledgeApplicationService(
            cast(
                KnowledgeUnitOfWorkFactory,
                InMemoryKnowledgeUnitOfWorkFactory(memory_access_store),
            ),
            unavailable,
            unavailable,
            upload_store,
            network_guard,
            MemoryKnowledgeDependencyVerifier(()),
            MemoryHybridKnowledgeSearch(()),
        )
        return _KnowledgeRuntimeComponents(
            application,
            None,
            local_upload_store,
            upload_erasure,
            None,
            upload_store,
        )

    if memory_access_store is not None:
        raise RuntimeError("PostgreSQL Knowledge runtime cannot use an in-memory Access store")
    registry = ConnectionProviderRegistry(
        tuple(
            _connection_provider_definition(item)
            for item in settings.knowledge.connections.providers
        )
    )
    launch_cipher = _knowledge_launch_cipher(settings)
    vault = _knowledge_credential_vault(settings, database)
    authorization_gateway: ConnectionAuthorizationGateway
    credential_broker: ConnectionCredentialBroker
    credential_revoker: KnowledgeCredentialRevoker
    if vault is None:
        if settings.knowledge.connections.providers:
            raise RuntimeError("configured Connection providers require a durable credential vault")
        authorization_gateway = UnavailableConnectionAdapter()
        credential_broker = UnavailableConnectionAdapter()
        credential_revoker = _UnavailableKnowledgeCredentialRevoker()
    else:
        provider_adapter = ProviderConnectionAdapter(
            registry,
            vault,
            connect_timeout_ms=settings.knowledge.connections.connect_timeout_ms,
            read_timeout_ms=settings.knowledge.connections.read_timeout_ms,
        )
        authorization_gateway = provider_adapter
        credential_broker = provider_adapter
        credential_revoker = ProviderCredentialRevoker(
            registry,
            vault,
            connect_timeout_ms=settings.knowledge.connections.connect_timeout_ms,
            read_timeout_ms=settings.knowledge.connections.read_timeout_ms,
        )
    application = V2KnowledgeApplicationService(
        cast(
            KnowledgeUnitOfWorkFactory,
            PostgresKnowledgeUnitOfWorkFactory(database, launch_cipher=launch_cipher),
        ),
        authorization_gateway,
        credential_broker,
        upload_store,
        network_guard,
        PostgresKnowledgeDependencyVerifier(database),
        PostgresHybridKnowledgeSearch(
            database,
            embedder,
            embedding_space,
            lexical_weight=settings.knowledge.search.lexical_weight,
            semantic_weight=settings.knowledge.search.semantic_weight,
            candidate_multiplier=settings.knowledge.search.candidate_multiplier,
        ),
    )
    fetcher = PinnedHttpSourceFetcher(
        network_guard,
        maximum_body_bytes=network.maximum_body_bytes,
        connect_timeout_ms=network.connect_timeout_ms,
        read_timeout_ms=network.read_timeout_ms,
    )
    worker = KnowledgeWorkerService(
        PostgresKnowledgeWorkerStore(database),
        credential_revoker,
        CompositeKnowledgeSourceEraser(upload_store),
        PostgresKnowledgeMaterialLoader(
            database,
            upload_store,
            fetcher,
            maximum_material_bytes=settings.knowledge.worker.maximum_material_bytes,
        ),
        KnowledgeIndexPipeline(
            parser,
            embedder,
            embedding_space,
            model_region=settings.ai.data_region,
            external_model_processing=(settings.ai.data_region != "private_deployment"),
            maximum_extracted_characters=(settings.knowledge.index.maximum_extracted_characters),
            maximum_chunks=settings.knowledge.index.maximum_chunks,
            chunk_max_characters=settings.knowledge.index.chunk_max_characters,
            chunk_overlap_characters=(settings.knowledge.index.chunk_overlap_characters),
            embedding_batch_size=settings.knowledge.index.embedding_batch_size,
        ),
        maximum_attempts=settings.knowledge.worker.maximum_attempts,
    )
    dispatcher = OutboxDispatchService(
        PostgresOutboxClaimRepository(
            database,
            event_types=KNOWLEDGE_WORK_EVENT_TYPES,
        ),
        {event_type: worker for event_type in KNOWLEDGE_WORK_EVENT_TYPES},
        required_event_types=KNOWLEDGE_WORK_EVENT_TYPES,
        settings=OutboxDispatchSettings(
            maximum_attempts=settings.knowledge.worker.maximum_attempts,
        ),
    )
    return _KnowledgeRuntimeComponents(
        application,
        dispatcher,
        local_upload_store,
        upload_erasure,
        vault,
        upload_store,
    )


def _knowledge_upload_store_for(
    settings: BackendSettings,
    *,
    runtime_root: Path,
    database: AsyncDatabase | None,
) -> tuple[LocalSignedUploadStore | S3UploadObjectStore, LocalSignedUploadStore | None]:
    """@brief 构造配额、扫描与对象存储一体边界 / Build the quota, scanner, and object-store boundary.

    @param settings 已验证配置 / Validated settings.
    @param runtime_root 本地目录基准 / Base for local paths.
    @param database 可选 PostgreSQL quota ledger / Optional PostgreSQL quota ledger.
    @return 对象 store 与可选 local PUT store / Object store and optional local PUT store.
    """

    upload = settings.knowledge.uploads
    limits = UploadSafetyLimits(
        upload.maximum_object_bytes,
        upload.maximum_archive_entries,
        upload.maximum_archive_depth,
        upload.maximum_expanded_bytes,
        upload.maximum_inflation_ratio,
        upload.maximum_scanner_chunk_bytes,
    )
    malware = upload.malware
    scanner: MalwareScanner
    if malware.mode == "dev":
        scanner = DevelopmentAllowAllMalwareScanner()
    elif malware.mode == "reject":
        scanner = RejectingMalwareScanner()
    else:
        if malware.clamav_host is None:
            raise RuntimeError("ClamAV mode requires an explicit host")
        scanner = ClamAvInstreamScanner(
            malware.clamav_host,
            malware.clamav_port,
            connect_timeout_ms=malware.connect_timeout_ms,
            read_timeout_ms=malware.read_timeout_ms,
            chunk_bytes=upload.maximum_scanner_chunk_bytes,
        )
    quota = (
        PostgresUploadQuotaLedger(database, upload.maximum_workspace_bytes)
        if database is not None
        else MemoryUploadQuotaLedger(upload.maximum_workspace_bytes)
    )
    storage = upload.storage
    if isinstance(storage, KnowledgeLocalUploadStorageSettings):
        signing_key = storage.signing_hmac_key or secrets.token_bytes(32)
        root = storage.directory
        if not root.is_absolute():
            root = runtime_root / root
        local = LocalSignedUploadStore(
            root,
            signing_key,
            scanner,
            quota,
            limits,
            public_origin=storage.public_origin,
        )
        return local, local
    if not isinstance(storage, KnowledgeS3UploadStorageSettings):
        raise TypeError("Knowledge upload storage has an unsupported variant")
    s3 = S3UploadObjectStore(
        S3UploadSettings(
            storage.endpoint,
            storage.region,
            storage.bucket,
            storage.access_key_id,
            storage.secret_access_key,
            storage.session_token,
            storage.object_prefix,
        ),
        scanner,
        quota,
        limits,
        connect_timeout_ms=storage.connect_timeout_ms,
        read_timeout_ms=storage.read_timeout_ms,
    )
    return s3, None


def _connection_provider_definition(
    item: object,
) -> ConnectionProviderDefinition:
    """@brief 将已验证配置投影为不可变 provider 定义 / Project validated config into an immutable provider definition.

    @param item ``KnowledgeConnectionProviderSettings`` 实例 / Provider-settings instance.
    @return infrastructure registry 定义 / Infrastructure registry definition.
    """

    from backend.config import KnowledgeConnectionProviderSettings

    if not isinstance(item, KnowledgeConnectionProviderSettings):
        raise TypeError("Connection provider configuration has an invalid type")
    validation = item.api_token_validation
    return ConnectionProviderDefinition(
        ConnectionProvider(item.provider),
        item.client_id,
        item.authorization_endpoint,
        item.token_endpoint,
        item.device_authorization_endpoint,
        item.redirect_uri,
        frozenset(item.allowed_scopes),
        (
            ApiTokenValidation(
                validation.endpoint,
                validation.method,
                validation.authorization_scheme,
                validation.scopes_field,
            )
            if validation is not None
            else None
        ),
        item.revocation_endpoint,
    )


def _knowledge_launch_cipher(settings: BackendSettings) -> AesGcmAuthorizationLaunchCipher:
    """@brief 构造 provider-session 独立 keyring / Build the independent provider-session keyring.

    @param settings 已验证配置 / Validated settings.
    @return authorization-launch AEAD / Authorization-launch AEAD.
    @note 仅 development/test 且 allowlist 为空时允许进程密钥 / A process-local key
        is allowed only in development/test with an empty provider allowlist.
    """

    keyring = settings.knowledge.connections.provider_session_keyring
    if keyring.active_key_id is None:
        if (
            settings.environment not in {"development", "test"}
            or settings.knowledge.connections.providers
        ):
            raise RuntimeError("Knowledge authorization launch keyring is not configured")
        return AesGcmAuthorizationLaunchCipher(
            {"dev-ephemeral": secrets.token_bytes(32)},
            active_key_id="dev-ephemeral",
        )
    return AesGcmAuthorizationLaunchCipher(
        {item.key_id: item.key for item in keyring.keys},
        active_key_id=keyring.active_key_id,
    )


def _knowledge_credential_vault(
    settings: BackendSettings,
    database: AsyncDatabase,
) -> PostgresConnectionCredentialVault | None:
    """@brief 在密钥域完整时构造 durable credential vault / Build the durable credential vault when all key domains exist.

    @param settings 已验证配置 / Validated settings.
    @param database PostgreSQL runtime / PostgreSQL runtime.
    @return durable vault，或开发空 allowlist 的 None / Durable vault, or ``None`` for an empty dev allowlist.
    """

    connection = settings.knowledge.connections
    provider_session_keys = connection.provider_session_keyring
    credential_keys = connection.credential_keyring
    fingerprint_key = connection.credential_fingerprint_hmac_key
    reference_key = connection.credential_reference_hmac_key
    if (
        provider_session_keys.active_key_id is None
        or credential_keys.active_key_id is None
        or fingerprint_key is None
        or reference_key is None
    ):
        if (
            settings.environment not in {"development", "test"}
            or connection.providers
            or provider_session_keys.keys
            or credential_keys.keys
        ):
            raise RuntimeError("Knowledge credential-vault key domains are incomplete")
        return None
    return PostgresConnectionCredentialVault(
        database,
        ConnectionSecretKeyring(
            provider_session_keys.active_key_id,
            tuple(ConnectionVaultKey(item.key_id, item.key) for item in provider_session_keys.keys),
        ),
        ConnectionSecretKeyring(
            credential_keys.active_key_id,
            tuple(ConnectionVaultKey(item.key_id, item.key) for item in credential_keys.keys),
        ),
        fingerprint_key=fingerprint_key,
        reference_key=reference_key,
        orphan_grace=timedelta(seconds=connection.orphan_grace_seconds),
    )


def _agent_model_routes(settings: BackendSettings) -> tuple[AgentModelRoute, ...]:
    """@brief 从私有 provider 配置冻结区域路由组 / Freeze regional route groups from private provider configuration.

    @param settings 已验证服务端设置 / Validated server settings.
    @return 每个 ``(region, external_processing)`` 一个不可变 route group / One immutable
        route group per ``(region, external_processing)`` pair.

    @note model ref 是不含 endpoint/key 的配置指纹；同区域 fallback 属于同一可审计路由组，
        配置变化会产生新 ID。/ The model ref is a configuration fingerprint containing neither
        endpoint nor key. Same-region fallbacks form one auditable route group, and a configuration
        change produces a new ID.
    """
    descriptors = [
        (
            settings.ai.provider,
            settings.ai.model,
            settings.ai.base_url or "local",
            ModelRegion(settings.ai.data_region),
            settings.ai.provider != "mock",
        )
    ]
    descriptors.extend(
        (
            endpoint.provider,
            endpoint.model,
            endpoint.base_url,
            ModelRegion(endpoint.data_region),
            True,
        )
        for endpoint in settings.ai.fallback_providers
    )
    grouped: dict[tuple[ModelRegion, bool], list[str]] = {}
    for provider_name, model_name, base_url, region, external in descriptors:
        grouped.setdefault((region, external), []).append(
            "\x00".join((provider_name, model_name, base_url))
        )
    routes: list[AgentModelRoute] = []
    for (region, external), members in grouped.items():
        digest = hashlib.sha256("\x1f".join(members).encode("utf-8")).hexdigest()
        routes.append(
            AgentModelRoute(
                ResourceRef("model", f"model_route_{digest[:32]}", 1),
                region,
                external,
            )
        )
    if not routes:
        raise RuntimeError("Agent runtime has no configured model route")
    return tuple(routes)


async def _run_outbox_dispatch_loop(
    service: _OutboxDispatchService,
    *,
    domain: str,
    poll_interval_seconds: float,
    stop: asyncio.Event,
) -> None:
    """@brief 周期执行单领域独占的 outbox 工作 / Periodically execute one domain's exclusively owned outbox work.

    @param service 只 claim 单领域 allowlist 的 dispatcher / Dispatcher claiming one domain allowlist.
    @param domain 日志使用的封闭领域名 / Closed domain name used for logging.
    @param poll_interval_seconds 空闲或失败后的轮询间隔 / Poll interval after an idle or failed pass.
    @param stop composition-owned 关闭事件 / Composition-owned shutdown event.
    @return stop 后返回；活动 provider 在 supervisor grace 后取消并由租约恢复 / Returns after
        stop; an active provider is cancelled after supervisor grace and recovered by its lease.
    """
    if poll_interval_seconds <= 0:
        raise ValueError("outbox dispatch poll interval must be positive")
    if domain not in {"agent", "interview", "knowledge", "resume"}:
        raise ValueError("outbox dispatch domain is unsupported")
    while not stop.is_set():
        claimed = 0
        try:
            result = await service.run_once()
            claimed = result.claimed
            if claimed:
                _log_outbox_dispatch_result(result, domain=domain)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                f"backend.{domain}.v2.dispatch.failed",
                extra={
                    "event_name": f"backend.{domain}.v2.dispatch.failed",
                    "telemetry_attributes": {
                        "operation": f"{domain}_v2_outbox",
                        "outcome": "failure",
                    },
                },
            )
        if claimed:
            await asyncio.sleep(0)
            continue
        try:
            await asyncio.wait_for(stop.wait(), timeout=poll_interval_seconds)
        except TimeoutError:
            pass


def _log_outbox_dispatch_result(
    result: _OutboxDispatchResult,
    *,
    domain: str,
) -> None:
    """@brief 记录不含业务内容/ID 的 worker 聚合计数 / Log worker counters without business content or identifiers.

    @param result 一轮 memory 或 PostgreSQL 结果 / One memory or PostgreSQL pass result.
    @param domain 封闭领域名 / Closed domain name.
    """
    if isinstance(result, OutboxDispatchResult):
        retried = result.retried
        failed = result.failed
        lost_leases = result.lost_leases
    elif isinstance(result, InMemoryAgentDispatchResult):
        retried = failed = lost_leases = 0
    else:
        raise TypeError("Agent dispatcher returned an unsupported result")
    attention = bool(retried or failed or lost_leases)
    logger.log(
        logging.WARNING if attention else logging.INFO,
        f"backend.{domain}.v2.dispatch.completed",
        extra={
            "event_name": f"backend.{domain}.v2.dispatch.completed",
            "telemetry_attributes": {
                "operation": f"{domain}_v2_outbox",
                "outcome": "attention_required" if attention else "success",
                "claimed": result.claimed,
                "completed": result.completed,
                "retried": retried,
                "failed": failed,
                "lost_leases": lost_leases,
            },
        },
    )


@asynccontextmanager
async def _password_breach_checker_for(
    settings: BackendSettings,
) -> AsyncIterator[BreachedPasswordChecker | None]:
    """@brief 创建 lifespan-owned 泄露密码检查器 / Create a lifespan-owned breached-password checker.

    @param settings 已验证的后端配置 / Validated backend settings.
    @return development/test 禁用时为 None，否则为固定 HTTPS k-anonymity adapter /
        ``None`` when disabled in development/test, otherwise the fixed-HTTPS k-anonymity adapter.
    @note 禁止环境代理自动注入；只有已验证的显式 outbound proxy 可以承载请求。
        / Environment proxies are disabled; only the validated explicit outbound proxy may carry
        the request.
    """

    breach = settings.hosted_identity.password_breach
    if breach.mode == "disabled":
        yield None
        return
    async with httpx.AsyncClient(
        proxy=settings.network.outbound_proxy_url,
        trust_env=False,
        follow_redirects=False,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    ) as client:
        yield PwnedPasswordsChecker(
            client,
            request_timeout=breach.request_timeout_ms / 1_000,
            cache_ttl=timedelta(seconds=breach.cache_ttl_seconds),
            max_cache_entries=breach.max_cache_entries,
        )


def _identity_email_runtime_for(
    settings: BackendSettings,
    database: AsyncDatabase | None,
) -> _IdentityEmailRuntimeComponents:
    """@brief 构造事务队列及可选 SMTP worker / Build the transactional queue and optional SMTP worker.

    @param settings 已验证的应用设置 / Validated application settings.
    @param database PostgreSQL 运行时；内存模式为空 / PostgreSQL runtime, absent in memory mode.
    @return Hosted identity 入队、可选 worker 与擦除端口 / Hosted-identity enqueue,
        optional worker, and erasure ports.
    @raise RuntimeError SMTP 缺少 durable PostgreSQL 或 key material 时 fail closed /
        Fail closed when SMTP lacks durable PostgreSQL or key material.

    @note SMTP transport 永不直接注入 application service；只有 durable worker 可执行网络 I/O。
        / The SMTP transport is never injected directly into the application service; only the
        durable worker may perform network I/O.
    """

    email = settings.hosted_identity.email
    transport = identity_email_transport_for(email, environment=settings.environment)
    if database is None:
        if not isinstance(transport, MemoryIdentityEmailSender):
            raise RuntimeError("SMTP identity email delivery requires PostgreSQL durable outbox")
        return _IdentityEmailRuntimeComponents(transport, None, None)

    outbox_settings = email.outbox
    active_key_id = outbox_settings.active_key_id
    rate_limit_hmac_key = outbox_settings.rate_limit_hmac_key
    if active_key_id is None or rate_limit_hmac_key is None:
        raise RuntimeError("PostgreSQL identity email outbox key material is not configured")
    keyring = IdentityEmailKeyring(
        active_key_id,
        {item.key_id: item.key for item in outbox_settings.encryption_keys},
    )
    outbox = PostgresIdentityEmailOutbox(
        database,
        keyring,
        rate_limit_hmac_key=rate_limit_hmac_key,
        lease_duration=timedelta(seconds=outbox_settings.lease_seconds),
        retention=timedelta(days=outbox_settings.retention_days),
    )
    worker = IdentityEmailOutboxWorker(
        outbox,
        transport,
        worker_id=f"{socket.gethostname()[:96]}:{secrets.token_urlsafe(24)}",
        batch_size=outbox_settings.batch_size,
        max_attempts=outbox_settings.max_attempts,
        retry_base=timedelta(seconds=outbox_settings.retry_base_seconds),
        retry_cap=timedelta(seconds=outbox_settings.retry_cap_seconds),
    )
    return _IdentityEmailRuntimeComponents(outbox, worker, outbox)


async def _run_identity_email_loop(
    worker: IdentityEmailOutboxWorker,
    *,
    poll_interval_seconds: float,
    stop: asyncio.Event,
) -> None:
    """@brief 周期排空加密身份邮件 outbox / Periodically drain the encrypted identity-email outbox.

    @param worker 单轮租约、发送与确认 worker / One-pass lease, delivery, and acknowledgement worker.
    @param poll_interval_seconds 空闲轮询间隔 / Idle polling interval.
    @param stop composition 拥有的关闭事件 / Composition-owned shutdown event.
    @return stop 后返回 / Returns after shutdown is requested.

    @note 单轮失败经过结构化记录后重试；日志只包含聚合计数，不包含地址、验证码或密文。
        / A failed pass is retried after structured logging; logs contain aggregate counts only,
        never addresses, verification codes, or ciphertext.
    """

    if poll_interval_seconds <= 0:
        raise ValueError("identity email poll interval must be positive")
    while not stop.is_set():
        try:
            _log_identity_email_result(await worker.run_once())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "backend.identity_email.outbox.failed",
                extra={
                    "event_name": "backend.identity_email.outbox.failed",
                    "telemetry_attributes": {
                        "operation": "identity_email_outbox",
                        "outcome": "failure",
                    },
                },
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=poll_interval_seconds)
        except TimeoutError:
            pass


def _log_identity_email_result(result: IdentityEmailWorkerResult) -> None:
    """@brief 记录无 PII 的邮件投递计数 / Record email delivery counters without PII.

    @param result 单轮聚合结果 / Aggregate result for one pass.
    @return 无返回值 / No return value.
    """

    logger.log(
        logging.WARNING if result.dead or result.lost_leases else logging.INFO,
        "backend.identity_email.outbox.completed",
        extra={
            "event_name": "backend.identity_email.outbox.completed",
            "telemetry_attributes": {
                "operation": "identity_email_outbox",
                "outcome": (
                    "attention_required" if result.dead or result.lost_leases else "success"
                ),
                "claimed": result.claimed,
                "sent": result.sent,
                "retried": result.retried,
                "dead": result.dead,
                "lost_leases": result.lost_leases,
                "purged_outbox_rows": result.purged_outbox_rows,
                "purged_rate_limit_rows": result.purged_rate_limit_rows,
            },
        },
    )


async def _run_maintenance_loop(
    service: V2MaintenanceService,
    *,
    interval_seconds: int,
    stop: asyncio.Event,
) -> None:
    """@brief 在 lifespan 内周期执行 V2 维护 / Run periodic V2 maintenance within the lifespan.

    @param service 单次、有界维护用例 / One-shot bounded maintenance use case.
    @param interval_seconds 两次运行之间的正数秒数 / Positive delay between runs.
    @param stop composition 拥有的关闭事件 / Composition-owned shutdown event.
    @return stop 后返回 / Returns after shutdown is requested.

    @note 多 worker 可并发运行；数据库函数使用 ``SKIP LOCKED`` 且状态推进幂等。单轮失败
        产生结构化错误日志并在下一周期重试，不终止进程中的其他后台工作。
        / Multiple workers may run concurrently; database functions use ``SKIP LOCKED`` and
        idempotent transitions. A failed run is logged and retried without terminating unrelated work.
    """
    if interval_seconds <= 0:
        raise ValueError("maintenance interval must be positive")
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            pass
        if stop.is_set():
            return
        try:
            _log_maintenance_result(await service.run_once())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "backend.maintenance.v2.failed",
                extra={
                    "event_name": "backend.maintenance.v2.failed",
                    "telemetry_attributes": {
                        "operation": "v2_maintenance",
                        "outcome": "failure",
                    },
                },
            )


async def _run_account_deletion_loop(
    service: AccountDeletionExecutionService,
    *,
    interval_seconds: int,
    stop: asyncio.Event,
) -> None:
    """@brief 周期执行到期账户删除 / Periodically execute due account deletions.

    @param service 有持久租约与擦除证据的执行器 / Executor with durable leases and
        erasure evidence.
    @param interval_seconds 空闲轮询间隔 / Idle polling interval.
    @param stop composition 拥有的关闭事件 / Composition-owned shutdown event.
    @return stop 后返回 / Return after shutdown is requested.

    @note 执行失败不会伪造 completed；未完成 claim 依靠租约过期恢复。
        / Failures never manufacture completion; unfinished claims recover through lease expiry.
    """

    if interval_seconds <= 0:
        raise ValueError("account deletion interval must be positive")
    while not stop.is_set():
        try:
            result = await service.run_once()
            if result.claimed:
                _log_account_deletion_result(result)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "backend.account-deletion.v2.failed",
                extra={
                    "event_name": "backend.account-deletion.v2.failed",
                    "telemetry_attributes": {
                        "operation": "account_deletion_v2",
                        "outcome": "failure",
                    },
                },
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            pass


def _log_account_deletion_result(result: AccountDeletionRunResult) -> None:
    """@brief 记录不含用户标识的删除计数 / Log deletion counters without user identifiers.

    @param result 一轮守恒结果 / One claim-conserving pass result.
    @return 无返回值 / No return value.
    """

    attention = bool(result.failed or result.retryable or result.stale_claims)
    logger.log(
        logging.WARNING if attention else logging.INFO,
        "backend.account-deletion.v2.completed",
        extra={
            "event_name": "backend.account-deletion.v2.completed",
            "telemetry_attributes": {
                "operation": "account_deletion_v2",
                "outcome": "attention_required" if attention else "success",
                "claimed": result.claimed,
                "completed": result.completed,
                "failed": result.failed,
                "retryable": result.retryable,
                "stale_claims": result.stale_claims,
            },
        },
    )


def _log_maintenance_result(result: MaintenanceRunResult) -> None:
    """@brief 记录成功维护与 stranded receipt 信号 / Record successful maintenance and stranded-receipt signals.

    @param result 强类型单轮结果 / Typed result for one run.
    @return 无返回值 / No return value.
    """
    idempotency = result.idempotency
    has_stranded = idempotency.stranded_pending_receipts > 0
    logger.log(
        logging.WARNING if has_stranded else logging.INFO,
        "backend.maintenance.v2.completed",
        extra={
            "event_name": "backend.maintenance.v2.completed",
            "telemetry_attributes": {
                "operation": "v2_maintenance",
                "outcome": "attention_required" if has_stranded else "success",
                "last_success_at": result.finished_at.isoformat(),
                "expired_invitations": result.expired_invitations,
                "purged_completed_receipts": idempotency.purged_completed_receipts,
                "stranded_pending_receipts": idempotency.stranded_pending_receipts,
                "has_more_stranded_pending_receipts": (
                    idempotency.has_more_stranded_pending_receipts
                ),
                "oldest_stranded_expires_at": (
                    idempotency.oldest_stranded_expires_at.isoformat()
                    if idempotency.oldest_stranded_expires_at is not None
                    else None
                ),
            },
        },
    )


async def _provider_for(settings: BackendSettings) -> AgentModelProvider:
    """@brief 从仅服务端设置构造主/备模型 Provider / Build primary/fallback model providers from server-only settings.

    @param settings 已验证的后端配置 / Validated backend settings.
    @return 可流式输出且可发现能力的 provider。
    @raise RuntimeError 非 mock provider 的密钥或端点缺失时抛出。

    @note 公开 API 永不接收 provider/model/key；fallback 只在首个文本前切换，避免将
    两个模型的半段回答拼接成看似连贯但不可审计的输出。每个实际 endpoint/credential
    拥有独立 worker-local 限流器，不能用同名 provider 标签把多个服务商/密钥误合并。
    """
    if settings.ai.provider == "mock":
        if settings.ai.fallback_providers:
            raise RuntimeError("mock model provider cannot be combined with production fallbacks")
        return MockModelProvider()

    endpoints = (
        AIProviderEndpoint(
            provider=settings.ai.provider,
            model=settings.ai.model,
            api_key=settings.ai.api_key or "",
            base_url=_required_provider_base_url(settings.ai.base_url),
            data_region=settings.ai.data_region,
        ),
        *settings.ai.fallback_providers,
    )
    credentials: list[tuple[AIProviderEndpoint, str]] = []
    for endpoint in endpoints:
        api_key = endpoint.api_key
        if not api_key:
            raise RuntimeError("model provider API key is not configured in config.jsonc")
        credentials.append((endpoint, api_key))

    created: list[AgentModelProvider] = []
    rate_limiters: dict[tuple[str, str, str], ProviderRateLimiter] = {}
    try:
        for endpoint, api_key in credentials:
            endpoint_key = (endpoint.provider, endpoint.base_url, endpoint.api_key)
            rate_limiter = rate_limiters.get(endpoint_key)
            if rate_limiter is None:
                rate_limiter = ProviderRateLimiter(
                    max_concurrent_requests=settings.ai.provider_rate_limit.max_concurrent_requests,
                    requests_per_minute=settings.ai.provider_rate_limit.requests_per_minute,
                    acquire_timeout_ms=settings.ai.provider_rate_limit.acquire_timeout_ms,
                )
                rate_limiters[endpoint_key] = rate_limiter
            created.append(
                OpenAICompatibleModelProvider(
                    provider=endpoint.provider,
                    model=endpoint.model,
                    base_url=endpoint.base_url,
                    api_key=api_key,
                    data_region=endpoint.data_region,
                    connect_timeout_ms=settings.network.connect_timeout_ms,
                    read_timeout_ms=settings.network.read_timeout_ms,
                    outbound_proxy_url=settings.network.outbound_proxy_url,
                    rate_limiter=rate_limiter,
                )
            )
    except BaseException:
        for candidate in created:
            await _close_provider(candidate)
        raise
    return created[0] if len(created) == 1 else FallbackModelProvider(created)


def _embedding_provider_for(settings: BackendSettings) -> EmbeddingProvider:
    """Build the configured embedding adapter from the private root config."""
    if settings.ai.embedding_provider == "mock":
        return DeterministicEmbeddingProvider(settings.ai.embedding_dimension)
    api_key = settings.ai.api_key
    if not api_key:
        raise RuntimeError("embedding provider API key is not configured in config.jsonc")
    return OpenAICompatibleEmbeddingProvider(
        base_url=_required_provider_base_url(settings.ai.base_url),
        api_key=api_key,
        model=settings.ai.embedding_model,
        dimension=settings.ai.embedding_dimension,
        connect_timeout_ms=settings.network.connect_timeout_ms,
        read_timeout_ms=settings.network.read_timeout_ms,
        outbound_proxy_url=settings.network.outbound_proxy_url,
    )


def _required_provider_base_url(base_url: str | None) -> str:
    """@brief 读取非 mock 主 provider 的 URL / Read the URL for a non-mock primary provider.

    @param base_url 已解析的服务端 URL。
    @return 非空 URL。
    @raise RuntimeError URL 缺失时抛出。
    """
    if not base_url:
        raise RuntimeError("a non-mock model provider requires ai.base_url")
    return base_url


async def _close_provider(provider: AgentModelProvider) -> None:
    """@brief 有条件关闭 provider 自有资源 / Conditionally close provider-owned resources.

    @param provider 已构造的运行时 provider。
    @return 无返回值。
    """
    close = getattr(provider, "aclose", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        await result


async def _close_runtime_resources(
    provider: AgentModelProvider | None,
    logging_runtime: LoggingRuntime | None,
    telemetry: ObservabilityPipeline | None,
    database: AsyncDatabase | None,
    telemetry_database: AsyncDatabase | None,
) -> None:
    """@brief 尽力关闭全部资源并在最后保留首个失败 / Close every resource and preserve the first failure.

    @param provider 可空模型 provider / Optional model provider.
    @param logging_runtime 可空日志资源 / Optional logging resources.
    @param telemetry 可空、但若已创建则必须最终刷新的遥测管线 / Optional telemetry pipeline that must receive a final flush attempt once created.
    @param database 可空业务数据库 / Optional business database.
    @param telemetry_database 可空独立遥测数据库 / Optional isolated telemetry database.
    @return 无返回值 / No return value.
    @raise BaseException 所有关闭动作完成后重新抛出第一个失败 / Re-raises the first failure after all close actions run.

    @note 顺序保持 provider → logging → telemetry → database：provider 关闭仍可产生日志，
    输出 listener 先排空，再由 telemetry 在数据库连接关闭前刷新。任何一步失败都不得跳过后续步骤。
    """

    failures: list[BaseException] = []

    if provider is not None:
        try:
            await _close_provider(provider)
        except BaseException as error:
            failures.append(error)
    if logging_runtime is not None:
        try:
            logging_runtime.close()
        except BaseException as error:
            failures.append(error)
    if telemetry is not None and logging_runtime is not None:
        try:
            telemetry.record_health_snapshot(
                output_dropped_count=logging_runtime.dropped_output_count,
                force=True,
            )
        except BaseException as error:
            failures.append(error)
    if telemetry is not None:
        try:
            await telemetry.close()
        except BaseException as error:
            failures.append(error)
    for resource in (database, telemetry_database):
        if resource is None:
            continue
        try:
            await resource.aclose()
        except BaseException as error:
            failures.append(error)
    if failures:
        first = failures[0]
        if len(failures) > 1:
            first.add_note(f"{len(failures) - 1} additional runtime cleanup failure(s) suppressed")
        raise first
