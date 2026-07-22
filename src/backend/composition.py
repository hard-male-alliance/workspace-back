"""@brief 后端 composition root / Backend composition root."""

from __future__ import annotations

import inspect
import logging
import socket
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from backend.application.concurrency import BoundedTaskSupervisor, WorkLimits
from backend.application.diagnostics import DiagnosticIngestionService, DiagnosticRateLimiter
from backend.application.identity import HostedIdentityService
from backend.application.oauth import OAuthAuthorizationService
from backend.application.services import (
    AgentApplicationService,
    InterviewApplicationService,
    KnowledgeApplicationService,
    ResumeApplicationService,
    ResumeProposalApplicationService,
    ScopedKeyLocks,
    ServiceDependencies,
    WorkspaceApplicationService,
)
from backend.config import AIProviderEndpoint, BackendSettings
from backend.domain.observability import ResourceMetadata
from backend.domain.ports import (
    AgentRepository,
    ArtifactRepository,
    EmbeddingProvider,
    HostedIdentityRepository,
    InterviewRepository,
    JobRepository,
    KnowledgeRepository,
    OAuthAuthorizationRequestRepository,
    ResumeProposalRepository,
    ResumeRepository,
    TelemetryWriter,
    WorkspaceRepository,
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
from backend.infrastructure.identity_email import identity_email_sender_for
from backend.infrastructure.knowledge_parsing import LocalKnowledgeFileParser
from backend.infrastructure.knowledge_storage import LocalKnowledgeBlobStorage
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
from backend.infrastructure.persistence import (
    AsyncDatabase,
    AsyncDatabaseOptions,
    PostgresIdempotencyRegistry,
    PostgresTelemetryWriter,
    PostgresWorkspaceRepository,
)
from backend.infrastructure.providers import (
    AgentModelProvider,
    FallbackModelProvider,
    MockModelProvider,
    OpenAICompatibleModelProvider,
    ProviderRateLimiter,
)
from backend.infrastructure.rendering import renderer_for
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
    proposals: ResumeProposalApplicationService
    agent: AgentApplicationService
    interview: InterviewApplicationService
    knowledge: KnowledgeApplicationService
    model_provider: AgentModelProvider
    supervisor: BoundedTaskSupervisor
    telemetry: ObservabilityPipeline
    telemetry_writer: TelemetryWriter
    logging_runtime: LoggingRuntime
    diagnostics: DiagnosticIngestionService
    oauth: OAuthAuthorizationService
    hosted_identity: HostedIdentityService
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
        ),
        settings.runtime.job_queue_capacity,
        settings.runtime.shutdown_grace_ms,
    )
    database: AsyncDatabase | None = None
    telemetry_database: AsyncDatabase | None = None
    oauth_repository: OAuthAuthorizationRequestRepository
    hosted_identity_repository: HostedIdentityRepository
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
            oauth_repository = PostgresOAuthAuthorizationRequestRepository(database)
            hosted_identity_repository = PostgresHostedIdentityRepository(database)
        else:
            storage = InMemoryWorkspaceRepository()
            telemetry_writer = InMemoryTelemetryWriter()
            idempotency = IdempotencyRegistry()
            oauth_repository = InMemoryOAuthAuthorizationRequestRepository()
            hosted_identity_repository = InMemoryHostedIdentityRepository()
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
        file_parser = LocalKnowledgeFileParser(settings.knowledge.max_extracted_characters)
        embedding_provider = _embedding_provider_for(settings)
        async with supervisor:
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
                renderer_for(settings.renderer),
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
            identity_email_sender = identity_email_sender_for(
                settings.hosted_identity.email,
                environment=settings.environment,
            )
            container = BackendContainer(
                settings=settings,
                identity=identity,
                contracts=ContractValidator.from_json(read_contract_schema_text()),
                contracts_v2=ContractValidator.from_jsonc(read_contract_schema_text("v2")),
                idempotency=idempotency,
                workspace=WorkspaceApplicationService(storage),
                resume=resume,
                proposals=proposals,
                agent=AgentApplicationService(storage, provider, knowledge, dependencies, locks),
                interview=InterviewApplicationService(storage, storage, dependencies, locks),
                knowledge=knowledge,
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
                hosted_identity=HostedIdentityService(
                    hosted_identity_repository,
                    oauth_service,
                    identity_email_sender,
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
                ),
                database=database,
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
