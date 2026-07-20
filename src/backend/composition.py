"""@brief 后端 composition root / Backend composition root."""

from __future__ import annotations

import inspect
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from backend.application.services import (
    AgentApplicationService,
    InterviewApplicationService,
    KnowledgeApplicationService,
    ResumeApplicationService,
    ResumeProposalApplicationService,
    ScopedKeyLocks,
    ServiceDependencies,
)
from backend.config import AIProviderEndpoint, BackendSettings
from backend.domain.ports import (
    AgentRepository,
    ArtifactRepository,
    InterviewRepository,
    JobRepository,
    KnowledgeRepository,
    ResumeProposalRepository,
    ResumeRepository,
    TelemetryWriter,
)
from backend.infrastructure.concurrency import BoundedTaskSupervisor, WorkLimits
from backend.infrastructure.contracts import ContractValidator
from backend.infrastructure.embeddings import DeterministicEmbeddingProvider
from backend.infrastructure.idempotency import IdempotencyRegistry
from backend.infrastructure.identity import IdentityResolver, build_identity_resolver
from backend.infrastructure.knowledge_parsing import LocalKnowledgeFileParser
from backend.infrastructure.knowledge_storage import LocalKnowledgeBlobStorage
from backend.infrastructure.logging import configure_logging, remove_logging_handler
from backend.infrastructure.memory import InMemoryWorkspaceRepository
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
from backend.infrastructure.telemetry import BufferedTelemetrySink, InMemoryTelemetryWriter


class WorkspaceRuntimeRepository(
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
    idempotency: IdempotencyRegistry | PostgresIdempotencyRegistry
    resume: ResumeApplicationService
    proposals: ResumeProposalApplicationService
    agent: AgentApplicationService
    interview: InterviewApplicationService
    knowledge: KnowledgeApplicationService
    model_provider: AgentModelProvider
    supervisor: BoundedTaskSupervisor
    telemetry: BufferedTelemetrySink
    telemetry_writer: TelemetryWriter
    database: AsyncDatabase | None


@asynccontextmanager
async def build_container(
    settings: BackendSettings, project_root: Path
) -> AsyncIterator[BackendContainer]:
    """@brief 创建并销毁一个后端运行时 / Create and tear down one backend runtime.

    @param settings 后端强类型设置 / Backend typed settings.
    @param project_root 项目根目录 / Project root directory.
    @return 生命周期拥有的容器 / Lifespan-owned container.
    @raise RuntimeError PostgreSQL DSN 缺失时抛出 / Raised when the PostgreSQL DSN is missing.
    """
    identity = build_identity_resolver(
        environment=settings.environment,
        default_scope=settings.default_scope,
        security=settings.security,
    )
    supervisor = BoundedTaskSupervisor(
        (
            WorkLimits("llm", settings.runtime.llm_concurrency),
            WorkLimits("render", settings.runtime.render_concurrency),
            WorkLimits("knowledge", settings.runtime.knowledge_concurrency),
            WorkLimits("interview", settings.runtime.interview_concurrency),
            WorkLimits("telemetry", 1),
        ),
        settings.runtime.job_queue_capacity,
        settings.runtime.shutdown_grace_ms,
    )
    database: AsyncDatabase | None = None
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
        telemetry_writer: TelemetryWriter = PostgresTelemetryWriter(database)
        idempotency: IdempotencyRegistry | PostgresIdempotencyRegistry = (
            PostgresIdempotencyRegistry(database)
        )
    else:
        storage = InMemoryWorkspaceRepository()
        telemetry_writer = InMemoryTelemetryWriter()
        idempotency = IdempotencyRegistry()
    telemetry = BufferedTelemetrySink(
        telemetry_writer,
        settings.observability.queue_capacity,
        settings.observability.batch_size,
        settings.observability.flush_interval_ms,
        settings.observability.drop_policy,
        settings.observability.enabled,
    )
    try:
        provider = await _provider_for(settings)
    except BaseException:
        if database is not None:
            await database.aclose()
        raise
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
        blob_root = project_root / blob_root
    blob_storage = LocalKnowledgeBlobStorage(blob_root)
    file_parser = LocalKnowledgeFileParser(settings.knowledge.max_extracted_characters)
    embedding_provider = DeterministicEmbeddingProvider(settings.ai.embedding_dimension)
    try:
        async with supervisor:
            telemetry.start(supervisor)
            logging_handler = configure_logging(settings.logging, telemetry)
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
            proposals = ResumeProposalApplicationService(storage, resume, knowledge, locks)
            container = BackendContainer(
                settings=settings,
                identity=identity,
                contracts=ContractValidator(
                    project_root / "contract" / "ai-job-workspace.contract.schema.json"
                ),
                idempotency=idempotency,
                resume=resume,
                proposals=proposals,
                agent=AgentApplicationService(storage, provider, dependencies, locks),
                interview=InterviewApplicationService(storage, storage, dependencies, locks),
                knowledge=knowledge,
                model_provider=provider,
                supervisor=supervisor,
                telemetry=telemetry,
                telemetry_writer=telemetry_writer,
                database=database,
            )
            try:
                yield container
            finally:
                remove_logging_handler(logging_handler)
                await telemetry.close()
    finally:
        await _close_provider(provider)
        if database is not None:
            await database.aclose()


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
            api_key_env=settings.ai.api_key_env,
            base_url=_required_provider_base_url(settings.ai.base_url),
            data_region=settings.ai.data_region,
        ),
        *settings.ai.fallback_providers,
    )
    credentials: list[tuple[AIProviderEndpoint, str]] = []
    for endpoint in endpoints:
        api_key = os.environ.get(endpoint.api_key_env)
        if not api_key:
            raise RuntimeError(
                "model provider API key environment variable is not configured: "
                f"{endpoint.api_key_env}"
            )
        credentials.append((endpoint, api_key))

    created: list[AgentModelProvider] = []
    rate_limiters: dict[tuple[str, str, str], ProviderRateLimiter] = {}
    try:
        for endpoint, api_key in credentials:
            endpoint_key = (endpoint.provider, endpoint.base_url, endpoint.api_key_env)
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
