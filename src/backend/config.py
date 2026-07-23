"""@brief 后端独立配置服务 / Backend-owned configuration service."""

from __future__ import annotations

import base64
import binascii
import hmac
import math
import re
from dataclasses import dataclass, field
from ipaddress import IPv4Network, IPv6Network, ip_address, ip_network
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from urllib.parse import urlsplit

import json5

from backend.domain.oauth_scopes import SUPPORTED_OAUTH_SCOPE_SET, SUPPORTED_OAUTH_SCOPES
from workspace_shared.jsonc import ConfigurationError, load_jsonc, require_mapping
from workspace_shared.tenancy import ActorScope

DatabaseMode = Literal["memory", "postgresql"]
"""@brief 数据库运行模式 / Database runtime modes."""

IdentityMode = Literal["disabled", "development_mock", "trusted_proxy_hmac"]
WorkspaceDataRegion = Literal["cn", "global", "private_deployment"]
"""@brief 新 Workspace 的显式数据驻留地域 / Explicit data-residency region for new workspaces."""
"""@brief 身份解析模式 / Identity resolution modes."""

_DEVELOPMENT_IDENTITY_ENVIRONMENTS = frozenset({"development", "test"})
"""@brief 允许 development mock 的环境 / Environments allowed to use development mocks."""

_SUPPORTED_ENVIRONMENTS = frozenset({"development", "test", "staging", "production"})
"""@brief 唯一允许的部署环境标签 / Only supported deployment-environment labels."""

_PRODUCTION_PUBLIC_ORIGIN = "https://api.hmalliances.org:8022"
"""@brief API Standard V2 冻结的生产公开 Origin / Production public origin frozen by API Standard V2."""

_MAX_TRUSTED_PROXY_CLOCK_SKEW_SECONDS = 600
"""@brief 允许的最大身份断言时钟偏差 / Maximum permitted identity-assertion clock skew in seconds."""

_OAUTH_SCOPE_CHARS = frozenset(
    "!#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~"
)

_DEFAULT_OAUTH_SCOPES = SUPPORTED_OAUTH_SCOPES
"""@brief 本地 public client 默认使用领域闭合 scope 目录 / Local public clients default to the closed domain scope catalog."""


OAuthClientType = Literal["web", "electron"]
IdentityEmailMode = Literal["memory", "smtp"]
PasswordBreachMode = Literal["disabled", "pwned_passwords"]
"""@brief 泄露密码检查模式 / Breached-password checking modes."""

KnowledgeUploadMode = Literal["local", "s3"]
"""@brief Knowledge 对象存储模式 / Knowledge object-storage modes."""

KnowledgeMalwareMode = Literal["dev", "clamav", "reject"]
"""@brief Knowledge 恶意软件扫描模式 / Knowledge malware-scanning modes."""

ConnectionValidationMethod = Literal["GET", "POST"]
"""@brief Connection API-token 验证方法 / Connection API-token validation methods."""

InterviewRealtimeTransport = Literal["webrtc", "websocket"]
"""@brief Interview V2 实时传输类型 / Interview V2 realtime transport types."""

_CONNECTION_PROVIDER_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief Connection provider 稳定名称语法 / Stable Connection-provider name grammar."""


@dataclass(frozen=True, slots=True)
class OAuthPublicClientSettings:
    """One registered OAuth public client; public clients never carry a static secret."""

    client_id: str
    client_type: OAuthClientType
    redirect_uris: tuple[str, ...]
    allowed_scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OAuthSettings:
    """Authorization Server transaction and public-client settings."""

    authorization_request_ttl_seconds: int
    authorization_code_ttl_seconds: int
    access_token_ttl_seconds: int
    refresh_token_ttl_seconds: int
    signing_private_key_paths: tuple[Path, ...]
    public_clients: tuple[OAuthPublicClientSettings, ...]


@dataclass(frozen=True, slots=True)
class Aes256KeySettings:
    """@brief 一把配置态 AES-256 key / One configured AES-256 key.

    @param key_id 可持久化、可轮换的非秘密标识 / Persistable non-secret rotation identifier.
    @param key 仅驻留后端内存的 256-bit key material / 256-bit key material held only in backend memory.
    """

    key_id: str
    key: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class IdentityEmailOutboxSettings:
    """@brief 加密 outbox、租约、retry 与 retention 设置 / Encrypted outbox, lease, retry, and retention settings."""

    active_key_id: str | None
    encryption_keys: tuple[Aes256KeySettings, ...] = field(repr=False)
    rate_limit_hmac_key: bytes | None = field(repr=False)
    poll_interval_ms: int
    batch_size: int
    lease_seconds: int
    retry_base_seconds: int
    retry_cap_seconds: int
    max_attempts: int
    retention_days: int


@dataclass(frozen=True, slots=True)
class IdentityEmailSettings:
    """@brief Hosted identity 邮件 transport 与 durable queue 设置 / Email transport and durable queue settings."""

    mode: IdentityEmailMode
    from_address: str | None
    smtp_host: str | None
    smtp_port: int
    smtp_username: str | None
    smtp_password: str | None = field(repr=False)
    smtp_start_tls: bool = True
    outbox: IdentityEmailOutboxSettings = field(
        default_factory=lambda: _development_identity_email_outbox()
    )


@dataclass(frozen=True, slots=True)
class PasswordBreachSettings:
    """@brief 泄露密码检查的生产边界 / Production boundary for breached-password checking.

    @param mode development/test 可关闭，部署环境必须启用 Pwned Passwords /
        May be disabled in development/test; deployed environments require Pwned Passwords.
    @param request_timeout_ms 单次 range 查询 timeout / Per-range-query timeout.
    @param cache_ttl_seconds 前缀响应缓存寿命 / Prefix-response cache lifetime.
    @param max_cache_entries 有界前缀缓存容量 / Bounded prefix-cache capacity.
    """

    mode: PasswordBreachMode
    request_timeout_ms: int
    cache_ttl_seconds: int
    max_cache_entries: int


@dataclass(frozen=True, slots=True)
class HostedIdentitySettings:
    """Hosted identity security lifetimes and delivery settings."""

    flow_ttl_seconds: int
    email_code_ttl_seconds: int
    email_code_max_attempts: int
    email_send_limit_per_hour: int
    session_idle_ttl_seconds: int
    session_absolute_ttl_seconds: int
    recent_reauthentication_seconds: int
    email: IdentityEmailSettings
    password_breach: PasswordBreachSettings


@dataclass(frozen=True, slots=True)
class NetworkSettings:
    """@brief 后端网络设置 / Backend network settings.

    @note ``trusted_proxy_cidrs`` 是 backend 实际看到的私有入口代理对端地址范围，
    不是 ``X-Forwarded-For`` 的来源。Uvicorn 保持 ``proxy_headers=False``，因此任何
    客户端提供的 forwarded header 都不能改变可信来源判定。
    """

    bind_host: str
    bind_port: int
    public_base_url: str
    cors_allowed_origins: tuple[str, ...]
    trusted_proxy_cidrs: tuple[IPv4Network | IPv6Network, ...]
    outbound_proxy_url: str | None
    connect_timeout_ms: int
    read_timeout_ms: int


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """@brief 后端数据库连接设置 / Backend database connection settings."""

    mode: DatabaseMode
    application_dsn: str | None = field(repr=False)
    pool_size: int
    max_overflow: int
    connect_timeout_ms: int
    statement_timeout_ms: int
    lock_timeout_ms: int


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """@brief 受控并发设置 / Bounded-concurrency settings."""

    shutdown_grace_ms: int
    request_timeout_ms: int
    llm_concurrency: int
    render_concurrency: int
    knowledge_concurrency: int
    interview_concurrency: int
    job_queue_capacity: int
    sse_heartbeat_ms: int
    maintenance_interval_seconds: int
    maintenance_invitation_batch_size: int
    maintenance_idempotency_batch_size: int


@dataclass(frozen=True, slots=True)
class Aes256KeyringSettings:
    """@brief 支持轮换重叠的 AES-256 keyring / AES-256 keyring supporting rotation overlap.

    @param active_key_id 当前写 key；仅开发内存模式可为空 / Active write key; nullable only
        for in-memory development.
    @param keys 当前与旧的可读 key / Current and old readable keys.
    """

    active_key_id: str | None
    keys: tuple[Aes256KeySettings, ...] = field(repr=False)


@dataclass(frozen=True, slots=True)
class Hmac256KeySettings:
    """@brief 一把配置态 HMAC-256 key / One configured HMAC-256 key.

    @param key_id 可持久化、可轮换的非秘密标识 / Persistable non-secret rotation identifier.
    @param key 仅驻留后端内存的 256-bit key material / 256-bit key material held only
        in backend memory.
    """

    key_id: str
    key: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class Hmac256KeyringSettings:
    """@brief 支持轮换重叠的 HMAC-256 keyring / HMAC-256 keyring supporting rotation overlap.

    @param active_key_id 当前签发 key / Active signing key.
    @param keys 当前与旧的验证 key / Current and previous verification keys.
    """

    active_key_id: str | None
    keys: tuple[Hmac256KeySettings, ...] = field(repr=False)


@dataclass(frozen=True, slots=True)
class ConnectionApiTokenValidationSettings:
    """@brief 一个 provider 的在线 API-token 验证边界 / Online API-token validation boundary for one provider."""

    endpoint: str
    method: ConnectionValidationMethod
    authorization_scheme: str
    scopes_field: str


@dataclass(frozen=True, slots=True)
class KnowledgeConnectionProviderSettings:
    """@brief allowlist 中一个 Connection provider / One Connection provider in the allowlist."""

    provider: str
    client_id: str = field(repr=False)
    authorization_endpoint: str | None
    token_endpoint: str | None
    device_authorization_endpoint: str | None
    redirect_uri: str | None
    allowed_scopes: tuple[str, ...]
    api_token_validation: ConnectionApiTokenValidationSettings | None
    revocation_endpoint: str | None


@dataclass(frozen=True, slots=True)
class KnowledgeConnectionSettings:
    """@brief Connection registry、授权事务与 credential vault 设置 / Connection registry, authorization transaction, and credential-vault settings."""

    provider_session_keyring: Aes256KeyringSettings
    credential_keyring: Aes256KeyringSettings
    credential_fingerprint_hmac_key: bytes | None = field(repr=False)
    credential_reference_hmac_key: bytes | None = field(repr=False)
    orphan_grace_seconds: int
    connect_timeout_ms: int
    read_timeout_ms: int
    providers: tuple[KnowledgeConnectionProviderSettings, ...]


@dataclass(frozen=True, slots=True)
class KnowledgeLocalUploadStorageSettings:
    """@brief 仅开发/测试使用的本地签名上传存储 / Local signed-upload storage for development/test only."""

    mode: Literal["local"]
    directory: Path
    public_origin: str
    signing_hmac_key: bytes | None = field(repr=False)


@dataclass(frozen=True, slots=True)
class KnowledgeS3UploadStorageSettings:
    """@brief 显式凭据的 S3-compatible SigV4 存储 / S3-compatible SigV4 storage with explicit credentials."""

    mode: Literal["s3"]
    endpoint: str
    region: str
    bucket: str
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    session_token: str | None = field(repr=False)
    object_prefix: str
    connect_timeout_ms: int
    read_timeout_ms: int


type KnowledgeUploadStorageSettings = (
    KnowledgeLocalUploadStorageSettings | KnowledgeS3UploadStorageSettings
)
"""@brief Knowledge 上传存储判别联合 / Discriminated union of Knowledge upload storage."""


@dataclass(frozen=True, slots=True)
class KnowledgeMalwareSettings:
    """@brief fail-closed 恶意软件扫描设置 / Fail-closed malware-scanning settings."""

    mode: KnowledgeMalwareMode
    clamav_host: str | None
    clamav_port: int
    connect_timeout_ms: int
    read_timeout_ms: int


@dataclass(frozen=True, slots=True)
class KnowledgeUploadSettings:
    """@brief 上传存储、原子 Workspace quota 与内容安全边界 / Upload storage, atomic Workspace quota, and content-safety bounds."""

    storage: KnowledgeUploadStorageSettings
    malware: KnowledgeMalwareSettings
    maximum_workspace_bytes: int
    maximum_object_bytes: int
    maximum_archive_entries: int
    maximum_archive_depth: int
    maximum_expanded_bytes: int
    maximum_inflation_ratio: float
    maximum_scanner_chunk_bytes: int
    erasure_batch_size: int


@dataclass(frozen=True, slots=True)
class KnowledgeSourceNetworkSettings:
    """@brief 每一跳重验的来源 URL 与 SSRF 边界 / Source-URL and SSRF bounds revalidated on every hop."""

    allowed_schemes: tuple[str, ...]
    allowed_ports: tuple[int, ...]
    allowed_host_patterns: tuple[str, ...]
    maximum_redirects: int
    allow_https_downgrade: bool
    maximum_body_bytes: int
    connect_timeout_ms: int
    read_timeout_ms: int


@dataclass(frozen=True, slots=True)
class KnowledgeWorkerSettings:
    """@brief Knowledge worker 重试与物化资源上限 / Knowledge-worker retry and materialization bounds."""

    maximum_attempts: int
    maximum_material_bytes: int


@dataclass(frozen=True, slots=True)
class KnowledgeSearchSettings:
    """@brief 混合检索融合与候选预算 / Hybrid-search fusion and candidate budgets."""

    lexical_weight: float
    semantic_weight: float
    candidate_multiplier: int


@dataclass(frozen=True, slots=True)
class KnowledgeIndexSettings:
    """@brief 解析、分块与 embedding 的有界参数 / Bounded parsing, chunking, and embedding parameters."""

    maximum_extracted_characters: int
    maximum_chunks: int
    chunk_max_characters: int
    chunk_overlap_characters: int
    embedding_batch_size: int


@dataclass(frozen=True, slots=True)
class KnowledgeSettings:
    """@brief Knowledge V2 外部端口的完整配置 / Complete configuration for Knowledge V2 external ports."""

    connections: KnowledgeConnectionSettings
    uploads: KnowledgeUploadSettings
    source_network: KnowledgeSourceNetworkSettings
    worker: KnowledgeWorkerSettings
    search: KnowledgeSearchSettings
    index: KnowledgeIndexSettings

    @property
    def blob_directory(self) -> Path:
        """@brief 返回 V1 本地 blob 兼容路径 / Return the V1 local-blob compatibility path.

        @return 本地模式目录；S3 模式使用不承载持久数据的占位路径 / Local-mode directory,
            or a non-persistent compatibility placeholder in S3 mode.
        """

        storage = self.uploads.storage
        if isinstance(storage, KnowledgeLocalUploadStorageSettings):
            return storage.directory
        return Path("data/knowledge-blobs-disabled")

    @property
    def max_upload_bytes(self) -> int:
        """@brief 返回统一对象大小上限 / Return the unified object-size limit.

        @return byte 上限 / Byte limit.
        """

        return self.uploads.maximum_object_bytes

    @property
    def max_extracted_characters(self) -> int:
        """@brief 返回统一提取字符上限 / Return the unified extracted-character limit.

        @return 字符上限 / Character limit.
        """

        return self.index.maximum_extracted_characters

    @property
    def chunk_max_characters(self) -> int:
        """@brief 返回统一 chunk 字符上限 / Return the unified chunk-character limit.

        @return 单 chunk 字符上限 / Per-chunk character limit.
        """

        return self.index.chunk_max_characters

    @property
    def chunk_overlap_characters(self) -> int:
        """@brief 返回统一 chunk overlap / Return the unified chunk overlap.

        @return 相邻 chunk 重叠字符数 / Overlapping characters between adjacent chunks.
        """

        return self.index.chunk_overlap_characters


@dataclass(frozen=True, slots=True)
class InterviewRealtimeSettings:
    """@brief Interview V2 短期实时凭据策略 / Interview V2 short-lived realtime credential policy.

    @param signing_keyring 独立的 HMAC 签名 keyring / Independent HMAC signing keyring.
    @param signaling_url 显式 HTTPS/WSS signaling endpoint / Explicit HTTPS/WSS signaling endpoint.
    @param allowed_transports 服务端支持的传输集 / Server-supported transport set.
    @param credential_ttl_seconds 一次性 grant 寿命 / One-time grant lifetime.
    @param heartbeat_interval_ms 客户端心跳间隔 / Client heartbeat interval.
    @param ice_urls 显式 STUN/TURN URI 集 / Explicit STUN/TURN URI set.
    @param turn_shared_secret coturn TURN REST 临时凭据独立 secret / Independent coturn TURN REST temporary-credential secret.
    """

    signing_keyring: Hmac256KeyringSettings
    signaling_url: str | None
    allowed_transports: tuple[InterviewRealtimeTransport, ...]
    credential_ttl_seconds: int
    heartbeat_interval_ms: int
    ice_urls: tuple[str, ...]
    turn_shared_secret: str | None = field(repr=False)

    @property
    def active_signing_key(self) -> bytes | None:
        """@brief 解析当前签发 key / Resolve the active signing key.

        @return 当前 key bytes；未配置的 development/test 为 ``None`` / Active key bytes,
            or ``None`` for an unconfigured development/test deployment.
        """

        active_key_id = self.signing_keyring.active_key_id
        if active_key_id is None:
            return None
        return next(
            item.key for item in self.signing_keyring.keys if item.key_id == active_key_id
        )


@dataclass(frozen=True, slots=True)
class InterviewSettings:
    """@brief Interview V2 外部端口配置 / Interview V2 external-port configuration.

    @param realtime 实时信令与 TURN 凭据策略 / Realtime signaling and TURN-credential policy.
    @param report_timeout_ms 一次模型报告生成的 wall-time 上限 / Whole-call wall-time limit
        for one model-backed report generation.
    """

    realtime: InterviewRealtimeSettings
    report_timeout_ms: int
    recording_directory: Path
    media_chunk_max_bytes: int
    media_session_max_bytes: int


@dataclass(frozen=True, slots=True)
class RendererSettings:
    """@brief 简历编译器设置 / Resume renderer settings."""

    adapter: Literal["mock", "xelatex"]
    xelatex_command: str
    timeout_ms: int
    max_input_bytes: int
    max_output_bytes: int
    memory_limit_bytes: int
    allowed_font_directories: tuple[str, ...]
    artifact_directory: Path


@dataclass(frozen=True, slots=True)
class AISettings:
    """@brief Agent 与 embedding 设置 / Agent and embedding settings."""

    provider: str
    model: str
    api_key: str | None = field(repr=False)
    base_url: str | None
    data_region: str
    fallback_providers: tuple[AIProviderEndpoint, ...]
    embedding_provider: str
    embedding_model: str
    embedding_model_revision: str
    embedding_dimension: int
    embedding_distance_metric: str
    embedding_normalization: str
    provider_rate_limit: ProviderRateLimitSettings
    metering: MeteringSettings


@dataclass(frozen=True, slots=True)
class ProviderRateLimitSettings:
    """@brief 单个模型 endpoint 的进程内限流设置 / Per-model-endpoint in-process rate limits.

    @note 该限制器位于 provider 适配器边界，独立于运行时全局 ``llm_concurrency``。
    前者按配置的实际 endpoint/credential 保护并发与每分钟请求预算；后者只限制本 worker
    所有 LLM 工作的总量。多 worker 部署仍应按 worker 数量向上游申请足够配额，或由
    入口/供应商施加全局配额。
    """

    max_concurrent_requests: int
    requests_per_minute: int
    acquire_timeout_ms: int


@dataclass(frozen=True, slots=True)
class MeteringSettings:
    """@brief 本地可审计 token/成本估算设置 / Local auditable token-and-cost estimate settings.

    单位为每一百万 token 的 micro-USD（``1 USD = 1_000_000 micro-USD``），从而避免
    配置与 JSON 持久化中的浮点货币误差。v0.1 的 token 是本地 UTF-8 估算，而不是
    provider invoice；公开结果会显式标记为 ``estimated``。
    """

    input_cost_microusd_per_million_tokens: int
    output_cost_microusd_per_million_tokens: int


@dataclass(frozen=True, slots=True)
class AIProviderEndpoint:
    """@brief 一个仅服务端可见的模型 fallback 端点 / A server-only model fallback endpoint.

    @param provider 稳定 provider 标签（例如 openrouter）/ Stable provider label.
    @param model 服务端模型标识 / Server-side model identifier.
    @param api_key 直接来自私有 config.jsonc 的 API key / API key read directly from private config.jsonc.
    @param base_url OpenAI-compatible API base URL / OpenAI-compatible API base URL.
    @param data_region 此端点实际处理数据的地域 / Region where this endpoint processes data.
    """

    provider: str
    model: str
    api_key: str = field(repr=False)
    base_url: str
    data_region: str


@dataclass(frozen=True, slots=True)
class ObservabilitySettings:
    """@brief 业务可观测性设置 / Business observability settings."""

    enabled: bool
    queue_capacity: int
    batch_size: int
    flush_interval_ms: int
    retention_days: int
    drop_policy: Literal["drop_newest", "drop_oldest"]
    shutdown_flush_timeout_ms: int
    writer: ObservabilityWriterSettings
    diagnostics: DiagnosticsSettings


@dataclass(frozen=True, slots=True)
class ObservabilityWriterSettings:
    """@brief 隔离的 telemetry PostgreSQL writer 设置 / Isolated telemetry PostgreSQL writer settings."""

    pool_size: int
    connect_timeout_ms: int
    statement_timeout_ms: int
    lock_timeout_ms: int


@dataclass(frozen=True, slots=True)
class DiagnosticsSettings:
    """@brief 浏览器诊断入口资源预算 / Browser diagnostics ingress resource budgets."""

    max_body_bytes: int
    max_batch_size: int
    max_event_age_seconds: int
    max_future_skew_seconds: int
    rate_limit_capacity: int
    rate_limit_refill_per_minute: int
    max_actor_buckets: int


LogSink = Literal["stdout", "stderr", "file"]
"""@brief 日志输出目标 / Log output targets."""


@dataclass(frozen=True, slots=True)
class LoggingRouteSettings:
    """@brief 一条精确等级日志路由 / One exact-level logging route."""

    sink: LogSink
    levels: tuple[str, ...]
    path: Path | None = None
    max_bytes: int | None = None
    backup_count: int | None = None


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    """@brief 结构化日志设置 / Structured logging settings."""

    queue_capacity: int
    routes: tuple[LoggingRouteSettings, ...]
    persist_structured_events: bool
    shutdown_timeout_ms: int = 5_000


@dataclass(frozen=True, slots=True)
class SecuritySettings:
    """@brief 身份边界安全设置 / Identity-boundary security settings.

    @note HMAC（Hash-based Message Authentication Code）密钥直接来自被 Git 忽略且
    权限受限的 config.jsonc；dataclass repr 不输出该值。
    """

    identity_mode: IdentityMode
    trusted_proxy_hmac_secret: str | None = field(repr=False)
    cursor_hmac_secret: str | None = field(repr=False)
    sensitive_idempotency_hmac_secret: str | None = field(repr=False)
    trusted_proxy_max_clock_skew_seconds: int


@dataclass(frozen=True, slots=True)
class ApiSurfaceSettings:
    """@brief HTTP API 表面与迁移开关 / HTTP API surface and migration switches.

    @note API V1 不是回退路径；它只允许在 development/test 的显式并行迁移中挂载。
        / API V1 is never a fallback path; it may only be mounted for an explicit
        development/test parallel migration.
    """

    legacy_v1_enabled: bool


@dataclass(frozen=True, slots=True)
class BackendSettings:
    """@brief 后端组合根的完整设置 / Complete settings for the backend composition root."""

    environment: str
    api: ApiSurfaceSettings
    default_scope: ActorScope
    workspace_default_data_region: WorkspaceDataRegion
    network: NetworkSettings
    database: DatabaseSettings
    runtime: RuntimeSettings
    knowledge: KnowledgeSettings
    interview: InterviewSettings
    renderer: RendererSettings
    ai: AISettings
    observability: ObservabilitySettings
    logging: LoggingSettings
    security: SecuritySettings
    oauth: OAuthSettings
    hosted_identity: HostedIdentitySettings
    config_path: Path

    @classmethod
    def from_file(cls, path: Path) -> BackendSettings:
        """@brief 从统一 JSONC 配置构建后端设置 / Build backend settings from unified JSONC.

        @param path 根配置路径 / Root configuration path.
        @return 后端独有的强类型设置 / Backend-owned typed settings.
        @raise ConfigurationError 配置类型或约束不成立时抛出 / Raised for invalid configuration.
        """
        _reject_duplicate_configuration_keys(path)
        root = load_jsonc(path)
        environment = _require_string(root, "environment")
        if environment not in _SUPPORTED_ENVIRONMENTS:
            raise ConfigurationError(
                "environment must be development, test, staging, or production"
            )
        api = _api_surface_settings(root.get("api"), environment=environment)
        workspace = require_mapping(root.get("workspace"), "workspace")
        network = require_mapping(root.get("network"), "network")
        database = require_mapping(root.get("database"), "database")
        runtime = require_mapping(root.get("runtime"), "runtime")
        knowledge = require_mapping(root.get("knowledge"), "knowledge")
        interview = require_mapping(root.get("interview"), "interview")
        renderer = require_mapping(root.get("resume_rendering"), "resume_rendering")
        ai = require_mapping(root.get("ai"), "ai")
        observability = require_mapping(root.get("observability"), "observability")
        logging = require_mapping(root.get("logging"), "logging")
        security = _security_settings(
            root.get("security"),
            environment,
            legacy_v1_enabled=api.legacy_v1_enabled,
        )
        oauth = _oauth_settings(root.get("oauth"), environment)
        hosted_identity = _hosted_identity_settings(root.get("hosted_identity"), environment)
        database_mode = cast(
            DatabaseMode, _require_choice(database, "mode", {"memory", "postgresql"})
        )
        if environment in {"staging", "production"} and database_mode != "postgresql":
            raise ConfigurationError("database.mode must be postgresql in staging/production")
        _validate_deployed_identity_email_outbox(hosted_identity.email.outbox, environment)
        application_dsn = _database_dsn(
            database,
            key="application_dsn",
            mode=database_mode,
        )
        knowledge_settings = _knowledge_settings(
            knowledge,
            environment=environment,
            database_mode=database_mode,
        )
        interview_settings = _interview_settings(
            interview,
            environment=environment,
        )
        _reject_cross_feature_key_reuse(
            hosted_identity.email.outbox,
            knowledge_settings,
            interview_settings,
            security,
        )
        renderer_adapter = cast(
            Literal["mock", "xelatex"], _require_choice(renderer, "adapter", {"mock", "xelatex"})
        )
        drop_policy = cast(
            Literal["drop_newest", "drop_oldest"],
            _require_choice(observability, "drop_policy", {"drop_newest", "drop_oldest"}),
        )
        observability_writer = _observability_writer_settings(observability.get("writer"))
        diagnostics = _diagnostics_settings(observability.get("diagnostics"))
        logging_settings = _logging_settings(logging)
        font_directories = renderer.get("allowed_font_directories", [])
        fallback_providers = ai.get("fallback_providers", [])
        if not isinstance(font_directories, list) or not all(
            isinstance(item, str) for item in font_directories
        ):
            raise ConfigurationError(
                "resume_rendering.allowed_font_directories must be a string array"
            )
        if not isinstance(fallback_providers, list):
            raise ConfigurationError("ai.fallback_providers must be an array")
        ai_provider = _require_string(ai, "provider")
        ai_model = _require_string(ai, "model")
        embedding_provider = _require_string(ai, "embedding_provider")
        embedding_model = _require_string(ai, "embedding_model")
        embedding_model_revision = _require_string(ai, "embedding_model_revision")
        ai_base_url = _optional_string(ai.get("base_url"))
        api_key = _direct_optional_secret(ai, "api_key", "ai.api_key")
        if (ai_provider != "mock" or embedding_provider != "mock") and api_key is None:
            raise ConfigurationError(
                "ai.api_key is required when a model or embedding provider is not mock"
            )
        public_base_url = _require_string(network, "public_base_url").rstrip("/")
        _validate_deployed_capabilities(
            environment=environment,
            public_base_url=public_base_url,
            renderer_adapter=renderer_adapter,
            ai_provider=ai_provider,
            ai_model=ai_model,
            embedding_provider=embedding_provider,
            embedding_model=embedding_model,
            embedding_model_revision=embedding_model_revision,
            ai_base_url=ai_base_url,
        )
        return cls(
            environment=environment,
            api=api,
            default_scope=ActorScope(
                actor_id=_require_string(workspace, "default_actor_id"),
                workspace_id=_require_string(workspace, "default_workspace_id"),
                resource_owner_id=_require_string(workspace, "default_resource_owner_id"),
            ),
            workspace_default_data_region=cast(
                WorkspaceDataRegion,
                _require_choice(
                    workspace,
                    "default_data_region",
                    {"cn", "global", "private_deployment"},
                ),
            ),
            network=NetworkSettings(
                bind_host=_require_string(network, "bind_host"),
                bind_port=_require_positive_int(network, "bind_port"),
                public_base_url=public_base_url,
                cors_allowed_origins=_require_cors_allowed_origins(
                    network.get("cors_allowed_origins", [])
                ),
                trusted_proxy_cidrs=_require_trusted_proxy_cidrs(
                    network.get("trusted_proxy_cidrs")
                ),
                outbound_proxy_url=_optional_proxy_url(network.get("outbound_proxy_url")),
                connect_timeout_ms=_require_positive_int(network, "connect_timeout_ms"),
                read_timeout_ms=_require_positive_int(network, "read_timeout_ms"),
            ),
            database=DatabaseSettings(
                mode=database_mode,
                application_dsn=application_dsn,
                pool_size=_require_positive_int(database, "pool_size"),
                max_overflow=_require_non_negative_int(database, "max_overflow"),
                connect_timeout_ms=_require_positive_int(database, "connect_timeout_ms"),
                statement_timeout_ms=_require_positive_int(database, "statement_timeout_ms"),
                lock_timeout_ms=_require_positive_int(database, "lock_timeout_ms"),
            ),
            runtime=RuntimeSettings(
                shutdown_grace_ms=_require_positive_int(runtime, "shutdown_grace_ms"),
                request_timeout_ms=_require_positive_int(runtime, "request_timeout_ms"),
                llm_concurrency=_require_positive_int(runtime, "llm_concurrency"),
                render_concurrency=_require_positive_int(runtime, "render_concurrency"),
                knowledge_concurrency=_require_positive_int(runtime, "knowledge_concurrency"),
                interview_concurrency=_require_positive_int(runtime, "interview_concurrency"),
                job_queue_capacity=_require_positive_int(runtime, "job_queue_capacity"),
                sse_heartbeat_ms=_require_positive_int(runtime, "sse_heartbeat_ms"),
                maintenance_interval_seconds=_optional_positive_int(
                    runtime, "maintenance_interval_seconds", 60
                ),
                maintenance_invitation_batch_size=_optional_bounded_positive_int(
                    runtime, "maintenance_invitation_batch_size", default=250, maximum=1_000
                ),
                maintenance_idempotency_batch_size=_optional_bounded_positive_int(
                    runtime, "maintenance_idempotency_batch_size", default=250, maximum=1_000
                ),
            ),
            knowledge=knowledge_settings,
            interview=interview_settings,
            renderer=RendererSettings(
                adapter=renderer_adapter,
                xelatex_command=_require_string(renderer, "xelatex_command"),
                timeout_ms=_require_positive_int(renderer, "timeout_ms"),
                max_input_bytes=_require_positive_int(renderer, "max_input_bytes"),
                max_output_bytes=_require_positive_int(renderer, "max_output_bytes"),
                memory_limit_bytes=_require_positive_int(renderer, "memory_limit_bytes"),
                allowed_font_directories=tuple(font_directories),
                artifact_directory=Path(_require_string(renderer, "artifact_directory")),
            ),
            ai=AISettings(
                provider=ai_provider,
                model=ai_model,
                api_key=api_key,
                base_url=ai_base_url,
                data_region=_require_choice(
                    ai, "data_region", {"cn", "global", "private_deployment"}
                ),
                fallback_providers=tuple(
                    _provider_endpoint(item, index) for index, item in enumerate(fallback_providers)
                ),
                embedding_provider=embedding_provider,
                embedding_model=embedding_model,
                embedding_model_revision=embedding_model_revision,
                embedding_dimension=_require_positive_int(ai, "embedding_dimension"),
                embedding_distance_metric=_require_string(ai, "embedding_distance_metric"),
                embedding_normalization=_require_string(ai, "embedding_normalization"),
                provider_rate_limit=_provider_rate_limit_settings(
                    require_mapping(ai.get("provider_rate_limit"), "ai.provider_rate_limit")
                ),
                metering=_metering_settings(require_mapping(ai.get("metering"), "ai.metering")),
            ),
            observability=ObservabilitySettings(
                enabled=_require_bool(observability, "enabled"),
                queue_capacity=_require_positive_int(observability, "queue_capacity"),
                batch_size=_require_positive_int(observability, "batch_size"),
                flush_interval_ms=_require_positive_int(observability, "flush_interval_ms"),
                retention_days=_require_non_negative_int(observability, "retention_days"),
                drop_policy=drop_policy,
                shutdown_flush_timeout_ms=_optional_positive_int(
                    observability, "shutdown_flush_timeout_ms", 5_000
                ),
                writer=observability_writer,
                diagnostics=diagnostics,
            ),
            logging=logging_settings,
            security=security,
            oauth=oauth,
            hosted_identity=hosted_identity,
            config_path=path,
        )


def _api_surface_settings(value: Any, *, environment: str) -> ApiSurfaceSettings:
    """@brief 解析显式 API 迁移表面 / Parse the explicit API migration surface.

    @param value 根 ``api`` 对象 / Root ``api`` object.
    @param environment 已验证环境标签 / Validated environment label.
    @return 强类型 API 表面设置 / Typed API-surface settings.
    @raise ConfigurationError 键未知、类型错误或部署环境尝试挂载 V1 时抛出 / Raised
        for unknown keys, invalid types, or attempts to mount V1 in a deployed environment.
    """

    mapping = require_mapping(value, "api")
    _reject_unknown_keys(mapping, {"legacy_v1_enabled"}, "api")
    legacy_v1_enabled = _require_bool(mapping, "legacy_v1_enabled")
    if legacy_v1_enabled and environment in {"staging", "production"}:
        raise ConfigurationError(
            "api.legacy_v1_enabled must be false in staging/production"
        )
    return ApiSurfaceSettings(legacy_v1_enabled=legacy_v1_enabled)


def _validate_deployed_capabilities(
    *,
    environment: str,
    public_base_url: str,
    renderer_adapter: str,
    ai_provider: str,
    ai_model: str,
    embedding_provider: str,
    embedding_model: str,
    embedding_model_revision: str,
    ai_base_url: str | None,
) -> None:
    """@brief 阻止部署环境把测试替身伪装成成功能力 / Prevent deployed environments from presenting test doubles as successful capabilities.

    @param environment 已验证环境标签 / Validated environment label.
    @param public_base_url 服务端生成绝对 URL 的公开 Origin / Public origin used for server-generated absolute URLs.
    @param renderer_adapter Resume renderer 适配器 / Resume-renderer adapter.
    @param ai_provider 主模型 provider / Primary model provider.
    @param ai_model 主模型标识 / Primary model identifier.
    @param embedding_provider embedding provider / Embedding provider.
    @param embedding_model embedding 模型标识 / Embedding-model identifier.
    @param embedding_model_revision embedding space revision / Embedding-space revision.
    @param ai_base_url OpenAI-compatible HTTPS endpoint / OpenAI-compatible HTTPS endpoint.
    @raise ConfigurationError staging/production 仍含 mock、占位模型或不安全 endpoint 时抛出 / Raised when staging or production retains mocks, placeholder models, or an unsafe endpoint.
    """

    if environment not in {"staging", "production"}:
        return
    if renderer_adapter == "mock":
        raise ConfigurationError(
            "resume_rendering.adapter must not be mock in staging/production"
        )
    if ai_provider == "mock" or ai_model.startswith("mock-"):
        raise ConfigurationError("ai.provider/model must be real in staging/production")
    if embedding_provider == "mock" or embedding_model.startswith("mock-"):
        raise ConfigurationError(
            "ai.embedding_provider/model must be real in staging/production"
        )
    if embedding_model_revision.lower() in {"mock", "test"}:
        raise ConfigurationError(
            "ai.embedding_model_revision must be production-identifying in staging/production"
        )
    _require_deployed_https_url(ai_base_url, "ai.base_url")
    if environment == "production" and public_base_url != _PRODUCTION_PUBLIC_ORIGIN:
        raise ConfigurationError(
            "network.public_base_url must match the API V2 production origin"
        )
    if environment == "staging":
        _require_deployed_https_url(public_base_url, "network.public_base_url")


def _require_deployed_https_url(value: str | None, label: str) -> None:
    """@brief 验证部署外部端点是无凭据 HTTPS URL / Validate a deployed external endpoint as a credential-free HTTPS URL.

    @param value 待验证 URL / URL to validate.
    @param label 安全配置字段名 / Safe configuration-field label.
    @raise ConfigurationError URL 缺失、非 HTTPS 或携带凭据/fragment 时抛出 / Raised for a missing, non-HTTPS, credential-bearing, or fragmented URL.
    """

    if value is None:
        raise ConfigurationError(f"{label} is required in staging/production")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ConfigurationError(f"{label} must be a credential-free HTTPS URL")


def _require_string(mapping: dict[str, Any], key: str) -> str:
    """@brief 读取非空字符串 / Read a non-empty string.

    @param mapping 配置对象 / Configuration object.
    @param key 键名 / Key name.
    @return 字符串值 / String value.
    @raise ConfigurationError 值不是非空字符串时抛出 / Raised for a non-empty non-string value.
    """
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"configuration key {key!r} must be a non-empty string")
    return value


def _optional_string(value: object) -> str | None:
    """@brief 读取可空字符串 / Read a nullable string.

    @param value 配置值 / Configuration value.
    @return 字符串或 None / String or None.
    @raise ConfigurationError 值类型错误时抛出 / Raised for an invalid type.
    """
    if value is None or isinstance(value, str):
        return value
    raise ConfigurationError("configuration value must be a string or null")


def _database_dsn(
    database: dict[str, Any],
    *,
    key: str,
    mode: DatabaseMode,
) -> str | None:
    """@brief 从统一配置读取数据库 DSN / Read a database DSN from unified configuration.

    @param database 根 database 配置节 / Root database configuration section.
    @param key DSN 字段名 / DSN field name.
    @param mode 数据库运行模式 / Database runtime mode.
    @return memory 模式下可为空的 DSN / DSN, nullable in memory mode.
    @raise ConfigurationError PostgreSQL 模式缺少 DSN 或字段类型错误时抛出。
    / Raised when PostgreSQL mode lacks a DSN or the field type is invalid.
    """
    dsn = _optional_string(database.get(key))
    if isinstance(dsn, str):
        dsn = dsn.strip()
        if not dsn:
            raise ConfigurationError(f"database.{key} must be a non-empty string or null")
    if mode == "postgresql" and dsn is None:
        raise ConfigurationError(f"database.{key} is required in postgresql mode")
    return dsn


def _optional_proxy_url(value: object) -> str | None:
    """@brief 读取并验证统一出站代理 URL / Read and validate the shared outbound proxy URL.

    @param value ``network.outbound_proxy_url`` 原始配置值 / Raw configuration value.
    @return 规范化前、但已验证的 HTTP(S) proxy URL；``null`` 时返回 ``None``。
    @raise ConfigurationError URL 缺少主机、带 fragment 或使用不支持协议时抛出。

    @note proxy 密码允许仅存在于 secret 注入后的配置文件中，但调用方绝不可记录该值。
    HTTPX（HTTP client）只接受 HTTP(S) proxy，显式拒绝 PAC、file 和任意 URI scheme，
    避免把“可选代理”变成不透明的网络能力。
    """
    proxy_url = _optional_string(value)
    if proxy_url is None:
        return None
    try:
        parsed = urlsplit(proxy_url)
    except ValueError as error:
        raise ConfigurationError("network.outbound_proxy_url is not a valid URL") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.fragment
        or parsed.query
    ):
        raise ConfigurationError(
            "network.outbound_proxy_url must be an http(s) URL with a host and no query/fragment"
        )
    return proxy_url


def _require_trusted_proxy_cidrs(value: object) -> tuple[IPv4Network | IPv6Network, ...]:
    """@brief 解析 backend 对端的可信代理 CIDR / Parse trusted proxy peer CIDRs.

    @param value ``network.trusted_proxy_cidrs`` 原始配置值 / Raw configuration value.
    @return 至少一个已规范化 IPv4/IPv6 网络 / At least one normalized IPv4/IPv6 network.
    @raise ConfigurationError 配置不是非空 CIDR 字符串数组时抛出。

    @note 这些网络用于限制可信 HMAC（Hash-based Message Authentication Code）断言可从
    哪些 TCP 对端到达 backend；不会读取或信任转发 header，也不会解析主机名。
    """
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise ConfigurationError("network.trusted_proxy_cidrs must be a non-empty string array")
    networks: list[IPv4Network | IPv6Network] = []
    for item in value:
        try:
            networks.append(ip_network(item, strict=False))
        except ValueError as error:
            raise ConfigurationError(
                "network.trusted_proxy_cidrs contains an invalid CIDR"
            ) from error
    return tuple(networks)


def _require_cors_allowed_origins(value: object) -> tuple[str, ...]:
    """读取浏览器可访问产品 API 的精确 Origin allowlist。"""
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigurationError("network.cors_allowed_origins must be a string array")
    origins: list[str] = []
    for item in value:
        if item == "*":
            raise ConfigurationError("network.cors_allowed_origins must not contain a wildcard")
        try:
            parsed = urlsplit(item)
        except ValueError as error:
            raise ConfigurationError(
                "network.cors_allowed_origins contains an invalid origin"
            ) from error
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ConfigurationError(
                "network.cors_allowed_origins entries must be exact http(s) origins"
            )
        origin = item.rstrip("/")
        if origin not in origins:
            origins.append(origin)
    return tuple(origins)


def _require_positive_int(mapping: dict[str, Any], key: str) -> int:
    """@brief 读取正整数 / Read a positive integer.

    @param mapping 配置对象 / Configuration object.
    @param key 键名 / Key name.
    @return 正整数 / Positive integer.
    """
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(f"configuration key {key!r} must be a positive integer")
    return value


def _require_non_negative_int(mapping: dict[str, Any], key: str) -> int:
    """@brief 读取非负整数 / Read a non-negative integer.

    @param mapping 配置对象 / Configuration object.
    @param key 键名 / Key name.
    @return 非负整数 / Non-negative integer.
    """
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigurationError(f"configuration key {key!r} must be a non-negative integer")
    return value


def _optional_positive_int(mapping: dict[str, Any], key: str, default: int) -> int:
    """@brief 读取带默认值的正整数 / Read an optional positive integer with a default.

    @param mapping 配置对象 / Configuration object.
    @param key 键名 / Key name.
    @param default 缺失时默认值 / Default used when absent.
    @return 正整数 / Positive integer.
    @raise ConfigurationError 值不是正整数时抛出 / Raised for an invalid value.
    """
    if key not in mapping:
        return default
    return _require_positive_int(mapping, key)


def _optional_bounded_positive_int(
    mapping: dict[str, Any],
    key: str,
    *,
    default: int,
    maximum: int,
) -> int:
    """@brief 读取有硬上限的可选正整数 / Read an optional hard-bounded positive integer.

    @param mapping 配置对象 / Configuration object.
    @param key 键名 / Key name.
    @param default 缺失时默认值 / Default used when absent.
    @param maximum 代码级不可提升上限 / Code-level upper bound that config cannot raise.
    @return 不超过上限的正整数 / Positive integer no greater than the bound.
    @raise ConfigurationError 配置值越界时抛出 / Raised when the configured value is out of range.
    """
    value = _optional_positive_int(mapping, key, default)
    if value > maximum:
        raise ConfigurationError(
            f"configuration key {key!r} must not exceed {maximum}"
        )
    return value


def _observability_writer_settings(value: object) -> ObservabilityWriterSettings:
    """@brief 解析隔离 telemetry writer 配置 / Parse isolated telemetry-writer settings.

    @param value ``observability.writer`` 配置对象或缺失值 / Mapping or missing value.
    @return 已验证小连接池设置 / Validated small-pool settings.
    @raise ConfigurationError 字段未知或 timeout 次序非法时抛出。
    """
    mapping = {} if value is None else require_mapping(value, "observability.writer")
    _reject_unknown_keys(
        mapping,
        {"pool_size", "connect_timeout_ms", "statement_timeout_ms", "lock_timeout_ms"},
        "observability.writer",
    )
    settings = ObservabilityWriterSettings(
        pool_size=_optional_positive_int(mapping, "pool_size", 2),
        connect_timeout_ms=_optional_positive_int(mapping, "connect_timeout_ms", 1_000),
        statement_timeout_ms=_optional_positive_int(mapping, "statement_timeout_ms", 2_000),
        lock_timeout_ms=_optional_positive_int(mapping, "lock_timeout_ms", 500),
    )
    if settings.lock_timeout_ms > settings.statement_timeout_ms:
        raise ConfigurationError(
            "observability.writer.lock_timeout_ms must not exceed statement_timeout_ms"
        )
    return settings


def _diagnostics_settings(value: object) -> DiagnosticsSettings:
    """@brief 解析前端诊断入口预算 / Parse frontend diagnostics ingress budgets.

    @param value ``observability.diagnostics`` 配置对象或缺失值 / Mapping or missing value.
    @return 已验证限额 / Validated resource limits.
    @raise ConfigurationError 字段未知或批大小超过硬上限时抛出。
    """
    mapping = {} if value is None else require_mapping(value, "observability.diagnostics")
    _reject_unknown_keys(
        mapping,
        {
            "max_body_bytes",
            "max_batch_size",
            "max_event_age_seconds",
            "max_future_skew_seconds",
            "rate_limit_capacity",
            "rate_limit_refill_per_minute",
            "max_actor_buckets",
        },
        "observability.diagnostics",
    )
    max_batch_size = _optional_positive_int(mapping, "max_batch_size", 50)
    if max_batch_size > 50:
        raise ConfigurationError("observability.diagnostics.max_batch_size cannot exceed 50")
    settings = DiagnosticsSettings(
        max_body_bytes=_optional_positive_int(mapping, "max_body_bytes", 65_536),
        max_batch_size=max_batch_size,
        max_event_age_seconds=_optional_positive_int(mapping, "max_event_age_seconds", 86_400),
        max_future_skew_seconds=_optional_positive_int(mapping, "max_future_skew_seconds", 300),
        rate_limit_capacity=_optional_positive_int(mapping, "rate_limit_capacity", 100),
        rate_limit_refill_per_minute=_optional_positive_int(
            mapping, "rate_limit_refill_per_minute", 200
        ),
        max_actor_buckets=_optional_positive_int(mapping, "max_actor_buckets", 10_000),
    )
    if settings.rate_limit_capacity < settings.max_batch_size:
        raise ConfigurationError(
            "observability.diagnostics.rate_limit_capacity must cover one maximum batch"
        )
    return settings


def _logging_settings(mapping: dict[str, Any]) -> LoggingSettings:
    """@brief 解析精确等级日志路由 / Parse exact-level logging routes.

    @param mapping ``logging`` 配置对象 / ``logging`` configuration mapping.
    @return 已验证日志设置 / Validated logging settings.
    @raise ConfigurationError route 结构、等级或文件设置非法时抛出。

    @note 日志路由属于产品运行契约，必须显式配置；缺失时 fail fast，避免部署环境
    因隐藏默认值产生不同的 STDOUT/STDERR 或文件行为。
    """
    _reject_unknown_keys(
        mapping,
        {
            "persist_structured_events",
            "queue_capacity",
            "routes",
            "shutdown_timeout_ms",
        },
        "logging",
    )
    persist = _require_bool(mapping, "persist_structured_events")
    queue_capacity = _optional_positive_int(mapping, "queue_capacity", 2_048)
    shutdown_timeout_ms = _optional_positive_int(mapping, "shutdown_timeout_ms", 5_000)
    if shutdown_timeout_ms > 60_000:
        raise ConfigurationError("logging.shutdown_timeout_ms must not exceed 60000")
    raw_routes = mapping.get("routes")
    if not isinstance(raw_routes, list) or not raw_routes:
        raise ConfigurationError("logging.routes must be a non-empty array")
    routes: list[LoggingRouteSettings] = []
    allowed_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    claimed_stream_sinks: set[LogSink] = set()
    claimed_file_paths: set[Path] = set()
    for index, raw_route in enumerate(raw_routes):
        route = require_mapping(raw_route, f"logging.routes[{index}]")
        _reject_unknown_keys(
            route,
            {"sink", "levels", "path", "max_bytes", "backup_count"},
            f"logging.routes[{index}]",
        )
        sink = cast(LogSink, _require_choice(route, "sink", {"stdout", "stderr", "file"}))
        raw_levels = route.get("levels")
        if (
            not isinstance(raw_levels, list)
            or not raw_levels
            or not all(isinstance(level, str) for level in raw_levels)
        ):
            raise ConfigurationError(f"logging.routes[{index}].levels must be a non-empty array")
        levels = tuple(level.upper() for level in raw_levels)
        if len(set(levels)) != len(levels) or not set(levels).issubset(allowed_levels):
            raise ConfigurationError(
                f"logging.routes[{index}].levels must be unique standard log levels"
            )
        if sink == "file":
            path = Path(_require_string(route, "path"))
            if path in claimed_file_paths:
                raise ConfigurationError(
                    f"logging.routes[{index}].path must identify a distinct file sink"
                )
            claimed_file_paths.add(path)
            max_bytes = _require_positive_int(route, "max_bytes")
            backup_count = _require_positive_int(route, "backup_count")
        else:
            if sink in claimed_stream_sinks:
                raise ConfigurationError(f"logging.routes[{index}] duplicates the {sink} sink")
            claimed_stream_sinks.add(sink)
            if any(key in route for key in ("path", "max_bytes", "backup_count")):
                raise ConfigurationError(
                    f"logging.routes[{index}] stream routes cannot configure file rotation"
                )
            path = None
            max_bytes = None
            backup_count = None
        routes.append(LoggingRouteSettings(sink, levels, path, max_bytes, backup_count))
    return LoggingSettings(
        queue_capacity,
        tuple(routes),
        persist,
        shutdown_timeout_ms,
    )


def _reject_unknown_keys(mapping: dict[str, Any], allowed: set[str], label: str) -> None:
    """@brief 拒绝嵌套配置拼写错误 / Reject misspelled nested configuration keys.

    @param mapping 待检查对象 / Mapping to inspect.
    @param allowed 允许字段 / Allowed fields.
    @param label 配置路径 / Configuration path.
    @raise ConfigurationError 出现未知字段时抛出 / Raised when unknown keys are present.
    """
    unknown = set(mapping).difference(allowed)
    if unknown:
        raise ConfigurationError(f"{label} contains unknown keys: {sorted(unknown)!r}")


def _reject_duplicate_configuration_keys(path: Path) -> None:
    """@brief 在通用 JSONC loader 前拒绝重复 object key / Reject duplicate object keys before the shared JSONC loader.

    @param path 根配置路径 / Root configuration path.
    @return 无返回值 / No return value.
    @raise ConfigurationError 任一层出现重复 key 时抛出 / Raised for a duplicate key at any depth.

    @note 语法错误仍交给共享 loader 产生统一诊断；这里只补上 JSON5 默认允许重复 key
        的 fail-closed 约束 / Syntax errors remain owned by the shared loader; this only closes
        JSON5's duplicate-key default.
    """

    if not path.is_file():
        return
    try:
        json5.loads(path.read_text(encoding="utf-8"), allow_duplicate_keys=False)
    except OSError:
        return
    except ValueError as error:
        if str(error).startswith("Duplicate key"):
            raise ConfigurationError("configuration contains a duplicate object key") from error


def _interview_settings(
    mapping: dict[str, Any],
    *,
    environment: str,
) -> InterviewSettings:
    """@brief 解析 Interview V2 外部端口配置 / Parse Interview V2 external-port configuration.

    @param mapping ``interview`` 配置对象 / ``interview`` configuration mapping.
    @param environment 部署环境 / Deployment environment.
    @return 完整 Interview V2 配置 / Complete Interview V2 settings.
    @raise ConfigurationError 未知字段或 realtime policy 不安全 / Unknown fields or an
        unsafe realtime policy.
    """

    _reject_unknown_keys(
        mapping,
        {
            "realtime",
            "report_timeout_ms",
            "recording_directory",
            "media_chunk_max_bytes",
            "media_session_max_bytes",
        },
        "interview",
    )
    report_timeout_ms = _optional_bounded_positive_int(
        mapping,
        "report_timeout_ms",
        default=60_000,
        maximum=120_000,
    )
    chunk_max = _optional_bounded_positive_int(
        mapping, "media_chunk_max_bytes", default=1_048_576, maximum=8_388_608
    )
    session_max = _optional_bounded_positive_int(
        mapping, "media_session_max_bytes", default=536_870_912, maximum=1_073_741_824
    )
    if session_max < chunk_max:
        raise ConfigurationError(
            "interview.media_session_max_bytes must not be smaller than media_chunk_max_bytes"
        )
    return InterviewSettings(
        _interview_realtime_settings(
            mapping.get("realtime"),
            environment=environment,
        ),
        report_timeout_ms,
        Path(_optional_string(mapping.get("recording_directory")) or "data/interview-media"),
        chunk_max,
        session_max,
    )


def _interview_realtime_settings(
    value: object,
    *,
    environment: str,
) -> InterviewRealtimeSettings:
    """@brief 解析短期 realtime credential policy / Parse the short-lived realtime credential policy.

    @param value ``interview.realtime`` 配置对象 / Configuration mapping.
    @param environment 部署环境 / Deployment environment.
    @return 类型化 realtime 设置 / Typed realtime settings.
    @raise ConfigurationError keyring、endpoint、transport、TTL 或 ICE URI 非法 / Invalid
        keyring, endpoint, transport, TTL, or ICE URI.
    """

    label = "interview.realtime"
    mapping = require_mapping(value, label)
    _reject_unknown_keys(
        mapping,
        {
            "signing_keyring",
            "signaling_url",
            "allowed_transports",
            "credential_ttl_seconds",
            "heartbeat_interval_ms",
            "ice_urls",
            "turn_shared_secret",
        },
        label,
    )
    keyring = _hmac256_keyring_settings(
        mapping.get("signing_keyring"),
        f"{label}.signing_keyring",
    )
    signaling_url = _named_optional_string(mapping, "signaling_url", label)
    signaling_hostname: str | None = None
    if signaling_url is not None:
        signaling_hostname = _require_interview_signaling_url(
            signaling_url,
            allow_development_endpoint=environment in _DEVELOPMENT_IDENTITY_ENVIRONMENTS,
        )
    raw_transports = _unique_string_array(
        mapping.get("allowed_transports"),
        f"{label}.allowed_transports",
        minimum=1,
        maximum=2,
    )
    if not set(raw_transports).issubset({"webrtc", "websocket"}):
        raise ConfigurationError(
            "interview.realtime.allowed_transports may contain only webrtc and websocket"
        )
    transports = cast(tuple[InterviewRealtimeTransport, ...], raw_transports)
    credential_ttl_seconds = _require_positive_int(mapping, "credential_ttl_seconds")
    heartbeat_interval_ms = _require_positive_int(mapping, "heartbeat_interval_ms")
    if credential_ttl_seconds > 900:
        raise ConfigurationError(
            "interview.realtime.credential_ttl_seconds must not exceed 900"
        )
    if not 1_000 <= heartbeat_interval_ms <= 120_000:
        raise ConfigurationError(
            "interview.realtime.heartbeat_interval_ms must be between 1000 and 120000"
        )
    ice_urls = _unique_string_array(
        mapping.get("ice_urls"),
        f"{label}.ice_urls",
        minimum=0,
        maximum=20,
    )
    ice_hostnames = tuple(_require_interview_ice_url(url) for url in ice_urls)
    turn_shared_secret = _optional_secret(
        mapping.get("turn_shared_secret"),
        f"{label}.turn_shared_secret",
    )
    if (
        turn_shared_secret is not None
        and len(turn_shared_secret.encode("utf-8")) < 32
    ):
        raise ConfigurationError(
            "interview.realtime.turn_shared_secret must contain at least 32 bytes"
        )
    has_turn_url = any(url.startswith(("turn:", "turns:")) for url in ice_urls)
    if has_turn_url != (turn_shared_secret is not None):
        raise ConfigurationError(
            "interview.realtime.turn_shared_secret must be configured exactly when TURN URLs are present"
        )
    active_signing_key = (
        next(
            item.key for item in keyring.keys if item.key_id == keyring.active_key_id
        )
        if keyring.active_key_id is not None
        else None
    )
    if (
        turn_shared_secret is not None
        and active_signing_key is not None
        and _text_secret_reuses_key_material(
            turn_shared_secret,
            (active_signing_key,),
        )
    ):
        raise ConfigurationError(
            "interview.realtime.turn_shared_secret must not reuse the realtime signing key"
        )
    deployed = environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS
    has_active_key = keyring.active_key_id is not None
    if has_active_key != (signaling_url is not None):
        raise ConfigurationError(
            "interview.realtime signaling_url and active signing key must be configured together"
        )
    if deployed and (not has_active_key or signaling_url is None):
        raise ConfigurationError(
            "interview.realtime signing keyring and signaling_url are required outside "
            "development/test"
        )
    if ice_urls and signaling_url is None:
        raise ConfigurationError(
            "interview.realtime.ice_urls require a configured realtime endpoint"
        )
    if deployed and signaling_hostname is not None and _is_development_endpoint_host(
        signaling_hostname
    ):
        raise ConfigurationError(
            "interview.realtime.signaling_url cannot use a development placeholder host"
        )
    if deployed and any(_is_development_endpoint_host(hostname) for hostname in ice_hostnames):
        raise ConfigurationError(
            "interview.realtime.ice_urls cannot use development placeholder hosts"
        )
    return InterviewRealtimeSettings(
        keyring,
        signaling_url,
        transports,
        credential_ttl_seconds,
        heartbeat_interval_ms,
        ice_urls,
        turn_shared_secret,
    )


def _hmac256_keyring_settings(value: object, label: str) -> Hmac256KeyringSettings:
    """@brief 解析一组可轮换 HMAC-256 keys / Parse one rotatable HMAC-256 keyring.

    @param value keyring 对象 / Keyring mapping.
    @param label 安全错误路径 / Safe error path.
    @return 解码后 HMAC keyring / Decoded HMAC keyring.
    @raise ConfigurationError key ID、材料或 active 指针非法 / Invalid key IDs, material,
        or active pointer.
    """

    active_key_id, materials = _decoded_256_keyring(value, label)
    return Hmac256KeyringSettings(
        active_key_id,
        tuple(Hmac256KeySettings(key_id, key) for key_id, key in materials),
    )


def _require_interview_signaling_url(
    value: str,
    *,
    allow_development_endpoint: bool,
) -> str:
    """@brief 校验并返回 signaling hostname / Validate and return the signaling hostname.

    @param value signaling URL / Signaling URL.
    @return 小写无尾点 hostname / Lowercase hostname without a trailing dot.
    @raise ConfigurationError URL 不是精确无 credential 的 HTTPS/WSS endpoint / URL is not an
        exact credential-free HTTPS/WSS endpoint.
    """

    label = "interview.realtime.signaling_url"
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as error:
        raise ConfigurationError(f"{label} must be an exact HTTPS/WSS URL") from error
    if (
        not value.isascii()
        or any(character.isspace() for character in value)
        or parsed.scheme not in {"http", "https", "ws", "wss"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "%" in parsed.hostname
    ):
        raise ConfigurationError(
            f"{label} must be an exact credential-free HTTPS/WSS URL"
        )
    hostname = parsed.hostname.rstrip(".").lower()
    development_endpoint = (
        parsed.scheme in {"http", "ws"}
        and hostname == "dev.hmalliances.org"
        and parsed.port == 9000
    )
    if parsed.scheme in {"http", "ws"} and (
        not allow_development_endpoint or not development_endpoint
    ):
        raise ConfigurationError(
            f"{label} must use HTTPS/WSS except for the contract development endpoint"
        )
    return hostname


def _require_interview_ice_url(value: str) -> str:
    """@brief 校验并返回 STUN/TURN hostname / Validate and return a STUN/TURN hostname.

    @param value RFC 7064/7065 形式 ICE URI / RFC 7064/7065-style ICE URI.
    @return 小写无尾点 hostname / Lowercase hostname without a trailing dot.
    @raise ConfigurationError scheme、authority、port 或 transport query 非法 / Invalid scheme,
        authority, port, or transport query.
    """

    label = "interview.realtime.ice_urls"
    scheme, separator, remainder = value.partition(":")
    if (
        separator != ":"
        or scheme not in {"stun", "stuns", "turn", "turns"}
        or not value.isascii()
        or any(character.isspace() for character in value)
    ):
        raise ConfigurationError(f"{label} contains an invalid STUN/TURN URI")
    try:
        parsed = urlsplit(f"//{remainder}")
        _ = parsed.port
    except ValueError as error:
        raise ConfigurationError(f"{label} contains an invalid STUN/TURN URI") from error
    valid_query = (
        not parsed.query
        if scheme in {"stun", "stuns"}
        else parsed.query in {"", "transport=udp", "transport=tcp"}
    )
    if (
        not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.fragment
        or not valid_query
        or "%" in parsed.hostname
    ):
        raise ConfigurationError(f"{label} contains an invalid STUN/TURN URI")
    return parsed.hostname.rstrip(".").lower()


def _is_development_endpoint_host(hostname: str) -> bool:
    """@brief 识别不得出现在部署环境的占位 hostname / Identify placeholder hosts forbidden in deployed environments.

    @param hostname 已规范化 hostname / Canonicalized hostname.
    @return 保留测试后缀、localhost 或非法服务 IP 时为真 / True for reserved
        test suffixes, localhost, or non-service IP addresses.
    """

    reserved_domains = (
        "localhost",
        "local",
        "test",
        "example",
        "invalid",
        "example.com",
        "example.net",
        "example.org",
    )
    if any(hostname == suffix or hostname.endswith(f".{suffix}") for suffix in reserved_domains):
        return True
    try:
        literal = ip_address(hostname)
    except ValueError:
        return False
    return (
        literal.is_loopback
        or literal.is_unspecified
        or literal.is_link_local
        or literal.is_multicast
        or literal.is_reserved
    )


def _knowledge_settings(
    mapping: dict[str, Any],
    *,
    environment: str,
    database_mode: DatabaseMode,
) -> KnowledgeSettings:
    """@brief 解析 Knowledge V2 外部端口配置 / Parse Knowledge V2 external-port configuration.

    @param mapping ``knowledge`` 配置对象 / ``knowledge`` configuration mapping.
    @param environment 部署环境 / Deployment environment.
    @param database_mode 持久化模式 / Persistence mode.
    @return 完整且跨节一致的 Knowledge 设置 / Complete, cross-section-consistent settings.
    @raise ConfigurationError 未知字段、越界预算或不安全部署组合 / Unknown fields,
        out-of-bound budgets, or unsafe deployment combinations.
    """

    _reject_unknown_keys(
        mapping,
        {"connections", "uploads", "source_network", "worker", "search", "index"},
        "knowledge",
    )
    connections = _knowledge_connection_settings(
        mapping.get("connections"),
        environment=environment,
        database_mode=database_mode,
    )
    uploads = _knowledge_upload_settings(
        mapping.get("uploads"),
        environment=environment,
        database_mode=database_mode,
    )
    source_network = _knowledge_source_network_settings(
        mapping.get("source_network"),
        environment=environment,
    )
    worker = _knowledge_worker_settings(mapping.get("worker"))
    search = _knowledge_search_settings(mapping.get("search"))
    index = _knowledge_index_settings(mapping.get("index"))
    if uploads.maximum_object_bytes > worker.maximum_material_bytes:
        raise ConfigurationError(
            "knowledge.uploads.maximum_object_bytes must not exceed worker.maximum_material_bytes"
        )
    if source_network.maximum_body_bytes > worker.maximum_material_bytes:
        raise ConfigurationError(
            "knowledge.source_network.maximum_body_bytes must not exceed "
            "worker.maximum_material_bytes"
        )
    _reject_knowledge_key_reuse(connections, uploads)
    return KnowledgeSettings(connections, uploads, source_network, worker, search, index)


def _knowledge_connection_settings(
    value: object,
    *,
    environment: str,
    database_mode: DatabaseMode,
) -> KnowledgeConnectionSettings:
    """@brief 解析显式 Connection registry 与独立密钥域 / Parse the explicit Connection registry and independent key domains.

    @param value ``knowledge.connections`` 对象 / Configuration object.
    @param environment 部署环境 / Deployment environment.
    @param database_mode 数据库模式 / Database mode.
    @return 已验证 Connection 设置 / Validated Connection settings.
    @raise ConfigurationError provider、endpoint、keyring 或预算非法 / Invalid providers,
        endpoints, keyrings, or budgets.
    """

    mapping = require_mapping(value, "knowledge.connections")
    _reject_unknown_keys(
        mapping,
        {
            "provider_session_keyring",
            "credential_keyring",
            "credential_fingerprint_hmac_key",
            "credential_reference_hmac_key",
            "orphan_grace_seconds",
            "connect_timeout_ms",
            "read_timeout_ms",
            "providers",
        },
        "knowledge.connections",
    )
    provider_session_keyring = _aes256_keyring_settings(
        mapping.get("provider_session_keyring"),
        "knowledge.connections.provider_session_keyring",
    )
    credential_keyring = _aes256_keyring_settings(
        mapping.get("credential_keyring"),
        "knowledge.connections.credential_keyring",
    )
    fingerprint_key = _base64url_256_key(
        mapping.get("credential_fingerprint_hmac_key"),
        "knowledge.connections.credential_fingerprint_hmac_key",
        nullable=True,
    )
    reference_key = _base64url_256_key(
        mapping.get("credential_reference_hmac_key"),
        "knowledge.connections.credential_reference_hmac_key",
        nullable=True,
    )
    orphan_grace_seconds = _require_positive_int(mapping, "orphan_grace_seconds")
    connect_timeout_ms = _require_positive_int(mapping, "connect_timeout_ms")
    read_timeout_ms = _require_positive_int(mapping, "read_timeout_ms")
    if not 300 <= orphan_grace_seconds <= 604_800:
        raise ConfigurationError(
            "knowledge.connections.orphan_grace_seconds must be between 300 and 604800"
        )
    if connect_timeout_ms > 60_000 or read_timeout_ms > 120_000:
        raise ConfigurationError("knowledge.connections timeouts exceed adapter hard caps")
    raw_providers = mapping.get("providers")
    if not isinstance(raw_providers, list) or len(raw_providers) > 100:
        raise ConfigurationError("knowledge.connections.providers must be an array of at most 100")
    providers = tuple(
        _knowledge_connection_provider(item, index)
        for index, item in enumerate(raw_providers)
    )
    provider_names = [provider.provider for provider in providers]
    if len(provider_names) != len(set(provider_names)):
        raise ConfigurationError("knowledge.connections.providers contains a duplicate provider")
    if database_mode == "memory" and providers:
        raise ConfigurationError(
            "knowledge.connections.providers must be empty in memory database mode"
        )
    requires_durable_keys = (
        bool(providers) or environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS
    )
    if requires_durable_keys and (
        provider_session_keyring.active_key_id is None
        or not provider_session_keyring.keys
        or credential_keyring.active_key_id is None
        or not credential_keyring.keys
        or fingerprint_key is None
        or reference_key is None
    ):
        raise ConfigurationError(
            "knowledge.connections durable AES and HMAC keys are required in PostgreSQL "
            "or deployed modes"
        )
    return KnowledgeConnectionSettings(
        provider_session_keyring,
        credential_keyring,
        fingerprint_key,
        reference_key,
        orphan_grace_seconds,
        connect_timeout_ms,
        read_timeout_ms,
        providers,
    )


def _aes256_keyring_settings(value: object, label: str) -> Aes256KeyringSettings:
    """@brief 解析一组可轮换 AES-256 keys / Parse one rotatable AES-256 keyring.

    @param value keyring 对象 / Keyring mapping.
    @param label 安全错误路径 / Safe error path.
    @return 解码后的 keyring / Decoded keyring.
    @raise ConfigurationError key ID、材料或 active 指针非法 / Invalid key IDs, material,
        or active pointer.
    """

    active_key_id, materials = _decoded_256_keyring(value, label)
    return Aes256KeyringSettings(
        active_key_id,
        tuple(Aes256KeySettings(key_id, key) for key_id, key in materials),
    )


def _decoded_256_keyring(
    value: object,
    label: str,
) -> tuple[str | None, tuple[tuple[str, bytes], ...]]:
    """@brief 解码 AES/HMAC 共用的 256-bit keyring shape / Decode the shared 256-bit AES/HMAC keyring shape.

    @param value keyring 对象 / Keyring mapping.
    @param label 安全错误路径 / Safe error path.
    @return active key ID 与保序 ``(key_id, key)`` tuple / Active key ID and ordered
        ``(key_id, key)`` tuples.
    @raise ConfigurationError key ID、材料或 active 指针非法 / Invalid key IDs, material,
        or active pointer.
    """

    mapping = require_mapping(value, label)
    _reject_unknown_keys(mapping, {"active_key_id", "keys"}, label)
    active_key_id = _optional_string(mapping.get("active_key_id"))
    if active_key_id is not None and (
        not 3 <= len(active_key_id) <= 63 or not _valid_key_id(active_key_id)
    ):
        raise ConfigurationError(f"{label}.active_key_id must be portable ASCII")
    raw_keys = require_mapping(mapping.get("keys"), f"{label}.keys")
    materials: list[tuple[str, bytes]] = []
    for key_id, raw_key in raw_keys.items():
        if not 3 <= len(key_id) <= 63 or not _valid_key_id(key_id):
            raise ConfigurationError(f"{label}.keys contains an invalid key ID")
        key = _base64url_256_key(raw_key, f"{label}.keys.{key_id}", nullable=False)
        materials.append((key_id, cast(bytes, key)))
    if len({key for _, key in materials}) != len(materials):
        raise ConfigurationError(f"{label}.keys must not reuse key material")
    if active_key_id is None and materials:
        raise ConfigurationError(f"{label}.active_key_id is required when keys are configured")
    if active_key_id is not None and active_key_id not in raw_keys:
        raise ConfigurationError(f"{label}.active_key_id is absent from keys")
    return active_key_id, tuple(materials)


def _knowledge_connection_provider(
    value: object,
    index: int,
) -> KnowledgeConnectionProviderSettings:
    """@brief 解析一个闭合 provider 能力项 / Parse one closed provider-capability entry.

    @param value provider 对象 / Provider object.
    @param index 数组下标 / Array index.
    @return 已验证 provider 设置 / Validated provider settings.
    @raise ConfigurationError provider shape、scope 或 endpoint 非法 / Invalid shape,
        scopes, or endpoints.
    """

    label = f"knowledge.connections.providers[{index}]"
    mapping = require_mapping(value, label)
    _reject_unknown_keys(
        mapping,
        {
            "provider",
            "client_id",
            "authorization_endpoint",
            "token_endpoint",
            "device_authorization_endpoint",
            "redirect_uri",
            "allowed_scopes",
            "api_token_validation",
            "revocation_endpoint",
        },
        label,
    )
    provider = _require_string(mapping, "provider")
    if _CONNECTION_PROVIDER_PATTERN.fullmatch(provider) is None:
        raise ConfigurationError(f"{label}.provider is invalid")
    client_id = _require_string(mapping, "client_id")
    if len(client_id) > 512 or client_id.strip() != client_id or "\x00" in client_id:
        raise ConfigurationError(f"{label}.client_id is invalid")
    authorization_endpoint = _named_optional_string(mapping, "authorization_endpoint", label)
    token_endpoint = _named_optional_string(mapping, "token_endpoint", label)
    device_endpoint = _named_optional_string(mapping, "device_authorization_endpoint", label)
    redirect_uri = _named_optional_string(mapping, "redirect_uri", label)
    revocation_endpoint = _named_optional_string(mapping, "revocation_endpoint", label)
    for endpoint, name in (
        (authorization_endpoint, "authorization_endpoint"),
        (token_endpoint, "token_endpoint"),
        (device_endpoint, "device_authorization_endpoint"),
        (revocation_endpoint, "revocation_endpoint"),
    ):
        if endpoint is not None:
            _require_exact_https_url(endpoint, f"{label}.{name}")
    if authorization_endpoint is not None and (token_endpoint is None or redirect_uri is None):
        raise ConfigurationError(
            f"{label} browser OAuth requires token_endpoint and redirect_uri"
        )
    if redirect_uri is not None:
        _require_exact_https_url(redirect_uri, f"{label}.redirect_uri")
    if device_endpoint is not None and token_endpoint is None:
        raise ConfigurationError(f"{label} device flow requires token_endpoint")
    scopes = _unique_string_array(
        mapping.get("allowed_scopes"),
        f"{label}.allowed_scopes",
        minimum=1,
        maximum=100,
    )
    if any(
        len(scope) > 200 or any(character not in _OAUTH_SCOPE_CHARS for character in scope)
        for scope in scopes
    ):
        raise ConfigurationError(f"{label}.allowed_scopes contains an invalid scope")
    api_validation = _connection_api_token_validation(
        mapping.get("api_token_validation"),
        f"{label}.api_token_validation",
    )
    return KnowledgeConnectionProviderSettings(
        provider,
        client_id,
        authorization_endpoint,
        token_endpoint,
        device_endpoint,
        redirect_uri,
        scopes,
        api_validation,
        revocation_endpoint,
    )


def _connection_api_token_validation(
    value: object,
    label: str,
) -> ConnectionApiTokenValidationSettings | None:
    """@brief 解析可选 API-token 在线验证规则 / Parse optional API-token online validation.

    @param value 配置对象或 null / Configuration object or null.
    @param label 安全错误路径 / Safe error path.
    @return 验证设置或 None / Validation settings or ``None``.
    @raise ConfigurationError endpoint、method 或响应字段非法 / Invalid endpoint, method,
        or response field.
    """

    if value is None:
        return None
    mapping = require_mapping(value, label)
    _reject_unknown_keys(
        mapping,
        {"endpoint", "method", "authorization_scheme", "scopes_field"},
        label,
    )
    endpoint = _require_string(mapping, "endpoint")
    _require_exact_https_url(endpoint, f"{label}.endpoint")
    method = cast(
        ConnectionValidationMethod,
        _require_choice(mapping, "method", {"GET", "POST"}),
    )
    authorization_scheme = _require_string(mapping, "authorization_scheme")
    if (
        not authorization_scheme.isascii()
        or any(character.isspace() for character in authorization_scheme)
        or len(authorization_scheme) > 100
    ):
        raise ConfigurationError(f"{label}.authorization_scheme is invalid")
    scopes_field = _require_string(mapping, "scopes_field")
    if (
        len(scopes_field) > 100
        or not scopes_field.isascii()
        or not scopes_field.replace("_", "").isalnum()
    ):
        raise ConfigurationError(f"{label}.scopes_field is invalid")
    return ConnectionApiTokenValidationSettings(
        endpoint,
        method,
        authorization_scheme,
        scopes_field,
    )


def _knowledge_upload_settings(
    value: object,
    *,
    environment: str,
    database_mode: DatabaseMode,
) -> KnowledgeUploadSettings:
    """@brief 解析上传存储、quota、archive 与 scanner 边界 / Parse upload storage, quota, archive, and scanner bounds.

    @param value ``knowledge.uploads`` 对象 / Configuration object.
    @param environment 部署环境 / Deployment environment.
    @param database_mode 数据库模式 / Database mode.
    @return 已验证上传设置 / Validated upload settings.
    @raise ConfigurationError adapter 组合或预算非法 / Invalid adapter combination or budget.
    """

    mapping = require_mapping(value, "knowledge.uploads")
    _reject_unknown_keys(
        mapping,
        {
            "storage",
            "malware",
            "maximum_workspace_bytes",
            "maximum_object_bytes",
            "maximum_archive_entries",
            "maximum_archive_depth",
            "maximum_expanded_bytes",
            "maximum_inflation_ratio",
            "maximum_scanner_chunk_bytes",
            "erasure_batch_size",
        },
        "knowledge.uploads",
    )
    storage = _knowledge_upload_storage(
        mapping.get("storage"),
        environment=environment,
        database_mode=database_mode,
    )
    malware = _knowledge_malware_settings(mapping.get("malware"), environment=environment)
    maximum_workspace_bytes = _bounded_positive_int(
        mapping,
        "maximum_workspace_bytes",
        maximum=10 * 1024**4,
        label="knowledge.uploads",
    )
    maximum_object_bytes = _bounded_positive_int(
        mapping,
        "maximum_object_bytes",
        maximum=1024**3,
        label="knowledge.uploads",
    )
    maximum_archive_entries = _bounded_positive_int(
        mapping,
        "maximum_archive_entries",
        maximum=100_000,
        label="knowledge.uploads",
    )
    maximum_archive_depth = _require_non_negative_int(mapping, "maximum_archive_depth")
    maximum_expanded_bytes = _bounded_positive_int(
        mapping,
        "maximum_expanded_bytes",
        maximum=10 * 1024**3,
        label="knowledge.uploads",
    )
    maximum_inflation_ratio = _require_positive_number(
        mapping,
        "maximum_inflation_ratio",
        label="knowledge.uploads",
    )
    maximum_scanner_chunk_bytes = _bounded_positive_int(
        mapping,
        "maximum_scanner_chunk_bytes",
        maximum=16 * 1024**2,
        label="knowledge.uploads",
    )
    erasure_batch_size = _bounded_positive_int(
        mapping,
        "erasure_batch_size",
        maximum=1_000,
        label="knowledge.uploads",
    )
    if maximum_workspace_bytes < maximum_object_bytes:
        raise ConfigurationError(
            "knowledge.uploads.maximum_workspace_bytes must cover one maximum object"
        )
    if not maximum_object_bytes <= maximum_expanded_bytes:
        raise ConfigurationError(
            "knowledge.uploads.maximum_expanded_bytes must not be smaller than maximum_object_bytes"
        )
    if maximum_archive_depth > 10:
        raise ConfigurationError("knowledge.uploads.maximum_archive_depth must not exceed 10")
    if not 1.0 <= maximum_inflation_ratio <= 1_000.0:
        raise ConfigurationError(
            "knowledge.uploads.maximum_inflation_ratio must be between 1 and 1000"
        )
    if maximum_scanner_chunk_bytes < 4_096:
        raise ConfigurationError(
            "knowledge.uploads.maximum_scanner_chunk_bytes must be at least 4096"
        )
    return KnowledgeUploadSettings(
        storage,
        malware,
        maximum_workspace_bytes,
        maximum_object_bytes,
        maximum_archive_entries,
        maximum_archive_depth,
        maximum_expanded_bytes,
        maximum_inflation_ratio,
        maximum_scanner_chunk_bytes,
        erasure_batch_size,
    )


def _knowledge_upload_storage(
    value: object,
    *,
    environment: str,
    database_mode: DatabaseMode,
) -> KnowledgeUploadStorageSettings:
    """@brief 解析 local/S3 判别联合 / Parse the local/S3 discriminated union.

    @param value ``knowledge.uploads.storage`` 对象 / Configuration object.
    @param environment 部署环境 / Deployment environment.
    @param database_mode 数据库模式 / Database mode.
    @return 模式对应的强类型存储设置 / Mode-specific typed storage settings.
    @raise ConfigurationError inactive adapter 非 null 或生产选择 local / Non-null inactive
        adapter or local storage selected outside development/test.
    """

    mapping = require_mapping(value, "knowledge.uploads.storage")
    _reject_unknown_keys(mapping, {"mode", "local", "s3"}, "knowledge.uploads.storage")
    mode = cast(
        KnowledgeUploadMode,
        _require_choice(mapping, "mode", {"local", "s3"}),
    )
    if mode == "local":
        if mapping.get("s3") is not None:
            raise ConfigurationError("knowledge.uploads.storage.s3 must be null in local mode")
        if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS:
            raise ConfigurationError(
                "knowledge.uploads.storage.mode must be s3 outside development/test"
            )
        local = require_mapping(mapping.get("local"), "knowledge.uploads.storage.local")
        _reject_unknown_keys(
            local,
            {"directory", "public_origin", "signing_hmac_key"},
            "knowledge.uploads.storage.local",
        )
        public_origin = _require_string(local, "public_origin")
        _require_local_upload_origin(public_origin)
        signing_key = _base64url_256_key(
            local.get("signing_hmac_key"),
            "knowledge.uploads.storage.local.signing_hmac_key",
            nullable=True,
        )
        if database_mode == "postgresql" and signing_key is None:
            raise ConfigurationError(
                "knowledge.uploads.storage.local.signing_hmac_key is required in PostgreSQL mode"
            )
        return KnowledgeLocalUploadStorageSettings(
            "local",
            Path(_require_string(local, "directory")),
            public_origin,
            signing_key,
        )
    if mapping.get("local") is not None:
        raise ConfigurationError("knowledge.uploads.storage.local must be null in s3 mode")
    s3 = require_mapping(mapping.get("s3"), "knowledge.uploads.storage.s3")
    _reject_unknown_keys(
        s3,
        {
            "endpoint",
            "region",
            "bucket",
            "access_key_id",
            "secret_access_key",
            "session_token",
            "object_prefix",
            "connect_timeout_ms",
            "read_timeout_ms",
        },
        "knowledge.uploads.storage.s3",
    )
    endpoint = _require_string(s3, "endpoint")
    _require_exact_https_url(endpoint, "knowledge.uploads.storage.s3.endpoint")
    access_key_id = _direct_optional_secret(
        s3,
        "access_key_id",
        "knowledge.uploads.storage.s3.access_key_id",
    )
    secret_access_key = _direct_optional_secret(
        s3,
        "secret_access_key",
        "knowledge.uploads.storage.s3.secret_access_key",
    )
    session_token = _direct_optional_secret(
        s3,
        "session_token",
        "knowledge.uploads.storage.s3.session_token",
    )
    if access_key_id is None or secret_access_key is None:
        raise ConfigurationError(
            "knowledge.uploads.storage.s3 access_key_id and secret_access_key are required"
        )
    object_prefix = _require_string(s3, "object_prefix")
    prefix = PurePosixPath(object_prefix)
    if prefix.is_absolute() or ".." in prefix.parts or not prefix.parts:
        raise ConfigurationError("knowledge.uploads.storage.s3.object_prefix is invalid")
    connect_timeout_ms = _require_positive_int(s3, "connect_timeout_ms")
    read_timeout_ms = _require_positive_int(s3, "read_timeout_ms")
    if connect_timeout_ms > 60_000 or read_timeout_ms > 300_000:
        raise ConfigurationError("knowledge.uploads.storage.s3 timeouts exceed adapter hard caps")
    return KnowledgeS3UploadStorageSettings(
        "s3",
        endpoint,
        _require_string(s3, "region"),
        _require_string(s3, "bucket"),
        access_key_id,
        secret_access_key,
        session_token,
        object_prefix,
        connect_timeout_ms,
        read_timeout_ms,
    )


def _knowledge_malware_settings(
    value: object,
    *,
    environment: str,
) -> KnowledgeMalwareSettings:
    """@brief 解析 dev/ClamAV/reject scanner / Parse the dev, ClamAV, or rejecting scanner.

    @param value ``knowledge.uploads.malware`` 对象 / Configuration object.
    @param environment 部署环境 / Deployment environment.
    @return fail-closed scanner 设置 / Fail-closed scanner settings.
    @raise ConfigurationError 部署环境选择 dev 或 clamd endpoint 非法 / Dev mode in a
        deployed environment or invalid clamd endpoint.
    """

    mapping = require_mapping(value, "knowledge.uploads.malware")
    _reject_unknown_keys(mapping, {"mode", "clamav"}, "knowledge.uploads.malware")
    mode = cast(
        KnowledgeMalwareMode,
        _require_choice(mapping, "mode", {"dev", "clamav", "reject"}),
    )
    if mode != "clamav":
        if mapping.get("clamav") is not None:
            raise ConfigurationError(
                "knowledge.uploads.malware.clamav must be null unless mode is clamav"
            )
        if mode == "dev" and environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS:
            raise ConfigurationError(
                "knowledge.uploads.malware.mode cannot be dev outside development/test"
            )
        return KnowledgeMalwareSettings(mode, None, 3310, 3_000, 30_000)
    clamav = require_mapping(mapping.get("clamav"), "knowledge.uploads.malware.clamav")
    _reject_unknown_keys(
        clamav,
        {"host", "port", "connect_timeout_ms", "read_timeout_ms"},
        "knowledge.uploads.malware.clamav",
    )
    host = _require_string(clamav, "host")
    port = _require_positive_int(clamav, "port")
    connect_timeout_ms = _require_positive_int(clamav, "connect_timeout_ms")
    read_timeout_ms = _require_positive_int(clamav, "read_timeout_ms")
    if host.strip() != host or len(host) > 253 or "\x00" in host or port > 65_535:
        raise ConfigurationError("knowledge.uploads.malware.clamav host or port is invalid")
    if connect_timeout_ms > 60_000 or read_timeout_ms > 300_000:
        raise ConfigurationError("knowledge.uploads.malware.clamav timeouts exceed hard caps")
    return KnowledgeMalwareSettings(
        "clamav",
        host,
        port,
        connect_timeout_ms,
        read_timeout_ms,
    )


def _knowledge_source_network_settings(
    value: object,
    *,
    environment: str,
) -> KnowledgeSourceNetworkSettings:
    """@brief 解析 URL scheme/port/host 与 redirect 的同一 allowlist / Parse one URL scheme, port, host, and redirect allowlist.

    @param value ``knowledge.source_network`` 对象 / Configuration object.
    @param environment 部署环境 / Deployment environment.
    @return 已验证 SSRF policy 与 fetch 预算 / Validated SSRF policy and fetch budgets.
    @raise ConfigurationError allowlist 开放、重复或预算越界 / Open-ended or duplicate
        allowlists, or out-of-bound budgets.
    """

    mapping = require_mapping(value, "knowledge.source_network")
    _reject_unknown_keys(
        mapping,
        {
            "allowed_schemes",
            "allowed_ports",
            "allowed_host_patterns",
            "maximum_redirects",
            "allow_https_downgrade",
            "maximum_body_bytes",
            "connect_timeout_ms",
            "read_timeout_ms",
        },
        "knowledge.source_network",
    )
    schemes = _unique_string_array(
        mapping.get("allowed_schemes"),
        "knowledge.source_network.allowed_schemes",
        minimum=1,
        maximum=2,
    )
    if not set(schemes) <= {"http", "https"}:
        raise ConfigurationError(
            "knowledge.source_network.allowed_schemes must contain only http/https"
        )
    if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS and "http" in schemes:
        raise ConfigurationError(
            "knowledge.source_network.allowed_schemes cannot include http outside development/test"
        )
    raw_ports = mapping.get("allowed_ports")
    if (
        not isinstance(raw_ports, list)
        or not raw_ports
        or len(raw_ports) > 64
        or any(
            isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65_535
            for port in raw_ports
        )
        or len(set(raw_ports)) != len(raw_ports)
    ):
        raise ConfigurationError(
            "knowledge.source_network.allowed_ports must be a unique non-empty port array"
        )
    patterns = _unique_string_array(
        mapping.get("allowed_host_patterns"),
        "knowledge.source_network.allowed_host_patterns",
        minimum=1,
        maximum=1_000,
    )
    canonical_patterns = tuple(_canonical_source_host_pattern(item) for item in patterns)
    if len(set(canonical_patterns)) != len(canonical_patterns):
        raise ConfigurationError(
            "knowledge.source_network.allowed_host_patterns contains canonical duplicates"
        )
    maximum_redirects = _require_non_negative_int(mapping, "maximum_redirects")
    if maximum_redirects > 20:
        raise ConfigurationError(
            "knowledge.source_network.maximum_redirects must not exceed 20"
        )
    maximum_body_bytes = _bounded_positive_int(
        mapping,
        "maximum_body_bytes",
        maximum=1024**3,
        label="knowledge.source_network",
    )
    connect_timeout_ms = _require_positive_int(mapping, "connect_timeout_ms")
    read_timeout_ms = _require_positive_int(mapping, "read_timeout_ms")
    if connect_timeout_ms > 60_000 or read_timeout_ms > 300_000:
        raise ConfigurationError("knowledge.source_network timeouts exceed adapter hard caps")
    allow_downgrade = _require_bool(mapping, "allow_https_downgrade")
    if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS and allow_downgrade:
        raise ConfigurationError(
            "knowledge.source_network.allow_https_downgrade must be false outside development/test"
        )
    return KnowledgeSourceNetworkSettings(
        schemes,
        tuple(raw_ports),
        canonical_patterns,
        maximum_redirects,
        allow_downgrade,
        maximum_body_bytes,
        connect_timeout_ms,
        read_timeout_ms,
    )


def _knowledge_worker_settings(value: object) -> KnowledgeWorkerSettings:
    """@brief 解析 Knowledge worker 硬预算 / Parse Knowledge-worker hard budgets.

    @param value ``knowledge.worker`` 对象 / Configuration object.
    @return 已验证 retry/material 设置 / Validated retry/material settings.
    @raise ConfigurationError 参数越界 / Out-of-bound parameters.
    """

    mapping = require_mapping(value, "knowledge.worker")
    _reject_unknown_keys(
        mapping,
        {"maximum_attempts", "maximum_material_bytes"},
        "knowledge.worker",
    )
    return KnowledgeWorkerSettings(
        _bounded_positive_int(
            mapping,
            "maximum_attempts",
            maximum=100,
            label="knowledge.worker",
        ),
        _bounded_positive_int(
            mapping,
            "maximum_material_bytes",
            maximum=1024**3,
            label="knowledge.worker",
        ),
    )


def _knowledge_search_settings(value: object) -> KnowledgeSearchSettings:
    """@brief 解析 hybrid search 融合参数 / Parse hybrid-search fusion parameters.

    @param value ``knowledge.search`` 对象 / Configuration object.
    @return 已验证正权重与候选倍数 / Validated positive weights and candidate multiplier.
    @raise ConfigurationError 权重非有限数或倍数越界 / Non-finite weights or out-of-bound multiplier.
    """

    mapping = require_mapping(value, "knowledge.search")
    _reject_unknown_keys(
        mapping,
        {"lexical_weight", "semantic_weight", "candidate_multiplier"},
        "knowledge.search",
    )
    lexical_weight = _require_positive_number(
        mapping,
        "lexical_weight",
        label="knowledge.search",
    )
    semantic_weight = _require_positive_number(
        mapping,
        "semantic_weight",
        label="knowledge.search",
    )
    if lexical_weight > 100.0 or semantic_weight > 100.0:
        raise ConfigurationError("knowledge.search weights must not exceed 100")
    candidate_multiplier = _require_positive_int(mapping, "candidate_multiplier")
    if not 2 <= candidate_multiplier <= 20:
        raise ConfigurationError(
            "knowledge.search.candidate_multiplier must be between 2 and 20"
        )
    return KnowledgeSearchSettings(
        lexical_weight,
        semantic_weight,
        candidate_multiplier,
    )


def _knowledge_index_settings(value: object) -> KnowledgeIndexSettings:
    """@brief 解析 parse/chunk/embed pipeline 参数 / Parse parse/chunk/embed pipeline parameters.

    @param value ``knowledge.index`` 对象 / Configuration object.
    @return 已验证 index 预算 / Validated index budgets.
    @raise ConfigurationError 任一边界与 pipeline 构造器不一致 / Any bound inconsistent
        with the pipeline constructor.
    """

    mapping = require_mapping(value, "knowledge.index")
    _reject_unknown_keys(
        mapping,
        {
            "maximum_extracted_characters",
            "maximum_chunks",
            "chunk_max_characters",
            "chunk_overlap_characters",
            "embedding_batch_size",
        },
        "knowledge.index",
    )
    maximum_extracted_characters = _bounded_positive_int(
        mapping,
        "maximum_extracted_characters",
        maximum=10_000_000,
        label="knowledge.index",
    )
    maximum_chunks = _bounded_positive_int(
        mapping,
        "maximum_chunks",
        maximum=100_000,
        label="knowledge.index",
    )
    chunk_max_characters = _require_positive_int(mapping, "chunk_max_characters")
    chunk_overlap_characters = _require_non_negative_int(
        mapping,
        "chunk_overlap_characters",
    )
    embedding_batch_size = _bounded_positive_int(
        mapping,
        "embedding_batch_size",
        maximum=512,
        label="knowledge.index",
    )
    if not 100 <= chunk_max_characters <= 50_000:
        raise ConfigurationError(
            "knowledge.index.chunk_max_characters must be between 100 and 50000"
        )
    if chunk_overlap_characters >= chunk_max_characters:
        raise ConfigurationError(
            "knowledge.index.chunk_overlap_characters must be smaller than chunk_max_characters"
        )
    return KnowledgeIndexSettings(
        maximum_extracted_characters,
        maximum_chunks,
        chunk_max_characters,
        chunk_overlap_characters,
        embedding_batch_size,
    )


def _reject_knowledge_key_reuse(
    connections: KnowledgeConnectionSettings,
    uploads: KnowledgeUploadSettings,
) -> None:
    """@brief 拒绝 Knowledge 加密与 HMAC 跨用途复用 / Reject cross-purpose reuse of Knowledge encryption and HMAC keys.

    @param connections Connection 密钥域 / Connection key domains.
    @param uploads upload signing 密钥域 / Upload-signing key domain.
    @return 无返回值 / No return value.
    @raise ConfigurationError 任意两个用途的 256-bit key 相同 / Any two purposes use the
        same 256-bit key.
    """

    materials = _knowledge_key_materials(connections, uploads)
    if len(set(materials)) != len(materials):
        raise ConfigurationError(
            "knowledge AES encryption and HMAC signing keys must be distinct across purposes"
        )


def _knowledge_key_materials(
    connections: KnowledgeConnectionSettings,
    uploads: KnowledgeUploadSettings,
) -> tuple[bytes, ...]:
    """@brief 汇总 Knowledge 256-bit 对称密钥而不派生指纹 / Collect Knowledge 256-bit symmetric keys without deriving fingerprints.

    @param connections Connection 密钥域 / Connection key domains.
    @param uploads upload signing 密钥域 / Upload-signing key domain.
    @return 仅供启动时等值检查的 key tuple / Key tuple used only for startup equality checks.
    """

    materials = [
        *(item.key for item in connections.provider_session_keyring.keys),
        *(item.key for item in connections.credential_keyring.keys),
    ]
    for key in (
        connections.credential_fingerprint_hmac_key,
        connections.credential_reference_hmac_key,
    ):
        if key is not None:
            materials.append(key)
    storage = uploads.storage
    if (
        isinstance(storage, KnowledgeLocalUploadStorageSettings)
        and storage.signing_hmac_key is not None
    ):
        materials.append(storage.signing_hmac_key)
    return tuple(materials)


def _reject_cross_feature_key_reuse(
    email: IdentityEmailOutboxSettings,
    knowledge: KnowledgeSettings,
    interview: InterviewSettings,
    security: SecuritySettings,
) -> None:
    """@brief 拒绝跨 feature 对称 key 复用 / Reject cross-feature symmetric-key reuse.

    @param email Identity email key domains / Identity-email key domains.
    @param knowledge Knowledge key domains / Knowledge key domains.
    @param interview Interview realtime HMAC key domain / Interview realtime HMAC key domain.
    @param security 通用安全 HMAC domains / General security HMAC domains.
    @return 无返回值 / No return value.
    @raise ConfigurationError 任意 feature 边界复用 key material / Key material is reused across
        feature boundaries.
    """

    email_materials = [item.key for item in email.encryption_keys]
    if email.rate_limit_hmac_key is not None:
        email_materials.append(email.rate_limit_hmac_key)
    knowledge_materials = _knowledge_key_materials(
        knowledge.connections,
        knowledge.uploads,
    )
    if set(email_materials).intersection(knowledge_materials):
        raise ConfigurationError(
            "identity-email and Knowledge encryption/HMAC keys must be distinct"
        )
    interview_materials = tuple(item.key for item in interview.realtime.signing_keyring.keys)
    if set(interview_materials).intersection(email_materials):
        raise ConfigurationError(
            "Interview realtime and identity-email HMAC/encryption keys must be distinct"
        )
    if set(interview_materials).intersection(knowledge_materials):
        raise ConfigurationError(
            "Interview realtime and Knowledge HMAC/encryption keys must be distinct"
        )
    turn_secret = interview.realtime.turn_shared_secret
    if turn_secret is not None and _text_secret_reuses_key_material(
        turn_secret,
        (*email_materials, *knowledge_materials, *interview_materials),
    ):
        raise ConfigurationError(
            "Interview TURN and other feature encryption/HMAC keys must be distinct"
        )
    idempotency_secret = security.sensitive_idempotency_hmac_secret
    if idempotency_secret is not None and _text_secret_reuses_key_material(
        idempotency_secret,
        (*email_materials, *knowledge_materials, *interview_materials),
    ):
        raise ConfigurationError(
            "sensitive-idempotency and feature encryption/HMAC keys must be distinct"
        )
    if (
        idempotency_secret is not None
        and turn_secret is not None
        and hmac.compare_digest(idempotency_secret, turn_secret)
    ):
        raise ConfigurationError(
            "sensitive-idempotency and Interview TURN secrets must be distinct"
        )


def _text_secret_reuses_key_material(
    secret: str,
    materials: tuple[bytes, ...],
) -> bool:
    """@brief 检测文本 HMAC secret 与原始/base64url key 复用 / Detect raw/base64url reuse by a textual HMAC secret.

    @param secret 直接 UTF-8 HMAC secret / Direct UTF-8 HMAC secret.
    @param materials 已解码的 256-bit key 材料 / Decoded 256-bit key materials.
    @return secret 与原始 bytes 或规范 base64url 表示相同时为真 / True when the secret
        equals either raw bytes or their canonical base64url representation.
    """

    encoded_secret = secret.encode("utf-8")
    return any(
        encoded_secret == material
        or secret
        == base64.urlsafe_b64encode(material).rstrip(b"=").decode("ascii")
        for material in materials
    )


def _bounded_positive_int(
    mapping: dict[str, Any],
    key: str,
    *,
    maximum: int,
    label: str,
) -> int:
    """@brief 读取不可由配置抬高的正整数 / Read a positive integer with a code-level cap.

    @param mapping 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param maximum 硬上限 / Hard maximum.
    @param label 完整配置路径 / Full configuration path.
    @return 有界正整数 / Bounded positive integer.
    @raise ConfigurationError 值超过硬上限 / Value exceeds the hard maximum.
    """

    value = _require_positive_int(mapping, key)
    if value > maximum:
        raise ConfigurationError(f"{label}.{key} must not exceed {maximum}")
    return value


def _require_positive_number(
    mapping: dict[str, Any],
    key: str,
    *,
    label: str,
) -> float:
    """@brief 读取有限正数 / Read a finite positive number.

    @param mapping 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param label 完整配置路径 / Full configuration path.
    @return ``float`` 正数 / Positive ``float``.
    @raise ConfigurationError bool、非数、无穷或非正值 / Boolean, non-number, infinity,
        or non-positive value.
    """

    value = mapping.get(key)
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ConfigurationError(f"{label}.{key} must be a finite positive number")
    return float(value)


def _unique_string_array(
    value: object,
    label: str,
    *,
    minimum: int,
    maximum: int,
) -> tuple[str, ...]:
    """@brief 读取有界无重复字符串数组 / Read a bounded duplicate-free string array.

    @param value 原始数组 / Raw array.
    @param label 完整配置路径 / Full configuration path.
    @param minimum 最小元素数 / Minimum item count.
    @param maximum 最大元素数 / Maximum item count.
    @return 保持顺序的字符串 tuple / Ordered string tuple.
    @raise ConfigurationError shape、空白或重复非法 / Invalid shape, whitespace, or duplicates.
    """

    if (
        not isinstance(value, list)
        or not minimum <= len(value) <= maximum
        or any(
            not isinstance(item, str)
            or not item
            or item.strip() != item
            or "\x00" in item
            for item in value
        )
    ):
        raise ConfigurationError(
            f"{label} must contain between {minimum} and {maximum} non-empty strings"
        )
    strings = cast(list[str], value)
    if len(set(strings)) != len(strings):
        raise ConfigurationError(f"{label} contains duplicates")
    return tuple(strings)


def _named_optional_string(mapping: dict[str, Any], key: str, label: str) -> str | None:
    """@brief 读取显式可空字符串并保留完整路径诊断 / Read an explicit nullable string with a full-path diagnostic.

    @param mapping 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param label 父路径 / Parent path.
    @return 非空字符串或 None / Non-empty string or ``None``.
    @raise ConfigurationError 字段缺失、为空或类型错误 / Missing, empty, or wrong-typed field.
    """

    if key not in mapping:
        raise ConfigurationError(f"configuration key {label}.{key!s} is required")
    value = mapping[key]
    if value is None:
        return None
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ConfigurationError(f"{label}.{key} must be a non-empty string or null")
    return value


def _require_exact_https_url(value: str, label: str) -> None:
    """@brief 校验固定无 credential/query/fragment HTTPS URL / Validate a fixed HTTPS URL without credentials, query, or fragment.

    @param value URL 文本 / URL text.
    @param label 配置路径 / Configuration path.
    @return 无返回值 / No return value.
    @raise ConfigurationError URL 不是 exact HTTPS endpoint / URL is not an exact HTTPS endpoint.
    """

    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as error:
        raise ConfigurationError(f"{label} must be an exact HTTPS URL") from error
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ConfigurationError(f"{label} must be an exact credential-free HTTPS URL")


def _require_local_upload_origin(value: str) -> None:
    """@brief 校验 HTTPS 或隔离开发 origin / Validate an HTTPS or isolated development origin.

    @param value origin 文本 / Origin text.
    @return 无返回值 / No return value.
    @raise ConfigurationError origin 携带 path/credential/query 或协议非法 / Origin contains
        a path, credential, query, or invalid scheme.
    """

    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as error:
        raise ConfigurationError(
            "knowledge.uploads.storage.local.public_origin is invalid"
        ) from error
    exact_origin = f"{parsed.scheme}://{parsed.netloc}"
    if (
        not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or (
            parsed.scheme != "https"
            and exact_origin != "http://dev.hmalliances.org:9000"
        )
    ):
        raise ConfigurationError(
            "knowledge.uploads.storage.local.public_origin must be HTTPS or the isolated dev origin"
        )


def _canonical_source_host_pattern(value: str) -> str:
    """@brief 规范化 exact/``*.`` 来源 hostname pattern / Canonicalize an exact or ``*.`` source-host pattern.

    @param value 原始 pattern / Raw pattern.
    @return 无尾点小写 IDNA pattern / Lowercase IDNA pattern without a trailing dot.
    @raise ConfigurationError pattern 为空、开放或 label 非法 / Empty, open-ended, or invalid pattern.
    """

    wildcard = value.startswith("*.")
    hostname = value[2:] if wildcard else value
    candidate = hostname.rstrip(".").lower()
    if not candidate or "*" in candidate or "%" in candidate or len(candidate) > 253:
        raise ConfigurationError(
            "knowledge.source_network.allowed_host_patterns contains an invalid pattern"
        )
    try:
        literal = ip_address(candidate)
    except ValueError:
        try:
            ascii_host = candidate.encode("idna").decode("ascii")
        except UnicodeError as error:
            raise ConfigurationError(
                "knowledge.source_network.allowed_host_patterns contains invalid IDNA"
            ) from error
    else:
        if wildcard:
            raise ConfigurationError(
                "knowledge.source_network wildcard patterns cannot target IP literals"
            )
        return literal.compressed
    labels = ascii_host.split(".")
    if any(
        not label
        or len(label) > 63
        or label.startswith("-")
        or label.endswith("-")
        for label in labels
    ):
        raise ConfigurationError(
            "knowledge.source_network.allowed_host_patterns contains an invalid hostname"
        )
    if wildcard and len(labels) < 2:
        raise ConfigurationError(
            "knowledge.source_network wildcard patterns require a registrable-style suffix"
        )
    return f"*.{ascii_host}" if wildcard else ascii_host


def _provider_rate_limit_settings(mapping: dict[str, Any]) -> ProviderRateLimitSettings:
    """@brief 读取模型 provider 的受限准入预算 / Read bounded model-provider admission budgets.

    @param mapping ``ai.provider_rate_limit`` 配置对象。
    @return 已验证的 provider 级并发、滑动窗口和等待超时设置。
    @raise ConfigurationError 任一预算不是正整数时抛出。
    """
    return ProviderRateLimitSettings(
        max_concurrent_requests=_require_positive_int(mapping, "max_concurrent_requests"),
        requests_per_minute=_require_positive_int(mapping, "requests_per_minute"),
        acquire_timeout_ms=_require_positive_int(mapping, "acquire_timeout_ms"),
    )


def _metering_settings(mapping: dict[str, Any]) -> MeteringSettings:
    """@brief 读取本地 token 成本估算单价 / Read local token-cost estimate prices.

    @param mapping ``ai.metering`` 配置对象。
    @return 以每百万 token micro-USD 表示的输入/输出估算费率。
    @raise ConfigurationError 费率不是非负整数时抛出。

    @note 零费率是明确、有效的保守默认值：系统仍持久化 token 用量与 ``0`` 成本，
    但不会编造供应商价格。
    """
    return MeteringSettings(
        input_cost_microusd_per_million_tokens=_require_non_negative_int(
            mapping, "input_cost_microusd_per_million_tokens"
        ),
        output_cost_microusd_per_million_tokens=_require_non_negative_int(
            mapping, "output_cost_microusd_per_million_tokens"
        ),
    )


def _require_bool(mapping: dict[str, Any], key: str) -> bool:
    """@brief 读取布尔值 / Read a boolean.

    @param mapping 配置对象 / Configuration object.
    @param key 键名 / Key name.
    @return 布尔值 / Boolean value.
    """
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise ConfigurationError(f"configuration key {key!r} must be a boolean")
    return value


def _require_choice(mapping: dict[str, Any], key: str, choices: set[str]) -> str:
    """@brief 读取枚举式配置 / Read an enumerated configuration setting.

    @param mapping 配置对象 / Configuration object.
    @param key 键名 / Key name.
    @param choices 允许值 / Allowed values.
    @return 已校验值 / Validated value.
    """
    value = _require_string(mapping, key)
    if value not in choices:
        raise ConfigurationError(f"configuration key {key!r} must be one of {sorted(choices)!r}")
    return value


def _security_settings(
    value: object,
    environment: str,
    *,
    legacy_v1_enabled: bool,
) -> SecuritySettings:
    """@brief 解析身份边界配置并在生产环境 fail closed / Parse identity security settings and fail closed in production.

    @param value ``security`` 配置节或缺失值 / ``security`` configuration section or missing value.
    @param environment 当前部署环境标签 / Current deployment environment label.
    @param legacy_v1_enabled 是否显式挂载旧产品路由 / Whether legacy product routes are explicitly mounted.
    @return 已校验的身份安全设置 / Validated identity security settings.
    @raise ConfigurationError 安全节缺失、直接密钥无效，或 mock 身份被用于非开发环境时抛出 /
        Raised when security is missing, a direct secret is invalid, or mock identity is used
        outside development.

    @note 身份模式和直接密钥字段始终必须在唯一配置文件中显式出现。
    """
    mapping = require_mapping(value, "security")
    identity_mode = cast(
        IdentityMode,
        _require_choice(
            mapping,
            "identity_mode",
            {"disabled", "development_mock", "trusted_proxy_hmac"},
        ),
    )
    secret = _direct_optional_secret(
        mapping,
        "trusted_proxy_hmac_secret",
        "security.trusted_proxy_hmac_secret",
    )
    cursor_secret = _direct_optional_secret(
        mapping,
        "cursor_hmac_secret",
        "security.cursor_hmac_secret",
    )
    sensitive_idempotency_secret = _direct_optional_secret(
        mapping,
        "sensitive_idempotency_hmac_secret",
        "security.sensitive_idempotency_hmac_secret",
    )
    max_clock_skew_seconds = _require_positive_int(mapping, "trusted_proxy_max_clock_skew_seconds")
    if identity_mode == "trusted_proxy_hmac" and secret is None:
        raise ConfigurationError(
            "security.trusted_proxy_hmac_secret is required for trusted_proxy_hmac"
        )
    if identity_mode == "disabled" and secret is not None:
        raise ConfigurationError(
            "security.trusted_proxy_hmac_secret must be null when legacy identity is disabled"
        )
    if cursor_secret is not None and len(cursor_secret.encode("utf-8")) < 32:
        raise ConfigurationError("security.cursor_hmac_secret must contain at least 32 bytes")
    if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS and cursor_secret is None:
        raise ConfigurationError(
            "security.cursor_hmac_secret is required outside development/test"
        )
    if (
        sensitive_idempotency_secret is not None
        and len(sensitive_idempotency_secret.encode("utf-8")) < 32
    ):
        raise ConfigurationError(
            "security.sensitive_idempotency_hmac_secret must contain at least 32 bytes"
        )
    if (
        environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS
        and sensitive_idempotency_secret is None
    ):
        raise ConfigurationError(
            "security.sensitive_idempotency_hmac_secret is required outside development/test"
        )
    if (
        cursor_secret is not None
        and sensitive_idempotency_secret is not None
        and cursor_secret == sensitive_idempotency_secret
    ):
        raise ConfigurationError(
            "cursor and sensitive-idempotency HMAC secrets must be distinct"
        )
    if max_clock_skew_seconds > _MAX_TRUSTED_PROXY_CLOCK_SKEW_SECONDS:
        raise ConfigurationError(
            "security.trusted_proxy_max_clock_skew_seconds must not exceed "
            f"{_MAX_TRUSTED_PROXY_CLOCK_SKEW_SECONDS}"
        )
    if identity_mode == "development_mock" and environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS:
        raise ConfigurationError(
            "security.identity_mode development_mock is only allowed in development/test"
        )
    if legacy_v1_enabled and identity_mode == "disabled":
        raise ConfigurationError(
            "security.identity_mode must authenticate explicitly enabled API V1 routes"
        )
    if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS and identity_mode != "disabled":
        raise ConfigurationError(
            "security.identity_mode must be disabled outside development/test"
        )
    return SecuritySettings(
        identity_mode=identity_mode,
        trusted_proxy_hmac_secret=secret,
        cursor_hmac_secret=cursor_secret,
        sensitive_idempotency_hmac_secret=sensitive_idempotency_secret,
        trusted_proxy_max_clock_skew_seconds=max_clock_skew_seconds,
    )


def _oauth_settings(value: object, environment: str) -> OAuthSettings:
    """Parse registered public clients and fail closed outside development/test."""

    if value is None:
        if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS:
            raise ConfigurationError("oauth configuration is required outside development/test")
        return OAuthSettings(
            authorization_request_ttl_seconds=600,
            authorization_code_ttl_seconds=60,
            access_token_ttl_seconds=600,
            refresh_token_ttl_seconds=2_592_000,
            signing_private_key_paths=(Path("data/oauth-signing-key.pem"),),
            public_clients=(
                OAuthPublicClientSettings(
                    client_id="aiws-web-local",
                    client_type="web",
                    redirect_uris=(
                        "http://127.0.0.1:5173/oauth/callback",
                        "http://localhost:5173/oauth/callback",
                    ),
                    allowed_scopes=_DEFAULT_OAUTH_SCOPES,
                ),
                OAuthPublicClientSettings(
                    client_id="aiws-electron-local",
                    client_type="electron",
                    redirect_uris=("http://127.0.0.1/oauth/callback",),
                    allowed_scopes=_DEFAULT_OAUTH_SCOPES,
                ),
            ),
        )

    mapping = require_mapping(value, "oauth")
    ttl_seconds = _require_positive_int(mapping, "authorization_request_ttl_seconds")
    if ttl_seconds > 900:
        raise ConfigurationError("oauth.authorization_request_ttl_seconds must not exceed 900")
    raw_clients = mapping.get("public_clients")
    if not isinstance(raw_clients, list) or not raw_clients:
        raise ConfigurationError("oauth.public_clients must be a non-empty array")
    clients = tuple(
        _oauth_public_client(item, index, environment) for index, item in enumerate(raw_clients)
    )
    client_ids = [client.client_id for client in clients]
    if len(client_ids) != len(set(client_ids)):
        raise ConfigurationError("oauth.public_clients contains a duplicate client_id")
    code_ttl_seconds = _require_positive_int(mapping, "authorization_code_ttl_seconds")
    access_ttl_seconds = _require_positive_int(mapping, "access_token_ttl_seconds")
    refresh_ttl_seconds = _require_positive_int(mapping, "refresh_token_ttl_seconds")
    if code_ttl_seconds > 300:
        raise ConfigurationError("oauth.authorization_code_ttl_seconds must not exceed 300")
    if access_ttl_seconds > 900:
        raise ConfigurationError("oauth.access_token_ttl_seconds must not exceed 900")
    if refresh_ttl_seconds > 31_536_000:
        raise ConfigurationError("oauth.refresh_token_ttl_seconds must not exceed 31536000")
    raw_key_paths = mapping.get("signing_private_key_paths")
    if not isinstance(raw_key_paths, list) or not raw_key_paths:
        raise ConfigurationError("oauth.signing_private_key_paths must be a non-empty string array")
    key_paths: list[Path] = []
    for raw_key_path in raw_key_paths:
        if not isinstance(raw_key_path, str) or not raw_key_path.strip():
            raise ConfigurationError(
                "oauth.signing_private_key_paths must be a non-empty string array"
            )
        key_path = Path(raw_key_path)
        if key_path in key_paths:
            raise ConfigurationError("oauth.signing_private_key_paths contains a duplicate path")
        key_paths.append(key_path)
    return OAuthSettings(
        ttl_seconds,
        code_ttl_seconds,
        access_ttl_seconds,
        refresh_ttl_seconds,
        tuple(key_paths),
        clients,
    )


def _development_identity_email_outbox() -> IdentityEmailOutboxSettings:
    """@brief 返回无持久化 development/test 默认值 / Return secret-free development/test defaults.

    @return 仅内存邮件模式可使用的设置 / Settings usable only with the in-memory email mode.
    """

    return IdentityEmailOutboxSettings(
        active_key_id=None,
        encryption_keys=(),
        rate_limit_hmac_key=None,
        poll_interval_ms=1_000,
        batch_size=50,
        lease_seconds=60,
        retry_base_seconds=5,
        retry_cap_seconds=3_600,
        max_attempts=8,
        retention_days=30,
    )


def _identity_email_outbox_settings(value: object) -> IdentityEmailOutboxSettings:
    """@brief 解析并验证 durable identity-email outbox / Parse and validate the durable identity-email outbox.

    @param value ``hosted_identity.email.outbox`` 对象 / Outbox configuration object.
    @return 强类型、已解码的 keyring 与 worker 边界 / Typed decoded keyring and worker limits.
    @raise ConfigurationError key material 或 worker 边界非法 / Invalid key material or worker limits.
    """

    mapping = require_mapping(value, "hosted_identity.email.outbox")
    _reject_unknown_keys(
        mapping,
        {
            "active_key_id",
            "encryption_keys",
            "rate_limit_hmac_key",
            "poll_interval_ms",
            "batch_size",
            "lease_seconds",
            "retry_base_seconds",
            "retry_cap_seconds",
            "max_attempts",
            "retention_days",
        },
        "hosted_identity.email.outbox",
    )
    active_key_id = _optional_string(mapping.get("active_key_id"))
    if active_key_id is not None and not _valid_key_id(active_key_id):
        raise ConfigurationError(
            "hosted_identity.email.outbox.active_key_id must be portable ASCII"
        )
    raw_keys = require_mapping(
        mapping.get("encryption_keys"),
        "hosted_identity.email.outbox.encryption_keys",
    )
    keys: list[Aes256KeySettings] = []
    for key_id, raw_key in raw_keys.items():
        if not _valid_key_id(key_id):
            raise ConfigurationError(
                "hosted_identity.email.outbox.encryption_keys contains an invalid key ID"
            )
        keys.append(
            Aes256KeySettings(
                key_id,
                cast(
                    bytes,
                    _base64url_256_key(
                        raw_key,
                        f"hosted_identity.email.outbox.encryption_keys.{key_id}",
                        nullable=False,
                    ),
                ),
            )
        )
    if len({item.key for item in keys}) != len(keys):
        raise ConfigurationError(
            "hosted_identity.email.outbox.encryption_keys must not reuse key material"
        )
    if active_key_id is not None and active_key_id not in raw_keys:
        raise ConfigurationError(
            "hosted_identity.email.outbox.active_key_id is absent from encryption_keys"
        )
    rate_limit_key = _base64url_256_key(
        mapping.get("rate_limit_hmac_key"),
        "hosted_identity.email.outbox.rate_limit_hmac_key",
        nullable=True,
    )
    if rate_limit_key is not None and any(rate_limit_key == item.key for item in keys):
        raise ConfigurationError(
            "hosted_identity email encryption and rate-limit HMAC keys must be distinct"
        )
    poll_interval_ms = _require_positive_int(mapping, "poll_interval_ms")
    batch_size = _require_positive_int(mapping, "batch_size")
    lease_seconds = _require_positive_int(mapping, "lease_seconds")
    retry_base_seconds = _require_positive_int(mapping, "retry_base_seconds")
    retry_cap_seconds = _require_positive_int(mapping, "retry_cap_seconds")
    max_attempts = _require_positive_int(mapping, "max_attempts")
    retention_days = _require_positive_int(mapping, "retention_days")
    if poll_interval_ms > 60_000:
        raise ConfigurationError(
            "hosted_identity.email.outbox.poll_interval_ms must not exceed 60000"
        )
    if batch_size > 1_000 or max_attempts > 100:
        raise ConfigurationError(
            "hosted_identity.email.outbox batch_size or max_attempts exceeds its hard cap"
        )
    if lease_seconds > 3_600:
        raise ConfigurationError(
            "hosted_identity.email.outbox.lease_seconds must not exceed 3600"
        )
    if retry_base_seconds > retry_cap_seconds or retry_cap_seconds > 86_400:
        raise ConfigurationError(
            "hosted_identity.email.outbox retry delays must be ordered and capped at one day"
        )
    if retention_days > 365:
        raise ConfigurationError(
            "hosted_identity.email.outbox.retention_days must not exceed 365"
        )
    return IdentityEmailOutboxSettings(
        active_key_id=active_key_id,
        encryption_keys=tuple(keys),
        rate_limit_hmac_key=rate_limit_key,
        poll_interval_ms=poll_interval_ms,
        batch_size=batch_size,
        lease_seconds=lease_seconds,
        retry_base_seconds=retry_base_seconds,
        retry_cap_seconds=retry_cap_seconds,
        max_attempts=max_attempts,
        retention_days=retention_days,
    )


def _base64url_256_key(value: object, name: str, *, nullable: bool) -> bytes | None:
    """@brief 解码规范 base64url 256-bit key / Decode a canonical base64url 256-bit key.

    @param value 直接配置的 base64url 字符串或允许的 null / Direct base64url string or allowed null.
    @param name 不泄露值的错误路径 / Error path that never reveals the value.
    @param nullable 是否允许 null / Whether null is allowed.
    @return 32-byte key，或允许时的 None / 32-byte key or allowed ``None``.
    @raise ConfigurationError 值缺失、编码非法或长度错误 / Missing, malformed, or wrong-length value.
    """

    if value is None:
        if nullable:
            return None
        raise ConfigurationError(f"configuration key {name!r} is required")
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{name} must be a base64url-encoded 256-bit key")
    padded = value + "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, binascii.Error) as error:
        raise ConfigurationError(f"{name} must be a base64url-encoded 256-bit key") from error
    if len(decoded) != 32:
        raise ConfigurationError(f"{name} must decode to exactly 256 bits")
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if value != canonical:
        raise ConfigurationError(f"{name} must use canonical unpadded base64url encoding")
    return decoded


def _valid_key_id(value: str) -> bool:
    """@brief 校验可持久化 key ID / Validate a persistable key identifier.

    @param value 待持久化标识 / Identifier to persist.
    @return 仅 portable ASCII 子集且不超过 64 字符时为真 / True for the portable subset up to 64 chars.
    """

    return bool(value) and len(value) <= 64 and all(
        character.isascii() and (character.isalnum() or character in "._-")
        for character in value
    )


def _validate_deployed_identity_email_outbox(
    settings: IdentityEmailOutboxSettings,
    environment: str,
) -> None:
    """@brief 部署环境缺少 durable encryption key 时 fail closed / Fail closed without durable encryption keys.

    @param settings 已完成结构校验的 outbox 设置 / Structurally validated outbox settings.
    @param environment 部署环境 / Deployment environment.
    @return 无返回值 / No return value.
    @raise ConfigurationError staging/production 未配置完整 key domains / Missing deployed key domains.
    """

    if environment in _DEVELOPMENT_IDENTITY_ENVIRONMENTS:
        return
    if (
        settings.active_key_id is None
        or not settings.encryption_keys
        or settings.rate_limit_hmac_key is None
    ):
        raise ConfigurationError(
            "hosted_identity.email.outbox encryption and rate-limit keys are required "
            "outside development/test"
        )


def _hosted_identity_settings(value: object, environment: str) -> HostedIdentitySettings:
    """Parse hosted identity limits and require real email delivery in deployed environments."""

    if value is None:
        if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS:
            raise ConfigurationError(
                "hosted_identity configuration is required outside development/test"
            )
        return HostedIdentitySettings(
            flow_ttl_seconds=600,
            email_code_ttl_seconds=600,
            email_code_max_attempts=5,
            email_send_limit_per_hour=5,
            session_idle_ttl_seconds=1_800,
            session_absolute_ttl_seconds=2_592_000,
            recent_reauthentication_seconds=300,
            email=IdentityEmailSettings("memory", None, None, 587, None, None),
            password_breach=PasswordBreachSettings("disabled", 3_000, 21_600, 4_096),
        )
    mapping = require_mapping(value, "hosted_identity")
    flow_ttl = _require_positive_int(mapping, "flow_ttl_seconds")
    code_ttl = _require_positive_int(mapping, "email_code_ttl_seconds")
    attempts = _require_positive_int(mapping, "email_code_max_attempts")
    send_limit = _require_positive_int(mapping, "email_send_limit_per_hour")
    idle_ttl = _require_positive_int(mapping, "session_idle_ttl_seconds")
    absolute_ttl = _require_positive_int(mapping, "session_absolute_ttl_seconds")
    reauth_ttl = _require_positive_int(mapping, "recent_reauthentication_seconds")
    if flow_ttl > 900 or code_ttl > 600:
        raise ConfigurationError(
            "hosted_identity flow TTL must not exceed 900 and email code TTL must not exceed 600"
        )
    if attempts > 10 or send_limit > 20:
        raise ConfigurationError("hosted_identity credential limits are too permissive")
    if idle_ttl > absolute_ttl:
        raise ConfigurationError(
            "hosted_identity.session_idle_ttl_seconds must not exceed the absolute lifetime"
        )
    email_mapping = require_mapping(mapping.get("email"), "hosted_identity.email")
    outbox = _identity_email_outbox_settings(email_mapping.get("outbox"))
    mode = cast(
        IdentityEmailMode,
        _require_choice(email_mapping, "mode", {"memory", "smtp"}),
    )
    if mode == "memory":
        email = IdentityEmailSettings(
            "memory",
            None,
            None,
            587,
            None,
            None,
            outbox=outbox,
        )
    else:
        email = IdentityEmailSettings(
            mode="smtp",
            from_address=_require_string(email_mapping, "from_address"),
            smtp_host=_require_string(email_mapping, "smtp_host"),
            smtp_port=_require_positive_int(email_mapping, "smtp_port"),
            smtp_username=_optional_string(email_mapping.get("smtp_username")),
            smtp_password=_direct_optional_secret(
                email_mapping, "smtp_password", "hosted_identity.email.smtp_password"
            ),
            smtp_start_tls=_require_bool(email_mapping, "smtp_start_tls"),
            outbox=outbox,
        )
        if (email.smtp_username is None) != (email.smtp_password is None):
            raise ConfigurationError(
                "hosted_identity.email SMTP username and password must be configured together"
            )
    if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS and mode != "smtp":
        raise ConfigurationError(
            "hosted_identity.email.mode must be smtp outside development/test"
        )

    breach_mapping = require_mapping(
        mapping.get("password_breach"), "hosted_identity.password_breach"
    )
    breach_mode = cast(
        PasswordBreachMode,
        _require_choice(breach_mapping, "mode", {"disabled", "pwned_passwords"}),
    )
    breach_timeout = _require_positive_int(breach_mapping, "request_timeout_ms")
    breach_cache_ttl = _require_positive_int(breach_mapping, "cache_ttl_seconds")
    breach_cache_entries = _require_positive_int(breach_mapping, "max_cache_entries")
    if breach_timeout > 10_000:
        raise ConfigurationError(
            "hosted_identity.password_breach.request_timeout_ms must not exceed 10000"
        )
    if breach_cache_ttl > 86_400:
        raise ConfigurationError(
            "hosted_identity.password_breach.cache_ttl_seconds must not exceed 86400"
        )
    if breach_cache_entries > 65_536:
        raise ConfigurationError(
            "hosted_identity.password_breach.max_cache_entries must not exceed 65536"
        )
    if (
        environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS
        and breach_mode != "pwned_passwords"
    ):
        raise ConfigurationError(
            "hosted_identity.password_breach.mode must be pwned_passwords outside development/test"
        )
    return HostedIdentitySettings(
        flow_ttl,
        code_ttl,
        attempts,
        send_limit,
        idle_ttl,
        absolute_ttl,
        reauth_ttl,
        email,
        PasswordBreachSettings(
            breach_mode,
            breach_timeout,
            breach_cache_ttl,
            breach_cache_entries,
        ),
    )


def _oauth_public_client(
    value: object,
    index: int,
    environment: str,
) -> OAuthPublicClientSettings:
    """Parse one secretless Web or Electron OAuth client registration."""

    path = f"oauth.public_clients[{index}]"
    mapping = require_mapping(value, path)
    client_id = _require_string(mapping, "client_id")
    if len(client_id) > 128 or not all(
        character.isalnum() or character in "._~-" for character in client_id
    ):
        raise ConfigurationError(f"{path}.client_id must be URL-safe ASCII")
    client_type = cast(
        OAuthClientType,
        _require_choice(mapping, "client_type", {"web", "electron"}),
    )
    raw_redirects = mapping.get("redirect_uris")
    if not isinstance(raw_redirects, list) or not raw_redirects:
        raise ConfigurationError(f"{path}.redirect_uris must be a non-empty string array")
    redirects: list[str] = []
    for raw_redirect in raw_redirects:
        if not isinstance(raw_redirect, str):
            raise ConfigurationError(f"{path}.redirect_uris must be a non-empty string array")
        _validate_oauth_redirect_uri(raw_redirect, client_type, environment, path)
        redirects.append(raw_redirect)
    if len(redirects) != len(set(redirects)):
        raise ConfigurationError(f"{path}.redirect_uris contains a duplicate URI")
    raw_scopes = mapping.get("allowed_scopes")
    if not isinstance(raw_scopes, list) or not raw_scopes:
        raise ConfigurationError(f"{path}.allowed_scopes must be a non-empty string array")
    scopes: list[str] = []
    for raw_scope in raw_scopes:
        if (
            not isinstance(raw_scope, str)
            or not raw_scope
            or any(character not in _OAUTH_SCOPE_CHARS for character in raw_scope)
        ):
            raise ConfigurationError(f"{path}.allowed_scopes contains an invalid OAuth scope")
        scopes.append(raw_scope)
    if "openid" not in scopes:
        raise ConfigurationError(f"{path}.allowed_scopes must include openid")
    if len(scopes) != len(set(scopes)):
        raise ConfigurationError(f"{path}.allowed_scopes contains a duplicate scope")
    if not set(scopes).issubset(SUPPORTED_OAUTH_SCOPE_SET):
        raise ConfigurationError(
            f"{path}.allowed_scopes contains a scope outside the supported catalog"
        )
    return OAuthPublicClientSettings(client_id, client_type, tuple(redirects), tuple(scopes))


def _validate_oauth_redirect_uri(
    uri: str,
    client_type: OAuthClientType,
    environment: str,
    path: str,
) -> None:
    """Validate registered redirects without allowing wildcard hosts or fragments."""

    parsed = urlsplit(uri)
    if (
        not parsed.scheme
        or not parsed.hostname
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ConfigurationError(f"{path}.redirect_uris contains an invalid absolute URI")
    is_loopback = parsed.hostname in {"127.0.0.1", "::1"}
    if client_type == "electron" and parsed.scheme == "http":
        if not is_loopback or parsed.port is not None:
            raise ConfigurationError(
                f"{path}.redirect_uris Electron HTTP registration must use a loopback IP without a port"
            )
        return
    if parsed.scheme == "https":
        return
    if (
        environment in _DEVELOPMENT_IDENTITY_ENVIRONMENTS
        and client_type == "web"
        and parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost"}
    ):
        return
    raise ConfigurationError(f"{path}.redirect_uris must use HTTPS")


def _provider_endpoint(value: object, index: int) -> AIProviderEndpoint:
    """@brief 解析一个 fallback 模型端点 / Parse one fallback model endpoint.

    @param value ``ai.fallback_providers`` 中的候选对象。
    @param index 当前数组下标，仅用于安全的配置诊断。
    @return 已校验的 AIProviderEndpoint。
    @raise ConfigurationError fallback 端点形状或直接密钥非法时抛出。

    @note fallback 不从主 provider 隐式继承密钥或 URL，避免配置改动时将 API key
    静默发送到错误的服务商。
    """
    mapping = require_mapping(value, f"ai.fallback_providers[{index}]")
    endpoint = AIProviderEndpoint(
        provider=_require_string(mapping, "provider"),
        model=_require_string(mapping, "model"),
        api_key=_require_string(mapping, "api_key"),
        base_url=_require_string(mapping, "base_url"),
        data_region=_require_choice(mapping, "data_region", {"cn", "global", "private_deployment"}),
    )
    return endpoint


def _optional_secret(value: object, name: str) -> str | None:
    """Read a direct secret value without including it in validation errors."""
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{name} must be a non-empty string or null")
    return value


def _direct_optional_secret(
    mapping: dict[str, Any],
    key: str,
    name: str,
) -> str | None:
    """Require a direct secret field while allowing an explicit null for mock modes."""
    if key not in mapping:
        raise ConfigurationError(f"configuration key {name!r} is required")
    return _optional_secret(mapping[key], name)
