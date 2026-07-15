"""@brief 后端独立配置服务 / Backend-owned configuration service."""

from __future__ import annotations

import re
from dataclasses import dataclass
from ipaddress import IPv4Network, IPv6Network, ip_network
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from workspace_shared.jsonc import ConfigurationError, load_jsonc, require_mapping
from workspace_shared.tenancy import ActorScope

DatabaseMode = Literal["memory", "postgresql"]
"""@brief 数据库运行模式 / Database runtime modes."""

IdentityMode = Literal["development_mock", "trusted_proxy_hmac"]
"""@brief 身份解析模式 / Identity resolution modes."""

_DEVELOPMENT_IDENTITY_ENVIRONMENTS = frozenset({"development", "test"})
"""@brief 允许 development mock 的环境 / Environments allowed to use development mocks."""

_SUPPORTED_ENVIRONMENTS = frozenset({"development", "test", "staging", "production"})
"""@brief 唯一允许的部署环境标签 / Only supported deployment-environment labels."""

_DEFAULT_TRUSTED_PROXY_HMAC_SECRET_ENV = "AIWS_TRUSTED_PROXY_HMAC_SECRET"
"""@brief 默认可信代理 HMAC 密钥环境变量 / Default trusted-proxy HMAC secret environment variable."""

_DEFAULT_TRUSTED_PROXY_MAX_CLOCK_SKEW_SECONDS = 300
"""@brief 默认身份断言时钟偏差 / Default identity-assertion clock skew in seconds."""

_MAX_TRUSTED_PROXY_CLOCK_SKEW_SECONDS = 600
"""@brief 允许的最大身份断言时钟偏差 / Maximum permitted identity-assertion clock skew in seconds."""


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
    trusted_proxy_cidrs: tuple[IPv4Network | IPv6Network, ...]
    outbound_proxy_url: str | None
    connect_timeout_ms: int
    read_timeout_ms: int


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """@brief 后端数据库连接设置 / Backend database connection settings."""

    mode: DatabaseMode
    application_dsn_env: str
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
    api_key_env: str
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
    @param api_key_env 密钥所在环境变量名 / Environment variable containing the API key.
    @param base_url OpenAI-compatible API base URL / OpenAI-compatible API base URL.
    @param data_region 此端点实际处理数据的地域 / Region where this endpoint processes data.
    """

    provider: str
    model: str
    api_key_env: str
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


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    """@brief 结构化日志设置 / Structured logging settings."""

    level: str
    persist_structured_events: bool


@dataclass(frozen=True, slots=True)
class SecuritySettings:
    """@brief 身份边界安全设置 / Identity-boundary security settings.

    @note ``trusted_proxy_hmac_secret_env`` 只保存环境变量名称，永不保存或输出实际
    HMAC（Hash-based Message Authentication Code）密钥。
    """

    identity_mode: IdentityMode
    trusted_proxy_hmac_secret_env: str
    trusted_proxy_max_clock_skew_seconds: int


@dataclass(frozen=True, slots=True)
class BackendSettings:
    """@brief 后端组合根的完整设置 / Complete settings for the backend composition root."""

    environment: str
    default_scope: ActorScope
    network: NetworkSettings
    database: DatabaseSettings
    runtime: RuntimeSettings
    renderer: RendererSettings
    ai: AISettings
    observability: ObservabilitySettings
    logging: LoggingSettings
    security: SecuritySettings
    config_path: Path

    @classmethod
    def from_file(cls, path: Path) -> BackendSettings:
        """@brief 从统一 JSONC 配置构建后端设置 / Build backend settings from unified JSONC.

        @param path 根配置路径 / Root configuration path.
        @return 后端独有的强类型设置 / Backend-owned typed settings.
        @raise ConfigurationError 配置类型或约束不成立时抛出 / Raised for invalid configuration.
        """
        root = load_jsonc(path)
        environment = _require_string(root, "environment")
        if environment not in _SUPPORTED_ENVIRONMENTS:
            raise ConfigurationError("environment must be development, test, staging, or production")
        workspace = require_mapping(root.get("workspace"), "workspace")
        network = require_mapping(root.get("network"), "network")
        database = require_mapping(root.get("database"), "database")
        runtime = require_mapping(root.get("runtime"), "runtime")
        renderer = require_mapping(root.get("resume_rendering"), "resume_rendering")
        ai = require_mapping(root.get("ai"), "ai")
        observability = require_mapping(root.get("observability"), "observability")
        logging = require_mapping(root.get("logging"), "logging")
        security = _security_settings(root.get("security"), environment)
        database_mode = cast(DatabaseMode, _require_choice(database, "mode", {"memory", "postgresql"}))
        if environment in {"staging", "production"} and database_mode != "postgresql":
            raise ConfigurationError("database.mode must be postgresql in staging/production")
        renderer_adapter = cast(
            Literal["mock", "xelatex"], _require_choice(renderer, "adapter", {"mock", "xelatex"})
        )
        drop_policy = cast(
            Literal["drop_newest", "drop_oldest"],
            _require_choice(observability, "drop_policy", {"drop_newest", "drop_oldest"}),
        )
        font_directories = renderer.get("allowed_font_directories", [])
        fallback_providers = ai.get("fallback_providers", [])
        if not isinstance(font_directories, list) or not all(isinstance(item, str) for item in font_directories):
            raise ConfigurationError("resume_rendering.allowed_font_directories must be a string array")
        if not isinstance(fallback_providers, list):
            raise ConfigurationError("ai.fallback_providers must be an array")
        return cls(
            environment=environment,
            default_scope=ActorScope(
                actor_id=_require_string(workspace, "default_actor_id"),
                workspace_id=_require_string(workspace, "default_workspace_id"),
                resource_owner_id=_require_string(workspace, "default_resource_owner_id"),
            ),
            network=NetworkSettings(
                bind_host=_require_string(network, "bind_host"),
                bind_port=_require_positive_int(network, "bind_port"),
                public_base_url=_require_string(network, "public_base_url").rstrip("/"),
                trusted_proxy_cidrs=_require_trusted_proxy_cidrs(
                    network.get("trusted_proxy_cidrs")
                ),
                outbound_proxy_url=_optional_proxy_url(network.get("outbound_proxy_url")),
                connect_timeout_ms=_require_positive_int(network, "connect_timeout_ms"),
                read_timeout_ms=_require_positive_int(network, "read_timeout_ms"),
            ),
            database=DatabaseSettings(
                mode=database_mode,
                application_dsn_env=_require_string(database, "application_dsn_env"),
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
            ),
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
                provider=_require_string(ai, "provider"),
                model=_require_string(ai, "model"),
                api_key_env=_require_string(ai, "api_key_env"),
                base_url=_optional_string(ai.get("base_url")),
                data_region=_require_choice(
                    ai, "data_region", {"cn", "global", "private_deployment"}
                ),
                fallback_providers=tuple(
                    _provider_endpoint(item, index)
                    for index, item in enumerate(fallback_providers)
                ),
                embedding_provider=_require_string(ai, "embedding_provider"),
                embedding_model=_require_string(ai, "embedding_model"),
                embedding_model_revision=_require_string(ai, "embedding_model_revision"),
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
            ),
            logging=LoggingSettings(
                level=_require_string(logging, "level").upper(),
                persist_structured_events=_require_bool(logging, "persist_structured_events"),
            ),
            security=security,
            config_path=path,
        )


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
            raise ConfigurationError("network.trusted_proxy_cidrs contains an invalid CIDR") from error
    return tuple(networks)


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


def _security_settings(value: object, environment: str) -> SecuritySettings:
    """@brief 解析身份边界配置并在生产环境 fail closed / Parse identity security settings and fail closed in production.

    @param value ``security`` 配置节或缺失值 / ``security`` configuration section or missing value.
    @param environment 当前部署环境标签 / Current deployment environment label.
    @return 已校验的身份安全设置 / Validated identity security settings.
    @raise ConfigurationError 非开发环境缺失安全节，或 mock 身份被用于非开发环境时抛出 /
        Raised when security is missing outside development or mock identity is used outside development.

    @note 为兼容历史本地配置，只有 ``development``/``test`` 缺失该节时才补出受限
    mock 默认值；任何其它环境都必须显式选择可验证的 HMAC 模式。
    """
    if value is None:
        if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS:
            raise ConfigurationError("security section is required outside development/test")
        return SecuritySettings(
            identity_mode="development_mock",
            trusted_proxy_hmac_secret_env=_DEFAULT_TRUSTED_PROXY_HMAC_SECRET_ENV,
            trusted_proxy_max_clock_skew_seconds=_DEFAULT_TRUSTED_PROXY_MAX_CLOCK_SKEW_SECONDS,
        )

    mapping = require_mapping(value, "security")
    identity_mode = cast(
        IdentityMode,
        _require_choice(mapping, "identity_mode", {"development_mock", "trusted_proxy_hmac"}),
    )
    secret_env = _require_string(mapping, "trusted_proxy_hmac_secret_env")
    max_clock_skew_seconds = _require_positive_int(mapping, "trusted_proxy_max_clock_skew_seconds")
    if not _ENVIRONMENT_VARIABLE_PATTERN.fullmatch(secret_env):
        raise ConfigurationError("security.trusted_proxy_hmac_secret_env must be an environment variable name")
    if max_clock_skew_seconds > _MAX_TRUSTED_PROXY_CLOCK_SKEW_SECONDS:
        raise ConfigurationError(
            "security.trusted_proxy_max_clock_skew_seconds must not exceed "
            f"{_MAX_TRUSTED_PROXY_CLOCK_SKEW_SECONDS}"
        )
    if environment not in _DEVELOPMENT_IDENTITY_ENVIRONMENTS and identity_mode != "trusted_proxy_hmac":
        raise ConfigurationError("security.identity_mode must be trusted_proxy_hmac outside development/test")
    return SecuritySettings(
        identity_mode=identity_mode,
        trusted_proxy_hmac_secret_env=secret_env,
        trusted_proxy_max_clock_skew_seconds=max_clock_skew_seconds,
    )


_ENVIRONMENT_VARIABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief secret 环境变量名称模式 / Environment variable name pattern for secrets."""


def _provider_endpoint(value: object, index: int) -> AIProviderEndpoint:
    """@brief 解析一个 fallback 模型端点 / Parse one fallback model endpoint.

    @param value ``ai.fallback_providers`` 中的候选对象。
    @param index 当前数组下标，仅用于安全的配置诊断。
    @return 已校验的 AIProviderEndpoint。
    @raise ConfigurationError fallback 端点形状或环境变量名非法时抛出。

    @note fallback 不从主 provider 隐式继承密钥或 URL，避免配置改动时将 API key
    静默发送到错误的服务商。
    """
    mapping = require_mapping(value, f"ai.fallback_providers[{index}]")
    endpoint = AIProviderEndpoint(
        provider=_require_string(mapping, "provider"),
        model=_require_string(mapping, "model"),
        api_key_env=_require_string(mapping, "api_key_env"),
        base_url=_require_string(mapping, "base_url"),
        data_region=_require_choice(
            mapping, "data_region", {"cn", "global", "private_deployment"}
        ),
    )
    if not _ENVIRONMENT_VARIABLE_PATTERN.fullmatch(endpoint.api_key_env):
        raise ConfigurationError(
            f"ai.fallback_providers[{index}].api_key_env must be an environment variable name"
        )
    return endpoint
