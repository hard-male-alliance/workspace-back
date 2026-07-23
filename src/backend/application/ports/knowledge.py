"""@brief API v2 Connection、Upload 与 Knowledge 应用 Ports / API v2 Connection, Upload, and Knowledge ports.

这些端口刻意保留 PostgreSQL adapter 所需的 ``workspace_id``、旧 revision/generation、
``for_update``、稳定 keyset 和同事务 Job/outbox。任何 adapter 都不得先按全局 ID 读取再在
应用层过滤 Workspace。普通可重试写路由由现有 ``V2IdempotencyExecutor`` 在 HTTP/application
边界外层保存逐字响应；本 UoW 的原子状态、Job 与 outbox 让首次执行可确定提交。返回
``user_code`` 的 device authorization session 不得进入通用 receipt，必须使用 provider
授权事务自身的一次性状态。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self

from backend.application.ports.access import AccessAuthorizer
from backend.application.ports.v2_idempotency import IdempotencyPreparationId
from backend.domain.connections import (
    Connection,
    ConnectionAggregate,
    ConnectionAuthorizationFlow,
    ConnectionAuthorizationRecord,
    ConnectionAuthorizationSessionId,
    ConnectionId,
    ConnectionOwnership,
    ConnectionProvider,
    CredentialReference,
    ProviderSessionReference,
    SecretValue,
)
from backend.domain.knowledge_jobs import KnowledgeJobSpec, KnowledgeOutboxEvent
from backend.domain.knowledge_retrieval import (
    KnowledgeSearchHit,
    KnowledgeSearchPlan,
)
from backend.domain.knowledge_sources import (
    GitSourceInput,
    KnowledgeSource,
    KnowledgeSourceId,
    KnowledgeSourceVersion,
    KnowledgeSourceVersionId,
    ResumeId,
    UrlSourceInput,
)
from backend.domain.platform import Job
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.upload_sessions import (
    UploadCompletionClaim,
    UploadDeclaration,
    UploadGrant,
    UploadSession,
    UploadSessionId,
    VerifiedUpload,
)


class KnowledgeCasMismatch(RuntimeError):
    """@brief repository 条件写入没有精确影响一行 / Repository conditional write did not affect exactly one row."""


class UploadVerificationRejected(RuntimeError):
    """@brief 存储内容确定性未通过安全门禁 / Stored content deterministically failed a security gate."""


@dataclass(frozen=True, slots=True)
class KnowledgePageRequest:
    """@brief 绑定过滤条件后的内部 keyset 分页请求 / Internal keyset page request bound to filters.

    @param limit 最大返回项目数 / Maximum returned item count.
    @param after 上一页最后稳定位置 / Last stable position from the previous page.
    """

    limit: int = 50
    after: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验 v2 分页范围 / Validate v2 pagination bounds.

        @raise ValueError limit 或位置非法时抛出 / Raised for an invalid limit or position.
        """
        if isinstance(self.limit, bool) or not 1 <= self.limit <= 200:
            raise ValueError("knowledge page limit must be between one and 200")
        if self.after is not None and (not self.after or len(self.after) > 2_048):
            raise ValueError(
                "knowledge keyset position must be non-empty and at most 2048 characters"
            )


@dataclass(frozen=True, slots=True)
class KnowledgePage[ItemT]:
    """@brief 稳定排序的 keyset 页面 / Stably ordered keyset page.

    @param items 当前页项目 / Current page items.
    @param next_position 下一页内部位置 / Internal next-page position.
    """

    items: tuple[ItemT, ...]
    next_position: str | None


@dataclass(frozen=True, slots=True)
class ConnectionAuthorizationLaunch:
    """@brief provider adapter 返回的授权启动投影 / Authorization launch returned by a provider adapter.

    @param provider_session_reference provider 私有事务引用 / Private provider transaction reference.
    @param expires_at provider 截止时间 / Provider deadline.
    @param authorization_url browser redirect URL / Browser redirect URL.
    @param verification_uri device flow verification URI / Device-flow verification URI.
    @param user_code 用户可见 device code；普通 repr 隐藏 / User-visible device code, hidden from repr.
    @param poll_interval_ms device polling 间隔 / Device polling interval.
    """

    provider_session_reference: ProviderSessionReference = field(repr=False)
    expires_at: datetime
    authorization_url: str | None = field(default=None, repr=False)
    verification_uri: str | None = None
    user_code: str | None = field(default=None, repr=False)
    poll_interval_ms: int | None = None


@dataclass(frozen=True, slots=True)
class ProvisionedConnectionCredential:
    """@brief 已验证且只以 server reference 表示的 credential / Validated credential represented only by a server reference.

    @param reference credential vault 引用 / Credential-vault reference.
    @param scopes provider 实际授予 scopes / Provider-granted scopes.
    @param validated_at 最近验证时刻 / Validation instant.
    """

    reference: CredentialReference = field(repr=False)
    scopes: tuple[str, ...]
    validated_at: datetime


@dataclass(frozen=True, slots=True)
class HybridSearchResponse:
    """@brief 混合检索 adapter 的候选与 policy 水位 / Hybrid-search candidates and policy watermark.

    @param hits 候选 hits / Candidate hits.
    @param policy_version 执行查询使用的 policy snapshot 水位 / Policy-snapshot watermark used by the query.
    """

    hits: tuple[KnowledgeSearchHit, ...]
    policy_version: int

    def __post_init__(self) -> None:
        """@brief 校验 adapter 响应边界 / Validate adapter-response bounds.

        @raise ValueError hit 数量或 policy 水位非法时抛出 / Raised for invalid output.
        """
        if len(self.hits) > 200 or self.policy_version < 1:
            raise ValueError("hybrid search response is outside configured bounds")


@dataclass(frozen=True, slots=True)
class IssuedUploadGrant:
    """@brief 可跨重试重放的对象存储授权结果 / Replayable object-storage grant result.

    @param grant 签名 PUT 授权 / Signed PUT grant.
    @param issued_at provider 实际签发时刻 / Provider's actual issuance instant.
    @param expires_at URL 的实际失效时刻 / Actual URL expiry.
    """

    grant: UploadGrant = field(repr=False)
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验实际授权时间窗 / Validate the actual grant time window.

        @raise ValueError 时间无时区或非正向时抛出 / Raised for naive or unordered timestamps.
        """

        if (
            self.issued_at.tzinfo is None
            or self.issued_at.utcoffset() is None
            or self.expires_at.tzinfo is None
            or self.expires_at.utcoffset() is None
            or self.expires_at <= self.issued_at
        ):
            raise ValueError("issued upload grant requires an aware positive time window")


class ConnectionAuthorizationGateway(Protocol):
    """@brief 启动外部 OAuth/device flow 的安全 adapter / Secure adapter starting OAuth/device flow."""

    async def begin(
        self,
        ownership: ConnectionOwnership,
        provider: ConnectionProvider,
        flow: ConnectionAuthorizationFlow,
        requested_scopes: tuple[str, ...],
        state: SecretValue,
    ) -> ConnectionAuthorizationLaunch:
        """@brief 启动 provider flow 且私存 code/token / Start a provider flow while privately storing codes/tokens.

        @param ownership 精确 Workspace 与 actor / Exact Workspace and actor.
        @param provider provider / Provider.
        @param flow browser 或 device flow / Browser or device flow.
        @param requested_scopes 去重 scopes / Unique scopes.
        @param state 单 session 随机 OAuth state / Per-session random OAuth state.
        @return 只含客户端需要字段和私有 reference 的 launch / Client fields plus private reference.
        @note provider device_code、OAuth code 和 token 必须留在 adapter 的 secret store；
            返回的 ``user_code`` 不能写通用日志或通用幂等 receipt / Provider device codes,
            OAuth codes, and tokens remain in the secret store; user_code is excluded from generic logs
            and generic idempotency receipts.
        """


class ConnectionCredentialBroker(Protocol):
    """@brief 验证并暂存 Connection credential 的专用 secret port / Dedicated secret port validating and staging credentials."""

    async def provision_api_token(
        self,
        ownership: ConnectionOwnership,
        connection_id: ConnectionId,
        provider: ConnectionProvider,
        token: SecretValue,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> ProvisionedConnectionCredential:
        """@brief 验证 API token 并返回 server reference / Validate an API token and return a server reference.

        @param ownership Workspace 与创建者 / Workspace and creator.
        @param connection_id 待创建 Connection / Connection being created.
        @param provider provider / Provider.
        @param token 默认脱敏 token / Redacted-by-default token.
        @param operation_id provider-side 去重使用的稳定 ID / Stable provider-side deduplication ID.
        @return reference、scopes 与验证时刻 / Reference, scopes, and validation instant.
        @note 外部 secret 写入发生在数据库事务外，并以 ``operation_id`` 去重；实现必须令
            同一 ID 返回同一 reference，并由 orphan reconciliation 清理由 prepare 成功但最终
            commit 永久失败留下的未引用 secret。Connection 只保存 reference。/
            External secret writes occur outside database transactions and are deduplicated by
            ``operation_id``. Implementations return the same reference for the same ID and use
            orphan reconciliation for secrets left unreferenced after a permanently failed final
            commit. Connection persists only the reference.
        """


class UploadObjectStore(Protocol):
    """@brief 签发直传 URL 并可信验证对象内容的 Port / Port issuing direct URLs and verifying object content."""

    async def issue_upload_grant(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        *,
        expires_at: datetime,
        operation_id: IdempotencyPreparationId,
    ) -> IssuedUploadGrant:
        """@brief 为隔离对象 key 签发短期 PUT / Issue a short-lived PUT for an isolated object key.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param upload_id session 标识 / Session identifier.
        @param declaration 冻结声明 / Frozen declaration.
        @param expires_at 授权截止 / Grant deadline.
        @param operation_id 跨崩溃重试稳定的 provider 去重 ID / Crash-stable provider deduplication ID.
        @return 包含 provider 实际时间窗的直传授权 / Direct-upload grant with the provider's
            actual time window.
        """

    async def verify_uploaded_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        declaration: UploadDeclaration,
        claim: UploadCompletionClaim,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> VerifiedUpload:
        """@brief 流式重算并执行 MIME/malware/archive/quota 门禁 / Stream, hash, and run all upload gates.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param upload_id session 标识 / Session identifier.
        @param declaration 创建声明 / Creation declaration.
        @param claim completion 声明 / Completion claim.
        @param operation_id 扫描/配额预留的稳定去重 ID / Stable scan and quota-reservation deduplication ID.
        @return 全部门禁通过的证明 / Evidence that every gate passed.
        @raise UploadVerificationRejected MIME、malware、archive 或 quota 确定性拒绝时抛出 /
            Raised for deterministic MIME, malware, archive, or quota rejection.
        @note 实现不得信任客户端 Content-Type/ETag；必须 MIME sniff、恶意内容扫描、限制
            zip 条目/深度/膨胀率、检查实际 size/hash 并原子预留配额 / Implementations must
            not trust Content-Type or ETag and must sniff MIME, scan malware, bound archives, verify
            actual size/hash, and reserve quota atomically.
        """

    async def delete_object(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> None:
        """@brief 幂等擦除一个 Workspace 上传对象 / Idempotently erase one Workspace upload object.

        @param workspace_id 对象所属 Workspace / Owning Workspace.
        @param upload_id 不透明上传标识 / Opaque upload identifier.
        @note 对象不存在必须视为成功；不得通过客户端 URL 或 ETag 定位删除目标 / A missing
            object is success; implementations must not locate deletion targets through client URLs or ETags.
        """


class SourceNetworkGuard(Protocol):
    """@brief URL/Git 网络来源的 SSRF 与 allowlist 策略 Port / SSRF and allowlist policy port for URL/Git sources."""

    async def validate(self, source_input: UrlSourceInput | GitSourceInput) -> None:
        """@brief 在登记前验证初始目标 / Validate the initial target before registration.

        @param source_input URL 或 Git 输入 / URL or Git input.
        @note worker 在每次 DNS 解析、连接和 redirect 后仍须重验：禁止 loopback、私网、
            link-local、云 metadata、非 allowlist scheme/port/host 与 DNS rebinding / Workers
            must revalidate after every resolution, connect, and redirect, rejecting loopback,
            private, link-local, metadata, non-allowlisted targets, and DNS rebinding.
        """


class KnowledgeDependencyVerifier(Protocol):
    """@brief 校验跨 bounded-context 引用仍属于路径 Workspace / Verify cross-context references belong to the path Workspace."""

    async def resume_exists(
        self,
        workspace_id: WorkspaceId,
        resume_id: ResumeId,
        *,
        actor_id: UserId,
    ) -> bool:
        """@brief 验证 Resume 归属 / Verify Resume ownership.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param resume_id Resume 标识 / Resume identifier.
        @param actor_id 已认证且安装进 RLS 的真实 actor / Authenticated actor installed into RLS.
        @return 同 Workspace 存在时为真 / True only when present in the same Workspace.
        """


class HybridKnowledgeSearch(Protocol):
    """@brief 只接受已授权 source/version allowlist 的混合检索 Port / Hybrid-search port accepting only an authorized allowlist."""

    async def search(self, plan: KnowledgeSearchPlan) -> HybridSearchResponse:
        """@brief 执行 lexical+dense 融合检索 / Execute lexical+dense fused retrieval.

        @param plan 精确 Workspace、source/version、filter 的授权计划 / Authorized exact plan.
        @return 含 provenance 和 score 分量的候选 / Candidates with provenance and score components.
        @note adapter 必须在 SQL/index 查询内部应用 Workspace/source/version/filter；应用层会
            再验证返回 provenance / The adapter must apply all boundaries inside the query; the
            application layer revalidates returned provenance.
        """


class KnowledgeRepository(Protocol):
    """@brief Workspace-first、CAS/row-lock 友好的 5.3 repository / Workspace-first repository supporting CAS and row locks."""

    async def list_connections(
        self,
        workspace_id: WorkspaceId,
        page: KnowledgePageRequest,
    ) -> KnowledgePage[Connection]:
        """@brief 以稳定 keyset 列出安全 Connection 投影 / List safe Connection projections by stable keyset."""

    async def get_connection(
        self,
        workspace_id: WorkspaceId,
        connection_id: ConnectionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAggregate | None:
        """@brief 在 Workspace 内读取 Connection / Read a Connection inside one Workspace."""

    async def add_connection(self, connection: ConnectionAggregate) -> None:
        """@brief 添加只持有 server credential reference 的 Connection / Add a Connection holding only a server reference."""

    async def save_connection(
        self,
        connection: ConnectionAggregate,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以旧 revision 做 Connection CAS / CAS a Connection using its old revision."""

    async def add_authorization_record(self, record: ConnectionAuthorizationRecord) -> None:
        """@brief 添加 state 摘要化授权记录 / Add an authorization record with hashed state."""

    async def get_authorization_record_by_idempotency(
        self,
        workspace_id: WorkspaceId,
        created_by: UserId,
        idempotency_key_hash: str,
        *,
        for_update: bool = False,
    ) -> ConnectionAuthorizationRecord | None:
        """@brief 按专用重放 scope 精确读取授权 session / Read an authorization session by its dedicated replay scope.

        @param workspace_id 路径 Workspace / Path Workspace.
        @param created_by 签名 principal 用户 / Signed-principal user.
        @param idempotency_key_hash 原始 key 的服务端 keyed hash / Server-keyed hash of the raw key.
        @param for_update 是否为并发首次创建锁定 scope / Whether to lock the scope for a
            concurrent first creation.
        @return 解密后的私有授权记录或空 / Decrypted private authorization record, if present.

        @note authorization URL 与 device code 只能由专用 AEAD adapter 解密；不得进入通用
            receipt、日志或 outbox / Authorization URLs and device codes are decrypted only by
            the dedicated AEAD adapter and never enter generic receipts, logs, or the outbox.
        """

    async def get_authorization_record(
        self,
        workspace_id: WorkspaceId,
        session_id: ConnectionAuthorizationSessionId,
        *,
        for_update: bool = False,
    ) -> ConnectionAuthorizationRecord | None:
        """@brief 在 Workspace 内读取 provider callback 记录 / Read a provider-callback record in a Workspace."""

    async def save_authorization_record(
        self,
        record: ConnectionAuthorizationRecord,
        *,
        expected_state: str,
    ) -> None:
        """@brief 以旧状态条件写保证 callback 一次完成 / Use old-state CAS for one callback completion."""

    async def list_sources(
        self,
        workspace_id: WorkspaceId,
        page: KnowledgePageRequest,
    ) -> KnowledgePage[KnowledgeSource]:
        """@brief 稳定 keyset 列出来源 / List sources by stable keyset."""

    async def get_source(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        *,
        for_update: bool = False,
    ) -> KnowledgeSource | None:
        """@brief 在 Workspace 内读取来源 / Read a source inside one Workspace."""

    async def list_policy_default_sources(
        self,
        workspace_id: WorkspaceId,
        *,
        include_source_ids: tuple[KnowledgeSourceId, ...],
        exclude_source_ids: tuple[KnowledgeSourceId, ...],
        limit: int,
    ) -> tuple[KnowledgeSource, ...]:
        """@brief 解析 policy_default 的有界候选 / Resolve bounded policy-default candidates."""

    async def add_source(
        self,
        source: KnowledgeSource,
        initial_version: KnowledgeSourceVersion | None,
    ) -> None:
        """@brief 原子添加来源与可选首版本 / Atomically add a source and optional first version."""

    async def save_source(
        self,
        source: KnowledgeSource,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以旧 revision 做来源 CAS / CAS a source using its old revision."""

    async def list_versions(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        page: KnowledgePageRequest,
    ) -> KnowledgePage[KnowledgeSourceVersion]:
        """@brief 在来源范围内按版本号 keyset 列出版本 / List versions by source-local version keyset."""

    async def get_version(
        self,
        workspace_id: WorkspaceId,
        source_id: KnowledgeSourceId,
        version_id: KnowledgeSourceVersionId,
    ) -> KnowledgeSourceVersion | None:
        """@brief 以 Workspace+source+version 三元组读取版本 / Read by Workspace+source+version tuple."""

    async def add_version(self, version: KnowledgeSourceVersion) -> None:
        """@brief 添加在 source 行锁内分配的版本 / Add a version allocated under the source row lock."""

    async def add_upload(self, upload: UploadSession) -> None:
        """@brief 添加直传 session / Add a direct-upload session."""

    async def get_upload(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
        *,
        for_update: bool = False,
    ) -> UploadSession | None:
        """@brief 在 Workspace 内读取 upload / Read an upload inside one Workspace."""

    async def save_upload(
        self,
        upload: UploadSession,
        *,
        expected_generation: int,
    ) -> None:
        """@brief 以内部 generation CAS 保存状态或唯一跨领域 claim / CAS status or sole cross-domain claim by generation.

        @note Resume import adapter 应通过其现有 focused port 对同一统一表执行
            ``workspace_id + completed + unclaimed`` 条件 UPDATE，不得建立平行 upload 表。
            / Resume import adapters should conditionally update this same unified store through
            their existing focused port and must not create a parallel upload table.
        """


class KnowledgeJobSink(Protocol):
    """@brief 与领域写入同事务的统一 Job sink / Unified Job sink sharing the domain transaction."""

    async def add(self, job: Job, spec: KnowledgeJobSpec) -> None:
        """@brief 持久化 platform Job 与 typed worker spec / Persist a platform Job and typed worker spec.

        @param job 复用统一 platform Job / Reused unified platform Job.
        @param spec 不进入公开 Job payload的 worker 输入 / Worker input excluded from public Job payload.
        """


class KnowledgeOutbox(Protocol):
    """@brief 与领域写入同事务的 transactional outbox / Transactional outbox sharing domain writes."""

    async def add(self, event: KnowledgeOutboxEvent) -> None:
        """@brief 添加 secret-free 事件 / Add a secret-free event.

        @param event 待发布事件 / Event awaiting publication.
        """


class KnowledgeUnitOfWork(Protocol):
    """@brief Connection、Upload、Knowledge、Job、outbox 原子工作单元 / Atomic 5.3 unit of work.

    @note PostgreSQL 实现必须通过数据库的 join/savepoint 模式加入外层
        ``AtomicPostgresIdempotencyExecutor`` transaction，使领域写入、outbox 与逐字 receipt
        一起提交或回滚 / PostgreSQL implementations must join the outer atomic-idempotency
        transaction through the database savepoint mode so domain writes, outbox, and byte-exact
        receipts commit or roll back together.
    """

    @property
    def repository(self) -> KnowledgeRepository:
        """@brief 返回事务绑定 repository / Return the transaction-bound repository."""

    @property
    def authorizer(self) -> AccessAuthorizer:
        """@brief 返回现有集中 AccessAuthorizer / Return the existing central AccessAuthorizer."""

    @property
    def jobs(self) -> KnowledgeJobSink:
        """@brief 返回统一 Job sink / Return the unified Job sink."""

    @property
    def outbox(self) -> KnowledgeOutbox:
        """@brief 返回 transactional outbox / Return the transactional outbox."""

    async def __aenter__(self) -> Self:
        """@brief 开始工作单元 / Enter the unit of work.

        @return 当前工作单元 / Current unit of work.
        """

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时回滚 / Roll back on exception or absent commit.

        @param exc_type 异常类型 / Exception type.
        @param exc 异常 / Exception.
        @param traceback traceback / Traceback.
        @return 不吞业务异常 / Does not suppress business exceptions.
        """

    async def commit(self) -> None:
        """@brief 原子提交领域写入、Job 与 outbox / Atomically commit domain writes, Job, and outbox."""

    async def rollback(self) -> None:
        """@brief 幂等回滚并清理 staged secrets / Idempotently roll back and clean staged secrets."""


class KnowledgeUnitOfWorkFactory(Protocol):
    """@brief 为每个 5.3 用例创建工作单元 / Create a unit of work for each section-5.3 use case."""

    def __call__(self) -> KnowledgeUnitOfWork:
        """@brief 创建未进入的工作单元 / Create a not-yet-entered unit of work.

        @return 新工作单元 / New unit of work.
        """


__all__ = [
    "ConnectionAuthorizationGateway",
    "ConnectionAuthorizationLaunch",
    "ConnectionCredentialBroker",
    "HybridKnowledgeSearch",
    "HybridSearchResponse",
    "KnowledgeCasMismatch",
    "KnowledgeDependencyVerifier",
    "KnowledgeJobSink",
    "KnowledgeOutbox",
    "KnowledgePage",
    "KnowledgePageRequest",
    "KnowledgeRepository",
    "KnowledgeUnitOfWork",
    "KnowledgeUnitOfWorkFactory",
    "ProvisionedConnectionCredential",
    "SourceNetworkGuard",
    "UploadObjectStore",
]
