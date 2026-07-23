"""@brief API v2 KnowledgeSource 与版本领域模型 / API v2 KnowledgeSource and version models.

KnowledgeSource 是可修改的 Workspace 资源；每个 KnowledgeVersionSnapshot 冻结内容摘要、
字节数、artifact 与单调版本号。索引状态可以演进，但任何状态迁移都不能替换内容快照。
来源私有输入与契约公开 ``public_config`` 明确分离。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from typing import NewType
from urllib.parse import urlsplit

from backend.domain.connections import ConnectionId
from backend.domain.platform import ProblemDetails
from backend.domain.principals import (
    DomainInvariantError,
    ResourceMeta,
    UserId,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef
from backend.domain.upload_sessions import MAX_UPLOAD_BYTES, UploadSessionId

KnowledgeSourceId = NewType("KnowledgeSourceId", str)
"""@brief KnowledgeSource 不透明标识 / Opaque KnowledgeSource identifier."""

KnowledgeSourceVersionId = NewType("KnowledgeSourceVersionId", str)
"""@brief KnowledgeSourceVersion 不透明标识 / Opaque KnowledgeSourceVersion identifier."""

ResumeId = NewType("ResumeId", str)
"""@brief Knowledge 输入引用的 Resume 标识 / Resume identifier referenced by Knowledge input."""

_OPAQUE_ID_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API v2 不透明标识语法 / API v2 opaque-identifier grammar."""

_STABLE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief agent scope 与 reason 名语法 / Agent-scope and reason-name grammar."""

_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
"""@brief 小写 SHA-256 语法 / Lower-case SHA-256 grammar."""


class KnowledgeDomainError(DomainInvariantError):
    """@brief API v2 Knowledge 领域不变量错误 / API v2 Knowledge-domain invariant error."""


class KnowledgeTransitionError(KnowledgeDomainError):
    """@brief KnowledgeSource 或 Version 状态机拒绝迁移 / Source or Version state transition rejected."""


class KnowledgeSourceType(StrEnum):
    """@brief 契约冻结的 KnowledgeSource 类型 / Contract-frozen KnowledgeSource types."""

    FILE = "file"
    URL = "url"
    WEBSITE = "website"
    BLOG_FEED = "blog_feed"
    GIT_REPOSITORY = "git_repository"
    MANUAL_NOTE = "manual_note"
    RESUME = "resume"
    CLOUD_DRIVE = "cloud_drive"

    @property
    def supports_sync(self) -> bool:
        """@brief 判断来源能否重新从权威上游同步 / Test whether the source can sync from upstream.

        @return URL、站点、feed、Git、Resume 或 cloud drive 时为真 / True for refreshable sources.
        """
        return self in {
            KnowledgeSourceType.URL,
            KnowledgeSourceType.WEBSITE,
            KnowledgeSourceType.BLOG_FEED,
            KnowledgeSourceType.GIT_REPOSITORY,
            KnowledgeSourceType.RESUME,
            KnowledgeSourceType.CLOUD_DRIVE,
        }


class KnowledgeSensitivity(StrEnum):
    """@brief Knowledge 数据敏感度 / Knowledge-data sensitivity."""

    NORMAL = "normal"
    CONFIDENTIAL = "confidential"
    HIGHLY_CONFIDENTIAL = "highly_confidential"


class PolicyEffect(StrEnum):
    """@brief visibility policy 的 allow/deny 效果 / Allow/deny visibility-policy effects."""

    ALLOW = "allow"
    DENY = "deny"


class KnowledgeOperation(StrEnum):
    """@brief visibility policy 控制的操作 / Operations governed by visibility policy."""

    RETRIEVE = "retrieve"
    QUOTE = "quote"
    SUMMARIZE = "summarize"
    DERIVE = "derive"
    WRITE_BACK = "write_back"


class ModelRegion(StrEnum):
    """@brief 契约冻结的模型数据区域 / Contract-frozen model-data regions."""

    CN = "cn"
    GLOBAL = "global"
    PRIVATE_DEPLOYMENT = "private_deployment"


class KnowledgeIngestionStatus(StrEnum):
    """@brief 契约冻结的 ingestion 状态 / Contract-frozen ingestion states."""

    NOT_STARTED = "not_started"
    QUEUED = "queued"
    FETCHING = "fetching"
    PARSING = "parsing"
    CHUNKING = "chunking"
    EMBEDDING = "embedding"
    READY = "ready"
    STALE = "stale"
    FAILED = "failed"
    DELETING = "deleting"
    DELETED = "deleted"

    @property
    def is_active(self) -> bool:
        """@brief 判断 ingestion 是否正在执行 / Test whether ingestion is active.

        @return queued 到 embedding 阶段时为真 / True from queued through embedding.
        """
        return self in {
            KnowledgeIngestionStatus.QUEUED,
            KnowledgeIngestionStatus.FETCHING,
            KnowledgeIngestionStatus.PARSING,
            KnowledgeIngestionStatus.CHUNKING,
            KnowledgeIngestionStatus.EMBEDDING,
        }


class KnowledgeVersionStatus(StrEnum):
    """@brief 契约冻结的 KnowledgeSourceVersion 状态 / Contract-frozen version states."""

    PENDING = "pending"
    INDEXING = "indexing"
    READY = "ready"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class AgentScopeGrant:
    """@brief 一个 agent scope 的显式 allow/deny grant / Explicit allow/deny grant for an agent scope.

    @param agent_scope 稳定 agent scope / Stable agent scope.
    @param effect allow 或 deny / Allow or deny effect.
    @param allowed_operations grant 适用的非空操作集合 / Non-empty operations to which the grant applies.
    """

    agent_scope: str
    effect: PolicyEffect
    allowed_operations: tuple[KnowledgeOperation, ...]

    def __post_init__(self) -> None:
        """@brief 校验 agent grant / Validate the agent grant.

        @raise KnowledgeDomainError scope 或 operations 非法时抛出 / Raised for invalid fields.
        """
        _require_stable_name(self.agent_scope, "agent scope")
        if not self.allowed_operations or len(set(self.allowed_operations)) != len(
            self.allowed_operations
        ):
            raise KnowledgeDomainError("agent grant operations must be non-empty and unique")

    def applies_to(self, agent_scope: str, operation: KnowledgeOperation) -> bool:
        """@brief 判断 grant 是否匹配一次操作 / Test whether the grant matches an operation.

        @param agent_scope 当前 agent scope / Current agent scope.
        @param operation 当前操作 / Current operation.
        @return scope 与 operation 均匹配时为真 / True when both scope and operation match.
        """
        return self.agent_scope == agent_scope and operation in self.allowed_operations


@dataclass(frozen=True, slots=True)
class KnowledgeVisibilityPolicy:
    """@brief Knowledge 可见性、模型区域与保留策略 / Knowledge visibility, region, and retention policy.

    @param sensitivity 敏感度 / Sensitivity.
    @param default_effect 无匹配 grant 时的效果 / Effect when no grant matches.
    @param agent_grants 显式 agent grants / Explicit agent grants.
    @param session_override_allowed session 是否可进一步收窄或放宽 / Whether a session override is allowed.
    @param allowed_model_regions 允许处理的模型区域 / Model regions allowed to process the data.
    @param allow_external_model_processing 是否允许外部模型 provider / Whether external model processing is allowed.
    @param retention_days 可空保留天数 / Optional retention in days.
    @param policy_version 单调 policy 版本 / Monotonic policy version.
    """

    sensitivity: KnowledgeSensitivity
    default_effect: PolicyEffect
    agent_grants: tuple[AgentScopeGrant, ...]
    session_override_allowed: bool
    allowed_model_regions: tuple[ModelRegion, ...]
    allow_external_model_processing: bool
    retention_days: int | None
    policy_version: int

    def __post_init__(self) -> None:
        """@brief 校验 visibility policy / Validate the visibility policy.

        @raise KnowledgeDomainError 数量、region、retention 或版本非法时抛出 / Raised for invalid policy.
        """
        if len(self.agent_grants) > 100:
            raise KnowledgeDomainError("visibility policy cannot exceed 100 agent grants")
        if not self.allowed_model_regions or len(set(self.allowed_model_regions)) != len(
            self.allowed_model_regions
        ):
            raise KnowledgeDomainError("allowed model regions must be non-empty and unique")
        if self.retention_days is not None and not 1 <= self.retention_days <= 3_650:
            raise KnowledgeDomainError("knowledge retention must be between one and 3650 days")
        if self.policy_version < 1:
            raise KnowledgeDomainError("knowledge policy version must be positive")


@dataclass(frozen=True, slots=True)
class FileSourceInput:
    """@brief 已完成 upload 引用的 file 来源输入 / File-source input referencing a completed upload.

    @param upload_session_id UploadSession 标识 / UploadSession identifier.
    @param source_type 判别值 / Discriminator.
    """

    upload_session_id: UploadSessionId
    source_type: KnowledgeSourceType = KnowledgeSourceType.FILE

    def __post_init__(self) -> None:
        """@brief 校验 file 输入 / Validate file input.

        @raise KnowledgeDomainError 标识或判别值非法时抛出 / Raised for invalid identity or type.
        """
        _require_opaque_id(self.upload_session_id, "file upload session id")
        if self.source_type is not KnowledgeSourceType.FILE:
            raise KnowledgeDomainError("file source input must use the file discriminator")


@dataclass(frozen=True, slots=True)
class UrlSourceInput:
    """@brief HTTP(S) URL、website 或 feed 输入 / HTTP(S) URL, website, or feed input.

    @param source_type url、website 或 blog_feed / URL, website, or blog-feed discriminator.
    @param url 不含 userinfo 的 HTTP(S) URL / HTTP(S) URL without userinfo.
    """

    source_type: KnowledgeSourceType
    url: str

    def __post_init__(self) -> None:
        """@brief 校验 URL 输入的静态语法 / Validate static URL-input syntax.

        @raise KnowledgeDomainError 类型或 URL 非法时抛出 / Raised for an invalid type or URL.
        @note DNS、redirect 与 IP 分类必须由每次 fetch 的网络策略 Port 重验 / DNS,
            redirects, and IP classification must be rechecked by the fetch-policy port.
        """
        if self.source_type not in {
            KnowledgeSourceType.URL,
            KnowledgeSourceType.WEBSITE,
            KnowledgeSourceType.BLOG_FEED,
        }:
            raise KnowledgeDomainError("URL source input has an invalid discriminator")
        _require_http_url(self.url, "knowledge source URL")


@dataclass(frozen=True, slots=True)
class GitSourceInput:
    """@brief Git repository 来源输入 / Git-repository source input.

    @param clone_url 契约允许的 HTTP(S) clone URL / Contract-compatible HTTP(S) clone URL.
    @param ref 可选 branch、tag 或 commit / Optional branch, tag, or commit.
    @param include_paths include 路径规则 / Include path rules.
    @param exclude_paths exclude 路径规则 / Exclude path rules.
    @param connection_id 可选同 Workspace credential 引用 / Optional same-Workspace credential reference.
    @param source_type 判别值 / Discriminator.
    """

    clone_url: str
    ref: str | None
    include_paths: tuple[str, ...]
    exclude_paths: tuple[str, ...]
    connection_id: ConnectionId | None
    source_type: KnowledgeSourceType = KnowledgeSourceType.GIT_REPOSITORY

    def __post_init__(self) -> None:
        """@brief 校验 Git 输入 / Validate Git input.

        @raise KnowledgeDomainError URL、ref、路径或 connection 非法时抛出 / Raised for invalid input.
        """
        if self.source_type is not KnowledgeSourceType.GIT_REPOSITORY:
            raise KnowledgeDomainError("Git source input has an invalid discriminator")
        _require_http_url(self.clone_url, "Git clone URL")
        if self.ref is not None:
            _require_text(self.ref, "Git ref", 1, 255)
        _require_paths(self.include_paths, "Git include paths")
        _require_paths(self.exclude_paths, "Git exclude paths")
        if self.connection_id is not None:
            _require_opaque_id(self.connection_id, "Git connection id")


@dataclass(frozen=True, slots=True)
class ManualSourceInput:
    """@brief 手工 note 私有内容输入 / Private manual-note content input.

    @param content 不进入公开 config 或普通 repr 的正文 / Body excluded from public config and repr.
    @param source_type 判别值 / Discriminator.
    """

    content: str = field(repr=False)
    source_type: KnowledgeSourceType = KnowledgeSourceType.MANUAL_NOTE

    def __post_init__(self) -> None:
        """@brief 校验手工正文 / Validate manual content.

        @raise KnowledgeDomainError 正文为空或超限时抛出 / Raised for empty or oversized content.
        """
        if self.source_type is not KnowledgeSourceType.MANUAL_NOTE:
            raise KnowledgeDomainError("manual source input has an invalid discriminator")
        if not 1 <= len(self.content) <= 200_000:
            raise KnowledgeDomainError("manual note must contain between one and 200000 characters")


@dataclass(frozen=True, slots=True)
class ResumeSourceInput:
    """@brief 同 Workspace Resume 来源输入 / Same-Workspace Resume source input.

    @param resume_id Resume 标识 / Resume identifier.
    @param source_type 判别值 / Discriminator.
    """

    resume_id: ResumeId
    source_type: KnowledgeSourceType = KnowledgeSourceType.RESUME

    def __post_init__(self) -> None:
        """@brief 校验 Resume 输入 / Validate Resume input.

        @raise KnowledgeDomainError 标识或判别值非法时抛出 / Raised for invalid identity or type.
        """
        _require_opaque_id(self.resume_id, "knowledge Resume id")
        if self.source_type is not KnowledgeSourceType.RESUME:
            raise KnowledgeDomainError("Resume source input has an invalid discriminator")


@dataclass(frozen=True, slots=True)
class CloudDriveSourceInput:
    """@brief cloud-drive Connection 与远端对象输入 / Cloud-drive Connection and remote-object input.

    @param connection_id 同 Workspace Connection / Same-Workspace Connection.
    @param remote_id provider 私有远端对象标识 / Provider-private remote object identifier.
    @param source_type 判别值 / Discriminator.
    """

    connection_id: ConnectionId
    remote_id: str = field(repr=False)
    source_type: KnowledgeSourceType = KnowledgeSourceType.CLOUD_DRIVE

    def __post_init__(self) -> None:
        """@brief 校验 cloud-drive 输入 / Validate cloud-drive input.

        @raise KnowledgeDomainError 标识或 remote id 非法时抛出 / Raised for invalid fields.
        """
        _require_opaque_id(self.connection_id, "cloud-drive connection id")
        _require_text(self.remote_id, "cloud-drive remote id", 1, 2_000)
        if self.source_type is not KnowledgeSourceType.CLOUD_DRIVE:
            raise KnowledgeDomainError("cloud-drive source input has an invalid discriminator")


type KnowledgeSourceInput = (
    FileSourceInput
    | UrlSourceInput
    | GitSourceInput
    | ManualSourceInput
    | ResumeSourceInput
    | CloudDriveSourceInput
)
"""@brief KnowledgeSourceInput 判别联合 / Discriminated KnowledgeSourceInput union."""


@dataclass(frozen=True, slots=True)
class FilePublicMetadata:
    """@brief 从已验证 upload 生成的 file 公开元数据 / File public metadata from a verified upload.

    @param filename 客户端显示文件名 / Client-visible filename.
    @param media_type MIME sniff 已确认的媒体类型 / MIME type confirmed by sniffing.
    """

    filename: str
    media_type: str

    def __post_init__(self) -> None:
        """@brief 校验 file 公开元数据 / Validate file public metadata.

        @raise KnowledgeDomainError 文本非法时抛出 / Raised for invalid text.
        """
        _require_text(self.filename, "knowledge filename", 1, 300)
        _require_text(self.media_type, "knowledge media type", 3, 200)


@dataclass(frozen=True, slots=True)
class PublicKnowledgeSourceConfig:
    """@brief 明确排除 credential 与私有正文的公开来源配置 / Public source config excluding credentials and body.

    @param filename file 名称 / File name.
    @param media_type file MIME / File MIME.
    @param url HTTP(S) 来源 / HTTP(S) source.
    @param clone_url Git clone URL / Git clone URL.
    @param ref Git ref / Git ref.
    @param resume_id Resume 来源 / Resume source ID.
    """

    filename: str | None = None
    media_type: str | None = None
    url: str | None = None
    clone_url: str | None = None
    ref: str | None = None
    resume_id: ResumeId | None = None


@dataclass(frozen=True, slots=True)
class KnowledgeIngestionState:
    """@brief KnowledgeSource ingestion 的公开状态快照 / Public KnowledgeSource ingestion snapshot.

    @param status ingestion 阶段 / Ingestion phase.
    @param document_count 成功索引文档数 / Successfully indexed document count.
    @param chunk_count 成功索引 chunk 数 / Successfully indexed chunk count.
    @param last_success_at 最近成功时刻 / Most recent success instant.
    @param last_problem 最近公开安全问题 / Most recent public-safe problem.
    """

    status: KnowledgeIngestionStatus = KnowledgeIngestionStatus.NOT_STARTED
    document_count: int = 0
    chunk_count: int = 0
    last_success_at: datetime | None = None
    last_problem: ProblemDetails | None = None

    def __post_init__(self) -> None:
        """@brief 校验 ingestion 计数与状态关联 / Validate ingestion counts and associations.

        @raise KnowledgeDomainError 计数、时间或 problem 关联非法时抛出 / Raised for invalid state.
        """
        if self.document_count < 0 or self.chunk_count < 0:
            raise KnowledgeDomainError("ingestion counts cannot be negative")
        if self.last_success_at is not None:
            _require_aware(self.last_success_at, "knowledge last_success_at")
        if self.status is KnowledgeIngestionStatus.FAILED and self.last_problem is None:
            raise KnowledgeDomainError("failed ingestion requires a public-safe problem")
        if self.status is not KnowledgeIngestionStatus.FAILED and self.last_problem is not None:
            raise KnowledgeDomainError("only failed ingestion can carry a problem")

    def queue(self, *, force: bool) -> KnowledgeIngestionState:
        """@brief 为 ingestion/sync Job 迁移为 queued / Move to queued for an ingestion or sync Job.

        @param force 是否显式重做已 ready 内容 / Whether ready content is explicitly reprocessed.
        @return queued 状态 / Queued state.
        @raise KnowledgeTransitionError 已活动、删除中或非 force ready 时抛出 / Raised when not queueable.
        """
        if self.status.is_active:
            raise KnowledgeTransitionError("knowledge ingestion already has active work")
        if self.status in {KnowledgeIngestionStatus.DELETING, KnowledgeIngestionStatus.DELETED}:
            raise KnowledgeTransitionError("deleting or deleted knowledge cannot be ingested")
        if self.status is KnowledgeIngestionStatus.READY and not force:
            raise KnowledgeTransitionError("force is required to re-ingest ready knowledge")
        return replace(self, status=KnowledgeIngestionStatus.QUEUED, last_problem=None)


@dataclass(frozen=True, slots=True)
class KnowledgeSource:
    """@brief Workspace 隔离且带单调版本计数的 KnowledgeSource / Workspace-isolated source with version counter.

    @param meta 可变来源资源元数据 / Mutable source-resource metadata.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param created_by 创建者 / Creator.
    @param name 显示名 / Display name.
    @param source_type 来源类型 / Source type.
    @param enabled 是否允许执行 / Whether execution is enabled.
    @param public_config 公开且无 credential 的配置 / Public credential-free config.
    @param visibility 可见性策略 / Visibility policy.
    @param ingestion ingestion 状态 / Ingestion state.
    @param current_version_id 当前内容版本 / Current content version.
    @param version_counter 单调分配计数；不进入公开 payload / Monotonic allocation counter, not public.
    @param source_input 私有 typed input；不进入公开 payload/repr / Private typed input, excluded from public payload/repr.
    """

    meta: ResourceMeta[KnowledgeSourceId]
    workspace_id: WorkspaceId
    created_by: UserId
    name: str
    source_type: KnowledgeSourceType
    enabled: bool
    public_config: PublicKnowledgeSourceConfig
    visibility: KnowledgeVisibilityPolicy
    ingestion: KnowledgeIngestionState
    current_version_id: KnowledgeSourceVersionId | None
    version_counter: int = field(repr=False)
    source_input: KnowledgeSourceInput = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验来源、版本计数与公开/私有配置一致性 / Validate source and config consistency.

        @raise KnowledgeDomainError 标识、类型、配置或状态非法时抛出 / Raised for invalid state.
        """
        _require_opaque_id(self.meta.id, "knowledge source id")
        _require_opaque_id(self.workspace_id, "knowledge workspace id")
        _require_opaque_id(self.created_by, "knowledge creator id")
        _require_text(self.name, "knowledge source name", 1, 300)
        if self.source_type is not self.source_input.source_type:
            raise KnowledgeDomainError("knowledge source type must match its private input")
        if self.version_counter < 0:
            raise KnowledgeDomainError("knowledge version counter cannot be negative")
        if (self.current_version_id is None) is (self.version_counter > 0):
            raise KnowledgeDomainError("current version presence must match a positive version counter")
        if self.current_version_id is not None:
            _require_opaque_id(self.current_version_id, "knowledge current version id")
        if self.ingestion.status in {
            KnowledgeIngestionStatus.DELETING,
            KnowledgeIngestionStatus.DELETED,
        } and self.enabled:
            raise KnowledgeDomainError("deleting or deleted knowledge source must be disabled")
        _validate_public_config(self.source_input, self.public_config)

    @classmethod
    def create(
        cls,
        *,
        meta: ResourceMeta[KnowledgeSourceId],
        workspace_id: WorkspaceId,
        created_by: UserId,
        name: str,
        source_input: KnowledgeSourceInput,
        visibility: KnowledgeVisibilityPolicy,
        file_metadata: FilePublicMetadata | None = None,
    ) -> KnowledgeSource:
        """@brief 从判别输入创建无版本来源 / Create a versionless source from discriminated input.

        @param meta 首个资源元数据 / Initial resource metadata.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param created_by 创建者 / Creator.
        @param name 显示名 / Display name.
        @param source_input 私有来源输入 / Private source input.
        @param visibility 初始 policy / Initial policy.
        @param file_metadata file 来源从完成 upload 取得的公开元数据 / File public metadata from completed upload.
        @return 新 KnowledgeSource / New KnowledgeSource.
        """
        return cls(
            meta,
            workspace_id,
            created_by,
            name,
            source_input.source_type,
            True,
            _public_config(source_input, file_metadata),
            visibility,
            KnowledgeIngestionState(),
            None,
            0,
            source_input,
        )

    def revise(
        self,
        *,
        name: str | None,
        visibility: KnowledgeVisibilityPolicy | None,
        at: datetime,
    ) -> KnowledgeSource:
        """@brief 应用 KnowledgeSource merge-patch / Apply a KnowledgeSource merge patch.

        @param name 可选完整目标名 / Optional complete target name.
        @param visibility 可选完整目标 policy / Optional complete target policy.
        @param at 修改时刻 / Modification instant.
        @return 下一 revision 来源 / Next-revision source.
        @raise KnowledgeDomainError patch 无变化或 policy 版本不连续时抛出 / Raised for no-op or bad policy version.
        """
        if name is None and visibility is None:
            raise KnowledgeDomainError("knowledge source patch must contain a field")
        target_name = self.name if name is None else name
        target_visibility = self.visibility if visibility is None else visibility
        if target_name == self.name and target_visibility == self.visibility:
            raise KnowledgeDomainError("knowledge source patch must change a value")
        if visibility is not None and visibility != self.visibility:
            if visibility.policy_version != self.visibility.policy_version + 1:
                raise KnowledgeDomainError("knowledge policy version must advance by exactly one")
        return replace(
            self,
            meta=self.meta.advance(at),
            name=target_name,
            visibility=target_visibility,
        )

    def queue_ingestion(self, *, force: bool, at: datetime) -> KnowledgeSource:
        """@brief 在同事务中将来源标记 queued / Mark the source queued in the same transaction.

        @param force 是否强制重新处理 ready 内容 / Whether ready content is forcibly reprocessed.
        @param at Job 创建时刻 / Job creation instant.
        @return queued 的下一 revision / Next revision in queued state.
        """
        if not self.enabled:
            raise KnowledgeTransitionError("disabled knowledge source cannot be ingested")
        return replace(
            self,
            meta=self.meta.advance(at),
            ingestion=self.ingestion.queue(force=force),
        )

    def begin_deletion(self, *, at: datetime) -> KnowledgeSource:
        """@brief 为异步删除禁用来源并标记 deleting / Disable and mark deleting for async deletion.

        @param at 删除请求时刻 / Deletion-request instant.
        @return deleting 的下一 revision / Next revision in deleting state.
        @raise KnowledgeTransitionError 已删除或删除中时抛出 / Raised when already deleting/deleted.
        """
        if self.ingestion.status in {
            KnowledgeIngestionStatus.DELETING,
            KnowledgeIngestionStatus.DELETED,
        }:
            raise KnowledgeTransitionError("knowledge source deletion has already started")
        return replace(
            self,
            meta=self.meta.advance(at),
            enabled=False,
            ingestion=KnowledgeIngestionState(
                status=KnowledgeIngestionStatus.DELETING,
                document_count=self.ingestion.document_count,
                chunk_count=self.ingestion.chunk_count,
                last_success_at=self.ingestion.last_success_at,
            ),
        )

    def allocate_version(
        self,
        *,
        version_id: KnowledgeSourceVersionId,
        content_sha256: str,
        size_bytes: int,
        artifact_ref: ResourceRef,
        at: datetime,
    ) -> tuple[KnowledgeSource, KnowledgeSourceVersion]:
        """@brief 在来源行锁内分配下一单调版本 / Allocate the next monotonic version under a source row lock.

        @param version_id 新版本标识 / New version identifier.
        @param content_sha256 已验证内容摘要 / Verified content digest.
        @param size_bytes 已验证内容字节数 / Verified content size.
        @param artifact_ref 已验证上传 artifact / Verified upload artifact.
        @param at 分配时刻 / Allocation instant.
        @return 更新来源与不可变内容版本 / Updated source and immutable-content version.
        @note adapter 必须锁定 source 行并以旧 revision 做 CAS，唯一约束为
            ``(workspace_id, source_id, version_number)`` / The adapter must lock the source row,
            CAS its revision, and enforce a unique version tuple.
        """
        if not self.enabled:
            raise KnowledgeTransitionError("disabled knowledge source cannot receive a version")
        _require_opaque_id(version_id, "knowledge version id")
        next_number = self.version_counter + 1
        snapshot = KnowledgeVersionSnapshot(
            self.meta.id,
            next_number,
            content_sha256,
            size_bytes,
            artifact_ref,
        )
        version = KnowledgeSourceVersion(
            ResourceMeta(version_id, 1, at, at),
            self.workspace_id,
            snapshot,
        )
        updated = replace(
            self,
            meta=self.meta.advance(at),
            current_version_id=version_id,
            version_counter=next_number,
            ingestion=replace(
                self.ingestion,
                status=KnowledgeIngestionStatus.STALE,
                last_problem=None,
            ),
        )
        return updated, version


@dataclass(frozen=True, slots=True)
class KnowledgeVersionSnapshot:
    """@brief 永不修改的 Knowledge 内容快照 / Immutable Knowledge content snapshot.

    @param source_id 所属来源 / Owning source.
    @param version_number 来源内单调版本号 / Monotonic source-local version number.
    @param content_sha256 内容摘要 / Content digest.
    @param size_bytes 内容字节数 / Content byte count.
    @param artifact_ref 服务端内容 artifact / Server-side content artifact.
    """

    source_id: KnowledgeSourceId
    version_number: int
    content_sha256: str
    size_bytes: int
    artifact_ref: ResourceRef = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验不可变内容快照 / Validate the immutable content snapshot.

        @raise KnowledgeDomainError 标识、版本、hash 或 size 非法时抛出 / Raised for invalid fields.
        """
        _require_opaque_id(self.source_id, "knowledge version source id")
        if self.version_number < 1:
            raise KnowledgeDomainError("knowledge version number must be positive")
        if _SHA256_PATTERN.fullmatch(self.content_sha256) is None:
            raise KnowledgeDomainError("knowledge content SHA-256 is invalid")
        if isinstance(self.size_bytes, bool) or not 0 <= self.size_bytes <= MAX_UPLOAD_BYTES:
            raise KnowledgeDomainError("knowledge content size must be between zero and one GiB")


@dataclass(frozen=True, slots=True)
class KnowledgeSourceVersion:
    """@brief 内容快照固定而索引状态可演进的版本资源 / Version resource with fixed content and evolving index state.

    @param meta 版本资源 revision 元数据 / Version-resource revision metadata.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param snapshot 永不替换的内容快照 / Never-replaced content snapshot.
    @param status 索引状态 / Index status.
    @param indexed_at ready 时的完成时间 / Completion instant when ready.
    """

    meta: ResourceMeta[KnowledgeSourceVersionId]
    workspace_id: WorkspaceId
    snapshot: KnowledgeVersionSnapshot
    status: KnowledgeVersionStatus = KnowledgeVersionStatus.PENDING
    indexed_at: datetime | None = None

    def __post_init__(self) -> None:
        """@brief 校验版本状态关联 / Validate version-state associations.

        @raise KnowledgeDomainError 标识、时间或状态非法时抛出 / Raised for invalid fields.
        """
        _require_opaque_id(self.meta.id, "knowledge version id")
        _require_opaque_id(self.workspace_id, "knowledge version workspace id")
        if self.status is KnowledgeVersionStatus.READY:
            if self.indexed_at is None:
                raise KnowledgeDomainError("ready knowledge version requires indexed_at")
            _require_aware(self.indexed_at, "knowledge version indexed_at")
        elif self.indexed_at is not None:
            raise KnowledgeDomainError("only ready knowledge version can have indexed_at")

    def begin_indexing(self, *, at: datetime) -> KnowledgeSourceVersion:
        """@brief 将 pending version 开始为 indexing / Start a pending version as indexing.

        @param at 开始时刻 / Start instant.
        @return indexing 的下一 revision / Next revision in indexing state.
        @raise KnowledgeTransitionError 当前不是 pending 时抛出 / Raised unless pending.
        """
        if self.status is not KnowledgeVersionStatus.PENDING:
            raise KnowledgeTransitionError("only a pending knowledge version can begin indexing")
        return replace(self, meta=self.meta.advance(at), status=KnowledgeVersionStatus.INDEXING)

    def mark_ready(self, *, at: datetime) -> KnowledgeSourceVersion:
        """@brief 完成 indexing 且保持相同内容快照 / Complete indexing while retaining the content snapshot.

        @param at 完成时刻 / Completion instant.
        @return ready 的下一 revision / Next revision in ready state.
        @raise KnowledgeTransitionError 当前不是 indexing 时抛出 / Raised unless indexing.
        """
        if self.status is not KnowledgeVersionStatus.INDEXING:
            raise KnowledgeTransitionError("only an indexing knowledge version can become ready")
        return replace(
            self,
            meta=self.meta.advance(at),
            status=KnowledgeVersionStatus.READY,
            indexed_at=at,
        )

    def mark_failed(self, *, at: datetime) -> KnowledgeSourceVersion:
        """@brief 终止 pending/indexing 版本且保持内容快照 / Fail a version without changing content.

        @param at 失败时刻 / Failure instant.
        @return failed 的下一 revision / Next revision in failed state.
        @raise KnowledgeTransitionError 当前已 ready/failed 时抛出 / Raised from a terminal state.
        """
        if self.status not in {KnowledgeVersionStatus.PENDING, KnowledgeVersionStatus.INDEXING}:
            raise KnowledgeTransitionError("terminal knowledge version cannot fail again")
        return replace(self, meta=self.meta.advance(at), status=KnowledgeVersionStatus.FAILED)


def _public_config(
    source_input: KnowledgeSourceInput,
    file_metadata: FilePublicMetadata | None,
) -> PublicKnowledgeSourceConfig:
    """@brief 从私有输入生成最小公开 config / Build the minimal public config from private input.

    @param source_input 私有来源输入 / Private source input.
    @param file_metadata file 已验证元数据 / Verified file metadata.
    @return 不含 credential、manual content 或 remote id 的配置 / Credential/body-free config.
    @raise KnowledgeDomainError file 元数据缺失或多余时抛出 / Raised for invalid file metadata use.
    """
    if isinstance(source_input, FileSourceInput):
        if file_metadata is None:
            raise KnowledgeDomainError("file source requires verified public file metadata")
        return PublicKnowledgeSourceConfig(
            filename=file_metadata.filename,
            media_type=file_metadata.media_type,
        )
    if file_metadata is not None:
        raise KnowledgeDomainError("only file source accepts public file metadata")
    if isinstance(source_input, UrlSourceInput):
        return PublicKnowledgeSourceConfig(url=source_input.url)
    if isinstance(source_input, GitSourceInput):
        return PublicKnowledgeSourceConfig(clone_url=source_input.clone_url, ref=source_input.ref)
    if isinstance(source_input, ResumeSourceInput):
        return PublicKnowledgeSourceConfig(resume_id=source_input.resume_id)
    return PublicKnowledgeSourceConfig()


def _validate_public_config(
    source_input: KnowledgeSourceInput,
    config: PublicKnowledgeSourceConfig,
) -> None:
    """@brief 防止私有来源信息误入公开 config / Prevent private source material entering public config.

    @param source_input 私有来源输入 / Private source input.
    @param config 公开配置 / Public config.
    @raise KnowledgeDomainError config 与来源类型不一致时抛出 / Raised for inconsistent config.
    """
    values = {
        "filename": config.filename,
        "media_type": config.media_type,
        "url": config.url,
        "clone_url": config.clone_url,
        "ref": config.ref,
        "resume_id": config.resume_id,
    }
    present = {key for key, value in values.items() if value is not None}
    if isinstance(source_input, FileSourceInput):
        if present != {"filename", "media_type"}:
            raise KnowledgeDomainError("file public config requires only filename and media_type")
        return
    if isinstance(source_input, UrlSourceInput):
        if present != {"url"} or config.url != source_input.url:
            raise KnowledgeDomainError("URL public config must contain only the source URL")
        return
    if isinstance(source_input, GitSourceInput):
        expected = {"clone_url"} | ({"ref"} if source_input.ref is not None else set())
        if present != expected or config.clone_url != source_input.clone_url or config.ref != source_input.ref:
            raise KnowledgeDomainError("Git public config must contain only clone_url and ref")
        return
    if isinstance(source_input, ResumeSourceInput):
        if present != {"resume_id"} or config.resume_id != source_input.resume_id:
            raise KnowledgeDomainError("Resume public config must contain only resume_id")
        return
    if present:
        raise KnowledgeDomainError("manual and cloud-drive public config must not expose private input")


def _require_paths(paths: tuple[str, ...], label: str) -> None:
    """@brief 校验 Git path 规则边界 / Validate Git path-rule bounds.

    @param paths 路径规则 / Path rules.
    @param label 错误标签 / Error label.
    @raise KnowledgeDomainError 数量或文本超限时抛出 / Raised for excessive or invalid paths.
    """
    if len(paths) > 100:
        raise KnowledgeDomainError(f"{label} cannot exceed 100 entries")
    for path in paths:
        _require_text(path, label, 1, 1_000)


def _require_text(value: str, label: str, minimum: int, maximum: int) -> None:
    """@brief 校验有界规范文本 / Validate bounded canonical text.

    @param value 文本 / Text.
    @param label 错误标签 / Error label.
    @param minimum 最小长度 / Minimum length.
    @param maximum 最大长度 / Maximum length.
    @raise KnowledgeDomainError 文本非法时抛出 / Raised for invalid text.
    """
    if not minimum <= len(value) <= maximum or value.strip() != value:
        raise KnowledgeDomainError(f"{label} must be canonical and {minimum} to {maximum} characters")


def _require_opaque_id(value: str, label: str) -> None:
    """@brief 校验 API v2 不透明标识 / Validate an API v2 opaque identifier.

    @param value 标识 / Identifier.
    @param label 错误标签 / Error label.
    @raise KnowledgeDomainError 标识非法时抛出 / Raised for an invalid identifier.
    """
    if _OPAQUE_ID_PATTERN.fullmatch(value) is None:
        raise KnowledgeDomainError(f"{label} does not satisfy the API v2 grammar")


def _require_stable_name(value: str, label: str) -> None:
    """@brief 校验稳定名称 / Validate a stable name.

    @param value 名称 / Name.
    @param label 错误标签 / Error label.
    @raise KnowledgeDomainError 名称非法时抛出 / Raised for an invalid name.
    """
    if _STABLE_NAME_PATTERN.fullmatch(value) is None:
        raise KnowledgeDomainError(f"{label} does not satisfy the stable-name grammar")


def _require_http_url(value: str, label: str) -> None:
    """@brief 校验无 userinfo 的 HTTP(S) URL 静态语法 / Validate static HTTP(S) URL syntax.

    @param value URL / URL.
    @param label 错误标签 / Error label.
    @raise KnowledgeDomainError URL 非法时抛出 / Raised for an invalid URL.
    """
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise KnowledgeDomainError(f"{label} must be HTTP(S) without userinfo")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 校验带时区时间 / Validate a timezone-aware datetime.

    @param value 时间 / Datetime.
    @param label 错误标签 / Error label.
    @raise KnowledgeDomainError 时间 naive 时抛出 / Raised for a naive datetime.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise KnowledgeDomainError(f"{label} must be timezone-aware")


__all__ = [
    "AgentScopeGrant",
    "CloudDriveSourceInput",
    "FilePublicMetadata",
    "FileSourceInput",
    "GitSourceInput",
    "KnowledgeDomainError",
    "KnowledgeIngestionState",
    "KnowledgeIngestionStatus",
    "KnowledgeOperation",
    "KnowledgeSensitivity",
    "KnowledgeSource",
    "KnowledgeSourceId",
    "KnowledgeSourceInput",
    "KnowledgeSourceType",
    "KnowledgeSourceVersion",
    "KnowledgeSourceVersionId",
    "KnowledgeTransitionError",
    "KnowledgeVersionSnapshot",
    "KnowledgeVersionStatus",
    "KnowledgeVisibilityPolicy",
    "ManualSourceInput",
    "ModelRegion",
    "PolicyEffect",
    "PublicKnowledgeSourceConfig",
    "ResumeId",
    "ResumeSourceInput",
    "UrlSourceInput",
]
