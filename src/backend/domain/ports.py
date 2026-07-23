"""@brief 领域端口（Repository 与 Provider 抽象）/ Domain ports (Repository and provider abstractions)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Any, Protocol

from backend.domain.agent import AgentRunRecord, ConversationRecord, MessageRecord
from backend.domain.common import Job
from backend.domain.identity import (
    IdentityAuthenticatorRecord,
    IdentityBrowserSessionRecord,
    IdentityFlowRecord,
    IdentitySessionRecord,
    IdentityUserRecord,
)
from backend.domain.interview import InterviewSessionRecord
from backend.domain.knowledge import (
    EmbeddingSpace,
    KnowledgeSourceRecord,
    ParsedKnowledgeDocument,
    StoredKnowledgeBlob,
)
from backend.domain.oauth import (
    AuthorizationCodeExchange,
    AuthorizationRequestRecord,
    RefreshTokenRotation,
)
from backend.domain.observability import (
    AttributeValue,
    MetricType,
    SeverityNumber,
    SignalSource,
    SpanStatus,
    TelemetrySignal,
)
from backend.domain.proposal import ResumeProposalRecord
from backend.domain.resume import ResumeRecord
from workspace_shared.tenancy import ActorScope


class OAuthAuthorizationRequestRepository(Protocol):
    """Persistence boundary for short-lived Authorization Server transactions."""

    async def create_authorization_request(self, record: AuthorizationRequestRecord) -> None:
        """Persist a newly validated authorization request atomically."""

    async def get_authorization_request(self, request_id: str) -> AuthorizationRequestRecord | None:
        """Read one authorization request without accepting client-provided state."""

    async def issue_authorization_code(
        self,
        request_id: str,
        *,
        subject: str,
        user_id: str,
        login_session_id: str,
        code_hash: str,
        auth_time: datetime,
        expires_at: datetime,
    ) -> bool:
        """Atomically complete a pending request and persist a code bound to its login session."""

    async def exchange_authorization_code(
        self,
        code_hash: str,
        *,
        client_id: str,
        redirect_uri: str,
        verifier_challenge: str,
        refresh_family_id: str | None,
        refresh_token_id: str | None,
        refresh_token_hash: str | None,
        refresh_expires_at: datetime | None,
    ) -> AuthorizationCodeExchange | None:
        """Consume a matching code and create its refresh family in one transaction."""

    async def rotate_refresh_token(
        self,
        token_hash: str,
        *,
        client_id: str,
        replacement_token_id: str,
        replacement_token_hash: str,
        replacement_expires_at: datetime,
    ) -> RefreshTokenRotation | None:
        """Rotate once, revoking the family on reuse of a consumed token."""

    async def revoke_refresh_token(self, token_hash: str) -> None:
        """Revoke the complete refresh family if the opaque token is known."""

    async def revoke_access_token(self, jti: str, expires_at: datetime) -> None:
        """Persist an access-token denylist entry until its natural expiration."""

    async def access_token_is_revoked(self, jti: str) -> bool:
        """Check an access-token JTI without exposing it outside the identity boundary."""

    async def revoke_access_tokens_for_user(
        self,
        user_id: str,
        revoked_before: datetime,
    ) -> None:
        """@brief 撤销用户在截止时刻及之前签发的全部 access token / Revoke all user access tokens issued at or before a cutoff.

        @param user_id 本地用户 ID / Local user identifier.
        @param revoked_before 带时区的 inclusive 签发截止时间 / Timezone-aware inclusive issue cutoff.
        """

    async def user_access_tokens_are_revoked(
        self,
        user_id: str,
        issued_at: datetime,
    ) -> bool:
        """@brief 检查用户级 token epoch / Check the user-level token epoch.

        @param user_id access token 的本地用户 claim / Local user claim from the access token.
        @param issued_at token 的 ``iat`` / Token ``iat`` instant.
        @return token 在用户级撤销截止内时为真 / True when the token falls within the user's
            revocation cutoff.
        """


class OAuthTokenIssuerVerifier(Protocol):
    """Asymmetric JWT issuance and verification port."""

    @property
    def jwks(self) -> dict[str, list[dict[str, str]]]:
        """Return public-only signing keys."""

    def issue_access_token(
        self,
        *,
        user_id: str,
        subject: str,
        client_id: str,
        scopes: tuple[str, ...],
        lifetime_seconds: int,
        now: datetime | None = None,
    ) -> tuple[str, datetime, str]:
        """Issue one Resource Server access token bound to its local user."""

    def issue_id_token(
        self,
        *,
        subject: str,
        client_id: str,
        nonce: str,
        lifetime_seconds: int,
        auth_time: datetime,
        now: datetime | None = None,
    ) -> str:
        """Issue one nonce-bound OIDC ID Token."""

    def verify_access_token(self, token: str, *, now: datetime | None = None) -> dict[str, Any]:
        """Verify one access token and return its required claims."""


class HostedIdentityRepository(Protocol):
    """Persistence boundary for hosted identity browser and flow state."""

    async def create_browser_session(self, record: IdentityBrowserSessionRecord) -> None:
        """Persist a new browser binding."""

    async def get_browser_session(self, session_id: str) -> IdentityBrowserSessionRecord | None:
        """Read a browser binding by opaque server identifier."""

    async def create_flow(self, record: IdentityFlowRecord) -> None:
        """Persist a new identity flow."""

    async def get_flow(self, flow_id: str) -> IdentityFlowRecord | None:
        """Read one identity flow."""

    async def transition_flow(
        self,
        flow_id: str,
        *,
        browser_session_id: str,
        step_id: str,
        expected_step: str,
        allowed_steps: tuple[str, ...],
        status: str,
        state_updates: dict[str, object],
        user_id: str | None = None,
        authorization_resume_uri: str | None = None,
        webauthn_options: dict[str, object] | None = None,
        completed_at: datetime | None = None,
    ) -> IdentityFlowRecord | None:
        """@brief 原子去重并应用一次允许的状态转换 / Atomically apply one allowed transition.

        @param flow_id 流程标识 / Flow identifier.
        @param browser_session_id 绑定的浏览器会话 / Bound browser session.
        @param step_id 客户端幂等步骤标识 / Client idempotency step identifier.
        @param expected_step 当前允许的步骤类型 / Currently allowed step kind.
        @param allowed_steps 转换后的允许步骤 / Allowed steps after transition.
        @param status 转换后的状态 / Status after transition.
        @param state_updates 私有状态增量 / Private-state delta.
        @param user_id 可选绑定用户 / Optional bound user.
        @param authorization_resume_uri 可选授权恢复 URI / Optional authorization resume URI.
        @param webauthn_options 可选 WebAuthn 参数 / Optional WebAuthn options.
        @param completed_at 进入 completed 状态的时刻 / Instant entering completed state.
        @return 转换后的流程；冲突时为空 / Transitioned flow, or ``None`` on conflict.
        """

    async def processed_step_kind(self, flow_id: str, step_id: str) -> str | None:
        """Return the non-secret kind of an already processed step receipt."""

    async def get_user_by_email(self, normalized_email: str) -> IdentityUserRecord | None:
        """Resolve an account internally without changing public enumeration behavior."""

    async def get_identity_user(self, user_id: str) -> IdentityUserRecord | None:
        """Read an account after a server-authenticated session resolves its opaque ID."""

    async def create_user_with_password(
        self,
        *,
        user: IdentityUserRecord,
        password_authenticator_id: str,
        password_verifier: str,
        now: datetime,
        passkey: IdentityAuthenticatorRecord | None = None,
    ) -> bool:
        """@brief 原子完成首次注册 provision / Atomically complete first-registration provisioning.

        @param user 唯一 identity 用户 / Unique identity user.
        @param password_authenticator_id 密码验证器 ID / Password-authenticator ID.
        @param password_verifier 不可逆密码 verifier / Irreversible password verifier.
        @param now 所有资源共享的创建时刻 / Creation instant shared by all resources.
        @param passkey 可选同时注册 passkey / Optional passkey registered at the same time.
        @return 用户、密码、可选 passkey、个人 Workspace、owner membership 与
            ``default_workspace_id`` 全部提交时为真；唯一性冲突时为假 / True only when the
            user, password, optional passkey, personal Workspace, owner membership, and
            ``default_workspace_id`` all commit; false on a uniqueness conflict.
        """

    async def password_verifier(self, user_id: str) -> str | None:
        """Read the active password verifier for authentication."""

    async def replace_password_and_revoke_sessions(
        self, user_id: str, *, password_verifier: str, now: datetime
    ) -> bool:
        """Atomically replace a password and revoke existing login/token families after recovery."""

    async def create_login_session(self, record: IdentitySessionRecord) -> None:
        """Persist a rotated Authorization Server login session."""

    async def get_login_session(self, session_id: str) -> IdentitySessionRecord | None:
        """Read a login session by opaque identifier."""

    async def bind_browser_user(self, browser_session_id: str, user_id: str) -> None:
        """Bind an authenticated user to the existing browser authorization session."""

    async def list_login_sessions(self, user_id: str) -> list[IdentitySessionRecord]:
        """List active login sessions for one authenticated account."""

    async def revoke_login_session(self, user_id: str, session_id: str, now: datetime) -> bool:
        """Revoke one owned login session and associated refresh families."""

    async def list_authenticators(self, user_id: str) -> list[IdentityAuthenticatorRecord]:
        """List active verifier metadata for one account."""

    async def replace_recovery_codes(
        self,
        user_id: str,
        *,
        authenticator_id: str,
        verifiers: tuple[str, ...],
        now: datetime,
    ) -> None:
        """Atomically revoke old recovery codes and persist a new verifier-only bundle."""

    async def revoke_authenticator(
        self, user_id: str, authenticator_id: str, now: datetime
    ) -> bool:
        """Revoke an owned authenticator only while another recovery path remains."""

    async def add_passkey(self, record: IdentityAuthenticatorRecord) -> bool:
        """Persist one verified unique WebAuthn credential."""

    async def get_passkey_by_credential_id(
        self, credential_id: str
    ) -> IdentityAuthenticatorRecord | None:
        """Resolve an active passkey by its base64url credential identifier."""

    async def update_passkey_sign_count(
        self, authenticator_id: str, *, expected: int, replacement: int, now: datetime
    ) -> bool:
        """Advance a WebAuthn signature counter with optimistic concurrency."""

    async def consume_recovery_code(self, user_id: str, verifier: str, now: datetime) -> bool:
        """Atomically consume one exact recovery-code verifier once."""


class IdentityEmailRateLimitExceeded(RuntimeError):
    """@brief 身份邮件的持久化频控额度已耗尽 / Durable identity-email budget is exhausted."""


class IdentityEmailEnqueueError(RuntimeError):
    """@brief 身份邮件无法原子写入 durable outbox / Identity email cannot enter the durable outbox."""


class IdentityEmailSender(Protocol):
    """@brief 身份邮件的事务型队列端口 / Transactional identity-email queue port.

    @note ``send_*`` 表示邮件已被可靠接纳，而不是 SMTP 已完成；网络投递只能由
        outbox worker 执行。 / ``send_*`` acknowledges durable acceptance, not SMTP delivery;
        only the outbox worker performs network I/O.
    """

    def atomic(self) -> AbstractAsyncContextManager[None]:
        """@brief 打开与身份仓储共享的原子事务 / Open an atomic transaction shared with identity storage.

        @return 正常退出才提交的异步上下文 / Async context committed only on normal exit.
        """

    async def send_verification_code(
        self,
        recipient: str,
        code: str,
        *,
        browser_session_id: str,
        network_identifier: str,
        limit_per_hour: int,
    ) -> None:
        """@brief 原子消费三维额度并入队验证码 / Atomically consume three budgets and enqueue a code.

        @param recipient 规范化收件地址 / Normalized recipient address.
        @param code 单次短期验证码 / Single-use short-lived code.
        @param browser_session_id 设备维度的浏览器绑定 / Browser binding for the device axis.
        @param network_identifier 可信网络维度 / Trusted network axis.
        @param limit_per_hour 每维每小时硬上限 / Per-dimension hourly hard limit.
        @raise IdentityEmailRateLimitExceeded 任一维额度耗尽 / Any dimension is exhausted.
        @raise IdentityEmailEnqueueError 密文无法持久化 / Encrypted payload cannot be persisted.
        """

    async def send_recovery_notification(self, recipient: str) -> None:
        """@brief 原子入队凭据轮换安全通知 / Atomically enqueue a credential-rotation notice.

        @param recipient 已恢复账户的地址 / Address of the recovered account.
        @raise IdentityEmailEnqueueError 密文无法持久化 / Encrypted payload cannot be persisted.
        """


class BreachedPasswordChecker(Protocol):
    """@brief 检查候选密码是否出现在泄露语料中 / Check whether a candidate password appears in breach corpora.

    @note 实现不得记录、持久化或向远端发送明文密码；外部检查应使用 k-anonymity
        或等价的隐私保护协议。 / Implementations must not log, persist, or transmit the
        plaintext password; remote checks should use k-anonymity or an equivalent privacy-preserving
        protocol.
    """

    async def is_breached(self, password: str) -> bool:
        """@brief 返回密码是否已泄露 / Return whether the password is known to be breached.

        @param password 仅在当前调用内存活的候选明文 / Candidate plaintext scoped to this call.
        @return 已出现在泄露语料时为真 / True when present in breach corpora.
        @raise RuntimeError 检查器无法给出可信结论时抛出 / Raised when the checker cannot
            produce a trustworthy decision.
        """


class WorkspaceRepository(Protocol):
    """Read-only current-user and workspace membership projections."""

    async def get_current_user(self, scope: ActorScope) -> dict[str, Any] | None:
        """Read the authenticated actor profile within the asserted scope."""

    async def list_workspaces(self, scope: ActorScope) -> list[dict[str, Any]]:
        """List workspaces authorized by the current identity assertion."""

    async def get_workspace(self, scope: ActorScope, workspace_id: str) -> dict[str, Any] | None:
        """Read one authorized workspace."""

    async def list_workspace_members(
        self, scope: ActorScope, workspace_id: str
    ) -> list[dict[str, Any]]:
        """List members without crossing the asserted workspace."""


class ResumeRepository(Protocol):
    """@brief 简历 Repository 端口 / Resume repository port."""

    async def create_resume(self, scope: ActorScope, record: ResumeRecord) -> None:
        """@brief 保存新简历 / Persist a new resume.

        @param scope workspace 范围 / Workspace scope.
        @param record 简历聚合 / Resume aggregate.
        """

    async def get_resume(self, scope: ActorScope, resume_id: str) -> ResumeRecord | None:
        """@brief 范围内查询简历 / Read a scoped resume.

        @param scope workspace 范围 / Workspace scope.
        @param resume_id 简历 ID / Resume ID.
        @return 聚合或 None / Aggregate or None.
        """

    async def list_resumes(self, scope: ActorScope) -> list[ResumeRecord]:
        """@brief 列出范围内简历 / List scoped resumes.

        @param scope workspace 范围 / Workspace scope.
        @return 简历聚合 / Resume aggregates.
        """

    async def save_resume(self, scope: ActorScope, record: ResumeRecord) -> None:
        """@brief 保存已有简历 / Persist an existing resume.

        @param scope workspace 范围 / Workspace scope.
        @param record 简历聚合 / Resume aggregate.
        """

    async def save_resume_and_job(
        self,
        scope: ActorScope,
        record: ResumeRecord,
        job: Job,
    ) -> None:
        """Atomically persist a Resume revision/idempotency result and its queued render Job."""

    async def commit_resume_workflow(
        self,
        scope: ActorScope,
        record: ResumeRecord,
        knowledge_source: KnowledgeSourceRecord,
        knowledge_job: Job,
        render_job: Job | None,
        *,
        create_resume: bool,
    ) -> None:
        """Atomically accept a Resume revision and all durable derived-work intents."""


class ResumeProposalRepository(Protocol):
    """Persistence port for reviewable Resume AI proposals."""

    async def create_proposal(self, scope: ActorScope, record: ResumeProposalRecord) -> None:
        """Persist a new proposal within the supplied tenant scope."""

    async def get_proposal(
        self, scope: ActorScope, proposal_id: str
    ) -> ResumeProposalRecord | None:
        """Read a proposal without crossing workspace or owner boundaries."""

    async def list_proposals(self, scope: ActorScope, resume_id: str) -> list[ResumeProposalRecord]:
        """List proposals for one scoped Resume in newest-first order."""

    async def save_proposal(self, scope: ActorScope, record: ResumeProposalRecord) -> None:
        """Persist proposal decision state."""


class ResumeKnowledgeBridge(Protocol):
    """@brief 简历到知识来源的内部派生桥 / Internal resume-to-knowledge-source derivation bridge.

    @note 这是应用层内部端口，不是新的 HTTP 或公开 contract。实现必须在相同的
    ``workspace_id`` 与 ``resource_owner_id`` 范围内工作，且只能从已持久化的
    ResumeDocument SIR（Semantic Intermediate Representation，语义中间表示）派生内容。
    """

    async def synchronize_resume(
        self,
        scope: ActorScope,
        document: dict[str, Any],
        request_id: str | None,
    ) -> None:
        """@brief 将一个 Resume revision 同步为其派生 KnowledgeSource / Synchronize one Resume revision into its derived KnowledgeSource.

        @param scope workspace/owner 范围 / Workspace and owner scope.
        @param document 已持久化 ResumeDocument 快照 / Persisted ResumeDocument snapshot.
        @param request_id 可选请求追踪 ID / Optional request trace ID.

        @note 同步可异步提交索引 Job，但不得创建新的公开 API 契约；过载必须保留为
        可观察的来源/Job 状态，而不是丢失派生意图。
        """

    async def prepare_resume_synchronization(
        self,
        scope: ActorScope,
        document: dict[str, Any],
        request_id: str | None,
    ) -> tuple[KnowledgeSourceRecord, Job]:
        """Prepare, but do not persist or dispatch, one revision-pinned ingestion intent."""

    async def dispatch_prepared_ingestion(
        self,
        scope: ActorScope,
        source: KnowledgeSourceRecord,
        job: Job,
    ) -> None:
        """Dispatch a previously committed ingestion intent."""


class AgentRepository(Protocol):
    """@brief Agent Repository 端口 / Agent repository port."""

    async def create_conversation(self, scope: ActorScope, record: ConversationRecord) -> None:
        """@brief 保存会话 / Persist a conversation.

        @param scope workspace 范围 / Workspace scope.
        @param record 会话聚合 / Conversation aggregate.
        """

    async def get_conversation(
        self, scope: ActorScope, conversation_id: str
    ) -> ConversationRecord | None:
        """@brief 范围内查询会话 / Read a scoped conversation.

        @param scope workspace 范围 / Workspace scope.
        @param conversation_id 会话 ID / Conversation ID.
        @return 会话或 None / Conversation or None.
        """

    async def create_message(self, scope: ActorScope, record: MessageRecord) -> None:
        """@brief 保存消息 / Persist a message.

        @param scope workspace 范围 / Workspace scope.
        @param record 消息实体 / Message entity.
        """

    async def get_message(self, scope: ActorScope, message_id: str) -> MessageRecord | None:
        """@brief 范围内查询消息 / Read a scoped message.

        @param scope workspace 范围 / Workspace scope.
        @param message_id 消息 ID / Message ID.
        @return 消息或 None / Message or None.
        """

    async def list_messages(self, scope: ActorScope, conversation_id: str) -> list[MessageRecord]:
        """@brief 列出会话消息 / List conversation messages.

        @param scope workspace 范围 / Workspace scope.
        @param conversation_id 会话 ID / Conversation ID.
        @return 消息列表 / Message list.
        """

    async def create_run(self, scope: ActorScope, record: AgentRunRecord) -> None:
        """@brief 保存 Agent Run / Persist an Agent Run.

        @param scope workspace 范围 / Workspace scope.
        @param record Run 记录 / Run record.
        """

    async def get_run(self, scope: ActorScope, run_id: str) -> AgentRunRecord | None:
        """@brief 范围内查询 Run / Read a scoped run.

        @param scope workspace 范围 / Workspace scope.
        @param run_id Run ID / Run ID.
        @return Run 或 None / Run or None.
        """

    async def save_run(self, scope: ActorScope, record: AgentRunRecord) -> None:
        """@brief 保存 Run 状态 / Persist Run state.

        @param scope workspace 范围 / Workspace scope.
        @param record Run 记录 / Run record.
        """


class InterviewRepository(Protocol):
    """@brief 面试 Repository 端口 / Interview repository port."""

    async def create_session(self, scope: ActorScope, record: InterviewSessionRecord) -> None:
        """@brief 保存面试会话 / Persist an interview session.

        @param scope workspace 范围 / Workspace scope.
        @param record Session 记录 / Session record.
        """

    async def get_session(
        self, scope: ActorScope, session_id: str
    ) -> InterviewSessionRecord | None:
        """@brief 范围内查询面试 / Read a scoped interview session.

        @param scope workspace 范围 / Workspace scope.
        @param session_id Session ID / Session ID.
        @return Session 或 None / Session or None.
        """

    async def list_sessions(self, scope: ActorScope) -> list[InterviewSessionRecord]:
        """List interview sessions within the supplied scope."""

    async def save_session(self, scope: ActorScope, record: InterviewSessionRecord) -> None:
        """@brief 保存面试状态 / Persist interview state.

        @param scope workspace 范围 / Workspace scope.
        @param record Session 记录 / Session record.
        """

    async def save_report(self, scope: ActorScope, report: dict[str, Any]) -> None:
        """@brief 保存面试报告 / Persist an interview report.

        @param scope workspace 范围 / Workspace scope.
        @param report 报告对象 / Report object.
        """

    async def get_report(self, scope: ActorScope, report_id: str) -> dict[str, Any] | None:
        """@brief 范围内查询报告 / Read a scoped interview report.

        @param scope workspace 范围 / Workspace scope.
        @param report_id 报告 ID / Report ID.
        @return 报告或 None / Report or None.
        """


class KnowledgeRepository(Protocol):
    """@brief 知识库 Repository 端口 / Knowledge repository port."""

    async def create_source(self, scope: ActorScope, record: KnowledgeSourceRecord) -> None:
        """@brief 保存知识来源 / Persist a knowledge source.

        @param scope workspace 范围 / Workspace scope.
        @param record 来源聚合 / Source aggregate.
        """

    async def get_source(self, scope: ActorScope, source_id: str) -> KnowledgeSourceRecord | None:
        """@brief 范围内查询来源 / Read a scoped source.

        @param scope workspace 范围 / Workspace scope.
        @param source_id 来源 ID / Source ID.
        @return 来源或 None / Source or None.
        """

    async def list_sources(self, scope: ActorScope) -> list[KnowledgeSourceRecord]:
        """@brief 列出范围内来源 / List scoped sources.

        @param scope workspace 范围 / Workspace scope.
        @return 来源聚合 / Source aggregates.
        """

    async def save_source(self, scope: ActorScope, record: KnowledgeSourceRecord) -> None:
        """@brief 保存来源状态 / Persist source state.

        @param scope workspace 范围 / Workspace scope.
        @param record 来源聚合 / Source aggregate.
        """

    async def save_source_if_revision(
        self,
        scope: ActorScope,
        record: KnowledgeSourceRecord,
        expected_revision: int,
    ) -> bool:
        """Compare-and-set a source update across workers."""

    async def save_source_and_job(
        self,
        scope: ActorScope,
        record: KnowledgeSourceRecord,
        job: Job,
    ) -> None:
        """Atomically publish a knowledge-source state transition and its Job state."""

    async def get_embedding_space(self, scope: ActorScope) -> EmbeddingSpace | None:
        """@brief 查询范围内默认 embedding space / Read the scoped default embedding space.

        @param scope workspace 范围 / Workspace scope.
        @return embedding space 或 None / Embedding space or None.
        """

    async def save_embedding_space(self, scope: ActorScope, space: EmbeddingSpace) -> None:
        """@brief 保存不可变 embedding space / Persist an immutable embedding space.

        @param scope workspace 范围 / Workspace scope.
        @param space embedding space / Embedding space.
        """

    async def rank_chunks_by_vector(
        self,
        scope: ActorScope,
        chunk_ids: list[str],
        embedding_space_id: str,
        query_vector: tuple[float, ...],
        limit: int,
    ) -> list[tuple[str, float]]:
        """Rank an authorized chunk subset with the configured vector space."""


class KnowledgeBlobStorage(Protocol):
    """Private binary storage used by file-backed knowledge sources."""

    async def put(
        self,
        scope: ActorScope,
        file_id: str,
        filename: str,
        content_type: str,
        content: bytes,
    ) -> StoredKnowledgeBlob:
        """Persist validated bytes and return opaque storage metadata."""

    async def read(self, scope: ActorScope, storage_key: str) -> bytes:
        """Read bytes only when the key belongs to the supplied actor scope."""

    async def delete(self, scope: ActorScope, storage_key: str) -> None:
        """Delete a blob owned by the supplied actor scope if it exists."""


class KnowledgeFileParser(Protocol):
    """Parser boundary for bounded, supported knowledge files."""

    async def parse(
        self,
        filename: str,
        content_type: str,
        content: bytes,
    ) -> ParsedKnowledgeDocument:
        """Parse bytes into semantic parts or raise a stable domain error."""


class EmbeddingProvider(Protocol):
    """Replaceable embedding adapter with an immutable configured dimension."""

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        """Return one normalized vector for each input text."""


class JobRepository(Protocol):
    """@brief Job Repository 端口 / Job repository port."""

    async def create_job(self, scope: ActorScope, job: Job) -> None:
        """@brief 保存 Job / Persist a job.

        @param scope workspace 范围 / Workspace scope.
        @param job Job 实体 / Job entity.
        """

    async def get_job(self, scope: ActorScope, job_id: str) -> Job | None:
        """@brief 范围内查询 Job / Read a scoped job.

        @param scope workspace 范围 / Workspace scope.
        @param job_id Job ID / Job ID.
        @return Job 或 None / Job or None.
        """

    async def claim_job(
        self,
        scope: ActorScope,
        job_id: str,
        stale_after_seconds: int = 900,
    ) -> Job | None:
        """Atomically claim queued work or reclaim a stale running lease."""

    async def save_job(self, scope: ActorScope, job: Job) -> None:
        """@brief 保存 Job 状态 / Persist job state.

        @param scope workspace 范围 / Workspace scope.
        @param job Job 实体 / Job entity.
        """


class ArtifactRepository(Protocol):
    """@brief 渲染产物 Repository 端口 / Render artifact repository port."""

    async def save_artifact(
        self,
        scope: ActorScope,
        artifact: dict[str, Any],
        content: bytes,
        source_map: dict[str, Any] | None,
    ) -> None:
        """@brief 保存渲染产物 / Persist a render artifact.

        @param scope workspace 范围 / Workspace scope.
        @param artifact 公开产物元数据 / Public artifact metadata.
        @param content 二进制内容 / Binary content.
        @param source_map 可选 source map / Optional source map.
        """

    async def save_artifact_and_job(
        self,
        scope: ActorScope,
        artifact: dict[str, Any],
        content: bytes,
        source_map: dict[str, Any] | None,
        job: Job,
    ) -> None:
        """Atomically publish one immutable artifact and the successful render Job."""

    async def get_artifact(
        self,
        scope: ActorScope,
        artifact_id: str,
    ) -> tuple[dict[str, Any], bytes, dict[str, Any] | None] | None:
        """@brief 范围内查询渲染产物 / Read a scoped render artifact.

        @param scope workspace 范围 / Workspace scope.
        @param artifact_id 产物 ID / Artifact ID.
        @return metadata、内容、source map 或 None / Metadata, content, source map, or None.
        """

    async def list_artifacts(
        self,
        scope: ActorScope,
        resume_id: str,
    ) -> list[dict[str, Any]]:
        """List artifact metadata for one scoped Resume in newest-first order."""


class Renderer(Protocol):
    """@brief 私有渲染器端口 / Private renderer port."""

    async def render(self, document: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
        """@brief 渲染 PDF 及 source map / Render a PDF and source map.

        @param document ResumeDocument SIR / ResumeDocument SIR.
        @return PDF 字节和 source map / PDF bytes and source map.
        """


class ModelProvider(Protocol):
    """@brief Provider 无关的模型端口 / Provider-independent model port."""

    def stream_text(self, prompt: str, request: dict[str, Any]) -> AsyncIterator[str]:
        """@brief 流式产生文本 / Stream text.

        @param prompt 已授权的输入文本 / Authorized input text.
        @param request 推理意图 / Inference intent.
        @return 文本分片异步迭代器 / Async iterator of text chunks.
        """


class TelemetryWriter(Protocol):
    """@brief cancellation-cooperative telemetry 持久化端口 / Cancellation-cooperative telemetry persistence port.

    @note 实现不得吞掉 ``CancelledError``，并必须让被取消的 I/O 有界收敛；Python task
    不能被外部强制终止，因此进程级关停上限依赖这一端口契约。/ Implementations must not
    swallow ``CancelledError`` and must let cancelled I/O converge within a bound. Python tasks
    cannot be forcibly terminated, so the process-level shutdown bound depends on this port contract.
    """

    async def write_batch(self, records: list[TelemetrySignal]) -> None:
        """@brief 批量写 telemetry / Write telemetry as a batch.

        @param records 已过滤记录 / Filtered records.

        @note 必须传播 cancellation 且不得启动脱离调用生命周期的后台写入 / Cancellation
        must propagate and no write may outlive this call in an unowned background task.
        """


class ObservabilityRecorder(Protocol):
    """@brief 应用层可依赖的非阻塞 observability 端口 / Non-blocking observability application port."""

    def record_metric(
        self,
        name: str,
        value: float,
        scope: ActorScope | None,
        request_id: str | None,
        attributes: dict[str, AttributeValue],
        *,
        service: str = "backend.worker",
        metric_type: MetricType = MetricType.COUNTER,
        unit: str = "{event}",
        source: SignalSource = SignalSource.BACKEND,
        client_event_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> bool:
        """@brief 非阻塞提交 metric / Non-blockingly submit a metric.

        @param name 稳定仪器名 / Stable instrument name.
        @param value 有限观测值 / Finite observed value.
        @param scope 可空租户范围 / Optional tenant scope.
        @param request_id 请求关联 ID / Request correlation ID.
        @param attributes 低基数属性 / Low-cardinality attributes.
        @param service 稳定服务名 / Stable service name.
        @param metric_type 仪器类型 / Instrument type.
        @param unit 规范单位 / Canonical unit.
        @param source 可信来源 / Trusted producer.
        @param client_event_id 前端幂等 ID / Frontend idempotency ID.
        @param occurred_at 事件发生时间 / Event occurrence time.
        @return 成功进入队列时为真 / True when admitted to the queue.
        """

    def record_log(
        self,
        name: str,
        severity_number: SeverityNumber,
        severity_text: str,
        scope: ActorScope | None,
        request_id: str | None,
        attributes: dict[str, AttributeValue],
        *,
        service: str = "backend",
        source: SignalSource = SignalSource.BACKEND,
        client_event_id: str | None = None,
        occurred_at: datetime | None = None,
        event_id: str | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
        parent_span_id: str | None = None,
    ) -> bool:
        """@brief 非阻塞提交稳定日志事件 / Non-blockingly submit a stable log event.

        @param name 稳定事件名 / Stable event name.
        @param severity_number OpenTelemetry 严重度 / OpenTelemetry severity number.
        @param severity_text 规范严重度文本 / Canonical severity text.
        @param scope 可空租户范围 / Optional tenant scope.
        @param request_id 请求关联 ID / Request correlation ID.
        @param attributes 低基数属性 / Low-cardinality attributes.
        @param service 稳定服务名 / Stable service name.
        @param source 可信来源 / Trusted producer.
        @param client_event_id 前端幂等 ID / Frontend idempotency ID.
        @param occurred_at 事件发生时间 / Event occurrence time.
        @param event_id 服务端已有事件 ID / Existing server event ID.
        @param trace_id 可选 W3C trace ID / Optional W3C trace ID.
        @param span_id 可选 W3C span ID / Optional W3C span ID.
        @param parent_span_id 可选 W3C parent span ID / Optional W3C parent span ID.
        @return 成功进入队列时为真 / True when admitted to the queue.
        """

    def record_span(
        self,
        name: str,
        duration_ms: float,
        status: SpanStatus,
        scope: ActorScope | None,
        request_id: str | None,
        attributes: dict[str, AttributeValue],
        *,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        service: str = "backend.api",
        occurred_at: datetime | None = None,
    ) -> bool:
        """@brief 非阻塞提交已完成 span / Non-blockingly submit a completed span.

        @param occurred_at span 开始时间 / Span start time.
        @return 成功进入队列时为真 / True when admitted to the queue.
        """
