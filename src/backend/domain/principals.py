"""@brief API v2 身份与资源标识原语 / API v2 principal and resource identity primitives."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, NewType

if TYPE_CHECKING:
    from backend.domain.workspaces import WorkspaceRole


UserId = NewType("UserId", str)
"""@brief 用户不透明标识 / Opaque user identifier."""

WorkspaceId = NewType("WorkspaceId", str)
"""@brief Workspace 不透明标识 / Opaque workspace identifier."""

MembershipId = NewType("MembershipId", str)
"""@brief 成员关系不透明标识 / Opaque membership identifier."""

InvitationId = NewType("InvitationId", str)
"""@brief 邀请不透明标识 / Opaque invitation identifier."""

Subject = NewType("Subject", str)
"""@brief OIDC subject 标识 / OIDC subject identifier."""

ClientId = NewType("ClientId", str)
"""@brief OAuth public client 标识 / OAuth public-client identifier."""

Scope = NewType("Scope", str)
"""@brief OAuth scope 原子值 / Atomic OAuth scope value."""


class DomainInvariantError(ValueError):
    """@brief 表示领域值破坏不变量 / Indicates a violated domain invariant."""


class WorkspaceAction(StrEnum):
    """@brief 需要 Workspace 成员授权的操作 / Operations requiring workspace membership."""

    READ = "workspace.read"
    UPDATE = "workspace.update"
    DELETE = "workspace.delete"
    LIST_MEMBERS = "workspace.members.list"
    READ_MEMBER = "workspace.members.read"
    UPDATE_MEMBER = "workspace.members.update"
    REMOVE_MEMBER = "workspace.members.remove"
    LIST_INVITATIONS = "workspace.invitations.list"
    READ_INVITATION = "workspace.invitations.read"
    CREATE_INVITATION = "workspace.invitations.create"
    REVOKE_INVITATION = "workspace.invitations.revoke"
    LIST_RESUMES = "resume.list"
    READ_RESUME = "resume.read"
    CREATE_RESUME = "resume.create"
    UPDATE_RESUME = "resume.update"
    DELETE_RESUME = "resume.delete"
    READ_RESUME_REVISIONS = "resume.revisions.read"
    APPLY_RESUME_OPERATIONS = "resume.operations.apply"
    CREATE_RESUME_IMPORT_JOB = "resume.import_jobs.create"
    CREATE_RESUME_RESTORE_JOB = "resume.restore_jobs.create"
    CREATE_RESUME_RENDER_JOB = "resume.render_jobs.create"
    LIST_RESUME_PROPOSALS = "resume.proposals.list"
    READ_RESUME_PROPOSAL = "resume.proposals.read"
    DECIDE_RESUME_PROPOSAL = "resume.proposals.decide"
    LIST_CONNECTIONS = "connection.list"
    CREATE_CONNECTION_AUTHORIZATION_SESSION = "connection.authorization_sessions.create"
    CREATE_CONNECTION = "connection.create"
    DELETE_CONNECTION = "connection.delete"
    LIST_KNOWLEDGE_SOURCES = "knowledge_source.list"
    READ_KNOWLEDGE_SOURCE = "knowledge_source.read"
    CREATE_KNOWLEDGE_SOURCE = "knowledge_source.create"
    UPDATE_KNOWLEDGE_SOURCE = "knowledge_source.update"
    DELETE_KNOWLEDGE_SOURCE = "knowledge_source.delete"
    READ_KNOWLEDGE_VERSIONS = "knowledge_source.versions.read"
    CREATE_KNOWLEDGE_VERSION = "knowledge_source.versions.create"
    CREATE_UPLOAD_SESSION = "upload_session.create"
    COMPLETE_UPLOAD_SESSION = "upload_session.complete"
    CREATE_KNOWLEDGE_JOB = "knowledge_source.jobs.create"
    SEARCH_KNOWLEDGE = "knowledge.search"
    EVALUATE_KNOWLEDGE_ACCESS = "knowledge.access.evaluate"
    LIST_CONVERSATIONS = "conversation.list"
    CREATE_CONVERSATION = "conversation.create"
    READ_CONVERSATION = "conversation.read"
    UPDATE_CONVERSATION = "conversation.update"
    DELETE_CONVERSATION = "conversation.delete"
    LIST_MESSAGES = "conversation.messages.list"
    CREATE_MESSAGE = "conversation.messages.create"
    CREATE_AGENT_RUN = "agent_run.create"
    READ_AGENT_RUN = "agent_run.read"
    CANCEL_AGENT_RUN = "agent_run.cancel"
    READ_TOOL_APPROVAL = "tool_approval.read"
    DECIDE_TOOL_APPROVAL = "tool_approval.decide"
    LIST_INTERVIEW_SCENARIOS = "interview_scenario.list"
    CREATE_INTERVIEW_SCENARIO = "interview_scenario.create"
    READ_INTERVIEW_SCENARIO = "interview_scenario.read"
    UPDATE_INTERVIEW_SCENARIO = "interview_scenario.update"
    LIST_INTERVIEW_SESSIONS = "interview_session.list"
    CREATE_INTERVIEW_SESSION = "interview_session.create"
    READ_INTERVIEW_SESSION = "interview_session.read"
    CREATE_INTERVIEW_CONNECTION = "interview_session.connection.create"
    END_INTERVIEW_SESSION = "interview_session.end"
    READ_INTERVIEW_TRANSCRIPT = "interview_session.transcript.read"
    CREATE_INTERVIEW_REPORT_JOB = "interview_session.report.create"
    READ_INTERVIEW_REPORT = "interview_report.read"
    LIST_JOBS = "job.list"
    READ_JOB = "job.read"
    CANCEL_JOB = "job.cancel"
    LIST_ARTIFACTS = "artifact.list"
    READ_ARTIFACT = "artifact.read"
    READ_ARTIFACT_CONTENT = "artifact.content.read"
    READ_ARTIFACT_SOURCE_MAP = "artifact.source_map.read"
    READ_EVENTS = "event.read"
    LIST_AUDIT_EVENTS = "audit_event.list"


@dataclass(frozen=True, slots=True)
class ResourceMeta[IdT: str]:
    """@brief 可组合的持久资源元数据 / Composable persistent-resource metadata.

    @param id 领域专用的不透明标识 / Domain-specific opaque identifier.
    @param revision 从一开始的领域版本 / One-based domain revision.
    @param created_at 带时区的创建时刻 / Timezone-aware creation instant.
    @param updated_at 带时区的最近修改时刻 / Timezone-aware last-modified instant.
    """

    id: IdT
    revision: int
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验资源元数据 / Validate resource metadata.

        @raise DomainInvariantError 标识、版本或时间无效时抛出 / Raised for invalid identity,
            revision, or timestamps.
        """
        _require_identifier(self.id, "resource id")
        if self.revision < 1:
            raise DomainInvariantError("resource revision must be at least one")
        _require_aware(self.created_at, "created_at")
        _require_aware(self.updated_at, "updated_at")
        if self.updated_at < self.created_at:
            raise DomainInvariantError("updated_at cannot precede created_at")

    def advance(self, updated_at: datetime) -> ResourceMeta[IdT]:
        """@brief 生成下一资源版本 / Produce the next resource revision.

        @param updated_at 新版本的修改时刻 / Modification instant of the new revision.
        @return 保留标识与创建时间的新元数据 / New metadata retaining identity and creation time.
        """
        _require_aware(updated_at, "updated_at")
        if updated_at < self.updated_at:
            raise DomainInvariantError("resource update time cannot move backwards")
        return ResourceMeta(self.id, self.revision + 1, self.created_at, updated_at)


@dataclass(frozen=True, slots=True)
class TokenPrincipal:
    """@brief 已完成密码学验证的 access-token 投影 / Verified access-token projection.

    @param user_id 仅由本服务签名 token 携带的本地用户标识 / Local user identifier carried only by this service's signed token.
    @param subject 发行方稳定 subject / Issuer-stable subject.
    @param client_id 获得 token 的 public client / Public client that obtained the token.
    @param scopes token 实际授予的 scope / Scopes actually granted to the token.
    @note 该类型不证明本地用户仍然存在 / This type does not prove a local user still exists.
    """

    user_id: UserId
    subject: Subject
    client_id: ClientId
    scopes: frozenset[Scope]

    def __post_init__(self) -> None:
        """@brief 校验 token 投影 / Validate the token projection.

        @raise DomainInvariantError subject、client 或 scope 无效时抛出 / Raised for invalid
            subject, client, or scope values.
        """
        _require_identifier(self.user_id, "user_id")
        _require_identifier(self.subject, "subject")
        _require_identifier(self.client_id, "client_id")
        if any(not scope or scope.strip() != scope for scope in self.scopes):
            raise DomainInvariantError("token scopes must be non-empty canonical values")

    def has_scope(self, scope: Scope) -> bool:
        """@brief 判断 token 是否含精确 scope / Test whether the token has an exact scope.

        @param scope 待检查 scope / Scope to check.
        @return 精确存在时为真 / True when present exactly.
        """
        return scope in self.scopes


@dataclass(frozen=True, slots=True)
class AuthenticatedActor:
    """@brief 已绑定本地账户的 token principal / Token principal bound to a local account.

    @param user_id 本地用户标识 / Local user identifier.
    @param principal 经验证的 token principal / Verified token principal.
    """

    user_id: UserId
    principal: TokenPrincipal

    def __post_init__(self) -> None:
        """@brief 校验本地用户标识 / Validate the local user identifier.

        @raise DomainInvariantError 用户标识为空时抛出 / Raised when the user identifier is empty.
        """
        _require_identifier(self.user_id, "user_id")


_WORKSPACE_CONTEXT_SEAL = object()
"""@brief Workspace 授权上下文构造密封 / Construction seal for workspace authorization contexts."""


@dataclass(frozen=True, slots=True, init=False)
class WorkspaceAccessContext:
    """@brief 一次精确 Workspace 操作的授权证明 / Authorization proof for one exact action.

    @param actor 已认证本地 actor / Authenticated local actor.
    @param workspace_id 路径选择的 Workspace / Workspace selected by the path.
    @param membership_id 授权所依据的成员关系 / Membership on which authorization was based.
    @param role 授权时读取的角色快照 / Role snapshot read during authorization.
    @param action 本次证明唯一允许的操作 / The only action authorized by this proof.
    @note 构造器由 application authorizer 密封；调用方不能自行声称 Workspace 上下文。
        / Construction is sealed for the application authorizer; callers cannot assert context.
    """

    actor: AuthenticatedActor
    workspace_id: WorkspaceId
    membership_id: MembershipId
    role: WorkspaceRole
    action: WorkspaceAction

    def __init__(
        self,
        actor: AuthenticatedActor,
        workspace_id: WorkspaceId,
        membership_id: MembershipId,
        role: WorkspaceRole,
        action: WorkspaceAction,
        *,
        _seal: object,
    ) -> None:
        """@brief 仅接受 authorizer 的密封构造 / Accept only sealed authorizer construction.

        @param actor 已认证 actor / Authenticated actor.
        @param workspace_id 已授权 Workspace / Authorized workspace.
        @param membership_id 授权成员关系 / Authorizing membership.
        @param role 授权角色 / Authorizing role.
        @param action 已授权操作 / Authorized action.
        @param _seal 模块私有构造密封 / Module-private construction seal.
        @raise TypeError 调用方绕过 authorizer 构造时抛出 / Raised on construction bypass.
        """
        if _seal is not _WORKSPACE_CONTEXT_SEAL:
            raise TypeError("WorkspaceAccessContext can only be issued by AccessAuthorizer")
        object.__setattr__(self, "actor", actor)
        object.__setattr__(self, "workspace_id", workspace_id)
        object.__setattr__(self, "membership_id", membership_id)
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "action", action)


def _issue_workspace_access_context(
    actor: AuthenticatedActor,
    workspace_id: WorkspaceId,
    membership_id: MembershipId,
    role: WorkspaceRole,
    action: WorkspaceAction,
) -> WorkspaceAccessContext:
    """@brief 为集中 authorizer 签发上下文 / Issue context for the central authorizer.

    @param actor 已认证 actor / Authenticated actor.
    @param workspace_id 已授权 Workspace / Authorized workspace.
    @param membership_id 授权成员关系 / Authorizing membership.
    @param role 授权角色 / Authorizing role.
    @param action 已授权操作 / Authorized action.
    @return 密封的操作级上下文 / Sealed action-level context.
    @note 这是 application authorizer 的内部协作入口；业务服务不得直接调用。
        / This is an internal collaborator for the application authorizer, not business services.
    """
    return WorkspaceAccessContext(
        actor,
        workspace_id,
        membership_id,
        role,
        action,
        _seal=_WORKSPACE_CONTEXT_SEAL,
    )


def _require_identifier(value: str, label: str) -> None:
    """@brief 校验非空不透明标识 / Validate a non-empty opaque identifier.

    @param value 标识值 / Identifier value.
    @param label 错误标签 / Error label.
    @raise DomainInvariantError 标识为空或含外围空白时抛出 / Raised for empty or padded IDs.
    """
    if not value or value.strip() != value:
        raise DomainInvariantError(f"{label} must be a non-empty canonical value")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 校验带时区时间 / Validate a timezone-aware datetime.

    @param value 待校验时间 / Datetime to validate.
    @param label 错误标签 / Error label.
    @raise DomainInvariantError 时间不含 UTC offset 时抛出 / Raised when no UTC offset exists.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainInvariantError(f"{label} must be timezone-aware")


__all__ = [
    "AuthenticatedActor",
    "ClientId",
    "DomainInvariantError",
    "InvitationId",
    "MembershipId",
    "ResourceMeta",
    "Scope",
    "Subject",
    "TokenPrincipal",
    "UserId",
    "WorkspaceAccessContext",
    "WorkspaceAction",
    "WorkspaceId",
]
