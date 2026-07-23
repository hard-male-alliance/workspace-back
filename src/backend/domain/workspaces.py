"""@brief API v2 Workspace、成员与邀请领域模型 / API v2 workspace domain models."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum

from backend.domain.principals import (
    DomainInvariantError,
    InvitationId,
    MembershipId,
    ResourceMeta,
    UserId,
    WorkspaceId,
)


class WorkspaceRole(StrEnum):
    """@brief Workspace 基础角色 / Workspace base roles."""

    OWNER = "owner"
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"


class MemberStatus(StrEnum):
    """@brief 成员关系状态 / Membership states."""

    ACTIVE = "active"
    SUSPENDED = "suspended"


class InvitationStatus(StrEnum):
    """@brief Workspace 邀请状态 / Workspace invitation states."""

    PENDING = "pending"
    ACCEPTED = "accepted"
    REVOKED = "revoked"
    EXPIRED = "expired"


class WorkspacePlan(StrEnum):
    """@brief Workspace 产品计划 / Workspace product plans."""

    PERSONAL = "personal"
    TEAM = "team"
    ENTERPRISE = "enterprise"


class DataRegion(StrEnum):
    """@brief Workspace 数据驻留区域 / Workspace data-residency regions."""

    CN = "cn"
    GLOBAL = "global"
    PRIVATE_DEPLOYMENT = "private_deployment"


class SoleOwnerViolation(DomainInvariantError):
    """@brief 拒绝移除最后一个活跃 owner / Refuses removal of the last active owner."""


class InvalidStateTransition(DomainInvariantError):
    """@brief 拒绝非法领域状态迁移 / Refuses an invalid domain-state transition."""


_SLUG_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
"""@brief API v2 Workspace slug 语法 / API v2 workspace slug syntax."""


@dataclass(frozen=True, slots=True)
class Workspace:
    """@brief Workspace 租户根 / Workspace tenant root.

    @param meta 可组合资源元数据 / Composable resource metadata.
    @param name Workspace 显示名 / Workspace display name.
    @param slug 稳定 URL 友好名称 / Stable URL-friendly name.
    @param plan 产品计划 / Product plan.
    @param data_region 数据驻留区域 / Data-residency region.
    """

    meta: ResourceMeta[WorkspaceId]
    name: str
    slug: str
    plan: WorkspacePlan
    data_region: DataRegion

    def __post_init__(self) -> None:
        """@brief 校验 Workspace 字段 / Validate workspace fields.

        @raise DomainInvariantError 名称或 slug 不符合契约时抛出 / Raised for invalid name or slug.
        """
        if not 1 <= len(self.name) <= 120 or self.name.strip() != self.name:
            raise DomainInvariantError("workspace name must be canonical and 1 to 120 characters")
        if _SLUG_PATTERN.fullmatch(self.slug) is None:
            raise DomainInvariantError("workspace slug does not satisfy the API v2 grammar")

    def revise(self, *, name: str, slug: str, updated_at: datetime) -> Workspace:
        """@brief 修改可变 Workspace 元数据 / Revise mutable workspace metadata.

        @param name 完整目标显示名 / Complete desired display name.
        @param slug 完整目标 slug / Complete desired slug.
        @param updated_at 修改时刻 / Modification instant.
        @return 下一版本 Workspace / Next workspace revision.
        """
        return replace(self, meta=self.meta.advance(updated_at), name=name, slug=slug)


@dataclass(frozen=True, slots=True)
class Membership:
    """@brief 用户与 Workspace 的角色关系 / User-to-workspace role relationship.

    @param meta 成员资源元数据 / Membership resource metadata.
    @param workspace_id 所属 Workspace / Owning workspace.
    @param user_id 成员用户 / Member user.
    @param display_name 成员列表显示快照 / Member-list display snapshot.
    @param role 基础角色 / Base role.
    @param status 成员状态 / Membership status.
    """

    meta: ResourceMeta[MembershipId]
    workspace_id: WorkspaceId
    user_id: UserId
    display_name: str
    role: WorkspaceRole
    status: MemberStatus

    def __post_init__(self) -> None:
        """@brief 校验成员投影 / Validate the membership projection.

        @raise DomainInvariantError 标识或显示名无效时抛出 / Raised for invalid identity or name.
        """
        if not self.workspace_id or not self.user_id:
            raise DomainInvariantError("membership requires workspace and user identifiers")
        if not 1 <= len(self.display_name) <= 120 or self.display_name.strip() != self.display_name:
            raise DomainInvariantError(
                "member display name must be canonical and 1 to 120 characters"
            )

    def revise(
        self,
        *,
        role: WorkspaceRole | None,
        status: MemberStatus | None,
        active_owner_count: int,
        updated_at: datetime,
    ) -> Membership:
        """@brief 应用普通 member PATCH 语义 / Apply ordinary member PATCH semantics.

        @param role 可选目标角色；owner 永远不是普通 PATCH 的输入 / Optional target role; owner
            is never an ordinary PATCH input.
        @param status 可选目标状态 / Optional target status.
        @param active_owner_count 修改前活跃 owner 数 / Active-owner count before the change.
        @param updated_at 修改时刻 / Modification instant.
        @return 下一版本成员关系 / Next membership revision.
        @raise SoleOwnerViolation 修改会失去唯一活跃 owner 时抛出 / Raised on loss of sole owner.
        @raise DomainInvariantError 普通 PATCH 尝试产生 owner 或无实际修改时抛出 / Raised when
            ordinary PATCH tries to create an owner or makes no change.
        """
        if role is WorkspaceRole.OWNER:
            raise DomainInvariantError("ordinary member PATCH cannot grant the owner role")
        target_role = self.role if role is None else role
        target_status = self.status if status is None else status
        if target_role is self.role and target_status is self.status:
            raise DomainInvariantError("member PATCH must change role or status")
        relinquishes_owner = (
            self.role is WorkspaceRole.OWNER
            and self.status is MemberStatus.ACTIVE
            and (target_role is not WorkspaceRole.OWNER or target_status is not MemberStatus.ACTIVE)
        )
        if relinquishes_owner and active_owner_count <= 1:
            raise SoleOwnerViolation("cannot demote or suspend the sole active owner")
        return replace(
            self,
            meta=self.meta.advance(updated_at),
            role=target_role,
            status=target_status,
        )

    def ensure_removable(self, *, active_owner_count: int) -> None:
        """@brief 验证成员可被删除 / Ensure the membership may be removed.

        @param active_owner_count 删除前活跃 owner 数 / Active-owner count before deletion.
        @raise SoleOwnerViolation 删除会移除唯一活跃 owner 时抛出 / Raised on sole-owner removal.
        """
        if (
            self.role is WorkspaceRole.OWNER
            and self.status is MemberStatus.ACTIVE
            and active_owner_count <= 1
        ):
            raise SoleOwnerViolation("cannot remove the sole active owner")


@dataclass(frozen=True, slots=True)
class Invitation:
    """@brief 由 status 判别并穷举校验的 Workspace 邀请 / Discriminated workspace invitation.

    @param meta 邀请资源元数据 / Invitation resource metadata.
    @param workspace_id 所属 Workspace / Owning workspace.
    @param email 规范化完整收件地址；API adapter 只投影 hint / Canonical full recipient address;
        the API adapter projects only a hint.
    @param role 接受后授予的非 owner 角色 / Non-owner role granted on acceptance.
    @param status 状态判别值 / State discriminator.
    @param expires_at 到期时刻 / Expiration instant.
    @param accepted_by 接受用户，仅 accepted 非空 / Accepting user, present only when accepted.
    @param resolved_at 状态离开 pending 的时刻 / Instant at which pending was resolved.
    """

    meta: ResourceMeta[InvitationId]
    workspace_id: WorkspaceId
    email: str
    role: WorkspaceRole
    status: InvitationStatus
    expires_at: datetime
    accepted_by: UserId | None = None
    resolved_at: datetime | None = None

    def __post_init__(self) -> None:
        """@brief 校验邀请状态判别式 / Validate invitation discriminants.

        @raise DomainInvariantError 角色、时间或状态关联字段无效时抛出 / Raised for invalid role,
            timing, or state-associated fields.
        """
        if self.role is WorkspaceRole.OWNER:
            raise DomainInvariantError("invitations cannot grant the owner role")
        if not self.email or len(self.email) > 320 or self.email.strip() != self.email:
            raise DomainInvariantError(
                "invitation email must be canonical and at most 320 characters"
            )
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise DomainInvariantError("invitation expiry must be timezone-aware")
        if self.expires_at <= self.meta.created_at:
            raise DomainInvariantError("invitation expiry must follow creation")
        if self.status is InvitationStatus.PENDING:
            if self.accepted_by is not None or self.resolved_at is not None:
                raise DomainInvariantError("pending invitation cannot have resolution fields")
        elif self.status is InvitationStatus.ACCEPTED:
            if self.accepted_by is None or self.resolved_at is None:
                raise DomainInvariantError("accepted invitation requires actor and resolution time")
        elif self.accepted_by is not None or self.resolved_at is None:
            raise DomainInvariantError(
                "revoked or expired invitation requires only resolution time"
            )

    def accept(
        self,
        *,
        user_id: UserId,
        actor_email: str,
        accepted_at: datetime,
    ) -> Invitation:
        """@brief 由匹配收件人一次性接受邀请 / Accept once by the matching recipient.

        @param user_id 接受邀请的用户 / User accepting the invitation.
        @param actor_email 已认证用户的规范化邮箱 / Authenticated user's canonical email.
        @param accepted_at 接受时刻 / Acceptance instant.
        @return accepted 状态的下一版本邀请 / Next invitation revision in accepted state.
        @raise InvalidStateTransition 邀请非 pending、已到期或收件人不匹配时抛出 / Raised when
            not pending, expired, or addressed to another user.
        """
        self._require_pending()
        if accepted_at >= self.expires_at:
            raise InvalidStateTransition("expired invitation cannot be accepted")
        if actor_email.casefold() != self.email.casefold():
            raise InvalidStateTransition("invitation recipient does not match authenticated user")
        return replace(
            self,
            meta=self.meta.advance(accepted_at),
            status=InvitationStatus.ACCEPTED,
            accepted_by=user_id,
            resolved_at=accepted_at,
        )

    def revoke(self, revoked_at: datetime) -> Invitation:
        """@brief 撤销 pending 邀请 / Revoke a pending invitation.

        @param revoked_at 撤销时刻 / Revocation instant.
        @return revoked 状态的下一版本邀请 / Next invitation revision in revoked state.
        """
        self._require_pending()
        return replace(
            self,
            meta=self.meta.advance(revoked_at),
            status=InvitationStatus.REVOKED,
            resolved_at=revoked_at,
        )

    def expire(self, expired_at: datetime) -> Invitation:
        """@brief 到期 pending 邀请 / Expire a pending invitation.

        @param expired_at 到期处理时刻 / Expiration-processing instant.
        @return expired 状态的下一版本邀请 / Next invitation revision in expired state.
        @raise InvalidStateTransition 尚未到 expires_at 时抛出 / Raised before expires_at.
        """
        self._require_pending()
        if expired_at < self.expires_at:
            raise InvalidStateTransition("invitation cannot expire before expires_at")
        return replace(
            self,
            meta=self.meta.advance(expired_at),
            status=InvitationStatus.EXPIRED,
            resolved_at=expired_at,
        )

    def _require_pending(self) -> None:
        """@brief 要求邀请仍为 pending / Require the invitation to remain pending.

        @raise InvalidStateTransition 邀请已进入终态时抛出 / Raised after a terminal transition.
        """
        if self.status is not InvitationStatus.PENDING:
            raise InvalidStateTransition("only pending invitations may transition")


__all__ = [
    "DataRegion",
    "InvalidStateTransition",
    "Invitation",
    "InvitationStatus",
    "MemberStatus",
    "Membership",
    "SoleOwnerViolation",
    "Workspace",
    "WorkspacePlan",
    "WorkspaceRole",
]
