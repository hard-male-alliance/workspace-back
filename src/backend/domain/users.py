"""@brief API v2 用户与账户删除领域模型 / API v2 user and account-deletion domain model."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from typing import NewType

from backend.domain.principals import (
    DomainInvariantError,
    ResourceMeta,
    Subject,
    UserId,
    WorkspaceId,
)

_DELETION_FAILURE_CODE = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief 可持久、可公开的删除失败码语法 / Persistable public account-deletion failure-code grammar."""

AccountDeletionId = NewType("AccountDeletionId", str)
"""@brief 账户删除请求不透明标识 / Opaque account-deletion request identifier."""


class AccountDeletionStatus(StrEnum):
    """@brief 账户删除请求状态 / Account-deletion request states."""

    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class AccountStatus(StrEnum):
    """@brief 本地用户账户状态 / Local user-account states."""

    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETION_SCHEDULED = "deletion_scheduled"
    DELETED = "deleted"


@dataclass(frozen=True, slots=True)
class User:
    """@brief 不依赖 token scope 的本地用户聚合 / Local user aggregate independent of token scope.

    @param meta 可组合资源元数据 / Composable resource metadata.
    @param subject OIDC 稳定 subject / Stable OIDC subject.
    @param email 规范化登录邮箱 / Canonical login email.
    @param email_verified 邮箱是否完成验证 / Whether the email has been verified.
    @param display_name 产品显示名 / Product display name.
    @param locale BCP 47 风格 locale / BCP 47-style locale.
    @param default_workspace_id 仅用于界面偏好的 Workspace / Workspace used only as a UI preference.
    @param account_status 服务端账户生命周期状态 / Server-side account lifecycle state.
    """

    meta: ResourceMeta[UserId]
    subject: Subject
    email: str
    email_verified: bool
    display_name: str
    locale: str
    default_workspace_id: WorkspaceId | None
    account_status: AccountStatus = AccountStatus.ACTIVE

    def __post_init__(self) -> None:
        """@brief 校验用户公开字段 / Validate user-facing fields.

        @raise DomainInvariantError 字段违反 v2 边界时抛出 / Raised when fields violate v2 bounds.
        """
        if not 1 <= len(self.subject) <= 255 or self.subject.strip() != self.subject:
            raise DomainInvariantError("user subject must be canonical and 1 to 255 characters")
        if not self.email or len(self.email) > 320 or self.email.strip() != self.email:
            raise DomainInvariantError("user email must be canonical and at most 320 characters")
        if not 1 <= len(self.display_name) <= 120 or self.display_name.strip() != self.display_name:
            raise DomainInvariantError("display name must be canonical and 1 to 120 characters")
        if not 2 <= len(self.locale) <= 35 or self.locale.strip() != self.locale:
            raise DomainInvariantError("locale must be canonical and 2 to 35 characters")

    def schedule_deletion(self, updated_at: datetime) -> User:
        """@brief 原子进入删除冷静期 / Enter the deletion cooling-off state atomically.

        @param updated_at 状态变更时刻 / State-transition instant.
        @return 下一版本用户 / Next user revision.
        @raise DomainInvariantError 非 active 账户重复安排删除时抛出 / Raised when deletion is
            scheduled for a non-active account.
        """
        if self.account_status is not AccountStatus.ACTIVE:
            raise DomainInvariantError("only an active account can schedule deletion")
        return replace(
            self,
            meta=self.meta.advance(updated_at),
            account_status=AccountStatus.DELETION_SCHEDULED,
        )

    def cancel_scheduled_deletion(self, updated_at: datetime) -> User:
        """@brief 从删除冷静期恢复 active / Restore an account from deletion cooling-off.

        @param updated_at 状态变更时刻 / State-transition instant.
        @return 下一版本用户 / Next user revision.
        @raise DomainInvariantError 账户并非 deletion_scheduled 时抛出 / Raised unless the
            account is currently scheduled for deletion.
        """
        if self.account_status is not AccountStatus.DELETION_SCHEDULED:
            raise DomainInvariantError("only a deletion-scheduled account can be restored")
        return replace(
            self,
            meta=self.meta.advance(updated_at),
            account_status=AccountStatus.ACTIVE,
        )

    def revise_profile(
        self,
        *,
        display_name: str,
        locale: str,
        default_workspace_id: WorkspaceId | None,
        updated_at: datetime,
    ) -> User:
        """@brief 用完整目标状态修改用户偏好 / Revise preferences using complete desired state.

        @param display_name 新显示名 / New display name.
        @param locale 新 locale / New locale.
        @param default_workspace_id 新界面偏好；不产生授权上下文 / New UI preference; grants no access.
        @param updated_at 修改时刻 / Modification instant.
        @return 下一版本用户 / Next user revision.
        """
        return replace(
            self,
            meta=self.meta.advance(updated_at),
            display_name=display_name,
            locale=locale,
            default_workspace_id=default_workspace_id,
        )


@dataclass(frozen=True, slots=True)
class AccountDeletionFailure:
    """@brief 不含 HTTP 语义的删除失败原因 / Deletion failure reason without HTTP semantics.

    @param code 稳定领域代码 / Stable domain code.
    @param detail 安全、可公开的说明 / Safe public explanation.
    """

    code: str
    detail: str

    def __post_init__(self) -> None:
        """@brief 校验失败原因 / Validate failure reason.

        @raise DomainInvariantError 代码或说明为空时抛出 / Raised for an empty code or detail.
        """
        if _DELETION_FAILURE_CODE.fullmatch(self.code) is None:
            raise DomainInvariantError("account deletion failure code is invalid")
        if (
            not 1 <= len(self.detail) <= 500
            or self.detail.strip() != self.detail
            or any(ord(character) < 32 and character not in "\t\n" for character in self.detail)
        ):
            raise DomainInvariantError("account deletion failure detail is invalid")


@dataclass(frozen=True, slots=True)
class ScheduledAccountDeletion:
    """@brief 冷静期中的账户删除 / Account deletion in its cooling-off period.

    @param meta 请求资源元数据 / Request resource metadata.
    @param user_id 请求所属用户 / User owning the request.
    @param scheduled_for 最早执行时刻 / Earliest execution instant.
    @param status 固定判别值 / Fixed discriminator.
    """

    meta: ResourceMeta[AccountDeletionId]
    user_id: UserId
    scheduled_for: datetime
    status: AccountDeletionStatus = field(default=AccountDeletionStatus.SCHEDULED, init=False)

    def __post_init__(self) -> None:
        """@brief 校验冷静期时间 / Validate cooling-off timing.

        @raise DomainInvariantError 执行时刻早于创建时刻时抛出 / Raised when due before creation.
        """
        if self.scheduled_for.tzinfo is None or self.scheduled_for.utcoffset() is None:
            raise DomainInvariantError("scheduled_for must be timezone-aware")
        if self.scheduled_for < self.meta.created_at:
            raise DomainInvariantError("scheduled_for cannot precede request creation")

    def cancel(self, cancelled_at: datetime) -> CancelledAccountDeletion:
        """@brief 在冷静期取消删除 / Cancel deletion during the cooling-off period.

        @param cancelled_at 取消时刻 / Cancellation instant.
        @return 已取消状态 / Cancelled state.
        """
        return CancelledAccountDeletion(
            self.meta.advance(cancelled_at), self.user_id, self.scheduled_for, cancelled_at
        )

    def start(self, started_at: datetime) -> RunningAccountDeletion:
        """@brief 到期后开始不可取消执行 / Start non-cancellable execution once due.

        @param started_at 开始时刻 / Start instant.
        @return 运行中状态 / Running state.
        @raise DomainInvariantError 冷静期尚未结束时抛出 / Raised before cooling-off expires.
        """
        if started_at < self.scheduled_for:
            raise DomainInvariantError("account deletion cannot start before scheduled_for")
        return RunningAccountDeletion(
            self.meta.advance(started_at), self.user_id, self.scheduled_for, started_at
        )


@dataclass(frozen=True, slots=True)
class RunningAccountDeletion:
    """@brief 正在执行且不可取消的账户删除 / Running, non-cancellable account deletion.

    @param meta 请求资源元数据 / Request resource metadata.
    @param user_id 请求所属用户 / User owning the request.
    @param scheduled_for 原计划执行时刻 / Original scheduled instant.
    @param started_at 实际开始时刻 / Actual start instant.
    @param status 固定判别值 / Fixed discriminator.
    """

    meta: ResourceMeta[AccountDeletionId]
    user_id: UserId
    scheduled_for: datetime
    started_at: datetime
    status: AccountDeletionStatus = field(default=AccountDeletionStatus.RUNNING, init=False)

    def complete(self, completed_at: datetime) -> CompletedAccountDeletion:
        """@brief 完成账户删除 / Complete account deletion.

        @param completed_at 完成时刻 / Completion instant.
        @return 已完成状态 / Completed state.
        """
        if completed_at < self.started_at:
            raise DomainInvariantError("account deletion cannot complete before it starts")
        return CompletedAccountDeletion(
            self.meta.advance(completed_at), self.user_id, self.scheduled_for, completed_at
        )

    def fail(self, failure: AccountDeletionFailure, failed_at: datetime) -> FailedAccountDeletion:
        """@brief 以结构化原因结束为失败 / Finish with a structured failure.

        @param failure 不含 transport 语义的失败原因 / Failure reason without transport semantics.
        @param failed_at 失败时刻 / Failure instant.
        @return 失败状态 / Failed state.
        """
        if failed_at < self.started_at:
            raise DomainInvariantError("account deletion cannot fail before it starts")
        return FailedAccountDeletion(
            self.meta.advance(failed_at), self.user_id, self.scheduled_for, failure, failed_at
        )


@dataclass(frozen=True, slots=True)
class CompletedAccountDeletion:
    """@brief 成功完成的账户删除 / Successfully completed account deletion.

    @param meta 请求资源元数据 / Request resource metadata.
    @param user_id 已删除或匿名化用户标识 / Deleted or anonymized user identifier.
    @param scheduled_for 原计划执行时刻 / Original scheduled instant.
    @param completed_at 完成时刻 / Completion instant.
    @param status 固定判别值 / Fixed discriminator.
    """

    meta: ResourceMeta[AccountDeletionId]
    user_id: UserId
    scheduled_for: datetime
    completed_at: datetime
    status: AccountDeletionStatus = field(default=AccountDeletionStatus.COMPLETED, init=False)


@dataclass(frozen=True, slots=True)
class CancelledAccountDeletion:
    """@brief 在冷静期取消的账户删除 / Account deletion cancelled during cooling-off.

    @param meta 请求资源元数据 / Request resource metadata.
    @param user_id 请求所属用户 / User owning the request.
    @param scheduled_for 原计划执行时刻 / Original scheduled instant.
    @param cancelled_at 取消时刻 / Cancellation instant.
    @param status 固定判别值 / Fixed discriminator.
    """

    meta: ResourceMeta[AccountDeletionId]
    user_id: UserId
    scheduled_for: datetime
    cancelled_at: datetime
    status: AccountDeletionStatus = field(default=AccountDeletionStatus.CANCELLED, init=False)


@dataclass(frozen=True, slots=True)
class FailedAccountDeletion:
    """@brief 执行失败的账户删除 / Failed account deletion.

    @param meta 请求资源元数据 / Request resource metadata.
    @param user_id 请求所属用户 / User owning the request.
    @param scheduled_for 原计划执行时刻 / Original scheduled instant.
    @param failure 结构化领域失败 / Structured domain failure.
    @param failed_at 失败时刻 / Failure instant.
    @param status 固定判别值 / Fixed discriminator.
    """

    meta: ResourceMeta[AccountDeletionId]
    user_id: UserId
    scheduled_for: datetime
    failure: AccountDeletionFailure
    failed_at: datetime
    status: AccountDeletionStatus = field(default=AccountDeletionStatus.FAILED, init=False)


type AccountDeletion = (
    ScheduledAccountDeletion
    | RunningAccountDeletion
    | CompletedAccountDeletion
    | CancelledAccountDeletion
    | FailedAccountDeletion
)
"""@brief 由 status 判别的账户删除联合 / Status-discriminated account-deletion union."""


__all__ = [
    "AccountDeletion",
    "AccountDeletionFailure",
    "AccountDeletionId",
    "AccountDeletionStatus",
    "AccountStatus",
    "CancelledAccountDeletion",
    "CompletedAccountDeletion",
    "FailedAccountDeletion",
    "RunningAccountDeletion",
    "ScheduledAccountDeletion",
    "User",
]
