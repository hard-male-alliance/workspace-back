"""@brief dbctl 领域错误 / Domain errors for dbctl."""

from dataclasses import dataclass
from typing import Any, Final, cast

_DOMAIN_MESSAGE_ATTRIBUTE: Final[str] = "_dbctl_domain_message"
"""@brief 领域错误构造时正文快照属性 / Attribute storing the domain-message construction snapshot."""

_DOMAIN_MESSAGE_GUARD: Final[object] = object()
"""@brief 防止形状相同的属性被意外误信 / Identity guard against accidental lookalike attributes."""


@dataclass(frozen=True, slots=True)
class _DomainMessagePayload:
    """@brief 不可变领域错误正文快照 / Immutable domain-error message snapshot.

    @param guard 模块私有身份 / Module-private identity.
    @param message 构造时的固定正文 / Text captured at construction.
    """

    guard: object
    message: str


class DomainError(ValueError):
    """@brief 领域不变量被破坏 / A domain invariant was violated."""

    def __init__(self, *arguments: object) -> None:
        """@brief 冻结构造时正文以防 args 后改写 / Freeze construction text against later args mutation.

        @param arguments 标准异常参数 / Standard exception arguments.
        @return 无返回值 / No return value.
        """

        super().__init__(*arguments)
        message = "；".join(argument for argument in arguments if type(argument) is str)
        if not message:
            return
        try:
            descriptor = cast(Any, vars(BaseException)["__dict__"])
            namespace = descriptor.__get__(self, BaseException)
            if type(namespace) is dict:
                namespace[_DOMAIN_MESSAGE_ATTRIBUTE] = _DomainMessagePayload(
                    guard=_DOMAIN_MESSAGE_GUARD,
                    message=message,
                )
        except Exception:
            return


class InvalidNameError(DomainError):
    """@brief PostgreSQL 名称不满足可移植约束 / A PostgreSQL name is not portable."""


class InvalidRoleSetError(DomainError):
    """@brief 数据库角色集合违反隔离约束 / A database role set violates isolation rules."""


class InvalidDatabaseModelError(DomainError):
    """@brief 数据库领域模型内部不一致 / The database domain model is inconsistent."""


class InvalidRetentionPolicyError(DomainError):
    """@brief 遥测保留策略或执行边界无效 / A telemetry policy or execution bound is invalid."""


_DOMAIN_ERROR_TYPES: Final[tuple[type[DomainError], ...]] = (
    DomainError,
    InvalidDatabaseModelError,
    InvalidNameError,
    InvalidRetentionPolicyError,
    InvalidRoleSetError,
)
"""@brief 可展示构造快照的精确领域错误 / Exact domain types whose snapshots may be displayed."""


def safe_domain_message(error: BaseException) -> str | None:
    """@brief 读取精确领域错误的不可变正文快照 / Read an exact domain error's immutable snapshot.

    @param error 候选异常 / Candidate exception.
    @return 构造时正文，类型或载荷不可信时为 None / Construction text, or ``None`` if untrusted.
    @note 调用方仍必须验证异常来源并执行终端脱敏；本函数只解决 ``args`` 可变性与属性伪造。
    / Callers must still validate provenance and redact for terminals; this function only addresses
    mutable ``args`` and lookalike attributes.
    """

    error_type = type(error)
    if not any(error_type is candidate for candidate in _DOMAIN_ERROR_TYPES):
        return None
    try:
        descriptor = cast(Any, vars(BaseException)["__dict__"])
        namespace = descriptor.__get__(error, BaseException)
    except Exception:
        return None
    if type(namespace) is not dict:
        return None
    payload = dict.get(namespace, _DOMAIN_MESSAGE_ATTRIBUTE)
    if type(payload) is not _DomainMessagePayload or payload.guard is not _DOMAIN_MESSAGE_GUARD:
        return None
    return payload.message if type(payload.message) is str and payload.message else None


__all__ = [
    "DomainError",
    "InvalidDatabaseModelError",
    "InvalidNameError",
    "InvalidRetentionPolicyError",
    "InvalidRoleSetError",
    "safe_domain_message",
]
