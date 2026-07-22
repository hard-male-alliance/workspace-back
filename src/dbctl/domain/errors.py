"""@brief dbctl 领域错误 / Domain errors for dbctl."""

import hmac
import os
from dataclasses import dataclass
from typing import Any, Final, cast

_DOMAIN_MESSAGE_ATTRIBUTE: Final[str] = "_dbctl_domain_message"
"""@brief 领域错误构造时正文快照属性 / Attribute storing the domain-message construction snapshot."""

_DOMAIN_MESSAGE_MAC_KEY: Final[bytes] = os.urandom(32)
"""@brief 进程内领域正文完整性密钥 / Per-process domain-message integrity key."""

_DOMAIN_MESSAGE_MAC_SIZE: Final[int] = 32
"""@brief SHA-256 HMAC 的固定字节数 / Fixed byte length of a SHA-256 HMAC."""

_DOMAIN_MESSAGE_MAX_LENGTH: Final[int] = 2_000
"""@brief 领域错误快照的最大字符数 / Maximum characters in a domain-error snapshot."""


@dataclass(frozen=True, slots=True)
class _DomainMessagePayload:
    """@brief 不可变领域错误正文快照 / Immutable domain-error message snapshot.

    @param mac 绑定异常身份与正文的 HMAC / HMAC binding exception identity and message.
    @param message 构造时的固定正文 / Text captured at construction.
    """

    mac: bytes
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
        if not message or len(message) > _DOMAIN_MESSAGE_MAX_LENGTH:
            return
        try:
            descriptor = cast(Any, vars(BaseException)["__dict__"])
            namespace = descriptor.__get__(self, BaseException)
            if type(namespace) is dict:
                namespace[_DOMAIN_MESSAGE_ATTRIBUTE] = _DomainMessagePayload(
                    mac=_domain_message_mac(self, message),
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
    @note 调用方仍必须验证异常来源并执行终端脱敏；本函数解决 ``args`` 可变性与载荷替换。
    / Callers must still validate provenance and redact for terminals; this function only addresses
    mutable ``args`` and payload replacement.
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
    if type(payload) is not _DomainMessagePayload:
        return None
    if type(payload.mac) is not bytes or len(payload.mac) != _DOMAIN_MESSAGE_MAC_SIZE:
        return None
    if (
        type(payload.message) is not str
        or not payload.message
        or len(payload.message) > _DOMAIN_MESSAGE_MAX_LENGTH
    ):
        return None
    expected = _domain_message_mac(error, payload.message)
    if not hmac.compare_digest(payload.mac, expected):
        return None
    return payload.message


def _domain_message_mac(error: BaseException, message: str) -> bytes:
    """@brief 认证异常身份与领域正文 / Authenticate exception identity and domain message.

    @param error 正文所属领域异常 / Domain exception owning the message.
    @param message 构造时正文 / Construction-time message.
    @return SHA-256 HMAC / SHA-256 HMAC.
    """

    material = repr((id(error), message)).encode("utf-8", errors="surrogatepass")
    return hmac.digest(_DOMAIN_MESSAGE_MAC_KEY, material, "sha256")


__all__ = [
    "DomainError",
    "InvalidDatabaseModelError",
    "InvalidNameError",
    "InvalidRetentionPolicyError",
    "InvalidRoleSetError",
    "safe_domain_message",
]
