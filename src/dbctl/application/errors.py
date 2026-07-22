"""@brief dbctl 应用层错误与安全诊断链 / Application errors and safe diagnostic chains."""

from __future__ import annotations

import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, TracebackType
from typing import Any, Final, cast

from dbctl.domain.errors import (
    DomainError,
    InvalidDatabaseModelError,
    InvalidNameError,
    InvalidRetentionPolicyError,
    InvalidRoleSetError,
    safe_domain_message,
)

_POSTGRES_URI_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?i)\bp\s*o\s*s\s*t\s*g\s*r\s*e\s*s(?:q\s*l)?"
    r"(?:\s*\+\s*[a-z0-9_]+)?\s*[：:]\s*[／/]\s*[／/][^\s\"'<>]+"
)
"""@brief PostgreSQL URI 的保守脱敏模式 / Conservative redaction pattern for PostgreSQL URIs."""

_SECRET_ASSIGNMENT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?i)(?<![A-Za-z0-9_])([\"']?(?:"
    r"p\s*a\s*s\s*s\s*w\s*o\s*r\s*d|"
    r"p\s*g\s*p\s*a\s*s\s*s\s*w\s*o\s*r\s*d|"
    r"s\s*s\s*l\s*p\s*a\s*s\s*s\s*w\s*o\s*r\s*d|"
    r"p\s*a\s*s\s*s\s*f\s*i\s*l\s*e)[\"']?)"
    r"\s*[=:：＝]\s*(?:E?'(?:''|\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"|(?:\\.|[^\s,;])+)"
)
"""@brief 常见 secret 键值形式 / Common secret key-value forms."""

_SQL_PASSWORD_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?is)\bp\s*a\s*s\s*s\s*w\s*o\s*r\s*d\s+E?'(?:''|\\.|[^'\\])*'"
)
"""@brief PostgreSQL PASSWORD 字面量 / PostgreSQL PASSWORD literals."""

_ANSI_ESCAPE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?:"
    r"\x1b\][^\x07]*(?:\x07|\x1b\\)|"
    r"\x1b\[[0-?]*[ -/]*[@-~]|"
    r"\x9d[^\x07\x9c]*(?:\x07|\x9c)|"
    r"\x9b[0-?]*[ -/]*[@-~]|"
    r"\x1b[ -/]*[@-~]"
    r")"
)
"""@brief ANSI/OSC 终端控制序列 / ANSI and OSC terminal-control sequences."""

_MAX_EXTERNAL_DETAIL_LENGTH: Final[int] = 2_000
"""@brief 单个底层诊断详情的最大字符数 / Maximum characters in one external diagnostic detail."""

_MIN_SAFE_INTEGER: Final[int] = -(2**31)
"""@brief 可安全格式化的最小外部整数 / Minimum external integer safe to format."""

_MAX_SAFE_INTEGER: Final[int] = 2**31 - 1
"""@brief 可安全格式化的最大外部整数 / Maximum external integer safe to format."""

_SAFE_DIAGNOSTIC_NOTES_ATTRIBUTE: Final[str] = "_dbctl_safe_diagnostic_notes"
"""@brief 显式可信诊断载荷的私有属性名 / Private attribute for explicitly trusted diagnostics."""

_SAFE_DIAGNOSTIC_GUARD: Final[object] = object()
"""@brief 防止同形属性被意外误信的模块身份 / Module identity rejecting accidental lookalike payloads."""

_EXTERNAL_ERROR_CATEGORIES: Final[tuple[tuple[type[BaseException], str], ...]] = (
    (FileNotFoundError, "builtins.FileNotFoundError"),
    (PermissionError, "builtins.PermissionError"),
    (TimeoutError, "builtins.TimeoutError"),
    (BrokenPipeError, "builtins.BrokenPipeError"),
    (ConnectionError, "builtins.ConnectionError"),
    (OSError, "builtins.OSError"),
    (UnicodeError, "builtins.UnicodeError"),
    (ValueError, "builtins.ValueError"),
    (TypeError, "builtins.TypeError"),
    (LookupError, "builtins.LookupError"),
    (ArithmeticError, "builtins.ArithmeticError"),
    (AssertionError, "builtins.AssertionError"),
    (RuntimeError, "builtins.RuntimeError"),
    (Exception, "builtins.Exception"),
    (BaseException, "builtins.BaseException"),
)
"""@brief 不读取动态类元数据的稳定异常分类 / Stable error categories without dynamic class metadata."""

_DOMAIN_ERROR_ORIGINS: Final[tuple[tuple[type[DomainError], tuple[str, ...]], ...]] = (
    (DomainError, ("dbctl.domain.roles",)),
    (InvalidDatabaseModelError, ("dbctl.domain.database",)),
    (InvalidNameError, ("dbctl.domain.names",)),
    (InvalidRetentionPolicyError, ("dbctl.domain.retention",)),
    (InvalidRoleSetError, ("dbctl.domain.roles",)),
)
"""@brief 精确领域错误及其唯一合法抛出模块 / Exact domain errors and allowed raise modules."""

_DBCTL_DOMAIN_ROOT: Final[str] = os.path.realpath(str(Path(__file__).parents[1] / "domain"))
"""@brief 领域错误正文可信来源的真实目录 / Real source root trusted for domain-error text."""


@dataclass(frozen=True, slots=True)
class _SafeDiagnosticPayload:
    """@brief 仅由本模块构造的安全诊断载荷 / Safe diagnostic payload constructed only here.

    @param guard 必须是模块私有身份对象 / Must be the module-private identity object.
    @param notes 已净化的不可变注释 / Sanitized immutable notes.
    @param message 已批准的异常正文；不存在时为 None / Approved error text, or ``None``.
    @param message_allows_external_origin 正文是否由安全外部代理工厂构造。
    / Whether a safe external-proxy factory constructed the text.
    """

    guard: object
    notes: tuple[str, ...] = ()
    message: str | None = None
    message_allows_external_origin: bool = False


class ApplicationError(RuntimeError):
    """@brief dbctl 用例未能完成 / A dbctl use case could not complete."""

    def __init__(self, *arguments: object) -> None:
        """@brief 冻结构造时安全正文而不调用 __str__ / Freeze construction text without ``__str__``.

        @param arguments 标准异常参数 / Standard exception arguments.
        @return 无返回值 / No return value.
        @note 展示边界仍会要求精确错误类型和可信 raise 来源；构造快照只防止外部后来改写
        ``args``。/ Presentation still requires an exact error type and trusted raise provenance;
        the snapshot only prevents later mutation of ``args``.
        """

        super().__init__(*arguments)
        message = "；".join(argument for argument in arguments if type(argument) is str)
        if message:
            _approve_safe_diagnostic_message(
                self,
                message,
                allows_external_origin=False,
            )


class ExternalDiagnosticError(ApplicationError):
    """@brief 已脱敏且可安全展示的底层异常代理 / Redacted proxy safe to expose as an external cause.

    @note 该异常绝不持有或格式化原始异常消息；其 traceback 可以来自原始异常，
    但终端渲染不得读取 locals、源码行或外部栈帧文本元数据。/ This exception never
    retains or formats an unsanitized exception message. Its traceback may originate from the raw
    exception, but terminal rendering must not read locals, source lines, or textual metadata from
    external frames.
    """


class DbctlConfigurationError(ApplicationError, ValueError):
    """@brief 外部配置无法构造合法领域模型 / External configuration cannot form a valid model."""


class DatabaseAlreadyExistsError(ApplicationError):
    """@brief 并发创建发现数据库已存在 / Concurrent creation found the database already exists.

    @note 仅 infrastructure runner 应抛出；bootstrap 用例只在条件创建阶段消费该信号。
    / Only an infrastructure runner should raise this signal; bootstrap consumes it solely for
    conditional database creation.
    """


class BootstrapExecutionError(ApplicationError):
    """@brief bootstrap 计划或执行失败 / Bootstrap planning or execution failed."""


class ContainerEntrypointError(ApplicationError):
    """@brief 容器运行配置投影或目标进程启动失败 / Container projection or target-process launch failed."""


class MigrationExecutionError(ApplicationError):
    """@brief 数据库迁移执行失败 / Database migration execution failed."""


class RetentionExecutionError(ApplicationError):
    """@brief 遥测保留清理执行失败 / Telemetry-retention pruning failed."""


class ShellExecutionError(ApplicationError):
    """@brief 交互式数据库 shell 执行失败 / Interactive database-shell execution failed."""


def safe_external_cause(
    error: BaseException,
    *,
    operation: str,
) -> ExternalDiagnosticError:
    """@brief 将底层异常转换为可展示的安全代理 cause / Convert a raw failure into a display-safe proxy cause.

    @param error 只用于稳定分类、内建 OSError errno 与 traceback 的底层异常。
    / Raw failure used only for stable classification, built-in OSError errno, and traceback.
    @param operation 不含用户秘密的失败动作 / Secret-free failed action.
    @return 带原始栈帧但不带原始异常对象/消息的代理异常。
    / Proxy exception with original frames but no raw exception object or unsanitized message.
    @note 永远不读取 ``str(error)``、动态类名或任意属性；第三方 descriptor
    也可执行 I/O 或抛出二次异常。/ ``str(error)``, dynamic class names, and arbitrary
    attributes are never read because third-party descriptors can perform I/O or raise secondary errors.
    """

    if type(operation) is not str or not operation.strip():
        raise ValueError("external diagnostic operation 不能为空。")
    safe_operation = redact_sensitive_text(operation)
    if not safe_operation:
        raise ValueError("external diagnostic operation 净化后不能为空。")
    error_category = safe_exception_category(error)
    metadata: list[str] = []
    errno = _built_in_errno(error)
    if errno is not None:
        metadata.append(f"errno={errno}")

    message = f"{safe_operation}；底层异常类别={error_category}"
    if metadata:
        message += f"（{', '.join(metadata)}）"
    proxy = ExternalDiagnosticError(message)
    _approve_safe_diagnostic_message(proxy, message, allows_external_origin=True)
    return proxy.with_traceback(safe_exception_traceback(error))


def safe_process_exit_cause(*, program: str, exit_code: int) -> ExternalDiagnosticError:
    """@brief 构造只公开程序名与退出码的安全 cause / Build a cause exposing only program and status.

    @param program 不含路径的受控程序名 / Controlled program basename without a path.
    @param exit_code 子进程整数退出状态 / Integer subprocess status.
    @return 带模块守卫安全正文的外部诊断 / External diagnostic with guarded safe text.
    @note 该函数不接收子进程 stderr/stdout，因而不会意外回显驱动或服务端正文。
    / Child stdout and stderr are intentionally not accepted, preventing accidental disclosure of
    driver or server text.
    """

    if type(program) is not str or re.fullmatch(r"[A-Za-z0-9_.-]+", program) is None:
        raise ValueError("external process program 必须是安全 basename。")
    if type(exit_code) is not int or exit_code < _MIN_SAFE_INTEGER or exit_code > _MAX_SAFE_INTEGER:
        raise ValueError("external process exit_code 必须是整数。")
    message = f"{program} 退出码={exit_code}"
    proxy = ExternalDiagnosticError(message)
    _approve_safe_diagnostic_message(proxy, message, allows_external_origin=True)
    return proxy


def safe_exception_category(error: BaseException) -> str:
    """@brief 不读取动态类名地将异常归入内建基类 / Classify an error by fixed built-in bases.

    @param error 不可信异常 / Untrusted exception.
    @return 不含运行时提供文本的稳定分类 / Stable category containing no runtime-provided text.
    """

    for category, label in _EXTERNAL_ERROR_CATEGORIES:
        if safe_exception_matches(error, category):
            return label
    return "builtins.BaseException"


def safe_exception_matches(error: BaseException, category: type[BaseException]) -> bool:
    """@brief 不读取实例 __class__ 地检查内建继承 / Check inheritance without instance reflection.

    @param error 不可信异常实例 / Untrusted exception instance.
    @param category 由 dbctl 选择且使用标准 type metaclass 的异常基类。
    / Exception base selected by dbctl and using the standard ``type`` metaclass.
    @return 真实运行时类型继承该基类时为真 / True when the real runtime type inherits the base.
    @note ``isinstance(error, category)`` 在不匹配路径可能读取实例伪造的 ``__class__``；
    这里直接从 ``type`` 内建 descriptor 读取 ``type(error)`` 的真实 MRO。/ A failed
    ``isinstance`` may read a forged instance ``__class__``; this function reads the real MRO of
    ``type(error)`` directly through the built-in ``type`` descriptor.
    """

    if type(category) is not type:
        return False
    try:
        mro = _read_builtin_descriptor(type, "__mro__", type(error))
    except Exception:
        return False
    return type(mro) is tuple and any(base is category for base in mro)


def safe_exception_traceback(error: BaseException) -> TracebackType | None:
    """@brief 绕过异常子类 property 读取原生 traceback / Read traceback without subclass properties.

    @param error 当前异常 / Current exception.
    @return 内建 traceback 链或 None / Built-in traceback chain or None.
    """

    return cast(
        TracebackType | None,
        _read_builtin_descriptor(BaseException, "__traceback__", error),
    )


def safe_exception_cause(error: BaseException) -> BaseException | None:
    """@brief 绕过异常子类 property 读取 cause / Read cause without subclass properties.

    @param error 当前异常 / Current exception.
    @return 内建显式 cause 或 None / Built-in explicit cause or None.
    """

    value = _read_builtin_descriptor(BaseException, "__cause__", error)
    if value is None:
        return None
    candidate = cast(BaseException, value)
    return candidate if safe_exception_matches(candidate, BaseException) else None


def safe_exception_context(error: BaseException) -> BaseException | None:
    """@brief 绕过异常子类 property 读取 context / Read context without subclass properties.

    @param error 当前异常 / Current exception.
    @return 内建隐式 context 或 None / Built-in implicit context or None.
    """

    value = _read_builtin_descriptor(BaseException, "__context__", error)
    if value is None:
        return None
    candidate = cast(BaseException, value)
    return candidate if safe_exception_matches(candidate, BaseException) else None


def safe_exception_suppresses_context(error: BaseException) -> bool:
    """@brief 绕过异常子类 property 读取抑制标志 / Read context suppression without subclass properties.

    @param error 当前异常 / Current exception.
    @return 内建抑制标志 / Built-in suppression flag.
    """

    return _read_builtin_descriptor(BaseException, "__suppress_context__", error) is True


def safe_domain_error_message(error: BaseException) -> str | None:
    """@brief 保留真实 domain raise frame 的领域正文 / Preserve text raised by a real domain module.

    @param error 配置装配捕获的候选领域错误 / Candidate domain error caught during configuration.
    @return 构造时快照的脱敏正文，来源不可信时为 None。
    / Redacted construction snapshot, or ``None`` when provenance is untrusted.
    @note 精确错误类型、最内层 frame namespace 身份以及 module/code 文件必须同时匹配；
    外部子类或仅伪造模块名/文件名均不能把正文升级为可信。/ Exact error type, innermost
    frame namespace identity, and module/code file must all match; an external subclass or a forged
    module name/filename alone cannot promote its text.
    """

    error_type = type(error)
    allowed_modules = next(
        (modules for candidate, modules in _DOMAIN_ERROR_ORIGINS if error_type is candidate),
        None,
    )
    if allowed_modules is None:
        return None
    traceback_value = safe_exception_traceback(error)
    if traceback_value is None:
        return None
    while traceback_value.tb_next is not None:
        traceback_value = traceback_value.tb_next
    frame = traceback_value.tb_frame
    namespace = frame.f_globals
    module_name = dict.get(namespace, "__name__")
    if type(module_name) is not str or module_name not in allowed_modules:
        return None
    module = sys.modules.get(module_name)
    if type(module) is not ModuleType or vars(module) is not namespace:
        return None
    module_file = dict.get(namespace, "__file__")
    code_filename = frame.f_code.co_filename
    if type(module_file) is not str or len(module_file) > 1_024 or len(code_filename) > 1_024:
        return None
    try:
        normalized_module = os.path.realpath(os.path.abspath(os.path.normpath(module_file)))
        normalized_code = os.path.realpath(os.path.abspath(os.path.normpath(code_filename)))
        if normalized_module != normalized_code:
            return None
        within_domain = os.path.commonpath((_DBCTL_DOMAIN_ROOT, normalized_module)) == (
            _DBCTL_DOMAIN_ROOT
        )
    except OSError, ValueError:
        return None
    if not within_domain:
        return None
    message = safe_domain_message(error)
    return redact_sensitive_text(message) if message is not None else None


def _built_in_errno(error: BaseException) -> int | None:
    """@brief 通过 OSError 内建 descriptor 读取 errno / Read errno through the built-in OSError descriptor.

    @param error 候选底层异常 / Candidate raw exception.
    @return 严格整数 errno，其他情况为 None / Strict integer errno, otherwise None.
    @note 显式调用基类 descriptor，避免触发子类同名 property。
    / Calling the base descriptor directly avoids a same-named subclass property.
    """

    if not safe_exception_matches(error, OSError):
        return None
    try:
        errno = _read_builtin_descriptor(OSError, "errno", error)
    except Exception:
        return None
    return errno if type(errno) is int and _MIN_SAFE_INTEGER <= errno <= _MAX_SAFE_INTEGER else None


def _read_builtin_descriptor(
    owner: type[object],
    name: str,
    instance: object,
) -> object:
    """@brief 直接调用内建基类 descriptor / Invoke a built-in base descriptor directly.

    @param owner 定义 descriptor 的内建类 / Built-in class defining the descriptor.
    @param name 内建 descriptor 名 / Built-in descriptor name.
    @param instance 不可信子类实例 / Untrusted subclass instance.
    @return descriptor 的内建存储值 / Value from the descriptor's built-in storage.
    @note 不执行子类 ``__getattribute__`` 或同名 property。
    / Subclass ``__getattribute__`` and same-named properties are not executed.
    """

    descriptor = cast(Any, vars(owner)[name])
    return cast(object, descriptor.__get__(instance, owner))


def redact_sensitive_text(text: str) -> str:
    """@brief 脱敏并限制一段外部诊断文本 / Redact and bound a piece of external diagnostic text.

    @param text 外部程序或驱动产生的文本 / Text produced by an external program or driver.
    @return 无终端控制符、DSN 与密码字面量的有界单行文本。
    / Bounded single-line text without terminal controls, DSNs, or password literals.
    """

    if type(text) is not str:
        return ""
    rendered = _ANSI_ESCAPE_PATTERN.sub("", text)
    rendered = _normalize_terminal_line_controls(rendered)
    rendered = _fold_backspaces(rendered)
    rendered = _fold_fullwidth_latin(rendered)
    rendered = _terminal_safe(rendered)
    rendered = _POSTGRES_URI_PATTERN.sub("<redacted-postgresql-dsn>", rendered)
    rendered = _SECRET_ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group(1)}=<redacted>", rendered
    )
    rendered = _SQL_PASSWORD_PATTERN.sub("PASSWORD <redacted>", rendered)
    rendered = _terminal_safe(rendered)
    rendered = " | ".join(part.strip() for part in rendered.splitlines() if part.strip())
    if len(rendered) > _MAX_EXTERNAL_DETAIL_LENGTH:
        rendered = rendered[: _MAX_EXTERNAL_DETAIL_LENGTH - 1].rstrip() + "…"
    return rendered


def _fold_fullwidth_latin(text: str) -> str:
    """@brief 仅折叠全角拉丁字母以阻止关键字绕过 / Fold fullwidth Latin keyword letters only.

    @param text 待检查诊断文本 / Diagnostic text to inspect.
    @return 保留中文全角标点但规范化拉丁字母的文本。
    / Text with Latin letters normalized while Chinese fullwidth punctuation remains unchanged.
    """

    return "".join(
        chr(ord(character) - 0xFEE0)
        if "Ａ" <= character <= "Ｚ" or "ａ" <= character <= "ｚ"
        else character
        for character in text
    )


def _fold_backspaces(text: str) -> str:
    """@brief 折叠终端退格覆盖以暴露脱敏关键字 / Fold terminal backspace overwrites before redaction.

    @param text 已移除 ANSI 序列的候选文本 / Candidate text after ANSI removal.
    @return 按终端可见语义删除被退格覆盖字符后的文本。
    / Text with characters erased by backspaces removed according to terminal-visible semantics.
    """

    rendered: list[str] = []
    for character in text:
        if character == "\b":
            if rendered and rendered[-1] != "\n":
                rendered.pop()
            continue
        rendered.append(character)
    return "".join(rendered)


def _normalize_terminal_line_controls(text: str) -> str:
    """@brief 将终端换行控制统一为换行符 / Normalize terminal line movement to newlines.

    @param text 已移除 ANSI 序列的候选文本 / Candidate text after ANSI removal.
    @return CR、VT、FF 与 NEL 均替换为 LF 的文本 / Text with CR, VT, FF, and NEL mapped to LF.
    @note 直接删除回车会把前缀与其后可见 secret 键连接起来，从而绕过关键字边界检查。
    / Dropping carriage returns would join a prefix to a visible secret key and bypass boundaries.
    """

    return text.translate(
        {
            ord("\r"): "\n",
            ord("\v"): "\n",
            ord("\f"): "\n",
            0x85: "\n",
        }
    )


def add_safe_diagnostic_note(error: BaseException, note: str) -> None:
    """@brief 附加由 dbctl 构造且允许展示的诊断注释 / Attach a dbctl-authored note approved for display.

    @param error 接收注释的当前异常 / Current exception receiving the note.
    @param note 只由已验证非秘密字段构造的说明 / Note built only from validated non-secret fields.
    @return 无返回值 / No return value.
    @note 不使用 Python ``BaseException.add_note``，因为第三方异常也可写入 ``__notes__``；
    终端边界无法判断这些任意文本是否含 secret。/ Python ``BaseException.add_note`` is not
    used because third-party exceptions can also populate ``__notes__`` with arbitrary text whose
    secrecy cannot be established at the terminal boundary.
    """

    normalized = redact_sensitive_text(note)
    if not normalized:
        return
    try:
        namespace = _exception_namespace(error)
        if namespace is None:
            return
        existing = _safe_diagnostic_payload(namespace)
        notes = existing.notes if existing is not None else ()
        message = existing.message if existing is not None else None
        allows_external_origin = (
            existing.message_allows_external_origin if existing is not None else False
        )
        namespace[_SAFE_DIAGNOSTIC_NOTES_ATTRIBUTE] = _SafeDiagnosticPayload(
            guard=_SAFE_DIAGNOSTIC_GUARD,
            notes=(*notes, normalized),
            message=message,
            message_allows_external_origin=allows_external_origin,
        )
    except Exception:
        # 诊断增强永远不能遮蔽原始业务失败 / Diagnostic enrichment must never mask the failure.
        return


def safe_diagnostic_notes(error: BaseException) -> tuple[str, ...]:
    """@brief 读取唯一允许进入终端的显式诊断注释 / Read explicitly approved terminal notes.

    @param error 当前异常 / Current exception.
    @return 已再次净化的不可变注释序列 / Re-sanitized immutable notes.
    """

    namespace = _exception_namespace(error)
    if namespace is None:
        return ()
    payload = _safe_diagnostic_payload(namespace)
    if payload is None:
        return ()
    return tuple(
        rendered
        for note in payload.notes
        if type(note) is str and (rendered := redact_sensitive_text(note))
    )


def safe_diagnostic_message_snapshot(
    error: BaseException,
) -> tuple[str, bool] | None:
    """@brief 原子读取模块批准的正文与来源授权 / Atomically read approved text and origin grant.

    @param error 当前异常 / Current exception.
    @return 再净化正文及其外部来源授权；未批准时为 None。
    / Re-sanitized text and its external-origin grant, or ``None`` when unapproved.
    """

    namespace = _exception_namespace(error)
    if namespace is None:
        return None
    payload = _safe_diagnostic_payload(namespace)
    if payload is None or payload.message is None:
        return None
    rendered = redact_sensitive_text(payload.message)
    if not rendered:
        return None
    return rendered, payload.message_allows_external_origin is True


def _approve_safe_diagnostic_message(
    error: BaseException,
    message: str,
    *,
    allows_external_origin: bool,
) -> None:
    """@brief 以模块身份批准由固定字段构造的正文 / Approve text built from fixed safe fields.

    @param error 接收安全正文的异常 / Exception receiving approved text.
    @param message 只由固定标签和验证值构造的正文 / Text built only from fixed labels and validated values.
    @param allows_external_origin 是否允许正文沿用外部 traceback 来源 / Whether external traceback provenance is valid.
    @return 无返回值 / No return value.
    """

    normalized = redact_sensitive_text(message)
    if not normalized:
        return
    namespace = _exception_namespace(error)
    if namespace is None:
        return
    existing = _safe_diagnostic_payload(namespace)
    notes = existing.notes if existing is not None else ()
    namespace[_SAFE_DIAGNOSTIC_NOTES_ATTRIBUTE] = _SafeDiagnosticPayload(
        guard=_SAFE_DIAGNOSTIC_GUARD,
        notes=notes,
        message=normalized,
        message_allows_external_origin=allows_external_origin,
    )


def _exception_namespace(error: BaseException) -> dict[str, object] | None:
    """@brief 绕过子类 property 读取异常原生 namespace / Read native exception storage safely.

    @param error 不可信异常 / Untrusted exception.
    @return 精确内建 dict 或 None / Exact built-in dictionary or ``None``.
    @note 直接调用 ``BaseException.__dict__`` descriptor，绝不执行子类伪造的
    ``__dict__`` property。/ The base ``__dict__`` descriptor is invoked directly, so a forged
    subclass ``__dict__`` property is never executed.
    """

    try:
        namespace = _read_builtin_descriptor(BaseException, "__dict__", error)
    except Exception:
        return None
    return cast(dict[str, object], namespace) if type(namespace) is dict else None


def _safe_diagnostic_payload(
    namespace: dict[str, object],
) -> _SafeDiagnosticPayload | None:
    """@brief 验证 namespace 中的模块守卫载荷 / Validate the guarded payload in a namespace.

    @param namespace 异常原生属性字典 / Native exception attribute dictionary.
    @return 身份匹配的不可变载荷或 None / Identity-matched immutable payload or ``None``.
    """

    payload = dict.get(namespace, _SAFE_DIAGNOSTIC_NOTES_ATTRIBUTE)
    if type(payload) is not _SafeDiagnosticPayload or payload.guard is not _SAFE_DIAGNOSTIC_GUARD:
        return None
    return payload


def _terminal_safe(text: str) -> str:
    """@brief 移除可伪造终端输出的控制字符 / Remove control characters capable of terminal-output spoofing.

    @param text 待净化文本 / Candidate text.
    @return 保留换行与制表符之外可打印内容的文本 / Text retaining printable content plus tabs/newlines.
    """

    return "".join(
        character
        for character in text
        if character in {"\n", "\t"} or unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
    )


__all__ = [
    "ApplicationError",
    "BootstrapExecutionError",
    "ContainerEntrypointError",
    "DatabaseAlreadyExistsError",
    "DbctlConfigurationError",
    "ExternalDiagnosticError",
    "MigrationExecutionError",
    "RetentionExecutionError",
    "ShellExecutionError",
    "add_safe_diagnostic_note",
    "redact_sensitive_text",
    "safe_diagnostic_message_snapshot",
    "safe_diagnostic_notes",
    "safe_domain_error_message",
    "safe_exception_category",
    "safe_exception_cause",
    "safe_exception_context",
    "safe_exception_matches",
    "safe_exception_suppresses_context",
    "safe_exception_traceback",
    "safe_external_cause",
    "safe_process_exit_cause",
]
