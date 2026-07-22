"""@brief dbctl 应用层错误与安全诊断链 / Application errors and safe diagnostic chains."""

from __future__ import annotations

import re
import unicodedata
from typing import Final

_POSTGRES_URI_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?i)\bpostgres(?:ql)?(?:\+[a-z0-9_]+)?://[^\s\"'<>]+"
)
"""@brief PostgreSQL URI 的保守脱敏模式 / Conservative redaction pattern for PostgreSQL URIs."""

_SECRET_ASSIGNMENT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?i)(?<![A-Za-z0-9_])([\"']?(?:password|pgpassword|sslpassword|passfile)[\"']?)"
    r"\s*[=:]\s*(?:E?'(?:''|[^'])*'|\"[^\"]*\"|[^\s,;]+)"
)
"""@brief 常见 secret 键值形式 / Common secret key-value forms."""

_SQL_PASSWORD_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?is)\bPASSWORD\s+E?'(?:''|[^'])*'")
"""@brief PostgreSQL PASSWORD 字面量 / PostgreSQL PASSWORD literals."""

_ANSI_ESCAPE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))"
)
"""@brief ANSI/OSC 终端控制序列 / ANSI and OSC terminal-control sequences."""

_MAX_EXTERNAL_DETAIL_LENGTH: Final[int] = 2_000
"""@brief 单个底层诊断详情的最大字符数 / Maximum characters in one external diagnostic detail."""

_SAFE_DIAGNOSTIC_NOTES_ATTRIBUTE: Final[str] = "_dbctl_safe_diagnostic_notes"
"""@brief 显式可信诊断注释的私有属性名 / Private attribute for explicitly trusted diagnostic notes."""


class ApplicationError(RuntimeError):
    """@brief dbctl 用例未能完成 / A dbctl use case could not complete."""


class ExternalDiagnosticError(ApplicationError):
    """@brief 已脱敏且可安全展示的底层异常代理 / Redacted proxy safe to expose as an external cause.

    @note 该异常绝不持有或格式化原始异常消息；其 traceback 可以来自原始异常，
    但终端渲染必须关闭 locals。/ This exception never retains or formats an unsanitized
    exception message. Its traceback may originate from the raw exception, but terminal rendering
    must keep locals disabled.
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

    @param error 只用于提取类型、安全结构字段与 traceback 的底层异常。
    / Raw failure used only for its type, safe structured fields, and traceback.
    @param operation 不含用户秘密的失败动作 / Secret-free failed action.
    @return 带原始栈帧但不带原始异常对象/消息的代理异常。
    / Proxy exception with original frames but no raw exception object or unsanitized message.
    @note 永远不读取 ``str(error)``；任意驱动消息可能包含行数据、触发器文本或未知 secret，
    无法通过启发式正则证明安全。/ ``str(error)`` is never read: arbitrary driver messages may
    contain row data, trigger text, or unknown secrets and cannot be proven safe with heuristics.
    """

    if not isinstance(operation, str) or not operation.strip():
        raise ValueError("external diagnostic operation 不能为空。")
    error_type = f"{type(error).__module__}.{type(error).__qualname__}"
    metadata: list[str] = []
    errno = getattr(error, "errno", None)
    if isinstance(errno, int) and not isinstance(errno, bool):
        metadata.append(f"errno={errno}")
    sqlstate = getattr(error, "sqlstate", None)
    if isinstance(sqlstate, str) and sqlstate:
        metadata.append(f"SQLSTATE={_terminal_safe(sqlstate)[:16]}")
    line = getattr(error, "lineno", None)
    column = getattr(error, "colno", None)
    if isinstance(line, int) and not isinstance(line, bool) and line > 0:
        location = f"line={line}"
        if isinstance(column, int) and not isinstance(column, bool) and column > 0:
            location += f", column={column}"
        metadata.append(location)

    message = f"{operation}；底层异常类型={error_type}"
    if metadata:
        message += f"（{', '.join(metadata)}）"
    proxy = ExternalDiagnosticError(message)
    return proxy.with_traceback(error.__traceback__)


def redact_sensitive_text(text: str) -> str:
    """@brief 脱敏并限制一段外部诊断文本 / Redact and bound a piece of external diagnostic text.

    @param text 外部程序或驱动产生的文本 / Text produced by an external program or driver.
    @return 无终端控制符、DSN 与密码字面量的有界单行文本。
    / Bounded single-line text without terminal controls, DSNs, or password literals.
    """

    if not isinstance(text, str):
        return ""
    rendered = _ANSI_ESCAPE_PATTERN.sub("", text)
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
    existing = getattr(error, _SAFE_DIAGNOSTIC_NOTES_ATTRIBUTE, ())
    notes = existing if isinstance(existing, tuple) else ()
    try:
        setattr(error, _SAFE_DIAGNOSTIC_NOTES_ATTRIBUTE, (*notes, normalized))
    except Exception:
        # 诊断增强永远不能遮蔽原始业务失败 / Diagnostic enrichment must never mask the failure.
        return


def safe_diagnostic_notes(error: BaseException) -> tuple[str, ...]:
    """@brief 读取唯一允许进入终端的显式诊断注释 / Read explicitly approved terminal notes.

    @param error 当前异常 / Current exception.
    @return 已再次净化的不可变注释序列 / Re-sanitized immutable notes.
    """

    notes = getattr(error, _SAFE_DIAGNOSTIC_NOTES_ATTRIBUTE, ())
    if not isinstance(notes, tuple):
        return ()
    return tuple(
        rendered
        for note in notes
        if isinstance(note, str) and (rendered := redact_sensitive_text(note))
    )


def _terminal_safe(text: str) -> str:
    """@brief 移除可伪造终端输出的控制字符 / Remove control characters capable of terminal-output spoofing.

    @param text 待净化文本 / Candidate text.
    @return 保留换行与制表符之外可打印内容的文本 / Text retaining printable content plus tabs/newlines.
    """

    return "".join(
        character
        for character in text
        if character in {"\n", "\t"}
        or unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
    )


__all__ = [
    "ApplicationError",
    "BootstrapExecutionError",
    "DatabaseAlreadyExistsError",
    "DbctlConfigurationError",
    "ExternalDiagnosticError",
    "MigrationExecutionError",
    "RetentionExecutionError",
    "ShellExecutionError",
    "add_safe_diagnostic_note",
    "redact_sensitive_text",
    "safe_diagnostic_notes",
    "safe_external_cause",
]
